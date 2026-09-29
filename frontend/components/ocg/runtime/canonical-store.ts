/**
 * Canonical projection store: the frontend's read model of OCG durable state.
 *
 * The store owns *presentation* state only. Every canonical value it holds came
 * from a validated backend snapshot or event; it never dispatches, completes,
 * replaces or edits a Run. Ordering rules mirror the existing runtime contract:
 * generation and sequence never move backwards, duplicate event identities are
 * idempotent, a gap forces a resync, and a payload for another Project is
 * rejected rather than rendered.
 */

import {
  CANONICAL_API_VERSION,
  CANONICAL_STREAM_ID,
  projectCanonicalSnapshot,
  type CanonicalProjectionResult,
  type CanonicalWorkProjection,
} from "./canonical-envelope";
import type { ProjectId } from "../project/domain";
import type { MissionExecution } from "../execution/domain";
import type { CanonicalWorkEvent } from "../contracts";
import { toMissionExecution } from "./canonical-envelope";
import { boundDiagnostics, compareGeneration, compareSequence, isSeen, rememberId } from "./event-gate";
import type { RuntimeDiagnosticSeverity } from "./runtime-envelope";
import type { RuntimeSyncStatus } from "./reconciler";
import { isRecord } from "@/lib/narrow";

/** The canonical tail syncs with the same vocabulary as the event stream. */
export type CanonicalSyncStatus = RuntimeSyncStatus;

export type CanonicalDiagnosticCode =
  | "snapshot-rejected"
  | "snapshot-stale"
  | "generation-stale"
  | "generation-unknown"
  | "sequence-stale"
  | "sequence-gap"
  | "duplicate-event"
  | "project-mismatch"
  | "resync-required"
  | "command-rejected";

export type CanonicalDiagnostic = {
  code: CanonicalDiagnosticCode;
  severity: RuntimeDiagnosticSeverity;
  message: string;
  eventId?: string;
  sequence?: number;
};

export type CanonicalCommandAck = {
  commandId: string;
  kind: "project-import" | "global-config" | "project-defaults" | "mission-config";
  accepted: boolean;
  revision?: number;
  message: string;
};

/**
 * A canonical work event as the backend sends it.
 *
 * This is the generated projection of `CanonicalWorkEvent` in
 * `src/orchestration/canonical_control.rs`, not a second declaration of it. The
 * control client hands the store exactly what the decoder produced.
 */
export type CanonicalBackendEvent = CanonicalWorkEvent;

export type CanonicalState = {
  status: CanonicalSyncStatus;
  streamId: string;
  /** Bumped on reconnect; events from an older generation are refused. */
  generation: number;
  /** Highest applied canonical sequence, or 0 when no snapshot is installed. */
  cursor: number;
  projectId: ProjectId | null;
  missionId: string | null;
  projection: CanonicalWorkProjection | null;
  seenEventIds: string[];
  diagnostics: CanonicalDiagnostic[];
  commandAcks: Record<string, CanonicalCommandAck>;
  resyncRequired: boolean;
};

export function createCanonicalState(): CanonicalState {
  return {
    status: "uninitialized",
    streamId: CANONICAL_STREAM_ID,
    generation: 0,
    cursor: 0,
    projectId: null,
    missionId: null,
    projection: null,
    seenEventIds: [],
    diagnostics: [],
    commandAcks: {},
    resyncRequired: false,
  };
}

function withDiagnostics(state: CanonicalState, diagnostics: readonly CanonicalDiagnostic[]): CanonicalState {
  return { ...state, diagnostics: boundDiagnostics([...state.diagnostics, ...diagnostics]) };
}

function diagnostic(
  code: CanonicalDiagnosticCode,
  message: string,
  severity: CanonicalDiagnostic["severity"] = "warning",
  extra: { eventId?: string; sequence?: number } = {},
): CanonicalDiagnostic {
  return { code, severity, message, ...extra };
}

function noteRejected(state: CanonicalState, result: Extract<CanonicalProjectionResult, { ok: false }>): CanonicalState {
  return withDiagnostics(state, [
    diagnostic("snapshot-rejected", result.issue.message, "error"),
  ]);
}

export type CanonicalSnapshotInput = {
  payload: unknown;
  projectId: ProjectId;
  generation?: number;
};

/**
 * Install a canonical baseline. The generation is the reconnect epoch, not the
 * Mission identity: a reconnect always installs a new snapshot, never a merge.
 */
export function applyCanonicalSnapshot(state: CanonicalState, input: CanonicalSnapshotInput): CanonicalState {
  const generation = input.generation ?? state.generation;
  if (compareGeneration(generation, state.generation) === "stale") {
    return withDiagnostics(state, [
      diagnostic("snapshot-stale", `Ignored canonical snapshot from old generation ${generation}.`),
    ]);
  }
  if (state.projection !== null && state.projectId === input.projectId) {
    const payload = isRecord(input.payload) ? input.payload : null;
    const incomingCursor = payload?.cursor;
    if (typeof incomingCursor === "number" && incomingCursor < state.cursor) {
      return withDiagnostics(state, [
        diagnostic("snapshot-stale", `Ignored canonical snapshot at cursor ${incomingCursor} (current ${state.cursor}).`),
      ]);
    }
  }
  const result = projectCanonicalSnapshot(input.payload, input.projectId);
  if (!result.ok) return noteRejected(state, result);
  return {
    status: "live",
    streamId: CANONICAL_STREAM_ID,
    generation,
    cursor: result.projection.cursor,
    projectId: input.projectId,
    missionId: result.projection.missionId,
    projection: result.projection,
    // A new baseline invalidates the seen set: the cursor already covers
    // everything at or below it.
    seenEventIds: [],
    diagnostics: [],
    commandAcks: state.commandAcks,
    resyncRequired: false,
  };
}

/** Record a command acknowledgement by stable command identity. */
export function applyCanonicalCommandAck(state: CanonicalState, ack: CanonicalCommandAck): CanonicalState {
  return {
    ...state,
    commandAcks: { ...state.commandAcks, [ack.commandId]: ack },
  };
}

/**
 * Apply a batch of canonical events strictly after the installed baseline.
 *
 * A gap requires a fresh snapshot rather than an optimistic merge, so a
 * reconnect can never present a partially applied Run history.
 */
export function applyCanonicalEvents(
  state: CanonicalState,
  events: readonly CanonicalBackendEvent[],
  options: { generation?: number; projectId: ProjectId } ,
): CanonicalState {
  const generation = options.generation ?? state.generation;
  const generationDecision = compareGeneration(generation, state.generation);
  if (generationDecision === "stale") {
    return withDiagnostics(state, [
      diagnostic("generation-stale", `Ignored canonical events from old generation ${generation}.`),
    ]);
  }
  if (generationDecision === "unknown") {
    return withDiagnostics(state, [
      diagnostic(
        "generation-unknown",
        `Canonical events from generation ${generation} need a new snapshot (current ${state.generation}).`,
      ),
    ]);
  }
  if (state.projection === null) {
    return withDiagnostics(state, [
      diagnostic("resync-required", "Canonical event arrived before an authoritative snapshot."),
    ]);
  }
  if (state.projectId !== options.projectId) {
    return withDiagnostics(state, [
      diagnostic("project-mismatch", `Canonical events target Project "${options.projectId}".`),
    ]);
  }
  const ordered = [...events].sort((a, b) => a.sequence - b.sequence);
  let next = state;
  for (const event of ordered) {
    if (event.api_version !== CANONICAL_API_VERSION) {
      next = withDiagnostics(next, [
        diagnostic("snapshot-rejected", `Unsupported canonical event API version "${String(event.api_version)}".`, "error", {
          eventId: event.event_id,
          sequence: event.sequence,
        }),
      ]);
      continue;
    }
    if (event.project_id !== options.projectId) {
      next = withDiagnostics(next, [
        diagnostic("project-mismatch", `Ignored canonical event for Project "${event.project_id}".`, "warning", {
          eventId: event.event_id,
          sequence: event.sequence,
        }),
      ]);
      continue;
    }
    if (event.mission_id !== state.missionId) {
      next = withDiagnostics(next, [
        diagnostic("project-mismatch", `Ignored canonical event for Mission "${event.mission_id}".`, "warning", {
          eventId: event.event_id,
          sequence: event.sequence,
        }),
      ]);
      continue;
    }
    if (isSeen(next.seenEventIds, event.event_id)) {
      next = withDiagnostics(next, [
        diagnostic("duplicate-event", `Ignored duplicate canonical event ${event.event_id}.`, "info", {
          eventId: event.event_id,
          sequence: event.sequence,
        }),
      ]);
      continue;
    }
    const decision = compareSequence(event.sequence, next.cursor);
    if (decision === "stale") {
      next = withDiagnostics(next, [
        diagnostic("sequence-stale", `Ignored stale canonical sequence ${event.sequence} (cursor ${next.cursor}).`, "warning", {
          eventId: event.event_id,
          sequence: event.sequence,
        }),
      ]);
      continue;
    }
    if (decision === "gap") {
      next = withDiagnostics(next, [
        diagnostic("sequence-gap", `Canonical sequence gap: expected ${next.cursor + 1}, received ${event.sequence}.`, "warning", {
          eventId: event.event_id,
          sequence: event.sequence,
        }),
      ]);
      next = { ...next, resyncRequired: true, status: "reconnecting" };
      break;
    }
    next = {
      ...next,
      cursor: event.sequence,
      seenEventIds: rememberId(next.seenEventIds, event.event_id),
    };
  }
  return { ...next, status: next.resyncRequired ? "reconnecting" : "live" };
}

export type CanonicalSelectors = {
  projection: CanonicalWorkProjection | null;
  execution: MissionExecution | null;
  /** Runs that no longer hold authority, retained as evidence only. */
  fencedRuns: Array<{ runId: number; nodeId: number; dispatchId: string }>;
  /** Dispatches that have not yet delivered a result. */
  pendingDispatchCount: number;
  hasPassingVerification: boolean;
  missionCompleted: boolean;
};

/** Read-only selectors over the canonical projection. */
export function selectCanonical(state: CanonicalState, title: string): CanonicalSelectors {
  const projection = state.projection;
  if (projection === null) {
    return {
      projection: null,
      execution: null,
      fencedRuns: [],
      pendingDispatchCount: 0,
      hasPassingVerification: false,
      missionCompleted: false,
    };
  }
  return {
    projection,
    execution: toMissionExecution(projection, title),
    fencedRuns: projection.runs
      .filter((run) => run.state === "fenced")
      .map((run) => ({
        runId: run.run_id,
        nodeId: run.node_id,
        dispatchId: run.witness?.dispatch_id ?? "",
      })),
    pendingDispatchCount: projection.runs.filter((run) => run.state === "active").length,
    hasPassingVerification: projection.verifications.some((record) => record.passed),
    missionCompleted: projection.workNodes.some(
      (node) => node.node_id === projection.rootNodeId && node.state === "completed",
    ),
  };
}
