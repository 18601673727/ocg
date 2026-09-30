/**
 * Canonical projection store for the durable Project -> Job -> Attempt ->
 * Executor -> Call read model.
 */

import {
  CANONICAL_API_VERSION,
  CANONICAL_STREAM_ID,
  assembleJobExecutionFromProjection,
  projectCanonicalSnapshot,
  type CanonicalCommandAck,
  type CanonicalDiagnostic,
  type CanonicalDiagnosticCode,
  type CanonicalExecutionProjection,
  type CanonicalProjectionResult,
  type CanonicalSnapshotInput,
  type CanonicalState,
  type CanonicalSyncStatus,
} from "./canonical-envelope";
import type { ProjectId } from "../project/domain";
import type { CanonicalJobEvent } from "../contracts";
import type { JobExecution } from "../execution/domain";
import { boundDiagnostics, compareFilteredSequence, compareGeneration, isSeen } from "./event-gate";

export {
  CANONICAL_API_VERSION,
  CANONICAL_STREAM_ID,
  projectCanonicalSnapshot,
  assembleJobExecutionFromProjection,
};
export type {
  CanonicalCommandAck,
  CanonicalDiagnostic,
  CanonicalDiagnosticCode,
  CanonicalExecutionProjection,
  CanonicalProjectionResult,
  CanonicalSnapshotInput,
  CanonicalState,
  CanonicalSyncStatus,
};

export type CanonicalBackendEvent = CanonicalJobEvent;

export function createCanonicalState(): CanonicalState {
  return {
    status: "uninitialized",
    streamId: CANONICAL_STREAM_ID,
    generation: 0,
    cursor: 0,
    projectId: null,
    jobId: null,
    projection: null,
    seenEventIds: [],
    diagnostics: [],
    commandAcks: {},
    resyncRequired: false,
  };
}

function diagnostic(
  code: CanonicalDiagnosticCode,
  message: string,
  severity: CanonicalDiagnostic["severity"] = "warning",
  extra: { eventId?: string; sequence?: number } = {},
): CanonicalDiagnostic {
  return { code, severity, message, ...extra };
}

function withDiagnostics(state: CanonicalState, entries: readonly CanonicalDiagnostic[]): CanonicalState {
  return { ...state, diagnostics: boundDiagnostics([...state.diagnostics, ...entries]) };
}

function rejected(state: CanonicalState, result: Extract<CanonicalProjectionResult, { ok: false }>): CanonicalState {
  return withDiagnostics(state, [diagnostic("snapshot-rejected", result.issue.message, "error")]);
}

export function applyCanonicalSnapshot(state: CanonicalState, input: CanonicalSnapshotInput): CanonicalState {
  const generation = input.generation ?? state.generation;
  if (compareGeneration(generation, state.generation) === "stale") {
    return withDiagnostics(state, [diagnostic("snapshot-stale", `Ignored canonical snapshot from old generation ${generation}.`)]);
  }
  if (state.projection !== null && state.projectId === input.projectId) {
    const payload = input.payload && typeof input.payload === "object" ? input.payload as Record<string, unknown> : null;
    if (typeof payload?.cursor === "number" && payload.cursor < state.cursor) {
      return withDiagnostics(state, [diagnostic("snapshot-stale", `Ignored canonical snapshot at cursor ${payload.cursor} (current ${state.cursor}).`)]);
    }
  }
  const result = projectCanonicalSnapshot(input.payload, input.projectId);
  if (!result.ok) return rejected(state, result);
  const projection = result.projection;
  return {
    ...state,
    status: "live",
    generation,
    cursor: projection.cursor,
    projectId: input.projectId,
    jobId: projection.jobId,
    projection,
    seenEventIds: [],
    diagnostics: [],
    resyncRequired: false,
  };
}

export function applyCanonicalCommandAck(state: CanonicalState, ack: CanonicalCommandAck): CanonicalState {
  return { ...state, commandAcks: { ...state.commandAcks, [ack.commandId]: ack } };
}

/**
 * Apply the canonical event tail for one Job.
 *
 * The canonical store cannot reduce an event into its projection: the event
 * carries only an opaque `kind`/`payload` pair, and the projection is built
 * exclusively from a whole authoritative snapshot. So a newer event is never
 * applied here — it sets `resyncRequired` and leaves the cursor where the
 * projection left it. The caller refetches the snapshot, which advances cursor
 * and projection together. This is also why a Job-filtered sequence that skips
 * numbers belonging to other Jobs is not treated as a gap.
 */
export function applyCanonicalEvents(
  state: CanonicalState,
  events: readonly CanonicalBackendEvent[],
  options: { generation?: number; projectId: ProjectId },
): CanonicalState {
  const generation = options.generation ?? state.generation;
  const generationDecision = compareGeneration(generation, state.generation);
  if (generationDecision === "stale") return withDiagnostics(state, [diagnostic("generation-stale", `Ignored canonical events from old generation ${generation}.`)]);
  if (generationDecision === "unknown") return withDiagnostics(state, [diagnostic("generation-unknown", `Canonical events from generation ${generation} need a new snapshot.`)]);
  if (state.projection === null) return withDiagnostics(state, [diagnostic("resync-required", "Canonical event arrived before an authoritative snapshot.")]);
  if (state.projectId !== options.projectId) return withDiagnostics(state, [diagnostic("project-mismatch", `Canonical events target Project "${options.projectId}".`)]);

  let next = state;
  for (const event of [...events].sort((left, right) => left.sequence - right.sequence)) {
    if (event.api_version !== CANONICAL_API_VERSION) {
      next = withDiagnostics(next, [diagnostic("snapshot-rejected", `Unsupported canonical event API version "${String(event.api_version)}".`, "error", { eventId: event.event_id, sequence: event.sequence })]);
      continue;
    }
    if (event.project_id !== options.projectId || event.job_id !== next.jobId) {
      next = withDiagnostics(next, [diagnostic("project-mismatch", "Ignored canonical event outside the installed Project and Job.", "warning", { eventId: event.event_id, sequence: event.sequence })]);
      continue;
    }
    if (isSeen(next.seenEventIds, event.event_id)) {
      next = withDiagnostics(next, [diagnostic("duplicate-event", `Ignored duplicate canonical event ${event.event_id}.`, "info", { eventId: event.event_id, sequence: event.sequence })]);
      continue;
    }
    if (compareFilteredSequence(event.sequence, next.cursor) === "stale") {
      next = withDiagnostics(next, [diagnostic("sequence-stale", `Ignored stale canonical sequence ${event.sequence}.`, "warning", { eventId: event.event_id, sequence: event.sequence })]);
      continue;
    }
    // The event is real, belongs to this Job, and is newer than the projection
    // the store holds. This store projects whole authoritative snapshots: the
    // canonical event carries only an opaque `kind`/`payload` pair, so the
    // projection cannot be advanced from it. Advancing the cursor alone would
    // leave the cursor ahead of a stale projection, so the event is refused and
    // an authoritative snapshot refresh is demanded instead. The snapshot
    // advances the cursor together with the projection it belongs to. The
    // stream is Job-filtered, so a sequence beyond `cursor + 1` is a skipped
    // foreign Job, not a gap.
    next = withDiagnostics(next, [diagnostic("resync-required", `Canonical event ${event.event_id} at sequence ${event.sequence} is newer than snapshot cursor ${next.cursor}; refetch the canonical snapshot.`, "warning", { eventId: event.event_id, sequence: event.sequence })]);
    next = { ...next, resyncRequired: true, status: "reconnecting" };
    break;
  }
  return { ...next, status: next.resyncRequired ? "reconnecting" : "live" };
}

export type CanonicalSelectors = {
  projection: CanonicalExecutionProjection | null;
  execution: JobExecution | null;
  attemptHistory: JobExecution["attempts"];
};

export function selectCanonical(state: CanonicalState): CanonicalSelectors {
  const execution = state.projection ? assembleJobExecutionFromProjection(state.projection) : null;
  return {
    projection: state.projection,
    execution,
    attemptHistory: execution?.attempts ?? [],
  };
}

export function selectJobExecution(state: CanonicalState): JobExecution | null {
  return selectCanonical(state).execution;
}
