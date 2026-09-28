/**
 * Pure runtime reconciler.
 *
 * `reconcileSnapshot` installs an authoritative baseline; `reconcileEvent`
 * applies one validated envelope to that baseline. Both are total, pure, and
 * identity-preserving when nothing changed, so they can back a single external
 * store shared by React and tests.
 *
 * Rules enforced here:
 * - protocol/schema validation happens before any mutation;
 * - a snapshot of an older generation or an older cursor is stale;
 * - duplicate event IDs are idempotent and sequence gaps require a resync;
 * - lower/older sequences can never roll state backwards;
 * - known session events whose Project scope does not match fixture ownership
 *   are consumed without mutating the snapshot;
 * - entity-missing/malformed payloads yield diagnostics, not corruption.
 */

import type { ChatMessage, Mission } from "../types";
import type { ProjectId } from "../project/domain";
import { PROJECTS } from "../project/domain";
import { projectLedgerMissionIds, projectSessionIds } from "../project/fixtures";
import type { MissionLaunchResult, RuntimeSnapshot, ScenarioId } from "./runtime-types";
import { boundActivities, boundTimeline } from "./observability";
import { normalizeLogEntry } from "../logs/domain";
import type {
  AnyRuntimeEnvelope,
  RuntimeCursor,
  RuntimeDiagnostic,
  RuntimeDiagnosticCode,
  RuntimeEventId,
  RuntimeGeneration,
  RuntimeResumeCursor,
  RuntimeStreamId,
} from "./runtime-envelope";
import { validateRuntimeEnvelope } from "./runtime-envelope";
import type { RuntimeSnapshotEnvelope, RuntimeSnapshotScope } from "./runtime-snapshot";
import { emptyRuntimeSnapshot, sameSnapshotScope, validateRuntimeSnapshotEnvelope } from "./runtime-snapshot";

export type RuntimeSyncStatus =
  | "uninitialized"
  | "loading-snapshot"
  | "live"
  | "reconnecting"
  | "stale"
  | "error";

export type RuntimeSyncState = {
  status: RuntimeSyncStatus;
  scope: RuntimeSnapshotScope | null;
  cursor: RuntimeCursor | null;
  streamId: RuntimeStreamId | null;
  generation: RuntimeGeneration | null;
  lastEventId: RuntimeEventId | null;
  snapshotRevision?: string;
  resumeCursor?: RuntimeResumeCursor;
  lastSuccessfulSyncAt?: string;
  /** True after a gap or an unknown generation; a snapshot is required. */
  resyncRequired: boolean;
  diagnostics: RuntimeDiagnostic[];
  seenEventIds: string[];
  seenDeltaKeys: string[];
  /** Latest command acknowledgement by stable command identity. */
  commandResults: Record<string, MissionLaunchResult>;
  /** Runtime ownership learned from scoped snapshots/events for dynamic sessions. */
  sessionProjects: Record<string, ProjectId>;
};

export type RuntimeState = {
  snapshot: RuntimeSnapshot;
  sync: RuntimeSyncState;
};

export const MAX_RUNTIME_DIAGNOSTICS = 50;
const MAX_SEEN_IDS = 500;

export function createUninitializedRuntimeState(scenario: ScenarioId): RuntimeState {
  return {
    snapshot: emptyRuntimeSnapshot(scenario),
    sync: {
      status: "uninitialized",
      scope: null,
      cursor: null,
      streamId: null,
      generation: null,
      lastEventId: null,
      resyncRequired: false,
      diagnostics: [],
      seenEventIds: [],
      seenDeltaKeys: [],
      commandResults: {},
      sessionProjects: {},
    },
  };
}

export function markLoadingSnapshot(state: RuntimeState): RuntimeState {
  if (state.sync.status === "loading-snapshot") return state;
  return { snapshot: state.snapshot, sync: { ...state.sync, status: "loading-snapshot" } };
}

export function markReconnecting(state: RuntimeState): RuntimeState {
  if (state.sync.status === "reconnecting") return state;
  return { snapshot: state.snapshot, sync: { ...state.sync, status: "reconnecting", resyncRequired: true } };
}

/** A degraded status is anything other than a healthy uninitialized/live sync. */
export function isDegradedSyncStatus(status: RuntimeSyncStatus | null | undefined): boolean {
  return status === "stale" || status === "error" || status === "reconnecting" || status === "loading-snapshot";
}

/** Preserve a diagnostic without allowing an invalid payload to reach the reducer. */
export function recordRuntimeDiagnostic(
  state: RuntimeState,
  diagnostic: RuntimeDiagnostic,
  status?: RuntimeSyncStatus,
): RuntimeState {
  return {
    snapshot: state.snapshot,
    sync: {
      ...state.sync,
      ...(status !== undefined ? { status } : {}),
      diagnostics: boundDiagnostics([...state.sync.diagnostics, diagnostic]),
    },
  };
}

/* -------------------------------------------------------------------------- */
/* Helpers                                                                    */
/* -------------------------------------------------------------------------- */

function boundDiagnostics(diagnostics: RuntimeDiagnostic[]): RuntimeDiagnostic[] {
  return diagnostics.length <= MAX_RUNTIME_DIAGNOSTICS
    ? diagnostics
    : diagnostics.slice(diagnostics.length - MAX_RUNTIME_DIAGNOSTICS);
}

function boundSeen(seen: readonly string[], next?: string): string[] {
  const combined = next !== undefined ? [...seen, next] : [...seen];
  return combined.length <= MAX_SEEN_IDS ? combined : combined.slice(combined.length - MAX_SEEN_IDS);
}

function withDiagnostics(state: RuntimeState, diagnostics: RuntimeDiagnostic[]): RuntimeState {
  if (diagnostics.length === 0) return state;
  return {
    snapshot: state.snapshot,
    sync: { ...state.sync, diagnostics: boundDiagnostics([...state.sync.diagnostics, ...diagnostics]) },
  };
}

function setSyncStatus(state: RuntimeState, status: RuntimeSyncStatus, diagnostic: RuntimeDiagnostic): RuntimeState {
  return {
    snapshot: state.snapshot,
    sync: {
      ...state.sync,
      status,
      diagnostics: boundDiagnostics([...state.sync.diagnostics, diagnostic]),
    },
  };
}

function requireResync(
  state: RuntimeState,
  diagnostic: RuntimeDiagnostic,
  options: { resumable?: boolean } = {},
): RuntimeState {
  if (state.sync.resyncRequired && state.sync.status === "stale") {
    return withDiagnostics(state, [diagnostic]);
  }
  void options;
  return {
    snapshot: state.snapshot,
    sync: {
      ...state.sync,
      status: "stale",
      resyncRequired: true,
      diagnostics: boundDiagnostics([...state.sync.diagnostics, diagnostic]),
    },
  };
}

/** Consume one in-sequence envelope without mutating the snapshot. */
function consumeEnvelope(
  state: RuntimeState,
  envelope: AnyRuntimeEnvelope,
  diagnostics: RuntimeDiagnostic[],
  deltaKey?: string,
): RuntimeState {
  const sync = state.sync;
  return {
    snapshot: state.snapshot,
    sync: {
      ...sync,
      cursor: { streamId: envelope.streamId, sequence: envelope.sequence },
      lastEventId: envelope.eventId,
      lastSuccessfulSyncAt: envelope.occurredAt,
      seenEventIds: boundSeen(sync.seenEventIds, envelope.eventId),
      seenDeltaKeys: deltaKey !== undefined ? boundSeen(sync.seenDeltaKeys, deltaKey) : sync.seenDeltaKeys,
      diagnostics: boundDiagnostics([...sync.diagnostics, ...diagnostics]),
    },
  };
}

function deltaDedupKey(envelope: AnyRuntimeEnvelope): string | undefined {
  if (envelope.type !== "conversation.message-delta") return undefined;
  const { turnId, deltaSequence } = envelope.payload;
  if (turnId === undefined || deltaSequence === undefined) return undefined;
  return `${envelope.streamId}\u0000${turnId}\u0000${deltaSequence}`;
}

function ownerProjectForSession(sessionId: string): ProjectId | null {
  for (const project of PROJECTS) {
    if (projectSessionIds(project.id).includes(sessionId)) return project.id;
  }
  return null;
}

function ownerProjectForMission(missionId: string): ProjectId | null {
  for (const project of PROJECTS) {
    if (projectLedgerMissionIds(project.id).includes(missionId)) return project.id;
  }
  return null;
}

/** True when a known entity's Project scope contradicts the envelope scope. */
function hasScopeViolation(sync: RuntimeSyncState, envelope: AnyRuntimeEnvelope): boolean {
  if (envelope.projectId === null) return false;
  if (!envelope.sessionId) return false;
  const owner = sync.sessionProjects[envelope.sessionId] ?? ownerProjectForSession(envelope.sessionId);
  return owner !== null && owner !== envelope.projectId;
}

function hasPayloadScopeViolation(sync: RuntimeSyncState, envelope: AnyRuntimeEnvelope): boolean {
  if (envelope.projectId === null) return false;
  if (hasScopeViolation(sync, envelope)) return true;
  if (envelope.type === "attention.updated") {
    return envelope.payload.item.projectId !== undefined && envelope.payload.item.projectId !== envelope.projectId;
  }
  if (envelope.type === "ledger.entry-added" || envelope.type === "ledger.entry-updated") {
    const owner = ownerProjectForMission(envelope.payload.entry.missionId);
    return owner !== null && owner !== envelope.projectId;
  }
  return false;
}

/* -------------------------------------------------------------------------- */
/* Snapshot reconciliation                                                    */
/* -------------------------------------------------------------------------- */

export function reconcileSnapshot(
  state: RuntimeState,
  envelope: RuntimeSnapshotEnvelope,
): RuntimeState {
  const validation = validateRuntimeSnapshotEnvelope(envelope);
  if (!validation.ok) {
    return setSyncStatus(state, "error", validation.diagnostic);
  }
  envelope = validation.envelope;
  if (envelope.snapshot.logs !== undefined && envelope.snapshot.logs.length > 0) {
    const normalizedLogs = envelope.snapshot.logs.map(normalizeLogEntry);
    const logsChanged = normalizedLogs.some((entry, index) => JSON.stringify(entry) !== JSON.stringify(envelope.snapshot.logs![index]));
    if (!logsChanged) return installValidatedSnapshot(state, envelope);
    envelope = {
      ...envelope,
      snapshot: {
        ...envelope.snapshot,
        logs: normalizedLogs,
      },
    };
  }

  return installValidatedSnapshot(state, envelope);
}

function installValidatedSnapshot(state: RuntimeState, envelope: RuntimeSnapshotEnvelope): RuntimeState {

  const sync = state.sync;
  const hasBaseline =
    sync.scope !== null && sync.streamId !== null && sync.generation !== null && sync.cursor !== null;

  if (hasBaseline) {
    if (envelope.generation < sync.generation!) {
      return withDiagnostics(state, [{
        code: "generation-stale",
        severity: "warning",
        message: `Ignored snapshot from stale generation ${envelope.generation} (current ${sync.generation}).`,
      }]);
    }
    if (envelope.generation === sync.generation!) {
      if (!sameSnapshotScope(envelope.scope, sync.scope!)) {
        return withDiagnostics(state, [{
          code: "snapshot-scope-mismatch",
          severity: "warning",
          message: "Ignored snapshot whose Project scope does not match the installed baseline.",
        }]);
      }
      if (envelope.streamId !== sync.streamId!) {
        return withDiagnostics(state, [{
          code: "stream-mismatch",
          severity: "warning",
          message: `Ignored snapshot from stream "${envelope.streamId}" (current "${sync.streamId}").`,
        }]);
      }
      if (envelope.cursor.sequence < sync.cursor!.sequence) {
        return withDiagnostics(state, [{
          code: "snapshot-stale",
          severity: "warning",
          message: `Ignored stale snapshot at sequence ${envelope.cursor.sequence} (current ${sync.cursor!.sequence}).`,
        }]);
      }
    }
  }

  const nextSync: RuntimeSyncState = {
    status: "live",
    scope: envelope.scope,
    cursor: envelope.cursor,
    streamId: envelope.streamId,
    generation: envelope.generation,
    lastEventId: null,
    ...(envelope.snapshotRevision !== undefined ? { snapshotRevision: envelope.snapshotRevision } : {}),
    ...(envelope.resumeCursor !== undefined ? { resumeCursor: envelope.resumeCursor } : {}),
    ...(envelope.occurredAt !== undefined
      ? { lastSuccessfulSyncAt: envelope.occurredAt }
      : sync.lastSuccessfulSyncAt !== undefined
        ? { lastSuccessfulSyncAt: sync.lastSuccessfulSyncAt }
        : {}),
    resyncRequired: false,
    diagnostics: sync.diagnostics,
    seenEventIds: [],
    seenDeltaKeys: [],
    commandResults: {},
    sessionProjects: Object.fromEntries(
      envelope.snapshot.sessions.flatMap((session) => {
        const owner = envelope.scope.kind === "project"
          ? envelope.scope.projectId
          : ownerProjectForSession(session.id);
        return owner ? [[session.id, owner] as const] : [];
      }),
    ),
  };

  return { snapshot: envelope.snapshot, sync: nextSync };
}

/* -------------------------------------------------------------------------- */
/* Event reconciliation                                                       */
/* -------------------------------------------------------------------------- */

export function reconcileEvent(state: RuntimeState, envelope: AnyRuntimeEnvelope): RuntimeState {
  const validation = validateRuntimeEnvelope(envelope);
  if (!validation.ok) {
    return setSyncStatus(state, "error", {
      ...validation.diagnostic,
      eventId: typeof envelope === "object" && envelope !== null && "eventId" in envelope && typeof envelope.eventId === "string" ? envelope.eventId : undefined,
      sequence: typeof envelope === "object" && envelope !== null && "sequence" in envelope && typeof envelope.sequence === "number" ? envelope.sequence : undefined,
    });
  }
  envelope = validation.envelope;

  const sync = state.sync;
  if (sync.status === "uninitialized" || sync.cursor === null || sync.streamId === null || sync.generation === null) {
    return requireResync(state, {
      code: "no-snapshot",
      severity: "warning",
      message: "Runtime event arrived before an authoritative snapshot was installed.",
      eventId: envelope.eventId,
      sequence: envelope.sequence,
    });
  }

  if (envelope.generation < sync.generation) {
    return withDiagnostics(state, [{
      code: "generation-stale",
      severity: "warning",
      message: `Ignored late event from old generation ${envelope.generation} (current ${sync.generation}).`,
      eventId: envelope.eventId,
      sequence: envelope.sequence,
    }]);
  }

  if (envelope.generation > sync.generation) {
    return requireResync(state, {
      code: "generation-unknown",
      severity: "warning",
      message: `Event generation ${envelope.generation} is newer than the installed baseline; snapshot required.`,
      eventId: envelope.eventId,
      sequence: envelope.sequence,
    });
  }

  if (envelope.streamId !== sync.streamId) {
    return requireResync(state, {
      code: "stream-mismatch",
      severity: "warning",
      message: `Event from stream "${envelope.streamId}" does not match "${sync.streamId}".`,
      eventId: envelope.eventId,
      sequence: envelope.sequence,
    });
  }

  // Duplicate event identity is a pure idempotent no-op.
  if (sync.seenEventIds.includes(envelope.eventId)) {
    return state;
  }

  if (envelope.sequence <= sync.cursor.sequence) {
    return withDiagnostics(state, [{
      code: "sequence-stale",
      severity: "warning",
      message: `Ignored stale sequence ${envelope.sequence} (cursor ${sync.cursor.sequence}).`,
      eventId: envelope.eventId,
      sequence: envelope.sequence,
    }]);
  }

  if (envelope.sequence > sync.cursor.sequence + 1) {
    return requireResync(state, {
      code: "sequence-gap",
      severity: "warning",
      message: `Sequence gap: expected ${sync.cursor.sequence + 1}, received ${envelope.sequence}.`,
      eventId: envelope.eventId,
      sequence: envelope.sequence,
    });
  }

  // A command acknowledgement is scoped to the command's requested Project,
  // even when the command is rejected because that Project does not own the
  // referenced session. It must remain observable rather than being mistaken
  // for a spoofed domain update.
  if (envelope.type !== "mission.launch-updated" && hasPayloadScopeViolation(sync, envelope)) {
    return consumeEnvelope(state, envelope, [{
      code: "project-scope-mismatch",
      severity: "warning",
      message: `Ignored event for session "${envelope.sessionId}" scoped to Project "${envelope.projectId}".`,
      eventId: envelope.eventId,
      sequence: envelope.sequence,
      sessionId: envelope.sessionId,
    }]);
  }

  const deltaKey = deltaDedupKey(envelope);
  if (deltaKey !== undefined && sync.seenDeltaKeys.includes(deltaKey)) {
    return consumeEnvelope(state, envelope, [{
      code: "duplicate-delta",
      severity: "info",
      message: "Ignored duplicate message delta for an already-applied turn/delta sequence.",
      eventId: envelope.eventId,
      sequence: envelope.sequence,
    }], deltaKey);
  }

  const applied = applyEnvelopeToSnapshot(state.snapshot, envelope);
  const diagnostics = boundDiagnostics([...sync.diagnostics, ...applied.diagnostics]);
  return {
    snapshot: applied.snapshot,
    sync: {
      ...sync,
      status: "live",
      resyncRequired: false,
      cursor: { streamId: envelope.streamId, sequence: envelope.sequence },
      lastEventId: envelope.eventId,
      lastSuccessfulSyncAt: envelope.occurredAt,
      seenEventIds: boundSeen(sync.seenEventIds, envelope.eventId),
      seenDeltaKeys: deltaKey !== undefined ? boundSeen(sync.seenDeltaKeys, deltaKey) : sync.seenDeltaKeys,
      commandResults: envelope.type === "mission.launch-updated"
        ? { ...sync.commandResults, [envelope.payload.result.commandId]: envelope.payload.result }
        : sync.commandResults,
      sessionProjects: envelope.sessionId && envelope.projectId !== null && !sync.sessionProjects[envelope.sessionId]
        ? { ...sync.sessionProjects, [envelope.sessionId]: envelope.projectId }
        : sync.sessionProjects,
      diagnostics,
    },
  };
}

/* -------------------------------------------------------------------------- */
/* Total reducer for the current event union                                  */
/* -------------------------------------------------------------------------- */

type ApplyResult = { snapshot: RuntimeSnapshot; diagnostics: RuntimeDiagnostic[] };

function diag(
  code: RuntimeDiagnosticCode,
  message: string,
  extra: Partial<RuntimeDiagnostic> = {},
  severity: RuntimeDiagnostic["severity"] = "warning",
): RuntimeDiagnostic {
  return { code, severity, message, ...extra };
}

function sessionExists(snapshot: RuntimeSnapshot, sessionId: string | undefined): sessionId is string {
  return sessionId !== undefined && snapshot.sessions.some((session) => session.id === sessionId);
}

function setMessages(snapshot: RuntimeSnapshot, sessionId: string, messages: ChatMessage[]): RuntimeSnapshot {
  return { ...snapshot, messagesBySession: { ...snapshot.messagesBySession, [sessionId]: messages } };
}

export function applyEnvelopeToSnapshot(snapshot: RuntimeSnapshot, envelope: AnyRuntimeEnvelope): ApplyResult {
  const sessionId = envelope.sessionId;
  switch (envelope.type) {
    case "runtime.status-changed":
      return { snapshot: { ...snapshot, status: envelope.payload.status }, diagnostics: [] };

    case "conversation.session-created": {
      const session = envelope.payload.session;
      const exists = snapshot.sessions.some((item) => item.id === session.id);
      if (exists) {
        return {
          snapshot: { ...snapshot, sessions: snapshot.sessions.map((item) => (item.id === session.id ? session : item)) },
          diagnostics: [],
        };
      }
      return {
        snapshot: {
          ...snapshot,
          sessions: [session, ...snapshot.sessions],
          messagesBySession: { ...snapshot.messagesBySession, [session.id]: snapshot.messagesBySession[session.id] ?? [] },
          missionsBySession: { ...snapshot.missionsBySession, [session.id]: snapshot.missionsBySession[session.id] ?? null },
          observabilityBySession: { ...snapshot.observabilityBySession, [session.id]: snapshot.observabilityBySession[session.id] ?? null },
          executionBySession: { ...snapshot.executionBySession, [session.id]: snapshot.executionBySession[session.id] ?? null },
        },
        diagnostics: [],
      };
    }

    case "conversation.session-updated": {
      const session = envelope.payload.session;
      if (!snapshot.sessions.some((item) => item.id === session.id)) {
        return { snapshot, diagnostics: [diag("unknown-session", `Update for unknown session "${session.id}".`, { sessionId: session.id })] };
      }
      return {
        snapshot: { ...snapshot, sessions: snapshot.sessions.map((item) => (item.id === session.id ? session : item)) },
        diagnostics: [],
      };
    }

    case "conversation.message-started": {
      if (!sessionExists(snapshot, sessionId)) {
        return { snapshot, diagnostics: [diag("unknown-session", `Message started for unknown session "${sessionId ?? ""}".`, { sessionId })] };
      }
      const message = envelope.payload.message;
      const messages = snapshot.messagesBySession[sessionId] ?? [];
      const next = messages.some((item) => item.id === message.id)
        ? messages.map((item) => (item.id === message.id ? message : item))
        : [...messages, message];
      return { snapshot: setMessages(snapshot, sessionId, next), diagnostics: [] };
    }

    case "conversation.message-delta": {
      if (!sessionExists(snapshot, sessionId)) {
        return { snapshot, diagnostics: [diag("unknown-session", `Message delta for unknown session "${sessionId ?? ""}".`, { sessionId })] };
      }
      const messages = snapshot.messagesBySession[sessionId] ?? [];
      const index = messages.findIndex((item) => item.id === envelope.payload.messageId);
      if (index < 0) {
        return { snapshot, diagnostics: [diag("unknown-entity", `Delta for unknown message "${envelope.payload.messageId}".`, { sessionId }, "info")] };
      }
      const current = messages[index];
      if (current.status === "cancelled" || current.status === "completed" || current.status === "failed") {
        return { snapshot, diagnostics: [diag("unknown-entity", `Delta ignored for terminal message "${current.id}" (${current.status}).`, { sessionId }, "info")] };
      }
      const next = [...messages];
      next[index] = { ...current, content: current.content + envelope.payload.delta };
      return { snapshot: setMessages(snapshot, sessionId, next), diagnostics: [] };
    }

    case "conversation.message-completed": {
      if (!sessionExists(snapshot, sessionId)) {
        return { snapshot, diagnostics: [diag("unknown-session", `Message completion for unknown session "${sessionId ?? ""}".`, { sessionId })] };
      }
      const message = envelope.payload.message;
      const messages = snapshot.messagesBySession[sessionId] ?? [];
      const exists = messages.some((item) => item.id === message.id);
      const next = exists
        ? messages.map((item) => (item.id === message.id ? message : item))
        : [...messages, message];
      return {
        snapshot: setMessages(snapshot, sessionId, next),
        diagnostics: exists
          ? []
          : [diag("unknown-entity", `Completion for message "${message.id}" with no prior partial; inserted authoritatively.`, { sessionId }, "info")],
      };
    }

    case "cancelled": {
      if (!sessionExists(snapshot, sessionId)) {
        return { snapshot, diagnostics: [diag("unknown-session", `Cancellation for unknown session "${sessionId ?? ""}".`, { sessionId })] };
      }
      if (envelope.payload.messageId === undefined) {
        return { snapshot, diagnostics: [] };
      }
      const messages = snapshot.messagesBySession[sessionId] ?? [];
      const index = messages.findIndex((item) => item.id === envelope.payload.messageId);
      if (index < 0) {
        return { snapshot, diagnostics: [diag("unknown-entity", `Cancellation for unknown message "${envelope.payload.messageId}".`, { sessionId }, "info")] };
      }
      const current = messages[index];
      if (current.status === "completed" || current.status === "failed" || current.status === "cancelled") {
        return {
          snapshot,
          diagnostics: [diag("unknown-entity", `Cancellation ignored; message "${current.id}" is already ${current.status}.`, { sessionId }, "info")],
        };
      }
      const next = [...messages];
      next[index] = { ...current, status: "cancelled" };
      return { snapshot: setMessages(snapshot, sessionId, next), diagnostics: [] };
    }

    case "activity.updated": {
      if (!sessionExists(snapshot, sessionId)) {
        return { snapshot, diagnostics: [diag("unknown-session", `Activity update for unknown session "${sessionId ?? ""}".`, { sessionId })] };
      }
      const messages = snapshot.messagesBySession[sessionId] ?? [];
      const index = messages.findIndex((item) => item.id === envelope.payload.messageId);
      if (index < 0) {
        return { snapshot, diagnostics: [diag("unknown-entity", `Activity for unknown message "${envelope.payload.messageId}".`, { sessionId }, "info")] };
      }
      const next = [...messages];
      next[index] = { ...messages[index], tool: envelope.payload.activity };
      return { snapshot: setMessages(snapshot, sessionId, next), diagnostics: [] };
    }

    case "mission.updated": {
      if (!sessionExists(snapshot, sessionId)) {
        return { snapshot, diagnostics: [diag("unknown-session", `Mission update for unknown session "${sessionId ?? ""}".`, { sessionId })] };
      }
      return {
        snapshot: { ...snapshot, missionsBySession: { ...snapshot.missionsBySession, [sessionId]: envelope.payload.mission } },
        diagnostics: [],
      };
    }

    case "observability.updated": {
      if (!sessionExists(snapshot, sessionId)) {
        return { snapshot, diagnostics: [diag("unknown-session", `Observability update for unknown session "${sessionId ?? ""}".`, { sessionId })] };
      }
      const observability = envelope.payload.observability;
      return {
        snapshot: {
          ...snapshot,
          observabilityBySession: {
            ...snapshot.observabilityBySession,
            [sessionId]: {
              ...observability,
              timeline: boundTimeline(observability.timeline),
              activities: boundActivities(observability.activities),
            },
          },
        },
        diagnostics: [],
      };
    }

    case "execution.updated": {
      if (!sessionExists(snapshot, sessionId)) {
        return { snapshot, diagnostics: [diag("unknown-session", `Execution update for unknown session "${sessionId ?? ""}".`, { sessionId })] };
      }
      return {
        snapshot: { ...snapshot, executionBySession: { ...snapshot.executionBySession, [sessionId]: envelope.payload.execution } },
        diagnostics: [],
      };
    }

    case "worker.updated": {
      if (!sessionExists(snapshot, sessionId)) {
        return { snapshot, diagnostics: [diag("unknown-session", `Worker update for unknown session "${sessionId ?? ""}".`, { sessionId })] };
      }
      const mission: Mission | null | undefined = snapshot.missionsBySession[sessionId];
      if (!mission) {
        return { snapshot, diagnostics: [diag("unknown-entity", `Worker update with no Mission for session "${sessionId}".`, { sessionId }, "info")] };
      }
      const worker = envelope.payload.worker;
      const workers = mission.workers.some((item) => item.id === worker.id)
        ? mission.workers.map((item) => (item.id === worker.id ? worker : item))
        : [...mission.workers, worker];
      return {
        snapshot: { ...snapshot, missionsBySession: { ...snapshot.missionsBySession, [sessionId]: { ...mission, workers } } },
        diagnostics: [],
      };
    }

    case "bootstrap.updated":
      return { snapshot: { ...snapshot, bootstrap: envelope.payload.bootstrap }, diagnostics: [] };

    case "mission.launch-updated": {
      const result = envelope.payload.result;
      if (result.outcome === "accepted") return { snapshot, diagnostics: [] };
      const code: RuntimeDiagnosticCode = result.outcome === "rejected" ? "command-rejected" : "command-failed";
      return {
        snapshot,
        diagnostics: [diag(code, result.message, { sessionId: envelope.sessionId }, "warning")],
      };
    }

    case "attention.updated": {
      const item = envelope.payload.item;
      const items = snapshot.attentionItems ?? [];
      const next = items.some((current) => current.id === item.id)
        ? items.map((current) => current.id === item.id ? item : current)
        : [...items, item];
      return { snapshot: { ...snapshot, attentionItems: next }, diagnostics: [] };
    }

    case "ledger.entry-added":
    case "ledger.entry-updated": {
      const entry = envelope.payload.entry;
      const ledger = snapshot.resourceLedger ?? { generatedAt: entry.timestamp, entries: [] };
      const index = ledger.entries.findIndex((current) => current.id === entry.id);
      if (envelope.type === "ledger.entry-added" && index >= 0) {
        return {
          snapshot,
          diagnostics: [diag("duplicate-event", `Ledger entry "${entry.id}" already exists; add ignored.`, {}, "info")],
        };
      }
      const entries = index >= 0
        ? ledger.entries.map((current) => current.id === entry.id ? entry : current)
        : [...ledger.entries, entry];
      return { snapshot: { ...snapshot, resourceLedger: { ...ledger, entries } }, diagnostics: [] };
    }

    case "log.appended": {
      const entry = normalizeLogEntry(envelope.payload.entry);
      const logs = snapshot.logs ?? [];
      if (logs.some((current) => current.id === entry.id)) {
        return {
          snapshot,
          diagnostics: [diag("duplicate-event", `Log entry "${entry.id}" already exists; append ignored.`, {}, "info")],
        };
      }
      return { snapshot: { ...snapshot, logs: [...logs, entry] }, diagnostics: [] };
    }

    case "warning":
      return { snapshot, diagnostics: [diag("runtime-warning", envelope.payload.message, { sessionId }, "warning")] };

    case "error":
      return { snapshot, diagnostics: [diag("runtime-error", envelope.payload.message, { sessionId }, "error")] };
  }
}
