/**
 * Transport-independent runtime event envelopes.
 *
 * This module is the canonical, typed boundary between a future transport and
 * the runtime reconciler. It deliberately contains no HTTP/SSE/WebSocket
 * knowledge and no untyped escape hatch: an event either matches one of the
 * known typed payloads or it is reported as a safe diagnostic.
 *
 * The existing `OcgRuntimeEvent` union remains the presentation-facing event
 * contract; `toRuntimeEvent` maps a validated envelope back to it.
 */

import type {
  ChatMessage,
  ChatSession,
  Mission,
  OcgRuntimeEvent,
  RuntimeStatus,
  ToolActivity,
  Worker,
} from "../types";
import type { BootstrapState } from "../bootstrap/types";
import type { MissionExecution } from "../execution/domain";
import { isProjectId, type ProjectId } from "../project/domain";
import type { MissionLaunchResult } from "./runtime-types";
import type { RuntimeObservability } from "./observability";
import type { AttentionItem } from "../attention/domain";
import type { ResourceLedgerEntry } from "../resource-ledger/types";
import type { LogEntry } from "../logs/domain";

export const RUNTIME_PROTOCOL_VERSION = 1;
export const RUNTIME_EVENT_VERSION = 1;

export type RuntimeProtocolVersion = number;
export type RuntimeStreamId = string;
export type RuntimeEventId = string;
export type RuntimeGeneration = number;

/** Ordering/resume primitive: a global per-stream high-watermark. */
export type RuntimeCursor = {
  streamId: RuntimeStreamId;
  sequence: number;
};

/** Opaque transport resume material; it is deliberately not a domain cursor. */
export type RuntimeResumeCursor = string;

/** Per-event normalized payloads for every currently meaningful event. */
export type RuntimeEnvelopePayloads = {
  "runtime.status-changed": { status: RuntimeStatus };
  "conversation.session-created": { session: ChatSession };
  "conversation.session-updated": { session: ChatSession };
  "conversation.message-started": { message: ChatMessage };
  "conversation.message-delta": {
    messageId: string;
    delta: string;
    /** Optional turn identity used for delta-sequence deduplication. */
    turnId?: string;
    /** Optional monotonic delta sequence within the turn. */
    deltaSequence?: number;
  };
  "conversation.message-completed": { message: ChatMessage };
  "activity.updated": { messageId: string; activity: ToolActivity };
  "mission.updated": { mission: Mission };
  "observability.updated": { observability: RuntimeObservability };
  "execution.updated": { execution: MissionExecution };
  "mission.launch-updated": { result: MissionLaunchResult };
  "attention.updated": { item: AttentionItem };
  "ledger.entry-added": { entry: ResourceLedgerEntry };
  "ledger.entry-updated": { entry: ResourceLedgerEntry };
  "log.appended": { entry: LogEntry };
  "bootstrap.updated": { bootstrap: BootstrapState };
  "worker.updated": { worker: Worker };
  warning: { message: string };
  error: { message: string };
  cancelled: { messageId?: string };
};

export type RuntimeEventType = keyof RuntimeEnvelopePayloads;

export const RUNTIME_EVENT_TYPES: readonly RuntimeEventType[] = [
  "runtime.status-changed",
  "conversation.session-created",
  "conversation.session-updated",
  "conversation.message-started",
  "conversation.message-delta",
  "conversation.message-completed",
  "activity.updated",
  "mission.updated",
  "observability.updated",
  "execution.updated",
  "mission.launch-updated",
  "attention.updated",
  "ledger.entry-added",
  "ledger.entry-updated",
  "log.appended",
  "bootstrap.updated",
  "worker.updated",
  "warning",
  "error",
  "cancelled",
];

export type RuntimeEnvelopeHeader = {
  protocolVersion: RuntimeProtocolVersion;
  eventVersion: number;
  streamId: RuntimeStreamId;
  generation: RuntimeGeneration;
  sequence: number;
  eventId: RuntimeEventId;
  /** Entity scope; `null` means a global/unscoped event. */
  projectId: ProjectId | null;
  occurredAt: string;
  commandId?: string;
  sessionId?: string;
  missionId?: string;
};

export type RuntimeEnvelope<
  T extends RuntimeEventType = RuntimeEventType,
> = RuntimeEnvelopeHeader & { type: T; payload: RuntimeEnvelopePayloads[T] };

/** Distributed union over every known event type. */
export type AnyRuntimeEnvelope = {
  [K in RuntimeEventType]: RuntimeEnvelope<K>;
}[RuntimeEventType];

/* -------------------------------------------------------------------------- */
/* Diagnostics / safe errors                                                  */
/* -------------------------------------------------------------------------- */

export type RuntimeDiagnosticSeverity = "info" | "warning" | "error";

export type RuntimeDiagnosticCode =
  | "protocol-incompatible"
  | "schema-invalid"
  | "unknown-event-type"
  | "no-snapshot"
  | "stream-mismatch"
  | "generation-stale"
  | "generation-unknown"
  | "sequence-gap"
  | "sequence-stale"
  | "duplicate-event"
  | "duplicate-delta"
  | "project-scope-mismatch"
  | "unknown-session"
  | "unknown-entity"
  | "command-rejected"
  | "command-failed"
  | "resync-required"
  | "snapshot-stale"
  | "snapshot-scope-mismatch"
  | "runtime-warning"
  | "runtime-error";

export type RuntimeDiagnostic = {
  code: RuntimeDiagnosticCode;
  severity: RuntimeDiagnosticSeverity;
  message: string;
  eventId?: string;
  sequence?: number;
  sessionId?: string;
};

export type RuntimeErrorCode =
  | "protocol_incompatible"
  | "validation_error"
  | "runtime_unavailable"
  | "cursor_expired"
  | "stale_command"
  | "unknown_resource"
  | "transport_unavailable";

export type RuntimeError = {
  code: RuntimeErrorCode;
  message: string;
  retryable: boolean;
  /** Bounded, display-safe details; never secrets or raw executor payloads. */
  details?: Record<string, string | number | boolean | null>;
};

export type RuntimeEnvelopeValidation =
  | { ok: true; envelope: AnyRuntimeEnvelope }
  | { ok: false; diagnostic: RuntimeDiagnostic };

/* -------------------------------------------------------------------------- */
/* Helpers                                                                    */
/* -------------------------------------------------------------------------- */

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.length > 0;
}

function isNonNegativeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) && value >= 0;
}

function isIsoDate(value: unknown): value is string {
  return isNonEmptyString(value) && Number.isFinite(Date.parse(value));
}

export function eventSessionId(event: OcgRuntimeEvent): string | undefined {
  if ("sessionId" in event && typeof event.sessionId === "string") return event.sessionId;
  if ((event.type === "conversation.session-created" || event.type === "conversation.session-updated") && typeof event.session.id === "string") {
    return event.session.id;
  }
  return undefined;
}

/* -------------------------------------------------------------------------- */
/* Envelope construction                                                      */
/* -------------------------------------------------------------------------- */

export type RuntimeEnvelopeHeaderInput = {
  streamId: RuntimeStreamId;
  generation: RuntimeGeneration;
  sequence: number;
  eventId: RuntimeEventId;
  occurredAt: string;
  projectId: ProjectId | null;
  protocolVersion?: RuntimeProtocolVersion;
  eventVersion?: number;
  commandId?: string;
  sessionId?: string;
  missionId?: string;
};

/** Build a typed envelope from a raw presentation event plus a stamped header. */
export function envelopeFromRuntimeEvent(
  event: OcgRuntimeEvent,
  header: RuntimeEnvelopeHeaderInput,
): AnyRuntimeEnvelope {
  const base: RuntimeEnvelopeHeader = {
    protocolVersion: header.protocolVersion ?? RUNTIME_PROTOCOL_VERSION,
    eventVersion: header.eventVersion ?? RUNTIME_EVENT_VERSION,
    streamId: header.streamId,
    generation: header.generation,
    sequence: header.sequence,
    eventId: header.eventId,
    projectId: header.projectId,
    occurredAt: header.occurredAt,
    ...(header.commandId !== undefined ? { commandId: header.commandId } : {}),
    ...(header.sessionId ?? eventSessionId(event) ?? undefined
      ? { sessionId: header.sessionId ?? eventSessionId(event)! }
      : {}),
    ...(header.missionId !== undefined ? { missionId: header.missionId } : {}),
  };

  switch (event.type) {
    case "runtime.status-changed":
      return { ...base, type: event.type, payload: { status: event.status } };
    case "conversation.session-created":
      return { ...base, type: event.type, payload: { session: event.session } };
    case "conversation.session-updated":
      return { ...base, type: event.type, payload: { session: event.session } };
    case "conversation.message-started":
      return { ...base, type: event.type, payload: { message: event.message } };
    case "conversation.message-delta":
      return { ...base, type: event.type, payload: { messageId: event.messageId, delta: event.delta } };
    case "conversation.message-completed":
      return { ...base, type: event.type, payload: { message: event.message } };
    case "activity.updated":
      return { ...base, type: event.type, payload: { messageId: event.messageId, activity: event.activity } };
    case "mission.updated":
      return { ...base, type: event.type, payload: { mission: event.mission } };
    case "observability.updated":
      return { ...base, type: event.type, payload: { observability: event.observability } };
    case "execution.updated":
      return { ...base, type: event.type, payload: { execution: event.execution } };
    case "mission.launch-updated":
      return { ...base, type: event.type, payload: { result: event.result } };
    case "attention.updated":
      return { ...base, type: event.type, payload: { item: event.item } };
    case "ledger.entry-added":
      return { ...base, type: event.type, payload: { entry: event.entry } };
    case "ledger.entry-updated":
      return { ...base, type: event.type, payload: { entry: event.entry } };
    case "log.appended":
      return { ...base, type: event.type, payload: { entry: event.entry } };
    case "bootstrap.updated":
      return { ...base, type: event.type, payload: { bootstrap: event.bootstrap } };
    case "worker.updated":
      return { ...base, type: event.type, payload: { worker: event.worker } };
    case "warning":
      return { ...base, type: event.type, payload: { message: event.message } };
    case "error":
      return { ...base, type: event.type, payload: { message: event.message } };
    case "cancelled":
      return {
        ...base,
        type: event.type,
        payload: event.messageId !== undefined ? { messageId: event.messageId } : {},
      };
  }
}

/** Deterministic, monotonic envelope allocator. No clock or randomness. */
export class RuntimeEnvelopeFactory {
  private sequence: number;
  private readonly baseTimeMs: number;

  constructor(
    private readonly streamId: RuntimeStreamId,
    private readonly generation: RuntimeGeneration,
    options: { startSequence?: number; baseTimeMs?: number } = {},
  ) {
    this.sequence = options.startSequence ?? 1;
    this.baseTimeMs = options.baseTimeMs ?? Date.UTC(2026, 0, 1, 0, 0, 0);
  }

  get nextSequence(): number {
    return this.sequence;
  }

  fromRuntimeEvent(
    event: OcgRuntimeEvent,
    scope: {
      projectId?: ProjectId | null;
      commandId?: string;
      sessionId?: string;
      missionId?: string;
    } = {},
  ): AnyRuntimeEnvelope {
    const sequence = this.sequence++;
    return envelopeFromRuntimeEvent(event, {
      streamId: this.streamId,
      generation: this.generation,
      sequence,
      eventId: `${this.streamId}:${sequence}`,
      occurredAt: this.isoAt(sequence),
      projectId: scope.projectId ?? null,
      commandId: scope.commandId,
      sessionId: scope.sessionId,
      missionId: scope.missionId,
    });
  }

  envelope<T extends RuntimeEventType>(
    type: T,
    payload: RuntimeEnvelopePayloads[T],
    scope: {
      projectId?: ProjectId | null;
      commandId?: string;
      sessionId?: string;
      missionId?: string;
    } = {},
  ): RuntimeEnvelope<T> {
    const sequence = this.sequence++;
    return {
      protocolVersion: RUNTIME_PROTOCOL_VERSION,
      eventVersion: RUNTIME_EVENT_VERSION,
      streamId: this.streamId,
      generation: this.generation,
      sequence,
      eventId: `${this.streamId}:${sequence}`,
      projectId: scope.projectId ?? null,
      occurredAt: this.isoAt(sequence),
      ...(scope.commandId !== undefined ? { commandId: scope.commandId } : {}),
      ...(scope.sessionId !== undefined ? { sessionId: scope.sessionId } : {}),
      ...(scope.missionId !== undefined ? { missionId: scope.missionId } : {}),
      type,
      payload,
    };
  }

  private isoAt(sequence: number): string {
    return new Date(this.baseTimeMs + sequence * 1000).toISOString();
  }
}

/* -------------------------------------------------------------------------- */
/* Mapping back to the presentation event union                               */
/* -------------------------------------------------------------------------- */

export function toRuntimeEvent(envelope: AnyRuntimeEnvelope): OcgRuntimeEvent {
  const sessionId = envelope.sessionId ?? "";
  switch (envelope.type) {
    case "runtime.status-changed":
      return { type: envelope.type, status: envelope.payload.status };
    case "conversation.session-created":
      return { type: envelope.type, session: envelope.payload.session };
    case "conversation.session-updated":
      return { type: envelope.type, session: envelope.payload.session };
    case "conversation.message-started":
      return { type: envelope.type, sessionId, message: envelope.payload.message };
    case "conversation.message-delta":
      return { type: envelope.type, sessionId, messageId: envelope.payload.messageId, delta: envelope.payload.delta };
    case "conversation.message-completed":
      return { type: envelope.type, sessionId, message: envelope.payload.message };
    case "activity.updated":
      return { type: envelope.type, sessionId, messageId: envelope.payload.messageId, activity: envelope.payload.activity };
    case "mission.updated":
      return { type: envelope.type, sessionId, mission: envelope.payload.mission };
    case "observability.updated":
      return { type: envelope.type, sessionId, observability: envelope.payload.observability };
    case "execution.updated":
      return { type: envelope.type, sessionId, execution: envelope.payload.execution };
    case "mission.launch-updated":
      return { type: envelope.type, sessionId, result: envelope.payload.result };
    case "attention.updated":
      return { type: envelope.type, item: envelope.payload.item };
    case "ledger.entry-added":
      return { type: envelope.type, entry: envelope.payload.entry };
    case "ledger.entry-updated":
      return { type: envelope.type, entry: envelope.payload.entry };
    case "log.appended":
      return { type: envelope.type, entry: envelope.payload.entry };
    case "bootstrap.updated":
      return { type: envelope.type, bootstrap: envelope.payload.bootstrap };
    case "worker.updated":
      return { type: envelope.type, sessionId, worker: envelope.payload.worker };
    case "warning":
      return { type: envelope.type, message: envelope.payload.message };
    case "error":
      return { type: envelope.type, message: envelope.payload.message };
    case "cancelled":
      return envelope.payload.messageId !== undefined
        ? { type: envelope.type, sessionId, messageId: envelope.payload.messageId }
        : { type: envelope.type, sessionId };
  }
}

/* -------------------------------------------------------------------------- */
/* Validation / normalization                                                 */
/* -------------------------------------------------------------------------- */

const CONNECTION_STATES = ["connected", "connecting", "disconnected", "failed"] as const;
const ATTENTION_STATES = ["pending", "acknowledged", "approved", "rejected", "resolved", "expired", "superseded"] as const;
const LOG_LEVELS = ["trace", "debug", "info", "warn", "error"] as const;

function diagnostic(
  code: RuntimeDiagnosticCode,
  message: string,
  severity: RuntimeDiagnosticSeverity = "error",
): RuntimeEnvelopeValidation {
  return { ok: false, diagnostic: { code, severity, message } };
}

function validatePayload(type: RuntimeEventType, payload: unknown): string | null {
  if (!isRecord(payload)) return `Event "${type}" payload must be an object.`;
  switch (type) {
    case "runtime.status-changed": {
      const status = payload.status;
      if (!isRecord(status) || typeof status.state !== "string") return "status.state is required.";
      if (!(CONNECTION_STATES as readonly string[]).includes(status.state)) {
        return `Unknown runtime connection state "${String(status.state)}".`;
      }
      if (status.detail !== undefined && typeof status.detail !== "string") return "status.detail must be a string when present.";
      return null;
    }
    case "conversation.session-created":
    case "conversation.session-updated": {
      const session = payload.session;
      if (!isRecord(session) || !isNonEmptyString(session.id)) return "session.id is required.";
      if (!isNonEmptyString(session.title)) return "session.title is required.";
      if (!isNonEmptyString(session.workType)) return "session.workType is required.";
      return null;
    }
    case "conversation.message-started":
    case "conversation.message-completed": {
      const message = payload.message;
      if (!isRecord(message) || !isNonEmptyString(message.id)) return "message.id is required.";
      if (!isNonEmptyString(message.role)) return "message.role is required.";
      if (!isNonEmptyString(message.status)) return "message.status is required.";
      if (typeof message.content !== "string") return "message.content must be a string.";
      return null;
    }
    case "conversation.message-delta": {
      if (!isNonEmptyString(payload.messageId)) return "messageId is required.";
      if (typeof payload.delta !== "string") return "delta must be a string.";
      if (payload.deltaSequence !== undefined && !isNonNegativeInteger(payload.deltaSequence)) {
        return "deltaSequence must be a non-negative integer when present.";
      }
      return null;
    }
    case "activity.updated": {
      if (!isNonEmptyString(payload.messageId)) return "messageId is required.";
      if (!isRecord(payload.activity) || !isNonEmptyString(payload.activity.id)) return "activity.id is required.";
      return null;
    }
    case "mission.updated": {
      const mission = payload.mission;
      if (!isRecord(mission) || !isNonEmptyString(mission.status)) return "mission.status is required.";
      if (!( ["planning", "running", "paused", "completed", "failed", "budget-exhausted"] as readonly string[]).includes(mission.status)) return "mission.status is invalid.";
      if (typeof mission.title !== "string") return "mission.title must be a string.";
      return null;
    }
    case "observability.updated": {
      const observability = payload.observability;
      if (!isRecord(observability)) return "observability must be an object.";
      if (!Array.isArray(observability.workers)) return "observability.workers must be an array.";
      if (!isRecord(observability.mission)) return "observability.mission must be an object.";
      return null;
    }
    case "execution.updated": {
      const execution = payload.execution;
      if (!isRecord(execution) || !isNonEmptyString(execution.missionId)) return "execution.missionId is required.";
      return null;
    }
    case "mission.launch-updated": {
      const result = payload.result;
      if (!isRecord(result) || !isNonEmptyString(result.outcome)) return "result.outcome is required.";
      if (!isNonEmptyString(result.commandId)) return "result.commandId is required.";
      return null;
    }
    case "attention.updated": {
      const item = payload.item;
      if (!isRecord(item) || !isNonEmptyString(item.id)) return "attention item.id is required.";
      if (!isNonEmptyString(item.status)) return "attention item.status is required.";
      if (!(ATTENTION_STATES as readonly string[]).includes(item.status)) return "attention item.status is invalid.";
      if (!isNonEmptyString(item.updatedAt)) return "attention item.updatedAt is required.";
      if (item.projectId !== undefined && !isProjectId(item.projectId)) return "attention item.projectId must be a known Project ID.";
      return null;
    }
    case "ledger.entry-added":
    case "ledger.entry-updated": {
      const entry = payload.entry;
      if (!isRecord(entry) || !isNonEmptyString(entry.id)) return "ledger entry.id is required.";
      if (!isNonEmptyString(entry.missionId)) return "ledger entry.missionId is required.";
      if (!isNonEmptyString(entry.timestamp)) return "ledger entry.timestamp is required.";
      if (!isNonNegativeInteger(entry.attempt) || entry.attempt < 1) return "ledger entry.attempt must be a positive integer.";
      if (entry.costMicros !== null && (typeof entry.costMicros !== "number" || !Number.isSafeInteger(entry.costMicros) || entry.costMicros < 0)) return "ledger entry.costMicros must be a non-negative safe integer or null.";
      return null;
    }
    case "log.appended": {
      const entry = payload.entry;
      if (!isRecord(entry) || !isNonEmptyString(entry.id)) return "log entry.id is required.";
      if (!isIsoDate(entry.timestamp)) return "log entry.timestamp must be a valid timestamp.";
      if (!isNonEmptyString(entry.source)) return "log entry.source is required.";
      if (!isNonEmptyString(entry.level) || !(LOG_LEVELS as readonly string[]).includes(entry.level)) return "log entry.level is invalid.";
      if (typeof entry.message !== "string") return "log entry.message must be a string.";
      return null;
    }
    case "bootstrap.updated": {
      if (!isRecord(payload.bootstrap)) return "bootstrap must be an object.";
      return null;
    }
    case "worker.updated": {
      const worker = payload.worker;
      if (!isRecord(worker) || !isNonEmptyString(worker.id)) return "worker.id is required.";
      return null;
    }
    case "warning":
    case "error": {
      if (typeof payload.message !== "string") return "message must be a string.";
      return null;
    }
    case "cancelled": {
      if (payload.messageId !== undefined && !isNonEmptyString(payload.messageId)) {
        return "messageId must be a non-empty string when present.";
      }
      return null;
    }
  }
}

/** Validate and normalize an unknown value into a typed envelope or diagnostic. */
export function validateRuntimeEnvelope(input: unknown): RuntimeEnvelopeValidation {
  if (!isRecord(input)) return diagnostic("schema-invalid", "Runtime envelope must be an object.");

  if (input.protocolVersion !== undefined && input.protocolVersion !== RUNTIME_PROTOCOL_VERSION) {
    return diagnostic(
      "protocol-incompatible",
      `Unsupported runtime protocol version "${String(input.protocolVersion)}".`,
    );
  }
  if (typeof input.protocolVersion !== "number") {
    return diagnostic("schema-invalid", "protocolVersion must be a number.");
  }
  if (!isNonNegativeInteger(input.eventVersion)) {
    return diagnostic("schema-invalid", "eventVersion must be a non-negative integer.");
  }
  if (!isNonEmptyString(input.streamId)) return diagnostic("schema-invalid", "streamId is required.");
  if (!isNonNegativeInteger(input.generation)) {
    return diagnostic("schema-invalid", "generation must be a non-negative integer.");
  }
  if (!isNonNegativeInteger(input.sequence)) {
    return diagnostic("schema-invalid", "sequence must be a non-negative integer.");
  }
  if (!isNonEmptyString(input.eventId)) return diagnostic("schema-invalid", "eventId is required.");
  if (input.projectId !== null && !isProjectId(input.projectId)) {
    return diagnostic("schema-invalid", "projectId must be a known Project ID or null.");
  }
  if (!isIsoDate(input.occurredAt)) return diagnostic("schema-invalid", "occurredAt must be a valid timestamp.");
  if (typeof input.type !== "string") return diagnostic("schema-invalid", "type is required.");

  if (!(RUNTIME_EVENT_TYPES as readonly string[]).includes(input.type)) {
    return diagnostic("unknown-event-type", `Unknown runtime event type "${input.type}".`, "warning");
  }

  const type = input.type as RuntimeEventType;
  const payloadError = validatePayload(type, input.payload);
  if (payloadError) return diagnostic("schema-invalid", payloadError);

  return { ok: true, envelope: input as unknown as AnyRuntimeEnvelope };
}
