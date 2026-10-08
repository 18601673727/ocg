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
  OcgRuntimeEvent,
  RuntimeStatus,
  ToolActivity,
} from "../types";
import { RUNTIME_CONNECTION_STATES } from "../types";
import type { BootstrapState } from "../bootstrap/types";
import { isProjectId, type ProjectId } from "../project/domain";
import { isNonEmptyString, isNonNegativeInteger, isOneOf, isRecord } from "@/lib/narrow";
import { ATTENTION_LIFECYCLES, type AttentionItem } from "../attention/domain";
import { LOG_LEVELS } from "../logs/domain";
import type { RuntimeObservability } from "./observability";
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
  "conversation.session-deleted": Record<string, never>;
  "conversation.message-image": { messageId: string; image: import("../contracts").ChatImage };
  "conversation.history-loaded": { messages: ChatMessage[] };
  "job.execution-updated": { execution: import("../execution/domain").JobExecution; accounting: import("../execution/accounting").JobAccounting | null };
  "job.launch-updated": { result: import("./runtime-types").JobLaunchResult };
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
  "conversation.message-reasoning-delta": {
    messageId: string;
    delta: string;
  };
  /**
   * The committed presentation of a streaming assistant message ends here.
   * Content and images past these lengths are the provisional output of a
   * provider round that a repeated HTTP attempt replaces, so a replacement can
   * never be appended onto the partial output of the attempt it replaces.
   */
  "conversation.message-round-committed": {
    messageId: string;
    committedContentLength: number;
    committedImageCount: number;
  };
  "conversation.queue-updated": { queue: import("../types").QueuedChatMessage[]; paused: boolean };
  "conversation.message-completed": { message: ChatMessage };
  "activity.updated": { messageId: string; activity: ToolActivity };
  "observability.updated": { observability: RuntimeObservability };
  "attention.updated": { item: AttentionItem };
  "ledger.entry-added": { entry: ResourceLedgerEntry };
  "ledger.entry-updated": { entry: ResourceLedgerEntry };
  "log.appended": { entry: LogEntry };
  "bootstrap.updated": { bootstrap: BootstrapState };
  warning: { message: string };
  error: { message: string };
  cancelled: { messageId?: string };
};

export type RuntimeEventType = keyof RuntimeEnvelopePayloads;

export const RUNTIME_EVENT_TYPES: readonly RuntimeEventType[] = [
  "conversation.session-deleted",
  "conversation.message-image",
  "conversation.queue-updated",
  "conversation.history-loaded",
  "job.execution-updated",
  "job.launch-updated",
  "runtime.status-changed",
  "conversation.session-created",
  "conversation.session-updated",
  "conversation.message-started",
  "conversation.message-delta",
  "conversation.message-reasoning-delta",
  "conversation.message-round-committed",
  "conversation.message-completed",
  "activity.updated",
  "observability.updated",
  "attention.updated",
  "ledger.entry-added",
  "ledger.entry-updated",
  "log.appended",
  "bootstrap.updated",
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

function isIsoDate(value: unknown): value is string {
  return isNonEmptyString(value) && Number.isFinite(Date.parse(value));
}

export function eventSessionId(event: OcgRuntimeEvent): string | undefined {
  if ("sessionId" in event && typeof event.sessionId === "string" && event.sessionId.length > 0) return event.sessionId;
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
  };

  switch (event.type) {
    case "conversation.session-deleted":
      return { ...base, type: event.type, payload: {} };
    case "conversation.queue-updated":
      return { ...base, type: event.type, payload: { queue: event.queue, paused: event.paused } };
    case "conversation.history-loaded":
      return { ...base, type: event.type, payload: { messages: event.messages } };
    case "runtime.status-changed":
      return { ...base, type: event.type, payload: { status: event.status } };
    case "conversation.session-created":
      return { ...base, type: event.type, payload: { session: event.session } };
    case "conversation.session-updated":
      return { ...base, type: event.type, payload: { session: event.session } };
    case "conversation.message-started":
      return { ...base, type: event.type, payload: { message: event.message } };
    case "conversation.message-image":
      return { ...base, type: event.type, payload: { messageId: event.messageId, image: event.image } };
    case "conversation.message-delta":
      return { ...base, type: event.type, payload: { messageId: event.messageId, delta: event.delta } };
    case "conversation.message-reasoning-delta":
      return { ...base, type: event.type, payload: { messageId: event.messageId, delta: event.delta } };
    case "conversation.message-round-committed":
      return {
        ...base,
        type: event.type,
        payload: {
          messageId: event.messageId,
          committedContentLength: event.committedContentLength,
          committedImageCount: event.committedImageCount,
        },
      };
    case "conversation.message-completed":
      return { ...base, type: event.type, payload: { message: event.message } };
    case "activity.updated":
      return { ...base, type: event.type, payload: { messageId: event.messageId, activity: event.activity } };
    case "job.execution-updated":
      return { ...base, type: event.type, payload: { execution: event.execution, accounting: event.accounting } };
    case "job.launch-updated":
      return { ...base, type: event.type, payload: { result: event.result } };
    case "observability.updated":
      return { ...base, type: event.type, payload: { observability: event.observability } };
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
    });
  }

  envelope<T extends RuntimeEventType>(
    type: T,
    payload: RuntimeEnvelopePayloads[T],
    scope: {
      projectId?: ProjectId | null;
      commandId?: string;
      sessionId?: string;
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
    case "conversation.session-deleted":
      return { type: envelope.type, sessionId };
    case "conversation.queue-updated":
      return { type: envelope.type, sessionId, queue: envelope.payload.queue, paused: envelope.payload.paused };
    case "conversation.history-loaded":
      return { type: envelope.type, sessionId, messages: envelope.payload.messages };
    case "job.execution-updated":
      return {
        type: envelope.type,
        ...(envelope.sessionId ? { sessionId: envelope.sessionId } : {}),
        execution: envelope.payload.execution,
        accounting: envelope.payload.accounting,
      };
    case "job.launch-updated":
      return { type: envelope.type, sessionId, result: envelope.payload.result };
    case "runtime.status-changed":
      return { type: envelope.type, status: envelope.payload.status };
    case "conversation.session-created":
      return { type: envelope.type, session: envelope.payload.session };
    case "conversation.session-updated":
      return { type: envelope.type, session: envelope.payload.session };
    case "conversation.message-started":
      return { type: envelope.type, sessionId, message: envelope.payload.message };
    case "conversation.message-image":
      return { type: envelope.type, sessionId, messageId: envelope.payload.messageId, image: envelope.payload.image };
    case "conversation.message-delta":
      return { type: envelope.type, sessionId, messageId: envelope.payload.messageId, delta: envelope.payload.delta };
    case "conversation.message-reasoning-delta":
      return { type: envelope.type, sessionId, messageId: envelope.payload.messageId, delta: envelope.payload.delta };
    case "conversation.message-round-committed":
      return {
        type: envelope.type,
        sessionId,
        messageId: envelope.payload.messageId,
        committedContentLength: envelope.payload.committedContentLength,
        committedImageCount: envelope.payload.committedImageCount,
      };
    case "conversation.message-completed":
      return { type: envelope.type, sessionId, message: envelope.payload.message };
    case "activity.updated":
      return { type: envelope.type, sessionId, messageId: envelope.payload.messageId, activity: envelope.payload.activity };
    case "observability.updated":
      return { type: envelope.type, sessionId, observability: envelope.payload.observability };
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
    case "conversation.session-deleted":
      return null;
    case "conversation.queue-updated": {
      if (typeof payload.paused !== "boolean" || !Array.isArray(payload.queue) || payload.queue.some(item =>
        !isRecord(item) || !isNonEmptyString(item.id) || !isRecord(item.input) || typeof item.input.content !== "string"
      )) return "queue must contain valid queued chat messages.";
      return null;
    }
    case "job.execution-updated": {
      const execution = payload.execution;
      if (!isRecord(execution) || !isNonEmptyString(execution.jobId)) return "execution.jobId is required.";
      if (!isProjectId(execution.projectId)) return "execution.projectId is required.";
      if (!isNonNegativeInteger(execution.generation) || !isNonNegativeInteger(execution.cursor)) return "execution generation and cursor must be non-negative integers.";
      if (!Array.isArray(execution.attempts) || !Array.isArray(execution.calls) || !Array.isArray(execution.executors)) return "execution entities must be arrays.";
      if (payload.accounting !== null && !isRecord(payload.accounting)) return "accounting must be an object or null.";
      return null;
    }
    case "runtime.status-changed": {
      const status = payload.status;
      if (!isRecord(status) || typeof status.state !== "string") return "status.state is required.";
      if (!isOneOf(status.state, RUNTIME_CONNECTION_STATES)) {
        return `Unknown runtime connection state "${String(status.state)}".`;
      }
      if (status.detail !== undefined && typeof status.detail !== "string") return "status.detail must be a string when present.";
      return null;
    }
    case "conversation.session-created":
    case "conversation.session-updated": {
      const session = payload.session;
      if (!isRecord(session) || !isNonEmptyString(session.id)) return "session.id is required.";
      if (typeof session.title !== "string") return "session.title must be a string.";
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
    case "conversation.history-loaded": {
      if (!Array.isArray(payload.messages) || payload.messages.some((message) =>
        !isRecord(message) || !isNonEmptyString(message.id) ||
        !isNonEmptyString(message.role) || !isNonEmptyString(message.status) ||
        typeof message.content !== "string"
      )) return "messages must contain valid chat messages.";
      return null;
    }
    case "conversation.message-image": {
      if (!isNonEmptyString(payload.messageId)) return "messageId is required.";
      const image = payload.image;
      if (!isRecord(image) || !isNonEmptyString(image.id) || typeof image.name !== "string" || !isNonEmptyString(image.media_type) || !isNonEmptyString(image.url)) return "image must contain a valid image.";
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
    case "conversation.message-reasoning-delta": {
      if (!isNonEmptyString(payload.messageId)) return "messageId is required.";
      if (typeof payload.delta !== "string") return "delta must be a string.";
      return null;
    }
    case "conversation.message-round-committed": {
      if (!isNonEmptyString(payload.messageId)) return "messageId is required.";
      if (!isNonNegativeInteger(payload.committedContentLength)) return "committedContentLength must be a non-negative integer.";
      if (!isNonNegativeInteger(payload.committedImageCount)) return "committedImageCount must be a non-negative integer.";
      return null;
    }
    case "activity.updated": {
      if (!isNonEmptyString(payload.messageId)) return "messageId is required.";
      if (!isRecord(payload.activity) || !isNonEmptyString(payload.activity.id)) return "activity.id is required.";
      return null;
    }
    case "observability.updated": {
      const observability = payload.observability;
      if (!isRecord(observability)) return "observability must be an object.";
      if (!Array.isArray(observability.workers)) return "observability.workers must be an array.";
      if (!isRecord(observability.job)) return "observability.job must be an object.";
      return null;
    }
    case "job.launch-updated": {
      const result = payload.result;
      if (!isRecord(result) || !isNonEmptyString(result.outcome)) return "result.outcome is required.";
      if (!isNonEmptyString(result.commandId)) return "result.commandId is required.";
      return null;
    }
    case "attention.updated": {
      const item = payload.item;
      if (!isRecord(item) || !isNonEmptyString(item.id)) return "attention item.id is required.";
      if (!isNonEmptyString(item.status)) return "attention item.status is required.";
      if (!isOneOf(item.status, ATTENTION_LIFECYCLES)) return "attention item.status is invalid.";
      if (!isNonEmptyString(item.updatedAt)) return "attention item.updatedAt is required.";
      if (item.projectId !== undefined && !isProjectId(item.projectId)) return "attention item.projectId must be a known Project ID.";
      return null;
    }
    case "ledger.entry-added":
    case "ledger.entry-updated": {
      const entry = payload.entry;
      if (!isRecord(entry) || !isNonEmptyString(entry.id)) return "ledger entry.id is required.";
      if (!isNonEmptyString(entry.jobId)) return "ledger entry.jobId is required.";
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
      if (!isOneOf(entry.level, LOG_LEVELS)) return "log entry.level is invalid.";
      if (typeof entry.message !== "string") return "log entry.message must be a string.";
      return null;
    }
    case "bootstrap.updated": {
      if (!isRecord(payload.bootstrap)) return "bootstrap must be an object.";
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

  if (!isOneOf(input.type, RUNTIME_EVENT_TYPES)) {
    return diagnostic("unknown-event-type", `Unknown runtime event type "${input.type}".`, "warning");
  }

  const type = input.type;
  const payloadError = validatePayload(type, input.payload);
  if (payloadError) return diagnostic("schema-invalid", payloadError);

  return { ok: true, envelope: input as unknown as AnyRuntimeEnvelope };
}
