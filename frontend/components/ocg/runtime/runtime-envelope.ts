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
import type { BootstrapState } from "../bootstrap/types";
import type { ProjectId } from "../project/domain";
import type { AttentionItem } from "../attention/domain";
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
  "job.execution-updated": {
    execution: import("../execution/domain").JobExecution;
    accounting: import("../execution/accounting").JobAccounting | null;
  };
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
    committedReasoningLength: number;
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

export type RuntimeEnvelope<T extends RuntimeEventType = RuntimeEventType> =
  RuntimeEnvelopeHeader & { type: T; payload: RuntimeEnvelopePayloads[T] };

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

export function eventSessionId(event: OcgRuntimeEvent): string | undefined {
  if ("sessionId" in event && typeof event.sessionId === "string" && event.sessionId.length > 0)
    return event.sessionId;
  if (
    (event.type === "conversation.session-created" ||
      event.type === "conversation.session-updated") &&
    typeof event.session.id === "string"
  ) {
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
    ...((header.sessionId ?? eventSessionId(event) ?? undefined)
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
      return {
        ...base,
        type: event.type,
        payload: { messageId: event.messageId, image: event.image },
      };
    case "conversation.message-delta":
      return {
        ...base,
        type: event.type,
        payload: { messageId: event.messageId, delta: event.delta },
      };
    case "conversation.message-reasoning-delta":
      return {
        ...base,
        type: event.type,
        payload: { messageId: event.messageId, delta: event.delta },
      };
    case "conversation.message-round-committed":
      return {
        ...base,
        type: event.type,
        payload: {
          messageId: event.messageId,
          committedContentLength: event.committedContentLength,
          committedImageCount: event.committedImageCount,
          committedReasoningLength: event.committedReasoningLength,
        },
      };
    case "conversation.message-completed":
      return { ...base, type: event.type, payload: { message: event.message } };
    case "activity.updated":
      return {
        ...base,
        type: event.type,
        payload: { messageId: event.messageId, activity: event.activity },
      };
    case "job.execution-updated":
      return {
        ...base,
        type: event.type,
        payload: { execution: event.execution, accounting: event.accounting },
      };
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
