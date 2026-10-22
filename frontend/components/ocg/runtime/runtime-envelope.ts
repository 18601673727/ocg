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
          committedReasoningLength: event.committedReasoningLength,
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
/* Validation / normalization                                                 */
/* -------------------------------------------------------------------------- */

/* -------------------------------------------------------------------------- */
/* Nested domain validators                                                 */
/* -------------------------------------------------------------------------- */

/**
 * Closed unions the reconciler and its consumers already assume. A message,
 * session, or acknowledgement outside these sets cannot be rendered or
 * reduced without a fallback, so unknown input carrying one is reported
 * instead of being typed as a complete envelope.
 *
 * The unions mirror `MessageRole`/`MessageStatus`/`WorkType` in
 * `../types`, `JobLaunchOutcome` in `./runtime-types`, and the attention,
 * bootstrap, and status domains; they are data here so a validator can check
 * membership without an `as` cast.
 */
export const MESSAGE_ROLES = ["user", "assistant", "call", "tool"] as const;
export const MESSAGE_STATUSES = ["pending", "streaming", "completed", "cancelled", "unknown", "failed"] as const;
export const CHAT_WORK_TYPES = ["research", "coding", "design", "devops"] as const;
export const JOB_LAUNCH_OUTCOMES = ["accepted", "rejected", "requires-attention", "failed"] as const;
export const SNAPSHOT_AUTHORITIES = ["canonical", "mock"] as const;

const ATTENTION_KINDS = [
  "approval",
  "budget",
  "policy",
  "permission",
  "blocked",
  "runtime-failure",
  "resource-degraded",
  "configuration",
  "retry",
  "escalation",
] as const;

const ATTENTION_SEVERITIES = ["info", "warning", "high", "critical"] as const;

const ATTENTION_SOURCES = ["execution", "runtime", "provider", "resource", "policy", "configuration"] as const;

const ATTENTION_DESTINATIONS = [
  "job-execution",
  "control-center",
  "resource-ledger",
  "logs",
  "settings",
  "chat",
] as const;

const BOOTSTRAP_ACCESS_STATES = [
  "local",
  "authenticated",
  "unauthenticated",
  "session-expired",
  "denied",
  "auth-required",
] as const;

const BOOTSTRAP_HANDOFF_STATES = ["unavailable", "idle", "pending", "complete", "failed"] as const;

const RUNTIME_STATUS_DETAIL_CODES = ["provider-ready", "configuration-required"] as const;

/** One managed image, as message images, chat messages, and queued inputs carry it. */
export function validateChatImageValue(image: unknown): string | null {
  if (
    !isRecord(image) || !isNonEmptyString(image.id) || typeof image.name !== "string" ||
    !isNonEmptyString(image.media_type) || !isNonEmptyString(image.url)
  ) {
    return "image must contain a valid image.";
  }
  return null;
}

/** One chat message, as history, message events, and snapshot maps carry it. */
export function validateChatMessageValue(message: unknown): string | null {
  if (!isRecord(message)) return "message must be an object.";
  if (!isNonEmptyString(message.id)) return "message.id is required.";
  if (!isOneOf(message.role, MESSAGE_ROLES)) {
    return `Unknown message role "${String(message.role)}".`;
  }
  if (!isOneOf(message.status, MESSAGE_STATUSES)) {
    return `Unknown message status "${String(message.status)}".`;
  }
  if (typeof message.content !== "string") return "message.content must be a string.";
  if (typeof message.createdAt !== "string") return "message.createdAt must be a string.";
  if (
    message.images !== undefined &&
    (!Array.isArray(message.images) || message.images.some((image) => validateChatImageValue(image) !== null))
  ) {
    return "message images must contain valid images.";
  }
  if (
    message.toolCalls !== undefined &&
    (!Array.isArray(message.toolCalls) ||
      message.toolCalls.some((call) => !isRecord(call) || typeof call.name !== "string" || typeof call.decision !== "string"))
  ) {
    return "message tool calls must contain valid tool calls.";
  }
  return null;
}

/** One chat session, as session events and snapshot session lists carry it. */
export function validateChatSessionValue(session: unknown): string | null {
  if (!isRecord(session)) return "session must be an object.";
  if (!isNonEmptyString(session.id)) return "session.id is required.";
  if (typeof session.title !== "string") return "session.title must be a string.";
  if (!isOneOf(session.workType, CHAT_WORK_TYPES)) {
    return `Unknown session work type "${String(session.workType)}".`;
  }
  if (typeof session.updatedAt !== "string") return "session.updatedAt must be a string.";
  for (const key of ["projectId", "sessionId", "agentProfile", "conversationId"] as const) {
    if (session[key] !== undefined && typeof session[key] !== "string") {
      return `session.${key} must be a string when present.`;
    }
  }
  return null;
}

/** Connection status, as status-changed events and snapshot baselines carry it. */
export function validateRuntimeStatusValue(status: unknown): string | null {
  if (!isRecord(status)) return "status must be an object.";
  if (!isOneOf(status.state, RUNTIME_CONNECTION_STATES)) {
    return `Unknown runtime connection state "${String(status.state)}".`;
  }
  if (status.detailCode !== undefined && !isOneOf(status.detailCode, RUNTIME_STATUS_DETAIL_CODES)) {
    return `Unknown runtime status detail "${String(status.detailCode)}".`;
  }
  if (status.detail !== undefined && typeof status.detail !== "string") {
    return "status.detail must be a string when present.";
  }
  if (status.sessionId !== undefined && !isNonEmptyString(status.sessionId)) {
    return "status.sessionId must be a non-empty string when present.";
  }
  return null;
}

/** Bootstrap state: the shell reads access, inventory lists, and readiness off it. */
export function validateBootstrapValue(bootstrap: unknown): string | null {
  if (!isRecord(bootstrap)) return "bootstrap must be an object.";
  const access = bootstrap.access;
  if (
    !isRecord(access) || !isOneOf(access.state, BOOTSTRAP_ACCESS_STATES) ||
    typeof access.remote !== "boolean" || !isOneOf(access.handoffState, BOOTSTRAP_HANDOFF_STATES)
  ) {
    return "bootstrap access must name a known access and handoff state.";
  }
  if (bootstrap.onboarding !== null && bootstrap.onboarding !== undefined && !isRecord(bootstrap.onboarding)) {
    return "bootstrap onboarding must be an object or null.";
  }
  for (const key of ["resources", "connections", "models", "capabilities", "profiles"] as const) {
    const list = bootstrap[key];
    if (!Array.isArray(list) || list.some((entry) => !isRecord(entry))) {
      return `bootstrap ${key} must be an array of records.`;
    }
  }
  if (
    bootstrap.providers !== undefined &&
    (!Array.isArray(bootstrap.providers) || bootstrap.providers.some((entry) => !isRecord(entry)))
  ) {
    return "bootstrap providers must be an array of records when present.";
  }
  if (typeof bootstrap.ready !== "boolean") return "bootstrap ready must be a boolean.";
  return null;
}

/** Observability projection: the reconciler bounds its timeline and activities. */
export function validateObservabilityValue(observability: unknown): string | null {
  if (!isRecord(observability)) return "observability must be an object.";
  if (!isRecord(observability.job)) return "observability.job must be an object.";
  if (!Array.isArray(observability.workers) || observability.workers.some((worker) => !isRecord(worker))) {
    return "observability.workers must be an array of records.";
  }
  if (!Array.isArray(observability.timeline)) return "observability.timeline must be an array.";
  if (!Array.isArray(observability.activities)) return "observability.activities must be an array.";
  return null;
}

/**
 * Canonical Job execution projection. Execution, call, and executor states
 * stay open: unknown states render verbatim downstream, so only identity,
 * ordering, and collection shape are checked here.
 */
export function validateJobExecutionValue(execution: unknown): string | null {
  if (!isRecord(execution)) return "execution must be an object.";
  if (!isNonEmptyString(execution.jobId)) return "execution.jobId is required.";
  if (!isProjectId(execution.projectId)) return "execution.projectId is required.";
  if (!isNonEmptyString(execution.apiVersion)) return "execution apiVersion is required.";
  if (!isNonNegativeInteger(execution.generation) || !isNonNegativeInteger(execution.cursor)) {
    return "execution generation and cursor must be non-negative integers.";
  }
  if (!Array.isArray(execution.attempts) || !Array.isArray(execution.calls) || !Array.isArray(execution.executors)) {
    return "execution entities must be arrays.";
  }
  return null;
}

/** Ledger entry, as ledger events and snapshot ledgers carry it. */
export function validateLedgerEntryValue(entry: unknown): string | null {
  if (!isRecord(entry) || !isNonEmptyString(entry.id)) return "ledger entry.id is required.";
  if (!isNonEmptyString(entry.jobId)) return "ledger entry.jobId is required.";
  if (!isNonEmptyString(entry.timestamp)) return "ledger entry.timestamp is required.";
  if (!isNonNegativeInteger(entry.attempt) || entry.attempt < 1) {
    return "ledger entry.attempt must be a positive integer.";
  }
  if (
    entry.costMicros !== null &&
    (typeof entry.costMicros !== "number" || !Number.isSafeInteger(entry.costMicros) || entry.costMicros < 0)
  ) {
    return "ledger entry.costMicros must be a non-negative safe integer or null.";
  }
  return null;
}

/** Attention item, as attention events and snapshot lists carry it. */
export function validateAttentionItemValue(item: unknown): string | null {
  if (!isRecord(item)) return "attention item must be an object.";
  if (!isNonEmptyString(item.id)) return "attention item.id is required.";
  if (!isOneOf(item.kind, ATTENTION_KINDS)) return `Unknown attention kind "${String(item.kind)}".`;
  if (!isOneOf(item.status, ATTENTION_LIFECYCLES)) return "attention item.status is invalid.";
  if (!isOneOf(item.severity, ATTENTION_SEVERITIES)) {
    return `Unknown attention severity "${String(item.severity)}".`;
  }
  if (typeof item.title !== "string" || typeof item.summary !== "string") {
    return "attention item title and summary must be strings.";
  }
  if (!isNonEmptyString(item.createdAt) || !isNonEmptyString(item.updatedAt)) {
    return "attention item timestamps are required.";
  }
  if (!isOneOf(item.source, ATTENTION_SOURCES)) return `Unknown attention source "${String(item.source)}".`;
  if (!isOneOf(item.destination, ATTENTION_DESTINATIONS)) {
    return `Unknown attention destination "${String(item.destination)}".`;
  }
  if (item.projectId !== undefined && !isProjectId(item.projectId)) {
    return "attention item.projectId must be a known Project ID.";
  }
  for (const key of ["approval", "blocked", "resolution"] as const) {
    if (item[key] !== undefined && item[key] !== null && !isRecord(item[key])) {
      return `attention item.${key} must be an object or null when present.`;
    }
  }
  return null;
}

/** Log entry, as log events and snapshot logs carry it. */
export function validateLogEntryValue(entry: unknown): string | null {
  if (!isRecord(entry) || !isNonEmptyString(entry.id)) return "log entry.id is required.";
  if (!isIsoDate(entry.timestamp)) return "log entry.timestamp must be a valid timestamp.";
  if (!isNonEmptyString(entry.source)) return "log entry.source is required.";
  if (!isOneOf(entry.level, LOG_LEVELS)) return "log entry.level is invalid.";
  if (typeof entry.message !== "string") return "log entry.message must be a string.";
  return null;
}

/** Command acknowledgement, as launch-updated events carry it. */
export function validateLaunchResultValue(result: unknown): string | null {
  if (!isRecord(result)) return "result must be an object.";
  if (!isOneOf(result.outcome, JOB_LAUNCH_OUTCOMES)) {
    return `Unknown launch outcome "${String(result.outcome)}".`;
  }
  if (!isNonEmptyString(result.commandId)) return "result.commandId is required.";
  if (!isNonEmptyString(result.draftId)) return "result.draftId is required.";
  if (!isProjectId(result.projectId)) return "result.projectId is required.";
  if (!isNonEmptyString(result.sessionId)) return "result.sessionId is required.";
  if (typeof result.message !== "string") return "result.message must be a string.";
  if (typeof result.duplicate !== "boolean") return "result.duplicate must be a boolean.";
  return null;
}

/** One queued chat message, as queue events and snapshot queues carry it. */
export function validateQueuedMessageValue(item: unknown): string | null {
  if (!isRecord(item) || !isNonEmptyString(item.id)) return "queued message.id is required.";
  if (!isRecord(item.input) || typeof item.input.content !== "string") {
    return "queued message input must carry content text.";
  }
  if (
    item.input.images !== undefined &&
    (!Array.isArray(item.input.images) || item.input.images.some((image) => validateChatImageValue(image) !== null))
  ) {
    return "queued message images must contain valid images.";
  }
  return null;
}

/** Tool activity, as activity events carry it. */
export function validateToolActivityValue(activity: unknown): string | null {
  if (!isRecord(activity) || !isNonEmptyString(activity.id)) return "activity.id is required.";
  if (
    typeof activity.name !== "string" || typeof activity.status !== "string" ||
    typeof activity.summary !== "string" || typeof activity.detail !== "string"
  ) {
    return "activity must name its tool and carry summary text.";
  }
  return null;
}

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
      if (typeof payload.paused !== "boolean" || !Array.isArray(payload.queue)) {
        return "queue must contain valid queued chat messages.";
      }
      for (const item of payload.queue) {
        const error = validateQueuedMessageValue(item);
        if (error) return error;
      }
      return null;
    }
    case "job.execution-updated": {
      const executionError = validateJobExecutionValue(payload.execution);
      if (executionError) return executionError;
      if (payload.accounting !== null && !isRecord(payload.accounting)) return "accounting must be an object or null.";
      return null;
    }
    case "runtime.status-changed": {
      return validateRuntimeStatusValue(payload.status);
    }
    case "conversation.session-created":
    case "conversation.session-updated": {
      return validateChatSessionValue(payload.session);
    }
    case "conversation.message-started":
    case "conversation.message-completed": {
      return validateChatMessageValue(payload.message);
    }
    case "conversation.history-loaded": {
      if (!Array.isArray(payload.messages)) return "messages must be an array.";
      for (const message of payload.messages) {
        const error = validateChatMessageValue(message);
        if (error) return error;
      }
      return null;
    }
    case "conversation.message-image": {
      if (!isNonEmptyString(payload.messageId)) return "messageId is required.";
      return validateChatImageValue(payload.image);
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
      if (!isNonNegativeInteger(payload.committedReasoningLength)) return "committedReasoningLength must be a non-negative integer.";
      return null;
    }
    case "activity.updated": {
      if (!isNonEmptyString(payload.messageId)) return "messageId is required.";
      return validateToolActivityValue(payload.activity);
    }
    case "observability.updated": {
      return validateObservabilityValue(payload.observability);
    }
    case "job.launch-updated": {
      return validateLaunchResultValue(payload.result);
    }
    case "attention.updated": {
      return validateAttentionItemValue(payload.item);
    }
    case "ledger.entry-added":
    case "ledger.entry-updated": {
      return validateLedgerEntryValue(payload.entry);
    }
    case "log.appended": {
      return validateLogEntryValue(payload.entry);
    }
    case "bootstrap.updated": {
      return validateBootstrapValue(payload.bootstrap);
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
