/**
 * Runtime decoders for the canonical control contract.
 *
 * The shapes below are the generated projection of `src/contracts.rs`. These
 * replace the unchecked `as` casts the canonical client used: a payload whose
 * shape has drifted from the Rust definition now raises a contract violation
 * naming the offending path instead of reaching a component as `undefined`.
 */

import type {
  ChatConversationView,
  ChatConversationsResponse,
  ChatMessageView,
  ChatImage,
  ChatMessagesResponse,
  CanonicalApiVersion,
  CanonicalConfigurationEnvelope,
  CanonicalConfigurationResponse,
  CanonicalDashboardResponse,
  CanonicalEventsEnvelope,
  CanonicalJobConfigEnvelope,
  CanonicalJobConfigResponse,
  CanonicalJobEvent,
  CanonicalJobRelations,
  CanonicalJobSpawnResponse,
  ChildPolicy,
  JobOrigin,
  CanonicalJobOperations,
  CanonicalJobOperationResponse,
  Failure,
  FailureClass,
  Actor,
  EntityRef,
  CanonicalJobSnapshot,
  CanonicalJobSummary,
  HealthProbeIntent,
  CanonicalProjectResponse,
  CanonicalProjectsResponse,
  DiskGuardConfig,
  DiskGuardStatus,
  DiskState,
  GlobalConfiguration,
  JsonValue,
  ProjectConfiguration,
  ProjectConfigurationView,
  ProjectRecord,
  RecursiveLimits,
  ResourceBudget,
} from "./generated";
import { CANONICAL_API_VERSION } from "./generated";
import {
  array,
  bad,
  atLeast,
  boolean,
  decode,
  identity,
  index,
  isRecord,
  jsonValue,
  literal,
  nullable,
  number,
  oneOf,
  opt,
  record,
  req,
  string,
  stringMap,
  yes,
  type DecodeResult,
  type Decoder,
} from "./decode";



const chatConversationView: Decoder<ChatConversationView> = (input, path) => {
  const rec = record(input, path, "ChatConversationView");
  if (!rec.ok) return rec;
  const conversation_id = req(rec.value, "conversation_id", identity, path);
  if (!conversation_id.ok) return conversation_id;
  const session_id = req(rec.value, "session_id", identity, path);
  if (!session_id.ok) return session_id;
  const title = req(rec.value, "title", nullable(string), path);
  if (!title.ok) return title;
  const created_at = req(rec.value, "created_at", string, path);
  if (!created_at.ok) return created_at;
  const updated_at = req(rec.value, "updated_at", string, path);
  if (!updated_at.ok) return updated_at;
  return yes({
    conversation_id: conversation_id.value,
    session_id: session_id.value,
    title: title.value,
    created_at: created_at.value,
    updated_at: updated_at.value,
  });
};

const chatConversationsResponse: Decoder<ChatConversationsResponse> = (input, path) => {
  const rec = record(input, path, "ChatConversationsResponse");
  if (!rec.ok) return rec;
  const api_version = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
  if (!api_version.ok) return api_version;
  const project_id = req(rec.value, "project_id", identity, path);
  if (!project_id.ok) return project_id;
  const conversations = req(rec.value, "conversations", array(chatConversationView), path);
  if (!conversations.ok) return conversations;
  return yes({
    api_version: api_version.value,
    project_id: project_id.value,
    conversations: conversations.value,
  });
};

export function decodeChatConversationsResponse(input: unknown): ChatConversationsResponse {
  return decode((input) => chatConversationsResponse(input, ""), input);
}

const chatImage: Decoder<ChatImage> = (input, path) => {
  const rec = record(input, path, "ChatImage");
  if (!rec.ok) return rec;
  const id = req(rec.value, "id", identity, path);
  if (!id.ok) return id;
  const name = req(rec.value, "name", string, path);
  if (!name.ok) return name;
  const media_type = req(rec.value, "media_type", string, path);
  if (!media_type.ok) return media_type;
  const url = req(rec.value, "url", string, path);
  if (!url.ok) return url;
  return yes({ id: id.value, name: name.value, media_type: media_type.value, url: url.value });
};

export function decodeChatImage(input: unknown): ChatImage {
  return decode((input) => chatImage(input, ""), input);
}

const chatMessageView: Decoder<ChatMessageView> = (input, path) => {
  const rec = record(input, path, "ChatMessageView");
  if (!rec.ok) return rec;
  const message_id = req(rec.value, "message_id", identity, path);
  if (!message_id.ok) return message_id;
  const command_id = req(rec.value, "command_id", identity, path);
  if (!command_id.ok) return command_id;
  const role = req(rec.value, "role", oneOf(["user", "assistant"] as const), path);
  if (!role.ok) return role;
  const state = req(rec.value, "state", oneOf(["pending", "streaming", "complete", "failed", "deleted"] as const), path);
  if (!state.ok) return state;
  const images = opt(rec.value, "images", array(chatImage), path);
  if (!images.ok) return images;
  const content = req(rec.value, "content", string, path);
  if (!content.ok) return content;
  const failure_reason = opt(rec.value, "failure_reason", nullable(string), path);
  if (!failure_reason.ok) return failure_reason;
  const job_id = opt(rec.value, "job_id", nullable(identity), path);
  if (!job_id.ok) return job_id;
  const created_at = req(rec.value, "created_at", string, path);
  if (!created_at.ok) return created_at;
  const updated_at = req(rec.value, "updated_at", string, path);
  if (!updated_at.ok) return updated_at;
  const attempt_state = req(rec.value, "attempt_state", string, path);
  if (!attempt_state.ok) return attempt_state;
  const replay_job_id = req(rec.value, "replay_job_id", nullable(identity), path);
  if (!replay_job_id.ok) return replay_job_id;
  return yes({
    message_id: message_id.value,
    command_id: command_id.value,
    role: role.value,
    state: state.value,
    content: content.value,
    images: images.value ?? [],
    failure_reason: failure_reason.value ?? null,
    job_id: job_id.value ?? null,
    created_at: created_at.value,
    updated_at: updated_at.value,
    attempt_state: attempt_state.value,
    replay_job_id: replay_job_id.value,
  });
};

const chatMessagesResponse: Decoder<ChatMessagesResponse> = (input, path) => {
  const rec = record(input, path, "ChatMessagesResponse");
  if (!rec.ok) return rec;
  const api_version = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
  if (!api_version.ok) return api_version;
  const project_id = req(rec.value, "project_id", identity, path);
  if (!project_id.ok) return project_id;
  const conversation = req(rec.value, "conversation", chatConversationView, path);
  if (!conversation.ok) return conversation;
  const messages = req(rec.value, "messages", array(chatMessageView), path);
  if (!messages.ok) return messages;
  return yes({
    api_version: api_version.value,
    project_id: project_id.value,
    conversation: conversation.value,
    messages: messages.value,
  });
};

export function decodeChatMessagesResponse(input: unknown): ChatMessagesResponse {
  return decode((input) => chatMessagesResponse(input, ""), input);
}

const projectRecord: Decoder<ProjectRecord> = (input, path) => {
  const rec = record(input, path, "a ProjectRecord");
  if (!rec.ok) return rec;
  const projectId = req(rec.value, "project_id", string, path);
  if (!projectId.ok) return projectId;
  const root = req(rec.value, "root", string, path);
  if (!root.ok) return root;
  const boundary = req(rec.value, "boundary", string, path);
  if (!boundary.ok) return boundary;
  const marker = req(rec.value, "marker", boolean, path);
  if (!marker.ok) return marker;
  const createdAt = req(rec.value, "created_at", number, path);
  if (!createdAt.ok) return createdAt;
  const updatedAt = req(rec.value, "updated_at", number, path);
  if (!updatedAt.ok) return updatedAt;
  return yes({
    project_id: projectId.value,
    root: root.value,
    boundary: boundary.value,
    marker: marker.value,
    created_at: createdAt.value,
    updated_at: updatedAt.value,
  });
};

/* -------------------------------------------------------------------------- */
/* The canonical execution state                                               */
/* -------------------------------------------------------------------------- */

/**
 * The Job, Attempt and Call records the canonical snapshot embeds.
 *
 * `CanonicalJobSnapshot::job` is an unconstrained `serde_json::Value` in
 * `src/orchestration/canonical_control.rs`, assembled by `canonical_snapshot`
 * from the authoritative records in `src/orchestration/domain.rs`. Those
 * records carry no `ts_rs` projection of their own — the field set here mirrors
 * the Rust structs exactly, and each decoder is the check that keeps the mirror
 * honest: a renamed or retyped field stops the payload here with a path,
 * instead of reaching a surface as `undefined`.
 */

/** The states `domain_jobs.state` admits. */
export const JOB_STATES = [
  "pending",
  "eligible",
  "running",
  "cancelling",
  "completed",
  "failed",
  "cancelled",
  "unknown",
  "orphaned",
] as const;
export type CanonicalJobState = (typeof JOB_STATES)[number];

/** The states `domain_attempts.state` admits. */
export const ATTEMPT_STATES = [
  "queued",
  "running",
  "cancelling",
  "completed",
  "failed",
  "cancelled",
  "unknown",
  "orphaned",
] as const;
export type CanonicalAttemptState = (typeof ATTEMPT_STATES)[number];

/** The effect classes `domain_dispatch_intents.effect_kind` admits. */
export const EFFECT_KINDS = [
  "idempotent",
  "strict_fenced",
  "reconcilable",
  "non_retryable",
] as const;
export type CanonicalEffectKind = (typeof EFFECT_KINDS)[number];

/** One unit of work, owned by exactly one Project. */
export type CanonicalJob = CanonicalJobRelations & CanonicalJobOperations & {
  termination_reason: Failure | null;
  id: string;
  project_id: string;
  state: CanonicalJobState;
  generation: number;
  authoritative_attempt_id: string | null;
  payload: string;
  created_at: number;
  updated_at: number;
  /**
   * `domain_jobs.waiting_for_children`: the Job's Attempt completed while a
   * required child Job is still unsettled, so the Job is held rather than
   * completed. The backend emits it on every Job; it is optional here only so
   * hand-built literals still typecheck, and the decoder requires the key.
   */
  waiting_for_children?: boolean;
  /**
   * The recursion guardrails recorded in the Job's own specification payload.
   * A child copies its root's limits, so these are the root's limits wherever
   * they are read. Absent when the stored payload predates them or is plain text.
   */
  recursive_limits?: RecursiveLimits;
};

/**
 * One Watchdog action, as `domain_watchdog_actions` recorded it.
 *
 * `WatchdogActionRecord` is serialized straight into the snapshot's `watchdog`
 * array and carries no `ts_rs` projection, so this mirrors its serde shape.
 * `classification`, `action` and `outcome` are bare strings in the substrate and
 * stay strings here: the UI renders the ones it recognizes and shows any other
 * word verbatim rather than coercing it. Absent optional ids are `null`.
 */
export type CanonicalWatchdogAction = {
  job_id: string;
  attempt_id: string | null;
  generation: number | null;
  call_id: string | null;
  executor_id: string | null;
  classification: string;
  action: string;
  evidence: JsonValue;
  outcome: string;
  replacement_attempt_id: string | null;
  created_at: number;
};

/**
 * One generation of a Job. At most one Attempt of a Job is authoritative at a
 * time; the rest are retained as history.
 */
export type CanonicalAttempt = {
  id: string;
  job_id: string;
  generation: number;
  state: CanonicalAttemptState;
  authoritative: boolean;
  created_at: number;
  finished_at: number | null;
};

/**
 * One invocation inside an Attempt.
 *
 * `state` is a bare column in the substrate rather than an enum, so it is
 * decoded as the string the backend stored instead of being forced into a
 * closed vocabulary the frontend would have to guess.
 */
export type CanonicalCall = {
  id: string;
  attempt_id: string;
  executor_id: string | null;
  generation: number;
  side_effect: boolean;
  effect_kind: CanonicalEffectKind;
  state: string;
  request: string;
  response: string | null;
  created_at: number;
  finished_at: number | null;
};

/**
 * One Executor the backend recorded for an Attempt.
 *
 * `kind` and `state` are unconstrained columns in the substrate, so they are
 * decoded as the strings the backend stored rather than being forced into a
 * closed vocabulary the frontend would have to guess.
 */
export type CanonicalExecutor = {
  id: string;
  attempt_id: string;
  kind: string;
  state: string;
  created_at: number;
};

/**
 * The effect lifecycle a DispatchIntent records.
 *
 * `state` and `effect_state` are the substrate's own columns; `effect_kind` is
 * the frozen effect class the Call schema admitted.
 */
export type CanonicalDispatchIntent = {
  id: string;
  call_id: string;
  job_id: string;
  attempt_id: string;
  executor_id: string | null;
  generation: number;
  state: string;
  effect_kind: CanonicalEffectKind;
  effect_state: string;
  request: string;
  budget_admitted: boolean;
  failure: string | null;
  created_at: number;
  updated_at: number;
  /**
   * The execution target the backend froze on this dispatch before the Provider
   * ran. Placement decides it once per admission and these columns are that
   * decision as it stood for *this* dispatch, so they are historical evidence
   * rather than a view of the current Profile.
   *
   * `null` means no target was recorded — a native tool Call is dispatched
   * without a Provider. The budget reservation the dispatch admitted against is
   * recorded alongside them.
   *
   * These are declared optional only so hand-built `CanonicalExecutionState`
   * literals still typecheck; the decoder requires every key, so nothing decoded
   * from a backend payload can leave them unset.
   */
  reservation_id?: string | null;
  provider_key?: string | null;
  model?: string | null;
  upstream_model_id?: string | null;
};

/**
 * The three outcomes `JobLaunchResponse.outcome` can carry.
 *
 * Rust types the field as a bare `String`; the canonical launch path only ever
 * writes these three, so the decoder admits exactly them and reports anything
 * else rather than coercing it into a nearest known value.
 */
export const JOB_LAUNCH_OUTCOMES = ["accepted", "rejected", "failed"] as const;
export type CanonicalJobLaunchOutcome = (typeof JOB_LAUNCH_OUTCOMES)[number];

/** The `JobLaunchResponse` whose `outcome` is narrowed to its real vocabulary. */
export type CanonicalJobLaunchAck = {
  api_version: CanonicalApiVersion;
  outcome: CanonicalJobLaunchOutcome;
  command_id: string;
  draft_id: string;
  project_id: string;
  session_id: string;
  job_id: string | null;
  message: string;
  duplicate: boolean;
};

/** The `job` payload of a `CanonicalJobSnapshot`. */
export type CanonicalExecutionState = {
  job: CanonicalJob;
  attempts: CanonicalAttempt[];
  executors: CanonicalExecutor[];
  calls: CanonicalCall[];
  dispatchIntents: CanonicalDispatchIntent[];
  /**
   * Watchdog actions for this Job, oldest first. An empty array means the
   * backend carried the record and it holds none. Optional only so hand-built
   * literals still typecheck; the decoder requires the key.
   */
  watchdog?: CanonicalWatchdogAction[];
  /** The backend's own note on its graph projection, kept verbatim. */
  executionGraph: string;
};

const jobState = oneOf<CanonicalJobState>(JOB_STATES);
const attemptState = oneOf<CanonicalAttemptState>(ATTEMPT_STATES);
const effectKind = oneOf<CanonicalEffectKind>(EFFECT_KINDS);

const entityRef: Decoder<EntityRef> = (input, path) => {
  const rec = record(input, path, "an entity reference");
  if (!rec.ok) return rec;
  const kind = req(rec.value, "kind", oneOf<EntityRef["kind"]>(["job", "attempt", "call", "command", "approval", "artifact", "change_set", "conversation", "message", "fact", "budget_scope", "capability_revocation"]), path);
  if (!kind.ok) return kind;
  const id = req(rec.value, "id", identity, path);
  if (!id.ok) return id;
  return yes({ kind: kind.value, id: id.value });
};

const actor: Decoder<Actor> = (input, path) => {
  const rec = record(input, path, "a canonical actor");
  if (!rec.ok) return rec;
  switch (rec.value.kind) {
    case "core": return yes({ kind: "core" });
    case "user": {
      const id = req(rec.value, "user_id", identity, path);
      return id.ok ? yes({ kind: "user", user_id: id.value }) : id;
    }
    case "client": {
      const kind = req(rec.value, "client_kind", oneOf<"pwa" | "cli">(["pwa", "cli"]), path);
      if (!kind.ok) return kind;
      const id = req(rec.value, "client_id", nullable(string), path);
      return id.ok ? yes({ kind: "client", client_kind: kind.value, client_id: id.value }) : id;
    }
    case "attempt": {
      const ref = req(rec.value, "attempt_ref", entityRef, path);
      return ref.ok ? yes({ kind: "attempt", attempt_ref: ref.value }) : ref;
    }
    case "call": {
      const ref = req(rec.value, "call_ref", entityRef, path);
      return ref.ok ? yes({ kind: "call", call_ref: ref.value }) : ref;
    }
    case "system": {
      const ref = req(rec.value, "policy_ref", nullable(string), path);
      return ref.ok ? yes({ kind: "system", policy_ref: ref.value }) : ref;
    }
    case "external": {
      const kind = req(rec.value, "source_kind", oneOf<"provider" | "capability" | "runtime">(["provider", "capability", "runtime"]), path);
      if (!kind.ok) return kind;
      const ref = req(rec.value, "source_ref", string, path);
      return ref.ok ? yes({ kind: "external", source_kind: kind.value, source_ref: ref.value }) : ref;
    }
    default: return bad(`${path}.kind`, "a canonical actor kind");
  }
};

const failure: Decoder<Failure> = (input, path) => {
  const rec = record(input, path, "a canonical Failure");
  if (!rec.ok) return rec;
  const code = req(rec.value, "code", identity, path);
  if (!code.ok) return code;
  const classValue = req(rec.value, "class", oneOf<FailureClass>(["validation", "authentication", "authorization", "conflict", "concurrency", "not_found", "sandbox", "capability", "provider", "budget", "resource_limit", "rate_limit", "timeout", "cancelled", "preempted", "internal", "unknown"]), path);
  if (!classValue.ok) return classValue;
  const message = req(rec.value, "message", string, path);
  if (!message.ok) return message;
  const source = req(rec.value, "source", actor, path);
  if (!source.ok) return source;
  const retryable = req(rec.value, "retryable", boolean, path);
  if (!retryable.ok) return retryable;
  const details = req(rec.value, "details", nullable(jsonValue), path);
  if (!details.ok) return details;
  const cause = req(rec.value, "cause", nullable(entityRef), path);
  if (!cause.ok) return cause;
  const entity = req(rec.value, "entity_ref", nullable(entityRef), path);
  if (!entity.ok) return entity;
  return yes({ code: code.value, class: classValue.value, message: message.value, source: source.value, retryable: retryable.value, details: details.value, cause: cause.value, entity_ref: entity.value });
};

const jobOperations: Decoder<CanonicalJobOperations & { termination_reason: Failure | null }> = (input, path) => {
  const rec = record(input, path, "canonical Job operation facts");
  if (!rec.ok) return rec;
  const cancel = req(rec.value, "can_cancel", boolean, path);
  if (!cancel.ok) return cancel;
  const retry = req(rec.value, "can_retry", boolean, path);
  if (!retry.ok) return retry;
  const reason = req(rec.value, "termination_reason", nullable(failure), path);
  if (!reason.ok) return reason;
  return yes({ can_cancel: cancel.value, can_retry: retry.value, termination_reason: reason.value });
};

const childPolicy: Decoder<ChildPolicy> = (input, path) => {
  const rec = record(input, path, "a child supervision policy");
  if (!rec.ok) return rec;
  const join = req(rec.value, "join", oneOf<ChildPolicy["join"]>(["required", "not_required"]), path);
  if (!join.ok) return join;
  const cancellation = req(rec.value, "cancellation", oneOf<ChildPolicy["cancellation"]>(["cascade", "independent"]), path);
  if (!cancellation.ok) return cancellation;
  const failure = req(rec.value, "failure", oneOf<ChildPolicy["failure"]>(["observe", "block_parent", "fail_parent"]), path);
  if (!failure.ok) return failure;
  return yes({ join: join.value, cancellation: cancellation.value, failure: failure.value });
};

const jobOrigin: Decoder<JobOrigin> = (input, path) => {
  const rec = record(input, path, "a child Job origin");
  if (!rec.ok) return rec;
  const parent = req(rec.value, "parent_job_id", identity, path);
  if (!parent.ok) return parent;
  const attempt = req(rec.value, "attempt_id", identity, path);
  if (!attempt.ok) return attempt;
  const generation = req(rec.value, "generation", index, path);
  if (!generation.ok) return generation;
  const key = req(rec.value, "spawn_key", nullable(identity), path);
  if (!key.ok) return key;
  const fingerprint = req(rec.value, "spawn_fingerprint", nullable(identity), path);
  if (!fingerprint.ok) return fingerprint;
  const callId = req(rec.value, "call_id", nullable(identity), path);
  if (!callId.ok) return callId;
  const policy = req(rec.value, "policy", nullable(childPolicy), path);
  if (!policy.ok) return policy;
  return yes({ parent_job_id: parent.value, attempt_id: attempt.value, generation: generation.value, spawn_key: key.value, spawn_fingerprint: fingerprint.value, call_id: callId.value, policy: policy.value });
};

const jobRelations: Decoder<CanonicalJobRelations> = (input, path) => {
  const rec = record(input, path, "canonical Job relationships");
  if (!rec.ok) return rec;
  const parentJobId = req(rec.value, "parent_job_id", nullable(identity), path);
  if (!parentJobId.ok) return parentJobId;
  const origin = opt(rec.value, "origin", nullable(jobOrigin), path);
  if (!origin.ok) return origin;
  if (origin.value && origin.value.parent_job_id !== parentJobId.value) return bad(`${path}.origin.parent_job_id`, "the projected parent Job identity");
  const childJobIds = req(rec.value, "child_job_ids", array(identity), path);
  if (!childJobIds.ok) return childJobIds;
  const dependsOn = req(rec.value, "depends_on", array(identity), path);
  if (!dependsOn.ok) return dependsOn;
  const blocks = req(rec.value, "blocks", array(identity), path);
  if (!blocks.ok) return blocks;
  const blockedBy = req(rec.value, "blocked_by", array(identity), path);
  if (!blockedBy.ok) return blockedBy;
  const blocked = req(rec.value, "blocked", boolean, path);
  if (!blocked.ok) return blocked;
  const rootJobId = req(rec.value, "root_job_id", identity, path);
  if (!rootJobId.ok) return rootJobId;
  const depth = req(rec.value, "depth", index, path);
  if (!depth.ok) return depth;
  const descendantJobIds = req(rec.value, "descendant_job_ids", array(identity), path);
  if (!descendantJobIds.ok) return descendantJobIds;
  const descendantSummary = req(rec.value, "descendant_summary", stringMap(number), path);
  if (!descendantSummary.ok) return descendantSummary;
  return yes({
    parent_job_id: parentJobId.value,
    origin: origin.value ?? null,
    child_job_ids: childJobIds.value,
    depends_on: dependsOn.value,
    blocks: blocks.value,
    blocked_by: blockedBy.value,
    blocked: blocked.value,
    root_job_id: rootJobId.value,
    depth: depth.value,
    descendant_job_ids: descendantJobIds.value,
    descendant_summary: descendantSummary.value,
  });
};

const recursiveLimits: Decoder<RecursiveLimits> = (input, path) => {
  const rec = record(input, path, "recursive limits");
  if (!rec.ok) return rec;
  const maxDepth = req(rec.value, "max_depth", index, path);
  if (!maxDepth.ok) return maxDepth;
  const maxChildren = req(rec.value, "max_children_per_job", index, path);
  if (!maxChildren.ok) return maxChildren;
  const maxDescendants = req(rec.value, "max_total_descendants_per_root", index, path);
  if (!maxDescendants.ok) return maxDescendants;
  return yes({
    max_depth: maxDepth.value,
    max_children_per_job: maxChildren.value,
    max_total_descendants_per_root: maxDescendants.value,
  });
};

/**
 * The recursion limits inside a Job's specification payload.
 *
 * `payload` is the JSON-encoded `JobSpec`. Older CLI Jobs stored plain-text
 * objectives, which the backend reads as an unconstrained default spec; here
 * that is "not recorded", because the PWA will not restate backend defaults as
 * though the Job had declared them. A JSON object that carries
 * `recursive_limits` must carry a well-formed one.
 */
const payloadRecursiveLimits: Decoder<RecursiveLimits | undefined> = (input, path) => {
  const payload = string(input, path);
  if (!payload.ok) return payload;
  let parsed: unknown;
  try {
    parsed = JSON.parse(payload.value);
  } catch {
    return yes(undefined);
  }
  if (!isRecord(parsed)) return yes(undefined);
  return opt(parsed, "recursive_limits", recursiveLimits, `${path}`);
};

const job: Decoder<CanonicalJob> = (input, path) => {
  const rec = record(input, path, "a canonical Job");
  if (!rec.ok) return rec;
  const id = req(rec.value, "id", identity, path);
  if (!id.ok) return id;
  const projectId = req(rec.value, "project_id", identity, path);
  if (!projectId.ok) return projectId;
  const state = req(rec.value, "state", jobState, path);
  if (!state.ok) return state;
  const generation = req(rec.value, "generation", index, path);
  if (!generation.ok) return generation;
  const authoritativeAttemptId = req(rec.value, "authoritative_attempt_id", nullable(identity), path);
  if (!authoritativeAttemptId.ok) return authoritativeAttemptId;
  const payload = req(rec.value, "payload", string, path);
  if (!payload.ok) return payload;
  const createdAt = req(rec.value, "created_at", number, path);
  if (!createdAt.ok) return createdAt;
  const updatedAt = req(rec.value, "updated_at", number, path);
  if (!updatedAt.ok) return updatedAt;
  const relations = jobRelations(rec.value, path);
  if (!relations.ok) return relations;
  const operations = jobOperations(rec.value, path);
  if (!operations.ok) return operations;
  const waitingForChildren = req(rec.value, "waiting_for_children", boolean, path);
  if (!waitingForChildren.ok) return waitingForChildren;
  const limits = payloadRecursiveLimits(payload.value, `${path}.payload`);
  if (!limits.ok) return limits;
  return yes({
    id: id.value,
    project_id: projectId.value,
    state: state.value,
    generation: generation.value,
    authoritative_attempt_id: authoritativeAttemptId.value,
    payload: payload.value,
    created_at: createdAt.value,
    updated_at: updatedAt.value,
    waiting_for_children: waitingForChildren.value,
    ...(limits.value ? { recursive_limits: limits.value } : {}),
    ...relations.value,
    ...operations.value,
  });
};

const attempt: Decoder<CanonicalAttempt> = (input, path) => {
  const rec = record(input, path, "a canonical Attempt");
  if (!rec.ok) return rec;
  const id = req(rec.value, "id", identity, path);
  if (!id.ok) return id;
  const jobId = req(rec.value, "job_id", identity, path);
  if (!jobId.ok) return jobId;
  const generation = req(rec.value, "generation", atLeast(1), path);
  if (!generation.ok) return generation;
  const state = req(rec.value, "state", attemptState, path);
  if (!state.ok) return state;
  const authoritative = req(rec.value, "authoritative", boolean, path);
  if (!authoritative.ok) return authoritative;
  const createdAt = req(rec.value, "created_at", number, path);
  if (!createdAt.ok) return createdAt;
  const finishedAt = req(rec.value, "finished_at", nullable(number), path);
  if (!finishedAt.ok) return finishedAt;
  return yes({
    id: id.value,
    job_id: jobId.value,
    generation: generation.value,
    state: state.value,
    authoritative: authoritative.value,
    created_at: createdAt.value,
    finished_at: finishedAt.value,
  });
};

const call: Decoder<CanonicalCall> = (input, path) => {
  const rec = record(input, path, "a canonical Call");
  if (!rec.ok) return rec;
  const id = req(rec.value, "id", identity, path);
  if (!id.ok) return id;
  const attemptId = req(rec.value, "attempt_id", identity, path);
  if (!attemptId.ok) return attemptId;
  const executorId = req(rec.value, "executor_id", nullable(identity), path);
  if (!executorId.ok) return executorId;
  const generation = req(rec.value, "generation", atLeast(1), path);
  if (!generation.ok) return generation;
  const sideEffect = req(rec.value, "side_effect", boolean, path);
  if (!sideEffect.ok) return sideEffect;
  const kind = req(rec.value, "effect_kind", effectKind, path);
  if (!kind.ok) return kind;
  const state = req(rec.value, "state", string, path);
  if (!state.ok) return state;
  const request = req(rec.value, "request", string, path);
  if (!request.ok) return request;
  const response = req(rec.value, "response", nullable(string), path);
  if (!response.ok) return response;
  const createdAt = req(rec.value, "created_at", number, path);
  if (!createdAt.ok) return createdAt;
  const finishedAt = req(rec.value, "finished_at", nullable(number), path);
  if (!finishedAt.ok) return finishedAt;
  return yes({
    id: id.value,
    attempt_id: attemptId.value,
    executor_id: executorId.value,
    generation: generation.value,
    side_effect: sideEffect.value,
    effect_kind: kind.value,
    state: state.value,
    request: request.value,
    response: response.value,
    created_at: createdAt.value,
    finished_at: finishedAt.value,
  });
};

const executor: Decoder<CanonicalExecutor> = (input, path) => {
  const rec = record(input, path, "a canonical Executor");
  if (!rec.ok) return rec;
  const id = req(rec.value, "id", identity, path);
  if (!id.ok) return id;
  const attemptId = req(rec.value, "attempt_id", identity, path);
  if (!attemptId.ok) return attemptId;
  const kind = req(rec.value, "kind", string, path);
  if (!kind.ok) return kind;
  const state = req(rec.value, "state", string, path);
  if (!state.ok) return state;
  const createdAt = req(rec.value, "created_at", number, path);
  if (!createdAt.ok) return createdAt;
  return yes({
    id: id.value,
    attempt_id: attemptId.value,
    kind: kind.value,
    state: state.value,
    created_at: createdAt.value,
  });
};

const dispatchIntent: Decoder<CanonicalDispatchIntent> = (input, path) => {
  const rec = record(input, path, "a canonical DispatchIntent");
  if (!rec.ok) return rec;
  const id = req(rec.value, "id", identity, path);
  if (!id.ok) return id;
  const callId = req(rec.value, "call_id", identity, path);
  if (!callId.ok) return callId;
  const jobId = req(rec.value, "job_id", identity, path);
  if (!jobId.ok) return jobId;
  const attemptId = req(rec.value, "attempt_id", identity, path);
  if (!attemptId.ok) return attemptId;
  const executorId = req(rec.value, "executor_id", nullable(identity), path);
  if (!executorId.ok) return executorId;
  const generation = req(rec.value, "generation", atLeast(1), path);
  if (!generation.ok) return generation;
  const state = req(rec.value, "state", string, path);
  if (!state.ok) return state;
  const kind = req(rec.value, "effect_kind", effectKind, path);
  if (!kind.ok) return kind;
  const effectState = req(rec.value, "effect_state", string, path);
  if (!effectState.ok) return effectState;
  const request = req(rec.value, "request", string, path);
  if (!request.ok) return request;
  const budgetAdmitted = req(rec.value, "budget_admitted", boolean, path);
  if (!budgetAdmitted.ok) return budgetAdmitted;
  const failure = req(rec.value, "failure", nullable(string), path);
  if (!failure.ok) return failure;
  const createdAt = req(rec.value, "created_at", number, path);
  if (!createdAt.ok) return createdAt;
  const updatedAt = req(rec.value, "updated_at", number, path);
  if (!updatedAt.ok) return updatedAt;
  // The Rust contract serializes these as `Option<String>` without
  // `skip_serializing_if`, so canonical output always carries the key holding a
  // string or `null`. A missing key is a contract violation, not a `None`.
  const reservationId = req(rec.value, "reservation_id", nullable(string), path);
  if (!reservationId.ok) return reservationId;
  const providerKey = req(rec.value, "provider_key", nullable(string), path);
  if (!providerKey.ok) return providerKey;
  const model = req(rec.value, "model", nullable(string), path);
  if (!model.ok) return model;
  const upstreamModelId = req(rec.value, "upstream_model_id", nullable(string), path);
  if (!upstreamModelId.ok) return upstreamModelId;
  return yes({
    id: id.value,
    call_id: callId.value,
    job_id: jobId.value,
    attempt_id: attemptId.value,
    executor_id: executorId.value,
    generation: generation.value,
    state: state.value,
    effect_kind: kind.value,
    effect_state: effectState.value,
    request: request.value,
    budget_admitted: budgetAdmitted.value,
    failure: failure.value,
    created_at: createdAt.value,
    updated_at: updatedAt.value,
    reservation_id: reservationId.value,
    provider_key: providerKey.value,
    model: model.value,
    upstream_model_id: upstreamModelId.value,
  });
};

const watchdogAction: Decoder<CanonicalWatchdogAction> = (input, path) => {
  const rec = record(input, path, "a canonical Watchdog action");
  if (!rec.ok) return rec;
  const jobId = req(rec.value, "job_id", identity, path);
  if (!jobId.ok) return jobId;
  // The Rust record skips these when `None`, so an absent key is a real null.
  const attemptId = opt(rec.value, "attempt_id", identity, path);
  if (!attemptId.ok) return attemptId;
  const generation = opt(rec.value, "generation", index, path);
  if (!generation.ok) return generation;
  const callId = opt(rec.value, "call_id", identity, path);
  if (!callId.ok) return callId;
  const executorId = opt(rec.value, "executor_id", identity, path);
  if (!executorId.ok) return executorId;
  const classification = req(rec.value, "classification", string, path);
  if (!classification.ok) return classification;
  const action = req(rec.value, "action", string, path);
  if (!action.ok) return action;
  const evidence = req(rec.value, "evidence", jsonValue, path);
  if (!evidence.ok) return evidence;
  const outcome = req(rec.value, "outcome", string, path);
  if (!outcome.ok) return outcome;
  const replacement = opt(rec.value, "replacement_attempt_id", identity, path);
  if (!replacement.ok) return replacement;
  const createdAt = req(rec.value, "created_at", number, path);
  if (!createdAt.ok) return createdAt;
  return yes({
    job_id: jobId.value,
    attempt_id: attemptId.value ?? null,
    generation: generation.value ?? null,
    call_id: callId.value ?? null,
    executor_id: executorId.value ?? null,
    classification: classification.value,
    action: action.value,
    evidence: evidence.value,
    outcome: outcome.value,
    replacement_attempt_id: replacement.value ?? null,
    created_at: createdAt.value,
  });
};

const executionState: Decoder<CanonicalExecutionState> = (input, path) => {
  const rec = record(input, path, "a canonical execution state");
  if (!rec.ok) return rec;
  const decodedJob = req(rec.value, "job", job, path);
  if (!decodedJob.ok) return decodedJob;
  const attempts = req(rec.value, "attempts", array(attempt), path);
  if (!attempts.ok) return attempts;
  const executors = req(rec.value, "executors", array(executor), path);
  if (!executors.ok) return executors;
  const calls = req(rec.value, "calls", array(call), path);
  if (!calls.ok) return calls;
  const dispatchIntents = req(rec.value, "dispatch_intents", array(dispatchIntent), path);
  if (!dispatchIntents.ok) return dispatchIntents;
  const watchdog = req(rec.value, "watchdog", array(watchdogAction), path);
  if (!watchdog.ok) return watchdog;
  const graph = req(rec.value, "execution_graph", string, path);
  if (!graph.ok) return graph;
  return yes({
    job: decodedJob.value,
    attempts: attempts.value,
    executors: executors.value,
    calls: calls.value,
    dispatchIntents: dispatchIntents.value,
    watchdog: watchdog.value,
    executionGraph: graph.value,
  });
};

const resourceBudget: Decoder<ResourceBudget> = (input, path) => {
  const rec = record(input, path, "a ResourceBudget");
  if (!rec.ok) return rec;
  const hardLimit = req(rec.value, "hard_limit", number, path);
  if (!hardLimit.ok) return hardLimit;
  const unit = req(rec.value, "unit", string, path);
  if (!unit.ok) return unit;
  return yes({ hard_limit: hardLimit.value, unit: unit.value });
};

const diskState: Decoder<DiskState> = (input, path) =>
  oneOf<DiskState>(["healthy", "pressure", "critical", "unknown"])(input, path);

const diskGuardConfig: Decoder<DiskGuardConfig> = (input, path) => {
  const rec = record(input, path, "a DiskGuardConfig");
  if (!rec.ok) return rec;
  const minimum = req(rec.value, "minimum_free_bytes", number, path);
  if (!minimum.ok) return minimum;
  const pressure = req(rec.value, "pressure_free_bytes", number, path);
  if (!pressure.ok) return pressure;
  const resume = req(rec.value, "resume_free_bytes", number, path);
  if (!resume.ok) return resume;
  return yes({
    minimum_free_bytes: minimum.value,
    pressure_free_bytes: pressure.value,
    resume_free_bytes: resume.value,
  });
};

const diskGuardStatus: Decoder<DiskGuardStatus> = (input, path) => {
  const rec = record(input, path, "a DiskGuardStatus");
  if (!rec.ok) return rec;
  const state = req(rec.value, "state", diskState, path);
  if (!state.ok) return state;
  const available = req(rec.value, "available_bytes", number, path);
  if (!available.ok) return available;
  const total = req(rec.value, "total_bytes", number, path);
  if (!total.ok) return total;
  const reserve = req(rec.value, "reserve_bytes", number, path);
  if (!reserve.ok) return reserve;
  const pressure = req(rec.value, "pressure_bytes", number, path);
  if (!pressure.ok) return pressure;
  const resume = req(rec.value, "resume_bytes", number, path);
  if (!resume.ok) return resume;
  const observedAt = req(rec.value, "observed_at", number, path);
  if (!observedAt.ok) return observedAt;
  const root = req(rec.value, "root", string, path);
  if (!root.ok) return root;
  const defersExecution = req(rec.value, "defers_new_execution", boolean, path);
  if (!defersExecution.ok) return defersExecution;
  const defersExpansion = req(rec.value, "defers_expansion", boolean, path);
  if (!defersExpansion.ok) return defersExpansion;
  return yes({
    state: state.value,
    available_bytes: available.value,
    total_bytes: total.value,
    reserve_bytes: reserve.value,
    pressure_bytes: pressure.value,
    resume_bytes: resume.value,
    observed_at: observedAt.value,
    root: root.value,
    defers_new_execution: defersExecution.value,
    defers_expansion: defersExpansion.value,
  });
};

const globalConfiguration: Decoder<GlobalConfiguration> = (input, path) => {
  const rec = record(input, path, "a GlobalConfiguration");
  if (!rec.ok) return rec;
  const provider = req(rec.value, "provider", nullable(string), path);
  if (!provider.ok) return provider;
  const model = req(rec.value, "model", nullable(string), path);
  if (!model.ok) return model;
  const profileName = req(rec.value, "profile", nullable(string), path);
  if (!profileName.ok) return profileName;
  const routing = req(rec.value, "routing", nullable(string), path);
  if (!routing.ok) return routing;
  const runtime = req(rec.value, "runtime", nullable(string), path);
  if (!runtime.ok) return runtime;
  const budget = req(rec.value, "resource_budget", nullable(resourceBudget), path);
  if (!budget.ok) return budget;
  const storageGuard = req(rec.value, "storage_guard", nullable(diskGuardConfig), path);
  if (!storageGuard.ok) return storageGuard;
  return yes({
    provider: provider.value,
    model: model.value,
    profile: profileName.value,
    routing: routing.value,
    runtime: runtime.value,
    resource_budget: budget.value,
    storage_guard: storageGuard.value,
  });
};

const projectConfiguration: Decoder<ProjectConfiguration> = (input, path) => {
  const rec = record(input, path, "a ProjectConfiguration");
  if (!rec.ok) return rec;
  const defaults = req(rec.value, "defaults", jsonValue, path);
  if (!defaults.ok) return defaults;
  return yes({ defaults: defaults.value });
};

const configurationView: Decoder<ProjectConfigurationView> = (input, path) => {
  const rec = record(input, path, "a ProjectConfigurationView");
  if (!rec.ok) return rec;
  const project = req(rec.value, "project", projectRecord, path);
  if (!project.ok) return project;
  const global = req(rec.value, "global", globalConfiguration, path);
  if (!global.ok) return global;
  const projectDefaults = req(rec.value, "project_defaults", projectConfiguration, path);
  if (!projectDefaults.ok) return projectDefaults;
  return yes({
    project: project.value,
    global: global.value,
    project_defaults: projectDefaults.value,
  });
};

const jobEvent: Decoder<CanonicalJobEvent> = (input, path) => {
  const rec = record(input, path, "a CanonicalJobEvent");
  if (!rec.ok) return rec;
  const apiVersion = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
  if (!apiVersion.ok) return apiVersion;
  const projectId = req(rec.value, "project_id", string, path);
  if (!projectId.ok) return projectId;
  const jobId = req(rec.value, "job_id", string, path);
  if (!jobId.ok) return jobId;
  const sequence = req(rec.value, "sequence", number, path);
  if (!sequence.ok) return sequence;
  const eventId = req(rec.value, "event_id", string, path);
  if (!eventId.ok) return eventId;
  const kind = req(rec.value, "kind", string, path);
  if (!kind.ok) return kind;
  const payload = req(rec.value, "payload", jsonValue, path);
  if (!payload.ok) return payload;
  return yes({
    api_version: apiVersion.value,
    project_id: projectId.value,
    job_id: jobId.value,
    sequence: sequence.value,
    event_id: eventId.value,
    kind: kind.value,
    payload: payload.value,
  });
};

const jobSnapshot: Decoder<CanonicalJobSnapshot> = (input, path) => {
  const rec = record(input, path, "a CanonicalJobSnapshot");
  if (!rec.ok) return rec;
  const apiVersion = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
  if (!apiVersion.ok) return apiVersion;
  const projectId = req(rec.value, "project_id", string, path);
  if (!projectId.ok) return projectId;
  const job = req(rec.value, "job", jsonValue, path);
  if (!job.ok) return job;
  const cursor = req(rec.value, "cursor", number, path);
  if (!cursor.ok) return cursor;
  return yes({
    api_version: apiVersion.value,
    project_id: projectId.value,
    job: job.value,
    cursor: cursor.value,
  });
};

const projectsResponse: Decoder<CanonicalProjectsResponse> = (input, path) => {
  const rec = record(input, path, "a CanonicalProjectsResponse");
  if (!rec.ok) return rec;
  const apiVersion = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
  if (!apiVersion.ok) return apiVersion;
  const projects = req(rec.value, "projects", array(projectRecord), path);
  if (!projects.ok) return projects;
  return yes({ api_version: apiVersion.value, projects: projects.value });
};

const projectResponse: Decoder<CanonicalProjectResponse> = (input, path) => {
  const rec = record(input, path, "a CanonicalProjectResponse");
  if (!rec.ok) return rec;
  const apiVersion = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
  if (!apiVersion.ok) return apiVersion;
  const commandId = req(rec.value, "command_id", string, path);
  if (!commandId.ok) return commandId;
  const accepted = req(rec.value, "accepted", boolean, path);
  if (!accepted.ok) return accepted;
  const project = req(rec.value, "project", projectRecord, path);
  if (!project.ok) return project;
  return yes({
    api_version: apiVersion.value,
    command_id: commandId.value,
    accepted: accepted.value,
    project: project.value,
  });
};

const configurationEnvelope: Decoder<CanonicalConfigurationEnvelope> = (input, path) => {
  const rec = record(input, path, "a CanonicalConfigurationEnvelope");
  if (!rec.ok) return rec;
  const apiVersion = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
  if (!apiVersion.ok) return apiVersion;
  const configuration = req(rec.value, "configuration", configurationView, path);
  if (!configuration.ok) return configuration;
  return yes({ api_version: apiVersion.value, configuration: configuration.value });
};

const jobConfigEnvelope: Decoder<CanonicalJobConfigEnvelope> = (input, path) => {
  const rec = record(input, path, "a CanonicalJobConfigEnvelope");
  if (!rec.ok) return rec;
  const apiVersion = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
  if (!apiVersion.ok) return apiVersion;
  const jobId = req(rec.value, "job_id", string, path);
  if (!jobId.ok) return jobId;
  const configuration = req(rec.value, "configuration", jsonValue, path);
  if (!configuration.ok) return configuration;
  const revision = req(rec.value, "revision", number, path);
  if (!revision.ok) return revision;
  return yes({
    api_version: apiVersion.value,
    job_id: jobId.value,
    configuration: configuration.value,
    revision: revision.value,
  });
};

const eventsEnvelope: Decoder<CanonicalEventsEnvelope> = (input, path) => {
  const rec = record(input, path, "a CanonicalEventsEnvelope");
  if (!rec.ok) return rec;
  const apiVersion = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
  if (!apiVersion.ok) return apiVersion;
  const projectId = req(rec.value, "project_id", string, path);
  if (!projectId.ok) return projectId;
  const jobId = req(rec.value, "job_id", string, path);
  if (!jobId.ok) return jobId;
  const events = req(rec.value, "events", array(jobEvent), path);
  if (!events.ok) return events;
  return yes({
    api_version: apiVersion.value,
    project_id: projectId.value,
    job_id: jobId.value,
    events: events.value,
  });
};

/**
 * The Provider × Model × Effort a Health Probe declared.
 *
 * `effort` is optional on the wire (`#[serde(default)]`), and `null` is a real
 * identity: a probe without effort is a different tuple from one that names one.
 */
const healthProbeIntent: Decoder<HealthProbeIntent> = (input, path) => {
  const rec = record(input, path, "a Health Probe target");
  if (!rec.ok) return rec;
  const provider = req(rec.value, "provider", identity, path);
  if (!provider.ok) return provider;
  const model = req(rec.value, "model", identity, path);
  if (!model.ok) return model;
  const effort = opt(rec.value, "effort", string, path);
  if (!effort.ok) return effort;
  return yes({ provider: provider.value, model: model.value, effort: effort.value ?? null });
};

const jobSummary: Decoder<CanonicalJobSummary> = (input, path) => {
  const rec = record(input, path, "a canonical Job summary");
  if (!rec.ok) return rec;
  const jobId = req(rec.value, "job_id", identity, path);
  if (!jobId.ok) return jobId;
  const state = req(rec.value, "state", jobState, path);
  if (!state.ok) return state;
  const createdAt = req(rec.value, "created_at", number, path);
  if (!createdAt.ok) return createdAt;
  const updatedAt = req(rec.value, "updated_at", number, path);
  if (!updatedAt.ok) return updatedAt;
  const relations = jobRelations(rec.value, path);
  if (!relations.ok) return relations;
  const operations = jobOperations(rec.value, path);
  if (!operations.ok) return operations;
  // Absent on ordinary Jobs. Present only when the backend serialized a probe.
  const healthProbe = opt(rec.value, "health_probe", healthProbeIntent, path);
  if (!healthProbe.ok) return healthProbe;
  return yes({
    job_id: jobId.value,
    state: state.value,
    created_at: createdAt.value,
    updated_at: updatedAt.value,
    ...(healthProbe.value ? { health_probe: healthProbe.value } : {}),
    ...relations.value,
    ...operations.value,
  });
};

const dashboardResponse: Decoder<CanonicalDashboardResponse> = (input, path) => {
  const rec = record(input, path, "a CanonicalDashboardResponse");
  if (!rec.ok) return rec;
  const apiVersion = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
  if (!apiVersion.ok) return apiVersion;
  const projectId = req(rec.value, "project_id", string, path);
  if (!projectId.ok) return projectId;
  const jobs = req(rec.value, "jobs", array(jobSummary), path);
  if (!jobs.ok) return jobs;
  const selectedJob = req(rec.value, "selected_job", nullable(jobSnapshot), path);
  if (!selectedJob.ok) return selectedJob;
  const diskGuard = req(rec.value, "disk_guard", nullable(diskGuardStatus), path);
  if (!diskGuard.ok) return diskGuard;
  return yes({
    api_version: apiVersion.value,
    project_id: projectId.value,
    jobs: jobs.value,
    selected_job: selectedJob.value,
    disk_guard: diskGuard.value,
  });
};

/**
 * The acknowledgement shape shared by the three configuration writes.
 *
 * The backend returns `CanonicalConfigurationResponse`, which carries the
 * command identity it actually accepted. Decoding it here is what lets the
 * client report the server's `command_id` and `accepted` flag instead of
 * echoing back the id it hoped would be honoured.
 */
export type ConfigurationAck = CanonicalConfigurationResponse;

const configurationAck: Decoder<ConfigurationAck> = (input, path) => {
  const rec = record(input, path, "a CanonicalConfigurationResponse");
  if (!rec.ok) return rec;
  const apiVersion = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
  if (!apiVersion.ok) return apiVersion;
  const commandId = req(rec.value, "command_id", string, path);
  if (!commandId.ok) return commandId;
  const accepted = req(rec.value, "accepted", boolean, path);
  if (!accepted.ok) return accepted;
  const projectId = req(rec.value, "project_id", string, path);
  if (!projectId.ok) return projectId;
  const revision = req(rec.value, "revision", number, path);
  if (!revision.ok) return revision;
  const configuration = req(rec.value, "configuration", configurationView, path);
  if (!configuration.ok) return configuration;
  return yes({
    api_version: apiVersion.value,
    command_id: commandId.value,
    accepted: accepted.value,
    project_id: projectId.value,
    revision: revision.value,
    configuration: configuration.value,
  });
};

const jobConfigResponse: Decoder<CanonicalJobConfigResponse> = (input, path) => {
  const rec = record(input, path, "a CanonicalJobConfigResponse");
  if (!rec.ok) return rec;
  const apiVersion = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
  if (!apiVersion.ok) return apiVersion;
  const commandId = req(rec.value, "command_id", string, path);
  if (!commandId.ok) return commandId;
  const accepted = req(rec.value, "accepted", boolean, path);
  if (!accepted.ok) return accepted;
  const jobId = req(rec.value, "job_id", string, path);
  if (!jobId.ok) return jobId;
  const revision = req(rec.value, "revision", number, path);
  if (!revision.ok) return revision;
  const configuration = req(rec.value, "configuration", jsonValue, path);
  if (!configuration.ok) return configuration;
  return yes({
    api_version: apiVersion.value,
    command_id: commandId.value,
    accepted: accepted.value,
    job_id: jobId.value,
    revision: revision.value,
    configuration: configuration.value,
  });
};

const jobLaunchAck: Decoder<CanonicalJobLaunchAck> = (input, path) => {
  const rec = record(input, path, "a JobLaunchResponse");
  if (!rec.ok) return rec;
  const apiVersion = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
  if (!apiVersion.ok) return apiVersion;
  const outcome = req(rec.value, "outcome", oneOf(JOB_LAUNCH_OUTCOMES), path);
  if (!outcome.ok) return outcome;
  const commandId = req(rec.value, "command_id", string, path);
  if (!commandId.ok) return commandId;
  const draftId = req(rec.value, "draft_id", string, path);
  if (!draftId.ok) return draftId;
  const projectId = req(rec.value, "project_id", string, path);
  if (!projectId.ok) return projectId;
  const sessionId = req(rec.value, "session_id", string, path);
  if (!sessionId.ok) return sessionId;
  const jobId = req(rec.value, "job_id", nullable(string), path);
  if (!jobId.ok) return jobId;
  const message = req(rec.value, "message", string, path);
  if (!message.ok) return message;
  const duplicate = req(rec.value, "duplicate", boolean, path);
  if (!duplicate.ok) return duplicate;
  return yes({
    api_version: apiVersion.value,
    outcome: outcome.value,
    command_id: commandId.value,
    draft_id: draftId.value,
    project_id: projectId.value,
    session_id: sessionId.value,
    job_id: jobId.value,
    message: message.value,
    duplicate: duplicate.value,
  });
};

export const decodeProjectRecord = (input: unknown, path = "project"): ProjectRecord =>
  decode((value) => projectRecord(value, path), input);

export const decodeConfigurationView = (
  input: unknown,
  path = "configuration",
): ProjectConfigurationView => decode((value) => configurationView(value, path), input);

export const decodeProjectsResponse = (input: unknown): CanonicalProjectsResponse =>
  decode((value) => projectsResponse(value, ""), input);

export const decodeProjectResponse = (input: unknown): CanonicalProjectResponse =>
  decode((value) => projectResponse(value, ""), input);

export const decodeConfigurationEnvelope = (input: unknown): CanonicalConfigurationEnvelope =>
  decode((value) => configurationEnvelope(value, ""), input);

export const decodeConfigurationAck = (input: unknown): ConfigurationAck =>
  decode((value) => configurationAck(value, ""), input);

export const decodeJobConfigEnvelope = (input: unknown): CanonicalJobConfigEnvelope =>
  decode((value) => jobConfigEnvelope(value, ""), input);

export const decodeJobConfigResponse = (input: unknown): CanonicalJobConfigResponse =>
  decode((value) => jobConfigResponse(value, ""), input);

export const decodeJobLaunchResponse = (input: unknown): CanonicalJobLaunchAck =>
  decode((value) => jobLaunchAck(value, ""), input);

export const decodeEventsEnvelope = (input: unknown): CanonicalEventsEnvelope =>
  decode((value) => eventsEnvelope(value, ""), input);

export const decodeJobSpawnResponse = (input: unknown): CanonicalJobSpawnResponse =>
  decode((input) => {
    const path = "";
    const rec = record(input, path, "a canonical child Job spawn response");
    if (!rec.ok) return rec;
    const version = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
    if (!version.ok) return version;
    const childId = req(rec.value, "child_job_id", identity, path);
    if (!childId.ok) return childId;
    const duplicate = req(rec.value, "duplicate", boolean, path);
    if (!duplicate.ok) return duplicate;
    const snapshot = req(rec.value, "snapshot", jobSnapshot, path);
    if (!snapshot.ok) return snapshot;
    return yes({ api_version: version.value, child_job_id: childId.value, duplicate: duplicate.value, snapshot: snapshot.value });
  }, input);

export const decodeJobOperationResponse = (input: unknown): CanonicalJobOperationResponse =>
  decode((input) => {
    const path = "";
    const rec = record(input, path, "a canonical Job operation response");
    if (!rec.ok) return rec;
    const apiVersion = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
    if (!apiVersion.ok) return apiVersion;
    const jobId = req(rec.value, "job_id", identity, path);
    if (!jobId.ok) return jobId;
    const accepted = req(rec.value, "accepted", boolean, path);
    if (!accepted.ok) return accepted;
    const snapshot = req(rec.value, "snapshot", jobSnapshot, path);
    if (!snapshot.ok) return snapshot;
    return yes({ api_version: apiVersion.value, job_id: jobId.value, accepted: accepted.value, snapshot: snapshot.value });
  }, input);

export const decodeDashboardResponse = (input: unknown): CanonicalDashboardResponse =>
  decode((value) => dashboardResponse(value, ""), input);

export const decodeJobSnapshot = (input: unknown): CanonicalJobSnapshot =>
  decode((value) => jobSnapshot(value, ""), input);

/**
 * The Job, Attempts and Calls a canonical snapshot carries.
 *
 * Read separately from the snapshot envelope because the envelope's `job`
 * field is an unconstrained `JsonValue`: the envelope decoder only proves a
 * value is there, and this one proves what is in it. A violation raises a
 * `ContractError` naming the path, which the projection reports as a rejected
 * snapshot rather than rendering a half-typed Job.
 */
export const decodeExecutionState = (input: unknown, path = "job"): CanonicalExecutionState =>
  decode((value) => executionState(value, path), input);

export const tryEventsEnvelope = (input: unknown): DecodeResult<CanonicalEventsEnvelope> =>
  eventsEnvelope(input, "");
