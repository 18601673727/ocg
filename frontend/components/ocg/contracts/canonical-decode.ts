/** Runtime validation of the frontend view models used by UI adapters. */

import type {
  CanonicalJobRelations,
  ChildPolicy,
  JobOrigin,
  CanonicalJobOperations,
  Failure,
  FailureClass,
  Actor,
  EntityRef,
  JsonValue,
  RecursiveLimits,
} from "./view-models";
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
  nullable,
  number,
  oneOf,
  opt,
  record,
  req,
  string,
  stringMap,
  yes,
  type Decoder,
} from "./decode";

/* -------------------------------------------------------------------------- */
/* The canonical execution state                                               */
/* -------------------------------------------------------------------------- */

/**
 * The Job, Attempt and Call records the canonical snapshot embeds.
 *
 * `CanonicalJobSnapshot::job` is an unconstrained `serde_json::Value` in
 * `src/orchestration/canonical_control.rs`, assembled by `canonical_snapshot`
 * from the authoritative records in `src/orchestration/domain.rs`. Those
 * records carry no wire projection of their own — the field set here mirrors
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
export type CanonicalJob = CanonicalJobRelations &
  CanonicalJobOperations & {
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
 * array and carries no wire projection, so this mirrors its serde shape.
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
  const kind = req(
    rec.value,
    "kind",
    oneOf<EntityRef["kind"]>([
      "job",
      "attempt",
      "call",
      "command",
      "approval",
      "artifact",
      "change_set",
      "conversation",
      "message",
      "fact",
      "budget_scope",
      "capability_revocation",
    ]),
    path,
  );
  if (!kind.ok) return kind;
  const id = req(rec.value, "id", identity, path);
  if (!id.ok) return id;
  return yes({ kind: kind.value, id: id.value });
};

const actor: Decoder<Actor> = (input, path) => {
  const rec = record(input, path, "a canonical actor");
  if (!rec.ok) return rec;
  switch (rec.value.kind) {
    case "core":
      return yes({ kind: "core" });
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
      const kind = req(
        rec.value,
        "source_kind",
        oneOf<"provider" | "capability" | "runtime">(["provider", "capability", "runtime"]),
        path,
      );
      if (!kind.ok) return kind;
      const ref = req(rec.value, "source_ref", string, path);
      return ref.ok
        ? yes({ kind: "external", source_kind: kind.value, source_ref: ref.value })
        : ref;
    }
    default:
      return bad(`${path}.kind`, "a canonical actor kind");
  }
};

const failure: Decoder<Failure> = (input, path) => {
  const rec = record(input, path, "a canonical Failure");
  if (!rec.ok) return rec;
  const code = req(rec.value, "code", identity, path);
  if (!code.ok) return code;
  const classValue = req(
    rec.value,
    "class",
    oneOf<FailureClass>([
      "validation",
      "authentication",
      "authorization",
      "conflict",
      "concurrency",
      "not_found",
      "sandbox",
      "capability",
      "provider",
      "budget",
      "resource_limit",
      "rate_limit",
      "timeout",
      "cancelled",
      "preempted",
      "internal",
      "unknown",
    ]),
    path,
  );
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
  return yes({
    code: code.value,
    class: classValue.value,
    message: message.value,
    source: source.value,
    retryable: retryable.value,
    details: details.value,
    cause: cause.value,
    entity_ref: entity.value,
  });
};

const jobOperations: Decoder<CanonicalJobOperations & { termination_reason: Failure | null }> = (
  input,
  path,
) => {
  const rec = record(input, path, "canonical Job operation facts");
  if (!rec.ok) return rec;
  const cancel = req(rec.value, "can_cancel", boolean, path);
  if (!cancel.ok) return cancel;
  const retry = req(rec.value, "can_retry", boolean, path);
  if (!retry.ok) return retry;
  const reason = req(rec.value, "termination_reason", nullable(failure), path);
  if (!reason.ok) return reason;
  return yes({
    can_cancel: cancel.value,
    can_retry: retry.value,
    termination_reason: reason.value,
  });
};

const childPolicy: Decoder<ChildPolicy> = (input, path) => {
  const rec = record(input, path, "a child supervision policy");
  if (!rec.ok) return rec;
  const join = req(
    rec.value,
    "join",
    oneOf<ChildPolicy["join"]>(["required", "not_required"]),
    path,
  );
  if (!join.ok) return join;
  const cancellation = req(
    rec.value,
    "cancellation",
    oneOf<ChildPolicy["cancellation"]>(["cascade", "independent"]),
    path,
  );
  if (!cancellation.ok) return cancellation;
  const failure = req(
    rec.value,
    "failure",
    oneOf<ChildPolicy["failure"]>(["observe", "block_parent", "fail_parent"]),
    path,
  );
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
  return yes({
    parent_job_id: parent.value,
    attempt_id: attempt.value,
    generation: generation.value,
    spawn_key: key.value,
    spawn_fingerprint: fingerprint.value,
    call_id: callId.value,
    policy: policy.value,
  });
};

const jobRelations: Decoder<CanonicalJobRelations> = (input, path) => {
  const rec = record(input, path, "canonical Job relationships");
  if (!rec.ok) return rec;
  const parentJobId = req(rec.value, "parent_job_id", nullable(identity), path);
  if (!parentJobId.ok) return parentJobId;
  const origin = opt(rec.value, "origin", nullable(jobOrigin), path);
  if (!origin.ok) return origin;
  if (origin.value && origin.value.parent_job_id !== parentJobId.value)
    return bad(`${path}.origin.parent_job_id`, "the projected parent Job identity");
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
  const authoritativeAttemptId = req(
    rec.value,
    "authoritative_attempt_id",
    nullable(identity),
    path,
  );
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
