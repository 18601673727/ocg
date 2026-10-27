/** Frontend view models retained by the UI adapters and local runtime. */

/**
 * An opaque JSON value, mirroring `serde_json::Value`. The backend does not
 * constrain its shape, so the client must not invent one.
 */
export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

/** The canonical control protocol version, from the Rust constant. */
export type CanonicalApiVersion = "ocg.canonical.v1";

/** The Profile control protocol version, from the Rust constant. */
export type ProfileApiVersion = "ocg.profile.v1";

export type Actor =
  | { kind: "user"; user_id: string }
  | { kind: "client"; client_kind: ClientKind; client_id: string | null }
  | { kind: "core" }
  | { kind: "attempt"; attempt_ref: EntityRef }
  | { kind: "call"; call_ref: EntityRef }
  | { kind: "system"; policy_ref: string | null }
  | { kind: "external"; source_kind: ExternalSourceKind; source_ref: string };

export type CanonicalJobOperations = { can_cancel: boolean; can_retry: boolean };

export type CanonicalJobRelations = {
  parent_job_id: string | null;
  /**
   * Set when an Executor spawned this Job under its parent Attempt.
   */
  origin: JobOrigin | null;
  /**
   * Set when an authorized principal attached this Job to an open
   * orchestration Job. A child has exactly one of `origin` and
   * `attachment`.
   */
  attachment?: JobAttachment | null;
  /**
   * Set when this Job is an orchestration scope.
   */
  scope?: JobScope | null;
  child_job_ids: Array<string>;
  depends_on: Array<string>;
  blocks: Array<string>;
  blocked_by: Array<string>;
  blocked: boolean;
  root_job_id: string;
  depth: number;
  descendant_job_ids: Array<string>;
  descendant_summary: { [key in string]: number };
};

export type ChatImage = { id: string; name: string; media_type: string; url: string };

export type ChatImageUploadRequest = { project_id: string; name: string; data_url: string };

export type ChatModelSelection = { model: string; effort: string | null };

export type ChildCancellationPolicy = "cascade" | "independent";

export type ChildFailurePolicy = "observe" | "block_parent" | "fail_parent";

export type ChildJoinPolicy = "required" | "not_required";

export type ChildPolicy = {
  join: ChildJoinPolicy;
  cancellation: ChildCancellationPolicy;
  failure: ChildFailurePolicy;
};

export type ClientKind = "pwa" | "cli";

/**
 * A lowercase canonical UUIDv7 text identifier.
 */
export type EntityId = string;

export type EntityKind =
  | "job"
  | "attempt"
  | "call"
  | "command"
  | "approval"
  | "artifact"
  | "change_set"
  | "conversation"
  | "message"
  | "fact"
  | "budget_scope"
  | "capability_revocation";

export type EntityRef = { kind: EntityKind; id: EntityId };

export type ExternalSourceKind = "provider" | "capability" | "runtime";

export type Failure = {
  code: string;
  class: FailureClass;
  message: string;
  source: Actor;
  retryable: boolean;
  details: JsonValue | null;
  cause: EntityRef | null;
  entity_ref: EntityRef | null;
};

export type FailureClass =
  | "validation"
  | "authentication"
  | "authorization"
  | "conflict"
  | "concurrency"
  | "not_found"
  | "sandbox"
  | "capability"
  | "provider"
  | "budget"
  | "resource_limit"
  | "rate_limit"
  | "timeout"
  | "cancelled"
  | "preempted"
  | "internal"
  | "unknown";

/**
 * The provenance of a child attached through Job management.
 */
export type JobAttachment = {
  parent_job_id: string;
  principal: JobPrincipal;
  command_id: string;
  policy: ChildPolicy;
  created_at: number;
};

export type JobOrigin = {
  parent_job_id: string;
  attempt_id: string;
  generation: number;
  spawn_key: string | null;
  spawn_fingerprint: string | null;
  call_id: string | null;
  policy: ChildPolicy | null;
};

/**
 * The principal the control plane authenticated and authorized for the
 * target Project. The domain records it; it never confers execution
 * authority.
 */
export type JobPrincipal = { kind: PrincipalKind; id: string };

/**
 * Whether a Job is an orchestration scope.
 *
 * An open scope accepts attached child work and never completes; closing it
 * is the explicit statement that no further work will be attached, after
 * which its required children decide its completion. An orchestration scope
 * is not an execution: it holds no Attempt and no Executor.
 */
export type JobScope = "none" | "open" | "closed";

/**
 * Who an authorized Job-management operation acts for.
 */
export type PrincipalKind = "operator" | "user" | "job";

export type RecursiveLimits = {
  max_depth: number;
  max_children_per_job: number;
  max_total_descendants_per_root: number;
};

export type UsageCompleteness = "complete" | "partial" | "unavailable";

export type UsageCost = {
  source: UsageCostSource;
  actual_micros: number | null;
  currency: string | null;
  currencies: Array<UsageCurrencyCost>;
  completeness: UsageCompleteness;
  settled_calls: number;
  unresolved_calls: number;
  unavailable_calls: number;
};

export type UsageCostSource = "canonical_settlement";

export type UsageCurrencyCost = { currency: string; actual_micros: number };

/** Protocol labels used by the UI adapters. */
export const CANONICAL_API_VERSION: CanonicalApiVersion = "ocg.canonical.v1";
export const PROFILE_API_VERSION: ProfileApiVersion = "ocg.profile.v1";

export type CanonicalJobState =
  | "pending"
  | "eligible"
  | "running"
  | "cancelling"
  | "completed"
  | "failed"
  | "cancelled"
  | "unknown"
  | "orphaned";

export type CanonicalAttemptState =
  | "queued"
  | "running"
  | "cancelling"
  | "completed"
  | "failed"
  | "cancelled"
  | "unknown"
  | "orphaned";

export type CanonicalEffectKind = "idempotent" | "strict_fenced" | "reconcilable" | "non_retryable";

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

export type CanonicalAttempt = {
  id: string;
  job_id: string;
  generation: number;
  state: CanonicalAttemptState;
  authoritative: boolean;
  created_at: number;
  finished_at: number | null;
};

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

export type CanonicalExecutor = {
  id: string;
  attempt_id: string;
  kind: string;
  state: string;
  created_at: number;
};

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
  reservation_id?: string | null;
  provider_key?: string | null;
  model?: string | null;
  upstream_model_id?: string | null;
};
