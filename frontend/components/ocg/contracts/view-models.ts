export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

export type Actor =
  | { kind: "user"; user_id: string }
  | { kind: "client"; client_kind: ClientKind; client_id: string | null }
  | { kind: "core" }
  | { kind: "attempt"; attempt_ref: EntityRef }
  | { kind: "call"; call_ref: EntityRef }
  | { kind: "system"; policy_ref: string | null }
  | { kind: "external"; source_kind: ExternalSourceKind; source_ref: string };

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

export type JobOrigin = {
  parent_job_id: string;
  attempt_id: string;
  generation: number;
  spawn_key: string | null;
  spawn_fingerprint: string | null;
  call_id: string | null;
  policy: ChildPolicy | null;
};

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

export type CanonicalAttempt = {
  id: string;
  job_id: string;
  generation: number;
  state: CanonicalAttemptState;
  authoritative: boolean;
  created_at: number;
  finished_at: number | null;
};
