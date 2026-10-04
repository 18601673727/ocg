/**
 * GENERATED FILE - DO NOT EDIT.
 *
 * Projected from the Rust wire contract in `src/contracts.rs` by
 * `cargo run --bin ocg-rs-ts`. Rust is the single source of truth for every
 * type below; this file is a projection of it, committed so the frontend needs
 * no build step and reviewers see contract changes in the diff.
 *
 * If you change a Rust contract, run `make contracts` and commit the result.
 * `make contracts-check` fails when this file drifts.
 */

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


export type ActivityCursor = { generation: string, sequence: string, };


export type Actor = { "kind": "user", user_id: string, } | { "kind": "client", client_kind: ClientKind, client_id: string | null, } | { "kind": "core" } | { "kind": "attempt", attempt_ref: EntityRef, } | { "kind": "call", call_ref: EntityRef, } | { "kind": "system", policy_ref: string | null, } | { "kind": "external", source_kind: ExternalSourceKind, source_ref: string, };


/**
 * The error envelope every failing control route returns.
 */
export type ApiErrorBody = { code: string, message: string, };


export type ApiErrorEnvelope = { error: ApiErrorBody, };


export type Approval = { id: EntityId, project_scope: ProjectScope, subject_ref: EntityRef, subject_fingerprint: string, requester: Actor, state: ApprovalState, decision: ApprovalDecision | null, expires_at: string | null, revision: number, created_at: string, resolved_at: string | null, };


export type ApprovalDecision = { actor: Actor, rationale: string | null, };


export type ApprovalState = "pending" | "approved" | "rejected" | "cancelled" | "expired";


export type AttemptLifecycle = "created" | "queued" | "running" | "succeeded" | "failed" | "cancelled" | "preempted";


export type BlockerKind = "dependency" | "required_child" | "approval" | "policy" | "budget";


export type BudgetScope = { id: EntityId, project_scope: ProjectScope, job_ref: EntityRef, ceiling_tokens: number | null, ceiling_cost: string | null, reserved_tokens: number | null, reserved_cost: string | null, consumed_tokens: number | null, consumed_cost: string | null, revision: number, created_at: string, updated_at: string, };


export type BudgetSnapshot = { tokens_remaining: number | null, cost_remaining: string | null, };


export type CallLifecycle = "created" | "queued" | "running" | "succeeded" | "failed" | "cancelled";


/**
 * The `configuration` envelope shared by the three configuration read routes.
 */
export type CanonicalConfigurationEnvelope = { api_version: CanonicalApiVersion, configuration: ProjectConfigurationView, };


export type CanonicalConfigurationResponse = { api_version: CanonicalApiVersion, command_id: string, accepted: boolean, project_id: string, revision: number, configuration: ProjectConfigurationView, };


export type CanonicalDashboardResponse = { api_version: CanonicalApiVersion, project_id: string, jobs: Array<JsonValue>, selected_job: CanonicalJobSnapshot | null, };


export type CanonicalEntityProjection = { entity_ref: EntityRef, project_scope: ProjectScope, revision: number, activity_cursor: ActivityCursor, state: JsonValue, };


/**
 * `GET /api/v1/canonical/jobs/events`
 */
export type CanonicalEventsEnvelope = { api_version: CanonicalApiVersion, project_id: string, job_id: string, events: Array<CanonicalJobEvent>, };


/**
 * `GET /api/v1/canonical/jobs/{job}/configuration`
 */
export type CanonicalJobConfigEnvelope = { api_version: CanonicalApiVersion, job_id: string,
/**
 * `null` when the Job has no stored pre-attempt configuration yet.
 */
configuration: JsonValue,
/**
 * The substrate revision the configuration was read at; `0` when unset.
 */
revision: number, };


export type CanonicalJobConfigResponse = { api_version: CanonicalApiVersion, command_id: string, accepted: boolean, job_id: string, revision: number, configuration: JsonValue, };


export type CanonicalJobEvent = { api_version: CanonicalApiVersion, project_id: string, job_id: string,
/**
 * The canonical execution journal cursor this event occupies.
 */
sequence: number, event_id: string, kind: string, payload: JsonValue, };


export type CanonicalJobSnapshot = { api_version: CanonicalApiVersion, project_id: string, job: JsonValue, cursor: number, };


export type CanonicalProjectResponse = { api_version: CanonicalApiVersion, command_id: string, accepted: boolean, project: ProjectRecord, };


/**
 * `GET /api/v1/canonical/projects`
 */
export type CanonicalProjectsResponse = { api_version: CanonicalApiVersion, projects: Array<ProjectRecord>, };


export type CanonicalSyncProjection = { project_scope: ProjectScope, sync_policy_version: string, window: SyncWindowPolicy, jobs: Array<CanonicalEntityProjection>, attempts: Array<CanonicalEntityProjection>, calls: Array<CanonicalEntityProjection>, commands: Array<CanonicalEntityProjection>, approvals: Array<CanonicalEntityProjection>, changesets: Array<CanonicalEntityProjection>, artifacts: Array<CanonicalEntityProjection>, conversations: Array<CanonicalEntityProjection>, messages: Array<CanonicalEntityProjection>, facts: Array<CanonicalEntityProjection>, budget_scopes: Array<CanonicalEntityProjection>, capability_revocations: Array<CanonicalEntityProjection>, ref_stubs: Array<EntityRef>, };


export type CapabilityConstraints = { filesystem: JsonValue, network: JsonValue, process: JsonValue, environment: JsonValue, };


export type CapabilityGrant = { capability_ref: CapabilityRef, constraints: CapabilityConstraints, };


export type CapabilityRef = { capability_id: string, version: number, };


export type CapabilityRevocation = { id: EntityId, project_scope: ProjectScope, capability_ref: CapabilityRef, attempt_ref: EntityRef | null, actor: Actor, reason_code: string, state: CapabilityRevocationState, revision: number, created_at: string, lifted_at: string | null, };


export type CapabilityRevocationState = "active" | "lifted";


export type CatalogModel = { id: string, label: string, metadata: ModelMetadata, raw: JsonValue, };


export type ChangeSetFingerprintInputV1 = { schema_version: number, project_scope: ProjectScope, targets: Array<JsonValue>, preconditions: Array<JsonValue>, operations: Array<JsonValue>, };


export type ChatConversationView = { conversation_id: string, session_id: string, title: string | null, created_at: string, updated_at: string, };


export type ChatConversationsResponse = { api_version: CanonicalApiVersion, project_id: string, conversations: Array<ChatConversationView>, };


export type ChatImage = { id: string, name: string, media_type: string, url: string, };


export type ChatImageUploadRequest = { project_id: string, name: string, data_url: string, };


export type ChatMessageRole = "user" | "assistant";


export type ChatMessageView = { message_id: string, command_id: string, role: ChatMessageRole, state: MessageLifecycle, content: string, failure_reason: string | null, images: Array<ChatImage>, job_id: string | null, created_at: string, updated_at: string, attempt_state: string, replay_job_id: string | null, };


export type ChatMessagesResponse = { api_version: CanonicalApiVersion, project_id: string, conversation: ChatConversationView, messages: Array<ChatMessageView>, };


export type ChatModelSelection = { model: string, effort: string | null, };


export type ChatSendRequest = { selection: ChatModelSelection | null, image_ids: Array<string>, command_id: string, draft_id: string, project_id: string, session_id: string, objective: string, success_criteria: string | null, constraints: string | null, hard_budget_micros: number, resource_commitment: number | null, };


export type ChildCancellationPolicy = "cascade" | "soft_cascade" | "independent";


export type ChildFailurePolicy = "block_parent" | "fail_parent" | "non_blocking";


export type ChildJoinPolicy = "required" | "not_required";


export type ChildPolicy = { join: ChildJoinPolicy, cancellation: ChildCancellationPolicy, failure: ChildFailurePolicy, grace_period_ms: number | null, };


export type ClientKind = "pwa" | "cli";


export type Command = { id: EntityId, project_scope: ProjectScope, origin: Actor, action: string, target: EntityRef | null, arguments: JsonValue, idempotency_key: string | null, request_fingerprint: string, correlation_id: EntityId, state: CommandState, result: JsonValue | null, failure: Failure | null, revision: number, created_at: string, completed_at: string | null, };


export type CommandFingerprintInputV1 = { schema_version: number, project_scope: ProjectScope, action: string, target: EntityRef | null, arguments: JsonValue, };


export type CommandState = "received" | "awaiting_approval" | "accepted" | "rejected" | "completed" | "failed" | "cancelled";


export type Consistency = "read_your_writes" | "monotonic" | "eventual" | "snapshot";


export type DerivedMetric = { metric: string, value: string | null, unit: string, quality: MeasurementQuality, };


/**
 * Durable execution outbox record. It is intentionally not an EntityRef.
 */
export type EffectIntent = { intent_id: EntityId, project_scope: ProjectScope, call_ref: EntityRef, capability_ref: CapabilityRef, reconciliation_key: string, input_fingerprint: string, effect_mode: SideEffectMode, admitted_fence_epoch: string, state: EffectIntentState, created_at: string, updated_at: string, };


export type EffectIntentState = "intended" | "effected" | "reconciled" | "unknown";


/**
 * A lowercase canonical UUIDv7 text identifier.
 */
export type EntityId = string;


export type EntityKind = "job" | "attempt" | "call" | "command" | "approval" | "artifact" | "change_set" | "conversation" | "message" | "fact" | "budget_scope" | "capability_revocation";


export type EntityRef = { kind: EntityKind, id: EntityId, };


export type EventEnvelope = { event_id: EntityId, project_scope: ProjectScope, entity_ref: EntityRef, generation: string, sequence: string, timestamp: string, correlation_id: EntityId, causation_event_id: EntityId | null, transaction_id: EntityId, transaction_index: number, transaction_count: number, event_type: EventType, schema_version: string, projection_effect: ProjectionEffect, payload: EventPayload, entity_post_image: JsonValue | null, };


export type EventPayload = { "event_type": "job_created", "payload": JsonValue } | { "event_type": "job_state_changed", "payload": JsonValue } | { "event_type": "job_dependencies_changed", "payload": JsonValue } | { "event_type": "job_reopened", "payload": JsonValue } | { "event_type": "job_archived", "payload": JsonValue } | { "event_type": "job_blocker_added", "payload": JsonValue } | { "event_type": "job_blocker_resolved", "payload": JsonValue } | { "event_type": "job_ready", "payload": JsonValue } | { "event_type": "dependency_satisfied", "payload": JsonValue } | { "event_type": "spawn_requested", "payload": JsonValue } | { "event_type": "spawn_deduplicated", "payload": JsonValue } | { "event_type": "scheduler_decision", "payload": JsonValue } | { "event_type": "retry_requested", "payload": JsonValue } | { "event_type": "replacement_requested", "payload": JsonValue } | { "event_type": "recovery_requested", "payload": JsonValue } | { "event_type": "attempt_created", "payload": JsonValue } | { "event_type": "attempt_queued", "payload": JsonValue } | { "event_type": "attempt_started", "payload": JsonValue } | { "event_type": "attempt_succeeded", "payload": JsonValue } | { "event_type": "attempt_failed", "payload": JsonValue } | { "event_type": "attempt_cancelled", "payload": JsonValue } | { "event_type": "attempt_preempted", "payload": JsonValue } | { "event_type": "attempt_fenced", "payload": JsonValue } | { "event_type": "call_created", "payload": JsonValue } | { "event_type": "call_queued", "payload": JsonValue } | { "event_type": "call_started", "payload": JsonValue } | { "event_type": "call_succeeded", "payload": JsonValue } | { "event_type": "call_failed", "payload": JsonValue } | { "event_type": "call_cancelled", "payload": JsonValue } | { "event_type": "call_fenced", "payload": JsonValue } | { "event_type": "command_received", "payload": JsonValue } | { "event_type": "command_awaiting_approval", "payload": JsonValue } | { "event_type": "command_accepted", "payload": JsonValue } | { "event_type": "command_rejected", "payload": JsonValue } | { "event_type": "command_completed", "payload": JsonValue } | { "event_type": "command_failed", "payload": JsonValue } | { "event_type": "command_cancelled", "payload": JsonValue } | { "event_type": "approval_created", "payload": JsonValue } | { "event_type": "approval_approved", "payload": JsonValue } | { "event_type": "approval_rejected", "payload": JsonValue } | { "event_type": "approval_cancelled", "payload": JsonValue } | { "event_type": "approval_expired", "payload": JsonValue } | { "event_type": "change_set_created", "payload": JsonValue } | { "event_type": "change_set_validated", "payload": JsonValue } | { "event_type": "change_set_applying", "payload": JsonValue } | { "event_type": "change_set_applied", "payload": JsonValue } | { "event_type": "change_set_verifying", "payload": JsonValue } | { "event_type": "change_set_verified", "payload": JsonValue } | { "event_type": "change_set_conflict", "payload": JsonValue } | { "event_type": "change_set_failed", "payload": JsonValue } | { "event_type": "change_set_cancelled", "payload": JsonValue } | { "event_type": "artifact_created", "payload": JsonValue } | { "event_type": "artifact_metadata_updated", "payload": JsonValue } | { "event_type": "artifact_archived", "payload": JsonValue } | { "event_type": "conversation_created", "payload": JsonValue } | { "event_type": "conversation_updated", "payload": JsonValue } | { "event_type": "conversation_archived", "payload": JsonValue } | { "event_type": "message_created", "payload": JsonValue } | { "event_type": "message_streaming_started", "payload": JsonValue } | { "event_type": "message_completed", "payload": JsonValue } | { "event_type": "message_failed", "payload": JsonValue } | { "event_type": "message_updated", "payload": JsonValue } | { "event_type": "message_deleted", "payload": JsonValue } | { "event_type": "fact_recorded", "payload": JsonValue } | { "event_type": "budget_reserved", "payload": JsonValue } | { "event_type": "budget_committed", "payload": JsonValue } | { "event_type": "budget_released", "payload": JsonValue } | { "event_type": "budget_ceiling_raised", "payload": JsonValue } | { "event_type": "budget_exceeded", "payload": JsonValue } | { "event_type": "capability_revocation_created", "payload": JsonValue } | { "event_type": "capability_revocation_lifted", "payload": JsonValue };


export type EventRegistryEntry = { event_type: EventType, subject_kind: EntityKind, schema_version: string, producer: string, post_image_policy: PostImagePolicy, projection_effect_policy: ProjectionEffectPolicy, };


/**
 * The stable event names are part of the sync protocol. Transient message
 * deltas intentionally do not appear here: they are droppable transport data,
 * not canonical facts.
 */
export type EventType = "job_created" | "job_state_changed" | "job_dependencies_changed" | "job_reopened" | "job_archived" | "job_blocker_added" | "job_blocker_resolved" | "job_ready" | "dependency_satisfied" | "spawn_requested" | "spawn_deduplicated" | "scheduler_decision" | "retry_requested" | "replacement_requested" | "recovery_requested" | "attempt_created" | "attempt_queued" | "attempt_started" | "attempt_succeeded" | "attempt_failed" | "attempt_cancelled" | "attempt_preempted" | "attempt_fenced" | "call_created" | "call_queued" | "call_started" | "call_succeeded" | "call_failed" | "call_cancelled" | "call_fenced" | "command_received" | "command_awaiting_approval" | "command_accepted" | "command_rejected" | "command_completed" | "command_failed" | "command_cancelled" | "approval_created" | "approval_approved" | "approval_rejected" | "approval_cancelled" | "approval_expired" | "change_set_created" | "change_set_validated" | "change_set_applying" | "change_set_applied" | "change_set_verifying" | "change_set_verified" | "change_set_conflict" | "change_set_failed" | "change_set_cancelled" | "artifact_created" | "artifact_metadata_updated" | "artifact_archived" | "conversation_created" | "conversation_updated" | "conversation_archived" | "message_created" | "message_streaming_started" | "message_completed" | "message_failed" | "message_updated" | "message_deleted" | "fact_recorded" | "budget_reserved" | "budget_committed" | "budget_released" | "budget_ceiling_raised" | "budget_exceeded" | "capability_revocation_created" | "capability_revocation_lifted";


export type ExecutionGraphEdge = { from: EntityRef, to: EntityRef, relation: string, };


export type ExecutionGraphNode = { entity_ref: EntityRef, level: string, state: string, activity_cursor: ActivityCursor | null, };


export type ExecutionGraphProjection = { project_scope: ProjectScope, nodes: Array<ExecutionGraphNode>, edges: Array<ExecutionGraphEdge>, };


export type ExecutionLeaseRecord = { attempt_ref: EntityRef, lease_id: EntityId, holder_id: string, fence_epoch: string, issued_at: string, expires_at: string, renewal_interval_ms: number, state: LeaseState, };


export type ExecutionLimits = { cpu: string | null, memory: string | null, wall_time: string | null, concurrency: number | null, process_count: number | null, };


export type ExecutionMode = "single_shot" | "change_set_edit";


export type ExecutionPolicy = { schema_version: number, mode: ExecutionMode, retry: ExecutionRetryPolicy, };


export type ExecutionRetryPolicy = { max_attempts_per_phase: number, retryable_failure_classes: Array<FailureClass>, };


export type ExecutionWitness = { job_id: string, attempt_id: string, executor_id: string, call_id: string, generation: number, };


export type ExecutorContract = { executor_contract_version: string, executor_ref: string, runtime_ref: string, provider_ref: EntityRef | null, model_ref: EntityRef | null, capability_grants: Array<CapabilityGrant>, execution_limits: ExecutionLimits, budget_scope_ref: EntityRef, budget_snapshot: BudgetSnapshot, deadline: string | null, execution_boundary: string, effective_config_fingerprint: string, created_at: string, };


export type ExternalSourceKind = "provider" | "capability" | "runtime";


export type Fact = { id: EntityId, project_scope: ProjectScope, subject_ref: EntityRef, metric: string, value: string | null, unit: string, source: Actor, observed_at: string, provenance: string | null, quality: MeasurementQuality, created_at: string, };


export type Failure = { code: string, class: FailureClass, message: string, source: Actor, retryable: boolean, details: JsonValue | null, cause: EntityRef | null, entity_ref: EntityRef | null, };


export type FailureClass = "validation" | "authentication" | "authorization" | "conflict" | "concurrency" | "not_found" | "sandbox" | "capability" | "provider" | "budget" | "resource_limit" | "rate_limit" | "timeout" | "cancelled" | "preempted" | "internal" | "unknown";


export type GlobalConfiguration = { provider: string | null, model: string | null, profile: string | null, routing: string | null, runtime: string | null, resource_budget: ResourceBudget | null, };


export type JobBlocker = { kind: BlockerKind, blocking_ref: EntityRef | null, reason_code: string, };


/**
 * `POST /api/v1/canonical/jobs/launch`
 */
export type JobLaunchRequest = { command_id: string, draft_id: string, project_id: string, session_id: string, objective: string, success_criteria: string | null, constraints: string | null, hard_budget_micros: number, resource_commitment: number | null, };


export type JobLaunchResponse = { api_version: CanonicalApiVersion,
/**
 * "accepted" | "rejected" | "failed"
 */
outcome: string, command_id: string, draft_id: string, project_id: string, session_id: string, job_id: string | null, message: string, duplicate: boolean, };


export type JobLifecycle = "pending" | "ready" | "settling" | "blocked" | "completed" | "failed" | "cancelled";


export type LeaseState = "active" | "fenced" | "expired";


export type MeasurementQuality = "measured" | "reported" | "estimated" | "derived" | "unknown";


export type MeasurementView = { subject_ref: EntityRef, facts: Array<Fact>, derived_metrics: Array<DerivedMetric>, };


export type Message = { id: EntityId, project_scope: ProjectScope, conversation_ref: EntityRef, author: Actor, produced_by_attempt_ref: EntityRef | null, blocks: Array<MessageBlock>, state: MessageLifecycle, revision: number, created_at: string, updated_at: string, deleted_at: string | null, };


export type MessageBlock = { kind: MessageBlockKind, content: string | null, entity_ref: EntityRef | null, artifact_ref: EntityRef | null, changeset_ref: EntityRef | null, projection_kind: string | null, raw: JsonValue | null, };


export type MessageBlockKind = "markdown" | "image" | "entity_ref" | "artifact_ref" | "diff_ref" | "status_projection" | "unknown";


export type MessageLifecycle = "pending" | "streaming" | "complete" | "failed" | "deleted";


export type Model = { provider: string, id: string,
/**
 * Optional selected reasoning variant; never inferred from a fixed tier.
 */
variant?: string | null, variants?: Array<string>, label?: string | null, metadata?: ModelMetadata | null, };


export type ModelMetadata = { variant: string | null, variants: Array<string> | null, effort: string | null, efforts: Array<string> | null, reasoning: boolean | null, fast_mode: boolean | null, context_window: number | null, tools: boolean | null, images: boolean | null, multimodal: boolean | null, pricing: JsonValue | null, };


export type Origin = "new";


export type PostImagePolicy = "required" | "forbidden";


export type Profile = { origin: Origin, defaultModel?: string | null, providers: { [key in string]: Provider }, models: { [key in string]: Model }, };


/**
 * The request bodies the PWA sends. Typed so a malformed body is a compile
 * error in the client rather than a runtime surprise.
 */
export type ProfileBootstrapRequest = {
/**
 * The explicit bootstrap action, currently `"new"`.
 */
choice: string, };


/**
 * `POST /api/v1/profile/credentials`
 */
export type ProfileCredentialRequest = {
/**
 * Vault credential name. Validated by the Vault, never logged.
 */
name: string,
/**
 * The secret value. Written to the Vault only; never returned.
 */
value: string, };


export type ProfileReplaceRequest = { revision: string, profile: Profile, };


/**
 * `GET|POST|PUT /api/v1/profile` and `/api/v1/profile/bootstrap`.
 *
 * This response used to be an anonymous `json!` literal, which is exactly the
 * kind of shape a hand-written TypeScript mirror cannot be checked against.
 */
export type ProfileView = { api_version: ProfileApiVersion, profile: Profile | null,
/**
 * SHA-256 of the Profile document. A write must present the same value;
 * a mismatch is rejected instead of overwriting a concurrent edit.
 */
revision: string | null,
/**
 * Backend-computed execution readiness: model keys that satisfy the
 * same provider/model/endpoint/credential rules as canonical launch.
 * The PWA decides onboarding vs workspace from this alone. Secrets are
 * never included.
 */
runnable_choices: Array<string>, };


export type ProjectConfiguration = { defaults: JsonValue, };


export type ProjectConfigurationView = { project: ProjectRecord, global: GlobalConfiguration, project_defaults: ProjectConfiguration, };


/**
 * Backend-owned Project identity. Import is a boundary validation operation,
 * not a general filesystem manager.
 */
export type ProjectRecord = { project_id: string, root: string, boundary: string, marker: boolean, created_at: number, updated_at: number, };


export type ProjectScope = string;


export type ProjectionEffect = "reducible" | "snapshot_barrier";


export type ProjectionEffectPolicy = "reducible_only" | "barrier_allowed";


export type Provider = { label: string,
/**
 * HTTPS endpoint for provider API calls. Must not include userinfo.
 */
endpoint?: string | null,
/**
 * Reference to a Vault credential name. The credential value is the raw
 * bearer token (without "Bearer " prefix); OCG constructs the Authorization
 * header at runtime.
 */
credential_ref?: string | null,
/**
 * The wire protocol this provider speaks. `None` means the provider speaks
 * OpenAI Chat Completions, which is what every Profile written before
 * protocols were explicit does. A key, a label or an endpoint host never
 * implies a protocol.
 */
protocol?: ProviderProtocol | null, catalog?: ProviderCatalog | null, };


export type ProviderCatalog = { discovered_at: number, models: Array<CatalogModel>, };


/**
 * The protocol OCG speaks to a provider endpoint.
 */
export type ProviderProtocol = "anthropic" | "openai" | "openai_compatible";


/**
 * A resource budget attached to the global configuration.
 *
 * This is a real struct rather than an opaque JSON value. It used to be
 * `Option<Value>`, which forced the PWA to guess a shape and cast its way
 * through an untyped value; typing it here means the budget the control
 * surface sends is validated at the boundary and the shape the PWA reads is
 * the one the backend actually stores.
 */
export type ResourceBudget = {
/**
 * Zero records "no explicit hard limit", not "a budget of nothing".
 */
hard_limit: number, unit: string, };


export type ResumeCursor = { cursor_schema_version: string, contract_version: string, project_scope: ProjectScope, generation: string, sequence: string, last_event_id: EntityId | null, };


/**
 * Request to browse filesystem directories.
 */
export type SetupBrowseRequest = {
/**
 * Path to browse (empty = home directory).
 */
path: string | null, };


/**
 * Response for directory listing.
 */
export type SetupBrowseResponse = { current: string, parent: string | null, entries: Array<SetupDirectoryEntry>, };


/**
 * Request to connect a new provider and discover models.
 */
export type SetupConnectRequest = {
/**
 * Human-readable provider name.
 */
name: string,
/**
 * Provider endpoint URL (base URL or full chat/completions URL).
 */
endpoint: string,
/**
 * API key for the provider.
 */
api_key: string, };


/**
 * Response after connecting a provider and discovering models.
 */
export type SetupConnectResponse = { api_version: ProfileApiVersion,
/**
 * Provider key assigned by the backend.
 */
provider_key: string,
/**
 * Normalized models from the provider.
 */
models: Array<SetupModel>,
/**
 * Profile revision after the provider was persisted, for the model save.
 */
revision: string, };


/**
 * A directory entry.
 */
export type SetupDirectoryEntry = { name: string, path: string, is_dir: boolean, };


/**
 * A model discovered from a provider.
 */
export type SetupModel = {
/**
 * OCG model key (provider_key/model_id normalized).
 */
key: string,
/**
 * Upstream provider model id.
 */
id: string,
/**
 * Display label.
 */
label: string,
/**
 * Known metadata (may be empty).
 */
metadata: ModelMetadata, };


/**
 * A model selected by the user during setup.
 */
export type SetupModelSelection = {
/**
 * OCG model key.
 */
key: string,
/**
 * Upstream provider model id.
 */
id: string, };


/**
 * Request to save selected models.
 */
export type SetupModelsRequest = {
/**
 * Provider key.
 */
provider_key: string,
/**
 * Models to enable, each carrying the OCG key and the upstream provider model id.
 */
models: Array<SetupModelSelection>,
/**
 * Default model key.
 */
default_model: string,
/**
 * Profile revision for optimistic locking.
 */
revision: string, };


/**
 * Response after saving models.
 */
export type SetupModelsResponse = { api_version: ProfileApiVersion, selected_models: Array<string>, default_model: string, runnable_choices: Array<string>, revision: string, };


/**
 * Request to initialize and import a project.
 */
export type SetupProjectRequest = { command_id: string,
/**
 * Root path of the project.
 */
root: string, };


/**
 * Response after project initialization.
 */
export type SetupProjectResponse = { api_version: CanonicalApiVersion, project_id: string, name: string, root: string, };


export type SetupRefreshRequest = { provider_key: string, revision: string, };


export type SideEffectMode = "idempotent" | "strict_fenced" | "reconcilable" | "non_retryable";


export type Snapshot = { project_scope: ProjectScope, generation: string, covered_sequence: string, last_event_id: EntityId | null, snapshot_schema_version: string, contract_version: string, captured_at: string, state_hash: string, state: CanonicalSyncProjection, };


export type SpawnChildRequest = { project_scope: ProjectScope, parent_ref: EntityRef, spawn_key: string, spawn_fingerprint: string, child_spec: JsonValue, child_policy: ChildPolicy, causation_event_id: EntityId, idempotency_key: string, };


export type SpawnChildSpecV1 = { spec: JsonValue, execution_policy: ExecutionPolicy, dependency_refs: Array<EntityRef>, };


export type SpawnFingerprintInputV1 = { schema_version: number, project_scope: ProjectScope, child_spec: SpawnChildSpecV1, child_policy: ChildPolicy, };


export type SyncWindowPolicy = { nonterminal_jobs_per_project: number, terminal_jobs_per_project: number, attempts_per_job: number, calls_per_attempt: number, commands_per_project: number, approvals_per_project: number, changesets_per_project: number, artifacts_per_project: number, active_conversations_per_project: number, messages_per_conversation: number, archived_conversations_per_project: number, facts_per_subject: number, capability_revocations_per_project: number, };


export type TransientMessageDelta = { project_scope: ProjectScope, generation: string, stream_id: string, message_ref: EntityRef, attempt_ref: EntityRef, chunk_index: string, delta_utf8: string, };

/**
 * The protocol versions this file was generated from. They are asserted equal to
 * the Rust constants by `make contracts-check`, so a version bump cannot silently
 * desynchronise the two sides.
 */
export const CANONICAL_API_VERSION: CanonicalApiVersion = "ocg.canonical.v1";
export const PROFILE_API_VERSION: ProfileApiVersion = "ocg.profile.v1";
