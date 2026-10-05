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


export type Actor = { "kind": "user", user_id: string, } | { "kind": "client", client_kind: ClientKind, client_id: string | null, } | { "kind": "core" } | { "kind": "attempt", attempt_ref: EntityRef, } | { "kind": "call", call_ref: EntityRef, } | { "kind": "system", policy_ref: string | null, } | { "kind": "external", source_kind: ExternalSourceKind, source_ref: string, };


/**
 * The error envelope every failing control route returns.
 */
export type ApiErrorBody = { code: string, message: string, };


export type ApiErrorEnvelope = { error: ApiErrorBody, };


/**
 * The `configuration` envelope shared by the three configuration read routes.
 */
export type CanonicalConfigurationEnvelope = { api_version: CanonicalApiVersion, configuration: ProjectConfigurationView, };


export type CanonicalConfigurationResponse = { api_version: CanonicalApiVersion, command_id: string, accepted: boolean, project_id: string, revision: number, configuration: ProjectConfigurationView, };


export type CanonicalDashboardResponse = { api_version: CanonicalApiVersion, project_id: string, jobs: Array<CanonicalJobSummary>, selected_job: CanonicalJobSnapshot | null, };


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


export type CanonicalJobOperationRequest = { expected_generation: number, };


export type CanonicalJobOperationResponse = { api_version: CanonicalApiVersion, job_id: string, accepted: boolean, snapshot: CanonicalJobSnapshot, };


export type CanonicalJobOperations = { can_cancel: boolean, can_retry: boolean, };


export type CanonicalJobRelations = { parent_job_id: string | null, origin: JobOrigin | null, child_job_ids: Array<string>, depends_on: Array<string>, blocks: Array<string>, blocked_by: Array<string>, blocked: boolean, };


export type CanonicalJobSnapshot = { api_version: CanonicalApiVersion, project_id: string, job: JsonValue, cursor: number, };


export type CanonicalJobSpawnRequest = { parent_attempt_id: string, expected_generation: number, spawn_key: string, spec: JobSpec, depends_on: Array<string>, executor_kind: string, policy: ChildPolicy, };


export type CanonicalJobSpawnResponse = { api_version: CanonicalApiVersion, child_job_id: string, duplicate: boolean, snapshot: CanonicalJobSnapshot, };


export type CanonicalJobSummary = { job_id: string, created_at: number, state: string, updated_at: number, termination_reason: Failure | null, parent_job_id: string | null, origin: JobOrigin | null, child_job_ids: Array<string>, depends_on: Array<string>, blocks: Array<string>, blocked_by: Array<string>, blocked: boolean, can_cancel: boolean, can_retry: boolean, };


export type CanonicalProjectResponse = { api_version: CanonicalApiVersion, command_id: string, accepted: boolean, project: ProjectRecord, };


/**
 * `GET /api/v1/canonical/projects`
 */
export type CanonicalProjectsResponse = { api_version: CanonicalApiVersion, projects: Array<ProjectRecord>, };


export type CapabilityConstraints = { filesystem: JsonValue, network: JsonValue, process: JsonValue, environment: JsonValue, };


export type CapabilityGrant = { capability_ref: CapabilityRef, constraints: CapabilityConstraints, };


export type CapabilityRef = { capability_id: string, version: number, };


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


export type ChildCancellationPolicy = "cascade" | "independent";


export type ChildFailurePolicy = "observe" | "block_parent" | "fail_parent";


export type ChildJoinPolicy = "required" | "not_required";


export type ChildPolicy = { join: ChildJoinPolicy, cancellation: ChildCancellationPolicy, failure: ChildFailurePolicy, };


export type ClientKind = "pwa" | "cli";


export type CommandFingerprintInputV1 = { schema_version: number, project_scope: ProjectScope, action: string, target: EntityRef | null, arguments: JsonValue, };


export type ContextCostTotals = { canonical_message_bytes: UsageQuantity, tool_schema_bytes: UsageQuantity, full_schema_baseline_bytes: UsageQuantity, schema_bytes_saved: UsageQuantity, wire_bytes: UsageQuantity, capsule_bytes: UsageQuantity, capsule_injected_requests: UsageQuantity, tool_result_bytes: UsageQuantity, };


export type ConversationUsageResponse = { api_version: CanonicalApiVersion, project_id: string, conversation_id: string, session_id: string, title: string | null, created_at: string, updated_at: string, latest_job_state: string | null, generated_at: number, totals: UsageTotals, providers: Array<UsageBreakdown>, models: Array<UsageBreakdown>, truncated: boolean, };


export type DerivedMetric = { metric: string, value: string | null, unit: string, quality: MeasurementQuality, };


/**
 * A lowercase canonical UUIDv7 text identifier.
 */
export type EntityId = string;


export type EntityKind = "job" | "attempt" | "call" | "command" | "approval" | "artifact" | "change_set" | "conversation" | "message" | "fact" | "budget_scope" | "capability_revocation";


export type EntityRef = { kind: EntityKind, id: EntityId, };


export type ExecutionLimits = { cpu: string | null, memory: string | null, wall_time: string | null, concurrency: number | null, process_count: number | null, };


export type ExecutionWitness = { job_id: string, attempt_id: string, executor_id: string, call_id: string, generation: number, };


export type ExternalSourceKind = "provider" | "capability" | "runtime";


export type Failure = { code: string, class: FailureClass, message: string, source: Actor, retryable: boolean, details: JsonValue | null, cause: EntityRef | null, entity_ref: EntityRef | null, };


export type FailureClass = "validation" | "authentication" | "authorization" | "conflict" | "concurrency" | "not_found" | "sandbox" | "capability" | "provider" | "budget" | "resource_limit" | "rate_limit" | "timeout" | "cancelled" | "preempted" | "internal" | "unknown";


export type GlobalConfiguration = { provider: string | null, model: string | null, profile: string | null, routing: string | null, runtime: string | null, resource_budget: ResourceBudget | null, };


/**
 * The exact executable placement target a probe answers for.
 *
 * This is the health identity: the tuple Placement will later ask about. It is
 * stored on the Job specification, so it is durable, journaled with the Job,
 * and reconstructable from the canonical post-image.
 */
export type HealthProbeIntent = {
/**
 * Profile provider key.
 */
provider: string,
/**
 * Profile model key, not the upstream model id.
 */
model: string,
/**
 * Reasoning effort under test; `None` means the tuple carries no effort.
 */
effort: string | null, };


/**
 * The wire projection: the latest usable health evidence for one candidate.
 *
 * This is the shape later Placement work asks its question against. It is
 * derived on read; nothing here is stored.
 */
export type HealthProbeObservation = { project_id: string, provider: string, model: string, effort: string | null, job_id: string, job_state: string, attempt_id: string | null, attempt_generation: number | null, attempt_state: string | null, call_id: string | null, dispatch_intent_id: string | null, upstream_model_id: string | null, started_at: number | null, completed_at: number | null, latency_seconds: number | null,
/**
 * `true` only when a probe Job completed a real provider round. This is
 * the single answer to "reachable and executable"; every other state is
 * described by `failure`.
 */
executable: boolean, failure: Failure | null, };


/**
 * `GET /api/v1/canonical/jobs/health-probe`
 */
export type HealthProbeQuery = { project_id: string, target: HealthProbeTarget, };


/**
 * `GET /api/v1/canonical/jobs/health-probe`
 *
 * `observation` is `null` when no probe has ever run for this candidate. That
 * is "no evidence", which is deliberately not the same claim as "unhealthy".
 */
export type HealthProbeQueryResponse = { api_version: CanonicalApiVersion, project_id: string, target: HealthProbeTarget, observation: HealthProbeObservation | null, };


/**
 * `POST /api/v1/canonical/jobs/health-probe`
 */
export type HealthProbeRequest = { command_id: string, project_id: string, target: HealthProbeTarget, };


/**
 * `POST /api/v1/canonical/jobs/health-probe`
 */
export type HealthProbeResponse = { api_version: CanonicalApiVersion,
/**
 * `accepted`, `rejected` or `failed`.
 */
outcome: string, command_id: string, project_id: string, target: HealthProbeTarget,
/**
 * The canonical probe Job, when one was created. Follow it with the
 * ordinary Job snapshot to read the terminal result.
 */
job_id: string | null, message: string, duplicate: boolean, };


/**
 * The executable placement target a Health Probe answers for.
 *
 * `effort` is part of the identity: a probe with `effort` proves a different
 * tuple than one without it.
 */
export type HealthProbeTarget = { provider: string, model: string, effort: string | null, };


/**
 * `POST /api/v1/canonical/jobs/launch`
 */
export type JobLaunchRequest = { command_id: string, draft_id: string, project_id: string, session_id: string, objective: string, success_criteria: string | null, constraints: string | null, hard_budget_micros: number, resource_commitment: number | null, };


export type JobLaunchResponse = { api_version: CanonicalApiVersion,
/**
 * "accepted" | "rejected" | "failed"
 */
outcome: string, command_id: string, draft_id: string, project_id: string, session_id: string, job_id: string | null, message: string, duplicate: boolean, };


export type JobOrigin = { parent_job_id: string, attempt_id: string, generation: number, spawn_key: string | null, spawn_fingerprint: string | null, policy: ChildPolicy | null, };


export type JobSpec = { provider: string | null, model: string | null, objective: string | null, success_criteria: string | null, constraints: string | null, hard_budget_micros: number | null, resource_commitment: number | null,
/**
 * Set when this Job is a Health Probe. A probe is an ordinary Job whose
 * declared purpose is to produce execution evidence for one
 * Provider x Model x Effort tuple, so the target rides the durable Job
 * specification rather than a second top-level entity.
 */
health_probe?: HealthProbeIntent | null, };


export type JobUsageResponse = { api_version: CanonicalApiVersion, project_id: string, job_id: string, generated_at: number, totals: UsageTotals, providers: Array<UsageBreakdown>, models: Array<UsageBreakdown>, truncated: boolean, };


export type MeasurementQuality = "measured" | "reported" | "estimated" | "derived" | "unknown";


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


export type ProjectUsageResponse = { api_version: CanonicalApiVersion, project_id: string, window: UsageWindow,
/**
 * Epoch seconds at which `window` begins. For `Today` this is the most
 * recent UTC midnight, never the viewer's local midnight.
 */
window_started_at: number | null, generated_at: number, conversations: number, totals: UsageTotals, providers: Array<UsageBreakdown>, models: Array<UsageBreakdown>, conversation_rows: Array<UsageConversationRow>, truncated: boolean, };


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


export type UsageBreakdown = { provider: string | null, model: string | null, totals: UsageTotals, };


export type UsageCompleteness = "complete" | "partial" | "unavailable";


export type UsageConversationRow = { conversation_id: string, session_id: string, title: string | null, updated_at: string, totals: UsageTotals, providers: Array<string>, models: Array<string>, };


export type UsageCost = { source: UsageCostSource, actual_micros: number | null, currency: string | null, currencies: Array<UsageCurrencyCost>, completeness: UsageCompleteness, settled_calls: number, unresolved_calls: number, unavailable_calls: number, };


export type UsageCostSource = "canonical_settlement";


export type UsageCurrencyCost = { currency: string, actual_micros: number, };


export type UsageQuantity = { value: number | null, completeness: UsageCompleteness, };


export type UsageTokenTotals = { input: UsageQuantity, output: UsageQuantity, reasoning: UsageQuantity, cache_read: UsageQuantity, cache_write: UsageQuantity, total: UsageQuantity, };


export type UsageTotals = { turns: number, jobs: number, attempts: number, provider_calls: number, native_calls: number, provider_requests: UsageQuantity, provider_rounds: UsageQuantity, tokens: UsageTokenTotals, cost: UsageCost, context_costs: ContextCostTotals, first_activity_at: number | null, last_activity_at: number | null, };


/**
 * The reporting window a usage projection is aggregated over.
 *
 * `Today` is a **UTC day**, not the viewer's local day: it begins at the most
 * recent UTC midnight. The backend carries no authoritative user timezone, so
 * the window boundary is UTC and the wire value is named and rendered as UTC
 * rather than presented as a local "Today".
 */
export type UsageWindow = "all" | "today" | "7d" | "30d";

/**
 * The protocol versions this file was generated from. They are asserted equal to
 * the Rust constants by `make contracts-check`, so a version bump cannot silently
 * desynchronise the two sides.
 */
export const CANONICAL_API_VERSION: CanonicalApiVersion = "ocg.canonical.v1";
export const PROFILE_API_VERSION: ProfileApiVersion = "ocg.profile.v1";
