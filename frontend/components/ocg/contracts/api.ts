import type {
  AgentCapability,
  JobUsage,
  JobActivity,
  JobPlanView,
  JsonValue,
  ProjectRecord,
  ProjectCommand as StoredProjectCommand,
  JobOutputRecord,
} from "./generated";
export type {
  AgentCapability,
  JobUsage,
  JobActivityPhase,
  JobActivity,
  JsonValue,
} from "./generated";

export type TaskPlan = JobPlanView;

export type Envelope<T> = { api_version: number } & T;

export type Error = { code: string; message: string };

export type ErrorResponse = { error: Error };

export type JobKind = "conversation" | "turn";

export type SettlementState = "unsettled" | "completed" | "failed" | "cancelled" | "unknown";

export type Settlement = { state: SettlementState; settled_at_us: number | null };

export type Job = {
  id: string;
  parent?: string;
  name: string | null;
  title: string | null;
  is_chat: boolean;
  execution_mode: string;
  input: string;
  previous: string | null;
  kind: JobKind;
  conversation: string | null;
  assigned_executor: string | null;
  created_at_us: number;
  updated_at_us: number;
  started_at_us: number | null;
  paused_at_us: number | null;
  sterilized_at_us: number | null;
  settlement: Settlement;
  stop_confirmed_at_us: number | null;
};

export type OutputKind =
  | "progress"
  | "evaluation"
  | "result"
  | "reasoning"
  | "decision"
  | "summarization";

export type Output = Omit<JobOutputRecord, "job" | "kind" | "value" | "created_at"> & {
  kind: OutputKind;
  value: unknown;
  created_at_us: JobOutputRecord["created_at"];
};

export type Jobs = { jobs: Array<Job>; next_before: string | null; has_more: boolean };

export type Outputs = {
  job: string;
  after_seq: number | null;
  outputs: Array<Output>;
  next_after_seq: number | null;
  has_more: boolean;
};

export type ProviderResponse = {
  request: number;
  text: string;
  text_offset: number;
  text_complete: boolean;
  requests_tools: boolean;
  created_at_us: number;
};

export type ProviderResponses = {
  job: string;
  responses: Array<ProviderResponse>;
  next_after: number | null;
  next_text_offset: number | null;
  has_more: boolean;
};

export type ToolOutcomeKind =
  | "completed"
  | "completed_withheld"
  | "not_applied"
  | "not_invoked"
  | "uncertain";

export type ToolCall = {
  tool_call_id: string;
  request: number;
  index: number;
  name: string;
  decision: string;
  outcome_kind: ToolOutcomeKind | null;
  arguments: string | null;
  arguments_cut: boolean;
  result: string | null;
  result_cut: boolean;
  duration_ms?: number;
};

export type Tools = {
  job: string;
  after: string | null;
  tools: Array<ToolCall>;
  next_after: string | null;
  has_more: boolean;
};

export type Actor =
  | { kind: "auto" }
  | { kind: "user" }
  | { kind: "loop" }
  | { kind: "agent"; conversation: string };

export type Hold = { by: Actor };

export type Conversation = {
  job_state: string;
  has_children: boolean;
  root_job: string;
  tip_job: string;
  agent: string | null;
  title: string;
  project: string | null;
  state: string;
  on_hold: Hold | null;
  tip_settlement_state: string;
  tip_created_at_us: number;
};

export type Conversations = {
  conversations: Array<Conversation>;
  has_more: boolean;
  next_before: string | null;
};

export type ConversationLimits = {
  conversation: string;
  depth: number;
  remaining_depth: number;
  max_depth: number;
  max_auto_turns: number;
  max_iterations: number;
};

export type CompactionPreset = "conservative" | "balanced" | "aggressive";

export type CustomCompaction = {
  trigger_tokens: number;
  target_tokens: number;
  reserved_output_tokens: number;
  minimum_source_tokens: number;
  minimum_savings_tokens: number;
  preserve_recent_turns: number;
  cooldown_seconds: number;
};

export type CompactionMode =
  | { kind: "off" }
  | { kind: "auto"; preset: CompactionPreset }
  | { kind: "custom"; policy: CustomCompaction };

export type PredictionAcceptance = {
  model: string;
  model_edit_seq: number;
  reported_model: string;
  confidence_semantics: string;
  minimum_probability_millionths: number;
  minimum_confidence_millionths: number;
};

export type PredictionMode = "shadow" | "prepare";

export type PredictiveCompaction = {
  decision_agent: string;
  minimum_interval_seconds: number;
  max_preparations: number;
  max_prediction_tokens: number;
  max_preparation_tokens: number;
  mode: PredictionMode;
  acceptance: PredictionAcceptance | null;
  max_preparations_per_hour: number;
  max_tokens_per_hour: number;
  max_requests_per_hour: number;
};

export type CompactionPolicy = {
  mode: CompactionMode;
  summarization_model: string | null;
  predictive: PredictiveCompaction | null;
};

export type GlobalCompactionSettings = {
  revision: number;
  prediction_enabled: boolean;
  policy: CompactionPolicy;
};

export type ActiveSystemSettings = { path: string; content: string };

export type ContextModel = {
  profile: string;
  edit_seq: number;
  model: string;
  capacity: number | null;
  capacity_source: string;
  reserved_output_tokens: number;
};

export type ContextUse = { turn: string; seq: number; artifact: string | null };

export type ContextBoundary = {
  turn: string;
  input_through: number;
  output_through: number;
  fingerprint: string;
};

export type ContextSource = {
  digest: string;
  first: ContextBoundary;
  through: ContextBoundary;
  turn_count: number;
};

export type ContextWork =
  | {
      kind: "summarization";
      job: string;
      conversation: string;
      state: string;
      summarizer: string;
      failure: string | null;
      automatic: boolean;
      stage: string;
      chain: string | null;
      prediction: string | null;
      reserved_tokens: number;
      reserved_requests: number;
      source: ContextSource;
    }
  | {
      kind: "prediction";
      job: string;
      conversation: string;
      state: string;
      predictor: string;
      model: string;
      activity_turn: string;
      policy_revision: number;
      /**
       * Immutable policy used by this prediction, not the current conversation policy.
       */
      policy: CompactionPolicy;
      max_tokens: number;
      forecast_tokens: number | null;
      result: JsonValue | null;
    };

export type ContextStatus = {
  conversation: string;
  /**
   * Effective mode; `policy_source` names the winning override.
   */
  mode: string;
  summarizer: string | null;
  predictive: boolean;
  predictive_unavailable: string | null;
  revision: number;
  /**
   * Preset numbers are a proposal, not a calibrated default.
   */
  uncalibrated: boolean;
  policy: CompactionPolicy;
  policy_override: CompactionPolicy | null;
  policy_source: string;
  predictor: string | null;
  predictor_model: string | null;
  prediction_mode: string;
  predictive_preparation_enabled: boolean;
  foreground_model: ContextModel | null;
  estimated_tokens: number | null;
  estimate_method: string;
  recent_growth_tokens: number;
  last_context_use: ContextUse | null;
  steer_blocked_by?: string;
  work: Array<ContextWork>;
  has_more: boolean;
  next_before: string | null;
};

export type Recurrence = { interval_s: number; phase: string; due_us: number | null };

export type Child = {
  job_state: string;
  id: string;
  origin?: string;
  title: string;
  agent: string | null;
  state: string;
  on_hold: Hold | null;
  keep_open: boolean;
  recurrence: Recurrence | null;
};

export type Children = {
  conversation: string;
  children: Array<Child>;
  has_more: boolean;
  next_before: string | null;
};

export type Model = { llm_profile: string; model: string };

export type Dependency = { job: string; condition: string; state: string };

export type TreeNode = {
  id: string;
  kind: JobKind;
  parent: string | null;
  title: string;
  state: string;
  created_at_us: number;
  agent: string | null;
  agent_name: string | null;
  model: Model | null;
  on_hold: Hold | null;
  origin: string | null;
  keep_open: boolean | null;
  active_turn: string | null;
  recurrence: Recurrence | null;
  conversation: string | null;
  previous: string | null;
  depends_on: Array<Dependency>;
  children: Array<TreeNode> | null;
  has_more_children: boolean;
  next_after: string | null;
};

export type Tree = { job: string; ancestors: Array<string>; root: TreeNode };

export type Configuration = {
  agent_profile: string | null;
  llm_profile: string;
  model: string;
  context_window: number | null;
};

export type Input = {
  id: string;
  objective: string | null;
  submitted_at_us: number | null;
  attachments: Array<string>;
  configuration: Configuration | null;
};

export type JobInput = { job: string; input: Input };

export type TurnEntry = { seq: number; at_us: number } & (
  | { kind: "context_preparation"; job: string; policy_revision: string }
  | { kind: "context_use"; artifact: string | null }
  | { kind: "user_message"; text: string; attachments: Array<string> }
  | { kind: "user_message_edit"; message: number; text: string; attachments: Array<string> }
  | { kind: "user_message_withdraw"; message: number }
  | { kind: "agent_message"; text: string; call: string }
  | { kind: "notice"; child: string; event: string; reference: string }
  | { kind: "decision"; approval: [string, number, number] }
  | { kind: "retry_of"; turn: string; by: Actor }
  | { kind: "iteration"; n: number }
  | ({ kind: "response_format" } & (
      | { type: "json_object" }
      | { type: "json_schema"; name: string }
    ))
);

export type UncertainCall = { request: number; index: number; tool: string; argv: string };

export type CompactionUse = { artifact: string; kind: string; at_us: number };

export type Turn = {
  compaction?: CompactionUse;
  job: Job;
  entries: Array<TurnEntry>;
  configuration: Configuration | null;
  outputs: Array<Output>;
  uncertain_calls: Array<UncertainCall>;
  outputs_truncated: boolean;
};

export type Turns = {
  conversation: string;
  turns: Array<Turn>;
  has_more: boolean;
  next_before: string | null;
};

export type LlmParameters = {
  temperature: number | null;
  top_p: number | null;
  max_tokens: number | null;
  reasoning_effort: string | null;
  max_completion_tokens: number | null;
  presence_penalty: number | null;
  frequency_penalty: number | null;
};

export type LlmTarget = {
  reasoning_efforts: string[] | null;
  id: string;
  name: string;
  model: string;
  context_window: number | null;
  parameters: LlmParameters;
  declared_capabilities: Array<string>;
  executors: Array<string>;
};

export type AgentProfile = {
  id: string;
  name: string;
  capabilities: AgentCapability[];
  llm_targets: Array<LlmTarget>;
};

export type AgentProfiles = {
  agent_profiles: Array<AgentProfile>;
  has_more: boolean;
  next_before: string | null;
};

export type LlmProfile = {
  reasoning_efforts: string[] | null;
  id: string;
  /**
   * Which kind of Profile this is: `llm`, `agent`, `decision_model` or
   * `decision_agent`. Optional for clients that do not read it.
   */
  profile_type?: string;
  name: string;
  model: string;
  context_window: number | null;
  protocol: string;
  endpoint: string | null;
  credential_ref: string | null;
  has_credential: boolean;
  declared_capabilities: Array<string>;
  parameters: LlmParameters;
  editable: boolean;
  blocking_jobs: Array<string>;
  more_blocking_jobs: boolean;
  created_at_us: number;
  updated_at_us: number;
};

export type ManagedAgent = {
  id: string;
  /**
   * Which kind of Profile this is: `llm`, `agent`, `decision_model` or
   * `decision_agent`. Optional for clients that do not read it.
   */
  profile_type?: string;
  name: string;
  instructions: string | null;
  capabilities: AgentCapability[];
  llm_targets: Array<string>;
  compaction_policy?: CompactionPolicy;
  editable: boolean;
  blocking_jobs: Array<string>;
  more_blocking_jobs: boolean;
  created_at_us: number;
  updated_at_us: number;
};

export type Provider = {
  name: string;
  protocol: string;
  endpoint: string | null;
  credential_ref: string | null;
  has_credential: boolean;
  profile_ids: Array<string>;
  models: Array<string>;
};

export type Profiles = {
  llm_profiles: Array<LlmProfile>;
  agent_profiles: Array<ManagedAgent>;
  /**
   * The Decision Model Profiles on this page, when any. Optional for
   * clients that do not read them.
   */
  decision_profiles?: Array<DecisionProfile>;
  /**
   * The Decision Profiles on this page, when any. Optional for
   * clients that do not read them.
   */
  decision_agent_profiles?: Array<DecisionAgentProfile>;
  /**
   * Enabled executor capabilities on this page. Collect all pages
   * and join these with each Model's protocol for compatibility.
   */
  executor_protocols: Array<ProfileExecutor>;
  /**
   * Provider groups for the LLM Profiles on this page, not for the
   * whole catalog; follow `next_before` to collect the rest.
   */
  providers: Array<Provider>;
  has_more: boolean;
  next_before: string | null;
  profile_id?: string;
};

export type ProfileExecutor = { id: string; protocol: string };

export type ProjectCommand = Pick<StoredProjectCommand, "name" | "argv"> & {
  owner: string;
  status: string;
  proposal_job: string | null;
};

export type ProjectDecisionConfig = {
  enabled: boolean;
  decision_profile: string | null;
  minimum_confidence: number;
};

export type Project = Pick<ProjectRecord, "id" | "name" | "description" | "root"> & {
  commands: ProjectCommand[];
  decision: ProjectDecisionConfig | null;
  created_at_us: ProjectRecord["created_at"];
  updated_at_us: ProjectRecord["updated_at"];
};

export type Projects = { projects: Array<Project>; has_more: boolean; next_before: string | null };

export type ProjectAnswer = { project: Project };

export type Pool = {
  id: string;
  name: string;
  concurrency_limit: number;
  enabled: boolean;
  /**
   * The operations this Pool offers: `llm_request` and `decision`.
   * Optional for clients that do not read it.
   */
  operations?: Array<string>;
};

export type PoolAnswer = { pool: Pool };

export type UsageObservation = {
  llm_profile: string | null;
  input_tokens: number | null;
  output_tokens: number | null;
  cache_read_tokens: number | null;
  cache_write_tokens: number | null;
  reasoning_tokens: number | null;
  ttfb_ms: number | null;
  finished_in_ms: number | null;
  eligibility: string | null;
  finality: string | null;
  id: string;
  job: string;
  request: number;
  observed_at_us: number;
  model: string | null;
  provider: string | null;
};

export type Usage = { observations: Array<UsageObservation>; next_after: string | null };

export type AvailabilityObservation = {
  status_code: number | null;
  finish_reason: string | null;
  classification: string;
  id: string;
  job: string;
  request: number;
  observed_at_us: number;
  model: string | null;
  provider: string | null;
};

export type Availability = {
  observations: Array<AvailabilityObservation>;
  next_after: string | null;
};

export type UsageBucket = {
  bucket_us: number;
  agent: string | null;
  provider: string;
  model: string | null;
  requests: number;
  complete_requests: number;
  input_tokens: number;
  output_tokens: number;
  cache_read_tokens: number;
  cache_write_tokens: number;
  reasoning_tokens: number;
};

export type UsageRetryRow = {
  agent: string | null;
  provider: string;
  model: string | null;
  retries: number;
  bucket_us: number;
};

export type UsageRetries = {
  /**
   * Partial groups to sum by their stable key; a page may be empty
   * while `next_after` still continues the traversal.
   */
  retry_rows: Array<UsageRetryRow>;
  /**
   * The canonical `original.retry` UUID cursor, `None` exactly at real
   * exhaustion.
   */
  next_after: string | null;
};

export type UsageBuckets = {
  from_us: number;
  until_us: number;
  complete: boolean;
  /**
   * The opaque source-row continuation; `None` exactly when complete.
   */
  resume_after: string | null;
  rows: Array<UsageBucket>;
  bucket: string;
  tz_offset_minutes: number;
};

export type Logs = {
  files: Array<string>;
  file: string | null;
  entries: Array<{ offset: number; line: string }>;
  next_before: number | null;
  truncated: boolean;
  files_truncated: boolean;
};

export type DatabaseHealth = {
  status: string;
  accepting_commands: boolean;
  readable: boolean;
  database_bytes: number | null;
  available_bytes: number | null;
  durable_commits: number;
  commit_us_mean: number | null;
  commit_us_max: number;
};

export type Claims = { count: number; mean_us: number | null; max_us: number };

export type Queue = {
  pending: number | null;
  ready: number | null;
  delayed: number | null;
  commands_waiting: number;
};

export type Process = { rss_bytes: number | null; peak_rss_bytes: number | null };

export type Quarantined = { job: string; pool: string | null; since: number };

export type Health = {
  host: string;
  database: DatabaseHealth;
  claims: Claims;
  queue: Queue;
  process: Process;
  quarantined: Array<Quarantined>;
};

export type SignedOut = { authenticated: false };

export type SignedIn = { authenticated: true; csrf_token: string; expires_at_us: number };

export type Session = SignedOut | SignedIn;

export type SessionResponse = { session: Session };

export type Attachment = {
  id: string;
  mime: string;
  byte_length: number;
  sha256: string;
  display_name: string | null;
  created_at_us: number;
  available: boolean;
};

export type JobDetails = {
  job: Job;
  result: Output | null;
  usage: JobUsage | null;
  activity: JobActivity;
  task_plan: TaskPlan | null;
  unknown_reason: string | null;
};

export type JobEvent = { job: Job };

export type OutputEvent = { job: string; output: Output };

export type SettlementEvent = {
  job: string;
  settlement: Settlement;
  stop_confirmed_at_us: number | null;
};

export type ApprovalAssessment = {
  state: "pending" | "running" | "completed" | "failed" | "unavailable";
  confidence: number | null;
  minimum_confidence: number;
};

export type Card =
  | {
      kind: "dispatch";
      conversation: string;
      parent: string | null;
      approval: [string, number, number];
      tool: string;
      arguments: string;
    }
  | {
      kind: "approval";
      conversation: string;
      parent: string | null;
      approval: [string, number, number];
      tool: string;
      arguments: string;
      assessment?: ApprovalAssessment;
    }
  | {
      kind: "question";
      conversation: string;
      parent: string | null;
      approval: [string, number, number];
      tool: string;
      question: string;
    }
  | { kind: "loop_hold"; conversation: string; parent: string | null }
  | { kind: "quarantine"; conversation: string; parent: string | null; turn: string };

export type Cards = { conversation: string; cards: Array<Card> };

export type Admission =
  | {
      status: "admitted";
      job: string;
      conversation: string | null;
      seq: number;
      joined: boolean;
      model: string;
      kept: boolean;
    }
  | { status: "pending"; job: string | null }
  | { status: "uncertain"; job: string | null }
  | { status: "not_found"; job: string | null };

export type AdmissionResponse = { admission: Admission };

export type DecisionLimits = {
  max_input_tokens_per_request: number | null;
  max_output_tokens_per_request: number | null;
  max_input_tokens_per_question: number | null;
  max_output_tokens_per_question: number | null;
};

export type DecisionProfile = {
  id: string;
  edit_seq: number;
  name: string;
  model: string;
  protocol: string;
  endpoint: string | null;
  credential_ref: string | null;
  has_credential: boolean;
  declared_capabilities: Array<string>;
  limits: DecisionLimits;
  editable: boolean;
  created_at_us: number;
  updated_at_us: number;
};

export type DecisionAgentProfile = {
  id: string;
  name: string;
  instructions: string | null;
  models: Array<string>;
  default_model: string | null;
  editable: boolean;
  created_at_us: number;
  updated_at_us: number;
};

export type ProviderModel = {
  id: string;
  reasoning_efforts: string[] | null;
  context_window: number | null;
};

export type ImportedModels = { profile_ids: Array<string>; created: number; existing: number };

export type ProviderModels = { models: Array<ProviderModel> };

export type Append = { conversation: string; turn: string; seq: number; joined: boolean };

export type Stop = { conversation: string; status: string; record_seq: number | null };

export type ConversationEdited = { conversation: string };

export type Compaction = {
  conversation: string;
  status: string;
  job: string | null;
  system_conversation: string | null;
  through: string | null;
  subsequent: boolean;
  reason: string | null;
};

export type ApprovalDecision = {
  approval: [string, number, number];
  decision: string;
  turn: string;
  seq: number;
  joined: boolean;
};

export type ReleaseSlot = { status: string };
