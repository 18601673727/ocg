/**
 * The generated wire contract and its runtime decoders.
 *
 * Rust owns the control protocol: `src/contracts.rs` declares every struct
 * that crosses the loopback boundary and `cargo run --bin ocg-rs-ts` projects
 * it into `./generated`. Nothing in the PWA hand-maintains a mirror of a Rust
 * struct, and `tests/contracts.rs` fails if the projection drifts.
 *
 * Frontend code should import from here rather than from `./generated`
 * directly, so the types and the decoders that check them stay together.
 */

export type {
  ChatSendRequest,
  ChatImage,
  ChatImageUploadRequest,
  ChatModelSelection,
  ChatConversationView,
  ChatConversationsResponse,
  ChatMessageView,
  ChatMessagesResponse,
  ApiErrorBody,
  ApiErrorEnvelope,
  CanonicalApiVersion,
  CanonicalConfigurationEnvelope,
  CanonicalConfigurationResponse,
  CanonicalDashboardResponse,
  CanonicalEventsEnvelope,
  CanonicalJobConfigEnvelope,
  CanonicalJobConfigResponse,
  CanonicalJobEvent,
  CanonicalJobRelations,
  CanonicalJobOperations,
  CanonicalJobOperationRequest,
  CanonicalJobOperationResponse,
  Failure,
  FailureClass,
  CanonicalJobSnapshot,
  CanonicalJobSummary,
  CanonicalProjectResponse,
  CanonicalProjectsResponse,
  GlobalConfiguration,
  JobLaunchRequest,
  JobLaunchResponse,
  JsonValue,
  Model,
  ModelMetadata,
  SetupModelSelection,
  SetupRefreshRequest,
  Origin,
  Profile,
  ProfileApiVersion,
  ProfileBootstrapRequest,
  ProfileReplaceRequest,
  ProfileView,
  ProjectConfiguration,
  ProjectConfigurationView,
  ProjectRecord,
  Provider,
  ProviderProtocol,
} from "./generated";

export { CANONICAL_API_VERSION, PROFILE_API_VERSION } from "./generated";

export { ContractError, decode } from "./decode";
export type { DecodeResult, Decoder } from "./decode";

export {
  decodeProfile,
  decodeProfileView,
  decodeProvider,
  tryProfileView,
} from "./profile-decode";

export {
  decodeSetupConnectResponse,
  decodeSetupModelsResponse,
  decodeSetupBrowseResponse,
  decodeSetupProjectResponse,
} from "./setup-decode";
export type {
  SetupModel,
  SetupConnectResponse,
  SetupModelsResponse,
  SetupDirectoryEntry,
  SetupBrowseResponse,
  SetupProjectResponse,
} from "./setup-decode";

export {
  decodeConfigurationAck,
  decodeChatConversationsResponse,
  decodeChatMessagesResponse,
  decodeChatImage,
  decodeConfigurationEnvelope,
  decodeConfigurationView,
  decodeDashboardResponse,
  decodeJobOperationResponse,
  decodeEventsEnvelope,
  decodeExecutionState,
  decodeJobConfigEnvelope,
  decodeJobConfigResponse,
  decodeJobLaunchResponse,
  decodeJobSnapshot,
  decodeProjectRecord,
  decodeProjectResponse,
  decodeProjectsResponse,
  tryEventsEnvelope,
} from "./canonical-decode";
export type {
  ATTEMPT_STATES,
  CanonicalAttempt,
  CanonicalAttemptState,
  CanonicalCall,
  CanonicalDispatchIntent,
  CanonicalEffectKind,
  CanonicalExecutor,
  CanonicalExecutionState,
  CanonicalJob,
  CanonicalJobLaunchAck,
  CanonicalJobLaunchOutcome,
  CanonicalJobState,
  ConfigurationAck,
  EFFECT_KINDS,
  JOB_LAUNCH_OUTCOMES,
  JOB_STATES,
} from "./canonical-decode";

export type { UsageQuantity, UsageTokenTotals, UsageCurrencyCost, UsageCost, ContextCostTotals, UsageTotals, UsageBreakdown, UsageConversationRow, ProjectUsageResponse, ConversationUsageResponse, JobUsageResponse, UsageCompleteness, UsageWindow, UsageCostSource } from "./generated";
export { decodeProjectUsageResponse, decodeConversationUsageResponse, decodeJobUsageResponse } from "./usage-decode";
