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
  AuthenticationMode,
  AuthenticationSession,
  ChatSendRequest,
  ChatImage,
  ChatImageUploadRequest,
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
  CanonicalJobSpawnRequest,
  CanonicalJobSpawnResponse,
  ChildPolicy,
  JobOrigin,
  JobSpec,
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
  RecursiveLimits,
} from "./generated";

export { CANONICAL_API_VERSION, PROFILE_API_VERSION } from "./generated";

export { ContractError, decode } from "./decode";
export type { DecodeResult, Decoder } from "./decode";

export {
  decodeExecutionState,
} from "./canonical-decode";
export type {
  CanonicalAttempt,
  CanonicalCall,
  CanonicalDispatchIntent,
  CanonicalEffectKind,
  CanonicalExecutor,
  CanonicalExecutionState,
  CanonicalWatchdogAction,
  CanonicalJob,
  CanonicalJobState,
} from "./canonical-decode";

export type { UsageQuantity, UsageTokenTotals, UsageCurrencyCost, UsageCost, ContextCostTotals, UsageTotals, UsageBreakdown, UsageConversationRow, ProjectUsageResponse, ConversationUsageResponse, JobUsageResponse, UsageCompleteness, UsageWindow, UsageCostSource } from "./generated";

export * from "./control-plane";

export type ChatModelSelection = import("./generated").ChatModelSelection & {
  agentProfile?: string;
  llmProfile?: string;
  executor?: string;
  // The Project of a conversation's first turn; chosen only for an Agent with a Workspace grant.
  project?: string;
};
