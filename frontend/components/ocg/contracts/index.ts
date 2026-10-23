/** Import frontend view models and checked Control Plane projections here. */

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
} from "./view-models";

export { CANONICAL_API_VERSION, PROFILE_API_VERSION } from "./view-models";

export { ContractError, decode } from "./decode";
export { decodeJobDetails } from "./control-plane";
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

export type { UsageQuantity, UsageTokenTotals, UsageCurrencyCost, UsageCost, ContextCostTotals, UsageTotals, UsageBreakdown, UsageConversationRow, ProjectUsageResponse, ConversationUsageResponse, JobUsageResponse, UsageCompleteness, UsageWindow, UsageCostSource } from "./view-models";

export * from "./control-plane";
export { CONTROL_API_VERSION } from "./generated";

export type ChatModelSelection = import("./view-models").ChatModelSelection & {
  agentProfile?: string;
  llmProfile?: string;
  executor?: string;
  // The Project of a conversation's first turn; chosen only for an Agent with a Workspace grant.
  project?: string;
};
