/** Import frontend view models and checked Control Plane projections here. */

export type {
  ChatImage,
  ChatImageUploadRequest,
  CanonicalApiVersion,
  CanonicalJobRelations,
  ChildPolicy,
  JobOrigin,
  CanonicalJobOperations,
  Failure,
  FailureClass,
  JsonValue,
  ProfileApiVersion,
  RecursiveLimits,
} from "./view-models";

export { CANONICAL_API_VERSION, PROFILE_API_VERSION } from "./view-models";

export { ContractError } from "./decode";

export type {
  CanonicalAttempt,
  CanonicalCall,
  CanonicalDispatchIntent,
  CanonicalEffectKind,
  CanonicalExecutor,
  CanonicalWatchdogAction,
  CanonicalJobState,
} from "./view-models";

export type {
  UsageCurrencyCost,
  UsageCost,
  UsageCompleteness,
  UsageCostSource,
} from "./view-models";

export type {
  ControlToolOutcomeKind,
  ControlToolCall,
  ControlAgentCapability,
  ControlTreeState,
  ControlTreeNode,
  ControlApproval,
} from "./control-shared";
export { POOL_OPERATIONS, TREE_MAX_DEPTH } from "./control-shared";
export * from "./control-jobs";
export * from "./control-conversation";
export * from "./control-catalog";
export * from "./control-settings";
export { CONTROL_API_VERSION } from "./generated";

export type ChatModelSelection = import("./view-models").ChatModelSelection & {
  agentProfile?: string;
  llmProfile?: string;
  executor?: string;
  // The Project of a conversation's first turn; chosen only for an Agent with a Workspace grant.
  project?: string;
};
