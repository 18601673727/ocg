/** Import frontend view models and checked Control Plane projections here. */

export type {
  ChatImage,
  ChatImageUploadRequest,
  ChildPolicy,
  JobOrigin,
  Failure,
  FailureClass,
  JsonValue,
  RecursiveLimits,
} from "./view-models";

export { ContractError } from "./decode";

export type {
  CanonicalAttempt,
  CanonicalEffectKind,
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
