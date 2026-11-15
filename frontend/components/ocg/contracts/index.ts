export type {
  ChatImage,
  ChatImageUploadRequest,
  JsonValue,
} from "./view-models";

export { ContractError } from "./decode";

export type {
  ToolOutcomeKind,
  ToolCall,
  AgentCapability,
  TreeState,
  TreeNode,
  Approval,
} from "./shared";
export { POOL_OPERATIONS, TREE_MAX_DEPTH } from "./shared";
export * from "./jobs";
export * from "./conversations";
export * from "./catalog";
export * from "./settings";
export { API_VERSION } from "./generated";

export type ChatModelSelection = import("./view-models").ChatModelSelection & {
  agentProfile?: string;
  llmProfile?: string;
  project?: string;
};
