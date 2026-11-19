export * from "./catalog";
export * from "./conversations";
export { ContractError } from "./decode";
export { API_VERSION } from "./generated";
export * from "./jobs";
export * from "./settings";
export type {
  AgentCapability,
  Approval,
  ToolCall,
  ToolOutcomeKind,
  TreeNode,
  TreeState,
} from "./shared";
export { TREE_MAX_DEPTH } from "./shared";
export type {
  ChatImage,
  ChatImageUploadRequest,
  JsonValue,
} from "./view-models";

export type ChatModelSelection = import("./view-models").ChatModelSelection & {
  agentProfile?: string;
  llmProfile?: string;
  project?: string;
};
