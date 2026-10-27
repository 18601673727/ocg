import type * as Api from "./api";

export type ControlToolOutcomeKind = Api.ToolOutcomeKind;
export type ControlToolCall = Api.ToolCall;
export type ControlAgentCapability = Api.AgentCapability;
export const POOL_OPERATIONS = ["llm_request", "decision"] as const;
export type ControlApproval = Api.ApprovalDecision["approval"];
export type ControlTreeState =
  | "open"
  | "closing"
  | "settled"
  | "completed"
  | "failed"
  | "cancelled"
  | "unknown"
  | "running"
  | "waiting_for_user"
  | "waiting"
  | "queued";
export type ControlTreeNode = Omit<
  Api.TreeNode,
  "state" | "origin" | "recurrence" | "depends_on" | "children"
> & {
  state: ControlTreeState;
  origin: "user" | "agent" | "system" | null;
  recurrence:
    | (Omit<Api.Recurrence, "phase"> & { phase: "active" | "waiting_due" | "halted" })
    | null;
  depends_on: (Omit<Api.Dependency, "condition" | "state"> & {
    condition: "settled" | "success";
    state: "waiting" | "satisfied" | "impossible";
  })[];
  children: ControlTreeNode[] | null;
};
export const TREE_MAX_DEPTH = 4;
