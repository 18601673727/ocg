import type * as Api from "./api";

export type ToolOutcomeKind = Api.ToolOutcomeKind;
export type ToolCall = Api.ToolCall;
export type AgentCapability = Api.AgentCapability;
export type Approval = Api.ApprovalDecision["approval"];
export type TreeState =
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
export type TreeNode = Omit<
  Api.TreeNode,
  "state" | "origin" | "recurrence" | "depends_on" | "children"
> & {
  state: TreeState;
  origin: "user" | "agent" | "system" | null;
  recurrence:
    | (Omit<Api.Recurrence, "phase"> & { phase: "active" | "waiting_due" | "halted" })
    | null;
  depends_on: (Omit<Api.Dependency, "condition" | "state"> & {
    condition: "settled" | "success";
    state: "waiting" | "satisfied" | "impossible";
  })[];
  children: TreeNode[] | null;
};
export const TREE_MAX_DEPTH = 4;
