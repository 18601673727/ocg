

export type WorkspaceView =
  | "jobs"
  | "chat"
  | "home"
  | "usage"
  | "attention"
  | "ledger"
  | "control-center"
  | "job-execution"
  | "health"
  | "logs"
  | "settings"
  | "canonical";

export function isJobIdentity(value: unknown): value is string {
  return typeof value === "string" && /^job:(0|[1-9][0-9]*)$/.test(value);
}

export function isReachableView(value: unknown): value is WorkspaceView {
  return typeof value === "string" && ["chat", "jobs", "usage", "logs", "health", "settings"].includes(value);
}
