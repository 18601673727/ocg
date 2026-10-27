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
  return (
    typeof value === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value)
  );
}

export function isReachableView(value: unknown): value is WorkspaceView {
  return (
    typeof value === "string" &&
    ["chat", "jobs", "usage", "logs", "health", "settings"].includes(value)
  );
}
