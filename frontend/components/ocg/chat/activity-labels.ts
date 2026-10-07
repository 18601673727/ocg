import type { TranslateFn } from "../i18n";
import type { ActivityStep, ExecutionPhase } from "./execution-status";

/**
 * One wording for observable execution steps, shared by the Chat model area
 * and the Job inspector so the two never describe the same Call differently.
 */
const ACTIVITY_LABELS = {
  read: "chat.activityRead", list: "chat.activityList", search: "chat.activitySearch",
  edit: "chat.activityEdit", run: "chat.activityRun", tool: "chat.activityTool",
} as const;

export function stepLabel(t: TranslateFn, step: ActivityStep): string {
  if (step.kind === "provider") return t("chat.activityWaitingProvider");
  if (step.kind === "provider-failed") return t("chat.activityProviderFailed");
  if (step.kind === "validate") return t("chat.activityValidate");
  if (step.kind === "snapshot") return t("chat.activitySnapshot");
  return t(ACTIVITY_LABELS[step.kind], { target: step.target }).trim();
}

export function currentLabel(t: TranslateFn, current: ActivityStep | ExecutionPhase): string {
  if (current === "queued") return t("chat.activityQueued");
  if (current === "cancelling") return t("chat.activityCancelling");
  if (current === "running") return t("chat.activityWaitingProvider");
  return current.kind === "provider" ? stepLabel(t, current) : t("chat.activityNow", { step: stepLabel(t, current) });
}
