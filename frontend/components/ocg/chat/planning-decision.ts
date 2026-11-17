import type { TranslateFn } from "../i18n";
import { runtimeStateLabel } from "../i18n";
import { executionLabel } from "../runtime/job-status";
import type { ChatMessage } from "../types";

export function planningConclusion(message: ChatMessage): string | undefined {
  const outcome = message.decision?.result?.outcome;
  if (
    message.status === "completed" &&
    outcome?.status === "answer" &&
    outcome.answer.kind === "choice"
  )
    return outcome.answer.selected;
}

export function planningLabel(t: TranslateFn, message: ChatMessage): string {
  const conclusion = planningConclusion(message);
  switch (conclusion) {
    case "direct":
      return t("planning.direct");
    case "phased":
      return t("planning.phased");
    case "delegated":
      return t("planning.delegated");
    case undefined:
      break;
    default:
      return conclusion;
  }
  const outcome = message.decision?.result?.outcome;
  if (outcome?.status === "uncertain") return t("planning.uncertain");
  if (outcome?.status === "unusable") return t("planning.unusable");
  if (message.status === "completed") return t("planning.waitingForResult");
  return message.execution
    ? executionLabel(t, message.execution)
    : runtimeStateLabel(t, message.status);
}

export function planningBlocker(t: TranslateFn, message: ChatMessage): string {
  const outcome = message.decision?.result?.outcome;
  const reason =
    message.decision?.failure ||
    message.failureReason ||
    (outcome?.status === "uncertain" || outcome?.status === "unusable"
      ? outcome.reason
      : undefined);
  return [planningLabel(t, message), reason].filter(Boolean).join(" · ");
}
