"use client";

import { useI18n } from "../i18n";
import { toolActivityLabels, type ToolActivity } from "./tool-activity-summary";

export function ToolActivityLabel({ tools }: { tools: readonly ToolActivity[] }) {
  const { t, locale } = useI18n();
  const label = toolActivityLabels(tools, t, locale);
  return <span className="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1 text-xs leading-5" data-slot="tool-activity-summary">
    <span>{label.categories}</span>
    <span className="font-normal tabular-nums text-muted-foreground" title={t("chat.tools.metricsHint")}>{label.tokens} · {label.duration}</span>
  </span>;
}
