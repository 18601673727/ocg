import { createMemo } from "solid-js";
import { useI18n } from "../i18n";
import { type ToolActivity, toolActivityLabels } from "./tool-activity-summary";

export function ToolActivityLabel(props: { tools: readonly ToolActivity[] }) {
  const i18n = useI18n();
  const label = createMemo(() => toolActivityLabels(props.tools, i18n.t, i18n.locale));
  return (
    <span
      class="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1 text-xs leading-5"
      data-slot="tool-activity-summary"
    >
      <span>{label().categories}</span>
      <span
        class="font-normal tabular-nums text-muted-foreground"
        title={i18n.t("chat.tools.metricsHint")}
      >
        {label().tokens} · {label().duration}
      </span>
    </span>
  );
}
