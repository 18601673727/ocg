"use client";

import { CostDetails } from "../usage/usage-values";
import { ExternalLink, Maximize2, Minimize2, X } from "lucide-react";
import { startTransition, useState, ViewTransition } from "react";
import { Button } from "@/components/ui/button";
import { SegmentedTabs, type TabItem } from "@/components/ocg/primitives";
import type { JobExecution } from "./domain";
import type { JobAccounting } from "./accounting";
import type { RuntimeObservability } from "../runtime/observability";
import { ObservabilityPanel } from "../observability/observability-panel";
import {
  INSPECTOR_TABS,
  toggleInspectorMode,
  type InspectorMode,
  type InspectorTab,
} from "../observability/inspector-state";
import { useI18n, type I18nKey } from "../i18n";
import { CanonicalJobOverview } from "./canonical-job-overview";

type JobInspectorProps = {
  execution: JobExecution;
  accounting?: JobAccounting | null;
  observability?: RuntimeObservability | null;
  mode?: InspectorMode;
  onModeChange?: (mode: InspectorMode) => void;
  onClose: () => void;
  onOpenJobExecution?: () => void;
};

const TAB_LABEL_KEYS: Record<InspectorTab, I18nKey> = {
  overview: "execution.overview",
  runtime: "execution.runtime",
  usage: "execution.usage",
};

export function JobInspector({
  execution,
  accounting = null,
  observability,
  mode = "docked",
  onModeChange,
  onClose,
  onOpenJobExecution,
}: JobInspectorProps) {
  const { t } = useI18n();
  const [tab, setTab] = useState<InspectorTab>("overview");
  const nextMode = toggleInspectorMode(mode);
  const tabs: TabItem<InspectorTab>[] = INSPECTOR_TABS.map((id) => ({ id, label: t(TAB_LABEL_KEYS[id]) }));
  const dockLabel = t("execution.dockInspector");
  const expandLabel = t("execution.expandInspector");
  const modeLabel = mode === "expanded" ? dockLabel : expandLabel;
  return (
    <div className="flex h-full w-full min-w-0 flex-col">
      <header className="flex shrink-0 items-center gap-1 border-b border-border px-3 py-2">
        <h2 className="min-w-0 flex-1 text-[12px] font-semibold">{t("execution.inspector")}</h2>
        {onOpenJobExecution && (
          <Button variant="ghost" size="icon-xs" onClick={onOpenJobExecution} aria-label={t("execution.openJobExecution")} title={t("execution.openJobExecution")}>
            <ExternalLink className="size-3.5" />
          </Button>
        )}
        {onModeChange && (
          <Button
            variant="ghost"
            size="icon-xs"
            className="hidden lg:inline-flex"
            onClick={() => onModeChange(nextMode)}
            aria-label={modeLabel}
            title={modeLabel}
          >
            {mode === "expanded" ? <Minimize2 className="size-3.5" /> : <Maximize2 className="size-3.5" />}
          </Button>
        )}
        <Button variant="ghost" size="icon-xs" onClick={onClose} aria-label={t("execution.collapseInspector")} title={t("execution.collapseInspector")}>
          <X className="size-4" />
        </Button>
      </header>

      <div className="min-h-0 flex-1 overflow-y-auto px-3 py-3">
        {accounting?.consumption ? <section className="mb-3 border-b border-border pb-3"><CostDetails cost={accounting.consumption} /></section> : null}
        <CanonicalJobOverview execution={execution} />
        {observability && (
          <section className="mt-4 border-t border-border pt-3" aria-label={t("execution.canonical.runtimeSupplement")}>
            <h3 className="mb-1.5 text-[11px] font-semibold">{t("execution.canonical.runtimeSupplement")}</h3>
            <SegmentedTabs
              tabs={tabs}
              value={tab}
              onSelect={(next) => startTransition(() => setTab(next))}
              ariaLabel={t("execution.inspectorSurfaces")}
              panelIdBase="job-inspector"
              className="grid-cols-3"
            />
            <ViewTransition key={tab} enter="vt-detail" exit="vt-detail" default="none">
              <div id={`job-inspector-${tab}`} role="tabpanel" aria-label={t(TAB_LABEL_KEYS[tab])} className="mt-3">
                <ObservabilityPanel execution={execution} accounting={accounting} observability={observability} tab={tab} />
              </div>
            </ViewTransition>
          </section>
        )}
      </div>
    </div>
  );
}
