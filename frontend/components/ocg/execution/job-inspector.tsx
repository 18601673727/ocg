"use client";

import { CostDetails } from "../usage/usage-values";
import { CircleDot, ExternalLink, Maximize2, Minimize2, X } from "lucide-react";
import { startTransition, useState, ViewTransition } from "react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import {
  EmptyState,
  JOB_STATE,
  Pill,
  SegmentedTabs,
  type TabItem,
} from "@/components/ocg/primitives";
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
import { runtimeStateLabel, useI18n, type I18nKey } from "../i18n";

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

/** Canonical Job summary: identity, Project, and the reported Job state. */
function JobContext({ execution, compact = false }: { execution: JobExecution; compact?: boolean }) {
  const { t } = useI18n();
  const state = JOB_STATE[execution.state];
  return (
    <div className={cn(compact ? "rounded-md border border-border bg-muted/20 px-2.5 py-2" : "", "min-w-0")}>
      <p
        className={cn("truncate font-semibold tracking-tight", compact ? "text-[12px]" : "text-[14px]")}
        title={execution.jobId}
      >
        {execution.jobId}
      </p>
      <div className="mt-1.5 flex items-center gap-1.5">
        <Pill
          tone={state.tone}
          dot
          pulse={state.pulse}
        >
          {runtimeStateLabel(t, execution.state)}
        </Pill>
      </div>
      {!compact && <p className="mt-1 text-[11px] text-muted-foreground">{t("execution.jobDetailsNotReported")}</p>}
    </div>
  );
}

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
      <header className="flex shrink-0 items-center gap-2 border-b border-border px-3 py-2.5">
        <CircleDot className="size-4 text-muted-foreground" aria-hidden="true" />
        <h2 className="flex-1 text-[13px] font-semibold tracking-tight">{t("execution.inspector")}</h2>
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
        {!observability ? (
          <div className="flex flex-col gap-3">
            <JobContext execution={execution} />
            <EmptyState>{t("execution.noObservability")}</EmptyState>
          </div>
        ) : (
          <>
            <JobContext execution={execution} compact={tab !== "overview"} />
            <nav className="sticky top-0 z-10 -mx-3 mt-3 border-y border-border bg-background/95 px-3 py-1.5 backdrop-blur">
              <SegmentedTabs
                tabs={tabs}
                value={tab}
                onSelect={(next) => startTransition(() => setTab(next))}
                ariaLabel={t("execution.inspectorSurfaces")}
                panelIdBase="job-inspector"
                className="grid-cols-3"
              />
            </nav>
            <ViewTransition key={tab} enter="vt-detail" exit="vt-detail" default="none">
              <div id={`job-inspector-${tab}`} role="tabpanel" aria-label={t(TAB_LABEL_KEYS[tab])} className="mt-3">
                <ObservabilityPanel execution={execution} accounting={accounting} observability={observability} tab={tab} />
              </div>
            </ViewTransition>
          </>
        )}
      </div>
    </div>
  );
}
