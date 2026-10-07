"use client";

import { useState } from "react";
import { Maximize2, Minimize2, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { SegmentedTabs } from "../primitives";
import { JobInspector } from "../execution/job-inspector";
import type { JobExecution } from "../execution/domain";
import type { JobAccounting } from "../execution/accounting";
import type { RuntimeObservability } from "../runtime/observability";
import type { InspectorMode } from "../observability/inspector-state";
import type { ConversationUsageResponse } from "../contracts";
import { useI18n, runtimeStateLabel } from "../i18n";
import { formatCount } from "@/lib/format";
import type { UsageRead } from "./use-usage";
import { ActivityMetrics, ContextMetrics, CostDetails, hasRecordedUsage, TokenMetrics, UsageBreakdownList, UsageMetric, UsageScopeNote, usageTimestamp } from "./usage-values";

export function ConversationInspector({ usage, execution, accounting, observability, mode, onModeChange, onClose, onOpenJobExecution }: {
  usage: UsageRead<ConversationUsageResponse>; execution: JobExecution | null; accounting: JobAccounting | null;
  observability?: RuntimeObservability | null; mode: InspectorMode; onModeChange?: (mode: InspectorMode) => void;
  onClose: () => void; onOpenJobExecution: () => void;
}) {
  const { t, locale } = useI18n();
  const [tab, setTab] = useState<"conversation" | "execution">("conversation");
  const data = usage.data;
  const initialLoading = usage.loading && data === null;
  return <div className="flex h-full min-w-0 flex-col">
    <nav className="shrink-0 border-b border-border p-2"><SegmentedTabs tabs={[{ id: "conversation", label: t("usage.conversation") }, { id: "execution", label: t("usage.execution") }]} value={tab} onSelect={setTab} ariaLabel={t("usage.inspector")} className="grid-cols-2" panelId="conversation-inspector-panel" /></nav>
    <div id="conversation-inspector-panel" role="tabpanel" aria-label={t(tab === "conversation" ? "usage.conversation" : "usage.execution")} className="flex min-h-0 flex-1 flex-col">
      {tab === "execution" && execution ? <JobInspector execution={execution} accounting={accounting} observability={observability} mode={mode} onModeChange={onModeChange} onClose={onClose} onOpenJobExecution={onOpenJobExecution} /> : <>
        <header className="flex shrink-0 items-center gap-1 border-b border-border px-3 py-2">
          <h2 className="min-w-0 flex-1 text-[12px] font-semibold">{t("usage.inspector")}</h2>
          {onModeChange ? <Button variant="ghost" size="icon-xs" onClick={() => onModeChange(mode === "expanded" ? "docked" : "expanded")} aria-label={t(mode === "expanded" ? "execution.dockInspector" : "execution.expandInspector")}>{mode === "expanded" ? <Minimize2 className="size-3.5" /> : <Maximize2 className="size-3.5" />}</Button> : null}
          <Button variant="ghost" size="icon-xs" onClick={onClose} aria-label={t("common.close")}><X className="size-4" /></Button>
        </header>
        <div className="min-h-0 flex-1 space-y-4 overflow-y-auto p-3 [scrollbar-gutter:stable]" data-usage-surface="conversation">
          {tab === "execution" ? <p className="text-[12px] text-muted-foreground">{t("execution.noExecution")}</p> : <>
            {initialLoading ? <p role="status" className="text-[12px] text-muted-foreground">{t("common.loading")}</p> : null}
            {usage.error ? <p role="alert" className="text-[12px] text-destructive">{t("usage.failed", { error: usage.error })}</p> : null}
            {!initialLoading && !usage.error && !data ? <p className="text-[12px] text-muted-foreground">{t("usage.noActivity")}</p> : null}
            {data ? <>
              <UsageScopeNote scope="conversation" generatedAt={data.generated_at} />
              {data.truncated ? <p className="text-[11px] text-amber-600 dark:text-amber-400">{t("usage.limited")}</p> : null}
              {hasRecordedUsage(data.totals) ? <>
                <section><h3 className="text-[11px] font-semibold">{t("usage.cost")}</h3><CostDetails cost={data.totals.cost} /></section>
                <section className="border-t border-border pt-3"><h3 className="text-[11px] font-semibold">{t("usage.tokens")}</h3><TokenMetrics totals={data.totals} /></section>
                <section className="border-t border-border pt-3"><h3 className="text-[11px] font-semibold">{t("usage.activity")}</h3><dl>
                  <UsageMetric label="usage.turns" field="turns">{formatCount(data.totals.turns)}</UsageMetric>
                  <UsageMetric label="usage.jobs" field="jobs">{formatCount(data.totals.jobs)}</UsageMetric>
                </dl><ActivityMetrics totals={data.totals} /></section>
                <section className="border-t border-border pt-3"><h3 className="text-[11px] font-semibold">{t("usage.breakdown")}</h3><UsageBreakdownList rows={data.models} /></section>
                <section className="border-t border-border pt-3"><h3 className="text-[11px] font-semibold">{t("usage.context")}</h3><ContextMetrics totals={data.totals} /><p className="mt-2 text-[10px] text-muted-foreground">{t("usage.efficiencyNote")}</p></section>
              </> : <p className="text-[12px] text-muted-foreground">{t("usage.noActivity")}</p>}
              <section className="border-t border-border pt-3"><dl>
                <UsageMetric label="usage.created">{usageTimestamp(data.created_at, locale)}</UsageMetric>
                <UsageMetric label="usage.updated">{usageTimestamp(data.updated_at, locale)}</UsageMetric>
                <UsageMetric label="usage.firstActivity">{usageTimestamp(data.totals.first_activity_at, locale)}</UsageMetric>
                <UsageMetric label="usage.lastActivity">{usageTimestamp(data.totals.last_activity_at, locale)}</UsageMetric>
                {data.latest_job_state ? <UsageMetric label="usage.latestJob">{runtimeStateLabel(t, data.latest_job_state)}</UsageMetric> : null}
              </dl></section>
            </> : null}
          </>}
        </div>
      </>}
    </div>
  </div>;
}
