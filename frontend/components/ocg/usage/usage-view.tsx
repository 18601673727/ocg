"use client";

import type { ReactNode } from "react";
import { RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";
import { PageSurface, SegmentedTabs } from "../primitives";
import { STATUS_TEXT } from "../primitives/tone";
import type { UsageTotals, UsageWindow } from "../contracts";
import { useI18n, type I18nKey } from "../i18n";
import { ContextMetrics, CostDetails, TokenMetrics, UsageMoney, UsageNumber, UsageMetric } from "./usage-values";

const WINDOWS: readonly { id: UsageWindow; label: I18nKey }[] = [
  { id: "today", label: "usage.today" }, { id: "7d", label: "usage.sevenDays" },
  { id: "30d", label: "usage.thirtyDays" }, { id: "all", label: "usage.allTime" },
];

export function UsageSection({ title, children }: { title: string; children: ReactNode }) {
  return <section className="min-w-0 rounded-md border border-border p-3 sm:p-4">
    <h2 className="mb-2 text-xs font-semibold tracking-tight">{title}</h2>{children}
  </section>;
}

function BreakdownTable({ rows, models = false }: { rows: readonly UsageViewBreakdown[]; models?: boolean }) {
  const { t } = useI18n();
  return <div className="overflow-x-auto"><table className="w-full text-left text-xs">
    <thead className="text-xs text-muted-foreground"><tr>
      <th className="pb-2 font-medium">{t(models ? "usage.providerModel" : "usage.providers")}</th>
      {(["usage.requests", "usage.input", "usage.output", "usage.total", "usage.cost"] as const).map(key => <th key={key} className="pb-2 pl-3 text-right font-medium">{t(key)}</th>)}
    </tr></thead>
    <tbody>{rows.map((row, index) => <tr key={`${row.provider}:${row.model}:${index}`} className="border-t border-border/60">
      <td className="max-w-56 break-all py-2.5 pr-3 font-medium">{row.provider ?? t("common.notReported")}{models ? <span className="block font-normal text-muted-foreground">{row.model ?? t("common.notReported")}</span> : null}</td>
      <td className="pl-3 text-right"><UsageNumber quantity={row.totals.provider_requests} /></td>
      <td className="pl-3 text-right"><UsageNumber quantity={row.totals.tokens.input} /></td>
      <td className="pl-3 text-right"><UsageNumber quantity={row.totals.tokens.output} /></td>
      <td className="pl-3 text-right"><UsageNumber quantity={row.totals.tokens.total} /></td>
      <td className="pl-3 text-right"><UsageMoney cost={row.totals.cost} /></td>
    </tr>)}</tbody>
  </table></div>;
}

export type UsageViewTotals = Pick<UsageTotals, "tokens" | "cost" | "context_costs" | "provider_requests">;
export type UsageViewBreakdown = {
  provider: string | null;
  model: string | null;
  totals: Pick<UsageViewTotals, "tokens" | "cost" | "provider_requests">;
};
export type UsageViewData = {
  totals: UsageViewTotals;
  providers: readonly UsageViewBreakdown[];
  models: readonly UsageViewBreakdown[];
  truncated: boolean;
  hasRecordedUsage: boolean;
};
export type UsageViewProps = {
  data: UsageViewData | null;
  loading: boolean;
  error: string | null;
  unavailable: boolean;
  window: UsageWindow;
  onWindowChange: (window: UsageWindow) => void;
  onRefresh: () => void;
  scopeNote?: ReactNode;
  activity?: ReactNode;
  children?: ReactNode;
};

export function UsageView({ data, loading, error, unavailable, window, onWindowChange, onRefresh, scopeNote, activity, children }: UsageViewProps) {
  const { t } = useI18n();
  return <PageSurface>
    <div className="mx-auto max-w-6xl space-y-5" data-usage-surface="project">
      <header className="flex flex-wrap items-start justify-between gap-3">
        <div><h1 className="text-lg font-semibold tracking-tight">{t("nav.usage")}</h1><p className="mt-1 text-xs text-muted-foreground">{t("usage.subtitle")}</p></div>
        <Button variant="outline" size="sm" disabled={unavailable || loading} onClick={onRefresh}><RefreshCw className="size-3.5" />{t("common.refresh")}</Button>
      </header>
      <div className="max-w-md"><SegmentedTabs tabs={WINDOWS.map(value => ({ id: value.id, label: t(value.label) }))} value={window} onSelect={onWindowChange} ariaLabel={t("usage.window")} className="grid-cols-4" /></div>
      {unavailable ? <p className="text-sm text-muted-foreground">{t("usage.noEndpoint")}</p> : error ? <div role="alert" className="text-sm text-destructive">{t("usage.failed", { error: error })}</div> : loading ? <p role="status" className="text-sm text-muted-foreground">{t("common.loading")}</p> : null}
      {data ? <>
        <div className="max-w-3xl">{scopeNote}</div>
        {data.truncated ? <p className={`text-xs ${STATUS_TEXT['warning']}`}>{t("usage.limited")}</p> : null}
        {!data.hasRecordedUsage ? <p className="text-xs text-muted-foreground">{t("usage.noProjectUsage")}</p> : null}
        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
          <UsageSection title={t("usage.cost")}><CostDetails cost={data.totals.cost} /></UsageSection>
          <UsageSection title={t("usage.tokens")}><TokenMetrics totals={data.totals} /></UsageSection>
          <UsageSection title={t("usage.activity")}>{activity ?? <dl><UsageMetric label="usage.requests"><UsageNumber quantity={data.totals.provider_requests} /></UsageMetric></dl>}</UsageSection>
        </div>
        <div className="grid gap-3 xl:grid-cols-2">
          <UsageSection title={t("usage.providers")}><BreakdownTable rows={data.providers} /></UsageSection>
          <UsageSection title={t("usage.models")}><BreakdownTable rows={data.models} models /></UsageSection>
        </div>
        <UsageSection title={t("usage.efficiency")}>
          <div className="max-w-xl"><ContextMetrics totals={data.totals} full /></div>
          <p className="mt-3 max-w-3xl text-xs text-muted-foreground">{t("usage.efficiencyNote")}</p>
        </UsageSection>
        {children}
      </> : null}
    </div>
  </PageSurface>;
}
