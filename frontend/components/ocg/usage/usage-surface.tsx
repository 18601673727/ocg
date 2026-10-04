"use client";

import { useState, type ReactNode } from "react";
import { useSearchParams } from "next/navigation";
import { RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";
import { PageSurface, SegmentedTabs } from "../primitives";
import { formatCount } from "@/lib/format";
import type { UsageBreakdown, UsageWindow } from "../contracts";
import { useI18n, type I18nKey } from "../i18n";
import { useProjectUsage } from "./use-usage";
import { ContextMetrics, CostDetails, TokenMetrics, UsageMoney, UsageNumber, UsageMetric, usageTimestamp } from "./usage-values";

const WINDOWS: readonly { id: UsageWindow; label: I18nKey }[] = [
  { id: "today", label: "usage.today" }, { id: "7d", label: "usage.sevenDays" },
  { id: "30d", label: "usage.thirtyDays" }, { id: "all", label: "usage.allTime" },
];
function windowFrom(value: string | null): UsageWindow { return value === "all" || value === "today" || value === "7d" ? value : "30d"; }

function Section({ title, children }: { title: string; children: ReactNode }) {
  return <section className="min-w-0 rounded-md border border-border p-3 sm:p-4">
    <h2 className="mb-2 text-[12px] font-semibold tracking-tight">{title}</h2>{children}
  </section>;
}

function BreakdownTable({ rows, models = false }: { rows: UsageBreakdown[]; models?: boolean }) {
  const { t } = useI18n();
  return <div className="overflow-x-auto"><table className="w-full text-left text-[12px]">
    <thead className="text-[11px] text-muted-foreground"><tr>
      <th className="pb-2 font-medium">{t(models ? "usage.providerModel" : "usage.providers")}</th>
      {(["usage.requests", "usage.input", "usage.output", "usage.total", "usage.cost"] as const).map(key => <th key={key} className="pb-2 pl-3 text-right font-medium">{t(key)}</th>)}
    </tr></thead>
    <tbody>{rows.map((row, index) => <tr key={`${row.provider}:${row.model}:${index}`} className="border-t border-border/60">
      <td className="max-w-56 break-all py-2.5 pr-3 font-medium">{row.provider ?? "—"}{models ? <span className="block font-normal text-muted-foreground">{row.model ?? "—"}</span> : null}</td>
      <td className="pl-3 text-right"><UsageNumber quantity={row.totals.provider_requests} /></td>
      <td className="pl-3 text-right"><UsageNumber quantity={row.totals.tokens.input} /></td>
      <td className="pl-3 text-right"><UsageNumber quantity={row.totals.tokens.output} /></td>
      <td className="pl-3 text-right"><UsageNumber quantity={row.totals.tokens.total} /></td>
      <td className="pl-3 text-right"><UsageMoney cost={row.totals.cost} /></td>
    </tr>)}</tbody>
  </table></div>;
}

export function UsageSurface({ baseUrl, projectId, onSelectSession }: { baseUrl: string | null; projectId: string; onSelectSession: (id: string) => void }) {
  const { t, locale } = useI18n();
  const params = useSearchParams();
  const [window, setWindow] = useState<UsageWindow>(() => windowFrom(params.get("usage_window")));
  const [order, setOrder] = useState<"recent" | "tokens">("recent");
  const usage = useProjectUsage(baseUrl, projectId, window);
  const data = usage.data;
  const selectWindow = (next: UsageWindow) => {
    setWindow(next);
    const url = new URL(globalThis.window.location.href);
    url.searchParams.set("usage_window", next);
    globalThis.window.history.replaceState(null, "", `${url.pathname}${url.search}`);
  };
  const rows = data ? order === "recent" ? data.conversation_rows : [...data.conversation_rows].sort((a, b) => (b.totals.tokens.total.value ?? -1) - (a.totals.tokens.total.value ?? -1) || a.conversation_id.localeCompare(b.conversation_id)) : [];
  return <PageSurface>
    <div className="mx-auto max-w-6xl space-y-5" data-usage-surface="project">
      <header className="flex flex-wrap items-start justify-between gap-3">
        <div><h1 className="text-lg font-semibold tracking-tight">{t("nav.usage")}</h1><p className="mt-1 text-[12px] text-muted-foreground">{t("usage.subtitle")}</p></div>
        <Button variant="outline" size="sm" disabled={!baseUrl || usage.loading} onClick={usage.refresh}><RefreshCw className="size-3.5" />{t("common.refresh")}</Button>
      </header>
      <div className="max-w-md"><SegmentedTabs tabs={WINDOWS.map(value => ({ id: value.id, label: t(value.label) }))} value={window} onSelect={selectWindow} ariaLabel={t("usage.window")} className="grid-cols-4" /></div>
      {!baseUrl || !projectId ? <p className="text-sm text-muted-foreground">{t("usage.noEndpoint")}</p> : usage.error ? <div role="alert" className="text-sm text-destructive">{t("usage.failed", { error: usage.error })}</div> : usage.loading ? <p role="status" className="text-sm text-muted-foreground">{t("common.loading")}</p> : null}
      {data ? <>
        {data.truncated ? <p className="text-[11px] text-amber-600 dark:text-amber-400">{t("usage.limited")}</p> : null}
        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
          <Section title={t("usage.tokens")}><TokenMetrics totals={data.totals} /></Section>
          <Section title={t("usage.cost")}><CostDetails cost={data.totals.cost} /></Section>
          <Section title={t("usage.activity")}><dl>
            <UsageMetric label="usage.requests" field="requests"><UsageNumber quantity={data.totals.provider_requests} /></UsageMetric>
            <UsageMetric label="usage.rounds"><UsageNumber quantity={data.totals.provider_rounds} /></UsageMetric>
            <UsageMetric label="usage.nativeCalls" field="native_calls">{formatCount(data.totals.native_calls)}</UsageMetric>
            <UsageMetric label="usage.conversations">{formatCount(data.conversations)}</UsageMetric>
            <UsageMetric label="usage.turns">{formatCount(data.totals.turns)}</UsageMetric>
            <UsageMetric label="usage.jobs">{formatCount(data.totals.jobs)}</UsageMetric>
          </dl></Section>
          <Section title={t("usage.context")}><ContextMetrics totals={data.totals} /></Section>
        </div>
        <div className="grid gap-3 xl:grid-cols-2">
          <Section title={t("usage.providers")}><BreakdownTable rows={data.providers} /></Section>
          <Section title={t("usage.models")}><BreakdownTable rows={data.models} models /></Section>
        </div>
        <Section title={t("usage.efficiency")}>
          <div className="max-w-xl"><ContextMetrics totals={data.totals} full /></div>
          <p className="mt-3 max-w-3xl text-[11px] text-muted-foreground">{t("usage.efficiencyNote")}</p>
        </Section>
        <Section title={t("usage.conversations")}>
          <div className="mb-3 max-w-xs"><SegmentedTabs tabs={[{ id: "recent", label: t("usage.recent") }, { id: "tokens", label: t("usage.mostTokens") }]} value={order} onSelect={setOrder} ariaLabel={t("usage.conversationOrder")} className="grid-cols-2" /></div>
          {rows.length ? <div className="overflow-x-auto"><table className="w-full text-left text-[12px]">
            <thead className="text-[11px] text-muted-foreground"><tr>
              <th className="pb-2 font-medium">{t("usage.conversation")}</th>
              <th className="pb-2 pl-3 font-medium">{t("usage.updated")}</th>
              <th className="pb-2 pl-3 text-right font-medium">{t("usage.turns")}</th>
              <th className="pb-2 pl-3 text-right font-medium">{t("usage.requests")}</th>
              <th className="pb-2 pl-3 text-right font-medium">{t("usage.total")}</th>
              <th className="pb-2 pl-3 text-right font-medium">{t("usage.cost")}</th>
              <th className="pb-2 pl-3 font-medium">{t("usage.providerModel")}</th>
            </tr></thead>
            <tbody>{rows.map(row => <tr key={row.conversation_id} className="border-t border-border/60">
              <td className="max-w-64 py-3"><button className="max-w-full truncate text-left font-medium underline-offset-4 hover:underline" onClick={() => onSelectSession(row.session_id)} title={row.title ?? row.session_id}>{row.title ?? t("usage.untitled")}</button></td>
              <td className="whitespace-nowrap pl-3 text-[11px] text-muted-foreground">{usageTimestamp(row.updated_at, locale)}</td>
              <td className="pl-3 text-right tabular-nums">{formatCount(row.totals.turns)}</td>
              <td className="pl-3 text-right"><UsageNumber quantity={row.totals.provider_requests} /></td>
              <td className="pl-3 text-right"><UsageNumber quantity={row.totals.tokens.total} /></td>
              <td className="pl-3 text-right"><UsageMoney cost={row.totals.cost} /></td>
              <td className="max-w-56 break-all pl-3 text-[11px] text-muted-foreground">{row.providers.join(", ") || "—"}<br />{row.models.join(", ") || "—"}</td>
            </tr>)}</tbody>
          </table></div> : <p className="py-4 text-[12px] text-muted-foreground">{t("usage.empty")}</p>}
        </Section>
      </> : null}
    </div>
  </PageSurface>;
}
