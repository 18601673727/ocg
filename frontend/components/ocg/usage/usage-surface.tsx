"use client";

import { useState } from "react";
import { useSearchParams } from "next/navigation";
import { SegmentedTabs } from "../primitives";
import { formatCount } from "@/lib/format";
import type { UsageWindow } from "../contracts";
import { useI18n } from "../i18n";
import { useProjectUsage } from "./use-usage";
import { ActivityMetrics, hasRecordedUsage, UsageMoney, UsageNumber, UsageMetric, UsageScopeNote, usageTimestamp } from "./usage-values";
import { UsageSection as Section, UsageView } from "./usage-view";

function windowFrom(value: string | null): UsageWindow { return value === "all" || value === "today" || value === "7d" ? value : "30d"; }

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
  return <UsageView
    data={data ? {
      totals: data.totals,
      providers: data.providers,
      models: data.models,
      truncated: data.truncated,
      hasRecordedUsage: hasRecordedUsage(data.totals),
    } : null}
    loading={usage.loading}
    error={usage.error}
    unavailable={!baseUrl || !projectId}
    window={window}
    onWindowChange={selectWindow}
    onRefresh={usage.refresh}
    scopeNote={data ? <UsageScopeNote scope="project" generatedAt={data.generated_at} /> : null}
    activity={data ? <><dl>
      <UsageMetric label="usage.conversations">{formatCount(data.conversations)}</UsageMetric>
      <UsageMetric label="usage.turns">{formatCount(data.totals.turns)}</UsageMetric>
      <UsageMetric label="usage.jobs">{formatCount(data.totals.jobs)}</UsageMetric>
    </dl><ActivityMetrics totals={data.totals} /></> : null}
  >
    {data ? <>
        <Section title={t("usage.conversations")}>
          <div className="mb-3 max-w-xs"><SegmentedTabs tabs={[{ id: "recent", label: t("usage.recent") }, { id: "tokens", label: t("usage.mostTokens") }]} value={order} onSelect={setOrder} ariaLabel={t("usage.conversationOrder")} className="grid-cols-2" /></div>
          {rows.length ? <div className="overflow-x-auto"><table className="w-full text-left text-xs">
            <thead className="text-xs text-muted-foreground"><tr>
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
              <td className="whitespace-nowrap pl-3 text-xs text-muted-foreground">{usageTimestamp(row.updated_at, locale)}</td>
              <td className="pl-3 text-right tabular-nums">{formatCount(row.totals.turns)}</td>
              <td className="pl-3 text-right"><UsageNumber quantity={row.totals.provider_requests} /></td>
              <td className="pl-3 text-right"><UsageNumber quantity={row.totals.tokens.total} /></td>
              <td className="pl-3 text-right"><UsageMoney cost={row.totals.cost} /></td>
              <td className="max-w-56 break-all pl-3 text-xs text-muted-foreground">{row.providers.join(", ") || "—"}<br />{row.models.join(", ") || "—"}</td>
            </tr>)}</tbody>
          </table></div> : <p className="py-4 text-xs text-muted-foreground">{t("usage.empty")}</p>}
        </Section>
    </> : null}
  </UsageView>;
}
