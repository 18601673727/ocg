"use client";

import type { ReactNode } from "react";
import { formatCount, UNKNOWN } from "@/lib/format";
import type { UsageCost, UsageQuantity, UsageTotals } from "../contracts";
import { useI18n, type I18nKey } from "../i18n";

export function UsageNumber({ quantity, bytes = false }: { quantity: UsageQuantity; bytes?: boolean }) {
  const { t } = useI18n();
  return <span className="tabular-nums" title={quantity.completeness === "partial" ? t("usage.partialHint") : quantity.completeness === "unavailable" ? t("usage.unknownHint") : undefined}>
    {formatCount(quantity.value)}{bytes && quantity.value !== null ? " B" : ""}
    {quantity.completeness === "partial" ? <span className="ml-1 text-[10px] font-normal text-muted-foreground">{t("usage.partial")}</span> : null}
  </span>;
}

export function UsageMoney({ cost }: { cost: UsageCost }) {
  const { t } = useI18n();
  return <span className="tabular-nums" title={cost.completeness === "partial" ? t("usage.partialHint") : t("usage.costSource")}>
    {cost.currencies.length ? cost.currencies.map(value => `${value.currency} ${(value.actual_micros / 1_000_000).toFixed(6)}`).join(" · ") : UNKNOWN}
    {cost.completeness === "partial" ? <span className="ml-1 text-[10px] font-normal text-muted-foreground">{t("usage.partial")}</span> : null}
  </span>;
}

export function UsageMetric({ label, children, field }: { label: I18nKey; children: ReactNode; field?: string }) {
  const { t } = useI18n();
  return <div className="flex min-w-0 items-baseline justify-between gap-3 py-1.5" data-usage-field={field}>
    <dt className="text-[11px] text-muted-foreground">{t(label)}</dt>
    <dd className="m-0 break-words text-right text-[12px] font-medium tabular-nums">{children}</dd>
  </div>;
}

export function TokenMetrics({ totals }: { totals: UsageTotals }) {
  return <dl>
    <UsageMetric label="usage.total" field="tokens.total"><UsageNumber quantity={totals.tokens.total} /></UsageMetric>
    <UsageMetric label="usage.input" field="tokens.input"><UsageNumber quantity={totals.tokens.input} /></UsageMetric>
    <UsageMetric label="usage.output" field="tokens.output"><UsageNumber quantity={totals.tokens.output} /></UsageMetric>
    <UsageMetric label="usage.cacheRead" field="tokens.cache_read"><UsageNumber quantity={totals.tokens.cache_read} /></UsageMetric>
    <UsageMetric label="usage.cacheWrite" field="tokens.cache_write"><UsageNumber quantity={totals.tokens.cache_write} /></UsageMetric>
    {totals.tokens.reasoning.value !== null ? <UsageMetric label="usage.reasoning" field="tokens.reasoning"><UsageNumber quantity={totals.tokens.reasoning} /></UsageMetric> : null}
  </dl>;
}

export function ContextMetrics({ totals, full = false }: { totals: UsageTotals; full?: boolean }) {
  const c = totals.context_costs;
  return <dl>
    <UsageMetric label="usage.wire" field="context.wire"><UsageNumber quantity={c.wire_bytes} bytes /></UsageMetric>
    <UsageMetric label="usage.schemas" field="context.schemas"><UsageNumber quantity={c.tool_schema_bytes} bytes /></UsageMetric>
    {full ? <UsageMetric label="usage.baseline" field="context.baseline"><UsageNumber quantity={c.full_schema_baseline_bytes} bytes /></UsageMetric> : null}
    <UsageMetric label="usage.avoided" field="context.avoided"><UsageNumber quantity={c.schema_bytes_saved} bytes /></UsageMetric>
    <UsageMetric label="usage.capsule" field="context.capsule"><UsageNumber quantity={c.capsule_bytes} bytes /></UsageMetric>
    {full ? <>
      <UsageMetric label="usage.capsuleRequests"><UsageNumber quantity={c.capsule_injected_requests} /></UsageMetric>
      <UsageMetric label="usage.messages"><UsageNumber quantity={c.canonical_message_bytes} bytes /></UsageMetric>
      <UsageMetric label="usage.toolResults"><UsageNumber quantity={c.tool_result_bytes} bytes /></UsageMetric>
      <UsageMetric label="usage.cacheRead"><UsageNumber quantity={totals.tokens.cache_read} /></UsageMetric>
    </> : null}
  </dl>;
}

export function CostDetails({ cost }: { cost: UsageCost }) {
  const { t } = useI18n();
  return <>
    <dl><UsageMetric label="usage.actualSettled" field="cost.actual"><UsageMoney cost={cost} /></UsageMetric></dl>
    {cost.unresolved_calls > 0 ? <p className="mt-1 text-[11px] text-amber-600 dark:text-amber-400">{t("usage.unresolved", { count: cost.unresolved_calls })}</p> : null}
    {cost.unavailable_calls > 0 ? <p className="mt-1 text-[11px] text-muted-foreground">{t("usage.unavailableCost", { count: cost.unavailable_calls })}</p> : null}
    <p className="mt-2 text-[10px] text-muted-foreground">{t("usage.costSource")}</p>
  </>;
}

export function usageTimestamp(value: number | string | null, locale: string): string {
  if (value === null) return UNKNOWN;
  const time = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(time)) return UNKNOWN;
  return new Date(time * 1000).toLocaleString(locale);
}
