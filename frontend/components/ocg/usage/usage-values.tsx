"use client";

import type { ReactNode } from "react";
import { formatCount, UNKNOWN } from "@/lib/format";
import type { UsageBreakdown, UsageCompleteness, UsageCost, UsageQuantity, UsageTotals } from "../contracts";
import { useI18n, type I18nKey } from "../i18n";

/** Visible completeness label; `complete` needs none. Text, not colour, carries the meaning. */
export function CompletenessTag({ completeness }: { completeness: UsageCompleteness }) {
  const { t } = useI18n();
  if (completeness === "complete") return null;
  return <span className="ml-1 text-[10px] font-normal text-muted-foreground" data-usage-completeness={completeness}>{t(completeness === "partial" ? "usage.partial" : "usage.unavailable")}</span>;
}

// An unavailable quantity carries no value: it renders as the label alone, never as 0.
export function UsageNumber({ quantity, bytes = false }: { quantity: UsageQuantity; bytes?: boolean }) {
  const { t } = useI18n();
  return <span className="tabular-nums" title={quantity.completeness === "partial" ? t("usage.partialHint") : quantity.completeness === "unavailable" ? t("usage.unknownHint") : undefined}>
    {quantity.value !== null ? `${formatCount(quantity.value)}${bytes ? " B" : ""}` : quantity.completeness === "unavailable" ? null : UNKNOWN}
    <CompletenessTag completeness={quantity.completeness} />
  </span>;
}

// Settled amounts per currency, as recorded; currencies are never converted or summed together.
export function UsageMoney({ cost }: { cost: UsageCost }) {
  const { t } = useI18n();
  return <span className="tabular-nums" title={cost.completeness === "partial" ? t("usage.partialHint") : cost.completeness === "unavailable" ? t("usage.costUnavailableHint") : t("usage.costSource")}>
    {cost.currencies.length ? cost.currencies.map(value => `${value.currency} ${(value.actual_micros / 1_000_000).toFixed(6)}`).join(" · ") : cost.completeness === "unavailable" ? null : UNKNOWN}
    <CompletenessTag completeness={cost.completeness} />
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
    {cost.settled_calls > 0 ? <p className="mt-1 text-[11px] text-muted-foreground">{t("usage.settledCalls", { count: cost.settled_calls })}</p> : null}
    {cost.unresolved_calls > 0 ? <p className="mt-1 text-[11px] text-amber-600 dark:text-amber-400">{t("usage.unresolved", { count: cost.unresolved_calls })}</p> : null}
    {cost.unavailable_calls > 0 ? <p className="mt-1 text-[11px] text-muted-foreground">{t("usage.unavailableCost", { count: cost.unavailable_calls })}</p> : null}
    <p className="mt-2 text-[10px] text-muted-foreground">{t("usage.costSource")}</p>
  </>;
}

/** Activity counters of one scope. Attempts include retries and recovery replacements. */
export function ActivityMetrics({ totals }: { totals: UsageTotals }) {
  return <dl>
    <UsageMetric label="usage.attempts" field="attempts">{formatCount(totals.attempts)}</UsageMetric>
    <UsageMetric label="usage.providerCalls" field="provider_calls">{formatCount(totals.provider_calls)}</UsageMetric>
    <UsageMetric label="usage.requests" field="requests"><UsageNumber quantity={totals.provider_requests} /></UsageMetric>
    <UsageMetric label="usage.rounds"><UsageNumber quantity={totals.provider_rounds} /></UsageMetric>
    <UsageMetric label="usage.nativeCalls" field="native_calls">{formatCount(totals.native_calls)}</UsageMetric>
  </dl>;
}

/**
 * Canonical Provider / model rows as a narrow-safe list. Each row is the
 * backend's own aggregate; rows are never summed into a scope total.
 */
export function UsageBreakdownList({ rows }: { rows: UsageBreakdown[] }) {
  const { t } = useI18n();
  if (!rows.length) return <p className="text-[11px] text-muted-foreground">{t("usage.noBreakdown")}</p>;
  return <ul className="divide-y divide-border/60">{rows.map((row, index) => <li key={`${row.provider}:${row.model}:${index}`} className="py-2" data-usage-breakdown="model">
    <p className="break-all text-[12px] font-medium">{row.provider ?? t("common.notReported")}<span className="font-normal text-muted-foreground"> / {row.model ?? t("common.notReported")}</span></p>
    <dl>
      <UsageMetric label="usage.cost"><UsageMoney cost={row.totals.cost} /></UsageMetric>
      <UsageMetric label="usage.total"><UsageNumber quantity={row.totals.tokens.total} /></UsageMetric>
      <UsageMetric label="usage.input"><UsageNumber quantity={row.totals.tokens.input} /></UsageMetric>
      <UsageMetric label="usage.output"><UsageNumber quantity={row.totals.tokens.output} /></UsageMetric>
    </dl>
  </li>)}</ul>;
}

export type UsageScope = "job" | "conversation" | "project";

const SCOPE_NOTE: Record<UsageScope, I18nKey> = {
  job: "usage.scopeJobNote",
  conversation: "usage.scopeConversationNote",
  project: "usage.scopeProjectNote",
};

/** States which canonical scope the numbers below belong to, and when they were projected. */
export function UsageScopeNote({ scope, generatedAt }: { scope: UsageScope; generatedAt?: number | null }) {
  const { t, locale } = useI18n();
  return <p className="text-[11px] leading-5 text-muted-foreground" data-usage-scope={scope}>
    {t(SCOPE_NOTE[scope])}
    {generatedAt != null ? <span className="block">{t("usage.recordedAsOf", { time: usageTimestamp(generatedAt, locale) })}</span> : null}
  </p>;
}

/**
 * A scope with no Calls has no recorded usage yet. Its counters read as a
 * complete zero on the wire, which is not something to present as spend.
 */
export function hasRecordedUsage(totals: UsageTotals): boolean {
  return totals.provider_calls > 0 || totals.native_calls > 0;
}

export function usageTimestamp(value: number | string | null, locale: string): string {
  if (value === null) return UNKNOWN;
  const time = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(time)) return UNKNOWN;
  return new Date(time * 1000).toLocaleString(locale);
}
