"use client";

import { useEffect, useId, useState } from "react";
import type { decodeAvailability, decodeUsage, decodeUsageBuckets, decodeUsageRetries } from "../contracts";
import type { HttpCanonicalControlClient } from "../runtime/control-client";
import { useI18n } from "../i18n";
import { summarizeReliability, type ReliabilitySummary } from "./reliability-chart-data";
import { StackedBarChart } from "./stacked-bar-chart";

const DAY_US = 86_400_000_000;
const select = "rounded-md border border-input bg-background px-3 py-2 text-sm";
type Bucket = "day" | "week";
type ChartData = ReliabilitySummary & { fromUs: number; untilUs: number; offset: number; partial: boolean };

const errorsColor = "#ef4444";
const retriesColor = "#f59e0b";
const cacheReadsColor = "#22c55e";
const cacheWritesColor = "#3b82f6";
const ttfbColor = "#8b5cf6";
const durationColor = "#14b8a6";

export function ReliabilityChart({ client, reloadKey }: { client: HttpCanonicalControlClient; reloadKey: number }) {
  const [days, setDays] = useState(30);
  const [bucket, setBucket] = useState<Bucket>("day");
  const id = useId();
  const { t } = useI18n();
  return (
    <section className="min-w-0 rounded-lg border border-border bg-card p-4" aria-labelledby={`${id}-title`}>
      <div className="mb-1 flex flex-wrap items-center justify-between gap-3">
        <h3 id={`${id}-title`} className="font-medium">{t("observations.reliabilityTitle")}</h3>
        <div className="flex flex-wrap gap-2">
          <label className="sr-only" htmlFor={`${id}-period`}>{t("observations.timeRange")}</label>
          <select id={`${id}-period`} className={select} value={days} onChange={(event) => setDays(Number(event.target.value))}>
            <option value={7}>{t("observations.last7")}</option>
            <option value={30}>{t("observations.last30")}</option>
            <option value={90}>{t("observations.last90")}</option>
          </select>
          <label className="sr-only" htmlFor={`${id}-bucket`}>{t("observations.bucketWidth")}</label>
          <select id={`${id}-bucket`} className={select} value={bucket} onChange={(event) => setBucket(event.target.value === "week" ? "week" : "day")}>
            <option value="day">{t("observations.day")}</option>
            <option value="week">{t("observations.week")}</option>
          </select>
        </div>
      </div>
      <p className="mb-5 text-sm text-muted-foreground">{t("observations.reliabilityDescription")}</p>
      <ReliabilityChartData key={`${days}:${bucket}:${reloadKey}`} client={client} days={days} bucket={bucket} />
    </section>
  );
}

function ReliabilityChartData({ client, days, bucket }: { client: HttpCanonicalControlClient; days: number; bucket: Bucket }) {
  const [result, setResult] = useState<{ data: ChartData | null; error: string | null }>({ data: null, error: null });
  const { t } = useI18n();
  useEffect(() => {
    const controller = new AbortController();
    const { signal } = controller;
    async function load() {
      try {
        const offset = Math.round(-new Date().getTimezoneOffset() / 60) * 60;
        const offsetUs = offset * 60_000_000;
        const until = (Math.floor((Date.now() * 1000 + offsetUs) / DAY_US) + 1) * DAY_US - offsetUs;
        let from = until - days * DAY_US;
        let firstFrom = from;
        let lastUntil = from;
        let bucketsPartial = false;
        const rows: ReturnType<typeof decodeUsageBuckets>["rows"] = [];
        const retryRows: ReturnType<typeof decodeUsageRetries>["retry_rows"] = [];
        for (let pageNumber = 0; pageNumber < 10; pageNumber++) {
          const page = await client.readUsageBuckets({ fromUs: from, untilUs: until, bucket, tzOffsetMinutes: offset, signal });
          if (signal.aborted) return;
          if (pageNumber === 0) firstFrom = page.from_us;
          lastUntil = page.until_us;
          rows.push(...page.rows);
          bucketsPartial = !page.complete;
          if (page.complete || page.resume_from_us === null) break;
          from = page.resume_from_us;
        }
        let retriesPartial = false;
        let retryAfter: string | undefined;
        for (let pageNumber = 0; pageNumber < 10; pageNumber++) {
          const page = await client.readUsageRetries({ fromUs: firstFrom, untilUs: lastUntil, bucket, tzOffsetMinutes: offset, after: retryAfter, signal });
          if (signal.aborted) return;
          retryRows.push(...page.retry_rows);
          retriesPartial = page.next_after !== null;
          if (!page.next_after) break;
          if (page.next_after === retryAfter) throw new Error("Retry cursor did not advance.");
          retryAfter = page.next_after;
        }
        const availability: ReturnType<typeof decodeAvailability>["observations"] = [];
        let availabilityPartial = false;
        let availabilityAfter: string | undefined;
        for (let pageNumber = 0; pageNumber < 10; pageNumber++) {
          const page = await client.readAvailability({ fromUs: firstFrom, untilUs: lastUntil, after: availabilityAfter, limit: 100, signal });
          if (signal.aborted) return;
          availability.push(...page.observations);
          availabilityPartial = page.next_after !== null;
          if (!page.next_after) break;
          if (page.next_after === availabilityAfter) throw new Error("Availability cursor did not advance.");
          availabilityAfter = page.next_after;
        }
        const observations: ReturnType<typeof decodeUsage>["observations"] = [];
        let observationsPartial = false;
        let after: string | undefined;
        for (let pageNumber = 0; pageNumber < 10; pageNumber++) {
          const page = await client.readUsage({ fromUs: firstFrom, untilUs: lastUntil, after, limit: 100, signal });
          if (signal.aborted) return;
          observations.push(...page.observations);
          observationsPartial = page.next_after !== null;
          if (!page.next_after) break;
          if (page.next_after === after) throw new Error("Usage cursor did not advance.");
          after = page.next_after;
        }
        if (!signal.aborted) {
          setResult({
            data: {
              ...summarizeReliability(rows, retryRows, observations, firstFrom, lastUntil, bucket === "day" ? DAY_US : 7 * DAY_US, availability),
              fromUs: firstFrom,
              untilUs: lastUntil,
              offset,
              partial: bucketsPartial || observationsPartial || retriesPartial || availabilityPartial,
            },
            error: null,
          });
        }
      } catch (error) {
        if (!signal.aborted) setResult({ data: null, error: error instanceof Error ? error.message : String(error) });
      }
    }
    void load();
    return () => controller.abort();
  }, [client, days, bucket]);
  if (result.error) return <p role="alert" className="text-sm text-destructive">{result.error}</p>;
  if (!result.data) return <p role="status">{t("common.loading")}</p>;
  const empty = result.data.totalRequests === 0 && result.data.totalRetries === 0
    && result.data.ttfbCount === 0 && result.data.durationCount === 0;
  return <>
    {result.data.partial ? <p role="status" className="mb-3 text-sm text-muted-foreground">{t("observations.reliabilityPartial")}</p> : null}
    {empty ? <p className="py-4 text-sm text-muted-foreground">{t("observations.reliabilityNoData")}</p> : <ReliabilityPlot data={result.data} />}
  </>;
}

function ReliabilityPlot({ data }: { data: ChartData }) {
  const { t, locale } = useI18n();
  const number = new Intl.NumberFormat(locale);
  const percent = new Intl.NumberFormat(locale, { style: "percent", minimumFractionDigits: 1, maximumFractionDigits: 1 });
  const ms = new Intl.NumberFormat(locale, { maximumFractionDigits: 1 });
  const date = new Intl.DateTimeFormat(locale, { year: "numeric", month: "short", day: "numeric", timeZone: "UTC" });
  const dateLabel = (us: number) => date.format(new Date((us + data.offset * 60_000_000) / 1000));
  const errorShare = data.totalRequests > 0 ? data.totalErrors / data.totalRequests : null;
  const stats: [string, string][] = [
    [t("observations.requests"), number.format(data.totalRequests)],
    [t("observations.reliabilityErrors"), errorShare === null
      ? `${number.format(data.totalErrors)}`
      : `${number.format(data.totalErrors)} (${percent.format(errorShare)})`],
    [t("observations.reliabilityRetries"), number.format(data.totalRetries)],
    [t("observations.reliabilityCacheReads"), number.format(data.totalCacheReads)],
    [t("observations.reliabilityCacheWrites"), number.format(data.totalCacheWrites)],
    [t("observations.reliabilityAvgTtfb"), data.ttfbAvgMs === null ? "—" : `${ms.format(data.ttfbAvgMs)} ms`],
    [t("observations.reliabilityAvgDuration"), data.durationAvgMs === null ? "—" : `${ms.format(data.durationAvgMs)} ms`],
  ];
  return <div className="flex flex-col gap-6">
    <dl className="grid gap-3 sm:grid-cols-3 lg:grid-cols-4">
      {stats.map(([label, value]) => (
        <div key={label} className="rounded-md border border-border px-3 py-2">
          <dt className="text-xs text-muted-foreground">{label}</dt>
          <dd className="mt-1 text-lg font-medium tabular-nums">{value}</dd>
        </div>
      ))}
    </dl>
    <div>
      <h4 className="mb-3 text-sm font-medium">{t("observations.reliabilityErrors")}</h4>
      <StackedBarChart metricLabel={t("observations.requests")} integerTicks
        ariaLabel={t("observations.reliabilityErrorsAria", {
          from: dateLabel(data.fromUs),
          until: dateLabel(data.untilUs - 1),
          errors: number.format(data.totalErrors),
          retries: number.format(data.totalRetries),
        })}
        series={[
          { key: "errors", label: t("observations.reliabilityErrors"), color: errorsColor, share: data.totalErrors + data.totalRetries ? data.totalErrors / (data.totalErrors + data.totalRetries) : 0 },
          { key: "retries", label: t("observations.reliabilityRetries"), color: retriesColor, share: data.totalErrors + data.totalRetries ? data.totalRetries / (data.totalErrors + data.totalRetries) : 0 },
        ]}
        bars={data.buckets.map((bar) => ({
          key: String(bar.bucketUs),
          label: dateLabel(bar.bucketUs),
          values: [bar.errors, bar.retries],
          tooltip: [dateLabel(bar.bucketUs),
            `${t("observations.requests")}: ${number.format(bar.requests)}`,
            `${t("observations.reliabilityErrors")}: ${number.format(bar.errors)}`,
            `${t("observations.reliabilityRetries")}: ${number.format(bar.retries)}`,
          ].join("\n"),
        }))} />
    </div>
    <div>
      <h4 className="mb-3 text-sm font-medium">{t("observations.tokens")}</h4>
      <StackedBarChart metricLabel={t("observations.tokens")} integerTicks
        ariaLabel={t("observations.reliabilityCacheAria", {
          from: dateLabel(data.fromUs),
          until: dateLabel(data.untilUs - 1),
          reads: number.format(data.totalCacheReads),
          writes: number.format(data.totalCacheWrites),
        })}
        series={[
          { key: "hits", label: t("observations.reliabilityCacheReads"), color: cacheReadsColor, share: data.totalCacheReads + data.totalCacheWrites ? data.totalCacheReads / (data.totalCacheReads + data.totalCacheWrites) : 0 },
          { key: "miss", label: t("observations.reliabilityCacheWrites"), color: cacheWritesColor, share: data.totalCacheReads + data.totalCacheWrites ? data.totalCacheWrites / (data.totalCacheReads + data.totalCacheWrites) : 0 },
        ]}
        bars={data.buckets.map((bar) => ({
          key: String(bar.bucketUs),
          label: dateLabel(bar.bucketUs),
          values: [bar.cacheReads, bar.cacheWrites],
          tooltip: [dateLabel(bar.bucketUs),
            `${t("observations.reliabilityCacheReads")}: ${number.format(bar.cacheReads)}`,
            `${t("observations.reliabilityCacheWrites")}: ${number.format(bar.cacheWrites)}`,
          ].join("\n"),
        }))} />
    </div>
    <div>
      <h4 className="mb-3 text-sm font-medium">{t("observations.firstByte")}</h4>
      <LatencyBars data={data} />
    </div>
  </div>;
}

function LatencyBars({ data }: { data: ChartData }) {
  const { t, locale } = useI18n();
  const [activeBar, setActiveBar] = useState<number | null>(null);
  const compact = new Intl.NumberFormat(locale, { notation: "compact", maximumFractionDigits: 1 });
  const number = new Intl.NumberFormat(locale, { maximumFractionDigits: 1 });
  const date = new Intl.DateTimeFormat(locale, { year: "numeric", month: "short", day: "numeric", timeZone: "UTC" });
  const dateLabel = (us: number) => date.format(new Date((us + data.offset * 60_000_000) / 1000));
  const largest = Math.max(0, ...data.buckets.map((bar) => Math.max(bar.ttfbAvgMs ?? 0, bar.durationAvgMs ?? 0)));
  if (largest === 0 || data.buckets.length === 0) return null;
  const top = 16, plotWidth = 660, plotHeight = 200;
  const left = 60;
  const step = plotWidth / data.buckets.length;
  const barWidth = Math.max(2, step * 0.32);
  const active = activeBar === null ? undefined : data.buckets[activeBar];
  const tooltip = (index: number) => {
    const bar = data.buckets[index];
    return [dateLabel(bar.bucketUs),
      `${t("observations.reliabilityAvgTtfb")}: ${bar.ttfbAvgMs === null ? "—" : `${number.format(bar.ttfbAvgMs)} ms`}`,
      `${t("observations.reliabilityAvgDuration")}: ${bar.durationAvgMs === null ? "—" : `${number.format(bar.durationAvgMs)} ms`}`,
    ].join("\n");
  };
  return <div className="grid min-w-0 gap-6 lg:grid-cols-[minmax(0,1fr)_16rem]">
    <div className="relative min-w-0">
      <p className="mb-2 text-xs text-muted-foreground">ms</p>
      <svg viewBox="0 0 740 260" width="100%" role="img"
        aria-label={t("observations.reliabilityLatencyAria", {
          from: dateLabel(data.fromUs),
          until: dateLabel(data.untilUs - 1),
          ttfb: data.ttfbAvgMs === null ? "—" : `${number.format(data.ttfbAvgMs)} ms`,
          duration: data.durationAvgMs === null ? "—" : `${number.format(data.durationAvgMs)} ms`,
        })}>
        {[0, 1, 2, 3].map((tick) => {
          const y = top + plotHeight - plotHeight * tick / 3;
          return <g key={tick}>
            <line x1={left} x2={left + plotWidth} y1={y} y2={y} stroke="var(--border)" />
            <text x={left - 8} y={y + 4} textAnchor="end" fill="var(--muted-foreground)" className="text-[24px] sm:text-[11px]">{compact.format(largest * tick / 3)}</text>
          </g>;
        })}
        {data.buckets.map((bar, index) => (
          <g key={bar.bucketUs} tabIndex={0} aria-label={tooltip(index)}
            onMouseEnter={() => setActiveBar(index)} onMouseLeave={() => setActiveBar(null)}
            onFocus={() => setActiveBar(index)} onBlur={() => setActiveBar(null)}
            onKeyDown={(event) => { if (event.key === "Escape") setActiveBar(null); }}
            className="outline-none focus-visible:stroke-foreground">
            <title>{tooltip(index)}</title>
            <rect x={left + index * step} y={top} width={step} height={plotHeight} fill="transparent" />
            {bar.ttfbAvgMs !== null ? <rect
              x={left + index * step + step * 0.1} y={top + plotHeight - bar.ttfbAvgMs / largest * plotHeight}
              width={barWidth} height={Math.max(1, bar.ttfbAvgMs / largest * plotHeight)} fill={ttfbColor} /> : null}
            {bar.durationAvgMs !== null ? <rect
              x={left + index * step + step * 0.1 + barWidth + 2} y={top + plotHeight - bar.durationAvgMs / largest * plotHeight}
              width={barWidth} height={Math.max(1, bar.durationAvgMs / largest * plotHeight)} fill={durationColor} /> : null}
          </g>
        ))}
        <text x={left} y={top + plotHeight + 26} fill="var(--muted-foreground)" className="text-[24px] sm:text-[11px]">{dateLabel(data.buckets[0].bucketUs)}</text>
        <text x={left + plotWidth} y={top + plotHeight + 26} textAnchor="end" fill="var(--muted-foreground)" className="text-[24px] sm:text-[11px]">{dateLabel(data.buckets[data.buckets.length - 1].bucketUs)}</text>
      </svg>
      {active ? <div role="tooltip" className="pointer-events-none absolute left-16 top-8 z-10 max-h-64 max-w-[calc(100%-4rem)] overflow-y-auto whitespace-pre-line rounded-md border border-border bg-popover px-3 py-2 text-xs text-popover-foreground shadow-md">{tooltip(data.buckets.indexOf(active))}</div> : null}
    </div>
    <ol className="min-w-0 space-y-3 text-sm">
      <li className="flex items-center gap-2">
        <span className="h-3 w-3 shrink-0 rounded-sm" style={{ backgroundColor: ttfbColor }} />
        <span className="min-w-0 flex-1 break-words">{t("observations.reliabilityAvgTtfb")}</span>
      </li>
      <li className="flex items-center gap-2">
        <span className="h-3 w-3 shrink-0 rounded-sm" style={{ backgroundColor: durationColor }} />
        <span className="min-w-0 flex-1 break-words">{t("observations.reliabilityAvgDuration")}</span>
      </li>
    </ol>
  </div>;
}
