"use client";

import { useEffect, useId, useState } from "react";
import type { decodeUsageBuckets } from "../contracts";
import type { HttpCanonicalControlClient } from "../runtime/control-client";
import { useI18n } from "../i18n";
import { summarize, type RankedUsage } from "./usage-chart.ts";

const DAY_US = 86_400_000_000;
const select = "rounded-md border border-input bg-background px-3 py-2 text-sm";
type Bucket = "day" | "week";
type ChartData = ReturnType<typeof summarize> & { fromUs: number; untilUs: number; offset: number; partial: boolean };
const color = (entry: RankedUsage, rank: number) => entry.other ? "var(--usage-other)" : `var(--usage-${rank + 1})`;

export function UsageChart({ client, reloadKey }: { client: HttpCanonicalControlClient; reloadKey: number }) {
  const [days, setDays] = useState(90);
  const [bucket, setBucket] = useState<Bucket>("day");
  const id = useId();
  const { t } = useI18n();
  return (
    <section className="min-w-0 rounded-lg border border-border bg-card p-4" aria-labelledby={`${id}-title`}>
      <div className="mb-5 flex flex-wrap items-center justify-between gap-3">
        <h3 id={`${id}-title`} className="font-medium">{t("observations.chartTitle")}</h3>
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
      <UsageChartData key={`${days}:${bucket}:${reloadKey}`} client={client} days={days} bucket={bucket} />
    </section>
  );
}

function UsageChartData({ client, days, bucket }: { client: HttpCanonicalControlClient; days: number; bucket: Bucket }) {
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
        let partial = false;
        const rows: ReturnType<typeof decodeUsageBuckets>["rows"] = [];
        for (let pageNumber = 0; pageNumber < 10; pageNumber++) {
          const page = await client.readUsageBuckets({ fromUs: from, untilUs: until, bucket, tzOffsetMinutes: offset, signal });
          if (signal.aborted) return;
          if (pageNumber === 0) firstFrom = page.from_us;
          lastUntil = page.until_us;
          rows.push(...page.rows);
          partial = !page.complete;
          if (page.complete || page.resume_from_us === null) break;
          from = page.resume_from_us;
        }
        if (!signal.aborted) {
          setResult({ data: { ...summarize(rows, firstFrom, lastUntil, bucket === "day" ? DAY_US : 7 * DAY_US), fromUs: firstFrom, untilUs: lastUntil, offset, partial }, error: null });
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
  return <>
    {result.data.partial ? <p role="status" className="mb-3 text-sm text-muted-foreground">{t("observations.usagePartial")}</p> : null}
    {result.data.total === 0 ? <p className="py-4 text-sm text-muted-foreground">{t("observations.noUsage")}</p> : <UsagePlot data={result.data} />}
  </>;
}

function UsagePlot({ data }: { data: ChartData }) {
  const { t, locale } = useI18n();
  const [activeBar, setActiveBar] = useState<number | null>(null);
  const compact = new Intl.NumberFormat(locale, { notation: "compact", maximumFractionDigits: 1 });
  const number = new Intl.NumberFormat(locale);
  const percent = new Intl.NumberFormat(locale, { style: "percent", minimumFractionDigits: 1, maximumFractionDigits: 1 });
  const date = new Intl.DateTimeFormat(locale, { year: "numeric", month: "short", day: "numeric", timeZone: "UTC" });
  const dateLabel = (us: number) => date.format(new Date((us + data.offset * 60_000_000) / 1000));
  const name = (entry: RankedUsage) => entry.other ? t("observations.other") : entry.unknown ? t("observations.unknownModel") : entry.model ?? "";
  const tooltip = (bar: ChartData["bars"][number]) => [
    dateLabel(bar.bucketUs),
    `${t("observations.tokens")}: ${number.format(bar.total)}`,
    ...[...bar.totals].filter(([, value]) => value > 0).map(([model, value]) => `${model ?? t("observations.unknownModel")}: ${number.format(value)}`),
  ].join("\n");
  const maximum = Math.max(...data.bars.map((bar) => bar.total));
  const left = 60, top = 16, plotWidth = 660, plotHeight = 230;
  const step = plotWidth / data.bars.length;
  const last = data.bars[data.bars.length - 1];
  return (
    <div className="grid min-w-0 gap-6 lg:grid-cols-[minmax(0,1fr)_16rem]">
      <div className="relative min-w-0">
        <p className="mb-2 text-xs text-muted-foreground">{t("observations.tokens")}</p>
        <svg viewBox="0 0 740 290" width="100%" role="img" aria-label={t("observations.usageAria", { from: dateLabel(data.fromUs), until: dateLabel(data.untilUs - 1), tokens: number.format(data.total) })}>
          {[0, 1, 2, 3].map((tick) => {
            const y = top + plotHeight - plotHeight * tick / 3;
            return <g key={tick}>
              <line x1={left} x2={left + plotWidth} y1={y} y2={y} stroke="var(--border)" />
              <text x={left - 8} y={y + 4} textAnchor="end" fill="var(--muted-foreground)" className="text-[24px] sm:text-[11px]">{compact.format(maximum * tick / 3)}</text>
            </g>;
          })}
          {data.bars.map((bar, index) => {
            let stacked = 0;
            const label = tooltip(bar);
            return <g key={bar.bucketUs} tabIndex={0} aria-label={label}
              onMouseEnter={() => setActiveBar(index)} onMouseLeave={() => setActiveBar(null)}
              onFocus={() => setActiveBar(index)} onBlur={() => setActiveBar(null)}
              onKeyDown={(event) => { if (event.key === "Escape") setActiveBar(null); }}
              className="outline-none focus-visible:stroke-foreground">
              <title>{label}</title>
              <rect x={left + index * step} y={top} width={step} height={plotHeight} fill="transparent" />
              {data.ranking.map((entry, rank) => {
                const value = entry.keys.reduce((sum, key) => sum + (bar.totals.get(key) ?? 0), 0);
                stacked += value;
                return value > 0 ? <rect key={rank} x={left + index * step + step * 0.1} y={top + plotHeight - stacked / maximum * plotHeight}
                  width={step * 0.8} height={value / maximum * plotHeight} fill={color(entry, rank)} /> : null;
              })}
            </g>;
          })}
          <text x={left} y={top + plotHeight + 26} fill="var(--muted-foreground)" className="text-[24px] sm:text-[11px]">{dateLabel(data.bars[0].bucketUs)}</text>
          <text x={left + plotWidth} y={top + plotHeight + 26} textAnchor="end" fill="var(--muted-foreground)" className="text-[24px] sm:text-[11px]">{dateLabel(last.bucketUs)}</text>
        </svg>
        {activeBar !== null ? <div role="tooltip" className="pointer-events-none absolute left-16 top-8 z-10 max-h-64 max-w-[calc(100%-4rem)] overflow-y-auto whitespace-pre-line rounded-md border border-border bg-popover px-3 py-2 text-xs text-popover-foreground shadow-md">{tooltip(data.bars[activeBar])}</div> : null}
      </div>
      <ol className="min-w-0 space-y-3 text-sm">
        {data.ranking.map((entry, rank) => <li key={rank} className="flex items-center gap-2">
          <span className="w-4 shrink-0 text-right text-xs text-muted-foreground">{rank + 1}</span>
          <span className="h-3 w-3 shrink-0 rounded-sm" style={{ backgroundColor: color(entry, rank) }} />
          <span className="min-w-0 flex-1 break-words">{name(entry)}</span>
          <span className="shrink-0 tabular-nums text-muted-foreground">{percent.format(entry.share)}</span>
        </li>)}
      </ol>
    </div>
  );
}
