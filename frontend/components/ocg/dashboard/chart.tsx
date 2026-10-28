"use client";

import type * as Api from "@/components/ocg/contracts/api";

import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import { useEffect, useId, useState } from "react";
import type { HttpCanonicalControlClient } from "../runtime/control-client";
import { useI18n } from "../i18n";
import { summarize, type RankedUsage } from "./usage-chart-data";
import { summarizeReliability, type ReliabilitySummary } from "./reliability-chart-data";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

const DAY_US = 86_400_000_000;
const select = "rounded-md border border-input bg-background px-3 py-2 text-sm";
type Bucket = "day" | "week";

async function paginateCursor<Page>({
  signal,
  read,
  apply,
  next,
  done,
  partial,
  cursorError,
}: {
  signal: AbortSignal;
  read: (after: string | undefined) => Promise<Page>;
  apply: (page: Page, pageNumber: number) => void;
  next: (page: Page) => string | null;
  done: (page: Page) => boolean;
  partial: (page: Page) => boolean;
  cursorError: string;
}): Promise<boolean> {
  let after: string | undefined;
  let result = false;
  for (let pageNumber = 0; pageNumber < 10; pageNumber++) {
    const page = await read(after);
    if (signal.aborted) return false;
    apply(page, pageNumber);
    result = partial(page);
    const nextAfter = next(page);
    if (done(page) || nextAfter === null) break;
    if (nextAfter === after) throw new Error(cursorError);
    after = nextAfter;
  }
  return result;
}

function ChartControls({
  id,
  days,
  bucket,
  onDaysChange,
  onBucketChange,
}: {
  id: string;
  days: number;
  bucket: Bucket;
  onDaysChange: (days: number) => void;
  onBucketChange: (bucket: Bucket) => void;
}) {
  const { t } = useI18n();
  return (
    <div className="flex flex-wrap gap-2">
      <label className="sr-only" htmlFor={`${id}-period`}>
        {t("observations.timeRange")}
      </label>
      <Select
        value={days}
        items={[
          { value: 7, label: t("observations.last7") },
          { value: 30, label: t("observations.last30") },
          { value: 90, label: t("observations.last90") },
        ]}
        onValueChange={(next) => {
          if (next !== null) onDaysChange(next);
        }}
      >
        <SelectTrigger id={`${id}-period`} className={select}>
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value={7}>{t("observations.last7")}</SelectItem>
          <SelectItem value={30}>{t("observations.last30")}</SelectItem>
          <SelectItem value={90}>{t("observations.last90")}</SelectItem>
        </SelectContent>
      </Select>
      <label className="sr-only" htmlFor={`${id}-bucket`}>
        {t("observations.bucketWidth")}
      </label>
      <Select
        value={bucket}
        items={[
          { value: "day", label: t("observations.day") },
          { value: "week", label: t("observations.week") },
        ]}
        onValueChange={(next) => onBucketChange(next === "week" ? "week" : "day")}
      >
        <SelectTrigger id={`${id}-bucket`} className={select}>
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="day">{t("observations.day")}</SelectItem>
          <SelectItem value="week">{t("observations.week")}</SelectItem>
        </SelectContent>
      </Select>
    </div>
  );
}

type ChartBar = { key: string; label: string; values: readonly number[]; tooltip: string };
type ChartSeries = {
  key: string;
  label: string;
  color: string;
  share?: number;
  legendValue?: string;
};

function CartesianBarPlot({
  bars,
  series,
  ariaLabel,
  integerTicks = false,
  mode = "stacked",
  plotHeight = 230,
}: {
  bars: readonly ChartBar[];
  series: readonly ChartSeries[];
  ariaLabel: string;
  integerTicks?: boolean;
  mode?: "stacked" | "grouped";
  plotHeight?: number;
}) {
  const { locale } = useI18n();
  const [activeBar, setActiveBar] = useState<number | null>(null);
  const compact = new Intl.NumberFormat(locale, { notation: "compact", maximumFractionDigits: 1 });
  const percent = new Intl.NumberFormat(locale, {
    style: "percent",
    minimumFractionDigits: 1,
    maximumFractionDigits: 1,
  });
  const largest = Math.max(
    0,
    ...bars.map((bar) =>
      mode === "stacked"
        ? bar.values.reduce((sum, value) => sum + value, 0)
        : Math.max(0, ...bar.values),
    ),
  );
  if (!bars.length || largest === 0) return null;
  const maximum = integerTicks ? Math.ceil(largest / 3) * 3 : largest;
  const left = 0,
    top = 0,
    plotWidth = 720;
  const step = plotWidth / bars.length;
  const groupedBarWidth = Math.max(2, step * 0.32);
  const active = activeBar === null ? undefined : bars[activeBar];
  return (
    <div className="grid min-w-0 gap-6 lg:grid-cols-[minmax(0,1fr)_16rem]">
      <div className="relative min-w-0">
        <svg viewBox={`0 0 740 ${plotHeight + 24}`} width="100%" role="img" aria-label={ariaLabel}>
          {[0, 1, 2, 3].map((tick) => {
            const y = top + plotHeight - (plotHeight * tick) / 3;
            return (
              <g key={tick}>
                <line x1={left} x2={left + plotWidth} y1={y} y2={y} stroke="var(--border)" />
                {tick !== 0 && (
                  <text
                    x={left}
                    y={y + 3}
                    dy="1em"
                    textAnchor="start"
                    fill="var(--muted-foreground)"
                    className="text-[24px] sm:text-[11px]"
                  >
                    {compact.format((maximum * tick) / 3)}
                  </text>
                )}
              </g>
            );
          })}
          {bars.map((bar, index) => {
            let stacked = 0;
            return (
              <g
                key={bar.key}
                tabIndex={0}
                aria-label={bar.tooltip}
                onMouseEnter={() => setActiveBar(index)}
                onMouseLeave={() => setActiveBar(null)}
                onFocus={() => setActiveBar(index)}
                onBlur={() => setActiveBar(null)}
                onKeyDown={(event) => {
                  if (event.key === "Escape") setActiveBar(null);
                }}
                className="outline-none focus-visible:stroke-foreground"
              >
                <title>{bar.tooltip}</title>
                <rect
                  x={left + index * step}
                  y={top}
                  width={step}
                  height={plotHeight}
                  fill="transparent"
                />
                {series.map((entry, rank) => {
                  const value = bar.values[rank] ?? 0;
                  stacked += value;
                  const height = (value / maximum) * plotHeight;
                  return value > 0 ? (
                    <rect
                      key={entry.key}
                      x={
                        left +
                        index * step +
                        step * 0.1 +
                        (mode === "grouped" ? rank * (groupedBarWidth + 2) : 0)
                      }
                      y={
                        top +
                        plotHeight -
                        ((mode === "stacked" ? stacked : value) / maximum) * plotHeight
                      }
                      width={mode === "grouped" ? groupedBarWidth : step * 0.8}
                      height={mode === "grouped" ? Math.max(1, height) : height}
                      fill={entry.color}
                    />
                  ) : null;
                })}
              </g>
            );
          })}
          <text
            x={left}
            y={top + plotHeight + 16}
            fill="var(--muted-foreground)"
            className="text-[24px] sm:text-[11px]"
          >
            {bars[0].label}
          </text>
          <text
            x={left + plotWidth}
            y={top + plotHeight + 16}
            textAnchor="end"
            fill="var(--muted-foreground)"
            className="text-[24px] sm:text-[11px]"
          >
            {bars[bars.length - 1].label}
          </text>
        </svg>
        {active ? (
          <div
            role="tooltip"
            className="pointer-events-none absolute left-16 top-8 z-10 max-w-[calc(100%-4rem)] whitespace-pre-line rounded-md border border-border bg-popover px-3 py-2 text-xs text-popover-foreground shadow-md"
          >
            {active.tooltip}
          </div>
        ) : null}
      </div>
      <ol className="min-w-0 space-y-3 text-sm">
        {series.map((entry) => {
          const value =
            entry.legendValue ?? (entry.share === undefined ? null : percent.format(entry.share));
          return (
            <li key={entry.key} className="flex items-center gap-2">
              <span
                className="h-3 w-3 shrink-0 rounded-sm"
                style={{ backgroundColor: entry.color }}
              />
              <span className="min-w-0 flex-1 break-words">{entry.label}</span>
              {value === null ? null : (
                <span className="shrink-0 tabular-nums text-muted-foreground">{value}</span>
              )}
            </li>
          );
        })}
      </ol>
    </div>
  );
}

type UsageData = ReturnType<typeof summarize> & {
  fromUs: number;
  untilUs: number;
  offset: number;
  partial: boolean;
};
const color = (entry: RankedUsage, rank: number) =>
  entry.other ? "var(--usage-other)" : `var(--usage-${rank + 1})`;

export function UsageChart({
  client,
  reloadKey,
}: {
  client: HttpCanonicalControlClient;
  reloadKey: number;
}) {
  const [days, setDays] = useState(90);
  const [bucket, setBucket] = useState<Bucket>("day");
  const id = useId();
  const { t } = useI18n();
  return (
    <section
      className="min-w-0 rounded-lg border border-border bg-card p-4"
      aria-labelledby={`${id}-title`}
    >
      <div className="mb-5 flex flex-wrap items-center justify-between gap-3">
        <h3 id={`${id}-title`} className="font-medium">
          {t("observations.chartTitle")}
        </h3>
        <ChartControls
          id={id}
          days={days}
          bucket={bucket}
          onDaysChange={setDays}
          onBucketChange={setBucket}
        />
      </div>
      <UsageChartData
        key={`${days}:${bucket}`}
        client={client}
        days={days}
        bucket={bucket}
        reloadKey={reloadKey}
      />
    </section>
  );
}

function UsageChartData({
  client,
  days,
  bucket,
  reloadKey,
}: {
  client: HttpCanonicalControlClient;
  days: number;
  bucket: Bucket;
  reloadKey: number;
}) {
  const [result, setResult] = useState<{ data: UsageData | null; error: string | null }>({
    data: null,
    error: null,
  });
  const { t } = useI18n();
  useEffect(() => {
    const controller = new AbortController();
    const { signal } = controller;
    async function load() {
      try {
        const offset = Math.round(-new Date().getTimezoneOffset() / 60) * 60;
        const offsetUs = offset * 60_000_000;
        const until = (Math.floor((Date.now() * 1000 + offsetUs) / DAY_US) + 1) * DAY_US - offsetUs;
        const from = until - days * DAY_US;
        let firstFrom = from;
        let lastUntil = from;
        const rows: Api.Envelope<Api.UsageBuckets>["rows"] = [];
        const partial = await paginateCursor({
          signal,
          read: (after) =>
            client.readUsageBuckets({
              fromUs: from,
              untilUs: until,
              bucket,
              tzOffsetMinutes: offset,
              scope: "generative",
              after,
              signal,
            }),
          apply: (page, pageNumber) => {
            if (pageNumber === 0) firstFrom = page.from_us;
            lastUntil = page.until_us;
            rows.push(...page.rows);
          },
          next: (page) => page.resume_after,
          done: (page) => page.complete,
          partial: (page) => !page.complete,
          cursorError: "Usage cursor did not advance.",
        });
        if (!signal.aborted) {
          setResult({
            data: {
              ...summarize(rows, firstFrom, lastUntil, bucket === "day" ? DAY_US : 7 * DAY_US),
              fromUs: firstFrom,
              untilUs: lastUntil,
              offset,
              partial,
            },
            error: null,
          });
        }
      } catch (error) {
        if (!signal.aborted)
          setResult((current) => ({
            data: current.data,
            error: error instanceof Error ? error.message : String(error),
          }));
      }
    }
    void load();
    return () => controller.abort();
  }, [client, days, bucket, reloadKey]);
  if (!result.data && result.error)
    return (
      <DismissibleNotice message={result.error} className="text-sm text-destructive">
        {result.error}
      </DismissibleNotice>
    );
  if (!result.data) return <p role="status">{t("common.loading")}</p>;
  return (
    <>
      {result.data.partial ? (
        <DismissibleNotice
          tone="warning"
          message={t("observations.usagePartial")}
          className="mb-3 text-sm text-muted-foreground"
        />
      ) : null}
      {result.data.total === 0 ? (
        <p className="py-4 text-sm text-muted-foreground">{t("observations.noUsage")}</p>
      ) : (
        <UsagePlot data={result.data} />
      )}
      {result.error ? (
        <DismissibleNotice message={result.error} className="mt-3 text-sm text-destructive">
          {result.error}
        </DismissibleNotice>
      ) : null}
    </>
  );
}

function UsagePlot({ data }: { data: UsageData }) {
  const { t, locale } = useI18n();
  const number = new Intl.NumberFormat(locale);
  const date = new Intl.DateTimeFormat(locale, {
    year: "numeric",
    month: "short",
    day: "numeric",
    timeZone: "UTC",
  });
  const dateLabel = (us: number) => date.format(new Date((us + data.offset * 60_000_000) / 1000));
  return (
    <CartesianBarPlot
      ariaLabel={t("observations.usageAria", {
        from: dateLabel(data.fromUs),
        until: dateLabel(data.untilUs - 1),
        tokens: number.format(data.total),
      })}
      series={data.ranking.map((entry, rank) => ({
        key: String(rank),
        label: entry.other
          ? t("observations.other")
          : entry.unknown
            ? t("observations.unknownModel")
            : (entry.model ?? ""),
        color: color(entry, rank),
        share: entry.share,
      }))}
      bars={data.bars.map((bar) => ({
        key: String(bar.bucketUs),
        label: dateLabel(bar.bucketUs),
        values: data.ranking.map((entry) =>
          entry.keys.reduce((sum, key) => sum + (bar.totals.get(key) ?? 0), 0),
        ),
        tooltip: [
          dateLabel(bar.bucketUs),
          `${t("observations.tokens")}: ${number.format(bar.total)}`,
          ...data.ranking.flatMap((entry) =>
            entry.keys.flatMap((model) => {
              const value = bar.totals.get(model) ?? 0;
              return value > 0
                ? [`${model ?? t("observations.unknownModel")}: ${number.format(value)}`]
                : [];
            }),
          ),
        ].join("\n"),
      }))}
    />
  );
}

type ReliabilityData = ReliabilitySummary & {
  fromUs: number;
  untilUs: number;
  offset: number;
  partial: boolean;
};

const errorsColor = "var(--destructive)";
const retriesColor = "var(--usage-3)";
const cacheReadsColor = "var(--diff-added)";
const cacheWritesColor = "var(--usage-1)";
const ttfbColor = "var(--usage-4)";
const durationColor = "var(--usage-5)";

export function ReliabilityChart({
  client,
  reloadKey,
}: {
  client: HttpCanonicalControlClient;
  reloadKey: number;
}) {
  const [days, setDays] = useState(30);
  const [bucket, setBucket] = useState<Bucket>("day");
  const id = useId();
  const { t } = useI18n();
  return (
    <section
      className="min-w-0 rounded-lg border border-border bg-card p-4"
      aria-labelledby={`${id}-title`}
    >
      <div className="mb-1 flex flex-wrap items-center justify-between gap-3">
        <h3 id={`${id}-title`} className="font-medium">
          {t("observations.reliabilityTitle")}
        </h3>
        <ChartControls
          id={id}
          days={days}
          bucket={bucket}
          onDaysChange={setDays}
          onBucketChange={setBucket}
        />
      </div>
      <p className="mb-5 text-sm text-muted-foreground">
        {t("observations.reliabilityDescription")}
      </p>
      <ReliabilityChartData
        key={`${days}:${bucket}`}
        client={client}
        days={days}
        bucket={bucket}
        reloadKey={reloadKey}
      />
    </section>
  );
}

function ReliabilityChartData({
  client,
  days,
  bucket,
  reloadKey,
}: {
  client: HttpCanonicalControlClient;
  days: number;
  bucket: Bucket;
  reloadKey: number;
}) {
  const [result, setResult] = useState<{ data: ReliabilityData | null; error: string | null }>({
    data: null,
    error: null,
  });
  const { t } = useI18n();
  useEffect(() => {
    const controller = new AbortController();
    const { signal } = controller;
    async function load() {
      try {
        const offset = Math.round(-new Date().getTimezoneOffset() / 60) * 60;
        const offsetUs = offset * 60_000_000;
        const until = (Math.floor((Date.now() * 1000 + offsetUs) / DAY_US) + 1) * DAY_US - offsetUs;
        const from = until - days * DAY_US;
        let firstFrom = from;
        let lastUntil = from;
        const rows: Api.Envelope<Api.UsageBuckets>["rows"] = [];
        const retryRows: Api.UsageRetries["retry_rows"] = [];
        const bucketsPartial = await paginateCursor({
          signal,
          read: (after) =>
            client.readUsageBuckets({
              fromUs: from,
              untilUs: until,
              bucket,
              tzOffsetMinutes: offset,
              scope: "generative",
              after,
              signal,
            }),
          apply: (page, pageNumber) => {
            if (pageNumber === 0) firstFrom = page.from_us;
            lastUntil = page.until_us;
            rows.push(...page.rows);
          },
          next: (page) => page.resume_after,
          done: (page) => page.complete,
          partial: (page) => !page.complete,
          cursorError: "Usage cursor did not advance.",
        });
        if (signal.aborted) return;
        const retriesPartial = await paginateCursor({
          signal,
          read: (after) =>
            client.readUsageRetries({
              fromUs: firstFrom,
              untilUs: lastUntil,
              bucket,
              tzOffsetMinutes: offset,
              after,
              signal,
            }),
          apply: (page) => retryRows.push(...page.retry_rows),
          next: (page) => page.next_after,
          done: (page) => page.next_after === null,
          partial: (page) => page.next_after !== null,
          cursorError: "Retry cursor did not advance.",
        });
        if (signal.aborted) return;
        const availability: Api.Availability["observations"] = [];
        const availabilityPartial = await paginateCursor({
          signal,
          read: (after) =>
            client.readAvailability({
              fromUs: firstFrom,
              untilUs: lastUntil,
              after,
              limit: 100,
              signal,
            }),
          apply: (page) => availability.push(...page.observations),
          next: (page) => page.next_after,
          done: (page) => page.next_after === null,
          partial: (page) => page.next_after !== null,
          cursorError: "Availability cursor did not advance.",
        });
        if (signal.aborted) return;
        const observations: Api.Usage["observations"] = [];
        const observationsPartial = await paginateCursor({
          signal,
          read: (after) =>
            client.readUsage({
              fromUs: firstFrom,
              untilUs: lastUntil,
              after,
              limit: 100,
              signal,
            }),
          apply: (page) => observations.push(...page.observations),
          next: (page) => page.next_after,
          done: (page) => page.next_after === null,
          partial: (page) => page.next_after !== null,
          cursorError: "Usage cursor did not advance.",
        });
        if (!signal.aborted) {
          setResult({
            data: {
              ...summarizeReliability(
                rows,
                retryRows,
                observations,
                firstFrom,
                lastUntil,
                bucket === "day" ? DAY_US : 7 * DAY_US,
                availability,
              ),
              fromUs: firstFrom,
              untilUs: lastUntil,
              offset,
              partial:
                bucketsPartial || observationsPartial || retriesPartial || availabilityPartial,
            },
            error: null,
          });
        }
      } catch (error) {
        if (!signal.aborted)
          setResult((current) => ({
            data: current.data,
            error: error instanceof Error ? error.message : String(error),
          }));
      }
    }
    void load();
    return () => controller.abort();
  }, [client, days, bucket, reloadKey]);
  if (!result.data && result.error)
    return (
      <DismissibleNotice message={result.error} className="text-sm text-destructive">
        {result.error}
      </DismissibleNotice>
    );
  if (!result.data) return <p role="status">{t("common.loading")}</p>;
  const empty =
    result.data.totalRequests === 0 &&
    result.data.totalRetries === 0 &&
    result.data.ttfbCount === 0 &&
    result.data.durationCount === 0;
  return (
    <>
      {result.data.partial ? (
        <DismissibleNotice
          tone="warning"
          message={t("observations.reliabilityPartial")}
          className="mb-3 text-sm text-muted-foreground"
        />
      ) : null}
      {empty ? (
        <p className="py-4 text-sm text-muted-foreground">{t("observations.reliabilityNoData")}</p>
      ) : (
        <ReliabilityPlot data={result.data} />
      )}
      {result.error ? (
        <DismissibleNotice message={result.error} className="mt-3 text-sm text-destructive">
          {result.error}
        </DismissibleNotice>
      ) : null}
    </>
  );
}

function ReliabilityPlot({ data }: { data: ReliabilityData }) {
  const { t, locale } = useI18n();
  const number = new Intl.NumberFormat(locale);
  const percent = new Intl.NumberFormat(locale, {
    style: "percent",
    minimumFractionDigits: 1,
    maximumFractionDigits: 1,
  });
  const ms = new Intl.NumberFormat(locale, { maximumFractionDigits: 1 });
  const date = new Intl.DateTimeFormat(locale, {
    year: "numeric",
    month: "short",
    day: "numeric",
    timeZone: "UTC",
  });
  const dateLabel = (us: number) => date.format(new Date((us + data.offset * 60_000_000) / 1000));
  const errorShare = data.totalRequests > 0 ? data.totalErrors / data.totalRequests : null;
  const cacheCount = (count: number) => (count > 0 ? number.format(count) : "—");
  const stats: [string, string][] = [
    [t("observations.requests"), number.format(data.totalRequests)],
    [
      t("observations.reliabilityErrors"),
      errorShare === null
        ? `${number.format(data.totalErrors)}`
        : `${number.format(data.totalErrors)} (${percent.format(errorShare)})`,
    ],
    [t("observations.reliabilityRetries"), number.format(data.totalRetries)],
    [t("observations.reliabilityCacheReads"), cacheCount(data.totalCacheReads)],
    [t("observations.reliabilityCacheWrites"), cacheCount(data.totalCacheWrites)],
    [
      t("observations.reliabilityAvgTtfb"),
      data.ttfbAvgMs === null ? "—" : `${ms.format(data.ttfbAvgMs)} ms`,
    ],
    [
      t("observations.reliabilityAvgDuration"),
      data.durationAvgMs === null ? "—" : `${ms.format(data.durationAvgMs)} ms`,
    ],
  ];
  return (
    <div className="flex flex-col gap-6">
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
        <CartesianBarPlot
          integerTicks
          ariaLabel={t("observations.reliabilityErrorsAria", {
            from: dateLabel(data.fromUs),
            until: dateLabel(data.untilUs - 1),
            errors: number.format(data.totalErrors),
            retries: number.format(data.totalRetries),
          })}
          series={[
            {
              key: "errors",
              label: t("observations.reliabilityErrors"),
              color: errorsColor,
              share:
                data.totalErrors + data.totalRetries
                  ? data.totalErrors / (data.totalErrors + data.totalRetries)
                  : 0,
            },
            {
              key: "retries",
              label: t("observations.reliabilityRetries"),
              color: retriesColor,
              share:
                data.totalErrors + data.totalRetries
                  ? data.totalRetries / (data.totalErrors + data.totalRetries)
                  : 0,
            },
          ]}
          bars={data.buckets.map((bar) => ({
            key: String(bar.bucketUs),
            label: dateLabel(bar.bucketUs),
            values: [bar.errors, bar.retries],
            tooltip: [
              dateLabel(bar.bucketUs),
              `${t("observations.requests")}: ${number.format(bar.requests)}`,
              `${t("observations.reliabilityErrors")}: ${number.format(bar.errors)}`,
              `${t("observations.reliabilityRetries")}: ${number.format(bar.retries)}`,
            ].join("\n"),
          }))}
        />
      </div>
      <div>
        <h4 className="mb-3 text-sm font-medium">{t("observations.tokens")}</h4>
        <p className="mb-3 text-xs text-muted-foreground">
          {t("observations.reliabilityCacheNote")}
        </p>
        <CartesianBarPlot
          integerTicks
          ariaLabel={t("observations.reliabilityCacheAria", {
            from: dateLabel(data.fromUs),
            until: dateLabel(data.untilUs - 1),
            reads: cacheCount(data.totalCacheReads),
            writes: cacheCount(data.totalCacheWrites),
          })}
          series={[
            {
              key: "hits",
              label: t("observations.reliabilityCacheReads"),
              color: cacheReadsColor,
              share: 0,
              legendValue: cacheCount(data.totalCacheReads),
            },
            {
              key: "miss",
              label: t("observations.reliabilityCacheWrites"),
              color: cacheWritesColor,
              share: 0,
              legendValue: cacheCount(data.totalCacheWrites),
            },
          ]}
          bars={data.buckets.map((bar) => ({
            key: String(bar.bucketUs),
            label: dateLabel(bar.bucketUs),
            values: [bar.cacheReads, bar.cacheWrites],
            tooltip: [
              dateLabel(bar.bucketUs),
              `${t("observations.reliabilityCacheReads")}: ${cacheCount(bar.cacheReads)}`,
              `${t("observations.reliabilityCacheWrites")}: ${cacheCount(bar.cacheWrites)}`,
            ].join("\n"),
          }))}
        />
      </div>
      <div>
        <h4 className="mb-3 text-sm font-medium">{t("observations.firstByte")}</h4>
        <LatencyBars data={data} />
      </div>
    </div>
  );
}

function LatencyBars({ data }: { data: ReliabilityData }) {
  const { t, locale } = useI18n();
  const number = new Intl.NumberFormat(locale, { maximumFractionDigits: 1 });
  const date = new Intl.DateTimeFormat(locale, {
    year: "numeric",
    month: "short",
    day: "numeric",
    timeZone: "UTC",
  });
  const dateLabel = (us: number) => date.format(new Date((us + data.offset * 60_000_000) / 1000));
  return (
    <CartesianBarPlot
      mode="grouped"
      plotHeight={200}
      ariaLabel={t("observations.reliabilityLatencyAria", {
        from: dateLabel(data.fromUs),
        until: dateLabel(data.untilUs - 1),
        ttfb: data.ttfbAvgMs === null ? "—" : `${number.format(data.ttfbAvgMs)} ms`,
        duration: data.durationAvgMs === null ? "—" : `${number.format(data.durationAvgMs)} ms`,
      })}
      series={[
        { key: "ttfb", label: t("observations.reliabilityAvgTtfb"), color: ttfbColor },
        {
          key: "duration",
          label: t("observations.reliabilityAvgDuration"),
          color: durationColor,
        },
      ]}
      bars={data.buckets.map((bar) => ({
        key: String(bar.bucketUs),
        label: dateLabel(bar.bucketUs),
        values: [bar.ttfbAvgMs ?? 0, bar.durationAvgMs ?? 0],
        tooltip: [
          dateLabel(bar.bucketUs),
          `${t("observations.reliabilityAvgTtfb")}: ${bar.ttfbAvgMs === null ? "—" : `${number.format(bar.ttfbAvgMs)} ms`}`,
          `${t("observations.reliabilityAvgDuration")}: ${bar.durationAvgMs === null ? "—" : `${number.format(bar.durationAvgMs)} ms`}`,
        ].join("\n"),
      }))}
    />
  );
}

const classifications = ["available", "unavailable", "rejected", "unconfirmed"];
const labels = [
  "observations.healthAvailable",
  "observations.healthUnavailable",
  "observations.healthRejected",
  "observations.healthUnconfirmed",
] as const;
const colors = [
  "var(--diff-added)",
  "var(--destructive)",
  "var(--usage-3)",
  "var(--muted-foreground)",
];
type HealthData = {
  bars: { fromUs: number; counts: number[] }[];
  totals: number[];
  total: number;
  partial: boolean;
};

export function HealthChart({
  client,
  fromUs,
  untilUs,
}: {
  client: HttpCanonicalControlClient;
  fromUs: number;
  untilUs: number;
}) {
  const { t, locale } = useI18n();
  const id = useId();
  const [result, setResult] = useState<{ data: HealthData | null; error: string | null }>({
    data: null,
    error: null,
  });
  const widthUs = untilUs - fromUs <= DAY_US ? DAY_US / 24 : DAY_US;
  useEffect(() => {
    const controller = new AbortController();
    const { signal } = controller;
    async function load() {
      try {
        const bars = Array.from(
          { length: Math.ceil((untilUs - fromUs) / widthUs) },
          (_, index) => ({ fromUs: fromUs + index * widthUs, counts: [0, 0, 0, 0] }),
        );
        const data: HealthData = { bars, totals: [0, 0, 0, 0], total: 0, partial: false };
        data.partial = await paginateCursor({
          signal,
          read: (after) =>
            client.readAvailability({
              fromUs,
              untilUs,
              after,
              limit: 100,
              signal,
            }),
          apply: (page) => {
            for (const row of page.observations) {
              const bar = bars[Math.floor((row.observed_at_us - fromUs) / widthUs)];
              if (!bar || row.observed_at_us >= untilUs) continue;
              const rank = classifications.indexOf(row.classification);
              if (rank < 0) throw new Error("Unsupported availability classification.");
              bar.counts[rank]++;
              data.totals[rank]++;
              data.total++;
            }
          },
          next: (page) => page.next_after,
          done: (page) => page.next_after === null,
          partial: (page) => page.next_after !== null,
          cursorError: "Availability cursor did not advance.",
        });
        if (!signal.aborted) setResult({ data, error: null });
      } catch (error) {
        if (!signal.aborted)
          setResult({ data: null, error: error instanceof Error ? error.message : String(error) });
      }
    }
    void load();
    return () => controller.abort();
  }, [client, fromUs, untilUs, widthUs]);
  const date = new Intl.DateTimeFormat(locale, {
    month: "short",
    day: "numeric",
    ...(widthUs < DAY_US ? { hour: "2-digit", minute: "2-digit" } : {}),
  });
  const dateLabel = (us: number) => date.format(new Date(us / 1000));
  const number = new Intl.NumberFormat(locale);
  const data = result.data;
  return (
    <section className="min-w-0 rounded-lg border border-border bg-card p-4" aria-labelledby={id}>
      <h3 id={id} className="font-medium">
        {t("observations.healthChartTitle")}
      </h3>
      <p className="mt-1 mb-5 text-sm text-muted-foreground">
        {t("observations.healthChartDescription")}
      </p>
      {result.error ? (
        <DismissibleNotice message={result.error} className="text-sm text-destructive">
          {result.error}
        </DismissibleNotice>
      ) : !data ? (
        <p role="status">{t("common.loading")}</p>
      ) : (
        <>
          {data.partial && (
            <DismissibleNotice
              tone="warning"
              message={t("observations.availabilityPartial")}
              className="mb-3 text-sm text-muted-foreground"
            />
          )}
          {!data.total ? (
            <p className="py-4 text-sm text-muted-foreground">{t("observations.noAvailability")}</p>
          ) : (
            <CartesianBarPlot
              integerTicks
              ariaLabel={t("observations.healthChartAria", {
                from: dateLabel(fromUs),
                until: dateLabel(untilUs),
                count: number.format(data.total),
              })}
              series={classifications.map((key, rank) => ({
                key,
                label: t(labels[rank]),
                color: colors[rank],
                share: data.totals[rank] / data.total,
              }))}
              bars={data.bars.map((bar) => ({
                key: String(bar.fromUs),
                label: dateLabel(bar.fromUs),
                values: bar.counts,
                tooltip: [
                  dateLabel(bar.fromUs),
                  `${t("observations.requests")}: ${number.format(bar.counts.reduce((sum, count) => sum + count, 0))}`,
                  ...labels.map((label, rank) => `${t(label)}: ${number.format(bar.counts[rank])}`),
                ].join("\n"),
              }))}
            />
          )}
        </>
      )}
    </section>
  );
}
