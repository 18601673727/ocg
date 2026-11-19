import { createEffect, createMemo, createSignal, createUniqueId } from "solid-js";
import type * as Api from "@/components/ocg/contracts/api";
import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Select, SelectItem } from "@/components/ui/select";
import { useI18n } from "../i18n";
import type { ApiClient } from "../runtime/api-client";
import { HoverMarquee } from "../sidebar/hover-marquee";
import { type ReliabilitySummary, summarizeReliability } from "./reliability-chart-data";
import { type RankedUsage, summarize } from "./usage-chart-data";

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

function ChartToolbar(props: {
  id: string;
  days: number;
  bucket: Bucket;
  onDaysChange: (days: number) => void;
  onBucketChange: (bucket: Bucket) => void;
}) {
  const i18n = useI18n();
  return (
    <div class="flex flex-wrap gap-2">
      <label class="sr-only" for={`${props.id}-period`}>
        {i18n.t("observations.timeRange")}
      </label>
      <Select
        value={props.days}
        items={[
          { value: 7, label: i18n.t("observations.last7") },
          { value: 30, label: i18n.t("observations.last30") },
          { value: 90, label: i18n.t("observations.last90") },
        ]}
        onValueChange={(next) => {
          if (next !== null) props.onDaysChange(next);
        }}
        id={`${props.id}-period`}
        class={select}
      >
        <SelectItem value={7}>{i18n.t("observations.last7")}</SelectItem>
        <SelectItem value={30}>{i18n.t("observations.last30")}</SelectItem>
        <SelectItem value={90}>{i18n.t("observations.last90")}</SelectItem>
      </Select>
      <label class="sr-only" for={`${props.id}-bucket`}>
        {i18n.t("observations.bucketWidth")}
      </label>
      <Select
        value={props.bucket}
        items={[
          { value: "day", label: i18n.t("observations.day") },
          { value: "week", label: i18n.t("observations.week") },
        ]}
        onValueChange={(next) => props.onBucketChange(next === "week" ? "week" : "day")}
        id={`${props.id}-bucket`}
        class={select}
      >
        <SelectItem value="day">{i18n.t("observations.day")}</SelectItem>
        <SelectItem value="week">{i18n.t("observations.week")}</SelectItem>
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

function CartesianBarPlot(props: {
  bars: readonly ChartBar[];
  series: readonly ChartSeries[];
  ariaLabel: string;
  integerTicks?: boolean;
  mode?: "stacked" | "grouped";
  plotHeight?: number;
}) {
  const i18n = useI18n();
  const [activeBar, setActiveBar] = createSignal<number | null>(null);
  const compact = new Intl.NumberFormat(i18n.locale, {
    notation: "compact",
    maximumFractionDigits: 1,
  });
  const percent = new Intl.NumberFormat(i18n.locale, {
    style: "percent",
    minimumFractionDigits: 1,
    maximumFractionDigits: 1,
  });
  const largest = createMemo(() =>
    Math.max(
      0,
      ...props.bars.map((bar) =>
        (props.mode === undefined ? "stacked" : props.mode) === "stacked"
          ? bar.values.reduce((sum, value) => sum + value, 0)
          : Math.max(0, ...bar.values),
      ),
    ),
  );
  return (
    <>
      {(() => {
        if (!props.bars.length || largest() === 0) return null;
        const maximum = createMemo(() =>
          (props.integerTicks === undefined ? false : props.integerTicks)
            ? Math.ceil(largest() / 3) * 3
            : largest(),
        );
        const left = 0,
          top = 0,
          plotWidth = 720;
        const step = createMemo(() => plotWidth / props.bars.length);
        const groupedBarWidth = createMemo(() => Math.max(2, step() * 0.32));
        const active = createMemo(() =>
          activeBar() === null ? undefined : props.bars[activeBar!()!],
        );
        return (
          <div class="grid min-w-0 gap-6 lg:grid-cols-[minmax(0,1fr)_16rem]">
            <div class="relative min-w-0">
              <svg
                viewBox={`0 0 740 ${(props.plotHeight === undefined ? 230 : props.plotHeight) + 24}`}
                width="100%"
                role="img"
                aria-label={props.ariaLabel}
              >
                {[0, 1, 2, 3].map((tick) => {
                  const y =
                    top +
                    (props.plotHeight === undefined ? 230 : props.plotHeight) -
                    ((props.plotHeight === undefined ? 230 : props.plotHeight) * tick) / 3;
                  return (
                    <g>
                      <line x1={left} x2={left + plotWidth} y1={y} y2={y} stroke="var(--border)" />
                      {tick !== 0 && (
                        <text
                          x={left}
                          y={y + 3}
                          dy="1em"
                          text-anchor="start"
                          fill="var(--muted-foreground)"
                          class="text-[24px] sm:text-[11px]"
                        >
                          {compact.format((maximum() * tick) / 3)}
                        </text>
                      )}
                    </g>
                  );
                })}
                {props.bars.map((bar, index) => {
                  let stacked = 0;
                  return (
                    <g
                      tabindex={0}
                      aria-label={bar.tooltip}
                      onMouseEnter={() => setActiveBar(index)}
                      onMouseLeave={() => setActiveBar(null)}
                      onFocus={() => setActiveBar(index)}
                      onBlur={() => setActiveBar(null)}
                      onKeyDown={(event) => {
                        if (event.key === "Escape") setActiveBar(null);
                      }}
                      class="outline-none focus-visible:stroke-foreground"
                    >
                      <title>{bar.tooltip}</title>
                      <rect
                        x={left + index * step()}
                        y={top}
                        width={step()}
                        height={props.plotHeight === undefined ? 230 : props.plotHeight}
                        fill="transparent"
                      />
                      {props.series.map((entry, rank) => {
                        const value = bar.values[rank] ?? 0;
                        stacked += value;
                        const height =
                          (value / maximum()) *
                          (props.plotHeight === undefined ? 230 : props.plotHeight);
                        return value > 0 ? (
                          <rect
                            x={
                              left +
                              index * step() +
                              step() * 0.1 +
                              ((props.mode === undefined ? "stacked" : props.mode) === "grouped"
                                ? rank * (groupedBarWidth() + 2)
                                : 0)
                            }
                            y={
                              top +
                              (props.plotHeight === undefined ? 230 : props.plotHeight) -
                              (((props.mode === undefined ? "stacked" : props.mode) === "stacked"
                                ? stacked
                                : value) /
                                maximum()) *
                                (props.plotHeight === undefined ? 230 : props.plotHeight)
                            }
                            width={
                              (props.mode === undefined ? "stacked" : props.mode) === "grouped"
                                ? groupedBarWidth()
                                : step() * 0.8
                            }
                            height={
                              (props.mode === undefined ? "stacked" : props.mode) === "grouped"
                                ? Math.max(1, height)
                                : height
                            }
                            fill={entry.color}
                          />
                        ) : null;
                      })}
                    </g>
                  );
                })}
                <text
                  x={left}
                  y={top + (props.plotHeight === undefined ? 230 : props.plotHeight) + 16}
                  fill="var(--muted-foreground)"
                  class="text-[24px] sm:text-[11px]"
                >
                  {props.bars[0].label}
                </text>
                <text
                  x={left + plotWidth}
                  y={top + (props.plotHeight === undefined ? 230 : props.plotHeight) + 16}
                  text-anchor="end"
                  fill="var(--muted-foreground)"
                  class="text-[24px] sm:text-[11px]"
                >
                  {props.bars[props.bars.length - 1].label}
                </text>
              </svg>
              {active() ? (
                <div
                  role="tooltip"
                  class="pointer-events-none absolute left-16 top-8 z-10 max-w-[calc(100%-4rem)] whitespace-pre-line rounded-md border border-border bg-popover px-3 py-2 text-xs text-popover-foreground shadow-md"
                >
                  {active()?.tooltip}
                </div>
              ) : null}
            </div>
            <div class="relative min-h-0 min-w-0">
              <div class="lg:absolute lg:inset-0">
                <ScrollArea
                  class="max-h-64 lg:h-full lg:max-h-none"
                  viewportProps={{
                    role: "region",
                    "aria-label": i18n.t("observations.chartLegend"),
                    class: "rounded-sm focus-visible:outline-2 focus-visible:outline-ring",
                    style: { "overflow-x": "hidden" },
                  }}
                  contentProps={{ class: "pr-3", style: { "min-width": `${0}px`, width: "100%" } }}
                >
                  <ol class="min-w-0 space-y-3 text-sm">
                    {props.series.map((entry) => {
                      const value =
                        entry.legendValue ??
                        (entry.share === undefined ? null : percent.format(entry.share));
                      return (
                        <li class="flex items-center gap-2">
                          <span
                            class="h-3 w-3 shrink-0 rounded-sm"
                            style={{ "background-color": entry.color }}
                          />
                          <HoverMarquee class="flex-1">{entry.label}</HoverMarquee>
                          {value === null ? null : (
                            <span class="shrink-0 tabular-nums text-muted-foreground">{value}</span>
                          )}
                        </li>
                      );
                    })}
                  </ol>
                </ScrollArea>
              </div>
            </div>
          </div>
        );
      })()}
    </>
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

export function UsageChart(props: { client: ApiClient; reloadKey: number }) {
  const [days, setDays] = createSignal(90);
  const [bucket, setBucket] = createSignal<Bucket>("day");
  const id = createUniqueId();
  const i18n = useI18n();
  return (
    <section
      class="min-w-0 rounded-xl border border-border bg-card p-4 sm:p-5"
      aria-labelledby={`${id}-title`}
    >
      <div class="mb-5 flex flex-wrap items-center justify-between gap-3">
        <h2 id={`${id}-title`} class="text-base font-semibold tracking-tight">
          {i18n.t("observations.chartTitle")}
        </h2>
        <ChartToolbar
          id={id}
          days={days()}
          bucket={bucket()}
          onDaysChange={setDays}
          onBucketChange={setBucket}
        />
      </div>
      <UsageChartData
        client={props.client}
        days={days()}
        bucket={bucket()}
        reloadKey={props.reloadKey}
      />
    </section>
  );
}

function UsageChartData(props: {
  client: ApiClient;
  days: number;
  bucket: Bucket;
  reloadKey: number;
}) {
  const [result, setResult] = createSignal<{ data: UsageData | null; error: string | null }>({
    data: null,
    error: null,
  });
  const i18n = useI18n();
  createEffect(
    () => [props.client, props.days, props.bucket, props.reloadKey],
    () => {
      const controller = new AbortController();
      const { signal } = controller;
      async function load() {
        try {
          const offset = Math.round(-new Date().getTimezoneOffset() / 60) * 60;
          const offsetUs = offset * 60_000_000;
          const until =
            (Math.floor((Date.now() * 1000 + offsetUs) / DAY_US) + 1) * DAY_US - offsetUs;
          const from = until - props.days * DAY_US;
          let firstFrom = from;
          let lastUntil = from;
          const rows: Api.Envelope<Api.UsageBuckets>["rows"] = [];
          const partial = await paginateCursor({
            signal,
            read: (after) =>
              props.client.readUsageBuckets({
                fromUs: from,
                untilUs: until,
                bucket: props.bucket,
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
                ...summarize(
                  rows,
                  firstFrom,
                  lastUntil,
                  props.bucket === "day" ? DAY_US : 7 * DAY_US,
                ),
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
    },
  );
  return (
    <>
      {(() => {
        if (!result().data && result().error)
          return (
            <DismissibleNotice message={result().error!} class="text-sm text-destructive">
              {result().error}
            </DismissibleNotice>
          );
        if (!result().data) return <p role="status">{i18n.t("common.loading")}</p>;
        return (
          <>
            {result()!.data!.partial ? (
              <DismissibleNotice
                tone="warning"
                message={i18n.t("observations.usagePartial")}
                class="mb-3 text-sm text-muted-foreground"
              />
            ) : null}
            {result()!.data!.total === 0 ? (
              <p class="py-4 text-sm text-muted-foreground">{i18n.t("observations.noUsage")}</p>
            ) : (
              <UsagePlot data={result().data!} />
            )}
            {result().error ? (
              <DismissibleNotice message={result().error!} class="mt-3 text-sm text-destructive">
                {result().error}
              </DismissibleNotice>
            ) : null}
          </>
        );
      })()}
    </>
  );
}

function UsagePlot(props: { data: UsageData }) {
  const i18n = useI18n();
  const number = new Intl.NumberFormat(i18n.locale);
  const date = new Intl.DateTimeFormat(i18n.locale, {
    year: "numeric",
    month: "short",
    day: "numeric",
    timeZone: "UTC",
  });
  const dateLabel = (us: number) =>
    date.format(new Date((us + props.data.offset * 60_000_000) / 1000));
  return (
    <CartesianBarPlot
      ariaLabel={i18n.t("observations.usageAria", {
        from: dateLabel(props.data.fromUs),
        until: dateLabel(props.data.untilUs - 1),
        tokens: number.format(props.data.total),
      })}
      series={props.data.ranking.map((entry, rank) => ({
        key: String(rank),
        label: entry.other
          ? i18n.t("observations.other")
          : entry.unknown
            ? i18n.t("observations.unknownModel")
            : (entry.model ?? ""),
        color: color(entry, rank),
        share: entry.share,
      }))}
      bars={props.data.bars.map((bar) => ({
        key: String(bar.bucketUs),
        label: dateLabel(bar.bucketUs),
        values: props.data.ranking.map((entry) =>
          entry.keys.reduce((sum, key) => sum + (bar.totals.get(key) ?? 0), 0),
        ),
        tooltip: [
          dateLabel(bar.bucketUs),
          `${i18n.t("observations.tokens")}: ${number.format(bar.total)}`,
          ...props.data.ranking.flatMap((entry) =>
            entry.keys.flatMap((model) => {
              const value = bar.totals.get(model) ?? 0;
              return value > 0
                ? [`${model ?? i18n.t("observations.unknownModel")}: ${number.format(value)}`]
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

export function ReliabilityChart(props: { client: ApiClient; reloadKey: number }) {
  const [days, setDays] = createSignal(30);
  const [bucket, setBucket] = createSignal<Bucket>("day");
  const id = createUniqueId();
  const i18n = useI18n();
  return (
    <section
      class="min-w-0 rounded-xl border border-border bg-card p-4 sm:p-5"
      aria-labelledby={`${id}-title`}
    >
      <div class="mb-1 flex flex-wrap items-center justify-between gap-3">
        <h2 id={`${id}-title`} class="text-base font-semibold tracking-tight">
          {i18n.t("observations.reliabilityTitle")}
        </h2>
        <ChartToolbar
          id={id}
          days={days()}
          bucket={bucket()}
          onDaysChange={setDays}
          onBucketChange={setBucket}
        />
      </div>
      <p class="mb-5 text-sm text-muted-foreground">
        {i18n.t("observations.reliabilityDescription")}
      </p>
      <ReliabilityChartData
        client={props.client}
        days={days()}
        bucket={bucket()}
        reloadKey={props.reloadKey}
      />
    </section>
  );
}

function ReliabilityChartData(props: {
  client: ApiClient;
  days: number;
  bucket: Bucket;
  reloadKey: number;
}) {
  const [result, setResult] = createSignal<{ data: ReliabilityData | null; error: string | null }>({
    data: null,
    error: null,
  });
  const i18n = useI18n();
  createEffect(
    () => [props.client, props.days, props.bucket, props.reloadKey],
    () => {
      const controller = new AbortController();
      const { signal } = controller;
      async function load() {
        try {
          const offset = Math.round(-new Date().getTimezoneOffset() / 60) * 60;
          const offsetUs = offset * 60_000_000;
          const until =
            (Math.floor((Date.now() * 1000 + offsetUs) / DAY_US) + 1) * DAY_US - offsetUs;
          const from = until - props.days * DAY_US;
          let firstFrom = from;
          let lastUntil = from;
          const rows: Api.Envelope<Api.UsageBuckets>["rows"] = [];
          const retryRows: Api.UsageRetries["retry_rows"] = [];
          const bucketsPartial = await paginateCursor({
            signal,
            read: (after) =>
              props.client.readUsageBuckets({
                fromUs: from,
                untilUs: until,
                bucket: props.bucket,
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
              props.client.readUsageRetries({
                fromUs: firstFrom,
                untilUs: lastUntil,
                bucket: props.bucket,
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
              props.client.readAvailability({
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
              props.client.readUsage({
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
                  props.bucket === "day" ? DAY_US : 7 * DAY_US,
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
    },
  );
  return (
    <>
      {(() => {
        if (!result().data && result().error)
          return (
            <DismissibleNotice message={result().error!} class="text-sm text-destructive">
              {result().error}
            </DismissibleNotice>
          );
        if (!result().data) return <p role="status">{i18n.t("common.loading")}</p>;
        const empty = createMemo(
          () =>
            result()!.data!.totalRequests === 0 &&
            result()!.data!.totalRetries === 0 &&
            result()!.data!.ttfbCount === 0 &&
            result()!.data!.durationCount === 0,
        );
        return (
          <>
            {result()!.data!.partial ? (
              <DismissibleNotice
                tone="warning"
                message={i18n.t("observations.reliabilityPartial")}
                class="mb-3 text-sm text-muted-foreground"
              />
            ) : null}
            {empty() ? (
              <p class="py-4 text-sm text-muted-foreground">
                {i18n.t("observations.reliabilityNoData")}
              </p>
            ) : (
              <ReliabilityPlot data={result().data!} />
            )}
            {result().error ? (
              <DismissibleNotice message={result().error!} class="mt-3 text-sm text-destructive">
                {result().error}
              </DismissibleNotice>
            ) : null}
          </>
        );
      })()}
    </>
  );
}

function ReliabilityPlot(props: { data: ReliabilityData }) {
  const i18n = useI18n();
  const number = new Intl.NumberFormat(i18n.locale);
  const percent = new Intl.NumberFormat(i18n.locale, {
    style: "percent",
    minimumFractionDigits: 1,
    maximumFractionDigits: 1,
  });
  const ms = new Intl.NumberFormat(i18n.locale, { maximumFractionDigits: 1 });
  const date = new Intl.DateTimeFormat(i18n.locale, {
    year: "numeric",
    month: "short",
    day: "numeric",
    timeZone: "UTC",
  });
  const dateLabel = (us: number) =>
    date.format(new Date((us + props.data.offset * 60_000_000) / 1000));
  const errorShare = createMemo(() =>
    props.data.totalRequests > 0 ? props.data.totalErrors / props.data.totalRequests : null,
  );
  const cacheCount = (count: number) => (count > 0 ? number.format(count) : "—");
  const stats = createMemo<[string, string][]>(() => [
    [i18n.t("observations.requests"), number.format(props.data.totalRequests)],
    [
      i18n.t("observations.reliabilityErrors"),
      errorShare() === null
        ? `${number.format(props.data.totalErrors)}`
        : `${number.format(props.data.totalErrors)} (${percent.format(errorShare()!)})`,
    ],
    [i18n.t("observations.reliabilityRetries"), number.format(props.data.totalRetries)],
    [i18n.t("observations.reliabilityCacheReads"), cacheCount(props.data.totalCacheReads)],
    [i18n.t("observations.reliabilityCacheWrites"), cacheCount(props.data.totalCacheWrites)],
    [
      i18n.t("observations.reliabilityAvgTtfb"),
      props.data.ttfbAvgMs === null ? "—" : `${ms.format(props.data.ttfbAvgMs)} ms`,
    ],
    [
      i18n.t("observations.reliabilityAvgDuration"),
      props.data.durationAvgMs === null ? "—" : `${ms.format(props.data.durationAvgMs)} ms`,
    ],
  ]);
  return (
    <div class="flex flex-col gap-6">
      <dl class="grid gap-3 sm:grid-cols-3 lg:grid-cols-4">
        {stats().map(([label, value]) => (
          <div class="rounded-md border border-border px-3 py-2">
            <dt class="text-xs text-muted-foreground">{label}</dt>
            <dd class="mt-1 text-lg font-medium tabular-nums">{value}</dd>
          </div>
        ))}
      </dl>
      <div>
        <h3 class="mb-3 text-sm font-medium">{i18n.t("observations.reliabilityErrors")}</h3>
        <CartesianBarPlot
          integerTicks
          ariaLabel={i18n.t("observations.reliabilityErrorsAria", {
            from: dateLabel(props.data.fromUs),
            until: dateLabel(props.data.untilUs - 1),
            errors: number.format(props.data.totalErrors),
            retries: number.format(props.data.totalRetries),
          })}
          series={[
            {
              key: "errors",
              label: i18n.t("observations.reliabilityErrors"),
              color: errorsColor,
              share:
                props.data.totalErrors + props.data.totalRetries
                  ? props.data.totalErrors / (props.data.totalErrors + props.data.totalRetries)
                  : 0,
            },
            {
              key: "retries",
              label: i18n.t("observations.reliabilityRetries"),
              color: retriesColor,
              share:
                props.data.totalErrors + props.data.totalRetries
                  ? props.data.totalRetries / (props.data.totalErrors + props.data.totalRetries)
                  : 0,
            },
          ]}
          bars={props.data.buckets.map((bar) => ({
            key: String(bar.bucketUs),
            label: dateLabel(bar.bucketUs),
            values: [bar.errors, bar.retries],
            tooltip: [
              dateLabel(bar.bucketUs),
              `${i18n.t("observations.requests")}: ${number.format(bar.requests)}`,
              `${i18n.t("observations.reliabilityErrors")}: ${number.format(bar.errors)}`,
              `${i18n.t("observations.reliabilityRetries")}: ${number.format(bar.retries)}`,
            ].join("\n"),
          }))}
        />
      </div>
      <div>
        <h3 class="mb-3 text-sm font-medium">{i18n.t("observations.tokens")}</h3>
        <p class="mb-3 text-xs text-muted-foreground">
          {i18n.t("observations.reliabilityCacheNote")}
        </p>
        <CartesianBarPlot
          integerTicks
          ariaLabel={i18n.t("observations.reliabilityCacheAria", {
            from: dateLabel(props.data.fromUs),
            until: dateLabel(props.data.untilUs - 1),
            reads: cacheCount(props.data.totalCacheReads),
            writes: cacheCount(props.data.totalCacheWrites),
          })}
          series={[
            {
              key: "hits",
              label: i18n.t("observations.reliabilityCacheReads"),
              color: cacheReadsColor,
              share: 0,
              legendValue: cacheCount(props.data.totalCacheReads),
            },
            {
              key: "miss",
              label: i18n.t("observations.reliabilityCacheWrites"),
              color: cacheWritesColor,
              share: 0,
              legendValue: cacheCount(props.data.totalCacheWrites),
            },
          ]}
          bars={props.data.buckets.map((bar) => ({
            key: String(bar.bucketUs),
            label: dateLabel(bar.bucketUs),
            values: [bar.cacheReads, bar.cacheWrites],
            tooltip: [
              dateLabel(bar.bucketUs),
              `${i18n.t("observations.reliabilityCacheReads")}: ${cacheCount(bar.cacheReads)}`,
              `${i18n.t("observations.reliabilityCacheWrites")}: ${cacheCount(bar.cacheWrites)}`,
            ].join("\n"),
          }))}
        />
      </div>
      <div>
        <h3 class="mb-3 text-sm font-medium">{i18n.t("observations.firstByte")}</h3>
        <LatencyBars data={props.data} />
      </div>
    </div>
  );
}

function LatencyBars(props: { data: ReliabilityData }) {
  const i18n = useI18n();
  const number = new Intl.NumberFormat(i18n.locale, { maximumFractionDigits: 1 });
  const date = new Intl.DateTimeFormat(i18n.locale, {
    year: "numeric",
    month: "short",
    day: "numeric",
    timeZone: "UTC",
  });
  const dateLabel = (us: number) =>
    date.format(new Date((us + props.data.offset * 60_000_000) / 1000));
  return (
    <CartesianBarPlot
      mode="grouped"
      plotHeight={200}
      ariaLabel={i18n.t("observations.reliabilityLatencyAria", {
        from: dateLabel(props.data.fromUs),
        until: dateLabel(props.data.untilUs - 1),
        ttfb: props.data.ttfbAvgMs === null ? "—" : `${number.format(props.data.ttfbAvgMs)} ms`,
        duration:
          props.data.durationAvgMs === null ? "—" : `${number.format(props.data.durationAvgMs)} ms`,
      })}
      series={[
        { key: "ttfb", label: i18n.t("observations.reliabilityAvgTtfb"), color: ttfbColor },
        {
          key: "duration",
          label: i18n.t("observations.reliabilityAvgDuration"),
          color: durationColor,
        },
      ]}
      bars={props.data.buckets.map((bar) => ({
        key: String(bar.bucketUs),
        label: dateLabel(bar.bucketUs),
        values: [bar.ttfbAvgMs ?? 0, bar.durationAvgMs ?? 0],
        tooltip: [
          dateLabel(bar.bucketUs),
          `${i18n.t("observations.reliabilityAvgTtfb")}: ${bar.ttfbAvgMs === null ? "—" : `${number.format(bar.ttfbAvgMs)} ms`}`,
          `${i18n.t("observations.reliabilityAvgDuration")}: ${bar.durationAvgMs === null ? "—" : `${number.format(bar.durationAvgMs)} ms`}`,
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

export function HealthChart(props: { client: ApiClient; fromUs: number; untilUs: number }) {
  const i18n = useI18n();
  const id = createUniqueId();
  const [result, setResult] = createSignal<{ data: HealthData | null; error: string | null }>({
    data: null,
    error: null,
  });
  const widthUs = createMemo(() => (props.untilUs - props.fromUs <= DAY_US ? DAY_US / 24 : DAY_US));
  createEffect(
    () => [props.client, props.fromUs, props.untilUs, widthUs()],
    () => {
      const controller = new AbortController();
      const { signal } = controller;
      async function load() {
        try {
          const bars = Array.from(
            { length: Math.ceil((props.untilUs - props.fromUs) / widthUs()) },
            (_, index) => ({ fromUs: props.fromUs + index * widthUs(), counts: [0, 0, 0, 0] }),
          );
          const data: HealthData = { bars, totals: [0, 0, 0, 0], total: 0, partial: false };
          data.partial = await paginateCursor({
            signal,
            read: (after) =>
              props.client.readAvailability({
                fromUs: props.fromUs,
                untilUs: props.untilUs,
                after,
                limit: 100,
                signal,
              }),
            apply: (page) => {
              for (const row of page.observations) {
                const bar = bars[Math.floor((row.observed_at_us - props.fromUs) / widthUs())];
                if (!bar || row.observed_at_us >= props.untilUs) continue;
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
            setResult({
              data: null,
              error: error instanceof Error ? error.message : String(error),
            });
        }
      }
      void load();
      return () => controller.abort();
    },
  );
  const date = new Intl.DateTimeFormat(i18n.locale, {
    month: "short",
    day: "numeric",
    ...(widthUs() < DAY_US ? { hour: "2-digit", minute: "2-digit" } : {}),
  });
  const dateLabel = (us: number) => date.format(new Date(us / 1000));
  const number = new Intl.NumberFormat(i18n.locale);
  const data = createMemo(() => result().data);
  return (
    <section
      class="min-w-0 rounded-xl border border-border bg-card p-4 sm:p-5"
      aria-labelledby={id}
    >
      <h2 id={id} class="text-base font-semibold tracking-tight">
        {i18n.t("observations.healthChartTitle")}
      </h2>
      <p class="mt-1 mb-5 text-sm text-muted-foreground">
        {i18n.t("observations.healthChartDescription")}
      </p>
      {result().error ? (
        <DismissibleNotice message={result().error!} class="text-sm text-destructive">
          {result().error}
        </DismissibleNotice>
      ) : !data() ? (
        <p role="status">{i18n.t("common.loading")}</p>
      ) : (
        <>
          {data()!.partial && (
            <DismissibleNotice
              tone="warning"
              message={i18n.t("observations.availabilityPartial")}
              class="mb-3 text-sm text-muted-foreground"
            />
          )}
          {!data()!.total ? (
            <p class="py-4 text-sm text-muted-foreground">
              {i18n.t("observations.noAvailability")}
            </p>
          ) : (
            <CartesianBarPlot
              integerTicks
              ariaLabel={i18n.t("observations.healthChartAria", {
                from: dateLabel(props.fromUs),
                until: dateLabel(props.untilUs),
                count: number.format(data()!.total),
              })}
              series={classifications.map((key, rank) => ({
                key,
                label: i18n.t(labels[rank]),
                color: colors[rank],
                share: data()!.totals[rank] / data()!.total,
              }))}
              bars={data()!.bars.map((bar) => ({
                key: String(bar.fromUs),
                label: dateLabel(bar.fromUs),
                values: bar.counts,
                tooltip: [
                  dateLabel(bar.fromUs),
                  `${i18n.t("observations.requests")}: ${number.format(bar.counts.reduce((sum, count) => sum + count, 0))}`,
                  ...labels.map(
                    (label, rank) => `${i18n.t(label)}: ${number.format(bar.counts[rank])}`,
                  ),
                ].join("\n"),
              }))}
            />
          )}
        </>
      )}
    </section>
  );
}
