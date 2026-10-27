"use client";

import type * as Api from "@/components/ocg/contracts/api";

import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import { useEffect, useId, useState } from "react";
import type { HttpCanonicalControlClient } from "../runtime/control-client";
import { useI18n } from "../i18n";
import { summarize, type RankedUsage } from "./usage-chart-data";
import { StackedBarChart } from "./stacked-bar-chart";
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
type ChartData = ReturnType<typeof summarize> & {
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
              if (next !== null) setDays(next);
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
            onValueChange={(next) => setBucket(next === "week" ? "week" : "day")}
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
      </div>
      <UsageChartData
        key={`${days}:${bucket}:${reloadKey}`}
        client={client}
        days={days}
        bucket={bucket}
      />
    </section>
  );
}

function UsageChartData({
  client,
  days,
  bucket,
}: {
  client: HttpCanonicalControlClient;
  days: number;
  bucket: Bucket;
}) {
  const [result, setResult] = useState<{ data: ChartData | null; error: string | null }>({
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
        let after: string | undefined;
        let partial = false;
        const rows: Api.Envelope<Api.UsageBuckets>["rows"] = [];
        for (let pageNumber = 0; pageNumber < 10; pageNumber++) {
          const page = await client.readUsageBuckets({
            fromUs: from,
            untilUs: until,
            bucket,
            tzOffsetMinutes: offset,
            scope: "generative",
            after,
            signal,
          });
          if (signal.aborted) return;
          if (pageNumber === 0) firstFrom = page.from_us;
          lastUntil = page.until_us;
          rows.push(...page.rows);
          partial = !page.complete;
          if (page.complete || page.resume_after === null) break;
          if (page.resume_after === after) throw new Error("Usage cursor did not advance.");
          after = page.resume_after;
        }
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
          setResult({ data: null, error: error instanceof Error ? error.message : String(error) });
      }
    }
    void load();
    return () => controller.abort();
  }, [client, days, bucket]);
  if (result.error)
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
    </>
  );
}

function UsagePlot({ data }: { data: ChartData }) {
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
    <StackedBarChart
      metricLabel={t("observations.tokens")}
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
