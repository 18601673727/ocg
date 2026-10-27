"use client";

import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import { useEffect, useId, useState } from "react";
import type { HttpCanonicalControlClient } from "../runtime/control-client";
import { useI18n } from "../i18n";
import { StackedBarChart } from "./stacked-bar-chart";

const DAY_US = 86_400_000_000;
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
type Data = {
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
  const [result, setResult] = useState<{ data: Data | null; error: string | null }>({
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
        const data: Data = { bars, totals: [0, 0, 0, 0], total: 0, partial: false };
        let after: string | undefined;
        for (let pageNumber = 0; pageNumber < 10; pageNumber++) {
          const page = await client.readAvailability({
            fromUs,
            untilUs,
            after,
            limit: 100,
            signal,
          });
          if (signal.aborted) return;
          for (const row of page.observations) {
            const bar = bars[Math.floor((row.observed_at_us - fromUs) / widthUs)];
            if (!bar || row.observed_at_us >= untilUs) continue;
            const rank = classifications.indexOf(row.classification);
            if (rank < 0) throw new Error("Unsupported availability classification.");
            bar.counts[rank]++;
            data.totals[rank]++;
            data.total++;
          }
          data.partial = page.next_after !== null;
          if (!page.next_after) break;
          if (page.next_after === after) throw new Error("Availability cursor did not advance.");
          after = page.next_after;
        }
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
            <StackedBarChart
              integerTicks
              metricLabel={t("observations.requests")}
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
