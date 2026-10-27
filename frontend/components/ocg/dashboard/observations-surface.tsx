"use client";

import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { HttpCanonicalControlClient } from "../runtime/control-client";
import type { decodeAvailability, decodeHealth, decodeUsage } from "../contracts";
import { useI18n } from "../i18n";
import { ContentContainer, ContentHeader } from "../layout/content-container";
import { UsageChart } from "./usage-chart";
import { ReliabilityChart } from "./reliability-chart";
import { HealthChart } from "./health-chart";
import { LoadMore } from "../primitives/load-more";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

type Usage = ReturnType<typeof decodeUsage>["observations"][number];
type Availability = ReturnType<typeof decodeAvailability>["observations"][number];
const cell = "px-3 py-3 align-top text-left";
const select = "rounded-md border border-input bg-background px-3 py-2 text-sm";
const number = (value: number | null) => (value === null ? "—" : value.toLocaleString());

export function ObservationsSurface({
  client,
  kind,
  health,
  onJob,
}: {
  client: HttpCanonicalControlClient;
  kind: "usage" | "health";
  health: ReturnType<typeof decodeHealth> | null;
  onJob: (job: string) => void;
}) {
  const [days, setDays] = useState(1);
  const [windowEnd, setWindowEnd] = useState(() => Date.now() * 1000);
  const [chartEnd, setChartEnd] = useState(windowEnd);
  const [items, setItems] = useState<(Usage | Availability)[]>([]);
  const [ready, setReady] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasMore, setHasMore] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const loadMoreRef = useRef<() => void>(() => {});
  const { t } = useI18n();
  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const fromUs = windowEnd - days * 86400_000_000;
    let next: string | null = null;
    let started = false;
    let busy = false;
    let retryFirst = false;
    let clearFirst = true;
    let version = 0;
    const merge = (incoming: (Usage | Availability)[]) => {
      if (!incoming.length) return;
      setItems((current) => {
        const byId = new Map(current.map((row) => [`${row.job}:${row.id}`, row]));
        for (const row of incoming) byId.set(`${row.job}:${row.id}`, row);
        return [...byId.values()];
      });
    };
    const readPage = (after?: string) => {
      const settings = { fromUs, after, limit: 50, signal: controller.signal };
      return kind === "usage" ? client.readUsage(settings) : client.readAvailability(settings);
    };
    const fail = (cause: unknown) => {
      if (!controller.signal.aborted) {
        setError(cause instanceof Error ? cause.message : String(cause));
        setReady(true);
      }
    };
    const load = async (after?: string) => {
      if (busy) return;
      busy = true;
      setLoadingMore(true);
      if (after === undefined && clearFirst) {
        setItems([]);
        clearFirst = false;
      }
      const run = version;
      try {
        const page = await readPage(after);
        if (controller.signal.aborted || run !== version) return;
        if (page.next_after !== null && page.next_after === after)
          throw new Error("Observation cursor did not advance.");
        merge(page.observations);
        next = page.next_after;
        retryFirst = false;
        setHasMore(next !== null);
        setError(null);
        setReady(true);
      } catch (cause) {
        if (after === undefined) retryFirst = true;
        setHasMore(true);
        fail(cause);
      } finally {
        busy = false;
        if (!controller.signal.aborted) {
          setLoadingMore(false);
          if (run !== version && started) void load();
        }
      }
    };
    loadMoreRef.current = () => {
      if (busy || (!retryFirst && next === null)) return;
      void load(retryFirst ? undefined : (next ?? undefined));
    };
    const unsubscribe = client.subscribeObservationChanges(
      (batch) => {
        if (batch.ready && !started) {
          started = true;
          void load();
        }
        const incoming = (kind === "usage" ? batch.usage : batch.availability).filter(
          (row) => row.observed_at_us >= fromUs,
        );
        merge(incoming);
        if (incoming.length) setError(null);
        if (incoming.length && !timer)
          timer = setTimeout(() => {
            timer = undefined;
            setChartEnd(Date.now() * 1000);
          }, 5000);
      },
      () => {
        version++;
        started = false;
        setItems([]);
        clearFirst = false;
        retryFirst = true;
        next = null;
        setHasMore(true);
      },
      fail,
    );
    return () => {
      controller.abort();
      unsubscribe();
      if (timer) clearTimeout(timer);
    };
  }, [client, days, windowEnd, kind]);
  const newestFirst = useMemo(
    () =>
      [...items].sort((a, b) => b.observed_at_us - a.observed_at_us || b.id.localeCompare(a.id)),
    [items],
  );
  const loadMore = useCallback(() => loadMoreRef.current(), []);

  const usageColumns = [
    t("observations.report"),
    t("observability.input"),
    t("observability.output"),
    t("observations.cacheReadWrite"),
    t("observability.reasoning"),
    t("observations.firstByte"),
  ];
  const providerColumns = [
    t("observations.classification"),
    t("observations.httpStatus"),
    t("observations.finishReason"),
  ];

  return (
    <ContentContainer>
      <ContentHeader
        title={kind === "usage" ? t("nav.usage") : t("observations.health")}
        actions={
          <>
            <label className="sr-only" htmlFor="observation-period">
              {t("observations.timeRange")}
            </label>
            <Select
              value={days}
              items={[
                { value: 1, label: t("observations.last24") },
                { value: 7, label: t("observations.last7") },
                { value: 30, label: t("observations.last30") },
              ]}
              onValueChange={(next) => {
                if (next === null) return;
                setDays(next);
                setWindowEnd(Date.now() * 1000);
                setChartEnd(Date.now() * 1000);
                setItems([]);
                setReady(false);
                setHasMore(false);
                setLoadingMore(false);
              }}
            >
              <SelectTrigger id="observation-period" className={select}>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value={1}>{t("observations.last24")}</SelectItem>
                <SelectItem value={7}>{t("observations.last7")}</SelectItem>
                <SelectItem value={30}>{t("observations.last30")}</SelectItem>
              </SelectContent>
            </Select>
          </>
        }
      />
      {kind === "usage" ? <UsageChart client={client} reloadKey={chartEnd} /> : null}
      {kind === "usage" ? <ReliabilityChart client={client} reloadKey={chartEnd} /> : null}
      {kind === "health" ? (
        <HealthChart
          key={`${days}:${chartEnd}`}
          client={client}
          fromUs={chartEnd - days * 86400_000_000}
          untilUs={chartEnd}
        />
      ) : null}
      {kind === "health" ? (
        <>
          <section
            aria-label={t("observations.runtimeHealth")}
            className="grid gap-3 sm:grid-cols-4"
          >
            {[
              [t("settings.control.host"), health?.host ?? t("settings.control.unreachable")],
              [t("settings.control.database"), health?.database.status ?? t("common.unknown")],
              [
                t("settings.control.reads"),
                !health
                  ? t("common.unknown")
                  : health.database.readable
                    ? t("settings.control.available")
                    : t("settings.control.unavailable"),
              ],
              [
                t("settings.control.commands"),
                !health
                  ? t("common.unknown")
                  : health.database.accepting_commands
                    ? t("settings.control.accepting")
                    : t("settings.control.notAccepting"),
              ],
            ].map(([label, value]) => (
              <div className="rounded-lg border border-border bg-card p-4" key={label}>
                <h3 className="text-xs text-muted-foreground">{label}</h3>
                <p className="mt-2 text-lg font-medium">{value}</p>
              </div>
            ))}
          </section>
          <section
            aria-label={t("observations.quarantinedCapacity")}
            className="min-w-0 rounded-lg border border-border bg-card p-4"
          >
            <h3 className="font-medium">{t("observations.quarantinedCapacity")}</h3>
            <p className="my-2 text-sm text-muted-foreground">
              {t("observations.quarantinedNote")}
            </p>
            {!health || health.quarantined.length === 0 ? (
              <p className="py-2 text-sm text-muted-foreground">
                {t("observations.noQuarantined")}
              </p>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="border-b border-border text-xs text-muted-foreground">
                    <tr>
                      <th className={cell}>{t("observations.job")}</th>
                      <th className={cell}>{t("observations.pool")}</th>
                      <th className={cell}>{t("observations.quarantinedSince")}</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border">
                    {health.quarantined.map((item) => (
                      <tr key={item.job}>
                        <td className={cell}>
                          <button
                            className="font-mono underline underline-offset-4"
                            onClick={() => onJob(item.job)}
                          >
                            {item.job}
                          </button>
                        </td>
                        <td className={`${cell} font-mono`}>{item.pool ?? "—"}</td>
                        <td className={`${cell} whitespace-nowrap`}>
                          {item.since ? new Date(item.since / 1000).toLocaleString() : "—"}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>
        </>
      ) : null}
      <section className="min-w-0 rounded-lg border border-border bg-card p-4">
        <h3 className="font-medium">
          {kind === "usage" ? t("observations.usageTable") : t("observations.providerTable")}
        </h3>
        <p className="my-3 text-sm text-muted-foreground">
          {kind === "usage" ? t("observations.usageNote") : t("observations.providerNote")}
        </p>
        {error ? (
          <DismissibleNotice message={error} className="mb-3 text-sm text-destructive">
            {error}
          </DismissibleNotice>
        ) : null}
        {!ready ? <p role="status">{t("observations.loading")}</p> : null}
        {ready && items.length === 0 && !error ? (
          <p className="py-4 text-sm text-muted-foreground">{t("observations.empty")}</p>
        ) : null}
        {items.length > 0 ? (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="border-b border-border text-xs text-muted-foreground">
                <tr>
                  <th className={cell}>{t("observations.observed")}</th>
                  <th className={cell}>{t("observations.jobRequest")}</th>
                  <th className={cell}>{t("observations.providerModel")}</th>
                  {(kind === "usage" ? usageColumns : providerColumns).map((label) => (
                    <th className={cell} key={label}>
                      {label}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {newestFirst.map((row) => (
                  <tr key={`${row.job}:${row.id}`}>
                    <td className={`${cell} whitespace-nowrap`}>
                      {new Date(row.observed_at_us / 1000).toLocaleString()}
                    </td>
                    <td className={cell}>
                      <button
                        className="font-mono underline underline-offset-4"
                        onClick={() => onJob(row.job)}
                      >
                        {row.job}
                      </button>
                      <p className="text-xs text-muted-foreground">
                        {t("observations.requestNumber", { request: row.request })}
                      </p>
                    </td>
                    <td className={`${cell} max-w-64 break-words`}>
                      {row.provider ?? t("observations.unknownProvider")}
                      <p className="text-xs text-muted-foreground">
                        {row.model ?? t("observations.unknownModel")}
                      </p>
                    </td>
                    {"input_tokens" in row ? (
                      <>
                        <td className={cell}>
                          {row.finality ?? t("observations.unannotated")}
                          <p className="text-xs text-muted-foreground">
                            {row.eligibility ?? t("observations.deliveryUnknown")}
                          </p>
                        </td>
                        <td className={cell}>{number(row.input_tokens)}</td>
                        <td className={cell}>{number(row.output_tokens)}</td>
                        <td className={cell}>
                          {number(row.cache_read_tokens)} / {number(row.cache_write_tokens)}
                        </td>
                        <td className={cell}>{number(row.reasoning_tokens)}</td>
                        <td className={cell}>
                          {number(row.ttfb_ms)} / {number(row.finished_in_ms)}
                        </td>
                      </>
                    ) : (
                      <>
                        <td className={cell}>{row.classification}</td>
                        <td className={cell}>{number(row.status_code)}</td>
                        <td className={cell}>{row.finish_reason ?? "—"}</td>
                      </>
                    )}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : null}
        {hasMore && <LoadMore loading={loadingMore} onLoad={loadMore} auto={!error} />}
      </section>
    </ContentContainer>
  );
}
