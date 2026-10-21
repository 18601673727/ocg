"use client";

import { useCallback, useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import type { HttpCanonicalControlClient } from "../runtime/control-client";
import { useControlPage } from "../runtime/use-control-page";
import type { decodeAvailability, decodeHealth, decodeUsage } from "../contracts";
import { useI18n } from "../i18n";
import { UsageChart } from "./usage-chart";

type Usage = ReturnType<typeof decodeUsage>["observations"][number];
type Availability = ReturnType<typeof decodeAvailability>["observations"][number];
const cell = "px-3 py-3 align-top text-left";
const select = "rounded-md border border-input bg-background px-3 py-2 text-sm";
const number = (value: number | null) => value === null ? "—" : value.toLocaleString();

export function ObservationsSurface({ client, kind, health, onJob }: {
  client: HttpCanonicalControlClient;
  kind: "usage" | "health";
  health: ReturnType<typeof decodeHealth> | null;
  onJob: (job: string) => void;
}) {
  const [days, setDays] = useState(1);
  const [windowEnd, setWindowEnd] = useState(() => Date.now() * 1000);
  const { t } = useI18n();
  const load = useCallback(async (after: string | undefined, signal: AbortSignal) => {
    let currentAfter = after;
    while (!signal.aborted) {
      const settings = { fromUs: windowEnd - days * 86400_000_000, untilUs: windowEnd, after: currentAfter, limit: 50, signal };
      const page = kind === "usage"
        ? await client.readUsage(settings)
        : await client.readAvailability(settings);
      const items = page.observations as (Usage | Availability)[];
      if (items.length > 0 || !page.next_after) {
        return { items, next: page.next_after };
      }
      currentAfter = page.next_after;
    }
    return { items: [], next: null };
  }, [client, days, windowEnd, kind]);
  const page = useControlPage(load);

  const { ready, items, next, loading, loadMore } = page;
  useEffect(() => {
    if (ready && items.length === 0 && next && !loading) {
      loadMore();
    }
  }, [ready, items.length, next, loading, loadMore]);

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
    <div className="mx-auto flex w-full max-w-7xl flex-col gap-5 px-4 py-6 sm:px-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h2 className="font-heading text-xl">{kind === "usage" ? t("nav.usage") : t("observations.health")}</h2>
        <div className="flex items-center gap-2">
          <label className="sr-only" htmlFor="observation-period">{t("observations.timeRange")}</label>
          <select id="observation-period" className={select} value={days} onChange={(event) => { setDays(Number(event.target.value)); setWindowEnd(Date.now() * 1000); }}>
            <option value={1}>{t("observations.last24")}</option><option value={7}>{t("observations.last7")}</option><option value={30}>{t("observations.last30")}</option>
          </select>
          <Button variant="outline" disabled={page.loading} onClick={() => setWindowEnd(Date.now() * 1000)}>{t("common.refresh")}</Button>
        </div>
      </div>
      {kind === "usage" ? <UsageChart client={client} reloadKey={windowEnd} /> : null}
      {kind === "health" ? (
        <>
          <section aria-label={t("observations.runtimeHealth")} className="grid gap-3 sm:grid-cols-4">
            {[[t("settings.control.host"), health?.host ?? t("settings.control.unreachable")], [t("settings.control.database"), health?.database.status ?? t("common.unknown")],
              [t("settings.control.reads"), !health ? t("common.unknown") : health.database.readable ? t("settings.control.available") : t("settings.control.unavailable")],
              [t("settings.control.commands"), !health ? t("common.unknown") : health.database.accepting_commands ? t("settings.control.accepting") : t("settings.control.notAccepting")]].map(([label, value]) => (
                <div className="rounded-lg border border-border bg-card p-4" key={label}><h3 className="text-xs text-muted-foreground">{label}</h3><p className="mt-2 text-lg font-medium">{value}</p></div>
              ))}
          </section>
          <section aria-label={t("observations.quarantinedCapacity")} className="min-w-0 rounded-lg border border-border bg-card p-4">
            <h3 className="font-medium">{t("observations.quarantinedCapacity")}</h3>
            <p className="my-2 text-sm text-muted-foreground">
              {t("observations.quarantinedNote")}
            </p>
            {!health || health.quarantined.length === 0 ? (
              <p className="py-2 text-sm text-muted-foreground">{t("observations.noQuarantined")}</p>
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
        <h3 className="font-medium">{kind === "usage" ? t("observations.usageTable") : t("observations.providerTable")}</h3>
        <p className="my-3 text-sm text-muted-foreground">
          {kind === "usage" ? t("observations.usageNote") : t("observations.providerNote")}
        </p>
        {page.error ? <p role="alert" className="mb-3 text-sm text-destructive">{page.error}</p> : null}
        {page.items.length === 0 && !page.error && (!page.ready || page.next || page.loading) ? <p role="status">{t("observations.loading")}</p> : null}
        {page.ready && page.items.length === 0 && !page.next && !page.loading && !page.error ? <p className="py-4 text-sm text-muted-foreground">{t("observations.empty")}</p> : null}
        {page.items.length > 0 ? <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="border-b border-border text-xs text-muted-foreground"><tr>
              <th className={cell}>{t("observations.observed")}</th><th className={cell}>{t("observations.jobRequest")}</th><th className={cell}>{t("observations.providerModel")}</th>
              {(kind === "usage" ? usageColumns : providerColumns).map((label) => <th className={cell} key={label}>{label}</th>)}
            </tr></thead>
            <tbody className="divide-y divide-border">
              {page.items.map((row) => <tr key={`${row.job}:${row.id}`}>
                <td className={`${cell} whitespace-nowrap`}>{new Date(row.observed_at_us / 1000).toLocaleString()}</td>
                <td className={cell}><button className="font-mono underline underline-offset-4" onClick={() => onJob(row.job)}>{row.job}</button><p className="text-xs text-muted-foreground">{t("observations.requestNumber", { request: row.request })}</p></td>
                <td className={`${cell} max-w-64 break-words`}>{row.provider ?? t("observations.unknownProvider")}<p className="text-xs text-muted-foreground">{row.model ?? t("observations.unknownModel")}</p></td>
                {"input_tokens" in row ? <>
                  <td className={cell}>{row.finality ?? t("observations.unannotated")}<p className="text-xs text-muted-foreground">{row.eligibility ?? t("observations.deliveryUnknown")}</p></td>
                  <td className={cell}>{number(row.input_tokens)}</td><td className={cell}>{number(row.output_tokens)}</td>
                  <td className={cell}>{number(row.cache_read_tokens)} / {number(row.cache_write_tokens)}</td>
                  <td className={cell}>{number(row.reasoning_tokens)}</td><td className={cell}>{number(row.ttfb_ms)} / {number(row.finished_in_ms)}</td>
                </> : <><td className={cell}>{row.classification}</td><td className={cell}>{number(row.status_code)}</td><td className={cell}>{row.finish_reason ?? "—"}</td></>}
              </tr>)}
            </tbody>
          </table>
        </div> : null}
        {page.next ? <Button className="mt-4" variant="outline" disabled={page.loading} onClick={page.loadMore}>{page.loading ? t("common.loading") : t("observations.loadMore")}</Button> : null}
      </section>
    </div>
  );
}
