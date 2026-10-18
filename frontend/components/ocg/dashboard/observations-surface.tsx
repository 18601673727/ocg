"use client";

import { useCallback, useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import type { HttpCanonicalControlClient } from "../runtime/canonical-client";
import { useControlPage } from "../runtime/use-control-page";
import type { decodeAvailability, decodeHealth, decodeUsage } from "../contracts";

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

  return (
    <div className="mx-auto flex w-full max-w-7xl flex-col gap-5 px-4 py-6 sm:px-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h2 className="font-heading text-xl">{kind === "usage" ? "Usage" : "Health"}</h2>
        <div className="flex items-center gap-2">
          <label className="sr-only" htmlFor="observation-period">Time range</label>
          <select id="observation-period" className={select} value={days} onChange={(event) => { setDays(Number(event.target.value)); setWindowEnd(Date.now() * 1000); }}>
            <option value={1}>Last 24 hours</option><option value={7}>Last 7 days</option><option value={30}>Last 30 days</option>
          </select>
          <Button variant="outline" disabled={page.loading} onClick={() => setWindowEnd(Date.now() * 1000)}>Refresh</Button>
        </div>
      </div>
      {kind === "health" ? (
        <section aria-label="Runtime health" className="grid gap-3 sm:grid-cols-4">
          {[["Host", health?.host ?? "unreachable"], ["Database", health?.database.status ?? "unknown"],
            ["Reads", !health ? "unknown" : health.database.readable ? "available" : "unavailable"],
            ["Commands", !health ? "unknown" : health.database.accepting_commands ? "accepting" : "not accepting"]].map(([label, value]) => (
              <div className="rounded-lg border border-border bg-card p-4" key={label}><h3 className="text-xs text-muted-foreground">{label}</h3><p className="mt-2 text-lg font-medium">{value}</p></div>
            ))}
        </section>
      ) : null}
      <section className="min-w-0 rounded-lg border border-border bg-card p-4">
        <h3 className="font-medium">{kind === "usage" ? "Token and latency observations" : "Provider observations"}</h3>
        <p className="my-3 text-sm text-muted-foreground">
          {kind === "usage"
            ? "Recorded usage for each provider request. Cumulative reports can repeat earlier counts and are not added together. A dash means the provider did not report a value."
            : "Recorded response evidence, grouped by provider and model. These are individual observations, not a current uptime rate or a provider probe."}
        </p>
        {page.error ? <p role="alert" className="mb-3 text-sm text-destructive">{page.error}</p> : null}
        {page.items.length === 0 && !page.error && (!page.ready || page.next || page.loading) ? <p role="status">Loading observations…</p> : null}
        {page.ready && page.items.length === 0 && !page.next && !page.loading && !page.error ? <p className="py-4 text-sm text-muted-foreground">No observations in this time range.</p> : null}
        {page.items.length > 0 ? <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="border-b border-border text-xs text-muted-foreground"><tr>
              <th className={cell}>Observed</th><th className={cell}>Job / request</th><th className={cell}>Provider / model</th>
              {(kind === "usage" ? ["Report", "Input", "Output", "Cache read / write", "Reasoning", "First byte / duration (ms)"] : ["Classification", "HTTP status", "Finish reason"]).map((label) => <th className={cell} key={label}>{label}</th>)}
            </tr></thead>
            <tbody className="divide-y divide-border">
              {page.items.map((row) => <tr key={`${row.job}:${row.id}`}>
                <td className={`${cell} whitespace-nowrap`}>{new Date(row.observed_at_us / 1000).toLocaleString()}</td>
                <td className={cell}><button className="font-mono underline underline-offset-4" onClick={() => onJob(row.job)}>{row.job}</button><p className="text-xs text-muted-foreground">Request {row.request}</p></td>
                <td className={`${cell} max-w-64 break-words`}>{row.provider ?? "Unknown provider"}<p className="text-xs text-muted-foreground">{row.model ?? "Unknown model"}</p></td>
                {"input_tokens" in row ? <>
                  <td className={cell}>{row.finality ?? "unannotated"}<p className="text-xs text-muted-foreground">{row.eligibility ?? "delivery unknown"}</p></td>
                  <td className={cell}>{number(row.input_tokens)}</td><td className={cell}>{number(row.output_tokens)}</td>
                  <td className={cell}>{number(row.cache_read_tokens)} / {number(row.cache_write_tokens)}</td>
                  <td className={cell}>{number(row.reasoning_tokens)}</td><td className={cell}>{number(row.ttfb_ms)} / {number(row.finished_in_ms)}</td>
                </> : <><td className={cell}>{row.classification}</td><td className={cell}>{number(row.status_code)}</td><td className={cell}>{row.finish_reason ?? "—"}</td></>}
              </tr>)}
            </tbody>
          </table>
        </div> : null}
        {page.next ? <Button className="mt-4" variant="outline" disabled={page.loading} onClick={page.loadMore}>{page.loading ? "Loading…" : "Load more observations"}</Button> : null}
      </section>
    </div>
  );
}
