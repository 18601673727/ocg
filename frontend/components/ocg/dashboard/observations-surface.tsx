import { createEffect, createMemo, createSignal } from "solid-js";
import type * as Api from "@/components/ocg/contracts/api";
import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import { Select, SelectItem } from "@/components/ui/select";
import { elementRef } from "@/lib/ui-state";
import { useI18n } from "../i18n";
import { ContentContainer, ContentHeader } from "../layout/content-container";
import { ListStatus } from "../primitives/list-status";
import { LoadMore } from "../primitives/load-more";
import type { ApiClient } from "../runtime/api-client";
import { HealthChart, ReliabilityChart, UsageChart } from "./chart";

type Usage = Api.Usage["observations"][number];
type Availability = Api.Availability["observations"][number];
const cell = "px-3 py-3 align-top text-left";

export function ObservationsSurface(props: {
  client: ApiClient;
  kind: "usage" | "health";
  health: Api.Health | null;
  onJob: (job: string) => void;
}) {
  const [days, setDays] = createSignal(1);
  const [windowEnd, setWindowEnd] = createSignal((() => Date.now() * 1000)());
  const [chartEnd, setChartEnd] = createSignal(windowEnd());
  const [items, setItems] = createSignal<(Usage | Availability)[]>([]);
  const [ready, setReady] = createSignal(false);
  const [error, setError] = createSignal<string | null>(null);
  const [hasMore, setHasMore] = createSignal(false);
  const [loadingMore, setLoadingMore] = createSignal(false);
  const loadMoreRef = elementRef<() => void>(() => {});
  const i18n = useI18n();
  const numberFormat = createMemo(() => new Intl.NumberFormat(i18n.locale));
  const number = (value: number | null) => (value === null ? "—" : numberFormat().format(value));
  const dateTime = createMemo(
    () => new Intl.DateTimeFormat(i18n.locale, { dateStyle: "medium", timeStyle: "medium" }),
  );
  createEffect(
    () => [props.client, days(), windowEnd(), props.kind],
    () => {
      const controller = new AbortController();
      let timer: ReturnType<typeof setTimeout> | undefined;
      const fromUs = windowEnd() - days() * 86400_000_000;
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
        return props.kind === "usage"
          ? props.client.readUsage(settings)
          : props.client.readAvailability(settings);
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
      const unsubscribe = props.client.subscribeObservationChanges(
        (batch) => {
          if (batch.ready && !started) {
            started = true;
            void load();
          }
          const incoming = (props.kind === "usage" ? batch.usage : batch.availability).filter(
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
    },
  );
  const newestFirst = createMemo(() =>
    [...items()].sort((a, b) => b.observed_at_us - a.observed_at_us || b.id.localeCompare(a.id)),
  );
  const loadMore = () => loadMoreRef.current();

  const usageColumns = [
    i18n.t("observations.report"),
    i18n.t("observability.input"),
    i18n.t("observability.output"),
    i18n.t("observations.cacheReadWrite"),
    i18n.t("observability.reasoning"),
    i18n.t("observations.firstByte"),
  ];
  const providerColumns = [
    i18n.t("observations.classification"),
    i18n.t("observations.httpStatus"),
    i18n.t("observations.finishReason"),
  ];

  const timeRange = createMemo(() => (
    <Select
      value={days()}
      items={[
        { value: 1, label: i18n.t("observations.last24") },
        { value: 7, label: i18n.t("observations.last7") },
        { value: 30, label: i18n.t("observations.last30") },
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
      aria-label={i18n.t("observations.timeRange")}
      class="border-input bg-card"
    >
      <SelectItem value={1}>{i18n.t("observations.last24")}</SelectItem>
      <SelectItem value={7}>{i18n.t("observations.last7")}</SelectItem>
      <SelectItem value={30}>{i18n.t("observations.last30")}</SelectItem>
    </Select>
  ));

  return (
    <ContentContainer class={props.kind === "usage" ? "[overflow-anchor:none]" : undefined}>
      <ContentHeader
        title={props.kind === "usage" ? i18n.t("nav.usage") : i18n.t("observations.health")}
        actions={props.kind === "health" ? timeRange() : undefined}
      />
      {props.kind === "usage" ? <UsageChart client={props.client} reloadKey={chartEnd()} /> : null}
      {props.kind === "usage" ? (
        <ReliabilityChart client={props.client} reloadKey={chartEnd()} />
      ) : null}
      {props.kind === "health" ? (
        <>
          <dl
            aria-label={i18n.t("observations.runtimeHealth")}
            class="grid grid-cols-2 gap-px overflow-hidden rounded-xl border border-border bg-border lg:grid-cols-4"
          >
            {[
              [
                i18n.t("settings.api.host"),
                props.health?.host ?? i18n.t("settings.api.unreachable"),
              ],
              [
                i18n.t("settings.api.database"),
                props.health?.database.status ?? i18n.t("common.unknown"),
              ],
              [
                i18n.t("settings.api.reads"),
                !props.health
                  ? i18n.t("common.unknown")
                  : props.health.database.readable
                    ? i18n.t("settings.api.available")
                    : i18n.t("settings.api.unavailable"),
              ],
              [
                i18n.t("settings.api.commands"),
                !props.health
                  ? i18n.t("common.unknown")
                  : props.health.database.accepting_commands
                    ? i18n.t("settings.api.accepting")
                    : i18n.t("settings.api.notAccepting"),
              ],
            ].map(([label, value]) => (
              <div class="min-w-0 bg-card p-4 sm:p-5">
                <dt class="text-xs font-medium text-muted-foreground">{label}</dt>
                <dd class="mt-2 text-lg font-semibold tracking-tight wrap-anywhere">{value}</dd>
              </div>
            ))}
          </dl>
          <HealthChart
            client={props.client}
            fromUs={chartEnd() - days() * 86400_000_000}
            untilUs={chartEnd()}
          />
          <section
            aria-label={i18n.t("observations.quarantinedCapacity")}
            class="min-w-0 rounded-xl border border-border bg-card p-4 sm:p-5"
          >
            <h2 class="text-base font-semibold tracking-tight">
              {i18n.t("observations.quarantinedCapacity")}
            </h2>
            <p class="my-2 text-sm text-muted-foreground">
              {i18n.t("observations.quarantinedNote")}
            </p>
            {!props.health || !props.health.quarantined || props.health.quarantined.length === 0 ? (
              <p class="py-2 text-sm text-muted-foreground">
                {i18n.t(
                  !props.health
                    ? "common.unknown"
                    : props.health.quarantined
                      ? "observations.noQuarantined"
                      : "observations.quarantinedUnreadable",
                )}
              </p>
            ) : (
              <div class="overflow-x-auto">
                <table class="w-full text-sm">
                  <thead class="border-b border-border text-xs text-muted-foreground">
                    <tr>
                      <th class={cell}>{i18n.t("observations.job")}</th>
                      <th class={cell}>{i18n.t("observations.pool")}</th>
                      <th class={cell}>{i18n.t("observations.quarantinedSince")}</th>
                    </tr>
                  </thead>
                  <tbody class="divide-y divide-border">
                    {props.health.quarantined.map((item) => (
                      <tr>
                        <td class={cell}>
                          <button
                            class="font-mono underline underline-offset-4"
                            onClick={() => props.onJob(item.job)}
                          >
                            {item.job}
                          </button>
                        </td>
                        <td class={`${cell} font-mono`}>{item.pool ?? "—"}</td>
                        <td class={`${cell} whitespace-nowrap`}>
                          {item.since ? dateTime().format(item.since / 1000) : "—"}
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
      <section class="min-w-0 overflow-hidden rounded-xl border border-border bg-card">
        <div class="flex flex-wrap items-start justify-between gap-3 border-b border-border p-4 sm:p-5">
          <div class="min-w-0 flex-1 basis-64">
            <h2 class="text-base font-semibold tracking-tight">
              {props.kind === "usage"
                ? i18n.t("observations.usageTable")
                : i18n.t("observations.providerTable")}
            </h2>
            <p class="mt-1 max-w-prose text-sm leading-relaxed text-muted-foreground">
              {props.kind === "usage"
                ? i18n.t("observations.usageNote")
                : i18n.t("observations.providerNote")}
            </p>
          </div>
          {props.kind === "usage" && timeRange()}
        </div>
        {error() ? (
          <DismissibleNotice message={error()!} class="m-4 text-sm text-destructive">
            {error()}
          </DismissibleNotice>
        ) : null}
        {!ready() && !error() ? (
          <ListStatus loading message={i18n.t("observations.loading")} />
        ) : null}
        {ready() && items().length === 0 && !error() ? (
          <ListStatus message={i18n.t("observations.empty")} />
        ) : null}
        {items().length > 0 ? (
          <div class="overflow-x-auto">
            <table
              class={props.kind === "usage" ? "w-full min-w-[1200px] text-sm" : "w-full text-sm"}
            >
              <thead class="border-b border-border bg-muted/50 text-xs text-muted-foreground [&_th]:font-medium">
                <tr>
                  <th class={`${cell} whitespace-nowrap`}>{i18n.t("observations.observed")}</th>
                  <th class={`${cell} whitespace-nowrap`}>{i18n.t("observations.jobRequest")}</th>
                  <th class={`${cell} whitespace-nowrap`}>
                    {i18n.t("observations.providerModel")}
                  </th>
                  {(props.kind === "usage" ? usageColumns : providerColumns).map((label) => (
                    <th class={`${cell} whitespace-nowrap`}>{label}</th>
                  ))}
                </tr>
              </thead>
              <tbody class="divide-y divide-border">
                {newestFirst().map((row) => (
                  <tr class="transition-colors hover:bg-muted/30">
                    <td class={`${cell} whitespace-nowrap`}>
                      {dateTime().format(row.observed_at_us / 1000)}
                    </td>
                    <td class={cell}>
                      <button
                        class="block max-w-48 truncate text-left font-mono underline decoration-muted-foreground underline-offset-4 hover:decoration-foreground focus-visible:outline-2 focus-visible:outline-offset-2"
                        title={row.job}
                        onClick={() => props.onJob(row.job)}
                      >
                        {row.job}
                      </button>
                      <p class="text-xs text-muted-foreground">
                        {i18n.t("observations.requestNumber", { request: row.request })}
                      </p>
                    </td>
                    <td class={`${cell} min-w-36 max-w-64 break-words`}>
                      {row.provider ?? i18n.t("observations.unknownProvider")}
                      <p class="text-xs text-muted-foreground">
                        {row.model ?? i18n.t("observations.unknownModel")}
                      </p>
                    </td>
                    {"input_tokens" in row ? (
                      <>
                        <td class={`${cell} min-w-32`}>
                          <span class="inline-flex rounded-md bg-muted px-2 py-0.5 text-xs font-medium">
                            {row.finality ?? i18n.t("observations.unannotated")}
                          </span>
                          <p class="mt-1 text-xs text-muted-foreground">
                            {row.eligibility ?? i18n.t("observations.deliveryUnknown")}
                          </p>
                        </td>
                        <td class={`${cell} whitespace-nowrap tabular-nums`}>
                          {number(row.input_tokens)}
                        </td>
                        <td class={`${cell} whitespace-nowrap tabular-nums`}>
                          {number(row.output_tokens)}
                        </td>
                        <td class={`${cell} whitespace-nowrap tabular-nums`}>
                          {number(row.cache_read_tokens)} / {number(row.cache_write_tokens)}
                        </td>
                        <td class={`${cell} whitespace-nowrap tabular-nums`}>
                          {number(row.reasoning_tokens)}
                        </td>
                        <td class={`${cell} whitespace-nowrap tabular-nums`}>
                          {number(row.ttfb_ms)} / {number(row.finished_in_ms)}
                        </td>
                      </>
                    ) : (
                      <>
                        <td class={cell}>{row.classification}</td>
                        <td class={cell}>{number(row.status_code)}</td>
                        <td class={cell}>{row.finish_reason ?? "—"}</td>
                      </>
                    )}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : null}
        {hasMore() && <LoadMore loading={loadingMore()} onLoad={loadMore} auto={!error()} />}
      </section>
    </ContentContainer>
  );
}
