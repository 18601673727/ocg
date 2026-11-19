import { MessageSquare, Search } from "@lucide/solid";
import { createEffect, createMemo, createSignal } from "solid-js";
import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectItem } from "@/components/ui/select";
import { bindElementRef, elementRef } from "@/lib/ui-state";
import { cn } from "@/lib/utils";
import type { ExecutionPhase } from "../contracts";
import { useI18n } from "../i18n";
import { ContentContainer, ContentHeader } from "../layout/content-container";
import { ListStatus } from "../primitives/list-status";
import { LoadMore } from "../primitives/load-more";
import { Pill } from "../primitives/pill";
import { SELECTED_ROW } from "../primitives/selection";
import type { ApiClient } from "../runtime/api-client";
import { EXECUTION_PHASE_KEYS, EXECUTION_PHASE_TONES } from "../runtime/job-status";

export function JobsSurface(props: {
  client: ApiClient;
  job: string | null;
  active: boolean;
  onJob: (job: string | null) => void;
  onOpenChat?: (job: string) => void;
}) {
  const i18n = useI18n();
  const dateTime = createMemo(
    () => new Intl.DateTimeFormat(i18n.locale, { dateStyle: "medium", timeStyle: "medium" }),
  );
  const [query, setQuery] = createSignal("");
  const searchInput = elementRef<HTMLInputElement>(null);
  const [status, setStatus] = createSignal<ExecutionPhase | "all">("all");
  const [records, setRecords] = createSignal<
    Awaited<ReturnType<typeof props.client.readJobs>>["jobs"]
  >([]);
  const visibleRecords = createMemo(() =>
    status() === "all"
      ? records()
      : records().filter((record) => record.execution_phase === status()),
  );
  const [ready, setReady] = createSignal(false);
  const [error, setError] = createSignal<string | null>(null);
  const [hasMore, setHasMore] = createSignal(false);
  const [loadingMore, setLoadingMore] = createSignal(false);
  const loadMoreRef = elementRef<() => void>(() => {});
  createEffect(
    () => [props.active, props.client, query()],
    () => {
      if (!props.active) return;
      const controller = new AbortController();
      const pages: (string | undefined)[] = [undefined];
      let next: string | null = null;
      let busy = false;
      let queued = false;
      let timer: ReturnType<typeof setTimeout> | undefined;
      const known = new Set<string>();
      const merge = (incoming: ReturnType<typeof records>) => {
        incoming.forEach((item) => known.add(item.id));
        setRecords((current) => {
          const byId = new Map(current.map((item) => [item.id, item]));
          incoming.forEach((item) => byId.set(item.id, item));
          return [...byId.values()].sort((a, b) => b.id.localeCompare(a.id));
        });
      };
      const fail = (cause: unknown) => {
        if (!controller.signal.aborted) {
          setError(cause instanceof Error ? cause.message : String(cause));
          setReady(true);
        }
      };
      const refresh = async () => {
        if (busy) {
          queued = true;
          return;
        }
        busy = true;
        setLoadingMore(true);
        try {
          const refreshedPages = await Promise.all(
            pages.map((before) =>
              props.client.readJobs({
                before,
                query: query(),
                limit: 50,
                signal: controller.signal,
              }),
            ),
          );
          if (controller.signal.aborted) return;
          const incoming = refreshedPages.flatMap((page) => page.jobs);
          next = refreshedPages.at(-1)?.next_before ?? null;
          const refreshed = new Set(incoming.map((item) => item.id));
          const displaced = [...known].filter((id) => !refreshed.has(id));
          for (let index = 0; index < displaced.length; index += 8) {
            incoming.push(
              ...(await Promise.all(
                displaced
                  .slice(index, index + 8)
                  .map((id) => props.client.readJob(id, controller.signal)),
              )),
            );
            if (controller.signal.aborted) return;
          }
          merge(incoming);
          setHasMore(next !== null);
          setError(null);
          setReady(true);
        } catch (cause) {
          fail(cause);
        } finally {
          busy = false;
          if (!controller.signal.aborted) setLoadingMore(false);
          if (queued && !controller.signal.aborted) {
            queued = false;
            void refresh();
          }
        }
      };
      loadMoreRef.current = () => {
        if (busy || !next) return;
        const before = next;
        busy = true;
        setLoadingMore(true);
        void props.client
          .readJobs({ before, query: query(), limit: 50, signal: controller.signal })
          .then((page) => {
            if (controller.signal.aborted) return;
            pages.push(before);
            next = page.next_before;
            merge(page.jobs);
            setHasMore(next !== null);
            setError(null);
          })
          .catch(fail)
          .finally(() => {
            busy = false;
            if (!controller.signal.aborted) setLoadingMore(false);
            if (queued && !controller.signal.aborted) {
              queued = false;
              void refresh();
            }
          });
      };
      const stop = props.client.subscribeCatalog(() => {
        if (!timer)
          timer = setTimeout(() => {
            timer = undefined;
            void refresh();
          }, 300);
      }, fail);
      return () => {
        controller.abort();
        stop();
        if (timer) clearTimeout(timer);
      };
    },
  );
  const loadMore = () => {
    loadMoreRef.current();
  };
  function updateQuery(value: string) {
    if (query() === value) return;
    setRecords([]);
    setReady(false);
    setError(null);
    setHasMore(false);
    setLoadingMore(false);
    setQuery(value);
  }
  const filtered = createMemo(() => status() !== "all" || query().length > 0);
  const options = [
    { value: "all" as const, label: i18n.t("jobs.statusAll") },
    ...(Object.keys(EXECUTION_PHASE_KEYS) as ExecutionPhase[]).map((value) => ({
      value,
      label: i18n.t(EXECUTION_PHASE_KEYS[value]),
    })),
  ];

  return (
    <ContentContainer>
      <ContentHeader title={i18n.t("jobs.title")} description={i18n.t("jobs.description")} />
      <section
        aria-label={i18n.t("jobs.title")}
        class="min-w-0 overflow-hidden rounded-xl border border-border bg-card"
      >
        <div class="flex flex-wrap items-center gap-2 border-b border-border p-3 sm:px-5 sm:py-4">
          <div class="relative min-w-36 flex-1 sm:max-w-sm">
            <Search
              aria-hidden="true"
              class="pointer-events-none absolute top-1/2 left-3 size-4 -translate-y-1/2 text-muted-foreground"
            />
            <Input
              ref={bindElementRef(searchInput)}
              type="search"
              aria-label={i18n.t("jobs.search")}
              placeholder={i18n.t("jobs.search")}
              class="h-9 bg-background pl-9"
              value={query()}
              onInput={(event) => updateQuery(event.target.value)}
            />
          </div>
          <Select<ExecutionPhase | "all">
            value={status()}
            items={options}
            onValueChange={(value) => {
              if (value !== null) setStatus(value);
            }}
            aria-label={i18n.t("jobs.statusFilter")}
            class="gap-2 border-input bg-background"
          >
            {options.map((option) => (
              <SelectItem value={option.value}>{option.label}</SelectItem>
            ))}
          </Select>
          {filtered() && (
            <Button
              type="button"
              variant="ghost"
              size="sm"
              onClick={() => {
                updateQuery("");
                setStatus("all");
                searchInput.current?.focus();
              }}
            >
              {i18n.t("common.reset")}
            </Button>
          )}
        </div>
        {error() ? (
          <DismissibleNotice message={error()!} class="m-4 text-sm text-destructive">
            {error()}
          </DismissibleNotice>
        ) : null}
        {status() !== "all" && hasMore() ? (
          <p class="border-b border-border bg-muted/30 px-5 py-2 text-xs text-muted-foreground">
            {i18n.t("jobs.filterNote")}
          </p>
        ) : null}
        {!ready() && !error() ? <ListStatus loading message={i18n.t("jobs.loading")} /> : null}
        {ready() && !error() && visibleRecords().length === 0 ? (
          <ListStatus message={i18n.t(filtered() ? "jobs.statusEmpty" : "jobs.empty")} />
        ) : null}
        {visibleRecords().length > 0 ? (
          <div class="overflow-x-auto">
            <table class="w-full text-sm">
              <thead class="border-b border-border bg-muted/50 text-xs text-muted-foreground [&_th]:font-medium">
                <tr>
                  <th scope="col" class="py-2.5 pr-3 pl-5 text-left">
                    {i18n.t("jobs.columnJob")}
                  </th>
                  <th scope="col" class="px-3 py-2 text-left">
                    {i18n.t("jobs.columnStatus")}
                  </th>
                  <th scope="col" class="whitespace-nowrap px-3 py-2 text-left">
                    {i18n.t("jobs.columnCreated")}
                  </th>
                  <th scope="col" class="px-3 py-2 text-left">
                    {i18n.t("jobs.columnActions")}
                  </th>
                </tr>
              </thead>
              <tbody class="divide-y divide-border">
                {visibleRecords().map((record) => (
                  <tr
                    class={cn(
                      "transition-colors",
                      props.job === record.id ? SELECTED_ROW : "hover:bg-muted/30",
                    )}
                  >
                    <td class="py-3 pr-3 pl-5">
                      <button
                        type="button"
                        onClick={() => props.onJob(record.id)}
                        aria-current={props.job === record.id ? "true" : undefined}
                        class="group block w-full rounded-sm text-left focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring"
                      >
                        <span class="block min-w-32 font-medium break-words decoration-primary underline-offset-4 group-hover:underline group-focus-visible:underline">
                          {record.kind === "turn" && record.conversation !== null && record.title
                            ? record.title
                            : (record.name ?? i18n.t("jobs.unnamed"))}
                        </span>
                        <span class="mt-1 block font-mono text-xs text-muted-foreground">
                          {record.id}
                        </span>
                      </button>
                    </td>
                    <td class="whitespace-nowrap px-3 py-3">
                      <Pill tone={EXECUTION_PHASE_TONES[record.execution_phase]} dot>
                        {i18n.t(EXECUTION_PHASE_KEYS[record.execution_phase])}
                      </Pill>
                    </td>
                    <td class="whitespace-nowrap px-3 py-3 text-xs tabular-nums text-muted-foreground">
                      <time datetime={new Date(record.created_at_us / 1000).toISOString()}>
                        {dateTime().format(record.created_at_us / 1000)}
                      </time>
                    </td>
                    <td class="px-3 py-3">
                      {record.kind === "turn" &&
                      record.conversation !== null &&
                      props.onOpenChat ? (
                        <Button
                          type="button"
                          variant="outline"
                          size="sm"
                          class="shrink-0"
                          onClick={() =>
                            record.conversation !== null && props!.onOpenChat!(record.conversation)
                          }
                        >
                          <MessageSquare class="size-3.5" aria-hidden="true" />
                          {i18n.t("jobs.openInChat")}
                        </Button>
                      ) : null}
                    </td>
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
