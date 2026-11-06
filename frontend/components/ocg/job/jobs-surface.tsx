"use client";

import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ListFilter, MessageSquare, Search } from "lucide-react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type { SettlementState } from "../contracts/api";
import type { HttpCanonicalControlClient } from "../runtime/control-client";
import { useI18n, type I18nKey } from "../i18n";
import { ContentContainer, ContentHeader } from "../layout/content-container";
import { SELECTED_ROW } from "../primitives/selection";
import { LoadMore } from "../primitives/load-more";
import { ListStatus } from "../primitives/list-status";
import { Pill } from "../primitives/pill";
import type { Tone } from "../primitives/tone";

const settlement = {
  unsettled: { label: "job.state.unsettled", tone: "violet" },
  completed: { label: "job.activity.completed", tone: "emerald" },
  failed: { label: "job.activity.failed", tone: "red" },
  cancelled: { label: "job.activity.cancelled", tone: "slate" },
  unknown: { label: "job.activity.unknown", tone: "slate" },
} as const satisfies Record<SettlementState, { label: I18nKey; tone: Tone }>;

export function JobsSurface({
  client,
  job,
  active,
  onJob,
  onOpenChat,
}: {
  client: HttpCanonicalControlClient;
  job: string | null;
  active: boolean;
  onJob: (job: string | null) => void;
  onOpenChat?: (job: string) => void;
}) {
  const { t, locale } = useI18n();
  const dateTime = useMemo(
    () => new Intl.DateTimeFormat(locale, { dateStyle: "medium", timeStyle: "medium" }),
    [locale],
  );
  const [query, setQuery] = useState("");
  const searchInput = useRef<HTMLInputElement>(null);
  const [status, setStatus] = useState<SettlementState | "all">("all");
  const [records, setRecords] = useState<Awaited<ReturnType<typeof client.readJobs>>["jobs"]>([]);
  const visibleRecords =
    status === "all" ? records : records.filter((record) => record.settlement.state === status);
  const [ready, setReady] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasMore, setHasMore] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const loadMoreRef = useRef<() => void>(() => {});
  useEffect(() => {
    if (!active) return;
    const controller = new AbortController();
    const pages: (string | undefined)[] = [undefined];
    let next: string | null = null;
    let busy = false;
    let queued = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const known = new Set<string>();
    const merge = (incoming: typeof records) => {
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
            client.readJobs({ before, query, limit: 50, signal: controller.signal }),
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
              displaced.slice(index, index + 8).map((id) => client.readJob(id, controller.signal)),
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
      void client
        .readJobs({ before, query, limit: 50, signal: controller.signal })
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
    const stop = client.subscribeCatalog(() => {
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
  }, [active, client, query]);
  const loadMore = useCallback(() => {
    loadMoreRef.current();
  }, []);
  function updateQuery(value: string) {
    if (query === value) return;
    setRecords([]);
    setReady(false);
    setError(null);
    setHasMore(false);
    setLoadingMore(false);
    setQuery(value);
  }
  const filtered = status !== "all" || query.length > 0;
  const options = [
    { value: "all" as const, label: t("jobs.statusAll") },
    ...Object.entries(settlement).map(([value, { label }]) => ({
      value: value as SettlementState,
      label: t(label),
    })),
  ];

  return (
    <ContentContainer>
      <ContentHeader title={t("jobs.title")} description={t("jobs.description")} />
      <section
        aria-label={t("jobs.title")}
        className="min-w-0 overflow-hidden rounded-xl border border-border bg-card"
      >
        <div className="flex flex-wrap items-center gap-2 border-b border-border p-3 sm:px-5 sm:py-4">
          <div className="relative min-w-36 flex-1 sm:max-w-sm">
            <Search
              aria-hidden="true"
              className="pointer-events-none absolute top-1/2 left-3 size-4 -translate-y-1/2 text-muted-foreground"
            />
            <Input
              ref={searchInput}
              type="search"
              aria-label={t("jobs.search")}
              placeholder={t("jobs.search")}
              className="h-9 bg-background pl-9"
              value={query}
              onChange={(event) => updateQuery(event.target.value)}
            />
          </div>
          <Select<SettlementState | "all">
            value={status}
            items={options}
            onValueChange={(value) => {
              if (value !== null) setStatus(value);
            }}
          >
            <SelectTrigger
              aria-label={t("jobs.statusFilter")}
              className="gap-2 border-input bg-background"
            >
              <ListFilter aria-hidden="true" className="size-4" />
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {options.map((option) => (
                <SelectItem key={option.value} value={option.value}>
                  {option.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          {filtered && (
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
              {t("common.reset")}
            </Button>
          )}
        </div>
        {error ? (
          <DismissibleNotice message={error} className="m-4 text-sm text-destructive">
            {error}
          </DismissibleNotice>
        ) : null}
        {status !== "all" && hasMore ? (
          <p className="border-b border-border bg-muted/30 px-5 py-2 text-xs text-muted-foreground">
            {t("jobs.filterNote")}
          </p>
        ) : null}
        {!ready && !error ? <ListStatus loading message={t("jobs.loading")} /> : null}
        {ready && !error && visibleRecords.length === 0 ? (
          <ListStatus message={t(filtered ? "jobs.statusEmpty" : "jobs.empty")} />
        ) : null}
        {visibleRecords.length > 0 ? (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="border-b border-border bg-muted/50 text-xs text-muted-foreground [&_th]:font-medium">
                <tr>
                  <th scope="col" className="py-2.5 pr-3 pl-5 text-left">
                    {t("jobs.columnJob")}
                  </th>
                  <th scope="col" className="px-3 py-2 text-left">
                    {t("jobs.columnStatus")}
                  </th>
                  <th scope="col" className="whitespace-nowrap px-3 py-2 text-left">
                    {t("jobs.columnCreated")}
                  </th>
                  <th scope="col" className="px-3 py-2 text-left">
                    {t("jobs.columnActions")}
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {visibleRecords.map((record) => (
                  <tr
                    key={record.id}
                    className={cn(
                      "transition-colors",
                      job === record.id ? SELECTED_ROW : "hover:bg-muted/30",
                    )}
                  >
                    <td className="py-3 pr-3 pl-5">
                      <button
                        type="button"
                        onClick={() => onJob(record.id)}
                        aria-current={job === record.id ? "true" : undefined}
                        className="group block w-full rounded-sm text-left focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring"
                      >
                        <span className="block min-w-32 font-medium break-words decoration-primary underline-offset-4 group-hover:underline group-focus-visible:underline">
                          {record.kind === "turn" && record.conversation !== null && record.title
                            ? record.title
                            : (record.name ?? t("jobs.unnamed"))}
                        </span>
                        <span className="mt-1 block font-mono text-xs text-muted-foreground">
                          {record.id}
                        </span>
                      </button>
                    </td>
                    <td className="whitespace-nowrap px-3 py-3">
                      <Pill tone={settlement[record.settlement.state].tone} dot>
                        {t(settlement[record.settlement.state].label)}
                      </Pill>
                    </td>
                    <td className="whitespace-nowrap px-3 py-3 text-xs tabular-nums text-muted-foreground">
                      <time dateTime={new Date(record.created_at_us / 1000).toISOString()}>
                        {dateTime.format(record.created_at_us / 1000)}
                      </time>
                    </td>
                    <td className="px-3 py-3">
                      {record.kind === "turn" && record.conversation !== null && onOpenChat ? (
                        <Button
                          type="button"
                          variant="outline"
                          size="sm"
                          className="shrink-0"
                          onClick={() => onOpenChat(record.id)}
                        >
                          <MessageSquare className="size-3.5" aria-hidden="true" />
                          {t("jobs.openInChat")}
                        </Button>
                      ) : null}
                    </td>
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
