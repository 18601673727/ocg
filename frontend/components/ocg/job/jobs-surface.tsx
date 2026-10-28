"use client";

import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import { useCallback, useEffect, useRef, useState } from "react";
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
import { useI18n } from "../i18n";
import { ContentContainer, ContentHeader } from "../layout/content-container";
import { SELECTED_ROW } from "../primitives/selection";
import { LoadMore } from "../primitives/load-more";

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
  const { t } = useI18n();
  const [query, setQuery] = useState("");
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
  const select = useCallback(
    (next: string) => {
      onJob(next);
    },
    [onJob],
  );

  return (
    <ContentContainer>
      <ContentHeader
        title={t("jobs.title")}
        actions={
          <div className="flex items-center gap-2">
            <Select
              value={status}
              onValueChange={(value) => {
                if (value !== null) setStatus(value as SettlementState | "all");
              }}
            >
              <SelectTrigger aria-label={t("jobs.statusFilter")} className="w-40">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">{t("jobs.statusAll")}</SelectItem>
                <SelectItem value="unsettled">{t("job.state.unsettled")}</SelectItem>
                <SelectItem value="completed">{t("job.activity.completed")}</SelectItem>
                <SelectItem value="failed">{t("job.activity.failed")}</SelectItem>
                <SelectItem value="cancelled">{t("job.activity.cancelled")}</SelectItem>
                <SelectItem value="unknown">{t("job.activity.unknown")}</SelectItem>
              </SelectContent>
            </Select>
            <Input
              aria-label={t("jobs.search")}
              placeholder={t("jobs.search")}
              className="w-64"
              value={query}
              onChange={(event) => {
                setRecords([]);
                setReady(false);
                setHasMore(false);
                setLoadingMore(false);
                setQuery(event.target.value);
              }}
            />
          </div>
        }
      />
      <section className="rounded-lg border border-border bg-card p-5">
        <p className="mb-4 text-sm text-muted-foreground">{t("jobs.description")}</p>
        {error ? (
          <DismissibleNotice message={error} className="mb-3 text-sm text-destructive">
            {error}
          </DismissibleNotice>
        ) : null}
        {!ready ? <p role="status">{t("jobs.loading")}</p> : null}
        {ready && visibleRecords.length === 0 ? (
          <p className="text-sm text-muted-foreground">
            {t(status === "all" ? "jobs.empty" : "jobs.statusEmpty")}
          </p>
        ) : null}
        {status !== "all" && hasMore ? (
          <p className="mb-3 text-xs text-muted-foreground">{t("jobs.filterNote")}</p>
        ) : null}
        {visibleRecords.length > 0 ? (
          <div className="overflow-x-auto rounded-lg border border-border">
            <table className="w-full text-sm">
              <thead className="border-b border-border bg-muted/40 text-xs text-muted-foreground">
                <tr>
                  <th scope="col" className="px-3 py-2 text-left">
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
                    className={cn(job === record.id ? SELECTED_ROW : "hover:bg-muted/30")}
                  >
                    <td className="px-3 py-3">
                      <button
                        type="button"
                        onClick={() => select(record.id)}
                        aria-current={job === record.id ? "true" : undefined}
                        className="text-left focus-visible:outline-2 focus-visible:outline-ring"
                      >
                        <span className="block font-medium break-words">
                          {record.kind === "turn" && record.conversation !== null && record.title
                            ? record.title
                            : (record.name ?? t("jobs.unnamed"))}
                        </span>
                        <span className="block font-mono text-xs text-muted-foreground">
                          {record.id}
                        </span>
                      </button>
                    </td>
                    <td className="whitespace-nowrap px-3 py-3">
                      {record.settlement.state === "unsettled"
                        ? t("job.state.unsettled")
                        : t(`job.activity.${record.settlement.state}`)}
                    </td>
                    <td className="whitespace-nowrap px-3 py-3 text-muted-foreground">
                      <time dateTime={new Date(record.created_at_us / 1000).toISOString()}>
                        {new Date(record.created_at_us / 1000).toLocaleString()}
                      </time>
                    </td>
                    <td className="px-3 py-3">
                      {record.kind === "turn" && record.conversation !== null && onOpenChat ? (
                        <Button
                          type="button"
                          variant="outline"
                          size="xs"
                          className="shrink-0"
                          onClick={() => onOpenChat(record.id)}
                        >
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
