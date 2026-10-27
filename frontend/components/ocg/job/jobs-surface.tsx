"use client";

import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import { useCallback, useEffect, useRef, useState } from "react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Dialog, DialogContent, DialogDescription, DialogTitle } from "@/components/ui/dialog";
import type { HttpCanonicalControlClient } from "../runtime/control-client";
import { JobWatch } from "./job-watch";
import { JobTree } from "./job-tree";
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
  const [records, setRecords] = useState<Awaited<ReturnType<typeof client.readJobs>>["jobs"]>([]);
  const [ready, setReady] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasMore, setHasMore] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const loadMoreRef = useRef<() => void>(() => {});
  // Picking a Job below the tree watches that Job without moving the tree's
  // root; every other change of `job` (list, form, URL) moves the root too.
  const [treeSelection, setTreeSelection] = useState<{ job: string; root: string } | null>(null);
  if (treeSelection !== null && treeSelection.job !== job) setTreeSelection(null);
  const treeRoot = treeSelection?.job === job ? treeSelection.root : job;
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
      setTreeSelection(null);
      onJob(next);
    },
    [onJob],
  );

  return (
    <ContentContainer>
      <ContentHeader
        title={t("jobs.title")}
        actions={
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
        {ready && records.length === 0 ? (
          <p className="text-sm text-muted-foreground">{t("jobs.empty")}</p>
        ) : null}
        <ul className="divide-y divide-border">
          {records.map((record) => (
            <li
              key={record.id}
              className={cn(
                "flex flex-wrap items-center justify-between gap-3 rounded px-2 py-3 text-sm",
                job === record.id ? cn(SELECTED_ROW, "font-medium") : "hover:bg-muted",
              )}
            >
              <button
                type="button"
                onClick={() => select(record.id)}
                aria-current={job === record.id ? "true" : undefined}
                className="flex min-w-0 flex-1 flex-wrap items-center justify-between gap-3 text-left focus-visible:outline-2 focus-visible:outline-ring"
              >
                <span className="min-w-0">
                  <span className="font-mono">{record.id}</span>
                  <span className="ml-3 break-words">
                    {record.kind === "turn" && record.conversation !== null && record.title
                      ? record.title
                      : (record.name ?? t("jobs.unnamed"))}
                  </span>
                </span>
                <span className="flex flex-wrap gap-3 text-xs text-muted-foreground">
                  <span>{record.settlement.state}</span>
                  <time>{new Date(record.created_at_us / 1000).toLocaleString()}</time>
                </span>
              </button>
              {record.kind === "turn" && record.conversation !== null && onOpenChat ? (
                <Button
                  type="button"
                  variant="outline"
                  size="xs"
                  className="shrink-0"
                  onClick={(event) => {
                    event.stopPropagation();
                    onOpenChat(record.id);
                  }}
                >
                  {t("jobs.openInChat")}
                </Button>
              ) : null}
            </li>
          ))}
        </ul>
        {hasMore && <LoadMore loading={loadingMore} onLoad={loadMore} auto={!error} />}
      </section>
      <Dialog
        open={job !== null}
        onOpenChange={(open) => {
          if (!open) onJob(null);
        }}
      >
        <DialogContent
          className="flex h-[calc(100dvh-2rem)] max-h-[calc(100dvh-2rem)] w-[calc(100vw-2rem)] max-w-none flex-col gap-0 overflow-hidden p-0 sm:max-w-none"
          closeButtonClassName="top-2"
        >
          <div className="border-b border-border px-6 py-4 pr-14">
            <DialogTitle>{t("jobs.detailsTitle")}</DialogTitle>
            <DialogDescription className="sr-only">{t("jobs.tree.description")}</DialogDescription>
          </div>
          {job && treeRoot ? (
            <div className="grid min-h-0 flex-1 grid-cols-1 overflow-auto lg:grid-cols-[minmax(18rem,0.8fr)_minmax(0,1.6fr)] lg:overflow-hidden">
              <section className="ocg-subtle-scrollbar min-w-0 border-b border-border p-5 lg:overflow-auto lg:border-r lg:border-b-0">
                <h2 className="mb-2 font-heading text-lg">{t("jobs.tree.title")}</h2>
                <p className="mb-4 text-sm text-muted-foreground">{t("jobs.tree.description")}</p>
                <JobTree
                  key={treeRoot}
                  client={client}
                  root={treeRoot}
                  selected={job}
                  onSelect={(next) => {
                    if (next !== job) setTreeSelection({ job: next, root: treeRoot });
                    onJob(next);
                  }}
                  onReroot={(next) => select(next)}
                  onOpenChat={onOpenChat}
                />
              </section>
              <div className="ocg-subtle-scrollbar min-w-0 p-5 lg:overflow-auto">
                <JobWatch key={job} client={client} jobId={job} bare />
              </div>
            </div>
          ) : null}
        </DialogContent>
      </Dialog>
    </ContentContainer>
  );
}
