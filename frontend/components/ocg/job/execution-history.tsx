"use client";

import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import type { ExecutionHistory as ExecutionHistoryRecord, ExecutionState } from "../contracts";
import { useI18n } from "../i18n";
import type { ApiClient } from "../runtime/api-client";

type Props = { client: ApiClient; jobId: string; context?: string };
type Query = { after?: string; revision: number };
type Page = {
  query: Query;
  history: ExecutionHistoryRecord;
};

function HistoryBody({ client, jobId, context }: Props) {
  const { t } = useI18n();
  const [query, setQuery] = useState<Query>({ revision: 0 });
  const [page, setPage] = useState<Page | null>(null);
  const [failedQuery, setFailedQuery] = useState<Query | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [snapshot, setSnapshot] = useState<{
    selected: string;
    state: ExecutionState | null;
  } | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    client
      .readExecutionHistory(jobId, {
        after: query.after,
        limit: 20,
        context,
        signal: controller.signal,
      })
      .then(
        (history) => {
          if (!controller.signal.aborted) setPage({ query, history });
        },
        () => {
          if (!controller.signal.aborted) setFailedQuery(query);
        },
      );
    return () => controller.abort();
  }, [client, jobId, context, query]);

  useEffect(() => {
    if (selected === null) return;
    const controller = new AbortController();
    client.readExecutionState(jobId, { atSeq: selected, context, signal: controller.signal }).then(
      (state) => {
        if (!controller.signal.aborted) setSnapshot({ selected, state });
      },
      () => {
        if (!controller.signal.aborted) setSnapshot({ selected, state: null });
      },
    );
    return () => controller.abort();
  }, [client, jobId, context, selected]);

  const currentPage = page?.query === query ? page : null;
  const failed = failedQuery === query;
  const loading = !currentPage && !failed;
  const currentSnapshot = snapshot?.selected === selected ? snapshot : null;

  return (
    <div className="mt-3 flex flex-col gap-3">
      <div className="flex flex-wrap gap-2">
        <Button
          size="sm"
          variant="outline"
          disabled={loading}
          onClick={() => setQuery({ revision: query.revision + 1 })}
        >
          {t("job.executionHistoryReload")}
        </Button>
        {currentPage?.history.next_after ? (
          <Button
            size="sm"
            variant="outline"
            onClick={() =>
              setQuery({ ...query, after: currentPage.history.next_after ?? undefined })
            }
          >
            {t("job.nextEvents")}
          </Button>
        ) : null}
      </div>
      {loading ? <p className="text-xs text-muted-foreground">{t("common.loading")}</p> : null}
      {failed ? (
        <p className="text-xs text-destructive">{t("job.executionHistoryFailed")}</p>
      ) : null}
      {currentPage ? (
        <>
          {currentPage.history.events.length === 0 ? (
            <p className="text-xs text-muted-foreground">{t("job.executionHistoryEmpty")}</p>
          ) : null}
          <ol className="flex flex-col gap-2">
            {currentPage.history.events.map((event) => (
              <li key={event.seq} className="rounded-md border border-border p-3 text-xs">
                <div className="flex flex-wrap items-center gap-2">
                  <code>#{event.seq}</code>
                  <span>{event.kind}</span>
                  <span className="text-muted-foreground">
                    {new Date(event.at_us / 1000).toLocaleString()}
                  </span>
                </div>
                <pre className="my-2 overflow-auto whitespace-pre-wrap break-words font-mono text-muted-foreground">
                  {JSON.stringify(event.facts, null, 2)}
                </pre>
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => setSelected(event.committed_seq)}
                >
                  {t("job.executionSnapshot", { sequence: event.committed_seq })}
                </Button>
              </li>
            ))}
          </ol>
        </>
      ) : null}
      {selected === null ? null : (
        <section className="flex flex-col gap-2 rounded-md bg-muted p-3 text-xs">
          <h3 className="font-medium">{t("job.executionSnapshot", { sequence: selected })}</h3>
          <p className="text-muted-foreground">{t("job.executionSnapshotBoundary")}</p>
          {!currentSnapshot ? (
            <p>{t("common.loading")}</p>
          ) : currentSnapshot.state ? (
            <pre className="overflow-auto whitespace-pre-wrap break-words font-mono">
              {JSON.stringify(currentSnapshot.state, null, 2)}
            </pre>
          ) : (
            <p className="text-destructive">{t("job.executionHistoryFailed")}</p>
          )}
        </section>
      )}
    </div>
  );
}

export function ExecutionHistory(props: Props) {
  const { t } = useI18n();
  const [open, setOpen] = useState(false);
  return (
    <details
      onToggle={(event) => {
        if (event.target === event.currentTarget) setOpen(event.currentTarget.open);
      }}
    >
      <summary className="cursor-pointer text-xs font-semibold tracking-widest uppercase text-muted-foreground">
        {t("job.executionHistory")}
      </summary>
      {open ? <HistoryBody key={props.jobId} {...props} /> : null}
    </details>
  );
}
