import { createEffect, createMemo, createSignal } from "solid-js";
import { Button } from "@/components/ui/button";
import type { ExecutionHistory, ExecutionState } from "../contracts";
import { useI18n } from "../i18n";
import type { ApiClient } from "../runtime/api-client";

type Props = { client: ApiClient; jobId: string; context?: string };
type Query = { after?: string; revision: number };
type Page = {
  query: Query;
  history: ExecutionHistory;
};

function HistoryBody(props: Props) {
  const i18n = useI18n();
  const [query, setQuery] = createSignal<Query>({ revision: 0 });
  const [page, setPage] = createSignal<Page | null>(null);
  const [failedQuery, setFailedQuery] = createSignal<Query | null>(null);
  const [selected, setSelected] = createSignal<string | null>(null);
  const [snapshot, setSnapshot] = createSignal<{
    selected: string;
    state: ExecutionState | null;
  } | null>(null);

  createEffect(
    () => [props.client, props.jobId, props.context, query()],
    () => {
      const controller = new AbortController();
      props.client
        .readExecutionHistory(props.jobId, {
          after: query().after,
          limit: 20,
          context: props.context,
          signal: controller.signal,
        })
        .then(
          (history) => {
            if (!controller.signal.aborted) setPage({ query: query(), history });
          },
          () => {
            if (!controller.signal.aborted) setFailedQuery(query());
          },
        );
      return () => controller.abort();
    },
  );

  createEffect(
    () => [props.client, props.jobId, props.context, selected()],
    () => {
      if (selected() === null) return;
      const controller = new AbortController();
      props.client
        .readExecutionState(props.jobId, {
          atSeq: selected()!,
          context: props.context,
          signal: controller.signal,
        })
        .then(
          (state) => {
            if (!controller.signal.aborted) setSnapshot({ selected: selected()!, state });
          },
          () => {
            if (!controller.signal.aborted) setSnapshot({ selected: selected()!, state: null });
          },
        );
      return () => controller.abort();
    },
  );

  const currentPage = createMemo(() => (page()?.query === query() ? page() : null));
  const failed = createMemo(() => failedQuery() === query());
  const loading = createMemo(() => !currentPage() && !failed());
  const currentSnapshot = createMemo(() =>
    snapshot()?.selected === selected() ? snapshot() : null,
  );

  return (
    <div class="mt-3 flex flex-col gap-3">
      <div class="flex flex-wrap gap-2">
        <Button
          size="sm"
          variant="outline"
          disabled={loading()}
          onClick={() => setQuery({ revision: query().revision + 1 })}
        >
          {i18n.t("job.executionHistoryReload")}
        </Button>
        {currentPage()?.history.next_after ? (
          <Button
            size="sm"
            variant="outline"
            onClick={() =>
              setQuery({ ...query(), after: currentPage()!.history.next_after ?? undefined })
            }
          >
            {i18n.t("job.nextEvents")}
          </Button>
        ) : null}
      </div>
      {loading() ? <p class="text-xs text-muted-foreground">{i18n.t("common.loading")}</p> : null}
      {failed() ? (
        <p class="text-xs text-destructive">{i18n.t("job.executionHistoryFailed")}</p>
      ) : null}
      {currentPage() ? (
        <>
          {currentPage()!.history.events.length === 0 ? (
            <p class="text-xs text-muted-foreground">{i18n.t("job.executionHistoryEmpty")}</p>
          ) : null}
          <ol class="flex flex-col gap-2">
            {currentPage()!.history.events.map((event) => (
              <li class="rounded-md border border-border p-3 text-xs">
                <div class="flex flex-wrap items-center gap-2">
                  <code>#{event.seq}</code>
                  <span>{event.kind}</span>
                  <span class="text-muted-foreground">
                    {new Date(event.at_us / 1000).toLocaleString()}
                  </span>
                </div>
                <pre class="my-2 overflow-auto whitespace-pre-wrap break-words font-mono text-muted-foreground">
                  {JSON.stringify(event.facts, null, 2)}
                </pre>
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => setSelected(event.committed_seq)}
                >
                  {i18n.t("job.executionSnapshot", { sequence: event.committed_seq })}
                </Button>
              </li>
            ))}
          </ol>
        </>
      ) : null}
      {selected() === null ? null : (
        <section class="flex flex-col gap-2 rounded-md bg-muted p-3 text-xs">
          <h3 class="font-medium">{i18n.t("job.executionSnapshot", { sequence: selected()! })}</h3>
          <p class="text-muted-foreground">{i18n.t("job.executionSnapshotBoundary")}</p>
          {!currentSnapshot() ? (
            <p>{i18n.t("common.loading")}</p>
          ) : currentSnapshot()!.state ? (
            <pre class="overflow-auto whitespace-pre-wrap break-words font-mono">
              {JSON.stringify(currentSnapshot()!.state, null, 2)}
            </pre>
          ) : (
            <p class="text-destructive">{i18n.t("job.executionHistoryFailed")}</p>
          )}
        </section>
      )}
    </div>
  );
}

export function ExecutionHistory(props: Props) {
  const i18n = useI18n();
  const [open, setOpen] = createSignal(false);
  return (
    <details
      onToggle={(event) => {
        if (event.target === event.currentTarget) setOpen(event.currentTarget.open);
      }}
    >
      <summary class="cursor-pointer text-xs font-semibold tracking-widest uppercase text-muted-foreground">
        {i18n.t("job.executionHistory")}
      </summary>
      {open() ? <HistoryBody {...props} /> : null}
    </details>
  );
}
