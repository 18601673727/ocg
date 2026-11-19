import { createEffect, createMemo, createSignal } from "solid-js";
import { Button } from "@/components/ui/button";
import type { ContextStatus } from "../contracts";
import { useI18n } from "../i18n";
import { useOcgRuntime } from "../runtime/runtime-context";

type SummaryWork = Extract<ContextStatus["work"][number], { kind: "summarization" }>;
const states = {
  queued: "chat.contextStateQueued",
  preparing: "chat.contextStatePreparing",
  prepared: "chat.contextStatePrepared",
  adopted: "chat.contextStateAdopted",
  part_ready: "chat.contextStatePartReady",
  failed: "chat.contextStateFailed",
  cancelled: "chat.contextStateCancelled",
  unknown: "chat.contextStateUnknown",
  discarded: "chat.contextStateDiscarded",
  incompatible: "chat.contextStateIncompatible",
  unverified: "chat.contextStateUnverified",
} as const;

export function ContextTranscript(props: { conversation?: string }) {
  const { api } = useOcgRuntime();
  const i18n = useI18n();
  const [pages, setPages] = createSignal(1);
  const [result, setResult] = createSignal<{
    work: SummaryWork[];
    more: boolean;
    error: string | null;
  } | null>(null);

  createEffect(
    () => [api, props.conversation, pages()],
    () => {
      if (!api || !props.conversation) return;
      const abort = new AbortController();
      let running = false;
      let again = false;
      const refresh = async () => {
        if (running) {
          again = true;
          return;
        }
        running = true;
        try {
          let before: string | undefined;
          const work: SummaryWork[] = [];
          let more = false;
          let firstWorkPage: number | null = null;
          for (let page = 0; ; page++) {
            const status = await api.readContextStatus(props.conversation!, {
              before,
              limit: 50,
              signal: abort.signal,
            });
            work.push(
              ...status.work.filter((item): item is SummaryWork => item.kind === "summarization"),
            );
            if (firstWorkPage === null && work.length > 0) firstWorkPage = page;
            more = status.next_before !== null;
            if (!status.next_before) break;
            if (status.next_before === before) throw new Error("Context cursor did not advance.");
            if (firstWorkPage !== null && page >= firstWorkPage + pages() - 1) break;
            before = status.next_before;
          }
          if (!abort.signal.aborted) setResult({ work, more, error: null });
        } catch (error) {
          if (!abort.signal.aborted)
            setResult((current) => ({
              work: current?.work ?? [],
              more: current?.more ?? false,
              error: error instanceof Error ? error.message : String(error),
            }));
        } finally {
          running = false;
          if (again && !abort.signal.aborted) {
            again = false;
            void refresh();
          }
        }
      };
      const unsubscribe = api.subscribeCatalog(
        () => void refresh(),
        () => void refresh(),
      );
      void refresh();
      return () => {
        abort.abort();
        unsubscribe();
      };
    },
  );

  return (
    <>
      {(() => {
        if (
          !props.conversation ||
          !api ||
          !result() ||
          (result()!.work.length === 0 && !result()!.error)
        )
          return null;
        return (
          <section class="my-4 text-sm" aria-label={i18n.t("chat.contextHistory")}>
            {result()!.error && (
              <p role="status" class="text-destructive">
                {result()!.error}
              </p>
            )}
            <ol class="space-y-4">
              {result()!.work.map((work) => (
                <ContextWork work={work} />
              ))}
            </ol>
            {result()!.more && (
              <Button variant="ghost" size="sm" onClick={() => setPages((count) => count + 1)}>
                {i18n.t("chat.contextHistoryMore")}
              </Button>
            )}
          </section>
        );
      })()}
    </>
  );
}

function ContextWork(props: { work: SummaryWork }) {
  const { api } = useOcgRuntime();
  const i18n = useI18n();
  const [open, setOpen] = createSignal(false);
  const [result, setResult] = createSignal<
    { summary: string; omissions: string[] } | string | null
  >(null);
  const reason = createMemo(() =>
    props.work.prediction
      ? i18n.t("chat.compactionPredictive")
      : props.work.automatic
        ? i18n.t("chat.compactionRule")
        : i18n.t("chat.compactionManual"),
  );
  const canRead = createMemo(() =>
    ["prepared", "adopted", "part_ready", "discarded", "incompatible", "unverified"].includes(
      props.work.state,
    ),
  );
  const stateKey = createMemo(() => states[props.work.state as keyof typeof states]);

  createEffect(
    () => [open(), api, canRead(), props.work.job, i18n.t],
    () => {
      if (!open() || !api || !canRead()) return;
      const abort = new AbortController();
      void api
        .readLatestOutput(props.work.job, abort.signal)
        .then((output) => {
          if (abort.signal.aborted) return;
          const value = output?.value;
          if (
            value &&
            typeof value === "object" &&
            "summary" in value &&
            typeof value.summary === "string"
          ) {
            const omissions =
              "omitted_details" in value && Array.isArray(value.omitted_details)
                ? value.omitted_details.filter((item): item is string => typeof item === "string")
                : [];
            setResult({ summary: value.summary, omissions });
          } else setResult(i18n.t("chat.contextSummaryUnavailable"));
        })
        .catch((error: unknown) => {
          if (!abort.signal.aborted)
            setResult(error instanceof Error ? error.message : String(error));
        });
      return () => abort.abort();
    },
  );

  return (
    <li class="text-xs text-muted-foreground">
      <p>
        {i18n.t("chat.contextHistory")} · {i18n.t("chat.contextWorkReason", { reason: reason() })}
      </p>
      <hr class="my-2 border-border" />
      <p>
        {i18n.t("chat.contextWorkState", {
          state: stateKey() ? i18n.t(stateKey()) : props.work.state,
        })}{" "}
        · {i18n.t("context.sourceRange", { count: props.work.source.turn_count })} ·{" "}
        {props.work.stage}
      </p>
      {props.work.failure && <p class="mt-1 text-destructive">{props.work.failure}</p>}
      {canRead() && (
        <details class="mt-2" onToggle={(event) => setOpen(event.currentTarget.open)}>
          <summary class="cursor-pointer">{i18n.t("chat.contextSummary")}</summary>
          {(() => {
            const value = result();
            if (value === null) return <p>{i18n.t("common.loading")}</p>;
            if (typeof value === "string") return <p>{value}</p>;
            return (
              <div class="mt-2 whitespace-pre-wrap break-words">
                <p>{value.summary}</p>
                {value.omissions.length > 0 && (
                  <p class="mt-2">
                    {i18n.t("chat.contextOmissions")}: {value.omissions.join("; ")}
                  </p>
                )}
              </div>
            );
          })()}
        </details>
      )}
    </li>
  );
}
