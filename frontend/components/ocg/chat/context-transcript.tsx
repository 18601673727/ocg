"use client";

import { useEffect, useState } from "react";
import type { ControlContextStatus } from "../contracts";
import { useI18n } from "../i18n";
import { useOcgRuntime } from "../runtime/runtime-context";
import { Button } from "@/components/ui/button";

type SummaryWork = Extract<ControlContextStatus["work"][number], { kind: "summarization" }>;
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

export function ContextTranscript({ conversation }: { conversation?: string }) {
  const { control } = useOcgRuntime();
  const { t } = useI18n();
  const [pages, setPages] = useState(1);
  const [result, setResult] = useState<{
    work: SummaryWork[];
    more: boolean;
    error: string | null;
  } | null>(null);

  useEffect(() => {
    if (!control || !conversation) return;
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
          const status = await control.readContextStatus(conversation, {
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
          if (firstWorkPage !== null && page >= firstWorkPage + pages - 1) break;
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
    const unsubscribe = control.subscribeCatalog(
      () => void refresh(),
      () => void refresh(),
    );
    void refresh();
    return () => {
      abort.abort();
      unsubscribe();
    };
  }, [control, conversation, pages]);

  if (!conversation || !control || !result || (result.work.length === 0 && !result.error))
    return null;
  return (
    <section className="my-4 text-sm" aria-label={t("chat.contextHistory")}>
      {result.error && (
        <p role="status" className="text-destructive">
          {result.error}
        </p>
      )}
      <ol className="space-y-4">
        {result.work.map((work) => (
          <ContextWork key={work.job} work={work} />
        ))}
      </ol>
      {result.more && (
        <Button variant="ghost" size="sm" onClick={() => setPages((count) => count + 1)}>
          {t("chat.contextHistoryMore")}
        </Button>
      )}
    </section>
  );
}

function ContextWork({ work }: { work: SummaryWork }) {
  const { control } = useOcgRuntime();
  const { t } = useI18n();
  const [open, setOpen] = useState(false);
  const [result, setResult] = useState<{ summary: string; omissions: string[] } | string | null>(
    null,
  );
  const reason = work.prediction
    ? t("chat.compactionPredictive")
    : work.automatic
      ? t("chat.compactionRule")
      : t("chat.compactionManual");
  const canRead = [
    "prepared",
    "adopted",
    "part_ready",
    "discarded",
    "incompatible",
    "unverified",
  ].includes(work.state);
  const stateKey = states[work.state as keyof typeof states];

  useEffect(() => {
    if (!open || !control || !canRead) return;
    const abort = new AbortController();
    void control
      .readLatestOutput(work.job, abort.signal)
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
        } else setResult(t("chat.contextSummaryUnavailable"));
      })
      .catch((error: unknown) => {
        if (!abort.signal.aborted)
          setResult(error instanceof Error ? error.message : String(error));
      });
    return () => abort.abort();
  }, [open, control, canRead, work.job, t]);

  return (
    <li className="text-xs text-muted-foreground">
      <p>
        {t("chat.contextHistory")} · {t("chat.contextWorkReason", { reason })}
      </p>
      <hr className="my-2 border-border" />
      <p>
        {t("chat.contextWorkState", { state: stateKey ? t(stateKey) : work.state })} ·{" "}
        {t("context.sourceRange", { count: work.source.turn_count })} · {work.stage}
      </p>
      {work.failure && <p className="mt-1 text-destructive">{work.failure}</p>}
      {canRead && (
        <details className="mt-2" onToggle={(event) => setOpen(event.currentTarget.open)}>
          <summary className="cursor-pointer">{t("chat.contextSummary")}</summary>
          {result === null ? (
            <p>{t("common.loading")}</p>
          ) : typeof result === "string" ? (
            <p>{result}</p>
          ) : (
            <div className="mt-2 whitespace-pre-wrap break-words">
              <p>{result.summary}</p>
              {result.omissions.length > 0 && (
                <p className="mt-2">
                  {t("chat.contextOmissions")}: {result.omissions.join("; ")}
                </p>
              )}
            </div>
          )}
        </details>
      )}
    </li>
  );
}
