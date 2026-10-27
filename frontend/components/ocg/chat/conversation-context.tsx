"use client";

import type { ControlContextStatus } from "../contracts";

import Link from "next/link";

import { useEffect, useRef, useState } from "react";
import { useOcgRuntime } from "../runtime/runtime-context";
import type { ControlDecisionAnswer, ControlProfilesSnapshot } from "../contracts";
import { describeError } from "../settings/settings-parts";
import { useI18n } from "../i18n";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from "@/components/ui/dialog";
import { DismissibleNotice } from "../primitives/dismissible-notice";

type Status = ControlContextStatus;

function formatShare(value: number): string {
  return Number.isInteger(value)
    ? String(value)
    : value.toFixed(4).replace(/0+$/, "").replace(/\.$/, "");
}

/** Presents a Decision answer plainly; provider confidence never reads as correctness. */
function describeDecisionAnswer(
  answer: ControlDecisionAnswer,
  t: ReturnType<typeof useI18n>["t"],
): string {
  if (answer.kind === "choice") {
    const parts = [t("context.predictionChoice", { selected: answer.selected })];
    if (answer.distribution) {
      parts.push(
        t("context.predictionDistribution", {
          values: Object.entries(answer.distribution)
            .map(([key, value]) => `${key}: ${formatShare(value)}`)
            .join(", "),
        }),
      );
    }
    if (answer.confidence) {
      parts.push(
        t("context.providerConfidence", {
          value: formatShare(answer.confidence.value),
          semantics: answer.confidence.semantics,
        }),
      );
    }
    return parts.join(" · ");
  }
  if (answer.kind === "binary_probability") {
    return t("context.predictionBinary", { value: formatShare(answer.true_probability) });
  }
  const parts = [t("context.predictionExpectation", { value: formatShare(answer.expectation) })];
  if (answer.distribution) {
    parts.push(
      t("context.predictionDistribution", {
        values: answer.distribution.map(formatShare).join(", "),
      }),
    );
  }
  if (answer.confidence) {
    parts.push(
      t("context.providerConfidence", {
        value: formatShare(answer.confidence.value),
        semantics: answer.confidence.semantics,
      }),
    );
  }
  return parts.join(" · ");
}
export function ConversationContext({ conversation }: { conversation: string }) {
  const { control } = useOcgRuntime();
  const { t } = useI18n();
  const [open, setOpen] = useState(false);
  const [profiles, setProfiles] = useState<ControlProfilesSnapshot>();
  const [status, setStatus] = useState<Status>();
  const [model, setModel] = useState("");
  const [error, setError] = useState<string>();
  const [notice, setNotice] = useState<string>();
  const [busy, setBusy] = useState(false);
  const [retrying, setRetrying] = useState(false);
  const [refresh, setRefresh] = useState(0);
  const [older, setOlder] = useState<Status["work"]>([]);
  const [before, setBefore] = useState<string | null>(null);
  const request = useRef<{ model: string; key: string } | null>(null);
  const initialized = useRef(false);
  useEffect(() => {
    if (!open || !control) return;
    const controller = new AbortController();
    let reading = false;
    async function tick() {
      if (reading) return;
      reading = true;
      try {
        const state = await control!.readContextStatus(conversation, { signal: controller.signal });
        if (controller.signal.aborted) return;
        setStatus(state);
        if (!initialized.current) {
          setModel(state.summarizer ?? "");
          setBefore(state.next_before);
          initialized.current = true;
        }
      } catch (cause) {
        if (!controller.signal.aborted) setError(describeError(cause, t));
      } finally {
        reading = false;
      }
    }
    void control.readProfiles(controller.signal).then(
      (value) => {
        if (!controller.signal.aborted) setProfiles(value);
      },
      (cause) => {
        if (!controller.signal.aborted) setError(describeError(cause, t));
      },
    );
    const unsubscribe = control.subscribeCatalog(
      () => void tick(),
      (cause) => {
        if (!controller.signal.aborted) setError(describeError(cause, t));
      },
    );
    void tick();
    return () => {
      controller.abort();
      unsubscribe();
    };
  }, [open, control, conversation, refresh, t]);
  async function act(action: () => Promise<void>) {
    setBusy(true);
    setError(undefined);
    setNotice(undefined);
    try {
      await action();
      setRefresh((value) => value + 1);
    } catch (cause) {
      setError(describeError(cause, t));
    } finally {
      setBusy(false);
    }
  }
  const work = [
    ...(status?.work ?? []),
    ...older.filter((work) => !status?.work.some((current) => current.job === work.job)),
  ];
  return (
    <>
      <Button
        type="button"
        variant="outline"
        size="sm"
        disabled={!control}
        onClick={() => setOpen(true)}
      >
        {t("context.title")}
      </Button>
      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent className="flex max-h-[90dvh] flex-col overflow-hidden sm:max-w-xl">
          <DialogHeader>
            <DialogTitle>{t("context.title")}</DialogTitle>
            <DialogDescription>{t("context.manualHint")}</DialogDescription>
          </DialogHeader>
          <ScrollArea
            className="min-h-0 flex-1"
            viewportProps={{ className: "ocg-subtle-scrollbar" }}
            contentProps={{ className: "space-y-6" }}
          >
            {error && <DismissibleNotice message={error} />}
            {notice && (
              <p role="status" className="text-sm">
                {notice}
              </p>
            )}
            {status && (
              <div className="space-y-2 text-xs text-muted-foreground">
                <p>
                  {t("context.usage", {
                    tokens: status.estimated_tokens ?? t("context.unknown"),
                    capacity: status.foreground_model?.capacity ?? t("context.unknown"),
                    mode: status.mode,
                    source: status.policy_source,
                  })}
                </p>
                <p>{t("context.preparedMode", { mode: status.prediction_mode })}</p>
                {status.predictive_unavailable && status.predictive && (
                  <p>{status.predictive_unavailable}</p>
                )}
              </div>
            )}
            {profiles && status && (
              <>
                <Link href="/settings/system" className="text-sm underline">
                  {t("context.globalTitle")}
                </Link>
                <div className="space-y-2 border-t border-border pt-3">
                  <label className="grid gap-1 text-xs">
                    {t("context.summarizer")}
                    <Select
                      disabled={busy}
                      value={model}
                      items={[
                        { value: "", label: t("context.summarizer") },
                        ...profiles.llm_profiles.map((model) => ({
                          value: model.id,
                          label: model.name,
                        })),
                      ]}
                      onValueChange={(next) => {
                        if (next === null) return;
                        setModel(next);
                        request.current = null;
                        setRetrying(false);
                      }}
                    >
                      <SelectTrigger className="min-h-10 w-full rounded-md bg-background px-2 text-sm">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="">{t("context.summarizer")}</SelectItem>
                        {profiles.llm_profiles.map((model) => (
                          <SelectItem key={model.id} value={model.id}>
                            {model.name}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </label>
                  <Button
                    disabled={busy || !model}
                    variant="outline"
                    onClick={() =>
                      void act(async () => {
                        if (!request.current || request.current.model !== model)
                          request.current = { model, key: crypto.randomUUID() };
                        setRetrying(true);
                        const result = await control!.compactConversation(
                          conversation,
                          model,
                          request.current.key,
                        );
                        request.current = null;
                        setRetrying(false);
                        setNotice(result.reason ?? t("context.queued"));
                      })
                    }
                  >
                    {busy
                      ? t("context.saving")
                      : retrying
                        ? t("context.retry")
                        : t("context.manual")}
                  </Button>
                </div>
              </>
            )}
            {work.length > 0 && (
              <ol className="space-y-2 border-t border-border pt-3">
                {work.map((work) => (
                  <li
                    key={work.job}
                    className="space-y-1 rounded-md bg-muted p-2 text-xs [overflow-wrap:anywhere]"
                  >
                    <p>
                      {work.kind === "summarization"
                        ? `${t("context.summarizer")} · ${work.stage}`
                        : t("context.predictor")}{" "}
                      · {work.state}
                    </p>
                    {work.kind === "summarization" && work.failure && (
                      <p className="text-destructive">{work.failure}</p>
                    )}
                    <p className="font-mono text-muted-foreground">{work.job}</p>
                    {work.kind === "summarization" && (
                      <>
                        <p>
                          {t("context.sourceRange", { count: work.source.turn_count })} ·{" "}
                          {work.source.first.turn} → {work.source.through.turn}
                        </p>
                        {work.prediction && (
                          <p>
                            {t("context.reservation", {
                              requests: work.reserved_requests,
                              tokens: work.reserved_tokens,
                            })}
                          </p>
                        )}
                      </>
                    )}
                    {work.kind === "prediction" && work.result && (
                      <>
                        {work.result.outcome.kind === "answer" ? (
                          <p>
                            {t("context.predictionAnswer")} ·{" "}
                            {describeDecisionAnswer(work.result.outcome.answer, t)}
                          </p>
                        ) : (
                          <p className="text-muted-foreground">
                            {t(
                              work.result.outcome.kind === "uncertain"
                                ? "context.predictionUncertain"
                                : "context.predictionUnusable",
                            )}{" "}
                            · {work.result.outcome.reason}
                          </p>
                        )}
                        {work.result.reported_model &&
                          work.result.reported_model !== work.result.requested_model && (
                            <p className="text-muted-foreground">
                              {t("context.predictionReportedModel", {
                                model: work.result.reported_model,
                              })}
                            </p>
                          )}
                      </>
                    )}
                    {work.kind === "summarization" &&
                      ["queued", "preparing", "prepared", "part_ready"].includes(work.state) && (
                        <Button
                          size="sm"
                          variant="ghost"
                          disabled={busy}
                          onClick={() =>
                            void act(async () => {
                              await control!.stopConversation(work.conversation);
                            })
                          }
                        >
                          {t("context.stop")}
                        </Button>
                      )}
                  </li>
                ))}
              </ol>
            )}
            {before && (
              <Button
                variant="outline"
                disabled={busy}
                onClick={() =>
                  void act(async () => {
                    const page = await control!.readContextStatus(conversation, { before });
                    if (page.next_before && page.next_before >= before)
                      throw new Error("Context cursor did not advance");
                    setOlder((items) => [...items, ...page.work]);
                    setBefore(page.next_before);
                  })
                }
              >
                {t("context.more")}
              </Button>
            )}
          </ScrollArea>
        </DialogContent>
      </Dialog>
    </>
  );
}
