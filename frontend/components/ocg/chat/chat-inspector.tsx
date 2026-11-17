"use client";

import { ChevronRight, ExternalLink, X } from "lucide-react";
import { useEffect, useRef } from "react";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import type { ChatMessage, ChatSession } from "../types";
import { runtimeStateLabel, useI18n } from "../i18n";
import { executionDetail, executionLabel } from "../runtime/job-status";
import { ConversationContext } from "./conversation-context";
import { ConversationLimitsEditor } from "./conversation-limits";
import { DecisionDetails } from "./decision-details";
import { planningLabel } from "./planning-decision";

export function ChatInspector({
  session,
  messages,
  planningFocus,
  onClose,
  onOpenJob,
}: {
  session?: ChatSession;
  messages: ChatMessage[];
  planningFocus?: { decisionId: string };
  onClose: () => void;
  onOpenJob: (job: string) => void;
}) {
  const { t, locale } = useI18n();
  const decisions = new Map<string, ChatMessage>();
  for (const message of messages) {
    if (message.planningTarget) decisions.set(message.planningTarget, message);
  }
  const rounds = new Map<string, ChatMessage | undefined>();
  for (const message of messages) {
    const round = message.planningTarget ?? message.jobId ?? message.id;
    if (message.role === "assistant" && !message.planningTarget) {
      rounds.set(round, message);
    } else if (decisions.has(round) && !rounds.has(round)) {
      rounds.set(round, undefined);
    }
  }
  const planningDetails = useRef<HTMLDetailsElement>(null);
  const hasFocusedDecision =
    !!planningFocus &&
    [...decisions.values()].some((decision) => decision.jobId === planningFocus.decisionId);
  useEffect(() => {
    if (!hasFocusedDecision) return;
    const details = planningDetails.current;
    if (!details) return;
    details.open = true;
    const summary = details.querySelector("summary");
    summary?.focus({ preventScroll: true });
    summary?.scrollIntoView({ block: "nearest" });
  }, [planningFocus, hasFocusedDecision]);

  return (
    <aside
      id="chat-inspector"
      aria-labelledby="chat-inspector-title"
      className="fixed inset-y-3 right-3 z-40 flex min-h-0 w-[min(24rem,calc(100%-1.5rem))] min-w-0 flex-col overflow-hidden rounded-3xl border-0 bg-muted text-foreground shadow-lg lg:relative lg:inset-auto lg:z-auto lg:m-3 lg:h-[calc(100%-1.5rem)] lg:w-80 lg:shrink-0 lg:shadow-none xl:w-96"
    >
      <header className="flex min-h-14 shrink-0 items-center gap-3 py-4 pr-14 pl-5">
        <h2 id="chat-inspector-title" className="min-w-0 flex-1 text-sm font-semibold">
          {t("usage.conversation")}
        </h2>
        <Button
          id="chat-inspector-close"
          type="button"
          variant="ghost"
          size="icon"
          className="absolute top-0 right-0 size-11 rounded-full"
          onClick={onClose}
          aria-label={t("topbar.collapseInspector")}
          title={t("topbar.collapseInspector")}
        >
          <X className="size-4" />
        </Button>
      </header>
      <ScrollArea
        className="min-h-0 flex-1"
        viewportProps={{
          className: "ocg-subtle-scrollbar overflow-x-hidden overscroll-contain",
          "data-chat-inspector-scroll": "",
        }}
        contentProps={{ className: "space-y-6 px-5 pb-6 [overflow-wrap:anywhere]" }}
      >
        {session ? (
          <>
            <section className="space-y-3">
              <h3 className="text-base font-medium">{session.title || t("sidebar.newChat")}</h3>
              <dl className="space-y-3 text-xs">
                <div className="flex justify-between gap-4">
                  <dt className="text-muted-foreground">{t("usage.turns")}</dt>
                  <dd className="tabular-nums">{rounds.size}</dd>
                </div>
                <div className="flex justify-between gap-4">
                  <dt className="text-muted-foreground">{t("usage.updated")}</dt>
                  <dd className="text-right">
                    {new Date(session.updatedAt).toLocaleString(locale)}
                  </dd>
                </div>
                {session.conversationId && (
                  <ConversationContext
                    key={`context:${session.conversationId}`}
                    conversation={session.conversationId}
                    agentProfile={session.agentProfile}
                    actionsSupported={
                      !messages.some((m) => m.execution?.actionsSupported === false)
                    }
                  />
                )}
              </dl>
            </section>
            {session.conversationId && (
              <ConversationLimitsEditor
                key={session.conversationId}
                conversation={session.conversationId}
              />
            )}
            {rounds.size > 0 ? (
              <section className="space-y-3">
                <h3 className="text-xs font-semibold text-muted-foreground">
                  {t("usage.execution")}
                </h3>
                <ol className="space-y-3">
                  {[...rounds].map(([round, message], index) => {
                    const planning = decisions.get(round);
                    const job = message?.jobId ?? planning?.planningTarget;
                    const detail = message?.execution && executionDetail(t, message.execution);
                    return (
                      <li
                        key={round}
                        className="space-y-2 rounded-2xl bg-background/60 p-4 text-xs"
                      >
                        <div className="flex items-center justify-between gap-3">
                          <span className="font-medium">
                            {t("usage.turns")} {index + 1}
                          </span>
                          {message && (
                            <span className="text-muted-foreground">
                              {message.execution
                                ? executionLabel(t, message.execution)
                                : runtimeStateLabel(t, message.status)}
                            </span>
                          )}
                        </div>
                        {detail && <p className="text-muted-foreground">{detail}</p>}
                        {job && (
                          <button
                            type="button"
                            className="flex max-w-full items-center gap-2 rounded-md py-1 text-muted-foreground hover:text-foreground focus-visible:outline-2 focus-visible:outline-ring"
                            onClick={() => onOpenJob(job)}
                          >
                            <span className="truncate font-mono">{job}</span>
                            <ExternalLink className="size-3.5 shrink-0" />
                            <span className="sr-only">{t("chat.openInJobs")}</span>
                          </button>
                        )}
                        {message?.failureReason && (
                          <p className="text-destructive">{message.failureReason}</p>
                        )}
                        {planning?.decision && (
                          <details
                            ref={
                              planningFocus && planning.jobId === planningFocus.decisionId
                                ? planningDetails
                                : undefined
                            }
                            className="group/planning border-t border-border pt-2"
                          >
                            <summary className="flex cursor-pointer items-baseline gap-2 rounded-sm py-1 focus-visible:outline-2 focus-visible:outline-ring">
                              <ChevronRight
                                aria-hidden="true"
                                className="size-3 shrink-0 self-center text-muted-foreground group-open/planning:rotate-90"
                              />
                              <span className="font-medium">{t("planning.label")}</span>
                              <span className="ml-auto text-right text-muted-foreground">
                                {planningLabel(t, planning)}
                              </span>
                            </summary>
                            <div className="mt-2 space-y-2 rounded-lg border p-3">
                              <DecisionDetails
                                decision={planning.decision}
                                message={planning}
                                full
                              />
                            </div>
                          </details>
                        )}
                      </li>
                    );
                  })}
                </ol>
              </section>
            ) : (
              <p className="text-sm text-muted-foreground">{t("execution.noExecution")}</p>
            )}
          </>
        ) : (
          <p className="text-sm text-muted-foreground">{t("execution.noExecution")}</p>
        )}
      </ScrollArea>
    </aside>
  );
}
