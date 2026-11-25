import { ChevronRight, ExternalLink, X } from "@lucide/solid";
import { createEffect, createMemo } from "solid-js";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import { bindElementRef, elementRef } from "@/lib/ui-state";
import { runtimeStateLabel, useI18n } from "../i18n";
import { executionDetail, executionLabel } from "../runtime/job-status";
import { providerRejectionText } from "../runtime/provider-rejection";
import type { ChatMessage, ChatSession } from "../types";
import { ConversationContext } from "./conversation-context";
import { ConversationLimitsEditor } from "./conversation-limits";
import { DecisionDetails } from "./decision-details";
import { planningLabel } from "./planning-decision";

export function ChatInspector(props: {
  session?: ChatSession;
  messages: ChatMessage[];
  planningFocus?: { decisionId: string };
  onClose: () => void;
  onOpenJob: (job: string) => void;
}) {
  const i18n = useI18n();
  const decisions = new Map<string, ChatMessage>();
  for (const message of props.messages) {
    if (message.planningTarget) decisions.set(message.planningTarget, message);
  }
  const rounds = new Map<string, ChatMessage | undefined>();
  for (const message of props.messages) {
    const round = message.planningTarget ?? message.jobId ?? message.id;
    if (message.role === "assistant" && !message.planningTarget) {
      rounds.set(round, message);
    } else if (decisions.has(round) && !rounds.has(round)) {
      rounds.set(round, undefined);
    }
  }
  const planningDetails = elementRef<HTMLDetailsElement>(null);
  const hasFocusedDecision = createMemo(
    () =>
      !!props.planningFocus &&
      [...decisions.values()].some(
        (decision) => decision.jobId === props.planningFocus!.decisionId,
      ),
  );
  createEffect(
    () => [props.planningFocus, hasFocusedDecision()],
    () => {
      if (!hasFocusedDecision()) return;
      const details = planningDetails.current;
      if (!details) return;
      details.open = true;
      const summary = details.querySelector("summary");
      summary?.focus({ preventScroll: true });
      summary?.scrollIntoView({ block: "nearest" });
    },
  );

  return (
    <aside
      id="chat-inspector"
      aria-labelledby="chat-inspector-title"
      class="fixed inset-y-3 right-3 z-40 flex min-h-0 w-[min(24rem,calc(100%-1.5rem))] min-w-0 flex-col overflow-hidden rounded-3xl border-0 bg-muted text-foreground shadow-lg lg:relative lg:inset-auto lg:z-auto lg:m-3 lg:h-[calc(100%-1.5rem)] lg:w-80 lg:shrink-0 lg:shadow-none xl:w-96"
    >
      <header class="flex min-h-14 shrink-0 items-center gap-3 py-4 pr-14 pl-5">
        <h2 id="chat-inspector-title" class="min-w-0 flex-1 text-sm font-semibold">
          {i18n.t("usage.conversation")}
        </h2>
        <Button
          id="chat-inspector-close"
          type="button"
          variant="ghost"
          size="icon"
          class="absolute top-0 right-0 size-11 rounded-full"
          onClick={props.onClose}
          aria-label={i18n.t("topbar.collapseInspector")}
          title={i18n.t("topbar.collapseInspector")}
        >
          <X class="size-4" />
        </Button>
      </header>
      <ScrollArea
        class="min-h-0 flex-1"
        viewportProps={{
          class: "ocg-subtle-scrollbar overflow-x-hidden overscroll-contain",
          "data-chat-inspector-scroll": "",
        }}
        contentProps={{ class: "space-y-6 px-5 pb-6 [overflow-wrap:anywhere]" }}
      >
        {props.session ? (
          <>
            <section class="space-y-3">
              <h3 class="text-base font-medium">
                {props.session.title || i18n.t("sidebar.newChat")}
              </h3>
              <dl class="space-y-3 text-xs">
                <div class="flex justify-between gap-4">
                  <dt class="text-muted-foreground">{i18n.t("usage.turns")}</dt>
                  <dd class="tabular-nums">{rounds.size}</dd>
                </div>
                <div class="flex justify-between gap-4">
                  <dt class="text-muted-foreground">{i18n.t("usage.updated")}</dt>
                  <dd class="text-right">
                    {new Date(props.session.updatedAt).toLocaleString(i18n.locale)}
                  </dd>
                </div>
                {props.session.conversationId && (
                  <ConversationContext
                    conversation={props.session.conversationId}
                    agentProfile={props.session.agentProfile}
                    actionsSupported={
                      !props.messages.some((m) => m.execution?.actionsSupported === false)
                    }
                  />
                )}
              </dl>
            </section>
            {props.session.conversationId && (
              <ConversationLimitsEditor conversation={props.session.conversationId} />
            )}
            {rounds.size > 0 ? (
              <section class="space-y-3">
                <h3 class="text-xs font-semibold text-muted-foreground">
                  {i18n.t("usage.execution")}
                </h3>
                <ol class="space-y-3">
                  {[...rounds].map(([round, message], index) => {
                    const planning = decisions.get(round);
                    const job = message?.jobId ?? planning?.planningTarget;
                    const detail = message?.execution && executionDetail(i18n.t, message.execution);
                    return (
                      <li class="space-y-2 rounded-2xl bg-background/60 p-4 text-xs">
                        <div class="flex items-center justify-between gap-3">
                          <span class="font-medium">
                            {i18n.t("usage.turns")} {index + 1}
                          </span>
                          {message && (
                            <span class="text-muted-foreground">
                              {message.execution
                                ? executionLabel(i18n.t, message.execution)
                                : runtimeStateLabel(i18n.t, message.status)}
                            </span>
                          )}
                        </div>
                        {detail && <p class="text-muted-foreground">{detail}</p>}
                        {job && (
                          <button
                            type="button"
                            class="flex max-w-full items-center gap-2 rounded-md py-1 text-muted-foreground hover:text-foreground focus-visible:outline-2 focus-visible:outline-ring"
                            onClick={() => props.onOpenJob(job)}
                          >
                            <span class="truncate font-mono">{job}</span>
                            <ExternalLink class="size-3.5 shrink-0" />
                            <span class="sr-only">{i18n.t("chat.openInJobs")}</span>
                          </button>
                        )}
                        {message?.failureRejection ? (
                          <p class="whitespace-pre-wrap text-destructive">
                            {providerRejectionText(i18n.t, message.failureRejection)}
                          </p>
                        ) : (
                          message?.failureReason && (
                            <p class="text-destructive">{message.failureReason}</p>
                          )
                        )}
                        {planning?.decision && (
                          <details
                            ref={
                              props.planningFocus &&
                              planning.jobId === props.planningFocus.decisionId
                                ? bindElementRef(planningDetails)
                                : undefined
                            }
                            class="group/planning border-t border-border pt-2"
                          >
                            <summary class="flex cursor-pointer items-baseline gap-2 rounded-sm py-1 focus-visible:outline-2 focus-visible:outline-ring">
                              <ChevronRight
                                aria-hidden="true"
                                class="size-3 shrink-0 self-center text-muted-foreground group-open/planning:rotate-90"
                              />
                              <span class="font-medium">{i18n.t("planning.label")}</span>
                              <span class="ml-auto text-right text-muted-foreground">
                                {planningLabel(i18n.t, planning)}
                              </span>
                            </summary>
                            <div class="mt-2 space-y-2 rounded-lg border p-3">
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
              <p class="text-sm text-muted-foreground">{i18n.t("execution.noExecution")}</p>
            )}
          </>
        ) : (
          <p class="text-sm text-muted-foreground">{i18n.t("execution.noExecution")}</p>
        )}
      </ScrollArea>
    </aside>
  );
}
