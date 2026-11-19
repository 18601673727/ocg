import { Pause, Play, Square, X } from "@lucide/solid";
import { createEffect, createMemo, createSignal } from "solid-js";
import { ElicitationForm } from "@/components/ocg/chat/ui/elicitation-form";
import { Button } from "@/components/ui/button";
import { elementRef } from "@/lib/ui-state";
import type { ChildConversation, ConversationCard, TaskPlan } from "../contracts";
import { useI18n } from "../i18n";
import { type ApiClient, ApiError } from "../runtime/api-client";
import { useOcgRuntime } from "../runtime/runtime-context";
import { ApprovalRequest } from "./approval-request";
import { useConversationError } from "./conversation-errors";
import { DispatchSelectionCard } from "./dispatch-selection-card";
import { outputDisplayText } from "./job-transcript-model";

function useConversation(turn?: string) {
  const { api } = useOcgRuntime();
  const [conversation, setConversation] = createSignal<string | null>(null);
  const [error, setError] = createSignal<string | null>(null);
  useConversationError(error);
  createEffect(
    () => [api, turn],
    () => {
      if (!api || !turn) return;
      const controller = new AbortController();
      void api
        .readJob(turn, controller.signal)
        .then((record) => {
          if (!controller.signal.aborted)
            setConversation(record.kind === "conversation" ? record.id : record.conversation);
        })
        .catch((cause) => {
          if (!controller.signal.aborted)
            setError(cause instanceof Error ? cause.message : String(cause));
        });
      return () => controller.abort();
    },
  );
  return { conversation: conversation(), error: error() };
}

function ChildConversationCard(props: {
  child: ChildConversation;
  agentName?: string;
  api: ApiClient;
  pending: boolean;
  onOpen: (conversation: string) => void;
  onAct: (child: ChildConversation, action: "stop" | "close" | "hold" | "unhold") => Promise<void>;
}) {
  const i18n = useI18n();
  const [detail, setDetail] = createSignal<Awaited<ReturnType<ApiClient["readJobDetails"]>> | null>(
    null,
  );
  const [model, setModel] = createSignal<string | null>(null);
  const [error, setError] = createSignal<string | null>(null);
  const [modelError, setModelError] = createSignal<string | null>(null);
  useConversationError(error, () => props.child.id);
  useConversationError(modelError, () => props.child.id);
  const phase = createMemo(() => props.child.recurrence?.phase);
  const due = createMemo(() => props.child.recurrence?.due_us);
  createEffect(
    () => [props.api, props.child.id, props.child.state, phase(), due()],
    () => {
      const controller = new AbortController();
      void props.api
        .readJobDetails(props.child.id, controller.signal)
        .then((result) => {
          if (!controller.signal.aborted) {
            setDetail(result);
            setError(null);
          }
        })
        .catch((cause) => {
          if (!controller.signal.aborted)
            setError(cause instanceof Error ? cause.message : String(cause));
        });
      return () => controller.abort();
    },
  );
  createEffect(
    () => [props.api, props.child.id, props.child.state, props.child.job_state, phase(), due()],
    () => {
      const controller = new AbortController();
      void props.api
        .readConversationTurns(props.child.id, undefined, 1, controller.signal)
        .then((result) => {
          if (!controller.signal.aborted) {
            setModel(result.turns[0]?.input.configuration?.model ?? null);
            setModelError(null);
          }
        })
        .catch((cause) => {
          if (!controller.signal.aborted)
            setModelError(cause instanceof Error ? cause.message : String(cause));
        });
      return () => controller.abort();
    },
  );
  const state = createMemo(() => detail()?.settlement.state);
  const status = createMemo(() => {
    const current = state();
    return current && current !== "unsettled"
      ? i18n.t(`state.${current}`)
      : i18n.t(`chat.child.${props.child.state}`);
  });
  const title = createMemo(() => props.child.title || i18n.t("chat.child.untitled"));
  const summary = createMemo(() =>
    outputDisplayText(detail()?.result)
      .trim()
      .split(/\n+/)[0]
      ?.replace(/^#{1,6}\s*/, "")
      .replace(/[*`]/g, "")
      .slice(0, 240),
  );
  return (
    <div
      class="relative flex w-72 max-w-full shrink-0 flex-col rounded-xl border border-border bg-muted/20 p-3 text-xs transition-colors hover:border-primary/50 focus-within:border-primary/50"
      data-slot="conversation-child-card"
    >
      <button
        type="button"
        class="absolute inset-0 rounded-xl text-left focus-visible:outline-2 focus-visible:outline-ring"
        onClick={() => props.onOpen(props.child.id)}
        aria-label={i18n.t("chat.child.openNamed", { title: title() })}
        title={`${i18n.t("chat.child.conversationId")}\n${props.child.id}\n${i18n.t("chat.agent")}\n${props.child.agent ?? "—"}`}
      />
      <div class="pointer-events-none space-y-2">
        <span class="line-clamp-2 font-medium" title={props.child.title}>
          {title()}
        </span>
        <span class="block text-muted-foreground">
          {props.agentName || i18n.t("chat.agent")}
          {model() ? ` · ${model()}` : ""} · {status()}
          {props.child.on_hold
            ? ` · ${i18n.t(`chat.child.hold.${props.child.on_hold.by.kind}`)}`
            : ""}
        </span>
        <span class="line-clamp-3 text-muted-foreground">
          {summary() ||
            detail()?.unknown_reason ||
            i18n.t(
              props.child.state === "settled" ? "chat.child.noResult" : "chat.child.pendingResult",
            )}
        </span>
      </div>
      {props.child.recurrence && (
        <p class="pointer-events-none mt-2 text-muted-foreground">
          {i18n.t("chat.child.every", { seconds: props.child.recurrence.interval_s })} ·{" "}
          {i18n.t(`chat.child.${props.child.recurrence.phase}`)}
          {props.child.recurrence.due_us !== null
            ? ` · ${new Date(props.child.recurrence.due_us / 1000).toLocaleString(i18n.locale)}`
            : ""}
        </p>
      )}
      <div class="relative mt-auto flex w-fit gap-1 pt-2">
        <Button
          size="icon-xs"
          variant="ghost"
          title={i18n.t("chat.child.stop")}
          aria-label={i18n.t("chat.child.stop")}
          disabled={props.pending || props.child.state !== "open"}
          onClick={() => void props.onAct(props.child, "stop")}
        >
          <Square aria-hidden="true" />
        </Button>
        <Button
          size="icon-xs"
          variant="ghost"
          title={i18n.t("common.close")}
          aria-label={i18n.t("common.close")}
          disabled={props.pending || props.child.state !== "open"}
          onClick={() => void props.onAct(props.child, "close")}
        >
          <X aria-hidden="true" />
        </Button>
        <Button
          size="icon-xs"
          variant="ghost"
          title={i18n.t(props.child.on_hold ? "chat.child.unhold" : "chat.child.hold")}
          aria-label={i18n.t(props.child.on_hold ? "chat.child.unhold" : "chat.child.hold")}
          disabled={props.pending || props.child.state !== "open"}
          onClick={() => void props.onAct(props.child, props.child.on_hold ? "unhold" : "hold")}
        >
          {props.child.on_hold ? <Play aria-hidden="true" /> : <Pause aria-hidden="true" />}
        </Button>
      </div>
    </div>
  );
}

export function ConversationChildrenStrip(props: {
  turn?: string;
  plan?: TaskPlan | null;
  onOpen: (conversation: string) => void;
}) {
  const { api } = useOcgRuntime();
  const i18n = useI18n();
  const { conversation } = useConversation(props.turn);
  const [children, setChildren] = createSignal<ChildConversation[]>([]);
  const [hasMore, setHasMore] = createSignal(false);
  const [pages, setPages] = createSignal(1);
  const [revision, setRevision] = createSignal(0);
  const [pending, setPending] = createSignal<string | null>(null);
  const [error, setError] = createSignal<string | null>(null);
  const [agentNames, setAgentNames] = createSignal<Record<string, string>>({});
  useConversationError(error);
  const lock = elementRef(false);
  const hasChildren = createMemo(() => children().length > 0);
  createEffect(
    () => [api, hasChildren()],
    () => {
      if (!api || !hasChildren()) return;
      const controller = new AbortController();
      const refresh = () => {
        void api
          .readProfiles(controller.signal)
          .then((profiles) => {
            if (!controller.signal.aborted)
              setAgentNames(
                Object.fromEntries(profiles.agent_profiles.map((agent) => [agent.id, agent.name])),
              );
          })
          .catch((cause) => {
            if (!controller.signal.aborted)
              setError(cause instanceof Error ? cause.message : String(cause));
          });
      };
      refresh();
      const unsubscribe = api.onProfilesChanged(refresh);
      return () => {
        controller.abort();
        unsubscribe();
      };
    },
  );
  createEffect(
    () => [api, conversation, pages(), revision()],
    () => {
      if (!api || !conversation) return;
      return api.subscribeChildren(
        conversation,
        pages(),
        (reply) => {
          setChildren(reply.children.filter((child) => child.origin !== "system"));
          setHasMore(reply.has_more);
          setError(null);
        },
        (cause) => setError(cause instanceof Error ? cause.message : String(cause)),
      );
    },
  );

  async function act(child: ChildConversation, action: "stop" | "close" | "hold" | "unhold") {
    if (!api || lock.current) return;
    lock.current = true;
    setPending(child.id);
    setError(null);
    try {
      if (action === "stop") await api.stopConversation(child.id);
      else if (action === "close") await api.closeConversation(child.id);
      else if (action === "hold") await api.holdConversation(child.id);
      else await api.unholdConversation(child.id);
      setRevision((value) => value + 1);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      lock.current = false;
      setPending(null);
    }
  }
  return (
    <>
      {(() => {
        if (!api || (!children().length && !hasMore())) return null;
        const stepOrder = new Map(
          props.plan?.steps.map((step, index) => [step.conversation, index]),
        );
        const orderedChildren = createMemo(() =>
          [...children()].sort(
            (left, right) =>
              (stepOrder.get(left.id) ?? stepOrder.size) -
                (stepOrder.get(right.id) ?? stepOrder.size) || left.id.localeCompare(right.id),
          ),
        );
        return (
          <section
            class="min-w-0 max-w-full py-2"
            aria-label={i18n.t("chat.children")}
            data-slot="conversation-child-cards"
          >
            <div class="ocg-subtle-scrollbar flex flex-nowrap items-stretch gap-3 overflow-x-auto overscroll-x-contain pb-2">
              {orderedChildren().map((child) => (
                <ChildConversationCard
                  child={child}
                  agentName={child.agent ? agentNames()[child.agent] : undefined}
                  api={api}
                  pending={pending() !== null}
                  onOpen={props.onOpen}
                  onAct={act}
                />
              ))}
              {hasMore() && (
                <Button
                  size="sm"
                  variant="ghost"
                  class="shrink-0 self-center"
                  onClick={() => setPages((value) => value + 1)}
                >
                  {i18n.t("chat.child.more")}
                </Button>
              )}
            </div>
          </section>
        );
      })()}
    </>
  );
}

function ConversationQuestion(props: {
  question: string;
  disabled: boolean;
  answer: (text: string) => void;
  dismiss: () => void;
}) {
  const i18n = useI18n();
  const [text, setText] = createSignal("");
  return (
    <ElicitationForm
      state="request"
      class="max-w-none"
      server={i18n.t("chat.card.question")}
      requestLabel=""
      message={props.question}
      fields={[
        {
          name: "answer",
          label: i18n.t("chat.card.answer"),
          kind: "multiline",
          placeholder: i18n.t("chat.card.answerPlaceholder"),
          value: text(),
          required: true,
        },
      ]}
      disabled={props.disabled}
      onFieldChange={(_, value) => setText(value)}
      onAccept={() => props.answer(text())}
      onDecline={props.dismiss}
      acceptLabel={i18n.t("chat.card.answer")}
      declineLabel={i18n.t("chat.card.dismiss")}
    />
  );
}

export function ConversationCardStrip(props: {
  turn?: string;
  conversationId?: string;
  onOpen: (conversation: string) => void;
}) {
  const { api, client } = useOcgRuntime();
  const i18n = useI18n();
  const { conversation: loadedConversation } = useConversation(
    props.conversationId ? undefined : props.turn,
  );
  const conversation = createMemo(() => props.conversationId ?? loadedConversation);
  const [cards, setCards] = createSignal<ConversationCard[]>([]);
  const [revision, setRevision] = createSignal(0);
  const [pending, setPending] = createSignal(false);
  const [error, setError] = createSignal<string | null>(null);
  useConversationError(error);
  const lock = elementRef(false);
  const actionVersion = elementRef(0);
  createEffect(
    () => [api, conversation(), revision()],
    () => {
      if (!api || !conversation()) return;
      const controller = new AbortController();
      let reading = false;
      const refresh = async () => {
        if (reading) return;
        reading = true;
        try {
          const version = actionVersion.current;
          const reply = await api.readCards(conversation()!, controller.signal);
          if (!controller.signal.aborted && !lock.current && version === actionVersion.current) {
            setCards(reply.cards);
            setError(null);
          }
        } catch (cause) {
          if (!controller.signal.aborted)
            setError(cause instanceof Error ? cause.message : String(cause));
        } finally {
          reading = false;
        }
      };
      const unsubscribe = api.subscribeCatalog(
        () => void refresh(),
        (cause) => {
          if (!controller.signal.aborted)
            setError(cause instanceof Error ? cause.message : String(cause));
        },
      );
      void refresh();
      return () => {
        controller.abort();
        unsubscribe();
      };
    },
  );

  async function act(
    card: ConversationCard,
    decision?: { decision: "approve" | "reject" } | { answer: string },
  ) {
    if (!api || lock.current) return;
    lock.current = true;
    actionVersion.current++;
    setPending(true);
    setError(null);
    try {
      if (card.kind === "approval" || card.kind === "question" || card.kind === "dispatch") {
        if (decision) {
          const res = await api.decideApproval(card.approval, decision);
          setCards((prev) =>
            prev.filter(
              (c) =>
                !(
                  c.kind === card.kind &&
                  (c.kind === "approval" || c.kind === "question" || c.kind === "dispatch") &&
                  c.approval[0] === card.approval[0] &&
                  c.approval[1] === card.approval[1] &&
                  c.approval[2] === card.approval[2]
                ),
            ),
          );
          if (client.openConversation) {
            await client.openConversation(res.turn);
          }
        }
      }
      setRevision((value) => value + 1);
    } catch (cause) {
      if (
        cause instanceof ApiError &&
        (cause.code === "approval_decided" || cause.code === "approval_not_found")
      ) {
        setRevision((value) => value + 1);
      } else {
        setError(
          cause instanceof ApiError && cause.code === "command_list_bytes"
            ? i18n.t("chat.card.commandBytes")
            : cause instanceof ApiError && cause.code === "command_list_full"
              ? i18n.t("chat.card.commandCapacity")
              : cause instanceof Error
                ? cause.message
                : String(cause),
        );
      }
    } finally {
      actionVersion.current++;
      lock.current = false;
      setPending(false);
    }
  }
  const groups = new Map<string, { parent: string | null; cards: ConversationCard[] }>();
  for (const card of cards()) {
    let group = groups.get(card.conversation);
    if (!group) {
      group = { parent: card.parent, cards: [] };
      groups.set(card.conversation, group);
    }
    group.parent = card.parent;
    group.cards.push(card);
    if (card.parent && card.parent !== conversation() && !groups.has(card.parent))
      groups.set(card.parent, { parent: conversation(), cards: [] });
  }
  function renderGroup(id: string, ancestors: Set<string>) {
    if (ancestors.has(id)) return null;
    const group = groups.get(id);
    if (!group) return null;
    const visited = new Set(ancestors).add(id);
    const isCurrent = id === conversation();
    return (
      <div class="flex flex-col gap-3" data-conversation={id}>
        {!isCurrent && (
          <div class="flex items-center gap-1.5">
            <Button
              size="xs"
              variant="ghost"
              class="h-6 font-mono text-xs text-muted-foreground hover:text-foreground"
              onClick={() => props.onOpen(id)}
            >
              {id}
            </Button>
          </div>
        )}
        {group.cards.map((card) => {
          const reject = () => void act(card, { decision: "reject" });
          if (card.kind === "approval") {
            return (
              <ApprovalRequest
                tool={card.tool}
                arguments={card.arguments}
                assessment={card.assessment}
                disabled={pending()}
                onApprove={() => void act(card, { decision: "approve" })}
                onReject={reject}
              />
            );
          }
          if (card.kind === "question") {
            return (
              <ConversationQuestion
                question={card.question}
                disabled={pending()}
                answer={(text) => void act(card, { answer: text })}
                dismiss={reject}
              />
            );
          }
          if (!api) return null;
          return (
            <div class="w-full rounded-xl border border-border bg-card p-4 text-xs shadow-xs">
              <div class="mb-2 flex items-center justify-between gap-2">
                <span class="text-sm font-medium text-foreground">
                  {i18n.t("chat.card.dispatch")}
                </span>
              </div>
              <DispatchSelectionCard
                client={api}
                arguments={card.arguments}
                disabled={pending()}
                onSelect={(selection) => void act(card, { answer: JSON.stringify(selection) })}
              />
              <Button
                size="xs"
                variant="outline"
                class="mt-3"
                disabled={pending()}
                onClick={reject}
              >
                {i18n.t("chat.card.dismiss")}
              </Button>
            </div>
          );
        })}
        {[...groups]
          .filter(([child, value]) => child !== id && value.parent === id)
          .map(([child]) => (
            <div class="ml-3 flex flex-col gap-3 border-l-2 border-border/60 pl-3">
              {renderGroup(child, visited)}
            </div>
          ))}
      </div>
    );
  }
  return (
    <>
      {(() => {
        if (!api || !cards().length) return null;
        return (
          <section
            class="w-full py-2"
            aria-label={i18n.t("chat.card.cards")}
            data-slot="conversation-cards"
          >
            <div class="flex flex-col gap-3">
              {[...groups]
                .filter(([, group]) => !group.parent || !groups.has(group.parent))
                .map(([id]) => renderGroup(id, new Set()))}
            </div>
          </section>
        );
      })()}
    </>
  );
}
