import { Pause, Play, Square, X } from "@lucide/solid";
import { createEffect, createMemo, createSignal, For, Match, Show, Switch } from "solid-js";
import { ElicitationForm } from "@/components/ocg/chat/ui/elicitation-form";
import { Button } from "@/components/ui/button";
import { elementRef } from "@/lib/ui-state";
import type { ChildConversation, ConversationCard, TaskPlan } from "../contracts";
import { useI18n } from "../i18n";
import { type ApiClient, ApiError } from "../runtime/api-client";
import { childConversationStatus } from "../runtime/job-status";
import { useOcgRuntime } from "../runtime/runtime-context";
import { ApprovalRequest } from "./approval-request";
import { useConversationError } from "./conversation-errors";
import { DispatchSelectionCard } from "./dispatch-selection-card";
import { outputDisplayText } from "./job-transcript-model";

function useConversation(turn: () => string | undefined) {
  const { api } = useOcgRuntime();
  const [conversation, setConversation] = createSignal<string | null>(null);
  const [error, setError] = createSignal<string | null>(null);
  useConversationError(error);
  createEffect(turn, (id) => {
    setConversation(null);
    setError(null);
    if (!api || !id) return;
    const controller = new AbortController();
    void api
      .readJob(id, controller.signal)
      .then((record) => {
        if (!controller.signal.aborted)
          setConversation(record.kind === "conversation" ? record.id : record.conversation);
      })
      .catch((cause) => {
        if (!controller.signal.aborted)
          setError(cause instanceof Error ? cause.message : String(cause));
      });
    return () => controller.abort();
  });
  return { conversation, error };
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
  const [preview, setPreview] = createSignal<{
    model?: string;
    text: string;
    earlier: boolean;
    planningFailed?: boolean;
  }>();
  const [error, setError] = createSignal<string | null>(null);
  useConversationError(error, () => props.child.id);
  createEffect(
    () => [
      props.api,
      props.child.id,
      props.child.state,
      props.child.job_state,
      props.child.recurrence?.phase,
      props.child.recurrence?.due_us,
    ],
    () => {
      const controller = new AbortController();
      const { signal } = controller;
      const load = async () => {
        let before: string | undefined;
        let model: string | undefined;
        let earlier = false;
        const failedPlans = new Set<string>();
        do {
          const page = await props.api.readConversationTurns(
            props.child.id,
            before,
            before ? 10 : 1,
            signal,
          );
          for (const turn of page.turns) {
            if (turn.planning_target) {
              if (turn.job.settlement.state === "failed") failedPlans.add(turn.planning_target);
              continue;
            }
            if (failedPlans.has(turn.job.id))
              return { model, text: "", earlier, planningFailed: true };
            model ??= turn.input.configuration?.model;
            const result = turn.outputs.find((output) => output.kind === "result");
            let text = outputDisplayText(result);
            if (!result && (turn.outputs_truncated || turn.job.settlement.state === "unknown")) {
              const detail = await props.api.readJobDetails(turn.job.id, signal);
              text = outputDisplayText(detail.result) || (!earlier && detail.unknown_reason) || "";
            }
            if (
              text ||
              props.child.state !== "settled" ||
              turn.job.settlement.state === "cancelled" ||
              turn.job.settlement.state === "failed"
            )
              return { model, text, earlier };
            earlier = true;
          }
          before = page.next_before ?? undefined;
        } while (before);
        return { model, text: "", earlier: false };
      };
      setPreview(undefined);
      setError(null);
      void load().then(
        (result) => {
          if (!signal.aborted) setPreview(result);
        },
        (cause) => {
          if (!signal.aborted) setError(cause instanceof Error ? cause.message : String(cause));
        },
      );
      return () => controller.abort();
    },
  );
  const status = createMemo(() => childConversationStatus(i18n.t, props.child));
  const title = createMemo(() => props.child.title || i18n.t("chat.child.untitled"));
  const noResult = createMemo(() => {
    if (!preview()) return i18n.t("common.loading");
    if (preview()?.planningFailed) return i18n.t("chat.child.planningFailed");
    switch (props.child.job_state) {
      case "failed":
        return i18n.t("chat.child.failedResult");
      case "cancelled":
        return i18n.t("chat.child.cancelledResult");
      case "unknown":
        return i18n.t("chat.child.unknownResult");
      case "completed":
        return i18n.t("chat.child.noResult");
      default:
        return i18n.t("chat.child.pendingResult");
    }
  });
  const summary = createMemo(() =>
    (preview()?.text ?? "")
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
          {preview()?.model ? ` · ${preview()!.model}` : ""}
        </span>
        <span class="block font-medium">
          {status()}
          {(props.child.state === "closing" ||
            props.child.on_hold ||
            (props.child.state === "open" && props.child.keep_open && !props.child.recurrence)) &&
            props.child.job_state === "completed" && (
              <span class="font-normal text-muted-foreground">
                {" · "}
                {i18n.t("chat.child.lastTurnCompleted")}
              </span>
            )}
          {props.child.state === "settled" && (
            <span class="font-normal text-muted-foreground">
              {" · "}
              {i18n.t("chat.child.closed")}
            </span>
          )}
        </span>
        <span class="line-clamp-3 text-muted-foreground">
          {summary() && preview()?.earlier && (
            <strong>{i18n.t("chat.child.earlierAnswer")} · </strong>
          )}
          {summary() || error() || noResult()}
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
          disabled={props.pending || props.child.state === "settled"}
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
  conversationId?: string;
  turn?: string;
  plan?: TaskPlan | null;
  onOpen: (conversation: string) => void;
}) {
  const { api } = useOcgRuntime();
  const i18n = useI18n();
  const { conversation: loaded } = useConversation(() =>
    props.conversationId ? undefined : props.turn,
  );
  const conversation = createMemo(() => props.conversationId ?? loaded());
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
    () => [api, conversation(), pages(), revision()],
    () => {
      const id = conversation();
      if (!api || !id) return;
      return api.subscribeChildren(
        id,
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
              <For each={orderedChildren()} keyed={(child) => child.id}>
                {(child) => (
                  <ChildConversationCard
                    child={child()}
                    agentName={agentNames()[child().agent ?? ""]}
                    api={api}
                    pending={pending() !== null}
                    onOpen={props.onOpen}
                    onAct={act}
                  />
                )}
              </For>
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

function PausedReportCard(props: {
  card: Extract<ConversationCard, { kind: "paused" }>;
  onOpen: (conversation: string) => void;
}) {
  const { api } = useOcgRuntime();
  const i18n = useI18n();
  const [limit, setLimit] = createSignal<number>();
  const [error, setError] = createSignal<string | null>(null);
  const [titles, setTitles] = createSignal(new Map<string, string>());
  const [pages, setPages] = createSignal(1);
  const parent = createMemo(() => props.card.conversation);
  const oldest = createMemo(() =>
    props.card.waiting.reduce<string | undefined>(
      (first, id) => (first === undefined || id < first ? id : first),
      undefined,
    ),
  );
  useConversationError(error);
  createEffect(parent, (id) => {
    if (!api) return;
    const controller = new AbortController();
    void api.readConversationLimits(id, controller.signal).then(
      (limits) => {
        if (!controller.signal.aborted) setLimit(limits.max_auto_turns);
      },
      (cause) => {
        if (!controller.signal.aborted) setError(String(cause));
      },
    );
    return () => controller.abort();
  });
  createEffect(
    () => [parent(), oldest(), pages()] as const,
    ([id, first, count]) =>
      api?.subscribeChildren(
        id,
        count,
        (page) => {
          setTitles(new Map(page.children.map((child) => [child.id, child.title])));
          if (first && page.next_before && first < page.next_before) setPages(count + 1);
        },
        (cause) => setError(cause instanceof Error ? cause.message : String(cause)),
      ),
  );
  return (
    <div class="rounded-xl border border-border bg-card p-4 text-sm">
      <p class="font-medium">
        {limit() === undefined
          ? i18n.t("chat.card.paused")
          : i18n.t("chat.card.pausedAfter", { count: limit()! })}
      </p>
      <p>{i18n.t("chat.card.resumeReports")}</p>
      <For each={props.card.waiting} keyed>
        {(id) => (
          <Button size="xs" variant="outline" onClick={() => props.onOpen(id)}>
            {titles().get(id) || id}
          </Button>
        )}
      </For>
    </div>
  );
}

export function ConversationCardStrip(props: {
  turn?: string;
  conversationId?: string;
  onOpen: (conversation: string) => void;
}) {
  const { api, client } = useOcgRuntime();
  const i18n = useI18n();
  const { conversation: loadedConversation } = useConversation(() =>
    props.conversationId ? undefined : props.turn,
  );
  const conversation = createMemo(() => props.conversationId ?? loadedConversation());
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
      let refreshPending = false;
      const refresh = async () => {
        if (reading) {
          refreshPending = true;
          return;
        }
        refreshPending = false;
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
          if (refreshPending && !controller.signal.aborted) void refresh();
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
  const groupedCards = createMemo(() => {
    const current = conversation();
    const groups = new Map<string, { parent: string | null; cards: ConversationCard[] }>();
    for (const card of cards()) {
      let group = groups.get(card.conversation);
      if (!group) {
        group = { parent: card.parent, cards: [] };
        groups.set(card.conversation, group);
      }
      group.parent = card.parent;
      group.cards.push(card);
      if (card.parent && card.parent !== current && !groups.has(card.parent))
        groups.set(card.parent, { parent: current, cards: [] });
    }
    return groups;
  });
  const roots = createMemo(() =>
    [...groupedCards()]
      .filter(([, group]) => !group.parent || !groupedCards().has(group.parent))
      .map(([id]) => id),
  );

  function renderGroup(id: string, ancestors: Set<string>) {
    if (ancestors.has(id)) return null;
    const visited = new Set(ancestors).add(id);
    const children = createMemo(() =>
      [...groupedCards()]
        .filter(([child, group]) => child !== id && group.parent === id)
        .map(([child]) => child),
    );
    return (
      <div class="flex flex-col gap-3" data-conversation={id}>
        {id !== conversation() && (
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
        <For
          each={groupedCards().get(id)?.cards}
          keyed={(card) =>
            card.kind === "paused" ? "paused:" + card.conversation : card.approval.join(":")
          }
        >
          {(card) => {
            function ofKind<K extends ConversationCard["kind"]>(kind: K) {
              const current = card();
              return current.kind === kind
                ? (current as Extract<ConversationCard, { kind: K }>)
                : undefined;
            }
            const reject = () => void act(card(), { decision: "reject" });
            return (
              <Switch>
                <Match when={ofKind("paused")}>
                  {(paused) => <PausedReportCard card={paused()} onOpen={props.onOpen} />}
                </Match>
                <Match when={ofKind("approval")}>
                  {(approval) => (
                    <ApprovalRequest
                      tool={approval().tool}
                      arguments={approval().arguments}
                      assessment={approval().assessment}
                      disabled={pending()}
                      onApprove={() => void act(card(), { decision: "approve" })}
                      onReject={reject}
                    />
                  )}
                </Match>
                <Match when={ofKind("question")}>
                  {(question) => (
                    <ConversationQuestion
                      question={question().question}
                      disabled={pending()}
                      answer={(text) => void act(card(), { answer: text })}
                      dismiss={reject}
                    />
                  )}
                </Match>
                <Match when={ofKind("dispatch")}>
                  {(dispatch) => (
                    <div class="w-full rounded-xl border border-border bg-card p-4 text-xs shadow-xs">
                      <div class="mb-2 flex items-center justify-between gap-2">
                        <span class="text-sm font-medium text-foreground">
                          {i18n.t("chat.card.dispatch")}
                        </span>
                      </div>
                      <DispatchSelectionCard
                        client={api!}
                        arguments={dispatch().arguments}
                        disabled={pending()}
                        onSelect={(selection) =>
                          void act(card(), { answer: JSON.stringify(selection) })
                        }
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
                  )}
                </Match>
              </Switch>
            );
          }}
        </For>
        <For each={children()} keyed>
          {(child) => (
            <div class="ml-3 flex flex-col gap-3 border-l-2 border-border/60 pl-3">
              {renderGroup(child, visited)}
            </div>
          )}
        </For>
      </div>
    );
  }
  return (
    <Show when={api && cards().length > 0}>
      <section
        class="w-full py-2"
        aria-label={i18n.t("chat.card.cards")}
        data-slot="conversation-cards"
      >
        <div class="flex flex-col gap-3">
          <For each={roots()} keyed>
            {(id) => renderGroup(id, new Set())}
          </For>
        </div>
      </section>
    </Show>
  );
}
