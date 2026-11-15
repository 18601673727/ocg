"use client";

import { useEffect, useRef, useState } from "react";
import { Pause, Play, Square, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useI18n } from "../i18n";
import { useOcgRuntime } from "../runtime/runtime-context";
import { ApiError, type ApiClient } from "../runtime/api-client";
import type { ConversationCard, ChildConversation, TaskPlan } from "../contracts";
import { useConversationError } from "./conversation-errors";
import { ApprovalRequest } from "./approval-request";
import { outputDisplayText } from "./job-transcript-model";
import { DispatchSelectionCard } from "./dispatch-selection-card";

function useConversation(turn?: string) {
  const { api } = useOcgRuntime();
  const [conversation, setConversation] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  useConversationError(error);
  useEffect(() => {
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
  }, [api, turn]);
  return { conversation, error };
}

function ChildConversationCard({
  child,
  agentName,
  api,
  pending,
  onOpen,
  onAct,
}: {
  child: ChildConversation;
  agentName?: string;
  api: ApiClient;
  pending: boolean;
  onOpen: (conversation: string) => void;
  onAct: (child: ChildConversation, action: "stop" | "close" | "hold" | "unhold") => Promise<void>;
}) {
  const { t, locale } = useI18n();
  const [detail, setDetail] = useState<Awaited<ReturnType<ApiClient["readJobDetails"]>> | null>(
    null,
  );
  const [model, setModel] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [modelError, setModelError] = useState<string | null>(null);
  useConversationError(error, child.id);
  useConversationError(modelError, child.id);
  const phase = child.recurrence?.phase;
  const due = child.recurrence?.due_us;
  useEffect(() => {
    const controller = new AbortController();
    void api
      .readJobDetails(child.id, controller.signal)
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
  }, [api, child.id, child.state, phase, due]);
  useEffect(() => {
    const controller = new AbortController();
    void api
      .readConversationTurns(child.id, undefined, 1, controller.signal)
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
  }, [api, child.id, child.state, child.job_state, phase, due]);
  const state = detail?.settlement.state;
  const status =
    state && state !== "unsettled" ? t(`state.${state}`) : t(`chat.child.${child.state}`);
  const title = child.title || t("chat.child.untitled");
  const summary = outputDisplayText(detail?.result)
    .trim()
    .split(/\n+/)[0]
    ?.replace(/^#{1,6}\s*/, "")
    .replace(/[*`]/g, "")
    .slice(0, 240);
  return (
    <div
      className="relative flex w-72 max-w-full shrink-0 flex-col rounded-xl border border-border bg-muted/20 p-3 text-xs transition-colors hover:border-primary/50 focus-within:border-primary/50"
      data-slot="conversation-child-card"
    >
      <button
        type="button"
        className="absolute inset-0 rounded-xl text-left focus-visible:outline-2 focus-visible:outline-ring"
        onClick={() => onOpen(child.id)}
        aria-label={t("chat.child.openNamed", { title })}
        title={`${t("chat.child.conversationId")}\n${child.id}\n${t("chat.agent")}\n${child.agent ?? "—"}`}
      />
      <div className="pointer-events-none space-y-2">
        <span className="line-clamp-2 font-medium" title={child.title}>
          {title}
        </span>
        <span className="block text-muted-foreground">
          {agentName || t("chat.agent")}
          {model ? ` · ${model}` : ""} · {status}
          {child.on_hold ? ` · ${t(`chat.child.hold.${child.on_hold.by.kind}`)}` : ""}
        </span>
        <span className="line-clamp-3 text-muted-foreground">
          {summary ||
            detail?.unknown_reason ||
            t(child.state === "settled" ? "chat.child.noResult" : "chat.child.pendingResult")}
        </span>
      </div>
      {child.recurrence && (
        <p className="pointer-events-none mt-2 text-muted-foreground">
          {t("chat.child.every", { seconds: child.recurrence.interval_s })} ·{" "}
          {t(`chat.child.${child.recurrence.phase}`)}
          {child.recurrence.due_us !== null
            ? ` · ${new Date(child.recurrence.due_us / 1000).toLocaleString(locale)}`
            : ""}
        </p>
      )}
      <div className="relative mt-auto flex w-fit gap-1 pt-2">
        <Button
          size="icon-xs"
          variant="ghost"
          title={t("chat.child.stop")}
          aria-label={t("chat.child.stop")}
          disabled={pending || child.state !== "open"}
          onClick={() => void onAct(child, "stop")}
        >
          <Square aria-hidden="true" />
        </Button>
        <Button
          size="icon-xs"
          variant="ghost"
          title={t("common.close")}
          aria-label={t("common.close")}
          disabled={pending || child.state !== "open"}
          onClick={() => void onAct(child, "close")}
        >
          <X aria-hidden="true" />
        </Button>
        <Button
          size="icon-xs"
          variant="ghost"
          title={t(child.on_hold ? "chat.child.unhold" : "chat.child.hold")}
          aria-label={t(child.on_hold ? "chat.child.unhold" : "chat.child.hold")}
          disabled={pending || child.state !== "open"}
          onClick={() => void onAct(child, child.on_hold ? "unhold" : "hold")}
        >
          {child.on_hold ? <Play aria-hidden="true" /> : <Pause aria-hidden="true" />}
        </Button>
      </div>
    </div>
  );
}

export function ConversationChildrenStrip({
  turn,
  plan,
  onOpen,
}: {
  turn?: string;
  plan?: TaskPlan | null;
  onOpen: (conversation: string) => void;
}) {
  const { api } = useOcgRuntime();
  const { t } = useI18n();
  const { conversation } = useConversation(turn);
  const [children, setChildren] = useState<ChildConversation[]>([]);
  const [hasMore, setHasMore] = useState(false);
  const [pages, setPages] = useState(1);
  const [revision, setRevision] = useState(0);
  const [pending, setPending] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [agentNames, setAgentNames] = useState<Record<string, string>>({});
  useConversationError(error);
  const lock = useRef(false);
  const hasChildren = children.length > 0;
  useEffect(() => {
    if (!api || !hasChildren) return;
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
  }, [api, hasChildren]);
  useEffect(() => {
    if (!api || !conversation) return;
    return api.subscribeChildren(
      conversation,
      pages,
      (reply) => {
        setChildren(reply.children.filter((child) => child.origin !== "system"));
        setHasMore(reply.has_more);
        setError(null);
      },
      (cause) => setError(cause instanceof Error ? cause.message : String(cause)),
    );
  }, [api, conversation, pages, revision]);

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
  if (!api || (!children.length && !hasMore)) return null;
  const stepOrder = new Map(plan?.steps.map((step, index) => [step.conversation, index]));
  const orderedChildren = [...children].sort(
    (left, right) =>
      (stepOrder.get(left.id) ?? stepOrder.size) - (stepOrder.get(right.id) ?? stepOrder.size) ||
      left.id.localeCompare(right.id),
  );
  return (
    <section
      className="min-w-0 max-w-full py-2"
      aria-label={t("chat.children")}
      data-slot="conversation-child-cards"
    >
      <div className="ocg-subtle-scrollbar flex flex-nowrap items-stretch gap-3 overflow-x-auto overscroll-x-contain pb-2">
        {orderedChildren.map((child) => (
          <ChildConversationCard
            key={child.id}
            child={child}
            agentName={child.agent ? agentNames[child.agent] : undefined}
            api={api}
            pending={pending !== null}
            onOpen={onOpen}
            onAct={act}
          />
        ))}
        {hasMore && (
          <Button
            size="sm"
            variant="ghost"
            className="shrink-0 self-center"
            onClick={() => setPages((value) => value + 1)}
          >
            {t("chat.child.more")}
          </Button>
        )}
      </div>
    </section>
  );
}

function ConversationQuestion({
  question,
  disabled,
  answer,
}: {
  question: string;
  disabled: boolean;
  answer: (text: string) => void;
}) {
  const { t } = useI18n();
  const [text, setText] = useState("");
  return (
    <form
      className="mt-2 flex flex-col gap-2"
      onSubmit={(event) => {
        event.preventDefault();
        answer(text);
      }}
    >
      <label className="flex flex-col gap-1">
        {question}
        <textarea
          className="rounded border border-border bg-background p-2"
          value={text}
          onChange={(event) => setText(event.target.value)}
          placeholder={t("chat.card.answerPlaceholder")}
          disabled={disabled}
        />
      </label>
      <Button type="submit" size="xs" disabled={disabled || !text.trim()}>
        {t("chat.card.answer")}
      </Button>
    </form>
  );
}

export function ConversationCardStrip({
  turn,
  conversationId,
  onOpen,
}: {
  turn?: string;
  conversationId?: string;
  onOpen: (conversation: string) => void;
}) {
  const { api, client } = useOcgRuntime();
  const { t } = useI18n();
  const { conversation: loadedConversation } = useConversation(conversationId ? undefined : turn);
  const conversation = conversationId ?? loadedConversation;
  const [cards, setCards] = useState<ConversationCard[]>([]);
  const [revision, setRevision] = useState(0);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  useConversationError(error);
  const lock = useRef(false);
  const actionVersion = useRef(0);
  useEffect(() => {
    if (!api || !conversation) return;
    const controller = new AbortController();
    let reading = false;
    const refresh = async () => {
      if (reading) return;
      reading = true;
      try {
        const version = actionVersion.current;
        const reply = await api.readCards(conversation, controller.signal);
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
  }, [api, conversation, revision]);

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
            ? t("chat.card.commandBytes")
            : cause instanceof ApiError && cause.code === "command_list_full"
              ? t("chat.card.commandCapacity")
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
  for (const card of cards) {
    let group = groups.get(card.conversation);
    if (!group) {
      group = { parent: card.parent, cards: [] };
      groups.set(card.conversation, group);
    }
    group.parent = card.parent;
    group.cards.push(card);
    if (card.parent && card.parent !== conversation && !groups.has(card.parent))
      groups.set(card.parent, { parent: conversation, cards: [] });
  }
  function renderGroup(id: string, ancestors: Set<string>) {
    if (ancestors.has(id)) return null;
    const group = groups.get(id);
    if (!group) return null;
    const visited = new Set(ancestors).add(id);
    const isCurrent = id === conversation;
    return (
      <div key={id} className="flex flex-col gap-3" data-conversation={id}>
        {!isCurrent && (
          <div className="flex items-center gap-1.5">
            <Button
              size="xs"
              variant="ghost"
              className="h-6 font-mono text-xs text-muted-foreground hover:text-foreground"
              onClick={() => onOpen(id)}
            >
              {id}
            </Button>
          </div>
        )}
        {group.cards.map((card) => {
          return (
            <div
              className="w-full rounded-xl border border-border bg-card p-4 text-xs shadow-xs"
              key={`${card.kind}:${card.approval.join(":")}`}
            >
              <div className="mb-2 flex items-center justify-between gap-2">
                <span className="text-sm font-medium text-foreground">
                  {t(`chat.card.${card.kind}`)}
                </span>
              </div>
              {card.kind === "approval" && (
                <>
                  <ApprovalRequest
                    tool={card.tool}
                    arguments={card.arguments}
                    assessment={card.assessment}
                  />
                  <div className="mt-3 flex gap-2">
                    <Button
                      size="xs"
                      disabled={pending}
                      onClick={() => void act(card, { decision: "approve" })}
                    >
                      {t("chat.card.approve")}
                    </Button>
                    <Button
                      size="xs"
                      variant="outline"
                      disabled={pending}
                      onClick={() => void act(card, { decision: "reject" })}
                    >
                      {t("chat.card.reject")}
                    </Button>
                  </div>
                </>
              )}
              {card.kind === "dispatch" && api && (
                <>
                  <DispatchSelectionCard
                    client={api}
                    arguments={card.arguments}
                    disabled={pending}
                    onSelect={(selection) => void act(card, { answer: JSON.stringify(selection) })}
                  />
                  <Button
                    size="xs"
                    variant="outline"
                    className="mt-3"
                    disabled={pending}
                    onClick={() => void act(card, { decision: "reject" })}
                  >
                    {t("chat.card.dismiss")}
                  </Button>
                </>
              )}
              {card.kind === "question" && (
                <>
                  <ConversationQuestion
                    question={card.question}
                    disabled={pending}
                    answer={(text) => void act(card, { answer: text })}
                  />
                  <Button
                    size="xs"
                    variant="outline"
                    disabled={pending}
                    onClick={() => void act(card, { decision: "reject" })}
                  >
                    {t("chat.card.dismiss")}
                  </Button>
                </>
              )}
            </div>
          );
        })}
        {[...groups]
          .filter(([child, value]) => child !== id && value.parent === id)
          .map(([child]) => (
            <div key={child} className="ml-3 flex flex-col gap-3 border-l-2 border-border/60 pl-3">
              {renderGroup(child, visited)}
            </div>
          ))}
      </div>
    );
  }
  if (!api || !cards.length) return null;
  return (
    <section
      className="w-full py-2"
      aria-label={t("chat.card.cards")}
      data-slot="conversation-cards"
    >
      <div className="flex flex-col gap-3">
        {[...groups]
          .filter(([, group]) => !group.parent || !groups.has(group.parent))
          .map(([id]) => renderGroup(id, new Set()))}
      </div>
    </section>
  );
}
