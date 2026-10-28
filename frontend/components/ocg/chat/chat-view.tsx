"use client";

import { useEffect, useRef, useState } from "react";
import { Pause, Play, Square, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useI18n, runtimeStatusDetail } from "../i18n";
import type { RuntimeStatus } from "../types";
import { ModelSelector } from "./model-selector";
import { useOcgRuntime } from "../runtime/runtime-context";
import { ControlTransportError, type HttpCanonicalControlClient } from "../runtime/control-client";
import type { ControlConversationCard } from "../contracts";
import type { ChatModelSelection } from "../contracts";
import type { ControlChildConversation, ControlTaskPlan } from "../contracts";
import {
  type ActiveExecutionTarget,
  type ActivityStep,
  type ExecutionPhase,
} from "./execution-status";
import { ChatComposerInput, ChatComposerView } from "./chat-composer-view";
import { AttachmentStaging, useImageAttachments } from "./image-attachments";
import { useConversationError } from "./conversation-errors";
import { ApprovalRequest } from "./approval-request";
import { outputDisplayText } from "./job-transcript-model";
import { DispatchSelectionCard } from "./dispatch-selection-card";
import {
  applyComposerSuggestion,
  matchComposerSuggestions,
  moveComposerSuggestionIndex,
  parseComposerIntent,
  type ComposerIntent,
  type ComposerSuggestion,
} from "../composer/domain";

/* ---------- composer ---------- */

export function ChatComposer({
  sessionId,
  conversationId,
  projectId,
  draft,
  onDraftChange,
  onIntent,
  runtimeStatus,
  busy,
  queueing,
  steerAfterCompletion,
  selection,
  onSelectionChange,
  onPreference,
  active,
  activity,
  onCancel,
  autoFocus = false,
  projectSelector,
}: {
  sessionId?: string;
  conversationId?: string;
  projectId?: string;
  busy: boolean;
  queueing: boolean;
  steerAfterCompletion: boolean;
  selection?: ChatModelSelection;
  onSelectionChange: (selection: ChatModelSelection) => void;
  onPreference: (selection: ChatModelSelection) => void;
  active: ActiveExecutionTarget | null;
  activity?: { current: ActivityStep | ExecutionPhase | null; steps: ActivityStep[] };
  onCancel?: () => Promise<void>;
  autoFocus?: boolean;
  /** Compact Project selector shown beside the model selector above the card. */
  projectSelector?: React.ReactNode;
  draft: string;
  onDraftChange: (v: string) => void;
  onIntent: (intent: ComposerIntent) => void | Promise<void>;
  runtimeStatus: RuntimeStatus;
}) {
  const { control } = useOcgRuntime();
  const [steerGate, setSteerGate] = useState<{
    conversation: string;
    blocked: boolean;
    kind?: "rule_based" | "manual";
  }>();
  const steerBlocked = Boolean(
    control && conversationId && (steerGate?.conversation !== conversationId || steerGate.blocked),
  );
  const defaultQueueing =
    queueing || Boolean(steerGate?.conversation === conversationId && steerGate?.kind);
  useEffect(() => {
    if (!control || !conversationId) return;
    const abort = new AbortController();
    let reading = false;
    const refresh = async () => {
      if (reading) return;
      reading = true;
      try {
        const status = await control.readContextStatus(conversationId, {
          limit: 1,
          signal: abort.signal,
        });
        if (!abort.signal.aborted)
          setSteerGate({
            conversation: conversationId,
            blocked: Boolean(status.steer_blocked_by),
            kind: status.steer_blocked_by,
          });
      } catch {
        if (!abort.signal.aborted) setSteerGate({ conversation: conversationId, blocked: true });
      } finally {
        reading = false;
      }
    };
    const unsubscribe = control.subscribeCatalog(
      () => void refresh(),
      () => {
        if (!abort.signal.aborted) setSteerGate({ conversation: conversationId, blocked: true });
      },
    );
    void refresh();
    return () => {
      abort.abort();
      unsubscribe();
    };
  }, [control, conversationId]);
  const ref = useRef<HTMLTextAreaElement>(null);
  const fileInput = useRef<HTMLInputElement>(null);
  const images = useImageAttachments(projectId);
  const [dragging, setDragging] = useState(false);
  const dragDepth = useRef(0);
  const { t } = useI18n();
  const [highlight, setHighlight] = useState<{ raw: string; index: number } | null>(null);
  const [dismissedFor, setDismissedFor] = useState<string | null>(null);
  const [commandError, setCommandError] = useState<{ raw: string; message: string } | null>(null);
  const chatReady = runtimeStatus.state === "connected";
  const actionLock = useRef(false);
  const [submitting, setSubmitting] = useState(false);

  // A drop that misses the composer must not navigate the page to the file.
  useEffect(() => {
    const block = (event: DragEvent) => {
      if (event.dataTransfer?.types.includes("Files")) event.preventDefault();
    };
    window.addEventListener("dragover", block);
    window.addEventListener("drop", block);
    return () => {
      window.removeEventListener("dragover", block);
      window.removeEventListener("drop", block);
    };
  }, []);
  const [cancelling, setCancelling] = useState(false);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    el.style.height = "auto";
    el.style.height = `${Math.min(el.scrollHeight, 160)}px`;
  }, [draft]);

  useEffect(() => {
    if (autoFocus) ref.current?.focus();
  }, [autoFocus]);

  const suggestions = matchComposerSuggestions(draft).filter(
    (suggestion) => suggestion.kind !== "job" && suggestion.kind !== "job.create",
  );
  const suggestionsOpen = suggestions.length > 0 && dismissedFor !== draft;
  const activeIndex =
    suggestionsOpen && highlight?.raw === draft && highlight.index < suggestions.length
      ? highlight.index
      : -1;
  const error = commandError && commandError.raw === draft ? commandError.message : null;
  const canSend = (draft.trim().length > 0 || images.attachments.length > 0) && images.ready;

  const closeSuggestions = () => {
    setHighlight(null);
    setDismissedFor(draft);
  };

  const applySuggestion = (suggestion: ComposerSuggestion) => {
    const applied = applyComposerSuggestion(suggestion);
    setHighlight(null);
    setDismissedFor(null);
    setCommandError(null);
    if (applied.action === "create-job") {
      onDraftChange("");
      onIntent(parseComposerIntent(applied.text));
      return;
    }
    onDraftChange(applied.text);
    ref.current?.focus();
  };

  const submit = async (mode?: "queue" | "steer") => {
    if (
      actionLock.current ||
      !images.ready ||
      (mode === "steer" && steerBlocked) ||
      (!mode && steerAfterCompletion && steerBlocked && !steerGate?.kind)
    )
      return;
    const intent = parseComposerIntent(draft);
    const effectiveMode = mode ?? (steerAfterCompletion && !steerBlocked ? "steer" : undefined);
    if (intent.kind === "unknown-command") {
      setDismissedFor(draft);
      setCommandError({ raw: draft, message: intent.reason });
      return;
    }
    if (intent.kind !== "chat" && images.attachments.length) {
      setCommandError({ raw: draft, message: t("chat.imagesRequireChat") });
      return;
    }
    if (intent.kind === "chat" && intent.text.length === 0 && !images.images.length) return;
    if (intent.kind === "chat" && !chatReady) return;
    actionLock.current = true;
    setSubmitting(true);
    setCommandError(null);
    try {
      await onIntent(
        intent.kind === "chat"
          ? { ...intent, selection, mode: effectiveMode, images: images.images }
          : intent,
      );
      onDraftChange("");
      images.clear();
    } catch (cause) {
      setCommandError({
        raw: draft,
        message: cause instanceof Error ? cause.message : String(cause),
      });
    } finally {
      actionLock.current = false;
      setSubmitting(false);
    }
  };

  const cancel = async () => {
    if (!onCancel || actionLock.current) return;
    actionLock.current = true;
    setCancelling(true);
    try {
      await onCancel();
    } catch (cause) {
      setCommandError({
        raw: draft,
        message: cause instanceof Error ? cause.message : String(cause),
      });
    } finally {
      actionLock.current = false;
      setCancelling(false);
    }
  };

  return (
    <ChatComposerView
      form={{
        onSubmit: (e) => {
          e.preventDefault();
          void submit();
        },
        onDragEnter: (event) => {
          if (!event.dataTransfer.types.includes("Files")) return;
          event.preventDefault();
          dragDepth.current += 1;
          setDragging(true);
        },
        onDragOver: (event) => {
          if (!event.dataTransfer.types.includes("Files")) return;
          event.preventDefault();
          event.dataTransfer.dropEffect = submitting || cancelling ? "none" : "copy";
        },
        onDragLeave: (event) => {
          event.preventDefault();
          dragDepth.current = Math.max(0, dragDepth.current - 1);
          if (dragDepth.current === 0) setDragging(false);
        },
        onDrop: (event) => {
          if (!event.dataTransfer.types.includes("Files")) return;
          event.preventDefault();
          dragDepth.current = 0;
          setDragging(false);
          if (!submitting && !cancelling) images.add(Array.from(event.dataTransfer.files));
        },
      }}
      dragging={dragging}
      attachmentInput={
        <input
          ref={fileInput}
          type="file"
          accept="image/png,image/jpeg,image/gif,image/webp"
          multiple
          hidden
          disabled={submitting || cancelling}
          aria-label={t("chat.attachFile")}
          onChange={(event) => {
            images.add(Array.from(event.target.files ?? []));
            event.target.value = "";
          }}
        />
      }
      unavailableMessage={
        chatReady
          ? undefined
          : runtimeStatus.state === "failed"
            ? t("chat.unavailable")
            : `${t("chat.unavailable")} · ${runtimeStatusDetail(t, runtimeStatus) ?? t("chat.unavailableFallback")}`
      }
    >
      <div className="rounded-lg border border-border bg-card shadow-[0_8px_30px_-12px_rgba(0,0,0,0.25)] transition-colors focus-within:border-ring">
        <AttachmentStaging state={images} disabled={submitting || cancelling} />
        <ChatComposerInput
          textarea={{
            ref,
            value: draft,
            readOnly: submitting || cancelling,
            onChange: (e) => {
              setDismissedFor(null);
              onDraftChange(e.target.value);
            },
            onPaste: (e) => {
              if (submitting || cancelling) return;
              const clipboard = e.clipboardData;
              if (!clipboard) return;
              // One image can arrive as both an item and a file. Collect once
              // and let the shared attachment state drop the duplicate.
              const pasted = [
                ...Array.from(clipboard.items).flatMap((item) =>
                  item.type.startsWith("image/") ? [item.getAsFile()] : [],
                ),
                ...Array.from(clipboard.files),
              ].filter((file): file is File => file !== null && file.type.startsWith("image/"));
              if (pasted.length > 0) {
                e.preventDefault();
                images.add(pasted);
              }
            },
            onKeyDown: (e) => {
              if (e.nativeEvent.isComposing || e.nativeEvent.keyCode === 229) return;
              if (e.key === "`" && !e.shiftKey && !e.ctrlKey && !e.metaKey && !e.altKey) {
                const input = e.currentTarget;
                const start = input.selectionStart;
                const end = input.selectionEnd;
                const value = draft;
                if (value.slice(Math.max(0, start - 2), start) === "``" && start === end) {
                  e.preventDefault();
                  const insertion = "`\n\n```";
                  const next = value.slice(0, start) + insertion + value.slice(end);
                  onDraftChange(next);
                  requestAnimationFrame(() => {
                    input.setSelectionRange(start + 2, start + 2);
                    input.focus();
                  });
                  return;
                }
              }
              if (e.key === "Escape" && suggestionsOpen) {
                e.preventDefault();
                closeSuggestions();
                return;
              }
              if (suggestionsOpen && (e.key === "ArrowDown" || e.key === "ArrowUp")) {
                e.preventDefault();
                setHighlight({
                  raw: draft,
                  index: moveComposerSuggestionIndex(
                    activeIndex,
                    e.key === "ArrowDown" ? 1 : -1,
                    suggestions.length,
                  ),
                });
                return;
              }
              if (e.key === "Enter" && !e.shiftKey) {
                if (suggestionsOpen && activeIndex >= 0) {
                  e.preventDefault();
                  applySuggestion(suggestions[activeIndex]);
                  return;
                }
                if (window.matchMedia("(pointer: coarse)").matches && !e.ctrlKey && !e.metaKey)
                  return;
                e.preventDefault();
                void submit();
              }
            },
            "aria-describedby": chatReady ? "composer-hint" : "composer-hint composer-status",
          }}
          suggestions={
            suggestionsOpen
              ? {
                  items: suggestions.map((suggestion) => ({
                    id: suggestion.id,
                    label: suggestion.label,
                    description: suggestion.description,
                    command: suggestion.command,
                    icon: suggestion.action === "create-job" ? "job" : "command",
                  })),
                  activeIndex,
                  onSelect: (index) => applySuggestion(suggestions[index]),
                }
              : undefined
          }
          error={error}
          selectors={
            <div className="flex min-w-0 flex-1 items-center gap-1.5">
              {projectSelector && (
                <div className="flex min-w-0 max-w-[min(18rem,40%)] shrink items-stretch">
                  {projectSelector}
                </div>
              )}
              <ModelSelector
                sessionId={sessionId}
                selection={selection}
                onChange={onSelectionChange}
                onPreference={onPreference}
                busy={busy || submitting || cancelling}
                active={active}
                activity={activity}
              />
            </div>
          }
          actions={{
            busy,
            queueing: defaultQueueing,
            submitting,
            cancelling,
            sendDisabled:
              !canSend ||
              submitting ||
              cancelling ||
              (steerAfterCompletion && steerBlocked && !steerGate?.kind) ||
              (!chatReady && parseComposerIntent(draft).kind === "chat"),
            steerDisabled: !canSend || !chatReady || submitting || cancelling || steerBlocked,
            steerTitle: steerBlocked
              ? t(steerGate?.kind ? "chat.compactionQueueOnly" : "chat.steerStatusPending")
              : t("chat.steer"),
            onAttach: () => fileInput.current?.click(),
            onSteer: () => void submit("steer"),
            onCancel: onCancel ? () => void cancel() : undefined,
          }}
        />
      </div>
    </ChatComposerView>
  );
}

function useConversation(turn?: string) {
  const { control } = useOcgRuntime();
  const [conversation, setConversation] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  useConversationError(error);
  useEffect(() => {
    if (!control || !turn) return;
    const controller = new AbortController();
    void control
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
  }, [control, turn]);
  return { conversation, error };
}

function ChildConversationCard({
  child,
  agentName,
  control,
  pending,
  onOpen,
  onAct,
}: {
  child: ControlChildConversation;
  agentName?: string;
  control: HttpCanonicalControlClient;
  pending: boolean;
  onOpen: (conversation: string) => void;
  onAct: (
    child: ControlChildConversation,
    action: "stop" | "close" | "hold" | "unhold",
  ) => Promise<void>;
}) {
  const { t, locale } = useI18n();
  const [detail, setDetail] = useState<Awaited<
    ReturnType<HttpCanonicalControlClient["readJobDetails"]>
  > | null>(null);
  const [model, setModel] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [modelError, setModelError] = useState<string | null>(null);
  useConversationError(error, child.id);
  useConversationError(modelError, child.id);
  const phase = child.recurrence?.phase;
  const due = child.recurrence?.due_us;
  useEffect(() => {
    const controller = new AbortController();
    void control
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
  }, [control, child.id, child.state, phase, due]);
  useEffect(() => {
    const controller = new AbortController();
    void control
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
  }, [control, child.id, child.state, child.job_state, phase, due]);
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
  plan?: ControlTaskPlan | null;
  onOpen: (conversation: string) => void;
}) {
  const { control } = useOcgRuntime();
  const { t } = useI18n();
  const { conversation } = useConversation(turn);
  const [children, setChildren] = useState<ControlChildConversation[]>([]);
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
    if (!control || !hasChildren) return;
    const controller = new AbortController();
    const refresh = () => {
      void control
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
    const unsubscribe = control.onProfilesChanged(refresh);
    return () => {
      controller.abort();
      unsubscribe();
    };
  }, [control, hasChildren]);
  useEffect(() => {
    if (!control || !conversation) return;
    return control.subscribeChildren(
      conversation,
      pages,
      (reply) => {
        setChildren(reply.children.filter((child) => child.origin !== "system"));
        setHasMore(reply.has_more);
        setError(null);
      },
      (cause) => setError(cause instanceof Error ? cause.message : String(cause)),
    );
  }, [control, conversation, pages, revision]);

  async function act(
    child: ControlChildConversation,
    action: "stop" | "close" | "hold" | "unhold",
  ) {
    if (!control || lock.current) return;
    lock.current = true;
    setPending(child.id);
    setError(null);
    try {
      if (action === "stop") await control.stopConversation(child.id);
      else if (action === "close") await control.closeConversation(child.id);
      else if (action === "hold") await control.holdConversation(child.id);
      else await control.unholdConversation(child.id);
      setRevision((value) => value + 1);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      lock.current = false;
      setPending(null);
    }
  }
  if (!control || (!children.length && !hasMore)) return null;
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
            control={control}
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
  const { control, client } = useOcgRuntime();
  const { t } = useI18n();
  const { conversation: loadedConversation } = useConversation(conversationId ? undefined : turn);
  const conversation = conversationId ?? loadedConversation;
  const [cards, setCards] = useState<ControlConversationCard[]>([]);
  const [revision, setRevision] = useState(0);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  useConversationError(error);
  const lock = useRef(false);
  const actionVersion = useRef(0);
  useEffect(() => {
    if (!control || !conversation) return;
    const controller = new AbortController();
    let reading = false;
    const refresh = async () => {
      if (reading) return;
      reading = true;
      try {
        const version = actionVersion.current;
        const reply = await control.readCards(conversation, controller.signal);
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
    const unsubscribe = control.subscribeCatalog(
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
  }, [control, conversation, revision]);

  async function act(
    card: ControlConversationCard,
    decision?: { decision: "approve" | "reject" } | { answer: string },
  ) {
    if (!control || lock.current) return;
    lock.current = true;
    actionVersion.current++;
    setPending(true);
    setError(null);
    try {
      if (card.kind === "approval" || card.kind === "question" || card.kind === "dispatch") {
        if (decision) {
          const res = await control.decideApproval(card.approval, decision);
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
      } else if (card.kind === "loop_hold") {
        await control.unholdConversation(card.conversation);
        setCards((prev) =>
          prev.filter((c) => !(c.kind === "loop_hold" && c.conversation === card.conversation)),
        );
        if (client.openConversation) {
          await client.openConversation(card.conversation);
        }
      } else {
        try {
          await control.releaseSlot(card.turn);
        } catch (cause) {
          if (!(cause instanceof ControlTransportError) || cause.code !== "confirm_required")
            throw cause;
          if (window.confirm(t("chat.card.confirm"))) await control.releaseSlot(card.turn, true);
        }
        setCards((prev) =>
          prev.filter(
            (c) =>
              !(
                c.kind === "quarantine" &&
                c.conversation === card.conversation &&
                c.turn === card.turn
              ),
          ),
        );
        if (client.openConversation) {
          await client.openConversation(card.conversation);
        }
      }
      setRevision((value) => value + 1);
    } catch (cause) {
      if (
        cause instanceof ControlTransportError &&
        (cause.code === "approval_decided" || cause.code === "approval_not_found")
      ) {
        setRevision((value) => value + 1);
      } else {
        setError(
          cause instanceof ControlTransportError && cause.code === "command_list_bytes"
            ? t("chat.card.commandBytes")
            : cause instanceof ControlTransportError && cause.code === "command_list_full"
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
  const groups = new Map<string, { parent: string | null; cards: ControlConversationCard[] }>();
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
              key={
                card.kind === "approval" || card.kind === "question" || card.kind === "dispatch"
                  ? `${card.approval.join(":")}`
                  : card.kind
              }
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
              {card.kind === "dispatch" && control && (
                <>
                  <DispatchSelectionCard
                    client={control}
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
              {card.kind === "loop_hold" && (
                <Button size="xs" disabled={pending} onClick={() => void act(card)}>
                  {t("chat.child.unhold")}
                </Button>
              )}
              {card.kind === "quarantine" && (
                <>
                  <p className="mb-2 font-mono text-muted-foreground">{card.turn}</p>
                  <Button size="xs" disabled={pending} onClick={() => void act(card)}>
                    {t("chat.card.release")}
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
  if (!control || !cards.length) return null;
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
