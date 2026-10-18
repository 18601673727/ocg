"use client";

import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import {
  ListPlus,
  X,
  ChevronDown,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { ActivityPulse } from "../activity-pulse";
import { useI18n, runtimeStatusDetail } from "../i18n";
import type { ChatMessage, ChatSession, RuntimeStatus } from "../types";
import { ModelSelector } from "./model-selector";
import type { ChatModelSelection } from "../contracts";
import type { QueuedChatMessage } from "../types";
import { retryInput } from "./retry";
import { activeExecutionTarget, currentActivity, executionActivity, isExecutionActive, type ActiveExecutionTarget, type ActivityStep, type ExecutionPhase } from "./execution-status";
import { readModelPreference, writeModelPreference } from "./model-preference";
import { ChatTranscript } from "./chat-transcript";
import { ChatComposerInput, ChatComposerView } from "./chat-composer-view";
import type { JobExecution } from "../execution/domain";
import { AttachmentStaging, ImageGallery, useImageAttachments } from "./image-attachments";
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
  projectId,
  draft,
  onDraftChange,
  onIntent,
  runtimeStatus,
  busy,
  queueing,
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
  projectId?: string;
  busy: boolean;
  queueing: boolean;
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

  const suggestions = matchComposerSuggestions(draft).filter(suggestion => suggestion.kind !== "job" && suggestion.kind !== "job.create");
  const suggestionsOpen = suggestions.length > 0 && dismissedFor !== draft;
  const activeIndex = suggestionsOpen && highlight?.raw === draft && highlight.index < suggestions.length
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

  const submit = async (mode: "queue" | "steer" = "queue") => {
    if (actionLock.current || !images.ready) return;
    const intent = parseComposerIntent(draft);
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
      await onIntent(intent.kind === "chat" ? { ...intent, selection, mode, images: images.images } : intent);
      onDraftChange("");
      images.clear();
    } catch (cause) {
      setCommandError({ raw: draft, message: cause instanceof Error ? cause.message : String(cause) });
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
      setCommandError({ raw: draft, message: cause instanceof Error ? cause.message : String(cause) });
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
      attachmentInput={<input ref={fileInput} type="file" accept="image/png,image/jpeg" multiple hidden disabled={submitting || cancelling} aria-label={t("chat.attachFile")} onChange={(event) => {
          images.add(Array.from(event.target.files ?? []));
          event.target.value = "";
        }} />}
      selectors={
        <>
          {/*
            The turn's two selections sit outside and above the card as compact
            dropdown buttons: which Project this turn belongs to, and the model
            and reasoning effort the next turn runs with. Both hug their label up
            to a share of the row and ellipsize beyond it, so they read as two
            buttons rather than one bar. Keeping them outside the card means the
            card stays the input, and a raised panel opens upward over the
            conversation instead of covering it.
          */}
          <div className="relative z-30 flex min-w-0 items-stretch gap-1.5 px-0.5 pb-1.5">
            {projectSelector && <div className="flex min-w-0 max-w-[min(18rem,58%)] shrink items-stretch [&>button]:h-full">{projectSelector}</div>}
            <ModelSelector sessionId={sessionId} selection={selection} onChange={onSelectionChange} onPreference={onPreference} busy={busy || submitting || cancelling} active={active} activity={activity} />
          </div>
        </>
      }
      unavailableMessage={chatReady ? undefined : `${t("chat.unavailable")} · ${runtimeStatusDetail(t, runtimeStatus) ?? t("chat.unavailableFallback")}`}
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
                ...Array.from(clipboard.items).flatMap(item => item.type.startsWith("image/") ? [item.getAsFile()] : []),
                ...Array.from(clipboard.files),
              ].filter((file): file is File => file !== null && file.type.startsWith("image/"));
              if (pasted.length > 0) {
                e.preventDefault();
                images.add(pasted);
              }
            },
            onKeyDown: (e) => {
              if (e.nativeEvent.isComposing || e.nativeEvent.keyCode === 229) return;
              if (e.key === "Escape" && suggestionsOpen) {
                e.preventDefault();
                closeSuggestions();
                return;
              }
              if (suggestionsOpen && (e.key === "ArrowDown" || e.key === "ArrowUp")) {
                e.preventDefault();
                setHighlight({
                  raw: draft,
                  index: moveComposerSuggestionIndex(activeIndex, e.key === "ArrowDown" ? 1 : -1, suggestions.length),
                });
                return;
              }
              if (e.key === "Enter" && !e.shiftKey) {
                if (suggestionsOpen && activeIndex >= 0) {
                  e.preventDefault();
                  applySuggestion(suggestions[activeIndex]);
                  return;
                }
                if (window.matchMedia("(pointer: coarse)").matches && !e.ctrlKey && !e.metaKey) return;
                e.preventDefault();
                void submit();
              }
            },
            "aria-describedby": chatReady ? "composer-hint" : "composer-hint composer-status",
          }}
          suggestions={suggestionsOpen ? {
            items: suggestions.map(suggestion => ({
              id: suggestion.id,
              label: suggestion.label,
              description: suggestion.description,
              command: suggestion.command,
              icon: suggestion.action === "create-job" ? "job" : "command",
            })),
            activeIndex,
            onSelect: index => applySuggestion(suggestions[index]),
          } : undefined}
          error={error}
          actions={{
            busy,
            queueing,
            submitting,
            cancelling,
            sendDisabled: busy || !canSend || submitting || cancelling || (!chatReady && parseComposerIntent(draft).kind === "chat"),
            steerDisabled: !canSend || !chatReady || submitting || cancelling,
            onAttach: () => fileInput.current?.click(),
            onSteer: () => void submit("steer"),
            onCancel: onCancel ? () => void cancel() : undefined,
          }}
        />
      </div>
    </ChatComposerView>
  );
}

/* ---------- chat view ---------- */

type ChatViewProps = {
  session: ChatSession;
  messages: ChatMessage[];
  /** The canonical execution of this Chat's latest Job, when one is known. */
  execution?: JobExecution | null;
  runtimeStatus: RuntimeStatus;
  onComposerIntent: (intent: ComposerIntent) => void | Promise<void>;
  onCancel?: () => Promise<void>;
  queueState?: { queue: QueuedChatMessage[]; paused: boolean };
  onRemoveQueued?: (id: string) => void;
  onResumeQueue?: () => void;
  onRetryMessage: (messageId: string) => Promise<void>;
  /** Structured command surfaces are composed by the shell, not selected here. */
  composerSurface?: React.ReactNode;
  composerSurfaceKey?: string | null;
};

const SUGGESTIONS = [
  "chat.suggestion.summary", "chat.suggestion.plan", "chat.suggestion.review",
] as const;

export function ChatView({
  session,
  messages,
  execution,
  runtimeStatus,
  onComposerIntent,
  onRetryMessage,
  onCancel,
  queueState,
  onRemoveQueued,
  onResumeQueue,
  composerSurface,
  composerSurfaceKey,
}: ChatViewProps) {
  const [draft, setDraft] = useState("");
  // ChatView is keyed per Chat, so the saved next-turn preference is read once per Chat.
  const [selection, setSelection] = useState<ChatModelSelection | undefined>(() => readModelPreference(session.projectId, session.sessionId ?? session.id));
  const live = messages.findLast(message => message.role === "assistant" && (message.status === "streaming" || message.status === "pending"));
  // The running turn's own Job is the only source of what is executing.
  const liveExecution = isExecutionActive(execution) && (!live?.jobId || live.jobId === execution.jobId) ? execution : null;
  const busy = Boolean(live) || liveExecution !== null;
  const active = activeExecutionTarget(liveExecution);
  const activity = useMemo(() => {
    if (!liveExecution) return undefined;
    const steps = executionActivity(liveExecution);
    return { steps, current: currentActivity(liveExecution, steps) };
  }, [liveExecution]);
  const { t } = useI18n();
  const scrollRef = useRef<HTMLDivElement>(null);
  const contentRef = useRef<HTMLDivElement>(null);
  const following = useRef(true);
  const lastScrollTop = useRef(0);
  const [showLatest, setShowLatest] = useState(false);
  const retryLock = useRef(false);
  const [retrying, setRetrying] = useState(false);
  const [retryError, setRetryError] = useState<string | null>(null);
  const retryableMessageIds = useMemo(() => new Set(
    messages.filter(message => retryInput(messages, message.id)).map(message => message.id),
  ), [messages]);

  async function retry(messageId: string) {
    if (retryLock.current || !retryInput(messages, messageId)) return;
    retryLock.current = true;
    setRetrying(true);
    setRetryError(null);
    try {
      await onRetryMessage(messageId);
    } catch (cause) {
      setRetryError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      retryLock.current = false;
      setRetrying(false);
    }
  }

  const empty = messages.length === 0 && !composerSurface;
  const scrollToLatest = () => {
    following.current = true;
    setShowLatest(false);
    const el = scrollRef.current;
    if (el) {
      el.scrollTop = el.scrollHeight;
      lastScrollTop.current = el.scrollTop;
    }
  };

  useLayoutEffect(() => {
    following.current = true;
    const el = scrollRef.current;
    if (el) {
      el.scrollTop = el.scrollHeight;
      lastScrollTop.current = el.scrollTop;
    }
  }, [session.id]);

  useLayoutEffect(() => {
    const el = scrollRef.current;
    if (el && following.current) {
      el.scrollTop = el.scrollHeight;
      lastScrollTop.current = el.scrollTop;
    }
  }, [messages, composerSurfaceKey]);

  useEffect(() => {
    const el = scrollRef.current;
    const content = contentRef.current;
    if (!el || !content) return;
    const followContent = () => {
      if (following.current) {
        el.scrollTop = el.scrollHeight;
        lastScrollTop.current = el.scrollTop;
      }
      setShowLatest(!following.current && el.scrollHeight - el.clientHeight - el.scrollTop > 80);
    };
    const observer = new ResizeObserver(followContent);
    observer.observe(content);
    observer.observe(el);
    followContent();
    return () => observer.disconnect();
  }, [session.id, empty, composerSurfaceKey]);

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div className="relative flex min-h-0 flex-1 flex-col">
        <div ref={scrollRef} onScroll={() => {
          const el = scrollRef.current;
          if (!el) return;
          const atBottom = el.scrollHeight - el.clientHeight - el.scrollTop <= 80;
          if (atBottom) following.current = true;
          else if (el.scrollTop < lastScrollTop.current) following.current = false;
          lastScrollTop.current = el.scrollTop;
          setShowLatest(!following.current && !atBottom);
        }} className="min-h-0 flex-1 overflow-y-auto overscroll-contain [overflow-anchor:none]" role="log" aria-label={t("chat.conversation", { title: session.title })}>
          {empty ? (
            <div ref={contentRef} className="relative mx-auto flex min-h-full max-w-3xl flex-col items-center justify-center px-5 py-8 text-center">
              <ActivityPulse className="size-20 opacity-80" label={t("chat.idleIllustration")} />
              <h2 className="mt-5 text-[15px] font-semibold tracking-tight">
                {t("chat.newThread")}
              </h2>
              <p className="mt-1 max-w-md text-[13px] leading-6 text-muted-foreground">
                {t("chat.emptyPrefix")} <span className="font-medium text-foreground">{session.title}</span>{t("chat.emptySuffix")}
              </p>
              <div className="mt-4 flex w-full max-w-md flex-col gap-1.5">
                {SUGGESTIONS.map((s) => (
                  <button
                    key={s}
                    type="button"
                    onClick={() => setDraft(t(s))}
                    className="min-h-11 rounded-md border border-border bg-muted/30 px-3 py-2 text-left text-sm text-muted-foreground outline-none transition-colors hover:bg-muted hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring/30"
                  >
                    {t(s)}
                  </button>
                ))}
              </div>
            </div>
          ) : (
            <div ref={contentRef} className="mx-auto flex max-w-3xl flex-col gap-5 px-3 py-5 sm:px-5">
              <ChatTranscript
                messages={messages}
                retryableMessageIds={retryableMessageIds}
                onRetryMessage={messageId => void retry(messageId)}
                retryDisabled={retrying || runtimeStatus.state !== "connected"}
              />
              {retryError && <p role="alert" className="text-sm text-destructive">{retryError}</p>}
              {composerSurface}
            </div>
          )}
        </div>
        {showLatest && <Button type="button" size="sm" variant="secondary" onClick={scrollToLatest} className="absolute bottom-3 left-1/2 h-11 -translate-x-1/2 rounded-md border border-border tracking-normal normal-case shadow-md">
          <ChevronDown className="size-4" />{t("chat.jumpToLatest")}
        </Button>}
      </div>
      {!!queueState?.queue.length && (
        <div className="mx-auto w-full max-w-3xl shrink-0 border-t border-border px-3 py-2 sm:px-5" aria-label={t("chat.queue")}>
          <div className="mb-1 flex items-center gap-2 text-xs text-muted-foreground" role="status">
            <ListPlus className="size-3.5" />{t("chat.queueCount", { count: queueState.queue.length })}
            {queueState.paused && <><span>· {t("chat.queuePaused")}</span><Button type="button" size="xs" variant="ghost" disabled={runtimeStatus.state !== "connected"} onClick={onResumeQueue}>{t("chat.resumeQueue")}</Button></>}
          </div>
          <ol className="max-h-[min(7rem,15dvh)] overflow-y-auto">
            {queueState.queue.map((item, index) => <li key={item.id} className="flex items-center gap-2 py-1 text-xs">
              <span className="text-muted-foreground">{index + 1}.</span><span className="min-w-0 flex-1 truncate">{item.input.content || t("chat.imageCount", { count: item.input.images?.length ?? 0 })}</span>
              <ImageGallery images={item.input.images} compact />
              <Button type="button" size="icon-xs" variant="ghost" className="size-11 rounded-md sm:size-7" aria-label={t("chat.removeQueued")} onClick={() => onRemoveQueued?.(item.id)}><X className="size-3" /></Button>
            </li>)}
          </ol>
        </div>
      )}
              <ChatComposer
        key={`${session.id}:${session.projectId ?? ""}`}
        projectId={session.projectId}
        busy={busy}
        queueing={busy || Boolean(queueState?.queue.length)}
        selection={selection}
        onSelectionChange={setSelection}
        onPreference={next => writeModelPreference(session.projectId, session.sessionId ?? session.id, next)}
        active={active}
        activity={activity}
        onCancel={onCancel}
        draft={draft}
        onDraftChange={setDraft}
        onIntent={async intent => {
          scrollToLatest();
          await onComposerIntent(intent);
          scrollToLatest();
        }}
        runtimeStatus={runtimeStatus}
      />
    </div>
  );
}
