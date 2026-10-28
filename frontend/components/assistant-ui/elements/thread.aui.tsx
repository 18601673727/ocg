"use client";

import {
  ComposerAddAttachment,
  ComposerAttachments,
  UserMessageAttachments,
} from "@/components/assistant-ui/elements/attachment.aui";
import { File, getFileHref } from "@/components/assistant-ui/elements/file";
import { ThreadFollowupSuggestions } from "@/components/assistant-ui/elements/follow-up-suggestions.aui";
import { Image } from "@/components/assistant-ui/elements/image";
import { MarkdownText } from "@/components/assistant-ui/elements/markdown-text";
import {
  Reasoning,
  ReasoningContent,
  ReasoningRoot,
  ReasoningText,
  ReasoningTrigger,
  estimateReasoningTokens,
} from "@/components/assistant-ui/elements/reasoning.aui";
import { SmoothMarkdownText } from "@/components/assistant-ui/elements/smooth-markdown-text";
import { ToolFallback } from "@/components/assistant-ui/elements/tool-fallback.aui";
import {
  ToolGroupContent,
  ToolGroupRoot,
  ToolGroupTrigger,
} from "@/components/assistant-ui/elements/tool-group.aui";
import { TooltipIconButton } from "@/components/assistant-ui/elements/tooltip-icon-button";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { cn } from "@/lib/utils";
import { ToolActivityLabel } from "@/components/ocg/chat/tool-activity-label";
import type { ToolActivity } from "@/components/ocg/chat/tool-activity-summary";
import {
  ActionBarMorePrimitive,
  ActionBarPrimitive,
  AuiIf,
  type AssistantState,
  BranchPickerPrimitive,
  ComposerPrimitive,
  groupPartByType,
  MessagePrimitive,
  SuggestionPrimitive,
  ThreadPrimitive,
  type FileMessagePartComponent,
  type ImageMessagePartComponent,
  type TextMessagePartComponent,
  type ToolCallMessagePartComponent,
  useAuiState,
} from "@assistant-ui/react";
import {
  ArrowDownIcon,
  ArrowUpIcon,
  AudioLinesIcon,
  CheckIcon,
  ChevronLeftIcon,
  ChevronRightIcon,
  CopyIcon,
  DownloadIcon,
  MicIcon,
  MoreHorizontalIcon,
  PhoneIcon,
  RefreshCwIcon,
  SquareIcon,
  ThumbsDownIcon,
  ThumbsUpIcon,
} from "lucide-react";
import {
  createContext,
  useContext,
  useLayoutEffect,
  useRef,
  type ComponentType,
  type FC,
  type PropsWithChildren,
  type ReactNode,
} from "react";
import { useI18n } from "@/components/ocg/i18n";

export type ThreadGroupPart = MessagePrimitive.GroupedParts.GroupPart;

const reasoningDuration = (timing: ThreadGroupPart["timing"]) =>
  timing?.completedAt === undefined
    ? undefined
    : Math.max(0, (timing.completedAt - timing.startedAt) / 1000);

/**
 * Optional component overrides for the thread. `AssistantMessage` and
 * `Welcome` replace whole sections; the remaining slots override how the
 * assistant message renders tool calls and part groups. Tool UIs registered
 * by name (toolkit `render`, `useAssistantDataUI`) take precedence over
 * `ToolFallback`. When `TaskGroup` is set, tool calls that carry a nested
 * conversation and have no registered UI render through it instead of the
 * tool group; without it they render like any other tool call.
 */
export type ThreadComponents = {
  AssistantMessage?: ComponentType | undefined;
  Welcome?: ComponentType | undefined;
  Composer?: ComponentType<{ autoFocus: boolean }> | undefined;
  MessageFooter?: ComponentType | undefined;
  ToolFallback?: ToolCallMessagePartComponent | undefined;
  ToolGroup?: ComponentType<PropsWithChildren<{ group: ThreadGroupPart }>> | undefined;
  ReasoningGroup?: ComponentType<PropsWithChildren<{ group: ThreadGroupPart }>> | undefined;
  TaskGroup?: ComponentType<{ group: ThreadGroupPart }> | undefined;
};

const messageGroupBy = groupPartByType({
  reasoning: ["group-chainOfThought", "group-reasoning"],
  "tool-call": ["group-chainOfThought", "group-tool"],
  "standalone-tool-call": [],
});

type ThreadGroupKey = "group-chainOfThought" | "group-reasoning" | "group-tool" | "group-task";

const TASK_GROUP_PATH: readonly ThreadGroupKey[] = ["group-chainOfThought", "group-task"];

const taskAwareGroupBy = (
  part: Parameters<typeof messageGroupBy>[0],
  context?: Parameters<typeof messageGroupBy>[1],
): readonly ThreadGroupKey[] => {
  const path = messageGroupBy(part, context);
  return part.type === "tool-call" &&
    part.messages !== undefined &&
    path.length > 0 &&
    !context?.toolUIs?.[part.toolName]?.length
    ? TASK_GROUP_PATH
    : path;
};

export type ThreadProps = {
  components?: ThreadComponents | undefined;
  autoFocus?: boolean | undefined;
  afterMessages?: ReactNode;
};

const EMPTY_COMPONENTS: ThreadComponents = {};

const ThreadComponentsContext = createContext<ThreadComponents>(EMPTY_COMPONENTS);

// Startup exposes a loading placeholder thread; treat it as a new chat so
// the composer mounts centered. Loads after startup keep the docked layout.
const isNewChatView = (s: AssistantState) =>
  s.thread.messages.length === 0 && (!s.thread.isLoading || s.threads.isLoading);

// A switched thread that is still fetching its history: skeleton, not welcome.
const isHistoryLoadingView = (s: AssistantState) =>
  s.thread.messages.length === 0 &&
  s.thread.isLoading &&
  !s.thread.isDisabled &&
  !s.threads.isLoading;

const ThreadHistorySkeleton: FC = () => {
  const { t } = useI18n();
  return (
    <div
      data-slot="aui_thread-history-skeleton"
      role="status"
      className="animate-in fade-in fill-mode-both flex flex-col gap-y-6 [animation-delay:150ms] [animation-duration:200ms]"
    >
      <span className="sr-only">{t("app.loadingConversation")}</span>
      <Skeleton className="ml-auto h-9 w-2/5 rounded-xl motion-reduce:animate-none" />
      <div className="flex flex-col gap-y-2">
        <Skeleton className="h-4 w-11/12 motion-reduce:animate-none" />
        <Skeleton className="h-4 w-4/5 motion-reduce:animate-none" />
        <Skeleton className="h-4 w-3/5 motion-reduce:animate-none" />
      </div>
      <Skeleton className="ml-auto h-9 w-1/3 rounded-xl motion-reduce:animate-none" />
      <div className="flex flex-col gap-y-2">
        <Skeleton className="h-4 w-10/12 motion-reduce:animate-none" />
        <Skeleton className="h-4 w-2/3 motion-reduce:animate-none" />
      </div>
    </div>
  );
};

export const Thread: FC<ThreadProps> = ({
  components = EMPTY_COMPONENTS,
  autoFocus = true,
  afterMessages,
}) => {
  const isEmpty = useAuiState(isNewChatView);

  return (
    <ThreadComponentsContext.Provider value={components}>
      <ThreadRoot isEmpty={isEmpty} autoFocus={autoFocus} afterMessages={afterMessages} />
    </ThreadComponentsContext.Provider>
  );
};

const ThreadRoot: FC<{ isEmpty: boolean; autoFocus: boolean; afterMessages?: ReactNode }> = ({
  isEmpty,
  autoFocus,
  afterMessages,
}) => {
  const { Welcome = ThreadWelcome, Composer: ComposerComponent = Composer } =
    useContext(ThreadComponentsContext);

  return (
    <ThreadPrimitive.Root
      className="aui-root aui-thread-root bg-background @container flex h-full min-h-0 flex-col"
      style={{
        ["--thread-max-width" as string]: "56rem",
        ["--composer-bg" as string]: "color-mix(in oklab, var(--color-muted) 30%, transparent)",
        ["--composer-radius" as string]: "1rem",
        ["--composer-padding" as string]: "8px",
      }}
    >
      <ThreadPrimitive.Viewport
        turnAnchor="bottom"
        autoScroll
        scrollToBottomOnRunStart={false}
        data-slot="aui_thread-viewport"
        className="ocg-subtle-scrollbar relative flex min-h-0 flex-1 flex-col overflow-x-auto overflow-y-auto overscroll-contain"
      >
        <div
          className={cn(
            "mx-auto flex w-full max-w-(--thread-max-width) flex-1 flex-col px-4 pt-16",
            isEmpty && "justify-center",
          )}
        >
          <AuiIf condition={isNewChatView}>
            <Welcome />
          </AuiIf>
          <AuiIf condition={isHistoryLoadingView}>
            <ThreadHistorySkeleton />
          </AuiIf>

          <ThreadLoadEarlier />

          <div data-slot="aui_message-group" className="mb-14 flex flex-col gap-y-6 empty:hidden">
            <ThreadPrimitive.Messages>{() => <ThreadMessage />}</ThreadPrimitive.Messages>
            {afterMessages}
          </div>

          <ThreadPrimitive.ViewportFooter
            className={cn(
              "aui-thread-viewport-footer pointer-events-none flex flex-col gap-4 overflow-visible bg-transparent pb-4 [&>*]:pointer-events-auto md:pb-6",
              !isEmpty && "sticky bottom-0 mt-auto rounded-t-(--composer-radius)",
            )}
          >
            <ThreadScrollToBottom />
            <ThreadFollowupSuggestions />
            <ComposerComponent autoFocus={autoFocus} />
            <AuiIf condition={(s) => isNewChatView(s) && s.composer.isEmpty}>
              <ThreadSuggestions />
            </AuiIf>
          </ThreadPrimitive.ViewportFooter>
        </div>
      </ThreadPrimitive.Viewport>
    </ThreadPrimitive.Root>
  );
};

const ThreadMessage: FC = () => {
  const { AssistantMessage: AssistantMessageComponent = AssistantMessage } =
    useContext(ThreadComponentsContext);
  const role = useAuiState((s) => s.message.role);
  const isEditing = useAuiState((s) => s.message.composer.isEditing);
  const isSpoken = useAuiState((s) => s.message.metadata.modality === "voice");

  if (isEditing) return <EditComposer />;
  if (isSpoken) return <SpokenMessage />;
  if (role === "user") return <UserMessage />;
  return <AssistantMessageComponent />;
};

type VoiceRunPosition = "single" | "start" | "middle" | "end";

const useVoiceRunPosition = (): VoiceRunPosition =>
  useAuiState((s) => {
    const before = s.thread.messages[s.message.index - 1]?.metadata.modality === "voice";
    const after = s.thread.messages[s.message.index + 1]?.metadata.modality === "voice";
    if (before) return after ? "middle" : "end";
    return after ? "start" : "single";
  });

const SpokenText: TextMessagePartComponent = ({ text }) => (
  <p className="aui-spoken-message-text m-0">{text}</p>
);

const SpokenMessage: FC = () => {
  const { t } = useI18n();
  const role = useAuiState((s) => s.message.role);
  const position = useVoiceRunPosition();
  const isSpeaking = useAuiState(
    (s) => s.message.role === "assistant" && s.message.status?.type === "running",
  );
  const opensExchange = position === "start" || position === "single";

  return (
    <MessagePrimitive.Root
      data-slot="aui_spoken-message-root"
      data-role={role}
      data-voice-run={position}
      className={cn(
        "aui-spoken-message bg-muted/40 mx-2 px-3 py-1.5 [contain-intrinsic-size:auto_48px] [content-visibility:auto]",
        position === "single" && "rounded-xl py-2",
        position === "start" && "rounded-t-xl pt-2",
        position === "middle" && "-mt-6",
        position === "end" && "-mt-6 rounded-b-xl pb-2",
      )}
    >
      {opensExchange && (
        <div
          data-slot="aui_spoken-exchange-header"
          className="text-muted-foreground mb-1.5 flex items-center gap-1.5 text-xs"
        >
          <PhoneIcon className="size-3" aria-hidden />
          <span>{t("aui.voiceConversation")}</span>
        </div>
      )}
      <div
        data-slot="aui_spoken-message-content"
        className="text-foreground flex items-start gap-2 text-sm leading-relaxed"
      >
        <span className="text-muted-foreground mt-1 shrink-0" aria-hidden>
          {role === "user" ? (
            <MicIcon className="size-3.5" />
          ) : (
            <AudioLinesIcon className="size-3.5" />
          )}
        </span>
        <span className="sr-only">
          {role === "user" ? t("aui.youSaid") : t("aui.assistantSaid")}
        </span>
        <div className="min-w-0 flex-1 wrap-break-word">
          <MessagePrimitive.Parts components={{ Text: SpokenText }} />
          {isSpeaking && (
            <span role="status" className="sr-only">
              {t("aui.speaking")}
            </span>
          )}
        </div>
        <SpokenActionBar />
      </div>
    </MessagePrimitive.Root>
  );
};

const SpokenActionBar: FC = () => {
  const { t } = useI18n();
  return (
    <ActionBarPrimitive.Root
      hideWhenRunning
      autohide="always"
      className="aui-spoken-action-bar text-muted-foreground flex shrink-0 gap-1"
    >
      <ActionBarPrimitive.Copy asChild>
        <TooltipIconButton tooltip={t("common.copy")} className="size-6">
          <AuiIf condition={(s) => s.message.isCopied}>
            <CheckIcon className="animate-in zoom-in-50 fade-in duration-200 ease-out" />
          </AuiIf>
          <AuiIf condition={(s) => !s.message.isCopied}>
            <CopyIcon className="animate-in zoom-in-75 fade-in duration-150" />
          </AuiIf>
        </TooltipIconButton>
      </ActionBarPrimitive.Copy>
    </ActionBarPrimitive.Root>
  );
};

const FOCUSABLE_SELECTOR =
  "a[href], button:not([disabled]), input:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex='-1']):not([disabled])";

const nextFocusable = (start: Element) => {
  for (let element = start.nextElementSibling; element; element = element.nextElementSibling) {
    const target = element.matches(FOCUSABLE_SELECTOR)
      ? element
      : element.querySelector(FOCUSABLE_SELECTOR);
    if (target instanceof HTMLElement) return target;
  }
  return null;
};

const ThreadLoadEarlier: FC = () => {
  const { t } = useI18n();
  const visible = useAuiState((s) => s.thread.hasEarlier || s.thread.isLoadingEarlier);
  const loading = useAuiState((s) => s.thread.isLoadingEarlier);
  const slotRef = useRef<HTMLDivElement>(null);
  const focusedRef = useRef<Element | null>(null);

  // The last page removes the button; a keyboard user on it continues from the
  // next control in tab order rather than from the document body.
  useLayoutEffect(() => {
    const focused = focusedRef.current;
    const slot = slotRef.current;
    if (visible || !focused || focused.isConnected || !slot) return;
    focusedRef.current = null;
    const active = document.activeElement;
    if (active && active !== document.body) return;
    nextFocusable(slot)?.focus();
  }, [visible]);

  return (
    <div
      ref={slotRef}
      className="contents"
      onFocus={(event) => {
        focusedRef.current = event.target;
      }}
    >
      <span role="status" className="sr-only">
        {loading ? t("aui.loadingEarlier") : ""}
      </span>
      {visible && (
        <ThreadPrimitive.LoadEarlier asChild>
          <Button
            variant="ghost"
            size="sm"
            data-slot="aui_thread-load-earlier"
            className="aui-thread-load-earlier text-muted-foreground mb-6 self-center rounded-full"
          >
            <span className={cn(loading && "shimmer motion-reduce:animate-none")}>
              {loading ? t("aui.loadingEarlier") : t("aui.loadEarlier")}
            </span>
          </Button>
        </ThreadPrimitive.LoadEarlier>
      )}
    </div>
  );
};

const ThreadScrollToBottom: FC = () => {
  const { t } = useI18n();
  return (
    <ThreadPrimitive.ScrollToBottom asChild>
      <TooltipIconButton
        tooltip={t("aui.scrollBottom")}
        variant="outline"
        className="aui-thread-scroll-to-bottom dark:border-border dark:bg-background dark:hover:bg-accent absolute -top-12 z-10 self-center rounded-full p-4 disabled:invisible"
      >
        <ArrowDownIcon />
      </TooltipIconButton>
    </ThreadPrimitive.ScrollToBottom>
  );
};

const ThreadWelcome: FC = () => {
  const { t } = useI18n();
  return (
    <div className="aui-thread-welcome-root mb-6 flex flex-col">
      <p className="aui-thread-welcome-message-inner fade-in slide-in-from-bottom-1 animate-in fill-mode-both text-2xl font-medium tracking-tight duration-200">
        {t("aui.welcome")}
      </p>
    </div>
  );
};

const ThreadSuggestions: FC = () => {
  return (
    <div className="aui-thread-welcome-suggestions flex w-full flex-col">
      <ThreadPrimitive.Suggestions>{() => <ThreadSuggestionItem />}</ThreadPrimitive.Suggestions>
    </div>
  );
};

const ThreadSuggestionItem: FC = () => {
  return (
    <div className="aui-thread-welcome-suggestion-display fade-in slide-in-from-bottom-2 animate-in fill-mode-both duration-200">
      <SuggestionPrimitive.Trigger send asChild>
        <button
          type="button"
          className="aui-thread-welcome-suggestion group hover:bg-foreground/[0.03] focus-visible:ring-ring/50 flex w-full items-baseline gap-2.5 rounded-md px-2 py-2 text-start text-sm transition-colors outline-none focus-visible:ring-1 motion-reduce:transition-none"
        >
          <span
            aria-hidden
            className="text-muted-foreground/60 group-hover:text-foreground font-mono text-xs transition-colors motion-reduce:transition-none"
          >
            {">"}
          </span>
          <span className="min-w-0 flex-1 truncate">
            <SuggestionPrimitive.Title className="aui-thread-welcome-suggestion-text-1 text-foreground" />{" "}
            <SuggestionPrimitive.Description className="aui-thread-welcome-suggestion-text-2 text-muted-foreground empty:hidden" />
          </span>
        </button>
      </SuggestionPrimitive.Trigger>
    </div>
  );
};

const Composer: FC<{ autoFocus: boolean }> = ({ autoFocus }) => {
  const { t } = useI18n();
  return (
    <ComposerPrimitive.Root className="aui-composer-root relative flex w-full flex-col">
      <ComposerPrimitive.AttachmentDropzone asChild>
        <div
          data-slot="aui_composer-shell"
          className="border-foreground/10 focus-within:border-foreground/25 data-[dragging=true]:border-ring flex w-full cursor-text flex-col gap-2 rounded-(--composer-radius) border bg-(--composer-bg) p-(--composer-padding) transition-[border-color] data-[dragging=true]:border-dashed data-[dragging=true]:bg-[color-mix(in_oklab,var(--color-accent)_50%,var(--color-background))]"
        >
          <ComposerAttachments />
          <ComposerPrimitive.Input
            placeholder={t("aui.composerPlaceholder")}
            className="aui-composer-input caret-primary placeholder:text-muted-foreground/60 max-h-48 min-h-10 w-full resize-none bg-transparent px-2.5 py-1 text-base leading-6 outline-none"
            rows={1}
            autoFocus={autoFocus}
            enterKeyHint="send"
            aria-label={t("aui.composerLabel")}
          />
          <ComposerAction />
        </div>
      </ComposerPrimitive.AttachmentDropzone>
    </ComposerPrimitive.Root>
  );
};

const ComposerAction: FC = () => {
  const { t } = useI18n();
  // The stop control only cancels the send while no run it could stop is going.
  const isSending = useAuiState(
    (s) =>
      s.composer.submission !== undefined && !(s.thread.isRunning && s.thread.capabilities.cancel),
  );

  return (
    <div className="aui-composer-action-wrapper relative flex items-center justify-between">
      <ComposerAddAttachment />
      <div className="flex items-center gap-1.5">
        <AuiIf condition={(s) => s.thread.capabilities.dictation}>
          <AuiIf condition={(s) => s.composer.dictation == null}>
            <ComposerPrimitive.Dictate asChild>
              <TooltipIconButton
                tooltip={t("aui.voiceInput")}
                side="bottom"
                type="button"
                variant="ghost"
                size="icon"
                className="aui-composer-dictate text-muted-foreground hover:text-foreground size-7 rounded-full"
                aria-label={t("aui.startDictation")}
              >
                <MicIcon className="aui-composer-dictate-icon size-4" />
              </TooltipIconButton>
            </ComposerPrimitive.Dictate>
          </AuiIf>
          <AuiIf condition={(s) => s.composer.dictation != null}>
            <ComposerPrimitive.StopDictation asChild>
              <TooltipIconButton
                tooltip={t("aui.stopDictation")}
                side="bottom"
                type="button"
                variant="ghost"
                size="icon"
                className="aui-composer-stop-dictation text-destructive size-7 rounded-full"
                aria-label={t("aui.stopVoiceInput")}
              >
                <SquareIcon className="aui-composer-stop-dictation-icon size-3.5 animate-pulse fill-current" />
              </TooltipIconButton>
            </ComposerPrimitive.StopDictation>
          </AuiIf>
        </AuiIf>
        <AuiIf
          condition={(s) =>
            !s.composer.canCancel ||
            (s.thread.voice !== undefined && s.composer.submission === undefined)
          }
        >
          <ComposerPrimitive.Send asChild>
            <TooltipIconButton
              tooltip={t("aui.sendMessage")}
              side="bottom"
              type="button"
              variant="default"
              size="icon"
              className="aui-composer-send size-7 rounded-full"
              aria-label={t("aui.sendMessage")}
            >
              <ArrowUpIcon className="aui-composer-send-icon size-4" />
            </TooltipIconButton>
          </ComposerPrimitive.Send>
        </AuiIf>
        <AuiIf
          condition={(s) =>
            s.composer.canCancel &&
            (s.thread.voice === undefined || s.composer.submission !== undefined)
          }
        >
          <ComposerPrimitive.Cancel asChild>
            <Button
              type="button"
              variant="default"
              size="icon"
              className="aui-composer-cancel size-7 rounded-full"
              aria-label={isSending ? t("aui.cancelSending") : t("aui.stopGenerating")}
            >
              <SquareIcon className="aui-composer-cancel-icon size-3.5 fill-current" />
            </Button>
          </ComposerPrimitive.Cancel>
        </AuiIf>
      </div>
    </div>
  );
};

const AssistantMessage: FC = () => {
  const {
    ToolFallback: ToolFallbackComponent = ToolFallback,
    ToolGroup,
    ReasoningGroup,
    TaskGroup: TaskGroupComponent,
    MessageFooter,
  } = useContext(ThreadComponentsContext);
  const groupBy = TaskGroupComponent ? taskAwareGroupBy : messageGroupBy;
  const messageParts = useAuiState((s) => s.message.parts);
  const toolMetrics = useAuiState((s) => s.message.metadata.custom.toolMetrics);
  const sourceSnapshots = useAuiState((s) => s.message.metadata.custom.sourceSnapshots) as
    | import("@/components/ocg/markdown/source-snapshot").FileSnapshot[]
    | undefined;
  const unsettled = useAuiState((s) => s.message.status?.type === "running");
  const compaction = useAuiState((s) => s.message.metadata.custom.compaction);
  const agentInstruction = useAuiState((s) => s.message.metadata.custom.agentInstruction === true);
  const intermediate = useAuiState((s) => s.message.metadata.custom.intermediate === true);
  const storedReasoningDuration = useAuiState((s) => {
    const value = s.message.metadata.custom.reasoningDurationMs;
    return typeof value === "number" && Number.isFinite(value) && value >= 0
      ? value / 1000
      : undefined;
  });
  const { t } = useI18n();
  const failureReason = useAuiState((s) => {
    const value = s.message.metadata.custom.failureReason;
    return typeof value === "string" ? value : null;
  });
  const failed = useAuiState(
    (s) => s.message.status?.type === "incomplete" && s.message.status.reason === "error",
  );
  const ACTION_BAR_PT = "pt-1.5";
  // Keep the action bar inside the contained root's paint box, then cancel its reserved space in flow.
  const ACTION_BAR_HEIGHT = `min-h-7.5 ${ACTION_BAR_PT}`;

  if (agentInstruction)
    return (
      <MessagePrimitive.Root
        data-slot="agent-instruction"
        className="rounded-xl border border-border bg-muted/30 px-4 py-3"
      >
        <div className="mb-2 flex items-center justify-between gap-2 text-xs font-medium text-muted-foreground">
          <span>{t("chat.agentInstruction")}</span>
          <ActionBarPrimitive.Root autohide="never">
            <ActionBarPrimitive.Copy asChild>
              <TooltipIconButton tooltip={t("common.copy")} className="size-6">
                <AuiIf condition={(s) => s.message.isCopied}>
                  <CheckIcon />
                </AuiIf>
                <AuiIf condition={(s) => !s.message.isCopied}>
                  <CopyIcon />
                </AuiIf>
              </TooltipIconButton>
            </ActionBarPrimitive.Copy>
          </ActionBarPrimitive.Root>
        </div>
        <div className="text-sm leading-relaxed wrap-break-word">
          <MessagePrimitive.Parts components={{ Text: MarkdownText }} />
        </div>
      </MessagePrimitive.Root>
    );

  if (compaction === "predictive" || compaction === "rule_based" || compaction === "manual")
    return (
      <MessagePrimitive.Root
        data-slot="context-compacted"
        className="my-4 flex items-center gap-3 px-2 text-xs text-muted-foreground"
        role="status"
      >
        <span className="h-px flex-1 bg-border" />
        <span>
          {t("chat.compacted")} ·{" "}
          {t(
            compaction === "predictive"
              ? "chat.compactionPredictive"
              : compaction === "rule_based"
                ? "chat.compactionRule"
                : "chat.compactionManual",
          )}
        </span>
        <span className="h-px flex-1 bg-border" />
      </MessagePrimitive.Root>
    );

  if ((intermediate || unsettled) && messageParts.length === 0 && !failureReason && !MessageFooter)
    return null;

  return (
    <MessagePrimitive.Root
      data-slot="aui_assistant-message-root"
      data-role="assistant"
      data-intermediate={intermediate || undefined}
      className={cn(
        "relative -mb-7.5 pb-7.5 [content-visibility:auto]",
        intermediate ? "[contain-intrinsic-size:auto_48px]" : "[contain-intrinsic-size:auto_200px]",
        unsettled && "fade-in slide-in-from-bottom-1 animate-in duration-150",
      )}
    >
      <div
        data-slot="aui_assistant-message-content"
        className={cn(
          "leading-relaxed wrap-break-word",
          intermediate ? "text-sm text-muted-foreground" : "text-foreground",
        )}
      >
        <MessagePrimitive.GroupedParts groupBy={groupBy}>
          {({ part, children }) => {
            switch (part.type) {
              case "group-chainOfThought":
                return <div data-slot="aui_chain-of-thought">{children}</div>;
              case "group-task":
                return TaskGroupComponent ? <TaskGroupComponent group={part} /> : null;
              case "group-tool":
                if (ToolGroup) {
                  return <ToolGroup group={part}>{children}</ToolGroup>;
                }
                return (
                  <ToolGroupRoot variant="ghost">
                    <ToolGroupTrigger
                      count={part.indices.length}
                      active={part.status.type === "running"}
                      summary={
                        <ToolActivityLabel
                          tools={part.indices.flatMap((index) => {
                            const call = messageParts[index];
                            if (call?.type !== "tool-call") return [];
                            const metric: unknown =
                              toolMetrics && typeof toolMetrics === "object"
                                ? Reflect.get(toolMetrics, call.toolCallId)
                                : undefined;
                            return [
                              {
                                name: call.toolName,
                                arguments: call.argsText,
                                result: call.result,
                                durationMs:
                                  metric &&
                                  typeof metric === "object" &&
                                  typeof Reflect.get(metric, "durationMs") === "number"
                                    ? (Reflect.get(metric, "durationMs") as number)
                                    : undefined,
                                truncated: Boolean(
                                  metric &&
                                    typeof metric === "object" &&
                                    Reflect.get(metric, "truncated"),
                                ),
                              } satisfies ToolActivity,
                            ];
                          })}
                        />
                      }
                    />
                    <ToolGroupContent streaming={part.status.type === "running"}>
                      {children}
                    </ToolGroupContent>
                  </ToolGroupRoot>
                );
              case "group-reasoning": {
                if (ReasoningGroup) {
                  return <ReasoningGroup group={part}>{children}</ReasoningGroup>;
                }
                const running = part.status.type === "running";
                return (
                  <ReasoningRoot streaming={running}>
                    <ReasoningTrigger
                      active={unsettled}
                      duration={storedReasoningDuration ?? reasoningDuration(part.timing)}
                      tokens={estimateReasoningTokens(
                        part.indices
                          .map((index) => {
                            const item = messageParts[index];
                            return item?.type === "reasoning" ? item.text : "";
                          })
                          .join(""),
                      )}
                    />
                    <ReasoningContent streaming={running} aria-busy={running}>
                      <ReasoningText>{children}</ReasoningText>
                    </ReasoningContent>
                  </ReasoningRoot>
                );
              }
              case "text":
                return (
                  <SmoothMarkdownText
                    text={part.text}
                    streaming={part.status.type === "running"}
                    sourceSnapshots={sourceSnapshots}
                  />
                );
              case "reasoning":
                return <Reasoning {...part} />;
              case "tool-call":
                return part.toolUI ?? <ToolFallbackComponent {...part} />;
              case "data":
                return part.dataRendererUI;
              case "file":
                return (
                  <div data-slot="aui_assistant-message-file" className="py-1">
                    <MessageFile {...part} />
                  </div>
                );
              case "image":
                return (
                  <div data-slot="aui_assistant-message-image" className="py-1">
                    <Image {...part} alt={part.filename ?? t("aui.messageImage")} />
                  </div>
                );
              case "indicator":
                return null;
              default:
                return null;
            }
          }}
        </MessagePrimitive.GroupedParts>
        {MessageFooter && <MessageFooter />}
        {(failureReason || failed) && (
          <p role="alert" className="text-destructive mt-2 text-sm whitespace-pre-wrap">
            {failureReason ?? t("chat.failed")}
          </p>
        )}
      </div>

      {!intermediate && (
        <div
          data-slot="aui_assistant-message-footer"
          className={cn("flex items-center", ACTION_BAR_HEIGHT)}
        >
          <BranchPicker />
          <AssistantActionBar />
        </div>
      )}
    </MessagePrimitive.Root>
  );
};

const AssistantActionBar: FC = () => {
  const { t } = useI18n();
  return (
    <ActionBarPrimitive.Root
      hideWhenRunning
      autohide="not-last"
      className="aui-assistant-action-bar-root text-muted-foreground animate-in fade-in col-start-3 row-start-2 -ms-1 flex gap-1 duration-200"
    >
      <ActionBarPrimitive.Copy asChild>
        <TooltipIconButton tooltip={t("common.copy")}>
          <AuiIf condition={(s) => s.message.isCopied}>
            <CheckIcon className="animate-in zoom-in-50 fade-in duration-200 ease-out" />
          </AuiIf>
          <AuiIf condition={(s) => !s.message.isCopied}>
            <CopyIcon className="animate-in zoom-in-75 fade-in duration-150" />
          </AuiIf>
        </TooltipIconButton>
      </ActionBarPrimitive.Copy>
      <AuiIf condition={(s) => s.thread.capabilities.feedback}>
        <ActionBarPrimitive.FeedbackPositive asChild>
          <TooltipIconButton
            tooltip={t("aui.helpful")}
            className="data-[submitted=true]:bg-accent data-[submitted=true]:text-accent-foreground"
          >
            <ThumbsUpIcon />
          </TooltipIconButton>
        </ActionBarPrimitive.FeedbackPositive>
        <ActionBarPrimitive.FeedbackNegative asChild>
          <TooltipIconButton
            tooltip={t("aui.notHelpful")}
            className="data-[submitted=true]:bg-accent data-[submitted=true]:text-accent-foreground"
          >
            <ThumbsDownIcon />
          </TooltipIconButton>
        </ActionBarPrimitive.FeedbackNegative>
      </AuiIf>
      <AuiIf condition={(s) => s.message.metadata.custom.canRetry === true}>
        <ActionBarPrimitive.Reload asChild>
          <TooltipIconButton tooltip={t("aui.retryTurn")}>
            <RefreshCwIcon />
          </TooltipIconButton>
        </ActionBarPrimitive.Reload>
      </AuiIf>
      <ActionBarMorePrimitive.Root>
        <ActionBarMorePrimitive.Trigger asChild>
          <TooltipIconButton tooltip={t("aui.more")} className="data-[state=open]:bg-accent">
            <MoreHorizontalIcon />
          </TooltipIconButton>
        </ActionBarMorePrimitive.Trigger>
        <ActionBarMorePrimitive.Content
          side="bottom"
          align="start"
          sideOffset={6}
          className="aui-action-bar-more-content bg-popover text-popover-foreground data-[state=open]:fade-in-0 data-[state=open]:zoom-in-95 data-[state=open]:animate-in data-[state=closed]:fade-out-0 data-[state=closed]:zoom-out-95 data-[state=closed]:animate-out data-[side=bottom]:slide-in-from-top-2 data-[side=left]:slide-in-from-right-2 data-[side=right]:slide-in-from-left-2 data-[side=top]:slide-in-from-bottom-2 z-50 min-w-[8rem] overflow-hidden rounded-xl border p-1.5"
        >
          <ActionBarPrimitive.ExportMarkdown asChild>
            <ActionBarMorePrimitive.Item className="aui-action-bar-more-item hover:bg-accent hover:text-accent-foreground focus:bg-accent focus:text-accent-foreground flex cursor-pointer items-center gap-2 rounded-lg px-2.5 py-1.5 text-sm outline-none select-none">
              <DownloadIcon className="size-4" />
              {t("aui.exportMarkdown")}
            </ActionBarMorePrimitive.Item>
          </ActionBarPrimitive.ExportMarkdown>
        </ActionBarMorePrimitive.Content>
      </ActionBarMorePrimitive.Root>
    </ActionBarPrimitive.Root>
  );
};

const MessageFile: FileMessagePartComponent = (part) => {
  const { t } = useI18n();
  const image = part.mimeType.toLowerCase().startsWith("image/")
    ? getFileHref(part.data, part.mimeType, part.sourceType)
    : null;
  if (!image) return <File {...part} />;
  return (
    <Image
      type="image"
      image={image}
      status={part.status}
      alt={part.filename ?? t("aui.messageImage")}
      {...(part.filename !== undefined && { filename: part.filename })}
    />
  );
};

const UserFilePart: FileMessagePartComponent = (part) => (
  <div data-slot="aui_user-message-file" className="py-1">
    <MessageFile {...part} />
  </div>
);

const UserImagePart: ImageMessagePartComponent = (part) => {
  const { t } = useI18n();
  return (
    <div data-slot="aui_user-message-image" className="py-1">
      <Image {...part} alt={part.filename ?? t("aui.messageImage")} />
    </div>
  );
};

const UserMessage: FC = () => {
  const { t } = useI18n();
  const queued = useAuiState((s) => s.message.metadata.custom.queued === true);
  const withdrawn = useAuiState((s) => s.message.metadata.custom.withdrawn === true);
  const history = useAuiState((s) => s.message.metadata.custom.queueHistory);
  const revisions = Array.isArray(history)
    ? history.filter(
        (item): item is { content: string; at: string } =>
          typeof item === "object" &&
          item !== null &&
          typeof item.content === "string" &&
          typeof item.at === "string",
      )
    : [];
  return (
    <MessagePrimitive.Root
      data-slot="aui_user-message-root"
      className="grid auto-rows-auto grid-cols-[minmax(72px,1fr)_auto] content-start gap-y-2 [contain-intrinsic-size:auto_80px] [content-visibility:auto] [&>*]:col-start-2"
      data-role="user"
    >
      <UserMessageAttachments />

      <div className="aui-user-message-content-wrapper relative col-start-2 min-w-0 justify-self-end">
        <div className="aui-user-message-content peer bg-muted text-foreground rounded-(--composer-radius) px-4 py-2 wrap-break-word empty:hidden">
          <MessagePrimitive.Parts components={{ File: UserFilePart, Image: UserImagePart }} />
        </div>
        <div className="aui-user-action-bar-wrapper absolute start-0 top-1/2 -translate-x-full -translate-y-1/2 pe-2 peer-empty:hidden rtl:translate-x-full">
          <UserActionBar />
        </div>
      </div>

      {withdrawn && (
        <p className="col-start-2 text-right text-xs text-muted-foreground">
          {t("chat.queueWithdrawn")}
        </p>
      )}
      {revisions.length > 0 && (
        <details className="col-start-2 max-w-prose text-xs text-muted-foreground">
          <summary className="cursor-pointer text-right">{t("chat.queueEdited")}</summary>
          {revisions.map((revision, index) => (
            <p key={index} className="mt-2 whitespace-pre-wrap break-words rounded border p-2">
              {revision.content}
            </p>
          ))}
        </details>
      )}
      {queued && (
        <p
          role="status"
          data-slot="queued-message-status"
          className="col-start-2 text-right text-xs text-muted-foreground"
        >
          {t("chat.queuedMessage")}
        </p>
      )}
      <BranchPicker
        data-slot="aui_user-branch-picker"
        className="col-span-full col-start-1 -me-1 justify-end"
      />
    </MessagePrimitive.Root>
  );
};

const UserActionBar: FC = () => {
  const { t } = useI18n();
  return (
    <ActionBarPrimitive.Root
      autohide="not-last"
      className="aui-user-action-bar-root flex flex-col items-end"
    >
      <ActionBarPrimitive.Copy asChild>
        <TooltipIconButton tooltip={t("common.copySource")} className="aui-user-action-copy">
          <AuiIf condition={(s) => s.message.isCopied}>
            <CheckIcon />
          </AuiIf>
          <AuiIf condition={(s) => !s.message.isCopied}>
            <CopyIcon />
          </AuiIf>
        </TooltipIconButton>
      </ActionBarPrimitive.Copy>
    </ActionBarPrimitive.Root>
  );
};

const EditComposer: FC = () => {
  const { t } = useI18n();
  return (
    <MessagePrimitive.Root
      data-slot="aui_edit-composer-wrapper"
      className="flex flex-col px-2 [contain-intrinsic-size:auto_200px] [content-visibility:auto]"
    >
      <ComposerPrimitive.Root className="aui-edit-composer-root border-foreground/10 focus-within:border-foreground/25 ms-auto flex w-full max-w-[85%] cursor-text flex-col rounded-(--composer-radius) border bg-(--composer-bg) transition-[border-color]">
        <ComposerPrimitive.Input
          className="aui-edit-composer-input text-foreground min-h-14 w-full resize-none bg-transparent px-4 pt-3 pb-1 text-base outline-none"
          autoFocus
        />
        <div className="aui-edit-composer-footer mx-2.5 mb-2.5 flex items-center gap-1.5 self-end">
          <ComposerPrimitive.Cancel asChild>
            <Button variant="ghost" size="sm" className="h-8 px-3">
              {t("common.cancel")}
            </Button>
          </ComposerPrimitive.Cancel>
          <ComposerPrimitive.Send asChild>
            <Button size="sm" className="h-8 px-3">
              {t("aui.editUpdate")}
            </Button>
          </ComposerPrimitive.Send>
        </div>
      </ComposerPrimitive.Root>
    </MessagePrimitive.Root>
  );
};

const BranchPicker: FC<BranchPickerPrimitive.Root.Props> = ({ className, ...rest }) => {
  const { t } = useI18n();
  const canSwitch = useAuiState((s) => s.thread.capabilities.switchToBranch);
  if (!canSwitch) return null;
  return (
    <BranchPickerPrimitive.Root
      hideWhenSingleBranch
      className={cn(
        "aui-branch-picker-root text-muted-foreground -ms-2 me-2 inline-flex items-center text-xs",
        className,
      )}
      {...rest}
    >
      <BranchPickerPrimitive.Previous asChild>
        <TooltipIconButton tooltip={t("aui.branchPrevious")}>
          <ChevronLeftIcon />
        </TooltipIconButton>
      </BranchPickerPrimitive.Previous>
      <span className="aui-branch-picker-state font-medium">
        <BranchPickerPrimitive.Number /> / <BranchPickerPrimitive.Count />
      </span>
      <BranchPickerPrimitive.Next asChild>
        <TooltipIconButton tooltip={t("aui.branchNext")}>
          <ChevronRightIcon />
        </TooltipIconButton>
      </BranchPickerPrimitive.Next>
    </BranchPickerPrimitive.Root>
  );
};
