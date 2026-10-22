"use client";

import {
  createContext,
  useContext,
  useEffect,
  useCallback,
  useMemo,
  useState,
  type FC,
  type ReactNode,
} from "react";
import { Thread } from "@/components/assistant-ui/elements/thread.aui";
import { OcgAssistantProvider } from "../runtime/assistant-ui-provider";
import { useOcgRuntime } from "../runtime/runtime-context";
import { ChatComposer, ConversationChildrenStrip, ConversationCardStrip } from "./chat-view";
import { ImageGallery } from "./image-attachments";
import { readModelPreference, writeModelPreference } from "./model-preference";
import {
  activeExecutionTarget,
  currentActivity,
  executionActivity,
  isExecutionActive,
  type ActiveExecutionTarget,
  type ActivityStep,
  type ExecutionPhase,
} from "./execution-status";
import { ListPlus, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { useI18n } from "../i18n";
import type { ChatSession, QueuedChatMessage, RuntimeStatus } from "../types";
import type { ChatModelSelection } from "../contracts";
import type { JobExecution } from "../execution/domain";
import type { ComposerIntent } from "../composer/domain";

type QueueState = { queue: QueuedChatMessage[]; paused: boolean };

export interface ChatScreenProps {
  session: ChatSession;
  onOpenConversation: (conversation: string) => void;
  execution: JobExecution | null;
  runtimeStatus: RuntimeStatus;
  onComposerIntent: (intent: ComposerIntent) => Promise<void> | void;
  onCancel: () => Promise<void>;
  queueState?: QueueState;
  onRemoveQueued?: (id: string) => void;
  onResumeQueue?: () => void;
  composerSurface?: ReactNode;
  /**
   * Compact Project selector for the row above the composer. The shell owns
   * the selection; the composer only presents it.
   */
  projectSelector?: ReactNode;
}

type ChatComposerContextValue = {
  session: ChatSession;
  busy: boolean;
  queueState?: QueueState;
  selection?: ChatModelSelection;
  setSelection: (selection: ChatModelSelection) => void;
  draft: string;
  setDraft: (draft: string) => void;
  active: ActiveExecutionTarget | null;
  activity?: { current: ActivityStep | ExecutionPhase | null; steps: ActivityStep[] };
  runtimeStatus: RuntimeStatus;
  onPreference: (selection: ChatModelSelection) => void;
  onComposerIntent: (intent: ComposerIntent) => Promise<void> | void;
  onCancel: () => Promise<void>;
  composerSurface?: ReactNode;
  projectSelector?: ReactNode;
  onRemoveQueued?: (id: string) => void;
  onResumeQueue?: () => void;
};

const ChatComposerContext = createContext<ChatComposerContextValue | null>(null);

const ChatComposerSlot: FC<{ autoFocus: boolean }> = ({ autoFocus }) => {
  const value = useContext(ChatComposerContext);
  if (!value) return null;

  return (
    <div className="flex w-full flex-col">
      {value.composerSurface && (
        <div className="border-b border-border">{value.composerSurface}</div>
      )}
      <QueuePanel
        queueState={value.queueState}
        runtimeStatus={value.runtimeStatus}
        onRemoveQueued={value.onRemoveQueued}
        onResumeQueue={value.onResumeQueue}
      />
      <ChatComposer
        sessionId={value.session.id}
        projectId={value.session.projectId}
        busy={value.busy}
        queueing={false}
        selection={value.selection}
        onSelectionChange={value.setSelection}
        onPreference={value.onPreference}
        active={value.active}
        activity={value.activity}
        onCancel={value.onCancel}
        autoFocus={autoFocus}
        projectSelector={value.projectSelector}
        draft={value.draft}
        onDraftChange={value.setDraft}
        onIntent={value.onComposerIntent}
        runtimeStatus={value.runtimeStatus}
      />
    </div>
  );
};

const Welcome: FC = () => {
  const { t } = useI18n();
  return (
    <div className="aui-thread-welcome-root mb-6 flex flex-col px-2">
      <p className="text-2xl font-medium tracking-tight">{t("chat.newThread")}</p>
      <p className="mt-1 text-sm text-muted-foreground">{t("chat.emptyPrefix")} {t("chat.emptySuffix")}</p>
    </div>
  );
};

function QueuePanel({
  queueState,
  runtimeStatus,
  onRemoveQueued,
  onResumeQueue,
}: {
  queueState?: QueueState;
  runtimeStatus: RuntimeStatus;
  onRemoveQueued?: (id: string) => void;
  onResumeQueue?: () => void;
}) {
  const { t } = useI18n();
  if (!queueState?.queue.length) return null;
  return (
    <div className="mx-auto w-full border-t border-border px-3 py-2 sm:px-5" aria-label={t("chat.queue")}>
      <div className="mb-1 flex items-center gap-2 text-xs text-muted-foreground" role="status">
        <ListPlus className="size-3.5" aria-hidden="true" />
        {t("chat.queueCount", { count: queueState.queue.length })}
        {queueState.paused && (
          <>
            <span>· {t("chat.queuePaused")}</span>
            <Button type="button" size="xs" variant="ghost" disabled={runtimeStatus.state !== "connected"} onClick={onResumeQueue}>
              {t("chat.resumeQueue")}
            </Button>
          </>
        )}
      </div>
      <ol className="max-h-[min(7rem,15dvh)] overflow-y-auto">
        {queueState.queue.map((item, index) => (
          <li key={item.id} className="flex items-center gap-2 py-1 text-xs">
            <span className="text-muted-foreground">{index + 1}.</span>
            <span className="min-w-0 flex-1 truncate">
              {item.input.content || t("chat.imageCount", { count: item.input.images?.length ?? 0 })}
            </span>
            <ImageGallery images={item.input.images} compact />
            <Button
              type="button"
              size="icon-xs"
              variant="ghost"
              className="size-11 rounded-md sm:size-7"
              aria-label={t("chat.removeQueued")}
              onClick={() => onRemoveQueued?.(item.id)}
            >
              <X className="size-3" aria-hidden="true" />
            </Button>
          </li>
        ))}
      </ol>
    </div>
  );
}

const THREAD_COMPONENTS = { Composer: ChatComposerSlot, Welcome };

export const ChatScreen: FC<ChatScreenProps> = ({
  session,
  onOpenConversation,
  execution,
  runtimeStatus,
  onComposerIntent,
  queueState,
  onRemoveQueued,
  onResumeQueue,
  onCancel,
  composerSurface,
  projectSelector,
}) => {
  const runtime = useOcgRuntime();
  const [draft, setDraft] = useState("");
  const [selection, setSelection] = useState<ChatModelSelection | undefined>(() =>
    readModelPreference(session.projectId, session.conversationId ?? session.id),
  );
  useEffect(() => {
    if (session.conversationId && selection) writeModelPreference(session.projectId, session.conversationId, selection);
  }, [session.conversationId, session.projectId, selection]);
  const messages = runtime.snapshot.messagesBySession[session.id] ?? [];
  const liveExecution = isExecutionActive(execution) ? execution : null;
  const messageStreaming = messages.some((message) =>
    message.role === "assistant" && (message.status === "streaming" || message.status === "pending"),
  );
  const busy = messageStreaming || liveExecution !== null;
  const active = activeExecutionTarget(liveExecution);
  const activity = useMemo(() => {
    if (!liveExecution) return undefined;
    const steps = executionActivity(liveExecution);
    return { steps, current: currentActivity(liveExecution, steps) };
  }, [liveExecution]);

  const onPreference = useCallback(
    (next: ChatModelSelection) =>
      writeModelPreference(session.projectId, session.conversationId ?? session.id, next),
    [session.id, session.projectId, session.conversationId],
  );
  const composerValue = useMemo<ChatComposerContextValue>(
    () => ({
      session,
      busy,
      queueState,
      selection,
      setSelection,
      draft,
      setDraft,
      active,
      activity,
      runtimeStatus,
      onPreference,
      onComposerIntent,
      onCancel,
      composerSurface,
      projectSelector,
      onRemoveQueued,
      onResumeQueue,
    }),
    [
      session,
      busy,
      queueState,
      selection,
      draft,
      active,
      activity,
      runtimeStatus,
      onPreference,
      onComposerIntent,
      onCancel,
      composerSurface,
      projectSelector,
      onRemoveQueued,
      onResumeQueue,
    ],
  );
  return (
    <OcgAssistantProvider sessionId={session.id} projectId={session.projectId ?? ""}>
      <ChatComposerContext.Provider value={composerValue}>
        <div className={cn("flex h-full min-h-0 w-full flex-col", busy && "[&_[data-slot=aui-thread-viewport]]:scroll-smooth")}>
          <ConversationCardStrip key={`cards:${session.id}`} turn={session.sessionId} onOpen={onOpenConversation} />
          <Thread components={THREAD_COMPONENTS} autoFocus afterMessages={<ConversationChildrenStrip key={session.id} turn={session.sessionId} onOpen={onOpenConversation} />} />
        </div>
      </ChatComposerContext.Provider>
    </OcgAssistantProvider>
  );
};
