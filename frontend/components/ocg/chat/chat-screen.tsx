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
import { useAuiState } from "@assistant-ui/react";
import { OcgAssistantProvider } from "../runtime/assistant-ui-provider";
import { useOcgRuntime } from "../runtime/runtime-context";
import { ChatComposer, ConversationChildrenStrip, ConversationCardStrip } from "./chat-view";
import { ImageGallery } from "./image-attachments";
import { TaskStatus } from "../job/task-status";
import { ConversationArtifacts } from "./conversation-artifacts";
import { FileChanges } from "./file-changes";
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
import { ListPlus, Pencil, X } from "lucide-react";
import { Textarea } from "@/components/ui/textarea";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import { useI18n } from "../i18n";
import type { ChatSession, QueuedChatMessage, RuntimeStatus } from "../types";
import type { ChatModelSelection } from "../contracts";
import type { JobExecution } from "../execution/domain";
import type { ComposerIntent } from "../composer/domain";
import { ContextTranscript } from "./context-transcript";

type QueueState = { queue: QueuedChatMessage[]; paused: boolean };

export interface ChatScreenProps {
  session: ChatSession;
  onOpenConversation: (conversation: string) => void;
  backButton?: ReactNode;
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
  queueing: boolean;
  steerAfterCompletion: boolean;
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
        sessionId={value.session.id}
        queueState={value.queueState}
        runtimeStatus={value.runtimeStatus}
        onResumeQueue={value.onResumeQueue}
      />
      <ChatComposer
        sessionId={value.session.id}
        conversationId={value.session.conversationId}
        projectId={value.session.projectId}
        busy={value.busy}
        queueing={value.queueing}
        steerAfterCompletion={value.steerAfterCompletion}
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
    <div className="aui-thread-welcome-root mb-6 flex flex-col">
      <p className="text-2xl font-medium tracking-tight">{t("chat.newThread")}</p>
      <p className="mt-1 text-sm text-muted-foreground">
        {t("chat.emptyPrefix")} {t("chat.emptySuffix")}
      </p>
    </div>
  );
};

function QueuePanel({
  sessionId,
  queueState,
  runtimeStatus,
  onResumeQueue,
}: {
  sessionId: string;
  queueState?: QueueState;
  runtimeStatus: RuntimeStatus;
  onResumeQueue?: () => void;
}) {
  const { t } = useI18n();
  if (!queueState?.queue.length) return null;
  return (
    <div
      className="mx-auto w-full border-t border-border px-3 py-2 sm:px-5"
      aria-label={t("chat.queue")}
    >
      <div className="mb-1 flex items-center gap-2 text-xs text-muted-foreground" role="status">
        <ListPlus className="size-3.5" aria-hidden="true" />
        {t("chat.queueCount", { count: queueState.queue.length })}
        {queueState.paused && (
          <>
            <span>· {t("chat.queuePaused")}</span>
            <Button
              type="button"
              size="xs"
              variant="ghost"
              disabled={runtimeStatus.state !== "connected"}
              onClick={onResumeQueue}
            >
              {t("chat.resumeQueue")}
            </Button>
          </>
        )}
      </div>
      <ScrollArea
        className="max-h-[min(7rem,15dvh)]"
        viewportProps={{ className: "ocg-subtle-scrollbar" }}
      >
        <ol>
          {queueState.queue.map((item, index) => (
            <QueuedMessageRow
              key={item.id}
              sessionId={sessionId}
              item={item}
              index={index}
              connected={runtimeStatus.state === "connected"}
            />
          ))}
        </ol>
      </ScrollArea>
    </div>
  );
}

function QueuedMessageRow({
  sessionId,
  item,
  index,
  connected,
}: {
  sessionId: string;
  item: QueuedChatMessage;
  index: number;
  connected: boolean;
}) {
  const { client } = useOcgRuntime();
  const { t } = useI18n();
  const [editing, setEditing] = useState(false);
  const content = item.input.content;
  const [draft, setDraft] = useState(content);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  const act = async (edit: boolean) => {
    if (busy) return;
    setBusy(true);
    setError(undefined);
    try {
      if (edit) {
        if (!client.editQueuedMessage) throw new Error(t("chat.queueEditUnavailable"));
        await client.editQueuedMessage(sessionId, item.id, draft);
        setEditing(false);
      } else {
        if (!client.removeQueuedMessage) throw new Error(t("chat.queueEditUnavailable"));
        await client.removeQueuedMessage(sessionId, item.id);
      }
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setBusy(false);
    }
  };
  return (
    <li className="py-1 text-xs">
      <div className="flex items-center gap-2">
        <span className="text-muted-foreground">{index + 1}.</span>
        <span className="min-w-0 flex-1 truncate">
          {content || t("chat.imageCount", { count: item.input.images?.length ?? 0 })}
        </span>
        <ImageGallery images={item.input.images} compact />
        <Button
          type="button"
          size="icon-xs"
          variant="ghost"
          disabled={!connected || busy}
          aria-label={t("common.edit")}
          onClick={() => {
            setDraft(content);
            setEditing(true);
          }}
        >
          <Pencil className="size-3" aria-hidden="true" />
        </Button>
        <Button
          type="button"
          size="icon-xs"
          variant="ghost"
          disabled={!connected || busy}
          aria-label={t("chat.removeQueued")}
          onClick={() => void act(false)}
        >
          <X className="size-3" aria-hidden="true" />
        </Button>
      </div>
      {editing && (
        <div className="mt-2 space-y-2">
          <Textarea
            value={draft}
            onChange={(event) => setDraft(event.target.value)}
            disabled={busy}
            aria-label={t("chat.editQueued")}
          />
          <div className="flex justify-end gap-2">
            <Button size="xs" variant="ghost" disabled={busy} onClick={() => setEditing(false)}>
              {t("common.cancel")}
            </Button>
            <Button
              size="xs"
              disabled={!connected || busy || !draft.trim()}
              onClick={() => void act(true)}
            >
              {t("common.save")}
            </Button>
          </div>
        </div>
      )}
      {error && (
        <p role="alert" className="mt-1 text-destructive">
          {error}
        </p>
      )}
    </li>
  );
}

function MessageFileChanges() {
  const value = useContext(ChatComposerContext);
  const id = useAuiState((state) => state.message.id);
  const { snapshot } = useOcgRuntime();
  const messages = value ? (snapshot.messagesBySession[value.session.id] ?? []) : [];
  const message = messages.find((message) => message.id === id);
  if (
    !message?.jobId ||
    messages.findLast((item) => item.role === "assistant" && item.jobId === message.jobId)?.id !==
      id
  )
    return null;
  const calls = messages
    .filter((item) => item.jobId === message.jobId)
    .flatMap((item) => item.toolCalls ?? []);
  const job = message.jobId;
  return <FileChanges changes={calls.map((call) => ({ job, call }))} />;
}

const THREAD_COMPONENTS = {
  Composer: ChatComposerSlot,
  Welcome,
  MessageFooter: MessageFileChanges,
};

export const ChatScreen: FC<ChatScreenProps> = ({
  session,
  onOpenConversation,
  backButton,
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
    if (session.conversationId && selection)
      writeModelPreference(session.projectId, session.conversationId, selection);
  }, [session.conversationId, session.projectId, selection]);
  const messages = runtime.snapshot.messagesBySession[session.id] ?? [];
  const planMessage = messages.findLast(
    (message) => message.role === "assistant" && message.taskPlan,
  );
  const liveExecution = isExecutionActive(execution) ? execution : null;
  const messageStreaming = messages.some(
    (message) =>
      message.role === "assistant" &&
      (message.status === "streaming" || message.status === "pending"),
  );
  const busy = messageStreaming || liveExecution !== null;
  const lastAssistant = messages.findLast(
    (message) => message.role === "assistant" && !message.intermediate,
  );
  const steerAfterCompletion = !messageStreaming && lastAssistant?.status === "completed";
  const queueing =
    !steerAfterCompletion &&
    (liveExecution?.state === "running" ||
      liveExecution?.state === "cancelling" ||
      messages.some((message) => message.role === "assistant" && message.status === "streaming"));
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
      queueing,
      steerAfterCompletion,
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
      queueing,
      steerAfterCompletion,
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
        <div className="flex h-full min-h-0 w-full flex-col">
          <Thread
            components={THREAD_COMPONENTS}
            footerAction={backButton}
            autoFocus
            afterMessages={
              <>
                <ContextTranscript
                  key={`context:${session.id}`}
                  conversation={session.conversationId}
                />
                {planMessage && <TaskStatus key={planMessage.jobId} plan={planMessage.taskPlan} />}
                <ConversationArtifacts key={`artifacts:${session.id}`} sessionId={session.id} />
                <ConversationCardStrip
                  key={`cards:${session.id}`}
                  turn={session.sessionId}
                  conversationId={session.conversationId}
                  onOpen={onOpenConversation}
                />
                <ConversationChildrenStrip
                  key={session.id}
                  turn={session.sessionId}
                  plan={planMessage?.taskPlan}
                  onOpen={onOpenConversation}
                />
              </>
            }
          />
        </div>
      </ChatComposerContext.Provider>
    </OcgAssistantProvider>
  );
};
