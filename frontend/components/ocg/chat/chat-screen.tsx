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
import { AssistantMessage, Thread } from "@/components/assistant-ui/elements/thread.aui";
import { useAuiState } from "@assistant-ui/react";
import { ChevronRight } from "lucide-react";
import { OcgAssistantProvider } from "../runtime/assistant-ui-provider";
import { useOcgRuntime } from "../runtime/runtime-context";
import { ConversationChildrenStrip, ConversationCardStrip } from "./chat-view";
import { WorkspaceToolFallback } from "./workspace-tool";
import { TaskStatus } from "../job/task-status";
import { ConversationArtifacts } from "./conversation-artifacts";
import { FileChanges } from "./file-changes";
import { readModelPreference, writeModelPreference } from "./model-preference";
import { useI18n } from "../i18n";
import type { ChatSession } from "../types";
import type { ChatModelSelection } from "../contracts";
import { ContextTranscript } from "./context-transcript";
import { MutableComposer, contextForSession } from "./mutable-composer";
import { DecisionDetails } from "./decision-details";
import { planningBlocker } from "./planning-decision";

export interface ChatScreenProps {
  session: ChatSession;
  onOpenConversation: (conversation: string) => void;
  onOpenPlanningDecision: (decisionId: string) => void;
  backButton?: ReactNode;
}

type ChatComposerContextValue = {
  session: ChatSession;
  selection?: ChatModelSelection;
  setSelection: (selection: ChatModelSelection | undefined) => void;
  onPreference: (selection: ChatModelSelection) => void;
  onOpenPlanningDecision: (decisionId: string) => void;
};

const ChatComposerContext = createContext<ChatComposerContextValue | null>(null);

const ChatComposerSlot: FC = () => {
  const value = useContext(ChatComposerContext);
  const { api } = useOcgRuntime();
  const [model, setModel] = useState<"checking" | "mutable" | "error">("checking");
  const [probeError, setProbeError] = useState("");
  const [composerContext, setComposerContext] = useState("");
  const [probedSession, setProbedSession] = useState("");
  const sessionId = value?.session.id;
  const conversationId = value?.session.conversationId;
  useEffect(() => {
    if (!api || !sessionId) return;
    const abort = new AbortController();
    void Promise.resolve()
      .then(() => {
        setModel("checking");
        return contextForSession(sessionId, conversationId);
      })
      .then((context) => {
        if (!abort.signal.aborted) setComposerContext(context);
        return api.currentMutableJob(context, abort.signal);
      })
      .then(() => {
        if (!abort.signal.aborted) {
          setProbedSession(sessionId);
          setModel("mutable");
        }
      })
      .catch((error: unknown) => {
        if (abort.signal.aborted) return;
        setProbedSession(sessionId);
        setProbeError(error instanceof Error ? error.message : String(error));
        setModel("error");
      });
    return () => abort.abort();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [api, sessionId]);
  if (!value) return null;
  if (model === "checking" || probedSession !== sessionId)
    return <p role="status">Checking Composer availability…</p>;
  if (model === "error") return <p role="alert">Composer unavailable: {probeError}</p>;
  return (
    <MutableComposer
      key={value.session.id}
      sessionId={value.session.id}
      conversation={value.session.conversationId}
      project={value.session.projectId}
      context={composerContext}
      selection={value.selection}
      onSelectionChange={value.setSelection}
      onPreference={value.onPreference}
    />
  );
};

const Welcome: FC = () => {
  const { t } = useI18n();
  return (
    <header className="aui-thread-welcome-root mb-4 sm:mb-5">
      <h2 className="text-3xl font-semibold tracking-tight sm:text-4xl">{t("chat.newThread")}</h2>
    </header>
  );
};

function MessageFileChanges() {
  const value = useContext(ChatComposerContext);
  const id = useAuiState((state) => state.message.id);
  const { snapshot } = useOcgRuntime();
  const messages = value ? (snapshot.messagesBySession[value.session.id] ?? []) : [];
  const message = messages.find((message) => message.id === id);
  if (message?.decision)
    return (
      <section className="my-3 rounded-lg border p-3" aria-label="Decision">
        <DecisionDetails decision={message.decision} message={message} />
      </section>
    );
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

function PlanningDecisionNotice() {
  const value = useContext(ChatComposerContext);
  const id = useAuiState((state) => state.message.id);
  const { snapshot } = useOcgRuntime();
  const { t } = useI18n();
  if (!value) return null;
  const message = snapshot.messagesBySession[value.session.id]?.find((item) => item.id === id);
  if (!message?.jobId) return null;
  const decisionId = message.jobId;
  const reason = t("planning.blocked", { reason: planningBlocker(t, message) });
  return (
    <div className="flex min-w-0 items-center gap-3 text-xs text-muted-foreground">
      <span className="min-w-0 truncate" title={reason} role="status">
        {reason}
      </span>
      <button
        id={`planning-notice-${decisionId}`}
        type="button"
        className="inline-flex shrink-0 items-center gap-1 rounded-sm py-1 text-primary underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-ring"
        aria-controls="chat-inspector"
        onClick={() => value.onOpenPlanningDecision(decisionId)}
      >
        {t("planning.openExecution")}
        <ChevronRight aria-hidden="true" className="size-3" />
      </button>
    </div>
  );
}

function ChatAssistantMessage() {
  const planning = useAuiState((state) => state.message.metadata.custom.planningTarget);
  return planning ? <PlanningDecisionNotice /> : <AssistantMessage />;
}

const THREAD_COMPONENTS = {
  AssistantMessage: ChatAssistantMessage,
  Composer: ChatComposerSlot,
  Welcome,
  ToolFallback: WorkspaceToolFallback,
  MessageFooter: MessageFileChanges,
};

export const ChatScreen: FC<ChatScreenProps> = ({
  session,
  onOpenConversation,
  onOpenPlanningDecision,
  backButton,
}) => {
  const runtime = useOcgRuntime();
  const [selection, setSelection] = useState<ChatModelSelection | undefined>(() => {
    if (session.llmProfile) {
      return {
        model: session.model ?? "",
        effort: session.reasoningEffort ?? null,
        agentProfile: session.agentProfile,
        llmProfile: session.llmProfile,
      };
    }
    return readModelPreference(session.projectId, session.conversationId ?? session.id);
  });
  useEffect(() => {
    if (session.conversationId && selection)
      writeModelPreference(session.projectId, session.conversationId, selection);
  }, [session.conversationId, session.projectId, selection]);
  const messages = runtime.snapshot.messagesBySession[session.id] ?? [];
  const planMessage = messages.findLast(
    (message) => message.role === "assistant" && message.taskPlan,
  );

  const onPreference = useCallback(
    (next: ChatModelSelection) =>
      writeModelPreference(session.projectId, session.conversationId ?? session.id, next),
    [session.id, session.projectId, session.conversationId],
  );
  const composerValue = useMemo<ChatComposerContextValue>(
    () => ({ session, selection, setSelection, onPreference, onOpenPlanningDecision }),
    [session, selection, onPreference, onOpenPlanningDecision],
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
