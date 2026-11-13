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
import { ConversationChildrenStrip, ConversationCardStrip } from "./chat-view";
import { WorkspaceToolFallback } from "./workspace-tool";
import { TaskStatus } from "../job/task-status";
import { ConversationArtifacts } from "./conversation-artifacts";
import { FileChanges } from "./file-changes";
import { readModelPreference, writeModelPreference } from "./model-preference";
import { useI18n } from "../i18n";
import { executionDetail, executionLabel } from "../runtime/job-status";
import type { ChatSession } from "../types";
import type { ChatModelSelection } from "../contracts";
import { ContextTranscript } from "./context-transcript";
import { MutableComposer, contextForSession } from "./mutable-composer";

export interface ChatScreenProps {
  session: ChatSession;
  onOpenConversation: (conversation: string) => void;
  backButton?: ReactNode;
}

type ChatComposerContextValue = {
  session: ChatSession;
  selection?: ChatModelSelection;
  setSelection: (selection: ChatModelSelection) => void;
  onPreference: (selection: ChatModelSelection) => void;
};

const ChatComposerContext = createContext<ChatComposerContextValue | null>(null);

const ChatComposerSlot: FC = () => {
  const value = useContext(ChatComposerContext);
  const { control } = useOcgRuntime();
  const [model, setModel] = useState<"checking" | "mutable" | "error">("checking");
  const [probeError, setProbeError] = useState("");
  const [composerContext, setComposerContext] = useState("");
  const [probedSession, setProbedSession] = useState("");
  const sessionId = value?.session.id;
  const conversationId = value?.session.conversationId;
  useEffect(() => {
    if (!control || !sessionId) return;
    const abort = new AbortController();
    void Promise.resolve()
      .then(() => {
        setModel("checking");
        return contextForSession(sessionId, conversationId);
      })
      .then((context) => {
        if (!abort.signal.aborted) setComposerContext(context);
        return control.currentMutableJob(context, abort.signal);
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
  }, [control, sessionId]);
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
  const { t } = useI18n();
  const value = useContext(ChatComposerContext);
  const id = useAuiState((state) => state.message.id);
  const { snapshot } = useOcgRuntime();
  const messages = value ? (snapshot.messagesBySession[value.session.id] ?? []) : [];
  const message = messages.find((message) => message.id === id);
  if (message?.decision) {
    const { request, result } = message.decision;
    return (
      <section className="my-3 rounded-lg border p-3" aria-label="Decision">
        <strong>Decision · {request.question.kind.replaceAll("_", " ")}</strong>
        <p className="mt-2 whitespace-pre-wrap">{request.question.prompt}</p>
        {request.question.options?.map((option) => (
          <p key={option.id}>
            {option.id}: {option.description}
          </p>
        ))}
        {request.question.levels?.map((level) => (
          <p key={level.value}>
            {level.value}: {level.description}
          </p>
        ))}
        <details className="mt-2">
          <summary>Caller state</summary>
          <pre className="overflow-x-auto whitespace-pre-wrap">
            {JSON.stringify(request.state, null, 2)}
          </pre>
        </details>
        {result ? (
          <>
            <strong>Result</strong>
            <pre className="overflow-x-auto whitespace-pre-wrap">
              {JSON.stringify(result.outcome, null, 2)}
            </pre>
          </>
        ) : (
          <div className="mt-2" role="status">
            <p>
              <strong>
                {message.execution
                  ? executionLabel(t, message.execution)
                  : message.status === "pending"
                    ? "Pending"
                    : `Job ${message.status}`}
              </strong>
              {message.decision.failure ? `: ${message.decision.failure}` : ""}
            </p>
            {message.execution && executionDetail(t, message.execution) && (
              <p className="text-muted-foreground">{executionDetail(t, message.execution)}</p>
            )}
            {message.status === "unknown" &&
              message.failureReason === undefined &&
              !(message.execution && executionDetail(t, message.execution)) && (
                <p className="text-muted-foreground">
                  The outcome could not be established; it is not reported as stopped.
                </p>
              )}
          </div>
        )}
      </section>
    );
  }
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
  ToolFallback: WorkspaceToolFallback,
  MessageFooter: MessageFileChanges,
};

export const ChatScreen: FC<ChatScreenProps> = ({ session, onOpenConversation, backButton }) => {
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
    () => ({ session, selection, setSelection, onPreference }),
    [session, selection, onPreference],
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
