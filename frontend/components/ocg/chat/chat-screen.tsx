import { ChevronRight } from "@lucide/solid";
import {
  type Component,
  createContext,
  createEffect,
  createMemo,
  createSignal,
  Show,
  useContext,
} from "solid-js";
import { shallowSelector, type View } from "@/lib/ui-state";
import type { ChatModelSelection } from "../contracts";
import { useI18n } from "../i18n";
import { TaskStatus } from "../job/task-status";
import { useOcgRuntime, useOcgRuntimeSnapshot } from "../runtime/runtime-context";
import type { ChatSession } from "../types";
import { ConversationCardStrip, ConversationChildrenStrip } from "./chat-view";
import { ContextTranscript } from "./context-transcript";
import { ConversationArtifacts } from "./conversation-artifacts";
import { ConversationView, useConversationView, useThreadMessage } from "./conversation-view";
import { DecisionDetails } from "./decision-details";
import { FileChanges } from "./file-changes";
import { readModelPreference, writeModelPreference } from "./model-preference";
import { contextForSession, MutableComposer } from "./mutable-composer";
import { planningBlocker } from "./planning-decision";
import { AssistantMessage, Thread } from "./thread";

export interface ChatScreenProps {
  session: ChatSession;
  onOpenConversation: (conversation: string) => void;
  onOpenPlanningDecision: (decisionId: string) => void;
  backButton?: View;
}

type ChatComposerContextValue = {
  session: ChatSession;
  selection?: ChatModelSelection;
  setSelection: (selection: ChatModelSelection | undefined) => void;
  onPreference: (selection: ChatModelSelection) => void;
  onOpenPlanningDecision: (decisionId: string) => void;
};

const ChatComposerContext = createContext<ChatComposerContextValue | null>(null);

const HistoryComposer: Component = () => {
  const value = useContext(ChatComposerContext);
  const i18n = useI18n();
  return (
    <p class="py-2 text-center text-sm text-muted-foreground">
      {i18n.t(value?.session.archived ? "chat.archivedReadOnly" : "chat.closedReadOnly")}
    </p>
  );
};

const ChatComposerSlot: Component = () => {
  const value = useContext(ChatComposerContext);
  const { api } = useOcgRuntime();
  const [model, setModel] = createSignal<"checking" | "mutable" | "error">("checking");
  const [probeError, setProbeError] = createSignal("");
  const [composerContext, setComposerContext] = createSignal("");
  const [probedSession, setProbedSession] = createSignal("");
  const sessionId = value?.session.id;
  const conversationId = value?.session.conversationId;
  createEffect(
    () => [api, sessionId],
    () => {
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
    },
  );
  if (!value) return null;
  return (
    <>
      {(() => {
        if (model() === "checking" || probedSession() !== sessionId)
          return <p role="status">Checking Composer availability…</p>;
        if (model() === "error") return <p role="alert">Composer unavailable: {probeError()}</p>;
        return (
          <MutableComposer
            sessionId={value.session.id}
            conversation={value.session.conversationId}
            project={value.session.projectId}
            context={composerContext()}
            selection={value.selection}
            onSelectionChange={value.setSelection}
            onPreference={value.onPreference}
          />
        );
      })()}
    </>
  );
};

const Welcome: Component = () => {
  const i18n = useI18n();
  return (
    <header class="aui-thread-welcome-root mb-4 sm:mb-5">
      <h2 class="text-3xl font-semibold tracking-tight sm:text-4xl">{i18n.t("chat.newThread")}</h2>
    </header>
  );
};

function MessageFileChanges() {
  const record = useThreadMessage();
  const { byJob } = useConversationView();
  const job = createMemo(() => record().jobId);
  const jobMessages = createMemo(
    () => {
      const id = job();
      return id ? byJob().get(id) : undefined;
    },
    {
      equals: (left, right) =>
        left === right ||
        Boolean(
          left &&
            right &&
            left.length === right.length &&
            left.every((entry, index) => entry === right[index]),
        ),
    },
  );
  return (
    <Show
      when={record().decision}
      fallback={
        <Show
          when={
            job() &&
            jobMessages()?.findLast((entry) => entry.role === "assistant")?.id === record().id
          }
        >
          <FileChanges
            changes={(jobMessages() ?? []).flatMap((item) =>
              (item.toolCalls ?? []).map((call) => ({ job: job()!, call })),
            )}
          />
        </Show>
      }
    >
      {(decision) => (
        <section class="my-3 rounded-lg border p-3" aria-label="Decision">
          <DecisionDetails decision={decision()} message={record()} />
        </section>
      )}
    </Show>
  );
}

function PlanningDecisionNotice() {
  const composer = useContext(ChatComposerContext);
  const message = useThreadMessage();
  const { t } = useI18n();
  const reason = createMemo(() => t("planning.blocked", { reason: planningBlocker(t, message()) }));
  return (
    <Show when={message().jobId}>
      {(id) => (
        <div class="flex min-w-0 items-center gap-3 text-xs text-muted-foreground">
          <span class="min-w-0 truncate" title={reason()} role="status">
            {reason()}
          </span>
          <button
            id={`planning-notice-${id()}`}
            type="button"
            class="inline-flex shrink-0 items-center gap-1 rounded-sm py-1 text-primary underline-offset-4 hover:underline focus-visible:outline-2 focus-visible:outline-ring"
            aria-controls="chat-inspector"
            onClick={() => composer?.onOpenPlanningDecision(id())}
          >
            {t("planning.openExecution")}
            <ChevronRight aria-hidden="true" class="size-3" />
          </button>
        </div>
      )}
    </Show>
  );
}

function ChatAssistantMessage() {
  const message = useThreadMessage();
  return (
    <Show
      when={message().planningTarget}
      fallback={<AssistantMessage footer={<MessageFileChanges />} />}
    >
      <PlanningDecisionNotice />
    </Show>
  );
}

export const ChatScreen: Component<ChatScreenProps> = (props) => {
  const [selection, setSelection] = createSignal<ChatModelSelection | undefined>(
    (() => {
      if (props.session.llmProfile) {
        return {
          model: props.session.model ?? "",
          effort: props.session.reasoningEffort ?? null,
          agentProfile: props.session.agentProfile,
          llmProfile: props.session.llmProfile,
        };
      }
      return readModelPreference(
        props.session.projectId,
        props.session.conversationId ?? props.session.id,
      );
    })(),
  );
  createEffect(
    () => [props.session.conversationId, props.session.projectId, selection()],
    () => {
      if (props.session.conversationId && selection())
        writeModelPreference(props.session.projectId, props.session.conversationId, selection()!);
    },
  );
  const selectedPlanPlanJobId = useOcgRuntimeSnapshot(
    shallowSelector((snapshot) => {
      const message = snapshot.messagesBySession[props.session.id]?.findLast(
        (item) => item.role === "assistant" && item.taskPlan,
      );
      return { plan: message?.taskPlan, planJobId: message?.jobId };
    }),
  );

  const onPreference = (next: ChatModelSelection) =>
    writeModelPreference(
      props.session.projectId,
      props.session.conversationId ?? props.session.id,
      next,
    );
  const composerValue: ChatComposerContextValue = {
    get session() {
      return props.session;
    },
    get selection() {
      return selection();
    },
    setSelection,
    onPreference,
    onOpenPlanningDecision: (id) => props.onOpenPlanningDecision(id),
  };
  return (
    <ConversationView sessionId={props.session.id}>
      <ChatComposerContext value={composerValue}>
        <div class="flex h-full min-h-0 w-full flex-col">
          <Thread
            Assistant={ChatAssistantMessage}
            Composer={
              props.session.archived || props.session.closed ? HistoryComposer : ChatComposerSlot
            }
            Welcome={Welcome}
            footerAction={props.backButton}
            afterMessages={
              <>
                <ContextTranscript conversation={props.session.conversationId} />
                {selectedPlanPlanJobId().plan && <TaskStatus plan={selectedPlanPlanJobId().plan} />}
                <ConversationArtifacts sessionId={props.session.id} />
                <ConversationCardStrip
                  turn={props.session.sessionId}
                  conversationId={props.session.conversationId}
                  onOpen={props.onOpenConversation}
                />
                <ConversationChildrenStrip
                  conversationId={props.session.conversationId}
                  turn={props.session.sessionId}
                  plan={selectedPlanPlanJobId().plan}
                  onOpen={props.onOpenConversation}
                />
              </>
            }
          />
        </div>
      </ChatComposerContext>
    </ConversationView>
  );
};
