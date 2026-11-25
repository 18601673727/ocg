import type { JSX } from "@solidjs/web";
import { type Accessor, createContext, createMemo, createSignal, useContext } from "solid-js";
import { useOcgRuntime, useOcgRuntimeSnapshot } from "../runtime/runtime-context";
import type { ChatMessage } from "../types";
import { type PresentedMessage, presentOcgMessages } from "./message-presentation";
import { planningConclusion } from "./planning-decision";

const EMPTY: ChatMessage[] = [];
type SubmissionNotice = {
  sessionId: string;
  user: ChatMessage;
  answer: ChatMessage;
};
const MessageContext = createContext<Accessor<PresentedMessage>>();
const ConversationContext = createContext<{
  messages: Accessor<PresentedMessage[]>;
  byJob: Accessor<Map<string, ChatMessage[]>>;
  hasEarlier: Accessor<boolean>;
  loadingEarlier: Accessor<boolean>;
  loadEarlier: (scope?: "turns") => Promise<boolean>;
  retry: (message: string) => Promise<void>;
  reportSubmissionNotice: (
    sessionId: string,
    message: string | undefined,
    input: Pick<ChatMessage, "content" | "images" | "jobId">,
    failure?: Pick<ChatMessage, "failureDetail" | "failureLink">,
  ) => void;
}>();

export { MessageContext };

export function ConversationView(props: { sessionId: string; children: JSX.Element }) {
  const { client } = useOcgRuntime();
  const history = useOcgRuntimeSnapshot(
    (snapshot) => snapshot.messagesBySession[props.sessionId] ?? EMPTY,
  );
  const [submissionNotices, setSubmissionNotices] = createSignal<SubmissionNotice[]>([]);
  function reportSubmissionNotice(
    sessionId: string,
    message: string | undefined,
    input: Pick<ChatMessage, "content" | "images" | "jobId">,
    failure?: Pick<ChatMessage, "failureDetail" | "failureLink">,
  ) {
    const previous = submissionNotices().filter(
      (item) => item.sessionId !== sessionId || !input.jobId || item.user.jobId !== input.jobId,
    );
    if (!message) {
      setSubmissionNotices(previous);
      return;
    }
    const id = crypto.randomUUID();
    const createdAt = new Date().toISOString();
    const notice: SubmissionNotice = {
      sessionId,
      user: { ...input, id: `${id}-input`, role: "user", status: "completed", createdAt },
      answer: {
        id,
        role: "assistant",
        content: "",
        failureReason: message,
        ...failure,
        status: "failed",
        createdAt,
      },
    };
    setSubmissionNotices([...previous, notice]);
  }
  const messages = createMemo(() => {
    const pending = new Set(
      submissionNotices().filter((notice) => notice.sessionId === props.sessionId),
    );
    const items: ChatMessage[] = [];
    for (const message of history()) {
      if (message.planningTarget && planningConclusion(message)) continue;
      items.push(message);
      if (message.role !== "user") continue;
      for (const notice of pending) {
        if (!message.jobId || notice.user.jobId !== message.jobId) continue;
        items.push(notice.answer);
        pending.delete(notice);
      }
    }
    for (const notice of pending) items.push(notice.user, notice.answer);
    return presentOcgMessages(items);
  });
  const byJob = createMemo<Map<string, ChatMessage[]>>((previous) => {
    const groups = new Map<string, ChatMessage[]>();
    const items = history();
    // The runtime publishes each turn's projected messages together.
    for (let start = 0; start < items.length; ) {
      const id = items[start].jobId;
      let end = start + 1;
      while (end < items.length && items[end].jobId === id) end++;
      if (id) {
        const prior = previous?.get(id);
        groups.set(
          id,
          prior?.length === end - start && prior.every((message, i) => message === items[start + i])
            ? prior
            : items.slice(start, end),
        );
      }
      start = end;
    }
    return groups;
  });
  const hasEarlier = createMemo(() => {
    history();
    return client.hasEarlierHistory(props.sessionId);
  });
  const [loadingEarlier, setLoadingEarlier] = createSignal(false);
  async function loadEarlier(scope?: "turns") {
    if (loadingEarlier()) return client.loadEarlierHistory(props.sessionId, scope);
    setLoadingEarlier(true);
    try {
      return await client.loadEarlierHistory(props.sessionId, scope);
    } finally {
      setLoadingEarlier(false);
    }
  }
  const value = {
    messages,
    byJob,
    hasEarlier,
    loadingEarlier,
    loadEarlier,
    retry: (message: string) => client.retryMessage(props.sessionId, message),
    reportSubmissionNotice,
  };
  return <ConversationContext value={value}>{props.children}</ConversationContext>;
}

export function useConversationView() {
  const state = useContext(ConversationContext);
  if (!state) throw new Error("Conversation view is missing");
  return state;
}

export function useThreadMessage() {
  const message = useContext(MessageContext);
  if (!message) throw new Error("Thread message is missing");
  return message;
}
