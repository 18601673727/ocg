import type { JSX } from "@solidjs/web";
import { type Accessor, createContext, createMemo, createSignal, useContext } from "solid-js";
import { useOcgRuntime, useOcgRuntimeSnapshot } from "../runtime/runtime-context";
import type { ChatMessage } from "../types";
import { type PresentedMessage, presentOcgMessages } from "./message-presentation";
import { planningConclusion } from "./planning-decision";

const EMPTY: ChatMessage[] = [];
const MessageContext = createContext<Accessor<PresentedMessage>>();
const ConversationContext = createContext<{
  messages: Accessor<PresentedMessage[]>;
  byJob: Accessor<Map<string, ChatMessage[]>>;
  hasEarlier: Accessor<boolean>;
  loadingEarlier: Accessor<boolean>;
  loadEarlier: () => Promise<void>;
  retry: (message: string) => Promise<void>;
}>();

export { MessageContext };

export function ConversationView(props: { sessionId: string; children: JSX.Element }) {
  const { client } = useOcgRuntime();
  const history = useOcgRuntimeSnapshot(
    (snapshot) => snapshot.messagesBySession[props.sessionId] ?? EMPTY,
  );
  const messages = createMemo(() =>
    presentOcgMessages(
      history().filter((message) => !message.planningTarget || !planningConclusion(message)),
    ),
  );
  const byJob = createMemo(() => {
    const groups = new Map<string, ChatMessage[]>();
    for (const message of history()) {
      if (!message.jobId) continue;
      const list = groups.get(message.jobId);
      if (list) list.push(message);
      else groups.set(message.jobId, [message]);
    }
    return groups;
  });
  const hasEarlier = createMemo(() => {
    history();
    return client.hasEarlierHistory(props.sessionId);
  });
  const [loadingEarlier, setLoadingEarlier] = createSignal(false);
  async function loadEarlier() {
    if (loadingEarlier()) return;
    setLoadingEarlier(true);
    try {
      await client.loadEarlierHistory(props.sessionId);
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
