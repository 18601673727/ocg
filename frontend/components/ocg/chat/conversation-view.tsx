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
