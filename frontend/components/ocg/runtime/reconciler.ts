import type { ChatMessage, OcgRuntimeEvent } from "../types";
import type { RuntimeSnapshot } from "./runtime-types";

function sessionExists(
  snapshot: RuntimeSnapshot,
  sessionId: string | undefined,
): sessionId is string {
  return sessionId !== undefined && snapshot.sessions.some((session) => session.id === sessionId);
}

function setMessages(
  snapshot: RuntimeSnapshot,
  sessionId: string,
  messages: ChatMessage[],
): RuntimeSnapshot {
  return {
    ...snapshot,
    messagesBySession: { ...snapshot.messagesBySession, [sessionId]: messages },
  };
}

export function reconcileRuntimeEvent(
  snapshot: RuntimeSnapshot,
  event: OcgRuntimeEvent,
): RuntimeSnapshot {
  const sessionId =
    "sessionId" in event && typeof event.sessionId === "string" ? event.sessionId : undefined;
  switch (event.type) {
    case "conversation.session-deleted": {
      if (!sessionExists(snapshot, sessionId)) return snapshot;
      const withoutSession = <T>(entries: Record<string, T>) =>
        Object.fromEntries(Object.entries(entries).filter(([id]) => id !== sessionId));
      return {
        ...snapshot,
        sessions: snapshot.sessions.filter((session) => session.id !== sessionId),
        messagesBySession: withoutSession(snapshot.messagesBySession),
        executionBySession: withoutSession(snapshot.executionBySession),
        ...(snapshot.chatQueues ? { chatQueues: withoutSession(snapshot.chatQueues) } : {}),
      };
    }
    case "conversation.queue-updated": {
      if (!sessionExists(snapshot, sessionId)) return snapshot;
      return {
        ...snapshot,
        chatQueues: {
          ...snapshot.chatQueues,
          [sessionId]: { queue: event.queue, paused: event.paused },
        },
      };
    }
    case "conversation.history-loaded": {
      if (!sessionExists(snapshot, sessionId)) return snapshot;
      return setMessages(snapshot, sessionId, event.messages);
    }
    case "runtime.status-changed":
      return { ...snapshot, status: event.status };

    case "conversation.session-created": {
      const session = event.session;
      const exists = snapshot.sessions.some((item) => item.id === session.id);
      if (exists) {
        return {
          ...snapshot,
          sessions: snapshot.sessions.map((item) => (item.id === session.id ? session : item)),
        };
      }
      return {
        ...snapshot,
        sessions: [session, ...snapshot.sessions],
        messagesBySession: {
          ...snapshot.messagesBySession,
          [session.id]: snapshot.messagesBySession[session.id] ?? [],
        },
        executionBySession: {
          ...snapshot.executionBySession,
          [session.id]: snapshot.executionBySession[session.id] ?? null,
        },
      };
    }

    case "conversation.session-updated": {
      const session = event.session;
      if (!snapshot.sessions.some((item) => item.id === session.id)) return snapshot;
      return {
        ...snapshot,
        sessions: snapshot.sessions.map((item) => (item.id === session.id ? session : item)),
      };
    }
  }
}
