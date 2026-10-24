import type { ChatSession } from "../types";
import type { HttpCanonicalControlClient } from "../runtime/control-client";

export type SearchConversation = Awaited<ReturnType<HttpCanonicalControlClient["searchConversations"]>>["conversations"][number];
export type SearchMatch = { id: string; title: string; conversation?: string };

export function appendSearchResults(previous: readonly SearchConversation[], page: readonly SearchConversation[]): SearchConversation[] {
  const conversations = new Map(previous.map(item => [item.root_job, item]));
  for (const item of page) conversations.set(item.root_job, item);
  return [...conversations.values()];
}

export function conversationSearchMatches(sessions: readonly ChatSession[], results: readonly SearchConversation[]): SearchMatch[] {
  return results.map(item => {
    const local = sessions.find(session => session.conversationId === item.root_job || session.id === item.root_job || session.id === item.tip_job);
    return { id: local?.id ?? item.root_job, title: item.title, conversation: item.root_job };
  });
}
