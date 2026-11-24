import type { ApiClient, ConversationSearchScope } from "../runtime/api-client";
import type { ChatSession } from "../types";

export type SearchConversation = Awaited<
  ReturnType<ApiClient["searchConversations"]>
>["conversations"][number];
export type SearchMatch = {
  id: string;
  title: string;
  conversation?: string;
  projectId?: string | null;
  snippet?: string;
};

export function sessionMatchesScope(
  session: { projectId?: string | null },
  scope: ConversationSearchScope,
): boolean {
  if (scope === "all") return true;
  if (scope === "conversations") return !session.projectId;
  return session.projectId === scope.slice("project:".length);
}

export function appendSearchResults(
  previous: readonly SearchConversation[],
  page: readonly SearchConversation[],
): SearchConversation[] {
  const conversations = new Map(previous.map((item) => [item.root_job, item]));
  for (const item of page) conversations.set(item.root_job, item);
  return [...conversations.values()].sort(
    (a, b) => b.tip_created_at_us - a.tip_created_at_us || b.root_job.localeCompare(a.root_job),
  );
}

export function conversationSearchMatches(
  sessions: readonly ChatSession[],
  results: readonly SearchConversation[],
): SearchMatch[] {
  return results.map((item) => {
    const local = sessions.find(
      (session) =>
        session.conversationId === item.root_job ||
        session.id === item.root_job ||
        session.id === item.tip_job,
    );
    return {
      id: local?.id ?? item.root_job,
      title: item.title,
      conversation: item.root_job,
      projectId: item.project ?? local?.projectId ?? null,
      snippet: item.snippet,
    };
  });
}
