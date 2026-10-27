import { isJobIdentity } from "./view-domain";

export function parseConversationPath(pathname: string): string[] | null {
  const parts = pathname.replace(/\/$/, "").split("/").slice(1);
  if (!parts.length || parts.length % 2 || parts.length > 34) return null;
  const ids: string[] = [];
  for (let i = 0; i < parts.length; i += 2) {
    if (parts[i] !== "jobs" || !isJobIdentity(parts[i + 1]) || ids.includes(parts[i + 1]))
      return null;
    ids.push(parts[i + 1]);
  }
  return ids;
}

export function conversationPath(ids: readonly string[]) {
  if (
    !ids.length ||
    ids.length > 17 ||
    ids.some((id) => !isJobIdentity(id)) ||
    new Set(ids).size !== ids.length
  )
    throw new Error("Invalid conversation path.");
  return "/" + ids.map((id) => `jobs/${id}`).join("/");
}

export async function conversationAncestors(
  readJob: (id: string) => Promise<{
    id: string;
    kind: "turn" | "conversation";
    conversation: string | null;
    parent?: string;
  }>,
  job: string,
): Promise<string[]> {
  const ids: string[] = [];
  let current = await readJob(job);
  if (current.kind === "turn") {
    if (!current.conversation) throw new Error("The Job does not belong to a conversation.");
    current = await readJob(current.conversation);
  }
  for (let depth = 0; depth <= 16; depth++) {
    if (current.kind !== "conversation" || ids.includes(current.id))
      throw new Error("Invalid conversation ancestry.");
    ids.push(current.id);
    if (!current.parent) return ids.reverse();
    current = await readJob(current.parent);
  }
  throw new Error("Conversation ancestry exceeds the depth limit.");
}
