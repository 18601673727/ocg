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
  resolve: (id: string) => Promise<string>,
  id: string,
): Promise<string[]> {
  return [await resolve(id)];
}
