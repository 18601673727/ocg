import type { ChatModelSelection } from "../contracts";

/**
 * The next-turn model preference of one Chat, kept in browser storage.
 *
 * It is presentation preference only: it never describes a running Job, and a
 * saved model the current Profile no longer offers falls back to the Profile
 * default in the selector rather than being sent.
 */
const PREFIX = "ocg.chat.next-turn-selection.v1";

type Storage = Pick<globalThis.Storage, "getItem" | "setItem">;

function storage(): Storage | null {
  try {
    return typeof window === "undefined" ? null : window.localStorage;
  } catch {
    return null;
  }
}

export function modelPreferenceKey(projectId: string, sessionId: string): string {
  return `${PREFIX}:${JSON.stringify([projectId, sessionId])}`;
}

export function readModelPreference(projectId: string | undefined, sessionId: string | undefined, store: Storage | null = storage()): ChatModelSelection | undefined {
  if (!projectId || !sessionId || !store) return undefined;
  try {
    const value: unknown = JSON.parse(store.getItem(modelPreferenceKey(projectId, sessionId)) ?? "null");
    if (typeof value !== "object" || value === null) return undefined;
    const record = value as Record<string, unknown>;
    if (typeof record.model !== "string" || !record.model) return undefined;
    return { model: record.model, effort: typeof record.effort === "string" && record.effort ? record.effort : null };
  } catch {
    return undefined;
  }
}

export function writeModelPreference(projectId: string | undefined, sessionId: string | undefined, selection: ChatModelSelection, store: Storage | null = storage()): void {
  if (!projectId || !sessionId || !store) return;
  try {
    store.setItem(modelPreferenceKey(projectId, sessionId), JSON.stringify({ model: selection.model, effort: selection.effort ?? null }));
  } catch {
    // A full or blocked store only loses the preference, never the turn.
  }
}
