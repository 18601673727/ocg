import type { ChatModelSelection } from "../contracts";

const PREFIX = "ocg.chat.next-turn-selection.v1";

type Storage = Pick<globalThis.Storage, "getItem" | "setItem">;

function storage(): Storage | null {
  try {
    return typeof window === "undefined" ? null : window.localStorage;
  } catch {
    return null;
  }
}

export function modelPreferenceKey(projectId: string | undefined, sessionId: string): string {
  return `${PREFIX}:${JSON.stringify([projectId ?? null, sessionId])}`;
}

export function readModelPreference(
  projectId: string | undefined,
  sessionId: string | undefined,
  store: Storage | null = storage(),
): ChatModelSelection | undefined {
  if (!sessionId || !store) return undefined;
  try {
    const value: unknown = JSON.parse(
      store.getItem(modelPreferenceKey(projectId, sessionId)) ?? "null",
    );
    if (typeof value !== "object" || value === null) return undefined;
    const record = value as Record<string, unknown>;
    if (typeof record.model !== "string" || !record.model) return undefined;
    const selection: ChatModelSelection = {
      model: record.model,
      effort: typeof record.effort === "string" && record.effort ? record.effort : null,
    };
    const uuidV7 = "[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}";
    for (const key of ["agentProfile", "llmProfile", "executor", "project"] as const) {
      const id = record[key];
      if (id !== undefined) {
        if (typeof id !== "string" || !new RegExp(`^${uuidV7}$`).test(id)) return undefined;
        selection[key] = id;
      }
    }
    return selection;
  } catch {
    return undefined;
  }
}

export function writeModelPreference(
  projectId: string | undefined,
  sessionId: string | undefined,
  selection: ChatModelSelection,
  store: Storage | null = storage(),
): void {
  if (!sessionId || !store) return;
  try {
    store.setItem(
      modelPreferenceKey(projectId, sessionId),
      JSON.stringify({
        model: selection.model,
        effort: selection.effort ?? null,
        agentProfile: selection.agentProfile,
        llmProfile: selection.llmProfile,
        executor: selection.executor,
        project: selection.project,
      }),
    );
  } catch {}
}
