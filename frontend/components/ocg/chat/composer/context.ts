import { configurationIdentity } from "../../runtime/api-client";
export const MUTABLE_CONTEXT_KEY = "ocg.mutable-composer.context.v1";
const QUEUED_KEY = "ocg.mutable-composer.queued.v1";
const UNFINISHED_KEY = "ocg.mutable-composer.unfinished.v1";
const PENDING_KEY = "ocg.mutable-composer.pending-submission.v1";
const contexts = new Map<string, string>();
export const pendingKey = (context: string) => `${PENDING_KEY}:${context}`;
type Pointer = { context: string; job: string };
const conversationContextKey = (conversation: string) =>
  `${MUTABLE_CONTEXT_KEY}:conversation:${conversation}`;
export function bindComposerContext(
  sessionId: string,
  context: string,
  conversation?: string,
): void {
  contexts.set(sessionId, context);
  const key = conversation ? conversationContextKey(conversation) : MUTABLE_CONTEXT_KEY;
  sessionStorage.setItem(key, context);
  localStorage.setItem(key, context);
}
export function contextForSession(sessionId: string, conversation?: string): string {
  const key = conversation ? conversationContextKey(conversation) : null;
  const context =
    contexts.get(sessionId) ??
    (key ? (sessionStorage.getItem(key) ?? localStorage.getItem(key)) : null) ??
    configurationIdentity();
  bindComposerContext(sessionId, context, conversation);
  return context;
}
export function rotateContext(sessionId: string, conversation?: string): string {
  const context = configurationIdentity();
  bindComposerContext(sessionId, context, conversation);
  return context;
}
export function clearGlobalContext(context: string) {
  if (localStorage.getItem(MUTABLE_CONTEXT_KEY) === context)
    localStorage.removeItem(MUTABLE_CONTEXT_KEY);
  if (sessionStorage.getItem(MUTABLE_CONTEXT_KEY) === context)
    sessionStorage.removeItem(MUTABLE_CONTEXT_KEY);
}
export function queuedPointers(): Pointer[] {
  try {
    const value: unknown = JSON.parse(localStorage.getItem(QUEUED_KEY) ?? "[]");
    if (!Array.isArray(value)) return [];
    return value.filter(
      (entry): entry is Pointer =>
        typeof entry?.context === "string" && typeof entry?.job === "string",
    );
  } catch {
    return [];
  }
}
export function savePointer(pointer: Pointer) {
  const list = queuedPointers().filter((entry) => entry.job !== pointer.job);
  localStorage.setItem(QUEUED_KEY, JSON.stringify([...list, pointer]));
}
export function forgetQueued(job: string) {
  localStorage.setItem(
    QUEUED_KEY,
    JSON.stringify(queuedPointers().filter((entry) => entry.job !== job)),
  );
}
export function unfinishedContexts(): string[] {
  try {
    const value: unknown = JSON.parse(localStorage.getItem(UNFINISHED_KEY) ?? "[]");
    return Array.isArray(value)
      ? value.filter((entry): entry is string => typeof entry === "string")
      : [];
  } catch {
    return [];
  }
}
export function rememberUnfinished(context: string) {
  localStorage.setItem(
    UNFINISHED_KEY,
    JSON.stringify([...new Set([...unfinishedContexts(), context])]),
  );
}
export function forgetUnfinished(context: string) {
  localStorage.setItem(
    UNFINISHED_KEY,
    JSON.stringify(unfinishedContexts().filter((item) => item !== context)),
  );
}
