import type { ChatSession } from "../types";

/**
 * Chat session identity: internal frontend key vs canonical URL identity.
 *
 * - `ChatSession.id` is the internal frontend key. For canonical sessions it
 *   is a composite `JSON.stringify([projectId, sessionId])` used for
 *   snapshot maps (`messagesBySession`, `executionBySession`, ...). For mock
 *   sessions without `sessionId` it is the existing fixture ID.
 * - `ChatSession.sessionId` is the canonical backend Conversation identity
 *   (`session_id`). Local untouched drafts already carry one; no backend
 *   Conversation exists until the first send.
 * - Canonical product URLs must use the raw `sessionId`, never the composite
 *   key. Mock sessions without `sessionId` keep using `id`.
 *
 * URL resolution is always Project-scoped: callers pass an already
 * Project-scoped session list (e.g. `selectProjectSnapshot(...).sessions`),
 * so a `session=` value can never select a session from another Project.
 */

/** Canonical `?session=` value for a Chat session. */
export function canonicalUrlSessionId(session: ChatSession): string {
  return session.sessionId ?? session.id;
}

/**
 * Find a session addressed by a `?session=` value within an already
 * Project-scoped list. Canonical `sessionId` wins; the internal composite
 * `session.id` is accepted for compatibility and normalized by callers.
 */
export function findSessionByUrlParam(
  sessions: readonly ChatSession[],
  param: string | null | undefined,
): ChatSession | undefined {
  if (!param) return undefined;
  return (
    sessions.find((session) => session.sessionId === param) ??
    sessions.find((session) => session.id === param)
  );
}

/**
 * Find a session by either its internal key or its canonical URL identity.
 * Used for in-memory selection state that may briefly hold a raw URL value
 * before it is normalized to the internal key.
 */
export function findSessionByKey(
  sessions: readonly ChatSession[],
  key: string | null | undefined,
): ChatSession | undefined {
  if (!key) return undefined;
  return (
    sessions.find((session) => session.id === key) ??
    sessions.find((session) => session.sessionId === key)
  );
}

/** Internal frontend key for a URL `session=` value, or `null` when unknown. */
export function internalIdForUrlParam(
  sessions: readonly ChatSession[],
  param: string | null | undefined,
): string | null {
  return findSessionByUrlParam(sessions, param)?.id ?? null;
}

/**
 * Canonical URL param for a known internal session key. Falls back to
 * decoding the composite key itself so URL writes stay canonical even when
 * the session object is not at hand (e.g. `rememberSession`).
 */
export function urlParamForInternalId(
  sessions: readonly ChatSession[],
  internalId: string,
): string {
  const session = sessions.find((item) => item.id === internalId);
  if (session) return canonicalUrlSessionId(session);
  return internalIdToUrlParam(internalId);
}

/**
 * Decode a canonical URL param from an internal composite key without a
 * session lookup. `JSON.stringify([projectId, sessionId])` carries the raw
 * `session_id` as its second element; anything else (mock IDs, already
 * canonical values) passes through unchanged.
 */
export function internalIdToUrlParam(internalId: string): string {
  if (!internalId) return internalId;
  try {
    const parsed: unknown = JSON.parse(internalId);
    if (
      Array.isArray(parsed) &&
      parsed.length === 2 &&
      typeof parsed[1] === "string" &&
      parsed[1].length > 0
    ) {
      return parsed[1];
    }
  } catch {
    // Not a composite key: mock IDs and raw session_ids pass through.
  }
  return internalId;
}
