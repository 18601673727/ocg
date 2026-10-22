import type { ControlChildConversation } from "../contracts";

/**
 * Idle polling policy for the conversation children strip.
 *
 * Discovery must continue while the view is idle — children created
 * elsewhere have to appear even when the current list is empty or settled —
 * but a fixed two-second loop spends a request per loaded page forever.
 * The strip therefore backs off along a bounded doubling cadence while
 * consecutive refreshes observe the same list, and returns to the base
 * cadence as soon as anything changes: a new signature, a local action
 * (which remounts the refresh loop), or a visibility restoration. A hidden
 * tab schedules nothing at all; returning to the tab refreshes promptly.
 */

/** Base cadence while the view is active: the previous fixed loop. */
export const CHILDREN_POLL_BASE_MS = 2000;

/** Idle ceiling: discovery continues, but never faster than this while quiet. */
export const CHILDREN_POLL_MAX_MS = 30_000;

/** Next delay after a quiet refresh: double, bounded by the idle ceiling. */
export function nextChildrenPollDelay(current: number): number {
  return Math.min(current * 2, CHILDREN_POLL_MAX_MS);
}

/** Stable identity of one refresh result: length, paging, and edge rows. */
export function childrenListSignature(
  children: readonly Pick<ControlChildConversation, "id">[],
  hasMore: boolean,
): string {
  return `${children.length}:${hasMore}:${children[0]?.id ?? ""}:${children.at(-1)?.id ?? ""}`;
}
