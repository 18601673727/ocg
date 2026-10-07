/**
 * Long Chat messages render inside a bounded viewport. These rules are the
 * presentation contract; nothing here is persisted or sent to the backend.
 */

/** Bounded height of a collapsed message: about half the viewport. */
export const BOUNDED_MESSAGE_CLASS = "max-h-[55vh] overflow-y-auto";

/** Which toggle a message offers: none for content that fits. */
export function boundedToggle(overflowing: boolean, expanded: boolean): "expand" | "collapse" | null {
  if (expanded) return "collapse";
  return overflowing ? "expand" : null;
}

export type ScrollMetrics = { scrollTop: number; scrollHeight: number; clientHeight: number };

/**
 * Whether a streaming message keeps following its newest output after the
 * reader scrolled. Reaching the bottom resumes following; moving up stops it,
 * so the reader's position is never fought.
 */
export function followAfterScroll(following: boolean, previousTop: number, metrics: ScrollMetrics): boolean {
  if (metrics.scrollHeight - metrics.clientHeight - metrics.scrollTop <= 8) return true;
  return metrics.scrollTop < previousTop ? false : following;
}
