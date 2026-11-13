export const BOUNDED_MESSAGE_CLASS = "ocg-subtle-scrollbar max-h-[55vh] overflow-y-auto";

export function boundedToggle(
  overflowing: boolean,
  expanded: boolean,
): "expand" | "collapse" | null {
  if (expanded) return "collapse";
  return overflowing ? "expand" : null;
}

export type ScrollMetrics = { scrollTop: number; scrollHeight: number; clientHeight: number };

export function followAfterScroll(
  following: boolean,
  previousTop: number,
  metrics: ScrollMetrics,
): boolean {
  if (metrics.scrollTop < previousTop) return false;
  if (metrics.scrollHeight - metrics.clientHeight - metrics.scrollTop <= 8) return true;
  return following;
}
