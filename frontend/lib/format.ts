/**
 * Shared display formatting. Presentation only: it never invents a value, and
 * missing data always renders as the unknown glyph.
 *
 * Every surface that shows a count, cost, duration or clock label uses these so
 * the same quantity reads identically in the ledger, the inspector and Job
 * Execution. A surface with a genuinely different rule (a ledger needs UTC, the
 * log tail needs a local clock) gets its own named function here rather than a
 * second private helper in a feature folder.
 */

export const UNKNOWN = "—";

/** Short UTC clock label, which is how the ledger stores and orders timestamps. */
export function formatTimestamp(timestamp: string | null | undefined): string {
  const ms = Date.parse(timestamp ?? "");
  if (!Number.isFinite(ms)) return UNKNOWN;
  return `${new Date(ms).toISOString().slice(11, 19)}Z`;
}
