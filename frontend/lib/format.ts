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

const integerFormat = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });
const ratioFormat = new Intl.NumberFormat("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const clockFormat = new Intl.DateTimeFormat("en", {
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
});

function isUnknown(value: number | null | undefined): boolean {
  return value === null || value === undefined || !Number.isFinite(value);
}

export function formatTokens(value: number | null | undefined): string {
  if (isUnknown(value)) return UNKNOWN;
  return integerFormat.format(value as number);
}

export function formatCount(value: number | null | undefined): string {
  if (isUnknown(value)) return UNKNOWN;
  return integerFormat.format(value as number);
}

/** Tokens at reading scale: 1_240 -> 1.24K, so a dense table stays scannable. */
export function formatCompactTokens(value: number | null | undefined): string {
  if (isUnknown(value)) return UNKNOWN;
  const amount = value as number;
  if (amount < 1000) return String(amount);
  return `${(amount / 1000).toFixed(amount >= 10000 ? 1 : 2)}K`;
}

/** Cost is integer micro-units of USD. Zero stays visible and distinct from unknown. */
export function formatCostMicros(value: number | null | undefined): string {
  if (isUnknown(value)) return UNKNOWN;
  return `$${(Math.round(value as number) / 1_000_000).toFixed(6)}`;
}

/** Whole-dollar budget view, distinct from the six-decimal per-call cost. */
export function formatDollars(value: number | null | undefined): string {
  if (isUnknown(value)) return UNKNOWN;
  return `$${(value as number).toFixed(2)}`;
}

export function microsToUsd(micros: number): number {
  return micros / 1_000_000;
}

export function usdToMicros(usd: number): number {
  return Math.round(usd * 1_000_000);
}

/** Formats a 0..1 share as a percentage. */
export function formatPercent(value: number | null | undefined): string {
  if (isUnknown(value)) return UNKNOWN;
  return `${((value as number) * 100).toFixed(1)}%`;
}


/** Formats a leverage ratio such as cached-per-fresh tokens. */
export function formatRatio(value: number | null | undefined): string {
  if (isUnknown(value)) return UNKNOWN;
  return `${ratioFormat.format(value as number)}×`;
}

export function formatNumber(value: number | null | undefined): string {
  if (isUnknown(value)) return UNKNOWN;
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 }).format(value as number);
}

export function formatDuration(milliseconds: number | null | undefined): string {
  if (isUnknown(milliseconds)) return UNKNOWN;
  const ms = milliseconds as number;
  if (ms < 1000) return `${Math.round(ms)}ms`;
  const seconds = ms / 1000;
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
}

/** Short UTC clock label, which is how the ledger stores and orders timestamps. */
export function formatTimestamp(timestamp: string | null | undefined): string {
  const ms = Date.parse(timestamp ?? "");
  if (!Number.isFinite(ms)) return UNKNOWN;
  return `${new Date(ms).toISOString().slice(11, 19)}Z`;
}

/** Local clock label for the live log tail, which a reader compares to their own clock. */
export function formatLocalClock(timestamp: string): string {
  const date = new Date(timestamp);
  return Number.isNaN(date.valueOf()) ? timestamp : clockFormat.format(date);
}

/** Full ISO instant, or the raw value when it is not a parseable timestamp. */
export function formatIsoTimestamp(timestamp: string): string {
  const date = new Date(timestamp);
  return Number.isNaN(date.valueOf()) ? timestamp : date.toISOString();
}

/** Status words render as words, not kebab-case wire identifiers. */
export function humanizeStatus(value: string): string {
  return value.replace(/-/g, " ");
}

/**
 * Marks a figure the runtime modelled rather than measured.
 *
 * Provenance is a fact about the number, so it belongs with the formatted
 * output and not in a second formatter per surface.
 */
export function withApproximation(formatted: string, approximate: boolean): string {
  return approximate ? `≈ ${formatted}` : formatted;
}
