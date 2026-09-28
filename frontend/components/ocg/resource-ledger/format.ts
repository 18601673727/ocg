/**
 * Display formatting for the Resource Ledger Inspector. Presentation only; it
 * never invents values. Missing data always renders as the unknown glyph.
 */

export const UNKNOWN = "—";

const integerFormat = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });
const ratioFormat = new Intl.NumberFormat("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });

export function formatTokens(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return UNKNOWN;
  return integerFormat.format(value);
}

/** Cost is integer micro-units of USD. Zero stays visible and distinct from unknown. */
export function formatCostMicros(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return UNKNOWN;
  return `$${(Math.round(value) / 1_000_000).toFixed(6)}`;
}

export function formatCount(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return UNKNOWN;
  return integerFormat.format(value);
}

/** Formats a 0..1 share as a percentage. */
export function formatPercent(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return UNKNOWN;
  return `${(value * 100).toFixed(1)}%`;
}

/** Formats a leverage ratio such as cached-per-fresh tokens. */
export function formatRatio(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return UNKNOWN;
  return `${ratioFormat.format(value)}×`;
}

export function formatDuration(milliseconds: number | null | undefined): string {
  if (milliseconds === null || milliseconds === undefined || !Number.isFinite(milliseconds)) {
    return UNKNOWN;
  }
  if (milliseconds < 1000) return `${Math.round(milliseconds)}ms`;
  const seconds = milliseconds / 1000;
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const minutes = Math.floor(seconds / 60);
  return `${minutes}m ${Math.round(seconds % 60)}s`;
}

/** Short UTC clock label for a timestamp; falls back to the unknown glyph. */
export function formatTimestamp(timestamp: string): string {
  const ms = Date.parse(timestamp);
  if (!Number.isFinite(ms)) return UNKNOWN;
  return `${new Date(ms).toISOString().slice(11, 19)}Z`;
}

/** ISO date label for a timestamp. */
export function formatDate(timestamp: string): string {
  const ms = Date.parse(timestamp);
  if (!Number.isFinite(ms)) return UNKNOWN;
  return new Date(ms).toISOString().slice(0, 10);
}
