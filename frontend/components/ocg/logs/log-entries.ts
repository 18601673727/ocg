export type LogLevel = "trace" | "debug" | "info" | "warn" | "error";

/**
 * Sources are intentionally strings rather than a closed union. A future
 * runtime adapter may add a source without requiring a UI release first.
 */
export type LogSource = string;

export type LogFieldValue =
  | string
  | number
  | boolean
  | null
  | LogFieldValue[]
  | { [key: string]: LogFieldValue };

export type LogEntry = {
  id: string;
  timestamp: string;
  level: LogLevel;
  source: LogSource;
  category?: string;
  message: string;
  jobId?: string;
  taskId?: string;
  workerId?: string;
  workerRole?: string;
  provider?: string;
  model?: string;
  correlationId?: string;
  sessionId?: string;
  invocationId?: string;
  fields?: Record<string, LogFieldValue>;
  redacted?: boolean;
  provenance?: "demo" | "derived" | "snapshot";
};

export type LogTimeWindow = "all" | "last-5-minutes" | "last-hour" | "last-day";

export type LogFilters = {
  text?: string;
  level?: LogLevel | "all";
  source?: string;
  jobId?: string;
  workerId?: string;
  workerRole?: string;
  provider?: string;
  model?: string;
  timeWindow?: LogTimeWindow;
};

export const LOG_LEVELS: readonly LogLevel[] = ["trace", "debug", "info", "warn", "error"];

/** How many tail entries a surface keeps. The renderer shows this bound. */
export const LOG_HISTORY_LIMIT = 120;

const SENSITIVE_FIELD = /api[-_ ]?key|authorization|auth[-_ ]?header|bearer|cookie|credential|jwt|password|secret|token/i;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function redactValue(value: unknown): { value: LogFieldValue; redacted: boolean } {
  if (Array.isArray(value)) {
    let redacted = false;
    const next = value.map((item) => {
      const result = redactValue(item);
      redacted ||= result.redacted;
      return result.value;
    });
    return { value: next, redacted };
  }

  if (isRecord(value)) {
    let redacted = false;
    const next: Record<string, LogFieldValue> = {};
    for (const [key, item] of Object.entries(value)) {
      if (SENSITIVE_FIELD.test(key)) {
        next[key] = "[redacted]";
        redacted = true;
        continue;
      }
      const result = redactValue(item);
      next[key] = result.value;
      redacted ||= result.redacted;
    }
    return { value: next, redacted };
  }

  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean" || value === null) {
    return { value, redacted: false };
  }

  return { value: String(value), redacted: false };
}

/** Normalize structured fields before they reach a log renderer. */
export function redactLogFields(fields: Record<string, unknown> | undefined): {
  fields?: Record<string, LogFieldValue>;
  redacted: boolean;
} {
  if (!fields) return { redacted: false };
  const result = redactValue(fields);
  return { fields: result.value as Record<string, LogFieldValue>, redacted: result.redacted };
}

/** Apply the frontend redaction policy to a normalized entry. */
export function normalizeLogEntry(entry: LogEntry): LogEntry {
  const safe = redactLogFields(entry.fields);
  return {
    ...entry,
    ...(safe.fields ? { fields: safe.fields } : {}),
    redacted: Boolean(entry.redacted || safe.redacted),
  };
}

export function boundLogEntries(entries: readonly LogEntry[], limit = LOG_HISTORY_LIMIT): LogEntry[] {
  if (limit <= 0) return [];
  return entries.slice(Math.max(0, entries.length - limit));
}

function matchesTimeWindow(entry: LogEntry, entries: readonly LogEntry[], timeWindow: LogTimeWindow): boolean {
  if (timeWindow === "all") return true;
  const latest = entries.reduce((max, item) => Math.max(max, Date.parse(item.timestamp)), Number.NEGATIVE_INFINITY);
  const timestamp = Date.parse(entry.timestamp);
  const duration = timeWindow === "last-5-minutes"
    ? 5 * 60_000
    : timeWindow === "last-hour"
      ? 60 * 60_000
      : 24 * 60 * 60_000;
  return Number.isFinite(timestamp) && Number.isFinite(latest) && timestamp >= latest - duration;
}

export function filterLogEntries(entries: readonly LogEntry[], filters: LogFilters = {}): LogEntry[] {
  const query = filters.text?.trim().toLowerCase();
  const timeWindow = filters.timeWindow ?? "all";
  return entries.filter((entry) => {
    const searchable = [
      entry.message,
      entry.source,
      entry.category,
      entry.jobId,
      entry.taskId,
      entry.workerId,
      entry.workerRole,
      entry.provider,
      entry.model,
      entry.correlationId,
      entry.sessionId,
      entry.invocationId,
    ].filter(Boolean).join(" ").toLowerCase();
    return (
      (!query || searchable.includes(query)) &&
      (!filters.level || filters.level === "all" || entry.level === filters.level) &&
      (!filters.source || entry.source === filters.source) &&
      (!filters.jobId || entry.jobId === filters.jobId) &&
      (!filters.workerId || entry.workerId === filters.workerId) &&
      (!filters.workerRole || entry.workerRole === filters.workerRole) &&
      (!filters.provider || entry.provider === filters.provider) &&
      (!filters.model || entry.model === filters.model) &&
      matchesTimeWindow(entry, entries, timeWindow)
    );
  });
}

export function newLogCount(
  currentCount: number,
  lastSeenCount: number,
  state: { following: boolean; atBottom: boolean },
): number {
  if (state.following && state.atBottom) return 0;
  return Math.max(0, currentCount - lastSeenCount);
}
