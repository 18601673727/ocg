import type { RuntimeSnapshot } from "../runtime/runtime-types";

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
  missionId?: string;
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
};

export type LogTimeWindow = "all" | "last-5-minutes" | "last-hour" | "last-day";

export type LogFilters = {
  text?: string;
  level?: LogLevel | "all";
  source?: string;
  missionId?: string;
  workerId?: string;
  workerRole?: string;
  provider?: string;
  model?: string;
  timeWindow?: LogTimeWindow;
};

export const LOG_LEVELS: readonly LogLevel[] = ["trace", "debug", "info", "warn", "error"];

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

export function boundLogEntries(entries: LogEntry[], limit = 120): LogEntry[] {
  if (limit <= 0) return [];
  return entries.slice(Math.max(0, entries.length - limit));
}

function matchesTimeWindow(entry: LogEntry, entries: LogEntry[], timeWindow: LogTimeWindow): boolean {
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

export function filterLogEntries(entries: LogEntry[], filters: LogFilters = {}): LogEntry[] {
  const query = filters.text?.trim().toLowerCase();
  const timeWindow = filters.timeWindow ?? "all";
  return entries.filter((entry) => {
    const searchable = [
      entry.message,
      entry.source,
      entry.category,
      entry.missionId,
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
      (!filters.missionId || entry.missionId === filters.missionId) &&
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

const LOG_BASE = "2026-09-25T10:00:00.000Z";

function at(minutes: number): string {
  return new Date(Date.parse(LOG_BASE) + minutes * 60_000).toISOString();
}

function fixtureEntry(entry: LogEntry): LogEntry {
  return normalizeLogEntry(entry);
}

/** Deterministic, display-safe sequence used by ?scenario=logs-live. */
export function createLogsLiveFixture(projectScope?: string): LogEntry[] {
  const entries = [
    fixtureEntry({ id: "logs-startup", timestamp: at(0), level: "info", source: "OCG Core", category: "startup", message: "Workspace runtime initialized", sessionId: "logs-live-session", correlationId: "corr-logs-live" }),
    fixtureEntry({ id: "logs-mission-start", timestamp: at(1), level: "info", source: "Mission", category: "lifecycle", message: "Mission started: diagnose provider recovery", missionId: "mission-logs-live", sessionId: "logs-live-session", correlationId: "corr-logs-live" }),
    fixtureEntry({ id: "logs-worker-invocation", timestamp: at(2), level: "info", source: "Worker", category: "invocation", message: "Worker invocation started", missionId: "mission-logs-live", taskId: "task-provider-health", workerId: "verify", workerRole: "Verify", provider: "OpenCode Zen", model: "Muse Spark 1.3 Contributor Free", invocationId: "invoke-verify-01", sessionId: "logs-live-session" }),
    fixtureEntry({ id: "logs-runtime-warning", timestamp: at(3), level: "warn", source: "Runtime", category: "health", message: "Provider response exceeded the expected latency window", missionId: "mission-logs-live", workerId: "verify", workerRole: "Verify", provider: "OpenCode Zen", model: "Muse Spark 1.3 Contributor Free", fields: { latencyMs: 1840, thresholdMs: 1200 } }),
    fixtureEntry({ id: "logs-retry", timestamp: at(4), level: "warn", source: "Orchestration", category: "retry", message: "Retry scheduled after transient provider timeout", missionId: "mission-logs-live", taskId: "task-provider-health", workerId: "verify", workerRole: "Verify", provider: "OpenCode Zen", model: "Muse Spark 1.3 Contributor Free", fields: { attempt: 2, maxAttempts: 3, backoffMs: 500 } }),
    fixtureEntry({ id: "logs-redaction", timestamp: at(5), level: "info", source: "Provider", category: "redaction", message: "Provider diagnostic payload normalized with sensitive fields removed", missionId: "mission-logs-live", provider: "OpenCode Zen", model: "Muse Spark 1.3 Contributor Free", redacted: true, fields: { redacted: true, sensitiveFieldRemoved: true, fieldCount: 2 } }),
    fixtureEntry({ id: "logs-error", timestamp: at(6), level: "error", source: "Model", category: "invocation", message: "Model invocation failed with a transient provider timeout", missionId: "mission-logs-live", taskId: "task-provider-health", workerId: "verify", workerRole: "Verify", provider: "OpenCode Zen", model: "Muse Spark 1.3 Contributor Free", invocationId: "invoke-verify-01", fields: { code: "provider-timeout", retryable: true } }),
    fixtureEntry({ id: "logs-recovery", timestamp: at(7), level: "info", source: "Runtime", category: "recovery", message: "Provider health recovered; retry may continue", missionId: "mission-logs-live", workerId: "verify", workerRole: "Verify", provider: "OpenCode Zen", model: "Muse Spark 1.3 Contributor Free", fields: { health: "ready" } }),
    fixtureEntry({ id: "logs-complete", timestamp: at(8), level: "info", source: "Mission", category: "lifecycle", message: "Mission completed successfully", missionId: "mission-logs-live", sessionId: "logs-live-session", correlationId: "corr-logs-live", fields: { completedTasks: 1, failedAttempts: 1 } }),
  ];
  if (!projectScope) return entries;
  const sessionId = `logs-${projectScope}-session`;
  return entries.map((entry) => ({ ...entry, sessionId }));
}

/** Build a useful read-only log view for non-stream scenarios. */
export function deriveRuntimeLogEntries(snapshot: RuntimeSnapshot, sessionId: string): LogEntry[] {
  const entries: LogEntry[] = [
    fixtureEntry({ id: "runtime-startup", timestamp: at(0), level: "info", source: "OCG Core", category: "startup", message: "Workspace runtime initialized", sessionId }),
    fixtureEntry({ id: "runtime-health", timestamp: at(1), level: snapshot.status.state === "failed" ? "error" : snapshot.status.state === "connected" ? "info" : "warn", source: "Runtime", category: "health", message: snapshot.status.detail ?? `Runtime is ${snapshot.status.state}`, sessionId }),
  ];
  const observability = snapshot.observabilityBySession[sessionId];
  for (const activity of observability?.activities ?? []) {
    const level: LogLevel = activity.kind === "invocation-failed" ? "error" : activity.kind === "retry-scheduled" ? "warn" : "info";
    entries.push(fixtureEntry({
      id: `observability-${activity.id}`,
      timestamp: at(Math.max(0, activity.elapsedMs / 60_000)),
      level,
      source: activity.role === "lead" ? "Orchestration" : "Worker",
      category: activity.kind,
      message: activity.summary,
      workerId: activity.workerId,
      workerRole: activity.role,
      provider: activity.provider,
      model: activity.model,
      sessionId,
    }));
  }
  const execution = snapshot.executionBySession[sessionId];
  for (const activity of execution?.activities ?? []) {
    entries.push(fixtureEntry({
      id: `execution-${activity.id}`,
      timestamp: at(Math.max(0, activity.elapsedMs / 60_000)),
      level: activity.status === "failed" || activity.status === "blocked" ? "error" : activity.status === "retrying" ? "warn" : "info",
      source: "Mission",
      category: activity.kind,
      message: activity.message,
      missionId: activity.missionId,
      taskId: activity.taskId,
      workerId: activity.workerId,
      sessionId,
    }));
  }
  return boundLogEntries(entries);
}
