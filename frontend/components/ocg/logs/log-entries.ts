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

export const LOG_LEVELS: readonly LogLevel[] = ["trace", "debug", "info", "warn", "error"];

const SENSITIVE_FIELD =
  /api[-_ ]?key|authorization|auth[-_ ]?header|bearer|cookie|credential|jwt|password|secret|token/i;

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

  if (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean" ||
    value === null
  ) {
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
