import type { RuntimeSnapshot } from "../runtime/runtime-types";
import { boundLogEntries, normalizeLogEntry, type LogEntry, type LogLevel } from "./log-entries";

export * from "./log-entries";

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
    fixtureEntry({ id: "logs-job-start", timestamp: at(1), level: "info", source: "Job", category: "lifecycle", message: "Job started: diagnose provider recovery", jobId: "job-logs-live", sessionId: "logs-live-session", correlationId: "corr-logs-live" }),
    fixtureEntry({ id: "logs-worker-invocation", timestamp: at(2), level: "info", source: "Worker", category: "invocation", message: "Worker invocation started", jobId: "job-logs-live", taskId: "task-provider-health", workerId: "verify", workerRole: "Verify", provider: "Catalog Provider", model: "example-model-free", invocationId: "invoke-verify-01", sessionId: "logs-live-session" }),
    fixtureEntry({ id: "logs-runtime-warning", timestamp: at(3), level: "warn", source: "Runtime", category: "health", message: "Provider response exceeded the expected latency window", jobId: "job-logs-live", workerId: "verify", workerRole: "Verify", provider: "Catalog Provider", model: "example-model-free", fields: { latencyMs: 1840, thresholdMs: 1200 } }),
    fixtureEntry({ id: "logs-retry", timestamp: at(4), level: "warn", source: "Orchestration", category: "retry", message: "Retry scheduled after transient provider timeout", jobId: "job-logs-live", taskId: "task-provider-health", workerId: "verify", workerRole: "Verify", provider: "Catalog Provider", model: "example-model-free", fields: { attempt: 2, maxAttempts: 3, backoffMs: 500 } }),
    fixtureEntry({ id: "logs-redaction", timestamp: at(5), level: "info", source: "Provider", category: "redaction", message: "Provider diagnostic payload normalized with sensitive fields removed", jobId: "job-logs-live", provider: "Catalog Provider", model: "example-model-free", redacted: true, fields: { redacted: true, sensitiveFieldRemoved: true, fieldCount: 2 } }),
    fixtureEntry({ id: "logs-error", timestamp: at(6), level: "error", source: "Model", category: "invocation", message: "Model invocation failed with a transient provider timeout", jobId: "job-logs-live", taskId: "task-provider-health", workerId: "verify", workerRole: "Verify", provider: "Catalog Provider", model: "example-model-free", invocationId: "invoke-verify-01", fields: { code: "provider-timeout", retryable: true } }),
    fixtureEntry({ id: "logs-recovery", timestamp: at(7), level: "info", source: "Runtime", category: "recovery", message: "Provider health recovered; retry may continue", jobId: "job-logs-live", workerId: "verify", workerRole: "Verify", provider: "Catalog Provider", model: "example-model-free", fields: { health: "ready" } }),
    fixtureEntry({ id: "logs-complete", timestamp: at(8), level: "info", source: "Job", category: "lifecycle", message: "Job completed successfully", jobId: "job-logs-live", sessionId: "logs-live-session", correlationId: "corr-logs-live", fields: { completedTasks: 1, failedAttempts: 1 } }),
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
      timestamp: new Date(activity.at * 1000).toISOString(),
      level: activity.status === "failed" ? "error" : activity.status === "unrecognized" ? "warn" : "info",
      source: "Orchestration",
      category: "call",
      message: activity.message,
      sessionId,
    }));
  }
  return boundLogEntries(entries);
}
