import type { WorkerStatus } from "../types";

export type UsageProvenance = "estimated" | "reported";

export type UsageValue = {
  value: number;
  provenance: UsageProvenance;
};

export type TokenUsage = {
  input?: UsageValue;
  output?: UsageValue;
  reasoning?: UsageValue;
  cacheRead?: UsageValue;
  cacheWrite?: UsageValue;
  total?: UsageValue;
};

export type WorkerRuntimeStats = {
  workerId: string;
  role: "lead" | "worker";
  label: string;
  provider: string;
  model: string;
  variant?: string;
  status: WorkerStatus;
  startedAt?: string;
  finishedAt?: string;
  elapsedMs?: number;
  invocationCount: number;
  retryCount: number;
  successCount?: number;
  failureCount?: number;
  tokenUsage: TokenUsage;
  costMicros?: UsageValue;
  latencyMs?: number;
  ttftMs?: number;
  tokensPerSecond?: number;
};

export type JobRuntimeStats = {
  jobId: string;
  tokenUsage: TokenUsage;
  costMicros?: UsageValue;
  estimatedFinalSpend?: UsageValue;
  elapsedMs?: number;
  invocationCount: number;
  retryCount: number;
  activeWorkerCount: number;
};

export type UsageTimelinePoint = {
  timestamp: string;
  elapsedMs: number;
  cumulativeUsage: TokenUsage;
};

export type RuntimeActivityKind =
  | "invocation-started"
  | "worker-started"
  | "worker-completed"
  | "retry-scheduled"
  | "invocation-failed"
  | "usage-finalized"
  | "worker-waiting"
  | "job-transition";

export type RuntimeActivityItem = {
  id: string;
  timestamp: string;
  elapsedMs: number;
  kind: RuntimeActivityKind;
  workerId?: string;
  workerLabel?: string;
  role?: "lead" | "worker";
  summary: string;
  provider?: string;
  model?: string;
  status?: WorkerStatus;
};

/** Normalized observability state. It intentionally contains no transport or backend DTOs. */
export type RuntimeObservability = {
  job: JobRuntimeStats;
  workers: WorkerRuntimeStats[];
  timeline: UsageTimelinePoint[];
  activities: RuntimeActivityItem[];
};

export function usage(value: number, provenance: UsageProvenance = "reported"): UsageValue {
  return { value, provenance };
}

export function boundTimeline(points: UsageTimelinePoint[], limit = 60): UsageTimelinePoint[] {
  return points.slice(Math.max(0, points.length - limit));
}

export function boundActivities(items: RuntimeActivityItem[], limit = 80): RuntimeActivityItem[] {
  return items.slice(Math.max(0, items.length - limit));
}
