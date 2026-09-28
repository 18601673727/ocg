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

export type ProviderStats = {
  provider: string;
  invocationCount: number;
  retryCount: number;
  activeWorkers: number;
  workerCount: number;
  tokenUsage: TokenUsage;
  costMicros?: UsageValue;
  successCount?: number;
  failureCount?: number;
  latencyMs?: number;
  ttftMs?: number;
};

export type ModelStats = {
  provider: string;
  model: string;
  invocationCount: number;
  retryCount: number;
  activeWorkers: number;
  workerCount: number;
  tokenUsage: TokenUsage;
  costMicros?: UsageValue;
  successCount?: number;
  failureCount?: number;
  latencyMs?: number;
  ttftMs?: number;
};

export type MissionRuntimeStats = {
  missionId: string;
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
  | "mission-transition";

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

export type BudgetUsageProjection = {
  spent: number;
  limit: number;
  remaining: number;
  percent: number;
  burnRatePerMinute?: number;
  estimatedFinalSpend?: UsageValue;
};

/** Normalized observability state. It intentionally contains no transport or backend DTOs. */
export type RuntimeObservability = {
  mission: MissionRuntimeStats;
  workers: WorkerRuntimeStats[];
  timeline: UsageTimelinePoint[];
  activities: RuntimeActivityItem[];
};

const USAGE_KEYS: (keyof TokenUsage)[] = [
  "input",
  "output",
  "reasoning",
  "cacheRead",
  "cacheWrite",
  "total",
];

export function usage(value: number, provenance: UsageProvenance = "reported"): UsageValue {
  return { value, provenance };
}

/** Missing values stay missing. An aggregate is estimated if any contributing value is estimated. */
export function sumUsageValues(values: (UsageValue | undefined)[]): UsageValue | undefined {
  const present = values.filter((value): value is UsageValue => value !== undefined);
  if (present.length === 0) return undefined;
  return {
    value: present.reduce((total, item) => total + item.value, 0),
    provenance: present.some((item) => item.provenance === "estimated") ? "estimated" : "reported",
  };
}

export function sumTokenUsage(usages: TokenUsage[]): TokenUsage {
  const result: TokenUsage = {};
  for (const key of USAGE_KEYS) {
    const value = sumUsageValues(usages.map((item) => item[key]));
    if (value) result[key] = value;
  }
  return result;
}

function average(values: (number | undefined)[]): number | undefined {
  const present = values.filter((value): value is number => value !== undefined);
  return present.length > 0 ? present.reduce((total, value) => total + value, 0) / present.length : undefined;
}

function sumOptional(values: (number | undefined)[]): number | undefined {
  const present = values.filter((value): value is number => value !== undefined);
  return present.length > 0 ? present.reduce((total, value) => total + value, 0) : undefined;
}

export function aggregateProviderStats(workers: WorkerRuntimeStats[]): ProviderStats[] {
  const groups = new Map<string, WorkerRuntimeStats[]>();
  for (const worker of workers) groups.set(worker.provider, [...(groups.get(worker.provider) ?? []), worker]);
  return [...groups.entries()].map(([provider, members]) => ({
    provider,
    invocationCount: members.reduce((total, worker) => total + worker.invocationCount, 0),
    retryCount: members.reduce((total, worker) => total + worker.retryCount, 0),
    activeWorkers: members.filter((worker) => worker.status === "active").length,
    workerCount: members.length,
    tokenUsage: sumTokenUsage(members.map((worker) => worker.tokenUsage)),
    costMicros: sumUsageValues(members.map((worker) => worker.costMicros)),
    successCount: sumOptional(members.map((worker) => worker.successCount)),
    failureCount: sumOptional(members.map((worker) => worker.failureCount)),
    latencyMs: average(members.map((worker) => worker.latencyMs)),
    ttftMs: average(members.map((worker) => worker.ttftMs)),
  }));
}

export function aggregateModelStats(workers: WorkerRuntimeStats[]): ModelStats[] {
  const groups = new Map<string, WorkerRuntimeStats[]>();
  for (const worker of workers) {
    const key = `${worker.provider}\u0000${worker.model}`;
    groups.set(key, [...(groups.get(key) ?? []), worker]);
  }
  return [...groups.values()].map((members) => ({
    provider: members[0].provider,
    model: members[0].model,
    invocationCount: members.reduce((total, worker) => total + worker.invocationCount, 0),
    retryCount: members.reduce((total, worker) => total + worker.retryCount, 0),
    activeWorkers: members.filter((worker) => worker.status === "active").length,
    workerCount: members.length,
    tokenUsage: sumTokenUsage(members.map((worker) => worker.tokenUsage)),
    costMicros: sumUsageValues(members.map((worker) => worker.costMicros)),
    successCount: sumOptional(members.map((worker) => worker.successCount)),
    failureCount: sumOptional(members.map((worker) => worker.failureCount)),
    latencyMs: average(members.map((worker) => worker.latencyMs)),
    ttftMs: average(members.map((worker) => worker.ttftMs)),
  }));
}

export function boundTimeline(points: UsageTimelinePoint[], limit = 60): UsageTimelinePoint[] {
  return points.slice(Math.max(0, points.length - limit));
}

export function boundActivities(items: RuntimeActivityItem[], limit = 80): RuntimeActivityItem[] {
  return items.slice(Math.max(0, items.length - limit));
}

export type ChartableTimelinePoint = {
  timestamp: string;
  elapsedMs: number;
  total: number | null;
  estimatedTotal: number | null;
  reportedTotal: number | null;
  provenance?: UsageProvenance;
};

/** Converts normalized points into explicit numeric/null chart values without interpolating missing usage. */
export function toChartableTimeline(points: UsageTimelinePoint[]): ChartableTimelinePoint[] {
  return boundTimeline(points).map((point) => {
    const total = point.cumulativeUsage.total;
    return {
      timestamp: point.timestamp,
      elapsedMs: point.elapsedMs,
      total: total?.value ?? null,
      estimatedTotal: total?.provenance === "estimated" ? total.value : null,
      reportedTotal: total?.provenance === "reported" ? total.value : null,
      provenance: total?.provenance,
    };
  });
}

export function deriveBudgetUsage(
  budget: { spent: number; limit: number },
  elapsedMs?: number,
  estimatedFinalSpend?: UsageValue,
): BudgetUsageProjection {
  const remaining = Math.max(0, budget.limit - budget.spent);
  return {
    spent: budget.spent,
    limit: budget.limit,
    remaining,
    percent: budget.limit > 0 ? Math.min(100, (budget.spent / budget.limit) * 100) : 0,
    burnRatePerMinute: elapsedMs && elapsedMs > 0 ? budget.spent / (elapsedMs / 60_000) : undefined,
    estimatedFinalSpend,
  };
}
