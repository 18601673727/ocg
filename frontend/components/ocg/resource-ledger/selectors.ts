/**
 * Pure selectors and math for the Resource Ledger Inspector.
 *
 * Every aggregate the UI shows flows through these functions so numbers cannot
 * diverge between tabs. The rules are intentionally strict:
 * - missing usage or cost is `null`, never `0`;
 * - ratios return `null` (unavailable) when a denominator is missing or
 *   invalid, so no `NaN`/`Infinity` can reach the UI;
 * - cost is summed as integer micro-units.
 */

import {
  ALL_FILTER_VALUE,
  ATTRIBUTION_CONFIDENCES,
  COST_PROVENANCES,
  DEFAULT_LEDGER_FILTER,
  RECONCILIATION_STATUSES,
  TIME_WINDOW_MS,
  USAGE_AUTHORITIES,
  USAGE_COMPONENTS,
  type AttributionConfidence,
  type CostProvenance,
  type FilterOption,
  type LedgerDimension,
  type LedgerFilter,
  type LedgerFilterOptions,
  type LedgerSeriesKey,
  type LedgerUsage,
  type ReconciliationStatus,
  type ResourceLedgerEntry,
  type UsageAuthority,
  type UsageComponent,
} from "./types";

export type ComponentTraffic = Record<UsageComponent, number | null> & { total: number | null };

/**
 * Sums finite token counts. Returns `null` when nothing was observed, which is
 * distinct from an observed zero.
 */
export function sumTokens(values: readonly (number | null | undefined)[]): number | null {
  let total = 0;
  let seen = false;
  for (const value of values) {
    if (typeof value === "number" && Number.isFinite(value)) {
      total += value;
      seen = true;
    }
  }
  return seen ? total : null;
}

/** Total traffic per usage component, plus the derived total. */
export function componentTraffic(entries: readonly ResourceLedgerEntry[]): ComponentTraffic {
  const traffic = {} as Record<UsageComponent, number | null>;
  for (const component of USAGE_COMPONENTS) {
    traffic[component] = sumTokens(entries.map((entry) => entry.usage?.[component]));
  }
  const total = sumTokens(USAGE_COMPONENTS.map((component) => traffic[component]));
  return { ...traffic, total };
}

/**
 * Cache read as a share of observed component traffic. The denominator is the
 * derived traffic total, so output/reasoning/cache-write traffic remains part
 * of the metric. Returns `null` when cache read or the denominator is unknown
 * or not positive.
 */
export function cacheShare(traffic: ComponentTraffic): number | null {
  const cacheRead = traffic.cacheRead;
  const denominator = traffic.total;
  if (cacheRead === null || denominator === null) return null;
  if (!Number.isFinite(denominator) || denominator <= 0) return null;
  return cacheRead / denominator;
}

/**
 * Cached tokens per fresh input token. `1.0` means the cache served as many
 * tokens as fresh input; higher is more reuse. Returns `null` for a missing or
 * non-positive fresh-input denominator.
 */
export function cacheLeverage(traffic: ComponentTraffic): number | null {
  const cacheRead = traffic.cacheRead;
  const freshInput = traffic.freshInput;
  if (cacheRead === null || freshInput === null) return null;
  if (!Number.isFinite(freshInput) || freshInput <= 0) return null;
  return cacheRead / freshInput;
}

/** Coerces a cost to an integer number of micro-units. `null` for unavailable or invalid input. */
export function toMicros(value: number | null | undefined): number | null {
  if (typeof value !== "number" || !Number.isFinite(value)) return null;
  return Math.round(value);
}

/**
 * A total is only as authoritative as its weakest known contributor:
 * estimated beats stored beats reported. An empty set is unavailable.
 */
export function deriveCostProvenance(provenances: readonly CostProvenance[]): CostProvenance {
  const known = provenances.filter((provenance) => provenance !== "unavailable");
  if (known.length === 0) return "unavailable";
  if (known.includes("estimated")) return "estimated";
  if (known.includes("stored")) return "stored";
  return "reported";
}

export type CostTotal = { micros: number | null; provenance: CostProvenance };

/**
 * Sums known costs as integer micro-units. An explicit zero is kept and makes
 * the result `0`; when no cost is known the result is `null` and unavailable.
 */
export function sumCostMicros(entries: readonly ResourceLedgerEntry[]): CostTotal {
  const contributors = entries.filter(
    (entry) => entry.costProvenance !== "unavailable" && toMicros(entry.costMicros) !== null,
  );
  if (contributors.length === 0) return { micros: null, provenance: "unavailable" };
  return {
    micros: contributors.reduce((total, entry) => total + (toMicros(entry.costMicros) ?? 0), 0),
    provenance: deriveCostProvenance(contributors.map((entry) => entry.costProvenance)),
  };
}

function countBy<T extends string>(items: readonly T[], keys: readonly T[]): Record<T, number> {
  const counts = {} as Record<T, number>;
  for (const key of keys) counts[key] = 0;
  for (const item of items) counts[item] += 1;
  return counts;
}

export type LedgerSummary = {
  entryCount: number;
  successCount: number;
  failureCount: number;
  retryingCount: number;
  /** Calls that were a retry attempt (attempt > 1). */
  retryCount: number;
  missionCount: number;
  taskCount: number;
  workerCount: number;
  leadEntryCount: number;
  traffic: ComponentTraffic;
  totalTokens: number | null;
  costMicros: number | null;
  costProvenance: CostProvenance;
  averageLatencyMs: number | null;
  cacheShare: number | null;
  cacheLeverage: number | null;
  byAuthority: Record<UsageAuthority, number>;
  byCostProvenance: Record<CostProvenance, number>;
  byAttribution: Record<AttributionConfidence, number>;
  byReconciliation: Record<ReconciliationStatus, number>;
};

/** Single source of truth for every headline metric shown in the inspector. */
export function summarize(entries: readonly ResourceLedgerEntry[]): LedgerSummary {
  const traffic = componentTraffic(entries);
  const cost = sumCostMicros(entries);
  const latencies = entries
    .map((entry) => entry.latencyMs)
    .filter((value): value is number => typeof value === "number" && Number.isFinite(value));
  return {
    entryCount: entries.length,
    successCount: entries.filter((entry) => entry.status === "success").length,
    failureCount: entries.filter((entry) => entry.status === "failure").length,
    retryingCount: entries.filter((entry) => entry.status === "retrying").length,
    retryCount: entries.filter((entry) => entry.attempt > 1).length,
    missionCount: new Set(entries.map((entry) => entry.missionId)).size,
    taskCount: new Set(entries.map((entry) => `${entry.missionId}\u0000${entry.taskId}`)).size,
    workerCount: new Set(entries.map((entry) => entry.workerId)).size,
    leadEntryCount: entries.filter((entry) => entry.role === "lead").length,
    traffic,
    totalTokens: traffic.total,
    costMicros: cost.micros,
    costProvenance: cost.provenance,
    averageLatencyMs: latencies.length > 0
      ? latencies.reduce((total, value) => total + value, 0) / latencies.length
      : null,
    cacheShare: cacheShare(traffic),
    cacheLeverage: cacheLeverage(traffic),
    byAuthority: countBy(entries.map((entry) => entry.usageAuthority), USAGE_AUTHORITIES),
    byCostProvenance: countBy(entries.map((entry) => entry.costProvenance), COST_PROVENANCES),
    byAttribution: countBy(entries.map((entry) => entry.attributionConfidence), ATTRIBUTION_CONFIDENCES),
    byReconciliation: countBy(entries.map((entry) => entry.reconciliation), RECONCILIATION_STATUSES),
  };
}

export type LedgerGroup = {
  key: string;
  label: string;
  detail: string | null;
  entries: ResourceLedgerEntry[];
  summary: LedgerSummary;
};

export const UNKNOWN_MISSION_KEY = "__unknown_mission__";
export const UNKNOWN_TASK_KEY = "__unknown_task__";

function dimensionOf(
  entry: ResourceLedgerEntry,
  dimension: LedgerDimension,
): { key: string; label: string; detail: string | null } {
  switch (dimension) {
    case "mission":
      return entry.attributedMissionId === null
        ? { key: UNKNOWN_MISSION_KEY, label: "Unknown Mission", detail: null }
        : { key: entry.attributedMissionId, label: entry.missionLabel, detail: entry.missionId };
    case "task":
      return entry.attributedTaskId === null
        ? { key: `${entry.attributedMissionId ?? UNKNOWN_MISSION_KEY}\u0000${UNKNOWN_TASK_KEY}`, label: "Unknown task", detail: entry.attributedMissionId ? entry.missionLabel : null }
        : {
            key: `${entry.attributedMissionId ?? UNKNOWN_MISSION_KEY}\u0000${entry.attributedTaskId}`,
            label: entry.taskLabel,
            detail: entry.missionLabel,
          };
    case "worker":
      return { key: entry.workerId, label: entry.workerLabel, detail: entry.role };
    case "provider":
      return { key: entry.provider, label: entry.provider, detail: null };
    case "model":
      return { key: modelKey(entry.provider, entry.model), label: entry.model, detail: entry.provider };
    case "modelVariant":
      return {
        key: `${modelKey(entry.provider, entry.model)}\u0000${entry.variant ?? ""}`,
        label: entry.variant ? `${entry.model} · ${entry.variant}` : entry.model,
        detail: entry.provider,
      };
    case "attribution":
      return { key: entry.attributionConfidence, label: entry.attributionConfidence, detail: null };
    case "reconciliation":
      return { key: entry.reconciliation, label: entry.reconciliation, detail: null };
  }
}

/** Groups entries by one dimension, preserving first-seen order. */
export function groupEntries(
  entries: readonly ResourceLedgerEntry[],
  dimension: LedgerDimension,
): LedgerGroup[] {
  const groups = new Map<string, { label: string; detail: string | null; entries: ResourceLedgerEntry[] }>();
  for (const entry of entries) {
    const { key, label, detail } = dimensionOf(entry, dimension);
    const group = groups.get(key);
    if (group) {
      group.entries.push(entry);
    } else {
      groups.set(key, { label, detail, entries: [entry] });
    }
  }
  return [...groups.entries()].map(([key, group]) => ({
    key,
    label: group.label,
    detail: group.detail,
    entries: group.entries,
    summary: summarize(group.entries),
  }));
}

export function modelKey(provider: string, model: string): string {
  return `${provider}\u0000${model}`;
}

/** Newest valid timestamp in the set, used as the default "now" for time windows. */
export function latestTimestampMs(entries: readonly ResourceLedgerEntry[]): number | null {
  let latest: number | null = null;
  for (const entry of entries) {
    const ms = Date.parse(entry.timestamp);
    if (Number.isFinite(ms) && (latest === null || ms > latest)) latest = ms;
  }
  return latest;
}

/**
 * Applies the inspector filter. `nowMs` defaults to the newest timestamp in the
 * provided set so window math stays deterministic in tests and fixtures.
 */
export function filterEntries(
  entries: readonly ResourceLedgerEntry[],
  filter: LedgerFilter,
  options: { nowMs?: number | null } = {},
): ResourceLedgerEntry[] {
  const nowMs = options.nowMs ?? latestTimestampMs(entries);
  const windowMs = filter.window === "all" ? null : TIME_WINDOW_MS[filter.window];

  return entries.filter((entry) => {
    if (filter.missionId !== ALL_FILTER_VALUE && entry.missionId !== filter.missionId) return false;
    if (filter.workerId !== ALL_FILTER_VALUE && entry.workerId !== filter.workerId) return false;
    if (filter.provider !== ALL_FILTER_VALUE && entry.provider !== filter.provider) return false;
    if (filter.modelKey !== ALL_FILTER_VALUE && modelKey(entry.provider, entry.model) !== filter.modelKey) {
      return false;
    }
    if (windowMs !== null) {
      if (nowMs === null || nowMs === undefined) return false;
      const ms = Date.parse(entry.timestamp);
      if (!Number.isFinite(ms)) return false;
      if (ms < nowMs - windowMs || ms > nowMs) return false;
    }
    return true;
  });
}

export function isFilterActive(filter: LedgerFilter): boolean {
  return (
    filter.window !== DEFAULT_LEDGER_FILTER.window ||
    filter.missionId !== DEFAULT_LEDGER_FILTER.missionId ||
    filter.workerId !== DEFAULT_LEDGER_FILTER.workerId ||
    filter.provider !== DEFAULT_LEDGER_FILTER.provider ||
    filter.modelKey !== DEFAULT_LEDGER_FILTER.modelKey
  );
}

function dedupe(
  entries: readonly ResourceLedgerEntry[],
  toOption: (entry: ResourceLedgerEntry) => FilterOption,
): FilterOption[] {
  const seen = new Set<string>();
  const options: FilterOption[] = [];
  for (const entry of entries) {
    const option = toOption(entry);
    if (seen.has(option.value)) continue;
    seen.add(option.value);
    options.push(option);
  }
  return options;
}

/**
 * Filter choices come from the full dataset, not the filtered one, so selecting
 * a value never removes it from its own dropdown.
 */
export function filterOptions(entries: readonly ResourceLedgerEntry[]): LedgerFilterOptions {
  return {
    missions: dedupe(entries, (entry) => ({ value: entry.missionId, label: entry.missionLabel })),
    workers: dedupe(entries, (entry) => ({
      value: entry.workerId,
      label: entry.workerLabel,
      detail: entry.role,
    })),
    providers: dedupe(entries, (entry) => ({ value: entry.provider, label: entry.provider })),
    models: dedupe(entries, (entry) => ({
      value: modelKey(entry.provider, entry.model),
      label: entry.model,
      detail: entry.provider,
    })),
  };
}

export type LedgerTimePoint = {
  timestampMs: number;
  timestamp: string;
  freshInput: number | null;
  cacheRead: number | null;
  cacheWrite: number | null;
  output: number | null;
  reasoning: number | null;
  total: number | null;
};

/**
 * Buckets timestamped entries into a bounded, chartable series. Missing
 * components stay `null` inside a bucket; no interpolation occurs.
 */
export function toLedgerTimeSeries(
  entries: readonly ResourceLedgerEntry[],
  options: { bucketMs?: number; limit?: number } = {},
): LedgerTimePoint[] {
  const bucketMs = options.bucketMs ?? 15 * 60_000;
  const limit = options.limit ?? 24;

  const stamped = entries
    .map((entry) => ({ entry, ms: Date.parse(entry.timestamp) }))
    .filter((item): item is { entry: ResourceLedgerEntry; ms: number } => Number.isFinite(item.ms));
  if (stamped.length === 0) return [];

  const minMs = Math.min(...stamped.map((item) => item.ms));
  const buckets = new Map<number, ResourceLedgerEntry[]>();
  for (const { entry, ms } of stamped) {
    const index = Math.floor((ms - minMs) / bucketMs);
    const bucket = buckets.get(index);
    if (bucket) bucket.push(entry);
    else buckets.set(index, [entry]);
  }

  const points = [...buckets.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([index, bucketEntries]) => {
      const timestampMs = minMs + index * bucketMs;
      return {
        timestampMs,
        timestamp: new Date(timestampMs).toISOString(),
        ...componentTraffic(bucketEntries),
      };
    });

  return points.slice(Math.max(0, points.length - limit));
}

/** Plain-language summary of a chart for screen readers and dense headers. */
export function describeTimeSeries(points: readonly LedgerTimePoint[]): string {
  if (points.length === 0) return "No timestamped usage to plot.";
  const withTotal = points.filter((point) => point.total !== null);
  if (withTotal.length === 0) return `${points.length} buckets · token totals unavailable.`;
  const latest = withTotal[withTotal.length - 1];
  const peak = withTotal.reduce(
    (max, point) => ((point.total ?? 0) > (max.total ?? 0) ? point : max),
    withTotal[0],
  );
  const clock = (point: LedgerTimePoint) => point.timestamp.slice(11, 16);
  return `${points.length} buckets · latest ${latest.total} tokens at ${clock(latest)}Z · peak ${peak.total} tokens at ${clock(peak)}Z`;
}

/** Reads a component series value from a chart point without leaking `undefined`. */
export function seriesValue(point: LedgerTimePoint, key: LedgerSeriesKey): number | null {
  return point[key];
}

/** Aggregate a usage map into a single observed total (or `null`). */
export function usageTotal(usage: LedgerUsage | null): number | null {
  if (!usage) return null;
  return sumTokens(USAGE_COMPONENTS.map((component) => usage[component]));
}
