/**
 * Health Matrix projection.
 *
 * A row is one Provider × Model × Effort tuple a Health Probe Job declared.
 * The backend stores no health table: the latest probe Job for that tuple is
 * the evidence, and `executable` is true only when that Job completed without
 * a termination reason. Everything else is the Job's own state and failure.
 *
 * This module does not decide whether a configured model is healthy. A Profile
 * entry that has never been probed does not appear here, because the backend
 * has recorded no evidence for it.
 */

import type { CanonicalJobSummary, Failure } from "../contracts";
import type { HealthProbeIntent } from "../contracts/generated";

const TERMINAL_STATES = new Set(["completed", "failed", "cancelled", "orphaned"]);

/** How the latest probe Job reads, without reclassifying the backend's verdict. */
export type HealthStatus = "available" | "unavailable" | "unknown" | "in-progress";

export type HealthEntry = {
  provider: string;
  model: string;
  /** `null` is its own tuple: a probe that named no effort. */
  effort: string | null;
  status: HealthStatus;
  jobId: string;
  jobState: string;
  updatedAt: number;
  createdAt: number;
  failure: Failure | null;
};

export type HealthProviderGroup = {
  provider: string;
  entries: readonly HealthEntry[];
};

export type HealthSummary = {
  total: number;
  available: number;
  unavailable: number;
  unknown: number;
  inProgress: number;
};

export type HealthMatrix = {
  groups: readonly HealthProviderGroup[];
  summary: HealthSummary;
};

function statusOf(job: CanonicalJobSummary): HealthStatus {
  if (job.state === "completed" && job.termination_reason === null) return "available";
  if (job.state === "failed" || job.termination_reason !== null) return "unavailable";
  if (TERMINAL_STATES.has(job.state)) return "unavailable";
  if (job.state === "running" || job.state === "cancelling") return "in-progress";
  return "unknown";
}

function targetKey(target: HealthProbeIntent): string {
  return JSON.stringify([target.provider, target.model, target.effort]);
}

function newer(left: CanonicalJobSummary, right: CanonicalJobSummary): boolean {
  return left.created_at > right.created_at
    || (left.created_at === right.created_at && left.job_id > right.job_id);
}

/**
 * The latest probe Job for each declared tuple.
 *
 * Ordering matches the backend's own read: newest `created_at`, then job id.
 * An older probe is not mixed in, even when the newer one has not finished.
 */
export function healthMatrixOf(jobs: readonly CanonicalJobSummary[]): HealthMatrix {
  const latest = new Map<string, CanonicalJobSummary>();
  for (const job of jobs) {
    const target = job.health_probe;
    if (!target || target.provider.length === 0 || target.model.length === 0) continue;
    const key = targetKey(target);
    const current = latest.get(key);
    if (!current || newer(job, current)) latest.set(key, job);
  }

  const entries = [...latest.values()]
    .map((job): HealthEntry => {
      const target = job.health_probe!;
      return {
        provider: target.provider,
        model: target.model,
        effort: target.effort,
        status: statusOf(job),
        jobId: job.job_id,
        jobState: job.state,
        updatedAt: job.updated_at,
        createdAt: job.created_at,
        failure: job.termination_reason,
      };
    })
    .sort((left, right) =>
      left.provider.localeCompare(right.provider)
      || left.model.localeCompare(right.model)
      || (left.effort ?? "").localeCompare(right.effort ?? "")
      || left.jobId.localeCompare(right.jobId));

  const groups: HealthProviderGroup[] = [];
  for (const entry of entries) {
    const current = groups[groups.length - 1];
    if (current && current.provider === entry.provider) current.entries = [...current.entries, entry];
    else groups.push({ provider: entry.provider, entries: [entry] });
  }

  const summary = entries.reduce<HealthSummary>((count, entry) => {
    count.total += 1;
    if (entry.status === "available") count.available += 1;
    else if (entry.status === "unavailable") count.unavailable += 1;
    else if (entry.status === "in-progress") count.inProgress += 1;
    else count.unknown += 1;
    return count;
  }, { total: 0, available: 0, unavailable: 0, unknown: 0, inProgress: 0 });

  return { groups, summary };
}
