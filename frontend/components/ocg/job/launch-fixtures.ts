/**
 * Deterministic fixtures for a Job launched through the mock runtime.
 *
 * Everything here is derived from the launch command, so repeated launches of
 * the same command produce byte-identical observability state. No timers,
 * randomness, or backend calls are involved.
 */

import { usage, type RuntimeActivityItem, type RuntimeObservability, type WorkerRuntimeStats } from "../runtime/observability";
import { microsToUsd, type JobLaunchCommand } from "./draft-domain";

/** First line of the objective, bounded for a Job header. */
export function jobTitleFromObjective(objective: string): string {
  const firstLine = objective.split(/\r?\n/)[0]?.trim() ?? "";
  if (firstLine.length === 0) return "Untitled Job";
  return firstLine.length > 64 ? `${firstLine.slice(0, 63)}…` : firstLine;
}

export function createJobLaunchObservability(command: JobLaunchCommand, jobId: string): RuntimeObservability {
  const lead: WorkerRuntimeStats = {
    workerId: "lead",
    role: "lead",
    label: "Lead",
    provider: "Command Code",
    model: "DeepSeek V4.1 Flash",
    variant: "mid",
    status: "active",
    startedAt: "00:00",
    elapsedMs: 0,
    invocationCount: 1,
    retryCount: 0,
    successCount: 0,
    tokenUsage: {},
    costMicros: usage(0),
  };

  const activity: RuntimeActivityItem = {
    id: `${jobId}-obs-1`,
    timestamp: "00:00",
    elapsedMs: 0,
    kind: "worker-started",
    workerId: lead.workerId,
    workerLabel: lead.label,
    role: lead.role,
    summary: "Lead started the launched Job.",
    provider: lead.provider,
    model: lead.model,
    status: lead.status,
  };

  return {
    job: {
      jobId,
      tokenUsage: {},
      costMicros: usage(0),
      estimatedFinalSpend: usage(microsToUsd(command.hardBudgetMicros), "estimated"),
      elapsedMs: 0,
      invocationCount: 1,
      retryCount: 0,
      activeWorkerCount: 1,
    },
    workers: [lead],
    activities: [activity],
    timeline: [
      { timestamp: "00:00", elapsedMs: 0, cumulativeUsage: { total: usage(0) } },
    ],
  };
}
