/**
 * Deterministic fixtures for a Mission launched through the mock runtime.
 *
 * Everything here is derived from the launch command, so repeated launches of
 * the same command produce byte-identical Mission, execution, and observability
 * state. No timers, randomness, or backend calls are involved.
 */

import { usage, type RuntimeActivityItem, type RuntimeObservability, type WorkerRuntimeStats } from "../runtime/observability";
import type { Mission, MissionTask, Worker } from "../types";
import { microsToUsd, type MissionLaunchCommand } from "./draft-domain";

const FALLBACK_TASK_TITLES = [
  "Plan the objective",
  "Execute the plan",
  "Verify the success criteria",
] as const;

const TASK_WORKERS = [
  { id: "lead", role: "lead" as const, label: "Lead", provider: "Command Code", model: "DeepSeek V4.1 Flash", variant: "mid" },
  { id: "build", role: "worker" as const, label: "Build", provider: "Command Code", model: "DeepSeek V4.1 Flash", variant: "flash" },
  { id: "verify", role: "worker" as const, label: "Verify", provider: "OpenCode Zen", model: "Muse Spark 1.3 Contributor Free", variant: "standard" },
];

/** Stable Mission identity derived from the draft, never from a clock. */
export function missionIdForLaunch(command: MissionLaunchCommand): string {
  return `mission-${command.draftId}`;
}

/** First line of the objective, bounded for the Mission header. */
export function missionTitleFromObjective(objective: string): string {
  const firstLine = objective.split(/\r?\n/)[0]?.trim() ?? "";
  if (firstLine.length === 0) return "Untitled Mission";
  return firstLine.length > 64 ? `${firstLine.slice(0, 63)}…` : firstLine;
}

/**
 * Derives up to three deterministic task titles from the success criteria.
 * Missing criteria fall back to fixed titles so a launch always has 3 tasks.
 */
export function missionFocusLines(command: MissionLaunchCommand): string[] {
  const fromCriteria = command.successCriteria
    .split(/\r?\n|;/)
    .map((line) => line.replace(/^[-*\d.)\s]+/, "").trim())
    .filter((line) => line.length > 0)
    .slice(0, 3);
  return FALLBACK_TASK_TITLES.map((fallback, index) => fromCriteria[index] ?? fallback);
}

export function createLaunchedMission(command: MissionLaunchCommand, missionId: string): Mission {
  const focus = missionFocusLines(command);
  const limit = microsToUsd(command.hardBudgetMicros);
  const workerCount = Math.max(1, Math.min(4, Math.round(command.resourceCommitment * 4) || 1));

  const tasks: MissionTask[] = focus.map((title, index) => ({
    id: `${missionId}-task-${index + 1}`,
    title,
    status: index === 0 ? "active" : "pending",
  }));

  const workers: Worker[] = TASK_WORKERS.map((worker, index) => ({
    id: `${missionId}-${worker.id}`,
    name: worker.label,
    status: index === 0 ? "active" : index === 1 ? "queued" : "waiting",
    task: focus[index],
  }));

  return {
    title: missionTitleFromObjective(command.objective),
    goal: command.objective,
    status: "running",
    completed: 0,
    total: tasks.length,
    current: focus[0],
    tasks,
    workers,
    elapsed: "0m",
    commitment: { workers: workerCount, mode: "capped" },
    budget: { spent: 0, limit, currency: "USD", status: "within-limit" },
    warnings: [],
  };
}

export function createLaunchedObservability(command: MissionLaunchCommand, missionId: string): RuntimeObservability {
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
    id: `${missionId}-obs-1`,
    timestamp: "00:00",
    elapsedMs: 0,
    kind: "worker-started",
    workerId: lead.workerId,
    workerLabel: lead.label,
    role: lead.role,
    summary: "Lead started the launched Mission.",
    provider: lead.provider,
    model: lead.model,
    status: lead.status,
  };

  return {
    mission: {
      missionId,
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
