import type {
  ControlExecutionPhase,
  ControlJob,
  ControlJobDetails,
  ControlTaskPlan,
} from "../contracts";
import type { I18nKey, TranslateFn } from "../i18n";
import type { Tone } from "../primitives/tone";

export function reconcileJob(current: ControlJob, incoming: ControlJob): ControlJob {
  if (
    incoming.updated_at_us < current.updated_at_us ||
    (current.settlement.state !== "unsettled" && incoming.settlement.state === "unsettled")
  )
    return current;
  return incoming;
}

export function taskPlanActive(plan: ControlTaskPlan | null | undefined): boolean {
  return (
    !!plan &&
    plan.state !== "completed" &&
    plan.state !== "failed" &&
    plan.state !== "cancelled" &&
    plan.state !== "unknown"
  );
}

export type NativeExecutionStatus = {
  phase: ControlExecutionPhase;
  holdsCapacity: boolean;
  owner?: { executor: string; pool: string };
  actionsSupported: boolean;
};

export const EXECUTION_PHASE_KEYS = {
  unsubmitted: "job.phase.unsubmitted",
  queued: "job.phase.queued",
  reserved: "job.phase.reserved",
  running: "job.phase.running",
  stop_requested: "job.phase.stopRequested",
  unknown: "job.phase.unknown",
  completed: "job.phase.completed",
  failed: "job.phase.failed",
  cancelled: "job.phase.cancelled",
} as const satisfies Record<ControlExecutionPhase, I18nKey>;

export const EXECUTION_PHASE_TONES = {
  unsubmitted: "slate",
  queued: "violet",
  reserved: "sky",
  running: "sky",
  stop_requested: "amber",
  unknown: "red",
  completed: "emerald",
  failed: "red",
  cancelled: "slate",
} as const satisfies Record<ControlExecutionPhase, Tone>;

export function nativeExecutionStatus(
  phase: ControlExecutionPhase,
  details: Pick<ControlJobDetails, "allocation" | "mutable_phase"> | undefined,
): NativeExecutionStatus {
  const allocation = details?.allocation;
  return {
    phase,
    holdsCapacity: allocation?.holds_capacity ?? false,
    actionsSupported: details?.mutable_phase === undefined,
    ...(allocation ? { owner: { executor: allocation.executor, pool: allocation.pool } } : {}),
  };
}

export function executionLabel(t: TranslateFn, status: NativeExecutionStatus): string {
  return t(EXECUTION_PHASE_KEYS[status.phase]);
}

export function executionDetail(t: TranslateFn, status: NativeExecutionStatus): string | undefined {
  const owner = status.owner ?? { executor: "—", pool: "—" };
  const vars = { executor: owner.executor, pool: owner.pool };
  switch (status.phase) {
    case "stop_requested":
      return t("job.phase.stopRequestedDetail", vars);
    case "unknown":
      return status.holdsCapacity ? t("job.phase.heldDetail", vars) : t("job.phase.unknownDetail");
    case "reserved":
    case "running":
      return status.holdsCapacity ? t("job.phase.holdsDetail", vars) : undefined;
    case "unsubmitted":
    case "queued":
    case "completed":
    case "failed":
    case "cancelled":
      return status.holdsCapacity ? t("job.phase.heldDetail", vars) : undefined;
  }
}
