import type { ChildConversation, ExecutionPhase, Job, JobDetails, TaskPlan } from "../contracts";
import { runtimeStateLabel } from "../i18n";
import type { I18nKey, TranslateFn } from "../i18n/dictionaries";
import type { Tone } from "../primitives/tone";

export function childConversationStatus(t: TranslateFn, child: ChildConversation): string {
  if (child.state === "closing") return t("chat.child.closing");
  if (child.state === "open" && child.on_hold) return t(`chat.child.hold.${child.on_hold.by.kind}`);
  if (
    child.state === "open" &&
    child.keep_open &&
    !child.recurrence &&
    child.job_state === "completed"
  )
    return t("chat.child.open");
  return runtimeStateLabel(t, child.job_state);
}

export function reconcileJob(current: Job, incoming: Job): Job {
  const currentSeq = current.event_seq;
  const incomingSeq = incoming.event_seq;
  const older =
    incomingSeq.length < currentSeq.length ||
    (incomingSeq.length === currentSeq.length && incomingSeq < currentSeq);
  const final =
    current.settlement.state === "completed" ||
    current.settlement.state === "failed" ||
    current.settlement.state === "cancelled";
  if (older || (final && incoming.settlement.state !== current.settlement.state)) return current;
  return incoming;
}

export function taskPlanActive(plan: TaskPlan | null | undefined): boolean {
  return (
    !!plan &&
    plan.state !== "completed" &&
    plan.state !== "failed" &&
    plan.state !== "cancelled" &&
    plan.state !== "unknown"
  );
}

export type NativeExecutionStatus = {
  phase: ExecutionPhase;
  holdsCapacity: boolean;
  owner?: { executor: string; pool: string };
  actionsSupported: boolean;
  retrySupported: boolean;
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
} as const satisfies Record<ExecutionPhase, I18nKey>;

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
} as const satisfies Record<ExecutionPhase, Tone>;

export function nativeExecutionStatus(
  phase: ExecutionPhase,
  details: Pick<JobDetails, "allocation" | "mutable_phase"> | undefined,
  retrySupported = false,
): NativeExecutionStatus {
  const allocation = details?.allocation;
  return {
    phase,
    holdsCapacity: allocation?.holds_capacity ?? false,
    actionsSupported: details?.mutable_phase === undefined,
    retrySupported,
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
