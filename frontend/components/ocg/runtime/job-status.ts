import type { ControlJob, ControlTaskPlan } from "../contracts";

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
