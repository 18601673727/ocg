import type * as Api from "./api";
import { ContractError } from "./decode";

export type Job = Api.Job;
export type ExecutionPhase = Api.ExecutionPhase;
export type ExecutionHistory = Api.ExecutionHistory;
export type ExecutionHistoryEvent = Api.ExecutionHistoryEvent;
export type ExecutionState = Api.ExecutionState;
export type Output = Api.Output;
export type JobDetails = Api.Job &
  Pick<Api.JobDetails, "result" | "unknown_reason" | "activity" | "task_plan" | "allocation"> & {
    mutable_phase?: string;
    context?: string;
  };
export type JobActivity = JobDetails["activity"];
export type TaskPlan = NonNullable<JobDetails["task_plan"]>;
export type JobToolsPage = Api.Tools;
export type ProviderResponse = Omit<Api.ProviderResponse, "text_offset" | "text_complete">;
export type Attachment = Omit<Api.Attachment, "created_at_us"> & {
  created_at_us: number | null;
  data_url: string | undefined;
};
export type JobEvent =
  | ({ type: "job" } & Api.JobEvent)
  | ({ type: "output" } & Api.OutputEvent)
  | ({ type: "settlement" } & Api.SettlementEvent)
  | ({ type: "stream_error" } & Api.ErrorResponse);

export function projectJobEvent(name: string, input: unknown): JobEvent {
  switch (name) {
    case "job": {
      const event = input as Api.JobEvent;
      executionSequence(event.job?.event_seq);
      return { ...event, type: "job" };
    }
    case "output":
      return { ...(input as Api.OutputEvent), type: "output" };
    case "settlement": {
      const event = input as Api.SettlementEvent;
      executionSequence(event.event_seq);
      return { ...event, type: "settlement" };
    }
    case "stream_error":
      return { ...(input as Api.ErrorResponse), type: "stream_error" };
    default:
      throw new ContractError("event", "job, output, settlement or stream_error");
  }
}

export function projectJob(input: Api.JobDetails) {
  executionSequence(input.job?.event_seq);
  return { ...input.job, unknown_reason: input.unknown_reason ?? null };
}

export function executionSequence(input: unknown): string {
  if (
    typeof input !== "string" ||
    !/^(0|[1-9][0-9]*)$/.test(input) ||
    input.length > 20 ||
    (input.length === 20 && input > "18446744073709551615")
  )
    throw new ContractError("event_seq", "an unsigned 64-bit decimal string without leading zeros");
  return input;
}

export function projectJobDetails(input: Api.JobDetails): JobDetails {
  return {
    ...projectJob(input),
    result: input.result,
    activity: input.activity,
    task_plan: input.task_plan,
    allocation: input.allocation,
    mutable_phase: (input as Api.JobDetails & { phase?: string }).phase,
    context: (input as Api.JobDetails & { context?: string }).context,
  };
}

export type NativeStopRequest = "ended_before_start" | "requested" | "already_requested";

export type NativeStop = {
  job: string;
  request: NativeStopRequest;
  stopped: boolean;
  execution_phase: "cancelled" | "stop_requested";
};

const JOB_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

export function projectNativeStop(status: number, input: unknown): NativeStop {
  const body = input as {
    api_version?: number;
    stop?: {
      job?: unknown;
      request?: unknown;
      stopped?: unknown;
      execution_phase?: unknown;
    };
  } | null;
  const stop = body?.stop;
  if (body?.api_version !== 1 || !stop || typeof stop.job !== "string" || !JOB_ID.test(stop.job))
    throw new ContractError("stop", "a valid native stop answer");
  if (stop.request === "ended_before_start") {
    if (status !== 200 || stop.stopped !== true || stop.execution_phase !== "cancelled")
      throw new ContractError("stop", "a confirmed stop");
  } else if (stop.request === "requested" || stop.request === "already_requested") {
    if (status !== 202 || stop.stopped !== false || stop.execution_phase !== "stop_requested")
      throw new ContractError("stop", "a requested stop");
  } else throw new ContractError("stop.request", "a native stop request outcome");
  return {
    job: stop.job,
    request: stop.request,
    stopped: stop.stopped,
    execution_phase: stop.execution_phase,
  };
}
