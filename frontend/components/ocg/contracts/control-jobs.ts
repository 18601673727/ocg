import type * as Api from "./api";
import { ContractError } from "./decode";

export type ControlJob = Api.Job;
export type ControlExecutionPhase = Api.ExecutionPhase;
export type ControlOutput = Api.Output;
export type ControlJobDetails = Api.Job &
  Pick<Api.JobDetails, "result" | "unknown_reason" | "activity" | "task_plan" | "allocation"> & {
    mutable_phase?: string;
    context?: string;
  };
export type ControlJobActivity = ControlJobDetails["activity"];
export type ControlTaskPlan = NonNullable<ControlJobDetails["task_plan"]>;
export type ControlJobToolsPage = Api.Tools;
export type ControlProviderResponse = Omit<Api.ProviderResponse, "text_offset" | "text_complete">;
export type ControlAttachment = Omit<Api.Attachment, "created_at_us"> & {
  created_at_us: number | null;
  data_url: string | undefined;
};
export type ControlJobEvent =
  | ({ type: "job" } & Api.JobEvent)
  | ({ type: "output" } & Api.OutputEvent)
  | ({ type: "settlement" } & Api.SettlementEvent)
  | ({ type: "stream_error" } & Api.ErrorResponse);

export function projectJobEvent(name: string, input: unknown): ControlJobEvent {
  switch (name) {
    case "job":
      return { ...(input as Api.JobEvent), type: "job" };
    case "output":
      return { ...(input as Api.OutputEvent), type: "output" };
    case "settlement":
      return { ...(input as Api.SettlementEvent), type: "settlement" };
    case "stream_error":
      return { ...(input as Api.ErrorResponse), type: "stream_error" };
    default:
      throw new ContractError("event", "job, output, settlement or stream_error");
  }
}

export function projectJob(input: Api.JobDetails) {
  return { ...input.job, unknown_reason: input.unknown_reason ?? null };
}

export function projectJobDetails(input: Api.JobDetails): ControlJobDetails {
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

export type ControlNativeStopRequest = "ended_before_start" | "requested" | "already_requested";

export type ControlNativeStop = {
  job: string;
  request: ControlNativeStopRequest;
  stopped: boolean;
  execution_phase: "cancelled" | "stop_requested";
};

const JOB_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

export function projectNativeStop(status: number, input: unknown): ControlNativeStop {
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
