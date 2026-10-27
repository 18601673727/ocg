import type * as Api from "./api";
import { ContractError } from "./decode";

export type ControlJob = Api.Job;
export type ControlOutput = Api.Output;
export type ControlJobDetails = Api.Job &
  Pick<Api.JobDetails, "result" | "unknown_reason" | "activity" | "task_plan">;
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
  };
}

export function projectAdmission(input: Api.AdmissionResponse) {
  const admission = input.admission;
  if (admission.status === "admitted") return admission;
  return {
    ...admission,
    conversation: null,
    seq: null,
    joined: false,
    model: null,
    kept: (admission as typeof admission & { kept?: boolean }).kept ?? false,
  };
}
