import {
  ContractError,
  executionSequence,
  projectJob,
  projectJobDetails,
  projectNativeStop,
} from "@/components/ocg/contracts";
import type * as Api from "@/components/ocg/contracts/api";
import { ApiError, type Transport } from "./transport";
import { identity, jobIdentity, sequence, submissionKey } from "./validators";

export type MutableJobInput = {
  text: string;
  attachments: string[];
  configuration: {
    llm_profile: string;
    agent_profile: string | null;
    reasoning_effort: string | null;
  } | null;
  priority: "foreground" | "automatic" | "speculative";
};

export type MutableJob = {
  id: string;
  conversation: string;
  previous: string | null;
  project: string | null;
  context: string;
  revision: number;
  phase: "unsubmitted" | "queued" | "executing" | "completed" | "failed" | "cancelled" | "unknown";
  submitted_at: number | null;
  unsupported_decision_history?: boolean;
  autonomous_loopback_eligibility?: AutonomousLoopbackEligibility;
  input: MutableJobInput;
};

type AutonomousLoopbackEligibility = {
  eligible: boolean;
  code: string;
  detail: string;
  endpoint_class: string | null;
  scope: "frozen_provider_configuration";
  not_assessed?: string[];
};

function autonomousLoopbackEligibility(value: unknown): AutonomousLoopbackEligibility | undefined {
  if (value === undefined) return undefined;
  const eligibility = value as AutonomousLoopbackEligibility | null;
  if (
    !eligibility ||
    typeof eligibility.eligible !== "boolean" ||
    typeof eligibility.code !== "string" ||
    typeof eligibility.detail !== "string" ||
    !(eligibility.endpoint_class === null || typeof eligibility.endpoint_class === "string") ||
    eligibility.scope !== "frozen_provider_configuration" ||
    !(
      eligibility.not_assessed === undefined ||
      (Array.isArray(eligibility.not_assessed) &&
        eligibility.not_assessed.every((field) => typeof field === "string"))
    )
  )
    throw new ContractError("job.autonomous_loopback_eligibility", "an eligibility explanation");
  return eligibility;
}

function mutableJob(value: unknown, context: string): MutableJob {
  const envelope = value as { api_version?: number; job?: MutableJob } | null;
  const job = envelope?.job;
  if (
    envelope?.api_version !== 1 ||
    !job ||
    job.context !== context ||
    !Number.isSafeInteger(job.revision) ||
    job.revision < 1 ||
    !["unsubmitted", "queued", "executing", "completed", "failed", "cancelled", "unknown"].includes(
      job.phase,
    ) ||
    !job.input ||
    typeof job.input.text !== "string" ||
    !Array.isArray(job.input.attachments) ||
    !job.input.attachments.every((id) => typeof id === "string") ||
    !["foreground", "automatic", "speculative"].includes(job.input.priority) ||
    !(
      job.unsupported_decision_history === undefined ||
      typeof job.unsupported_decision_history === "boolean"
    ) ||
    !(job.submitted_at === null || Number.isSafeInteger(job.submitted_at))
  ) {
    throw new ContractError("job", "a valid mutable Job");
  }
  job.autonomous_loopback_eligibility = autonomousLoopbackEligibility(
    job.autonomous_loopback_eligibility,
  );
  identity(job.id, "job");
  identity(job.conversation, "conversation");
  if (job.previous !== null) identity(job.previous, "previous");
  if (job.project !== null) identity(job.project, "project");
  for (const id of job.input.attachments) identity(id, "attachment");
  if (job.input.configuration !== null) {
    const config = job.input.configuration;
    if (
      !config ||
      typeof config.llm_profile !== "string" ||
      !(config.agent_profile === null || typeof config.agent_profile === "string") ||
      !(config.reasoning_effort === null || typeof config.reasoning_effort === "string")
    )
      throw new ContractError("job.input.configuration", "a valid configuration");
    identity(config.llm_profile, "llm_profile");
    if (config.agent_profile) identity(config.agent_profile, "agent_profile");
  }
  return job;
}

function editableJob(value: unknown): unknown {
  const body = value as {
    api_version?: number;
    job?: { id?: string; conversation?: string | null; previous?: string | null };
    project?: string | null;
    context?: string;
    revision?: number;
    phase?: MutableJob["phase"];
    submitted_at?: number | null;
    unsupported_decision_history?: boolean;
    autonomous_loopback_eligibility?: unknown;
    input?: MutableJobInput;
  } | null;
  if (!body?.job) throw new ContractError("job", "a valid mutable Job");
  return {
    api_version: body.api_version,
    job: {
      id: body.job.id,
      conversation: body.job.conversation,
      previous: body.job.previous ?? null,
      project: body.project ?? null,
      context: body.context,
      revision: body.revision,
      phase: body.phase,
      submitted_at: body.submitted_at ?? null,
      unsupported_decision_history: body.unsupported_decision_history,
      autonomous_loopback_eligibility: body.autonomous_loopback_eligibility,
      input: body.input,
    },
  };
}

export function createJobsApi({ send, programmatic }: Transport) {
  return {
    async currentMutableJob(context: string, signal?: AbortSignal): Promise<MutableJob | null> {
      try {
        return mutableJob(
          (
            await send("GET", "/api/v1/jobs/current", {
              context,
              signal,
            })
          ).input,
          context,
        );
      } catch (error) {
        if (error instanceof ApiError && error.status === 404 && error.code === "not_found")
          return null;
        throw error;
      }
    },
    async readMutableJob(context: string, job: string, signal?: AbortSignal): Promise<MutableJob> {
      return mutableJob(
        editableJob(
          (
            await send("GET", `/api/v1/jobs/${jobIdentity(job)}`, {
              context,
              signal,
            })
          ).input,
        ),
        context,
      );
    },
    async createMutableJob(
      context: string,
      input: MutableJobInput,
      key: string,
      association: { conversation?: string; project?: string } = {},
    ): Promise<MutableJob> {
      return mutableJob(
        (
          await send("POST", "/api/v1/jobs", {
            context,
            body: { ...input, ...association },
            key: submissionKey(key),
            sign: !programmatic,
          })
        ).input,
        context,
      );
    },
    async updateMutableJob(
      context: string,
      job: string,
      revision: number,
      input: MutableJobInput,
    ): Promise<MutableJob> {
      return mutableJob(
        (
          await send("POST", `/api/v1/jobs/${jobIdentity(job)}`, {
            context,
            body: { revision: sequence(revision), input },
            sign: !programmatic,
          })
        ).input,
        context,
      );
    },
    async submitMutableJob(
      context: string,
      job: string,
      revision: number,
      input: MutableJobInput,
      key: string,
    ): Promise<MutableJob> {
      return mutableJob(
        (
          await send("POST", `/api/v1/jobs/${jobIdentity(job)}/submit`, {
            context,
            body: { revision: sequence(revision), input },
            key: submissionKey(key),
            sign: !programmatic,
          })
        ).input,
        context,
      );
    },
    async discardMutableJob(context: string, job: string, revision: number): Promise<void> {
      await send("DELETE", `/api/v1/jobs/${jobIdentity(job)}`, {
        context,
        revision,
        sign: !programmatic,
      });
    },
    async cancelUnsupportedQueuedJob(
      context: string,
      job: string,
      revision: number,
    ): Promise<MutableJob> {
      return mutableJob(
        (
          await send("POST", `/api/v1/jobs/${jobIdentity(job)}/cancel-unsupported`, {
            context,
            body: { revision: sequence(revision) },
            sign: !programmatic,
          })
        ).input,
        context,
      );
    },
    async withdrawQueuedJob(context: string, job: string, revision: number): Promise<MutableJob> {
      return mutableJob(
        (
          await send("POST", `/api/v1/jobs/${jobIdentity(job)}/withdraw`, {
            context,
            body: { revision: sequence(revision) },
            sign: !programmatic,
          })
        ).input,
        context,
      );
    },
    async readJobs(
      settings: { before?: string; limit?: number; query?: string; signal?: AbortSignal } = {},
    ) {
      const query = [`limit=${settings.limit ?? 50}`];
      if (settings.before) query.push(`before=${jobIdentity(settings.before)}`);
      if (settings.query?.trim()) query.push(`query=${encodeURIComponent(settings.query.trim())}`);
      return (
        await send("GET", `/api/v1/job-history?${query.join("&")}`, {
          signal: settings.signal,
        })
      ).input as Api.Jobs;
    },
    async readJob(job: string, signal?: AbortSignal) {
      jobIdentity(job);
      return projectJob(
        (await send("GET", `/api/v1/jobs/${job}`, { signal })).input as Api.JobDetails,
      );
    },
    async readJobDetails(job: string, signal?: AbortSignal) {
      jobIdentity(job);
      return projectJobDetails(
        (await send("GET", `/api/v1/jobs/${job}`, { signal })).input as Api.JobDetails,
      );
    },

    async readExecutionHistory(
      job: string,
      settings: { after?: string; limit?: number; context?: string; signal?: AbortSignal } = {},
    ): Promise<Api.ExecutionHistory> {
      jobIdentity(job);
      const query = `?limit=${sequence(settings.limit ?? 20)}${settings.after === undefined ? "" : `&after=${executionSequence(settings.after)}`}`;
      const answer = (await send("GET", `/api/v1/jobs/${job}/history${query}`, settings))
        .input as Api.ExecutionHistory;
      if (answer.job !== job || !Array.isArray(answer.events))
        throw new ContractError("history", "execution history for the requested Job");
      executionSequence(answer.event_seq);
      if (answer.next_after !== null) executionSequence(answer.next_after);
      if (answer.has_more !== (answer.next_after !== null))
        throw new ContractError("next_after", "a valid execution history cursor");
      for (const event of answer.events) {
        executionSequence(event.seq);
        executionSequence(event.previous_seq);
        executionSequence(event.transaction);
        executionSequence(event.committed_seq);
      }
      return answer;
    },

    async readExecutionState(
      job: string,
      settings: { atSeq?: string; context?: string; signal?: AbortSignal } = {},
    ): Promise<Api.ExecutionState> {
      jobIdentity(job);
      const query =
        settings.atSeq === undefined ? "" : `?at_seq=${executionSequence(settings.atSeq)}`;
      const answer = (await send("GET", `/api/v1/jobs/${job}/state${query}`, settings))
        .input as Api.ExecutionState;
      if (answer.job !== job || answer.at_seq !== (settings.atSeq ?? null))
        throw new ContractError("state", "the requested Job and historical cutoff");
      executionSequence(answer.event_seq);
      return answer;
    },

    async stopNativeJob(context: string, job: string, signal?: AbortSignal) {
      const identity = jobIdentity(job);
      const answer = await send("POST", `/api/v1/jobs/${identity}/stop`, {
        context,
        body: {},
        sign: !programmatic,
        signal,
      });
      const stop = projectNativeStop(answer.status, answer.input);
      if (stop.job !== identity)
        throw new ContractError("stop.job", "the stop answer names the requested Job");
      return stop;
    },
    async readJobInput(job: string, signal?: AbortSignal) {
      jobIdentity(job);
      const answer = (await send("GET", `/api/v1/jobs/${job}/input`, { signal }))
        .input as Api.JobInput;
      return { ...answer.input, job: answer.job };
    },
  };
}
