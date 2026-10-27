import { ContractError, isRecord } from "./decode";
import type * as Api from "./api";
import { CONTROL_API_VERSION } from "./generated";

type ProjectedAgent = Omit<Api.AgentProfile, "llm_targets"> & {
  llm_targets: (Omit<Api.LlmTarget, "parameters"> & { default_effort: string | null })[];
};
type ProjectedManagedAgent = Api.ManagedAgent;
type ProjectedEntry<E = Api.TurnEntry> = E extends unknown
  ? Omit<
      E,
      | "text"
      | "attachments"
      | "call"
      | "child"
      | "event"
      | "reference"
      | "approval"
      | "turn"
      | "by"
      | "n"
      | "type"
      | "name"
      | "job"
      | "policy_revision"
      | "artifact"
      | "message"
    > & { text: string | null; attachments: string[]; message: number | null }
  : never;
type ProjectedTurn = Omit<Api.Turn, "entries" | "configuration" | "uncertain_calls"> & {
  entries: ProjectedEntry[];
  input: ReturnType<typeof jobInputRecord>;
};

export { ContractError } from "./decode";

function record(value: unknown, path: string): Record<string, unknown> {
  if (!isRecord(value)) throw new ContractError(path, "an object");
  return value;
}

function string(value: unknown, path: string): string {
  if (typeof value !== "string") throw new ContractError(path, "a string");
  return value;
}

function llmProtocol(
  value: unknown,
  path: string,
): "openai_chat_completions" | "openai_responses" | "anthropic_messages" {
  if (
    value === "openai_chat_completions" ||
    value === "openai_responses" ||
    value === "anthropic_messages"
  )
    return value;
  throw new ContractError(path, "openai_chat_completions, openai_responses or anthropic_messages");
}

function integer(value: unknown, path: string): number {
  // Fail closed rather than round Rust i64 cursors into another output.
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new ContractError(path, "a non-negative safe integer");
  }
  return value;
}

function signedInteger(value: unknown, path: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value))
    throw new ContractError(path, "a signed safe integer");
  return value;
}

function timestamp(value: unknown, path: string): number | null {
  return value === null ? null : integer(value, path);
}

function boolean(value: unknown, path: string): boolean {
  if (typeof value !== "boolean") throw new ContractError(path, "a boolean");
  return value;
}

function settlement(input: unknown): {
  state: "unsettled" | "completed" | "failed" | "cancelled" | "unknown";
  settled_at_us: number | null;
} {
  const value = record(input, "settlement");
  const state = value.state;
  if (
    state !== "unsettled" &&
    state !== "completed" &&
    state !== "failed" &&
    state !== "cancelled" &&
    state !== "unknown"
  ) {
    throw new ContractError("settlement.state", "a Control Plane settlement state");
  }
  const at = timestamp(value.settled_at_us, "settlement.settled_at_us");
  if ((state === "unsettled") !== (at === null)) {
    throw new ContractError("settlement.settled_at_us", "a timestamp only for an outcome");
  }
  return { state, settled_at_us: at } satisfies Api.Settlement;
}

export function decodeControlError(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  const value = record(envelope.error, "error");
  return {
    code: string(value.code, "error.code"),
    message: string(value.message, "error.message"),
  } satisfies Api.Error;
}

const CANONICAL_IDENTITY = "[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}";
const IDENTITY = new RegExp(`^${CANONICAL_IDENTITY}$`);
const RETRY_CURSOR = new RegExp(`^${CANONICAL_IDENTITY}\\.${CANONICAL_IDENTITY}$`);

function identity(value: unknown, path: string): string {
  const text = string(value, path);
  if (!IDENTITY.test(text)) {
    throw new ContractError(path, "a canonical UUIDv7 identity");
  }
  return text;
}

// These are checked projections of the JSON emitted by control_plane.rs,
// not the incompatible 0.4.5 Job/Attempt/Call DTOs or a second lifecycle.
function jobRecord(input: unknown) {
  const job = record(input, "job");
  const mode = job.execution_mode;
  if (mode !== "once" && mode !== "continuous" && mode !== "periodic" && mode !== "container") {
    throw new ContractError("job.execution_mode", "a Control Plane execution mode");
  }
  return {
    id: identity(job.id, "job.id"),
    ...(job.parent === undefined || job.parent === null
      ? {}
      : { parent: identity(job.parent, "job.parent") }),
    kind:
      job.kind === "conversation"
        ? ("conversation" as const)
        : job.kind === "turn"
          ? ("turn" as const)
          : (() => {
              throw new ContractError("job.kind", "conversation or turn");
            })(),
    conversation: job.conversation === null ? null : identity(job.conversation, "job.conversation"),
    name: job.name === null ? null : string(job.name, "job.name"),
    title: job.title === undefined || job.title === null ? null : string(job.title, "job.title"),
    is_chat: typeof job.is_chat === "boolean" ? job.is_chat : false,
    execution_mode: mode,
    input: string(job.input, "job.input"),
    previous:
      job.previous === undefined || job.previous === null
        ? null
        : identity(job.previous, "job.previous"),
    assigned_executor:
      job.assigned_executor === null
        ? null
        : identity(job.assigned_executor, "job.assigned_executor"),
    created_at_us: integer(job.created_at_us, "job.created_at_us"),
    updated_at_us: integer(job.updated_at_us, "job.updated_at_us"),
    started_at_us: timestamp(job.started_at_us, "job.started_at_us"),
    paused_at_us: timestamp(job.paused_at_us, "job.paused_at_us"),
    sterilized_at_us: timestamp(job.sterilized_at_us, "job.sterilized_at_us"),
    settlement: settlement(job.settlement),
    stop_confirmed_at_us: timestamp(job.stop_confirmed_at_us, "job.stop_confirmed_at_us"),
  } satisfies Api.Job;
}

function outputRecord(input: unknown) {
  const output = record(input, "output");
  const seq = integer(output.seq, "output.seq");
  if (seq === 0) throw new ContractError("output.seq", "a positive stored sequence");
  const kind = output.kind;
  if (
    kind !== "progress" &&
    kind !== "evaluation" &&
    kind !== "result" &&
    kind !== "reasoning" &&
    kind !== "decision" &&
    kind !== "summarization"
  ) {
    throw new ContractError(
      "output.kind",
      "progress, evaluation, result, reasoning, decision or summarization",
    );
  }
  if (output.goal_reached !== null && typeof output.goal_reached !== "boolean") {
    throw new ContractError("output.goal_reached", "a boolean or null");
  }
  if (!("value" in output)) throw new ContractError("output.value", "a JSON value");
  return {
    id: string(output.id, "output.id"),
    seq,
    kind,
    summary: output.summary === null ? null : string(output.summary, "output.summary"),
    code: output.code === null ? null : string(output.code, "output.code"),
    goal_reached: output.goal_reached,
    value: output.value,
    created_at_us: integer(output.created_at_us, "output.created_at_us"),
  } satisfies Api.Output;
}

export function decodeJobEvent(name: string, input: unknown, jobId: string, id: string | null) {
  const value = record(input, "event");
  if (value.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  if (name !== "output" && id !== null) throw new ContractError("id", "an id only on output");
  if (name === "stream_error") {
    return {
      type: "stream_error" as const,
      error: decodeControlError(value),
    } satisfies Api.ErrorResponse & { type: "stream_error" };
  }
  if (name === "job") {
    const job = jobRecord(value.job);
    if (job.id !== jobId) throw new ContractError("job.id", "the requested Job");
    return { type: "job" as const, job: { ...job, id: jobId } } satisfies Api.JobEvent & {
      type: "job";
    };
  }
  if (value.job !== jobId) throw new ContractError("job", "the requested Job");
  if (name === "settlement") {
    const outcome = settlement(value.settlement);
    const stopped = timestamp(value.stop_confirmed_at_us, "stop_confirmed_at_us");
    if (outcome.state === "unsettled" || (outcome.state === "unknown" && stopped === null)) {
      throw new ContractError(
        "settlement",
        "a concluded outcome, with stop confirmation for unknown",
      );
    }
    return {
      type: "settlement" as const,
      job: jobId,
      settlement: outcome,
      stop_confirmed_at_us: stopped,
    } satisfies Api.SettlementEvent & { type: "settlement" };
  }
  if (name !== "output")
    throw new ContractError("event", "job, output, settlement or stream_error");
  const output = outputRecord(value.output);
  if (id !== String(output.seq)) throw new ContractError("id", "the output's positive stored seq");
  return { type: "output" as const, job: jobId, output } satisfies Api.OutputEvent & {
    type: "output";
  };
}

export type ControlJobEvent = ReturnType<typeof decodeJobEvent>;
export type ControlJob = ReturnType<typeof jobRecord>;
export type ControlOutput = ReturnType<typeof outputRecord>;

export function decodeProviderResponses(input: unknown, jobId: string) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  const job = identity(envelope.job, "job");
  if (job !== jobId) throw new ContractError("job", "the requested Job");
  if (!Array.isArray(envelope.responses)) throw new ContractError("responses", "an array");
  let previous = 0;
  const count = envelope.responses.length;
  const responses = envelope.responses.map((input, index) => {
    const value = record(input, `responses[${index}]`);
    const request = integer(value.request, `responses[${index}].request`);
    if (request <= previous || request > 0xffffffff)
      throw new ContractError("responses.request", "strictly increasing positive request numbers");
    previous = request;
    const text_offset = integer(value.text_offset, "responses.text_offset");
    const text_complete = boolean(value.text_complete, "responses.text_complete");
    const text = string(value.text, "responses.text");
    if (
      text_offset > 0xffffffff ||
      (index > 0 && text_offset !== 0) ||
      (!text_complete && (index !== count - 1 || text.length === 0))
    )
      throw new ContractError("responses", "contiguous nonempty reply chunks");
    return {
      request,
      text,
      text_offset,
      text_complete,
      requests_tools: boolean(value.requests_tools, `responses[${index}].requests_tools`),
      created_at_us: integer(value.created_at_us, `responses[${index}].created_at_us`),
    } satisfies Api.ProviderResponse;
  });
  const has_more = boolean(envelope.has_more, "has_more");
  const next_after =
    envelope.next_after === null ? null : integer(envelope.next_after, "next_after");
  const next_text_offset =
    envelope.next_text_offset === null
      ? null
      : integer(envelope.next_text_offset, "next_text_offset");
  const last = responses.at(-1);
  const expectedOffset =
    last && !last.text_complete ? last.text_offset + new TextEncoder().encode(last.text).length : 0;
  if (
    has_more
      ? !last ||
        expectedOffset > 0xffffffff ||
        next_after !== last.request ||
        next_text_offset !== expectedOffset
      : next_after !== null || next_text_offset !== null || last?.text_complete === false
  )
    throw new ContractError("next_after", "an exact advancing reply chunk cursor");
  return { job, responses, next_after, next_text_offset, has_more } satisfies Api.ProviderResponses;
}

export type ControlProviderResponse = Omit<Api.ProviderResponse, "text_offset" | "text_complete">;

export function decodeConversationLimits(input: unknown, conversation: string) {
  const item = record(input, "response");
  if (item.api_version !== CONTROL_API_VERSION || item.conversation !== conversation)
    throw new ContractError("conversation", conversation);
  const depth = integer(item.depth, "depth");
  const remaining_depth = integer(item.remaining_depth, "remaining_depth");
  const max_depth = integer(item.max_depth, "max_depth");
  const max_auto_turns = integer(item.max_auto_turns, "max_auto_turns");
  const max_iterations = integer(item.max_iterations, "max_iterations");
  if (
    depth > 16 ||
    max_depth > 16 ||
    max_auto_turns > 255 ||
    max_iterations > 10000 ||
    remaining_depth !== Math.max(0, max_depth - depth)
  )
    throw new ContractError("limits", "bounded conversation limits with matching remaining depth");
  return {
    conversation,
    depth,
    remaining_depth,
    max_depth,
    max_auto_turns,
    max_iterations,
  } satisfies Api.ConversationLimits;
}

/** What the host establishes about itself and its database, now. */
export function decodeHealth(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  const host = string(envelope.host, "host");
  const database = record(envelope.database, "database");
  const status = database.status;
  if (
    status !== "open" &&
    status !== "protected" &&
    status !== "shutting_down" &&
    status !== "stopped" &&
    status !== "closed"
  ) {
    throw new ContractError("database.status", "a Control Plane database status");
  }
  const quarantined = listOf(envelope.quarantined, "quarantined", (entry, path) => {
    const item = record(entry, path);
    return {
      job: identity(item.job, `${path}.job`),
      pool: item.pool === null ? null : identity(item.pool, `${path}.pool`),
      since: integer(item.since, `${path}.since`),
    } satisfies Api.Quarantined;
  });
  return {
    host,
    database: {
      status,
      accepting_commands: boolean(database.accepting_commands, "database.accepting_commands"),
      readable: boolean(database.readable, "database.readable"),
    },
    quarantined,
  } satisfies Omit<Api.Health, "database" | "claims" | "queue" | "process"> & {
    database: Omit<
      Api.DatabaseHealth,
      "database_bytes" | "available_bytes" | "durable_commits" | "commit_us_mean" | "commit_us_max"
    >;
  };
}

/** The Job's persisted record, as `GET /api/v1/jobs/{job}` reports it. */
export function decodeJob(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  const unknown = envelope.unknown_reason;
  const unknown_reason =
    unknown === undefined || unknown === null ? null : string(unknown, "unknown_reason");
  return { ...jobRecord(envelope.job), unknown_reason } satisfies Api.Job &
    Omit<Api.JobDetails, "job" | "result" | "usage" | "activity" | "task_plan">;
}

function activityPhase(input: unknown): Api.JobActivityPhase {
  if (
    input === "queued" ||
    input === "starting" ||
    input === "waiting_for_provider" ||
    input === "running_tools" ||
    input === "waiting_for_user" ||
    input === "rate_limited" ||
    input === "running_tasks" ||
    input === "synthesizing" ||
    input === "completed" ||
    input === "failed" ||
    input === "cancelled" ||
    input === "unknown"
  )
    return input;
  throw new ContractError("activity.phase", "a Job activity phase");
}

function taskPlan(input: unknown) {
  const plan = record(input, "task_plan");
  const steps = listOf(plan.steps, "task_plan.steps", (value) => {
    const step = record(value, "task_plan.steps[]");
    return {
      depends_on: listOf(step.depends_on, "task_step.depends_on", (value) =>
        identity(value, "task_step.depends_on[]"),
      ),
      conversation: identity(step.conversation, "task_step.conversation"),
      turn: identity(step.turn, "task_step.turn"),
      title: string(step.title, "task_step.title"),
      agent: step.agent === null ? null : identity(step.agent, "task_step.agent"),
      model: step.model === null ? null : identity(step.model, "task_step.model"),
      state: activityPhase(step.state),
    } satisfies Api.TaskStep;
  });
  const earlier = new Set<string>();
  for (const step of steps) {
    if (
      new Set(step.depends_on).size !== step.depends_on.length ||
      step.depends_on.some((id) => !earlier.has(id))
    ) {
      throw new ContractError("task_step.depends_on", "unique references to earlier steps");
    }
    earlier.add(step.conversation);
  }
  return {
    owner: identity(plan.owner, "task_plan.owner"),
    continuation: identity(plan.continuation, "task_plan.continuation"),
    state: activityPhase(plan.state),
    steps,
  } satisfies Api.TaskPlan;
}

export function decodeJobDetails(input: unknown) {
  const envelope = record(input, "response");
  const activity = record(envelope.activity, "activity");
  return {
    ...decodeJob(input),
    result: envelope.result === null ? null : outputRecord(envelope.result),
    activity: {
      phase: activityPhase(activity.phase),
      request: activity.request === null ? null : integer(activity.request, "activity.request"),
      request_started_at_us: timestamp(
        activity.request_started_at_us,
        "activity.request_started_at_us",
      ),
      last_activity_at_us: integer(activity.last_activity_at_us, "activity.last_activity_at_us"),
      last_output_at_us: timestamp(activity.last_output_at_us, "activity.last_output_at_us"),
      last_output_seq: integer(activity.last_output_seq, "activity.last_output_seq"),
      provider_status:
        activity.provider_status === null
          ? null
          : integer(activity.provider_status, "activity.provider_status"),
      tool: activity.tool === null ? null : string(activity.tool, "activity.tool"),
    } satisfies Api.JobActivity,
    task_plan: envelope.task_plan === null ? null : taskPlan(envelope.task_plan),
  } satisfies Api.Job &
    Pick<Api.JobDetails, "result" | "unknown_reason" | "activity" | "task_plan">;
}

export type ControlJobDetails = ReturnType<typeof decodeJobDetails>;
export type ControlJobActivity = ControlJobDetails["activity"];
export type ControlTaskPlan = NonNullable<ControlJobDetails["task_plan"]>;

export function decodeJobs(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  return {
    jobs: listOf(envelope.jobs, "jobs", (job) => jobRecord(job)),
    next_before:
      envelope.next_before === null ? null : identity(envelope.next_before, "next_before"),
    has_more: boolean(envelope.has_more, "has_more"),
  } satisfies Api.Jobs;
}

/** A hold actor, as conversation, child, and tree boundaries carry it. */
function holdActor(input: unknown, path: string): Api.Actor {
  const by = record(input, path);
  const kind = oneOf(by.kind, `${path}.kind`, ["user", "agent", "loop", "auto"] as const);
  if (kind === "agent") {
    return { kind, conversation: identity(by.conversation, `${path}.conversation`) };
  }
  return { kind };
}

function conversationRecord(input: unknown) {
  const c = record(input, "conversation");
  const hold = c.on_hold === null ? null : record(c.on_hold, "conversation.on_hold");
  return {
    job_state: string(c.job_state, "conversation.job_state"),
    has_children: boolean(c.has_children, "conversation.has_children"),
    root_job: identity(c.root_job, "conversation.root_job"),
    tip_job: identity(c.tip_job, "conversation.tip_job"),
    agent: c.agent === null ? null : identity(c.agent, "conversation.agent"),
    title: string(c.title, "conversation.title"),
    state:
      c.state === "open" || c.state === "closing" || c.state === "settled"
        ? c.state
        : (() => {
            throw new ContractError("conversation.state", "open, closing or settled");
          })(),
    on_hold: hold === null ? null : { by: holdActor(hold.by, "conversation.on_hold.by") },
    project: c.project === null ? null : identity(c.project, "conversation.project"),
    tip_settlement_state: string(c.tip_settlement_state, "conversation.tip_settlement_state"),
    tip_created_at_us: integer(c.tip_created_at_us, "conversation.tip_created_at_us"),
  } satisfies Api.Conversation;
}

export function decodeConversations(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  return {
    conversations: listOf(envelope.conversations, "conversations", (conversation) =>
      conversationRecord(conversation),
    ),
    next_before:
      envelope.next_before === null || envelope.next_before === undefined
        ? null
        : identity(envelope.next_before, "next_before"),
    has_more: boolean(envelope.has_more, "has_more"),
  } satisfies Api.Conversations;
}

function observationPage(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  const next = envelope.next_after;
  if (next !== null && (typeof next !== "string" || !/^[A-Za-z0-9_-]{1,400}$/.test(next))) {
    throw new ContractError("next_after", "an observation cursor or null");
  }
  return { envelope, next_after: next as string | null };
}

function observation(input: unknown, path: string) {
  const value = record(input, path);
  return {
    value,
    common: {
      id: string(value.id, `${path}.id`),
      job: identity(value.job, `${path}.job`),
      request: integer(value.request, `${path}.request`),
      observed_at_us: integer(value.observed_at_us, `${path}.observed_at_us`),
      model: optionalString(value.model, `${path}.model`),
      provider: optionalString(value.provider, `${path}.provider`),
    },
  };
}

export function decodeUsage(input: unknown) {
  const { envelope, next_after } = observationPage(input);
  return {
    next_after,
    observations: listOf(envelope.observations, "observations", (input, path) => {
      const { value, common } = observation(input, path);
      if (
        value.finality !== null &&
        value.finality !== "final" &&
        value.finality !== "cumulative"
      ) {
        throw new ContractError(`${path}.finality`, "final, cumulative or null");
      }
      if (
        value.eligibility !== null &&
        value.eligibility !== "eligible" &&
        value.eligibility !== "excluded"
      ) {
        throw new ContractError(`${path}.eligibility`, "eligible, excluded or null");
      }
      return {
        ...common,
        llm_profile:
          value.llm_profile === null ? null : identity(value.llm_profile, `${path}.llm_profile`),
        input_tokens: timestamp(value.input_tokens, `${path}.input_tokens`),
        output_tokens: timestamp(value.output_tokens, `${path}.output_tokens`),
        cache_read_tokens: timestamp(value.cache_read_tokens, `${path}.cache_read_tokens`),
        cache_write_tokens: timestamp(value.cache_write_tokens, `${path}.cache_write_tokens`),
        reasoning_tokens: timestamp(value.reasoning_tokens, `${path}.reasoning_tokens`),
        ttfb_ms: timestamp(value.ttfb_ms, `${path}.ttfb_ms`),
        finished_in_ms: timestamp(value.finished_in_ms, `${path}.finished_in_ms`),
        finality: value.finality,
        eligibility: value.eligibility,
      } satisfies Api.UsageObservation;
    }),
  } satisfies Api.Usage;
}

export function decodeUsageBuckets(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  const bucket = envelope.bucket;
  if (bucket !== "day" && bucket !== "week") throw new ContractError("bucket", "day or week");
  const offset = signedInteger(envelope.tz_offset_minutes, "tz_offset_minutes");
  if (offset < -840 || offset > 840 || offset % 60 !== 0) {
    throw new ContractError("tz_offset_minutes", "whole hours from -840 to 840 minutes");
  }
  const complete = boolean(envelope.complete, "complete");
  const resume =
    envelope.resume_after === null ? null : string(envelope.resume_after, "resume_after");
  if (complete !== (resume === null)) {
    throw new ContractError("resume_after", "null exactly when complete");
  }
  return {
    api_version: CONTROL_API_VERSION,
    from_us: signedInteger(envelope.from_us, "from_us"),
    until_us: signedInteger(envelope.until_us, "until_us"),
    complete,
    resume_after: resume,
    bucket,
    tz_offset_minutes: offset,
    rows: listOf(envelope.rows, "rows", (input, path) => {
      const row = record(input, path);
      return {
        bucket_us: signedInteger(row.bucket_us, `${path}.bucket_us`),
        agent: row.agent === null ? null : identity(row.agent, `${path}.agent`),
        provider: identity(row.provider, `${path}.provider`),
        model: row.model === null ? null : string(row.model, `${path}.model`),
        requests: integer(row.requests, `${path}.requests`),
        complete_requests: integer(row.complete_requests, `${path}.complete_requests`),
        input_tokens: integer(row.input_tokens, `${path}.input_tokens`),
        output_tokens: integer(row.output_tokens, `${path}.output_tokens`),
        cache_read_tokens: integer(row.cache_read_tokens, `${path}.cache_read_tokens`),
        cache_write_tokens: integer(row.cache_write_tokens, `${path}.cache_write_tokens`),
        reasoning_tokens: integer(row.reasoning_tokens, `${path}.reasoning_tokens`),
      } satisfies Api.UsageBucket;
    }),
  } satisfies Api.Envelope<Api.UsageBuckets>;
}

export function decodeUsageRetries(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  const next = envelope.next_after;
  if (next !== null && (typeof next !== "string" || !RETRY_CURSOR.test(next)))
    throw new ContractError("next_after", "a retry cursor or null");
  return {
    next_after: next,
    retry_rows: listOf(envelope.retry_rows, "retry_rows", (input, path) => {
      const row = record(input, path);
      return {
        agent: row.agent === null ? null : identity(row.agent, `${path}.agent`),
        provider: identity(row.provider, `${path}.provider`),
        model: row.model === null ? null : string(row.model, `${path}.model`),
        retries: integer(row.retries, `${path}.retries`),
        bucket_us: signedInteger(row.bucket_us, `${path}.bucket_us`),
      } satisfies Api.UsageRetryRow;
    }),
  } satisfies Api.UsageRetries;
}

export function decodeAvailability(input: unknown) {
  const { envelope, next_after } = observationPage(input);
  return {
    next_after,
    observations: listOf(envelope.observations, "observations", (input, path) => {
      const { value, common } = observation(input, path);
      const classification = string(value.classification, `${path}.classification`);
      if (!["available", "unavailable", "rejected", "unconfirmed"].includes(classification)) {
        throw new ContractError(`${path}.classification`, "a provider observation classification");
      }
      return {
        ...common,
        status_code: timestamp(value.status_code, `${path}.status_code`),
        finish_reason: optionalString(value.finish_reason, `${path}.finish_reason`),
        classification,
      } satisfies Api.AvailabilityObservation;
    }),
  } satisfies Api.Availability;
}

export function decodeLogs(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  return {
    file: optionalString(envelope.file, "file"),
    files: listOf(envelope.files, "files", string),
    entries: listOf(envelope.entries, "entries", (input, path) => {
      const entry = record(input, path);
      return {
        offset: integer(entry.offset, `${path}.offset`),
        line: string(entry.line, `${path}.line`),
      };
    }),
    next_before:
      envelope.next_before === null ? null : integer(envelope.next_before, "next_before"),
    truncated: boolean(envelope.truncated, "truncated"),
    files_truncated: boolean(envelope.files_truncated, "files_truncated"),
  } satisfies Api.Logs;
}

/** One stored page of a Job's outputs, in `seq` order. */
export function decodeOutputsPage(input: unknown, jobId: string) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  if (envelope.job !== jobId) throw new ContractError("job", "the requested Job");
  const after = timestamp(envelope.after_seq, "after_seq");
  const listed = envelope.outputs;
  if (!Array.isArray(listed)) throw new ContractError("outputs", "an array");
  const outputs = listed.map((output) => outputRecord(output));
  let previous = after ?? 0;
  for (const output of outputs) {
    // The stored order is the processing order; a page that breaks it is
    // refused rather than reordered.
    if (output.seq <= previous)
      throw new ContractError("output.seq", "the page's outputs in stored order");
    previous = output.seq;
  }
  return {
    job: jobId,
    after_seq: after,
    outputs,
    next_after_seq: timestamp(envelope.next_after_seq, "next_after_seq"),
    has_more: boolean(envelope.has_more, "has_more"),
  } satisfies Api.Outputs;
}

export type ControlToolOutcomeKind = Api.ToolOutcomeKind;
export type ControlToolCall = Api.ToolCall;
export type ControlJobToolsPage = Api.Tools;

function toolOutcomeKind(value: unknown, path: string): ControlToolOutcomeKind | null {
  if (value === null || value === undefined) return null;
  if (
    value === "completed" ||
    value === "completed_withheld" ||
    value === "not_applied" ||
    value === "not_invoked" ||
    value === "uncertain"
  ) {
    return value;
  }
  throw new ContractError(path, "a tool outcome kind or null");
}

function toolCallRecord(value: unknown, path: string): ControlToolCall {
  const item = record(value, path);
  return {
    tool_call_id: string(item.tool_call_id, `${path}.tool_call_id`),
    request: integer(item.request, `${path}.request`),
    index: integer(item.index, `${path}.index`),
    name: string(item.name, `${path}.name`),
    decision: string(item.decision, `${path}.decision`),
    outcome_kind: toolOutcomeKind(item.outcome_kind, `${path}.outcome_kind`),
    arguments:
      item.arguments === null || item.arguments === undefined
        ? null
        : string(item.arguments, `${path}.arguments`),
    arguments_cut: boolean(item.arguments_cut, `${path}.arguments_cut`),
    result:
      item.result === null || item.result === undefined
        ? null
        : string(item.result, `${path}.result`),
    result_cut: boolean(item.result_cut, `${path}.result_cut`),
    ...(item.duration_ms === undefined || item.duration_ms === null
      ? {}
      : { duration_ms: integer(item.duration_ms, `${path}.duration_ms`) }),
  } satisfies Api.ToolCall;
}

/** One stored page of a Job's tool evidence. */
export function decodeJobTools(input: unknown, jobId: string): ControlJobToolsPage {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  if (envelope.job !== jobId) throw new ContractError("job", "the requested Job");
  const after =
    envelope.after === null || envelope.after === undefined
      ? null
      : string(envelope.after, "after");
  const nextAfter =
    envelope.next_after === null || envelope.next_after === undefined
      ? null
      : string(envelope.next_after, "next_after");
  const listed = envelope.tools;
  if (!Array.isArray(listed)) throw new ContractError("tools", "an array");
  const tools = listed.map((tool, index) => toolCallRecord(tool, `tools[${index}]`));
  return {
    job: jobId,
    after,
    tools,
    next_after: nextAfter,
    has_more: boolean(envelope.has_more, "has_more"),
  } satisfies Api.Tools;
}

/** What one `Idempotency-Key` created, from submission or lookup. */
export function decodeAdmission(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  const admission = record(envelope.admission, "admission");
  const status = admission.status;
  if (
    status !== "admitted" &&
    status !== "pending" &&
    status !== "uncertain" &&
    status !== "not_found"
  ) {
    throw new ContractError("admission.status", "a Control Plane admission status");
  }
  const job = admission.job === null ? null : identity(admission.job, "admission.job");
  if ((status === "admitted") !== (job !== null)) {
    throw new ContractError("admission.job", "a Job exactly when the key was admitted");
  }
  if (status === "admitted") {
    const admitted = {
      status,
      job: identity(admission.job, "admission.job"),
      conversation: identity(admission.conversation, "admission.conversation"),
      seq: integer(admission.seq, "admission.seq"),
      joined: boolean(admission.joined, "admission.joined"),
      model: string(admission.model, "admission.model"),
      kept: admission.kept === true,
    } satisfies Extract<Api.Admission, { status: "admitted" }>;
    return admitted;
  }
  const waiting = { status, job } satisfies Exclude<Api.Admission, { status: "admitted" }>;
  return {
    ...waiting,
    conversation: null,
    seq: null,
    joined: false,
    model: null,
    kept: admission.kept === true,
  };
}

/** What a stop request reached: never how the Job will settle. */
export function decodeStop(input: unknown, jobId: string) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  if (envelope.conversation !== jobId)
    throw new ContractError("conversation", "the requested conversation");
  const status = string(envelope.status, "status");
  return {
    conversation: jobId,
    status,
    record_seq: envelope.record_seq === null ? null : integer(envelope.record_seq, "record_seq"),
  } satisfies Api.Stop;
}

/** The browser's own session: its CSRF token while signed in. */
export function decodeSession(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  const session = record(envelope.session, "session");
  if (session.authenticated === false)
    return { authenticated: false as const } satisfies Api.SignedOut;
  if (session.authenticated !== true) throw new ContractError("session.authenticated", "a boolean");
  return {
    authenticated: true as const,
    csrf_token: string(session.csrf_token, "session.csrf_token"),
    expires_at_us: integer(session.expires_at_us, "session.expires_at_us"),
  } satisfies Api.SignedIn;
}

function jobInputRecord(input: unknown, path: string, jobId: string) {
  const value = record(input, path);
  const attachments = value.attachments;
  if (!Array.isArray(attachments)) throw new ContractError(`${path}.attachments`, "an array");
  let configuration: Api.Configuration | null = null;
  if (value.configuration !== null) {
    const frozen = record(value.configuration, `${path}.configuration`);
    configuration = {
      agent_profile:
        frozen.agent_profile === null
          ? null
          : identity(frozen.agent_profile, `${path}.configuration.agent_profile`),
      llm_profile: identity(frozen.llm_profile, `${path}.configuration.llm_profile`),
      model: string(frozen.model, `${path}.configuration.model`),
      context_window: optionalInteger(
        frozen.context_window,
        `${path}.configuration.context_window`,
      ),
    };
  }
  return {
    job: jobId,
    id: identity(value.id, `${path}.id`),
    objective: value.objective === null ? null : string(value.objective, `${path}.objective`),
    submitted_at_us: timestamp(value.submitted_at_us, `${path}.submitted_at_us`),
    attachments: attachments.map((attachment, index) =>
      identity(attachment, `${path}.attachments[${index}]`),
    ),
    configuration,
  } satisfies Api.Input & { job: string };
}

/**
 * A Job's frozen input as `GET /api/v1/jobs/{job}/input` reports it: the
 * objective as text (the Job record names its input only by identity), its
 * Attachments by identity, and the Profiles and model it was frozen with.
 */
export function decodeJobInput(input: unknown, jobId: string) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  if (envelope.job !== jobId) throw new ContractError("job", "the requested Job");
  return jobInputRecord(envelope.input, "input", jobId);
}

/**
 * Reconstructs the bounded conversation ending at `jobId` from
 * `GET /api/v1/jobs/{job}/conversation`, ordered from oldest turn to `jobId`.
 */
export function decodeConversationTurns(input: unknown, conversationId: string) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  if (envelope.conversation !== conversationId)
    throw new ContractError("conversation", "the requested conversation");
  const turns = listOf(envelope.turns, "turns", (entry, path) => {
    const turn = record(entry, path);
    const job = jobRecord(turn.job);
    const entries = listOf(turn.entries, `${path}.entries`, (input, path) => {
      const entry = record(input, path);
      const kind = oneOf(entry.kind, `${path}.kind`, [
        "user_message",
        "user_message_edit",
        "user_message_withdraw",
        "agent_message",
        "notice",
        "decision",
        "retry_of",
        "iteration",
        "response_format",
        "context_preparation",
        "context_use",
      ] as const);
      if (
        ![
          "user_message",
          "user_message_edit",
          "user_message_withdraw",
          "agent_message",
          "notice",
          "decision",
          "retry_of",
          "iteration",
          "response_format",
          "context_preparation",
          "context_use",
        ].includes(kind)
      )
        throw new ContractError(`${path}.kind`, "a turn entry kind");
      if (kind === "user_message_edit" || kind === "user_message_withdraw") {
        if (integer(entry.message, `${path}.message`) < 1)
          throw new ContractError(`${path}.message`, "a positive original message sequence");
        if (kind === "user_message_edit") {
          string(entry.text, `${path}.text`);
          listOf(entry.attachments, `${path}.attachments`, (id, path) => identity(id, path));
        }
      }
      return {
        seq: integer(entry.seq, `${path}.seq`),
        kind,
        message: entry.message === undefined ? null : integer(entry.message, `${path}.message`),
        text: entry.text === undefined ? null : string(entry.text, `${path}.text`),
        attachments:
          entry.attachments === undefined
            ? []
            : listOf(entry.attachments, `${path}.attachments`, (id, path) => identity(id, path)),
        at_us: integer(entry.at_us, `${path}.at_us`),
      } satisfies ProjectedEntry;
    });
    const userEntries = materializeUserEntries(entries).filter((entry) => !entry.withdrawn);
    const frozenInput = jobInputRecord(
      {
        id: job.input,
        objective: userEntries.length
          ? userEntries.map((entry) => entry.text ?? "").join("\n")
          : null,
        submitted_at_us: job.created_at_us,
        attachments: userEntries.flatMap((entry) => entry.attachments),
        configuration: turn.configuration,
      },
      `${path}.input`,
      job.id,
    );
    const outputs = listOf(turn.outputs, `${path}.outputs`, (output) => outputRecord(output));
    const compact =
      turn.compaction === undefined ? undefined : record(turn.compaction, `${path}.compaction`);
    const compaction = compact
      ? {
          artifact: identity(compact.artifact, `${path}.compaction.artifact`),
          kind: oneOf(compact.kind, `${path}.compaction.kind`, [
            "predictive",
            "rule_based",
            "manual",
          ] as const),
          at_us: integer(compact.at_us, `${path}.compaction.at_us`),
        }
      : undefined;
    return {
      job,
      entries,
      compaction,
      input: frozenInput,
      outputs,
      outputs_truncated: boolean(turn.outputs_truncated, `${path}.outputs_truncated`),
    } satisfies ProjectedTurn;
  });
  return {
    conversation: conversationId,
    turns,
    has_more: boolean(envelope.has_more, "has_more"),
    next_before:
      envelope.next_before === null ? null : identity(envelope.next_before, "next_before"),
  } satisfies Omit<Api.Turns, "turns"> & { turns: ProjectedTurn[] };
}
export function materializeUserEntries(entries: readonly ProjectedEntry[]) {
  const visible = new Map<
    number,
    ProjectedEntry & { withdrawn: boolean; history: { content: string; at: string }[] }
  >();
  for (const entry of entries) {
    if (entry.kind === "user_message" || entry.kind === "agent_message") {
      visible.set(entry.seq, { ...entry, withdrawn: false, history: [] });
    } else if (entry.kind === "user_message_edit" || entry.kind === "user_message_withdraw") {
      const original = entry.message === null ? undefined : visible.get(entry.message);
      if (
        !original ||
        original.kind !== "user_message" ||
        original.withdrawn ||
        original.seq >= entry.seq
      )
        throw new ContractError("entries.message", "an earlier queued user message");
      if (entry.kind === "user_message_edit") {
        original.history.push({
          content: original.text ?? "",
          at: new Date(entry.at_us / 1000).toISOString(),
        });
        original.text = entry.text;
        original.attachments = entry.attachments;
      } else {
        original.withdrawn = true;
      }
    }
  }
  return [...visible.values()];
}
export type ControlConversation = ReturnType<typeof decodeConversationTurns>;
export type ControlConversationTurn = ControlConversation["turns"][number];

export type ControlAgentCapability = Api.AgentCapability;
function agentCapabilities(value: unknown, path: string): ControlAgentCapability[] {
  const capabilities = listOf(value, path, (entry, itemPath) =>
    oneOf(entry, itemPath, ["chat", "coding", "decision"] as const),
  );
  if (!capabilities.length || new Set(capabilities).size !== capabilities.length)
    throw new ContractError(path, "unique Agent capabilities");
  return capabilities;
}

const CAPABILITIES = ["chat", "tools", "vision", "reasoning"] as const;
export type ControlCapability = (typeof CAPABILITIES)[number];

function listOf<T>(value: unknown, path: string, item: (entry: unknown, path: string) => T): T[] {
  if (!Array.isArray(value)) throw new ContractError(path, "an array");
  return value.map((entry, index) => item(entry, `${path}[${index}]`));
}

/**
 * One page of Agent Profiles from `GET /api/v1/agent-profiles`, each with the
 * LLM Profiles it may run with and the Executors that can run each now.
 */
export function decodeAgentProfiles(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  const nextBefore =
    envelope.next_before === null ? null : identity(envelope.next_before, "next_before");
  const hasMore = boolean(envelope.has_more, "has_more");
  if (hasMore !== (nextBefore !== null)) {
    throw new ContractError("next_before", "a cursor exactly when more Profiles remain");
  }
  return {
    agent_profiles: listOf(envelope.agent_profiles, "agent_profiles", (entry, path) => {
      const agent = record(entry, path);
      return {
        id: identity(agent.id, `${path}.id`),
        name: string(agent.name, `${path}.name`),
        capabilities: agentCapabilities(agent.capabilities, `${path}.capabilities`),
        llm_targets: listOf(agent.llm_targets, `${path}.llm_targets`, (target, targetPath) => {
          const llm = record(target, targetPath);
          return {
            id: identity(llm.id, `${targetPath}.id`),
            name: string(llm.name, `${targetPath}.name`),
            model: string(llm.model, `${targetPath}.model`),
            context_window: optionalInteger(llm.context_window, `${targetPath}.context_window`),
            reasoning_efforts:
              llm.reasoning_efforts == null
                ? null
                : listOf(llm.reasoning_efforts, `${targetPath}.reasoning_efforts`, string),
            default_effort: optionalString(
              record(llm.parameters, `${targetPath}.parameters`).reasoning_effort,
              `${targetPath}.parameters.reasoning_effort`,
            ),
            declared_capabilities: listOf(
              llm.declared_capabilities,
              `${targetPath}.declared_capabilities`,
              (capability, capabilityPath) => {
                const named = string(capability, capabilityPath);
                const known = CAPABILITIES.find((candidate) => candidate === named);
                if (!known) throw new ContractError(capabilityPath, "a declared model capability");
                return known;
              },
            ),
            executors: listOf(llm.executors, `${targetPath}.executors`, (executor, executorPath) =>
              identity(executor, executorPath),
            ),
          } satisfies Omit<Api.LlmTarget, "parameters"> & { default_effort: string | null };
        }),
      } satisfies ProjectedAgent;
    }),
    has_more: hasMore,
    next_before: nextBefore,
  } satisfies Omit<Api.AgentProfiles, "agent_profiles"> & { agent_profiles: ProjectedAgent[] };
}

function commandOwner(value: unknown, path: string): "user" | "decision" {
  if (value === "user" || value === "decision") return value;
  throw new ContractError(path, "user or decision");
}
function commandStatus(value: unknown, path: string): "allowed" | "review" | "blocked" {
  if (value === "allowed" || value === "review" || value === "blocked") return value;
  throw new ContractError(path, "allowed, review or blocked");
}
function commandManagement(value: unknown, path: string) {
  const policy = record(value, path);
  const confidence = policy.minimum_confidence;
  if (
    typeof confidence !== "number" ||
    !Number.isFinite(confidence) ||
    confidence < 0 ||
    confidence > 1
  )
    throw new ContractError(`${path}.minimum_confidence`, "a confidence between zero and one");
  return {
    enabled: boolean(policy.enabled, `${path}.enabled`),
    decision_profile:
      policy.decision_profile === null
        ? null
        : identity(policy.decision_profile, `${path}.decision_profile`),
    minimum_confidence: confidence,
  } satisfies Api.ProjectDecisionConfig;
}

function projectRecord(input: unknown, path: string) {
  const project = record(input, path);
  return {
    id: identity(project.id, `${path}.id`),
    name: string(project.name, `${path}.name`),
    description: optionalString(project.description, `${path}.description`),
    root: string(project.root, `${path}.root`),
    commands: listOf(project.commands, `${path}.commands`, (command, commandPath) => {
      const named = record(command, commandPath);
      return {
        name: string(named.name, `${commandPath}.name`),
        argv: listOf(named.argv, `${commandPath}.argv`, (argument, argumentPath) =>
          string(argument, argumentPath),
        ),
        owner: commandOwner(named.owner, `${commandPath}.owner`),
        status: commandStatus(named.status, `${commandPath}.status`),
        proposal_job:
          named.proposal_job === null
            ? null
            : identity(named.proposal_job, `${commandPath}.proposal_job`),
      } satisfies Api.ProjectCommand;
    }),
    decision:
      project.decision === null ? null : commandManagement(project.decision, `${path}.decision`),
    created_at_us: integer(project.created_at_us, `${path}.created_at_us`),
    updated_at_us: integer(project.updated_at_us, `${path}.updated_at_us`),
  } satisfies Api.Project;
}

/** One page of Projects from `GET /api/v1/projects`, newest first. */
export function decodeProjects(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  const nextBefore =
    envelope.next_before === null ? null : identity(envelope.next_before, "next_before");
  const hasMore = boolean(envelope.has_more, "has_more");
  if (hasMore !== (nextBefore !== null)) {
    throw new ContractError("next_before", "a cursor exactly when more Projects remain");
  }
  return {
    projects: listOf(envelope.projects, "projects", (entry, path) => projectRecord(entry, path)),
    has_more: hasMore,
    next_before: nextBefore,
  } satisfies Api.Projects;
}

export function decodeProjectAnswer(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  return projectRecord(envelope.project, "project");
}

export const POOL_OPERATIONS = ["llm_request", "decision"] as const;
export type ControlPoolOperation = (typeof POOL_OPERATIONS)[number];

function poolOperation(value: unknown, path: string): ControlPoolOperation {
  const named = string(value, path);
  const known = POOL_OPERATIONS.find((candidate) => candidate === named);
  if (!known) throw new ContractError(path, "llm_request or decision");
  return known;
}

function poolRecord(input: unknown, path: string) {
  const pool = record(input, path);
  const concurrencyLimit = integer(pool.concurrency_limit, `${path}.concurrency_limit`);
  if (concurrencyLimit < 1) {
    throw new ContractError(`${path}.concurrency_limit`, "an integer >= 1");
  }
  return {
    id: identity(pool.id, `${path}.id`),
    name: string(pool.name, `${path}.name`),
    concurrency_limit: concurrencyLimit,
    enabled: boolean(pool.enabled, `${path}.enabled`),
    // Absent means the Pool reported no operation information; an empty list
    // means it offers nothing. The two stay distinct.
    operations:
      pool.operations === undefined
        ? undefined
        : listOf(pool.operations, `${path}.operations`, poolOperation),
  } satisfies Omit<Api.Pool, "operations"> & { operations: ControlPoolOperation[] | undefined };
}

export function decodePools(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  return {
    pools: listOf(envelope.pools, "pools", (entry, path) => poolRecord(entry, path)),
  } satisfies Api.Pools;
}

export function decodePoolAnswer(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  return poolRecord(envelope.pool, "pool");
}

export function decodeRetry(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  return {
    api_version: CONTROL_API_VERSION,
    conversation: identity(envelope.conversation, "conversation"),
    turn: identity(envelope.turn, "turn"),
    seq: integer(envelope.seq, "seq"),
    joined: boolean(envelope.joined, "joined"),
  } satisfies Api.Envelope<Api.Append>;
}

export function decodeConversationEdit(input: unknown, root: string): string {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  const conversation = string(envelope.conversation, "conversation");
  if (conversation !== root) {
    throw new ContractError("conversation", `matching root ${root}`);
  }
  const answer = { conversation } satisfies Api.ConversationEdited;
  return answer.conversation;
}

export type ControlPools = ReturnType<typeof decodePools>;
export type ControlPool = ControlPools["pools"][number];
export type ControlProjects = ReturnType<typeof decodeProjects>;
export type ControlProject = ControlProjects["projects"][number];
export type ControlAgentProfiles = ReturnType<typeof decodeAgentProfiles>;
export type ControlAgentProfile = ControlAgentProfiles["agent_profiles"][number];

function optionalString(value: unknown, path: string): string | null {
  return value === null ? null : string(value, path);
}

function optionalFiniteNumber(value: unknown, path: string): number | null {
  if (value === null) return null;
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new ContractError(path, "a finite number or null");
  }
  return value;
}

function finiteNumber(value: unknown, path: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new ContractError(path, "a finite number");
  }
  return value;
}

function optionalInteger(value: unknown, path: string): number | null {
  return value === null || value === undefined ? null : integer(value, path);
}

/**
 * One page of Profile management data from `GET /api/v1/profiles`,
 * `POST /api/v1/profiles`, and `POST /api/v1/profiles/{profile}`, including
 * LLM Profiles, Agent Profiles, and the Provider projection of the LLM
 * Profiles on that page. Follow `next_before` until `has_more` is false for
 * the whole catalog, including `executor_protocols`. The client joins Model
 * compatibility after collecting every page. A mutation response names the
 * saved Profile in `profile_id` and returns the first page.
 */

export function decodeProfiles(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  const profileId =
    envelope.profile_id === undefined || envelope.profile_id === null
      ? null
      : identity(envelope.profile_id, "profile_id");
  return {
    profile_id: profileId,
    llm_profiles: listOf(envelope.llm_profiles, "llm_profiles", (entry, path) => {
      const llm = record(entry, path);
      const protocol = llmProtocol(llm.protocol, `${path}.protocol`);
      const params = record(llm.parameters, `${path}.parameters`);
      return {
        id: identity(llm.id, `${path}.id`),
        name: string(llm.name, `${path}.name`),
        model: string(llm.model, `${path}.model`),
        context_window: optionalInteger(llm.context_window, `${path}.context_window`),
        reasoning_efforts:
          llm.reasoning_efforts == null
            ? null
            : listOf(llm.reasoning_efforts, `${path}.reasoning_efforts`, string),
        protocol,
        endpoint: optionalString(llm.endpoint, `${path}.endpoint`),
        credential_ref: optionalString(llm.credential_ref, `${path}.credential_ref`),
        has_credential: boolean(llm.has_credential, `${path}.has_credential`),
        declared_capabilities: listOf(
          llm.declared_capabilities,
          `${path}.declared_capabilities`,
          (capability, capabilityPath) => {
            const named = string(capability, capabilityPath);
            const known = CAPABILITIES.find((candidate) => candidate === named);
            if (!known) throw new ContractError(capabilityPath, "a declared model capability");
            return known;
          },
        ),
        parameters: {
          temperature: optionalFiniteNumber(params.temperature, `${path}.parameters.temperature`),
          top_p: optionalFiniteNumber(params.top_p, `${path}.parameters.top_p`),
          max_tokens: timestamp(params.max_tokens, `${path}.parameters.max_tokens`),
          reasoning_effort: optionalString(
            params.reasoning_effort,
            `${path}.parameters.reasoning_effort`,
          ),
          max_completion_tokens: optionalInteger(
            params.max_completion_tokens,
            `${path}.parameters.max_completion_tokens`,
          ),
          presence_penalty: optionalFiniteNumber(
            params.presence_penalty,
            `${path}.parameters.presence_penalty`,
          ),
          frequency_penalty: optionalFiniteNumber(
            params.frequency_penalty,
            `${path}.parameters.frequency_penalty`,
          ),
        },
        executors: [] as string[],
        editable: boolean(llm.editable, `${path}.editable`),
        blocking_jobs: listOf(llm.blocking_jobs, `${path}.blocking_jobs`, (job, jobPath) =>
          identity(job, jobPath),
        ),
        more_blocking_jobs: boolean(llm.more_blocking_jobs, `${path}.more_blocking_jobs`),
        created_at_us: integer(llm.created_at_us, `${path}.created_at_us`),
        updated_at_us: integer(llm.updated_at_us, `${path}.updated_at_us`),
      } satisfies Api.LlmProfile & { executors: string[] };
    }),
    agent_profiles: listOf(envelope.agent_profiles, "agent_profiles", (entry, path) => {
      const agent = record(entry, path);
      return {
        id: identity(agent.id, `${path}.id`),
        name: string(agent.name, `${path}.name`),
        capabilities: agentCapabilities(agent.capabilities, `${path}.capabilities`),
        instructions: optionalString(agent.instructions, `${path}.instructions`),
        llm_targets: listOf(agent.llm_targets, `${path}.llm_targets`, (target, targetPath) =>
          identity(target, targetPath),
        ),
        compaction_policy:
          agent.compaction_policy === undefined
            ? undefined
            : decodeCompactionPolicy(agent.compaction_policy, `${path}.compaction_policy`),
        editable: boolean(agent.editable, `${path}.editable`),
        blocking_jobs: listOf(agent.blocking_jobs, `${path}.blocking_jobs`, (job, jobPath) =>
          identity(job, jobPath),
        ),
        more_blocking_jobs: boolean(agent.more_blocking_jobs, `${path}.more_blocking_jobs`),
        created_at_us: integer(agent.created_at_us, `${path}.created_at_us`),
        updated_at_us: integer(agent.updated_at_us, `${path}.updated_at_us`),
      } satisfies ProjectedManagedAgent;
    }),
    decision_profiles: listOf(
      envelope.decision_profiles ?? [],
      "decision_profiles",
      (input, path) => {
        const model = record(input, path);
        return {
          id: identity(model.id, `${path}.id`),
          edit_seq: integer(model.edit_seq, `${path}.edit_seq`),
          name: string(model.name, `${path}.name`),
          model: string(model.model, `${path}.model`),
          protocol: string(model.protocol, `${path}.protocol`),
          endpoint: optionalString(model.endpoint, `${path}.endpoint`),
          credential_ref: optionalString(model.credential_ref, `${path}.credential_ref`),
          has_credential: boolean(model.has_credential, `${path}.has_credential`),
          declared_capabilities: listOf(
            model.declared_capabilities,
            `${path}.declared_capabilities`,
            string,
          ),
          limits: decisionLimits(model.limits, `${path}.limits`),
          editable: boolean(model.editable, `${path}.editable`),
          created_at_us: integer(model.created_at_us, `${path}.created_at_us`),
          updated_at_us: integer(model.updated_at_us, `${path}.updated_at_us`),
        } satisfies Api.DecisionProfile;
      },
    ),
    decision_agent_profiles: listOf(
      envelope.decision_agent_profiles ?? [],
      "decision_agent_profiles",
      (input, path) => {
        const agent = record(input, path);
        return {
          id: identity(agent.id, `${path}.id`),
          name: string(agent.name, `${path}.name`),
          instructions: optionalString(agent.instructions, `${path}.instructions`),
          models: listOf(agent.models, `${path}.models`, identity),
          default_model:
            agent.default_model === null
              ? null
              : identity(agent.default_model, `${path}.default_model`),
          editable: boolean(agent.editable, `${path}.editable`),
          created_at_us: integer(agent.created_at_us, `${path}.created_at_us`),
          updated_at_us: integer(agent.updated_at_us, `${path}.updated_at_us`),
        } satisfies Api.DecisionAgentProfile;
      },
    ),
    executor_protocols: listOf(envelope.executor_protocols, "executor_protocols", (entry, path) => {
      const executor = record(entry, path);
      const protocol = llmProtocol(executor.protocol, `${path}.protocol`);
      return { id: identity(executor.id, `${path}.id`), protocol } satisfies Api.ProfileExecutor;
    }),
    providers: listOf(envelope.providers, "providers", (entry, path) => {
      const provider = record(entry, path);
      const protocol = llmProtocol(provider.protocol, `${path}.protocol`);
      return {
        name: string(provider.name, `${path}.name`),
        protocol,
        endpoint: optionalString(provider.endpoint, `${path}.endpoint`),
        credential_ref: optionalString(provider.credential_ref, `${path}.credential_ref`),
        has_credential: boolean(provider.has_credential, `${path}.has_credential`),
        profile_ids: listOf(provider.profile_ids, `${path}.profile_ids`, (id, idPath) =>
          identity(id, idPath),
        ),
        models: listOf(provider.models, `${path}.models`, (model, modelPath) =>
          string(model, modelPath),
        ),
      } satisfies Api.Provider;
    }),
    has_more: boolean(envelope.has_more, "has_more"),
    next_before:
      envelope.next_before === undefined || envelope.next_before === null
        ? null
        : identity(envelope.next_before, "next_before"),
  } satisfies Omit<Api.Profiles, "profile_id" | "agent_profiles"> & {
    profile_id: string | null;
    agent_profiles: ProjectedManagedAgent[];
  };
}

export type ControlProfilesSnapshot = ReturnType<typeof decodeProfiles>;

export function decodeProviderModels(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  return {
    models: listOf(envelope.models, "models", (entry, path) => {
      const model = record(entry, path);
      return {
        id: string(model.id, `${path}.id`),
        reasoning_efforts:
          model.reasoning_efforts == null
            ? null
            : listOf(model.reasoning_efforts, `${path}.reasoning_efforts`, string),
        context_window: optionalInteger(model.context_window, `${path}.context_window`),
      } satisfies Api.ProviderModel;
    }),
  } satisfies Api.ProviderModels;
}

export function decodeImportedModels(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  return {
    profile_ids: listOf(envelope.profile_ids, "profile_ids", identity),
    created: integer(envelope.created, "created"),
    existing: integer(envelope.existing, "existing"),
  } satisfies Api.ImportedModels;
}

export type ControlProviderModel = ReturnType<typeof decodeProviderModels>["models"][number];
export type ControlLlmProfile = ControlProfilesSnapshot["llm_profiles"][number];
export type ControlManagedAgentProfile = ControlProfilesSnapshot["agent_profiles"][number];

/**
 * An image Attachment as `POST /api/v1/attachments` and
 * `GET /api/v1/attachments/{attachment}` report it.
 */
export function decodeAttachment(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  const attachment = record(envelope.attachment, "attachment");
  return {
    id: identity(attachment.id, "attachment.id"),
    mime: string(attachment.mime, "attachment.mime"),
    byte_length: integer(attachment.byte_length, "attachment.byte_length"),
    sha256: string(attachment.sha256, "attachment.sha256"),
    display_name:
      attachment.display_name === null
        ? null
        : string(attachment.display_name, "attachment.display_name"),
    created_at_us: timestamp(attachment.created_at_us, "attachment.created_at_us"),
    available: typeof attachment.available === "boolean" ? attachment.available : true,
    data_url: typeof attachment.data_url === "string" ? attachment.data_url : undefined,
  } satisfies Omit<Api.Attachment, "created_at_us"> & {
    created_at_us: Api.Attachment["created_at_us"] | null;
    data_url: string | undefined;
  };
}

export type ControlAttachment = ReturnType<typeof decodeAttachment>;

function oneOf<T extends string>(value: unknown, path: string, choices: readonly T[]): T {
  const found = choices.find((choice) => choice === value);
  if (found === undefined) throw new ContractError(path, choices.join(", "));
  return found;
}

export function decodeChildren(input: unknown, conversationId: string) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  if (envelope.conversation !== conversationId)
    throw new ContractError("conversation", "the requested conversation");
  const children = listOf(envelope.children, "children", (input, path) => {
    const child = record(input, path);
    const state = oneOf(child.state, `${path}.state`, ["open", "closing", "settled"]);
    if (state !== "open" && state !== "closing" && state !== "settled")
      throw new ContractError(`${path}.state`, "a conversation state");
    const hold =
      child.on_hold === null
        ? null
        : record(record(child.on_hold, `${path}.on_hold`).by, `${path}.on_hold.by`);
    const by =
      hold === null
        ? null
        : oneOf(hold.kind, `${path}.on_hold.by.kind`, ["user", "agent", "loop", "auto"]);
    if (by !== null && !["user", "agent", "loop", "auto"].includes(by))
      throw new ContractError(`${path}.on_hold.by.kind`, "an actor");
    const recurrence =
      child.recurrence === null ? null : record(child.recurrence, `${path}.recurrence`);
    const phase =
      recurrence === null
        ? null
        : oneOf(recurrence.phase, `${path}.recurrence.phase`, ["active", "waiting_due", "halted"]);
    if (phase !== null && !["active", "waiting_due", "halted"].includes(phase))
      throw new ContractError(`${path}.recurrence.phase`, "a recurrence phase");
    const interval =
      recurrence === null ? null : integer(recurrence.interval_s, `${path}.recurrence.interval_s`);
    if (interval !== null && (interval < 60 || interval > 2592000))
      throw new ContractError(`${path}.recurrence.interval_s`, "60 to 2592000");
    return {
      job_state: oneOf(child.job_state, `${path}.job_state`, [
        "running",
        "queued",
        "waiting",
        "waiting_for_user",
        "completed",
        "failed",
        "cancelled",
        "unknown",
      ] as const),
      id: identity(child.id, `${path}.id`),
      title: string(child.title, `${path}.title`),
      ...(child.origin === undefined
        ? {}
        : { origin: oneOf(child.origin, `${path}.origin`, ["user", "agent", "system"] as const) }),
      agent: child.agent === null ? null : identity(child.agent, `${path}.agent`),
      state,
      on_hold:
        by === null
          ? null
          : {
              by:
                by === "agent"
                  ? {
                      kind: by,
                      conversation: identity(hold?.conversation, `${path}.on_hold.by.conversation`),
                    }
                  : { kind: by },
            },
      keep_open: boolean(child.keep_open, `${path}.keep_open`),
      recurrence:
        recurrence === null
          ? null
          : {
              interval_s: interval!,
              phase: phase!,
              due_us:
                recurrence.due_us === null
                  ? null
                  : integer(recurrence.due_us, `${path}.recurrence.due_us`),
            },
    } satisfies Api.Child;
  });
  return {
    conversation: conversationId,
    children,
    has_more: boolean(envelope.has_more, "has_more"),
    next_before:
      envelope.next_before === null ? null : identity(envelope.next_before, "next_before"),
  } satisfies Api.Children;
}
export type ControlChildConversation = ReturnType<typeof decodeChildren>["children"][number];

/** Every state a node of a Job tree may be in. */
export type ControlTreeState =
  | "open"
  | "closing"
  | "settled"
  | "completed"
  | "failed"
  | "cancelled"
  | "unknown"
  | "running"
  | "waiting_for_user"
  | "waiting"
  | "queued";

export type ControlTreeNode = Omit<
  Api.TreeNode,
  "state" | "origin" | "recurrence" | "depends_on" | "children"
> & {
  state: ControlTreeState;
  origin: "user" | "agent" | "system" | null;
  recurrence:
    | (Omit<Api.Recurrence, "phase"> & { phase: "active" | "waiting_due" | "halted" })
    | null;
  depends_on: (Omit<Api.Dependency, "condition" | "state"> & {
    condition: "settled" | "success";
    state: "waiting" | "satisfied" | "impossible";
  })[];
  children: ControlTreeNode[] | null;
};

const TREE_STATES = [
  "open",
  "closing",
  "settled",
  "completed",
  "failed",
  "cancelled",
  "unknown",
  "running",
  "waiting_for_user",
  "waiting",
  "queued",
] as const;

// Depth counts edges below the root, matching the server's tree query.
export const TREE_MAX_DEPTH = 4;

function treeNode(input: unknown, path: string, depth: number): ControlTreeNode {
  if (depth > TREE_MAX_DEPTH)
    throw new ContractError(path, `a tree of at most ${TREE_MAX_DEPTH} edges below the root`);
  const node = record(input, path);
  const model =
    node.model === null
      ? null
      : (() => {
          const frozen = record(node.model, `${path}.model`);
          return {
            llm_profile: identity(frozen.llm_profile, `${path}.model.llm_profile`),
            model: string(frozen.model, `${path}.model.model`),
          };
        })();
  const hold =
    node.on_hold === null
      ? null
      : record(record(node.on_hold, `${path}.on_hold`).by, `${path}.on_hold.by`);
  const by =
    hold === null
      ? null
      : oneOf(hold.kind, `${path}.on_hold.by.kind`, ["user", "agent", "loop", "auto"] as const);
  const origin =
    node.origin === null
      ? null
      : oneOf(node.origin, `${path}.origin`, ["user", "agent", "system"] as const);
  const recurrence =
    node.recurrence === null
      ? null
      : (() => {
          const row = record(node.recurrence, `${path}.recurrence`);
          return {
            interval_s: integer(row.interval_s, `${path}.recurrence.interval_s`),
            phase: oneOf(row.phase, `${path}.recurrence.phase`, [
              "active",
              "waiting_due",
              "halted",
            ] as const),
            due_us: row.due_us === null ? null : integer(row.due_us, `${path}.recurrence.due_us`),
          };
        })();
  return {
    id: identity(node.id, `${path}.id`),
    kind: oneOf(node.kind, `${path}.kind`, ["conversation", "turn"] as const),
    parent: node.parent === null ? null : identity(node.parent, `${path}.parent`),
    title: string(node.title, `${path}.title`),
    state: oneOf(node.state, `${path}.state`, TREE_STATES),
    created_at_us: integer(node.created_at_us, `${path}.created_at_us`),
    agent: node.agent === null ? null : identity(node.agent, `${path}.agent`),
    agent_name: node.agent_name === null ? null : string(node.agent_name, `${path}.agent_name`),
    model,
    on_hold:
      by === null
        ? null
        : {
            by:
              by === "agent"
                ? {
                    kind: by,
                    conversation: identity(hold?.conversation, `${path}.on_hold.by.conversation`),
                  }
                : { kind: by },
          },
    origin,
    keep_open: node.keep_open === null ? null : boolean(node.keep_open, `${path}.keep_open`),
    active_turn:
      node.active_turn === null ? null : identity(node.active_turn, `${path}.active_turn`),
    recurrence,
    conversation:
      node.conversation === null ? null : identity(node.conversation, `${path}.conversation`),
    previous: node.previous === null ? null : identity(node.previous, `${path}.previous`),
    depends_on: listOf(node.depends_on, `${path}.depends_on`, (entry, entryPath) => {
      const dependency = record(entry, entryPath);
      return {
        job: identity(dependency.job, `${entryPath}.job`),
        condition: oneOf(dependency.condition, `${entryPath}.condition`, [
          "settled",
          "success",
        ] as const),
        state: oneOf(dependency.state, `${entryPath}.state`, [
          "waiting",
          "satisfied",
          "impossible",
        ] as const),
      };
    }),
    children:
      node.children === null
        ? null
        : listOf(node.children, `${path}.children`, (entry, entryPath) =>
            treeNode(entry, entryPath, depth + 1),
          ),
    has_more_children: boolean(node.has_more_children, `${path}.has_more_children`),
    next_after: node.next_after === null ? null : identity(node.next_after, `${path}.next_after`),
  };
}

/**
 * The tree of the work below a Job, as `GET /api/v1/jobs/{job}/tree` reports
 * it: the ancestors above the root, and the nodes below it.
 */
export function decodeJobTree(input: unknown, job: string) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  if (envelope.job !== job) throw new ContractError("job", "the requested Job");
  const root = treeNode(envelope.root, "root", 0);
  if (root.id !== job) throw new ContractError("root.id", "the requested Job");
  return {
    job,
    ancestors: listOf(envelope.ancestors, "ancestors", (entry, path) => identity(entry, path)),
    root,
  } satisfies Api.Tree;
}

export type ControlApproval = Api.ApprovalDecision["approval"];

function approvalRef(input: unknown, path: string): ControlApproval {
  if (!Array.isArray(input) || input.length !== 3)
    throw new ContractError(path, "an approval triple");
  const turn = identity(input[0], `${path}.turn`);
  const request = integer(input[1], `${path}.request`);
  const index = integer(input[2], `${path}.index`);
  if (request === 0 || request > 0xffffffff || index > 0xffffffff)
    throw new ContractError(path, "a stored call reference");
  return [turn, request, index];
}

export type ControlConversationCard = Api.Card;

function approvalAssessment(input: unknown, path: string): Api.ApprovalAssessment | undefined {
  if (input === undefined) return undefined;
  const assessment = record(input, path);
  const state = oneOf(assessment.state, `${path}.state`, [
    "pending",
    "running",
    "completed",
    "failed",
    "unavailable",
  ] as const);
  const probability = (value: unknown, field: string) => {
    const number = finiteNumber(value, field);
    if (number < 0 || number > 1) throw new ContractError(field, "a number from 0 to 1");
    return number;
  };
  return {
    state,
    confidence:
      assessment.confidence === null
        ? null
        : probability(assessment.confidence, `${path}.confidence`),
    minimum_confidence: probability(assessment.minimum_confidence, `${path}.minimum_confidence`),
  };
}

export function decodeCards(input: unknown, conversationId: string) {
  const envelope = record(input, "response");
  if (envelope.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  if (envelope.conversation !== conversationId)
    throw new ContractError("conversation", `matching ${conversationId}`);
  const cards = listOf(envelope.cards, "cards", (input, path): ControlConversationCard => {
    const card = record(input, path);
    const common = {
      conversation: identity(card.conversation, `${path}.conversation`),
      parent: card.parent === null ? null : identity(card.parent, `${path}.parent`),
    };
    switch (card.kind) {
      case "dispatch":
        return {
          ...common,
          kind: card.kind,
          approval: approvalRef(card.approval, `${path}.approval`),
          tool: string(card.tool, `${path}.tool`),
          arguments: string(card.arguments, `${path}.arguments`),
        } satisfies Api.Card;
      case "approval":
        return {
          ...common,
          kind: card.kind,
          approval: approvalRef(card.approval, `${path}.approval`),
          tool: string(card.tool, `${path}.tool`),
          arguments: string(card.arguments, `${path}.arguments`),
          assessment: approvalAssessment(card.assessment, `${path}.assessment`),
        } satisfies Api.Card;
      case "question":
        return {
          ...common,
          kind: card.kind,
          approval: approvalRef(card.approval, `${path}.approval`),
          tool: string(card.tool, `${path}.tool`),
          question: string(card.question, `${path}.question`),
        } satisfies Api.Card;
      case "loop_hold":
        return { ...common, kind: card.kind } satisfies Api.Card;
      case "quarantine":
        return {
          ...common,
          kind: card.kind,
          turn: identity(card.turn, `${path}.turn`),
        } satisfies Api.Card;
      default:
        throw new ContractError(
          `${path}.kind`,
          "an approval, question, loop_hold or quarantine card",
        );
    }
  });
  return { conversation: conversationId, cards } satisfies Api.Cards;
}

export function decodeApprovalDecision(input: unknown, expected: ControlApproval) {
  const value = record(input, "response");
  if (value.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  const approval = approvalRef(value.approval, "approval");
  if (approval.some((part, index) => part !== expected[index]))
    throw new ContractError("approval", "the requested approval");
  const decision = value.decision;
  if (decision !== "approved" && decision !== "rejected" && decision !== "answered")
    throw new ContractError("decision", "an approval decision");
  return {
    approval,
    decision,
    turn: identity(value.turn, "turn"),
    seq: integer(value.seq, "seq"),
    joined: boolean(value.joined, "joined"),
  } satisfies Api.ApprovalDecision;
}

export function decodeReleaseSlot(input: unknown) {
  const value = record(input, "response");
  if (value.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", String(CONTROL_API_VERSION));
  if (value.status !== "released" && value.status !== "already_released")
    throw new ContractError("status", "a released slot");
  return { status: value.status } satisfies Api.ReleaseSlot;
}

export type ControlCompactionPolicy = Api.CompactionPolicy;
export type ControlPredictiveCompaction = Api.PredictiveCompaction;
export type ControlDecisionLimits = Api.DecisionLimits;

function decisionLimits(input: unknown, path: string): Api.DecisionLimits {
  const value = record(input, path);
  return {
    max_input_tokens_per_request: timestamp(
      value.max_input_tokens_per_request,
      `${path}.max_input_tokens_per_request`,
    ),
    max_output_tokens_per_request: timestamp(
      value.max_output_tokens_per_request,
      `${path}.max_output_tokens_per_request`,
    ),
    max_input_tokens_per_question: timestamp(
      value.max_input_tokens_per_question,
      `${path}.max_input_tokens_per_question`,
    ),
    max_output_tokens_per_question: timestamp(
      value.max_output_tokens_per_question,
      `${path}.max_output_tokens_per_question`,
    ),
  };
}

export function decodeCompactionPolicy(input: unknown, path = "policy"): Api.CompactionPolicy {
  const value = record(input, path),
    mode = record(value.mode, `${path}.mode`);
  let decodedMode: Api.CompactionMode;
  if (mode.kind === "off") decodedMode = { kind: "off" };
  else if (
    mode.kind === "auto" &&
    (mode.preset === "conservative" || mode.preset === "balanced" || mode.preset === "aggressive")
  )
    decodedMode = { kind: "auto", preset: mode.preset };
  else if (mode.kind === "custom") {
    const custom = record(mode.policy, `${path}.mode.policy`);
    decodedMode = {
      kind: "custom",
      policy: {
        trigger_tokens: integer(custom.trigger_tokens, `${path}.trigger_tokens`),
        target_tokens: integer(custom.target_tokens, `${path}.target_tokens`),
        reserved_output_tokens: integer(
          custom.reserved_output_tokens,
          `${path}.reserved_output_tokens`,
        ),
        minimum_source_tokens: integer(
          custom.minimum_source_tokens,
          `${path}.minimum_source_tokens`,
        ),
        minimum_savings_tokens: integer(
          custom.minimum_savings_tokens,
          `${path}.minimum_savings_tokens`,
        ),
        preserve_recent_turns: integer(
          custom.preserve_recent_turns,
          `${path}.preserve_recent_turns`,
        ),
        cooldown_seconds: integer(custom.cooldown_seconds, `${path}.cooldown_seconds`),
      },
    };
  } else throw new ContractError(`${path}.mode`, "off, an automatic preset, or custom");
  let predictive: Api.PredictiveCompaction | null = null;
  if (value.predictive !== null) {
    const prediction = record(value.predictive, `${path}.predictive`);
    const mode = prediction.mode ?? "shadow";
    if (mode !== "shadow" && mode !== "prepare")
      throw new ContractError(`${path}.predictive.mode`, "shadow or prepare");
    const pin =
      prediction.acceptance === null || prediction.acceptance === undefined
        ? null
        : record(prediction.acceptance, `${path}.predictive.acceptance`);
    predictive = {
      decision_agent: identity(prediction.decision_agent, `${path}.decision_agent`),
      minimum_interval_seconds: integer(
        prediction.minimum_interval_seconds,
        `${path}.minimum_interval_seconds`,
      ),
      max_preparations: integer(prediction.max_preparations, `${path}.max_preparations`),
      max_prediction_tokens: integer(
        prediction.max_prediction_tokens,
        `${path}.max_prediction_tokens`,
      ),
      max_preparation_tokens: integer(
        prediction.max_preparation_tokens,
        `${path}.max_preparation_tokens`,
      ),
      mode,
      acceptance: pin
        ? {
            model: identity(pin.model, `${path}.acceptance.model`),
            model_edit_seq: integer(pin.model_edit_seq, `${path}.acceptance.model_edit_seq`),
            reported_model: string(pin.reported_model, `${path}.acceptance.reported_model`),
            confidence_semantics: string(
              pin.confidence_semantics,
              `${path}.acceptance.confidence_semantics`,
            ),
            minimum_probability_millionths: integer(
              pin.minimum_probability_millionths,
              `${path}.acceptance.minimum_probability_millionths`,
            ),
            minimum_confidence_millionths: integer(
              pin.minimum_confidence_millionths,
              `${path}.acceptance.minimum_confidence_millionths`,
            ),
          }
        : null,
      max_preparations_per_hour: integer(
        prediction.max_preparations_per_hour ?? 0,
        `${path}.max_preparations_per_hour`,
      ),
      max_tokens_per_hour: integer(
        prediction.max_tokens_per_hour ?? 0,
        `${path}.max_tokens_per_hour`,
      ),
      max_requests_per_hour: integer(
        prediction.max_requests_per_hour ?? 0,
        `${path}.max_requests_per_hour`,
      ),
    };
  }
  return {
    mode: decodedMode,
    summarization_model:
      value.summarization_model === null
        ? null
        : identity(value.summarization_model, `${path}.summarization_model`),
    predictive,
  };
}

export function decodeActiveSystemSettings(input: unknown) {
  const value = record(input, "response");
  if (value.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", "the supported API version");
  return {
    path: string(value.path, "path"),
    content: string(value.content, "content"),
  } satisfies Api.ActiveSystemSettings;
}

export type ControlGlobalCompactionSettings = Api.GlobalCompactionSettings;
export function decodeGlobalCompactionSettings(input: unknown): ControlGlobalCompactionSettings {
  const value = record(input, "response");
  if (value.api_version !== CONTROL_API_VERSION)
    throw new ContractError("api_version", "the supported API version");
  return {
    revision: integer(value.revision, "revision"),
    prediction_enabled: boolean(value.prediction_enabled, "prediction_enabled"),
    policy: decodeCompactionPolicy(value.policy),
  } satisfies Api.GlobalCompactionSettings;
}

export function decodeCompaction(input: unknown, conversation: string) {
  const value = record(input, "response");
  if (value.api_version !== CONTROL_API_VERSION || value.conversation !== conversation)
    throw new ContractError("conversation", "the requested conversation");
  return {
    conversation,
    status: string(value.status, "status"),
    job: value.job === null ? null : identity(value.job, "job"),
    system_conversation:
      value.system_conversation === null
        ? null
        : identity(value.system_conversation, "system_conversation"),
    through: value.through === null ? null : identity(value.through, "through"),
    subsequent: boolean(value.subsequent, "subsequent"),
    reason: optionalString(value.reason, "reason"),
  } satisfies Api.Compaction;
}

function contextBoundary(input: unknown, path: string): Api.ContextBoundary {
  const value = record(input, path);
  return {
    turn: identity(value.turn, `${path}.turn`),
    input_through: integer(value.input_through, `${path}.input_through`),
    output_through: integer(value.output_through, `${path}.output_through`),
    fingerprint: string(value.fingerprint, `${path}.fingerprint`),
  };
}
/**
 * The readable shape of one Decision answer. Provider confidence travels with
 * the semantics identifier it was reported under; it is not calibrated
 * correctness.
 */
function decisionConfidence(input: unknown, path: string) {
  if (input === undefined || input === null) return undefined;
  const confidence = record(input, path);
  return {
    value: finiteNumber(confidence.value, `${path}.value`),
    semantics: string(confidence.semantics, `${path}.semantics`),
  };
}

function decisionAnswer(input: unknown, path: string) {
  const answer = record(input, path);
  if (answer.kind === "choice") {
    return {
      kind: "choice" as const,
      selected: string(answer.selected, `${path}.selected`),
      distribution:
        answer.distribution === undefined || answer.distribution === null
          ? undefined
          : Object.fromEntries(
              Object.entries(record(answer.distribution, `${path}.distribution`)).map(
                ([key, value]) => [key, finiteNumber(value, `${path}.distribution.${key}`)],
              ),
            ),
      confidence: decisionConfidence(answer.confidence, `${path}.confidence`),
    };
  }
  if (answer.kind === "binary_probability") {
    return {
      kind: "binary_probability" as const,
      true_probability: finiteNumber(answer.true_probability, `${path}.true_probability`),
    };
  }
  if (answer.kind === "ordinal_score") {
    return {
      kind: "ordinal_score" as const,
      expectation: finiteNumber(answer.expectation, `${path}.expectation`),
      distribution:
        answer.distribution === undefined || answer.distribution === null
          ? undefined
          : listOf(answer.distribution, `${path}.distribution`, (entry, entryPath) =>
              finiteNumber(entry, entryPath),
            ),
      confidence: decisionConfidence(answer.confidence, `${path}.confidence`),
    };
  }
  throw new ContractError(`${path}.kind`, "choice, binary_probability or ordinal_score");
}

function predictionResult(input: unknown, path: string) {
  const value = record(input, path);
  const outcome = record(value.outcome, `${path}.outcome`);
  const decoded =
    outcome.status === "answer"
      ? {
          kind: "answer" as const,
          answer: decisionAnswer(outcome.answer, `${path}.outcome.answer`),
        }
      : outcome.status === "uncertain"
        ? { kind: "uncertain" as const, reason: string(outcome.reason, `${path}.outcome.reason`) }
        : outcome.status === "unusable"
          ? { kind: "unusable" as const, reason: string(outcome.reason, `${path}.outcome.reason`) }
          : null;
  if (!decoded) throw new ContractError(`${path}.outcome.status`, "answer, uncertain or unusable");
  return {
    schema_version: integer(value.schema_version, `${path}.schema_version`),
    outcome: decoded,
    requested_model: string(value.requested_model, `${path}.requested_model`),
    reported_model: optionalString(value.reported_model, `${path}.reported_model`),
    model_version: optionalString(value.model_version, `${path}.model_version`),
  };
}

export type ControlDecisionAnswer = ReturnType<typeof decisionAnswer>;
export type ControlPredictionResult = ReturnType<typeof predictionResult>;

export function decodeContextStatus(input: unknown, conversation: string) {
  const value = record(input, "response");
  if (value.api_version !== CONTROL_API_VERSION || value.conversation !== conversation)
    throw new ContractError("conversation", "the requested conversation");
  const model =
    value.foreground_model === null ? null : record(value.foreground_model, "foreground_model");
  const use =
    value.last_context_use === null ? null : record(value.last_context_use, "last_context_use");
  return {
    conversation,
    mode: string(value.mode, "mode"),
    summarizer: optionalString(value.summarizer, "summarizer"),
    predictive: boolean(value.predictive, "predictive"),
    predictive_unavailable: optionalString(value.predictive_unavailable, "predictive_unavailable"),
    revision: integer(value.revision, "revision"),
    uncalibrated: boolean(value.uncalibrated, "uncalibrated"),
    policy: decodeCompactionPolicy(value.policy),
    policy_override:
      value.policy_override === null
        ? null
        : decodeCompactionPolicy(value.policy_override, "policy_override"),
    policy_source: string(value.policy_source, "policy_source"),
    predictor: optionalString(value.predictor, "predictor"),
    predictor_model: optionalString(value.predictor_model, "predictor_model"),
    prediction_mode: string(value.prediction_mode, "prediction_mode"),
    predictive_preparation_enabled: boolean(
      value.predictive_preparation_enabled,
      "predictive_preparation_enabled",
    ),
    foreground_model: model
      ? {
          profile: identity(model.profile, "foreground_model.profile"),
          edit_seq: integer(model.edit_seq, "foreground_model.edit_seq"),
          model: string(model.model, "foreground_model.model"),
          capacity: timestamp(model.capacity, "foreground_model.capacity"),
          capacity_source: string(model.capacity_source, "foreground_model.capacity_source"),
          reserved_output_tokens: integer(
            model.reserved_output_tokens,
            "foreground_model.reserved_output_tokens",
          ),
        }
      : null,
    estimated_tokens: timestamp(value.estimated_tokens, "estimated_tokens"),
    estimate_method: string(value.estimate_method, "estimate_method"),
    recent_growth_tokens: integer(value.recent_growth_tokens, "recent_growth_tokens"),
    steer_blocked_by:
      value.steer_blocked_by === undefined
        ? undefined
        : oneOf(value.steer_blocked_by, "steer_blocked_by", ["rule_based", "manual"] as const),
    last_context_use: use
      ? {
          turn: identity(use.turn, "last_context_use.turn"),
          seq: integer(use.seq, "last_context_use.seq"),
          artifact:
            use.artifact === null ? null : identity(use.artifact, "last_context_use.artifact"),
        }
      : null,
    work: listOf(value.work, "work", (input, path) => {
      const work = record(input, path),
        base = {
          job: identity(work.job, `${path}.job`),
          conversation: identity(work.conversation, `${path}.conversation`),
          state: string(work.state, `${path}.state`),
        };
      if (work.kind === "summarization") {
        const source = record(work.source, `${path}.source`);
        return {
          ...base,
          kind: "summarization",
          summarizer: identity(work.summarizer, `${path}.summarizer`),
          failure: optionalString(work.failure, `${path}.failure`),
          automatic: boolean(work.automatic, `${path}.automatic`),
          stage: string(work.stage, `${path}.stage`),
          chain: work.chain === null ? null : identity(work.chain, `${path}.chain`),
          prediction:
            work.prediction === null ? null : identity(work.prediction, `${path}.prediction`),
          reserved_tokens: integer(work.reserved_tokens, `${path}.reserved_tokens`),
          reserved_requests: integer(work.reserved_requests, `${path}.reserved_requests`),
          source: {
            digest: string(source.digest, `${path}.source.digest`),
            first: contextBoundary(source.first, `${path}.source.first`),
            through: contextBoundary(source.through, `${path}.source.through`),
            turn_count: integer(source.turn_count, `${path}.source.turn_count`),
          },
        } satisfies Extract<Api.ContextWork, { kind: "summarization" }>;
      }
      if (work.kind === "prediction")
        return {
          ...base,
          kind: "prediction",
          predictor: identity(work.predictor, `${path}.predictor`),
          model: identity(work.model, `${path}.model`),
          activity_turn: identity(work.activity_turn, `${path}.activity_turn`),
          policy_revision: integer(work.policy_revision, `${path}.policy_revision`),
          policy: decodeCompactionPolicy(work.policy, `${path}.policy`),
          max_tokens: integer(work.max_tokens, `${path}.max_tokens`),
          forecast_tokens: timestamp(work.forecast_tokens, `${path}.forecast_tokens`),
          result:
            work.result === undefined || work.result === null
              ? null
              : predictionResult(work.result, `${path}.result`),
        } satisfies Omit<Extract<Api.ContextWork, { kind: "prediction" }>, "result"> & {
          result: ControlPredictionResult | null;
        };
      throw new ContractError(`${path}.kind`, "summarization or prediction");
    }),
    has_more: boolean(value.has_more, "has_more"),
    next_before: value.next_before === null ? null : identity(value.next_before, "next_before"),
  } satisfies Omit<Api.ContextStatus, "work"> & {
    work: Array<
      | Extract<Api.ContextWork, { kind: "summarization" }>
      | (Omit<Extract<Api.ContextWork, { kind: "prediction" }>, "result"> & {
          result: ControlPredictionResult | null;
        })
    >;
  };
}
