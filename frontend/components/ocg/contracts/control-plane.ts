import { ContractError, isRecord } from "./decode";

export { ContractError } from "./decode";

function record(value: unknown, path: string): Record<string, unknown> {
  if (!isRecord(value)) throw new ContractError(path, "an object");
  return value;
}

function string(value: unknown, path: string): string {
  if (typeof value !== "string") throw new ContractError(path, "a string");
  return value;
}

function integer(value: unknown, path: string): number {
  // Fail closed rather than round Rust i64 cursors into another output.
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new ContractError(path, "a non-negative safe integer");
  }
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
  if (state !== "unsettled" && state !== "completed" && state !== "failed" &&
    state !== "cancelled" && state !== "unknown") {
    throw new ContractError("settlement.state", "a Control Plane settlement state");
  }
  const at = timestamp(value.settled_at_us, "settlement.settled_at_us");
  if ((state === "unsettled") !== (at === null)) {
    throw new ContractError("settlement.settled_at_us", "a timestamp only for an outcome");
  }
  return { state, settled_at_us: at };
}

export function decodeControlError(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
  const value = record(envelope.error, "error");
  return { code: string(value.code, "error.code"), message: string(value.message, "error.message") };
}

function identity(value: unknown, path: string, prefix: string): string {
  const text = string(value, path);
  const number = text.slice(prefix.length);
  if (!text.startsWith(prefix) || !/^(0|[1-9][0-9]*)$/.test(number)) {
    throw new ContractError(path, `a canonical ${prefix.slice(0, -1)} identity`);
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
    id: identity(job.id, "job.id", "job:"),
    kind: job.kind === "conversation" ? "conversation" as const : job.kind === "turn" ? "turn" as const : (() => { throw new ContractError("job.kind", "conversation or turn"); })(),
    conversation: job.conversation === null ? null : identity(job.conversation, "job.conversation", "job:"),
    name: job.name === null ? null : string(job.name, "job.name"),
    title: job.title === undefined || job.title === null ? null : string(job.title, "job.title"),
    is_chat: typeof job.is_chat === "boolean" ? job.is_chat : false,
    execution_mode: mode,
    input: string(job.input, "job.input"),
    previous: job.previous === undefined || job.previous === null
      ? null
      : identity(job.previous, "job.previous", "job:"),
    assigned_executor: job.assigned_executor === null
      ? null
      : identity(job.assigned_executor, "job.assigned_executor", "executor:"),
    created_at_us: integer(job.created_at_us, "job.created_at_us"),
    updated_at_us: integer(job.updated_at_us, "job.updated_at_us"),
    started_at_us: timestamp(job.started_at_us, "job.started_at_us"),
    paused_at_us: timestamp(job.paused_at_us, "job.paused_at_us"),
    sterilized_at_us: timestamp(job.sterilized_at_us, "job.sterilized_at_us"),
    settlement: settlement(job.settlement),
    stop_confirmed_at_us: timestamp(job.stop_confirmed_at_us, "job.stop_confirmed_at_us"),
  };
}

function outputRecord(input: unknown) {
  const output = record(input, "output");
  const seq = integer(output.seq, "output.seq");
  if (seq === 0) throw new ContractError("output.seq", "a positive stored sequence");
  const kind = output.kind;
  if (kind !== "progress" && kind !== "evaluation" && kind !== "result" && kind !== "reasoning") {
    throw new ContractError("output.kind", "progress, evaluation, result or reasoning");
  }
  if (output.goal_reached !== null && typeof output.goal_reached !== "boolean") {
    throw new ContractError("output.goal_reached", "a boolean or null");
  }
  if (!("value" in output)) throw new ContractError("output.value", "a JSON value");
  return {
    id: string(output.id, "output.id"), seq, kind,
    summary: output.summary === null ? null : string(output.summary, "output.summary"),
    code: output.code === null ? null : string(output.code, "output.code"),
    goal_reached: output.goal_reached,
    value: output.value,
    created_at_us: integer(output.created_at_us, "output.created_at_us"),
  };
}

export function decodeJobEvent(name: string, input: unknown, jobId: string, id: string | null) {
  const value = record(input, "event");
  if (value.api_version !== 1) throw new ContractError("api_version", "1");
  if (name !== "output" && id !== null) throw new ContractError("id", "an id only on output");
  if (name === "stream_error") {
    return { type: "stream_error" as const, error: decodeControlError(value) };
  }
  if (name === "job") {
    const job = jobRecord(value.job);
    if (job.id !== jobId) throw new ContractError("job.id", "the requested Job");
    return { type: "job" as const, job: { ...job, id: jobId } };
  }
  if (value.job !== jobId) throw new ContractError("job", "the requested Job");
  if (name === "settlement") {
    const outcome = settlement(value.settlement);
    const stopped = timestamp(value.stop_confirmed_at_us, "stop_confirmed_at_us");
    if (outcome.state === "unsettled" || (outcome.state === "unknown" && stopped === null)) {
      throw new ContractError("settlement", "a concluded outcome, with stop confirmation for unknown");
    }
    return { type: "settlement" as const, job: jobId, settlement: outcome, stop_confirmed_at_us: stopped };
  }
  if (name !== "output") throw new ContractError("event", "job, output, settlement or stream_error");
  const output = outputRecord(value.output);
  if (id !== String(output.seq)) throw new ContractError("id", "the output's positive stored seq");
  return { type: "output" as const, job: jobId, output };
}

export type ControlJobEvent = ReturnType<typeof decodeJobEvent>;
export type ControlJob = ReturnType<typeof jobRecord>;
export type ControlOutput = ReturnType<typeof outputRecord>;

/** What the host establishes about itself and its database, now. */
export function decodeHealth(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
  const host = string(envelope.host, "host");
  const database = record(envelope.database, "database");
  const status = database.status;
  if (status !== "open" && status !== "protected" && status !== "shutting_down" &&
    status !== "stopped" && status !== "closed") {
    throw new ContractError("database.status", "a Control Plane database status");
  }
  const quarantined = listOf(envelope.quarantined, "quarantined", (entry, path) => {
    const item = record(entry, path);
    return {
      job: identity(item.job, `${path}.job`, "job:"),
      pool: item.pool === null ? null : identity(item.pool, `${path}.pool`, "execution_pool:"),
      since: integer(item.since, `${path}.since`),
    };
  });
  return {
    host,
    database: {
      status,
      accepting_commands: boolean(database.accepting_commands, "database.accepting_commands"),
      readable: boolean(database.readable, "database.readable"),
    },
    quarantined,
  };
}

/** The Job's persisted record, as `GET /api/v1/jobs/{job}` reports it. */
export function decodeJob(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
  return jobRecord(envelope.job);
}

export function decodeJobs(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
  return {
    jobs: listOf(envelope.jobs, "jobs", (job) => jobRecord(job)),
    next_before: envelope.next_before === null ? null : identity(envelope.next_before, "next_before", "job:"),
    has_more: boolean(envelope.has_more, "has_more"),
  };
}

function conversationRecord(input: unknown) {
  const c = record(input, "conversation");
  return {
    root_job: identity(c.root_job, "conversation.root_job", "job:"),
    tip_job: identity(c.tip_job, "conversation.tip_job", "job:"),
    agent: c.agent === null ? null : identity(c.agent, "conversation.agent", "profile:"),
    title: string(c.title, "conversation.title"),
    state: c.state === "open" || c.state === "closing" || c.state === "settled" ? c.state : (() => { throw new ContractError("conversation.state", "open, closing or settled"); })(),
    on_hold: c.on_hold === null ? null : { by: record(record(c.on_hold, "conversation.on_hold").by, "conversation.on_hold.by") },
    project: c.project === null ? null : identity(c.project, "conversation.project", "project:"),
    tip_settlement_state: string(c.tip_settlement_state, "conversation.tip_settlement_state"),
    tip_created_at_us: integer(c.tip_created_at_us, "conversation.tip_created_at_us"),
  };
}

export function decodeConversations(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
  return {
    conversations: listOf(envelope.conversations, "conversations", (conversation) => conversationRecord(conversation)),
    next_before: envelope.next_before === null || envelope.next_before === undefined
      ? null
      : identity(envelope.next_before, "next_before", "job:"),
    has_more: boolean(envelope.has_more, "has_more"),
  };
}

function observationPage(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
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
      job: identity(value.job, `${path}.job`, "job:"),
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
      if (value.finality !== null && value.finality !== "final" && value.finality !== "cumulative") {
        throw new ContractError(`${path}.finality`, "final, cumulative or null");
      }
      if (value.eligibility !== null && value.eligibility !== "eligible" && value.eligibility !== "excluded") {
        throw new ContractError(`${path}.eligibility`, "eligible, excluded or null");
      }
      return {
        ...common,
        llm_profile: value.llm_profile === null ? null : identity(value.llm_profile, `${path}.llm_profile`, "profile:"),
        input_tokens: timestamp(value.input_tokens, `${path}.input_tokens`),
        output_tokens: timestamp(value.output_tokens, `${path}.output_tokens`),
        cache_read_tokens: timestamp(value.cache_read_tokens, `${path}.cache_read_tokens`),
        cache_write_tokens: timestamp(value.cache_write_tokens, `${path}.cache_write_tokens`),
        reasoning_tokens: timestamp(value.reasoning_tokens, `${path}.reasoning_tokens`),
        ttfb_ms: timestamp(value.ttfb_ms, `${path}.ttfb_ms`),
        finished_in_ms: timestamp(value.finished_in_ms, `${path}.finished_in_ms`),
        finality: value.finality,
        eligibility: value.eligibility,
      };
    }),
  };
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
      };
    }),
  };
}

export function decodeLogs(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
  return {
    file: optionalString(envelope.file, "file"),
    files: listOf(envelope.files, "files", string),
    lines: listOf(envelope.lines, "lines", string),
    truncated: boolean(envelope.truncated, "truncated"),
    files_truncated: boolean(envelope.files_truncated, "files_truncated"),
  };
}

/** One stored page of a Job's outputs, in `seq` order. */
export function decodeOutputsPage(input: unknown, jobId: string) {
  const envelope = record(input, "response");
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
  if (envelope.job !== jobId) throw new ContractError("job", "the requested Job");
  const after = timestamp(envelope.after_seq, "after_seq");
  const listed = envelope.outputs;
  if (!Array.isArray(listed)) throw new ContractError("outputs", "an array");
  const outputs = listed.map((output) => outputRecord(output));
  let previous = after ?? 0;
  for (const output of outputs) {
    // The stored order is the processing order; a page that breaks it is
    // refused rather than reordered.
    if (output.seq <= previous) throw new ContractError("output.seq", "the page's outputs in stored order");
    previous = output.seq;
  }
  return {
    job: jobId,
    after_seq: after,
    outputs,
    next_after_seq: timestamp(envelope.next_after_seq, "next_after_seq"),
    has_more: boolean(envelope.has_more, "has_more"),
  };
}

export type ControlToolOutcomeKind =
  | "completed"
  | "completed_withheld"
  | "not_applied"
  | "not_invoked"
  | "uncertain";

export type ControlToolCall = {
  tool_call_id: string;
  request: number;
  index: number;
  name: string;
  decision: string;
  outcome_kind: ControlToolOutcomeKind | null;
  arguments: string | null;
  arguments_cut: boolean;
  result: string | null;
  result_cut: boolean;
};

export type ControlJobToolsPage = {
  job: string;
  after: string | null;
  tools: ControlToolCall[];
  next_after: string | null;
  has_more: boolean;
};

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
  };
}

/** One stored page of a Job's tool evidence. */
export function decodeJobTools(input: unknown, jobId: string): ControlJobToolsPage {
  const envelope = record(input, "response");
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
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
  };
}

/** What one `Idempotency-Key` created, from submission or lookup. */
export function decodeAdmission(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
  const admission = record(envelope.admission, "admission");
  const status = admission.status;
  if (status !== "admitted" && status !== "pending" && status !== "uncertain" && status !== "not_found") {
    throw new ContractError("admission.status", "a Control Plane admission status");
  }
  const job = admission.job === null ? null : identity(admission.job, "admission.job", "job:");
  if ((status === "admitted") !== (job !== null)) {
    throw new ContractError("admission.job", "a Job exactly when the key was admitted");
  }
  return { status, job, conversation: status === "admitted" ? identity(admission.conversation, "admission.conversation", "job:") : null, seq: status === "admitted" ? integer(admission.seq, "admission.seq") : null, joined: status === "admitted" ? boolean(admission.joined, "admission.joined") : false, model: status === "admitted" ? string(admission.model, "admission.model") : null, kept: admission.kept === true };
}

/** What a stop request reached: never how the Job will settle. */
export function decodeStop(input: unknown, jobId: string) {
  const envelope = record(input, "response");
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
  if (envelope.conversation !== jobId) throw new ContractError("conversation", "the requested conversation");
  const status = string(envelope.status, "status");
  return { conversation: jobId, status, record_seq: envelope.record_seq === null ? null : integer(envelope.record_seq, "record_seq") };
}

/** The browser's own session: its CSRF token while signed in. */
export function decodeSession(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
  const session = record(envelope.session, "session");
  if (session.authenticated === false) return { authenticated: false as const };
  if (session.authenticated !== true) throw new ContractError("session.authenticated", "a boolean");
  return {
    authenticated: true as const,
    csrf_token: string(session.csrf_token, "session.csrf_token"),
    expires_at_us: integer(session.expires_at_us, "session.expires_at_us"),
  };
}

function jobInputRecord(input: unknown, path: string, jobId: string) {
  const value = record(input, path);
  const attachments = value.attachments;
  if (!Array.isArray(attachments)) throw new ContractError(`${path}.attachments`, "an array");
  let configuration: { agent_profile: string | null; llm_profile: string; model: string; context_window: number | null } | null = null;
  if (value.configuration !== null) {
    const frozen = record(value.configuration, `${path}.configuration`);
    configuration = {
      agent_profile: frozen.agent_profile === null
        ? null
        : identity(frozen.agent_profile, `${path}.configuration.agent_profile`, "profile:"),
      llm_profile: identity(frozen.llm_profile, `${path}.configuration.llm_profile`, "profile:"),
      model: string(frozen.model, `${path}.configuration.model`),
      context_window: optionalInteger(frozen.context_window, `${path}.configuration.context_window`),
    };
  }
  return {
    job: jobId,
    id: identity(value.id, `${path}.id`, "job_input:"),
    objective: value.objective === null ? null : string(value.objective, `${path}.objective`),
    submitted_at_us: timestamp(value.submitted_at_us, `${path}.submitted_at_us`),
    attachments: attachments.map((attachment, index) =>
      identity(attachment, `${path}.attachments[${index}]`, "attachment:")),
    configuration,
  };
}

/**
 * A Job's frozen input as `GET /api/v1/jobs/{job}/input` reports it: the
 * objective as text (the Job record names its input only by identity), its
 * Attachments by identity, and the Profiles and model it was frozen with.
 */
export function decodeJobInput(input: unknown, jobId: string) {
  const envelope = record(input, "response");
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
  if (envelope.job !== jobId) throw new ContractError("job", "the requested Job");
  return jobInputRecord(envelope.input, "input", jobId);
}

/**
 * Reconstructs the bounded conversation ending at `jobId` from
 * `GET /api/v1/jobs/{job}/conversation`, ordered from oldest turn to `jobId`.
 */
export function decodeConversationTurns(input: unknown, conversationId: string) {
  const envelope = record(input, "response");
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
  if (envelope.conversation !== conversationId) throw new ContractError("conversation", "the requested conversation");
  const turns = listOf(envelope.turns, "turns", (entry, path) => {
    const turn = record(entry, path);
    const job = jobRecord(turn.job);
    const entries = listOf(turn.entries, `${path}.entries`, (input, path) => {
      const entry = record(input, path);
      const kind = string(entry.kind, `${path}.kind`);
      if (!["user_message", "agent_message", "notice", "decision", "retry_of", "iteration", "response_format"].includes(kind)) throw new ContractError(`${path}.kind`, "a turn entry kind");
      return { seq: integer(entry.seq, `${path}.seq`), kind, text: entry.text === undefined ? null : string(entry.text, `${path}.text`), attachments: entry.attachments === undefined ? [] : listOf(entry.attachments, `${path}.attachments`, (id, path) => identity(id, path, "attachment:")), at_us: integer(entry.at_us, `${path}.at_us`) };
    });
    const userEntries = entries.filter(entry => entry.kind === "user_message" || entry.kind === "agent_message");
    const frozenInput = jobInputRecord({ id: job.input, objective: userEntries.map(entry => entry.text ?? "").join("\n"), submitted_at_us: job.created_at_us, attachments: userEntries.flatMap(entry => entry.attachments), configuration: turn.configuration }, `${path}.input`, job.id);
    const outputs = listOf(turn.outputs, `${path}.outputs`, output => outputRecord(output));
    return { job, entries, input: frozenInput, outputs };
  });
  return { conversation: conversationId, turns, has_more: boolean(envelope.has_more, "has_more"), next_before: envelope.next_before === null ? null : identity(envelope.next_before, "next_before", "job:") };
}
export type ControlConversation = ReturnType<typeof decodeConversationTurns>;
export type ControlConversationTurn = ControlConversation["turns"][number];

export type ControlToolGrant = "read" | "edit" | "code" | null;

function toolGrant(value: unknown, path: string): ControlToolGrant {
  if (value === null || value === undefined) return null;
  if (value === "read" || value === "edit" || value === "code") return value;
  throw new ContractError(path, "null, read, edit or code");
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
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
  const nextBefore = envelope.next_before === null
    ? null
    : identity(envelope.next_before, "next_before", "profile:");
  const hasMore = boolean(envelope.has_more, "has_more");
  if (hasMore !== (nextBefore !== null)) {
    throw new ContractError("next_before", "a cursor exactly when more Profiles remain");
  }
  return {
    agent_profiles: listOf(envelope.agent_profiles, "agent_profiles", (entry, path) => {
      const agent = record(entry, path);
      return {
        id: identity(agent.id, `${path}.id`, "profile:"),
        name: string(agent.name, `${path}.name`),
        tool_grant: toolGrant(agent.tool_grant, `${path}.tool_grant`),
        llm_targets: listOf(agent.llm_targets, `${path}.llm_targets`, (target, targetPath) => {
          const llm = record(target, targetPath);
          return {
            id: identity(llm.id, `${targetPath}.id`, "profile:"),
            name: string(llm.name, `${targetPath}.name`),
            model: string(llm.model, `${targetPath}.model`),
            context_window: optionalInteger(llm.context_window, `${targetPath}.context_window`),
            declared_capabilities: listOf(llm.declared_capabilities, `${targetPath}.declared_capabilities`,
              (capability, capabilityPath) => {
                const named = string(capability, capabilityPath);
                const known = CAPABILITIES.find((candidate) => candidate === named);
                if (!known) throw new ContractError(capabilityPath, "a declared model capability");
                return known;
              }),
            executors: listOf(llm.executors, `${targetPath}.executors`,
              (executor, executorPath) => identity(executor, executorPath, "executor:")),
          };
        }),
      };
    }),
    has_more: hasMore,
    next_before: nextBefore,
  };
}

function projectRecord(input: unknown, path: string) {
  const project = record(input, path);
  return {
    id: identity(project.id, `${path}.id`, "project:"),
    name: string(project.name, `${path}.name`),
    description: optionalString(project.description, `${path}.description`),
    root: string(project.root, `${path}.root`),
    commands: listOf(project.commands, `${path}.commands`, (command, commandPath) => {
      const named = record(command, commandPath);
      return {
        name: string(named.name, `${commandPath}.name`),
        argv: listOf(named.argv, `${commandPath}.argv`, (argument, argumentPath) => string(argument, argumentPath)),
      };
    }),
    created_at_us: integer(project.created_at_us, `${path}.created_at_us`),
    updated_at_us: integer(project.updated_at_us, `${path}.updated_at_us`),
  };
}

/** One page of Projects from `GET /api/v1/projects`, newest first. */
export function decodeProjects(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
  const nextBefore = envelope.next_before === null
    ? null
    : identity(envelope.next_before, "next_before", "project:");
  const hasMore = boolean(envelope.has_more, "has_more");
  if (hasMore !== (nextBefore !== null)) {
    throw new ContractError("next_before", "a cursor exactly when more Projects remain");
  }
  return {
    projects: listOf(envelope.projects, "projects", (entry, path) => projectRecord(entry, path)),
    has_more: hasMore,
    next_before: nextBefore,
  };
}

export function decodeProjectAnswer(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
  return projectRecord(envelope.project, "project");
}

function poolRecord(input: unknown, path: string) {
  const pool = record(input, path);
  const concurrencyLimit = integer(pool.concurrency_limit, `${path}.concurrency_limit`);
  if (concurrencyLimit < 1) {
    throw new ContractError(`${path}.concurrency_limit`, "an integer >= 1");
  }
  return {
    id: identity(pool.id, `${path}.id`, "execution_pool:"),
    name: string(pool.name, `${path}.name`),
    concurrency_limit: concurrencyLimit,
    enabled: boolean(pool.enabled, `${path}.enabled`),
  };
}

export function decodePools(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
  return {
    pools: listOf(envelope.pools, "pools", (entry, path) => poolRecord(entry, path)),
  };
}

export function decodePoolAnswer(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
  return poolRecord(envelope.pool, "pool");
}

export function decodeRetry(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
  return {
    api_version: 1,
    conversation: identity(envelope.conversation, "conversation", "job:"),
    turn: identity(envelope.turn, "turn", "job:"),
    seq: integer(envelope.seq, "seq"),
    joined: boolean(envelope.joined, "joined"),
  };
}

export function decodeConversationEdit(input: unknown, root: string): string {
  const envelope = record(input, "response");
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
  const conversation = string(envelope.conversation, "conversation");
  if (conversation !== root) {
    throw new ContractError("conversation", `matching root ${root}`);
  }
  return root;
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

function optionalInteger(value: unknown, path: string): number | null {
  return value === null || value === undefined ? null : integer(value, path);
}

/**
 * Full Profile management snapshot from `GET /api/v1/profiles`,
 * `POST /api/v1/profiles`, and `POST /api/v1/profiles/{profile}`, including
 * LLM Profiles, Agent Profiles, and the computed Provider projection.
 */
export function decodeProfiles(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
  const profileId = envelope.profile_id === undefined || envelope.profile_id === null
    ? null
    : identity(envelope.profile_id, "profile_id", "profile:");
  return {
    profile_id: profileId,
    llm_profiles: listOf(envelope.llm_profiles, "llm_profiles", (entry, path) => {
      const llm = record(entry, path);
      const protocol = string(llm.protocol, `${path}.protocol`);
      if (protocol !== "openai_chat_completions") {
        throw new ContractError(`${path}.protocol`, "openai_chat_completions");
      }
      const params = record(llm.parameters, `${path}.parameters`);
      return {
        id: identity(llm.id, `${path}.id`, "profile:"),
        name: string(llm.name, `${path}.name`),
        model: string(llm.model, `${path}.model`),
        context_window: optionalInteger(llm.context_window, `${path}.context_window`),
        protocol,
        endpoint: optionalString(llm.endpoint, `${path}.endpoint`),
        credential_ref: optionalString(llm.credential_ref, `${path}.credential_ref`),
        has_credential: boolean(llm.has_credential, `${path}.has_credential`),
        declared_capabilities: listOf(llm.declared_capabilities, `${path}.declared_capabilities`,
          (capability, capabilityPath) => {
            const named = string(capability, capabilityPath);
            const known = CAPABILITIES.find((candidate) => candidate === named);
            if (!known) throw new ContractError(capabilityPath, "a declared model capability");
            return known;
          }),
        parameters: {
          temperature: optionalFiniteNumber(params.temperature, `${path}.parameters.temperature`),
          top_p: optionalFiniteNumber(params.top_p, `${path}.parameters.top_p`),
          max_tokens: timestamp(params.max_tokens, `${path}.parameters.max_tokens`),
          reasoning_effort: optionalString(params.reasoning_effort, `${path}.parameters.reasoning_effort`),
          max_completion_tokens: optionalInteger(params.max_completion_tokens, `${path}.parameters.max_completion_tokens`),
          presence_penalty: optionalFiniteNumber(params.presence_penalty, `${path}.parameters.presence_penalty`),
          frequency_penalty: optionalFiniteNumber(params.frequency_penalty, `${path}.parameters.frequency_penalty`),
        },
        executors: listOf(llm.executors, `${path}.executors`,
          (executor, executorPath) => identity(executor, executorPath, "executor:")),
        editable: boolean(llm.editable, `${path}.editable`),
        blocking_jobs: listOf(llm.blocking_jobs, `${path}.blocking_jobs`,
          (job, jobPath) => identity(job, jobPath, "job:")),
        more_blocking_jobs: boolean(llm.more_blocking_jobs, `${path}.more_blocking_jobs`),
        created_at_us: integer(llm.created_at_us, `${path}.created_at_us`),
        updated_at_us: integer(llm.updated_at_us, `${path}.updated_at_us`),
      };
    }),
    agent_profiles: listOf(envelope.agent_profiles, "agent_profiles", (entry, path) => {
      const agent = record(entry, path);
      const rawBehavior = string(agent.behavior, `${path}.behavior`);
      if (rawBehavior !== "chat" && rawBehavior !== "general") {
        throw new ContractError(`${path}.behavior`, "chat or general");
      }
      return {
        id: identity(agent.id, `${path}.id`, "profile:"),
        name: string(agent.name, `${path}.name`),
        tool_grant: toolGrant(agent.tool_grant, `${path}.tool_grant`),
        instructions: optionalString(agent.instructions, `${path}.instructions`),
        behavior: rawBehavior as ControlAgentBehavior,
        llm_targets: listOf(agent.llm_targets, `${path}.llm_targets`,
          (target, targetPath) => identity(target, targetPath, "profile:")),
        dispatch_to: agent.dispatch_to === undefined ? undefined : listOf(agent.dispatch_to, `${path}.dispatch_to`,
          (target, targetPath) => identity(target, targetPath, "profile:")),
        default_model: agent.default_model === undefined ? undefined : agent.default_model === null ? null
          : identity(agent.default_model, `${path}.default_model`, "profile:"),
        editable: boolean(agent.editable, `${path}.editable`),
        blocking_jobs: listOf(agent.blocking_jobs, `${path}.blocking_jobs`,
          (job, jobPath) => identity(job, jobPath, "job:")),
        more_blocking_jobs: boolean(agent.more_blocking_jobs, `${path}.more_blocking_jobs`),
        created_at_us: integer(agent.created_at_us, `${path}.created_at_us`),
        updated_at_us: integer(agent.updated_at_us, `${path}.updated_at_us`),
      };
    }),
    providers: listOf(envelope.providers, "providers", (entry, path) => {
      const provider = record(entry, path);
      const protocol = string(provider.protocol, `${path}.protocol`);
      if (protocol !== "openai_chat_completions") {
        throw new ContractError(`${path}.protocol`, "openai_chat_completions");
      }
      return {
        protocol,
        endpoint: optionalString(provider.endpoint, `${path}.endpoint`),
        credential_ref: optionalString(provider.credential_ref, `${path}.credential_ref`),
        has_credential: boolean(provider.has_credential, `${path}.has_credential`),
        profile_ids: listOf(provider.profile_ids, `${path}.profile_ids`,
          (id, idPath) => identity(id, idPath, "profile:")),
        models: listOf(provider.models, `${path}.models`,
          (model, modelPath) => string(model, modelPath)),
      };
    }),
  };
}

export type ControlAgentBehavior = "chat" | "general";
export type ControlProfilesSnapshot = ReturnType<typeof decodeProfiles>;
export type ControlLlmProfile = ControlProfilesSnapshot["llm_profiles"][number];
export type ControlManagedAgentProfile = ControlProfilesSnapshot["agent_profiles"][number];

/**
 * An image Attachment as `POST /api/v1/attachments` and
 * `GET /api/v1/attachments/{attachment}` report it.
 */
export function decodeAttachment(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
  const attachment = record(envelope.attachment, "attachment");
  return {
    id: identity(attachment.id, "attachment.id", "attachment:"),
    mime: string(attachment.mime, "attachment.mime"),
    byte_length: integer(attachment.byte_length, "attachment.byte_length"),
    sha256: string(attachment.sha256, "attachment.sha256"),
    display_name:
      attachment.display_name === null ? null : string(attachment.display_name, "attachment.display_name"),
    created_at_us: timestamp(attachment.created_at_us, "attachment.created_at_us"),
    available: typeof attachment.available === "boolean" ? attachment.available : true,
    data_url: typeof attachment.data_url === "string" ? attachment.data_url : undefined,
  };
}

export type ControlAttachment = ReturnType<typeof decodeAttachment>;


function oneOf<T extends string>(value: unknown, path: string, choices: readonly T[]): T {
  const found = choices.find(choice => choice === value);
  if (found === undefined) throw new ContractError(path, choices.join(", "));
  return found;
}

export function decodeChildren(input: unknown, conversationId: string) {
  const envelope = record(input, "response");
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
  if (envelope.conversation !== conversationId) throw new ContractError("conversation", "the requested conversation");
  const children = listOf(envelope.children, "children", (input, path) => {
    const child = record(input, path);
    const state = oneOf(child.state, `${path}.state`, ["open", "closing", "settled"]);
    if (state !== "open" && state !== "closing" && state !== "settled") throw new ContractError(`${path}.state`, "a conversation state");
    const hold = child.on_hold === null ? null : record(record(child.on_hold, `${path}.on_hold`).by, `${path}.on_hold.by`);
    const by = hold === null ? null : oneOf(hold.kind, `${path}.on_hold.by.kind`, ["user", "agent", "loop", "auto"]);
    if (by !== null && !["user", "agent", "loop", "auto"].includes(by)) throw new ContractError(`${path}.on_hold.by.kind`, "an actor");
    const recurrence = child.recurrence === null ? null : record(child.recurrence, `${path}.recurrence`);
    const phase = recurrence === null ? null : oneOf(recurrence.phase, `${path}.recurrence.phase`, ["active", "waiting_due", "halted"]);
    if (phase !== null && !["active", "waiting_due", "halted"].includes(phase)) throw new ContractError(`${path}.recurrence.phase`, "a recurrence phase");
    const interval = recurrence === null ? null : integer(recurrence.interval_s, `${path}.recurrence.interval_s`);
    if (interval !== null && (interval < 60 || interval > 2592000)) throw new ContractError(`${path}.recurrence.interval_s`, "60 to 2592000");
    return {
      id: identity(child.id, `${path}.id`, "job:"), title: string(child.title, `${path}.title`),
      agent: child.agent === null ? null : identity(child.agent, `${path}.agent`, "profile:"), state,
      on_hold: by === null ? null : { by: by === "agent"
        ? { kind: by, conversation: identity(hold?.conversation, `${path}.on_hold.by.conversation`, "job:") }
        : { kind: by } }, keep_open: boolean(child.keep_open, `${path}.keep_open`),
      recurrence: recurrence === null ? null : { interval_s: interval!, phase: phase!, due_us: recurrence.due_us === null ? null : integer(recurrence.due_us, `${path}.recurrence.due_us`) },
    };
  });
  return { conversation: conversationId, children, has_more: boolean(envelope.has_more, "has_more"), next_before: envelope.next_before === null ? null : identity(envelope.next_before, "next_before", "job:") };
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
  | "waiting"
  | "queued";

export type ControlTreeNode = {
  id: string;
  kind: "conversation" | "turn";
  parent: string | null;
  title: string;
  state: ControlTreeState;
  created_at_us: number;
  agent: string | null;
  agent_name: string | null;
  model: { llm_profile: string; model: string } | null;
  on_hold: { by: { kind: "agent"; conversation: string } | { kind: "user" | "loop" | "auto" } } | null;
  origin: "user" | "agent" | "system" | null;
  keep_open: boolean | null;
  active_turn: string | null;
  recurrence: {
    interval_s: number;
    phase: "active" | "waiting_due" | "halted";
    due_us: number | null;
  } | null;
  conversation: string | null;
  previous: string | null;
  depends_on: {
    job: string;
    condition: "settled" | "success";
    state: "waiting" | "satisfied" | "impossible";
  }[];
  children: ControlTreeNode[] | null;
  has_more_children: boolean;
  next_after: string | null;
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
  "waiting",
  "queued",
] as const;

// Depth counts edges below the root, matching the server's tree query.
export const TREE_MAX_DEPTH = 4;

function treeNode(input: unknown, path: string, depth: number): ControlTreeNode {
  if (depth > TREE_MAX_DEPTH) throw new ContractError(path, `a tree of at most ${TREE_MAX_DEPTH} edges below the root`);
  const node = record(input, path);
  const model = node.model === null ? null : (() => {
    const frozen = record(node.model, `${path}.model`);
    return {
      llm_profile: identity(frozen.llm_profile, `${path}.model.llm_profile`, "profile:"),
      model: string(frozen.model, `${path}.model.model`),
    };
  })();
  const hold = node.on_hold === null
    ? null
    : record(record(node.on_hold, `${path}.on_hold`).by, `${path}.on_hold.by`);
  const by = hold === null
    ? null
    : oneOf(hold.kind, `${path}.on_hold.by.kind`, ["user", "agent", "loop", "auto"] as const);
  const origin = node.origin === null
    ? null
    : oneOf(node.origin, `${path}.origin`, ["user", "agent", "system"] as const);
  const recurrence = node.recurrence === null ? null : (() => {
    const row = record(node.recurrence, `${path}.recurrence`);
    return {
      interval_s: integer(row.interval_s, `${path}.recurrence.interval_s`),
      phase: oneOf(row.phase, `${path}.recurrence.phase`, ["active", "waiting_due", "halted"] as const),
      due_us: row.due_us === null ? null : integer(row.due_us, `${path}.recurrence.due_us`),
    };
  })();
  return {
    id: identity(node.id, `${path}.id`, "job:"),
    kind: oneOf(node.kind, `${path}.kind`, ["conversation", "turn"] as const),
    parent: node.parent === null ? null : identity(node.parent, `${path}.parent`, "job:"),
    title: string(node.title, `${path}.title`),
    state: oneOf(node.state, `${path}.state`, TREE_STATES),
    created_at_us: integer(node.created_at_us, `${path}.created_at_us`),
    agent: node.agent === null ? null : identity(node.agent, `${path}.agent`, "profile:"),
    agent_name: node.agent_name === null ? null : string(node.agent_name, `${path}.agent_name`),
    model,
    on_hold: by === null ? null : { by: by === "agent"
      ? { kind: by, conversation: identity(hold?.conversation, `${path}.on_hold.by.conversation`, "job:") }
      : { kind: by } },
    origin,
    keep_open: node.keep_open === null ? null : boolean(node.keep_open, `${path}.keep_open`),
    active_turn: node.active_turn === null ? null : identity(node.active_turn, `${path}.active_turn`, "job:"),
    recurrence,
    conversation: node.conversation === null ? null : identity(node.conversation, `${path}.conversation`, "job:"),
    previous: node.previous === null ? null : identity(node.previous, `${path}.previous`, "job:"),
    depends_on: listOf(node.depends_on, `${path}.depends_on`, (entry, entryPath) => {
      const dependency = record(entry, entryPath);
      return {
        job: identity(dependency.job, `${entryPath}.job`, "job:"),
        condition: oneOf(dependency.condition, `${entryPath}.condition`, ["settled", "success"] as const),
        state: oneOf(dependency.state, `${entryPath}.state`, ["waiting", "satisfied", "impossible"] as const),
      };
    }),
    children: node.children === null
      ? null
      : listOf(node.children, `${path}.children`, (entry, entryPath) => treeNode(entry, entryPath, depth + 1)),
    has_more_children: boolean(node.has_more_children, `${path}.has_more_children`),
    next_after: node.next_after === null ? null : identity(node.next_after, `${path}.next_after`, "job:"),
  };
}

/**
 * The tree of the work below a Job, as `GET /api/v1/jobs/{job}/tree` reports
 * it: the ancestors above the root, and the nodes below it.
 */
export function decodeJobTree(input: unknown, job: string) {
  const envelope = record(input, "response");
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
  if (envelope.job !== job) throw new ContractError("job", "the requested Job");
  const root = treeNode(envelope.root, "root", 0);
  if (root.id !== job) throw new ContractError("root.id", "the requested Job");
  return {
    job,
    ancestors: listOf(envelope.ancestors, "ancestors", (entry, path) => identity(entry, path, "job:")),
    root,
  };
}

export type ControlApproval = [number, number, number];

function approvalRef(input: unknown, path: string): ControlApproval {
  if (!Array.isArray(input) || input.length !== 3) throw new ContractError(path, "an approval triple");
  const turn = integer(input[0], `${path}.turn`);
  const request = integer(input[1], `${path}.request`);
  const index = integer(input[2], `${path}.index`);
  if (turn === 0 || request === 0 || request > 0xffffffff || index > 0xffffffff) throw new ContractError(path, "a stored call reference");
  return [turn, request, index];
}

export type ControlConversationCard = { conversation: string; parent: string | null } & (
  | { kind: "approval"; approval: ControlApproval; tool: string; arguments: string }
  | { kind: "question"; approval: ControlApproval; tool: string; question: string }
  | { kind: "loop_hold" }
  | { kind: "quarantine"; turn: string }
);

export function decodeCards(input: unknown, conversationId: string) {
  const envelope = record(input, "response");
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
  if (envelope.conversation !== conversationId) throw new ContractError("conversation", `matching ${conversationId}`);
  const cards = listOf(envelope.cards, "cards", (input, path): ControlConversationCard => {
    const card = record(input, path);
    const common = { conversation: identity(card.conversation, `${path}.conversation`, "job:"), parent: card.parent === null ? null : identity(card.parent, `${path}.parent`, "job:") };
    switch (card.kind) {
      case "approval": return { ...common, kind: card.kind, approval: approvalRef(card.approval, `${path}.approval`), tool: string(card.tool, `${path}.tool`), arguments: string(card.arguments, `${path}.arguments`) };
      case "question": return { ...common, kind: card.kind, approval: approvalRef(card.approval, `${path}.approval`), tool: string(card.tool, `${path}.tool`), question: string(card.question, `${path}.question`) };
      case "loop_hold": return { ...common, kind: card.kind };
      case "quarantine": return { ...common, kind: card.kind, turn: identity(card.turn, `${path}.turn`, "job:") };
      default: throw new ContractError(`${path}.kind`, "an approval, question, loop_hold or quarantine card");
    }
  });
  return { conversation: conversationId, cards };
}

export function decodeApprovalDecision(input: unknown, expected: ControlApproval) {
  const value = record(input, "response");
  if (value.api_version !== 1) throw new ContractError("api_version", "1");
  const approval = approvalRef(value.approval, "approval");
  if (approval.some((part, index) => part !== expected[index])) throw new ContractError("approval", "the requested approval");
  const decision = value.decision;
  if (decision !== "approved" && decision !== "rejected" && decision !== "answered") throw new ContractError("decision", "an approval decision");
  return { approval, decision, turn: identity(value.turn, "turn", "job:"), seq: integer(value.seq, "seq"), joined: boolean(value.joined, "joined") };
}

export function decodeReleaseSlot(input: unknown) {
  const value = record(input, "response");
  if (value.api_version !== 1) throw new ContractError("api_version", "1");
  if (value.status !== "released" && value.status !== "already_released") throw new ContractError("status", "a released slot");
  return { status: value.status };
}
