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
    name: job.name === null ? null : string(job.name, "job.name"),
    execution_mode: mode,
    input: string(job.input, "job.input"),
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
  if (kind !== "progress" && kind !== "evaluation" && kind !== "result") {
    throw new ContractError("output.kind", "progress, evaluation or result");
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
  return {
    host,
    database: {
      status,
      accepting_commands: boolean(database.accepting_commands, "database.accepting_commands"),
      readable: boolean(database.readable, "database.readable"),
    },
  };
}

/** The Job's persisted record, as `GET /api/v1/jobs/{job}` reports it. */
export function decodeJob(input: unknown) {
  const envelope = record(input, "response");
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
  return jobRecord(envelope.job);
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
  return { status, job };
}

/** What a stop request reached: never how the Job will settle. */
export function decodeStop(input: unknown, jobId: string) {
  const envelope = record(input, "response");
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
  const stop = record(envelope.stop, "stop");
  if (stop.job !== jobId) throw new ContractError("stop.job", "the requested Job");
  const status = stop.status;
  if (status !== "requested" && status !== "already_settled") {
    throw new ContractError("stop.status", "requested or already_settled");
  }
  return { job: jobId, status };
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

/**
 * A Job's frozen input as `GET /api/v1/jobs/{job}/input` reports it: the
 * objective as text (the Job record names its input only by identity), its
 * Attachments by identity, and the Profiles and model it was frozen with.
 */
export function decodeJobInput(input: unknown, jobId: string) {
  const envelope = record(input, "response");
  if (envelope.api_version !== 1) throw new ContractError("api_version", "1");
  if (envelope.job !== jobId) throw new ContractError("job", "the requested Job");
  const value = record(envelope.input, "input");
  const attachments = value.attachments;
  if (!Array.isArray(attachments)) throw new ContractError("input.attachments", "an array");
  let configuration: { agent_profile: string | null; llm_profile: string; model: string } | null = null;
  if (value.configuration !== null) {
    const frozen = record(value.configuration, "input.configuration");
    configuration = {
      agent_profile: frozen.agent_profile === null
        ? null
        : identity(frozen.agent_profile, "input.configuration.agent_profile", "profile:"),
      llm_profile: identity(frozen.llm_profile, "input.configuration.llm_profile", "profile:"),
      model: string(frozen.model, "input.configuration.model"),
    };
  }
  return {
    job: jobId,
    id: identity(value.id, "input.id", "job_input:"),
    objective: value.objective === null ? null : string(value.objective, "input.objective"),
    submitted_at_us: timestamp(value.submitted_at_us, "input.submitted_at_us"),
    attachments: attachments.map((attachment, index) =>
      identity(attachment, `input.attachments[${index}]`, "attachment:")),
    configuration,
  };
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
        llm_targets: listOf(agent.llm_targets, `${path}.llm_targets`, (target, targetPath) => {
          const llm = record(target, targetPath);
          return {
            id: identity(llm.id, `${targetPath}.id`, "profile:"),
            name: string(llm.name, `${targetPath}.name`),
            model: string(llm.model, `${targetPath}.model`),
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

export type ControlJobInput = ReturnType<typeof decodeJobInput>;
export type ControlAgentProfiles = ReturnType<typeof decodeAgentProfiles>;
export type ControlAgentProfile = ControlAgentProfiles["agent_profiles"][number];
