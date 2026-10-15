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

function settlement(input: unknown) {
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

// These are checked projections of the JSON emitted by control_plane.rs,
// not the incompatible 0.4.5 Job/Attempt/Call DTOs or a second lifecycle.
export function decodeJobEvent(name: string, input: unknown, jobId: string, id: string | null) {
  const value = record(input, "event");
  if (value.api_version !== 1) throw new ContractError("api_version", "1");
  if (name !== "output" && id !== null) throw new ContractError("id", "an id only on output");
  if (name === "stream_error") {
    return { type: "stream_error" as const, error: decodeControlError(value) };
  }
  if (name === "job") {
    const job = record(value.job, "job");
    if (job.id !== jobId) throw new ContractError("job.id", "the requested Job");
    const mode = job.execution_mode;
    if (mode !== "once" && mode !== "continuous" && mode !== "periodic" && mode !== "container") {
      throw new ContractError("job.execution_mode", "a Control Plane execution mode");
    }
    return {
      type: "job" as const, job: {
        id: jobId,
        name: job.name === null ? null : string(job.name, "job.name"),
        execution_mode: mode,
        input: string(job.input, "job.input"),
        assigned_executor: job.assigned_executor === null ? null : string(job.assigned_executor, "job.assigned_executor"),
        created_at_us: integer(job.created_at_us, "job.created_at_us"),
        updated_at_us: integer(job.updated_at_us, "job.updated_at_us"),
        started_at_us: timestamp(job.started_at_us, "job.started_at_us"),
        paused_at_us: timestamp(job.paused_at_us, "job.paused_at_us"),
        sterilized_at_us: timestamp(job.sterilized_at_us, "job.sterilized_at_us"),
        settlement: settlement(job.settlement),
        stop_confirmed_at_us: timestamp(job.stop_confirmed_at_us, "job.stop_confirmed_at_us"),
      }
    };
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
  const output = record(value.output, "output");
  const seq = integer(output.seq, "output.seq");
  if (seq === 0 || id !== String(seq)) throw new ContractError("id", "the output's positive stored seq");
  const kind = output.kind;
  if (kind !== "progress" && kind !== "evaluation" && kind !== "result") {
    throw new ContractError("output.kind", "progress, evaluation or result");
  }
  if (output.goal_reached !== null && typeof output.goal_reached !== "boolean") {
    throw new ContractError("output.goal_reached", "a boolean or null");
  }
  if (!("value" in output)) throw new ContractError("output.value", "a JSON value");
  return {
    type: "output" as const, job: jobId, output: {
      id: string(output.id, "output.id"), seq, kind,
      summary: output.summary === null ? null : string(output.summary, "output.summary"),
      code: output.code === null ? null : string(output.code, "output.code"),
      goal_reached: output.goal_reached,
      value: output.value,
      created_at_us: integer(output.created_at_us, "output.created_at_us"),
    }
  };
}

export type ControlJobEvent = ReturnType<typeof decodeJobEvent>;
