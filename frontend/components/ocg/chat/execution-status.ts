import type { ExecutionCall, JobExecution } from "../execution/domain";

/**
 * What a running Chat turn is actually executing, read from the Job's frozen
 * canonical facts. The composer's next-turn selection never feeds this.
 */
export type ActiveExecutionTarget = {
  jobId: string;
  generation: number;
  providerKey: string;
  /** The Profile model key placement froze. */
  model: string;
  upstreamModelId: string | null;
  /** The frozen effort, or `null` when the request carried none. */
  effort: string | null;
};

export type ActivityStepKind =
  | "read" | "list" | "search" | "edit" | "run" | "validate" | "snapshot" | "tool"
  | "provider" | "provider-failed";

/** One observable execution step. Built from Call names and arguments only. */
export type ActivityStep = {
  id: string;
  kind: ActivityStepKind;
  /** File, query, command or tool name the step acted on. */
  target: string;
  status: "done" | "running" | "failed";
};

export type ExecutionPhase = "queued" | "running" | "cancelling";

const ACTIVE_STATES = new Set(["pending", "eligible", "running", "cancelling"]);

export function isExecutionActive(execution: JobExecution | null | undefined): execution is JobExecution {
  return Boolean(execution && ACTIVE_STATES.has(execution.state));
}

export function executionPhase(execution: JobExecution): ExecutionPhase {
  if (execution.state === "cancelling") return "cancelling";
  return execution.state === "running" ? "running" : "queued";
}

type ParsedRequest = { provider: false; name: string; arguments: Record<string, unknown> } | { provider: true; effort: string | null } | null;

// Call requests are immutable durable payloads and provider ones carry the
// whole conversation, so each is parsed once.
const parsed = new Map<string, ParsedRequest>();

function parseRequest(id: string, request: string): ParsedRequest {
  const cached = parsed.get(id);
  if (cached !== undefined) return cached;
  let result: ParsedRequest = null;
  try {
    const value: unknown = JSON.parse(request);
    if (typeof value === "object" && value !== null) {
      const record = value as Record<string, unknown>;
      const args = typeof record.arguments === "object" && record.arguments !== null ? record.arguments as Record<string, unknown> : {};
      if (record.executor_transport === "provider") {
        result = { provider: true, effort: typeof args.reasoning_effort === "string" ? args.reasoning_effort : null };
      } else if (record.kind === "native_tool" && typeof record.name === "string") {
        result = { provider: false, name: record.name, arguments: args };
      }
    }
  } catch {
    result = null;
  }
  if (parsed.size > 512) parsed.clear();
  parsed.set(id, result);
  return result;
}

/**
 * The frozen Provider × Model × Effort of the Job's current generation.
 * An earlier generation is never substituted: until this generation's
 * dispatch is visible the target is unknown, not the previous Attempt's.
 */
export function executionTarget(execution: JobExecution | null | undefined): ActiveExecutionTarget | null {
  if (!execution) return null;
  // Only this Job's current generation. An earlier Attempt's frozen target is
  // that Attempt's own identity and must not stand in for a generation that
  // has not frozen one yet.
  const intents = execution.dispatchIntents
    .filter(intent => intent.generation === execution.generation && intent.providerKey && intent.model)
    .sort((left, right) => right.createdAt - left.createdAt);
  const intent = intents[0];
  if (!intent?.providerKey || !intent.model) return null;
  const request = parseRequest(`intent:${intent.dispatchIntentId}`, intent.request);
  return {
    jobId: execution.jobId,
    generation: execution.generation,
    providerKey: intent.providerKey,
    model: intent.model,
    upstreamModelId: intent.upstreamModelId,
    effort: request?.provider ? request.effort : null,
  };
}

/**
 * The frozen Provider × Model × Effort of a Job that is still executing.
 * Same rule as `executionTarget`: this generation only, never an earlier one.
 */
export function activeExecutionTarget(execution: JobExecution | null | undefined): ActiveExecutionTarget | null {
  if (!isExecutionActive(execution)) return null;
  return executionTarget(execution);
}

function basename(path: unknown): string {
  if (typeof path !== "string" || !path) return "";
  const trimmed = path.replace(/[\\/]+$/, "");
  return trimmed.slice(Math.max(trimmed.lastIndexOf("/"), trimmed.lastIndexOf("\\")) + 1) || trimmed;
}

function clip(text: string, limit = 60): string {
  const single = text.replace(/\s+/g, " ").trim();
  return single.length > limit ? `${single.slice(0, limit - 1)}…` : single;
}

function toolStep(name: string, args: Record<string, unknown>): Pick<ActivityStep, "kind" | "target"> {
  switch (name) {
    case "filesystem.read":
      return { kind: "read", target: basename(args.path) };
    case "context.read": {
      const first = Array.isArray(args.items) ? args.items[0] as Record<string, unknown> | undefined : undefined;
      return { kind: "read", target: basename(first?.path) };
    }
    case "filesystem.list":
      return { kind: "list", target: basename(args.path) || "." };
    case "filesystem.search":
    case "context.search":
      return { kind: "search", target: clip(typeof args.query === "string" ? args.query : "", 40) };
    case "filesystem.edit":
      return { kind: "edit", target: basename(args.file) };
    case "process.exec": {
      const argv = [args.program, ...(Array.isArray(args.args) ? args.args : [])].filter(part => typeof part === "string");
      return { kind: "run", target: clip(argv.join(" ")) };
    }
    case "context.validation":
      return { kind: "validate", target: "" };
    case "context.snapshot":
      return { kind: "snapshot", target: "" };
    default:
      return { kind: "tool", target: name };
  }
}

function stepStatus(call: ExecutionCall): ActivityStep["status"] | null {
  if (call.status === "completed") return "done";
  if (call.status === "failed") return "failed";
  if (call.status === "queued" || call.status === "running") return "running";
  return null;
}

/**
 * Observable steps of the Job's current generation, oldest first: the Native
 * Tool Calls the provider asked for and, while no tool runs, the provider
 * round itself. Only Call names and arguments are read — never provider
 * responses or reasoning.
 */
export function executionActivity(execution: JobExecution | null | undefined): ActivityStep[] {
  if (!execution) return [];
  const steps: ActivityStep[] = [];
  const calls = execution.calls
    .filter(call => call.attemptGeneration === execution.generation)
    .sort((left, right) => left.createdAt - right.createdAt || left.callId.localeCompare(right.callId));
  for (const call of calls) {
    const status = stepStatus(call);
    const request = parseRequest(`call:${call.callId}`, call.request);
    if (!status || !request) continue;
    if (request.provider) {
      if (status === "failed") steps.push({ id: call.callId, kind: "provider-failed", target: "", status });
      continue;
    }
    steps.push({ id: call.callId, ...toolStep(request.name, request.arguments), status });
  }
  return steps;
}

/** The single step to show beside the model while the Job is active. */
export function currentActivity(execution: JobExecution | null | undefined, steps: readonly ActivityStep[]): ActivityStep | ExecutionPhase | null {
  if (!isExecutionActive(execution)) return null;
  const phase = executionPhase(execution);
  if (phase !== "running") return phase;
  const running = [...steps].reverse().find(step => step.status === "running");
  return running ?? { id: "provider", kind: "provider", target: "", status: "running" };
}

/** Whether a Call is a provider round trip or a Native Tool Call, from its durable request. */
export function callKindOf(call: ExecutionCall): "provider" | "native" | null {
  const request = parseRequest(`call:${call.callId}`, call.request);
  return request === null ? null : request.provider ? "provider" : "native";
}

/** The canonical native tool name, when the Call request identifies one. */
export function nativeToolNameOf(call: ExecutionCall): string | null {
  const request = parseRequest(`call:${call.callId}`, call.request);
  return request && !request.provider ? request.name : null;
}

/** The reasoning effort a provider dispatch froze, or `null` when its request carried none. */
export function frozenEffort(execution: JobExecution, dispatchIntentId: string): string | null {
  const intent = execution.dispatchIntents.find(item => item.dispatchIntentId === dispatchIntentId);
  if (!intent) return null;
  const request = parseRequest(`intent:${intent.dispatchIntentId}`, intent.request);
  return request?.provider ? request.effort : null;
}
