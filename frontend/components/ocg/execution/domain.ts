/**
 * The canonical execution read model.
 *
 * The SQLite substrate is the execution authority: a Project owns Jobs, a Job
 * owns Attempts, an Attempt owns Executors and Calls, and exactly one Attempt
 * of a Job is authoritative at a time. This module is the *shape* the PWA
 * renders that authority through — nothing more.
 *
 * Two rules hold throughout:
 * - every field is either a canonical value the backend sent or a pure
 *   derivation from canonical values. A value the projection does not carry is
 *   absent, never invented;
 * - the PWA owns no execution authority. It cannot create a Job, promote an
 *   Attempt, admit a Call, or settle an effect.
 *
 * The one constructor of this shape lives in `runtime/canonical-envelope`, so
 * there is exactly one path from durable canonical state to the execution UI.
 */

import type {
  CanonicalAttempt,
  CanonicalCall,
  CanonicalDispatchIntent,
  CanonicalEffectKind,
  CanonicalExecutor,
  CanonicalJobRelations,
  CanonicalJobOperations,
  Failure,
  CanonicalJobState,
} from "../contracts";
import type { ProjectId } from "../project/domain";
import { formatTimestamp } from "@/lib/format";

/* -------------------------------------------------------------------------- */
/* Identity                                                                   */
/* -------------------------------------------------------------------------- */

const ENTITY_PREFIX = {
  job: "job",
  attempt: "attempt",
  call: "call",
  executor: "executor",
  dispatch_intent: "dispatch-intent",
} as const;

export type CanonicalEntityKind = keyof typeof ENTITY_PREFIX;

/**
 * A stable, Project-scoped presentation id for one canonical entity.
 *
 * The substrate identity stays readable inside it. This decorates an identity
 * for React keys and cross-surface links; it never replaces it.
 */
export function canonicalEntityId(
  projectId: ProjectId,
  kind: CanonicalEntityKind,
  id: string,
): string {
  return `${projectId}:${ENTITY_PREFIX[kind]}:${id}`;
}

/* -------------------------------------------------------------------------- */
/* Call                                                                       */
/* -------------------------------------------------------------------------- */

/**
 * How a Call is rendered.
 *
 * `domain_calls.state` is an unconstrained column, so the words the substrate
 * actually writes are spelled out here. Anything else stays on screen as
 * `unrecognized` with its raw state beside it, rather than being coerced into a
 * status this projection cannot justify.
 */
export type CallStatus = "queued" | "running" | "completed" | "failed" | "unrecognized";

const CALL_STATUS: Record<string, CallStatus> = {
  created: "queued",
  queued: "queued",
  running: "running",
  succeeded: "completed",
  completed: "completed",
  failed: "failed",
  cancelled: "unrecognized",
};

export function callStatus(call: CanonicalCall): CallStatus {
  return CALL_STATUS[call.state] ?? "unrecognized";
}

/** A Call as the read model renders it. Ids are Project-scoped for presentation. */
export type ExecutionCall = {
  /** Stable Project-scoped presentation identity. */
  id: string;
  /** The canonical Call identity the substrate wrote. */
  callId: string;
  attemptId: string;
  /** The generation of the Attempt that owns this Call. */
  attemptGeneration: number;
  /** The Executor that ran this Call, when the substrate named one. */
  executorId: string | null;
  generation: number;
  status: CallStatus;
  /** The raw state column, shown verbatim whenever this view cannot interpret it. */
  rawState: string;
  /** The effect kind the durable DispatchIntent froze for this Call. */
  effectKind: CanonicalEffectKind;
  sideEffect: boolean;
  request: string;
  response: string | null;
  /** Canonical epoch seconds, as the substrate stores them. */
  createdAt: number;
  finishedAt: number | null;
  /** Derived from the two canonical instants; absent while the Call is open. */
  elapsedMs: number | undefined;
  /** Why a Call reads as failed or unrecognized, in the substrate's own words. */
  reason: string | undefined;
};

/** The substrate stores epoch seconds; durations are counted in milliseconds. */
function elapsedMs(call: CanonicalCall): number | undefined {
  if (call.finished_at === null) return undefined;
  return Math.max(0, (call.finished_at - call.created_at) * 1000);
}

function callReason(call: CanonicalCall, status: CallStatus): string | undefined {
  if (status === "unrecognized") {
    return `Call state "${call.state}" is not one this view interprets.`;
  }
  if (status === "failed") return call.response ?? undefined;
  return undefined;
}

function toExecutionCall(
  projectId: ProjectId,
  call: CanonicalCall,
  generationByAttempt: ReadonlyMap<string, number>,
): ExecutionCall {
  return {
    id: canonicalEntityId(projectId, "call", call.id),
    callId: call.id,
    attemptId: call.attempt_id,
    attemptGeneration: generationByAttempt.get(call.attempt_id) ?? 0,
    executorId: call.executor_id,
    generation: call.generation,
    status: callStatus(call),
    rawState: call.state,
    effectKind: call.effect_kind,
    sideEffect: call.side_effect,
    request: call.request,
    response: call.response,
    createdAt: call.created_at,
    finishedAt: call.finished_at,
    elapsedMs: elapsedMs(call),
    reason: callReason(call, callStatus(call)),
  };
}

export function isSettled(call: ExecutionCall): boolean {
  return call.status === "completed" || call.status === "failed";
}

/* -------------------------------------------------------------------------- */
/* Attempt                                                                    */
/* -------------------------------------------------------------------------- */

/** One generation of one Job, as the substrate recorded it. */
export type ExecutionAttempt = {
  id: string;
  attemptId: string;
  generation: number;
  /** The canonical Attempt state, unmodified. */
  state: CanonicalAttempt["state"];
  /** The backend's own decision. This view never promotes an Attempt. */
  authoritative: boolean;
  createdAt: number;
  finishedAt: number | null;
  callCount: number;
  settledCallCount: number;
};

/** The Attempt history of one Job, ordered by generation. */
export function attemptHistoryOf(
  projectId: ProjectId,
  attempts: readonly CanonicalAttempt[],
  calls: readonly ExecutionCall[],
): ExecutionAttempt[] {
  return attempts
    .slice()
    .sort((left, right) => left.generation - right.generation)
    .map((attempt) => {
      const owned = calls.filter((call) => call.attemptId === attempt.id);
      return {
        id: canonicalEntityId(projectId, "attempt", attempt.id),
        attemptId: attempt.id,
        generation: attempt.generation,
        state: attempt.state,
        authoritative: attempt.authoritative,
        createdAt: attempt.created_at,
        finishedAt: attempt.finished_at,
        callCount: owned.length,
        settledCallCount: owned.filter(isSettled).length,
      };
    });
}

/* -------------------------------------------------------------------------- */
/* Executor                                                                   */
/* -------------------------------------------------------------------------- */

export type ExecutorStatus = "running" | "queued" | "settled" | "unrecognized";

/**
 * How an Executor's durable state is rendered.
 *
 * `domain_executors.state` is an unconstrained column, so the words the
 * substrate actually writes are spelled out here. `created` and `ready` are
 * awaiting work; `running` is live; `fenced` is the terminal state written when
 * the owning Attempt is replaced or closed; the remaining words are the
 * Attempt's own terminal states, written through verbatim. Anything else stays
 * `unrecognized` with its raw state beside it, rather than being coerced into a
 * status this projection cannot justify.
 */
const EXECUTOR_STATUS: Record<string, ExecutorStatus> = {
  created: "queued",
  ready: "queued",
  running: "running",
  fenced: "settled",
  completed: "settled",
  failed: "unrecognized",
  cancelled: "unrecognized",
  unknown: "unrecognized",
  orphaned: "unrecognized",
};

export function executorStatus(executor: CanonicalExecutor): ExecutorStatus {
  return EXECUTOR_STATUS[executor.state] ?? "unrecognized";
}

/**
 * An Executor as the backend recorded it.
 *
 * The canonical snapshot carries an Executor itself — the Attempt that owns it,
 * its kind and its durable state — so this projection reads those values rather
 * than reconstructing an Executor out of the Calls that reference it. The Calls
 * it ran are still listed, because they are the Calls whose canonical
 * `executor_id` names this Executor.
 */
export type ExecutionExecutor = {
  /** Stable Project-scoped presentation identity. */
  id: string;
  /** The canonical Executor identity the substrate wrote. */
  executorId: string;
  attemptId: string;
  /** The substrate's own kind column, shown verbatim. */
  kind: string;
  status: ExecutorStatus;
  /** The raw state column, shown verbatim whenever this view cannot interpret it. */
  rawState: string;
  callIds: string[];
  runningCallId: string | undefined;
  settledCallCount: number;
};

export function executorsOf(
  projectId: ProjectId,
  executors: readonly CanonicalExecutor[],
  calls: readonly ExecutionCall[],
): ExecutionExecutor[] {
  return executors.map((executor) => {
    const owned = calls.filter((call) => call.executorId === executor.id);
    return {
      id: canonicalEntityId(projectId, "executor", executor.id),
      executorId: executor.id,
      attemptId: executor.attempt_id,
      kind: executor.kind,
      status: executorStatus(executor),
      rawState: executor.state,
      callIds: owned.map((call) => call.id),
      runningCallId: owned.find((call) => call.status === "running")?.id,
      settledCallCount: owned.filter(isSettled).length,
    };
  });
}

/* -------------------------------------------------------------------------- */
/* DispatchIntent                                                             */
/* -------------------------------------------------------------------------- */

/**
 * How a DispatchIntent's durable state is rendered.
 *
 * `domain_dispatch_intents.state` admits `pending`, `queued`, `running`,
 * `completed`, `failed` and `fenced`. `fenced` is the state written when the
 * owning Attempt is replaced or closed, and `failed` when the effect could not
 * be carried out; the raw state stays beside the rendering either way.
 */
export type DispatchIntentStatus =
  | "pending"
  | "running"
  | "settled"
  | "failed"
  | "fenced"
  | "unrecognized";

const DISPATCH_INTENT_STATUS: Record<string, DispatchIntentStatus> = {
  pending: "pending",
  queued: "pending",
  running: "running",
  completed: "settled",
  failed: "failed",
  fenced: "fenced",
};

export function dispatchIntentStatus(intent: CanonicalDispatchIntent): DispatchIntentStatus {
  return DISPATCH_INTENT_STATUS[intent.state] ?? "unrecognized";
}

/**
 * A DispatchIntent as the backend recorded it: the durable admission of one
 * Call's effect, frozen before the provider ran. It is a canonical entity in
 * its own right, not a derivation from the Call it admits.
 */
export type ExecutionDispatchIntent = {
  /** Stable Project-scoped presentation identity. */
  id: string;
  /** The canonical DispatchIntent identity the substrate wrote. */
  dispatchIntentId: string;
  callId: string;
  attemptId: string;
  executorId: string | null;
  generation: number;
  status: DispatchIntentStatus;
  /** The raw state column, shown verbatim whenever this view cannot interpret it. */
  rawState: string;
  effectKind: CanonicalEffectKind;
  /** The raw effect lifecycle column, shown verbatim. */
  effectState: string;
  request: string;
  budgetAdmitted: boolean;
  failure: string | null;
  createdAt: number;
  updatedAt: number;
};

export function dispatchIntentsOf(
  projectId: ProjectId,
  intents: readonly CanonicalDispatchIntent[],
): ExecutionDispatchIntent[] {
  return intents.map((intent) => ({
    id: canonicalEntityId(projectId, "dispatch_intent", intent.id),
    dispatchIntentId: intent.id,
    callId: intent.call_id,
    attemptId: intent.attempt_id,
    executorId: intent.executor_id,
    generation: intent.generation,
    status: dispatchIntentStatus(intent),
    rawState: intent.state,
    effectKind: intent.effect_kind,
    effectState: intent.effect_state,
    request: intent.request,
    budgetAdmitted: intent.budget_admitted,
    failure: intent.failure,
    createdAt: intent.created_at,
    updatedAt: intent.updated_at,
  }));
}

/* -------------------------------------------------------------------------- */
/* Derived activity                                                           */
/* -------------------------------------------------------------------------- */

/**
 * One line of Call history.
 *
 * The work snapshot carries no event log, so the only timeline this projection
 * can honestly draw is the Call records themselves: one line per Call, at the
 * instant the substrate says it settled or was created.
 */
export type CallActivityItem = {
  id: string;
  jobId: string;
  callId: string;
  executorId: string | undefined;
  attemptGeneration: number;
  status: CallStatus;
  /** Canonical epoch seconds of the settled (or created) instant. */
  at: number;
  timestamp: string;
  message: string;
  elapsedMs?: number;
};

function callActivity(call: ExecutionCall, jobId: string): CallActivityItem {
  const at = call.finishedAt ?? call.createdAt;
  const message =
    call.status === "completed"
      ? `Call settled · ${call.effectKind}`
      : call.status === "running"
        ? `Call running · ${call.effectKind}`
        : call.status === "queued"
          ? `Call queued · ${call.effectKind}`
          : (call.reason ?? `Call ${call.rawState}`);
  return {
    id: `${jobId}:call:${call.callId}:${at}`,
    jobId,
    callId: call.callId,
    executorId: call.executorId ?? undefined,
    attemptGeneration: call.attemptGeneration,
    status: call.status,
    at,
    timestamp: formatTimestamp(new Date(at * 1000).toISOString()),
    message,
  };
}

export function boundActivity(
  items: readonly CallActivityItem[],
  limit = 80,
): CallActivityItem[] {
  return items.slice(Math.max(0, items.length - limit));
}

/* -------------------------------------------------------------------------- */
/* Job                                                                        */
/* -------------------------------------------------------------------------- */

/**
 * Progress this projection is allowed to state.
 *
 * There is no canonical percentage. The one explainable measure is how many of
 * the Calls the authoritative Attempt carries have settled, so every reader of
 * this figure sees its denominator with it. It is `null`, not zero, when the
 * authoritative Attempt carries no Calls.
 */
export type JobProgress = {
  settled: number;
  total: number;
  percent: number;
};

export type JobCallSummary = {
  total: number;
  running: number;
  queued: number;
  completed: number;
  failed: number;
  unrecognized: number;
};

export function summarizeCalls(calls: readonly ExecutionCall[]): JobCallSummary {
  return {
    total: calls.length,
    running: calls.filter((call) => call.status === "running").length,
    queued: calls.filter((call) => call.status === "queued").length,
    completed: calls.filter((call) => call.status === "completed").length,
    failed: calls.filter((call) => call.status === "failed").length,
    unrecognized: calls.filter((call) => call.status === "unrecognized").length,
  };
}

export function progressOf(calls: readonly ExecutionCall[]): JobProgress | null {
  if (calls.length === 0) return null;
  const settled = calls.filter(isSettled).length;
  return {
    settled,
    total: calls.length,
    percent: Math.round((settled / calls.length) * 100),
  };
}

/** The most recently settled (or still open) Call, by canonical instant. */
export function latestCallOf(calls: readonly ExecutionCall[]): ExecutionCall | null {
  if (calls.length === 0) return null;
  return calls.reduce((latest, call) =>
    (call.finishedAt ?? call.createdAt) >= (latest.finishedAt ?? latest.createdAt) ? call : latest,
  );
}

/** The execution of one canonical Job, ready to render. */
export type JobExecution = {
  apiVersion: string;
  projectId: ProjectId;
  jobId: string;
  parentJobId: string | null;
  childJobIds: string[];
  dependsOn: string[];
  blocks: string[];
  blockedBy: string[];
  blocked: boolean;
  canCancel: boolean;
  canRetry: boolean;
  terminationReason: Failure | null;
  /** The journal cursor this snapshot was taken at. */
  cursor: number;
  /** The canonical Job state, unmodified. */
  state: CanonicalJobState;
  generation: number;
  /** The Attempt the backend named authoritative, or `null` before one exists. */
  authoritativeAttemptId: string | null;
  /** Canonical Job instants; `updatedAt - createdAt` is the only honest elapsed. */
  createdAt: number;
  updatedAt: number;
  attempts: ExecutionAttempt[];
  calls: ExecutionCall[];
  /** Calls owned by the authoritative Attempt, in canonical creation order. */
  authoritativeCalls: ExecutionCall[];
  executors: ExecutionExecutor[];
  dispatchIntents: ExecutionDispatchIntent[];
  summary: JobCallSummary;
  /** Progress over the authoritative Attempt's Calls, or `null` when undecidable. */
  progress: JobProgress | null;
  currentCall: ExecutionCall | null;
  latestCall: ExecutionCall | null;
  activities: CallActivityItem[];
};

export type CallFilters = {
  query?: string;
  status?: CallStatus;
  executorId?: string;
  /** Restrict to one Attempt generation; `undefined` means every generation. */
  attemptGeneration?: number;
};

/** Pure Call filter for the dense execution tables. */
export function filterCalls(
  calls: readonly ExecutionCall[],
  filters: CallFilters = {},
): ExecutionCall[] {
  const query = filters.query?.trim().toLowerCase();
  return calls.filter((call) => {
    if (filters.status !== undefined && call.status !== filters.status) return false;
    if (filters.executorId !== undefined && call.executorId !== filters.executorId) return false;
    if (
      filters.attemptGeneration !== undefined &&
      call.attemptGeneration !== filters.attemptGeneration
    ) {
      return false;
    }
    if (!query) return true;
    return (
      call.callId.toLowerCase().includes(query) ||
      (call.executorId?.toLowerCase().includes(query) ?? false) ||
      call.rawState.toLowerCase().includes(query) ||
      call.effectKind.toLowerCase().includes(query)
    );
  });
}

/** Calls one Executor ran, in canonical creation order. */
export function callsForExecutor(
  calls: readonly ExecutionCall[],
  executor: ExecutionExecutor,
): ExecutionCall[] {
  const ids = new Set(executor.callIds);
  return calls.filter((call) => ids.has(call.id));
}

/** Calls owned by one Attempt generation, in canonical creation order. */
export function callsForGeneration(
  calls: readonly ExecutionCall[],
  generation: number,
): ExecutionCall[] {
  return calls.filter((call) => call.attemptGeneration === generation);
}

/**
 * Assembles the read model from validated canonical entities.
 *
 * `projectCanonicalSnapshot` has already proved the hierarchy (every Attempt
 * belongs to this Job, every Executor and Call to one of its Attempts, every
 * DispatchIntent to this Job and one of its Attempts, and the authoritative
 * Attempt is carried), so this function only projects. It is exported for that
 * one caller and for nothing else.
 */
export function assembleJobExecution(input: {
  apiVersion: string;
  projectId: ProjectId;
  cursor: number;
  job: CanonicalJobRelations & CanonicalJobOperations & {
    termination_reason: Failure | null;
    id: string;
    state: CanonicalJobState;
    generation: number;
    authoritative_attempt_id: string | null;
    created_at: number;
    updated_at: number;
  };
  attempts: readonly CanonicalAttempt[];
  executors: readonly CanonicalExecutor[];
  calls: readonly CanonicalCall[];
  dispatchIntents: readonly CanonicalDispatchIntent[];
}): JobExecution {
  const { projectId } = input;
  const generationByAttempt = new Map(input.attempts.map((attempt) => [attempt.id, attempt.generation]));
  const calls = input.calls.map((call) => toExecutionCall(projectId, call, generationByAttempt));
  const authoritativeCalls = input.job.authoritative_attempt_id === null
    ? []
    : calls.filter((call) => call.attemptId === input.job.authoritative_attempt_id);

  return {
    apiVersion: input.apiVersion,
    projectId,
    jobId: input.job.id,
    parentJobId: input.job.parent_job_id,
    childJobIds: input.job.child_job_ids,
    dependsOn: input.job.depends_on,
    blocks: input.job.blocks,
    blockedBy: input.job.blocked_by,
    blocked: input.job.blocked,
    canCancel: input.job.can_cancel,
    canRetry: input.job.can_retry,
    terminationReason: input.job.termination_reason,
    cursor: input.cursor,
    state: input.job.state,
    generation: input.job.generation,
    authoritativeAttemptId: input.job.authoritative_attempt_id,
    createdAt: input.job.created_at,
    updatedAt: input.job.updated_at,
    attempts: attemptHistoryOf(projectId, input.attempts, calls),
    calls,
    authoritativeCalls,
    executors: executorsOf(projectId, input.executors, calls),
    dispatchIntents: dispatchIntentsOf(projectId, input.dispatchIntents),
    summary: summarizeCalls(calls),
    progress: progressOf(authoritativeCalls),
    currentCall: authoritativeCalls.find((call) => call.status === "running") ?? null,
    latestCall: latestCallOf(calls),
    activities: calls.map((call) => callActivity(call, input.job.id)),
  };
}
