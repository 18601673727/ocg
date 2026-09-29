/**
 * Backend-backed canonical execution projection.
 *
 * The SQLite substrate is the execution authority. A Project owns Jobs, a Job
 * owns Attempts, an Attempt owns Executors and Calls, and exactly one Attempt
 * of a Job is authoritative at a time. This module is the *projection*
 * boundary: it validates the durable contract and maps it onto the existing
 * RuntimeStore presentation shapes.
 *
 * Invariants preserved here:
 * - the API version, Project identity and canonical cursor are carried through
 *   unchanged, so a stale generation or a foreign Project payload is rejected
 *   rather than rendered;
 * - a Job, Attempt, Executor or Call is never invented, and the frontend derives
 *   no execution identity of its own. It reads the authoritative Attempt the
 *   backend named and shows every other generation as history rather than
 *   promoting one;
 * - the frontend owns no execution authority. It cannot admit, complete or
 *   replace a Call, and it never decides which Attempt is authoritative.
 */

import type {
  ExecutionAttempt,
  ExecutionActivityItem,
  ExecutionEdge,
  ExecutionStatus,
  ExecutionTask,
  MissionExecution,
  WorkerExecution,
} from "../execution/domain";
import type { ProjectId } from "../project/domain";

// The protocol version is owned by Rust and projected here, so the envelope
// validator and the control client cannot disagree with the definition that
// actually confers authority.
export { CANONICAL_API_VERSION } from "../contracts";

import { CANONICAL_API_VERSION, ContractError, decodeExecutionState } from "../contracts";
import type {
  CanonicalAttempt,
  CanonicalAttemptState,
  CanonicalCall,
  CanonicalExecutionState,
  CanonicalJob,
  CanonicalJobState,
} from "../contracts";
import { formatTimestamp } from "@/lib/format";
import { isNonNegativeInteger, isRecord } from "@/lib/narrow";

export const CANONICAL_STREAM_ID = "ocg.canonical.work";

/** The validated backend snapshot, in the substrate's own entities. */
export type CanonicalExecutionProjection = {
  apiVersion: string;
  projectId: ProjectId;
  jobId: string;
  cursor: number;
  job: CanonicalJob;
  attempts: CanonicalAttempt[];
  calls: CanonicalCall[];
  /** The backend's note on its graph projection; the PWA draws no graph itself. */
  executionGraph: string;
};

export type CanonicalProjectionIssue = {
  code: "api-version" | "malformed" | "project-mismatch" | "cursor";
  message: string;
};

export type CanonicalProjectionResult =
  | { ok: true; projection: CanonicalExecutionProjection }
  | { ok: false; issue: CanonicalProjectionIssue };

function fail(code: CanonicalProjectionIssue["code"], message: string): CanonicalProjectionResult {
  return { ok: false, issue: { code, message } };
}

/**
 * Validate one backend snapshot against the durable canonical contract.
 *
 * A wrong API version, a foreign Project identity, a cursor behind its own
 * event stream, or a Job/Attempt/Call that does not match the contract is
 * rejected here; nothing is partially rendered.
 */
export function projectCanonicalSnapshot(
  input: unknown,
  expectedProjectId: ProjectId,
): CanonicalProjectionResult {
  if (!isRecord(input)) return fail("malformed", "Canonical snapshot must be an object.");
  if (input.api_version !== CANONICAL_API_VERSION) {
    return fail("api-version", `Unsupported canonical API version "${String(input.api_version)}".`);
  }
  if (input.project_id !== expectedProjectId) {
    return fail("project-mismatch", "Canonical snapshot belongs to another Project.");
  }
  if (!isNonNegativeInteger(input.cursor)) {
    return fail("cursor", "Canonical snapshot cursor must be a non-negative integer.");
  }

  let state: CanonicalExecutionState;
  try {
    state = decodeExecutionState(input.mission, "mission");
  } catch (error) {
    if (error instanceof ContractError) return fail("malformed", error.message);
    throw error;
  }

  // The Job carries its own Project scope, so a snapshot whose envelope and
  // Job disagree is a cross-Project leak rather than a rendering problem.
  if (state.job.project_id !== expectedProjectId) {
    return fail("project-mismatch", `Canonical Job ${state.job.id} belongs to another Project.`);
  }

  // Hierarchy integrity: an Attempt belongs to this Job, and a Call belongs to
  // one of its Attempts. The backend builds the lists that way, so a violation
  // means the payload is not the projection of one Job.
  const attemptIds = new Set(state.attempts.map((item) => item.id));
  for (const item of state.attempts) {
    if (item.job_id !== state.job.id) {
      return fail("malformed", `Attempt ${item.id} does not belong to Job ${state.job.id}.`);
    }
  }
  for (const item of state.calls) {
    if (!attemptIds.has(item.attempt_id)) {
      return fail("malformed", `Call ${item.id} names an Attempt the snapshot does not carry.`);
    }
  }
  // The authoritative Attempt is the backend's decision. A snapshot that names
  // an Attempt it does not carry is refused rather than rendered as unattached.
  const authoritative = state.job.authoritative_attempt_id;
  if (authoritative !== null && !attemptIds.has(authoritative)) {
    return fail("malformed", `Job ${state.job.id} names an authoritative Attempt it does not carry.`);
  }

  // The event tail is synthesized one event per Attempt, so a cursor that does
  // not cover its own Attempts is behind the stream it claims to summarize.
  if (input.cursor < state.attempts.length) {
    return fail("cursor", "Canonical cursor is behind its own event stream.");
  }

  return {
    ok: true,
    projection: {
      apiVersion: CANONICAL_API_VERSION,
      projectId: expectedProjectId,
      jobId: state.job.id,
      cursor: input.cursor,
      job: state.job,
      attempts: state.attempts,
      calls: state.calls,
      executionGraph: state.executionGraph,
    },
  };
}

/* -------------------------------------------------------------------------- */
/* Presentation projection                                                    */
/* -------------------------------------------------------------------------- */

/**
 * Job state as the read model reports it.
 *
 * `unknown` and `orphaned` are the substrate's own words for "this Job has no
 * authority behind it any more", which the read model shows as blocked rather
 * than guessing an outcome for it.
 */
const JOB_STATUS: Record<CanonicalJobState, MissionExecution["status"]> = {
  pending: "waiting",
  eligible: "waiting",
  running: "running",
  cancelling: "running",
  completed: "completed",
  failed: "failed",
  cancelled: "cancelled",
  unknown: "blocked",
  orphaned: "blocked",
};

/**
 * Call state as the read model reports it.
 *
 * `domain_calls.state` is an unconstrained column, so the vocabulary the
 * substrate writes is spelled out here and anything else stays visible as its
 * raw value instead of being coerced into a status it might contradict.
 */
const CALL_STATUS: Record<string, ExecutionStatus> = {
  created: "queued",
  running: "running",
  completed: "completed",
  failed: "failed",
  unknown: "blocked",
};

/** Attempt state as the read model's three-valued attempt history reports it. */
const ATTEMPT_STATUS: Record<CanonicalAttemptState, ExecutionAttempt["status"]> = {
  queued: "running",
  running: "running",
  cancelling: "running",
  completed: "completed",
  failed: "failed",
  cancelled: "failed",
  unknown: "failed",
  orphaned: "failed",
};

const ENTITY_PREFIX = {
  job: "job",
  call: "call",
  executor: "executor",
} as const;

/** A stable, Project-scoped presentation id for one canonical entity. */
export function canonicalEntityId(projectId: ProjectId, kind: keyof typeof ENTITY_PREFIX, id: string): string {
  return `${projectId}:${ENTITY_PREFIX[kind]}:${id}`;
}

function callStatus(call: CanonicalCall): ExecutionStatus {
  return CALL_STATUS[call.state] ?? "blocked";
}

function callReason(call: CanonicalCall, status: ExecutionStatus): string | undefined {
  if (status === "blocked" && CALL_STATUS[call.state] === undefined) {
    return `Call state "${call.state}" is not one this view interprets.`;
  }
  if (status === "failed" || status === "blocked") return call.response ?? undefined;
  return undefined;
}

/** The substrate stores epoch seconds; the read model counts milliseconds. */
function elapsedMs(call: CanonicalCall): number | undefined {
  if (call.finished_at === null) return undefined;
  return Math.max(0, (call.finished_at - call.created_at) * 1000);
}

function instant(seconds: number): string {
  return new Date(seconds * 1000).toISOString();
}

function callActivity(
  projectId: ProjectId,
  jobId: string,
  call: CanonicalCall,
  taskId: string,
  attempt: CanonicalAttempt | undefined,
): ExecutionActivityItem {
  const status = callStatus(call);
  const settled = call.finished_at ?? call.created_at;
  const message = callReason(call, status) ?? `Call ${call.state}`;
  const kind =
    status === "completed"
      ? "task-completed"
      : status === "running"
        ? "task-started"
        : status === "queued"
          ? "task-waiting"
          : status === "failed" || status === "blocked"
            ? "task-blocked"
            : "mission-transition";
  return {
    id: `${jobId}:call:${call.id}:${settled}`,
    elapsedMs: Math.max(0, (settled - call.created_at) * 1000),
    timestamp: formatTimestamp(instant(settled)),
    missionId: jobId,
    taskId,
    workerId:
      call.executor_id === null
        ? undefined
        : canonicalEntityId(projectId, "executor", call.executor_id),
    kind,
    message: attempt === undefined ? message : `Generation ${attempt.generation} · ${message}`,
    status,
  };
}

/**
 * Project the canonical execution state onto the existing Execution Graph and
 * Mission Control read model.
 *
 * A Call is the unit of work, an Attempt is one generation of the Job that owns
 * it, and an Executor is whoever ran it — those are the only three relationships
 * the substrate reports, so those are the only ones drawn. The Job's dependency
 * edges live in the substrate but are not part of this projection, so the graph
 * is a flat list rather than a DAG the PWA would have to invent edges for.
 */
export function toMissionExecution(
  projection: CanonicalExecutionProjection,
  title: string,
): MissionExecution {
  const { projectId, jobId } = projection;
  const attemptById = new Map(projection.attempts.map((item) => [item.id, item]));

  const tasks: ExecutionTask[] = projection.calls.map((call) => {
    const attempt = attemptById.get(call.attempt_id);
    const status = callStatus(call);
    const workerId =
      call.executor_id === null
        ? undefined
        : canonicalEntityId(projectId, "executor", call.executor_id);
    return {
      id: canonicalEntityId(projectId, "call", call.id),
      missionId: jobId,
      title: call.id,
      description: [
        attempt === undefined ? undefined : `Attempt ${attempt.id} generation ${attempt.generation}`,
        call.side_effect ? "side effect" : "no side effect",
        call.effect_kind,
      ]
        .filter(Boolean)
        .join(" · "),
      kind: "task",
      category: call.effect_kind,
      status,
      // The projection carries no prerequisite edges, so none are claimed.
      dependencies: [],
      dependents: [],
      workerId,
      startedAt: instant(call.created_at),
      finishedAt: call.finished_at === null ? undefined : instant(call.finished_at),
      elapsedMs: elapsedMs(call),
      blockedReason: callReason(call, status),
      outputSummary: call.response ?? undefined,
    };
  });

  const taskIdByCall = new Map(projection.calls.map((call, index) => [call.id, tasks[index]!.id]));

  const workers: WorkerExecution[] = [
    ...new Set(projection.calls.map((call) => call.executor_id).filter((id): id is string => id !== null)),
  ].map((executorId) => {
    const owned = projection.calls.filter((call) => call.executor_id === executorId);
    const statuses = owned.map(callStatus);
    const running = owned.find((call) => callStatus(call) === "running");
    const status: WorkerExecution["status"] = statuses.includes("running")
      ? "active"
      : statuses.every((item) => item === "completed")
        ? "completed"
        : statuses.some((item) => item === "failed" || item === "blocked")
          ? "blocked"
          : "queued";
    return {
      id: canonicalEntityId(projectId, "executor", executorId),
      role: "worker",
      label: executorId,
      status,
      currentTaskId: running === undefined ? undefined : taskIdByCall.get(running.id),
      completedTaskIds: owned
        .filter((call) => callStatus(call) === "completed")
        .map((call) => taskIdByCall.get(call.id) ?? ""),
      invocationCount: owned.length,
    };
  });

  const edges: ExecutionEdge[] = [];

  const summary = tasks.reduce(
    (counts, task) => {
      if (task.status === "completed") counts.completed += 1;
      if (task.status === "running") counts.running += 1;
      if (task.status === "ready" || task.status === "queued") counts.waiting += 1;
      if (task.status === "blocked" || task.status === "waiting") counts.blocked += 1;
      if (task.status === "failed") counts.failed += 1;
      if (task.status === "retrying") counts.retrying += 1;
      counts.total += 1;
      return counts;
    },
    { completed: 0, running: 0, waiting: 0, blocked: 0, failed: 0, retrying: 0, total: 0 },
  );

  return {
    missionId: jobId,
    title,
    status: JOB_STATUS[projection.job.state],
    taskIds: tasks.map((task) => task.id),
    edgeIds: edges.map((edge) => edge.id),
    workerIds: workers.map((worker) => worker.id),
    tasks,
    edges,
    workers,
    // The substrate has no waves: an Attempt is a generation of the same Job,
    // not a batch of parallel work, so none are claimed.
    waves: [],
    gates: [],
    activities: projection.calls.map((call) =>
      callActivity(
        projectId,
        jobId,
        call,
        taskIdByCall.get(call.id) ?? call.id,
        attemptById.get(call.attempt_id),
      ),
    ),
    summary,
    nextTaskIds: [],
  };
}

/** The Attempts of the projected Job, ordered by generation. */
export function selectAttemptHistory(
  projection: CanonicalExecutionProjection,
): ExecutionAttempt[] {
  return projection.attempts
    .slice()
    .sort((left, right) => left.generation - right.generation)
    .map((attempt) => ({
      number: attempt.generation,
      status: ATTEMPT_STATUS[attempt.state],
      reason: attempt.authoritative ? "authoritative generation" : "superseded generation",
    }));
}
