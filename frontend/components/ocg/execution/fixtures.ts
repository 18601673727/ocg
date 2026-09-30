/**
 * Deterministic canonical execution fixtures.
 *
 * Each session's execution is a set of *canonical* entities — a Job, its
 * Attempt history, and the Calls those Attempts own — shaped exactly like the
 * `mission` payload the control plane returns for a real Project. The fixtures
 * are therefore projected by the same `assembleJobExecution` path the backend
 * uses; there is no parallel Mission-shaped fixture model to drift from it.
 *
 * Accounting is ceiling-only. The control plane exposes a hard budget in
 * global, Project-default, or Job configuration, and exposes nothing about what
 * a Job has spent, so no fixture invents a consumption figure.
 */

import type {
  CanonicalAttempt,
  CanonicalCall,
  CanonicalEffectKind,
  CanonicalExecutionState,
  CanonicalJob,
  CanonicalJobState,
} from "../contracts";
import type { ProjectId } from "../project/domain";
import { emptyAccounting, type JobAccounting } from "./accounting";

/** One fixed instant, so every fixture is byte-stable across runs. */
const T0 = 1_750_000_000;

const OBJECTIVE = "Fixture objective recorded by the canonical Job payload.";

type JobSeed = {
  id: string;
  projectId: ProjectId;
  state: CanonicalJobState;
  generation: number;
  authoritativeAttemptId: string | null;
  createdAt?: number;
  updatedAt?: number;
};

function job(seed: JobSeed): CanonicalJob {
  const createdAt = seed.createdAt ?? T0;
  return {
    id: seed.id,
    project_id: seed.projectId,
    state: seed.state,
    generation: seed.generation,
    authoritative_attempt_id: seed.authoritativeAttemptId,
    payload: JSON.stringify({ objective: OBJECTIVE }),
    created_at: createdAt,
    updated_at: seed.updatedAt ?? createdAt,
  };
}

function attempt(
  id: string,
  jobId: string,
  generation: number,
  state: CanonicalAttempt["state"],
  authoritative: boolean,
  createdAt: number,
  finishedAt: number | null,
): CanonicalAttempt {
  return { id, job_id: jobId, generation, state, authoritative, created_at: createdAt, finished_at: finishedAt };
}

type CallSeed = {
  id: string;
  attempt: CanonicalAttempt;
  generation: number;
  executorId: string | null;
  effectKind: CanonicalEffectKind | "read" | "search" | "write" | "validate" | "resource_control" | "message";
  state: string;
  createdAt: number;
  finishedAt: number | null;
  sideEffect?: boolean;
  request?: string;
  response?: string | null;
};

/** `generation` on a Call is the generation of the Attempt it belongs to. */
function call(seed: CallSeed): CanonicalCall {
  const effectKind: CanonicalEffectKind = seed.effectKind === "read"
    ? "idempotent"
    : seed.effectKind === "search"
      ? "reconcilable"
      : seed.effectKind === "write" || seed.effectKind === "resource_control"
        ? "strict_fenced"
        : seed.effectKind === "validate" || seed.effectKind === "message"
          ? "non_retryable"
          : seed.effectKind;
  const sideEffect = seed.sideEffect ?? (effectKind === "strict_fenced" || effectKind === "non_retryable");
  return {
    id: seed.id,
    attempt_id: seed.attempt.id,
    executor_id: seed.executorId,
    generation: seed.attempt.generation,
    side_effect: sideEffect,
    effect_kind: effectKind,
    state: seed.state,
    request: seed.request ?? JSON.stringify({ call: seed.id }),
    response: seed.response ?? null,
    created_at: seed.createdAt,
    finished_at: seed.finishedAt,
  };
}

const GRAPH_NOTE = "canonical state projection";

/* -------------------------------------------------------------------------- */
/* Canonical execution states                                                 */
/* -------------------------------------------------------------------------- */

/** Two generations: a failed first Attempt retained as history, a live second. */
const jobMain = job({ id: "job-main", projectId: "zhuju", state: "running", generation: 2, authoritativeAttemptId: "attempt-main-2", updatedAt: T0 + 480 });
const attemptMain1 = attempt("attempt-main-1", jobMain.id, 1, "failed", false, T0, T0 + 120);
const attemptMain2 = attempt("attempt-main-2", jobMain.id, 2, "running", true, T0 + 120, null);
const mainExecution: CanonicalExecutionState = {
  job: jobMain,
  attempts: [attemptMain1, attemptMain2],
  calls: [
    call({ id: "call-main-1", attempt: attemptMain1, generation: 1, executorId: null, effectKind: "idempotent", state: "completed", createdAt: T0 + 10, finishedAt: T0 + 30 }),
    call({ id: "call-main-2", attempt: attemptMain1, generation: 1, executorId: null, effectKind: "strict_fenced", state: "failed", createdAt: T0 + 30, finishedAt: T0 + 50, response: "Sandbox rejected the resource control before the Attempt was replaced." }),
    call({ id: "call-main-3", attempt: attemptMain2, generation: 2, executorId: "executor-main-a", effectKind: "idempotent", state: "completed", createdAt: T0 + 130, finishedAt: T0 + 150 }),
    call({ id: "call-main-4", attempt: attemptMain2, generation: 2, executorId: "executor-main-a", effectKind: "reconcilable", state: "running", createdAt: T0 + 150, finishedAt: null }),
    call({ id: "call-main-5", attempt: attemptMain2, generation: 2, executorId: "executor-main-b", effectKind: "non_retryable", state: "created", createdAt: T0 + 160, finishedAt: null }),
  ],
  executionGraph: GRAPH_NOTE,
};

const jobCompleted = job({ id: "job-completed", projectId: "zhuju", state: "completed", generation: 1, authoritativeAttemptId: "attempt-completed-1", updatedAt: T0 + 600 });
const attemptCompleted = attempt("attempt-completed-1", jobCompleted.id, 1, "completed", true, T0, T0 + 600);
const completedExecution: CanonicalExecutionState = {
  job: jobCompleted,
  attempts: [attemptCompleted],
  calls: [
    call({ id: "call-completed-1", attempt: attemptCompleted, generation: 1, executorId: "executor-completed-a", effectKind: "read", state: "completed", createdAt: T0 + 10, finishedAt: T0 + 40 }),
    call({ id: "call-completed-2", attempt: attemptCompleted, generation: 1, executorId: "executor-completed-a", effectKind: "write", state: "completed", createdAt: T0 + 40, finishedAt: T0 + 120, response: "Settled the selected change." }),
  ],
  executionGraph: GRAPH_NOTE,
};

/** Every Call shape the snapshot admits: settled, live, and unstarted. */
const jobCallHeavy = job({ id: "job-call-heavy", projectId: "ocg", state: "running", generation: 1, authoritativeAttemptId: "attempt-call-heavy-1", updatedAt: T0 + 900 });
const attemptCallHeavy = attempt("attempt-call-heavy-1", jobCallHeavy.id, 1, "running", true, T0, null);
const callHeavyExecution: CanonicalExecutionState = {
  job: jobCallHeavy,
  attempts: [attemptCallHeavy],
  calls: [
    call({ id: "call-heavy-1", attempt: attemptCallHeavy, generation: 1, executorId: "executor-heavy-a", effectKind: "read", state: "completed", createdAt: T0 + 10, finishedAt: T0 + 30 }),
    call({ id: "call-heavy-2", attempt: attemptCallHeavy, generation: 1, executorId: "executor-heavy-a", effectKind: "resource_control", state: "completed", createdAt: T0 + 30, finishedAt: T0 + 50 }),
    call({ id: "call-heavy-3", attempt: attemptCallHeavy, generation: 1, executorId: "executor-heavy-b", effectKind: "write", state: "completed", createdAt: T0 + 50, finishedAt: T0 + 70 }),
    call({ id: "call-heavy-4", attempt: attemptCallHeavy, generation: 1, executorId: "executor-heavy-b", effectKind: "resource_control", state: "running", createdAt: T0 + 70, finishedAt: null }),
    call({ id: "call-heavy-5", attempt: attemptCallHeavy, generation: 1, executorId: "executor-heavy-c", effectKind: "read", state: "completed", createdAt: T0 + 80, finishedAt: T0 + 90 }),
    call({ id: "call-heavy-6", attempt: attemptCallHeavy, generation: 1, executorId: "executor-heavy-c", effectKind: "search", state: "created", createdAt: T0 + 90, finishedAt: null }),
    call({ id: "call-heavy-7", attempt: attemptCallHeavy, generation: 1, executorId: null, effectKind: "message", state: "cancelled", createdAt: T0 + 95, finishedAt: T0 + 100 }),
  ],
  executionGraph: GRAPH_NOTE,
};

/** One Attempt whose Calls are spread across four distinct Executors. */
const jobParallel = job({ id: "job-parallel", projectId: "ocg", state: "running", generation: 1, authoritativeAttemptId: "attempt-parallel-1", updatedAt: T0 + 300 });
const attemptParallel = attempt("attempt-parallel-1", jobParallel.id, 1, "running", true, T0, null);
const parallelExecution: CanonicalExecutionState = {
  job: jobParallel,
  attempts: [attemptParallel],
  calls: [
    call({ id: "call-parallel-1", attempt: attemptParallel, generation: 1, executorId: "executor-lead", effectKind: "read", state: "completed", createdAt: T0 + 10, finishedAt: T0 + 40 }),
    call({ id: "call-parallel-2", attempt: attemptParallel, generation: 1, executorId: "executor-explore", effectKind: "search", state: "completed", createdAt: T0 + 10, finishedAt: T0 + 50 }),
    call({ id: "call-parallel-3", attempt: attemptParallel, generation: 1, executorId: "executor-build", effectKind: "write", state: "running", createdAt: T0 + 20, finishedAt: null }),
    call({ id: "call-parallel-4", attempt: attemptParallel, generation: 1, executorId: "executor-verify", effectKind: "validate", state: "queued", createdAt: T0 + 30, finishedAt: null }),
    call({ id: "call-parallel-5", attempt: attemptParallel, generation: 1, executorId: "executor-lead", effectKind: "resource_control", state: "created", createdAt: T0 + 40, finishedAt: null }),
  ],
  executionGraph: GRAPH_NOTE,
};

/** A live Call in an uninterpretable state, to keep the unknown path visible. */
const jobUnknownState = job({ id: "job-unknown-state", projectId: "zhuju", state: "running", generation: 1, authoritativeAttemptId: "attempt-unknown-1", updatedAt: T0 + 240 });
const attemptUnknown = attempt("attempt-unknown-1", jobUnknownState.id, 1, "running", true, T0, null);
const unknownStateExecution: CanonicalExecutionState = {
  job: jobUnknownState,
  attempts: [attemptUnknown],
  calls: [
    call({ id: "call-unknown-1", attempt: attemptUnknown, generation: 1, executorId: "executor-unknown-a", effectKind: "read", state: "completed", createdAt: T0 + 10, finishedAt: T0 + 40 }),
    call({ id: "call-unknown-2", attempt: attemptUnknown, generation: 1, executorId: "executor-unknown-a", effectKind: "write", state: "reconciling", createdAt: T0 + 40, finishedAt: null }),
  ],
  executionGraph: GRAPH_NOTE,
};

const jobFailed = job({ id: "job-failed", projectId: "zhuju", state: "failed", generation: 1, authoritativeAttemptId: "attempt-failed-1", updatedAt: T0 + 180 });
const attemptFailed = attempt("attempt-failed-1", jobFailed.id, 1, "failed", true, T0, T0 + 180);
const failedExecution: CanonicalExecutionState = {
  job: jobFailed,
  attempts: [attemptFailed],
  calls: [
    call({ id: "call-failed-1", attempt: attemptFailed, generation: 1, executorId: "executor-failed-a", effectKind: "validate", state: "failed", createdAt: T0 + 10, finishedAt: T0 + 50, response: "Sandbox capability check failed." }),
  ],
  executionGraph: GRAPH_NOTE,
};

const jobCecece = job({ id: "job-cecece", projectId: "cecece", state: "failed", generation: 1, authoritativeAttemptId: "attempt-cecece-1", updatedAt: T0 + 150 });
const attemptCecece = attempt("attempt-cecece-1", jobCecece.id, 1, "failed", true, T0, T0 + 150);
const cececeExecution: CanonicalExecutionState = {
  job: jobCecece,
  attempts: [attemptCecece],
  calls: [
    call({ id: "call-cecece-1", attempt: attemptCecece, generation: 1, executorId: "executor-cecece-a", effectKind: "message", state: "failed", createdAt: T0 + 10, finishedAt: T0 + 40, response: "Provider requirement is not satisfied for this Project." }),
  ],
  executionGraph: GRAPH_NOTE,
};

/* -------------------------------------------------------------------------- */
/* Session fixtures                                                           */
/* -------------------------------------------------------------------------- */

const EXECUTION_BY_SESSION: Record<string, CanonicalExecutionState> = {
  "chat-zhuju-main": mainExecution,
  "chat-route-1": completedExecution,
  "chat-tool-gateway": callHeavyExecution,
  "chat-design-system": parallelExecution,
  "chat-worker-pool": parallelExecution,
  "chat-paused-budget": unknownStateExecution,
  "chat-cecece-billing": failedExecution,
  "chat-cecece-1": cececeExecution,
};

/** The canonical execution the session's Job records, or `null` for none. */
export function executionFixtureForSession(sessionId: string): CanonicalExecutionState | null {
  return EXECUTION_BY_SESSION[sessionId] ?? null;
}

/** Legacy scenario adapter; canonical fixtures remain the source of truth. */
export function createMissionControlExecution(): import("./domain").MissionExecution {
  return {
    missionId: "job-main",
    title: "Canonical Job",
    status: "running",
    taskIds: [],
    edgeIds: [],
    workerIds: [],
    tasks: [],
    edges: [],
    workers: [],
    waves: [],
    gates: [],
    activities: [],
    summary: { completed: 0, total: 0, running: 0, waiting: 0, blocked: 0, failed: 0, retrying: 0 },
  };
}

/** Project-scoped ceilings the configuration surface records. */
const PROJECT_CEILING: Partial<Record<ProjectId, JobAccounting["ceiling"]>> = {
  zhuju: { amount: 25, unit: "USD", source: "project-default" },
  ocg: { amount: 100, unit: "USD", source: "global-default" },
  "route-lace": { amount: 40, unit: "USD", source: "project-default" },
  cecece: { amount: 5, unit: "USD", source: "job-configuration" },
};

/**
 * The accounting context for a session's Job.
 *
 * Only the ceiling is populated. A Project with no recorded ceiling, and every
 * session with no Job, report `emptyAccounting()` rather than a zero budget.
 */
export function accountingFixtureForSession(sessionId: string, projectId: ProjectId): JobAccounting {
  if (executionFixtureForSession(sessionId) === null) return emptyAccounting();
  const ceiling = PROJECT_CEILING[projectId] ?? null;
  return { ceiling, consumption: null };
}
