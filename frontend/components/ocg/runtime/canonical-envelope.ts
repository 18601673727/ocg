/**
 * Backend-backed canonical execution projection.
 *
 * The SQLite substrate is the execution authority. A Project owns Jobs, a
 * Job owns Attempts, an Attempt owns Executors and Calls, and exactly one
 * Attempt of a Job is authoritative at a time. This module is the
 * projection boundary: it validates the durable contract and maps it onto
 * the existing RuntimeStore presentation shapes.
 *
 * Invariants preserved here:
 * - the API version, Project identity and canonical cursor are carried
 *   through unchanged, so a stale generation or a foreign Project payload
 *   is rejected rather than rendered;
 * - a Job, Attempt, Executor or Call is never invented, and the frontend
 *   derives no execution identity of its own. It reads the authoritative
 *   Attempt the backend named and shows every other generation as
 *   history rather than promoting one;
 * - the frontend owns no execution authority. It cannot admit, complete
 *   or replace a Call, and it never decides which Attempt is
 *   authoritative.
 */

import type { ProjectId } from "../project/domain";
import { CANONICAL_API_VERSION, ContractError, decodeExecutionState } from "../contracts";
import type { CanonicalExecutionState, CanonicalJobSnapshot } from "../contracts";
import type { JobExecution } from "../execution/domain";
import { assembleJobExecution } from "../execution/domain";
import { isNonNegativeInteger, isRecord } from "@/lib/narrow";

export { CANONICAL_API_VERSION } from "../contracts";

export const CANONICAL_STREAM_ID = "ocg.canonical.jobs";

/** The validated backend snapshot, in the substrate's own entities. */
export type CanonicalExecutionProjection = {
  apiVersion: string;
  projectId: ProjectId;
  jobId: string;
  cursor: number;
  job: CanonicalExecutionState["job"];
  attempts: CanonicalExecutionState["attempts"];
  executors: CanonicalExecutionState["executors"];
  calls: CanonicalExecutionState["calls"];
  dispatchIntents: CanonicalExecutionState["dispatchIntents"];
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
 * A wrong API version, a foreign Project identity, a malformed cursor, or a
 * Job/Attempt/Executor/Call/DispatchIntent that does not match the contract is
 * rejected here; nothing is partially rendered. The cursor is a position in
 * the backend's execution journal, not a count of entities, so it is checked
 * for well-formedness only and never against the number of Attempts.
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
    state = decodeExecutionState(input.job, "job");
  } catch (error) {
    if (error instanceof ContractError) return fail("malformed", error.message);
    throw error;
  }

  if (state.job.project_id !== expectedProjectId) {
    return fail("project-mismatch", `Canonical Job ${state.job.id} belongs to another Project.`);
  }

  const attemptIds = new Set(state.attempts.map((item) => item.id));
  for (const item of state.attempts) {
    if (item.job_id !== state.job.id) {
      return fail("malformed", `Attempt ${item.id} does not belong to Job ${state.job.id}.`);
    }
  }
  for (const item of state.executors) {
    if (!attemptIds.has(item.attempt_id)) {
      return fail("malformed", `Executor ${item.id} names an Attempt the snapshot does not carry.`);
    }
  }
  for (const item of state.calls) {
    if (!attemptIds.has(item.attempt_id)) {
      return fail("malformed", `Call ${item.id} names an Attempt the snapshot does not carry.`);
    }
  }
  for (const item of state.dispatchIntents) {
    if (item.job_id !== state.job.id) {
      return fail("malformed", `DispatchIntent ${item.id} does not belong to Job ${state.job.id}.`);
    }
    if (!attemptIds.has(item.attempt_id)) {
      return fail("malformed", `DispatchIntent ${item.id} names an Attempt the snapshot does not carry.`);
    }
  }
  const authoritative = state.job.authoritative_attempt_id;
  if (authoritative !== null && !attemptIds.has(authoritative)) {
    return fail("malformed", `Job ${state.job.id} names an authoritative Attempt it does not carry.`);
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
      executors: state.executors,
      calls: state.calls,
      dispatchIntents: state.dispatchIntents,
      executionGraph: state.executionGraph,
    },
  };
}

/**
 * Assemble a `JobExecution` read model from a validated canonical
 * projection, or return the diagnostic when validation failed.
 *
 * Callers (the store) keep the raw projection for the event log and
 * the assembled read model for the UI; neither is rendered when the
 * other failed.
 */
export function assembleJobExecutionFromProjection(
  projection: CanonicalExecutionProjection,
): JobExecution {
  return assembleJobExecution({
    apiVersion: projection.apiVersion,
    projectId: projection.projectId,
    cursor: projection.cursor,
    job: projection.job,
    attempts: projection.attempts,
    executors: projection.executors,
    calls: projection.calls,
    dispatchIntents: projection.dispatchIntents,
  });
}

/**
 * The canonical tail syncs with the same vocabulary as the event stream.
 */
export type CanonicalSyncStatus = "uninitialized" | "loading-snapshot" | "live" | "reconnecting" | "stale" | "error";

export type CanonicalDiagnosticCode =
  | "snapshot-accepted"
  | "snapshot-rejected"
  | "snapshot-stale"
  | "generation-stale"
  | "generation-unknown"
  | "sequence-stale"
  | "sequence-gap"
  | "duplicate-event"
  | "project-mismatch"
  | "resync-required"
  | "command-rejected";

export type CanonicalDiagnostic = {
  code: CanonicalDiagnosticCode;
  severity: "info" | "warning" | "error";
  message: string;
  eventId?: string;
  sequence?: number;
};

export type CanonicalCommandAck = {
  commandId: string;
  kind: "project-import" | "global-config" | "project-defaults" | "job-config";
  accepted: boolean;
  revision?: number;
  message: string;
};

export type CanonicalBackendEvent = CanonicalJobSnapshot;

export type CanonicalState = {
  status: CanonicalSyncStatus;
  streamId: string;
  generation: number;
  cursor: number;
  projectId: ProjectId | null;
  jobId: string | null;
  projection: CanonicalExecutionProjection | null;
  seenEventIds: string[];
  diagnostics: CanonicalDiagnostic[];
  commandAcks: Record<string, CanonicalCommandAck>;
  resyncRequired: boolean;
};

export type CanonicalSnapshotInput = {
  payload: unknown;
  projectId: ProjectId;
  generation?: number;
};

export function createCanonicalState(): CanonicalState {
  return {
    status: "uninitialized",
    streamId: CANONICAL_STREAM_ID,
    generation: 0,
    cursor: 0,
    projectId: null,
    jobId: null,
    projection: null,
    seenEventIds: [],
    diagnostics: [],
    commandAcks: {},
    resyncRequired: false,
  };
}


