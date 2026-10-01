/**
 * Backend-backed Job launch adapter.
 *
 * The shell runtime is a fixture projection, but a Job launch is a real
 * product side effect. This client extends `MockOcgRuntimeClient` so the shell
 * keeps its single runtime store, and overrides exactly one operation:
 * `launchJob`.
 *
 * A launch is:
 *   1. `POST /api/v1/canonical/jobs/launch`, decoded against the generated
 *      `JobLaunchResponse` contract;
 *   2. on acceptance, a read of the authoritative canonical snapshot and its
 *      event tail for the Job the backend named;
 *   3. projection of that snapshot through the same
 *      `RuntimeStore`/`canonical-store` path the control surface uses, so the
 *      `JobExecution` the UI renders is assembled from backend entities by
 *      `assembleJobExecution` and never fabricated here.
 *
 * No credential, provider selection, or execution identity is decided in the
 * client: the backend freezes all of it and this adapter only reports what it
 * returned.
 */

import type { ProjectId } from "../project/domain";
import type { JobLaunchRequest } from "../contracts";
import type { JobLaunchCommand, JobLaunchResult, ScenarioId } from "./runtime-types";
import { MockOcgRuntimeClient } from "./mock-client";
import {
  createHttpCanonicalControlClient,
  isCanonicalRejection,
  type CanonicalControlClient,
  type CanonicalJobLaunchAck,
} from "./canonical-client";
import type { JobExecution } from "../execution/domain";

/** Bounds the snapshot/event refetch loop when the backend keeps demanding a resync. */
const MAX_REFRESH_ROUNDS = 4;

export class CanonicalOcgRuntimeClient extends MockOcgRuntimeClient {
  private readonly control: CanonicalControlClient;

  constructor(scenario: ScenarioId, control: CanonicalControlClient) {
    super(scenario);
    this.control = control;
  }

  /** Build the adapter for a loopback control base URL. */
  static connect(scenario: ScenarioId, baseUrl: string, fetchImpl: typeof fetch): CanonicalOcgRuntimeClient {
    return new CanonicalOcgRuntimeClient(
      scenario,
      createHttpCanonicalControlClient({ baseUrl, fetch: fetchImpl }),
    );
  }

  async launchJob(command: JobLaunchCommand): Promise<JobLaunchResult> {
    const response = await this.control.launchJob(toLaunchRequest(command));
    if (isCanonicalRejection(response)) {
      const result = failedLaunch(command, response.message);
      this.emitLaunchResult(command, result);
      return result;
    }

    const result = launchResultFrom(command, response);
    this.emitLaunchResult(command, result);

    if (result.outcome === "accepted" && response.job_id !== null) {
      const execution = await this.projectCanonicalExecution(response.project_id, response.job_id);
      if (execution !== null) {
        // The canonical execution's Project is the backend's identity, which is
        // not necessarily a Project this fixture shell knows, so the update is
        // emitted unscoped: the reconciler must not drop it as foreign.
        this.emit(
          { type: "job.execution-updated", sessionId: command.sessionId, execution, accounting: null },
          { projectId: null, commandId: command.commandId },
        );
      }
    }

    return result;
  }

  private emitLaunchResult(command: JobLaunchCommand, result: JobLaunchResult): void {
    this.emit(
      { type: "job.launch-updated", sessionId: command.sessionId, result },
      { projectId: null, commandId: command.commandId },
    );
  }

  /**
   * Read the authoritative canonical snapshot and project it.
   *
   * The canonical store projects whole snapshots and never advances its cursor
   * from a raw event, so an event tail means the installed snapshot is behind:
   * the loop refetches until the tail is empty. Each round advances the cursor
   * together with the projection it belongs to.
   */
  private async projectCanonicalExecution(
    projectId: string,
    jobId: string,
  ): Promise<JobExecution | null> {
    const scoped: ProjectId = projectId;
    for (let attempt = 0; attempt < MAX_REFRESH_ROUNDS; attempt += 1) {
      const snapshot = await this.control.readJobSnapshot(projectId, jobId);
      if (isCanonicalRejection(snapshot)) return null;

      const generation = this.store.getCanonical().generation + 1;
      const next = this.store.applyCanonicalSnapshot({ payload: snapshot, projectId: scoped, generation });
      // A rejected or stale snapshot leaves the projection and its cursor
      // untouched, so there is no coherent new tail to read.
      if (next.projection === null || next.cursor !== snapshot.cursor) return null;

      const events = await this.control.readJobEvents(projectId, jobId, next.cursor);
      if (isCanonicalRejection(events)) return this.store.selectCanonical().execution;

      const applied = this.store.applyCanonicalEvents(events, { projectId: scoped, generation });
      if (!applied.resyncRequired) return this.store.selectCanonical().execution;
    }
    return this.store.selectCanonical().execution;
  }
}

function toLaunchRequest(command: JobLaunchCommand): JobLaunchRequest {
  return {
    command_id: command.commandId,
    draft_id: command.draftId,
    project_id: command.projectId,
    session_id: command.sessionId,
    objective: command.objective,
    success_criteria: command.successCriteria ?? null,
    constraints: command.constraints ?? null,
    hard_budget_micros: command.hardBudgetMicros,
    resource_commitment: command.resourceCommitment ?? null,
  };
}

function launchResultFrom(command: JobLaunchCommand, response: CanonicalJobLaunchAck): JobLaunchResult {
  return {
    outcome: response.outcome,
    commandId: response.command_id,
    draftId: response.draft_id,
    projectId: response.project_id,
    sessionId: response.session_id,
    ...(response.job_id !== null ? { jobId: response.job_id } : {}),
    message: response.message,
    duplicate: response.duplicate,
  };
}

function failedLaunch(command: JobLaunchCommand, message: string): JobLaunchResult {
  return {
    outcome: "failed",
    commandId: command.commandId,
    draftId: command.draftId,
    projectId: command.projectId,
    sessionId: command.sessionId,
    message,
    duplicate: false,
  };
}
