import { test } from "node:test";
import assert from "node:assert/strict";
import {
  CANONICAL_API_VERSION,
  isPreRunConfigurationMutable,
  projectCanonicalSnapshot,
  toMissionExecution,
  type CanonicalWorkProjection,
} from "./canonical-envelope";
import type { ProjectId } from "../project/domain";

const PROJECT = "zhuju" as ProjectId;

function backendSnapshot(overrides: Record<string, unknown> = {}) {
  return {
    api_version: CANONICAL_API_VERSION,
    project_id: PROJECT,
    cursor: 6,
    mission: {
      mission_id: "wn-dogfood",
      root_node_id: 0,
      work_nodes: [
        {
          node_id: 0,
          parent_node_id: null,
          spawned_by_run_id: null,
          state: "running",
          generation: 1,
          active_run_id: 0,
          payload: "define and implement the canonical execution inspector",
        },
        {
          node_id: 1,
          parent_node_id: 0,
          spawned_by_run_id: 0,
          state: "completed",
          generation: 1,
          active_run_id: null,
          payload: "A: trace the substrate",
        },
        {
          node_id: 2,
          parent_node_id: 0,
          spawned_by_run_id: 0,
          state: "running",
          generation: 2,
          active_run_id: 2,
          payload: "B: backend canonical inspection",
        },
      ],
      dependencies: [{ node_id: 2, depends_on_node_id: 1 }],
      runs: [
        {
          run_id: 0,
          node_id: 0,
          generation: 1,
          state: "active",
          contract: { executor: "lead-high", model: "openai/gpt-6-astra", role: "lead" },
          runtime_execution_id: "lead-session",
          host_session_id: "lead-session",
          result: null,
          witness: {
            mission_id: "wn-dogfood",
            work_node_id: 0,
            run_id: 0,
            run_generation: 1,
            runtime_execution_id: "lead-session",
            dispatch_id: "d-aaaa-r0-g1",
          },
        },
        {
          run_id: 1,
          node_id: 1,
          generation: 1,
          state: "completed",
          contract: { executor: "ocg-explore", model: "openai/gpt-6-astra", role: "explore" },
          runtime_execution_id: "session-a",
          host_session_id: "session-a",
          result: "A done",
          witness: {
            mission_id: "wn-dogfood",
            work_node_id: 1,
            run_id: 1,
            run_generation: 1,
            runtime_execution_id: "session-a",
            dispatch_id: "d-bbbb-r1-g1",
          },
        },
        {
          run_id: 2,
          node_id: 2,
          generation: 1,
          state: "fenced",
          contract: { executor: "ocg-build", model: "openai/gpt-6-astra", role: "build" },
          runtime_execution_id: "session-b1",
          host_session_id: "session-b1",
          result: "provider lost",
          witness: {
            mission_id: "wn-dogfood",
            work_node_id: 2,
            run_id: 2,
            run_generation: 1,
            runtime_execution_id: "session-b1",
            dispatch_id: "d-cccc-r2-g1",
          },
        },
        {
          run_id: 3,
          node_id: 2,
          generation: 2,
          state: "active",
          contract: { executor: "ocg-build", model: "openai/gpt-6-astra", role: "build" },
          runtime_execution_id: "session-b2",
          host_session_id: null,
          result: null,
          witness: {
            mission_id: "wn-dogfood",
            work_node_id: 2,
            run_id: 3,
            run_generation: 2,
            runtime_execution_id: "session-b2",
            dispatch_id: "d-dddd-r3-g2",
          },
        },
      ],
      events: [
        { seq: 1, kind: "mission_created", payload: "{}", by_run_id: null },
        { seq: 2, kind: "run_started", payload: "{}", by_run_id: 0 },
        { seq: 3, kind: "run_dispatched", payload: "{}", by_run_id: 0 },
        { seq: 4, kind: "run_completed", payload: "{}", by_run_id: 1 },
        { seq: 5, kind: "run_fenced", payload: "{}", by_run_id: 2 },
        { seq: 6, kind: "late_result_retained", payload: "{}", by_run_id: 2 },
      ],
      verifications: [
        {
          verification_id: "vf-d-bbbb-r1-g1-1",
          node_id: 1,
          run_id: 1,
          dispatch_id: "d-bbbb-r1-g1",
          outcome: "passed",
          passed: true,
          commands: ["cargo test"],
          created_at: 10,
        },
      ],
      late_results: [
        { node_id: 2, run_id: 2, dispatch_id: "d-cccc-r2-g1", result: "stale success" },
      ],
    },
    ...overrides,
  };
}

function project() {
  const result = projectCanonicalSnapshot(backendSnapshot(), PROJECT);
  assert.ok(result.ok, result.ok ? "" : result.issue.message);
  return result.projection;
}

test("a valid backend snapshot is accepted with its canonical identities intact", () => {
  const projection = project();
  assert.equal(projection.apiVersion, CANONICAL_API_VERSION);
  assert.equal(projection.missionId, "wn-dogfood");
  assert.equal(projection.rootNodeId, 0);
  assert.equal(projection.workNodes.length, 3);
  assert.equal(projection.runs.length, 4);
  assert.equal(projection.dependencies.length, 1);
  assert.equal(projection.cursor, 6);
  // Frozen contracts and witnesses are carried through, never re-derived.
  const fenced = projection.runs.find((run) => run.run_id === 2)!;
  assert.equal(fenced.state, "fenced");
  assert.equal(fenced.contract.model, "openai/gpt-6-astra");
  assert.equal(fenced.witness?.dispatch_id, "d-cccc-r2-g1");
  const replacement = projection.runs.find((run) => run.run_id === 3)!;
  assert.equal(replacement.generation, 2);
  assert.equal(replacement.witness?.run_generation, 2);
  assert.equal(projection.lateResults.length, 1);
  assert.equal(projection.verifications[0]?.commands[0], "cargo test");
});

test("a wrong API version is rejected", () => {
  const result = projectCanonicalSnapshot(backendSnapshot({ api_version: "ocg.canonical.v0" }), PROJECT);
  assert.equal(result.ok, false);
  assert.equal(result.ok === false && result.issue.code, "api-version");
});

test("a wrong-project snapshot is rejected", () => {
  const result = projectCanonicalSnapshot(backendSnapshot({ project_id: "ocg" }), PROJECT);
  assert.equal(result.ok, false);
  assert.equal(result.ok === false && result.issue.code, "project-mismatch");
});

test("a cursor behind its own event stream is rejected", () => {
  const result = projectCanonicalSnapshot(backendSnapshot({ cursor: 2 }), PROJECT);
  assert.equal(result.ok, false);
  assert.equal(result.ok === false && result.issue.code, "cursor");
});

test("a Run without a complete frozen contract is rejected", () => {
  const snapshot = backendSnapshot();
  const mission = snapshot.mission as { runs: Record<string, unknown>[] };
  delete mission.runs[1]!.contract;
  const result = projectCanonicalSnapshot(snapshot, PROJECT);
  assert.equal(result.ok, false);
  assert.equal(result.ok === false && result.issue.code, "malformed");
});

test("an unknown canonical state is rejected rather than rendered", () => {
  const snapshot = backendSnapshot();
  (snapshot.mission as { work_nodes: Record<string, unknown>[] }).work_nodes[1]!.state = "vibing";
  const result = projectCanonicalSnapshot(snapshot, PROJECT);
  assert.equal(result.ok, false);
  assert.equal(result.ok === false && result.issue.code, "malformed");
});

test("the execution projection mirrors the ownership tree and dependency DAG", () => {
  const projection = project();
  const execution = toMissionExecution(projection, "Canonical execution inspector");
  assert.equal(execution.missionId, "wn-dogfood");
  assert.equal(execution.tasks.length, 3);
  const root = execution.tasks.find((task) => task.title.includes("canonical execution inspector"))!;
  const nodeB = execution.tasks.find((task) => task.title.startsWith("B:"))!;
  const nodeA = execution.tasks.find((task) => task.title.startsWith("A:"))!;
  // Ownership is a tree; dependencies are a separate DAG edge.
  assert.equal(nodeB.dependencies.length, 1);
  assert.equal(nodeB.dependencies[0], nodeA.id);
  assert.equal(nodeA.dependents.length, 1);
  assert.equal(nodeA.dependents[0], nodeB.id);
  assert.equal(root.status, "running");
  assert.equal(nodeA.status, "completed");
  assert.equal(nodeB.status, "running");
  // Both generations of B are visible with their own frozen identities.
  const bWorkers = execution.workers.filter((worker) => worker.model === "openai/gpt-6-astra" && worker.label.includes("ocg-build"));
  assert.equal(bWorkers.length, 2);
  assert.ok(bWorkers.some((worker) => worker.retryCount === 1));
  assert.equal(execution.summary.completed, 1);
  assert.equal(execution.summary.running, 2);
  assert.equal(execution.summary.total, 3);
  assert.equal(execution.summary.failed, 0);
});

test("a fenced generation never presents as a completed task", () => {
  const projection = project();
  const execution = toMissionExecution(projection, "Inspector");
  const nodeB = execution.tasks.find((task) => task.title.startsWith("B:"))!;
  assert.equal(nodeB.status, "running");
  assert.equal(nodeB.retryCount, 1);
  assert.equal(projection.runs.filter((run) => run.state === "fenced").length, 1);
});

test("pre-run configuration is mutable only before the first dispatch", () => {
  const projection = project();
  assert.equal(isPreRunConfigurationMutable(projection), false);
  const empty: CanonicalWorkProjection = {
    ...projection,
    runs: [],
    verifications: [],
    lateResults: [],
  };
  assert.equal(isPreRunConfigurationMutable(empty), true);
});
