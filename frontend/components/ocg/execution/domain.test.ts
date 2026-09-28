import assert from "node:assert/strict";
import test from "node:test";
import { createMissionControlExecution } from "./fixtures";
import {
  currentWave,
  dependencyClosure,
  deriveExecutionGraph,
  filterExecutionTasks,
  hasDependencyCycle,
  parallelTasks,
  taskStatusCounts,
} from "./domain";

test("mission-control fixture is deterministic and has a useful execution shape", () => {
  const first = createMissionControlExecution();
  const second = createMissionControlExecution();
  assert.deepEqual(first, second);
  assert.equal(first.tasks.length >= 12, true);
  assert.equal(first.waves.length >= 5, true);
  assert.equal(first.tasks.every((task) => task.missionId === first.missionId), true);
});

test("task and edge identities are unique and all dependency references resolve", () => {
  const execution = createMissionControlExecution();
  assert.equal(new Set(execution.taskIds).size, execution.taskIds.length);
  assert.equal(new Set(execution.edgeIds).size, execution.edgeIds.length);
  const ids = new Set(execution.taskIds);
  for (const task of execution.tasks) {
    for (const id of [...task.dependencies, ...task.dependents]) assert.equal(ids.has(id), true, id);
  }
  for (const edge of execution.edges) {
    assert.equal(ids.has(edge.fromTaskId), true);
    assert.equal(ids.has(edge.toTaskId), true);
  }
});

test("fixture is acyclic, grouped into waves, and exposes current parallel work", () => {
  const execution = createMissionControlExecution();
  assert.equal(hasDependencyCycle(execution), false);
  assert.equal(currentWave(execution), 4);
  assert.deepEqual(execution.waves[0].taskIds, ["recon", "inventory"]);
  assert.equal(parallelTasks(execution, 1).length, 0);
  assert.equal(parallelTasks(execution, 3).length, 2);
  assert.equal(execution.tasks.find((task) => task.id === "recon")?.dependents.length, 2);
  assert.equal(execution.tasks.find((task) => task.id === "integration-gate")?.dependencies.length, 4);
});

test("status counts, gate, blocked reason, retry, and escalation remain normalized", () => {
  const execution = createMissionControlExecution();
  const counts = taskStatusCounts(execution.tasks);
  assert.equal(counts.total, 14);
  assert.equal(counts.completed, 8);
  assert.equal(counts.blocked, 1);
  assert.equal(counts.retrying, 1);
  assert.equal(execution.gates.some((gate) => gate.taskId === "integration-gate" && gate.type === "integration"), true);
  assert.match(execution.tasks.find((task) => task.id === "conflict-debug")?.blockedReason ?? "", /overlap/);
  const retried = execution.tasks.find((task) => task.id === "resource-ledger");
  assert.equal(retried?.attemptHistory?.length, 2);
  assert.deepEqual(retried?.escalationHistory?.map((item) => item.model), ["DeepSeek V4.1 Flash", "Sol"]);
});

test("workers include Lead and provider/model assignment is inspectable", () => {
  const execution = createMissionControlExecution();
  assert.equal(execution.workers.some((worker) => worker.role === "lead"), true);
  assert.equal(execution.workers.some((worker) => worker.label === "Verify"), true);
  assert.equal(new Set(execution.tasks.map((task) => task.provider)).size > 2, true);
  assert.equal(execution.tasks.every((task) => task.workerId), true);
});

test("selected task closures, graph nodes/edges, and accessible list data are derived", () => {
  const execution = createMissionControlExecution();
  const upstream = dependencyClosure(execution, "release-gate");
  assert.equal(upstream.has("integration-gate"), true);
  assert.equal(upstream.has("recon"), true);
  const graph = deriveExecutionGraph(execution);
  assert.equal(graph.nodes.length, execution.tasks.length);
  assert.equal(graph.edges.length, execution.edges.length);
  assert.equal(graph.width > 0 && graph.height > 0, true);
  const listEquivalent = filterExecutionTasks(execution.tasks, { query: "ledger", status: "retrying" });
  assert.deepEqual(listEquivalent.map((task) => task.id), ["resource-ledger"]);
  assert.equal(execution.tasks.find((task) => task.id === "integration-gate")?.description !== undefined, true);
});

test("activity history is bounded and includes mission-readable decisions", () => {
  const execution = createMissionControlExecution();
  assert.equal(execution.activities.length <= 80, true);
  assert.equal(execution.activities.some((item) => item.kind === "task-blocked"), true);
  assert.equal(execution.activities.some((item) => item.kind === "retry-scheduled"), true);
  assert.equal(execution.activities.some((item) => item.message.includes("independent verification")), true);
  assert.equal(execution.tasks.find((task) => task.id === "verification")?.waitingReason?.includes("Integration"), true);
});
