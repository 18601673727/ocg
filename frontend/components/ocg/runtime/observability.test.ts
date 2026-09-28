import assert from "node:assert/strict";
import { test } from "node:test";
import {
  aggregateModelStats,
  aggregateProviderStats,
  boundActivities,
  boundTimeline,
  deriveBudgetUsage,
  sumTokenUsage,
  sumUsageValues,
  toChartableTimeline,
  type WorkerRuntimeStats,
} from "./observability";
import { createScenarioFixture } from "./scenarios";
import { isInspectorTab, restoreWorkerSelection, toggleInspectorMode } from "../observability/inspector-state";

function worker(overrides: Partial<WorkerRuntimeStats> = {}): WorkerRuntimeStats {
  return {
    workerId: "worker-1",
    role: "worker",
    label: "Worker",
    provider: "Provider A",
    model: "Model A",
    status: "completed",
    invocationCount: 1,
    retryCount: 0,
    successCount: 1,
    tokenUsage: { input: { value: 100, provenance: "reported" }, total: { value: 150, provenance: "reported" } },
    ...overrides,
  };
}

test("aggregates workers by provider and model without losing provenance", () => {
  const workers = [
    worker(),
    worker({ workerId: "worker-2", provider: "Provider A", model: "Model B", status: "active", invocationCount: 2, tokenUsage: { input: { value: 50, provenance: "estimated" }, total: { value: 80, provenance: "estimated" } } }),
    worker({ workerId: "worker-3", provider: "Provider B", model: "Model C", tokenUsage: { total: { value: 40, provenance: "reported" } } }),
  ];

  const providers = aggregateProviderStats(workers);
  assert.equal(providers.length, 2);
  assert.equal(providers[0].invocationCount, 3);
  assert.equal(providers[0].activeWorkers, 1);
  assert.deepEqual(providers[0].tokenUsage.total, { value: 230, provenance: "estimated" });

  const models = aggregateModelStats(workers);
  assert.equal(models.length, 3);
  assert.equal(models.find((model) => model.model === "Model B")?.activeWorkers, 1);
});

test("optional provider metrics remain unavailable instead of becoming zero", () => {
  const combined = sumTokenUsage([{ input: { value: 12, provenance: "reported" } }, { output: { value: 4, provenance: "reported" } }]);
  assert.equal(combined.reasoning, undefined);
  assert.equal(combined.cacheRead, undefined);
  assert.equal(combined.total, undefined);
  assert.equal(sumUsageValues([undefined, undefined]), undefined);
  assert.equal(aggregateProviderStats([worker()])[0].ttftMs, undefined);
});

test("estimated usage transitions to reported usage explicitly", () => {
  assert.deepEqual(sumUsageValues([
    { value: 24000, provenance: "estimated" },
    { value: 25300, provenance: "reported" },
  ]), { value: 49300, provenance: "estimated" });
  assert.deepEqual(sumUsageValues([{ value: 25300, provenance: "reported" }]), { value: 25300, provenance: "reported" });
});

test("timeline history is bounded to the newest points", () => {
  const points = Array.from({ length: 65 }, (_, index) => ({ timestamp: String(index), elapsedMs: index, cumulativeUsage: {} }));
  const bounded = boundTimeline(points, 60);
  assert.equal(bounded.length, 60);
  assert.equal(bounded[0].timestamp, "5");
});

test("timeline normalization preserves plotted values and provenance", () => {
  const points = toChartableTimeline(createScenarioFixture("observability-live").observabilityBySession["design-pwa-shell"]!.timeline);
  assert.ok(points.length >= 4);
  assert.ok(points.some((point) => point.estimatedTotal !== null));
  assert.ok(points.every((point) => point.total !== null));
  assert.equal(points[1].provenance, "estimated");
});

test("budget projection exposes remaining, percentage, and burn rate", () => {
  const budget = deriveBudgetUsage({ spent: 8, limit: 20 }, 120_000, { value: 12, provenance: "estimated" });
  assert.equal(budget.remaining, 12);
  assert.equal(budget.percent, 40);
  assert.equal(budget.burnRatePerMinute, 4);
  assert.deepEqual(budget.estimatedFinalSpend, { value: 12, provenance: "estimated" });
});

test("activity feed is bounded and worker selection survives compatible updates", () => {
  const activities = Array.from({ length: 85 }, (_, index) => ({ id: String(index), timestamp: String(index), elapsedMs: index, kind: "worker-started" as const, summary: "started" }));
  assert.equal(boundActivities(activities).length, 80);
  assert.equal(restoreWorkerSelection("build", ["lead", "build"]), "build");
  assert.equal(restoreWorkerSelection("build", ["lead"]), null);
  assert.equal(isInspectorTab("usage"), true);
  assert.equal(isInspectorTab("charts"), false);
  assert.equal(toggleInspectorMode("docked"), "expanded");
  assert.equal(toggleInspectorMode("expanded"), "docked");
});

test("observability live fixture is deterministic and includes staged updates", () => {
  const first = createScenarioFixture("observability-live");
  const second = createScenarioFixture("observability-live");
  assert.deepEqual(first.observabilityBySession, second.observabilityBySession);
  assert.deepEqual(first.observabilityUpdates, second.observabilityUpdates);
  assert.equal(first.observabilityUpdates?.length, 3);
  assert.equal(first.observabilityBySession["design-pwa-shell"]?.workers.some((item) => item.role === "lead"), true);
  assert.deepEqual(first.observabilityBySession["design-pwa-shell"]?.workers.map((item) => item.label), ["Lead-Mid", "Explore", "Explore Deep", "Build", "Verify", "Debug", "Docs"]);
});
