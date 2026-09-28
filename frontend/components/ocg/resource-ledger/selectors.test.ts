import assert from "node:assert/strict";
import { test } from "node:test";
import {
  cacheLeverage,
  cacheShare,
  componentTraffic,
  deriveCostProvenance,
  describeTimeSeries,
  filterEntries,
  filterOptions,
  groupEntries,
  isFilterActive,
  latestTimestampMs,
  modelKey,
  seriesValue,
  sumCostMicros,
  summarize,
  sumTokens,
  toLedgerTimeSeries,
  toMicros,
  usageTotal,
} from "./selectors";
import { createResourceLedgerFixture } from "./fixtures";
import { createScenarioFixture } from "../runtime/scenarios";
import { DEFAULT_LEDGER_FILTER, type ResourceLedgerEntry } from "./types";
import {
  formatCostMicros,
  formatCount,
  formatPercent,
  formatRatio,
  formatTimestamp,
  formatTokens,
  UNKNOWN,
} from "./format";

function entry(overrides: Partial<ResourceLedgerEntry> = {}): ResourceLedgerEntry {
  return {
    id: "e1",
    timestamp: "2026-03-04T09:00:00.000Z",
    startedAt: null,
    finishedAt: null,
    sessionId: null,
    invocationId: "e1",
    elapsedMs: 0,
    durationMs: null,
    latencyMs: null,
    ttftMs: null,
    missionId: "m1",
    missionLabel: "Mission One",
    taskId: "t1",
    taskLabel: "Task One",
    workerId: "w1",
    workerLabel: "Worker One",
    role: "worker",
    provider: "Provider A",
    model: "Model A",
    variant: "standard",
    callIndex: 1,
    attempt: 1,
    status: "success",
    usage: { freshInput: 100, cacheRead: 200, output: 50 },
    usageAuthority: "reportedCall",
    costMicros: 1_000,
    costProvenance: "reported",
    attributionConfidence: "exact",
    attributedMissionId: "m1",
    attributedTaskId: "t1",
    reconciliation: "reconciled",
    note: null,
    ...overrides,
  };
}

test("sumTokens distinguishes observed zero from unavailable and ignores non-finite values", () => {
  assert.equal(sumTokens([null, undefined]), null);
  assert.equal(sumTokens([]), null);
  assert.equal(sumTokens([null, 0]), 0);
  assert.equal(sumTokens([1, 2, null]), 3);
  assert.equal(sumTokens([Number.NaN, Number.POSITIVE_INFINITY, 4]), 4);
});

test("componentTraffic totals each component and keeps unobserved components null", () => {
  const traffic = componentTraffic([
    entry(),
    entry({ id: "e2", usage: { freshInput: 10, cacheRead: 0, output: 5, reasoning: 7 } }),
  ]);
  assert.equal(traffic.freshInput, 110);
  assert.equal(traffic.cacheRead, 200);
  assert.equal(traffic.cacheWrite, null);
  assert.equal(traffic.reasoning, 7);
  assert.equal(traffic.total, 110 + 200 + 55 + 7);
  assert.equal(componentTraffic([]).total, null);
});

test("cacheShare and cacheLeverage stay unavailable for missing or invalid denominators", () => {
  const healthy = componentTraffic([entry()]);
  assert.equal(cacheShare(healthy), 200 / 350);
  assert.equal(cacheLeverage(healthy), 2);

  const missingFresh = componentTraffic([entry({ usage: { cacheRead: 200, output: 10 } })]);
  assert.equal(cacheShare(missingFresh), 200 / 210);
  assert.equal(cacheLeverage(missingFresh), null);

  const zeroDenominator = componentTraffic([entry({ usage: { freshInput: 0, cacheRead: 0 } })]);
  assert.equal(cacheShare(zeroDenominator), null);
  assert.equal(cacheLeverage(zeroDenominator), null);

  const noCache = componentTraffic([entry({ usage: { freshInput: 0, cacheRead: 200 } })]);
  assert.equal(cacheLeverage(noCache), null);
  const share = cacheShare(noCache);
  assert.ok(share === 1);

  const zeroCache = componentTraffic([entry({ usage: { freshInput: 100, cacheRead: 0, output: 10 } })]);
  assert.equal(cacheShare(zeroCache), 0);
  assert.equal(cacheLeverage(zeroCache), 0);

  const unknownCache = componentTraffic([entry({ usage: { freshInput: 100, output: 10 } })]);
  assert.equal(cacheShare(unknownCache), null);
  assert.equal(cacheLeverage(unknownCache), null);

  // No NaN/Infinity may ever escape the selectors.
  for (const value of [cacheShare(zeroDenominator), cacheLeverage(zeroDenominator), cacheLeverage(noCache)]) {
    assert.ok(value === null || Number.isFinite(value));
  }
});

test("cost is summed as integer micro-units and an explicit zero is not unavailable", () => {
  assert.equal(toMicros(12.4), 12);
  assert.equal(toMicros(Number.NaN), null);
  assert.equal(toMicros(null), null);

  const total = sumCostMicros([
    entry({ id: "a", costMicros: 1_500, costProvenance: "reported" }),
    entry({ id: "b", costMicros: 0, costProvenance: "reported" }),
    entry({ id: "c", costMicros: null, costProvenance: "unavailable" }),
  ]);
  assert.equal(total.micros, 1_500);
  assert.equal(total.provenance, "reported");
  assert.ok(Number.isInteger(total.micros));

  const zeroOnly = sumCostMicros([entry({ costMicros: 0, costProvenance: "reported" })]);
  assert.equal(zeroOnly.micros, 0);
  assert.equal(zeroOnly.provenance, "reported");

  const missing = sumCostMicros([entry({ costMicros: null, costProvenance: "unavailable" })]);
  assert.equal(missing.micros, null);
  assert.equal(missing.provenance, "unavailable");

  assert.equal(deriveCostProvenance(["reported", "stored"]), "stored");
  assert.equal(deriveCostProvenance(["reported", "estimated"]), "estimated");
  assert.equal(deriveCostProvenance(["reported", "unavailable"]), "reported");
  assert.equal(deriveCostProvenance(["unavailable"]), "unavailable");
});

test("summarize derives every headline metric from one dataset", () => {
  const summary = summarize([
    entry({ id: "a" }),
    entry({ id: "b", status: "failure", attempt: 2, costMicros: null, costProvenance: "unavailable" }),
    entry({ id: "c", role: "lead", missionId: "m2", missionLabel: "Mission Two", taskId: "t2", usageAuthority: "estimated", attributionConfidence: "unknown", reconciliation: "pending" }),
  ]);
  assert.equal(summary.entryCount, 3);
  assert.equal(summary.successCount, 2);
  assert.equal(summary.failureCount, 1);
  assert.equal(summary.retryCount, 1);
  assert.equal(summary.missionCount, 2);
  assert.equal(summary.taskCount, 2);
  assert.equal(summary.workerCount, 1);
  assert.equal(summary.leadEntryCount, 1);
  assert.equal(summary.byAuthority.reportedCall, 2);
  assert.equal(summary.byAuthority.estimated, 1);
  assert.equal(summary.byAttribution.unknown, 1);
  assert.equal(summary.byReconciliation.pending, 1);
  assert.equal(summary.byCostProvenance.unavailable, 1);
  assert.equal(summary.costMicros, 2_000);
});

test("groupEntries supports every requested dimension including provider+model+variant", () => {
  const entries = [
    entry(),
    entry({ id: "b", missionId: "m2", missionLabel: "Mission Two", taskId: "t2", taskLabel: "Task Two", attributedMissionId: "m2", attributedTaskId: "t2", provider: "Provider B" }),
    entry({ id: "c", variant: "deep", provider: "Provider A", attributionConfidence: "inferred", reconciliation: "pending" }),
  ];

  assert.deepEqual(groupEntries(entries, "mission").map((group) => group.key), ["m1", "m2"]);
  assert.deepEqual(groupEntries(entries, "task").map((group) => group.entries.length), [2, 1]);
  assert.deepEqual(groupEntries(entries, "worker").map((group) => group.key), ["w1"]);
  assert.deepEqual(groupEntries(entries, "provider").map((group) => group.key), ["Provider A", "Provider B"]);
  assert.deepEqual(
    groupEntries(entries, "model").map((group) => group.key),
    [modelKey("Provider A", "Model A"), modelKey("Provider B", "Model A")],
  );

  const variants = groupEntries(entries, "modelVariant");
  assert.equal(variants.length, 3);
  assert.ok(variants.some((group) => group.label === "Model A · standard"));
  assert.ok(variants.some((group) => group.label === "Model A · deep"));

  assert.equal(groupEntries(entries, "attribution").find((group) => group.key === "inferred")?.summary.entryCount, 1);
  assert.equal(groupEntries(entries, "reconciliation").find((group) => group.key === "pending")?.summary.entryCount, 1);
  assert.equal(groupEntries([entry({ attributedMissionId: null, attributedTaskId: null })], "mission")[0].label, "Unknown Mission");
  assert.equal(groupEntries([entry({ attributedMissionId: "m1", attributedTaskId: null })], "task")[0].label, "Unknown task");
});

test("grouping keeps the same model on different providers distinct", () => {
  const entries = [
    entry({ id: "a", provider: "Command Code", model: "Muse" }),
    entry({ id: "b", provider: "GOAT", model: "Muse" }),
  ];
  const byModel = groupEntries(entries, "model");
  assert.equal(byModel.length, 2);
  assert.notEqual(byModel[0].key, byModel[1].key);
});

test("filterEntries applies mission/worker/provider/model and deterministic time windows", () => {
  const entries = [
    entry({ id: "a", timestamp: "2026-03-04T09:00:00.000Z", missionId: "m1", workerId: "w1", provider: "Provider A" }),
    entry({ id: "b", timestamp: "2026-03-04T10:00:00.000Z", missionId: "m2", workerId: "w2", provider: "Provider B" }),
    entry({ id: "c", timestamp: "2026-03-04T10:30:00.000Z", missionId: "m2", workerId: "w2", provider: "Provider B", model: "Model B" }),
    entry({ id: "d", timestamp: "not-a-date", missionId: "m2", workerId: "w2", provider: "Provider B" }),
  ];
  const nowMs = Date.parse("2026-03-04T10:30:00.000Z");

  assert.equal(filterEntries(entries, DEFAULT_LEDGER_FILTER).length, 4);
  assert.deepEqual(
    filterEntries(entries, { ...DEFAULT_LEDGER_FILTER, missionId: "m2" }, { nowMs }).map((item) => item.id),
    ["b", "c", "d"],
  );
  assert.deepEqual(
    filterEntries(entries, { ...DEFAULT_LEDGER_FILTER, workerId: "w1" }, { nowMs }).map((item) => item.id),
    ["a"],
  );
  assert.deepEqual(
    filterEntries(entries, { ...DEFAULT_LEDGER_FILTER, provider: "Provider B" }, { nowMs }).map((item) => item.id),
    ["b", "c", "d"],
  );
  assert.deepEqual(
    filterEntries(entries, { ...DEFAULT_LEDGER_FILTER, modelKey: modelKey("Provider B", "Model B") }, { nowMs }).map((item) => item.id),
    ["c"],
  );
  assert.deepEqual(
    filterEntries(entries, { ...DEFAULT_LEDGER_FILTER, window: "last15m" }, { nowMs }).map((item) => item.id),
    ["c"],
  );
  assert.deepEqual(
    filterEntries(entries, { ...DEFAULT_LEDGER_FILTER, window: "last1h" }, { nowMs }).map((item) => item.id),
    ["b", "c"],
  );
  // Unparseable timestamps are excluded from a bounded window but kept in "all".
  assert.equal(filterEntries(entries, { ...DEFAULT_LEDGER_FILTER, window: "last24h" }, { nowMs }).length, 3);
});

test("filterOptions come from the full set and isFilterActive detects narrowing", () => {
  const entries = [
    entry({ id: "a", provider: "Command Code", model: "Muse" }),
    entry({ id: "b", provider: "GOAT", model: "Muse" }),
  ];
  const options = filterOptions(entries);
  assert.equal(options.providers.length, 2);
  assert.equal(options.models.length, 2);
  assert.deepEqual(options.workers.map((option) => option.value), ["w1"]);
  assert.equal(isFilterActive(DEFAULT_LEDGER_FILTER), false);
  assert.equal(isFilterActive({ ...DEFAULT_LEDGER_FILTER, provider: "GOAT" }), true);
  assert.equal(isFilterActive({ ...DEFAULT_LEDGER_FILTER, window: "last1h" }), true);
});

test("toLedgerTimeSeries buckets, bounds, and never interpolates missing components", () => {
  const entries = [
    entry({ id: "a", timestamp: "2026-03-04T09:00:00.000Z", usage: { freshInput: 10 } }),
    entry({ id: "b", timestamp: "2026-03-04T09:05:00.000Z", usage: { freshInput: 5, reasoning: 3 } }),
    entry({ id: "c", timestamp: "2026-03-04T09:20:00.000Z", usage: { output: 7 } }),
    entry({ id: "d", timestamp: "invalid", usage: { output: 100 } }),
  ];
  const series = toLedgerTimeSeries(entries, { bucketMs: 10 * 60_000, limit: 24 });
  assert.equal(series.length, 2);
  assert.equal(series[0].freshInput, 15);
  assert.equal(series[0].reasoning, 3);
  assert.equal(series[0].cacheRead, null);
  assert.equal(series[0].output, null);
  assert.equal(series[0].total, 18);
  assert.equal(series[1].output, 7);
  assert.equal(series[1].freshInput, null);

  const bounded = toLedgerTimeSeries(entries, { bucketMs: 60_000, limit: 1 });
  assert.equal(bounded.length, 1);
  assert.equal(toLedgerTimeSeries([entry({ timestamp: "invalid" })]).length, 0);
});

test("describeTimeSeries is a faithful textual summary", () => {
  assert.equal(describeTimeSeries([]), "No timestamped usage to plot.");
  const series = toLedgerTimeSeries(
    [
      entry({ id: "a", timestamp: "2026-03-04T09:00:00.000Z", usage: { freshInput: 100 } }),
      entry({ id: "b", timestamp: "2026-03-04T09:30:00.000Z", usage: { freshInput: 250 } }),
    ],
    { bucketMs: 15 * 60_000 },
  );
  const text = describeTimeSeries(series);
  assert.ok(text.includes("buckets"));
  assert.ok(text.includes("latest 250 tokens"));
  assert.ok(text.includes("peak"));

  const missing = describeTimeSeries([{ timestampMs: 0, timestamp: "x", freshInput: null, cacheRead: null, cacheWrite: null, output: null, reasoning: null, total: null }]);
  assert.ok(missing.includes("unavailable"));
});

test("seriesValue and usageTotal expose values without leaking undefined", () => {
  const point = toLedgerTimeSeries([entry()], { bucketMs: 60_000 })[0];
  assert.equal(seriesValue(point, "freshInput"), 100);
  assert.equal(seriesValue(point, "reasoning"), null);
  assert.equal(usageTotal({ freshInput: 1, output: 2 }), 3);
  assert.equal(usageTotal({}), null);
  assert.equal(usageTotal(null), null);
});

test("resource-ledger fixture is deterministic and covers every domain state", () => {
  const first = createResourceLedgerFixture("resource-ledger");
  const second = createResourceLedgerFixture("resource-ledger");
  assert.ok(first);
  assert.deepEqual(first, second);
  assert.equal(createResourceLedgerFixture("normal-chat"), null);
  // Fresh objects: mutating one copy cannot affect the next fixture build.
  first.entries[0].costMicros = 999;
  assert.equal(createResourceLedgerFixture("resource-ledger")?.entries[0].costMicros, 18_400);

  const fixture = createResourceLedgerFixture("resource-ledger");
  assert.ok(fixture);
  const entries = fixture.entries;
  const summary = summarize(entries);

  assert.equal(entries.length, 34);
  assert.deepEqual(summary.traffic, {
    freshInput: 21_410,
    cacheRead: 81_800,
    cacheWrite: 4_140,
    output: 8_640,
    reasoning: 1_660,
    total: 117_650,
  });
  assert.equal(summary.costMicros, 231_400);
  assert.equal(summary.costProvenance, "estimated");
  assert.equal(summary.missionCount, 3);
  assert.equal(summary.workerCount, 8);
  assert.equal(summary.leadEntryCount, 7);
  assert.equal(summary.failureCount, 2);
  assert.equal(summary.retryingCount, 2);
  assert.equal(summary.retryCount, 6);

  // Every authority, attribution confidence, cost provenance, and reconciliation status is present.
  assert.ok(Object.values(summary.byAuthority).every((count) => count > 0));
  assert.ok(Object.values(summary.byAttribution).every((count) => count > 0));
  assert.ok(Object.values(summary.byCostProvenance).every((count) => count > 0));
  assert.ok(Object.values(summary.byReconciliation).every((count) => count > 0));

  // Known, explicit-zero, and unavailable cost are all represented.
  assert.ok(entries.some((item) => item.costMicros === 0 && item.costProvenance === "reported"));
  assert.ok(entries.some((item) => item.costMicros === null && item.costProvenance === "unavailable"));
  assert.ok(entries.some((item) => (item.costMicros ?? 0) > 0));
  // Missing reasoning is distinct from zero reasoning.
  assert.ok(entries.some((item) => item.usage?.reasoning === undefined));
  // Same model appears on different providers.
  assert.equal(groupEntries(entries, "model").filter((group) => group.label === "Muse Spark 1.3 Contributor").length, 2);
  // Variants are distinct.
  assert.ok(groupEntries(entries, "modelVariant").length > groupEntries(entries, "model").length);
  // Heavy cache reuse is present.
  const leverage = summary.cacheLeverage;
  assert.ok(leverage !== null && leverage > 1);
  assert.ok(summary.cacheShare !== null && summary.cacheShare > 0.5);
});

test("filtered fixture dataset is deterministic and shares one time reference", () => {
  const ledger = createResourceLedgerFixture("resource-ledger");
  assert.ok(ledger);
  const entries = ledger.entries;
  const nowMs = latestTimestampMs(entries);
  assert.equal(new Date(nowMs ?? 0).toISOString(), "2026-03-04T11:30:00.000Z");

  const lastHour = filterEntries(entries, { ...DEFAULT_LEDGER_FILTER, window: "last1h" }, { nowMs });
  assert.equal(lastHour.length, 11);
  const last15 = filterEntries(entries, { ...DEFAULT_LEDGER_FILTER, window: "last15m" }, { nowMs });
  assert.equal(last15.length, 3);
  assert.equal(filterEntries(entries, { ...DEFAULT_LEDGER_FILTER, window: "today" }, { nowMs }).length, entries.length);
  assert.equal(filterEntries(entries, { ...DEFAULT_LEDGER_FILTER, window: "last7d" }, { nowMs }).length, entries.length);
  assert.ok(last15.every((item) => lastHour.some((long) => long.id === item.id)));

  const mission = filterEntries(entries, { ...DEFAULT_LEDGER_FILTER, missionId: "mission-deploy" }, { nowMs });
  const summary = summarize(mission);
  assert.equal(summary.missionCount, 1);
  assert.equal(summary.entryCount, 14);
  assert.equal(summary.totalTokens, 53_650);
  assert.equal(summary.costMicros, 90_500);
});

test("the resource-ledger scenario is additive and does not alter existing scenarios", () => {
  const scenario = createScenarioFixture("resource-ledger");
  assert.ok(scenario.resourceLedger);
  assert.equal(scenario.resourceLedger?.entries.length, 34);

  const normal = createScenarioFixture("normal-chat");
  assert.equal(normal.resourceLedger, null);
  assert.ok(normal.observabilityBySession["design-pwa-shell"]);
  assert.ok(!("resource-ledger" in normal.observabilityBySession));
});

test("formatting renders unknown as a glyph and keeps explicit zero cost visible", () => {
  assert.equal(formatTokens(null), UNKNOWN);
  assert.equal(formatTokens(undefined), UNKNOWN);
  assert.equal(formatTokens(0), "0");
  assert.equal(formatTokens(12_345), "12,345");
  assert.equal(formatCostMicros(null), UNKNOWN);
  assert.equal(formatCostMicros(0), "$0.000000");
  assert.equal(formatCostMicros(92_000), "$0.092000");
  assert.equal(formatPercent(null), UNKNOWN);
  assert.equal(formatPercent(0.5), "50.0%");
  assert.equal(formatRatio(null), UNKNOWN);
  assert.equal(formatRatio(3.82), "3.82×");
  assert.equal(formatCount(null), UNKNOWN);
  assert.equal(formatTimestamp("not-a-date"), UNKNOWN);
  assert.equal(formatTimestamp("2026-03-04T09:05:00.000Z"), "09:05:00Z");
});
