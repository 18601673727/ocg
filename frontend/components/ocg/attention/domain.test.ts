import assert from "node:assert/strict";
import { test } from "node:test";
import { createScenarioFixture } from "../runtime/scenarios";
import {
  ATTENTION_KIND_LABELS,
  ATTENTION_STATUS_LABELS,
  isApprovalItem,
  isBlockedItem,
  isResolvedHistory,
  isUnresolved,
  type AttentionItem,
} from "./domain";
import { createAttentionQueue } from "./fixtures";
import {
  acknowledgeAttentionItem,
  applyAttentionDecision,
  attentionId,
  filterAttentionItems,
  resolveAttentionItem,
  selectAttentionItems,
  selectAttentionSummary,
  selectKindCounts,
  selectSeverityCounts,
  selectTabItems,
  sortAttentionByUrgency,
} from "./selectors";

function item(overrides: Partial<AttentionItem> & { id: string }): AttentionItem {
  return {
    kind: "approval",
    status: "pending",
    severity: "warning",
    title: `Title ${overrides.id}`,
    summary: `Summary ${overrides.id}`,
    whatHappened: "Something happened.",
    whyNeeded: "Judgment is required.",
    inactionConsequence: "It stays unresolved.",
    createdAt: "2026-09-25T09:00:00Z",
    updatedAt: "2026-09-25T09:00:00Z",
    source: "mission",
    destination: "mission-control",
    approval: null,
    blocked: null,
    resolution: null,
    ...overrides,
  };
}

function approvalItem(id: string, status: AttentionItem["status"] = "pending"): AttentionItem {
  return item({
    id,
    kind: "approval",
    status,
    approval: {
      id: `appr-${id}`,
      type: "mission-launch",
      requestedAction: "Launch the Mission.",
      reason: "Preflight passed.",
      requester: "OCG planner",
      requestedAt: "2026-09-25T09:00:00Z",
      approveConsequence: "Mission starts.",
      rejectConsequence: "Mission stays queued.",
      decision: "pending",
    },
  });
}

// ---------------------------------------------------------------------------
// Lifecycle semantics
// ---------------------------------------------------------------------------

test("rejected stays distinct from resolved", () => {
  const rejected = item({ id: "a", status: "rejected" });
  const resolved = item({ id: "b", status: "resolved" });
  assert.ok(isResolvedHistory(rejected));
  assert.ok(isResolvedHistory(resolved));
  assert.notEqual(rejected.status, resolved.status);
  assert.equal(ATTENTION_STATUS_LABELS.rejected, "Rejected");
  assert.equal(ATTENTION_STATUS_LABELS.resolved, "Resolved");
});

test("expired stays distinct from rejected", () => {
  const expired = item({ id: "a", status: "expired" });
  const rejected = item({ id: "b", status: "rejected" });
  assert.ok(isResolvedHistory(expired));
  assert.notEqual(expired.status, rejected.status);
});

test("blocked work is not an approval", () => {
  const blocked = item({
    id: "blocked-1",
    kind: "blocked",
    blocked: {
      missionId: "m1",
      missionTitle: "M",
      reason: "Held on a dependency.",
      unblocksWhen: "The dependency completes.",
    },
  });
  assert.ok(isBlockedItem(blocked));
  assert.ok(!isApprovalItem(blocked));
});

// ---------------------------------------------------------------------------
// Summary / aggregation
// ---------------------------------------------------------------------------

test("summary keeps resolved history separate from unresolved", () => {
  const items = [
    approvalItem("pending-1"),
    item({ id: "blocked-1", kind: "blocked", blocked: { missionId: "m", missionTitle: "M", reason: "r", unblocksWhen: "w" } }),
    item({ id: "done-1", status: "resolved", resolution: { outcome: "resolved", at: "2026-09-25T09:01:00Z" } }),
    item({ id: "done-2", status: "rejected", approval: approvalItem("x").approval, resolution: { outcome: "rejected", at: "2026-09-25T09:01:00Z" } }),
  ];
  const summary = selectAttentionSummary(items);
  assert.equal(summary.needsAction, 2);
  assert.equal(summary.awaitingApproval, 1);
  assert.equal(summary.blocked, 1);
  assert.equal(summary.resolved, 2);
});

test("severity aggregation counts unresolved only", () => {
  const items = [
    item({ id: "c1", severity: "critical" }),
    item({ id: "h1", severity: "high" }),
    item({ id: "old", severity: "critical", status: "resolved", resolution: { outcome: "resolved", at: "2026-09-25T09:01:00Z" } }),
  ];
  assert.deepEqual(selectSeverityCounts(items), { info: 0, warning: 0, high: 1, critical: 1 });
});

test("kind counts cover every kind without inventing kinds", () => {
  const counts = selectKindCounts([]);
  assert.deepEqual(Object.keys(counts).sort(), Object.keys(ATTENTION_KIND_LABELS).sort());
  assert.ok(Object.values(counts).every((count) => count === 0));
});

// ---------------------------------------------------------------------------
// Ordering / tabs / filtering
// ---------------------------------------------------------------------------

test("overview orders by urgency, not decoration", () => {
  const info = item({ id: "info", severity: "info" });
  const warning = item({ id: "warning", severity: "warning" });
  const critical = item({ id: "critical", severity: "critical", createdAt: "2026-09-25T09:05:00Z" });
  const criticalEarly = item({ id: "critical-early", severity: "critical", createdAt: "2026-09-25T09:01:00Z" });
  const ordered = sortAttentionByUrgency([info, warning, critical, criticalEarly]);
  assert.deepEqual(ordered.map((entry) => entry.id), ["critical-early", "critical", "warning", "info"]);
});

test("tab scoping separates approvals, blocked, and resolved history", () => {
  const items = [
    approvalItem("appr-1"),
    item({ id: "blocked-1", kind: "blocked", blocked: { missionId: "m", missionTitle: "M", reason: "r", unblocksWhen: "w" } }),
    item({ id: "plain-1", kind: "configuration" }),
    item({ id: "done-1", status: "resolved", resolution: { outcome: "resolved", at: "2026-09-25T09:01:00Z" } }),
  ];
  assert.deepEqual(selectTabItems(items, "overview").map((entry) => entry.id).sort(), ["appr-1", "blocked-1", "plain-1"]);
  assert.deepEqual(selectTabItems(items, "approvals").map((entry) => entry.id), ["appr-1"]);
  assert.deepEqual(selectTabItems(items, "blocked").map((entry) => entry.id), ["blocked-1"]);
  assert.deepEqual(selectTabItems(items, "resolved").map((entry) => entry.id), ["done-1"]);
});

test("resolved history is bounded and newest first", () => {
  const items = Array.from({ length: 30 }, (_, index) =>
    item({
      id: `done-${index}`,
      status: "resolved",
      updatedAt: `2026-09-${String(10 + (index % 15)).padStart(2, "0")}T09:00:00Z`,
      resolution: { outcome: "resolved", at: "2026-09-25T09:00:00Z" },
    }),
  );
  const resolved = selectTabItems(items, "resolved");
  assert.ok(resolved.length <= 25);
  for (let index = 1; index < resolved.length; index += 1) {
    assert.ok(resolved[index - 1]!.updatedAt >= resolved[index]!.updatedAt);
  }
});

test("filters combine search, kind, severity, and source", () => {
  const items = [
    item({ id: "a", kind: "budget", severity: "high", source: "policy", missionTitle: "Ledger inspector" }),
    item({ id: "b", kind: "blocked", severity: "high", source: "execution", missionTitle: "Consolidation" }),
  ];
  assert.deepEqual(
    filterAttentionItems(items, { tab: "overview", query: "ledger", kind: "all", severity: "all", source: "all" }).map((entry) => entry.id),
    ["a"],
  );
  assert.deepEqual(
    filterAttentionItems(items, { tab: "overview", query: "", kind: "blocked", severity: "all", source: "all" }).map((entry) => entry.id),
    ["b"],
  );
  assert.deepEqual(
    filterAttentionItems(items, { tab: "overview", query: "", kind: "all", severity: "info", source: "all" }),
    [],
  );
  assert.deepEqual(
    filterAttentionItems(items, { tab: "overview", query: "", kind: "all", severity: "all", source: "execution" }).map((entry) => entry.id),
    ["b"],
  );
});

test("ids are stable and never title-derived", () => {
  const first = attentionId("execution-blocked", "mission-architecture-consolidation");
  const second = attentionId("execution-blocked", "mission-architecture-consolidation");
  assert.equal(first, second);
  assert.ok(!first.includes("Consolidate"));
});

test("snapshot derivation deduplicates stable ids", () => {
  const fixture = createScenarioFixture("attention-overview");
  const snapshot = {
    scenario: fixture.id,
    missionsBySession: fixture.missionsBySession,
    executionBySession: fixture.executionBySession,
    bootstrap: fixture.bootstrap,
    resourceLedger: fixture.resourceLedger,
  };
  const queue = createAttentionQueue("attention-overview");
  const items = selectAttentionItems(snapshot, queue);
  const ids = items.map((entry) => entry.id);
  assert.equal(new Set(ids).size, ids.length);
  assert.ok(ids.every((id) => !id.includes("Consolidate OCG frontend architecture")));
});

// ---------------------------------------------------------------------------
// Approval lifecycle transitions
// ---------------------------------------------------------------------------

test("approve and reject transition deterministically and stay distinct", () => {
  const pending = approvalItem("appr-1");
  const approved = applyAttentionDecision([pending], pending.id, "approved", "2026-09-25T10:00:00Z");
  assert.equal(approved[0]!.status, "approved");
  assert.equal(approved[0]!.approval!.decision, "approved");
  assert.equal(approved[0]!.resolution!.outcome, "approved");
  assert.ok(!isUnresolved(approved[0]!));

  const rejected = applyAttentionDecision([pending], pending.id, "rejected", "2026-09-25T10:00:00Z");
  assert.equal(rejected[0]!.status, "rejected");
  assert.equal(rejected[0]!.approval!.decision, "rejected");
  assert.equal(rejected[0]!.resolution!.outcome, "rejected");
  assert.notEqual(rejected[0]!.status, approved[0]!.status);
});

test("decisions are idempotent once made", () => {
  const pending = approvalItem("appr-1");
  const once = applyAttentionDecision([pending], pending.id, "approved", "2026-09-25T10:00:00Z");
  const twice = applyAttentionDecision(once, pending.id, "rejected", "2026-09-25T11:00:00Z");
  assert.equal(twice[0]!.status, "approved");
  assert.equal(twice[0]!.updatedAt, "2026-09-25T10:00:00Z");
});

test("acknowledge only moves pending items", () => {
  const pending = item({ id: "a" });
  const acked = acknowledgeAttentionItem([pending], "a", "2026-09-25T10:00:00Z");
  assert.equal(acked[0]!.status, "acknowledged");
  assert.ok(isUnresolved(acked[0]!));
  const again = acknowledgeAttentionItem(acked, "a", "2026-09-25T11:00:00Z");
  assert.equal(again[0]!.updatedAt, "2026-09-25T10:00:00Z");
});

test("resolve applies to non-approvals only", () => {
  const plain = item({ id: "plain", kind: "configuration" });
  const approval = approvalItem("appr");
  const next = resolveAttentionItem([plain, approval], "plain", "2026-09-25T10:00:00Z");
  assert.equal(next[0]!.status, "resolved");
  const untouched = resolveAttentionItem([approval], "appr", "2026-09-25T10:00:00Z");
  assert.equal(untouched[0]!.status, "pending");
});

// ---------------------------------------------------------------------------
// Scenario behavior
// ---------------------------------------------------------------------------

test("attention-overview has a representative actionable mix", () => {
  const fixture = createScenarioFixture("attention-overview");
  const items = selectAttentionItems(
    {
      scenario: fixture.id,
      missionsBySession: fixture.missionsBySession,
      executionBySession: fixture.executionBySession,
      bootstrap: fixture.bootstrap,
      resourceLedger: fixture.resourceLedger,
    },
    createAttentionQueue("attention-overview"),
  );
  const summary = selectAttentionSummary(items);
  assert.ok(summary.needsAction >= 5, `expected a mixed queue, got ${summary.needsAction}`);
  assert.ok(summary.awaitingApproval >= 2, "expected multiple approvals");
  assert.ok(summary.blocked >= 1, "expected blocked work distinct from approvals");
  assert.ok(summary.critical >= 1, "expected at least one critical item");

  const kinds = new Set(items.filter(isUnresolved).map((entry) => entry.kind));
  assert.ok(kinds.has("budget"), "expected a spend/budget approval");
  assert.ok(kinds.has("blocked"), "expected blocked work");
  assert.ok(kinds.has("resource-degraded"), "expected provider degradation");
  assert.ok(kinds.has("retry"), "expected a retry/escalation item");
  assert.ok(kinds.has("runtime-failure"), "expected a runtime failure");

  const blocked = items.filter((entry) => entry.kind === "blocked");
  assert.ok(blocked.every((entry) => entry.approval === null), "blocked work must never be a fake approval");

  const approvals = items.filter(isUnresolved).filter((entry) => entry.approval !== null);
  assert.ok(approvals.length >= 2);
  for (const entry of approvals) {
    assert.ok(entry.approval!.requestedAction.length > 0);
    assert.ok(entry.approval!.approveConsequence.length > 0);
    assert.ok(entry.approval!.rejectConsequence.length > 0);
  }

  assert.ok(items.some((entry) => !isUnresolved(entry)), "expected resolved history");

  const destinations = new Set(items.map((entry) => entry.destination));
  for (const destination of destinations) {
    assert.ok(
      ["mission-control", "control-center", "resource-ledger", "logs", "settings", "chat"].includes(destination),
      `invalid destination: ${destination}`,
    );
  }
});

test("attention-calm has no unresolved items and intentional history", () => {
  const fixture = createScenarioFixture("attention-calm");
  const items = selectAttentionItems(
    {
      scenario: fixture.id,
      missionsBySession: fixture.missionsBySession,
      executionBySession: fixture.executionBySession,
      bootstrap: fixture.bootstrap,
      resourceLedger: fixture.resourceLedger,
    },
    createAttentionQueue("attention-calm"),
  );
  const summary = selectAttentionSummary(items);
  assert.equal(summary.needsAction, 0);
  assert.equal(summary.awaitingApproval, 0);
  assert.equal(summary.blocked, 0);
  assert.equal(summary.critical, 0);
  assert.ok(items.length > 0, "calm keeps bounded history so the empty state looks intentional");
  assert.ok(items.every((entry) => !isUnresolved(entry)));
});

test("for every attention-overview item, destination mapping is exhaustive", () => {
  const fixture = createScenarioFixture("attention-overview");
  const items = selectAttentionItems(
    {
      scenario: fixture.id,
      missionsBySession: fixture.missionsBySession,
      executionBySession: fixture.executionBySession,
      bootstrap: fixture.bootstrap,
      resourceLedger: fixture.resourceLedger,
    },
    createAttentionQueue("attention-overview"),
  );
  const known = ["mission-control", "control-center", "resource-ledger", "logs", "settings", "chat"];
  for (const entry of items) {
    assert.ok(known.includes(entry.destination), `unmapped destination for ${entry.id}`);
  }
});

test("other scenarios start with an empty queue", () => {
  const queue = createAttentionQueue("normal-chat");
  assert.deepEqual(queue, { approvals: [], history: [] });
});
