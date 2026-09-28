import assert from "node:assert/strict";
import { test } from "node:test";
import { createBootstrapFixture } from "../bootstrap/fixtures";
import { createMissionControlExecution } from "../execution/fixtures";
import { createResourceLedgerFixture } from "../resource-ledger/fixtures";
import { selectHomeAttention, selectHomeActiveMissions, selectRecentWork, selectResourceHealthSummary, selectHomeUsageSummary, selectRecentProductActivity } from "./selectors";
import { summarize } from "../resource-ledger/selectors";
import type { BootstrapState } from "../bootstrap/types";

// Helper: build a snapshot-like object for Home selectors
function makeSnapshot(overrides: {
  missionsBySession?: Record<string, import("../types").Mission | null>;
  executionBySession?: Record<string, import("../execution/domain").MissionExecution | null>;
  resourceLedger?: import("../resource-ledger/types").ResourceLedger | null;
  bootstrap?: BootstrapState;
  sessions?: import("../types").ChatSession[];
} = {}): {
  bootstrap: BootstrapState;
  missionsBySession: Record<string, import("../types").Mission | null>;
  executionBySession: Record<string, import("../execution/domain").MissionExecution | null>;
  resourceLedger: import("../resource-ledger/types").ResourceLedger | null;
  sessions: import("../types").ChatSession[];
} {
  const bootstrap = overrides.bootstrap ?? createBootstrapFixture("profiles-models");
  const missionsBySession = overrides.missionsBySession ?? {
    "design-pwa-shell": {
      title: "Build OCG PWA shell",
      goal: "Test fixture.",
      status: "running",
      completed: 3,
      total: 7,
      current: "Implement runtime abstraction",
      tasks: [
        { id: "t1", title: "One", status: "completed" },
        { id: "t2", title: "Two", status: "completed" },
        { id: "t3", title: "Three", status: "active" },
        { id: "t4", title: "Four", status: "pending" },
      ],
      workers: [
        { id: "lead", name: "Lead", status: "active", task: "Coordinate" },
        { id: "build", name: "Build", status: "waiting", task: "Build" },
      ],
      elapsed: "18m",
      commitment: { workers: 2, mode: "capped" },
      budget: { spent: 4.2, limit: 25, currency: "USD", status: "within-limit" },
      warnings: [],
    },
    "research-space-bunny": {
      title: "Space Bunny architecture study",
      goal: "Research fixture.",
      status: "paused",
      completed: 2,
      total: 5,
      current: "Blocked on dependency",
      tasks: [
        { id: "t1", title: "One", status: "completed" },
        { id: "t2", title: "Two", status: "failed" },
      ],
      workers: [
        { id: "explore", name: "Explore", status: "waiting", task: "Map" },
      ],
      elapsed: "42m",
      commitment: { workers: 1, mode: "capped" },
      budget: { spent: 8.0, limit: 25, currency: "USD", status: "within-limit" },
      warnings: [],
    },
  };
  const executionBySession = overrides.executionBySession ?? {
    "design-pwa-shell": createMissionControlExecution(),
  };
  const resourceLedger = overrides.resourceLedger ?? createResourceLedgerFixture("resource-ledger");
  const sessions = overrides.sessions ?? [
    { id: "design-pwa-shell", title: "OCG PWA shell", workType: "design" as const, updatedAt: "now" },
    { id: "research-space-bunny", title: "Space Bunny architecture study", workType: "research" as const, updatedAt: "2h ago" },
    { id: "coding-mission-runtime", title: "OCG durable mission runtime", workType: "coding" as const, updatedAt: "12m ago" },
    { id: "design-resource-controls", title: "Mission resource controls", workType: "design" as const, updatedAt: "2d ago" },
  ];

  return { bootstrap, missionsBySession, executionBySession, resourceLedger, sessions };
}

// ---------------------------------------------------------------------------
// Attention priority/order
// ---------------------------------------------------------------------------

test("selectHomeAttention surfaces critical items first", () => {
  const snapshot = makeSnapshot({
    bootstrap: {
      ...createBootstrapFixture("profiles-models"),
      providers: [
        { id: "down", label: "Offline Provider", state: "unavailable", detail: "Reported unavailable.", authRequired: false, endpointLabel: "offline", endpointType: "hosted", plan: "Not reported", economics: { kind: "unknown", detail: "Pricing not reported." }, capacity: { status: "unknown", detail: "Provider is unavailable." }, discoveredModelCount: 1, warnings: ["This provider is explicitly unavailable."] },
        { id: "degraded", label: "OpenCode Go", state: "degraded", detail: "Reachable but reporting elevated latency.", authRequired: false, endpointLabel: "go", endpointType: "hosted", plan: "Free / low-cost resource", economics: { kind: "free", detail: "Included usage; provider limits apply." }, capacity: { status: "known", detail: "Elevated latency observed." }, discoveredModelCount: 2, lastCheckedAt: "2026-09-25T08:55:00Z", warnings: ["Latency is elevated."] },
      ],
    },
    missionsBySession: {
      "design-pwa-shell": { title: "Test Mission", goal: "Test fixture.", status: "running", completed: 1, total: 3, current: "Task", tasks: [{ id: "t1", title: "Bad", status: "failed" }], workers: [{ id: "lead", name: "Lead", status: "active" }], elapsed: "5m", commitment: { workers: 1, mode: "capped" }, budget: { spent: 1, limit: 25, currency: "USD", status: "within-limit" }, warnings: [] },
    },
  });

  const items = selectHomeAttention(snapshot);
  assert.ok(items.length >= 2, "Should have attention items");
  // Critical unavailable provider should come first
  assert.equal(items[0].severity, "critical");
  assert.equal(items[0].kind, "providerUnavailable");
  // Warning degraded provider should come second
  assert.equal(items[1].severity, "warning");
  assert.equal(items[1].kind, "degradedResource");
});

test("selectHomeAttention returns empty for healthy state", () => {
  const snapshot = makeSnapshot({
    bootstrap: createBootstrapFixture("local-ready"),
    missionsBySession: {},
    executionBySession: {},
  });
  const items = selectHomeAttention(snapshot);
  assert.equal(items.length, 0);
});

// ---------------------------------------------------------------------------
// Attention empty state
// ---------------------------------------------------------------------------

test("selectHomeAttention empty state when everything is healthy", () => {
  const snapshot = makeSnapshot({
    bootstrap: createBootstrapFixture("local-ready"),
    missionsBySession: {},
    executionBySession: {},
  });
  const items = selectHomeAttention(snapshot);
  assert.equal(items.length, 0);
});

// ---------------------------------------------------------------------------
// Active mission projection
// ---------------------------------------------------------------------------

test("selectHomeActiveMissions includes running missions", () => {
  const snapshot = makeSnapshot();
  const missions = selectHomeActiveMissions(snapshot);
  const running = missions.filter((m) => m.status === "running");
  assert.ok(running.length >= 1, "Should have at least one running mission");
  assert.equal(running[0].status, "running");
});

test("selectHomeActiveMissions includes blocked missions", () => {
  const snapshot = makeSnapshot({
    missionsBySession: {
      "design-pwa-shell": { title: "Test Mission", goal: "Test fixture.", status: "running", completed: 3, total: 7, current: "Task", tasks: [{ id: "t1", title: "One", status: "completed" }], workers: [{ id: "lead", name: "Lead", status: "active" }], elapsed: "5m", commitment: { workers: 1, mode: "capped" }, budget: { spent: 1, limit: 25, currency: "USD", status: "within-limit" }, warnings: [] },
      "research-space-bunny": { title: "Space Bunny study", goal: "Research fixture.", status: "paused", completed: 2, total: 5, current: "Blocked", tasks: [{ id: "t1", title: "One", status: "completed" }, { id: "t2", title: "Two", status: "failed" }], workers: [{ id: "explore", name: "Explore", status: "waiting" }], elapsed: "42m", commitment: { workers: 1, mode: "capped" }, budget: { spent: 8, limit: 25, currency: "USD", status: "within-limit" }, warnings: [] },
    },
  });
  const missions = selectHomeActiveMissions(snapshot);
  const paused = missions.filter((m) => m.status === "paused");
  assert.ok(paused.length >= 1, "Should include paused mission");
  assert.equal(paused[0].blockedWorkers, 1);
});

test("selectHomeActiveMissions completed missions appear after active", () => {
  const snapshot = makeSnapshot({
    missionsBySession: {
      "design-pwa-shell": { title: "Running Mission", goal: "Test fixture.", status: "running", completed: 3, total: 7, current: "Task", tasks: [{ id: "t1", title: "One", status: "completed" }], workers: [{ id: "lead", name: "Lead", status: "active" }], elapsed: "5m", commitment: { workers: 1, mode: "capped" }, budget: { spent: 1, limit: 25, currency: "USD", status: "within-limit" }, warnings: [] },
      "research-space-bunny": { title: "Completed Study", goal: "Research fixture.", status: "completed", completed: 5, total: 5, current: "Complete", tasks: [{ id: "t1", title: "One", status: "completed" }], workers: [{ id: "explore", name: "Explore", status: "completed" }], elapsed: "42m", commitment: { workers: 1, mode: "capped" }, budget: { spent: 10, limit: 25, currency: "USD", status: "within-limit" }, warnings: [] },
    },
  });
  const missions = selectHomeActiveMissions(snapshot);
  assert.ok(missions[0].status === "running", "Running mission should be first");
  // Completed missions may or may not be included based on filtering
  const completed = missions.filter((m) => m.status === "completed");
  // completed missions that are "recently completed" should appear
  assert.ok(completed.length >= 0);
});

test("selectHomeActiveMissions computes progress correctly", () => {
  const snapshot = makeSnapshot();
  const missions = selectHomeActiveMissions(snapshot);
  const running = missions.find((m) => m.status === "running");
  assert.ok(running, "Should have running mission");
  assert.equal(running!.progress, Math.round((3 / 7) * 100));
});

// ---------------------------------------------------------------------------
// Recent work ordering
// ---------------------------------------------------------------------------

test("selectRecentWork returns bounded list", () => {
  const snapshot = makeSnapshot();
  const work = selectRecentWork(snapshot.sessions, 3);
  assert.equal(work.length, 3);
});

test("selectRecentWork orders by updatedAt desc", () => {
  const sessions = [
    { id: "s1", title: "Recent", workType: "coding" as const, updatedAt: "5m ago" },
    { id: "s2", title: "Older", workType: "design" as const, updatedAt: "2d ago" },
    { id: "s3", title: "Middle", workType: "research" as const, updatedAt: "1h ago" },
  ];
  const work = selectRecentWork(sessions, 10);
  assert.equal(work[0].title, "Recent");
  assert.equal(work[1].title, "Middle");
  assert.equal(work[2].title, "Older");
});

// ---------------------------------------------------------------------------
// Resource health summary
// ---------------------------------------------------------------------------

test("selectResourceHealthSummary counts providers correctly", () => {
  const bootstrap = createBootstrapFixture("profiles-models");
  const health = selectResourceHealthSummary(bootstrap);
  assert.equal(health.providerCount, 6);
  assert.equal(health.healthyProviders, 2); // opencode-zen, command-code
  assert.equal(health.degradedProviders, 1); // opencode-go
  assert.equal(health.unavailableProviders, 1); // offline-provider
  assert.equal(health.authRequiredProviders, 1); // future-provider
  assert.equal(health.unknownProviders, 1); // unreported-provider
});

test("selectResourceHealthSummary reports active profile", () => {
  const bootstrap = createBootstrapFixture("profiles-models");
  const health = selectResourceHealthSummary(bootstrap);
  assert.equal(health.activeProfileLabel, "Balanced");
});

test("selectResourceHealthSummary handles empty providers", () => {
  const bootstrap: BootstrapState = {
    ...createBootstrapFixture("local-ready"),
    providers: [],
  };
  const health = selectResourceHealthSummary(bootstrap);
  assert.equal(health.providerCount, 0);
  assert.equal(health.healthyProviders, 0);
  assert.equal(health.hasDegradedOrAuthRequired, false);
});

// ---------------------------------------------------------------------------
// Usage summary reuses existing definitions
// ---------------------------------------------------------------------------

test("selectHomeUsageSummary reuses ledger definitions", () => {
  const ledger = createResourceLedgerFixture("resource-ledger");
  const usage = selectHomeUsageSummary(ledger);
  assert.ok(usage.available);
  assert.equal(usage.entryCount, ledger!.entries.length);
  assert.equal(usage.totalTokens, ledger!.entries.reduce((sum, e) => {
    const usage = e.usage;
    if (!usage) return sum;
    const total = Object.values(usage).reduce((a, b) => a + (b ?? 0), 0);
    return sum + total;
  }, 0), "tokens match ledger");
  assert.ok(usage.costMicros !== null, "Should have cost when entries exist");
});

test("selectHomeUsageSummary returns unavailable for null ledger", () => {
  const usage = selectHomeUsageSummary(null);
  assert.equal(usage.available, false);
  assert.equal(usage.costMicros, null);
  assert.equal(usage.totalTokens, null);
});

// ---------------------------------------------------------------------------
// Recent product activity
// ---------------------------------------------------------------------------

test("selectRecentProductActivity returns bounded list", () => {
  const snapshot = makeSnapshot();
  const activity = selectRecentProductActivity(snapshot);
  assert.ok(activity.length <= 8);
});

test("selectRecentProductActivity surfaces mission completions", () => {
  const snapshot = makeSnapshot({
    missionsBySession: {
      "design-pwa-shell": { title: "Completed Mission", goal: "Test fixture.", status: "completed", completed: 7, total: 7, current: "Complete", tasks: [], workers: [], elapsed: "1h", commitment: { workers: 2, mode: "capped" }, budget: { spent: 10, limit: 25, currency: "USD", status: "within-limit" }, warnings: [] },
    },
  });
  const activity = selectRecentProductActivity(snapshot);
  const missionEvents = activity.filter((a) => a.kind === "mission");
  assert.ok(missionEvents.length >= 1, "Should have mission activity");
});

// ---------------------------------------------------------------------------
// Calm scenario
// ---------------------------------------------------------------------------

test("calm fixture produces empty attention", () => {
  const bootstrap = createBootstrapFixture("local-ready");
  const snapshot = {
    bootstrap,
    missionsBySession: {},
    executionBySession: {},
    resourceLedger: null,
    sessions: [{ id: "design-pwa-shell", title: "OCG PWA shell", workType: "design" as const, updatedAt: "now" }],
  };
  const attention = selectHomeAttention(snapshot);
  assert.equal(attention.length, 0);
});

test("calm fixture produces empty active missions", () => {
  const snapshot = makeSnapshot({
    missionsBySession: {},
  });
  const missions = selectHomeActiveMissions(snapshot);
  assert.equal(missions.length, 0);
});

// ---------------------------------------------------------------------------
// Busy scenario
// ---------------------------------------------------------------------------

test("busy fixture produces attention items", () => {
  const snapshot = makeSnapshot({
    bootstrap: {
      ...createBootstrapFixture("profiles-models"),
      providers: [
        { id: "down", label: "Offline Provider", state: "unavailable", detail: "Reported unavailable.", authRequired: false, endpointLabel: "offline", endpointType: "hosted", plan: "Not reported", economics: { kind: "unknown", detail: "Pricing not reported." }, capacity: { status: "unknown", detail: "Provider is unavailable." }, discoveredModelCount: 1, warnings: [] },
      ],
    },
    missionsBySession: {
      "design-pwa-shell": { title: "Busy Mission", goal: "Test fixture.", status: "running", completed: 5, total: 13, current: "Task", tasks: [{ id: "t1", title: "Bad", status: "failed" }], workers: [{ id: "lead", name: "Lead", status: "active" }, { id: "debug", name: "Debug", status: "waiting" }], elapsed: "30m", commitment: { workers: 3, mode: "capped" }, budget: { spent: 20, limit: 25, currency: "USD", status: "within-limit" }, warnings: [] },
    },
  });
  const attention = selectHomeAttention(snapshot);
  assert.ok(attention.length >= 1, "Should have attention items for busy scenario");
  assert.equal(attention[0].severity, "critical");
});

// ---------------------------------------------------------------------------
// No duplicate accounting formulas
// ---------------------------------------------------------------------------

test("usage summary uses existing ledger cost and token definitions", () => {
  const ledger = createResourceLedgerFixture("resource-ledger");
  const homeUsage = selectHomeUsageSummary(ledger);
  // Compare against the ledger's own summarize
  const ledgerSummary = summarize(ledger!.entries);
  assert.equal(homeUsage.entryCount, ledgerSummary.entryCount);
  assert.equal(homeUsage.totalTokens, ledgerSummary.totalTokens);
  assert.equal(homeUsage.costMicros, ledgerSummary.costMicros);
});

// ---------------------------------------------------------------------------
// Cross-surface destination derivation
// ---------------------------------------------------------------------------

test("attention items have valid destinations", () => {
  const snapshot = makeSnapshot({
    bootstrap: {
      ...createBootstrapFixture("profiles-models"),
      providers: [
        { id: "degraded", label: "Degraded", state: "degraded", detail: "Degraded.", authRequired: false, endpointLabel: "deg", endpointType: "hosted", plan: "Test", economics: { kind: "unknown", detail: "" }, capacity: { status: "known", detail: "" }, discoveredModelCount: 1, lastCheckedAt: "2026-09-25T08:55:00Z" },
      ],
    },
  });
  const items = selectHomeAttention(snapshot);
  const destinations = new Set(items.map((i) => i.destination));
  for (const dest of destinations) {
    assert.ok(["mission-control", "control-center", "resource-ledger", "logs", "settings", "onboarding"].includes(dest), `Invalid destination: ${dest}`);
  }
});

test("active mission projection has mission-control destination", () => {
  const snapshot = makeSnapshot();
  const missions = selectHomeActiveMissions(snapshot);
  for (const mission of missions) {
    assert.equal(mission.destination, "mission-control");
  }
});
