import assert from "node:assert/strict";
import { test } from "node:test";
import { createScenarioFixture } from "../runtime/scenarios";
import type { RuntimeSnapshot } from "../runtime/runtime-types";
import { createAttentionQueue } from "../attention/fixtures";
import {
  DEFAULT_PROJECT_ID,
  PROJECTS,
  isProjectId,
  projectOptions,
  projectSwitcherLabel,
  resolveProjectId,
  resolveProjectParam,
  selectProject,
  withProjectParam,
} from "./domain";
import {
  PROJECT_FIXTURES,
  findProject,
  projectFixture,
  projectLedgerMissionIds,
  projectSessionIds,
} from "./fixtures";
import {
  selectActiveProject,
  selectProjectAttentionItems,
  selectProjectAttentionQueue,
  selectProjectSnapshot,
  selectProjectSessions,
  resolveSelectedSessionId,
} from "./selectors";

function snapshotFixture(): RuntimeSnapshot {
  const fixture = createScenarioFixture("attention-overview");
  return {
    scenario: fixture.id,
    status: fixture.runtimeStatus,
    sessions: fixture.sessions,
    messagesBySession: fixture.messagesBySession,
    missionsBySession: fixture.missionsBySession,
    observabilityBySession: fixture.observabilityBySession,
    executionBySession: fixture.executionBySession,
    resourceLedger: fixture.resourceLedger,
    bootstrap: fixture.bootstrap,
  };
}

// ---------------------------------------------------------------------------
// Stable IDs, names, lookup, fallback
// ---------------------------------------------------------------------------

test("stable project IDs and names are exact, including RouteLace spelling", () => {
  assert.deepEqual(PROJECTS.map((project) => project.id), ["zhuju", "route-lace", "ocg", "cecece"]);
  assert.deepEqual(PROJECTS.map((project) => project.name), ["Zhuju", "RouteLace", "OCG", "CECECE"]);
  assert.equal(findProject("route-lace").name, "RouteLace");
  assert.notEqual(findProject("route-lace").name, "Routelace");
});

test("invalid or missing IDs fall back to Zhuju deterministically", () => {
  assert.equal(DEFAULT_PROJECT_ID, "zhuju");
  assert.equal(resolveProjectId(undefined), "zhuju");
  assert.equal(resolveProjectId(null), "zhuju");
  assert.equal(resolveProjectId("does-not-exist"), "zhuju");
  assert.equal(resolveProjectId(42), "zhuju");
  assert.equal(selectProject(undefined).id, "zhuju");
  assert.equal(selectActiveProject("bogus").name, "Zhuju");
  assert.ok(isProjectId("cecece"));
  assert.ok(!isProjectId("cece"));
});

test("URL params preserve absence and resolve invalid IDs safely", () => {
  assert.equal(resolveProjectParam("route-lace"), "route-lace");
  assert.equal(resolveProjectParam(undefined), undefined);
  assert.equal(resolveProjectParam("nope"), "zhuju");
});

test("navigation preserves the explicit active project", () => {
  assert.equal(withProjectParam("/", "route-lace"), "/?project=route-lace");
  assert.equal(withProjectParam("/?scenario=attention-overview", "route-lace"), "/?scenario=attention-overview&project=route-lace");
});

// ---------------------------------------------------------------------------
// Switcher semantics
// ---------------------------------------------------------------------------

test("switcher options mark exactly one active project", () => {
  const options = projectOptions("route-lace");
  assert.equal(options.length, 4);
  assert.deepEqual(options.filter((option) => option.active).map((option) => option.id), ["route-lace"]);
  assert.deepEqual(options.map((option) => option.name), ["Zhuju", "RouteLace", "OCG", "CECECE"]);

  const fallback = projectOptions("unknown");
  assert.deepEqual(fallback.filter((option) => option.active).map((option) => option.id), ["zhuju"]);
});

test("expanded and collapsed switcher labels carry the active project", () => {
  const routeLace = findProject("route-lace");
  assert.match(projectSwitcherLabel(routeLace), /RouteLace/);
  assert.match(projectSwitcherLabel(routeLace), /Switch project/);
  assert.match(projectSwitcherLabel(routeLace, true), /RouteLace/);
  assert.match(projectSwitcherLabel(routeLace, true), /Project:/);
  assert.doesNotMatch(projectSwitcherLabel(routeLace, true), /Switch project/);
});

// ---------------------------------------------------------------------------
// Scoped sessions / snapshot / attention queue
// ---------------------------------------------------------------------------

test("project fixtures map existing session and ledger Mission IDs", () => {
  assert.deepEqual(projectFixture("zhuju"), PROJECT_FIXTURES.zhuju);
  assert.ok(projectSessionIds("zhuju").includes("design-pwa-shell"));
  assert.ok(!projectSessionIds("zhuju").includes("research-space-bunny"));
  assert.deepEqual(projectLedgerMissionIds("zhuju"), ["mission-runtime", "mission-ledger"]);
  assert.deepEqual(projectLedgerMissionIds("route-lace"), ["mission-deploy"]);
  for (const project of PROJECTS) {
    assert.ok(projectSessionIds(project.id).length > 0, `${project.id} needs at least one session`);
  }
});

test("project session selection never leaks another project's sessions", () => {
  const snapshot = snapshotFixture();
  const zhuju = selectProjectSessions(snapshot.sessions, "zhuju");
  assert.deepEqual(zhuju.map((session) => session.id).sort(), [...projectSessionIds("zhuju")].sort());
  assert.ok(!zhuju.some((session) => session.id === "research-space-bunny"));

  const routeLace = selectProjectSessions(snapshot.sessions, "route-lace");
  assert.ok(routeLace.some((session) => session.id === "research-space-bunny"));
  assert.ok(!routeLace.some((session) => session.id === "design-pwa-shell"));
});

test("project snapshot filters sessions, maps, and ledger entries", () => {
  const snapshot = snapshotFixture();
  const scoped = selectProjectSnapshot(snapshot, "zhuju");

  assert.deepEqual(scoped.sessions.map((session) => session.id).sort(), [...projectSessionIds("zhuju")].sort());
  assert.deepEqual(
    Object.keys(scoped.missionsBySession).sort(),
    [...projectSessionIds("zhuju")].sort(),
  );
  assert.ok(!("research-space-bunny" in scoped.messagesBySession));

  assert.ok(scoped.resourceLedger);
  const allowed = new Set(projectLedgerMissionIds("zhuju"));
  assert.ok(scoped.resourceLedger!.entries.length > 0);
  assert.ok(scoped.resourceLedger!.entries.every((entry) => allowed.has(entry.missionId)));

  const routeLace = selectProjectSnapshot(snapshot, "route-lace");
  assert.ok(routeLace.resourceLedger!.entries.every((entry) => entry.missionId === "mission-deploy"));
  assert.ok(!routeLace.resourceLedger!.entries.some((entry) => entry.missionId === "mission-runtime"));
});

test("project snapshot accepts extra registered session IDs", () => {
  const snapshot = snapshotFixture();
  const registeredSession = {
    id: "mock-session-registered",
    title: "Registered thread",
    workType: "coding" as const,
    updatedAt: "now",
  };
  const withExtra: RuntimeSnapshot = {
    ...snapshot,
    sessions: [registeredSession, ...snapshot.sessions],
    messagesBySession: { ...snapshot.messagesBySession, [registeredSession.id]: [] },
    missionsBySession: { ...snapshot.missionsBySession, [registeredSession.id]: null },
  };

  const withoutExtra = selectProjectSnapshot(withExtra, "zhuju");
  assert.ok(!withoutExtra.sessions.some((session) => session.id === registeredSession.id));
  assert.ok(!(registeredSession.id in withoutExtra.messagesBySession));

  const withRegistered = selectProjectSnapshot(withExtra, "zhuju", [registeredSession.id]);
  assert.ok(withRegistered.sessions.some((session) => session.id === registeredSession.id));
  assert.ok(registeredSession.id in withRegistered.messagesBySession);
  assert.ok(registeredSession.id in withRegistered.missionsBySession);
});

test("selected session state is re-derived when a project changes", () => {
  const snapshot = snapshotFixture();
  const routeLace = selectProjectSessions(snapshot.sessions, "route-lace");
  assert.equal(resolveSelectedSessionId("design-pwa-shell", routeLace), routeLace[0]!.id);
  assert.equal(resolveSelectedSessionId(routeLace[1]!.id, routeLace), routeLace[1]!.id);
  assert.equal(resolveSelectedSessionId("design-pwa-shell", []), null);
});

test("attention queue filtering is by explicit projectId and never leaks", () => {
  const queue = createAttentionQueue("attention-overview");
  const zhuju = selectProjectAttentionQueue(queue, "zhuju");
  const routeLace = selectProjectAttentionQueue(queue, "route-lace");

  const zhujuIds = [...zhuju.approvals, ...zhuju.history].map((item) => item.id);
  const routeLaceIds = [...routeLace.approvals, ...routeLace.history].map((item) => item.id);

  assert.deepEqual(zhujuIds.sort(), [
    "attention-approval-retry",
    "attention-approval-spend",
    "attention-history-config-resolved",
    "attention-history-spend-approved",
    "attention-runtime-inspect",
  ]);
  assert.deepEqual(routeLaceIds.sort(), [
    "attention-approval-launch",
    "attention-history-provider-rejected",
  ]);

  assert.ok(!zhujuIds.includes("attention-approval-launch"));
  assert.ok(!routeLaceIds.includes("attention-approval-spend"));

  // At least two projects differ.
  assert.ok(zhujuIds.length > 0 && routeLaceIds.length > 0);

  const items = [...queue.approvals, ...queue.history];
  assert.ok(selectProjectAttentionItems(items, "zhuju").every((item) => item.projectId === "zhuju"));
  assert.ok(selectProjectAttentionItems(items, "route-lace").every((item) => item.projectId === "route-lace"));
  assert.deepEqual(selectProjectAttentionItems(items, "ocg"), []);
});
