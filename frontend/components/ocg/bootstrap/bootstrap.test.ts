import assert from "node:assert/strict";
import { test } from "node:test";
import { createBootstrapFixture } from "./fixtures";
import { resolveScenario } from "../runtime/scenarios";
import { MockOcgRuntimeClient } from "../runtime/mock-client";
import type { ScenarioId } from "../runtime/runtime-types";
import {
  advanceOnboarding,
  canResumeOnboarding,
  isOnboardingComplete,
  isOnboardingStageId,
  normalizeOnboardingMode,
  resolveAccessHandoff,
  resolveBootstrapRetry,
  selectActiveOnboardingStage,
  selectAvailableModels,
  selectAvailableResources,
  selectBlockingFailure,
  selectBootstrapEntry,
  selectNextStage,
  selectRecommendedProfile,
  selectStageGate,
} from "./selectors";
import { ONBOARDING_STAGES, type OnboardingStageId } from "./types";

const LOCAL_SCENARIOS: ScenarioId[] = ["normal-chat", "local-ready", "local-first-run", "runtime-disconnected"];

function entryFor(scenario: ScenarioId) {
  return selectBootstrapEntry(createBootstrapFixture(scenario));
}

test("local scenarios never route to login", () => {
  for (const scenario of LOCAL_SCENARIOS) {
    assert.notEqual(entryFor(scenario), "login", `${scenario} must not show login`);
  }
});

test("entry routing separates app, login, and onboarding", () => {
  assert.equal(entryFor("local-ready"), "app");
  assert.equal(entryFor("local-first-run"), "onboarding");
  assert.equal(entryFor("remote-unauthenticated"), "login");
  assert.equal(entryFor("remote-session-expired"), "login");
  assert.equal(entryFor("remote-denied"), "login");
  assert.equal(entryFor("remote-authenticated-ready"), "app");
  assert.equal(entryFor("remote-authenticated-first-run"), "onboarding");
  assert.equal(entryFor("onboarding-auth-required"), "login");
  assert.equal(entryFor("onboarding-connection-failure"), "onboarding");
  assert.equal(entryFor("onboarding-invalid-configuration"), "onboarding");
  assert.equal(entryFor("onboarding-ready"), "onboarding");
});

test("access gating takes precedence over pending onboarding", () => {
  const authRequired = createBootstrapFixture("onboarding-auth-required");
  assert.equal(selectBootstrapEntry(authRequired), "login");
  const granted = { ...authRequired, access: resolveAccessHandoff(authRequired.access) };
  assert.equal(selectBootstrapEntry(granted), "onboarding");
});

test("prerequisite gating blocks stages without known prerequisites", () => {
  const ready = createBootstrapFixture("local-ready");
  assert.equal(selectStageGate(ready, "resources").canAdvance, true);
  assert.equal(selectStageGate(ready, "connections").canAdvance, true);
  assert.equal(selectStageGate(ready, "discovery").canAdvance, true);
  assert.equal(selectStageGate(ready, "profile").canAdvance, true);

  const noConnections = {
    ...ready,
    connections: ready.connections.map((connection) => ({ ...connection, state: "unconfigured" as const })),
  };
  const gate = selectStageGate(noConnections, "connections");
  assert.equal(gate.canAdvance, false);
  assert.ok(gate.blockers.includes("required-connection-incomplete"));

  const noModels = { ...ready, models: [] };
  assert.equal(selectStageGate(noModels, "discovery").canAdvance, false);
  assert.equal(selectStageGate(noModels, "overview").canAdvance, false);

  const noProfile = {
    ...ready,
    profiles: ready.profiles.map((profile) => ({ ...profile, recommended: false })),
  };
  assert.equal(selectStageGate(noProfile, "profile").canAdvance, false);
});

test("pending failure blocks advancement at every stage", () => {
  const failing = createBootstrapFixture("onboarding-connection-failure");
  const gate = selectStageGate(failing, "connections");
  assert.equal(gate.canAdvance, false);
  assert.ok(gate.blockers.includes("connection-failed"));
  assert.ok(gate.blockers.includes("required-connection-incomplete"));
});

test("stage progression is deterministic and finalizes at ready", () => {
  assert.deepEqual(ONBOARDING_STAGES, [
    "welcome",
    "resources",
    "connections",
    "discovery",
    "overview",
    "profile",
    "ready",
  ]);
  assert.equal(ONBOARDING_STAGES.length, 7);

  const first = createBootstrapFixture("local-first-run");
  assert.equal(selectActiveOnboardingStage(first), "welcome");
  const advanced = advanceOnboarding(first);
  assert.deepEqual(advanced.onboarding?.completedStages, ["welcome"]);
  assert.equal(advanced.onboarding?.stage, "resources");

  const stagedAtConnections = {
    ...first,
    onboarding: { ...first.onboarding!, stage: "connections" as const },
    connections: first.connections.map((connection) => ({ ...connection, state: "failed" as const })),
  };
  assert.equal(advanceOnboarding(stagedAtConnections).onboarding?.stage, "connections");

  const finalStage = createBootstrapFixture("onboarding-ready");
  const done = advanceOnboarding(finalStage);
  assert.equal(done.ready, true);
  assert.ok(done.onboarding?.completedStages.includes("ready"));
  assert.equal(selectNextStage("ready"), null);
});

test("resumability requires usable access and saved progress", () => {
  assert.equal(canResumeOnboarding(createBootstrapFixture("onboarding-resume")), true);
  assert.equal(canResumeOnboarding(createBootstrapFixture("onboarding-ready")), true);
  assert.equal(canResumeOnboarding(createBootstrapFixture("local-first-run")), false);
  assert.equal(canResumeOnboarding(createBootstrapFixture("onboarding-auth-required")), false);
  assert.equal(canResumeOnboarding(createBootstrapFixture("local-ready")), false);
  assert.equal(isOnboardingComplete(createBootstrapFixture("onboarding-ready")), false);
  assert.equal(isOnboardingComplete(createBootstrapFixture("local-ready")), true);
});

test("unknown values degrade to explicit defaults instead of guessing", () => {
  assert.equal(resolveScenario("not-a-scenario"), "normal-chat");
  assert.equal(resolveScenario("local-first-run"), "local-first-run");

  const base = createBootstrapFixture("local-first-run");
  const bogus = {
    ...base,
    onboarding: { ...base.onboarding!, stage: "nonsense" as unknown as OnboardingStageId },
  };
  assert.equal(selectActiveOnboardingStage(bogus), "welcome");

  assert.equal(normalizeOnboardingMode("nonsense"), "firstRun");
  assert.equal(normalizeOnboardingMode(undefined), "firstRun");
  assert.equal(normalizeOnboardingMode("resume"), "resume");
  assert.equal(isOnboardingStageId("discovery"), true);
  assert.equal(isOnboardingStageId("nope"), false);
});

test("unknown statuses are never treated as available", () => {
  const discovery = createBootstrapFixture("onboarding-discovery");
  assert.ok(discovery.models.some((model) => model.status === "unknown"));
  assert.equal(selectAvailableModels(discovery).length, 1);
  assert.ok(selectAvailableModels(discovery).every((model) => model.status === "available"));
  assert.ok(discovery.capabilities.every((capability) => capability.status === "unknown"));

  const allUnknown = {
    ...discovery,
    resources: discovery.resources.map((resource) => ({ ...resource, status: "unknown" as const })),
  };
  assert.equal(selectAvailableResources(allUnknown).length, 0);
});

test("recommended profile is derived from normalized resources", () => {
  const state = createBootstrapFixture("local-first-run");
  assert.equal(selectRecommendedProfile(state)?.id, "balanced");
  const noRecommendation = {
    ...state,
    profiles: state.profiles.map((profile) => ({ ...profile, recommended: false })),
  };
  assert.equal(selectRecommendedProfile(noRecommendation), null);
});

test("fixtures preserve resource diversity and explicit unknown economics", () => {
  const state = createBootstrapFixture("local-first-run");
  assert.ok(state.resources.some((resource) => resource.economics.kind === "subscription"));
  assert.ok(state.resources.some((resource) => resource.economics.kind === "free"));
  assert.ok(state.resources.some((resource) => resource.provider === "OpenAI-compatible"));
  assert.ok(state.resources.some((resource) => resource.capacity.status === "unknown"));
  assert.ok(state.resources.some((resource) => resource.status === "unavailable"));
  assert.ok(state.connections.some((connection) => connection.state === "auth-required"));
});

test("actionable failures clear through the mock retry reducer", () => {
  const failing = createBootstrapFixture("onboarding-connection-failure");
  assert.ok(selectBlockingFailure(failing));
  const retried = resolveBootstrapRetry(failing);
  assert.equal(selectBlockingFailure(retried), null);
  assert.equal(retried.connections.find((connection) => connection.id === "provider-local")?.state, "connected");

  const local = createBootstrapFixture("local-ready");
  assert.equal(resolveAccessHandoff(local.access).state, "local");
});

test("authorization denial remains denied after a handoff retry", () => {
  const denied = createBootstrapFixture("remote-denied");
  const retried = resolveAccessHandoff(denied.access);
  assert.equal(retried.state, "denied");
  assert.equal(retried.handoffState, "failed");
});

function walk(value: unknown, visit: (key: string | null, value: unknown) => void, key: string | null = null): void {
  visit(key, value);
  if (Array.isArray(value)) {
    value.forEach((item) => walk(item, visit, null));
    return;
  }
  if (value && typeof value === "object") {
    for (const [childKey, childValue] of Object.entries(value)) walk(childValue, visit, childKey);
  }
}

test("fixtures are deterministic and contain no credential material", () => {
  const forbiddenKey = /(password|passwd|secret|api[_-]?key|token|credential)/i;
  const forbiddenValue = /(password|bearer\s|api[_-]?key|secret)/i;
  const scenarios: ScenarioId[] = [
    "local-ready",
    "local-first-run",
    "remote-unauthenticated",
    "onboarding-auth-required",
    "onboarding-connection-failure",
    "onboarding-discovery",
    "onboarding-ready",
  ];

  for (const scenario of scenarios) {
    const fixture = createBootstrapFixture(scenario);
    assert.deepEqual(fixture, createBootstrapFixture(scenario), `${scenario} must be deterministic`);
    walk(fixture, (key, value) => {
      if (key !== null) assert.doesNotMatch(key, forbiddenKey, `${scenario} exposes a forbidden field`);
      if (typeof value === "string") assert.doesNotMatch(value, forbiddenValue, `${scenario} exposes a forbidden value`);
    });
  }
});

test("mock client represents bootstrap setup and applies the mock handoff", async () => {
  const client = new MockOcgRuntimeClient("remote-unauthenticated");
  const events: string[] = [];
  client.subscribe((event) => events.push(event.type));

  const before = await client.getBootstrap();
  assert.equal(before.access.state, "unauthenticated");
  assert.equal(client.getSnapshot().bootstrap.access.state, "unauthenticated");

  await client.requestAccessHandoff();
  const after = await client.getBootstrap();
  assert.equal(after.access.state, "authenticated");
  assert.equal(client.getSnapshot().bootstrap.access.state, "authenticated");
  assert.ok(events.includes("bootstrap.updated"));
});

test("mock client advances stages and completes onboarding", async () => {
  const client = new MockOcgRuntimeClient("local-first-run");
  await client.setOnboardingStage("resources");
  await client.setOnboardingStage("connections");
  await client.setOnboardingStage("discovery");
  await client.setOnboardingStage("overview");
  await client.setOnboardingStage("profile");
  await client.setOnboardingStage("ready");
  assert.equal((await client.getBootstrap()).onboarding?.stage, "ready");

  await client.retryBootstrap();
  assert.equal(selectBlockingFailure(await client.getBootstrap()), null);

  await client.completeOnboarding();
  const completed = await client.getBootstrap();
  assert.equal(completed.ready, true);
  assert.equal(completed.onboarding?.completedStages.includes("ready"), true);
});
