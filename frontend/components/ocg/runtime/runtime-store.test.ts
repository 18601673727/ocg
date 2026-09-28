import { test } from "node:test";
import assert from "node:assert/strict";
import { createScenarioFixture } from "./scenarios";
import { createSnapshotEnvelopeFromFixture } from "./runtime-snapshot";
import { createRuntimeStore } from "./runtime-store";
import { RuntimeEnvelopeFactory } from "./runtime-envelope";
import type { RuntimeState } from "./reconciler";

function makeStore() {
  const fixture = createScenarioFixture("normal-chat");
  const seed = createSnapshotEnvelopeFromFixture(fixture, { streamId: "stream:store", generation: 1, sequence: 5 });
  return {
    store: createRuntimeStore(seed),
    seed,
    fixture,
    factory: new RuntimeEnvelopeFactory("stream:store", 1, { startSequence: 6 }),
  };
}

test("getState and getSnapshot keep stable identity between commits", () => {
  const { store } = makeStore();
  assert.equal(store.getState(), store.getState());
  assert.equal(store.getSnapshot(), store.getState().snapshot);
  assert.equal(store.getSnapshot(), store.getSnapshot());
});

test("state is committed before listeners are notified", () => {
  const { store, factory } = makeStore();
  let observedState: RuntimeState | null = null;
  const sequences: number[] = [];
  const unsubscribe = store.subscribe(() => {
    observedState = store.getState();
    sequences.push(store.getSync().cursor?.sequence ?? -1);
  });

  const mission = store.getSnapshot().missionsBySession["design-pwa-shell"]!;
  const envelope = factory.envelope(
    "mission.updated",
    { mission: { ...mission, current: "Committed first" } },
    { projectId: "zhuju", sessionId: "design-pwa-shell" },
  );
  store.applyEnvelope(envelope);

  assert.deepEqual(sequences, [6]);
  assert.equal(observedState!.snapshot.missionsBySession["design-pwa-shell"]?.current, "Committed first");

  unsubscribe();
  store.applyEnvelope(factory.envelope("runtime.status-changed", { status: { state: "failed" } }));
  assert.deepEqual(sequences, [6]);
});

test("applyMany commits once and a duplicate event never notifies", () => {
  const { store, factory } = makeStore();
  let notifications = 0;
  store.subscribe(() => { notifications += 1; });

  const first = factory.envelope("runtime.status-changed", { status: { state: "connecting" } });
  const second = factory.envelope("runtime.status-changed", { status: { state: "disconnected" } });
  store.applyMany([first, second]);
  assert.equal(notifications, 1);
  assert.equal(store.getSync().cursor?.sequence, 7);

  const before = store.getState();
  store.applyEnvelope(first); // duplicate event identity
  store.applyEnvelope(second); // duplicate event identity
  assert.equal(store.getState(), before);
  assert.equal(notifications, 1);
});

test("resetSnapshot installs a fresh baseline and clears diagnostics", () => {
  const { store, factory, fixture } = makeStore();
  const rejected = factory.envelope(
    "mission.launch-updated",
    {
      result: {
        outcome: "failed",
        commandId: "cmd-reset",
        draftId: "draft-reset",
        projectId: "zhuju",
        sessionId: "design-pwa-shell",
        message: "runtime down",
        duplicate: false,
      },
    },
    { projectId: "zhuju", sessionId: "design-pwa-shell" },
  );
  store.applyEnvelope(rejected);
  assert.ok(store.getDiagnostics().length > 0);

  const reset = createSnapshotEnvelopeFromFixture(fixture, { streamId: "stream:store", generation: 2, sequence: 0 });
  store.resetSnapshot(reset);
  assert.equal(store.getSync().generation, 2);
  assert.deepEqual(store.getDiagnostics(), []);
  assert.equal(store.getSync().cursor?.sequence, 0);
});

test("loading and reconnecting are observable degraded states", () => {
  const { store } = makeStore();
  store.markLoading();
  assert.equal(store.getSync().status, "loading-snapshot");
  store.markReconnecting();
  assert.equal(store.getSync().status, "reconnecting");
  assert.equal(store.getSync().resyncRequired, true);
});

test("unknown transport payloads are rejected before they can mutate canonical state", () => {
  const { store } = makeStore();
  const before = store.getSnapshot();
  store.applyUnknown({
    protocolVersion: 1,
    eventVersion: 1,
    streamId: "stream:store",
    generation: 1,
    sequence: 6,
    eventId: "bad:6",
    projectId: "zhuju",
    occurredAt: "not-a-date",
    type: "mission.updated",
    payload: { mission: null },
  });
  assert.equal(store.getSnapshot(), before);
  assert.equal(store.getSync().status, "error");
  assert.ok(store.getDiagnostics().some((diagnostic) => diagnostic.code === "schema-invalid"));
});

test("malformed snapshots are rejected without replacing the current baseline", () => {
  const { store } = makeStore();
  const before = store.getSnapshot();
  store.installUnknownSnapshot({ protocolVersion: 1, streamId: "stream:store", generation: 2 });
  assert.equal(store.getSnapshot(), before);
  assert.equal(store.getSync().status, "error");
  assert.ok(store.getDiagnostics().some((diagnostic) => diagnostic.code === "schema-invalid"));
});

/* -------------------------------------------------------------------------- */
/* Canonical WorkNode/Run projection                                          */
/* -------------------------------------------------------------------------- */

import { CANONICAL_API_VERSION } from "./canonical-envelope";
import type { ProjectId } from "../project/domain";

const CANONICAL_PROJECT = "project-abc123" as ProjectId;

function canonicalSnapshot(overrides: Record<string, unknown> = {}) {
  return {
    api_version: CANONICAL_API_VERSION,
    project_id: CANONICAL_PROJECT,
    cursor: 2,
    mission: {
      mission_id: "wn-store-canonical",
      root_node_id: 0,
      work_nodes: [
        { node_id: 0, parent_node_id: null, spawned_by_run_id: null, state: "running", generation: 1, active_run_id: 0, payload: "root" },
        { node_id: 1, parent_node_id: 0, spawned_by_run_id: 0, state: "completed", generation: 1, active_run_id: null, payload: "A" },
      ],
      dependencies: [],
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
          witness: { mission_id: "wn-store-canonical", work_node_id: 0, run_id: 0, run_generation: 1, runtime_execution_id: "lead-session", dispatch_id: "d-a-r0-g1" },
        },
        {
          run_id: 1,
          node_id: 1,
          generation: 1,
          state: "completed",
          contract: { executor: "ocg-explore", model: "openai/gpt-6-astra", role: "explore" },
          runtime_execution_id: "session-a",
          host_session_id: "session-a",
          result: "A",
          witness: { mission_id: "wn-store-canonical", work_node_id: 1, run_id: 1, run_generation: 1, runtime_execution_id: "session-a", dispatch_id: "d-b-r1-g1" },
        },
      ],
      events: [
        { seq: 1, kind: "mission_created", payload: "{}", by_run_id: null },
        { seq: 2, kind: "run_completed", payload: "{}", by_run_id: 1 },
      ],
      verifications: [
        { verification_id: "vf-1", node_id: 1, run_id: 1, dispatch_id: "d-b-r1-g1", outcome: "passed", passed: true, commands: ["cargo test"], created_at: 1 },
      ],
      late_results: [],
    },
    ...overrides,
  };
}

function canonicalEvent(sequence: number, overrides: Record<string, unknown> = {}) {
  return {
    api_version: CANONICAL_API_VERSION,
    project_id: CANONICAL_PROJECT,
    mission_id: "wn-store-canonical",
    sequence,
    event_id: `wn-store-canonical:${sequence}`,
    kind: "run_dispatched",
    payload: {},
    ...overrides,
  };
}

test("the existing RuntimeStore receives a canonical snapshot and projects it", () => {
  const { store } = makeStore();
  assert.equal(store.getCanonical().status, "uninitialized");
  const canonical = store.applyCanonicalSnapshot({
    payload: canonicalSnapshot(),
    projectId: CANONICAL_PROJECT,
    generation: 1,
  });
  assert.equal(canonical.status, "live");
  assert.equal(canonical.cursor, 2);
  const selected = store.selectCanonical("Inspector");
  assert.equal(selected.execution?.tasks.length, 2);
  assert.equal(selected.execution?.summary.completed, 1);
  assert.equal(selected.hasPassingVerification, true);
  assert.equal(selected.missionCompleted, false);
  // The runtime snapshot baseline is untouched by a canonical commit.
  assert.equal(store.getSnapshot().scenario, "normal-chat");
});

test("a wrong-project canonical snapshot is refused by the same store", () => {
  const { store } = makeStore();
  store.applyCanonicalSnapshot({ payload: canonicalSnapshot(), projectId: CANONICAL_PROJECT, generation: 1 });
  const refused = store.applyCanonicalSnapshot({
    payload: canonicalSnapshot({ project_id: "project-someone-else" }),
    projectId: CANONICAL_PROJECT,
    generation: 2,
  });
  assert.equal(refused.projection?.projectId, CANONICAL_PROJECT);
  assert.ok(refused.diagnostics.some((item) => item.code === "snapshot-rejected"));
});

test("a stale canonical generation cannot roll the canonical cursor back", () => {
  const { store } = makeStore();
  store.applyCanonicalSnapshot({ payload: canonicalSnapshot(), projectId: CANONICAL_PROJECT, generation: 2 });
  const stale = store.applyCanonicalSnapshot({
    payload: canonicalSnapshot({ cursor: 1 }),
    projectId: CANONICAL_PROJECT,
    generation: 1,
  });
  assert.equal(stale.cursor, 2);
  assert.ok(stale.diagnostics.some((item) => item.code === "snapshot-stale"));
});

test("canonical events, stale events and command acknowledgements share one store", () => {
  const { store } = makeStore();
  let notifications = 0;
  store.subscribe(() => {
    notifications += 1;
  });
  store.applyCanonicalSnapshot({ payload: canonicalSnapshot(), projectId: CANONICAL_PROJECT, generation: 1 });
  assert.equal(notifications, 1);
  const advanced = store.applyCanonicalEvents([canonicalEvent(3)], { projectId: CANONICAL_PROJECT });
  assert.equal(advanced.cursor, 3);
  assert.equal(notifications, 2);
  const replayed = store.applyCanonicalEvents([canonicalEvent(3)], { projectId: CANONICAL_PROJECT });
  assert.equal(replayed.cursor, 3);
  assert.equal(notifications, 3);
  const acked = store.applyCanonicalCommandAck({
    commandId: "cmd-project-import-1",
    kind: "project-import",
    accepted: true,
    message: "imported",
  });
  assert.equal(acked.commandAcks["cmd-project-import-1"].accepted, true);
  // A runtime envelope and a canonical projection stay independent
  // authorities: applying one never mutates the other.
  const { store: seeded, factory } = makeStore();
  const canonicalOnly = seeded.applyCanonicalSnapshot({
    payload: canonicalSnapshot(),
    projectId: CANONICAL_PROJECT,
    generation: 1,
  });
  const state = seeded.applyEnvelope(
    factory.envelope("runtime.status-changed", { status: { state: "connected", detail: "canonical probe" } }),
  );
  assert.equal(state.snapshot.status.state, "connected");
  assert.equal(seeded.getCanonical().status, "live");
  assert.equal(seeded.getCanonical().missionId, canonicalOnly.missionId);
});
