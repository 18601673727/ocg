import { test } from "node:test";
import assert from "node:assert/strict";
import {
  applyCanonicalCommandAck,
  applyCanonicalEvents,
  applyCanonicalSnapshot,
  createCanonicalState,
  selectCanonical,
  type CanonicalBackendEvent,
} from "./canonical-store";
import { CANONICAL_API_VERSION } from "./canonical-envelope";
import type { ProjectId } from "../project/domain";

const PROJECT = "zhuju" as ProjectId;

function snapshotPayload(cursor = 3) {
  return {
    api_version: CANONICAL_API_VERSION,
    project_id: PROJECT,
    cursor,
    mission: {
      mission_id: "wn-store",
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
          witness: { mission_id: "wn-store", work_node_id: 0, run_id: 0, run_generation: 1, runtime_execution_id: "lead-session", dispatch_id: "d-a-r0-g1" },
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
          witness: { mission_id: "wn-store", work_node_id: 1, run_id: 1, run_generation: 1, runtime_execution_id: "session-a", dispatch_id: "d-b-r1-g1" },
        },
      ],
      events: [
        { seq: 1, kind: "mission_created", payload: "{}", by_run_id: null },
        { seq: 2, kind: "run_started", payload: "{}", by_run_id: 0 },
        { seq: 3, kind: "run_completed", payload: "{}", by_run_id: 1 },
      ],
      verifications: [
        { verification_id: "vf-1", node_id: 1, run_id: 1, dispatch_id: "d-b-r1-g1", outcome: "passed", passed: true, commands: ["cargo test"], created_at: 1 },
      ],
      late_results: [],
    },
  };
}

function event(sequence: number, overrides: Partial<CanonicalBackendEvent> = {}): CanonicalBackendEvent {
  return {
    api_version: CANONICAL_API_VERSION,
    project_id: PROJECT,
    mission_id: "wn-store",
    sequence,
    event_id: `wn-store:${sequence}`,
    kind: "run_completed",
    payload: {},
    ...overrides,
  };
}

function seeded() {
  return applyCanonicalSnapshot(createCanonicalState(), { payload: snapshotPayload(), projectId: PROJECT, generation: 1 });
}

test("an authoritative canonical snapshot installs a live baseline", () => {
  const state = seeded();
  assert.equal(state.status, "live");
  assert.equal(state.generation, 1);
  assert.equal(state.cursor, 3);
  assert.equal(state.missionId, "wn-store");
  assert.equal(state.projectId, PROJECT);
  assert.deepEqual(state.diagnostics, []);
  const selected = selectCanonical(state, "Inspector");
  assert.equal(selected.execution?.tasks.length, 2);
  assert.equal(selected.hasPassingVerification, true);
  assert.equal(selected.pendingDispatchCount, 1);
  assert.equal(selected.missionCompleted, false);
});

test("a wrong-project canonical snapshot is rejected and never rendered", () => {
  const state = applyCanonicalSnapshot(createCanonicalState(), {
    payload: { ...snapshotPayload(), project_id: "ocg" },
    projectId: PROJECT,
    generation: 1,
  });
  assert.equal(state.projection, null);
  assert.equal(state.status, "uninitialized");
  assert.ok(state.diagnostics.some((item) => item.code === "snapshot-rejected"));
});

test("a stale snapshot cannot roll the canonical cursor back", () => {
  const state = seeded();
  const older = applyCanonicalSnapshot(state, { payload: snapshotPayload(1), projectId: PROJECT, generation: 1 });
  assert.equal(older.cursor, 3);
  assert.ok(older.diagnostics.some((item) => item.code === "snapshot-stale"));
  const olderGeneration = applyCanonicalSnapshot(state, { payload: snapshotPayload(9), projectId: PROJECT, generation: 0 });
  assert.equal(olderGeneration.cursor, 3);
  assert.ok(olderGeneration.diagnostics.some((item) => item.code === "snapshot-stale"));
});

test("events advance the cursor and a duplicate delivery is idempotent", () => {
  const state = seeded();
  const advanced = applyCanonicalEvents(state, [event(4), event(5)], { projectId: PROJECT, generation: 1 });
  assert.equal(advanced.cursor, 5);
  assert.equal(advanced.status, "live");
  const replayed = applyCanonicalEvents(advanced, [event(4), event(5)], { projectId: PROJECT, generation: 1 });
  assert.equal(replayed.cursor, 5);
  assert.ok(replayed.diagnostics.some((item) => item.code === "duplicate-event"));
  // A *different* identity carrying an already-applied sequence is stale.
  const stale = applyCanonicalEvents(advanced, [event(4, { event_id: "wn-store:4-rebatched" })], {
    projectId: PROJECT,
    generation: 1,
  });
  assert.equal(stale.cursor, 5);
  assert.ok(stale.diagnostics.some((item) => item.code === "sequence-stale"));
});

test("a sequence gap forces a resync instead of an optimistic merge", () => {
  const state = seeded();
  const gapped = applyCanonicalEvents(state, [event(6)], { projectId: PROJECT, generation: 1 });
  assert.equal(gapped.resyncRequired, true);
  assert.equal(gapped.status, "reconnecting");
  assert.equal(gapped.cursor, 3);
  assert.ok(gapped.diagnostics.some((item) => item.code === "sequence-gap"));
});

test("an event from a newer generation requires a new snapshot", () => {
  const state = seeded();
  const newer = applyCanonicalEvents(state, [event(4)], { projectId: PROJECT, generation: 2 });
  assert.equal(newer.cursor, 3);
  assert.ok(newer.diagnostics.some((item) => item.code === "generation-unknown"));
});

test("an event for another Project or Mission is refused", () => {
  const state = seeded();
  const foreign = applyCanonicalEvents(state, [event(4, { project_id: "ocg" })], { projectId: PROJECT, generation: 1 });
  assert.equal(foreign.cursor, 3);
  assert.ok(foreign.diagnostics.some((item) => item.code === "project-mismatch"));
  const other = applyCanonicalEvents(state, [event(4, { mission_id: "wn-other" })], { projectId: PROJECT, generation: 1 });
  assert.equal(other.cursor, 3);
  assert.ok(other.diagnostics.some((item) => item.code === "project-mismatch"));
});

test("a reconnect installs a new generation baseline and clears the seen set", () => {
  const state = applyCanonicalEvents(seeded(), [event(4)], { projectId: PROJECT, generation: 1 });
  assert.equal(state.cursor, 4);
  const reconnected = applyCanonicalSnapshot(state, {
    payload: snapshotPayload(7),
    projectId: PROJECT,
    generation: 2,
  });
  assert.equal(reconnected.generation, 2);
  assert.equal(reconnected.cursor, 7);
  assert.deepEqual(reconnected.seenEventIds, []);
  const stale = applyCanonicalEvents(reconnected, [event(5)], { projectId: PROJECT, generation: 1 });
  assert.equal(stale.cursor, 7);
  assert.ok(stale.diagnostics.some((item) => item.code === "generation-stale"));
});

test("command acknowledgements are correlated by stable command identity", () => {
  const state = applyCanonicalCommandAck(seeded(), {
    commandId: "cmd-project-import-1",
    kind: "project-import",
    accepted: true,
    message: "imported",
  });
  const rejection = applyCanonicalCommandAck(state, {
    commandId: "cmd-mission-config-1",
    kind: "mission-config",
    accepted: false,
    message: "Mission is already dispatched: pre-run configuration is frozen",
  });
  assert.equal(rejection.commandAcks["cmd-project-import-1"].accepted, true);
  assert.equal(rejection.commandAcks["cmd-mission-config-1"].accepted, false);
  assert.match(rejection.commandAcks["cmd-mission-config-1"].message, /frozen/);
});

test("a fenced generation is surfaced as evidence, never as a live task", () => {
  const payload = snapshotPayload(3);
  payload.mission.runs[1]!.state = "fenced";
  const state = applyCanonicalSnapshot(createCanonicalState(), { payload, projectId: PROJECT, generation: 1 });
  const selected = selectCanonical(state, "Inspector");
  assert.equal(selected.fencedRuns.length, 1);
  assert.equal(selected.fencedRuns[0]!.runId, 1);
  assert.equal(selected.execution?.tasks.find((task) => task.title === "A")?.status, "completed");
});
