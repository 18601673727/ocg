import test from "node:test";
import assert from "node:assert/strict";
import {
  RuntimeEnvelopeFactory,
  validateRuntimeEnvelope,
} from "../components/ocg/runtime/runtime-envelope";
import {
  createSnapshotEnvelopeFromFixture,
  validateRuntimeSnapshotEnvelope,
} from "../components/ocg/runtime/runtime-snapshot";
import { RuntimeStore } from "../components/ocg/runtime/runtime-store";
import { createUninitializedRuntimeState } from "../components/ocg/runtime/reconciler";
import {
  SCENARIO_IDS,
  createScenarioFixture,
} from "../components/ocg/runtime/scenarios";

test("a completed message with an unknown role and status fails validation", () => {
  const factory = new RuntimeEnvelopeFactory("stream:probe", 1);
  const envelope = factory.envelope("conversation.message-completed", {
    message: {
      id: "m1",
      role: "invalid-role",
      status: "invalid-status",
      content: "hello",
      createdAt: new Date(0).toISOString(),
    },
  } as never, { sessionId: "s1" });
  const result = validateRuntimeEnvelope(envelope);
  assert.equal(result.ok, false);
});

test("a snapshot with non-session entries, no status, and an empty bootstrap fails validation", () => {
  const seed = createSnapshotEnvelopeFromFixture(createScenarioFixture("normal-chat"));
  const cases: Array<{
    name: string;
    apply: (snapshot: Record<string, unknown>) => void;
  }> = [
    { name: "sessions", apply: (snapshot) => { snapshot.sessions = [42]; } },
    { name: "status", apply: (snapshot) => { delete snapshot.status; } },
    { name: "bootstrap", apply: (snapshot) => { snapshot.bootstrap = {}; } },
  ];
  const combined = JSON.parse(JSON.stringify(seed)) as Record<string, unknown>;
  const combinedSnapshot = combined.snapshot as Record<string, unknown>;
  combinedSnapshot.sessions = [42];
  delete combinedSnapshot.status;
  combinedSnapshot.bootstrap = {};
  assert.equal(validateRuntimeSnapshotEnvelope(combined).ok, false);
  for (const { name, apply } of cases) {
    const solo = JSON.parse(JSON.stringify(seed)) as Record<string, unknown>;
    apply(solo.snapshot as Record<string, unknown>);
    assert.equal(validateRuntimeSnapshotEnvelope(solo).ok, false, name);
  }
});

test("every scenario fixture still validates and reconciles", () => {
  for (const id of SCENARIO_IDS) {
    const seed = createSnapshotEnvelopeFromFixture(createScenarioFixture(id));
    const validation = validateRuntimeSnapshotEnvelope(JSON.parse(JSON.stringify(seed)));
    assert.equal(validation.ok, true, `fixture snapshot ${id} validates`);
    const store = new RuntimeStore(createUninitializedRuntimeState(id));
    const state = store.installSnapshot(seed);
    assert.equal(state.sync.status, "live", `fixture snapshot ${id} reconciles`);
  }
});

test("a canonical-shaped history payload still validates", () => {
  const factory = new RuntimeEnvelopeFactory("stream:canonical", 1);
  const envelope = factory.envelope("conversation.history-loaded", {
    messages: [
      {
        id: "00000001-0000-7001-8000-000000000001:entry:1",
        jobId: "00000001-0000-7001-8000-000000000001",
        commandId: "00000001-0000-7001-8000-000000000001",
        role: "user",
        content: "do it",
        status: "completed",
        createdAt: new Date(0).toISOString(),
        images: [],
      },
      {
        id: "00000001-0000-7001-8000-000000000001:answer",
        jobId: "00000001-0000-7001-8000-000000000001",
        commandId: "00000001-0000-7001-8000-000000000001",
        role: "assistant",
        content: "done",
        status: "completed",
        createdAt: new Date(1).toISOString(),
        toolCalls: [{
          tool_call_id: "call:1",
          request: 1,
          index: 0,
          name: "workspace.read",
          decision: "admitted",
          outcome_kind: "completed",
          arguments: "{}",
          arguments_cut: false,
          result: "ok",
          result_cut: false,
        }],
      },
    ],
  }, { sessionId: "s1" });
  assert.equal(validateRuntimeEnvelope(envelope).ok, true);
});
