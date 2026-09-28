import { test } from "node:test";
import assert from "node:assert/strict";
import { createScenarioFixture } from "./scenarios";
import { createSnapshotEnvelopeFromFixture, type RuntimeSnapshotEnvelope } from "./runtime-snapshot";
import {
  createUninitializedRuntimeState,
  isDegradedSyncStatus,
  reconcileEvent,
  reconcileSnapshot,
} from "./reconciler";
import { RuntimeEnvelopeFactory, type AnyRuntimeEnvelope } from "./runtime-envelope";
import { selectProjectSnapshot } from "../project/selectors";
import { createAttentionQueue } from "../attention/fixtures";
import { createLogsLiveFixture } from "../logs/domain";
import type { Mission } from "../types";
import type { ScenarioId } from "./runtime-types";

function seeded(scenario: ScenarioId = "normal-chat", sequence = 10) {
  const fixture = createScenarioFixture(scenario);
  const streamId = `stream:test:${scenario}`;
  const snapshot = createSnapshotEnvelopeFromFixture(fixture, { streamId, generation: 1, sequence });
  const state = reconcileSnapshot(createUninitializedRuntimeState(scenario), snapshot);
  const factory = new RuntimeEnvelopeFactory(streamId, 1, { startSequence: sequence + 1 });
  return { fixture, snapshot, state, factory, streamId };
}

function missionFor(state: ReturnType<typeof seeded>["state"], sessionId: string): Mission {
  const mission = state.snapshot.missionsBySession[sessionId];
  assert.ok(mission, `expected a fixture Mission for ${sessionId}`);
  return mission;
}

/* -------------------------------------------------------------------------- */
/* Snapshot reconciliation                                                    */
/* -------------------------------------------------------------------------- */

test("an authoritative snapshot installs a live baseline", () => {
  const { state, streamId } = seeded();
  assert.equal(state.sync.status, "live");
  assert.equal(state.sync.generation, 1);
  assert.equal(state.sync.cursor?.streamId, streamId);
  assert.equal(state.sync.cursor?.sequence, 10);
  assert.equal(state.sync.resyncRequired, false);
  assert.deepEqual(state.sync.diagnostics, []);
});

test("a newer snapshot at the same generation replaces the baseline", () => {
  const { state, snapshot, streamId } = seeded();
  const newer: RuntimeSnapshotEnvelope = {
    ...snapshot,
    cursor: { streamId, sequence: 20 },
    snapshot: { ...snapshot.snapshot, status: { state: "failed", detail: "newer" } },
  };
  const next = reconcileSnapshot(state, newer);
  assert.equal(next.sync.cursor?.sequence, 20);
  assert.equal(next.snapshot.status.state, "failed");
  assert.equal(next.snapshot, newer.snapshot);
});

test("a stale snapshot is ignored and cannot roll the cursor back", () => {
  const { state, snapshot, streamId } = seeded();
  const stale: RuntimeSnapshotEnvelope = { ...snapshot, cursor: { streamId, sequence: 5 } };
  const next = reconcileSnapshot(state, stale);
  assert.equal(next.snapshot, state.snapshot);
  assert.equal(next.sync.cursor?.sequence, 10);
  assert.ok(next.sync.diagnostics.some((item) => item.code === "snapshot-stale"));
});

test("a snapshot with a different Project scope is rejected", () => {
  const { fixture, state, streamId } = seeded();
  const scoped = createSnapshotEnvelopeFromFixture(fixture, {
    streamId,
    generation: 1,
    sequence: 20,
    projectId: "zhuju",
  });
  const next = reconcileSnapshot(state, scoped);
  assert.equal(next.snapshot, state.snapshot);
  assert.ok(next.sync.diagnostics.some((item) => item.code === "snapshot-scope-mismatch"));
});

test("a new generation snapshot replaces the previous baseline", () => {
  const { fixture, state, streamId } = seeded();
  const gen2 = createSnapshotEnvelopeFromFixture(fixture, { streamId, generation: 2, sequence: 0 });
  const next = reconcileSnapshot(state, gen2);
  assert.equal(next.sync.generation, 2);
  assert.equal(next.sync.cursor?.sequence, 0);
  assert.equal(next.snapshot, gen2.snapshot);
  assert.deepEqual(next.sync.seenEventIds, []);
});

test("an unsupported protocol snapshot enters an error sync state", () => {
  const { state, snapshot } = seeded();
  const bad = { ...snapshot, protocolVersion: 2 } as RuntimeSnapshotEnvelope;
  const next = reconcileSnapshot(state, bad);
  assert.equal(next.snapshot, state.snapshot);
  assert.equal(next.sync.status, "error");
  assert.ok(next.sync.diagnostics.some((item) => item.code === "protocol-incompatible"));
});

/* -------------------------------------------------------------------------- */
/* Event reconciliation                                                       */
/* -------------------------------------------------------------------------- */

test("ordered events apply and advance the cursor", () => {
  const { state, factory } = seeded();
  const mission = missionFor(state, "design-pwa-shell");
  const envelope = factory.envelope(
    "mission.updated",
    { mission: { ...mission, current: "Cursor advanced" } },
    { projectId: "zhuju", sessionId: "design-pwa-shell" },
  );
  const next = reconcileEvent(state, envelope);
  assert.equal(next.sync.cursor?.sequence, 11);
  assert.equal(next.sync.lastEventId, envelope.eventId);
  assert.equal(next.sync.status, "live");
  assert.equal(next.snapshot.missionsBySession["design-pwa-shell"]?.current, "Cursor advanced");
});

test("duplicate event identities are idempotent with stable state identity", () => {
  const { state, factory } = seeded();
  const mission = missionFor(state, "design-pwa-shell");
  const envelope = factory.envelope(
    "mission.updated",
    { mission: { ...mission, current: "Applied once" } },
    { projectId: "zhuju", sessionId: "design-pwa-shell" },
  );
  const once = reconcileEvent(state, envelope);
  const twice = reconcileEvent(once, envelope);
  assert.equal(twice, once);
});

test("stale lower sequences cannot roll state back", () => {
  const { state, factory } = seeded();
  const applied = factory.envelope("runtime.status-changed", { status: { state: "connecting" } });
  const first = reconcileEvent(state, applied);
  const stale = { ...applied, sequence: 10, eventId: "stale:10" } as AnyRuntimeEnvelope;
  const next = reconcileEvent(first, stale);
  assert.equal(next.snapshot, first.snapshot);
  assert.equal(next.sync.cursor?.sequence, 11);
  assert.ok(next.sync.diagnostics.some((item) => item.code === "sequence-stale"));
});

test("out-of-order delivery requires a gap resync and recovers when the gap closes", () => {
  const { state, factory } = seeded();
  const first = factory.envelope("runtime.status-changed", { status: { state: "connecting" } });
  const second = factory.envelope("runtime.status-changed", { status: { state: "disconnected" } });

  const gap = reconcileEvent(state, second); // sequence 12, cursor is 10
  assert.equal(gap.sync.status, "stale");
  assert.equal(gap.sync.resyncRequired, true);
  assert.equal(gap.snapshot, state.snapshot);
  assert.ok(gap.sync.diagnostics.some((item) => item.code === "sequence-gap"));

  const recovered = reconcileEvent(gap, first); // sequence 11 closes the gap
  assert.equal(recovered.sync.status, "live");
  assert.equal(recovered.sync.resyncRequired, false);
  assert.equal(recovered.sync.cursor?.sequence, 11);

  const replayed = reconcileEvent(recovered, second);
  assert.equal(replayed.sync.cursor?.sequence, 12);
  assert.equal(replayed.snapshot.status.state, "disconnected");
});

test("old-generation late events cannot mutate current state", () => {
  const { fixture, state, factory, streamId } = seeded();
  const gen2 = createSnapshotEnvelopeFromFixture(fixture, { streamId, generation: 2, sequence: 0 });
  const current = reconcileSnapshot(state, gen2);
  const late = {
    ...factory.envelope("conversation.session-updated", { session: { ...current.snapshot.sessions[0], title: "Late" } }),
    streamId: "stream:old-generation",
    generation: 1,
    sequence: 4,
    eventId: "late:4",
  } as AnyRuntimeEnvelope;
  const next = reconcileEvent(current, late);
  assert.equal(next.snapshot, current.snapshot);
  assert.equal(next.sync.status, "live");
  assert.ok(next.sync.diagnostics.some((item) => item.code === "generation-stale"));
});

test("events for another Project leave the active Project projection unchanged", () => {
  const { state, factory } = seeded();
  const before = selectProjectSnapshot(state.snapshot, "zhuju");
  const mission = missionFor(state, "research-space-bunny");
  const envelope = factory.envelope(
    "mission.updated",
    { mission: { ...mission, current: "Owned by RouteLace" } },
    { projectId: "route-lace", sessionId: "research-space-bunny" },
  );
  const next = reconcileEvent(state, envelope);
  assert.deepEqual(selectProjectSnapshot(next.snapshot, "zhuju"), before);
  assert.equal(next.snapshot.missionsBySession["research-space-bunny"]?.current, "Owned by RouteLace");
});

test("a scoped event contradicting fixture ownership is consumed without mutation", () => {
  const { state, factory } = seeded();
  const mission = missionFor(state, "design-pwa-shell");
  const envelope = factory.envelope(
    "mission.updated",
    { mission: { ...mission, current: "Spoofed" } },
    { projectId: "route-lace", sessionId: "design-pwa-shell" },
  );
  const next = reconcileEvent(state, envelope);
  assert.equal(next.snapshot, state.snapshot);
  assert.equal(next.sync.cursor?.sequence, 11);
  assert.ok(next.sync.diagnostics.some((item) => item.code === "project-scope-mismatch"));
});

test("an accepted command acknowledgement advances the cursor without mutating the snapshot", () => {
  const { state, factory } = seeded();
  const envelope = factory.envelope(
    "mission.launch-updated",
    {
      result: {
        outcome: "accepted",
        commandId: "cmd-1",
        draftId: "draft-1",
        projectId: "zhuju",
        sessionId: "design-pwa-shell",
        missionId: "mission-draft-1",
        message: "accepted",
        duplicate: false,
      },
    },
    { projectId: "zhuju", sessionId: "design-pwa-shell", commandId: "cmd-1" },
  );
  const next = reconcileEvent(state, envelope);
  assert.equal(next.snapshot, state.snapshot);
  assert.equal(next.sync.cursor?.sequence, 11);
  assert.equal(next.sync.diagnostics.length, state.sync.diagnostics.length);
  assert.equal(next.sync.commandResults["cmd-1"]?.outcome, "accepted");
});

test("a rejected command acknowledgement records an observable diagnostic", () => {
  const { state, factory } = seeded();
  const envelope = factory.envelope(
    "mission.launch-updated",
    {
      result: {
        outcome: "rejected",
        commandId: "cmd-2",
        draftId: "draft-2",
        projectId: "zhuju",
        sessionId: "design-pwa-shell",
        message: "not owned",
        duplicate: false,
      },
    },
    { projectId: "zhuju", sessionId: "design-pwa-shell", commandId: "cmd-2" },
  );
  const next = reconcileEvent(state, envelope);
  assert.equal(next.snapshot, state.snapshot);
  assert.ok(next.sync.diagnostics.some((item) => item.code === "command-rejected"));
});

test("message deltas dedupe by turn/delta sequence and completion replaces content", () => {
  const { state, factory } = seeded();
  const scope = { projectId: "zhuju" as const, sessionId: "design-pwa-shell" };

  const started = factory.envelope(
    "conversation.message-started",
    { message: { id: "msg-1", role: "assistant", content: "", createdAt: "00:00", status: "streaming" } },
    scope,
  );
  const s1 = reconcileEvent(state, started);

  const delta1 = factory.envelope(
    "conversation.message-delta",
    { messageId: "msg-1", delta: "Hello ", turnId: "turn-1", deltaSequence: 1 },
    scope,
  );
  const s2 = reconcileEvent(s1, delta1);

  const duplicateKey = {
    ...factory.envelope(
      "conversation.message-delta",
      { messageId: "msg-1", delta: "Hello ", turnId: "turn-1", deltaSequence: 1 },
      scope,
    ),
    eventId: "dup-delta",
  } as AnyRuntimeEnvelope;
  const s3 = reconcileEvent(s2, duplicateKey);
  assert.equal(
    s3.snapshot.messagesBySession["design-pwa-shell"]?.find((item) => item.id === "msg-1")?.content,
    "Hello ",
  );
  assert.ok(s3.sync.diagnostics.some((item) => item.code === "duplicate-delta"));

  const delta2 = factory.envelope(
    "conversation.message-delta",
    { messageId: "msg-1", delta: "world", turnId: "turn-1", deltaSequence: 2 },
    scope,
  );
  const s4 = reconcileEvent(s3, delta2);
  assert.equal(
    s4.snapshot.messagesBySession["design-pwa-shell"]?.find((item) => item.id === "msg-1")?.content,
    "Hello world",
  );

  const completed = factory.envelope(
    "conversation.message-completed",
    { message: { id: "msg-1", role: "assistant", content: "Hello world", createdAt: "00:00", status: "completed" } },
    scope,
  );
  const s5 = reconcileEvent(s4, completed);
  assert.equal(
    s5.snapshot.messagesBySession["design-pwa-shell"]?.find((item) => item.id === "msg-1")?.status,
    "completed",
  );
});

test("cancellation never mutates a completed message", () => {
  const { state, factory } = seeded();
  const envelope = factory.envelope(
    "cancelled",
    { messageId: "normal-a1" },
    { projectId: "zhuju", sessionId: "design-pwa-shell" },
  );
  const next = reconcileEvent(state, envelope);
  assert.equal(next.snapshot, state.snapshot);
  assert.equal(
    next.snapshot.messagesBySession["design-pwa-shell"]?.find((item) => item.id === "normal-a1")?.status,
    "completed",
  );
  assert.ok(next.sync.diagnostics.some((item) => item.code === "unknown-entity" && item.severity === "info"));
});

test("mission and worker updates project into the canonical snapshot", () => {
  const { state, factory } = seeded();
  const mission = missionFor(state, "design-pwa-shell");
  const nextMission = { ...mission, workers: mission.workers.map((worker) => ({ ...worker, status: "active" as const })) };
  const envelope = factory.envelope(
    "mission.updated",
    { mission: nextMission },
    { projectId: "zhuju", sessionId: "design-pwa-shell" },
  );
  const s1 = reconcileEvent(state, envelope);

  const worker = s1.snapshot.missionsBySession["design-pwa-shell"]!.workers[0];
  const workerUpdate = factory.envelope(
    "worker.updated",
    { worker: { ...worker, status: "completed" } },
    { projectId: "zhuju", sessionId: "design-pwa-shell" },
  );
  const s2 = reconcileEvent(s1, workerUpdate);
  assert.equal(s2.snapshot.missionsBySession["design-pwa-shell"]?.workers[0].status, "completed");
});

test("Attention updates are revision-safe and remain Project scoped", () => {
  const { state, factory } = seeded();
  const item = createAttentionQueue("attention-overview").approvals.find((candidate) => candidate.id === "attention-approval-spend");
  assert.ok(item);
  const resolved = factory.envelope(
    "attention.updated",
    { item: { ...item, status: "resolved", updatedAt: "2026-09-25T10:00:00Z", resolution: { outcome: "resolved", at: "2026-09-25T10:00:00Z" } } },
    { projectId: "zhuju" },
  );
  const open = { ...factory.envelope("attention.updated", { item }, { projectId: "zhuju" }), sequence: 10, eventId: "stale-attention:10" } as AnyRuntimeEnvelope;
  const afterResolved = reconcileEvent(state, resolved);
  const afterStale = reconcileEvent(afterResolved, open);
  assert.equal(afterResolved.snapshot.attentionItems?.find((candidate) => candidate.id === item.id)?.status, "resolved");
  assert.equal(afterStale.snapshot.attentionItems?.find((candidate) => candidate.id === item.id)?.status, "resolved");

  const otherProject = { ...factory.envelope("attention.updated", { item }, { projectId: "route-lace" }), sequence: 12, eventId: "wrong-project-attention:12" } as AnyRuntimeEnvelope;
  const rejected = reconcileEvent(afterStale, otherProject);
  assert.equal(rejected.snapshot.attentionItems?.find((candidate) => candidate.id === item.id)?.status, "resolved");
  assert.ok(rejected.sync.diagnostics.some((diagnostic) => diagnostic.code === "project-scope-mismatch"));
});

test("ledger replay and log replay preserve stable identities", () => {
  const { state, factory } = seeded("resource-ledger");
  const entry = state.snapshot.resourceLedger?.entries[0];
  assert.ok(entry);
  const firstLedger = factory.envelope("ledger.entry-added", { entry }, { projectId: "zhuju" });
  const duplicateLedger = { ...factory.envelope("ledger.entry-added", { entry }, { projectId: "zhuju" }), sequence: 12, eventId: "ledger-replay:12" } as AnyRuntimeEnvelope;
  const log = createLogsLiveFixture()[0]!;
  const firstLog = factory.envelope("log.appended", { entry: log }, { projectId: "zhuju", sessionId: "design-pwa-shell" });
  const duplicateLog = { ...factory.envelope("log.appended", { entry: log }, { projectId: "zhuju", sessionId: "design-pwa-shell" }), sequence: 14, eventId: "log-replay:14" } as AnyRuntimeEnvelope;

  const withLedger = reconcileEvent(state, firstLedger);
  const withDuplicateLedger = reconcileEvent(withLedger, duplicateLedger);
  const withLog = reconcileEvent(withDuplicateLedger, firstLog);
  const withDuplicateLog = reconcileEvent(withLog, duplicateLog);
  assert.equal(withDuplicateLedger.snapshot.resourceLedger?.entries.filter((candidate) => candidate.id === entry.id).length, 1);
  assert.equal(withDuplicateLog.snapshot.logs?.filter((candidate) => candidate.id === log.id).length, 1);
});

test("deltas for unknown entities produce diagnostics rather than corruption", () => {
  const { state, factory } = seeded();
  const envelope = factory.envelope(
    "conversation.message-delta",
    { messageId: "does-not-exist", delta: "x", turnId: "t", deltaSequence: 1 },
    { projectId: "zhuju", sessionId: "design-pwa-shell" },
  );
  const next = reconcileEvent(state, envelope);
  assert.equal(next.snapshot, state.snapshot);
  assert.equal(next.sync.cursor?.sequence, 11);
  assert.ok(next.sync.diagnostics.some((item) => item.code === "unknown-entity"));
});

test("degraded sync status only reports non-live states", () => {
  assert.equal(isDegradedSyncStatus("live"), false);
  assert.equal(isDegradedSyncStatus("uninitialized"), false);
  assert.equal(isDegradedSyncStatus("stale"), true);
  assert.equal(isDegradedSyncStatus("error"), true);
  assert.equal(isDegradedSyncStatus("reconnecting"), true);
});
