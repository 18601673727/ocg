import { test } from "node:test";
import assert from "node:assert/strict";
import {
  ReplayRuntimeTransport,
  createReplayScenario,
  createReplayTransport,
  type ReplayScenarioId,
} from "./transport";
import { createRuntimeStore } from "./runtime-store";

async function drive(id: ReplayScenarioId) {
  const replay = createReplayScenario(id);
  const transport = new ReplayRuntimeTransport(replay.steps);
  const snapshot = await transport.getSnapshot();
  const store = createRuntimeStore(snapshot);
  const delivered: string[] = [];
  const unsubscribe = transport.subscribe((envelope) => delivered.push(envelope.eventId));
  let step = transport.step();
  while (step) {
    if (step.kind === "snapshot") store.installSnapshot(step.envelope);
    else store.applyEnvelope(step.envelope);
    step = transport.step();
  }
  unsubscribe();
  return { replay, transport, store, delivered };
}

function messageContent(store: ReturnType<typeof createRuntimeStore>, sessionId: string, messageId: string) {
  return store.getSnapshot().messagesBySession[sessionId]?.find((item) => item.id === messageId)?.content;
}

test("a clean replay stream applies in order and reaches the completed message", async () => {
  const { store, delivered } = await drive("clean-stream");
  assert.equal(store.getSync().status, "live");
  assert.equal(store.getSync().cursor?.sequence, 4);
  assert.equal(messageContent(store, "design-pwa-shell", "replay-msg-1"), "Hello world");
  assert.equal(
    store.getSnapshot().messagesBySession["design-pwa-shell"]?.find((item) => item.id === "replay-msg-1")?.status,
    "completed",
  );
  assert.equal(delivered.length, 4);
});

test("duplicate replay delivery is idempotent", async () => {
  const { store, delivered } = await drive("duplicate");
  assert.equal(delivered.length, 3);
  assert.equal(store.getSync().cursor?.sequence, 2);
  assert.equal(messageContent(store, "design-pwa-shell", "replay-msg-1"), "once");
  assert.equal(store.getDiagnostics().some((item) => item.code === "sequence-gap"), false);
});

test("a gap marks the store stale without mutating the snapshot", async () => {
  const { store } = await drive("gap");
  assert.equal(store.getSync().status, "stale");
  assert.equal(store.getSync().resyncRequired, true);
  assert.equal(store.getSync().cursor?.sequence, 1);
  assert.ok(store.getDiagnostics().some((item) => item.code === "sequence-gap"));
  assert.equal(messageContent(store, "design-pwa-shell", "replay-msg-1"), "");
});

test("out-of-order replay recovers when the gap closes", async () => {
  const { store } = await drive("out-of-order");
  assert.equal(store.getSync().status, "live");
  assert.equal(store.getSync().resyncRequired, false);
  assert.equal(store.getSync().cursor?.sequence, 3);
  assert.equal(messageContent(store, "design-pwa-shell", "replay-msg-1"), "firstsecond");
});

test("reconnect installs a new generation snapshot and resumes", async () => {
  const { store } = await drive("reconnect-new-generation");
  assert.equal(store.getSync().generation, 2);
  assert.equal(store.getSync().status, "live");
  assert.equal(messageContent(store, "design-pwa-shell", "replay-msg-2"), "resumed");
});

test("an old-generation late replay cannot mutate the current baseline", async () => {
  const { store } = await drive("old-generation-late-event");
  assert.equal(store.getSync().generation, 2);
  assert.equal(store.getSync().cursor?.sequence, 10);
  assert.ok(store.getDiagnostics().some((item) => item.code === "generation-stale"));
});

test("command acknowledgements advance the cursor and surface rejections", async () => {
  const { store } = await drive("command-ack");
  assert.equal(store.getSync().cursor?.sequence, 3);
  assert.ok(store.getDiagnostics().some((item) => item.code === "command-rejected"));
  assert.equal(
    store.getSnapshot().missionsBySession["design-pwa-shell"]?.title,
    "Consolidate OCG frontend architecture",
  );
});

test("replay execute is idempotent per command identity and close stops delivery", async () => {
  const transport = createReplayTransport("clean-stream");
  const first = await transport.execute({ kind: "send-message", commandId: "cmd-a", sessionId: "design-pwa-shell", content: "hi" });
  assert.equal(first.status, "accepted");
  const replay = await transport.execute({ kind: "send-message", commandId: "cmd-a", sessionId: "design-pwa-shell", content: "hi" });
  assert.equal(replay.status, "duplicate");

  const seen: string[] = [];
  transport.subscribe((envelope) => seen.push(envelope.eventId));
  transport.step();
  transport.step();
  assert.equal(seen.length, 1);
  transport.close();
  assert.equal(transport.step(), null);
  assert.equal(transport.remaining(), transport.remaining());
});
