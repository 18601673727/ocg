import { test } from "node:test";
import assert from "node:assert/strict";
import { MockOcgRuntimeClient } from "./mock-client";
import { resolveScenario } from "./scenarios";
import { createMissionDraft, toMissionLaunchCommand, type MissionLaunchCommand } from "../mission/draft-domain";
import { selectProjectSnapshot } from "../project/selectors";

function launchCommand(projectId: string, sessionId: string): MissionLaunchCommand {
  const draft = createMissionDraft({
    projectId,
    sessionId,
    objective: "Ship the P0 Mission launch vertical slice",
    successCriteria: "Draft opens inline\nLaunch projects an execution\nMission Control shows the new execution",
  });
  const command = toMissionLaunchCommand(draft);
  assert.ok(command, "expected a launchable draft");
  return command;
}

test("scenario resolution falls back to normal chat", () => {
  assert.equal(resolveScenario("tool-heavy"), "tool-heavy");
  assert.equal(resolveScenario("not-a-scenario"), "normal-chat");
  assert.equal(resolveScenario(undefined), "normal-chat");
});

test("mock client exposes fixture sessions and missions", async () => {
  const client = new MockOcgRuntimeClient("budget-exhausted");
  const sessions = await client.listSessions();
  const mission = await client.getMission("design-pwa-shell");

  assert.ok(sessions.some((session) => session.id === "design-pwa-shell"));
  assert.equal(mission?.status, "budget-exhausted");
  assert.equal(mission?.budget.status, "exhausted");
});

test("mock client emits deterministic send events and supports unsubscribe", async () => {
  const client = new MockOcgRuntimeClient("normal-chat");
  const events: string[] = [];
  const unsubscribe = client.subscribe((event) => events.push(event.type));

  await client.sendMessage("design-pwa-shell", { content: "Test the local runtime" });
  await new Promise((resolve) => setTimeout(resolve, 350));

  assert.ok(events.includes("conversation.message-started"));
  assert.ok(events.includes("conversation.message-delta"));
  assert.ok(events.includes("conversation.message-completed"));
  assert.ok(events.includes("mission.updated"));
  const messages = await client.getMessages("design-pwa-shell");
  assert.equal(messages.at(-1)?.status, "completed");
  assert.equal(
    messages.at(-1)?.content,
    "Mock runtime received your message. This response is streamed locally, one deterministic chunk at a time.",
  );

  const eventCount = events.length;
  unsubscribe();
  await client.createSession({ workType: "coding" });
  assert.equal(events.length, eventCount);
});

test("mock client cancels an active local stream", async () => {
  const client = new MockOcgRuntimeClient("long-stream");
  await client.sendMessage("design-pwa-shell", { content: "Cancel this" });
  await client.cancel?.("design-pwa-shell");

  const messages = await client.getMessages("design-pwa-shell");
  assert.equal(messages.at(-1)?.status, "cancelled");
});

test("disconnected runtime does not pretend to send a message", async () => {
  const client = new MockOcgRuntimeClient("runtime-disconnected");
  const before = await client.getMessages("design-pwa-shell");
  const events: string[] = [];
  client.subscribe((event) => events.push(event.type));

  await client.sendMessage("design-pwa-shell", { content: "This stays local" });

  assert.deepEqual(await client.getMessages("design-pwa-shell"), before);
  assert.ok(events.includes("warning"));
});

test("live observability updates flow through the runtime snapshot", async () => {
  const client = new MockOcgRuntimeClient("observability-live");
  const events: string[] = [];
  client.subscribe((event) => events.push(event.type));

  await new Promise((resolve) => setTimeout(resolve, 2200));

  const observability = await client.getObservability("design-pwa-shell");
  const snapshotObservability = client.getSnapshot().observabilityBySession["design-pwa-shell"];
  const mission = await client.getMission("design-pwa-shell");
  assert.equal(mission?.status, "completed");
  assert.equal(observability?.mission.tokenUsage.total?.provenance, "reported");
  assert.equal(observability?.mission.activeWorkerCount, 0);
  assert.equal(snapshotObservability?.mission.tokenUsage.total?.value, 16300);
  assert.ok(events.filter((event) => event === "observability.updated").length >= 3);
  assert.ok((observability?.timeline.length ?? 0) <= 60);
});

/* -------------------------------------------------------------------------- */
/* Mission launch boundary                                                    */
/* -------------------------------------------------------------------------- */

test("launchMission projects a deterministic Mission, execution, and observability", async () => {
  const client = new MockOcgRuntimeClient("normal-chat");
  const events: string[] = [];
  client.subscribe((event) => events.push(event.type));
  const command = launchCommand("zhuju", "design-pwa-shell");

  const result = await client.launchMission!(command);
  assert.equal(result.outcome, "accepted");
  assert.equal(result.duplicate, false);
  assert.ok(result.missionId);

  const mission = await client.getMission("design-pwa-shell");
  assert.equal(mission?.status, "running");
  assert.equal(mission?.goal, command.objective);
  assert.equal(mission?.tasks.length, 3);
  assert.equal(mission?.budget.limit, 25);
  assert.equal(mission?.budget.spent, 0);
  assert.equal(mission?.commitment.mode, "capped");

  const snapshot = client.getSnapshot();
  const execution = snapshot.executionBySession["design-pwa-shell"];
  assert.ok(execution);
  assert.equal(execution!.tasks.length, 3);
  assert.equal(execution!.status, "running");
  assert.equal(execution!.budget?.commitmentPercent, 50);
  assert.ok(snapshot.observabilityBySession["design-pwa-shell"]);

  assert.ok(events.includes("mission.updated"));
  assert.ok(events.includes("execution.updated"));
  assert.ok(events.includes("observability.updated"));
  assert.ok(events.includes("mission.launch-updated"));

  // Byte-identical projection for a fresh client and the same command.
  const replay = new MockOcgRuntimeClient("normal-chat");
  await replay.launchMission!(command);
  assert.deepEqual(
    replay.getSnapshot().executionBySession["design-pwa-shell"],
    execution,
  );
});

test("launchMission rejects a Project that does not own the session", async () => {
  const client = new MockOcgRuntimeClient("normal-chat");
  const before = client.getSnapshot();
  const command = launchCommand("zhuju", "research-space-bunny");
  const result = await client.launchMission!(command);

  assert.equal(result.outcome, "rejected");
  assert.match(result.message, /does not own/);
  assert.equal(client.getSyncState().commandResults[command.commandId]?.outcome, "rejected");
  assert.deepEqual(client.getSnapshot().missionsBySession, before.missionsBySession);
  assert.deepEqual(client.getSnapshot().executionBySession, before.executionBySession);
  assert.equal(client.getSnapshot().executionBySession["research-space-bunny"], null);
});

test("launchMission returns the recorded result for a duplicate command identity", async () => {
  const client = new MockOcgRuntimeClient("normal-chat");
  const command = launchCommand("zhuju", "design-pwa-shell");

  const first = await client.launchMission!(command);
  const missionAfterFirst = client.getSnapshot().missionsBySession["design-pwa-shell"];
  const executionAfterFirst = client.getSnapshot().executionBySession["design-pwa-shell"];

  const second = await client.launchMission!(command);
  assert.equal(second.outcome, "accepted");
  assert.equal(second.duplicate, true);
  assert.equal(second.missionId, first.missionId);
  assert.deepEqual(client.getSnapshot().missionsBySession["design-pwa-shell"], missionAfterFirst);
  assert.deepEqual(client.getSnapshot().executionBySession["design-pwa-shell"], executionAfterFirst);
});

test("a disconnected runtime fails the launch without mutating state", async () => {
  const client = new MockOcgRuntimeClient("runtime-disconnected");
  const before = client.getSnapshot();
  const events: string[] = [];
  client.subscribe((event) => events.push(event.type));

  const result = await client.launchMission!(launchCommand("zhuju", "design-pwa-shell"));
  assert.equal(result.outcome, "failed");
  assert.match(result.message, /not connected|disconnected|no local runtime/i);
  assert.deepEqual(client.getSnapshot().missionsBySession, before.missionsBySession);
  assert.deepEqual(client.getSnapshot().executionBySession, before.executionBySession);
  assert.ok(events.includes("mission.launch-updated"));

  const retry = await client.launchMission!(launchCommand("zhuju", "design-pwa-shell"));
  assert.equal(retry.outcome, "failed");
  assert.equal(retry.duplicate, false);
});

test("an accepted launch is visible through the Project-scoped snapshot", async () => {
  const client = new MockOcgRuntimeClient("normal-chat");
  await client.launchMission!(launchCommand("zhuju", "design-pwa-shell"));

  const scoped = selectProjectSnapshot(client.getSnapshot(), "zhuju");
  assert.ok(scoped.missionsBySession["design-pwa-shell"]);
  assert.ok(scoped.executionBySession["design-pwa-shell"]);
  assert.equal(scoped.executionBySession["design-pwa-shell"]!.tasks.length, 3);

  const otherProject = selectProjectSnapshot(client.getSnapshot(), "route-lace");
  assert.equal(otherProject.executionBySession["design-pwa-shell"], undefined);
});
