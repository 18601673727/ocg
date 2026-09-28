/**
 * Deterministic runtime transport contract and a replay/local implementation.
 *
 * There is intentionally no HTTP, SSE, or WebSocket implementation. The
 * `RuntimeTransport` interface is the seam a future adapter will implement; the
 * replay transport delivers scripted envelopes step by step so sync tests are
 * fully deterministic (no timers, no randomness).
 */

import type { ProjectId } from "../project/domain";
import type { MissionLaunchCommand } from "../mission/draft-domain";
import type { AnyRuntimeEnvelope, RuntimeError, RuntimeResumeCursor } from "./runtime-envelope";
import {
  RuntimeEnvelopeFactory,
  RUNTIME_PROTOCOL_VERSION,
} from "./runtime-envelope";
import type { RuntimeSnapshotEnvelope } from "./runtime-snapshot";
import { createSnapshotEnvelopeFromFixture } from "./runtime-snapshot";
import type { RuntimeSnapshot, ScenarioId } from "./runtime-types";
import { createScenarioFixture } from "./scenarios";

export type RuntimeTransportCommand =
  | {
      kind: "launch-mission";
      commandId: string;
      projectId: ProjectId;
      sessionId: string;
      command: MissionLaunchCommand;
    }
  | { kind: "send-message"; commandId: string; sessionId: string; content: string }
  | { kind: "cancel"; commandId: string; sessionId: string; messageId?: string };

export type RuntimeCommandStatus = "accepted" | "duplicate" | "rejected";

export type RuntimeCommandResult = {
  status: RuntimeCommandStatus;
  commandId: string;
  events: readonly AnyRuntimeEnvelope[];
  error?: RuntimeError;
};

export interface RuntimeTransport {
  getSnapshot(projectId?: ProjectId): Promise<RuntimeSnapshotEnvelope>;
  subscribe(
    listener: (envelope: AnyRuntimeEnvelope) => void,
    options?: { projectId?: ProjectId; resumeCursor?: RuntimeResumeCursor },
  ): () => void;
  execute(command: RuntimeTransportCommand): Promise<RuntimeCommandResult>;
  close(): void;
}

export type ReplayStep =
  | { kind: "snapshot"; envelope: RuntimeSnapshotEnvelope }
  | { kind: "event"; envelope: AnyRuntimeEnvelope };

export class ReplayRuntimeTransport implements RuntimeTransport {
  private index = 0;
  private closed = false;
  private readonly listeners = new Set<(envelope: AnyRuntimeEnvelope) => void>();
  private readonly recorded = new Map<string, RuntimeCommandResult>();

  constructor(
    private readonly steps: readonly ReplayStep[],
    private readonly scriptedCommands: Readonly<Record<string, RuntimeCommandResult>> = {},
  ) {}

  /** The baseline the replay is built on. */
  seedSnapshot(): RuntimeSnapshot {
    const step = this.steps.find((item) => item.kind === "snapshot");
    if (!step) throw new Error("Replay transport has no snapshot step.");
    return step.envelope.snapshot;
  }

  async getSnapshot(projectId?: ProjectId): Promise<RuntimeSnapshotEnvelope> {
    void projectId;
    const step = this.steps.find((item) => item.kind === "snapshot");
    if (!step) throw new Error("Replay transport has no snapshot step.");
    return step.envelope;
  }

  subscribe(
    listener: (envelope: AnyRuntimeEnvelope) => void,
    options?: { projectId?: ProjectId; resumeCursor?: RuntimeResumeCursor },
  ): () => void {
    void options;
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  /** Deliver exactly one scripted step. Returns the step, or null at the end. */
  step(): ReplayStep | null {
    if (this.closed) return null;
    const current = this.steps[this.index];
    if (!current) return null;
    this.index += 1;
    if (current.kind === "event") {
      for (const listener of [...this.listeners]) listener(current.envelope);
    }
    return current;
  }

  /** Deliver every remaining event step and return them. */
  drain(): AnyRuntimeEnvelope[] {
    const delivered: AnyRuntimeEnvelope[] = [];
    while (this.index < this.steps.length) {
      const next = this.step();
      if (next?.kind === "event") delivered.push(next.envelope);
    }
    return delivered;
  }

  remaining(): number {
    return Math.max(0, this.steps.length - this.index);
  }

  async execute(command: RuntimeTransportCommand): Promise<RuntimeCommandResult> {
    if (this.closed) {
      return {
        status: "rejected",
        commandId: command.commandId,
        events: [],
        error: { code: "transport_unavailable", message: "Replay transport is closed.", retryable: false },
      };
    }
    const prior = this.recorded.get(command.commandId);
    if (prior) return { ...prior, status: "duplicate" };
    const scripted = this.scriptedCommands[command.commandId];
    const result: RuntimeCommandResult = scripted ?? { status: "accepted", commandId: command.commandId, events: [] };
    this.recorded.set(command.commandId, result);
    return result;
  }

  close(): void {
    this.closed = true;
    this.listeners.clear();
  }
}

/* -------------------------------------------------------------------------- */
/* Deterministic replay scenarios                                             */
/* -------------------------------------------------------------------------- */

export type ReplayScenarioId =
  | "clean-stream"
  | "duplicate"
  | "out-of-order"
  | "gap"
  | "reconnect-new-generation"
  | "old-generation-late-event"
  | "command-ack";

export type ReplayScenario = {
  id: ReplayScenarioId;
  streamId: string;
  generation: number;
  steps: ReplayStep[];
};

type Seed = {
  scenario: ScenarioId;
  generation: number;
  sequence: number;
  streamId: string;
  snapshot: RuntimeSnapshotEnvelope;
  fixture: ReturnType<typeof createScenarioFixture>;
  factory: RuntimeEnvelopeFactory;
};

function createSeed(scenario: ScenarioId, generation = 1, sequence = 0): Seed {
  const streamId = `stream:replay:${scenario}`;
  const fixture = createScenarioFixture(scenario);
  const snapshot = createSnapshotEnvelopeFromFixture(fixture, { streamId, generation, sequence });
  return {
    scenario,
    generation,
    sequence,
    streamId,
    snapshot,
    fixture,
    factory: new RuntimeEnvelopeFactory(streamId, generation, { startSequence: sequence + 1 }),
  };
}

function snapshotStep(seed: Seed): ReplayStep {
  return { kind: "snapshot", envelope: seed.snapshot };
}

function eventStep(envelope: AnyRuntimeEnvelope): ReplayStep {
  return { kind: "event", envelope };
}

/** Build a deterministic replay scenario from scripted envelopes. */
export function createReplayScenario(id: ReplayScenarioId, scenario: ScenarioId = "normal-chat"): ReplayScenario {
  switch (id) {
    case "clean-stream": {
      const seed = createSeed(scenario);
      const sessionId = "design-pwa-shell";
      const started = seed.factory.envelope(
        "conversation.message-started",
        { message: { id: "replay-msg-1", role: "assistant", content: "", createdAt: "00:00", status: "streaming" } },
        { projectId: "zhuju", sessionId },
      );
      const delta1 = seed.factory.envelope(
        "conversation.message-delta",
        { messageId: "replay-msg-1", delta: "Hello ", turnId: "replay-turn-1", deltaSequence: 1 },
        { projectId: "zhuju", sessionId },
      );
      const delta2 = seed.factory.envelope(
        "conversation.message-delta",
        { messageId: "replay-msg-1", delta: "world", turnId: "replay-turn-1", deltaSequence: 2 },
        { projectId: "zhuju", sessionId },
      );
      const completed = seed.factory.envelope(
        "conversation.message-completed",
        { message: { id: "replay-msg-1", role: "assistant", content: "Hello world", createdAt: "00:00", status: "completed" } },
        { projectId: "zhuju", sessionId },
      );
      return { id, streamId: seed.streamId, generation: seed.generation, steps: [snapshotStep(seed), eventStep(started), eventStep(delta1), eventStep(delta2), eventStep(completed)] };
    }

    case "duplicate": {
      const seed = createSeed(scenario);
      const sessionId = "design-pwa-shell";
      const started = seed.factory.envelope(
        "conversation.message-started",
        { message: { id: "replay-msg-1", role: "assistant", content: "", createdAt: "00:00", status: "streaming" } },
        { projectId: "zhuju", sessionId },
      );
      const delta = seed.factory.envelope(
        "conversation.message-delta",
        { messageId: "replay-msg-1", delta: "once", turnId: "replay-turn-1", deltaSequence: 1 },
        { projectId: "zhuju", sessionId },
      );
      return { id, streamId: seed.streamId, generation: seed.generation, steps: [snapshotStep(seed), eventStep(started), eventStep(delta), eventStep(delta)] };
    }

    case "out-of-order": {
      const seed = createSeed(scenario);
      const sessionId = "design-pwa-shell";
      const started = seed.factory.envelope(
        "conversation.message-started",
        { message: { id: "replay-msg-1", role: "assistant", content: "", createdAt: "00:00", status: "streaming" } },
        { projectId: "zhuju", sessionId },
      );
      const first = seed.factory.envelope(
        "conversation.message-delta",
        { messageId: "replay-msg-1", delta: "first", turnId: "replay-turn-1", deltaSequence: 1 },
        { projectId: "zhuju", sessionId },
      );
      const second = seed.factory.envelope(
        "conversation.message-delta",
        { messageId: "replay-msg-1", delta: "second", turnId: "replay-turn-1", deltaSequence: 2 },
        { projectId: "zhuju", sessionId },
      );
      return { id, streamId: seed.streamId, generation: seed.generation, steps: [snapshotStep(seed), eventStep(started), eventStep(second), eventStep(first), eventStep(second)] };
    }

    case "gap": {
      const seed = createSeed(scenario);
      const sessionId = "design-pwa-shell";
      const started = seed.factory.envelope(
        "conversation.message-started",
        { message: { id: "replay-msg-1", role: "assistant", content: "", createdAt: "00:00", status: "streaming" } },
        { projectId: "zhuju", sessionId },
      );
      const late = seed.factory.envelope(
        "conversation.message-delta",
        { messageId: "replay-msg-1", delta: "late", turnId: "replay-turn-1", deltaSequence: 1 },
        { projectId: "zhuju", sessionId },
      );
      // Force a gap by skipping two sequences.
      const gapped: AnyRuntimeEnvelope = { ...late, sequence: 5, eventId: `${seed.streamId}:5` };
      return { id, streamId: seed.streamId, generation: seed.generation, steps: [snapshotStep(seed), eventStep(started), eventStep(gapped)] };
    }

    case "reconnect-new-generation": {
      const seed = createSeed(scenario);
      const sessionId = "design-pwa-shell";
      const started = seed.factory.envelope(
        "conversation.message-started",
        { message: { id: "replay-msg-1", role: "assistant", content: "", createdAt: "00:00", status: "streaming" } },
        { projectId: "zhuju", sessionId },
      );
      const stale = seed.factory.envelope(
        "conversation.message-delta",
        { messageId: "replay-msg-1", delta: "stale", turnId: "replay-turn-1", deltaSequence: 1 },
        { projectId: "zhuju", sessionId },
      );
      const gapped: AnyRuntimeEnvelope = { ...stale, sequence: 9, eventId: `${seed.streamId}:9` };
      const next = createSeed(scenario, 2, 10);
      const resumedStart = next.factory.envelope(
        "conversation.message-started",
        { message: { id: "replay-msg-2", role: "assistant", content: "", createdAt: "00:00", status: "streaming" } },
        { projectId: "zhuju", sessionId },
      );
      const resumed = next.factory.envelope(
        "conversation.message-delta",
        { messageId: "replay-msg-2", delta: "resumed", turnId: "replay-turn-2", deltaSequence: 1 },
        { projectId: "zhuju", sessionId },
      );
      return {
        id,
        streamId: next.streamId,
        generation: 2,
        steps: [snapshotStep(seed), eventStep(started), eventStep(gapped), snapshotStep(next), eventStep(resumedStart), eventStep(resumed)],
      };
    }

    case "old-generation-late-event": {
      const seed = createSeed(scenario, 2, 10);
      const old: AnyRuntimeEnvelope = {
        protocolVersion: RUNTIME_PROTOCOL_VERSION,
        eventVersion: 1,
        streamId: seed.streamId,
        generation: 1,
        sequence: 4,
        eventId: `${seed.streamId}:old:4`,
        projectId: "zhuju",
        sessionId: "design-pwa-shell",
        occurredAt: new Date(0).toISOString(),
        type: "conversation.message-delta",
        payload: { messageId: "replay-msg-1", delta: "old", turnId: "replay-turn-old", deltaSequence: 1 },
      };
      return { id, streamId: seed.streamId, generation: 2, steps: [snapshotStep(seed), eventStep(old)] };
    }

    case "command-ack": {
      const seed = createSeed("mission-control");
      const sessionId = "design-pwa-shell";
      const mission = seed.fixture.missionsBySession[sessionId]!;
      const accepted = seed.factory.envelope(
        "mission.launch-updated",
        {
          result: {
            outcome: "accepted",
            commandId: "replay-command-1",
            draftId: "replay-draft-1",
            projectId: "zhuju",
            sessionId,
            missionId: "mission-replay-draft-1",
            message: "Mission accepted.",
            duplicate: false,
          },
        },
        { projectId: "zhuju", sessionId, commandId: "replay-command-1" },
      );
      const missionUpdate = seed.factory.envelope(
        "mission.updated",
        { mission },
        { projectId: "zhuju", sessionId },
      );
      const rejected = seed.factory.envelope(
        "mission.launch-updated",
        {
          result: {
            outcome: "rejected",
            commandId: "replay-command-2",
            draftId: "replay-draft-2",
            projectId: "zhuju",
            sessionId,
            message: "Launch rejected.",
            duplicate: false,
          },
        },
        { projectId: "zhuju", sessionId, commandId: "replay-command-2" },
      );
      return { id, streamId: seed.streamId, generation: seed.generation, steps: [snapshotStep(seed), eventStep(accepted), eventStep(missionUpdate), eventStep(rejected)] };
    }
  }
}

/** Build a replay transport directly from a scripted scenario. */
export function createReplayTransport(id: ReplayScenarioId, scenario: ScenarioId = "normal-chat"): ReplayRuntimeTransport {
  const replay = createReplayScenario(id, scenario);
  return new ReplayRuntimeTransport(replay.steps);
}
