import type {
  ChatMessage,
  ChatSession,
  Mission,
  OcgRuntimeEvent,
  RuntimeStatus,
  SendMessageInput,
  WorkType,
} from "../types";
import { createScenarioFixture } from "./scenarios";
import {
  advanceOnboarding,
  resolveAccessHandoff,
  resolveBootstrapRetry,
  selectActiveOnboardingStage,
  selectNextStage,
} from "../bootstrap/selectors";
import { ONBOARDING_STAGES, type BootstrapState, type OnboardingStageId } from "../bootstrap/types";
import type {
  CreateSessionInput,
  MissionLaunchCommand,
  MissionLaunchResult,
  OcgRuntimeClient,
  RuntimeSnapshot,
  ScenarioId,
} from "./runtime-types";
import { PROJECTS, isProjectId, type ProjectId } from "../project/domain";
import { projectSessionIds } from "../project/fixtures";
import {
  createLaunchedExecution,
  createLaunchedMission,
  createLaunchedObservability,
  missionIdForLaunch,
} from "../mission/launch-fixtures";
import { RuntimeEnvelopeFactory, eventSessionId, type AnyRuntimeEnvelope } from "./runtime-envelope";
import { createSnapshotEnvelopeFromFixture } from "./runtime-snapshot";
import { RuntimeStore } from "./runtime-store";
import { createUninitializedRuntimeState, type RuntimeState, type RuntimeSyncState } from "./reconciler";

type Timer = ReturnType<typeof setTimeout>;

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function clockLabel(): string {
  const now = new Date();
  return `${String(now.getHours()).padStart(2, "0")}:${String(now.getMinutes()).padStart(2, "0")}`;
}

export class MockOcgRuntimeClient implements OcgRuntimeClient {
  private readonly scenario: ScenarioId;
  private readonly listeners = new Set<(event: OcgRuntimeEvent) => void>();
  private readonly timers = new Map<string, Timer[]>();
  private nextId = 0;
  private readonly store: RuntimeStore;
  private readonly envelopes: RuntimeEnvelopeFactory;
  private liveScenarioStarted = false;
  /** Accepted launch results keyed by stable command identity for idempotency. */
  private readonly launchResults = new Map<string, MissionLaunchResult>();

  constructor(scenario: ScenarioId) {
    this.scenario = scenario;
    const fixture = createScenarioFixture(scenario);
    const seed = createSnapshotEnvelopeFromFixture(fixture, { streamId: `stream:${scenario}`, generation: 1 });
    this.store = new RuntimeStore(createUninitializedRuntimeState(scenario));
    this.store.installSnapshot(seed);
    this.envelopes = new RuntimeEnvelopeFactory(seed.streamId, seed.generation, {
      startSequence: seed.cursor.sequence + 1,
    });
  }

  getSnapshot(): RuntimeSnapshot {
    return this.store.getSnapshot();
  }

  getRuntimeState(): RuntimeState {
    return this.store.getState();
  }

  getSyncState(): RuntimeSyncState {
    return this.store.getSync();
  }

  async getRuntimeStatus(): Promise<RuntimeStatus> {
    return clone(this.store.getSnapshot().status);
  }

  async listSessions(): Promise<ChatSession[]> {
    return clone(this.store.getSnapshot().sessions);
  }

  async getSession(id: string): Promise<ChatSession | null> {
    const session = this.store.getSnapshot().sessions.find((item) => item.id === id);
    return session ? clone(session) : null;
  }

  async getMessages(sessionId: string): Promise<ChatMessage[]> {
    return clone(this.store.getSnapshot().messagesBySession[sessionId] ?? []);
  }

  async getMission(sessionId: string): Promise<Mission | null> {
    return clone(this.store.getSnapshot().missionsBySession[sessionId] ?? null);
  }

  async getObservability(sessionId: string) {
    const observability = this.store.getSnapshot().observabilityBySession[sessionId];
    return observability ? clone(observability) : null;
  }

  async getBootstrap(): Promise<BootstrapState> {
    return clone(this.store.getSnapshot().bootstrap);
  }

  async requestAccessHandoff(): Promise<void> {
    const bootstrap = this.store.getSnapshot().bootstrap;
    const access = resolveAccessHandoff(bootstrap.access);
    if (access === bootstrap.access) return;
    this.updateBootstrap({ ...bootstrap, access });
  }

  async setOnboardingStage(stage: OnboardingStageId): Promise<void> {
    const bootstrap = this.store.getSnapshot().bootstrap;
    const onboarding = bootstrap.onboarding;
    if (!onboarding) return;

    const current = selectActiveOnboardingStage(bootstrap);
    const next = selectNextStage(current);
    if (stage === next) {
      const advanced = advanceOnboarding(bootstrap);
      if (advanced !== bootstrap) this.updateBootstrap(advanced);
      return;
    }

    // Back navigation may revisit a completed stage. Future stages are never
    // directly selectable, even if a caller bypasses the presentational UI.
    const currentIndex = ONBOARDING_STAGES.indexOf(current);
    const requestedIndex = ONBOARDING_STAGES.indexOf(stage);
    if (requestedIndex < 0 || requestedIndex > currentIndex || (requestedIndex < currentIndex && !onboarding.completedStages.includes(stage))) return;
    this.updateBootstrap({
      ...bootstrap,
      onboarding: { ...onboarding, stage, canResume: onboarding.canResume || onboarding.completedStages.length > 0, failure: undefined },
    });
  }

  async completeOnboarding(): Promise<void> {
    const bootstrap = this.store.getSnapshot().bootstrap;
    const onboarding = bootstrap.onboarding;
    if (!onboarding || selectActiveOnboardingStage(bootstrap) !== "ready") return;
    this.updateBootstrap({
      ...bootstrap,
      ready: true,
      onboarding: {
        ...onboarding,
        stage: "ready",
        completedStages: onboarding.completedStages.includes("ready")
          ? onboarding.completedStages
          : [...onboarding.completedStages, "ready"],
        failure: undefined,
      },
    });
  }

  async retryBootstrap(): Promise<void> {
    this.updateBootstrap(resolveBootstrapRetry(this.store.getSnapshot().bootstrap));
  }

  /**
   * Frontend-only profile selection. Only a profile that exists in the
   * normalized state can become active, and nothing is written outside the
   * in-memory snapshot.
   */
  async setActiveProfile(profileId: string): Promise<void> {
    const bootstrap = this.store.getSnapshot().bootstrap;
    const profile = bootstrap.profiles.find((item) => item.id === profileId);
    if (!profile) return;
    if (bootstrap.activeProfileId === profile.id) return;
    this.updateBootstrap({ ...bootstrap, activeProfileId: profile.id });
  }

  async createSession(input: CreateSessionInput): Promise<ChatSession> {
    const id = `mock-session-${Date.now()}-${this.nextId++}`;
    const session: ChatSession = {
      id,
      title: input.title?.trim() || "Untitled thread",
      workType: input.workType,
      updatedAt: "now",
    };
    this.emit({ type: "conversation.session-created", session: clone(session) });
    return clone(session);
  }

  async sendMessage(sessionId: string, input: SendMessageInput): Promise<void> {
    const content = input.content.trim();
    if (!content) return;

    const snapshot = this.store.getSnapshot();
    if (snapshot.status.state !== "connected") {
      this.emit({
        type: "warning",
        message: snapshot.status.detail ?? "The local runtime is not connected.",
      });
      return;
    }

    const session = snapshot.sessions.find((item) => item.id === sessionId);
    if (!session) {
      this.emit({ type: "error", message: `Unknown mock session: ${sessionId}` });
      return;
    }

    const userMessage: ChatMessage = {
      id: `mock-user-${Date.now()}-${this.nextId++}`,
      role: "user",
      content,
      createdAt: clockLabel(),
      status: "completed",
    };
    this.emit({ type: "conversation.message-started", sessionId, message: clone(userMessage) });

    const updatedSession = { ...session, updatedAt: "now" };
    this.emit({ type: "conversation.session-updated", session: clone(updatedSession) });

    const currentMission = this.store.getSnapshot().missionsBySession[sessionId];
    if (currentMission?.status === "running") {
      const updatedMission = { ...currentMission, current: "Responding to operator" };
      this.emit({ type: "mission.updated", sessionId, mission: clone(updatedMission) });
    }

    const assistantId = `mock-assistant-${Date.now()}-${this.nextId++}`;
    const assistant: ChatMessage = {
      id: assistantId,
      role: "assistant",
      content: "",
      createdAt: clockLabel(),
      status: "streaming",
    };
    this.emit({ type: "conversation.message-started", sessionId, message: clone(assistant) });

    const fixture = createScenarioFixture(this.scenario);
    const chunks = fixture.streamChunks ?? ["Mock runtime response."];
    const delay = fixture.streamDelayMs ?? 90;
    const timers: Timer[] = [];

    chunks.forEach((chunk, index) => {
      timers.push(setTimeout(() => {
        const current = this.store.getSnapshot().messagesBySession[sessionId]?.find((item) => item.id === assistantId);
        if (!current || current.status === "cancelled") return;
        this.emit({ type: "conversation.message-delta", sessionId, messageId: assistantId, delta: chunk });

        if (index === chunks.length - 1) {
          // The delta has already gone through the canonical reconciler. Read
          // the committed message back so multi-chunk streams do not drop
          // earlier chunks when the terminal replacement arrives.
          const afterDelta = this.store.getSnapshot().messagesBySession[sessionId]?.find((item) => item.id === assistantId);
          if (!afterDelta) return;
          const completed = { ...afterDelta, status: "completed" as const };
          this.emit({ type: "conversation.message-completed", sessionId, message: clone(completed) });
          this.timers.delete(sessionId);
        }
      }, delay * (index + 1)));
    });
    this.timers.set(sessionId, timers);
  }

  subscribe(listener: (event: OcgRuntimeEvent) => void): () => void {
    this.listeners.add(listener);
    this.startLiveScenario();
    return () => this.listeners.delete(listener);
  }

  async cancel(sessionId: string): Promise<void> {
    const timers = this.timers.get(sessionId) ?? [];
    timers.forEach((timer) => clearTimeout(timer));
    this.timers.delete(sessionId);
    const messages = this.store.getSnapshot().messagesBySession[sessionId] ?? [];
    const active = [...messages].reverse().find((message) => message.status === "streaming");
    if (active) {
      this.emit({ type: "cancelled", sessionId, messageId: active.id });
    } else {
      this.emit({ type: "cancelled", sessionId });
    }
  }

  /**
   * Deterministic, frontend-only Mission launch.
   *
   * - disconnected runtimes fail without mutating the snapshot;
   * - a session owned by another Project is rejected;
   * - a repeated accepted command identity returns the previously recorded result;
   * - rejected/failed attempts remain retryable if the runtime condition changes;
   * - an accepted command projects Mission/execution/observability through the
   *   canonical reconciler, never through a parallel UI mutation.
   */
  async launchMission(command: MissionLaunchCommand): Promise<MissionLaunchResult> {
    const prior = this.launchResults.get(command.commandId);
    if (prior) {
      return { ...clone(prior), duplicate: true };
    }

    const result = this.resolveLaunch(command);
    // Only an accepted projection is durable within this fixture. Adapter
    // failures and rejections do not mutate state and must remain retryable;
    // accepted commands are the ones that need duplicate protection.
    if (result.outcome === "accepted") this.launchResults.set(command.commandId, clone(result));
    this.emit(
      { type: "mission.launch-updated", sessionId: command.sessionId, result: clone(result) },
      { projectId: command.projectId ?? null, commandId: command.commandId },
    );
    return clone(result);
  }

  private resolveLaunch(command: MissionLaunchCommand): MissionLaunchResult {
    const base = {
      commandId: command.commandId,
      draftId: command.draftId,
      projectId: command.projectId,
      sessionId: command.sessionId,
      duplicate: false,
    };

    const snapshot = this.store.getSnapshot();
    if (snapshot.status.state !== "connected") {
      return {
        ...base,
        outcome: "failed",
        message: snapshot.status.detail ?? "The local runtime is not connected; launch not attempted.",
      };
    }

    if (!isProjectId(command.projectId)) {
      return { ...base, outcome: "rejected", message: `Unknown Project "${command.projectId}"; launch rejected.` };
    }

    const session = snapshot.sessions.find((item) => item.id === command.sessionId);
    if (!session) {
      return { ...base, outcome: "failed", message: `Unknown session "${command.sessionId}"; launch failed.` };
    }

    const owner = this.ownerProjectForSession(command.sessionId);
    if (owner && owner !== command.projectId) {
      return {
        ...base,
        outcome: "rejected",
        message: `Project "${command.projectId}" does not own session "${command.sessionId}" (owned by "${owner}"); launch rejected.`,
      };
    }

    if (!Number.isSafeInteger(command.hardBudgetMicros) || command.hardBudgetMicros <= 0) {
      return { ...base, outcome: "rejected", message: "The hard budget must be a positive whole number of micros; launch rejected." };
    }

    const missionId = missionIdForLaunch(command);
    const mission = createLaunchedMission(command, missionId);
    const execution = createLaunchedExecution(command, missionId);
    const observability = createLaunchedObservability(command, missionId);
    const projectId = command.projectId ?? null;
    const result: MissionLaunchResult = {
      ...base,
      outcome: "accepted",
      missionId,
      message: `Mission "${mission.title}" accepted for execution.`,
    };

    // Acknowledgement and entity projections share the same canonical path.
    // The acknowledgement is first so command correlation is observable before
    // the resulting Mission/execution projections arrive.
    this.emit(
      { type: "mission.launch-updated", sessionId: command.sessionId, result: clone(result) },
      { projectId, commandId: command.commandId, missionId },
    );
    const correlation = { projectId, commandId: command.commandId, missionId };
    this.emit({ type: "mission.updated", sessionId: command.sessionId, mission: clone(mission) }, correlation);
    this.emit({ type: "execution.updated", sessionId: command.sessionId, execution: clone(execution) }, correlation);
    this.emit({ type: "observability.updated", sessionId: command.sessionId, observability: clone(observability) }, correlation);

    return result;
  }

  private ownerProjectForSession(sessionId: string): ProjectId | null {
    for (const project of PROJECTS) {
      if (projectSessionIds(project.id).includes(sessionId)) return project.id;
    }
    return null;
  }

  private updateBootstrap(bootstrap: BootstrapState): void {
    this.emit({ type: "bootstrap.updated", bootstrap: clone(bootstrap) });
  }

  /**
   * Stamp a deterministic envelope, apply it through the canonical store, then
   * notify raw listeners. State is always updated before listeners run.
   */
  private emit(
    event: OcgRuntimeEvent,
    scope: { projectId?: ProjectId | null; commandId?: string; missionId?: string } = {},
  ): void {
    const envelope = this.stampEnvelope(event, scope);
    this.store.applyEnvelope(envelope);
    for (const listener of this.listeners) listener(event);
  }

  private stampEnvelope(
    event: OcgRuntimeEvent,
    scope: { projectId?: ProjectId | null; commandId?: string; missionId?: string },
  ): AnyRuntimeEnvelope {
    const sessionId = eventSessionId(event);
    const projectId = scope.projectId !== undefined
      ? scope.projectId
      : sessionId
        ? this.ownerProjectForSession(sessionId)
        : null;
    return this.envelopes.fromRuntimeEvent(event, {
      projectId,
      commandId: scope.commandId,
      missionId: scope.missionId,
      sessionId,
    });
  }

  private startLiveScenario(): void {
    if (this.scenario !== "observability-live" || this.liveScenarioStarted) return;
    this.liveScenarioStarted = true;
    const fixture = createScenarioFixture(this.scenario);
    fixture.observabilityUpdates?.forEach((update) => {
      const timer = setTimeout(() => {
        this.emit({
          type: "observability.updated",
          sessionId: update.sessionId,
          observability: clone(update.observability),
        });
        if (update.mission) {
          this.emit({ type: "mission.updated", sessionId: update.sessionId, mission: clone(update.mission) });
        }
      }, update.afterMs);
      const timers = this.timers.get("__observability__") ?? [];
      this.timers.set("__observability__", [...timers, timer]);
    });
  }
}

export function createMockOcgRuntimeClient(scenario: ScenarioId): OcgRuntimeClient {
  return new MockOcgRuntimeClient(scenario);
}

export type MockWorkType = WorkType;
