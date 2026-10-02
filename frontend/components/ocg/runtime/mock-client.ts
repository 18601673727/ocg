import type {
  ChatMessage,
  ChatSession,
  OcgRuntimeEvent,
  RuntimeStatus,
  SendMessageInput,
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
  OcgRuntimeClient,
  RuntimeAuthority,
  RuntimeSnapshot,
  ScenarioId,
} from "./runtime-types";
import { type ProjectId } from "../project/domain";
import { FIXTURE_PROJECT_IDS, projectSessionIds } from "../project/fixtures";
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
  /**
   * The fixture runtime never owns Chat for a session that has a loopback
   * control endpoint: that path is the canonical backend's. This client is
   * reached only when the invocation has no control endpoint to be canonical
   * for, and it answers with the seeded fixtures rather than an execution.
   */
  readonly authority: RuntimeAuthority = "mock";

  private readonly scenario: ScenarioId;
  private readonly listeners = new Set<(event: OcgRuntimeEvent) => void>();
  private readonly timers = new Map<string, Timer[]>();
  private nextId = 0;
  /** The single runtime store. A backend-backed subclass projects real Job
   * execution through it rather than keeping a second runtime store. */
  protected readonly store: RuntimeStore;
  private readonly envelopes: RuntimeEnvelopeFactory;
  private liveScenarioStarted = false;

  constructor(scenario: ScenarioId, canonicalChat = false) {
    this.scenario = scenario;
    const fixture = createScenarioFixture(scenario);
    const seed = createSnapshotEnvelopeFromFixture(fixture, { streamId: `stream:${scenario}`, generation: 1 });
    if (canonicalChat) {
      this.liveScenarioStarted = true;
      seed.snapshot = {
        ...seed.snapshot,
        status: { state: "connecting", detail: "Loading OCG runtime status" },
        sessions: [], messagesBySession: {}, observabilityBySession: {},
        executionBySession: {}, accountingBySession: {},
      };
    }
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

  // This client deliberately does not implement `launchJob`. A mock cannot
  // create product execution: the normal path launches through the loopback
  // control plane (`CanonicalOcgRuntimeClient`) and projects the authoritative
  // canonical snapshot it returns. Fixture scenarios render the execution
  // fixtures they were seeded with, not a fabricated launch result.

  private ownerProjectForSession(sessionId: string): ProjectId | null {
    for (const projectId of FIXTURE_PROJECT_IDS) {
      if (projectSessionIds(projectId).includes(sessionId)) return projectId;
    }
    return null;
  }

  protected updateBootstrap(bootstrap: BootstrapState): void {
    this.emit({ type: "bootstrap.updated", bootstrap: clone(bootstrap) });
  }

  /**
   * Stamp a deterministic envelope, apply it through the canonical store, then
   * notify raw listeners. State is always updated before listeners run.
   */
  protected emit(
    event: OcgRuntimeEvent,
    scope: { projectId?: ProjectId | null; commandId?: string } = {},
  ): void {
    const envelope = this.stampEnvelope(event, scope);
    this.store.applyEnvelope(envelope);
    for (const listener of this.listeners) listener(event);
  }

  private stampEnvelope(
    event: OcgRuntimeEvent,
    scope: { projectId?: ProjectId | null; commandId?: string },
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
      }, update.afterMs);
      const timers = this.timers.get("__observability__") ?? [];
      this.timers.set("__observability__", [...timers, timer]);
    });
  }
}
