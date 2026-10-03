import type {
  ChatMessage,
  ChatSession,
  OcgRuntimeEvent,
  RuntimeStatus,
  SendMessageInput,
} from "../types";
import { createScenarioFixture } from "./scenarios";
import { retryContent } from "../chat/retry";
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
import { RuntimeEnvelopeFactory, eventSessionId, RUNTIME_PROTOCOL_VERSION, type AnyRuntimeEnvelope } from "./runtime-envelope";
import { createSnapshotEnvelopeFromFixture, emptyRuntimeSnapshot } from "./runtime-snapshot";
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

/** Shared runtime store/event machinery. Authority-specific clients choose
 * their seed explicitly; this base never makes fixture data authoritative. */
export class RuntimeClientBase implements OcgRuntimeClient {
  /**
   * The fixture runtime never owns Chat for a session that has a loopback
   * control endpoint: that path is the canonical backend's. This client is
   * reached only when the invocation has no control endpoint to be canonical
   * for, and it answers with the seeded fixtures rather than an execution.
   */
  readonly authority: RuntimeAuthority;

  private readonly scenario: ScenarioId;
  private readonly listeners = new Set<(event: OcgRuntimeEvent) => void>();
  private readonly timers = new Map<string, Timer[]>();
  private nextId = 0;
  private readonly drainingQueues = new Set<string>();
  /** The single runtime store. A backend-backed subclass projects real Job
   * execution through it rather than keeping a second runtime store. */
  protected readonly store: RuntimeStore;
  private readonly envelopes: RuntimeEnvelopeFactory;
  private liveScenarioStarted = false;

  constructor(scenario: ScenarioId, canonicalChat = false, authority: RuntimeAuthority = "mock") {
    this.authority = authority;
    this.scenario = scenario;
    const seed = canonicalChat
      ? {
        protocolVersion: RUNTIME_PROTOCOL_VERSION,
        streamId: "canonical",
        generation: 1,
        cursor: { streamId: "canonical", sequence: 0 },
        scope: { kind: "all-projects" as const },
        snapshot: { ...emptyRuntimeSnapshot(scenario), authority: "canonical" as const },
      }
      : createSnapshotEnvelopeFromFixture(createScenarioFixture(scenario), { streamId: `stream:${scenario}`, generation: 1 });
    this.liveScenarioStarted = canonicalChat;
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

  async retryMessage(sessionId: string, messageId: string): Promise<void> {
    const snapshot = this.store.getSnapshot();
    const content = retryContent(snapshot.messagesBySession[sessionId] ?? [], messageId);
    const session = snapshot.sessions.find(item => item.id === sessionId);
    if (!content || !session) throw new Error("This turn cannot be retried while another turn is active.");
    await this.sendMessage(sessionId, { content, projectId: session.projectId });
  }

  async sendMessage(sessionId: string, input: SendMessageInput): Promise<void> {
    if (this.enqueueIfBusy(sessionId, input)) return;
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

    if (input.mode === "steer") {
      (this.timers.get(sessionId) ?? []).forEach(timer => clearTimeout(timer));
      this.timers.delete(sessionId);
      for (const message of snapshot.messagesBySession[sessionId] ?? []) {
        if (message.role === "assistant" && message.status === "streaming") {
          this.emit({ type: "cancelled", sessionId, messageId: message.id });
        }
      }
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
    this.pauseQueue(sessionId);
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
  protected enqueueIfBusy(sessionId: string, input: SendMessageInput): boolean {
    if (!input.content.trim() || input.mode === "steer") return false;
    const snapshot = this.store.getSnapshot();
    if (!snapshot.sessions.some(session => session.id === sessionId)) return false;
    const active = (snapshot.messagesBySession[sessionId] ?? []).some(message =>
      message.role === "assistant" && (message.status === "streaming" || message.status === "pending"));
    const existing = snapshot.chatQueues?.[sessionId];
    if (!active && !existing?.queue.length) return false;
    this.emit({ type: "conversation.queue-updated", sessionId,
      queue: [...(existing?.queue ?? []), { id: crypto.randomUUID(), input: structuredClone(input) }],
      paused: existing?.paused ?? false });
    if (!active && !existing?.paused) this.drainQueue(sessionId);
    return true;
  }

  protected pauseQueue(sessionId: string): void {
    const queue = this.store.getSnapshot().chatQueues?.[sessionId]?.queue ?? [];
    this.emit({ type: "conversation.queue-updated", sessionId, queue, paused: true });
  }

  removeQueuedMessage(sessionId: string, id: string): void {
    const state = this.store.getSnapshot().chatQueues?.[sessionId];
    if (!state) return;
    this.emit({ type: "conversation.queue-updated", sessionId, queue: state.queue.filter(item => item.id !== id), paused: state.paused });
  }

  resumeQueue(sessionId: string): void {
    const state = this.store.getSnapshot().chatQueues?.[sessionId];
    if (!state) return;
    this.emit({ type: "conversation.queue-updated", sessionId, queue: state.queue, paused: false });
    this.drainQueue(sessionId);
  }

  private drainQueue(sessionId: string): void {
    if (this.drainingQueues.has(sessionId)) return;
    this.drainingQueues.add(sessionId);
    queueMicrotask(async () => {
      try {
        const snapshot = this.store.getSnapshot();
        const state = snapshot.chatQueues?.[sessionId];
        if (!state?.queue.length || state.paused) return;
        if (snapshot.status.state !== "connected") {
          this.pauseQueue(sessionId);
          return;
        }
        if ((snapshot.messagesBySession[sessionId] ?? []).some(message => message.role === "assistant" &&
            (message.status === "streaming" || message.status === "pending"))) return;
        const [next, ...queue] = state.queue;
        this.emit({ type: "conversation.queue-updated", sessionId, queue, paused: false });
        await this.sendMessage(sessionId, { ...next.input, mode: "steer" });
      } catch (cause) {
        this.pauseQueue(sessionId);
        this.emit({ type: "error", message: cause instanceof Error ? cause.message : String(cause) });
      } finally {
        this.drainingQueues.delete(sessionId);
      }
    });
  }

  protected emit(
    event: OcgRuntimeEvent,
    scope: { projectId?: ProjectId | null; commandId?: string } = {},
  ): void {
    const envelope = this.stampEnvelope(event, scope);
    this.store.applyEnvelope(envelope);
    for (const listener of this.listeners) listener(event);
    if (event.type === "conversation.message-completed" && event.message.role === "assistant") {
      if (event.message.status === "completed") this.drainQueue(event.sessionId);
      else this.pauseQueue(event.sessionId);
    }
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

/** Fixture/demo runtime. It is the only client that seeds scenario data. */
export class MockOcgRuntimeClient extends RuntimeClientBase {
  constructor(scenario: ScenarioId) {
    super(scenario, false, "mock");
  }
}
