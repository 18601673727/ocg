import type {
  ChatMessage,
  ChatSession,
  OcgRuntimeEvent,
  RuntimeStatus,
  SendMessageInput,
} from "../types";
import type { BootstrapState } from "../bootstrap/types";
import type { ProjectId } from "../project/domain";
import type { CreateSessionInput, OcgRuntimeClient, RuntimeSnapshot } from "./runtime-types";
import {
  RuntimeEnvelopeFactory,
  eventSessionId,
  RUNTIME_PROTOCOL_VERSION,
} from "./runtime-envelope";
import { emptyRuntimeSnapshot } from "./runtime-snapshot";
import { RuntimeStore } from "./runtime-store";
import {
  createUninitializedRuntimeState,
  type RuntimeState,
  type RuntimeSyncState,
} from "./reconciler";

function clone<T>(value: T): T {
  return structuredClone(value);
}

export abstract class RuntimeClientBase implements OcgRuntimeClient {
  readonly authority = "canonical" as const;
  private readonly listeners = new Set<(event: OcgRuntimeEvent) => void>();
  protected readonly store: RuntimeStore;
  private readonly envelopes: RuntimeEnvelopeFactory;

  constructor() {
    const seed = {
      protocolVersion: RUNTIME_PROTOCOL_VERSION,
      streamId: "canonical",
      generation: 1,
      cursor: { streamId: "canonical", sequence: 0 },
      scope: { kind: "all-projects" as const },
      snapshot: { ...emptyRuntimeSnapshot(), authority: "canonical" as const },
    };
    this.store = new RuntimeStore(createUninitializedRuntimeState());
    this.store.installSnapshot(seed);
    this.envelopes = new RuntimeEnvelopeFactory(seed.streamId, seed.generation, {
      startSequence: 1,
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
    return clone(this.getSnapshot().status);
  }
  async listSessions(): Promise<ChatSession[]> {
    return clone(this.getSnapshot().sessions);
  }
  async getSession(id: string): Promise<ChatSession | null> {
    const session = this.getSnapshot().sessions.find((item) => item.id === id);
    return session ? clone(session) : null;
  }
  async getMessages(sessionId: string): Promise<ChatMessage[]> {
    return clone(this.getSnapshot().messagesBySession[sessionId] ?? []);
  }
  async getBootstrap(): Promise<BootstrapState> {
    return clone(this.getSnapshot().bootstrap);
  }

  abstract createSession(input: CreateSessionInput): Promise<ChatSession>;
  abstract deleteSession(sessionId: string): Promise<void>;
  abstract sendMessage(sessionId: string, input: SendMessageInput): Promise<void>;
  abstract retryMessage(sessionId: string, messageId: string): Promise<void>;
  abstract cancel(sessionId: string): Promise<void>;
  abstract removeQueuedMessage(sessionId: string, id: string): Promise<void>;
  abstract editQueuedMessage(sessionId: string, id: string, content: string): Promise<void>;

  subscribe(listener: (event: OcgRuntimeEvent) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }
  dispose(): void {
    this.listeners.clear();
  }

  protected emit(
    event: OcgRuntimeEvent,
    scope: { projectId?: ProjectId | null; commandId?: string } = {},
  ): void {
    const sessionId = eventSessionId(event);
    const projectId = scope.projectId ?? null;
    const envelope = this.envelopes.fromRuntimeEvent(event, {
      projectId,
      commandId: scope.commandId,
      sessionId,
    });
    this.store.applyEnvelope(envelope);
    for (const listener of this.listeners) listener(event);
  }
}
