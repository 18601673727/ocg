import type {
  ChatMessage,
  ChatSession,
  OcgRuntimeEvent,
  RuntimeStatus,
  SendMessageInput,
} from "../types";
import type { CreateSessionInput, OcgRuntimeClient, RuntimeSnapshot } from "./runtime-types";
import { emptyRuntimeSnapshot, RuntimeStore } from "./runtime-store";

function clone<T>(value: T): T {
  return structuredClone(value);
}

export abstract class RuntimeClientBase implements OcgRuntimeClient {
  readonly authority = "canonical" as const;
  private readonly listeners = new Set<(event: OcgRuntimeEvent) => void>();
  private readonly store: RuntimeStore;

  constructor() {
    this.store = new RuntimeStore({ ...emptyRuntimeSnapshot(), authority: this.authority });
  }

  getSnapshot(): RuntimeSnapshot {
    return this.store.getSnapshot();
  }
  getDiagnostics() {
    return this.store.getDiagnostics();
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

  protected emit(event: OcgRuntimeEvent): void {
    this.store.apply(event);
    for (const listener of this.listeners) listener(event);
  }
}
