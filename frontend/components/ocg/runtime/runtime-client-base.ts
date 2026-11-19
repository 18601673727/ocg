import type { ChatSession, OcgRuntimeEvent } from "../types";
import { emptyRuntimeSnapshot, RuntimeStore } from "./runtime-store";
import type { CreateSessionInput, OcgRuntimeClient, RuntimeSnapshot } from "./runtime-types";

export abstract class RuntimeClientBase implements OcgRuntimeClient {
  private readonly listeners = new Set<(event: OcgRuntimeEvent) => void>();
  private readonly store: RuntimeStore;

  constructor() {
    this.store = new RuntimeStore(emptyRuntimeSnapshot());
  }

  getSnapshot(): RuntimeSnapshot {
    return this.store.getSnapshot();
  }
  abstract createSession(input: CreateSessionInput): Promise<ChatSession>;
  abstract deleteSession(sessionId: string): Promise<void>;
  abstract retryMessage(sessionId: string, messageId: string): Promise<void>;
  abstract cancel(sessionId: string): Promise<void>;

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
