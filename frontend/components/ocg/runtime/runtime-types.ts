import type { ChatMessage, ChatSession, OcgRuntimeEvent, RuntimeStatus } from "../types";

export type RuntimeSnapshot = {
  status: RuntimeStatus;
  sessions: ChatSession[];
  messagesBySession: Record<string, ChatMessage[]>;
  chatQueues?: Record<string, { queue: import("../types").QueuedChatMessage[]; paused: boolean }>;
  conversationList?: "loading" | "loaded" | "failed";
};

export type CreateSessionInput = {
  title?: string;
  projectId?: string;
  workType: ChatSession["workType"];
};

export type HideSessionOutcome = "stop-required" | "stop-unconfirmed" | void;

export interface OcgRuntimeClient {
  isListedConversation?(sessionId: string): boolean;
  openConversation?(job: string): Promise<string>;
  presentMutableJob?(sessionId: string, job: import("./api-client").MutableJob): Promise<void>;
  hasEarlierHistory?(sessionId: string): boolean;
  loadEarlierHistory?(sessionId: string): Promise<void>;
  setChatSelection?(sessionId: string, selection: import("../contracts").ChatModelSelection): void;
  retryConversations?(): void;
  createSession(input: CreateSessionInput): Promise<ChatSession>;
  deleteSession?(sessionId: string): Promise<void>;
  renameSession?(sessionId: string, title: string): Promise<void>;
  hideSession?(sessionId: string, stopFirst: boolean): Promise<HideSessionOutcome>;
  presentArchivedConversation?(conversation: import("../contracts/api").Conversation): void;
  restoreConversation?(conversation: import("../contracts/api").Conversation): Promise<void>;
  uploadChatImage?(
    request: import("../contracts").ChatImageUploadRequest,
    signal?: AbortSignal,
  ): Promise<import("../contracts").ChatImage>;
  retryMessage(sessionId: string, messageId: string): Promise<void>;
  subscribe(listener: (event: OcgRuntimeEvent) => void): () => void;
  getSnapshot(): RuntimeSnapshot;
  cancel?(sessionId: string): Promise<void>;
  dispose?(): void;
}
