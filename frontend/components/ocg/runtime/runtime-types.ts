import type { ChatMessage, ChatSession, OcgRuntimeEvent, RuntimeStatus } from "../types";
import type { JobExecution } from "../execution/domain";

export type RuntimeAuthority = "canonical";

export type RuntimeSnapshot = {
  status: RuntimeStatus;
  sessions: ChatSession[];
  messagesBySession: Record<string, ChatMessage[]>;
  chatQueues?: Record<string, { queue: import("../types").QueuedChatMessage[]; paused: boolean }>;
  executionBySession: Record<string, JobExecution | null>;
  conversationList?: "loading" | "loaded" | "failed";
};

export type CreateSessionInput = {
  title?: string;
  projectId?: string;
  workType: ChatSession["workType"];
};

export interface OcgRuntimeClient {
  isListedConversation?(sessionId: string): boolean;
  openConversation?(job: string): Promise<string>;
  presentMutableJob?(sessionId: string, job: import("./control-client").MutableJob): Promise<void>;
  hasEarlierHistory?(sessionId: string): boolean;
  loadEarlierHistory?(sessionId: string): Promise<void>;
  setChatSelection?(sessionId: string, selection: import("../contracts").ChatModelSelection): void;
  retryConversations?(): void;
  createSession(input: CreateSessionInput): Promise<ChatSession>;
  deleteSession?(sessionId: string): Promise<void>;
  renameSession?(sessionId: string, title: string): Promise<void>;
  hideSession?(sessionId: string): Promise<void>;
  uploadChatImage?(
    request: import("../contracts").ChatImageUploadRequest,
    signal?: AbortSignal,
  ): Promise<import("../contracts").ChatImage>;
  retryMessage(sessionId: string, messageId: string): Promise<void>;
  subscribe(listener: (event: OcgRuntimeEvent) => void): () => void;
  getSnapshot(): RuntimeSnapshot;
  cancel?(sessionId: string): Promise<void>;
  removeQueuedMessage?(sessionId: string, id: string): void | Promise<void>;
  editQueuedMessage?(sessionId: string, id: string, content: string): Promise<void>;
  dispose?(): void;
}
