import type {
  ChatMessage,
  ChatSession,
  OcgRuntimeEvent,
  RuntimeStatus,
  SendMessageInput,
} from "../types";
import type { ResourceLedger } from "../resource-ledger/types";
import type { BootstrapState, OnboardingStageId } from "../bootstrap/types";
import type { JobExecution } from "../execution/domain";
import type { JobAccounting } from "../execution/accounting";
import type { ProjectId } from "../project/domain";
import type { RuntimeSyncState } from "./reconciler";
import type { AttentionItem } from "../attention/domain";
import type { LogEntry } from "../logs/domain";
import type { RuntimeObservability } from "./observability";

export type JobLaunchOutcome = "accepted" | "rejected" | "requires-attention" | "failed";

export type JobLaunchResult = {
  outcome: JobLaunchOutcome;
  commandId: string;
  draftId: string;
  projectId: ProjectId;
  sessionId: string;
  jobId?: string;
  message: string;
  duplicate: boolean;
};

export type JobLaunchCommand = {
  commandId: string;
  draftId: string;
  projectId: ProjectId;
  sessionId: string;
  objective: string;
  successCriteria?: string;
  constraints?: string;
  hardBudgetMicros: number;
  resourceCommitment?: number;
};

export type RuntimeAuthority = "canonical";

/** Runtime projection: Project-owned canonical Jobs plus session-scoped Chat presentation. */
export type RuntimeSnapshot = {
  authority?: RuntimeAuthority;
  status: RuntimeStatus;
  sessions: ChatSession[];
  messagesBySession: Record<string, ChatMessage[]>;
  chatQueues?: Record<string, { queue: import("../types").QueuedChatMessage[]; paused: boolean }>;
  observabilityBySession: Record<string, RuntimeObservability | null>;
  /** Canonical Project-owned Jobs. Conversation/session maps are presentation-only. */
  executionsByProject?: Record<ProjectId, Record<string, JobExecution>>;
  /** Latest Job presented by each Chat session; never used to discover Project Jobs. */
  executionBySession: Record<string, JobExecution | null>;
  accountingBySession: Record<string, JobAccounting | null>;
  resourceLedger: ResourceLedger | null;
  attentionItems?: AttentionItem[];
  logs?: LogEntry[];
  bootstrap: BootstrapState;
};

export type CreateSessionInput = {
  title?: string;
  projectId?: string;
  workType: ChatSession["workType"];
};

export interface OcgRuntimeClient {
  /** The runtime that answers Chat for this client. Never inferred from a scenario name. */
  readonly authority: RuntimeAuthority;
  isListedConversation?(sessionId: string): boolean;
  openConversation?(job: string): Promise<string>;
  hasEarlierHistory?(sessionId: string): boolean;
  loadEarlierHistory?(sessionId: string): Promise<void>;
  setChatSelection?(sessionId: string, selection: import("../contracts").ChatModelSelection): void;
  getRuntimeStatus(): Promise<RuntimeStatus>;
  listSessions(): Promise<ChatSession[]>;
  getSession(id: string): Promise<ChatSession | null>;
  getMessages(sessionId: string): Promise<ChatMessage[]>;
  getExecution?(sessionId: string): Promise<JobExecution | null>;
  getAccounting?(sessionId: string): Promise<JobAccounting | null>;
  getBootstrap(): Promise<BootstrapState>;
  createSession(input: CreateSessionInput): Promise<ChatSession>;
  deleteSession?(sessionId: string): Promise<void>;
  renameSession?(sessionId: string, title: string): Promise<void>;
  hideSession?(sessionId: string): Promise<void>;
  uploadChatImage?(
    request: import("../contracts").ChatImageUploadRequest,
    signal?: AbortSignal,
  ): Promise<import("../contracts").ChatImage>;
  sendMessage(sessionId: string, input: SendMessageInput): Promise<void>;
  retryMessage(sessionId: string, messageId: string): Promise<void>;
  subscribe(listener: (event: OcgRuntimeEvent) => void): () => void;
  getSnapshot(): RuntimeSnapshot;
  getSyncState?(): RuntimeSyncState;
  cancel?(sessionId: string): Promise<void>;
  cancelJob?(jobId: string, expectedGeneration: number): Promise<void>;
  retryJob?(jobId: string, expectedGeneration: number): Promise<void>;
  removeQueuedMessage?(sessionId: string, id: string): void | Promise<void>;
  editQueuedMessage?(sessionId: string, id: string, content: string): Promise<void>;
  resumeQueue?(sessionId: string): void;
  /** Bind a session to its owning Project so a later send launches into that Project. */
  bindSessionProject?(sessionId: string, projectId: string): void;
  hydrateProject?(projectId: string): Promise<void>;
  /** Release runtime-owned subscription/transport resources when its provider unmounts. */
  dispose?(): void;
  /** Canonical Job launch boundary, projecting backend execution into session maps. */
  launchJob?(command: JobLaunchCommand): Promise<JobLaunchResult>;
  requestAccessHandoff?(): Promise<void>;
  setOnboardingStage?(stage: OnboardingStageId): Promise<void>;
  completeOnboarding?(): Promise<void>;
  retryBootstrap?(): Promise<void>;
  setActiveProfile?(profileId: string): Promise<void>;
}
