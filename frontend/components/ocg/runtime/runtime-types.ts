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

/**
 * Which runtime owns Chat execution.
 *
 * The authority follows the loopback control endpoint, never a fixture name: a
 * session attached to an OCG control endpoint is answered by the canonical
 * backend, and the fixture runtime is what is left when the invocation has no
 * backend to be canonical for. `?scenario=` selects the shell projection a
 * fixture seeds; it never demotes a real session to the mock. A missing backend
 * must never be reported as if it were the mock either.
 */
export type RuntimeAuthority = "canonical" | "mock";

export type ScenarioId =
  | "normal-chat"
  | "long-stream"
  | "tool-heavy"
  | "worker-parallel"
  | "build-failed"
  | "retry-success"
  | "job-completed"
  | "budget-exhausted"
  | "runtime-disconnected"
  | "runtime-connecting"
  | "runtime-failed"
  | "permission-required"
  | "observability-live"
  | "resource-ledger"
  | "local-ready"
  | "local-first-run"
  | "remote-unauthenticated"
  | "remote-session-expired"
  | "remote-denied"
  | "remote-authenticated-ready"
  | "remote-authenticated-first-run"
  | "onboarding-resume"
  | "onboarding-migration"
  | "onboarding-recovery"
  | "onboarding-invalid-configuration"
  | "onboarding-auth-required"
  | "onboarding-connection-failure"
  | "onboarding-discovery"
  | "onboarding-ready"
  | "profiles-models"
  | "job-execution"
  | "logs-live"
  | "home-overview"
  | "home-calm"
  | "attention-overview"
  | "attention-calm";

/** Single execution read model: canonical Job/Attempt/Call + accounting ceiling. */
export type RuntimeSnapshot = {
  authority?: RuntimeAuthority;
  scenario: ScenarioId;
  status: RuntimeStatus;
  sessions: ChatSession[];
  messagesBySession: Record<string, ChatMessage[]>;
  chatQueues?: Record<string, { queue: import("../types").QueuedChatMessage[]; paused: boolean }>;
  observabilityBySession: Record<string, RuntimeObservability | null>;
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
  getRuntimeStatus(): Promise<RuntimeStatus>;
  listSessions(): Promise<ChatSession[]>;
  getSession(id: string): Promise<ChatSession | null>;
  getMessages(sessionId: string): Promise<ChatMessage[]>;
  getExecution?(sessionId: string): Promise<JobExecution | null>;
  getAccounting?(sessionId: string): Promise<JobAccounting | null>;
  getBootstrap(): Promise<BootstrapState>;
  createSession(input: CreateSessionInput): Promise<ChatSession>;
  sendMessage(sessionId: string, input: SendMessageInput): Promise<void>;
  retryMessage(sessionId: string, messageId: string): Promise<void>;
  subscribe(listener: (event: OcgRuntimeEvent) => void): () => void;
  getSnapshot(): RuntimeSnapshot;
  getSyncState?(): RuntimeSyncState;
  cancel?(sessionId: string): Promise<void>;
  removeQueuedMessage?(sessionId: string, id: string): void;
  resumeQueue?(sessionId: string): void;
  /** Bind a session to its owning Project so a later send launches into that Project. */
  bindSessionProject?(sessionId: string, projectId: string): void;
  hydrateProject?(projectId: string): Promise<void>;
  /** Canonical Job launch boundary, projecting backend execution into session maps. */
  launchJob?(command: JobLaunchCommand): Promise<JobLaunchResult>;
  requestAccessHandoff?(): Promise<void>;
  setOnboardingStage?(stage: OnboardingStageId): Promise<void>;
  completeOnboarding?(): Promise<void>;
  retryBootstrap?(): Promise<void>;
  setActiveProfile?(profileId: string): Promise<void>;
}
