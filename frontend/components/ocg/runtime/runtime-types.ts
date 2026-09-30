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
import type { Mission } from "../types";
import type { RuntimeObservability } from "./observability";

export type JobLaunchOutcome = "accepted" | "rejected" | "requires-attention" | "failed";

export type JobLaunchResult = {
  outcome: JobLaunchOutcome;
  commandId: string;
  draftId: string;
  projectId: ProjectId;
  sessionId: string;
  jobId?: string;
  missionId?: string;
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

/** Compatibility names for surfaces that are still being moved to Job terminology. */
export type MissionLaunchResult = JobLaunchResult;
export type MissionLaunchCommand = JobLaunchCommand;

export type ScenarioId =
  | "normal-chat"
  | "long-stream"
  | "call-heavy"
  | "executor-parallel"
  | "build-failed"
  | "retry-success"
  | "job-completed"
  | "runtime-disconnected"
  | "runtime-connecting"
  | "runtime-failed"
  | "permission-required"
  | "execution-live"
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
  | "attention-calm"
  | "tool-heavy"
  | "worker-parallel"
  | "mission-complete"
  | "budget-exhausted"
  | "observability-live"
  | "mission-control";

/** Single execution read model: canonical Job/Attempt/Call + accounting ceiling. */
export type RuntimeSnapshot = {
  scenario: ScenarioId;
  status: RuntimeStatus;
  sessions: ChatSession[];
  messagesBySession: Record<string, ChatMessage[]>;
  missionsBySession: Record<string, Mission | null>;
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
  workType: ChatSession["workType"];
};

export interface OcgRuntimeClient {
  getRuntimeStatus(): Promise<RuntimeStatus>;
  listSessions(): Promise<ChatSession[]>;
  getSession(id: string): Promise<ChatSession | null>;
  getMessages(sessionId: string): Promise<ChatMessage[]>;
  getExecution?(sessionId: string): Promise<JobExecution | null>;
  getAccounting?(sessionId: string): Promise<JobAccounting | null>;
  getBootstrap(): Promise<BootstrapState>;
  createSession(input: CreateSessionInput): Promise<ChatSession>;
  sendMessage(sessionId: string, input: SendMessageInput): Promise<void>;
  subscribe(listener: (event: OcgRuntimeEvent) => void): () => void;
  getSnapshot(): RuntimeSnapshot;
  getSyncState?(): RuntimeSyncState;
  cancel?(sessionId: string): Promise<void>;
  /** Frontend-only Job launch boundary. Validates against the mock snapshot and projects canonical execution into the per-session maps. */
  launchJob?(command: JobLaunchCommand): Promise<JobLaunchResult>;
  requestAccessHandoff?(): Promise<void>;
  setOnboardingStage?(stage: OnboardingStageId): Promise<void>;
  completeOnboarding?(): Promise<void>;
  retryBootstrap?(): Promise<void>;
  setActiveProfile?(profileId: string): Promise<void>;
}
