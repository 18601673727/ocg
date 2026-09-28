import type {
  ChatMessage,
  ChatSession,
  Mission,
  OcgRuntimeEvent,
  RuntimeStatus,
  SendMessageInput,
} from "../types";
import type { RuntimeObservability } from "./observability";
import type { ResourceLedger } from "../resource-ledger/types";
import type { BootstrapState, OnboardingStageId } from "../bootstrap/types";
import type { MissionExecution } from "../execution/domain";
import type { ProjectId } from "../project/domain";
import type { MissionLaunchCommand } from "../mission/draft-domain";
import type { RuntimeSyncState } from "./reconciler";
import type { AttentionItem } from "../attention/domain";
import type { LogEntry } from "../logs/domain";

/**
 * Launch command produced by the pure Mission draft domain and consumed at the
 * runtime boundary. It is intentionally transport-free: no HTTP, SSE, or
 * backend DTO is implied.
 */
export type { MissionLaunchCommand } from "../mission/draft-domain";

export type MissionLaunchOutcome = "accepted" | "rejected" | "requires-attention" | "failed";

/**
 * Normalized launch result. `requires-attention` is reserved for a future
 * fixture that needs explicit operator input; the current mock runtime emits
 * only accepted, rejected, or failed.
 */
export type MissionLaunchResult = {
  outcome: MissionLaunchOutcome;
  commandId: string;
  draftId: string;
  projectId: ProjectId;
  sessionId: string;
  missionId?: string;
  message: string;
  /** True when the runtime returned a previously recorded result for this command identity. */
  duplicate: boolean;
};


export type ScenarioId =
  | "normal-chat"
  | "long-stream"
  | "tool-heavy"
  | "worker-parallel"
  | "build-failed"
  | "retry-success"
  | "mission-complete"
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
  | "mission-control"
  | "logs-live"
  | "home-overview"
  | "home-calm"
  | "attention-overview"
  | "attention-calm";

export type RuntimeSnapshot = {
  scenario: ScenarioId;
  status: RuntimeStatus;
  sessions: ChatSession[];
  messagesBySession: Record<string, ChatMessage[]>;
  missionsBySession: Record<string, Mission | null>;
  observabilityBySession: Record<string, RuntimeObservability | null>;
  executionBySession: Record<string, MissionExecution | null>;
  resourceLedger: ResourceLedger | null;
  /** Explicit runtime Attention items. Derived Attention remains selector-owned. */
  attentionItems?: AttentionItem[];
  /** Append-oriented runtime logs. Surfaces may combine these with projections. */
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
  getMission(sessionId: string): Promise<Mission | null>;
  getObservability(sessionId: string): Promise<RuntimeObservability | null>;
  getBootstrap(): Promise<BootstrapState>;
  createSession(input: CreateSessionInput): Promise<ChatSession>;
  sendMessage(sessionId: string, input: SendMessageInput): Promise<void>;
  subscribe(listener: (event: OcgRuntimeEvent) => void): () => void;
  getSnapshot(): RuntimeSnapshot;
  /** Canonical synchronization metadata for the external store, when available. */
  getSyncState?(): RuntimeSyncState;
  cancel?(sessionId: string): Promise<void>;
  /**
   * Frontend-only Mission launch boundary. Validates the command against the
   * mock snapshot and, on acceptance, projects a Mission + execution into the
   * existing per-session snapshot maps. No backend API is called.
   */
  launchMission?(command: MissionLaunchCommand): Promise<MissionLaunchResult>;
  /** Mock Cloudflare Access handoff. Never a real redirect or credential exchange. */
  requestAccessHandoff?(): Promise<void>;
  setOnboardingStage?(stage: OnboardingStageId): Promise<void>;
  completeOnboarding?(): Promise<void>;
  /** Mock recovery for an actionable bootstrap failure. */
  retryBootstrap?(): Promise<void>;
  /**
   * Frontend-only active profile selection. Updates the normalized bootstrap
   * snapshot and emits `bootstrap.updated`; it never persists or writes config.
   */
  setActiveProfile?(profileId: string): Promise<void>;
}
