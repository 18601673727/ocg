import type {
  CanonicalAttempt,
  CanonicalEffectKind,
  CanonicalJobState,
  Failure,
  JobOrigin,
  RecursiveLimits,
} from "../contracts";
import type { ProjectId } from "../project/domain";

export type CallStatus = "queued" | "running" | "completed" | "failed" | "unrecognized";

export type ExecutionCall = {
  id: string;
  callId: string;
  attemptId: string;
  attemptGeneration: number;
  executorId: string | null;
  generation: number;
  status: CallStatus;
  rawState: string;
  effectKind: CanonicalEffectKind;
  sideEffect: boolean;
  request: string;
  response: string | null;
  createdAt: number;
  finishedAt: number | null;
  elapsedMs: number | undefined;
  reason: string | undefined;
};

export type ExecutionAttempt = {
  id: string;
  attemptId: string;
  generation: number;
  state: CanonicalAttempt["state"];
  authoritative: boolean;
  createdAt: number;
  finishedAt: number | null;
  callCount: number;
  settledCallCount: number;
};

export type ExecutorStatus = "running" | "queued" | "settled" | "unrecognized";

export type ExecutionExecutor = {
  id: string;
  executorId: string;
  attemptId: string;
  kind: string;
  status: ExecutorStatus;
  rawState: string;
  callIds: string[];
  runningCallId: string | undefined;
  settledCallCount: number;
};

export type DispatchIntentStatus =
  | "pending"
  | "running"
  | "settled"
  | "failed"
  | "fenced"
  | "unrecognized";

export type ExecutionDispatchIntent = {
  id: string;
  dispatchIntentId: string;
  callId: string;
  attemptId: string;
  executorId: string | null;
  generation: number;
  status: DispatchIntentStatus;
  rawState: string;
  effectKind: CanonicalEffectKind;
  effectState: string;
  request: string;
  budgetAdmitted: boolean;
  providerKey: string | null;
  model: string | null;
  upstreamModelId: string | null;
  reservationId: string | null;
  failure: string | null;
  createdAt: number;
  updatedAt: number;
};

export type CallActivityItem = {
  id: string;
  jobId: string;
  callId: string;
  executorId: string | undefined;
  attemptGeneration: number;
  status: CallStatus;
  at: number;
  timestamp: string;
  message: string;
  elapsedMs?: number;
};

export type JobProgress = {
  settled: number;
  total: number;
  percent: number;
};

export type JobCallSummary = {
  total: number;
  running: number;
  queued: number;
  completed: number;
  failed: number;
  unrecognized: number;
};

export type RequestedExecutionTarget = {
  provider: string | null;
  model: string | null;
};

export type JobRecoveryEvent = {
  id: string;
  attemptId: string | null;
  generation: number | null;
  callId: string | null;
  executorId: string | null;
  classification: string;
  action: string;
  outcome: string;
  replacementAttemptId: string | null;
  createdAt: number;
  supersededAttempt: boolean;
};

export type JobExecution = {
  apiVersion: string;
  projectId: ProjectId;
  jobId: string;
  parentJobId: string | null;
  origin: JobOrigin | null;
  childJobIds: string[];
  dependsOn: string[];
  blocks: string[];
  blockedBy: string[];
  blocked: boolean;
  rootJobId: string;
  depth: number;
  descendantJobIds: string[];
  descendantSummary: Record<string, number>;
  waitingForChildren: boolean;
  recursiveLimits: RecursiveLimits | undefined;
  requestedTarget: RequestedExecutionTarget | null;
  recovery: JobRecoveryEvent[];
  canCancel: boolean;
  canRetry: boolean;
  terminationReason: Failure | null;
  cursor: number;
  state: CanonicalJobState;
  generation: number;
  authoritativeAttemptId: string | null;
  createdAt: number;
  updatedAt: number;
  attempts: ExecutionAttempt[];
  calls: ExecutionCall[];
  authoritativeCalls: ExecutionCall[];
  executors: ExecutionExecutor[];
  dispatchIntents: ExecutionDispatchIntent[];
  summary: JobCallSummary;
  progress: JobProgress | null;
  currentCall: ExecutionCall | null;
  latestCall: ExecutionCall | null;
  activities: CallActivityItem[];
};
