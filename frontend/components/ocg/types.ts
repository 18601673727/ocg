export type WorkType = "research" | "coding" | "design" | "devops";

export type ChatSession = {
  id: string;
  title: string;
  workType: WorkType;
  updatedAt: string;
};

export type MessageRole = "user" | "assistant" | "tool";

export type MessageStatus =
  | "pending"
  | "streaming"
  | "completed"
  | "cancelled"
  | "failed";

export type ToolActivityStatus =
  | "pending"
  | "running"
  | "success"
  | "failure"
  | "retrying"
  | "waiting-approval";

export type ToolActivity = {
  id: string;
  name: string;
  status: ToolActivityStatus;
  durationMs: number;
  summary: string;
  detail: string;
  retryCount?: number;
};

export type ChatMessage = {
  id: string;
  role: MessageRole;
  content: string;
  createdAt: string;
  status: MessageStatus;
  tool?: ToolActivity;
};

export type MissionTaskStatus = "pending" | "active" | "completed" | "failed";

export type MissionTask = {
  id: string;
  title: string;
  status: MissionTaskStatus;
};

export type MissionStatus =
  | "planning"
  | "running"
  | "paused"
  | "completed"
  | "failed"
  | "budget-exhausted";

export type WorkerStatus =
  | "queued"
  | "starting"
  | "active"
  | "waiting"
  | "idle"
  | "completed"
  | "failed"
  | "cancelled";

export type Worker = {
  id: string;
  name: string;
  status: WorkerStatus;
  task?: string;
};

export type ResourceCommitment = {
  workers: number;
  mode: "capped" | "flexible";
};

export type BudgetState = {
  spent: number;
  limit: number;
  currency: "USD";
  status: "within-limit" | "exhausted";
};

export type Mission = {
  title: string;
  goal: string;
  status: MissionStatus;
  completed: number;
  total: number;
  current: string;
  tasks: MissionTask[];
  workers: Worker[];
  elapsed: string;
  commitment: ResourceCommitment;
  budget: BudgetState;
  warnings: string[];
};

export type RuntimeConnectionState = "connected" | "connecting" | "disconnected" | "failed";

export type RuntimeStatus = {
  state: RuntimeConnectionState;
  detail?: string;
};

export type SendMessageInput = {
  content: string;
};

export type OcgRuntimeEvent =
  | { type: "runtime.status-changed"; status: RuntimeStatus }
  | { type: "conversation.session-created"; session: ChatSession }
  | { type: "conversation.session-updated"; session: ChatSession }
  | { type: "conversation.message-started"; sessionId: string; message: ChatMessage }
  | { type: "conversation.message-delta"; sessionId: string; messageId: string; delta: string }
  | { type: "conversation.message-completed"; sessionId: string; message: ChatMessage }
  | { type: "activity.updated"; sessionId: string; messageId: string; activity: ToolActivity }
  | { type: "mission.updated"; sessionId: string; mission: Mission }
  | { type: "observability.updated"; sessionId: string; observability: import("./runtime/observability").RuntimeObservability }
  | { type: "execution.updated"; sessionId: string; execution: import("./execution/domain").MissionExecution }
  | { type: "mission.launch-updated"; sessionId: string; result: import("./runtime/runtime-types").MissionLaunchResult }
  | { type: "attention.updated"; item: import("./attention/domain").AttentionItem }
  | { type: "ledger.entry-added"; entry: import("./resource-ledger/types").ResourceLedgerEntry }
  | { type: "ledger.entry-updated"; entry: import("./resource-ledger/types").ResourceLedgerEntry }
  | { type: "log.appended"; entry: import("./logs/domain").LogEntry }
  | { type: "bootstrap.updated"; bootstrap: import("./bootstrap/types").BootstrapState }
  | { type: "worker.updated"; sessionId: string; worker: Worker }
  | { type: "warning"; message: string }
  | { type: "error"; message: string }
  | { type: "cancelled"; sessionId: string; messageId?: string };

export const WORK_TYPE_LABEL: Record<WorkType, string> = {
  research: "Research",
  coding: "Coding",
  design: "Design",
  devops: "DevOps",
};
