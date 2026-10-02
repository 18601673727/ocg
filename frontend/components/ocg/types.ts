export type WorkType = "research" | "coding" | "design" | "devops";

export type ChatSession = {
  id: string;
  title: string;
  workType: WorkType;
  updatedAt: string;
};

/**
 * Who authored a message.
 *
 * `call` mirrors the canonical `Actor` kind of the same name: a Call is the
 * managed action the substrate ran, and its output can be a message of its own.
 */
export type MessageRole = "user" | "assistant" | "call" | "tool";

export type MessageStatus =
  | "pending"
  | "streaming"
  | "completed"
  | "cancelled"
  | "failed";

/**
 * The lifecycle words a Call admits, drawn from the contract's `CallLifecycle`.
 *
 * `succeeded` is the contract word for a settled Call; `completed` is the
 * display alias this UI uses, since the control surface renders the word
 * `completed` in the substrate column even for a successful outcome.
 */
export type CallActivityStatus =
  | "created"
  | "queued"
  | "running"
  | "completed"
  | "failed"
  | "cancelled";

/**
 * Maps the contract's canonical state onto the UI-visible vocabulary.
 *
 * `completed` is mapped from `succeeded` to match the substrate's column word;
 * the rest are preserved verbatim so the UI is always readable in the contract's
 * own dialect.
 */
export function callActivityStatusFrom(state: string): CallActivityStatus {
  switch (state) {
    case "created":
    case "queued":
    case "running":
    case "succeeded":
    case "completed":
    case "failed":
    case "cancelled":
      return state === "succeeded" ? "completed" : state;
    default:
      return "completed";
  }
}

/** One managed Call shown inline in the conversation. */
export type CallActivity = {
  id: string;
  /** The canonical effect identity, when the substrate named one. */
  name: string;
  status: string;
  durationMs: number;
  summary: string;
  detail: string;
  /** The Executor the substrate attributed this Call to. */
  executorId?: string;
};

export type ChatMessage = {
  id: string;
  role: MessageRole;
  content: string;
  createdAt: string;
  status: MessageStatus;
  call?: CallActivity;
  /** Temporary read-only compatibility field for the legacy fixture renderer. */
  tool?: ToolActivity;
};

/** Status an observability participant (a provider invocation) can report. */
export type WorkerStatus = "queued" | "starting" | "active" | "waiting" | "idle" | "completed" | "failed" | "cancelled";
export type ToolActivityStatus = "pending" | "running" | "success" | "failure" | "retrying" | "waiting-approval";
export type ToolActivity = { id: string; name: string; status: string; durationMs: number; summary: string; detail: string; retryCount?: number };

export type RuntimeConnectionState = "connected" | "connecting" | "disconnected" | "failed";

/** The same states, as data, so a validator can check membership. */
export const RUNTIME_CONNECTION_STATES: readonly RuntimeConnectionState[] = [
  "connected",
  "connecting",
  "disconnected",
  "failed",
];

export type RuntimeStatus = {
  state: RuntimeConnectionState;
  detail?: string;
};

export type SendMessageInput = {
  content: string;
  /** Explicit owning Project from real UI state (the active project at send time). */
  projectId?: string;
};

export type OcgRuntimeEvent =
  | { type: "runtime.status-changed"; status: RuntimeStatus }
  | { type: "conversation.session-created"; session: ChatSession }
  | { type: "conversation.session-updated"; session: ChatSession }
  | { type: "conversation.message-started"; sessionId: string; message: ChatMessage }
  | { type: "conversation.message-delta"; sessionId: string; messageId: string; delta: string }
  | { type: "conversation.message-completed"; sessionId: string; message: ChatMessage }
  | { type: "activity.updated"; sessionId: string; messageId: string; activity: CallActivity }
  | { type: "job.execution-updated"; sessionId: string; execution: import("./execution/domain").JobExecution; accounting: import("./execution/accounting").JobAccounting | null }
  | { type: "job.launch-updated"; sessionId: string; result: import("./runtime/runtime-types").JobLaunchResult }
  | { type: "observability.updated"; sessionId: string; observability: import("./runtime/observability").RuntimeObservability }
  | { type: "attention.updated"; item: import("./attention/domain").AttentionItem }
  | { type: "ledger.entry-added"; entry: import("./resource-ledger/types").ResourceLedgerEntry }
  | { type: "ledger.entry-updated"; entry: import("./resource-ledger/types").ResourceLedgerEntry }
  | { type: "log.appended"; entry: import("./logs/domain").LogEntry }
  | { type: "bootstrap.updated"; bootstrap: import("./bootstrap/types").BootstrapState }
  | { type: "warning"; message: string }
  | { type: "error"; message: string }
  | { type: "cancelled"; sessionId: string; messageId?: string };

export const WORK_TYPE_LABEL: Record<WorkType, string> = {
  research: "Research",
  coding: "Coding",
  design: "Design",
  devops: "DevOps",
};
