export type WorkType = "research" | "coding" | "design" | "devops";

export type ChatSession = {
  id: string;
  title: string;
  workType: WorkType;
  updatedAt: string;
  projectId?: string;
  sessionId?: string;
  agentProfile?: string;
  conversationId?: string;
  jobState?: string;
  hasChildren?: boolean;
};

export type MessageRole = "user" | "assistant" | "call" | "tool";

export type MessageStatus =
  | "pending"
  | "streaming"
  | "completed"
  | "cancelled"
  | "unknown"
  | "failed";

export type ChatMessage = {
  agentInstruction?: boolean;
  compaction?: import("./contracts").ControlConversationTurn["compaction"];
  withdrawn?: boolean;
  queued?: boolean;
  queueHistory?: { content: string; at: string }[];
  activity?: import("./contracts").ControlJobActivity;
  taskPlan?: import("./contracts").ControlTaskPlan | null;
  toolCalls?: import("./contracts").ControlToolCall[];
  images?: import("./contracts").ChatImage[];
  id: string;
  commandId?: string;
  jobId?: string;
  optimistic?: boolean;
  role: MessageRole;
  content: string;
  reasoning?: string;
  sourceSnapshots?: import("./markdown/source-snapshot").FileSnapshot[];
  reasoningDurationMs?: number;
  intermediate?: boolean;
  failureReason?: string;
  failureAt?: number;
  failureCode?: "project-missing" | "configuration-required" | "stream-closed";
  createdAt: string;
  status: MessageStatus;
};

export type WorkerStatus =
  | "queued"
  | "starting"
  | "active"
  | "waiting"
  | "idle"
  | "completed"
  | "failed"
  | "cancelled";

export type RuntimeConnectionState = "connected" | "connecting" | "disconnected" | "failed";

export type RuntimeStatus = {
  state: RuntimeConnectionState;
  sessionId?: string;
  detailCode?: "provider-ready" | "configuration-required";
  detail?: string;
};

export type SendMessageInput = {
  images?: import("./contracts").ChatImage[];
  selection?: import("./contracts").ChatModelSelection;
  mode?: "queue" | "steer";
  content: string;
  projectId?: string;
};

export type QueuedChatMessage = { id: string; input: SendMessageInput };

export type OcgRuntimeEvent =
  | { type: "conversation.session-deleted"; sessionId: string }
  | {
      type: "conversation.message-image";
      sessionId: string;
      messageId: string;
      image: import("./contracts").ChatImage;
    }
  | {
      type: "conversation.queue-updated";
      sessionId: string;
      queue: QueuedChatMessage[];
      paused: boolean;
    }
  | { type: "conversation.history-loaded"; sessionId: string; messages: ChatMessage[] }
  | { type: "runtime.status-changed"; status: RuntimeStatus }
  | { type: "conversation.session-created"; session: ChatSession }
  | { type: "conversation.session-updated"; session: ChatSession }
  | { type: "conversation.message-started"; sessionId: string; message: ChatMessage }
  | { type: "conversation.message-delta"; sessionId: string; messageId: string; delta: string }
  | {
      type: "conversation.message-reasoning-delta";
      sessionId: string;
      messageId: string;
      delta: string;
    }
  | {
      type: "conversation.message-round-committed";
      sessionId: string;
      messageId: string;
      committedContentLength: number;
      committedImageCount: number;
      committedReasoningLength: number;
    }
  | { type: "conversation.message-completed"; sessionId: string; message: ChatMessage }
  | {
      type: "job.execution-updated";
      sessionId?: string;
      execution: import("./execution/domain").JobExecution;
      accounting: import("./execution/accounting").JobAccounting | null;
    }
  | {
      type: "job.launch-updated";
      sessionId: string;
      result: import("./runtime/runtime-types").JobLaunchResult;
    }
  | {
      type: "observability.updated";
      sessionId: string;
      observability: import("./runtime/observability").RuntimeObservability;
    }
  | { type: "warning"; message: string }
  | { type: "error"; message: string }
  | { type: "cancelled"; sessionId: string; messageId?: string };
