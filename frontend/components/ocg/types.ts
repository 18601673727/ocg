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
      type: "conversation.queue-updated";
      sessionId: string;
      queue: QueuedChatMessage[];
      paused: boolean;
    }
  | { type: "conversation.history-loaded"; sessionId: string; messages: ChatMessage[] }
  | { type: "runtime.status-changed"; status: RuntimeStatus }
  | { type: "conversation.session-created"; session: ChatSession }
  | { type: "conversation.session-updated"; session: ChatSession };
