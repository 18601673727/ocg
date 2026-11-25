type WorkType = "research" | "coding" | "design" | "devops";

export type ChatSession = {
  id: string;
  title: string;
  workType: WorkType;
  updatedAt: string;
  projectId?: string;
  sessionId?: string;
  agentProfile?: string;
  llmProfile?: string;
  reasoningEffort?: string | null;
  model?: string;
  conversationId?: string;
  jobState?: string;
  hasChildren?: boolean;
  archived?: boolean;
  closed?: boolean;
  settled?: boolean;
};

type MessageRole = "user" | "assistant" | "call" | "tool";

type MessageStatus = "pending" | "streaming" | "completed" | "cancelled" | "unknown" | "failed";

export type ChatMessage = {
  decision?: import("./contracts/conversations").NativeDecisionTurn;
  planningTarget?: string;
  compaction?: import("./contracts").ConversationTurn["compaction"];
  queued?: boolean;
  execution?: import("./runtime/job-status").NativeExecutionStatus;
  activity?: import("./contracts").JobActivity;
  taskPlan?: import("./contracts").TaskPlan | null;
  toolCalls?: import("./contracts").ToolCall[];
  images?: import("./contracts").ChatImage[];
  id: string;
  commandId?: string;
  jobId?: string;
  optimistic?: boolean;
  role: MessageRole;
  content: string;
  reasoning?: string;
  sourceSnapshots?: import("./markdown/file-snapshot").FileSnapshot[];
  reasoningDurationMs?: number;
  intermediate?: boolean;
  failureReason?: string;
  failureRejection?: import("./runtime/provider-rejection").ProviderRejection;
  failureAt?: number;
  failureDetail?: string;
  failureLink?: { href: string; label: string };
  failureCode?:
    | "project-missing"
    | "configuration-required"
    | "stream-closed"
    | "outcome-held"
    | "outcome-unconfirmed"
    | "outcome-stopped";
  createdAt: string;
  status: MessageStatus;
};

type RuntimeConnectionState = "connected" | "connecting" | "disconnected" | "failed";

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
  | { type: "conversation.session-updated"; session: ChatSession }
  | { type: "conversation.list-state"; state: "loading" | "loaded" | "failed" };
