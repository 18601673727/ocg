import type { ChatMessage, SendMessageInput } from "../types";

export function retryContent(messages: readonly ChatMessage[], messageId: string): string | null {
  return retryInput(messages, messageId)?.content.trim() || null;
}

export function retryInput(messages: readonly ChatMessage[], messageId: string): SendMessageInput | null {
  if (messages.some(message => message.role === "assistant" && (message.status === "pending" || message.status === "streaming"))) return null;
  const index = messages.findIndex(message => message.id === messageId);
  const failed = messages[index];
  if (!failed || failed.role !== "assistant" || (failed.status !== "failed" && failed.status !== "cancelled")) return null;
  const user = messages.slice(0, index).reverse().find(message => message.role === "user" && (!failed.commandId || message.commandId === failed.commandId));
  return user && (user.content.trim() || user.images?.length) ? { content: user.content.trim(), images: user.images } : null;
}

/**
 * The same assistant Message, restarted for a canonical Retry of its Job.
 *
 * Everything the previous execution presented is dropped before the
 * replacement Attempt's first delta, so its output never appends to the old
 * text and its first round boundary starts from an empty commit.
 */
export function retryPresentation(message: ChatMessage): ChatMessage {
  return {
    id: message.id,
    role: message.role,
    commandId: message.commandId,
    jobId: message.jobId,
    createdAt: message.createdAt,
    content: "",
    status: "streaming",
  };
}
