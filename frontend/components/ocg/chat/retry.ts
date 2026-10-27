import type { ChatMessage, SendMessageInput } from "../types";

export function retryInput(
  messages: readonly ChatMessage[],
  messageId: string,
): SendMessageInput | null {
  if (
    messages.some(
      (message) =>
        message.role === "assistant" &&
        (message.status === "pending" || message.status === "streaming"),
    )
  )
    return null;
  const index = messages.findIndex((message) => message.id === messageId);
  const failed = messages[index];
  if (
    !failed ||
    failed.role !== "assistant" ||
    (failed.status !== "failed" && failed.status !== "unknown" && failed.status !== "cancelled")
  )
    return null;
  if (messages.slice(index + 1).some((message) => message.role === "assistant")) return null;
  const user = messages
    .slice(0, index)
    .reverse()
    .find(
      (message) =>
        message.role === "user" &&
        !message.withdrawn &&
        (!failed.commandId || message.commandId === failed.commandId),
    );
  return user && (user.content.trim() || user.images?.length)
    ? { content: user.content.trim(), images: user.images }
    : null;
}
