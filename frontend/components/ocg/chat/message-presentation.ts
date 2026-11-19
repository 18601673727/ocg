import type { ChatMessage } from "../types";
import { retryInput } from "./retry";

type MessageContext = { activityId: string | undefined; retryId: string | undefined };

export type PresentedMessage = ChatMessage & {
  showActivityIndicator?: boolean;
  canRetry?: boolean;
};

function messageContext(messages: readonly ChatMessage[]): MessageContext {
  const transcript = messages.filter((message) => !message.planningTarget);
  const activity =
    transcript.find(
      (item) => item.role === "assistant" && !item.intermediate && item.status === "streaming",
    ) ??
    transcript.find(
      (item) => item.role === "assistant" && !item.intermediate && item.status === "pending",
    );
  const lastAssistant = transcript.findLast((item) => item.role === "assistant");
  return {
    activityId: activity?.id,
    retryId:
      lastAssistant && retryInput(transcript, lastAssistant.id) !== null
        ? lastAssistant.id
        : undefined,
  };
}

export function presentOcgMessages(messages: ChatMessage[]): PresentedMessage[] {
  const { activityId, retryId } = messageContext(messages);
  return messages.map((message) => {
    const showActivityIndicator = message.id === activityId;
    const canRetry = message.id === retryId && message.execution?.retrySupported === true;
    if (!showActivityIndicator && !canRetry) return message;
    return { ...message, showActivityIndicator, canRetry };
  });
}
