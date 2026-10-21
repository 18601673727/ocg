/**
 * The assistant-ui adapter is a view of OCG's canonical conversation state.
 * It never owns messages or execution state; those remain in the runtime
 * snapshot and are passed through as assistant-ui message-likes.
 */

import type {
  AppendMessage,
  FileMessagePart,
  ImageMessagePart,
  MessageStatus,
  ReasoningMessagePart,
  TextMessagePart,
  ThreadAssistantMessagePart,
  ThreadMessageLike,
  ThreadUserMessagePart,
  ToolCallMessagePart,
} from "@assistant-ui/react";
import type { ChatMessage, MessageRole } from "../types";
import type { JobExecution } from "../execution/domain";
import { retryInput } from "../chat/retry";
import type { TranslateFn } from "../i18n";
import { en } from "../i18n/dictionaries";

type OcgMetadata = Record<string, unknown>;

function metadataFor(message: ChatMessage, messages: readonly ChatMessage[]): OcgMetadata {
  return {
    ocgRole: message.role,
    ocgStatus: message.status,
    commandId: message.commandId,
    jobId: message.jobId,
    optimistic: message.optimistic,
    reasoning: message.reasoning,
    failureReason: message.failureReason,
    failureCode: message.failureCode,
    call: message.call ?? message.tool,
    canRetry: retryInput(messages, message.id) !== null,
  };
}

function statusFor(status: ChatMessage["status"]): MessageStatus {
  switch (status) {
    case "pending":
    case "streaming":
      return { type: "running" };
    case "completed":
      return { type: "complete", reason: "stop" };
    case "cancelled":
      return { type: "incomplete", reason: "cancelled" };
    case "unknown":
      return { type: "incomplete", reason: "other" };
    case "failed":
      return { type: "incomplete", reason: "error" };
  }
}

function textPart(text: string): TextMessagePart {
  return { type: "text", text };
}

/**
 * Managed images are served by OCG's control server, so their stored URL is a
 * path relative to that server (e.g. `/api/v1/attachments/...`). A
 * browser can only fetch it once it is resolved against the control base; the
 * page origin is not necessarily that server. Already-absolute URLs (https,
 * data, blob) are left untouched, and with no control base a relative path is
 * passed through as-is rather than being pointed at a fabricated host.
 */
function resolveManagedImageUrl(url: string, baseUrl: string | null | undefined): string {
  if (/^(https?:\/\/|data:|blob:)/i.test(url)) return url;
  if (!baseUrl) return url;
  try {
    return new URL(url, baseUrl).toString();
  } catch {
    return url;
  }
}

/**
 * An assistant-turn image, emitted as a file part.
 *
 * The installed SDK's converter drops assistant `image` parts whose URL is not
 * `https:`/`blob:`/`data:image`, which would discard OCG's loopback (`http:`)
 * managed images. A `file` part with an `image/*` mime type passes through the
 * converter unchanged and renders through the same `Image` surface, keeping the
 * managed image visible in both a live stream and stored history.
 */
function assistantImagePart(
  image: NonNullable<ChatMessage["images"]>[number],
  baseUrl: string | null | undefined,
): FileMessagePart {
  return {
    type: "file",
    id: image.id,
    filename: image.name,
    mimeType: image.media_type,
    sourceType: "url",
    data: resolveManagedImageUrl(image.url, baseUrl),
  };
}

/**
 * A user-turn image, emitted as an image part inside its attachment.
 *
 * The SDK's converter passes user image parts through untouched, so the image
 * part survives regardless of scheme; only the URL needs resolving so the
 * preview thumbnail is fetched from the control server, not the page origin.
 */
function userImagePart(
  image: NonNullable<ChatMessage["images"]>[number],
  baseUrl: string | null | undefined,
): ImageMessagePart {
  return {
    type: "image",
    id: image.id,
    image: resolveManagedImageUrl(image.url, baseUrl),
    filename: image.name,
  };
}

function toolPart(message: ChatMessage): ToolCallMessagePart {
  const activity = message.call ?? message.tool;
  const name = activity?.name ?? "unknown";
  const summary = activity?.summary ?? "";
  const detail = activity?.detail ?? "";
  const args = { summary, detail };
  return {
    type: "tool-call",
    toolCallId: activity?.id ?? message.id,
    toolName: name,
    args,
    argsText: JSON.stringify(args),
    ...(detail ? { result: detail } : {}),
    ...(activity?.status === "failed" || activity?.status === "failure" ? { isError: true } : {}),
  };
}

function assistantParts(
  message: ChatMessage,
  baseUrl: string | null | undefined,
  t: TranslateFn,
): readonly ThreadAssistantMessagePart[] {
  const parts: ThreadAssistantMessagePart[] = [];
  // Reasoning precedes the answer so the thought is read before the result.
  if (message.reasoning) {
    const reasoning: ReasoningMessagePart = { type: "reasoning", text: message.reasoning };
    parts.push(reasoning);
  }
  if (message.content) parts.push(textPart(message.content));
  if (message.role === "call" || message.role === "tool" || message.call || message.tool) {
    parts.push(toolPart(message));
  }
  for (const call of message.toolCalls ?? []) {
    let args: unknown = {};
    try { args = JSON.parse(call.arguments ?? "{}"); } catch { args = { raw: call.arguments }; }
    const checkedArgs = typeof args === "object" && args !== null && !Array.isArray(args) ? Object.fromEntries(Object.entries(args)) : { value: args };
    const refused = !call.outcome_kind && call.decision !== "admitted" && call.decision !== "awaiting_approval" && call.decision !== "awaiting_answer";
    parts.push({ type: "tool-call", toolCallId: call.tool_call_id, toolName: call.name,
      args: checkedArgs, argsText: call.arguments ?? "{}",
      ...(refused ? { result: `${t("transcript.outcome.refused")}: ${call.decision}` } : call.outcome_kind ? { result: call.result ?? call.outcome_kind } : {}),
      isError: refused || call.outcome_kind === "not_applied" || call.outcome_kind === "not_invoked" || call.outcome_kind === "uncertain",
    });
  }
  for (const image of message.images ?? []) parts.push(assistantImagePart(image, baseUrl));
  return parts;
}

function userParts(message: ChatMessage): readonly ThreadUserMessagePart[] {
  return message.content ? [textPart(message.content)] : [];
}

function userAttachments(
  message: ChatMessage,
  baseUrl: string | null | undefined,
): NonNullable<ThreadMessageLike["attachments"]> {
  return (message.images ?? []).map((image) => ({
    id: image.id,
    name: image.name,
    type: "image" as const,
    contentType: image.media_type,
    status: { type: "complete" as const },
    content: [userImagePart(image, baseUrl)],
  }));
}

/** Convert one canonical OCG message to the installed assistant-ui shape. */
export function convertOcgMessage(
  message: ChatMessage,
  _idx: number,
  messages: readonly ChatMessage[] = [message],
  baseUrl: string | null = null,
  t: TranslateFn = (key) => en[key],
): ThreadMessageLike {
  const role: MessageRole = message.role === "user" ? "user" : message.role === "assistant" ? "assistant" : "assistant";
  const common = {
    id: message.id,
    createdAt: new Date(message.createdAt),
    metadata: { custom: metadataFor(message, messages) },
  };

  if (role === "user") {
    return {
      ...common,
      role,
      content: userParts(message),
      attachments: userAttachments(message, baseUrl),
    };
  }

  return {
    ...common,
    role: "assistant",
    content: assistantParts(message, baseUrl, t),
    status: statusFor(message.status),
  };
}

export type OcgConversationState = {
  messages: ChatMessage[];
  hasEarlier: boolean;
  isRunning: boolean;
  execution: JobExecution | null;
  sessionId: string | null;
  projectId: string | null;
};

export function createOcgExternalStoreAdapter(
  getState: () => OcgConversationState,
  onNew: (message: AppendMessage) => Promise<void>,
  onEdit?: (message: AppendMessage) => Promise<void>,
  onReload?: (parentId: string | null, config: { parentId: string | null; sourceId: string | null; runConfig: { custom?: Record<string, unknown> } }) => Promise<void>,
  onCancel?: () => Promise<void>,
  controlBaseUrl?: string | null,
  onLoadEarlier?: () => Promise<void>,
  t?: TranslateFn,
): import("@assistant-ui/react").ExternalStoreAdapter<ChatMessage> {
  const state = getState();
  return {
    messages: state.messages,
    hasEarlier: state.hasEarlier,
    onLoadEarlier,
    isRunning: state.isRunning,
    isLoading: false,
    convertMessage: (message, idx) => {
      const current = getState();
      return convertOcgMessage(message, idx, current.messages, controlBaseUrl ?? null, t);
    },
    onNew,
    onEdit,
    onReload,
    onCancel,
  };
}
