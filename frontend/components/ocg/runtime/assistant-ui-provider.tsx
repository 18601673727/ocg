"use client";

/**
 * AssistantUI Provider that bridges OCG's runtime-store with assistant-ui components.
 * 
 * This provider wraps the assistant-ui ExternalStoreRuntime with OCG's existing
 * runtime store, allowing assistant-ui Thread components to render OCG conversations
 * while preserving all OCG-specific functionality.
 */

import { type FC, type ReactNode, useMemo, useCallback } from "react";
import { AssistantRuntimeProvider, useExternalStoreRuntime, type AppendMessage } from "@assistant-ui/react";
import { useOcgRuntime } from "./runtime-context";
import { createOcgExternalStoreAdapter, type OcgConversationState } from "./assistant-ui-adapter";
import { useOcgControlUrl } from "../profile/control-url";
import type { ChatImage } from "../contracts";

type OcgReloadConfig = {
  parentId: string | null;
  sourceId: string | null;
  runConfig: { custom?: Record<string, unknown> };
};

interface OcgAssistantProviderProps {
  children: ReactNode;
  sessionId: string;
  projectId: string;
}

/**
 * Provider that connects OCG's runtime store to assistant-ui.
 * 
 * Usage:
 * ```tsx
 * <OcgAssistantProvider>
 *   <Thread />
 * </OcgAssistantProvider>
 * ```
 */
export const OcgAssistantProvider: FC<OcgAssistantProviderProps> = ({ children, sessionId, projectId }) => {
  const runtime = useOcgRuntime();
  // Managed images are served by the control server; resolve their paths
  // against this base so the browser can fetch them from any page origin.
  const controlBaseUrl = useOcgControlUrl();

  // Get current conversation from snapshot
  const messages = useMemo(() => {
    return runtime.snapshot.messagesBySession[sessionId] ?? [];
  }, [runtime.snapshot.messagesBySession, sessionId]);

  const execution = useMemo(() => {
    return runtime.snapshot.executionBySession[sessionId] ?? null;
  }, [runtime.snapshot.executionBySession, sessionId]);

  const isRunning = useMemo(() => {
    const messageStreaming = messages.some((message) =>
      message.role === "assistant" && (message.status === "streaming" || message.status === "pending"),
    );
    const executionRunning = execution?.state === "pending" || execution?.state === "eligible" || execution?.state === "running" || execution?.state === "cancelling";
    return messageStreaming || executionRunning;
  }, [messages, execution]);

  // Get current conversation state
  const getState = useCallback((): OcgConversationState => {
    return {
      messages,
      isRunning,
      execution,
      sessionId,
      projectId,
    };
  }, [messages, isRunning, execution, sessionId, projectId]);

  // Handle new message from user
  const onNew = useCallback(async (message: AppendMessage) => {
    const content = message.content
      .map((part) => {
        if (part.type === "text") return part.text;
        return "";
      })
      .join("");

    const images: ChatImage[] = (message.attachments ?? [])
      .flatMap((attachment) => attachment.content)
      .flatMap((part): ChatImage[] => part.type === "image" && part.id && part.filename
        ? [{ id: part.id, name: part.filename, media_type: "image/*", url: part.image }]
        : []);
    await runtime.sendMessage(sessionId, { content, images, projectId, mode: message.steer ? "steer" : "queue" });
  }, [projectId, runtime, sessionId]);

  // Handle message reload/regenerate
  const onReload = useCallback(async (_parentId: string | null, config: OcgReloadConfig) => {
    if (config.sourceId) await runtime.retryMessage(sessionId, config.sourceId);
  }, [runtime, sessionId]);

  // Handle cancellation
  const onCancel = useCallback(async () => {
    await runtime.cancel(sessionId);
  }, [runtime, sessionId]);

  // Create the external store adapter
  const adapter = useMemo(() => {
    return createOcgExternalStoreAdapter(
      getState,
      onNew,
      undefined, // onEdit not supported
      onReload,
      onCancel,
      controlBaseUrl
    );
  }, [getState, onNew, onReload, onCancel, controlBaseUrl]);

  // Create the assistant-ui runtime from the external store
  const assistantRuntime = useExternalStoreRuntime(adapter);

  return (
    <AssistantRuntimeProvider runtime={assistantRuntime}>
      {children}
    </AssistantRuntimeProvider>
  );
};
