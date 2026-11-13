"use client";

import { type FC, type ReactNode, useMemo, useCallback } from "react";
import { AssistantRuntimeProvider, useExternalStoreRuntime } from "@assistant-ui/react";
import { useOcgRuntime } from "./runtime-context";
import { createOcgExternalStoreAdapter, type OcgConversationState } from "./assistant-ui-adapter";
import { useOcgControlUrl } from "../profile/control-url";
import { useI18n } from "../i18n";

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

export const OcgAssistantProvider: FC<OcgAssistantProviderProps> = ({
  children,
  sessionId,
  projectId,
}) => {
  const runtime = useOcgRuntime();
  const { t } = useI18n();
  const controlBaseUrl = useOcgControlUrl();

  const messages = useMemo(() => {
    return runtime.snapshot.messagesBySession[sessionId] ?? [];
  }, [runtime.snapshot.messagesBySession, sessionId]);

  const hasEarlier = runtime.client.hasEarlierHistory?.(sessionId) ?? false;

  const execution = useMemo(() => {
    return runtime.snapshot.executionBySession[sessionId] ?? null;
  }, [runtime.snapshot.executionBySession, sessionId]);

  const isRunning = useMemo(() => {
    const messageStreaming = messages.some(
      (message) =>
        message.role === "assistant" &&
        (message.status === "streaming" || message.status === "pending"),
    );
    const executionRunning =
      execution?.state === "pending" ||
      execution?.state === "eligible" ||
      execution?.state === "running" ||
      execution?.state === "cancelling";
    return messageStreaming || executionRunning;
  }, [messages, execution]);

  const getState = useCallback((): OcgConversationState => {
    return {
      messages,
      hasEarlier,
      isRunning,
      execution,
      sessionId,
      projectId,
    };
  }, [messages, hasEarlier, isRunning, execution, sessionId, projectId]);

  const onNew = useCallback(async () => {
    throw new Error(t("app.jobLaunchUnavailable"));
  }, [t]);

  const onReload = useCallback(
    async (_parentId: string | null, config: OcgReloadConfig) => {
      if (config.sourceId) await runtime.retryMessage(sessionId, config.sourceId);
    },
    [runtime, sessionId],
  );

  const onCancel = useCallback(async () => {
    await runtime.cancel(sessionId);
  }, [runtime, sessionId]);

  const onLoadEarlier = useCallback(
    () => runtime.client.loadEarlierHistory?.(sessionId) ?? Promise.resolve(),
    [runtime.client, sessionId],
  );

  const adapter = useMemo(() => {
    return createOcgExternalStoreAdapter(
      getState,
      onNew,
      undefined,
      onReload,
      onCancel,
      controlBaseUrl,
      onLoadEarlier,
      t,
    );
  }, [getState, onNew, onReload, onCancel, controlBaseUrl, onLoadEarlier, t]);

  const assistantRuntime = useExternalStoreRuntime(adapter);

  return <AssistantRuntimeProvider runtime={assistantRuntime}>{children}</AssistantRuntimeProvider>;
};
