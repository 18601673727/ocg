"use client";

import { type FC, type ReactNode, useMemo, useCallback } from "react";
import { AssistantRuntimeProvider, useExternalStoreRuntime } from "@assistant-ui/react";
import { useOcgRuntime } from "./runtime-context";
import { createOcgExternalStoreAdapter, type OcgConversationState } from "./assistant-ui-adapter";
import { useOcgApiUrl } from "../profile/api-url";
import { useI18n } from "../i18n";
import { planningConclusion } from "../chat/planning-decision";

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
  const apiBaseUrl = useOcgApiUrl();

  const messages = useMemo(() => {
    return (runtime.snapshot.messagesBySession[sessionId] ?? []).filter(
      (message) => !message.planningTarget || !planningConclusion(message),
    );
  }, [runtime.snapshot.messagesBySession, sessionId]);

  const hasEarlier = runtime.client.hasEarlierHistory?.(sessionId) ?? false;

  const isRunning = useMemo(() => {
    return messages.some(
      (message) =>
        message.role === "assistant" &&
        (message.status === "streaming" || message.status === "pending"),
    );
  }, [messages]);

  const getState = useCallback((): OcgConversationState => {
    return {
      messages,
      hasEarlier,
      isRunning,
      sessionId,
      projectId,
    };
  }, [messages, hasEarlier, isRunning, sessionId, projectId]);

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
      apiBaseUrl,
      onLoadEarlier,
      t,
    );
  }, [getState, onNew, onReload, onCancel, apiBaseUrl, onLoadEarlier, t]);

  const assistantRuntime = useExternalStoreRuntime(adapter);

  return <AssistantRuntimeProvider runtime={assistantRuntime}>{children}</AssistantRuntimeProvider>;
};
