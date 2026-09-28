"use client";

import { createContext, useCallback, useContext, useMemo, useSyncExternalStore } from "react";
import type { ReactNode } from "react";
import { MockOcgRuntimeClient } from "./mock-client";
import type { CreateSessionInput, MissionLaunchResult, OcgRuntimeClient, ScenarioId } from "./runtime-types";
import type { MissionLaunchCommand } from "../mission/draft-domain";
import type { ChatSession, SendMessageInput } from "../types";
import type { RuntimeSnapshot } from "./runtime-types";
import type { RuntimeDiagnostic } from "./runtime-envelope";
import type { RuntimeSyncState } from "./reconciler";
import type { OnboardingStageId } from "../bootstrap/types";

type RuntimeContextValue = {
  client: OcgRuntimeClient;
  snapshot: RuntimeSnapshot;
  /** Canonical synchronization metadata for the runtime store, when available. */
  sync: RuntimeSyncState | null;
  /** Bounded, display-safe diagnostics observed by the reconciler. */
  diagnostics: readonly RuntimeDiagnostic[];
  createSession: (input: CreateSessionInput) => Promise<ChatSession>;
  sendMessage: (sessionId: string, input: SendMessageInput) => Promise<void>;
  cancel: (sessionId: string) => Promise<void>;
  requestAccessHandoff: () => Promise<void>;
  setOnboardingStage: (stage: OnboardingStageId) => Promise<void>;
  completeOnboarding: () => Promise<void>;
  retryBootstrap: () => Promise<void>;
  setActiveProfile: (profileId: string) => Promise<void>;
  launchMission: (command: MissionLaunchCommand) => Promise<MissionLaunchResult>;
};

const RuntimeContext = createContext<RuntimeContextValue | null>(null);

const EMPTY_DIAGNOSTICS: readonly RuntimeDiagnostic[] = [];

export function OcgRuntimeProvider({ scenario, children }: { scenario: ScenarioId; children: ReactNode }) {
  const client = useMemo(() => new MockOcgRuntimeClient(scenario), [scenario]);
  const subscribe = useCallback(
    (onStoreChange: () => void) => client.subscribe(() => onStoreChange()),
    [client],
  );
  const getSnapshot = useCallback(() => client.getSnapshot(), [client]);
  const snapshot = useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
  const getSyncState = useCallback(() => client.getSyncState?.() ?? null, [client]);
  const sync = useSyncExternalStore(subscribe, getSyncState, getSyncState);
  const diagnostics = sync?.diagnostics ?? EMPTY_DIAGNOSTICS;

  const createSession = useCallback(async (input: CreateSessionInput) => {
    return client.createSession(input);
  }, [client]);
  const sendMessage = useCallback(async (sessionId: string, input: SendMessageInput) => {
    await client.sendMessage(sessionId, input);
  }, [client]);
  const cancel = useCallback(async (sessionId: string) => {
    await client.cancel?.(sessionId);
  }, [client]);
  const requestAccessHandoff = useCallback(async () => {
    await client.requestAccessHandoff?.();
  }, [client]);
  const setOnboardingStage = useCallback(async (stage: OnboardingStageId) => {
    await client.setOnboardingStage?.(stage);
  }, [client]);
  const completeOnboarding = useCallback(async () => {
    await client.completeOnboarding?.();
  }, [client]);
  const retryBootstrap = useCallback(async () => {
    await client.retryBootstrap?.();
  }, [client]);
  const setActiveProfile = useCallback(async (profileId: string) => {
    await client.setActiveProfile?.(profileId);
  }, [client]);
  const launchMission = useCallback(async (command: MissionLaunchCommand): Promise<MissionLaunchResult> => {
    if (!client.launchMission) {
      return {
        outcome: "failed",
        commandId: command.commandId,
        draftId: command.draftId,
        projectId: command.projectId,
        sessionId: command.sessionId,
        message: "This runtime client does not support Mission launch.",
        duplicate: false,
      };
    }
    return client.launchMission(command);
  }, [client]);

  const value = useMemo(
    () => ({
      client,
      snapshot,
      sync,
      diagnostics,
      createSession,
      sendMessage,
      cancel,
      requestAccessHandoff,
      setOnboardingStage,
      completeOnboarding,
      retryBootstrap,
      setActiveProfile,
      launchMission,
    }),
    [cancel, client, completeOnboarding, createSession, diagnostics, launchMission, requestAccessHandoff, retryBootstrap, sendMessage, setActiveProfile, setOnboardingStage, snapshot, sync],
  );
  return <RuntimeContext.Provider value={value}>{children}</RuntimeContext.Provider>;
}

export function useOcgRuntime(): RuntimeContextValue {
  const context = useContext(RuntimeContext);
  if (!context) throw new Error("useOcgRuntime must be used inside OcgRuntimeProvider");
  return context;
}
