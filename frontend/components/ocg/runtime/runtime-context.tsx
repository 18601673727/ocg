"use client";

import { createContext, useCallback, useContext, useMemo, useSyncExternalStore } from "react";
import type { ReactNode } from "react";
import { MockOcgRuntimeClient } from "./mock-client";
import { CanonicalOcgRuntimeClient } from "./canonical-launch-client";
import { useOcgControlUrl } from "../profile/control-url";
import type { CreateSessionInput, JobLaunchResult, OcgRuntimeClient, RuntimeAuthority, ScenarioId } from "./runtime-types";
import type { JobLaunchCommand } from "../job/draft-domain";
import type { ChatSession, SendMessageInput } from "../types";
import type { RuntimeSnapshot } from "./runtime-types";
import type { RuntimeDiagnostic } from "./runtime-envelope";
import type { RuntimeSyncState } from "./reconciler";
import type { OnboardingStageId } from "../bootstrap/types";

type RuntimeContextValue = {
  client: OcgRuntimeClient;
  /** Which runtime answers Chat. Derived from the control endpoint, not a fixture name. */
  authority: RuntimeAuthority;
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
  launchJob: (command: JobLaunchCommand) => Promise<JobLaunchResult>;
};

const RuntimeContext = createContext<RuntimeContextValue | null>(null);

const EMPTY_DIAGNOSTICS: readonly RuntimeDiagnostic[] = [];

export function OcgRuntimeProvider({ scenario, children }: { scenario: ScenarioId; children: ReactNode }) {
  // A loopback control URL means a real OCG backend is in the invocation, and
  // that backend owns Chat as well as Job launch. Without one there is nothing
  // to be canonical for, so the fixture runtime is what is left — it is the
  // explicit dev/mock path, never a silent product fallback. The scenario
  // seeds the shell projection either way; it never decides this.
  const controlUrl = useOcgControlUrl();
  const client = useMemo<OcgRuntimeClient>(() => {
    if (controlUrl) return CanonicalOcgRuntimeClient.connect(scenario, controlUrl, fetch);
    return new MockOcgRuntimeClient(scenario);
  }, [scenario, controlUrl]);
  const authority = client.authority;
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
  const launchJob = useCallback(async (command: JobLaunchCommand): Promise<JobLaunchResult> => {
    if (!client.launchJob) {
      return {
        outcome: "failed",
        commandId: command.commandId,
        draftId: command.draftId,
        projectId: command.projectId,
        sessionId: command.sessionId,
        message: "This runtime client does not support Job launch.",
        duplicate: false,
      };
    }
    return client.launchJob(command);
  }, [client]);

  const value = useMemo(
    () => ({
      client,
      authority,
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
      launchJob,
    }),
    [authority, cancel, client, completeOnboarding, createSession, diagnostics, launchJob, requestAccessHandoff, retryBootstrap, sendMessage, setActiveProfile, setOnboardingStage, snapshot, sync],
  );
  return <RuntimeContext.Provider value={value}>{children}</RuntimeContext.Provider>;
}

export function useOcgRuntime(): RuntimeContextValue {
  const context = useContext(RuntimeContext);
  if (!context) throw new Error("useOcgRuntime must be used inside OcgRuntimeProvider");
  return context;
}
