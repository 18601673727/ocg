"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useSyncExternalStore,
} from "react";
import type { ReactNode } from "react";
import { CanonicalOcgRuntimeClient } from "./canonical-launch-client";
import type {
  CreateSessionInput,
  JobLaunchResult,
  OcgRuntimeClient,
  RuntimeAuthority,
} from "./runtime-types";
import type { JobLaunchCommand } from "../job/draft-domain";
import type { ChatSession, SendMessageInput } from "../types";
import type { RuntimeSnapshot } from "./runtime-types";
import type { RuntimeDiagnostic } from "./runtime-envelope";
import type { RuntimeSyncState } from "./reconciler";
import type { OnboardingStageId } from "../bootstrap/types";

import type { HttpCanonicalControlClient } from "./control-client";

type RuntimeContextValue = {
  control: HttpCanonicalControlClient | null;
  client: OcgRuntimeClient;
  /** Which runtime answers Chat. Derived from the control endpoint, not a fixture name. */
  authority: RuntimeAuthority;
  snapshot: RuntimeSnapshot;
  /** Canonical synchronization metadata for the runtime store, when available. */
  sync: RuntimeSyncState | null;
  /** Bounded, display-safe diagnostics observed by the reconciler. */
  diagnostics: readonly RuntimeDiagnostic[];
  createSession: (input: CreateSessionInput) => Promise<ChatSession>;
  deleteSession: (sessionId: string) => Promise<void>;
  sendMessage: (sessionId: string, input: SendMessageInput) => Promise<void>;
  retryMessage: (sessionId: string, messageId: string) => Promise<void>;
  cancel: (sessionId: string) => Promise<void>;
  requestAccessHandoff: () => Promise<void>;
  setOnboardingStage: (stage: OnboardingStageId) => Promise<void>;
  completeOnboarding: () => Promise<void>;
  retryBootstrap: () => Promise<void>;
  setActiveProfile: (profileId: string) => Promise<void>;
  launchJob: (command: JobLaunchCommand) => Promise<JobLaunchResult>;
};

const RuntimeContext = createContext<RuntimeContextValue | null>(null);

const subscribeHydration = () => () => {};
const browserHydrated = () => true;
const serverHydrated = () => false;

const EMPTY_DIAGNOSTICS: readonly RuntimeDiagnostic[] = [];

export function OcgRuntimeProvider({
  children,
  controlClient,
}: {
  children: ReactNode;
  controlClient: HttpCanonicalControlClient;
}) {
  const hydrated = useSyncExternalStore(subscribeHydration, browserHydrated, serverHydrated);
  const client = useMemo<OcgRuntimeClient>(
    () => new CanonicalOcgRuntimeClient(controlClient),
    [controlClient],
  );
  const leases = useRef(new Map<OcgRuntimeClient, symbol>());
  useEffect(() => {
    const token = Symbol();
    const retained = leases.current;
    retained.set(client, token);
    return () => {
      // StrictMode reattaches the same client before this microtask runs.
      queueMicrotask(() => {
        if (retained.get(client) !== token) return;
        retained.delete(client);
        client.dispose?.();
      });
    };
  }, [client]);
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

  const createSession = useCallback(
    async (input: CreateSessionInput) => {
      return client.createSession(input);
    },
    [client],
  );
  const sendMessage = useCallback(
    async (sessionId: string, input: SendMessageInput) => {
      await client.sendMessage(sessionId, input);
    },
    [client],
  );
  const deleteSession = useCallback(
    async (sessionId: string) => {
      if (!client.deleteSession) throw new Error("This runtime does not support deleting chats.");
      await client.deleteSession(sessionId);
    },
    [client],
  );
  const cancel = useCallback(
    async (sessionId: string) => {
      await client.cancel?.(sessionId);
    },
    [client],
  );
  const retryMessage = useCallback(
    async (sessionId: string, messageId: string) => {
      await client.retryMessage(sessionId, messageId);
    },
    [client],
  );
  const requestAccessHandoff = useCallback(async () => {
    await client.requestAccessHandoff?.();
  }, [client]);
  const setOnboardingStage = useCallback(
    async (stage: OnboardingStageId) => {
      await client.setOnboardingStage?.(stage);
    },
    [client],
  );
  const completeOnboarding = useCallback(async () => {
    await client.completeOnboarding?.();
  }, [client]);
  const retryBootstrap = useCallback(async () => {
    await client.retryBootstrap?.();
  }, [client]);
  const setActiveProfile = useCallback(
    async (profileId: string) => {
      await client.setActiveProfile?.(profileId);
    },
    [client],
  );
  const launchJob = useCallback(
    async (command: JobLaunchCommand): Promise<JobLaunchResult> => {
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
    },
    [client],
  );

  const value = useMemo(
    () => ({
      client,
      control:
        controlClient ?? (client instanceof CanonicalOcgRuntimeClient ? client.control : null),
      authority,
      snapshot,
      sync,
      diagnostics,
      createSession,
      deleteSession,
      sendMessage,
      retryMessage,
      cancel,
      requestAccessHandoff,
      setOnboardingStage,
      completeOnboarding,
      retryBootstrap,
      setActiveProfile,
      launchJob,
    }),
    [
      authority,
      cancel,
      client,
      controlClient,
      completeOnboarding,
      createSession,
      deleteSession,
      diagnostics,
      launchJob,
      requestAccessHandoff,
      retryBootstrap,
      retryMessage,
      sendMessage,
      setActiveProfile,
      setOnboardingStage,
      snapshot,
      sync,
    ],
  );
  if (!hydrated) return null;
  return <RuntimeContext.Provider value={value}>{children}</RuntimeContext.Provider>;
}

export function useOcgRuntime(): RuntimeContextValue {
  const context = useContext(RuntimeContext);
  if (!context) throw new Error("useOcgRuntime must be used inside OcgRuntimeProvider");
  return context;
}
