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
import { ConversationRuntimeClient } from "./conversation-client";
import type { CreateSessionInput, OcgRuntimeClient, RuntimeSnapshot } from "./runtime-types";
import type { ChatSession } from "../types";
import type { ApiClient } from "./api-client";

type RuntimeContextValue = {
  api: ApiClient;
  client: OcgRuntimeClient;
  snapshot: RuntimeSnapshot;
  createSession: (input: CreateSessionInput) => Promise<ChatSession>;
  deleteSession: (sessionId: string) => Promise<void>;
  retryMessage: (sessionId: string, messageId: string) => Promise<void>;
  cancel: (sessionId: string) => Promise<void>;
};

const RuntimeContext = createContext<RuntimeContextValue | null>(null);

const subscribeHydration = () => () => {};
const browserHydrated = () => true;
const serverHydrated = () => false;

export function OcgRuntimeProvider({
  children,
  apiClient,
}: {
  children: ReactNode;
  apiClient: ApiClient;
}) {
  const hydrated = useSyncExternalStore(subscribeHydration, browserHydrated, serverHydrated);
  const client = useMemo<OcgRuntimeClient>(
    () => new ConversationRuntimeClient(apiClient),
    [apiClient],
  );
  const leases = useRef(new Map<OcgRuntimeClient, symbol>());
  useEffect(() => {
    const token = Symbol();
    const retained = leases.current;
    retained.set(client, token);
    return () => {
      queueMicrotask(() => {
        if (retained.get(client) !== token) return;
        retained.delete(client);
        client.dispose?.();
      });
    };
  }, [client]);
  const subscribe = useCallback(
    (onStoreChange: () => void) => client.subscribe(() => onStoreChange()),
    [client],
  );
  const getSnapshot = useCallback(() => client.getSnapshot(), [client]);
  const snapshot = useSyncExternalStore(subscribe, getSnapshot, getSnapshot);

  const createSession = useCallback(
    async (input: CreateSessionInput) => client.createSession(input),
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
  const value = useMemo(
    () => ({
      client,
      api: apiClient,
      snapshot,
      createSession,
      deleteSession,
      retryMessage,
      cancel,
    }),
    [cancel, client, apiClient, createSession, deleteSession, retryMessage, snapshot],
  );
  if (!hydrated) return null;
  return <RuntimeContext.Provider value={value}>{children}</RuntimeContext.Provider>;
}

export function useOcgRuntime(): RuntimeContextValue {
  const context = useContext(RuntimeContext);
  if (!context) throw new Error("useOcgRuntime must be used inside OcgRuntimeProvider");
  return context;
}
