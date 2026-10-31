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
import type { CreateSessionInput, OcgRuntimeClient, RuntimeSnapshot } from "./runtime-types";
import type { ChatSession, SendMessageInput } from "../types";
import type { HttpCanonicalControlClient } from "./control-client";

type RuntimeContextValue = {
  control: HttpCanonicalControlClient | null;
  client: OcgRuntimeClient;
  snapshot: RuntimeSnapshot;
  createSession: (input: CreateSessionInput) => Promise<ChatSession>;
  deleteSession: (sessionId: string) => Promise<void>;
  sendMessage: (sessionId: string, input: SendMessageInput) => Promise<void>;
  retryMessage: (sessionId: string, messageId: string) => Promise<void>;
  cancel: (sessionId: string) => Promise<void>;
};

const RuntimeContext = createContext<RuntimeContextValue | null>(null);

const subscribeHydration = () => () => {};
const browserHydrated = () => true;
const serverHydrated = () => false;

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
  const value = useMemo(
    () => ({
      client,
      control:
        controlClient ?? (client instanceof CanonicalOcgRuntimeClient ? client.control : null),
      snapshot,
      createSession,
      deleteSession,
      sendMessage,
      retryMessage,
      cancel,
    }),
    [
      cancel,
      client,
      controlClient,
      createSession,
      deleteSession,
      retryMessage,
      sendMessage,
      snapshot,
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
