import type { JSX } from "@solidjs/web";
import { createContext, createMemo, createSignal, onCleanup, useContext } from "solid-js";
import type { ApiClient } from "./api-client";
import { ConversationRuntimeClient } from "./conversation-client";
import type { CreateSessionInput, RuntimeSnapshot } from "./runtime-types";

const RuntimeContext = createContext<ReturnType<typeof createRuntime>>();

function createRuntime(api: ApiClient) {
  const client = new ConversationRuntimeClient(api);
  const [snapshot, setSnapshot] = createSignal(client.getSnapshot());
  const unsubscribe = client.subscribe(() => setSnapshot(client.getSnapshot()));
  onCleanup(() => {
    unsubscribe();
    client.dispose();
  });
  return {
    api,
    client,
    snapshot,
    createSession: (input: CreateSessionInput) => client.createSession(input),
    deleteSession: (id: string) => client.deleteSession(id),
    retryMessage: (id: string, message: string) => client.retryMessage(id, message),
    cancel: (id: string) => client.cancel(id),
  };
}

export function OcgRuntimeProvider(props: { apiClient: ApiClient; children: JSX.Element }) {
  const runtime = createRuntime(props.apiClient);
  return <RuntimeContext value={runtime}>{props.children}</RuntimeContext>;
}

export function useOcgRuntime() {
  const runtime = useContext(RuntimeContext);
  if (!runtime) throw new Error("useOcgRuntime must be used inside OcgRuntimeProvider");
  return runtime;
}

export function useOcgRuntimeSnapshot<T>(select: (snapshot: RuntimeSnapshot) => T) {
  const { snapshot } = useOcgRuntime();
  return createMemo(() => select(snapshot()));
}
