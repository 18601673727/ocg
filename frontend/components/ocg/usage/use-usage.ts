"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import type { UsageWindow } from "../contracts";
import { createHttpCanonicalControlClient, isCanonicalRejection, type CanonicalResult } from "../runtime/canonical-client";

export type UsageRead<T> = { data: T | null; loading: boolean; error: string | null; refresh: () => void };

function useRead<T>(key: string, read: ((signal: AbortSignal) => Promise<CanonicalResult<T>>) | null): UsageRead<T> {
  const [revision, setRevision] = useState(0);
  const [result, setResult] = useState<{ key: string; revision: number; data: T | null; error: string | null } | null>(null);
  const refresh = useCallback(() => setRevision(value => value + 1), []);
  useEffect(() => {
    if (!read) return;
    const controller = new AbortController();
    void read(controller.signal).then(value => {
      if (controller.signal.aborted) return;
      if (isCanonicalRejection(value)) setResult({ key, revision, data: null, error: value.message });
      else setResult({ key, revision, data: value, error: null });
    }).catch((cause: unknown) => {
      if (!controller.signal.aborted) setResult({ key, revision, data: null, error: cause instanceof Error ? cause.message : String(cause) });
    });
    return () => controller.abort();
  }, [read, key, revision]);
  const current = result?.key === key ? result : null;
  return { data: current?.data ?? null, loading: Boolean(read && (!current || current.revision !== revision)), error: current?.error ?? null, refresh };
}

function useClient(baseUrl: string | null) {
  return useMemo(() => baseUrl ? createHttpCanonicalControlClient({ baseUrl, fetch }) : null, [baseUrl]);
}

export function useProjectUsage(baseUrl: string | null, project: string, window: UsageWindow) {
  const client = useClient(baseUrl);
  const read = useCallback((signal: AbortSignal) => {
    if (!client) throw new Error("Usage endpoint unavailable");
    return client.readProjectUsage(project, window, signal);
  }, [client, project, window]);
  return useRead(JSON.stringify([baseUrl, project, window]), client && project ? read : null);
}

export function useConversationUsage(baseUrl: string | null, project: string, session: string, refreshKey: string, recorded: boolean) {
  const client = useClient(baseUrl);
  const read = useCallback((signal: AbortSignal) => {
    if (!client) throw new Error("Usage endpoint unavailable");
    return client.readConversationUsage(project, session, signal);
  }, [client, project, session]);
  return useRead(JSON.stringify([baseUrl, project, session, refreshKey, recorded]), client && project && session && recorded ? read : null);
}

export function useJobUsage(baseUrl: string | null, project: string, job: string, refreshKey: string) {
  const client = useClient(baseUrl);
  const read = useCallback((signal: AbortSignal) => {
    if (!client) throw new Error("Usage endpoint unavailable");
    return client.readJobUsage(project, job, signal);
  }, [client, project, job]);
  return useRead(JSON.stringify([baseUrl, project, job, refreshKey]), client && project && job ? read : null);
}
