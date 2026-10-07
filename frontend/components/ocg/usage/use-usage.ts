"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import type { JobExecution } from "../execution/domain";
import { isExecutionActive } from "../chat/execution-status";
import type { ChatMessage } from "../types";
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

/** Cadence of Conversation Usage re-reads while its Chat executes. */
export const LIVE_USAGE_REFRESH_MS = 1_750;

type Timers = { set: (callback: () => void, ms: number) => unknown; clear: (handle: unknown) => void };

const browserTimers: Timers = {
  set: (callback, ms) => setTimeout(callback, ms),
  clear: handle => clearTimeout(handle as ReturnType<typeof setTimeout>),
};

/**
 * Whether a Chat's usage can still change: its canonical execution is active,
 * or its assistant turn is still being answered.
 */
export function conversationUsageLive(execution: JobExecution | null | undefined, messages: readonly ChatMessage[]): boolean {
  return isExecutionActive(execution) ||
    messages.some(message => message.role === "assistant" && (message.status === "streaming" || message.status === "pending"));
}

/**
 * Re-reads canonical usage while one scope is live, and once more when that
 * scope settles so its final totals appear. Usage changes per provider round,
 * finer than any Chat or Job event, so the read is timed rather than keyed.
 * A read in flight is never overtaken: the next one is scheduled after it lands.
 */
export class LiveUsageRefresh {
  private timer: unknown = null;
  private state: { scope: string; live: boolean } | null = null;

  constructor(
    private readonly refresh: () => void,
    private readonly timers: Timers = browserTimers,
    private readonly interval = LIVE_USAGE_REFRESH_MS,
  ) {}

  update(scope: string, live: boolean, loading: boolean): void {
    const previous = this.state;
    this.state = { scope, live };
    this.cancel();
    if (previous?.scope === scope && previous.live && !live) {
      this.refresh();
      return;
    }
    if (live && !loading) {
      this.timer = this.timers.set(() => {
        this.timer = null;
        this.refresh();
      }, this.interval);
    }
  }

  dispose(): void {
    this.cancel();
  }

  private cancel(): void {
    if (this.timer === null) return;
    this.timers.clear(this.timer);
    this.timer = null;
  }
}

/** Drive a usage read with [`LiveUsageRefresh`] for the given scope. */
export function useLiveUsageRefresh(read: Pick<UsageRead<unknown>, "refresh" | "loading">, scope: string, live: boolean): void {
  const { refresh, loading } = read;
  const [scheduler] = useState(() => new LiveUsageRefresh(refresh));
  useEffect(() => {
    scheduler.update(scope, live, loading);
  }, [scheduler, scope, live, loading]);
  useEffect(() => () => scheduler.dispose(), [scheduler]);
}
