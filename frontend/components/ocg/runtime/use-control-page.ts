"use client";

import { useEffect, useState } from "react";

export function useControlPage<T>(
  load: (cursor: string | undefined, signal: AbortSignal) => Promise<{ items: T[]; next: string | null }>,
  active = true,
) {
  const [request, setRequest] = useState<{ source: typeof load | null; cursor?: string; revision: number }>({ source: null, revision: 0 });
  const [result, setResult] = useState<{
    source: typeof load | null; cursor?: string; revision: number;
    items: T[]; next: string | null; error: string | null;
  }>({ source: null, revision: -1, items: [], next: null, error: null });
  const cursor = request.source === load ? request.cursor : undefined;
  const revision = request.revision;
  const loading = active && (result.source !== load || result.cursor !== cursor || result.revision !== revision);

  useEffect(() => {
    if (!active) return;
    const controller = new AbortController();
    load(cursor, controller.signal).then((page) => {
      if (controller.signal.aborted) return;
      setResult((current) => ({
        source: load, cursor, revision, error: null, next: page.next,
        items: cursor && current.source === load
          ? current.cursor === cursor && current.revision === revision ? current.items : [...current.items, ...page.items]
          : page.items,
      }));
    }).catch((error: unknown) => {
      if (controller.signal.aborted) return;
      setResult((current) => ({
        ...current, source: load, cursor, revision,
        items: current.source === load ? current.items : [],
        error: error instanceof Error ? error.message : "OCG could not be reached.",
      }));
    });
    return () => controller.abort();
  }, [active, load, cursor, revision]);

  return {
    items: result.source === load ? result.items : [],
    next: result.source === load ? result.next : null,
    loading, ready: result.source === load && !result.error, error: result.source === load ? result.error : null,
    refresh: () => setRequest({ source: load, revision: revision + 1 }),
    loadMore: () => { if (result.next && !loading) setRequest({ source: load, cursor: result.next, revision: revision + 1 }); },
  };
}
