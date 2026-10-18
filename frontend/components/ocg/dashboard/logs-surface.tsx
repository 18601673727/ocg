"use client";

import { useCallback, useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import type { HttpCanonicalControlClient } from "../runtime/canonical-client";
import type { decodeLogs } from "../contracts";

export function LogsSurface({ client }: { client: HttpCanonicalControlClient }) {
  const [file, setFile] = useState("");
  const [attempt, setAttempt] = useState(0);
  const [live, setLive] = useState(true);
  const [snapshot, setSnapshot] = useState<ReturnType<typeof decodeLogs> | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const refresh = useCallback(() => setAttempt((current) => current + 1), []);

  useEffect(() => {
    const controller = new AbortController();
    (async () => {
      setLoading(true);
      try {
        const answer = await client.readLogs({ file: file || undefined, signal: controller.signal });
        if (!controller.signal.aborted) { setSnapshot(answer); setError(null); }
      } catch (error) {
        if (!controller.signal.aborted) setError(error instanceof Error ? error.message : "Logs could not be read.");
      } finally {
        if (!controller.signal.aborted) setLoading(false);
      }
    })();
    return () => controller.abort();
  }, [client, file, attempt]);

  useEffect(() => {
    if (!live) return;
    const timer = setInterval(refresh, 5000);
    return () => clearInterval(timer);
  }, [live, refresh]);

  return <div className="mx-auto flex w-full max-w-7xl flex-col gap-4 px-4 py-6 sm:px-6">
    <div className="flex flex-wrap items-center justify-between gap-3"><h2 className="font-heading text-xl">Logs</h2>
      <div className="flex flex-wrap items-center gap-3">
        <label className="flex items-center gap-2 text-sm"><input type="checkbox" checked={live} onChange={(event) => setLive(event.target.checked)} />Auto-refresh</label>
        <Button variant="outline" disabled={loading} onClick={refresh}>Refresh</Button>
      </div>
    </div>
    <label className="flex flex-col gap-2 text-sm">Runtime log file
      <select className="min-w-0 rounded-md border border-input bg-background px-3 py-2 font-mono text-xs" value={file} onChange={(event) => { setFile(event.target.value); setSnapshot(null); }}>
        <option value="">Latest file</option>
        {snapshot?.files.map((name) => <option key={name} value={name}>{name}</option>)}
      </select>
    </label>
    {error ? <p role="alert" className="text-sm text-destructive">{error} <Button variant="ghost" size="sm" onClick={() => { setFile(""); refresh(); }}>Read latest file</Button></p> : null}
    {!snapshot && loading ? <p role="status">Loading runtime logs…</p> : null}
    {snapshot ? <section aria-label="Runtime log output" className="min-w-0 rounded-lg border border-border bg-card p-4">
      <div className="mb-3 flex flex-wrap justify-between gap-2 text-xs text-muted-foreground"><span className="font-mono">{snapshot.file ?? "No runtime log file yet"}</span><span>{snapshot.lines.length} lines · {live ? "refreshes every 5 seconds" : "auto-refresh paused"}</span></div>
      {snapshot.lines.length ? <pre className="max-h-[65vh] overflow-auto whitespace-pre-wrap break-words font-mono text-xs leading-6">{snapshot.lines.join("\n")}</pre> : <p className="text-sm text-muted-foreground">No complete log records have been written yet.</p>}
      {snapshot.truncated ? <p className="mt-3 text-xs text-muted-foreground">Showing the latest 100 lines within the last 64 KiB.</p> : null}
      {snapshot.files_truncated ? <p className="mt-2 text-xs text-muted-foreground">The file picker shows up to 100 recent runtime logs.</p> : null}
    </section> : null}
  </div>;
}
