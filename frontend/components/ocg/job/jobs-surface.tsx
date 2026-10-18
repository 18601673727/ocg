"use client";

import { useCallback, useState } from "react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import type { HttpCanonicalControlClient } from "../runtime/canonical-client";
import { useControlPage } from "../runtime/use-control-page";
import { JobSubmissionForm } from "./job-submission-form";
import { JobWatch } from "./job-watch";
import { isJobIdentity } from "../layout/view-domain";

export function JobsSurface({ client, job, active, onJob, onOpenChat }: {
  client: HttpCanonicalControlClient;
  job: string | null;
  active: boolean;
  onJob: (job: string) => void;
  onOpenChat?: (job: string) => void;
}) {
  const [inspect, setInspect] = useState("");
  const load = useCallback(async (before: string | undefined, signal: AbortSignal) => {
    if (!active) return { items: [], next: null };
    const page = await client.readJobs({ before, limit: 50, signal });
    return { items: page.jobs, next: page.next_before };
  }, [client, active]);
  const page = useControlPage(load, active);

  return (
    <div className="mx-auto flex w-full max-w-5xl flex-col gap-4 px-4 py-6 sm:px-6">
      <section className="rounded-lg border border-border bg-card p-5">
        <div className="mb-3 flex items-center justify-between gap-3">
          <h2 className="font-heading text-lg">Jobs</h2>
          <Button variant="outline" size="sm" disabled={page.loading} onClick={page.refresh}>Refresh</Button>
        </div>
        <p className="mb-4 text-sm text-muted-foreground">Saved Jobs, newest first. Open one to inspect its input, output, and settlement.</p>
        {page.error ? <p role="alert" className="mb-3 text-sm text-destructive">{page.error}</p> : null}
        {!page.ready && page.loading ? <p role="status">Loading Jobs…</p> : null}
        {page.ready && page.items.length === 0 ? <p className="text-sm text-muted-foreground">No Jobs have been recorded.</p> : null}
        <ul className="divide-y divide-border">
          {page.items.map((record) => (
            <li
              key={record.id}
              className={cn(
                "flex flex-wrap items-center justify-between gap-3 rounded px-2 py-3 text-sm hover:bg-muted",
                job === record.id ? "bg-muted" : undefined,
              )}
            >
              <button
                type="button"
                onClick={() => onJob(record.id)}
                aria-current={job === record.id ? "true" : undefined}
                className="flex min-w-0 flex-1 flex-wrap items-center justify-between gap-3 text-left focus-visible:outline-2 focus-visible:outline-ring"
              >
                <span className="min-w-0">
                  <span className="font-mono">{record.id}</span>
                  <span className="ml-3 break-words">
                    {record.is_chat && record.title ? record.title : (record.name ?? "Unnamed Job")}
                  </span>
                </span>
                <span className="flex flex-wrap gap-3 text-xs text-muted-foreground">
                  <span>{record.settlement.state}</span>
                  <time>{new Date(record.created_at_us / 1000).toLocaleString()}</time>
                </span>
              </button>
              {record.is_chat && onOpenChat ? (
                <Button
                  type="button"
                  variant="outline"
                  size="xs"
                  className="shrink-0"
                  onClick={(event) => {
                    event.stopPropagation();
                    onOpenChat(record.id);
                  }}
                >
                  Open in chat
                </Button>
              ) : null}
            </li>
          ))}
        </ul>
        {page.next ? <Button className="mt-3" variant="outline" disabled={page.loading} onClick={page.loadMore}>{page.loading ? "Loading…" : "Load older Jobs"}</Button> : null}
      </section>
      <section className="flex flex-col gap-3 rounded-lg border border-border bg-card p-5">
        <h2 className="font-heading text-lg">Inspect a Job</h2>
        <form className="flex items-end gap-2" onSubmit={(event) => {
          event.preventDefault();
          if (isJobIdentity(inspect.trim())) onJob(inspect.trim());
        }}>
          <label className="flex flex-1 flex-col gap-1.5 text-sm">Job identity
            <Input value={inspect} onChange={(event) => setInspect(event.target.value)} placeholder="job:<n>" spellCheck={false} />
          </label>
          <Button type="submit" variant="outline" disabled={!isJobIdentity(inspect.trim())}>Watch</Button>
        </form>
      </section>
      {job ? <JobWatch key={job} client={client} jobId={job} /> : null}
      <JobSubmissionForm client={client} onJob={(created) => { page.refresh(); onJob(created); }} />
    </div>
  );
}
