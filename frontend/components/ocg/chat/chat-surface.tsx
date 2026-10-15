"use client";

import { useState } from "react";

import { EmptyState } from "@/components/ocg/primitives";
import type { HttpCanonicalControlClient } from "@/components/ocg/runtime/canonical-client";
import { JobSubmissionForm, type SubmissionTarget } from "@/components/ocg/job/job-submission-form";
import { JobWatch } from "@/components/ocg/job/job-watch";
import { AgentSelector } from "./agent-selector";

/**
 * Chat over the Job-only substrate: a message is one Job submitted as the
 * selected Agent Profile, and its transcript is that Job's frozen objective
 * and its stored outputs, followed live. Chat owns no state the Control Plane
 * does not: the Job in view is the address's `?job=`, so a reload recovers it.
 */
export function ChatSurface({
  client,
  job,
  onJob,
}: {
  client: HttpCanonicalControlClient;
  job: string | null;
  onJob: (job: string) => void;
}) {
  const [target, setTarget] = useState<SubmissionTarget | null>(null);
  return (
    <div className="mx-auto flex w-full max-w-3xl flex-col gap-4 px-4 py-6 sm:px-6">
      <p role="note" className="rounded-lg border border-border bg-muted/20 px-3 py-2 text-xs leading-5 text-muted-foreground">
        Each message is its own Job. Earlier messages are not sent to the model
        as context: a Chat identity that groups Jobs into one conversation, and
        the context that would come with it, do not exist yet, so follow-up
        messages do not see this one.
      </p>
      {job ? (
        <JobWatch key={job} client={client} jobId={job} presentation="chat" />
      ) : (
        <EmptyState>No message sent yet. Choose an Agent below and send one.</EmptyState>
      )}
      <AgentSelector client={client} onChange={setTarget} />
      <JobSubmissionForm client={client} onJob={onJob} target={target} />
    </div>
  );
}
