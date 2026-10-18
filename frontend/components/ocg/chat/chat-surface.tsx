"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import { EmptyState } from "@/components/ocg/primitives";
import type { ControlConversationTurn, ControlToolCall } from "@/components/ocg/contracts";
import type { HttpCanonicalControlClient } from "@/components/ocg/runtime/canonical-client";
import { JobSubmissionForm, type SubmissionTarget } from "@/components/ocg/job/job-submission-form";
import { JobWatch } from "@/components/ocg/job/job-watch";
import { AgentSelector } from "./agent-selector";
import { JobTranscript } from "./job-transcript";
import { jobTranscript } from "./job-transcript-model";

type SettlementState = "unsettled" | "completed" | "failed" | "cancelled" | "unknown";

const SETTLEMENT_BADGE: Record<SettlementState, string> = {
  unsettled: "bg-muted text-muted-foreground",
  completed: "bg-primary/10 text-primary",
  failed: "bg-destructive/10 text-destructive",
  cancelled: "bg-muted text-muted-foreground",
  unknown: "bg-destructive/10 text-destructive",
};

/**
 * Chat over the Job-only substrate: each turn is one Job submitted as the
 * selected Agent Profile, linked to its predecessor turn via `previous`, and
 * the conversation is a computed projection reconstructed from persisted Job
 * facts (`GET /api/v1/jobs/{job}/conversation`). The Job in view is the
 * address's `?job=`, so a reload recovers the full conversation chain.
 */
export function ChatSurface({
  client,
  job,
  onJob,
  onNewConversation,
  onInspectJob,
}: {
  client: HttpCanonicalControlClient;
  job: string | null;
  onJob: (job: string) => void;
  onNewConversation?: () => void;
  onInspectJob?: (job: string) => void;
}) {
  const [target, setTarget] = useState<SubmissionTarget | null>(null);
  const [turns, setTurns] = useState<ControlConversationTurn[]>([]);
  const [toolsByJob, setToolsByJob] = useState<Record<string, ControlToolCall[]>>({});
  const loadedToolsJobs = useRef<Set<string>>(new Set());
  const toolsAbortController = useRef<AbortController>(new AbortController());
  const conversationEpoch = useRef(0);
  const [conversationError, setConversationError] = useState<string | null>(null);
  const [headState, setHeadState] = useState<SettlementState | null>(null);

  const loadConversation = useCallback(
    async (targetJob: string, signal?: AbortSignal) => {
      try {
        const response = await client.readJobConversation(targetJob, signal);
        if (signal?.aborted) return;
        setTurns(response.turns);
        setConversationError(null);
      } catch (err) {
        if (signal?.aborted) return;
        setConversationError(err instanceof Error ? err.message : "Could not read conversation turns.");
      }
    },
    [client],
  );

  useEffect(() => {
    if (!job) return;
    const controller = new AbortController();
    let aborted = false;
    (async () => {
      try {
        const response = await client.readJobConversation(job, controller.signal);
        if (!aborted) {
          setTurns(response.turns);
          setConversationError(null);
        }
      } catch (err) {
        if (!aborted) {
          setConversationError(err instanceof Error ? err.message : "Could not read conversation turns.");
        }
      }
    })();
    return () => {
      aborted = true;
      controller.abort();
    };
  }, [client, job]);

  useEffect(() => {
    return () => {
      toolsAbortController.current.abort();
    };
  }, []);

  useEffect(() => {
    if (turns.length === 0) return;
    if (toolsAbortController.current.signal.aborted) {
      toolsAbortController.current = new AbortController();
    }
    const signal = toolsAbortController.current.signal;
    const epoch = conversationEpoch.current;
    const earlierTurns = job ? turns.filter((turn) => turn.job.id !== job) : turns;
    for (const turn of earlierTurns) {
      const jobId = turn.job.id;
      if (loadedToolsJobs.current.has(jobId)) continue;
      loadedToolsJobs.current.add(jobId);
      client
        .readAllJobTools(jobId, signal)
        .then((tools) => {
          if (signal.aborted || conversationEpoch.current !== epoch) return;
          setToolsByJob((prev) => ({ ...prev, [jobId]: tools }));
        })
        .catch(() => {
          if (!signal.aborted && conversationEpoch.current === epoch) {
            loadedToolsJobs.current.delete(jobId);
          }
        });
    }
  }, [client, job, turns]);

  const handleNewConversation = useCallback(() => {
    toolsAbortController.current.abort();
    toolsAbortController.current = new AbortController();
    conversationEpoch.current += 1;
    setTurns([]);
    setToolsByJob({});
    loadedToolsJobs.current.clear();
    setConversationError(null);
    setHeadState(null);
    onNewConversation?.();
  }, [onNewConversation]);

  const handleHeadStateChange = useCallback(
    (state: SettlementState) => {
      setHeadState(state);
      if (job && state !== "unsettled") {
        void loadConversation(job);
      }
    },
    [job, loadConversation],
  );

  const priorTurns = job ? turns.filter((turn) => turn.job.id !== job) : [];
  const headTurn = job ? turns.find((turn) => turn.job.id === job) ?? null : null;
  const effectiveHeadState: SettlementState | null =
    headState ?? headTurn?.job.settlement.state ?? null;
  const lastCompletedAncestor =
    [...priorTurns].reverse().find((turn) => turn.job.settlement.state === "completed")?.job.id ?? null;

  const preferredAgentProfile =
    headTurn?.input?.configuration
      ? headTurn.input.configuration.agent_profile
      : null;
  const preferredLlmProfile =
    headTurn?.input?.configuration
      ? headTurn.input.configuration.llm_profile
      : null;

  const previousJobForNextTurn = !job
    ? null
    : effectiveHeadState === "completed"
      ? job
      : null;

  const composerDisabledReason = !job
    ? null
    : effectiveHeadState === null || effectiveHeadState === "unsettled"
      ? `Waiting for ${job} to finish before sending the next turn…`
      : effectiveHeadState !== "completed"
        ? `${job} ended in state "${effectiveHeadState}". Inspect the Job record, continue from an earlier completed turn, or start a new conversation.`
        : null;

  return (
    <div className="mx-auto flex w-full max-w-3xl flex-col gap-4 px-4 py-6 sm:px-6">
      <div className="flex flex-wrap items-center justify-between gap-2 rounded-lg border border-border bg-card px-4 py-2.5 text-xs">
        <div className="flex flex-wrap items-center gap-2 text-muted-foreground">
          <span className="font-semibold text-foreground">Conversation projection</span>
          {job ? (
            <>
              <span>·</span>
              <span>
                { Math.max(turns.length, 1) } { Math.max(turns.length, 1) === 1 ? "turn" : "turns" }
              </span>
              <span>·</span>
              <span>
                Head: <code className="font-mono text-foreground">{job}</code>
              </span>
            </>
          ) : (
            <span>· New conversation</span>
          )}
        </div>
        <div className="flex flex-wrap items-center gap-2">
          {job && onInspectJob ? (
            <Button type="button" size="sm" variant="outline" onClick={() => onInspectJob(job)}>
              Inspect Job
            </Button>
          ) : null}
          {job && onNewConversation ? (
            <Button type="button" size="sm" variant="ghost" onClick={handleNewConversation}>
              New conversation
            </Button>
          ) : null}
        </div>
      </div>

      {conversationError ? (
        <p role="alert" className="rounded-md border border-destructive/40 bg-destructive/10 px-3 py-2 text-xs text-destructive">
          {conversationError}
        </p>
      ) : null}

      {priorTurns.map((turn, index) => {
        const objective = turn.input ? turn.input.objective : null;
        const turnTools = toolsByJob[turn.job.id];
        const transcript = jobTranscript(
          turn.job.id,
          objective,
          turn.outputs,
          turn.job.settlement,
          false,
          turn.input ? turn.input.attachments : undefined,
          turnTools,
        );
        return (
          <section
            key={turn.job.id}
            aria-label={`Turn ${index + 1} (${turn.job.id})`}
            className="flex flex-col gap-3 rounded-lg border border-border bg-card p-5 text-card-foreground"
          >
            <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-muted-foreground">
              <div className="flex items-center gap-2">
                <span className="font-semibold text-foreground">Turn {index + 1}</span>
                <button
                  type="button"
                  onClick={() => onJob(turn.job.id)}
                  className="font-mono underline underline-offset-2 hover:text-foreground"
                >
                  {turn.job.id}
                </button>
                {turn.job.previous ? (
                  <span>
                    after <code className="font-mono">{turn.job.previous}</code>
                  </span>
                ) : null}
              </div>
              <div className="flex items-center gap-2">
                <span
                  className={`rounded-md px-2 py-0.5 text-[11px] font-semibold uppercase ${SETTLEMENT_BADGE[turn.job.settlement.state]}`}
                >
                  {turn.job.settlement.state}
                </span>
                {onInspectJob ? (
                  <button
                    type="button"
                    onClick={() => onInspectJob(turn.job.id)}
                    className="underline underline-offset-2 hover:text-foreground"
                  >
                    Inspect
                  </button>
                ) : null}
              </div>
            </div>
            <JobTranscript
              turns={transcript}
              userLabel="You"
              attachmentContentUrl={(id) => client.attachmentContentUrl(id)}
            />
          </section>
        );
      })}

      {job ? (
        <div className="flex flex-col gap-2">
          {priorTurns.length > 0 ? (
            <div className="flex items-center justify-between px-1 text-xs text-muted-foreground">
              <span className="font-semibold text-foreground">
                Turn {priorTurns.length + 1} (current)
              </span>
              <span>
                Preceded by <code className="font-mono">{priorTurns[priorTurns.length - 1]?.job.id}</code>
              </span>
            </div>
          ) : null}
          <JobWatch
            key={job}
            client={client}
            jobId={job}
            presentation="chat"
            onStateChange={handleHeadStateChange}
          />
        </div>
      ) : (
        <EmptyState>No message sent yet. Choose an Agent below and send one.</EmptyState>
      )}

      {job && effectiveHeadState && effectiveHeadState !== "unsettled" && effectiveHeadState !== "completed" ? (
        <div
          role="status"
          className="flex flex-wrap items-center justify-between gap-2 rounded-lg border border-border bg-muted/30 px-4 py-3 text-xs text-muted-foreground"
        >
          <span>
            Turn <code className="font-mono text-foreground">{job}</code> did not complete ({effectiveHeadState}).
          </span>
          <div className="flex flex-wrap items-center gap-2">
            {lastCompletedAncestor ? (
              <Button
                type="button"
                size="sm"
                variant="outline"
                onClick={() => onJob(lastCompletedAncestor)}
              >
                Return to {lastCompletedAncestor}
              </Button>
            ) : null}
            {onNewConversation ? (
              <Button type="button" size="sm" variant="outline" onClick={handleNewConversation}>
                Start new conversation
              </Button>
            ) : null}
          </div>
        </div>
      ) : null}

      <AgentSelector
        client={client}
        onChange={setTarget}
        preferredAgentProfile={preferredAgentProfile}
        preferredLlmProfile={preferredLlmProfile}
      />
      <JobSubmissionForm
        client={client}
        onJob={onJob}
        target={target}
        previousJob={previousJobForNextTurn}
        disabledReason={composerDisabledReason}
      />
    </div>
  );
}
