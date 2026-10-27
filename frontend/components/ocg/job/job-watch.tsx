"use client";

import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import { useEffect, useMemo, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import { TaskStatus } from "./task-status";
import { reconcileJob, taskPlanActive } from "../runtime/job-status";
import { JobTranscript } from "@/components/ocg/chat/job-transcript";
import { jobTranscript } from "@/components/ocg/chat/job-transcript-model";
import {
  JOB_WATCH_RESIDENT_OUTPUTS,
  JOB_WATCH_WINDOW_STEP,
  hasEvictedOutputs,
  mergeOutputWindow,
} from "./output-window";
import {
  ControlTransportError,
  readTranscriptEvidence,
  readOutputWindow,
  type HttpCanonicalControlClient,
  type JobStreamEvent,
  type JobStreamSubscription,
} from "@/components/ocg/runtime/control-client";
import {
  ContractError,
  type ControlJob,
  type ControlJobDetails,
  type ControlOutput,
  type ControlProviderResponse,
  type ControlToolCall,
} from "@/components/ocg/contracts";
import { useI18n, type I18nKey, type TranslateFn } from "@/components/ocg/i18n";

type StreamState = "loading" | "live" | "reconnecting" | "settled" | "stream_error" | "closed";

type Settlement = {
  state: "unsettled" | "completed" | "failed" | "cancelled" | "unknown";
  settled_at_us: number | null;
  stop_confirmed_at_us: number | null;
};

function formatMicros(micros: number | null): string {
  return micros === null ? "—" : new Date(micros / 1000).toLocaleString();
}

function failureText(t: TranslateFn, failure: { code: string; scope: "read" | "stream" }): string {
  if (failure.code === "job_not_found") return t("job.failure.notFound");
  if (failure.code === "unauthenticated") return t("job.failure.unauthenticated");
  if (failure.code === "contract") return t("job.failure.contract");
  if (failure.code === "unreachable") return t("job.failure.unreachable");
  return failure.scope === "stream"
    ? t("job.failure.stream", { code: failure.code })
    : t("job.failure.read", { code: failure.code });
}

const STATE_KEYS: Record<string, I18nKey> = {
  loading: "job.state.loading",
  unavailable: "job.state.unavailable",
  unsettled: "job.state.unsettled",
  completed: "state.completed",
  failed: "state.failed",
  cancelled: "state.cancelled",
  unknown: "state.unknown",
  live: "job.stream.live",
  reconnecting: "job.stream.reconnecting",
  settled: "state.settled",
  stream_error: "job.stream.error",
  closed: "job.stream.closed",
};

const TONE: Record<string, string> = {
  loading: "bg-muted text-muted-foreground",
  unavailable: "bg-muted text-muted-foreground",
  unsettled: "bg-muted text-muted-foreground",
  completed: "bg-primary/10 text-primary",
  failed: "bg-destructive/10 text-destructive",
  cancelled: "bg-muted text-muted-foreground",
  unknown: "bg-destructive/10 text-destructive",
};

/**
 * One Job as the Control Plane records it: its stored outputs, its actual
 * output stream, and its actual outcome. Nothing here is simulated; the
 * stream is the stored history continued live, and a reconnect replays
 * from the last output this view processed, never twice.
 *
 * State retains only the latest resident output window (see
 * `./output-window`): every accepted output is appended once and the window
 * is trimmed from the front, so neither the retained rows nor the work per
 * event grows with the Job. The processed cursor is the highest stored
 * sequence and is independent of the visible rows.
 */
export function JobWatch(props: {
  client: HttpCanonicalControlClient;
  jobId: string;
  // The inspector shows the Job's record around its answer; chat shows the
  // answer first and keeps the record one click away.
  presentation?: "inspector" | "chat";
  bare?: boolean;
  onStateChange?: (
    state: "unsettled" | "completed" | "failed" | "cancelled" | "unknown",
    job: ControlJob | null,
  ) => void;
}) {
  // Retry re-runs the whole load (record, stored history, stream) from scratch.
  const [attempt, setAttempt] = useState(0);
  return (
    <JobWatchBody key={attempt} {...props} onRetry={() => setAttempt((current) => current + 1)} />
  );
}

function JobWatchBody({
  client,
  jobId,
  presentation = "inspector",
  bare = false,
  onStateChange,
  onRetry,
}: {
  client: HttpCanonicalControlClient;
  jobId: string;
  presentation?: "inspector" | "chat";
  bare?: boolean;
  onStateChange?: (
    state: "unsettled" | "completed" | "failed" | "cancelled" | "unknown",
    job: ControlJob | null,
  ) => void;
  onRetry: () => void;
}) {
  const { t } = useI18n();
  const [details, setDetails] = useState<ControlJobDetails | null>(null);
  const [statusFailure, setStatusFailure] = useState(false);
  const [job, setJob] = useState<ControlJob | null>(null);
  const [outputs, setOutputs] = useState<ControlOutput[]>([]);
  // Highest stored sequence processed so far: the reconnect/replay cursor,
  // independent of how many rows the resident window retains.
  const [storedTotal, setStoredTotal] = useState(0);
  const [storedOpen, setStoredOpen] = useState(false);
  // Automatically retained rows; earlier outputs load only on explicit request.
  const windowRef = useRef(JOB_WATCH_RESIDENT_OUTPUTS);
  const [loadingEarlier, setLoadingEarlier] = useState(false);
  const [earlierFailure, setEarlierFailure] = useState<{ code: string; scope: "read" } | null>(
    null,
  );
  const earlierLoad = useRef<AbortController | null>(null);
  const [tools, setTools] = useState<ControlToolCall[]>([]);
  const [evidenceFailure, setEvidenceFailure] = useState<string>();
  const [responses, setResponses] = useState<ControlProviderResponse[]>([]);
  const [settlement, setSettlement] = useState<Settlement | null>(null);
  const [stream, setStream] = useState<StreamState>("loading");
  const [failure, setFailure] = useState<{ code: string; scope: "read" | "stream" } | null>(null);
  // The objective and attachments are read from the Job's frozen input; the record names it only by identity.
  const [input, setInput] = useState<
    { objective: string | null; attachments: string[] } | { error: string } | null
  >(null);
  const [stop, setStop] = useState<string | null>(null);
  const [stopBusy, setStopBusy] = useState(false);
  const [unknownReason, setUnknownReason] = useState<string | null>(null);
  // One settlement arrives as both a "job" and a "settlement" event; the
  // unknown reason is refreshed on whichever arrives first, at most once
  // per watched Job.
  const unknownReasonRefreshed = useRef(false);
  useEffect(
    () => () => {
      earlierLoad.current?.abort();
      earlierLoad.current = null;
    },
    [client, jobId],
  );
  useEffect(() => {
    unknownReasonRefreshed.current = false;
  }, [jobId]);

  useEffect(() => {
    if (!job && !settlement) return;
    onStateChange?.(settlement?.state ?? "unsettled", job);
  }, [job, settlement, onStateChange]);

  useEffect(() => {
    let alive = true;
    let subscription: JobStreamSubscription | null = null;
    // The application consumes each stored output once: everything the
    // page reads and everything the stream replays passes this cursor.
    let processed = 0;
    let historyComplete = false;
    let replies: ControlProviderResponse[] = [];
    let currentJob: ControlJob | null = null;
    let refreshingStatus = false;
    let completed = false;
    let reconciled = false;
    const evidenceController = new AbortController();

    // `read` failures are of the Job's record or stored history; `stream` ones of the live stream.
    const fail = (error: unknown, scope: "read" | "stream") => {
      if (error instanceof ControlTransportError) setFailure({ code: error.code, scope });
      else if (error instanceof ContractError) setFailure({ code: "contract", scope });
      else setFailure({ code: "unreachable", scope });
      setStream("closed");
    };

    let reloading = false;
    let reloadQueued = false;

    const requestReload = () => {
      if (!alive) return;
      if (reloading) {
        reloadQueued = true;
        return;
      }
      reloading = true;
      const completeWindow = historyComplete && processed <= windowRef.current;
      const catchUp = completeWindow && completed && !reconciled;
      if (catchUp) reconciled = true;
      readTranscriptEvidence(client, jobId, {
        signal: evidenceController.signal,
        previous: completeWindow ? replies : [],
        settled: catchUp,
        readReplies: completeWindow,
      })
        .then(
          (evidence) => {
            if (!alive) return;
            if (evidence.tools) setTools(evidence.tools);
            if (completeWindow && processed <= windowRef.current)
              replies = evidence.responses ?? replies;
            else replies = [];
            setResponses(replies);
            setEvidenceFailure(evidence.error);
          },
          (error) => {
            if (alive && !evidenceController.signal.aborted)
              setEvidenceFailure(
                `Transcript evidence unavailable; showing stored output. ${error instanceof Error ? error.message : String(error)}`,
              );
          },
        )
        .finally(() => {
          reloading = false;
          if (reloadQueued && alive) {
            reloadQueued = false;
            requestReload();
          }
        });
    };

    const refreshStatus = async () => {
      if (!alive || refreshingStatus) return;
      refreshingStatus = true;
      try {
        const record = await client.readJobDetails(jobId, evidenceController.signal);
        if (!alive) return;
        currentJob = currentJob ? reconcileJob(currentJob, record) : record;
        setJob(currentJob);
        setDetails(record);
        setStatusFailure(false);
        setUnknownReason(record.unknown_reason);
        completed = currentJob.settlement.state === "completed";
        if (record.activity.last_output_seq > processed) {
          const window = await readOutputWindow(client, jobId, {
            signal: evidenceController.signal,
          });
          if (!alive) return;
          processed = Math.max(processed, window.latestSeq);
          setStoredTotal(processed);
          setOutputs((existing) => mergeOutputWindow(existing, window.outputs, windowRef.current));
        }
        if (currentJob.settlement.state !== "unsettled") {
          setSettlement({
            ...currentJob.settlement,
            stop_confirmed_at_us: currentJob.stop_confirmed_at_us,
          });
          setStream("settled");
          requestReload();
        }
      } catch {
        if (alive) setStatusFailure(true);
      } finally {
        refreshingStatus = false;
      }
    };

    let settledAtFirstRead = false;
    const refreshUnknownReason = () => {
      client.readJob(jobId).then(
        (record) => {
          if (alive) setUnknownReason(record.unknown_reason ?? null);
        },
        () => {},
      );
    };
    const refreshUnknownReasonOnce = () => {
      if (unknownReasonRefreshed.current) return;
      unknownReasonRefreshed.current = true;
      refreshUnknownReason();
    };

    const listener = (event: JobStreamEvent) => {
      if (!alive) return;
      // Anything the server sent proves the connection is back.
      setStream((current) => (current === "reconnecting" ? "live" : current));
      if (event.type === "job") {
        currentJob = currentJob ? reconcileJob(currentJob, event.job) : event.job;
        setJob(currentJob);
        completed = currentJob.settlement.state === "completed";
        if (currentJob !== event.job) return;
        if (event.job.settlement.state !== "unsettled") {
          setSettlement({
            state: event.job.settlement.state,
            settled_at_us: event.job.settlement.settled_at_us,
            stop_confirmed_at_us: event.job.stop_confirmed_at_us,
          });
          void refreshStatus();
          requestReload();
          if (event.job.settlement.state === "unknown" && !settledAtFirstRead)
            refreshUnknownReasonOnce();
        }
      } else if (event.type === "output") {
        if (event.output.seq <= processed) return;
        processed = event.output.seq;
        setStoredTotal(event.output.seq);
        const incoming = event.output;
        setOutputs((existing) => mergeOutputWindow(existing, [incoming], windowRef.current));
        requestReload();
      } else if (event.type === "settlement") {
        completed = event.settlement.state === "completed";
        setSettlement({
          state: event.settlement.state,
          settled_at_us: event.settlement.settled_at_us,
          stop_confirmed_at_us: event.stop_confirmed_at_us,
        });
        setStream("settled");
        void refreshStatus();
        requestReload();
        if (event.settlement.state === "unknown" && !settledAtFirstRead) refreshUnknownReasonOnce();
      } else if (event.type === "stream_error") {
        setFailure({ code: event.error.code, scope: "stream" });
        setStream("stream_error");
        void refreshStatus();
      }
    };

    const unsubscribeCatalog = client.subscribeCatalog(
      () => {
        requestReload();
        void refreshStatus();
      },
      () => {
        if (alive) setStatusFailure(true);
      },
    );

    (async () => {
      try {
        const record = await client.readJobDetails(jobId, evidenceController.signal);
        if (!alive) return;
        currentJob = record;
        setJob(record);
        setDetails(record);
        completed = record.settlement.state === "completed";
        setUnknownReason(record.unknown_reason ?? null);
        // The record is the Job's own truth, whether or not the stream ever connects.
        if (record.settlement.state !== "unsettled") {
          settledAtFirstRead = true;
          setSettlement({
            state: record.settlement.state,
            settled_at_us: record.settlement.settled_at_us,
            stop_confirmed_at_us: record.stop_confirmed_at_us,
          });
        }
      } catch (error) {
        if (alive) fail(error, "read");
        return;
      }
      client.readJobInput(jobId).then(
        (frozen) => {
          if (alive) setInput({ objective: frozen.objective, attachments: frozen.attachments });
        },
        (error: unknown) => {
          if (alive)
            setInput({
              error: error instanceof ControlTransportError ? error.code : "unreachable",
            });
        },
      );
      requestReload();
      try {
        const window = await readOutputWindow(client, jobId, { signal: evidenceController.signal });
        if (!alive) return;
        processed = window.latestSeq;
        setStoredTotal(processed);
        setOutputs(window.outputs);
      } catch (error) {
        if (alive) fail(error, "read");
        return;
      }
      historyComplete = true;
      requestReload();
      try {
        subscription = client.subscribeJobEvents(jobId, listener, {
          afterSeq: processed,
          onReconnect: () => {
            if (alive) setStream("reconnecting");
          },
        });
        if (!alive) {
          subscription.close();
          return;
        }
        setStream("live");
        subscription.closed.then(
          (end) => {
            if (!alive) return;
            void refreshStatus();
            if (end === "settled") {
              setStream("settled");
              requestReload();
            } else if (end === "stream_error") {
              setStream("stream_error");
            } else {
              setStream("closed");
            }
          },
          // A stream that cannot go on, such as an ended session, is a failure to show, not a lost rejection.
          (error: unknown) => {
            if (alive) fail(error, "stream");
          },
        );
      } catch (error) {
        if (alive) fail(error, "stream");
      }
    })();

    return () => {
      alive = false;
      unsubscribeCatalog();
      subscription?.close();
      evidenceController.abort();
    };
  }, [client, jobId]);

  async function requestStop() {
    setStopBusy(true);
    setStop(null);
    try {
      const result = await client.stopConversation(job?.conversation ?? jobId);
      setStop(result.status === "requested" ? t("job.stop.requested") : t("job.stop.settled"));
    } catch (error) {
      if (error instanceof ControlTransportError) {
        setStop(
          error.code === "not_stoppable"
            ? t("job.stop.notStoppable")
            : t("job.stop.refused", { code: error.code }),
        );
      } else if (error instanceof ContractError) {
        setStop(t("job.stop.contract"));
      } else {
        setStop(t("job.stop.unreachable"));
      }
    } finally {
      setStopBusy(false);
    }
  }

  // Until the record is read there is no settlement to claim.
  const state = settlement?.state ?? (job ? "unsettled" : null);
  const badge = state ?? (failure ? "unavailable" : "loading");

  // Earlier stored outputs are durable on the server, not in state. Loading
  // them is an explicit walk from the first stored sequence, merged by `seq`
  // with whatever the live stream appended meanwhile.
  async function loadEarlierOutputs() {
    if (earlierLoad.current) return;
    const controller = new AbortController();
    earlierLoad.current = controller;
    const nextWindow = windowRef.current + JOB_WATCH_WINDOW_STEP;
    setLoadingEarlier(true);
    setEarlierFailure(null);
    try {
      const { outputs: walked } = await readOutputWindow(client, jobId, {
        beforeSeq: outputs[0]?.seq ?? 1,
        limit: JOB_WATCH_WINDOW_STEP,
        signal: controller.signal,
      });
      if (controller.signal.aborted) return;
      windowRef.current = nextWindow;
      const incoming = walked;
      setOutputs((existing) => mergeOutputWindow([...outputs, ...existing], incoming, nextWindow));
      if ((walked[0]?.seq ?? outputs[0]?.seq) === 1) {
        const evidence = await readTranscriptEvidence(client, jobId, { signal: controller.signal });
        if (!controller.signal.aborted) {
          setResponses(evidence.responses ?? []);
          setEvidenceFailure(evidence.error);
        }
      }
    } catch (error) {
      if (!controller.signal.aborted) {
        setEarlierFailure({
          code:
            error instanceof ControlTransportError
              ? error.code
              : error instanceof ContractError
                ? "contract"
                : "unreachable",
          scope: "read",
        });
      }
    } finally {
      if (earlierLoad.current === controller) {
        earlierLoad.current = null;
        setLoadingEarlier(false);
      }
    }
  }

  const objective = input !== null && "objective" in input ? input.objective : null;
  const attachments = input !== null && "attachments" in input ? input.attachments : undefined;
  const streaming = stream === "live" || stream === "reconnecting";
  const turns = useMemo(
    () =>
      jobTranscript(
        jobId,
        objective,
        outputs,
        settlement,
        streaming,
        attachments,
        tools,
        job?.previous === null,
        responses,
      ),
    [
      jobId,
      objective,
      outputs,
      settlement,
      streaming,
      attachments,
      tools,
      job?.previous,
      responses,
    ],
  );
  const evictedOutputs = hasEvictedOutputs(storedTotal, outputs.length);

  const recordFacts = (
    <dl className="grid gap-x-6 gap-y-1 text-xs text-muted-foreground sm:grid-cols-2">
      <div className="grid grid-cols-[5rem_minmax(0,1fr)] gap-x-3">
        <dt>{t("job.fact.created")}</dt>
        <dd className="min-w-0 break-all">{formatMicros(job?.created_at_us ?? null)}</dd>
      </div>
      <div className="grid grid-cols-[5rem_minmax(0,1fr)] gap-x-3">
        <dt>{t("job.fact.started")}</dt>
        <dd className="min-w-0 break-all">{formatMicros(job?.started_at_us ?? null)}</dd>
      </div>
      <div className="grid grid-cols-[5rem_minmax(0,1fr)] gap-x-3">
        <dt>{t("job.fact.updated")}</dt>
        <dd className="min-w-0 break-all">{formatMicros(job?.updated_at_us ?? null)}</dd>
      </div>
      <div className="grid grid-cols-[5rem_minmax(0,1fr)] gap-x-3">
        <dt>{t("job.fact.settled")}</dt>
        <dd className="min-w-0 break-all">{formatMicros(settlement?.settled_at_us ?? null)}</dd>
      </div>
      {job ? (
        <div className="grid grid-cols-[5rem_minmax(0,1fr)] gap-x-3">
          <dt>{t("job.fact.input")}</dt>
          <dd className="min-w-0 break-all">
            <code className="font-mono">{job.input}</code>
          </dd>
        </div>
      ) : null}
      {job?.previous ? (
        <div className="grid grid-cols-[5rem_minmax(0,1fr)] gap-x-3">
          <dt>{t("job.fact.previous")}</dt>
          <dd className="min-w-0 break-all">
            <code className="font-mono">{job.previous}</code>
          </dd>
        </div>
      ) : null}
      {settlement?.state === "unknown" ? (
        <div className="grid grid-cols-[5rem_minmax(0,1fr)] gap-x-3">
          <dt>{t("job.fact.stopConfirmed")}</dt>
          <dd className="min-w-0 break-all">{formatMicros(settlement.stop_confirmed_at_us)}</dd>
        </div>
      ) : null}
      {settlement?.state === "unknown" ? (
        <div className="grid grid-cols-[5rem_minmax(0,1fr)] gap-x-3">
          <dt>{t("job.fact.unknownReason")}</dt>
          <dd className="min-w-0 break-all">{unknownReason ?? "—"}</dd>
        </div>
      ) : null}
    </dl>
  );
  const storedOutputs = job ? (
    <details
      onToggle={(event) => {
        if (event.target === event.currentTarget) setStoredOpen(event.currentTarget.open);
      }}
      className="flex flex-col gap-2"
    >
      <summary className="cursor-pointer text-xs font-semibold tracking-widest uppercase text-muted-foreground">
        {evictedOutputs
          ? t("job.storedOutputsWindowed", { shown: outputs.length, total: storedTotal })
          : t("job.storedOutputs", { count: outputs.length })}
      </summary>
      {storedOpen && (
        <>
          {evictedOutputs ? (
            <div className="mt-2">
              <Button
                variant="outline"
                size="sm"
                onClick={() => void loadEarlierOutputs()}
                disabled={loadingEarlier}
              >
                {loadingEarlier ? t("common.loading") : t("job.loadEarlierOutputs")}
              </Button>
              {earlierFailure ? (
                <DismissibleNotice
                  message={failureText(t, earlierFailure)}
                  className="mt-2 text-sm text-destructive"
                >
                  {failureText(t, earlierFailure)}
                </DismissibleNotice>
              ) : null}
            </div>
          ) : null}
          <ol className="mt-2 flex flex-col gap-2">
            {outputs.map((output) => (
              <li
                key={output.seq}
                className="flex flex-col gap-1.5 rounded-md border border-border p-3"
              >
                <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
                  <code className="font-mono">#{output.seq}</code>
                  <span className="rounded-full bg-muted px-2 py-0.5 font-semibold tracking-widest uppercase">
                    {output.kind}
                  </span>
                  <span>{formatMicros(output.created_at_us)}</span>
                  {output.goal_reached === null ? null : (
                    <span>
                      {output.goal_reached ? t("job.goalReached") : t("job.goalNotReached")}
                    </span>
                  )}
                </div>
                {output.summary ? <p className="text-sm">{output.summary}</p> : null}
                {output.code ? (
                  <pre className="ocg-subtle-scrollbar overflow-x-auto whitespace-pre-wrap rounded-md bg-muted p-3 font-mono text-xs">
                    {output.code}
                  </pre>
                ) : null}
                {output.value === null || output.value === undefined ? null : (
                  <pre className="ocg-subtle-scrollbar overflow-x-auto whitespace-pre-wrap rounded-md bg-muted p-3 font-mono text-xs">
                    {JSON.stringify(output.value, null, 2)}
                  </pre>
                )}
              </li>
            ))}
          </ol>
        </>
      )}
    </details>
  ) : null;
  const answer = job ? (
    <>
      {evidenceFailure && (
        <DismissibleNotice message={evidenceFailure} className="text-xs text-destructive">
          {evidenceFailure}
        </DismissibleNotice>
      )}
      <JobTranscript
        turns={turns}
        userLabel={presentation === "chat" ? t("job.you") : t("job.objective")}
        attachmentContentUrl={(id) => client.attachmentContentUrl(id)}
      />
      {input !== null && "error" in input ? (
        <p className="text-xs text-muted-foreground">
          {t("job.objectiveUnreadable", { error: input.error })}
        </p>
      ) : null}
    </>
  ) : null;

  return (
    <section
      className={
        bare
          ? "flex flex-col gap-4 text-card-foreground"
          : "flex flex-col gap-4 rounded-lg border border-border bg-card p-5 text-card-foreground"
      }
    >
      <header className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex min-w-0 flex-col gap-1">
          <h2 className="break-all font-heading text-lg">{job?.name ?? jobId}</h2>
          <p className="break-all text-xs text-muted-foreground">
            <code className="font-mono">{jobId}</code>
            {job ? ` · ${job.execution_mode}` : ""}
            {job?.assigned_executor ? ` · ${job.assigned_executor}` : ""}
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <span
            className={`rounded-full px-3 py-1 text-xs font-semibold tracking-widest uppercase ${TONE[badge] ?? TONE.unsettled}`}
          >
            {t(STATE_KEYS[badge] ?? "state.unknown")}
          </span>
          <span className="rounded-full bg-muted px-3 py-1 text-xs font-semibold tracking-widest uppercase text-muted-foreground">
            {t(STATE_KEYS[stream] ?? "state.unknown")}
          </span>
          <Button
            variant="outline"
            size="sm"
            onClick={() => void requestStop()}
            disabled={stopBusy || (state !== "unsettled" && !taskPlanActive(details?.task_plan))}
          >
            {stopBusy ? t("job.requestingStop") : t("job.requestStop")}
          </Button>
        </div>
      </header>
      <TaskStatus plan={details?.task_plan} />
      {statusFailure && (
        <p className="text-sm text-destructive">{t("job.activity.statusFailed")}</p>
      )}
      {stop ? <p className="text-sm text-muted-foreground">{stop}</p> : null}
      {failure ? (
        <DismissibleNotice message={failureText(t, failure)} className="text-sm text-destructive">
          <p>{failureText(t, failure)}</p>
          <Button variant="outline" size="sm" onClick={onRetry}>
            {t("common.retry")}
          </Button>
        </DismissibleNotice>
      ) : null}
      {presentation === "chat" ? (
        <>
          {answer}
          <details className="flex flex-col gap-3">
            <summary className="cursor-pointer text-xs font-semibold tracking-widest uppercase text-muted-foreground">
              {t("job.record")}
            </summary>
            <div className="mt-3 flex flex-col gap-4">
              {recordFacts}
              {storedOutputs}
            </div>
          </details>
        </>
      ) : (
        <>
          {recordFacts}
          {answer}
          {storedOutputs}
        </>
      )}
    </section>
  );
}
