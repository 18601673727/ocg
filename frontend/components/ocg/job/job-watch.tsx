"use client";

import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import { useEffect, useMemo, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import { TaskStatus } from "./task-status";
import {
  EXECUTION_PHASE_KEYS,
  executionDetail,
  nativeExecutionStatus,
  reconcileJob,
} from "../runtime/job-status";
import { JobTranscript } from "@/components/ocg/chat/job-transcript";
import { jobTranscript, outputText } from "@/components/ocg/chat/job-transcript-model";
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
  type ControlExecutionPhase,
  type ControlJob,
  type ControlJobDetails,
  type ControlNativeStopRequest,
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

function OutputContent({ output }: { output: ControlOutput }) {
  const { t } = useI18n();
  const [open, setOpen] = useState(false);
  const body = outputText(output);
  const value = output.value;
  let structured = value;
  if (body !== null && value !== null && typeof value === "object" && !Array.isArray(value)) {
    const fields = Object.fromEntries(Object.entries(value).filter(([key]) => key !== "text"));
    structured = Object.keys(fields).length > 0 ? fields : null;
  }

  return (
    <>
      {output.summary && output.summary !== body ? (
        <p className="whitespace-pre-wrap break-words text-sm">{output.summary}</p>
      ) : null}
      {body ? <p className="whitespace-pre-wrap break-words text-sm">{body}</p> : null}
      {output.code ? (
        <pre className="ocg-subtle-scrollbar overflow-x-auto whitespace-pre-wrap rounded-md bg-muted p-3 font-mono text-xs">
          {output.code}
        </pre>
      ) : null}
      {structured !== null && structured !== undefined ? (
        <details onToggle={(event) => setOpen(event.currentTarget.open)}>
          <summary className="cursor-pointer text-xs text-muted-foreground">
            {t("job.structuredData")}
          </summary>
          {open && (
            <pre className="ocg-subtle-scrollbar mt-1 overflow-x-auto whitespace-pre-wrap break-words rounded-md bg-muted p-3 font-mono text-xs">
              {JSON.stringify(structured, null, 2)}
            </pre>
          )}
        </details>
      ) : null}
    </>
  );
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

const STREAM_KEYS: Record<StreamState, I18nKey> = {
  loading: "job.state.loading",
  live: "job.stream.live",
  reconnecting: "job.stream.reconnecting",
  settled: "state.settled",
  stream_error: "job.stream.error",
  closed: "job.stream.closed",
};

const TONE = { loading: "bg-muted text-muted-foreground" };

type StopState = "idle" | "sending" | "locked";
type StopNotice = { message: string; tone: "status" | "warning" | "error" };

function classifyStopFailure(t: TranslateFn, cause: unknown): StopNotice & { locked: boolean } {
  if (cause instanceof ContractError) {
    return { message: t("job.stop.contract"), tone: "error", locked: true };
  }
  if (cause instanceof ControlTransportError) {
    if (cause.code === "connection_lost") {
      return { message: t("job.stop.uncertainCommit"), tone: "warning", locked: true };
    }
    switch (cause.status) {
      case 400:
        return { message: t("job.stop.invalid"), tone: "warning", locked: false };
      case 401:
      case 403:
        return { message: t("job.stop.unauthorized"), tone: "warning", locked: false };
      case 404:
        return { message: t("job.stop.notFound"), tone: "warning", locked: false };
      case 409:
        if (cause.code === "job_not_running")
          return { message: t("job.stop.notRunning"), tone: "warning", locked: false };
        if (cause.code === "job_already_ended")
          return { message: t("job.stop.alreadyEnded"), tone: "warning", locked: false };
        return { message: t("job.stop.conflict"), tone: "warning", locked: false };
      case 503:
        if (cause.code === "mutable_job_outcome_unknown")
          return { message: t("job.stop.uncertainCommit"), tone: "warning", locked: true };
        return { message: t("job.stop.transient"), tone: "warning", locked: false };
      default:
        return {
          message: t("job.stop.failed", { code: cause.code }),
          tone: "error",
          locked: false,
        };
    }
  }
  return { message: t("job.stop.failed", { code: "unreachable" }), tone: "error", locked: false };
}

const PHASE_BADGE = {
  unsubmitted: "bg-muted text-muted-foreground",
  queued: "bg-muted text-muted-foreground",
  reserved: "bg-primary/10 text-primary",
  running: "bg-primary/10 text-primary",
  stop_requested: "bg-amber-500/15 text-amber-700 dark:text-amber-300",
  unknown: "bg-destructive/10 text-destructive",
  completed: "bg-primary/10 text-primary",
  failed: "bg-destructive/10 text-destructive",
  cancelled: "bg-muted text-muted-foreground",
} as const satisfies Record<ControlExecutionPhase, string>;

export function JobWatch(props: {
  client: HttpCanonicalControlClient;
  jobId: string;
  presentation?: "inspector" | "chat";
  bare?: boolean;
  onStateChange?: (
    state: "unsettled" | "completed" | "failed" | "cancelled" | "unknown",
    job: ControlJob | null,
  ) => void;
}) {
  const [attempt, setAttempt] = useState(0);
  return (
    <JobWatchBody
      key={`${props.jobId}:${attempt}`}
      {...props}
      onRetry={() => setAttempt((current) => current + 1)}
    />
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
  const storedTotal = outputs.at(-1)?.seq ?? 0;
  const [storedOpen, setStoredOpen] = useState(false);
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
  const [input, setInput] = useState<
    { objective: string | null; attachments: string[] } | { error: string } | null
  >(null);
  const unknownReason = details?.unknown_reason ?? null;
  const reloadRef = useRef<() => void>(() => {});
  const refreshRef = useRef<() => void>(() => {});
  const stopRequest = useRef<AbortController | null>(null);
  const [stopState, setStopState] = useState<StopState>("idle");
  const [stopOutcome, setStopOutcome] = useState<ControlNativeStopRequest | null>(null);
  const [stopNotice, setStopNotice] = useState<StopNotice | null>(null);
  const statusReads = useRef(0);
  const [readCount, setReadCount] = useState(0);
  const [stopLockedAt, setStopLockedAt] = useState<number | null>(null);
  const watchRef = useRef({
    evidenceRevision: 0,
    processed: 0,
    historyComplete: false,
    completed: false,
    reconciled: false,
    replies: [] as ControlProviderResponse[],
  });

  useEffect(
    () => () => {
      earlierLoad.current?.abort();
      earlierLoad.current = null;
      stopRequest.current?.abort();
      stopRequest.current = null;
    },
    [client, jobId],
  );
  useEffect(() => {
    if (!job && !settlement) return;
    onStateChange?.(settlement?.state ?? "unsettled", job);
  }, [job, settlement, onStateChange]);

  useEffect(() => {
    let alive = true;
    let subscription: JobStreamSubscription | null = null;
    const watch = watchRef.current;
    watch.processed = 0;
    watch.historyComplete = false;
    watch.completed = false;
    watch.reconciled = false;
    watch.replies = [];
    windowRef.current = JOB_WATCH_RESIDENT_OUTPUTS;
    let currentJob: ControlJob | null = null;
    let refreshingStatus = true;
    let statusQueued = false;
    const evidenceController = new AbortController();

    const fail = (error: unknown, scope: "read" | "stream") => {
      if (error instanceof ControlTransportError) setFailure({ code: error.code, scope });
      else if (error instanceof ContractError) setFailure({ code: "contract", scope });
      else setFailure({ code: "unreachable", scope });
      setStream("closed");
    };

    let reloading = false;
    let reloadQueued = false;
    let retryAfter = 0;
    let retryTimer: ReturnType<typeof setTimeout> | undefined;

    const invalidateReplies = () => {
      watch.evidenceRevision++;
      watch.reconciled = false;
      watch.replies = [];
      setResponses([]);
    };

    const requestReload = () => {
      if (!alive) return;
      if (reloading) {
        reloadQueued = true;
        return;
      }
      const delay = retryAfter - Date.now();
      if (delay > 0) {
        retryTimer ??= setTimeout(() => {
          retryTimer = undefined;
          requestReload();
        }, delay);
        return;
      }
      reloading = true;
      const baseRevision = watch.evidenceRevision;
      const completeWindow = watch.historyComplete && watch.processed <= windowRef.current;
      const catchUp = completeWindow && watch.completed && !watch.reconciled;
      readTranscriptEvidence(client, jobId, {
        signal: evidenceController.signal,
        previous: completeWindow ? watch.replies : [],
        settled: catchUp,
        readReplies: completeWindow,
      })
        .then(
          (evidence) => {
            if (!alive) return;
            retryAfter = evidence.error ? Date.now() + 2000 : 0;
            const windowCompleteNow = watch.historyComplete && watch.processed <= windowRef.current;
            if (watch.evidenceRevision !== baseRevision || (!completeWindow && windowCompleteNow)) {
              reloadQueued = true;
              return;
            }
            if (catchUp) watch.reconciled = true;
            if (evidence.tools) setTools(evidence.tools);
            watch.replies =
              completeWindow && windowCompleteNow ? (evidence.responses ?? watch.replies) : [];
            setResponses(watch.replies);
            setEvidenceFailure(evidence.error);
          },
          (error) => {
            if (!alive || evidenceController.signal.aborted) return;
            retryAfter = Date.now() + 2000;
            const windowCompleteNow = watch.historyComplete && watch.processed <= windowRef.current;
            if (watch.evidenceRevision !== baseRevision || (!completeWindow && windowCompleteNow)) {
              reloadQueued = true;
              return;
            }
            if (catchUp) watch.reconciled = true;
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
    reloadRef.current = requestReload;
    const onOnline = () => {
      retryAfter = 0;
      if (retryTimer !== undefined) clearTimeout(retryTimer);
      retryTimer = undefined;
      requestReload();
    };
    if (typeof window !== "undefined") window.addEventListener("online", onOnline);

    const refreshStatus = async () => {
      if (!alive) return;
      if (refreshingStatus) {
        statusQueued = true;
        return;
      }
      refreshingStatus = true;
      try {
        const record = await client.readJobDetails(jobId, evidenceController.signal);
        if (!alive) return;
        const reconciled = currentJob ? reconcileJob(currentJob, record) : record;
        if (
          reconciled === record &&
          !(currentJob?.stop_confirmed_at_us != null && record.stop_confirmed_at_us === null)
        ) {
          currentJob = record;
          setDetails(record);
        } else {
          currentJob = reconciled === record ? currentJob : reconciled;
        }
        setJob(currentJob);
        setStatusFailure(false);
        statusReads.current += 1;
        setReadCount(statusReads.current);
        watch.completed = currentJob?.settlement.state === "completed";
        if (record.activity.last_output_seq > watch.processed) {
          const window = await readOutputWindow(client, jobId, {
            signal: evidenceController.signal,
          });
          if (!alive) return;
          const previousProcessed = watch.processed;
          if (window.latestSeq > previousProcessed) {
            const gap = (window.outputs[0]?.seq ?? 1) > previousProcessed + 1;
            const lostCompleteness =
              watch.historyComplete &&
              previousProcessed <= windowRef.current &&
              window.latestSeq > windowRef.current;
            if (gap) {
              earlierLoad.current?.abort();
              earlierLoad.current = null;
              setLoadingEarlier(false);
              windowRef.current = JOB_WATCH_RESIDENT_OUTPUTS;
            }
            if (gap || lostCompleteness) invalidateReplies();
            watch.processed = window.latestSeq;
            setOutputs((existing) =>
              gap ? window.outputs : mergeOutputWindow(existing, window.outputs, windowRef.current),
            );
            if (gap || lostCompleteness) requestReload();
          }
        }
        if (currentJob && currentJob.settlement.state !== "unsettled") {
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
        finishStatus();
      }
    };

    refreshRef.current = () => {
      void refreshStatus();
    };

    const finishStatus = () => {
      refreshingStatus = false;
      if (statusQueued && alive) {
        statusQueued = false;
        void refreshStatus();
      }
    };

    const listener = (event: JobStreamEvent) => {
      if (!alive) return;
      setStream((current) => (current === "reconnecting" ? "live" : current));
      if (event.type === "job") {
        currentJob = currentJob ? reconcileJob(currentJob, event.job) : event.job;
        setJob(currentJob);
        watch.completed = currentJob.settlement.state === "completed";
        if (currentJob !== event.job) return;
        if (event.job.settlement.state !== "unsettled") {
          setSettlement({
            state: event.job.settlement.state,
            settled_at_us: event.job.settlement.settled_at_us,
            stop_confirmed_at_us: event.job.stop_confirmed_at_us,
          });
          void refreshStatus();
          requestReload();
        }
      } else if (event.type === "output") {
        if (event.output.seq <= watch.processed) return;
        if (
          watch.historyComplete &&
          watch.processed <= windowRef.current &&
          event.output.seq > windowRef.current
        )
          invalidateReplies();
        watch.processed = event.output.seq;
        const incoming = event.output;
        setOutputs((existing) => mergeOutputWindow(existing, [incoming], windowRef.current));
        requestReload();
      } else if (event.type === "settlement") {
        if (currentJob) {
          const confirmed = currentJob.stop_confirmed_at_us ?? event.stop_confirmed_at_us;
          currentJob = {
            ...currentJob,
            settlement: event.settlement,
            stop_confirmed_at_us: confirmed,
            updated_at_us: Math.max(
              currentJob.updated_at_us,
              event.settlement.settled_at_us ?? 0,
              confirmed ?? 0,
            ),
          };
          setJob(currentJob);
        }
        watch.completed = event.settlement.state === "completed";
        setSettlement({
          state: event.settlement.state,
          settled_at_us: event.settlement.settled_at_us,
          stop_confirmed_at_us: currentJob?.stop_confirmed_at_us ?? event.stop_confirmed_at_us,
        });
        setStream("settled");
        void refreshStatus();
        requestReload();
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
        watch.completed = record.settlement.state === "completed";
        if (record.settlement.state !== "unsettled") {
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
        watch.processed = window.latestSeq;
        setOutputs(window.outputs);
      } catch (error) {
        if (alive) fail(error, "read");
        return;
      }
      watch.historyComplete = true;
      requestReload();
      try {
        subscription = client.subscribeJobEvents(jobId, listener, {
          afterSeq: watch.processed,
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
          (error: unknown) => {
            if (alive) fail(error, "stream");
          },
        );
      } catch (error) {
        if (alive) fail(error, "stream");
      }
    })().finally(finishStatus);

    return () => {
      alive = false;
      if (retryTimer !== undefined) clearTimeout(retryTimer);
      if (typeof window !== "undefined") window.removeEventListener("online", onOnline);
      if (reloadRef.current === requestReload) reloadRef.current = () => {};
      refreshRef.current = () => {};
      unsubscribeCatalog();
      subscription?.close();
      evidenceController.abort();
    };
  }, [client, jobId]);

  const phase: ControlExecutionPhase | null =
    settlement && settlement.state !== "unsettled"
      ? settlement.state
      : (job?.execution_phase ?? null);
  const execution = phase ? nativeExecutionStatus(phase, details ?? undefined) : null;
  const phaseNote = execution ? executionDetail(t, execution) : undefined;
  const badgeText = phase
    ? t(EXECUTION_PHASE_KEYS[phase])
    : t(failure ? "job.state.unavailable" : "job.state.loading");
  const badgeClass = phase ? PHASE_BADGE[phase] : TONE.loading;
  const stopPhase = phase === "reserved" || phase === "running";
  const stopAvailable = Boolean(details?.context);
  const stopRetry =
    stopState === "locked" &&
    stopOutcome === null &&
    stopLockedAt !== null &&
    readCount > stopLockedAt;

  async function requestStop() {
    const context = details?.context;
    if (!context || stopRequest.current) return;
    const controller = new AbortController();
    stopRequest.current = controller;
    setStopState("sending");
    setStopOutcome(null);
    setStopNotice(null);
    try {
      const stop = await client.stopNativeJob(context, jobId, controller.signal);
      if (controller.signal.aborted) return;
      setStopOutcome(stop.request);
      setStopState("locked");
    } catch (cause) {
      if (controller.signal.aborted) return;
      const failure = classifyStopFailure(t, cause);
      setStopNotice(failure);
      setStopLockedAt(failure.locked ? statusReads.current : null);
      setStopState(failure.locked ? "locked" : "idle");
    } finally {
      if (stopRequest.current === controller) stopRequest.current = null;
    }
    if (!controller.signal.aborted) refreshRef.current();
  }

  const stopMessage = (() => {
    if (stopNotice) return stopNotice;
    if (!stopOutcome) return null;
    if (phase === "completed")
      return { message: t("job.stop.completedFirst"), tone: "warning" as const };
    if (phase === "failed") return { message: t("job.stop.failedFirst"), tone: "warning" as const };
    if (phase === "unknown")
      return { message: t("job.stop.outcomeUnknown"), tone: "warning" as const };
    if (phase === "cancelled")
      return {
        message:
          stopOutcome === "ended_before_start"
            ? t("job.stop.endedBeforeStart")
            : t("job.stop.cancelledOutcome"),
        tone: "status" as const,
      };
    if (stopOutcome === "ended_before_start")
      return { message: t("job.stop.endedBeforeStart"), tone: "status" as const };
    return {
      message: t(stopOutcome === "requested" ? "job.stop.requested" : "job.stop.alreadyRequested"),
      tone: "warning" as const,
    };
  })();

  async function loadEarlierOutputs() {
    if (earlierLoad.current) return;
    const controller = new AbortController();
    earlierLoad.current = controller;
    const watch = watchRef.current;
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
      if (watch.processed - (outputs.at(-1)?.seq ?? 0) > windowRef.current) return;
      windowRef.current = nextWindow;
      const incoming = walked;
      setOutputs((existing) => mergeOutputWindow([...outputs, ...existing], incoming, nextWindow));
      if ((walked[0]?.seq ?? outputs[0]?.seq) === 1 && watch.processed <= windowRef.current) {
        reloadRef.current();
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
      } else if (earlierLoad.current === null) {
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
                <OutputContent output={output} />
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
        jobId={jobId}
        client={client}
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
          ? "flex flex-col text-card-foreground lg:h-full lg:min-h-0"
          : "flex flex-col rounded-lg border border-border bg-card p-5 text-card-foreground"
      }
    >
      <header
        className={
          bare
            ? "sticky top-0 z-10 flex min-h-16 shrink-0 flex-wrap items-center gap-3 border-b border-border bg-popover px-5 py-3 lg:static"
            : "flex flex-wrap items-center gap-3"
        }
      >
        <h2 className="min-w-0 flex-1 break-all font-heading text-lg">{job?.name ?? jobId}</h2>
        <div className="ml-auto flex flex-wrap items-center justify-end gap-2">
          <span
            className={`rounded-full px-3 py-1 text-xs font-semibold tracking-widest uppercase ${badgeClass}`}
          >
            {badgeText}
          </span>
          <span className="rounded-full bg-muted px-3 py-1 text-xs font-semibold tracking-widest uppercase text-muted-foreground">
            {t(STREAM_KEYS[stream])}
          </span>
        </div>
      </header>
      <div
        className={
          bare
            ? "ocg-subtle-scrollbar flex min-h-0 flex-col gap-4 p-5 lg:flex-1 lg:overflow-auto"
            : "mt-4 flex flex-col gap-4"
        }
      >
        <p className="break-all text-xs text-muted-foreground">
          <code className="font-mono">{jobId}</code>
          {job ? ` · ${job.execution_mode}` : ""}
          {job?.assigned_executor ? ` · ${job.assigned_executor}` : ""}
        </p>
        <TaskStatus plan={details?.task_plan} />
        {statusFailure && (
          <p className="text-sm text-destructive">{t("job.activity.statusFailed")}</p>
        )}
        {phaseNote ? (
          <p className="text-sm text-muted-foreground" role="status">
            {phaseNote}
          </p>
        ) : null}
        {stopPhase && stopAvailable && (stopState !== "locked" || stopRetry) ? (
          <div className="flex flex-wrap items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => void requestStop()}
              disabled={stopState === "sending"}
            >
              {stopState === "sending" ? t("job.stop.stopping") : t("job.stop.action")}
            </Button>
          </div>
        ) : null}
        {stopPhase && !stopAvailable ? (
          <p className="text-sm text-muted-foreground">{t("job.stop.unavailable")}</p>
        ) : null}
        {stopMessage ? (
          <DismissibleNotice
            message={stopMessage.message}
            tone={stopMessage.tone === "error" ? "error" : "warning"}
          >
            <p>{stopMessage.message}</p>
          </DismissibleNotice>
        ) : null}
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
      </div>
    </section>
  );
}
