"use client";

import { useEffect, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  ControlTransportError,
  type HttpCanonicalControlClient,
  type JobStreamEvent,
  type JobStreamSubscription,
} from "@/components/ocg/runtime/canonical-client";
import {
  ContractError,
  type ControlJob,
  type ControlOutput,
} from "@/components/ocg/contracts";

type StreamState = "loading" | "live" | "reconnecting" | "settled" | "stream_error" | "closed";

type Settlement = {
  state: "unsettled" | "completed" | "failed" | "cancelled" | "unknown";
  settled_at_us: number | null;
  stop_confirmed_at_us: number | null;
};

function formatMicros(micros: number | null): string {
  return micros === null ? "—" : new Date(micros / 1000).toLocaleString();
}

const TONE: Record<string, string> = {
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
 */
export function JobWatch({
  client,
  jobId,
}: {
  client: HttpCanonicalControlClient;
  jobId: string;
}) {
  const [job, setJob] = useState<ControlJob | null>(null);
  const [outputs, setOutputs] = useState<ControlOutput[]>([]);
  const [settlement, setSettlement] = useState<Settlement | null>(null);
  const [stream, setStream] = useState<StreamState>("loading");
  const [failure, setFailure] = useState<{ code: string; message: string } | null>(null);
  const [stop, setStop] = useState<string | null>(null);
  const [stopBusy, setStopBusy] = useState(false);

  useEffect(() => {
    let alive = true;
    let subscription: JobStreamSubscription | null = null;
    // The application consumes each stored output once: everything the
    // page reads and everything the stream replays passes this cursor.
    let processed = 0;

    const fail = (error: unknown) => {
      if (error instanceof ControlTransportError) setFailure({ code: error.code, message: error.message });
      else if (error instanceof ContractError) setFailure({ code: "contract", message: error.message });
      else setFailure({ code: "unreachable", message: "OCG could not be reached." });
      setStream("closed");
    };

    const listener = (event: JobStreamEvent) => {
      if (!alive) return;
      if (event.type === "job") {
        setJob(event.job);
        if (event.job.settlement.state !== "unsettled") {
          setSettlement({
            state: event.job.settlement.state,
            settled_at_us: event.job.settlement.settled_at_us,
            stop_confirmed_at_us: event.job.stop_confirmed_at_us,
          });
        }
      } else if (event.type === "output") {
        if (event.output.seq <= processed) return;
        processed = event.output.seq;
        setOutputs((existing) => [...existing, event.output]);
      } else if (event.type === "settlement") {
        setSettlement({
          state: event.settlement.state,
          settled_at_us: event.settlement.settled_at_us,
          stop_confirmed_at_us: event.stop_confirmed_at_us,
        });
        setStream("settled");
      } else if (event.type === "stream_error") {
        setFailure(event.error);
        setStream("stream_error");
      }
    };

    const open = async (): Promise<JobStreamSubscription> => {
      for (let attempt = 0; ; attempt++) {
        try {
          return client.subscribeJobEvents(
            jobId,
            listener,
            client.lastOutputSequence(jobId) === undefined
              ? {
                  afterSeq: processed,
                  onReconnect: () => {
                    if (alive) setStream("reconnecting");
                  },
                }
              : {
                  onReconnect: () => {
                    if (alive) setStream("reconnecting");
                  },
                },
          );
        } catch (error) {
          // A subscription of an earlier view is closing; wait for it.
          if (attempt >= 4 || !(error instanceof ControlTransportError) ||
            error.code !== "job_stream_already_open") {
            throw error;
          }
          await new Promise((resolve) => setTimeout(resolve, 200));
        }
      }
    };

    (async () => {
      try {
        const record = await client.readJob(jobId);
        if (!alive) return;
        setJob(record);
      } catch (error) {
        if (alive) fail(error);
        return;
      }
      // The stored history first, in pages and in order; the stream then
      // continues after the last output this view processed.
      try {
        for (;;) {
          const page = await client.readOutputs(jobId, { afterSeq: processed });
          if (!alive) return;
          for (const output of page.outputs) {
            if (output.seq > processed) {
              processed = output.seq;
              setOutputs((existing) => [...existing, output]);
            }
          }
          if (!page.has_more) break;
        }
      } catch (error) {
        if (alive) fail(error);
        return;
      }
      try {
        subscription = await open();
        if (!alive) {
          subscription.close();
          return;
        }
        setStream("live");
        void subscription.closed.then((end) => {
          if (!alive) return;
          if (end === "settled") setStream("settled");
          else if (end === "stream_error") setStream("stream_error");
          else setStream("closed");
        });
      } catch (error) {
        if (alive) fail(error);
      }
    })();

    return () => {
      alive = false;
      subscription?.close();
    };
  }, [client, jobId]);

  async function requestStop() {
    setStopBusy(true);
    setStop(null);
    try {
      const result = await client.stopJob(jobId);
      setStop(
        result.status === "requested"
          ? "The stop was requested of this Job's execution. Its record will say how it settled."
          : "The Job had already settled.",
      );
    } catch (error) {
      if (error instanceof ControlTransportError) {
        setStop(
          error.code === "not_stoppable"
            ? "This Job has no execution here to stop."
            : `The stop was refused (${error.code}).`,
        );
      } else if (error instanceof ContractError) {
        setStop("The stop answer did not match the control contract.");
      } else {
        setStop("OCG could not be reached.");
      }
    } finally {
      setStopBusy(false);
    }
  }

  const state = settlement?.state ?? "unsettled";

  return (
    <section className="flex flex-col gap-4 rounded-lg border border-border bg-card p-5 text-card-foreground">
      <header className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-col gap-1">
          <h2 className="font-heading text-lg">{job?.name ?? jobId}</h2>
          <p className="text-xs text-muted-foreground">
            <code className="font-geist-mono">{jobId}</code>
            {job ? ` · ${job.execution_mode}` : ""}
            {job?.assigned_executor ? ` · ${job.assigned_executor}` : ""}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <span className={`rounded-full px-3 py-1 text-xs font-semibold tracking-widest uppercase ${TONE[state]}`}>
            {state}
          </span>
          <span className="rounded-full bg-muted px-3 py-1 text-xs font-semibold tracking-widest uppercase text-muted-foreground">
            {stream}
          </span>
          <Button
            variant="outline"
            size="sm"
            onClick={() => void requestStop()}
            disabled={stopBusy || state !== "unsettled"}
          >
            {stopBusy ? "Requesting…" : "Request stop"}
          </Button>
        </div>
      </header>
      {stop ? <p className="text-sm text-muted-foreground">{stop}</p> : null}
      {failure ? (
        <p className="text-sm text-destructive">
          The stream failed ({failure.code}).
        </p>
      ) : null}
      <dl className="grid gap-x-6 gap-y-1 text-xs text-muted-foreground sm:grid-cols-2">
        <div className="flex justify-between gap-2">
          <dt>Created</dt>
          <dd>{formatMicros(job?.created_at_us ?? null)}</dd>
        </div>
        <div className="flex justify-between gap-2">
          <dt>Started</dt>
          <dd>{formatMicros(job?.started_at_us ?? null)}</dd>
        </div>
        <div className="flex justify-between gap-2">
          <dt>Updated</dt>
          <dd>{formatMicros(job?.updated_at_us ?? null)}</dd>
        </div>
        <div className="flex justify-between gap-2">
          <dt>Settled</dt>
          <dd>{formatMicros(settlement?.settled_at_us ?? null)}</dd>
        </div>
        {settlement?.state === "unknown" ? (
          <div className="flex justify-between gap-2">
            <dt>Stop confirmed</dt>
            <dd>{formatMicros(settlement.stop_confirmed_at_us)}</dd>
          </div>
        ) : null}
      </dl>
      {job?.input ? (
        <details className="text-xs text-muted-foreground">
          <summary className="cursor-pointer font-semibold tracking-widest uppercase">
            Objective
          </summary>
          <pre className="ocg-subtle-scrollbar mt-2 overflow-x-auto whitespace-pre-wrap rounded-md bg-muted p-3 font-geist-mono">
            {job.input}
          </pre>
        </details>
      ) : null}
      <div className="flex flex-col gap-2">
        <h3 className="text-xs font-semibold tracking-widest uppercase text-muted-foreground">
          Outputs ({outputs.length})
        </h3>
        <ol className="flex flex-col gap-2">
          {outputs.map((output) => (
            <li
              key={output.seq}
              className="flex flex-col gap-1.5 rounded-md border border-border p-3"
            >
              <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
                <code className="font-geist-mono">#{output.seq}</code>
                <span className="rounded-full bg-muted px-2 py-0.5 font-semibold tracking-widest uppercase">
                  {output.kind}
                </span>
                <span>{formatMicros(output.created_at_us)}</span>
                {output.goal_reached === null ? null : (
                  <span>{output.goal_reached ? "goal reached" : "goal not reached"}</span>
                )}
              </div>
              {output.summary ? <p className="text-sm">{output.summary}</p> : null}
              {output.code ? (
                <pre className="ocg-subtle-scrollbar overflow-x-auto whitespace-pre-wrap rounded-md bg-muted p-3 font-geist-mono text-xs">
                  {output.code}
                </pre>
              ) : null}
              {output.value === null || output.value === undefined ? null : (
                <pre className="ocg-subtle-scrollbar overflow-x-auto whitespace-pre-wrap rounded-md bg-muted p-3 font-geist-mono text-xs">
                  {JSON.stringify(output.value, null, 2)}
                </pre>
              )}
            </li>
          ))}
        </ol>
      </div>
    </section>
  );
}
