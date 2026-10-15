"use client";

import { useEffect, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import {
  ControlTransportError,
  type ControlSubmission,
  type HttpCanonicalControlClient,
} from "@/components/ocg/runtime/canonical-client";
import { ContractError } from "@/components/ocg/contracts";

type Phase = "editing" | "submitting" | "pending" | "uncertain" | "admitted" | "refused";

/** A fresh idempotency key names one submission intent, once. */
function makeKey(): string {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

// A refusal that says nothing was created may start a new intent; anything
// that may have created a Job keeps its key and is never submitted again.
function outcomeOf(error: unknown): "refused" | "uncertain" {
  if (error instanceof ControlTransportError) {
    if (error.status === 0) return "uncertain";
    if (error.status >= 400 && error.status < 500) return "refused";
    if (error.status === 503) return "refused";
    return "uncertain";
  }
  return "uncertain";
}

/** What Chat has already chosen: the form then asks only for the message. */
export type SubmissionTarget = {
  agentProfile: string;
  llmProfile: string;
  executor: string;
};

/**
 * Submits one Provider Job under one Idempotency-Key. The key survives
 * every retry and admission lookup of this intent: no timeout or pending
 * answer ever produces a second Job.
 */
export function JobSubmissionForm({
  client,
  onJob,
  target,
}: {
  client: HttpCanonicalControlClient;
  onJob: (jobId: string) => void;
  // Present in Chat: the Agent Profile, LLM target and Executor come from the
  // selector. Absent in the Jobs view: they are typed by identity.
  target?: SubmissionTarget | null;
}) {
  const chat = target !== undefined;
  const [llmProfile, setLlmProfile] = useState("");
  const [executor, setExecutor] = useState("");
  const [objective, setObjective] = useState("");
  const [name, setName] = useState("");
  const [phase, setPhase] = useState<Phase>("editing");
  const [message, setMessage] = useState<string | null>(null);
  const [key, setKey] = useState<string | null>(null);
  const keyRef = useRef<string | null>(null);
  // What the held key was submitted with. A retry replays exactly this, so a
  // later selection or edit can never turn one key into two requests.
  const requestRef = useRef<ControlSubmission | null>(null);
  const reconciliation = useRef<ReturnType<typeof setTimeout> | null>(null);
  const mounted = useRef(true);

  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
      if (reconciliation.current) clearTimeout(reconciliation.current);
    };
  }, []);

  const settled = phase === "submitting" || phase === "pending" || phase === "uncertain";

  function clearReconciliation() {
    if (reconciliation.current) clearTimeout(reconciliation.current);
    reconciliation.current = null;
  }

  function reconcile(held: string) {
    client
      .readAdmission(held)
      .then((admission) => {
        if (!mounted.current || keyRef.current !== held) return;
        if (admission.status === "admitted" && admission.job) {
          clearReconciliation();
          setPhase("admitted");
          setMessage(`${admission.job} is admitted and running.`);
          onJob(admission.job);
        } else if (admission.status === "uncertain") {
          clearReconciliation();
          setPhase("uncertain");
          setMessage("Whether this key created a Job is unknown. It is not submitted again.");
        } else if (admission.status === "not_found") {
          clearReconciliation();
          setPhase("refused");
          setMessage("This key created no Job, and none can be created with it any more.");
        } else {
          setMessage("Admission is still running under this key; it is not submitted again.");
          reconciliation.current = setTimeout(() => reconcile(held), 2000);
        }
      })
      .catch((error: unknown) => {
        if (!mounted.current || keyRef.current !== held) return;
        const code = error instanceof ControlTransportError ? error.code : "unreachable";
        setMessage(`The key could not be looked up (${code}); trying again.`);
        reconciliation.current = setTimeout(() => reconcile(held), 4000);
      });
  }

  async function submit(replay = false) {
    clearReconciliation();
    const held = keyRef.current ?? makeKey();
    keyRef.current = held;
    setKey(held);
    setPhase("submitting");
    setMessage(null);
    try {
      const request: ControlSubmission =
        replay && requestRef.current
          ? requestRef.current
          : {
              ...(target ? { agent_profile: target.agentProfile } : {}),
              llm_profile: target ? target.llmProfile : llmProfile.trim(),
              executor: target ? target.executor : executor.trim(),
              objective,
              ...(name.trim() ? { name: name.trim() } : {}),
            };
      requestRef.current = request;
      const result = await client.submitJob(request, held);
      if (!mounted.current || keyRef.current !== held) return;
      if (result.status === "admitted" && result.job) {
        setPhase("admitted");
        setMessage(
          result.created
            ? `${result.job} was created and admitted by this request.`
            : `${result.job} was already admitted under this key.`,
        );
        onJob(result.job);
      } else {
        setPhase("pending");
        setMessage("Admission is still running under this key; it is not submitted again.");
        reconciliation.current = setTimeout(() => reconcile(held), 2000);
      }
    } catch (error) {
      if (!mounted.current || keyRef.current !== held) return;
      const outcome = phase === "uncertain" ? "uncertain" : outcomeOf(error);
      setPhase(outcome);
      const code =
        error instanceof ControlTransportError
          ? error.code
          : error instanceof ContractError
            ? "contract"
            : "unreachable";
      setMessage(
        outcome === "uncertain"
          ? `The submission did not finish (${code}). Whether it created a Job is unknown; this key is not submitted again under another.`
          : `Nothing was submitted (${code}).`,
      );
      if (outcome === "uncertain") {
        reconciliation.current = setTimeout(() => reconcile(held), 4000);
      }
    }
  }

  function retrySame() {
    // The same intent, the same key: the API answers one Job for it.
    void submit(true);
  }

  function fresh() {
    if (phase !== "admitted" && phase !== "refused") return;
    clearReconciliation();
    keyRef.current = null;
    requestRef.current = null;
    setKey(null);
    setPhase("editing");
    setMessage(null);
    setObjective("");
    setName("");
  }

  return (
    <section className="flex flex-col gap-4 rounded-lg border border-border bg-card p-5 text-card-foreground">
      <header className="flex flex-col gap-1">
        <h2 className="font-heading text-lg">{chat ? "Send a message" : "Submit a Job"}</h2>
        <p className="text-xs text-muted-foreground">
          {chat
            ? "The message becomes one Job of the selected Agent. One Idempotency-Key names it through every retry."
            : "An existing LLM Profile and Executor, by their canonical identities, and an objective. One Idempotency-Key names this submission through every retry."}
        </p>
      </header>
      {chat ? null : (
        <>
          <div className="grid gap-3 sm:grid-cols-2">
            <label className="flex flex-col gap-1.5 text-xs font-semibold tracking-widest uppercase text-muted-foreground">
              LLM Profile
              <Input
                value={llmProfile}
                onChange={(event) => setLlmProfile(event.target.value)}
                placeholder="profile:<n>"
                spellCheck={false}
                disabled={settled}
              />
            </label>
            <label className="flex flex-col gap-1.5 text-xs font-semibold tracking-widest uppercase text-muted-foreground">
              Executor
              <Input
                value={executor}
                onChange={(event) => setExecutor(event.target.value)}
                placeholder="executor:<n>"
                spellCheck={false}
                disabled={settled}
              />
            </label>
          </div>
        </>
      )}
      <label className="flex flex-col gap-1.5 text-xs font-semibold tracking-widest uppercase text-muted-foreground">
        {chat ? "Message" : "Objective"}
        <Textarea
          value={objective}
          onChange={(event) => setObjective(event.target.value)}
          rows={4}
          disabled={settled}
        />
      </label>
      {chat ? null : (
        <label className="flex flex-col gap-1.5 text-xs font-semibold tracking-widest uppercase text-muted-foreground">
          Name (optional)
          <Input
            value={name}
            onChange={(event) => setName(event.target.value)}
            disabled={settled}
          />
        </label>
      )}
      {key ? (
        <p className="text-xs text-muted-foreground">
          Idempotency-Key: <code className="font-geist-mono">{key}</code>
        </p>
      ) : null}
      {message ? <p className="text-sm text-muted-foreground">{message}</p> : null}
      {settled ? (
        <p className="text-xs text-muted-foreground">
          Resolve this submission before starting a new Job intent. Retries and lookups keep this key.
        </p>
      ) : null}
      <div className="flex flex-wrap gap-2">
        {phase === "editing" || phase === "refused" ? (
          <Button
            onClick={() => void submit()}
            disabled={objective.trim().length === 0 || (chat && !target)}
          >
            {phase === "editing" ? (chat ? "Send" : "Submit Job") : chat ? "Send again" : "Submit again"}
          </Button>
        ) : null}
        {phase === "uncertain" ? (
          <Button variant="outline" onClick={retrySame}>
            Retry with this key
          </Button>
        ) : null}
        {phase === "uncertain" || phase === "pending" ? (
          <Button
            variant="outline"
            onClick={() => {
              if (key) {
                clearReconciliation();
                reconcile(key);
              }
            }}
          >
            Look the key up
          </Button>
        ) : null}
        {phase !== "editing" ? (
          <Button variant="ghost" disabled={settled} onClick={fresh}>
            {chat ? "New message" : "New submission"}
          </Button>
        ) : null}
      </div>
    </section>
  );
}
