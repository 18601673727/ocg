"use client";

import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Pill, SectionTitle } from "../primitives";
import { filterCalls, type JobExecution } from "./domain";

export function JobExecutionSurface({ execution, onOpenInspector }: {
  execution: JobExecution;
  onOpenInspector?: () => void;
}) {
  const [query, setQuery] = useState("");
  const [attemptId, setAttemptId] = useState("");
  const calls = filterCalls(execution.calls, { query }).filter((call) => !attemptId || call.attemptId === attemptId);

  return (
    <div className="flex h-full min-h-0 w-full flex-col overflow-auto bg-background p-4 text-xs">
      <header className="mb-4 flex items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="text-[10px] uppercase tracking-wider text-muted-foreground">Job execution</p>
          <h1 className="mt-1 break-all text-lg font-semibold">{execution.jobId}</h1>
          <p className="mt-1 text-muted-foreground">Project {execution.projectId} · generation {execution.generation} · cursor {execution.cursor}</p>
        </div>
        <Pill tone={execution.state === "completed" ? "emerald" : execution.state === "failed" ? "red" : "slate"}>{execution.state}</Pill>
        {onOpenInspector && <Button variant="outline" size="xs" onClick={onOpenInspector}>Open inspector</Button>}
      </header>
      <section className="mb-4 rounded border border-border p-3">
        <SectionTitle>Attempts</SectionTitle>
        <p className="mb-2 text-muted-foreground">Authoritative Attempt: {execution.authoritativeAttemptId ?? "None"}</p>
        {execution.progress && <p className="mb-2">{execution.progress.settled} / {execution.progress.total} authoritative Calls settled ({execution.progress.percent}%)</p>}
        <div className="flex flex-wrap gap-2">
          <Button size="xs" variant={attemptId === "" ? "secondary" : "outline"} onClick={() => setAttemptId("")}>All Attempts</Button>
          {execution.attempts.map((attempt) => (
            <Button key={attempt.id} size="xs" variant={attemptId === attempt.attemptId ? "secondary" : "outline"} onClick={() => setAttemptId(attempt.attemptId)}>
              {attempt.attemptId} · {attempt.state}{attempt.authoritative ? " · authoritative" : ""}
            </Button>
          ))}
        </div>
      </section>
      <section className="mb-4 rounded border border-border p-3">
        <SectionTitle>Executors</SectionTitle>
        {execution.executors.length === 0 && <p className="text-muted-foreground">No Executor is reported.</p>}
        <ul className="space-y-1">{execution.executors.map((executor) => <li key={executor.id}>{executor.executorId} · {executor.status} · {executor.callIds.length} Calls</li>)}</ul>
      </section>
      <section className="rounded border border-border p-3">
        <SectionTitle detail={`${calls.length} Calls`}>Calls</SectionTitle>
        <Input className="mb-3" aria-label="Search Calls" placeholder="Search Call, Executor, state or effect" value={query} onChange={(event) => setQuery(event.target.value)} />
        {calls.length === 0 && <p className="text-muted-foreground">No Calls match this selection.</p>}
        <ul className="space-y-2">{calls.map((call) => (
          <li key={call.id} className="rounded border border-border p-3">
            <div className="flex flex-wrap justify-between gap-2"><span className="break-all font-medium">{call.callId}</span><span>{call.rawState}</span></div>
            <p className="mt-1 text-muted-foreground">Attempt {call.attemptId} · Executor {call.executorId ?? "Not reported"} · {call.effectKind}</p>
            {call.reason && <p className="mt-2">{call.reason}</p>}
            <details className="mt-2"><summary className="cursor-pointer">Request and response</summary><pre className="mt-2 whitespace-pre-wrap break-all">{call.request}</pre><pre className="mt-2 whitespace-pre-wrap break-all">{call.response ?? "No response recorded"}</pre></details>
          </li>
        ))}</ul>
      </section>
    </div>
  );
}
