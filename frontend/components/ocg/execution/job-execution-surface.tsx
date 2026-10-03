"use client";

import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Pill, SectionTitle } from "../primitives";
import { filterCalls, type JobExecution } from "./domain";
import { runtimeStateLabel, useI18n } from "../i18n";

export function JobExecutionSurface({ execution, onOpenInspector }: {
  execution: JobExecution;
  onOpenInspector?: () => void;
}) {
  const { t } = useI18n();
  const [query, setQuery] = useState("");
  const [attemptId, setAttemptId] = useState("");
  const calls = filterCalls(execution.calls, { query }).filter((call) => !attemptId || call.attemptId === attemptId);

  return (
    <div className="flex h-full min-h-0 w-full flex-col overflow-auto bg-background p-4 text-xs">
      <header className="mb-4 flex items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="text-[10px] uppercase tracking-wider text-muted-foreground">{t("execution.title")}</p>
          <h1 className="mt-1 break-all text-lg font-semibold">{execution.jobId}</h1>
          <p className="mt-1 text-muted-foreground">{t("execution.projectMeta", { project: execution.projectId, generation: execution.generation, cursor: execution.cursor })}</p>
        </div>
        <Pill tone={execution.state === "completed" ? "emerald" : execution.state === "failed" ? "red" : "slate"}>{runtimeStateLabel(t, execution.state)}</Pill>
        {onOpenInspector && <Button variant="outline" size="xs" onClick={onOpenInspector}>{t("execution.openInspector")}</Button>}
      </header>
      <section className="mb-4 rounded border border-border p-3">
        <SectionTitle>{t("execution.attempts")}</SectionTitle>
        <p className="mb-2 text-muted-foreground">{t("execution.authoritativeAttempt", { id: execution.authoritativeAttemptId ?? t("common.none") })}</p>
        {execution.progress && <p className="mb-2">{t("execution.callsSettled", { settled: execution.progress.settled, total: execution.progress.total, percent: execution.progress.percent })}</p>}
        <div className="flex flex-wrap gap-2">
          <Button size="xs" variant={attemptId === "" ? "secondary" : "outline"} onClick={() => setAttemptId("")}>{t("execution.allAttempts")}</Button>
          {execution.attempts.map((attempt) => (
            <Button key={attempt.id} size="xs" variant={attemptId === attempt.attemptId ? "secondary" : "outline"} onClick={() => setAttemptId(attempt.attemptId)}>
              {attempt.attemptId} · {runtimeStateLabel(t, attempt.state)}{attempt.authoritative ? ` · ${t("execution.authoritative")}` : ""}
            </Button>
          ))}
        </div>
      </section>
      <section className="mb-4 rounded border border-border p-3">
        <SectionTitle>{t("execution.executors")}</SectionTitle>
        {execution.executors.length === 0 && <p className="text-muted-foreground">{t("execution.noExecutors")}</p>}
        <ul className="space-y-1">{execution.executors.map((executor) => <li key={executor.id}>{executor.executorId} · {runtimeStateLabel(t, executor.status)} · {t("execution.callsCount", { count: executor.callIds.length })}</li>)}</ul>
      </section>
      <section className="rounded border border-border p-3">
        <SectionTitle detail={t("execution.callsCount", { count: calls.length })}>{t("execution.calls")}</SectionTitle>
        <Input className="mb-3" aria-label={t("execution.searchCalls")} placeholder={t("execution.searchCallsPlaceholder")} value={query} onChange={(event) => setQuery(event.target.value)} />
        {calls.length === 0 && <p className="text-muted-foreground">{t("execution.noCallsMatch")}</p>}
        <ul className="space-y-2">{calls.map((call) => (
          <li key={call.id} className="rounded border border-border p-3">
            <div className="flex flex-wrap justify-between gap-2"><span className="break-all font-medium">{call.callId}</span><span>{runtimeStateLabel(t, call.rawState)}</span></div>
            <p className="mt-1 text-muted-foreground">{t("execution.callMeta", { attempt: call.attemptId, executor: call.executorId ?? t("common.notReported"), effect: call.effectKind })}</p>
            {call.reason && <p className="mt-2">{call.reason}</p>}
            <details className="mt-2"><summary className="cursor-pointer">{t("execution.requestAndResponse")}</summary><pre className="mt-2 whitespace-pre-wrap break-all">{call.request}</pre><pre className="mt-2 whitespace-pre-wrap break-all">{call.response ?? t("execution.noResponse")}</pre></details>
          </li>
        ))}</ul>
      </section>
    </div>
  );
}
