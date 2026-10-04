"use client";

import { useState, ViewTransition } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { PageSurface, JOB_STATE, Pill, SectionTitle } from "../primitives";
import { filterCalls, type JobExecution } from "./domain";
import { jobElapsedMs, type JobAccounting } from "./accounting";
import { ContextMetrics, CostDetails, TokenMetrics } from "../usage/usage-values";
import { runtimeStateLabel, useI18n } from "../i18n";
import { formatDuration, formatNumber } from "@/lib/format";
import type { JobUsageResponse } from "../contracts";

export function JobExecutionSurface({ execution, accounting = null, usage = null, usageLoading = false, usageError = null, onRetryUsage, onOpenInspector, embedded = false }: {
  execution: JobExecution;
  accounting?: JobAccounting | null;
  usage?: JobUsageResponse | null;
  usageLoading?: boolean;
  usageError?: string | null;
  onRetryUsage?: () => void;
  onOpenInspector?: () => void;
  embedded?: boolean;
}) {
  const { t } = useI18n();
  const [query, setQuery] = useState("");
  const [attemptId, setAttemptId] = useState("");
  const calls = filterCalls(execution.calls, { query }).filter((call) => !attemptId || call.attemptId === attemptId);
  const reportedFailure = [...execution.authoritativeCalls]
    .reverse()
    .find((call) => call.status === "failed" && call.reason)?.reason;
  const createdAt = new Date(execution.createdAt * 1000).toISOString();
  const updatedAt = new Date(execution.updatedAt * 1000).toISOString();
  const elapsed = jobElapsedMs(execution);
  const jobState = JOB_STATE[execution.state];

  return (
    <PageSurface className={embedded ? "space-y-5 p-4 text-xs sm:p-4 lg:p-4" : "space-y-5 text-xs"}>
      <header className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0 basis-full sm:flex-1 sm:basis-auto">
          <p className="text-[10px] uppercase tracking-wider text-muted-foreground">{t("execution.title")}</p>
          <ViewTransition name={`job-${execution.jobId}`} share="vt-shared" default="none">
            <h1 className="mt-1 break-all text-lg font-semibold">{execution.jobId}</h1>
          </ViewTransition>
          <p className="mt-1 text-muted-foreground">{t("home.projectLabel", { project: execution.projectId })}</p>
        </div>
        <Pill tone={jobState.tone} dot pulse={jobState.pulse}>{runtimeStateLabel(t, execution.state)}</Pill>
        {onOpenInspector && <Button variant="outline" size="xs" onClick={onOpenInspector}>{t("execution.openInspector")}</Button>}
      </header>

      <section aria-label={t("execution.currentActivity")} className="rounded-lg border border-border bg-card p-3">
        <SectionTitle>{t("execution.currentActivity")}</SectionTitle>
        {execution.currentCall ? (
          <p className="mt-2 flex items-center gap-2 text-[12px]">
            <Pill tone="amber" dot pulse>{t("state.running")}</Pill>
            <span>{t("execution.activityRunning")}</span>
          </p>
        ) : (
          <p className="mt-2 text-[11px] text-muted-foreground">{t("execution.activityNotReported")}</p>
        )}
      </section>

      <section aria-label={t("execution.overview")} className="rounded-lg border border-border bg-card p-4">
        <SectionTitle>{t("execution.overview")}</SectionTitle>
        <dl className="mt-3 grid gap-3 sm:grid-cols-2">
          <div>
            <dt className="text-[11px] text-muted-foreground">{t("execution.createdAt")}</dt>
            <dd><time dateTime={createdAt}>{createdAt}</time></dd>
          </div>
          <div>
            <dt className="text-[11px] text-muted-foreground">{t("execution.updatedAt")}</dt>
            <dd><time dateTime={updatedAt}>{updatedAt}</time></dd>
          </div>
          <div>
            <dt className="text-[11px] text-muted-foreground">{t("execution.elapsedAtUpdate")}</dt>
            <dd>{formatDuration(elapsed)}</dd>
          </div>
        </dl>
        {execution.state === "failed" && reportedFailure && (
          <div role="alert" className="mt-4 rounded-md border border-destructive/30 bg-destructive/5 p-3">
            <p className="text-[11px] font-semibold">{t("execution.reportedFailureDetail")}</p>
            <p className="mt-1 whitespace-pre-wrap break-words text-[12px] text-muted-foreground">{reportedFailure}</p>
          </div>
        )}
        <p className="mt-4 text-[11px] leading-5 text-muted-foreground">{t("execution.jobDetailsNotReported")}</p>
      </section>

      <section aria-label={t("execution.usage")} className="rounded-lg border border-border bg-card p-4">
        <SectionTitle>{t("execution.usage")}</SectionTitle>
        {accounting?.ceiling && (
          <p className="mt-3 text-[11px] text-muted-foreground">
            {t("execution.budgetCeiling")}: <strong className="font-medium text-foreground">{formatNumber(accounting.ceiling.amount)} {accounting.ceiling.unit}</strong>
            <span> · {t("execution.budgetCeilingSource", { source: accounting.ceiling.source })}</span>
          </p>
        )}
        {usage ? (
          <div className="mt-3 space-y-3">
            <section aria-label={t("usage.tokens")}>
              <SectionTitle>{t("usage.tokens")}</SectionTitle>
              <TokenMetrics totals={usage.totals} />
            </section>
            <section aria-label={t("usage.context")}>
              <SectionTitle>{t("usage.context")}</SectionTitle>
              <ContextMetrics totals={usage.totals} />
            </section>
            <section aria-label={t("usage.cost")}>
              <SectionTitle>{t("usage.cost")}</SectionTitle>
              <CostDetails cost={usage.totals.cost} />
            </section>
            {usage.providers.length > 0 && (
              <section aria-label={t("execution.providersInUsage")}>
                <SectionTitle>{t("execution.providersInUsage")}</SectionTitle>
                <ul className="mt-1 flex flex-wrap gap-1">{usage.providers.map((row, index) => <li key={`${row.provider ?? "unknown"}-${index}`}><Pill tone="slate">{row.provider ?? t("common.notReported")}</Pill></li>)}</ul>
              </section>
            )}
            {usage.models.length > 0 && (
              <section aria-label={t("execution.modelsInUsage")}>
                <SectionTitle>{t("execution.modelsInUsage")}</SectionTitle>
                <ul className="mt-1 flex flex-wrap gap-1">{usage.models.map((row, index) => <li key={`${row.provider ?? "unknown"}-${row.model ?? "unknown"}-${index}`}><Pill tone="slate">{[row.provider, row.model].filter(Boolean).join(" · ") || t("common.notReported")}</Pill></li>)}</ul>
              </section>
            )}
            {usage.truncated && <p className="text-[10px] text-muted-foreground">{t("usage.limited")}</p>}
          </div>
        ) : (
          <div className="mt-3 space-y-3">
            {accounting?.consumption && <CostDetails cost={accounting.consumption} />}
            {usageLoading && <p role="status" className="text-[11px] text-muted-foreground">{t("execution.usageLoading")}</p>}
            {usageError && (
              <div role="alert" className="flex flex-wrap items-center gap-2 text-[11px] text-destructive">
                <span>{t("execution.usageLoadFailed")}</span>
                {onRetryUsage && <Button size="xs" variant="outline" onClick={onRetryUsage}>{t("execution.retryUsage")}</Button>}
              </div>
            )}
            {!accounting?.consumption && !usageLoading && !usageError && <p className="text-[11px] text-muted-foreground">{t("execution.usageNotReported")}</p>}
          </div>
        )}
      </section>

      <details className="rounded-lg border border-border bg-card p-4">
        <summary className="cursor-pointer text-[13px] font-semibold">{t("execution.diagnosticsDetails")}</summary>
        <div className="mt-3 space-y-4">
          <p className="text-[11px] text-muted-foreground">
            {t("execution.projectMeta", { project: execution.projectId, generation: execution.generation, cursor: execution.cursor })}
          </p>
          <section className="rounded border border-border p-3">
            <SectionTitle>{t("execution.attempts")}</SectionTitle>
            <p className="mb-2 text-muted-foreground">{t("execution.authoritativeAttempt", { id: execution.authoritativeAttemptId ?? t("common.none") })}</p>
            {execution.progress && <p className="mb-2">{t("execution.callsSettled", { settled: execution.progress.settled, total: execution.progress.total, percent: execution.progress.percent })}</p>}
            <div className="flex flex-wrap gap-2">
              <Button size="xs" variant={attemptId === "" ? "secondary" : "outline"} onClick={() => setAttemptId("")}>{t("execution.allAttempts")}</Button>
              {execution.attempts.map((attempt) => (
                <Button key={attempt.id} size="xs" className="h-auto max-w-full whitespace-normal break-all text-left" variant={attemptId === attempt.attemptId ? "secondary" : "outline"} onClick={() => setAttemptId(attempt.attemptId)}>
                  {attempt.attemptId} · {runtimeStateLabel(t, attempt.state)}{attempt.authoritative ? ` · ${t("execution.authoritative")}` : ""}
                </Button>
              ))}
            </div>
          </section>
          <section className="rounded border border-border p-3">
            <SectionTitle>{t("execution.executors")}</SectionTitle>
            {execution.executors.length === 0 && <p className="text-muted-foreground">{t("execution.noExecutors")}</p>}
            <ul className="space-y-1">{execution.executors.map((executor) => <li key={executor.id} className="break-words">{executor.executorId} · {runtimeStateLabel(t, executor.status)} · {t("execution.callsCount", { count: executor.callIds.length })}</li>)}</ul>
          </section>
          <section className="rounded border border-border p-3">
            <SectionTitle detail={t("execution.callsCount", { count: calls.length })}>{t("execution.calls")}</SectionTitle>
            <Input className="mb-3" aria-label={t("execution.searchCalls")} placeholder={t("execution.searchCallsPlaceholder")} value={query} onChange={(event) => setQuery(event.target.value)} />
            {calls.length === 0 && <p className="text-muted-foreground">{t("execution.noCallsMatch")}</p>}
            <ul className="space-y-2">{calls.map((call) => (
              <li key={call.id} className="rounded border border-border p-3">
                <div className="flex flex-wrap justify-between gap-2"><span className="break-all font-medium">{call.callId}</span><span>{runtimeStateLabel(t, call.rawState)}</span></div>
                <p className="mt-1 break-words text-muted-foreground">{t("execution.callMeta", { attempt: call.attemptId, executor: call.executorId ?? t("common.notReported"), effect: call.effectKind })}</p>
                {call.reason && <p className="mt-2 whitespace-pre-wrap break-words">{call.reason}</p>}
                <details className="mt-2"><summary className="cursor-pointer">{t("execution.requestAndResponse")}</summary><pre className="mt-2 whitespace-pre-wrap break-all">{call.request}</pre><pre className="mt-2 whitespace-pre-wrap break-all">{call.response ?? t("execution.noResponse")}</pre></details>
              </li>
            ))}</ul>
          </section>
        </div>
      </details>
    </PageSurface>
  );
}
