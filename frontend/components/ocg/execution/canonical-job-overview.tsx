"use client";

import { useState } from "react";
import { Check, ChevronDown, Circle, X } from "lucide-react";
import { cn } from "@/lib/utils";
import { formatCount, formatDuration } from "@/lib/format";
import { JOB_STATE, Pill } from "@/components/ocg/primitives";
import type { CanonicalJobState } from "../contracts";
import { runtimeStateLabel, useI18n, type I18nKey } from "../i18n";
import { callKindOf, currentActivity, executionActivity, frozenEffort, nativeToolNameOf } from "../chat/execution-status";
import { currentLabel, stepLabel } from "../chat/activity-labels";
import { isTerminalJobState, summarizeCalls, type JobExecution } from "./domain";
import { placementOf } from "./placement";

const RECENT_STEPS = 8;

function stateVisual(state: string) {
  return JOB_STATE[state as CanonicalJobState] ?? { tone: "slate" as const };
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="border-t border-border pt-3">
      <h3 className="mb-1.5 text-[11px] font-semibold">{title}</h3>
      {children}
    </section>
  );
}

function Fact({ label, children, title }: { label: I18nKey; children: React.ReactNode; title?: string }) {
  const { t } = useI18n();
  return (
    <div className="flex min-w-0 items-baseline justify-between gap-3 py-0.5 text-[12px]">
      <dt className="shrink-0 text-muted-foreground">{t(label)}</dt>
      <dd className="min-w-0 truncate text-right font-medium tabular-nums" title={title}>{children}</dd>
    </div>
  );
}

function shortId(id: string): string {
  return id.length > 18 ? `${id.slice(0, 12)}…${id.slice(-4)}` : id;
}

const MAX_FAILURE_REASON_LENGTH = 500;

function boundedReason(reason: string): string {
  const singleLine = reason.replace(/\s+/g, " ").trim();
  return singleLine.length > MAX_FAILURE_REASON_LENGTH
    ? `${singleLine.slice(0, MAX_FAILURE_REASON_LENGTH - 1)}…`
    : singleLine;
}

/**
 * How one canonical Job executed, read only from its JobExecution: identity,
 * state, generation, frozen target, Attempts, observable activity, Call
 * counts, termination, recovery and relations. Nothing here depends on
 * RuntimeObservability, and nothing reports progress the backend did not record.
 */
export function CanonicalJobOverview({ execution }: { execution: JobExecution }) {
  const { t, locale } = useI18n();
  const [expandedFailures, setExpandedFailures] = useState<Set<string>>(() => new Set());
  const state = JOB_STATE[execution.state];
  const terminal = isTerminalJobState(execution.state);
  const placement = placementOf(execution);
  const selected = placement.selected;
  const effort = selected ? frozenEffort(execution, selected.target.firstDispatchIntentId) : null;
  const steps = executionActivity(execution);
  const now = currentActivity(execution, steps);
  const recent = steps.slice(-RECENT_STEPS).reverse();
  const summary = summarizeCalls(execution.calls);
  const kinds = execution.calls.map(callKindOf);
  const attempts = [...execution.attempts].sort((left, right) => right.generation - left.generation);
  const reason = execution.terminationReason;
  const latestRecovery = execution.recovery.at(-1);
  const hasRelations = execution.parentJobId !== null || execution.childJobIds.length > 0 ||
    execution.dependsOn.length > 0 || execution.blockedBy.length > 0 || execution.waitingForChildren;
  const time = (seconds: number) => new Date(seconds * 1000).toLocaleString(locale);

  return (
    <div className="flex flex-col gap-3" data-canonical-job={execution.jobId}>
      <div className="min-w-0">
        <p className="truncate text-[14px] font-semibold tracking-tight" title={execution.jobId}>{execution.jobId}</p>
        <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
          <Pill tone={state.tone} dot pulse={state.pulse}>{runtimeStateLabel(t, execution.state)}</Pill>
          <span className="text-[11px] text-muted-foreground">{t("execution.canonical.generation", { generation: execution.generation })}</span>
        </div>
        {execution.attempts.length > 1 && (
          <p className="mt-1 text-[11px] text-muted-foreground">{t("execution.canonical.retried", { count: execution.attempts.length })}</p>
        )}
        <p className="mt-1 text-[10px] text-muted-foreground" title={`${time(execution.createdAt)} → ${time(execution.updatedAt)}`}>
          {t("execution.createdAt")} {time(execution.createdAt)} · {t("execution.updatedAt")} {time(execution.updatedAt)}
        </p>
      </div>

      {reason && (
        <section role="status" className={cn("rounded-md border px-2.5 py-2 text-[12px]", execution.state === "failed" || execution.state === "orphaned" ? "border-red-500/40 bg-red-500/5" : "border-border bg-muted/30")}>
          <h3 className="text-[11px] font-semibold">{t("execution.canonical.termination")}</h3>
          <p className="mt-0.5 break-words">{reason.message}</p>
          <p className="mt-0.5 font-mono text-[10px] text-muted-foreground">{reason.code} · {reason.class}</p>
        </section>
      )}

      <Section title={t("execution.canonical.target")}>
        {selected ? (
          <dl data-execution-target={selected.kind}>
            <Fact label="execution.canonical.provider">{selected.target.providerKey}</Fact>
            <Fact label="execution.canonical.model" title={selected.target.upstreamModelId ?? undefined}>{selected.target.model}</Fact>
            <Fact label="execution.canonical.effort">{effort ?? t("execution.canonical.effortNotSet")}</Fact>
            <p className="mt-1 text-[10px] text-muted-foreground">
              {t(selected.kind === "current" ? "execution.canonical.targetCurrent" : "execution.canonical.targetFinal")} · #{selected.target.generation}
            </p>
          </dl>
        ) : (
          <p className="text-[12px] text-muted-foreground">{t(placement.outcome === "rejected" ? "execution.canonical.targetRejected" : "execution.canonical.targetPending")}</p>
        )}
      </Section>

      <Section title={t("execution.attempts")}>
        <ol className="space-y-1">
          {attempts.map(attempt => {
            const visual = stateVisual(attempt.state);
            const marker = attempt.generation !== execution.generation ? null : terminal ? "execution.canonical.attemptFinal" : "execution.canonical.attemptCurrent";
            return (
              <li key={attempt.id} data-attempt-generation={attempt.generation} className="flex min-w-0 items-center gap-2 text-[12px]">
                <span className="w-6 shrink-0 font-mono text-muted-foreground">#{attempt.generation}</span>
                <span className="min-w-0 flex-1 truncate font-mono text-[11px]" title={attempt.attemptId}>{shortId(attempt.attemptId)}</span>
                <Pill tone={visual.tone} dot pulse={visual.pulse}>{runtimeStateLabel(t, attempt.state)}</Pill>
                {marker && <span className="shrink-0 text-[10px] font-semibold text-foreground">{t(marker)}</span>}
                <span className="shrink-0 text-[10px] text-muted-foreground tabular-nums" title={`${time(attempt.createdAt)}${attempt.finishedAt !== null ? ` → ${time(attempt.finishedAt)}` : ""}`}>
                  {attempt.finishedAt !== null ? formatDuration((attempt.finishedAt - attempt.createdAt) * 1000) : t("execution.callsCount", { count: attempt.callCount })}
                </span>
              </li>
            );
          })}
        </ol>
      </Section>

      <Section title={t("execution.canonical.recentActivity", { generation: execution.generation })}>
        {now && (
          <p className="mb-1.5 flex min-w-0 items-center gap-1.5 text-[12px] font-medium text-amber-600 dark:text-amber-400" data-current-activity>
            <span className="size-1.5 shrink-0 animate-pulse rounded-full bg-amber-500" aria-hidden="true" />
            <span className="truncate">{currentLabel(t, now)}</span>
          </p>
        )}
        {recent.length ? (
          <ol className="space-y-0.5 text-[11px] text-muted-foreground">
            {recent.map(step => {
              const call = execution.calls.find(item => item.callId === step.id);
              const expandableFailure = step.status === "failed" && Boolean(call && callKindOf(call) === "native" && call.reason?.trim());
              const expanded = expandedFailures.has(step.id);
              const toolName = call ? nativeToolNameOf(call) : null;
              return (
                <li key={step.id} className={cn("min-w-0", step.status === "failed" && "text-destructive")}>
                  <div className={cn("flex min-w-0 items-center gap-1.5", step.status === "running" && "text-foreground")}>
                    {step.status === "done" ? <Check className="size-3 shrink-0" aria-hidden="true" />
                      : step.status === "failed" ? <X className="size-3 shrink-0" aria-hidden="true" />
                        : <Circle className="size-3 shrink-0" aria-hidden="true" />}
                    {expandableFailure ? (
                      <button
                        type="button"
                        className="flex min-w-0 items-center gap-1.5 text-left hover:text-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
                        aria-expanded={expanded}
                        aria-controls={`failure-${step.id}`}
                        onClick={() => setExpandedFailures(current => {
                          const next = new Set(current);
                          if (next.has(step.id)) next.delete(step.id); else next.add(step.id);
                          return next;
                        })}
                      >
                        <ChevronDown className={cn("size-3 shrink-0 transition-transform", expanded && "rotate-180")} aria-hidden="true" />
                        <span className="truncate">{stepLabel(t, step)}</span>
                      </button>
                    ) : <span className="truncate">{stepLabel(t, step)}</span>}
                  </div>
                  {expandableFailure && expanded && call && (
                    <dl id={`failure-${step.id}`} className="ml-7 mt-1 space-y-0.5 border-l border-destructive/30 pl-2 text-[10px] text-muted-foreground">
                      {toolName && <div><dt className="inline">{t("execution.canonical.tool")}: </dt><dd className="inline font-mono">{toolName}</dd></div>}
                      <div><dt className="inline">{t("execution.canonical.callId")}: </dt><dd className="inline font-mono" >{call.callId}</dd></div>
                      <div><dt className="inline">{t("execution.canonical.failureReason")}: </dt><dd className="inline break-words">{boundedReason(call.reason!)}</dd></div>
                    </dl>
                  )}
                </li>
              );
            })}
          </ol>
        ) : !now && <p className="text-[12px] text-muted-foreground">{t("execution.canonical.noActivity", { generation: execution.generation })}</p>}
      </Section>

      <Section title={t("execution.canonical.counts")}>
        <dl>
          <Fact label="execution.attempts">{formatCount(execution.attempts.length)}</Fact>
          <Fact label="execution.executors">{formatCount(execution.executors.length)}</Fact>
          <Fact label="execution.calls">{formatCount(summary.total)}</Fact>
          <Fact label="execution.canonical.providerCalls">{formatCount(kinds.filter(kind => kind === "provider").length)}</Fact>
          <Fact label="execution.canonical.nativeCalls">{formatCount(kinds.filter(kind => kind === "native").length)}</Fact>
          <Fact label="execution.canonical.running">{formatCount(summary.running + summary.queued)}</Fact>
          <Fact label="execution.canonical.completed">{formatCount(summary.completed)}</Fact>
          <Fact label="execution.canonical.failed">{formatCount(summary.failed)}</Fact>
        </dl>
      </Section>

      {latestRecovery && (
        <Section title={t("execution.canonical.recovery")}>
          <p className="text-[12px] text-muted-foreground">
            {t("execution.canonical.recoveryCount", { count: execution.recovery.length, latest: `${latestRecovery.classification} → ${latestRecovery.action} (${latestRecovery.outcome})` })}
          </p>
        </Section>
      )}

      {hasRelations && (
        <Section title={t("execution.canonical.relations")}>
          <dl>
            {execution.parentJobId !== null && <Fact label="execution.canonical.parentJob" title={execution.parentJobId}>{shortId(execution.parentJobId)}</Fact>}
            {execution.childJobIds.length > 0 && <Fact label="execution.canonical.childJobs" title={execution.childJobIds.join("\n")}>{formatCount(execution.childJobIds.length)}</Fact>}
            {execution.dependsOn.length > 0 && <Fact label="execution.canonical.dependsOn" title={execution.dependsOn.join("\n")}>{formatCount(execution.dependsOn.length)}</Fact>}
            {execution.blockedBy.length > 0 && <Fact label="execution.canonical.blockedBy" title={execution.blockedBy.join("\n")}>{formatCount(execution.blockedBy.length)}</Fact>}
            {execution.waitingForChildren && <p className="text-[12px] text-muted-foreground">{t("execution.canonical.waitingForChildren")}</p>}
          </dl>
        </Section>
      )}
    </div>
  );
}
