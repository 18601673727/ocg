"use client";

import { cn } from "@/lib/utils";
import {
  ATTRIBUTION_CONFIDENCES,
  ATTRIBUTION_CONFIDENCE_LABEL,
  type ResourceLedgerEntry,
  type LedgerRole,
} from "./types";
import { formatCostMicros, formatPercent, formatRatio, formatTimestamp, formatTokens } from "./format";
import { groupEntries, usageTotal, type LedgerGroup, type LedgerSummary } from "./selectors";
import {
  ATTRIBUTION_TONE,
  AttributionPill,
  AuthorityPill,
  CostPill,
  Pill,
  ReconciliationIndicator,
  ReconciliationPill,
  SectionTitle,
} from "./ledger-primitives";

function rolePill(role: LedgerRole) {
  return (
    <Pill tone={role === "lead" ? "violet" : "slate"} title={`Role: ${role}`}>
      {role}
    </Pill>
  );
}

function CompactMetrics({ summary }: { summary: LedgerSummary }) {
  return (
    <dl className="grid grid-cols-2 gap-x-3 gap-y-1.5 text-[11px] sm:grid-cols-3">
      <div>
        <dt className="text-[10px] text-muted-foreground">Calls</dt>
        <dd className="font-medium tabular-nums">
          {summary.entryCount}
          {summary.retryCount > 0 ? ` · ${summary.retryCount} retries` : ""}
        </dd>
      </div>
      <div>
        <dt className="text-[10px] text-muted-foreground">Success / failure</dt>
        <dd className="font-medium tabular-nums">
          {summary.successCount} / {summary.failureCount}
        </dd>
      </div>
      <div>
        <dt className="text-[10px] text-muted-foreground">Workers (lead calls)</dt>
        <dd className="font-medium tabular-nums">
          {summary.workerCount} ({summary.leadEntryCount})
        </dd>
      </div>
      <div>
        <dt className="text-[10px] text-muted-foreground">Total tokens</dt>
        <dd className="font-medium tabular-nums">{formatTokens(summary.totalTokens)}</dd>
      </div>
      <div>
        <dt className="text-[10px] text-muted-foreground">Fresh input</dt>
        <dd className="font-medium tabular-nums">{formatTokens(summary.traffic.freshInput)}</dd>
      </div>
      <div>
        <dt className="text-[10px] text-muted-foreground">Cache read</dt>
        <dd className="font-medium tabular-nums">{formatTokens(summary.traffic.cacheRead)}</dd>
      </div>
      <div>
        <dt className="text-[10px] text-muted-foreground">Cache write</dt>
        <dd className="font-medium tabular-nums">{formatTokens(summary.traffic.cacheWrite)}</dd>
      </div>
      <div>
        <dt className="text-[10px] text-muted-foreground">Output</dt>
        <dd className="font-medium tabular-nums">{formatTokens(summary.traffic.output)}</dd>
      </div>
      <div>
        <dt className="text-[10px] text-muted-foreground">Reasoning</dt>
        <dd className="font-medium tabular-nums">{formatTokens(summary.traffic.reasoning)}</dd>
      </div>
      <div>
        <dt className="text-[10px] text-muted-foreground">Cost</dt>
        <dd className="font-medium tabular-nums">{formatCostMicros(summary.costMicros)}</dd>
      </div>
      <div>
        <dt className="text-[10px] text-muted-foreground">Cache share</dt>
        <dd className="font-medium tabular-nums">{formatPercent(summary.cacheShare)}</dd>
      </div>
      <div>
        <dt className="text-[10px] text-muted-foreground">Cache leverage</dt>
        <dd className="font-medium tabular-nums">{formatRatio(summary.cacheLeverage)}</dd>
      </div>
      <div>
        <dt className="text-[10px] text-muted-foreground">Avg latency</dt>
        <dd className="font-medium tabular-nums">{summary.averageLatencyMs === null ? "—" : `${Math.round(summary.averageLatencyMs)}ms`}</dd>
      </div>
      <div className="col-span-2 sm:col-span-3">
        <dt className="mb-0.5 text-[10px] text-muted-foreground">Reconciliation</dt>
        <dd>
          <ReconciliationIndicator counts={summary.byReconciliation} />
        </dd>
      </div>
    </dl>
  );
}

/** Expandable aggregate list for the Providers and Models tabs. */
export function AggregateGroupList({
  groups,
  emptyLabel,
}: {
  groups: LedgerGroup[];
  emptyLabel: string;
}) {
  if (groups.length === 0) {
    return (
      <p className="rounded-md border border-dashed border-border px-2 py-4 text-[11px] text-muted-foreground">
        {emptyLabel}
      </p>
    );
  }
  return (
    <ul className="flex flex-col gap-1.5">
      {groups.map((group) => (
        <li key={group.key} className="min-w-0">
          <details className="min-w-0 rounded-md border border-border open:bg-muted/20">
            <summary className="flex min-w-0 cursor-pointer list-none items-center gap-2 px-2.5 py-2 marker:hidden">
              <div className="min-w-0 flex-1">
                <p className="truncate text-[12px] font-medium" title={group.label}>
                  {group.label}
                </p>
                {group.detail && (
                  <p className="truncate text-[10px] text-muted-foreground" title={group.detail}>
                    {group.detail}
                  </p>
                )}
              </div>
              <span className="shrink-0 text-right text-[10px] tabular-nums text-muted-foreground">
                {formatTokens(group.summary.totalTokens)} tok
                <br />
                {formatCostMicros(group.summary.costMicros)}
              </span>
            </summary>
            <div className="border-t border-border px-2.5 py-2">
              <CompactMetrics summary={group.summary} />
            </div>
          </details>
        </li>
      ))}
    </ul>
  );
}

/** One selectable provider call row for the Ledger tab. */
export function LedgerCallRow({
  entry,
  onSelect,
}: {
  entry: ResourceLedgerEntry;
  onSelect: (entry: ResourceLedgerEntry) => void;
}) {
  const total = usageTotal(entry.usage);
  return (
    <li className="min-w-0">
      <button
        type="button"
        onClick={() => onSelect(entry)}
        aria-haspopup="dialog"
        title={`Open detail for ${entry.workerLabel} on ${entry.model}`}
        className="flex w-full min-w-0 flex-col gap-1 rounded-md border border-border px-2.5 py-2 text-left transition-colors hover:bg-muted/50 focus-visible:outline-2 focus-visible:outline-offset-1 focus-visible:outline-ring"
      >
        <div className="flex min-w-0 items-center gap-1.5">
          <span className="shrink-0 font-mono text-[10px] text-muted-foreground">
            {formatTimestamp(entry.timestamp)}
          </span>
          <span className="min-w-0 truncate text-[12px] font-medium" title={entry.workerLabel}>
            {entry.workerLabel}
          </span>
          {entry.role === "lead" && (
            <Pill tone="violet" title="Lead participant">
              lead
            </Pill>
          )}
          <Pill
            tone={entry.status === "success" ? "emerald" : entry.status === "failure" ? "red" : "amber"}
            title={`Call status: ${entry.status}`}
          >
            {entry.status}
          </Pill>
          {entry.attempt > 1 && (
            <span className="shrink-0 text-[10px] text-amber-600 dark:text-amber-400" title={`Attempt ${entry.attempt}`}>
              ×{entry.attempt}
            </span>
          )}
          <span className="ml-auto shrink-0 text-[11px] font-semibold tabular-nums" title="Observed tokens">
            {formatTokens(total)} tok
          </span>
        </div>
        <div className="flex min-w-0 flex-wrap items-center gap-x-1.5 gap-y-1 text-[10px] text-muted-foreground">
          <span className="min-w-0 max-w-full truncate" title={entry.missionLabel}>
            {entry.missionLabel}
          </span>
          <span aria-hidden>·</span>
          <span className="min-w-0 max-w-full truncate" title={entry.taskLabel}>
            {entry.taskLabel}
          </span>
          <span aria-hidden>·</span>
          <span className="min-w-0 max-w-full truncate" title={`${entry.provider} · ${entry.model}`}>
            {entry.provider} · {entry.model}
            {entry.variant ? ` · ${entry.variant}` : ""}
          </span>
          <span className="ml-auto shrink-0 font-medium tabular-nums text-foreground" title="Cost">
            {formatCostMicros(entry.costMicros)}
          </span>
        </div>
        <div className="flex flex-wrap items-center gap-1">
          <AuthorityPill value={entry.usageAuthority} />
          <AttributionPill value={entry.attributionConfidence} />
          <CostPill value={entry.costProvenance} />
          <ReconciliationPill value={entry.reconciliation} />
        </div>
      </button>
    </li>
  );
}

/** Headline attribution and reconciliation context for the Attribution tab. */
export function AttributionSummary({ summary }: { summary: LedgerSummary }) {
  return (
    <div className="flex min-w-0 flex-col gap-2 rounded-md border border-border bg-muted/20 px-2.5 py-2">
      <div className="flex min-w-0 flex-wrap gap-1.5">
        {ATTRIBUTION_CONFIDENCES.map((confidence) => (
          <span
            key={confidence}
            className={cn(
              "inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-[10px]",
              ATTRIBUTION_TONE[confidence],
              summary.byAttribution[confidence] === 0 && "opacity-50",
            )}
            title={`${ATTRIBUTION_CONFIDENCE_LABEL[confidence]} attribution`}
          >
            {ATTRIBUTION_CONFIDENCE_LABEL[confidence]}
            <strong className="font-semibold tabular-nums">{summary.byAttribution[confidence]}</strong>
          </span>
        ))}
      </div>
      <p className="text-[10px] text-muted-foreground">
        {summary.leadEntryCount} of {summary.entryCount} calls were made by the Lead. Unknown attribution is
        surfaced rather than dropped.
      </p>
      <ReconciliationIndicator counts={summary.byReconciliation} />
    </div>
  );
}

/** Hierarchical Mission → task → worker attribution drilldown, including unknowns. */
export function AttributionBreakdown({
  missions,
  unknownEntries,
  onSelectEntry,
}: {
  missions: LedgerGroup[];
  unknownEntries: ResourceLedgerEntry[];
  onSelectEntry: (entry: ResourceLedgerEntry) => void;
}) {
  if (missions.length === 0 && unknownEntries.length === 0) {
    return (
      <p className="rounded-md border border-dashed border-border px-2 py-4 text-[11px] text-muted-foreground">
        No attributed calls match the current filters.
      </p>
    );
  }
  return (
    <div className="flex min-w-0 flex-col gap-2">
      {missions.map((mission) => (
        <details key={mission.key} className="min-w-0 rounded-md border border-border open:bg-muted/20">
          <summary className="flex min-w-0 cursor-pointer list-none items-center gap-2 px-2.5 py-2 marker:hidden">
            <div className="min-w-0 flex-1">
              <p className="truncate text-[12px] font-medium" title={mission.label}>
                {mission.label}
              </p>
              <p className="truncate text-[10px] text-muted-foreground">
                {mission.summary.entryCount} calls · {mission.summary.workerCount} workers ·{" "}
                {formatTokens(mission.summary.totalTokens)} tokens
              </p>
            </div>
            <div className="flex shrink-0 flex-wrap justify-end gap-1">
              {ATTRIBUTION_CONFIDENCES.map((confidence) => {
                const count = mission.summary.byAttribution[confidence];
                if (count === 0) return null;
                return (
                  <span
                    key={confidence}
                    className={cn(
                      "rounded-full border px-1.5 py-0.5 text-[9px] capitalize",
                      ATTRIBUTION_TONE[confidence],
                    )}
                    title={`${ATTRIBUTION_CONFIDENCE_LABEL[confidence]} attribution: ${count}`}
                  >
                    {confidence} {count}
                  </span>
                );
              })}
            </div>
          </summary>
          <div className="min-w-0 border-t border-border px-2.5 py-2">
            <TaskBreakdown mission={mission} onSelectEntry={onSelectEntry} />
          </div>
        </details>
      ))}

      {unknownEntries.length > 0 && (
        <section
          className="rounded-md border border-amber-500/30 bg-amber-500/5 px-2.5 py-2"
          aria-label="Unknown attribution"
        >
          <SectionTitle detail={`${unknownEntries.length} calls`}>Unknown attribution</SectionTitle>
          <p className="mb-1.5 text-[10px] text-muted-foreground">
            These calls could not be assigned to a Mission or task and are surfaced instead of being hidden.
          </p>
          <ul className="flex flex-col gap-1">
            {unknownEntries.map((entry) => (
              <li key={entry.id} className="min-w-0">
                <button
                  type="button"
                  onClick={() => onSelectEntry(entry)}
                  aria-haspopup="dialog"
                  className="flex w-full min-w-0 items-center gap-2 rounded border border-border bg-background px-2 py-1 text-left text-[10px] hover:bg-muted/50 focus-visible:outline-2 focus-visible:outline-offset-1 focus-visible:outline-ring"
                >
                  <span className="shrink-0 font-mono text-muted-foreground">
                    {formatTimestamp(entry.timestamp)}
                  </span>
                  <span className="min-w-0 truncate" title={entry.model}>
                    {entry.provider} · {entry.model}
                  </span>
                  <span className="ml-auto shrink-0 tabular-nums">
                    {formatTokens(usageTotal(entry.usage))} tok
                  </span>
                </button>
              </li>
            ))}
          </ul>
        </section>
      )}
    </div>
  );
}

function TaskBreakdown({
  mission,
  onSelectEntry,
}: {
  mission: LedgerGroup;
  onSelectEntry: (entry: ResourceLedgerEntry) => void;
}) {
  const tasks = groupEntries(mission.entries, "task");
  return (
    <div className="flex min-w-0 flex-col gap-2">
      {mission.summary.byAttribution.unknown > 0 && (
        <p className="text-[10px] text-amber-600 dark:text-amber-400">
          {mission.summary.byAttribution.unknown} call(s) under this Mission have unknown attribution.
        </p>
      )}
      {mission.summary.byReconciliation.mismatch > 0 && (
        <p className="text-[10px] text-red-600 dark:text-red-400">
          {mission.summary.byReconciliation.mismatch} reconciliation mismatch(es) need review.
        </p>
      )}
      <ul className="flex flex-col gap-1.5">
        {tasks.map((task) => (
          <li key={task.key} className="min-w-0">
            <details className="min-w-0 rounded border border-border/70">
              <summary className="flex min-w-0 cursor-pointer list-none items-center gap-2 px-2 py-1.5 marker:hidden">
                <span className="min-w-0 truncate text-[11px] font-medium" title={task.label}>
                  {task.label}
                </span>
                <span className="ml-auto shrink-0 text-[10px] tabular-nums text-muted-foreground">
                  {task.summary.entryCount} calls · {formatTokens(task.summary.totalTokens)} tok
                </span>
              </summary>
              <ul className="border-t border-border/70 px-2 py-1.5">
                {groupEntries(task.entries, "worker").map((worker) => (
                  <li key={worker.key} className="min-w-0 py-1 text-[10px]">
                    <details>
                      <summary className="flex min-w-0 cursor-pointer list-none items-center gap-2 marker:hidden">
                        <span className="min-w-0 truncate" title={worker.label}>
                          {worker.label}
                        </span>
                        {rolePill(worker.entries[0].role)}
                        <AttributionPill value={worker.entries[0].attributionConfidence} />
                        <span className="ml-auto shrink-0 tabular-nums text-muted-foreground">
                          {worker.summary.entryCount} calls · {formatTokens(worker.summary.totalTokens)} tok
                        </span>
                      </summary>
                      <div className="mt-1 rounded border border-border/60 bg-muted/20 p-1.5">
                        <CompactMetrics summary={worker.summary} />
                      </div>
                    </details>
                  </li>
                ))}
              </ul>
            </details>
          </li>
        ))}
      </ul>
      <button
        type="button"
        onClick={() => onSelectEntry(mission.entries[0])}
        className="self-start rounded border border-border px-2 py-0.5 text-[10px] hover:bg-muted focus-visible:outline-2 focus-visible:outline-offset-1 focus-visible:outline-ring"
        title="Inspect the first call in this Mission"
      >
        Inspect first call
      </button>
    </div>
  );
}
