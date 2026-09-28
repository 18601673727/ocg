"use client";

import {
  USAGE_COMPONENT_LABEL,
  USAGE_COMPONENTS,
  type LedgerCallStatus,
  type ResourceLedgerEntry,
} from "./types";
import { formatCostMicros, formatDuration, formatTimestamp, UNKNOWN } from "./format";
import { cacheLeverage, cacheShare, componentTraffic } from "./selectors";
import {
  AttributionPill,
  AuthorityPill,
  CostPill,
  Pill,
  ReconciliationPill,
  SectionTitle,
  Unknown,
} from "./ledger-primitives";

const STATUS_TONE: Record<LedgerCallStatus, "emerald" | "red" | "amber"> = {
  success: "emerald",
  failure: "red",
  retrying: "amber",
};

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="min-w-0">
      <dt className="text-[10px] tracking-wider text-muted-foreground uppercase">{label}</dt>
      <dd className="min-w-0 break-words text-[11px] font-medium">{children}</dd>
    </div>
  );
}

/** Full, inspectable detail for one provider call. */
export function CallDetail({ entry }: { entry: ResourceLedgerEntry }) {
  const traffic = componentTraffic([entry]);
  const share = cacheShare(traffic);
  const leverage = cacheLeverage(traffic);
  return (
    <div className="flex min-w-0 flex-col gap-3">
      <div className="flex min-w-0 flex-wrap items-center gap-1.5">
        <Pill tone={STATUS_TONE[entry.status]} title={`Call status: ${entry.status}`}>
          {entry.status}
        </Pill>
        <Pill tone={entry.role === "lead" ? "violet" : "slate"} title={`Role: ${entry.role}`}>
          {entry.role}
        </Pill>
        {entry.attempt > 1 && (
          <Pill tone="amber" title={`Retry attempt ${entry.attempt}`}>
            attempt {entry.attempt}
          </Pill>
        )}
        <span className="ml-auto font-mono text-[10px] text-muted-foreground" title={entry.timestamp}>
          {formatTimestamp(entry.timestamp)}
        </span>
      </div>

      <dl className="grid grid-cols-2 gap-x-3 gap-y-2 sm:grid-cols-3">
        <Field label="Worker">
          {entry.workerLabel} <span className="text-muted-foreground">({entry.workerId})</span>
        </Field>
        <Field label="Mission">
          <span title={entry.missionId}>{entry.missionLabel}</span>
        </Field>
        <Field label="Task">
          <span title={entry.taskId}>{entry.taskLabel}</span>
        </Field>
        <Field label="Provider">{entry.provider}</Field>
        <Field label="Model">{entry.model}</Field>
        <Field label="Variant">{entry.variant ?? <Unknown />}</Field>
         <Field label="Call index">{entry.callIndex}</Field>
         <Field label="Attempt">{entry.attempt}</Field>
         <Field label="Elapsed">{formatDuration(entry.elapsedMs)}</Field>
         <Field label="Invocation ID">{entry.invocationId ?? <Unknown />}</Field>
         <Field label="Session ID">{entry.sessionId ?? <Unknown />}</Field>
         <Field label="Duration">{formatDuration(entry.durationMs)}</Field>
         <Field label="Latency / TTFT">{formatDuration(entry.latencyMs)} / {formatDuration(entry.ttftMs)}</Field>
         <Field label="Started">{entry.startedAt ? formatTimestamp(entry.startedAt) : <Unknown />}</Field>
         <Field label="Finished">{entry.finishedAt ? formatTimestamp(entry.finishedAt) : <Unknown />}</Field>
      </dl>

      <section aria-label="Usage components">
        <SectionTitle detail="tokens">Usage</SectionTitle>
        <table className="w-full table-fixed text-[11px]">
          <tbody>
            {USAGE_COMPONENTS.map((component) => {
              const value = entry.usage?.[component];
              return (
                <tr key={component} className="border-b border-border/60 last:border-0">
                  <th scope="row" className="py-1 text-left font-normal text-muted-foreground">
                    {USAGE_COMPONENT_LABEL[component]}
                  </th>
                  <td className="py-1 text-right font-medium tabular-nums">
                    {value === undefined ? <Unknown /> : value.toLocaleString("en-US")}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
        <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
          <span className="text-[10px] text-muted-foreground">Usage authority</span>
          <AuthorityPill value={entry.usageAuthority} />
        </div>
        {entry.usage === null && (
          <p className="mt-1 text-[10px] text-muted-foreground">
            No usage was observed for this call. This is unavailable, not zero.
          </p>
        )}
        <dl className="mt-2 grid grid-cols-3 gap-2 rounded border border-border/60 bg-muted/20 p-2 text-[10px]">
          <div>
            <dt className="text-muted-foreground">Component traffic</dt>
            <dd className="font-semibold tabular-nums">{traffic.total === null ? UNKNOWN : traffic.total.toLocaleString("en-US")}</dd>
          </div>
          <div>
            <dt className="text-muted-foreground">Cache share</dt>
            <dd className="font-semibold tabular-nums">{share === null ? UNKNOWN : `${(share * 100).toFixed(1)}%`}</dd>
          </div>
          <div>
            <dt className="text-muted-foreground">Cache leverage</dt>
            <dd className="font-semibold tabular-nums">{leverage === null ? UNKNOWN : `${leverage.toFixed(2)}×`}</dd>
          </div>
        </dl>
      </section>

      <section aria-label="Cost">
        <SectionTitle>Cost</SectionTitle>
        <div className="flex flex-wrap items-center gap-1.5">
          <span className="text-[13px] font-semibold tabular-nums" title={`${entry.costMicros ?? UNKNOWN} micros`}>
            {formatCostMicros(entry.costMicros)}
          </span>
          <CostPill value={entry.costProvenance} />
        </div>
        {entry.costMicros === 0 && (
          <p className="mt-1 text-[10px] text-muted-foreground">Explicit zero: this call was known to be free.</p>
        )}
        {entry.costMicros === null && (
          <p className="mt-1 text-[10px] text-muted-foreground">Cost was not observed for this call.</p>
        )}
      </section>

      <section aria-label="Attribution">
        <SectionTitle>Attribution</SectionTitle>
        <div className="flex flex-wrap items-center gap-1.5">
          <AttributionPill value={entry.attributionConfidence} />
          <span className="text-[10px] text-muted-foreground">
            Attributed to {entry.attributedMissionId ?? "an unknown Mission"}
            {entry.attributedTaskId ? ` / ${entry.attributedTaskId}` : " / unknown task"}
          </span>
        </div>
      </section>

      <section aria-label="Reconciliation">
        <SectionTitle>Reconciliation</SectionTitle>
        <ReconciliationPill value={entry.reconciliation} />
      </section>

      {entry.note && (
        <p className="rounded-md border border-border bg-muted/20 px-2 py-1.5 text-[11px] text-muted-foreground">
          {entry.note}
        </p>
      )}
    </div>
  );
}
