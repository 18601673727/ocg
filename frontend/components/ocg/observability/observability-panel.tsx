"use client";

import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import {
  Activity,
  Clock3,
  Coins,
  Cpu,
  Gauge,
  GitBranch,
  Users,
  Wallet,
  Zap,
} from "lucide-react";
import { useMemo, useState } from "react";
import {
  UNKNOWN,
  formatCompactTokens,
  formatCostMicros,
  formatDollars,
  formatDuration,
  formatNumber,
  humanizeStatus,
  withApproximation,
} from "@/lib/format";
import { cn } from "@/lib/utils";
import {
  EmptyState,
  Metric,
  Pill,
  SectionTitle,
  StatusDot,
  WORKER_STATUS,
} from "@/components/ocg/primitives";
import type { JobExecution } from "../execution/domain";
import type { JobAccounting } from "../execution/accounting";
import {
  aggregateModelStats,
  aggregateProviderStats,
  boundActivities,
  deriveBudgetUsage,
  toChartableTimeline,
  type ModelStats,
  type RuntimeActivityItem,
  type RuntimeObservability,
  type UsageValue,
  type WorkerRuntimeStats,
} from "../runtime/observability";
import { restoreWorkerSelection, type InspectorTab } from "./inspector-state";

const CHART_ESTIMATED = "var(--chart-4)";
const CHART_REPORTED = "var(--chart-2)";
const CHART_BAR = "var(--chart-3)";
const CHART_SELECTED = "var(--primary)";
const CHART_GRID = "var(--border)";

/**
 * A usage figure carries its own provenance, so the `≈` marker for a modelled
 * number is added here rather than in a second formatter per surface.
 */
function formatTokenUsage(value?: UsageValue): string {
  return value
    ? withApproximation(formatCompactTokens(value.value), value.provenance === "estimated")
    : UNKNOWN;
}

function formatCostUsage(value?: UsageValue): string {
  return value
    ? withApproximation(formatCostMicros(value.value), value.provenance === "estimated")
    : UNKNOWN;
}

function TokenMetrics({ tokenUsage }: { tokenUsage: RuntimeObservability["job"]["tokenUsage"] }) {
  return (
    <div className="grid grid-cols-2 gap-1.5 sm:grid-cols-3 lg:grid-cols-2 2xl:grid-cols-3">
      <Metric label="Total tokens" value={formatTokenUsage(tokenUsage.total)} icon={Gauge} />
      <Metric label="Input" value={formatTokenUsage(tokenUsage.input)} icon={Activity} />
      <Metric label="Output" value={formatTokenUsage(tokenUsage.output)} icon={Activity} />
      <Metric label="Reasoning" value={formatTokenUsage(tokenUsage.reasoning)} icon={GitBranch} />
      <Metric label="Cache read" value={formatTokenUsage(tokenUsage.cacheRead)} icon={Zap} />
      <Metric label="Cache write" value={formatTokenUsage(tokenUsage.cacheWrite)} icon={Zap} />
    </div>
  );
}

function BudgetSection({ accounting, observability }: { accounting: JobAccounting | null; observability: RuntimeObservability }) {
  const budget = deriveBudgetUsage(accounting?.ceiling ?? null, observability.job.estimatedFinalSpend);
  return (
    <section className="rounded-md border border-border bg-muted/20 p-2.5" aria-label="Job budget">
      <div className="flex items-center gap-1.5">
        <Wallet className="size-3.5 text-muted-foreground" aria-hidden="true" />
        <h3 className="text-[11px] font-semibold tracking-wider text-muted-foreground uppercase">Budget</h3>
        <span className="ml-auto text-[10px] text-muted-foreground">hard limit</span>
      </div>
      {budget.limit === null ? (
        <p className="mt-2 text-[11px] text-muted-foreground">No hard budget ceiling is recorded for this Job.</p>
      ) : (
        <>
          <div className="mt-2 flex items-baseline justify-between gap-2">
            <span className="text-[16px] font-semibold tabular-nums">{formatDollars(budget.limit)}</span>
            <span className="text-[11px] text-muted-foreground">{budget.unit ?? "USD"} ceiling</span>
            <span className="ml-auto text-[10px] text-muted-foreground">{budget.source}</span>
          </div>
          <p className="mt-1 text-[10px] text-muted-foreground">Consumption is not reported by the control plane; the ceiling stays authoritative.</p>
          {budget.estimatedFinalSpend && <p className="mt-1 text-[10px] text-muted-foreground">Forecast is {budget.estimatedFinalSpend.provenance}; hard budget remains authoritative.</p>}
        </>
      )}
    </section>
  );
}

function CurrentRuntimeSummary({ observability }: { observability: RuntimeObservability }) {
  const current = observability.workers.find((worker) => worker.status === "active" || worker.status === "starting");
  return (
    <div className="rounded-md border border-border bg-muted/30 px-2.5 py-2">
      <div className="flex items-center gap-1.5">
        <Cpu className="size-3.5 text-muted-foreground" aria-hidden="true" />
        <span className="text-[11px] font-semibold tracking-wider text-muted-foreground uppercase">Current runtime</span>
      </div>
      {current ? (
        <div className="mt-1.5 min-w-0">
          <p className="text-[12px] font-medium">{current.label} · {current.variant ?? "standard"}</p>
          <p className="break-words text-[11px] text-muted-foreground">{current.provider} · {current.model}</p>
        </div>
      ) : <p className="mt-1.5 text-[11px] text-muted-foreground">No participant is currently active.</p>}
    </div>
  );
}

/** Canonical Call/Attempt counts, the only execution figures the projection states. */
function CallSummary({ execution }: { execution: JobExecution }) {
  const summary = execution.summary;
  return (
    <div className="grid grid-cols-2 gap-1.5 sm:grid-cols-4">
      <Metric label="Calls" value={`${summary.total}`} />
      <Metric label="Running" value={`${summary.running}`} />
      <Metric label="Settled" value={`${summary.completed}`} />
      <Metric label="Failed" value={`${summary.failed}`} />
    </div>
  );
}

function ActivityRow({ item }: { item: RuntimeActivityItem }) {
  return (
    <li className="flex items-start gap-2 border-b border-border/70 py-2 last:border-0">
      <span className="mt-0.5 shrink-0 font-mono text-[10px] text-muted-foreground">{item.timestamp}</span>
      <div className="min-w-0 flex-1">
        <p className="text-[11px] leading-4"><strong className="font-medium">{item.workerLabel ?? (item.role === "lead" ? "Lead" : "Job")}</strong> {item.summary}</p>
        {(item.provider || item.model) && <p className="break-words text-[10px] text-muted-foreground">{item.provider}{item.provider && item.model ? " · " : ""}{item.model}</p>}
      </div>
      {item.status && <Pill tone={WORKER_STATUS[item.status].tone} variant="quiet" dot pulse={WORKER_STATUS[item.status].pulse}>{humanizeStatus(item.status)}</Pill>}
    </li>
  );
}

function WorkerRow({ worker, selected, onSelect }: { worker: WorkerRuntimeStats; selected: boolean; onSelect: () => void }) {
  const total = worker.tokenUsage.total;
  return (
    <li>
      <button type="button" onClick={onSelect} aria-pressed={selected} className={cn("w-full rounded-md border px-2.5 py-2 text-left transition-colors", selected ? "border-foreground/40 bg-muted" : "border-border hover:bg-muted/50")}>
        <div className="flex items-start gap-2">
          <StatusDot tone={WORKER_STATUS[worker.status].tone} pulse={WORKER_STATUS[worker.status].pulse} className="mt-1" />
          <div className="min-w-0 flex-1">
            <div className="flex min-w-0 items-center gap-1.5">
              <p className="truncate text-[12px] font-medium">{worker.label}</p>
              {worker.role === "lead" && <span className="rounded border border-border px-1 text-[9px] text-muted-foreground">lead</span>}
            </div>
            <p className="break-words text-[10px] text-muted-foreground">{worker.provider} · {worker.model}{worker.variant ? ` · ${worker.variant}` : ""}</p>
          </div>
          <Pill tone={WORKER_STATUS[worker.status].tone} variant="quiet" dot pulse={WORKER_STATUS[worker.status].pulse}>{humanizeStatus(worker.status)}</Pill>
        </div>
        <div className="mt-1.5 grid grid-cols-2 gap-x-3 gap-y-1 pl-3.5 text-[10px] text-muted-foreground sm:grid-cols-4 lg:grid-cols-2 2xl:grid-cols-4">
          <span>tokens <strong className="font-medium text-foreground" title={total ? `${total.provenance} usage` : "Unavailable"}>{formatTokenUsage(total)}</strong></span>
          <span>calls <strong className="font-medium text-foreground">{worker.invocationCount}</strong>{worker.retryCount ? ` · ${worker.retryCount} retry` : ""}</span>
          <span>elapsed <strong className="font-medium text-foreground">{formatDuration(worker.elapsedMs)}</strong></span>
          <span>throughput <strong className="font-medium text-foreground">{worker.tokensPerSecond === undefined ? "—" : `${formatNumber(worker.tokensPerSecond)}/s`}</strong></span>
        </div>
      </button>
    </li>
  );
}

function WorkerDetail({ worker }: { worker: WorkerRuntimeStats }) {
  return (
    <section className="rounded-md border border-border bg-muted/20 p-2.5" aria-label={`${worker.label} worker detail`}>
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <p className="text-[13px] font-semibold">{worker.label}</p>
          <p className="text-[10px] uppercase tracking-wider text-muted-foreground">{worker.role} participant</p>
        </div>
        <Pill tone={WORKER_STATUS[worker.status].tone} variant="quiet" dot pulse={WORKER_STATUS[worker.status].pulse}>{humanizeStatus(worker.status)}</Pill>
      </div>
      <dl className="mt-2 grid grid-cols-2 gap-x-3 gap-y-2 text-[11px]">
        <div className="col-span-2"><dt className="text-muted-foreground">Provider</dt><dd className="break-words font-medium">{worker.provider}</dd></div>
        <div className="col-span-2"><dt className="text-muted-foreground">Model</dt><dd className="break-words font-medium">{worker.model}</dd></div>
        <div><dt className="text-muted-foreground">Variant / effort</dt><dd className="font-medium">{worker.variant ?? UNKNOWN}</dd></div>
        <div><dt className="text-muted-foreground">Elapsed</dt><dd className="font-medium">{formatDuration(worker.elapsedMs)}</dd></div>
        <div><dt className="text-muted-foreground">Started</dt><dd className="font-medium">{worker.startedAt ?? UNKNOWN}</dd></div>
        <div><dt className="text-muted-foreground">Finished</dt><dd className="font-medium">{worker.finishedAt ?? UNKNOWN}</dd></div>
        <div><dt className="text-muted-foreground">Invocations</dt><dd className="font-medium">{worker.invocationCount}</dd></div>
        <div><dt className="text-muted-foreground">Retries</dt><dd className="font-medium">{worker.retryCount}</dd></div>
        <div><dt className="text-muted-foreground">Success / failure</dt><dd className="font-medium">{worker.successCount ?? UNKNOWN} / {worker.failureCount ?? UNKNOWN}</dd></div>
        <div><dt className="text-muted-foreground">Cost</dt><dd className="font-medium">{formatCostUsage(worker.costMicros)}</dd></div>
        <div><dt className="text-muted-foreground">Latency / TTFT</dt><dd className="font-medium">{formatDuration(worker.latencyMs)} / {formatDuration(worker.ttftMs)}</dd></div>
        <div><dt className="text-muted-foreground">Throughput</dt><dd className="font-medium">{worker.tokensPerSecond === undefined ? "—" : `${formatNumber(worker.tokensPerSecond)}/s`}</dd></div>
      </dl>
      <div className="mt-2 border-t border-border pt-2"><p className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">Token breakdown</p><TokenMetrics tokenUsage={worker.tokenUsage} /></div>
    </section>
  );
}

function ChartFrame({ label, children }: { label: string; children: React.ReactNode }) {
  return <div className="relative h-40 min-h-[160px] min-w-0 w-full overflow-hidden" role="img" aria-label={label}>{children}</div>;
}

function TimelineChart({ observability }: { observability: RuntimeObservability }) {
  const data = useMemo(() => toChartableTimeline(observability.timeline), [observability.timeline]);
  const latest = data.at(-1);
  if (data.length === 0 || data.every((point) => point.total === null)) return <EmptyState className="px-2 py-4">Cumulative token usage is not available yet.</EmptyState>;
  return (
    <>
      <ChartFrame label={`Job cumulative token usage, ${data.length} points; latest ${latest?.total ?? "unavailable"} tokens`}>
        <ResponsiveContainer width="100%" height="100%" initialDimension={{ width: 320, height: 160 }} minWidth={48} minHeight={120} debounce={50}>
          <LineChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke={CHART_GRID} vertical={false} />
            <XAxis dataKey="timestamp" tick={{ fontSize: 9, fill: "var(--muted-foreground)" }} axisLine={false} tickLine={false} interval="preserveStartEnd" minTickGap={20} />
            <YAxis domain={[0, "auto"]} tick={{ fontSize: 9, fill: "var(--muted-foreground)" }} axisLine={false} tickLine={false} width={38} tickFormatter={(value) => formatCompactTokens(Number(value))} />
            <Tooltip formatter={(value, name) => [`${value ?? UNKNOWN} tokens`, name === "estimatedTotal" ? "Estimated" : name === "reportedTotal" ? "Reported" : "Cumulative"]} labelFormatter={(label) => `Elapsed ${label}`} />
            <Line type="monotone" dataKey="total" name="Cumulative" stroke={CHART_REPORTED} strokeWidth={2} dot={{ r: 2, fill: CHART_REPORTED }} activeDot={{ r: 4 }} connectNulls={false} isAnimationActive={false} />
            <Line type="monotone" dataKey="estimatedTotal" name="Estimated" stroke={CHART_ESTIMATED} strokeWidth={2} strokeDasharray="5 4" dot={false} connectNulls={false} isAnimationActive={false} />
            <Line type="monotone" dataKey="reportedTotal" name="Reported" stroke={CHART_REPORTED} strokeWidth={2} dot={false} connectNulls={false} isAnimationActive={false} />
          </LineChart>
        </ResponsiveContainer>
      </ChartFrame>
      <div className="mt-1 flex items-center justify-between gap-2 text-[10px] text-muted-foreground"><span>{data.length} bounded points · latest {formatTokenUsage(latest?.total === null || latest?.total === undefined ? undefined : { value: latest.total, provenance: latest.provenance ?? "reported" })}</span><span>— estimated · — reported</span></div>
    </>
  );
}

function WorkerBreakdown({ workers, selectedWorkerId }: { workers: WorkerRuntimeStats[]; selectedWorkerId: string | null }) {
  const data = workers.map((worker) => ({ name: worker.label, workerId: worker.workerId, tokens: worker.tokenUsage.total?.value ?? null })).filter((worker): worker is { name: string; workerId: string; tokens: number } => worker.tokens !== null);
  if (data.length === 0) return <EmptyState className="px-2 py-4">Worker token totals are unavailable.</EmptyState>;
  return (
    <>
      <ChartFrame label={`Worker resource comparison for ${data.length} participants`}>
        <ResponsiveContainer width="100%" height="100%" initialDimension={{ width: 320, height: 160 }} minWidth={48} minHeight={120} debounce={50}>
          <BarChart data={data} layout="vertical" margin={{ top: 2, right: 8, left: 4, bottom: 2 }}>
            <CartesianGrid strokeDasharray="3 3" stroke={CHART_GRID} horizontal={false} />
            <XAxis type="number" tick={{ fontSize: 9, fill: "var(--muted-foreground)" }} axisLine={false} tickLine={false} />
            <YAxis type="category" dataKey="name" width={76} tick={{ fontSize: 9, fill: "var(--muted-foreground)" }} axisLine={false} tickLine={false} />
            <Tooltip formatter={(value) => [`${value ?? UNKNOWN} tokens`, "Total"]} />
            <Bar dataKey="tokens" radius={[0, 3, 3, 0]} barSize={12} isAnimationActive={false}>
              {data.map((item) => <Cell key={item.workerId} fill={item.workerId === selectedWorkerId ? CHART_SELECTED : CHART_BAR} />)}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </ChartFrame>
      <p className="mt-1 text-[10px] text-muted-foreground">{data.length} participants with reported or estimated totals · select a worker in Runtime for detail.</p>
    </>
  );
}

type AggregateSelection = { kind: "provider" | "model"; key: string } | null;

function AggregateRow({ name, detail, tokens, calls, active, cost, success, failure, latency, selected, onSelect }: { name: string; detail?: string; tokens?: UsageValue; calls: number; active: number; cost?: UsageValue; success?: number; failure?: number; latency?: number; selected: boolean; onSelect: () => void }) {
  return (
    <li>
      <button type="button" onClick={onSelect} aria-pressed={selected} className={cn("flex w-full items-start gap-2 border-b border-border/70 px-1 py-2 text-left last:border-0", selected && "bg-muted/60")}>
        <div className="min-w-0 flex-1"><p className="break-words text-[11px] font-medium">{name}</p>{detail && <p className="break-words text-[10px] text-muted-foreground">{detail}</p>}</div>
        <span className="w-20 shrink-0 text-right text-[10px] text-muted-foreground" title={tokens ? `${tokens.provenance} usage` : "Unavailable"}>{formatTokenUsage(tokens)} · {calls} calls</span>
        <span className="w-14 shrink-0 text-right text-[10px] text-muted-foreground">{active} active</span>
        <span className="w-14 shrink-0 text-right text-[10px] text-muted-foreground">{formatCostUsage(cost)}</span>
      </button>
      {selected && <div className="border-b border-border/70 bg-muted/30 px-1 pb-2 text-[10px] text-muted-foreground">{success === undefined && failure === undefined ? "Success / failure unavailable" : `${success ?? UNKNOWN} successful · ${failure ?? UNKNOWN} failed`} · {latency === undefined ? "latency unavailable" : `${formatDuration(latency)} average latency`}</div>}
    </li>
  );
}

function AggregateDetail({ selection, providers, models }: { selection: AggregateSelection; providers: ReturnType<typeof aggregateProviderStats>; models: ModelStats[] }) {
  if (!selection) return <p className="mt-1 text-[10px] text-muted-foreground">Select a provider or model for its complete identity and metrics.</p>;
  const item = selection.kind === "provider" ? providers.find((provider) => provider.provider === selection.key) : models.find((model) => `${model.provider}\u0000${model.model}` === selection.key);
  if (!item) return null;
  const name = "model" in item ? `${item.provider} · ${item.model}` : item.provider;
  return <div className="mt-2 rounded-md border border-border bg-muted/20 p-2 text-[11px]" aria-label="Selected aggregate detail"><p className="break-words font-medium">{name}</p><div className="mt-1.5 grid grid-cols-2 gap-1.5 text-muted-foreground"><span>workers <strong className="text-foreground">{item.workerCount}</strong></span><span>active <strong className="text-foreground">{item.activeWorkers}</strong></span><span>calls <strong className="text-foreground">{item.invocationCount}</strong></span><span>retries <strong className="text-foreground">{item.retryCount}</strong></span><span>tokens <strong className="text-foreground">{formatTokenUsage(item.tokenUsage.total)}</strong></span><span>cost <strong className="text-foreground">{formatCostUsage(item.costMicros)}</strong></span><span>latency <strong className="text-foreground">{formatDuration(item.latencyMs)}</strong></span></div></div>;
}

function OverviewSurface({ execution, accounting, observability }: { execution: JobExecution; accounting: JobAccounting | null; observability: RuntimeObservability }) {
  const latestActivity = observability.activities.at(-1);
  const currentCall = execution.currentCall ?? execution.latestCall;
  return (
    <div className="flex flex-col gap-3">
      <div className="grid grid-cols-2 gap-1.5"><Metric label="Tokens" value={formatTokenUsage(observability.job.tokenUsage.total)} icon={Gauge} /><Metric label="Cost" value={formatCostUsage(observability.job.costMicros)} icon={Coins} /><Metric label="Elapsed" value={formatDuration(observability.job.elapsedMs)} icon={Clock3} /><Metric label="Active workers" value={`${observability.job.activeWorkerCount}`} icon={Users} /></div>
      <p className="text-[12px] leading-5 text-muted-foreground"><span className="font-medium text-foreground">{execution.jobId}</span> · {execution.projectId} · {humanizeStatus(execution.state)}</p>
      <CurrentRuntimeSummary observability={observability} />
      <BudgetSection accounting={accounting} observability={observability} />
      <section><SectionTitle detail={execution.progress ? `${execution.progress.settled}/${execution.progress.total}` : `${execution.summary.total}`}>Calls</SectionTitle><CallSummary execution={execution} /></section>
      <section><SectionTitle>Current call</SectionTitle><p className="rounded-md border border-border bg-muted/30 px-2.5 py-2 text-[12px] font-medium">{currentCall ? `Call ${currentCall.callId} · ${currentCall.effectKind} · ${humanizeStatus(currentCall.status)}` : "No Call has been admitted for this Job yet."}</p></section>
      {latestActivity && <section><SectionTitle detail="latest">Runtime activity</SectionTitle><ul className="rounded-md border border-border px-2"><ActivityRow item={latestActivity} /></ul></section>}
    </div>
  );
}

function RuntimeSurface({ observability }: { observability: RuntimeObservability }) {
  const [selectedWorkerId, setSelectedWorkerId] = useState<string | null>(observability.workers.find((worker) => worker.status === "active")?.workerId ?? observability.workers[0]?.workerId ?? null);
  const selectedId = restoreWorkerSelection(selectedWorkerId, observability.workers.map((worker) => worker.workerId)) ?? observability.workers.find((worker) => worker.status === "active")?.workerId ?? observability.workers[0]?.workerId ?? null;
  const selectedWorker = observability.workers.find((worker) => worker.workerId === selectedId);
  const activities = boundActivities(observability.activities);
  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-center justify-between"><SectionTitle detail={`${observability.workers.length} participants`}>Runtime participants</SectionTitle><span className="text-[10px] text-muted-foreground">Lead included</span></div>
      <ul className="flex flex-col gap-1.5">{observability.workers.map((worker) => <WorkerRow key={worker.workerId} worker={worker} selected={selectedId === worker.workerId} onSelect={() => setSelectedWorkerId(worker.workerId)} />)}</ul>
      {selectedWorker && <WorkerDetail worker={selectedWorker} />}
      <section><SectionTitle detail={`${activities.length} bounded events`}>Live activity</SectionTitle><ul className="rounded-md border border-border px-2">{activities.length > 0 ? activities.slice().reverse().map((item) => <ActivityRow key={item.id} item={item} />) : <li className="py-3 text-[11px] text-muted-foreground">No normalized runtime activity yet.</li>}</ul></section>
    </div>
  );
}

function UsageSurface({ accounting, observability }: { accounting: JobAccounting | null; observability: RuntimeObservability }) {
  const [selection, setSelection] = useState<AggregateSelection>(null);
  const providers = useMemo(() => aggregateProviderStats(observability.workers), [observability.workers]);
  const models = useMemo(() => aggregateModelStats(observability.workers), [observability.workers]);
  return (
    <div className="flex flex-col gap-4">
      <BudgetSection accounting={accounting} observability={observability} />
      <section><SectionTitle detail="cumulative · bounded history">Job token timeline</SectionTitle><TimelineChart observability={observability} /></section>
      <section><SectionTitle detail="tokens by participant">Worker resource breakdown</SectionTitle><WorkerBreakdown workers={observability.workers} selectedWorkerId={null} /></section>
      <section><SectionTitle>Token breakdown</SectionTitle><TokenMetrics tokenUsage={observability.job.tokenUsage} /></section>
      <section><SectionTitle detail="select for detail">Provider statistics</SectionTitle><ul className="rounded-md border border-border px-2">{providers.map((provider) => <AggregateRow key={provider.provider} name={provider.provider} tokens={provider.tokenUsage.total} calls={provider.invocationCount} active={provider.activeWorkers} cost={provider.costMicros} success={provider.successCount} failure={provider.failureCount} latency={provider.latencyMs} selected={selection?.kind === "provider" && selection.key === provider.provider} onSelect={() => setSelection({ kind: "provider", key: provider.provider })} />)}</ul></section>
      <section><SectionTitle detail="select for detail">Model statistics</SectionTitle><ul className="rounded-md border border-border px-2">{models.map((model) => <AggregateRow key={`${model.provider}-${model.model}`} name={model.model} detail={model.provider} tokens={model.tokenUsage.total} calls={model.invocationCount} active={model.activeWorkers} cost={model.costMicros} success={model.successCount} failure={model.failureCount} latency={model.latencyMs} selected={selection?.kind === "model" && selection.key === `${model.provider}\u0000${model.model}`} onSelect={() => setSelection({ kind: "model", key: `${model.provider}\u0000${model.model}` })} />)}</ul><AggregateDetail selection={selection} providers={providers} models={models} /></section>
    </div>
  );
}

export function ObservabilityPanel({ execution, accounting, observability, tab }: { execution: JobExecution; accounting: JobAccounting | null; observability: RuntimeObservability; tab: InspectorTab }) {
  if (tab === "overview") return <OverviewSurface execution={execution} accounting={accounting} observability={observability} />;
  if (tab === "runtime") return <RuntimeSurface observability={observability} />;
  return <UsageSurface accounting={accounting} observability={observability} />;
}
