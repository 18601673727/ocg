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
  AlertCircle,
  Check,
  Clock3,
  Coins,
  Cpu,
  Gauge,
  GitBranch,
  RefreshCw,
  Users,
  Wallet,
  Zap,
} from "lucide-react";
import { useMemo, useState } from "react";
import { cn } from "@/lib/utils";
import type { Mission, MissionTask, WorkerStatus } from "../types";
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
import type { InspectorMode, InspectorTab } from "./inspector-state";
import { restoreWorkerSelection } from "./inspector-state";

const CHART_ESTIMATED = "var(--chart-4)";
const CHART_REPORTED = "var(--chart-2)";
const CHART_BAR = "var(--chart-3)";
const CHART_SELECTED = "var(--primary)";
const CHART_GRID = "var(--border)";

export type MissionInspectorProps = {
  mission: Mission;
  observability: RuntimeObservability;
  mode: InspectorMode;
  tab: InspectorTab;
  onTabChange: (tab: InspectorTab) => void;
};

function formatTokens(value?: UsageValue): string {
  if (!value) return "—";
  const amount = value.value >= 1000
    ? `${(value.value / 1000).toFixed(value.value >= 10000 ? 1 : 2)}K`
    : String(value.value);
  return value.provenance === "estimated" ? `≈ ${amount}` : amount;
}

function formatCostMicros(value?: UsageValue): string {
  if (!value) return "—";
  const amount = `$${(value.value / 1_000_000).toFixed(3)}`;
  return value.provenance === "estimated" ? `≈ ${amount}` : amount;
}

function formatDollars(value?: UsageValue): string {
  if (!value) return "—";
  const amount = `$${value.value.toFixed(2)}`;
  return value.provenance === "estimated" ? `≈ ${amount}` : amount;
}

function formatDuration(milliseconds?: number): string {
  if (milliseconds === undefined) return "—";
  if (milliseconds < 1000) return `${milliseconds}ms`;
  const seconds = milliseconds / 1000;
  return seconds < 60
    ? `${seconds.toFixed(1)}s`
    : `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
}

function formatNumber(value?: number): string {
  return value === undefined
    ? "—"
    : new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 }).format(value);
}

function formatStatus(status: WorkerStatus): string {
  return status.replace("-", " ");
}

function statusTone(status: WorkerStatus): string {
  switch (status) {
    case "active": return "bg-amber-500";
    case "starting": return "bg-sky-500";
    case "queued": return "bg-slate-400";
    case "waiting": return "bg-violet-500";
    case "completed": return "bg-emerald-500";
    case "failed": return "bg-red-500";
    case "cancelled": return "bg-slate-500";
    default: return "bg-muted-foreground/40";
  }
}

function StatusPill({ status }: { status: WorkerStatus }) {
  return (
    <span className="inline-flex items-center gap-1.5 rounded-full border border-border bg-muted/40 px-1.5 py-0.5 text-[10px] capitalize text-muted-foreground">
      <span className={cn("size-1.5 rounded-full", statusTone(status), status === "active" && "animate-pulse")} aria-hidden="true" />
      {formatStatus(status)}
    </span>
  );
}

function Metric({ label, value, icon: Icon, detail }: { label: string; value: string; icon: typeof Activity; detail?: string }) {
  return (
    <div className="min-w-0 rounded-md border border-border bg-background px-2 py-1.5">
      <div className="flex items-center gap-1 text-[10px] text-muted-foreground">
        <Icon className="size-3" aria-hidden="true" />
        <span className="truncate">{label}</span>
      </div>
      <p className="mt-0.5 truncate text-[13px] font-semibold tabular-nums">{value}</p>
      {detail && <p className="truncate text-[10px] text-muted-foreground">{detail}</p>}
    </div>
  );
}

function SectionTitle({ children, detail }: { children: React.ReactNode; detail?: string }) {
  return (
    <div className="mb-1.5 flex items-center gap-1.5">
      <h3 className="text-[11px] font-semibold tracking-wider text-muted-foreground uppercase">{children}</h3>
      {detail && <span className="text-[10px] text-muted-foreground">{detail}</span>}
    </div>
  );
}

function TokenMetrics({ tokenUsage }: { tokenUsage: RuntimeObservability["mission"]["tokenUsage"] }) {
  return (
    <div className="grid grid-cols-2 gap-1.5 sm:grid-cols-3 lg:grid-cols-2 2xl:grid-cols-3">
      <Metric label="Total tokens" value={formatTokens(tokenUsage.total)} icon={Gauge} />
      <Metric label="Input" value={formatTokens(tokenUsage.input)} icon={Activity} />
      <Metric label="Output" value={formatTokens(tokenUsage.output)} icon={Activity} />
      <Metric label="Reasoning" value={formatTokens(tokenUsage.reasoning)} icon={GitBranch} />
      <Metric label="Cache read" value={formatTokens(tokenUsage.cacheRead)} icon={Zap} />
      <Metric label="Cache write" value={formatTokens(tokenUsage.cacheWrite)} icon={Zap} />
    </div>
  );
}

function BudgetSection({ mission, observability }: { mission: Mission; observability: RuntimeObservability }) {
  const budget = deriveBudgetUsage(
    mission.budget,
    observability.mission.elapsedMs,
    observability.mission.estimatedFinalSpend,
  );
  return (
    <section className="rounded-md border border-border bg-muted/20 p-2.5" aria-label="Mission budget">
      <div className="flex items-center gap-1.5">
        <Wallet className="size-3.5 text-muted-foreground" aria-hidden="true" />
        <h3 className="text-[11px] font-semibold tracking-wider text-muted-foreground uppercase">Budget</h3>
        <span className="ml-auto text-[10px] text-muted-foreground">hard limit</span>
      </div>
      <div className="mt-2 flex items-baseline justify-between gap-2">
        <span className="text-[16px] font-semibold tabular-nums">${budget.spent.toFixed(2)}</span>
        <span className="text-[11px] text-muted-foreground">of ${budget.limit.toFixed(2)}</span>
        <span className="ml-auto text-[11px] font-medium tabular-nums">{budget.percent.toFixed(0)}%</span>
      </div>
      <div className="mt-1.5 h-1.5 overflow-hidden rounded-full bg-muted" role="progressbar" aria-label="Budget consumed" aria-valuemin={0} aria-valuemax={100} aria-valuenow={budget.percent}>
        <div className={cn("h-full rounded-full transition-[width] duration-300", budget.percent >= 90 ? "bg-red-500" : "bg-foreground")} style={{ width: `${budget.percent}%` }} />
      </div>
      <div className="mt-2 grid grid-cols-2 gap-2 text-[10px] text-muted-foreground">
        <span>remaining <strong className="font-medium text-foreground">${budget.remaining.toFixed(2)}</strong></span>
        <span className="text-right">burn <strong className="font-medium text-foreground">{budget.burnRatePerMinute === undefined ? "—" : `$${budget.burnRatePerMinute.toFixed(2)}/min`}</strong></span>
        <span>forecast <strong className="font-medium text-foreground">{formatDollars(budget.estimatedFinalSpend)}</strong></span>
        <span className="text-right">commitment <strong className="font-medium text-foreground">{mission.commitment.workers} {mission.commitment.mode}</strong></span>
      </div>
      {budget.estimatedFinalSpend && <p className="mt-1 text-[10px] text-muted-foreground">Forecast is {budget.estimatedFinalSpend.provenance}; hard budget remains authoritative.</p>}
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

function TaskList({ tasks }: { tasks: MissionTask[] }) {
  return (
    <ul className="flex flex-col">
      {tasks.map((task) => (
        <li key={task.id} className={cn("flex items-center gap-2 rounded-md px-2 py-1.5 text-[12px]", task.status === "active" && "bg-muted font-medium", task.status !== "active" && "text-muted-foreground", task.status === "failed" && "text-red-600 dark:text-red-400")}>
          {task.status === "completed" ? <Check className="size-3.5 text-emerald-600" aria-hidden="true" /> : task.status === "active" ? <RefreshCw className="size-3.5 animate-spin text-foreground" aria-hidden="true" /> : task.status === "failed" ? <AlertCircle className="size-3.5 text-red-500" aria-hidden="true" /> : <span className="size-3.5 rounded-full border border-muted-foreground/40" aria-hidden="true" />}
          <span className={cn("min-w-0 flex-1 truncate", task.status === "completed" && "line-through decoration-muted-foreground/50")}>{task.title}</span>
          {task.status === "active" && <span className="rounded border border-border bg-background px-1 text-[9px]">now</span>}
        </li>
      ))}
    </ul>
  );
}

function ActivityRow({ item }: { item: RuntimeActivityItem }) {
  return (
    <li className="flex items-start gap-2 border-b border-border/70 py-2 last:border-0">
      <span className="mt-0.5 shrink-0 font-mono text-[10px] text-muted-foreground">{item.timestamp}</span>
      <div className="min-w-0 flex-1">
        <p className="text-[11px] leading-4"><strong className="font-medium">{item.workerLabel ?? (item.role === "lead" ? "Lead" : "Mission")}</strong> {item.summary}</p>
        {(item.provider || item.model) && <p className="break-words text-[10px] text-muted-foreground">{item.provider}{item.provider && item.model ? " · " : ""}{item.model}</p>}
      </div>
      {item.status && <StatusPill status={item.status} />}
    </li>
  );
}

function WorkerRow({ worker, selected, onSelect }: { worker: WorkerRuntimeStats; selected: boolean; onSelect: () => void }) {
  const total = worker.tokenUsage.total;
  return (
    <li>
      <button type="button" onClick={onSelect} aria-pressed={selected} className={cn("w-full rounded-md border px-2.5 py-2 text-left transition-colors", selected ? "border-foreground/40 bg-muted" : "border-border hover:bg-muted/50")}>
        <div className="flex items-start gap-2">
          <span className={cn("mt-1 size-1.5 shrink-0 rounded-full", statusTone(worker.status), worker.status === "active" && "animate-pulse")} aria-hidden="true" />
          <div className="min-w-0 flex-1">
            <div className="flex min-w-0 items-center gap-1.5">
              <p className="truncate text-[12px] font-medium">{worker.label}</p>
              {worker.role === "lead" && <span className="rounded border border-border px-1 text-[9px] text-muted-foreground">lead</span>}
            </div>
            <p className="break-words text-[10px] text-muted-foreground">{worker.provider} · {worker.model}{worker.variant ? ` · ${worker.variant}` : ""}</p>
          </div>
          <StatusPill status={worker.status} />
        </div>
        <div className="mt-1.5 grid grid-cols-2 gap-x-3 gap-y-1 pl-3.5 text-[10px] text-muted-foreground sm:grid-cols-4 lg:grid-cols-2 2xl:grid-cols-4">
          <span>tokens <strong className="font-medium text-foreground" title={total ? `${total.provenance} usage` : "Unavailable"}>{formatTokens(total)}</strong></span>
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
        <StatusPill status={worker.status} />
      </div>
      <dl className="mt-2 grid grid-cols-2 gap-x-3 gap-y-2 text-[11px]">
        <div className="col-span-2"><dt className="text-muted-foreground">Provider</dt><dd className="break-words font-medium">{worker.provider}</dd></div>
        <div className="col-span-2"><dt className="text-muted-foreground">Model</dt><dd className="break-words font-medium">{worker.model}</dd></div>
        <div><dt className="text-muted-foreground">Variant / effort</dt><dd className="font-medium">{worker.variant ?? "—"}</dd></div>
        <div><dt className="text-muted-foreground">Elapsed</dt><dd className="font-medium">{formatDuration(worker.elapsedMs)}</dd></div>
        <div><dt className="text-muted-foreground">Started</dt><dd className="font-medium">{worker.startedAt ?? "—"}</dd></div>
        <div><dt className="text-muted-foreground">Finished</dt><dd className="font-medium">{worker.finishedAt ?? "—"}</dd></div>
        <div><dt className="text-muted-foreground">Invocations</dt><dd className="font-medium">{worker.invocationCount}</dd></div>
        <div><dt className="text-muted-foreground">Retries</dt><dd className="font-medium">{worker.retryCount}</dd></div>
        <div><dt className="text-muted-foreground">Success / failure</dt><dd className="font-medium">{worker.successCount ?? "—"} / {worker.failureCount ?? "—"}</dd></div>
        <div><dt className="text-muted-foreground">Cost</dt><dd className="font-medium">{formatCostMicros(worker.costMicros)}</dd></div>
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
  if (data.length === 0 || data.every((point) => point.total === null)) return <p className="rounded-md border border-dashed border-border px-2 py-4 text-[11px] text-muted-foreground">Cumulative token usage is not available yet.</p>;
  return (
    <>
      <ChartFrame label={`Mission cumulative token usage, ${data.length} points; latest ${latest?.total ?? "unavailable"} tokens`}>
        <ResponsiveContainer width="100%" height="100%" initialDimension={{ width: 320, height: 160 }} minWidth={48} minHeight={120} debounce={50}>
          <LineChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke={CHART_GRID} vertical={false} />
            <XAxis dataKey="timestamp" tick={{ fontSize: 9, fill: "var(--muted-foreground)" }} axisLine={false} tickLine={false} interval="preserveStartEnd" minTickGap={20} />
            <YAxis domain={[0, "auto"]} tick={{ fontSize: 9, fill: "var(--muted-foreground)" }} axisLine={false} tickLine={false} width={38} tickFormatter={(value) => formatTokens({ value: Number(value), provenance: "reported" })} />
            <Tooltip formatter={(value, name) => [`${value ?? "—"} tokens`, name === "estimatedTotal" ? "Estimated" : name === "reportedTotal" ? "Reported" : "Cumulative"]} labelFormatter={(label) => `Elapsed ${label}`} />
            <Line type="monotone" dataKey="total" name="Cumulative" stroke={CHART_REPORTED} strokeWidth={2} dot={{ r: 2, fill: CHART_REPORTED }} activeDot={{ r: 4 }} connectNulls={false} isAnimationActive={false} />
            <Line type="monotone" dataKey="estimatedTotal" name="Estimated" stroke={CHART_ESTIMATED} strokeWidth={2} strokeDasharray="5 4" dot={false} connectNulls={false} isAnimationActive={false} />
            <Line type="monotone" dataKey="reportedTotal" name="Reported" stroke={CHART_REPORTED} strokeWidth={2} dot={false} connectNulls={false} isAnimationActive={false} />
          </LineChart>
        </ResponsiveContainer>
      </ChartFrame>
      <div className="mt-1 flex items-center justify-between gap-2 text-[10px] text-muted-foreground"><span>{data.length} bounded points · latest {formatTokens(latest?.total === null || latest?.total === undefined ? undefined : { value: latest.total, provenance: latest.provenance ?? "reported" })}</span><span>— estimated · — reported</span></div>
    </>
  );
}

function WorkerBreakdown({ workers, selectedWorkerId }: { workers: WorkerRuntimeStats[]; selectedWorkerId: string | null }) {
  const data = workers.map((worker) => ({ name: worker.label, workerId: worker.workerId, tokens: worker.tokenUsage.total?.value ?? null })).filter((worker): worker is { name: string; workerId: string; tokens: number } => worker.tokens !== null);
  if (data.length === 0) return <p className="rounded-md border border-dashed border-border px-2 py-4 text-[11px] text-muted-foreground">Worker token totals are unavailable.</p>;
  return (
    <>
      <ChartFrame label={`Worker resource comparison for ${data.length} participants`}>
        <ResponsiveContainer width="100%" height="100%" initialDimension={{ width: 320, height: 160 }} minWidth={48} minHeight={120} debounce={50}>
          <BarChart data={data} layout="vertical" margin={{ top: 2, right: 8, left: 4, bottom: 2 }}>
            <CartesianGrid strokeDasharray="3 3" stroke={CHART_GRID} horizontal={false} />
            <XAxis type="number" tick={{ fontSize: 9, fill: "var(--muted-foreground)" }} axisLine={false} tickLine={false} />
            <YAxis type="category" dataKey="name" width={76} tick={{ fontSize: 9, fill: "var(--muted-foreground)" }} axisLine={false} tickLine={false} />
            <Tooltip formatter={(value) => [`${value ?? "—"} tokens`, "Total"]} />
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
        <span className="w-20 shrink-0 text-right text-[10px] text-muted-foreground" title={tokens ? `${tokens.provenance} usage` : "Unavailable"}>{formatTokens(tokens)} · {calls} calls</span>
        <span className="w-14 shrink-0 text-right text-[10px] text-muted-foreground">{active} active</span>
        <span className="w-14 shrink-0 text-right text-[10px] text-muted-foreground">{formatCostMicros(cost)}</span>
      </button>
      {selected && <div className="border-b border-border/70 bg-muted/30 px-1 pb-2 text-[10px] text-muted-foreground">{success === undefined && failure === undefined ? "Success / failure unavailable" : `${success ?? "—"} successful · ${failure ?? "—"} failed`} · {latency === undefined ? "latency unavailable" : `${formatDuration(latency)} average latency`}</div>}
    </li>
  );
}

function AggregateDetail({ selection, providers, models }: { selection: AggregateSelection; providers: ReturnType<typeof aggregateProviderStats>; models: ModelStats[] }) {
  if (!selection) return <p className="mt-1 text-[10px] text-muted-foreground">Select a provider or model for its complete identity and metrics.</p>;
  const item = selection.kind === "provider" ? providers.find((provider) => provider.provider === selection.key) : models.find((model) => `${model.provider}\u0000${model.model}` === selection.key);
  if (!item) return null;
  const name = "model" in item ? `${item.provider} · ${item.model}` : item.provider;
  return <div className="mt-2 rounded-md border border-border bg-muted/20 p-2 text-[11px]" aria-label="Selected aggregate detail"><p className="break-words font-medium">{name}</p><div className="mt-1.5 grid grid-cols-2 gap-1.5 text-muted-foreground"><span>workers <strong className="text-foreground">{item.workerCount}</strong></span><span>active <strong className="text-foreground">{item.activeWorkers}</strong></span><span>calls <strong className="text-foreground">{item.invocationCount}</strong></span><span>retries <strong className="text-foreground">{item.retryCount}</strong></span><span>tokens <strong className="text-foreground">{formatTokens(item.tokenUsage.total)}</strong></span><span>cost <strong className="text-foreground">{formatCostMicros(item.costMicros)}</strong></span><span>latency <strong className="text-foreground">{formatDuration(item.latencyMs)}</strong></span></div></div>;
}

function OverviewSurface({ mission, observability }: { mission: Mission; observability: RuntimeObservability }) {
  const latestActivity = observability.activities.at(-1);
  return (
    <div className="flex flex-col gap-3">
      <div className="grid grid-cols-2 gap-1.5"><Metric label="Tokens" value={formatTokens(observability.mission.tokenUsage.total)} icon={Gauge} /><Metric label="Cost" value={formatCostMicros(observability.mission.costMicros)} icon={Coins} /><Metric label="Elapsed" value={formatDuration(observability.mission.elapsedMs)} icon={Clock3} /><Metric label="Active workers" value={`${observability.mission.activeWorkerCount}`} icon={Users} /></div>
      <p className="text-[12px] leading-5 text-muted-foreground">{mission.goal}</p>
      <CurrentRuntimeSummary observability={observability} />
      <BudgetSection mission={mission} observability={observability} />
      <section><SectionTitle detail={`${mission.completed}/${mission.total}`}>Tasks</SectionTitle><TaskList tasks={mission.tasks} /></section>
      <section><SectionTitle>Current task</SectionTitle><p className="rounded-md border border-border bg-muted/30 px-2.5 py-2 text-[12px] font-medium">{mission.current}</p></section>
      {latestActivity && <section><SectionTitle detail="latest">Runtime activity</SectionTitle><ul className="rounded-md border border-border px-2"><ActivityRow item={latestActivity} /></ul></section>}
      {mission.warnings.length > 0 && <div className="rounded-md border border-amber-500/30 bg-amber-500/5 px-2.5 py-2 text-[11px] text-muted-foreground">{mission.warnings.map((warning) => <p key={warning}>{warning}</p>)}</div>}
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

function UsageSurface({ mission, observability }: { mission: Mission; observability: RuntimeObservability }) {
  const [selection, setSelection] = useState<AggregateSelection>(null);
  const providers = useMemo(() => aggregateProviderStats(observability.workers), [observability.workers]);
  const models = useMemo(() => aggregateModelStats(observability.workers), [observability.workers]);
  return (
    <div className="flex flex-col gap-4">
      <BudgetSection mission={mission} observability={observability} />
      <section><SectionTitle detail="cumulative · bounded history">Mission token timeline</SectionTitle><TimelineChart observability={observability} /></section>
      <section><SectionTitle detail="tokens by participant">Worker resource breakdown</SectionTitle><WorkerBreakdown workers={observability.workers} selectedWorkerId={null} /></section>
      <section><SectionTitle>Token breakdown</SectionTitle><TokenMetrics tokenUsage={observability.mission.tokenUsage} /></section>
      <section><SectionTitle detail="select for detail">Provider statistics</SectionTitle><ul className="rounded-md border border-border px-2">{providers.map((provider) => <AggregateRow key={provider.provider} name={provider.provider} tokens={provider.tokenUsage.total} calls={provider.invocationCount} active={provider.activeWorkers} cost={provider.costMicros} success={provider.successCount} failure={provider.failureCount} latency={provider.latencyMs} selected={selection?.kind === "provider" && selection.key === provider.provider} onSelect={() => setSelection({ kind: "provider", key: provider.provider })} />)}</ul></section>
      <section><SectionTitle detail="select for detail">Model statistics</SectionTitle><ul className="rounded-md border border-border px-2">{models.map((model) => <AggregateRow key={`${model.provider}-${model.model}`} name={model.model} detail={model.provider} tokens={model.tokenUsage.total} calls={model.invocationCount} active={model.activeWorkers} cost={model.costMicros} success={model.successCount} failure={model.failureCount} latency={model.latencyMs} selected={selection?.kind === "model" && selection.key === `${model.provider}\u0000${model.model}`} onSelect={() => setSelection({ kind: "model", key: `${model.provider}\u0000${model.model}` })} />)}</ul><AggregateDetail selection={selection} providers={providers} models={models} /></section>
    </div>
  );
}

export function ObservabilityPanel({ mission, observability, tab }: { mission: Mission; observability: RuntimeObservability; tab: InspectorTab }) {
  if (tab === "overview") return <OverviewSurface mission={mission} observability={observability} />;
  if (tab === "runtime") return <RuntimeSurface observability={observability} />;
  return <UsageSurface mission={mission} observability={observability} />;
}
