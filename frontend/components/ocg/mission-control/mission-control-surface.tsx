"use client";

import {
  Activity,
  AlertTriangle,
  ArrowDownToLine,
  ArrowRight,
  Check,
  CircleDot,
  Clock3,
  Code2,
  Crosshair,
  GitBranch,
  GitCommitHorizontal,
  Layers3,
  Maximize2,
  Minus,
  PanelRight,
  Plus,
  RefreshCw,
  Search,
  ShieldCheck,
  Timer,
  Users,
  Wallet,
  X,
  Zap,
} from "lucide-react";
import { useMemo, useRef, useState } from "react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  dependencyClosure,
  dependentClosure,
  deriveExecutionGraph,
  filterExecutionTasks,
  formatExecutionStatus,
  type ExecutionActivityItem,
  type ExecutionStatus,
  type ExecutionTask,
  type MissionExecution,
} from "../execution/domain";

type MissionControlTab = "overview" | "graph" | "tasks" | "workers" | "activity";
type FocusMode = "all" | "running" | "blocked" | "critical";

const TABS: { id: MissionControlTab; label: string; icon: typeof Activity }[] = [
  { id: "overview", label: "Overview", icon: CircleDot },
  { id: "graph", label: "Graph", icon: GitBranch },
  { id: "tasks", label: "Tasks", icon: Layers3 },
  { id: "workers", label: "Workers", icon: Users },
  { id: "activity", label: "Activity", icon: Activity },
];

const STATUS_TONE: Record<ExecutionStatus, { dot: string; border: string; text: string }> = {
  planned: { dot: "bg-slate-400", border: "border-slate-300 dark:border-slate-700", text: "text-slate-600 dark:text-slate-300" },
  queued: { dot: "bg-slate-400", border: "border-slate-300 dark:border-slate-700", text: "text-slate-600 dark:text-slate-300" },
  ready: { dot: "bg-sky-500", border: "border-sky-500/40", text: "text-sky-700 dark:text-sky-300" },
  starting: { dot: "bg-sky-500", border: "border-sky-500/40", text: "text-sky-700 dark:text-sky-300" },
  running: { dot: "bg-amber-500", border: "border-amber-500/50", text: "text-amber-700 dark:text-amber-300" },
  waiting: { dot: "bg-violet-500", border: "border-violet-500/40", text: "text-violet-700 dark:text-violet-300" },
  blocked: { dot: "bg-red-500", border: "border-red-500/50", text: "text-red-700 dark:text-red-300" },
  verifying: { dot: "bg-cyan-500", border: "border-cyan-500/50", text: "text-cyan-700 dark:text-cyan-300" },
  retrying: { dot: "bg-orange-500", border: "border-orange-500/50", text: "text-orange-700 dark:text-orange-300" },
  completed: { dot: "bg-emerald-500", border: "border-emerald-500/40", text: "text-emerald-700 dark:text-emerald-300" },
  failed: { dot: "bg-red-500", border: "border-red-500/50", text: "text-red-700 dark:text-red-300" },
  cancelled: { dot: "bg-slate-500", border: "border-slate-500/40", text: "text-slate-600 dark:text-slate-300" },
  skipped: { dot: "bg-slate-400", border: "border-slate-300 dark:border-slate-700", text: "text-slate-600 dark:text-slate-300" },
};

function formatDuration(milliseconds?: number): string {
  if (milliseconds === undefined) return "—";
  const seconds = Math.round(milliseconds / 1000);
  if (seconds < 60) return `${seconds}s`;
  return `${Math.floor(seconds / 60)}m ${String(seconds % 60).padStart(2, "0")}s`;
}

function formatCost(value?: number): string {
  return value === undefined ? "—" : `$${value.toFixed(2)}`;
}

function StatusPill({ status }: { status: ExecutionStatus }) {
  const tone = STATUS_TONE[status];
  return (
    <span className={cn("inline-flex items-center gap-1.5 rounded-full border bg-muted/30 px-1.5 py-0.5 text-[10px] capitalize", tone.border, tone.text)}>
      <span className={cn("size-1.5 rounded-full", tone.dot, ["running", "starting"].includes(status) && "animate-pulse")} aria-hidden="true" />
      {formatExecutionStatus(status)}
    </span>
  );
}

function Metric({ label, value, detail, icon: Icon }: { label: string; value: string; detail?: string; icon: typeof Activity }) {
  return (
    <div className="min-w-0 border-l border-border pl-3 first:border-l-0 first:pl-0">
      <div className="flex items-center gap-1 text-[10px] text-muted-foreground"><Icon className="size-3" aria-hidden="true" /><span>{label}</span></div>
      <p className="mt-0.5 truncate text-[14px] font-semibold tabular-nums">{value}</p>
      {detail && <p className="truncate text-[10px] text-muted-foreground">{detail}</p>}
    </div>
  );
}

function SectionTitle({ children, detail }: { children: React.ReactNode; detail?: string }) {
  return <div className="mb-2 flex items-center gap-2"><h2 className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">{children}</h2>{detail && <span className="text-[10px] text-muted-foreground">{detail}</span>}</div>;
}

function TaskLabel({ task, compact = false }: { task: ExecutionTask; compact?: boolean }) {
  return <span className={cn("inline-flex min-w-0 items-center gap-1.5", compact && "text-[11px]")}><span className={cn("size-1.5 shrink-0 rounded-full", STATUS_TONE[task.status].dot)} aria-hidden="true" /><span className="truncate">{task.title}</span>{task.kind === "gate" && <ShieldCheck className="size-3 shrink-0 text-cyan-600" aria-label="Gate task" />}</span>;
}

function WaveStrip({ execution }: { execution: MissionExecution }) {
  return (
    <div className="grid gap-1.5 sm:grid-cols-3 xl:grid-cols-6" aria-label="Mission waves">
      {execution.waves.map((wave) => {
        const tasks = execution.tasks.filter((task) => wave.taskIds.includes(task.id));
        const running = tasks.filter((task) => ["running", "starting", "verifying", "retrying"].includes(task.status)).length;
        return (
          <div key={wave.index} className={cn("rounded-md border px-2 py-2", wave.index === execution.currentWave ? "border-foreground/30 bg-muted/50" : "border-border bg-background")}>
            <div className="flex items-center justify-between gap-2"><span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">Wave {wave.index}</span>{wave.index === execution.currentWave && <span className="text-[9px] font-medium text-amber-600">current</span>}</div>
            <p className="mt-1 text-[11px] font-medium">{tasks.length} tasks</p>
            <div className="mt-1 flex items-center gap-1.5 text-[10px] text-muted-foreground"><span className={cn("size-1.5 rounded-full", wave.status === "completed" ? "bg-emerald-500" : wave.status === "active" ? "bg-amber-500" : "bg-slate-400")} aria-hidden="true" />{running ? `${running} active` : wave.status}</div>
          </div>
        );
      })}
    </div>
  );
}

function ActivityRow({ item, execution }: { item: ExecutionActivityItem; execution: MissionExecution }) {
  const task = item.taskId ? execution.tasks.find((candidate) => candidate.id === item.taskId) : undefined;
  return (
    <li className="flex min-w-0 items-start gap-2.5 border-b border-border/70 py-2.5 last:border-b-0">
      <span className="w-12 shrink-0 pt-0.5 text-[10px] tabular-nums text-muted-foreground">{item.timestamp}</span>
      <span className={cn("mt-1.5 size-1.5 shrink-0 rounded-full", item.status ? STATUS_TONE[item.status].dot : "bg-muted-foreground/60")} aria-hidden="true" />
      <div className="min-w-0 flex-1"><p className="text-[11px] leading-4">{item.message}</p><p className="mt-0.5 truncate text-[10px] text-muted-foreground">{task?.title ?? "Mission"}{item.workerId ? ` · ${execution.workers.find((worker) => worker.id === item.workerId)?.label ?? item.workerId}` : ""}</p></div>
      {item.status && <StatusPill status={item.status} />}
    </li>
  );
}

function TaskDetails({ task, execution, onClose }: { task: ExecutionTask; execution: MissionExecution; onClose?: () => void }) {
  const worker = execution.workers.find((candidate) => candidate.id === task.workerId);
  const relatedActivity = execution.activities.filter((item) => item.taskId === task.id).slice(-6).reverse();
  return (
    <aside className="flex min-h-0 min-w-0 flex-col border-border bg-background lg:border-l" aria-label={`Task details for ${task.title}`}>
      <header className="flex shrink-0 items-start gap-2 border-b border-border px-3 py-3"><div className="min-w-0 flex-1"><div className="flex items-center gap-1.5"><span className="text-[10px] uppercase tracking-wider text-muted-foreground">Task detail</span>{task.kind === "gate" && <ShieldCheck className="size-3.5 text-cyan-600" aria-label="Gate task" />}</div><h2 className="mt-1 text-[14px] font-semibold leading-5">{task.title}</h2></div>{onClose && <Button variant="ghost" size="icon-xs" onClick={onClose} aria-label="Close task details" title="Close task details"><X className="size-4" /></Button>}</header>
      <div className="min-h-0 flex-1 overflow-y-auto px-3 py-3">
        <div className="flex flex-wrap items-center gap-1.5"><StatusPill status={task.status} />{task.wave && <span className="rounded-full border border-border bg-muted/30 px-1.5 py-0.5 text-[10px] text-muted-foreground">Wave {task.wave}</span>}{task.retryCount ? <span className="inline-flex items-center gap-1 rounded-full border border-orange-500/40 px-1.5 py-0.5 text-[10px] text-orange-700 dark:text-orange-300"><RefreshCw className="size-3" />attempt {task.attempt} / {task.maxAttempts}</span> : null}</div>
        <p className="mt-3 text-[11px] leading-5 text-muted-foreground">{task.description ?? "No task description was provided by this fixture."}</p>
        {(task.blockedReason || task.waitingReason) && <div className={cn("mt-3 rounded-md border px-2.5 py-2 text-[11px] leading-4", task.status === "blocked" ? "border-red-500/30 bg-red-500/5 text-red-800 dark:text-red-200" : "border-violet-500/30 bg-violet-500/5 text-violet-800 dark:text-violet-200")}><strong>{task.status === "blocked" ? "Blocked" : "Waiting"}:</strong> {task.blockedReason ?? task.waitingReason}</div>}
        <section className="mt-4"><SectionTitle>Assignment</SectionTitle><dl className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1.5 text-[11px]"><dt className="text-muted-foreground">Worker</dt><dd className="truncate font-medium">{worker?.label ?? task.workerRole ?? "Unassigned"}</dd><dt className="text-muted-foreground">Provider</dt><dd className="truncate">{task.provider ?? worker?.provider ?? "Not assigned"}</dd><dt className="text-muted-foreground">Model</dt><dd className="truncate">{task.model ?? worker?.model ?? "Not assigned"}</dd><dt className="text-muted-foreground">Variant</dt><dd>{task.variant ?? worker?.variant ?? "standard"}</dd><dt className="text-muted-foreground">Capability</dt><dd>{task.capabilityRequirement ?? "—"}</dd></dl></section>
        <section className="mt-4"><SectionTitle>Timing & usage</SectionTitle><div className="grid grid-cols-2 gap-1.5"><div className="rounded-md border border-border px-2 py-1.5"><p className="text-[10px] text-muted-foreground">Elapsed</p><p className="mt-0.5 text-[12px] font-medium tabular-nums">{formatDuration(task.elapsedMs)}</p></div><div className="rounded-md border border-border px-2 py-1.5"><p className="text-[10px] text-muted-foreground">Usage</p><p className="mt-0.5 text-[12px] font-medium tabular-nums">{task.usageSummary?.tokens?.toLocaleString() ?? "—"} tokens</p></div></div><p className="mt-1.5 text-[10px] text-muted-foreground">{task.startedAt ? `Started ${task.startedAt}` : "Not started"}{task.finishedAt ? ` · finished ${task.finishedAt}` : ""}{task.usageSummary?.cost !== undefined ? ` · ${formatCost(task.usageSummary.cost)}` : ""}</p></section>
        <section className="mt-4"><SectionTitle>Dependencies</SectionTitle><div className="space-y-1.5 text-[11px]">{task.dependencies.length === 0 && <p className="text-muted-foreground">No prerequisites.</p>}{task.dependencies.map((id) => { const dependency = execution.tasks.find((candidate) => candidate.id === id); return <div key={id} className="flex items-center gap-1.5"><ArrowRight className="size-3 text-muted-foreground" aria-hidden="true" /><TaskLabel task={dependency ?? { ...task, id, title: id }} compact /></div>; })}<p className="pt-1 text-[10px] text-muted-foreground">{task.dependents.length} downstream dependent{task.dependents.length === 1 ? "" : "s"}</p></div></section>
        {task.outputSummary && <section className="mt-4"><SectionTitle>System output</SectionTitle><p className="rounded-md border border-border bg-muted/20 px-2.5 py-2 text-[11px] leading-4">{task.outputSummary}</p></section>}
        {task.verificationSummary && <section className="mt-4"><SectionTitle>Verification</SectionTitle><p className="rounded-md border border-cyan-500/30 bg-cyan-500/5 px-2.5 py-2 text-[11px] leading-4">{task.verificationSummary}</p></section>}
        {task.attemptHistory && <section className="mt-4"><SectionTitle>Attempt history</SectionTitle><div className="space-y-1.5">{task.attemptHistory.map((attempt) => <div key={attempt.number} className="rounded-md border border-border px-2 py-1.5 text-[10px]"><div className="flex items-center justify-between gap-2"><span className="font-medium">Attempt {attempt.number}</span><span className="capitalize text-muted-foreground">{attempt.status}</span></div><p className="mt-0.5 truncate text-muted-foreground">{attempt.model ?? "model unknown"} · {attempt.reason ?? "No reason recorded"}</p></div>)}</div></section>}
        {task.escalationHistory && <section className="mt-4"><SectionTitle>Model escalation</SectionTitle><ol className="space-y-1.5 border-l border-border pl-3 text-[10px]">{task.escalationHistory.map((item, index) => <li key={`${item.model}-${index}`}><p className="font-medium">{item.provider} · {item.model}</p><p className="text-muted-foreground">{item.reason}</p></li>)}</ol></section>}
        {relatedActivity.length > 0 && <section className="mt-4"><SectionTitle>Task activity</SectionTitle><ul>{relatedActivity.map((item) => <ActivityRow key={item.id} item={item} execution={execution} />)}</ul></section>}
      </div>
    </aside>
  );
}

function GraphSurface({ execution, selectedTaskId, onSelectTask, focusMode, setFocusMode }: { execution: MissionExecution; selectedTaskId: string | null; onSelectTask: (id: string) => void; focusMode: FocusMode; setFocusMode: (mode: FocusMode) => void }) {
  const graph = useMemo(() => deriveExecutionGraph(execution), [execution]);
  const [zoom, setZoom] = useState(0.82);
  const [pan, setPan] = useState({ x: 0, y: 0 });
  const drag = useRef<{ x: number; y: number; panX: number; panY: number } | null>(null);
  const selected = selectedTaskId ? execution.tasks.find((task) => task.id === selectedTaskId) : undefined;
  const emphasized = useMemo(() => {
    if (!selectedTaskId) return new Set<string>();
    return new Set([...dependencyClosure(execution, selectedTaskId), ...dependentClosure(execution, selectedTaskId)]);
  }, [execution, selectedTaskId]);
  const visible = (task: ExecutionTask) => focusMode === "all" || (focusMode === "running" ? ["running", "starting", "verifying", "retrying"].includes(task.status) : focusMode === "blocked" ? task.status === "blocked" : execution.criticalPathTaskIds?.includes(task.id));
  const waveByTask = new Map(execution.waves.flatMap((wave) => wave.taskIds.map((id) => [id, wave.index] as const)));

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div className="flex flex-wrap items-center gap-1.5 border-b border-border px-3 py-2">
        <div className="flex items-center gap-1 rounded-md border border-border bg-muted/20 p-0.5" role="group" aria-label="Graph focus"><span className="px-1.5 text-[10px] text-muted-foreground">Focus</span>{(["all", "running", "blocked", "critical"] as FocusMode[]).map((mode) => <button key={mode} type="button" onClick={() => setFocusMode(mode)} className={cn("rounded px-2 py-1 text-[10px] capitalize transition-colors", focusMode === mode ? "bg-background font-medium shadow-sm" : "text-muted-foreground hover:text-foreground")} aria-pressed={focusMode === mode}>{mode === "critical" ? "critical path" : mode}</button>)}</div>
        <span className="hidden text-[10px] text-muted-foreground sm:inline">Select a Task to emphasize upstream and downstream dependencies</span>
        <div className="ml-auto flex items-center gap-1"><Button variant="ghost" size="icon-xs" onClick={() => setZoom((value) => Math.min(1.6, value + 0.12))} aria-label="Zoom in" title="Zoom in"><Plus className="size-3.5" /></Button><span className="w-10 text-center text-[10px] tabular-nums text-muted-foreground">{Math.round(zoom * 100)}%</span><Button variant="ghost" size="icon-xs" onClick={() => setZoom((value) => Math.max(0.45, value - 0.12))} aria-label="Zoom out" title="Zoom out"><Minus className="size-3.5" /></Button><Button variant="ghost" size="icon-xs" onClick={() => { setZoom(0.82); setPan({ x: 0, y: 0 }); }} aria-label="Fit graph" title="Fit graph"><Maximize2 className="size-3.5" /></Button><Button variant="ghost" size="icon-xs" onClick={() => { setZoom(1); setPan({ x: 0, y: 0 }); }} aria-label="Reset graph view" title="Reset graph view"><Crosshair className="size-3.5" /></Button></div>
      </div>
      <div className="relative min-h-[360px] flex-1 overflow-hidden bg-muted/10" onWheel={(event) => { event.preventDefault(); setZoom((value) => Math.max(0.45, Math.min(1.6, value + (event.deltaY > 0 ? -0.06 : 0.06)))); }} onPointerDown={(event) => { if (event.target === event.currentTarget) { drag.current = { x: event.clientX, y: event.clientY, panX: pan.x, panY: pan.y }; (event.currentTarget as HTMLElement).setPointerCapture(event.pointerId); } }} onPointerMove={(event) => { if (!drag.current) return; setPan({ x: drag.current.panX + event.clientX - drag.current.x, y: drag.current.panY + event.clientY - drag.current.y }); }} onPointerUp={() => { drag.current = null; }} onPointerCancel={() => { drag.current = null; }}>
        <svg className="size-full min-h-[360px] touch-none select-none" viewBox={`0 0 ${graph.width} ${graph.height}`} role="application" aria-label="Interactive Mission Execution Graph">
          <defs><marker id="execution-arrow" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto"><path d="M0,0 L7,3.5 L0,7 z" fill="var(--muted-foreground)" /></marker></defs>
          <g transform={`translate(${pan.x} ${pan.y}) scale(${zoom})`}>
            {execution.waves.map((wave) => { const nodes = graph.nodes.filter((node) => wave.taskIds.includes(node.id)); if (nodes.length === 0) return null; const x = nodes[0].x - 14; const maxY = Math.max(...nodes.map((node) => node.y + node.height)) + 14; return <g key={wave.index}><rect x={x} y={20} width={188 + 28} height={maxY - 6} rx={8} fill="var(--muted)" opacity={wave.index === execution.currentWave ? 0.32 : 0.16} stroke="var(--border)" strokeDasharray={wave.status === "planned" ? "4 5" : undefined} /><text x={x + 10} y={38} fontSize="10" fontWeight="600" fill="var(--muted-foreground)">WAVE {wave.index}{wave.index === execution.currentWave ? " · CURRENT" : ""}</text></g>; })}
            {graph.edges.map((edge) => { const active = !selectedTaskId || emphasized.has(edge.source.id) && emphasized.has(edge.target.id); const blocked = edge.kind === "conflict"; const sx = edge.source.x + edge.source.width; const sy = edge.source.y + edge.source.height / 2; const tx = edge.target.x; const ty = edge.target.y + edge.target.height / 2; const curve = Math.max(28, (tx - sx) / 2); return <path key={edge.id} d={`M ${sx} ${sy} C ${sx + curve} ${sy}, ${tx - curve} ${ty}, ${tx} ${ty}`} fill="none" stroke={blocked ? "#ef4444" : active ? "var(--muted-foreground)" : "var(--border)"} strokeWidth={blocked ? 1.8 : active ? 1.4 : 1} strokeDasharray={blocked ? "4 4" : edge.kind === "gate" ? "3 3" : undefined} markerEnd="url(#execution-arrow)" opacity={active ? 0.78 : 0.22} />; })}
            {graph.nodes.map((node) => { const task = node.task; const isVisible = visible(task); const isSelected = selectedTaskId === task.id; const isRelated = selectedTaskId ? emphasized.has(task.id) : true; const titleLines = task.title.length > 25 ? [task.title.slice(0, 25), task.title.slice(25, 48) + (task.title.length > 48 ? "…" : "")] : [task.title]; return <g key={node.id} role="button" tabIndex={isVisible ? 0 : -1} aria-label={`${task.title}, ${formatExecutionStatus(task.status)}, wave ${waveByTask.get(task.id) ?? task.wave ?? "unassigned"}`} onClick={() => onSelectTask(task.id)} onKeyDown={(event) => { if (event.key === "Enter" || event.key === " ") { event.preventDefault(); onSelectTask(task.id); } }} opacity={isVisible ? isRelated ? 1 : 0.32 : 0.16} className="cursor-pointer outline-none"><rect x={node.x} y={node.y} width={node.width} height={node.height} rx={8} fill="var(--background)" stroke={isSelected ? "var(--foreground)" : task.kind === "gate" ? "#0891b2" : "var(--border)"} strokeWidth={isSelected ? 2.4 : task.kind === "gate" ? 1.8 : 1.2} strokeDasharray={task.kind === "gate" ? "5 3" : undefined} /><rect x={node.x} y={node.y} width={4} height={node.height} rx={2} fill={task.status === "completed" ? "#10b981" : task.status === "blocked" ? "#ef4444" : task.status === "waiting" ? "#8b5cf6" : task.status === "verifying" ? "#06b6d4" : task.status === "retrying" ? "#f97316" : task.status === "running" ? "#f59e0b" : "#94a3b8"} /><text x={node.x + 14} y={node.y + 22} fontSize="11" fontWeight="600" fill="var(--foreground)">{titleLines.map((line, index) => <tspan key={line} x={node.x + 14} dy={index === 0 ? 0 : 14}>{line}</tspan>)}</text><text x={node.x + 14} y={node.y + 54} fontSize="9.5" fill="var(--muted-foreground)">{formatExecutionStatus(task.status)}{task.elapsedMs ? ` · ${formatDuration(task.elapsedMs)}` : ""}</text><text x={node.x + 14} y={node.y + 70} fontSize="9" fill="var(--muted-foreground)">{task.workerRole ?? "unassigned"}{task.model ? ` · ${task.model.length > 19 ? `${task.model.slice(0, 19)}…` : task.model}` : ""}</text>{task.retryCount ? <text x={node.x + node.width - 12} y={node.y + 17} fontSize="9" textAnchor="end" fill="#f97316">↻ {task.attempt}/{task.maxAttempts}</text> : null}<title>{`${task.title} · ${formatExecutionStatus(task.status)} · ${task.workerRole ?? "unassigned"}`}</title></g>; })}
          </g>
        </svg>
        <div className="pointer-events-none absolute bottom-2 left-2 rounded border border-border bg-background/90 px-2 py-1 text-[10px] text-muted-foreground">Drag empty space to pan · wheel to zoom</div>
      </div>
      {selected && <div className="border-t border-border px-3 py-2 text-[10px] text-muted-foreground"><span className="font-medium text-foreground">Selected:</span> {selected.title} · {selected.dependencies.length} upstream · {selected.dependents.length} downstream</div>}
    </div>
  );
}

function TasksSurface({ execution, selectedTaskId, onSelectTask }: { execution: MissionExecution; selectedTaskId: string | null; onSelectTask: (id: string) => void }) {
  const [query, setQuery] = useState("");
  const [status, setStatus] = useState("all");
  const [wave, setWave] = useState("all");
  const [role, setRole] = useState("all");
  const [provider, setProvider] = useState("all");
  const filtered = useMemo(() => filterExecutionTasks(execution.tasks, {
    query,
    status: status === "all" ? undefined : status as ExecutionStatus,
    wave: wave === "all" ? undefined : Number(wave),
    workerRole: role === "all" ? undefined : role,
    provider: provider === "all" ? undefined : provider,
  }), [execution.tasks, query, status, wave, role, provider]);
  const roles = [...new Set(execution.tasks.map((task) => task.workerRole).filter(Boolean))];
  const providers = [...new Set(execution.tasks.map((task) => task.provider).filter(Boolean))];
  return <div className="flex min-h-0 flex-1 flex-col"><div className="flex flex-wrap items-center gap-1.5 border-b border-border px-3 py-2"><label className="relative min-w-[180px] flex-1 sm:max-w-[280px]"><Search className="pointer-events-none absolute left-2 top-1/2 size-3.5 -translate-y-1/2 text-muted-foreground" aria-hidden="true" /><Input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Search tasks" aria-label="Search tasks" className="h-7 border border-border pl-7 text-[11px]" /></label><select value={status} onChange={(event) => setStatus(event.target.value)} aria-label="Filter tasks by status" className="h-7 rounded border border-border bg-background px-2 text-[10px]"><option value="all">All status</option>{Object.keys(STATUS_TONE).map((item) => <option key={item} value={item}>{formatExecutionStatus(item as ExecutionStatus)}</option>)}</select><select value={wave} onChange={(event) => setWave(event.target.value)} aria-label="Filter tasks by wave" className="h-7 rounded border border-border bg-background px-2 text-[10px]"><option value="all">All waves</option>{execution.waves.map((item) => <option key={item.index} value={item.index}>Wave {item.index}</option>)}</select><select value={role} onChange={(event) => setRole(event.target.value)} aria-label="Filter tasks by Worker role" className="h-7 rounded border border-border bg-background px-2 text-[10px]"><option value="all">All Workers</option>{roles.map((item) => <option key={item} value={item}>{item}</option>)}</select><select value={provider} onChange={(event) => setProvider(event.target.value)} aria-label="Filter tasks by provider" className="h-7 rounded border border-border bg-background px-2 text-[10px]"><option value="all">All providers</option>{providers.map((item) => <option key={item} value={item}>{item}</option>)}</select><span className="ml-auto text-[10px] text-muted-foreground">{filtered.length} of {execution.tasks.length}</span></div><div className="min-h-0 flex-1 overflow-auto"><table className="w-full min-w-[760px] border-collapse text-left text-[11px]"><caption className="sr-only">Mission execution tasks and their assignments</caption><thead className="sticky top-0 z-10 bg-muted/95 text-[10px] uppercase tracking-wider text-muted-foreground"><tr><th className="px-3 py-2 font-medium">Task</th><th className="px-3 py-2 font-medium">Status</th><th className="px-3 py-2 font-medium">Wave</th><th className="px-3 py-2 font-medium">Worker</th><th className="px-3 py-2 font-medium">Provider / model</th><th className="px-3 py-2 font-medium">Deps</th><th className="px-3 py-2 font-medium">Duration</th></tr></thead><tbody>{filtered.map((task) => <tr key={task.id} className={cn("border-t border-border/70 hover:bg-muted/40", selectedTaskId === task.id && "bg-muted/60")}><td className="max-w-[240px] px-3 py-2"><button type="button" onClick={() => onSelectTask(task.id)} className="w-full text-left focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-ring"><TaskLabel task={task} /></button>{(task.blockedReason || task.waitingReason) && <p className="mt-1 truncate text-[10px] text-muted-foreground">{task.blockedReason ?? task.waitingReason}</p>}</td><td className="px-3 py-2"><StatusPill status={task.status} /></td><td className="px-3 py-2 tabular-nums text-muted-foreground">{task.wave ?? "—"}</td><td className="px-3 py-2">{task.workerRole ?? "—"}</td><td className="max-w-[210px] px-3 py-2"><p className="truncate">{task.model ?? "—"}</p><p className="truncate text-[10px] text-muted-foreground">{task.provider ?? ""}</p></td><td className="px-3 py-2 tabular-nums text-muted-foreground">{task.dependencies.length} / {task.dependents.length}</td><td className="px-3 py-2 tabular-nums text-muted-foreground">{formatDuration(task.elapsedMs)}{task.retryCount ? <span className="ml-1 text-orange-600">· ↻{task.retryCount}</span> : null}</td></tr>)}</tbody></table>{filtered.length === 0 && <div className="p-8 text-center text-[11px] text-muted-foreground">No Tasks match these filters.</div>}</div></div>;
}

function WorkersSurface({ execution, onSelectTask }: { execution: MissionExecution; onSelectTask: (id: string) => void }) {
  return <div className="min-h-0 flex-1 overflow-auto p-3"><div className="grid gap-2 md:grid-cols-2 xl:grid-cols-3">{execution.workers.map((worker) => { const current = worker.currentTaskId ? execution.tasks.find((task) => task.id === worker.currentTaskId) : undefined; return <article key={worker.id} className="rounded-md border border-border bg-background p-3"><div className="flex items-start gap-2"><div className={cn("mt-1 size-2 rounded-full", worker.status === "active" ? "bg-amber-500 animate-pulse" : worker.status === "blocked" ? "bg-red-500" : worker.status === "completed" ? "bg-emerald-500" : "bg-slate-400")} aria-hidden="true" /><div className="min-w-0 flex-1"><div className="flex items-center gap-2"><h3 className="truncate text-[12px] font-semibold">{worker.label}</h3>{worker.role === "lead" && <span className="rounded border border-foreground/20 px-1 py-0.5 text-[9px] uppercase tracking-wider">Lead</span>}</div><p className="mt-0.5 text-[10px] capitalize text-muted-foreground">{worker.status}</p></div></div><dl className="mt-3 grid grid-cols-2 gap-x-3 gap-y-2 text-[10px]"><div><dt className="text-muted-foreground">Provider</dt><dd className="truncate">{worker.provider ?? "—"}</dd></div><div><dt className="text-muted-foreground">Model</dt><dd className="truncate">{worker.model ?? "—"}</dd></div><div><dt className="text-muted-foreground">Completed</dt><dd>{worker.completedTaskIds.length} Tasks</dd></div><div><dt className="text-muted-foreground">Invocations</dt><dd>{worker.invocationCount ?? 0} · ↻ {worker.retryCount ?? 0}</dd></div><div><dt className="text-muted-foreground">Active time</dt><dd>{formatDuration(worker.elapsedMs)}</dd></div><div><dt className="text-muted-foreground">Usage</dt><dd>{worker.usageSummary?.tokens?.toLocaleString() ?? "0"} tokens</dd></div></dl>{current ? <button type="button" onClick={() => onSelectTask(current.id)} className="mt-3 flex w-full items-center gap-1.5 rounded border border-border bg-muted/30 px-2 py-1.5 text-left text-[10px] hover:bg-muted/60"><Zap className="size-3 text-amber-600" aria-hidden="true" /><span className="truncate">Current: {current.title}</span></button> : <p className="mt-3 rounded border border-dashed border-border px-2 py-1.5 text-[10px] text-muted-foreground">No current Task</p>}</article>; })}</div></div>;
}

function OverviewSurface({ execution, onSelectTask }: { execution: MissionExecution; onSelectTask: (id: string) => void }) {
  const issues = execution.tasks.filter((task) => ["blocked", "waiting", "retrying", "verifying"].includes(task.status));
  const models = [...new Set(execution.tasks.map((task) => task.model).filter(Boolean))];
  return <div className="min-h-0 flex-1 overflow-auto p-3"><div className="grid gap-3 xl:grid-cols-[1.3fr_1fr]"><section className="rounded-md border border-border bg-background p-3"><SectionTitle detail={`${execution.summary.completed} of ${execution.summary.total} complete`}>Execution health</SectionTitle><div className="grid grid-cols-2 gap-2 sm:grid-cols-4"><Metric label="Running" value={String(execution.summary.running)} detail="active or verifying" icon={Zap} /><Metric label="Waiting" value={String(execution.summary.waiting)} detail="queued or planned" icon={Clock3} /><Metric label="Blocked" value={String(execution.summary.blocked)} detail="needs resolution" icon={AlertTriangle} /><Metric label="Models" value={String(models.length)} detail="assigned in fixture" icon={Code2} /></div><div className="mt-4"><div className="flex items-center justify-between text-[10px] text-muted-foreground"><span>Task progress</span><span>{Math.round((execution.summary.completed / execution.summary.total) * 100)}%</span></div><div className="mt-1 h-1.5 overflow-hidden rounded-full bg-muted"><div className="h-full rounded-full bg-emerald-500" style={{ width: `${(execution.summary.completed / execution.summary.total) * 100}%` }} /></div></div></section><section className="rounded-md border border-border bg-background p-3"><SectionTitle>Current execution</SectionTitle><dl className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-2 text-[11px]"><dt className="text-muted-foreground">Wave</dt><dd className="font-medium">{execution.currentWave} / {execution.totalWaves}</dd><dt className="text-muted-foreground">Elapsed</dt><dd>55m</dd><dt className="text-muted-foreground">Budget</dt><dd>{formatCost(execution.budget?.spent)} / {formatCost(execution.budget?.limit)} · {execution.budget?.commitmentPercent}% committed</dd><dt className="text-muted-foreground">Tokens</dt><dd>{execution.budget?.tokens.toLocaleString()}</dd><dt className="text-muted-foreground">Next likely</dt><dd>{execution.nextTaskIds?.map((id) => execution.tasks.find((task) => task.id === id)?.title).join(" · ")}</dd></dl></section></div><section className="mt-3"><SectionTitle detail="parallel work and planned follow-up">Waves</SectionTitle><WaveStrip execution={execution} /></section><div className="mt-3 grid gap-3 xl:grid-cols-2"><section className="rounded-md border border-border bg-background p-3"><SectionTitle detail="discoverable reasons">Blocking & gates</SectionTitle><ul className="divide-y divide-border">{issues.map((task) => <li key={task.id} className="flex items-start gap-2 py-2 first:pt-0 last:pb-0"><span className={cn("mt-1.5 size-1.5 shrink-0 rounded-full", STATUS_TONE[task.status].dot)} aria-hidden="true" /><button type="button" onClick={() => onSelectTask(task.id)} className="min-w-0 flex-1 text-left"><p className="truncate text-[11px] font-medium">{task.title}</p><p className="mt-0.5 line-clamp-2 text-[10px] text-muted-foreground">{task.blockedReason ?? task.waitingReason ?? task.verificationSummary ?? "Retry or gate activity is in progress."}</p></button><StatusPill status={task.status} /></li>)}</ul></section><section className="rounded-md border border-border bg-background p-3"><SectionTitle detail="system-readable scheduling metadata">Next likely Tasks</SectionTitle><ul className="space-y-2">{(execution.nextTaskIds ?? []).map((id) => { const task = execution.tasks.find((item) => item.id === id); if (!task) return null; return <li key={id} className="flex items-start gap-2"><ArrowDownToLine className="mt-0.5 size-3.5 text-muted-foreground" aria-hidden="true" /><button type="button" onClick={() => onSelectTask(id)} className="text-left"><p className="text-[11px] font-medium">{task.title}</p><p className="mt-0.5 text-[10px] text-muted-foreground">{task.schedulingReason ?? "Ready when prerequisites resolve."}</p></button></li>; })}</ul></section></div><section className="mt-3 rounded-md border border-border bg-background p-3"><SectionTitle detail="bounded normalized event history">Recent activity</SectionTitle><ul>{execution.activities.slice(-5).reverse().map((item) => <ActivityRow key={item.id} item={item} execution={execution} />)}</ul></section></div>;
}

export function MissionControlSurface({ execution, onOpenInspector }: { execution: MissionExecution; onOpenInspector?: () => void }) {
  const [tab, setTab] = useState<MissionControlTab>("graph");
  const firstTaskId = execution.tasks[0]?.id ?? null;
  const [selection, setSelection] = useState<{ missionId: string; taskId: string | null }>(() => ({
    missionId: execution.missionId,
    taskId: firstTaskId,
  }));
  const [focusMode, setFocusMode] = useState<FocusMode>("all");
  // Derive the visible selection from the current execution so a newly
  // launched Mission never keeps a stale task id from a previous execution.
  const selectedTaskId = selection.missionId === execution.missionId ? selection.taskId : firstTaskId;
  const selectedTask = selectedTaskId ? execution.tasks.find((task) => task.id === selectedTaskId) : undefined;
  const selectTask = (id: string) => { setSelection({ missionId: execution.missionId, taskId: id }); if (tab === "overview") setTab("graph"); };
  const closeTask = () => setSelection({ missionId: execution.missionId, taskId: null });
  return <div className="flex h-full min-h-0 w-full min-w-0 flex-col bg-background"><header className="shrink-0 border-b border-border px-3 py-3 sm:px-4"><div className="flex flex-wrap items-start gap-3"><div className="min-w-0 flex-1"><div className="flex items-center gap-2"><GitCommitHorizontal className="size-4 text-muted-foreground" aria-hidden="true" /><span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">Mission Control</span><span className="inline-flex items-center gap-1.5 rounded-full border border-amber-500/40 bg-amber-500/5 px-1.5 py-0.5 text-[10px] text-amber-700 dark:text-amber-300"><span className="size-1.5 animate-pulse rounded-full bg-amber-500" aria-hidden="true" />Running</span></div><h1 className="mt-1 truncate text-[16px] font-semibold tracking-tight sm:text-[18px]">{execution.title}</h1><p className="mt-0.5 truncate text-[11px] text-muted-foreground">Execution Graph · frontend read model · no scheduler commands</p></div>{onOpenInspector && <Button variant="outline" size="xs" onClick={onOpenInspector}><PanelRight className="size-3.5" />Mission Inspector</Button>}</div><div className="mt-3 flex flex-wrap items-center gap-x-4 gap-y-2"><Metric label="Wave" value={`${execution.currentWave} / ${execution.totalWaves}`} icon={GitBranch} /><Metric label="Tasks" value={`${execution.summary.completed} / ${execution.summary.total}`} icon={Check} /><Metric label="Running" value={String(execution.summary.running)} icon={Zap} /><Metric label="Blocked" value={String(execution.summary.blocked)} icon={AlertTriangle} /><Metric label="Workers" value={String(execution.workers.length)} icon={Users} /><Metric label="Elapsed" value="55m" icon={Timer} /><Metric label="Budget" value={`${formatCost(execution.budget?.spent)} / ${formatCost(execution.budget?.limit)}`} detail={`${execution.budget?.tokens.toLocaleString()} tokens`} icon={Wallet} /></div></header><nav className="flex shrink-0 items-center gap-1 overflow-x-auto border-b border-border px-3 py-1.5" aria-label="Mission Control projections" role="tablist">{TABS.map(({ id, label, icon: Icon }) => <button key={id} type="button" role="tab" aria-selected={tab === id} aria-controls={`mission-control-${id}`} onClick={() => setTab(id)} className={cn("inline-flex shrink-0 items-center gap-1.5 rounded px-2.5 py-1.5 text-[11px] font-medium transition-colors focus-visible:outline-2 focus-visible:outline-offset-1 focus-visible:outline-ring", tab === id ? "bg-muted text-foreground" : "text-muted-foreground hover:text-foreground")}><Icon className="size-3.5" aria-hidden="true" />{label}{id === "tasks" && <span className="text-[10px] text-muted-foreground">{execution.summary.total}</span>}</button>)}</nav><div id={`mission-control-${tab}`} role="tabpanel" aria-label={TABS.find((item) => item.id === tab)?.label} className="flex min-h-0 flex-1">{tab === "overview" && <OverviewSurface execution={execution} onSelectTask={selectTask} />}{tab === "graph" && <div className="flex min-h-0 min-w-0 flex-1 flex-col lg:flex-row"><div className="min-h-0 min-w-0 flex-1"><GraphSurface execution={execution} selectedTaskId={selectedTaskId} onSelectTask={selectTask} focusMode={focusMode} setFocusMode={setFocusMode} /></div>{selectedTask && <div className="max-h-[42vh] min-h-0 lg:max-h-none lg:w-[330px] lg:shrink-0"><TaskDetails task={selectedTask} execution={execution} onClose={closeTask} /></div>}</div>}{tab === "tasks" && <div className="flex min-h-0 min-w-0 flex-1 flex-col lg:flex-row"><div className="min-h-0 min-w-0 flex-1"><TasksSurface execution={execution} selectedTaskId={selectedTaskId} onSelectTask={selectTask} /></div>{selectedTask && <div className="max-h-[42vh] min-h-0 lg:max-h-none lg:w-[330px] lg:shrink-0"><TaskDetails task={selectedTask} execution={execution} onClose={closeTask} /></div>}</div>}{tab === "workers" && <WorkersSurface execution={execution} onSelectTask={selectTask} />}{tab === "activity" && <div className="min-h-0 flex-1 overflow-auto p-3"><div className="mx-auto max-w-3xl rounded-md border border-border bg-background px-3"><SectionTitle detail={`${execution.activities.length} bounded events`}>Mission activity</SectionTitle><ul>{[...execution.activities].reverse().map((item) => <ActivityRow key={item.id} item={item} execution={execution} />)}</ul></div></div>}</div></div>;
}
