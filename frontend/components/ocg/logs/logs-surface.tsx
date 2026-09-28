"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { Pause, Play, Search, ShieldCheck, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";
import {
  boundLogEntries,
  createLogsLiveFixture,
  deriveRuntimeLogEntries,
  filterLogEntries,
  newLogCount,
  type LogEntry,
  type LogFilters,
  type LogLevel,
  type LogTimeWindow,
} from "./domain";
import type { RuntimeSnapshot } from "../runtime/runtime-types";
import type { ProjectId } from "../project/domain";
import { selectProject } from "../project/domain";

const INITIAL_LIVE_ENTRIES = 3;
const HISTORY_LIMIT = 120;
const EMPTY_LOG_ENTRIES: readonly LogEntry[] = [];

const LEVEL_TONE: Record<LogLevel, string> = {
  trace: "text-slate-600 dark:text-slate-300",
  debug: "text-violet-700 dark:text-violet-300",
  info: "text-sky-700 dark:text-sky-300",
  warn: "text-amber-700 dark:text-amber-300",
  error: "text-red-700 dark:text-red-300",
};

const LEVEL_DOT: Record<LogLevel, string> = {
  trace: "bg-slate-400",
  debug: "bg-violet-500",
  info: "bg-sky-500",
  warn: "bg-amber-500",
  error: "bg-red-500",
};

function formatTimestamp(timestamp: string): string {
  const date = new Date(timestamp);
  if (Number.isNaN(date.valueOf())) return timestamp;
  return new Intl.DateTimeFormat("en", { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false }).format(date);
}

function formatFullTimestamp(timestamp: string): string {
  const date = new Date(timestamp);
  return Number.isNaN(date.valueOf()) ? timestamp : date.toISOString();
}

function LogLevelPill({ level }: { level: LogLevel }) {
  return (
    <span className={cn("inline-flex items-center gap-1 text-[10px] font-semibold uppercase", LEVEL_TONE[level])}>
      <span className={cn("size-1.5 rounded-full", LEVEL_DOT[level])} aria-hidden="true" />
      {level}
    </span>
  );
}

function FilterSelect({
  label,
  value,
  onChange,
  children,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  children: React.ReactNode;
}) {
  return (
    <label className="flex min-w-0 flex-col gap-1 text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
      {label}
      <select
        aria-label={label}
        value={value}
        onChange={(event) => onChange(event.target.value)}
        className="h-8 min-w-0 rounded-md border border-border bg-background px-2 text-[11px] font-normal tracking-normal text-foreground outline-none focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/30"
      >
        {children}
      </select>
    </label>
  );
}

function FieldList({ fields }: { fields: NonNullable<LogEntry["fields"]> }) {
  const entries = Object.entries(fields);
  if (entries.length === 0) return <p className="text-[11px] text-muted-foreground">No structured fields.</p>;
  return (
    <dl className="divide-y divide-border/70 rounded-md border border-border">
      {entries.map(([key, value]) => (
        <div key={key} className="grid grid-cols-[minmax(110px,0.45fr)_minmax(0,1fr)] gap-3 px-2.5 py-2 text-[11px]">
          <dt className="break-words text-muted-foreground">{key}</dt>
          <dd className="break-words font-mono text-[10px]">{typeof value === "string" ? value : JSON.stringify(value)}</dd>
        </div>
      ))}
    </dl>
  );
}

function LogDetail({ entry, onClose }: { entry: LogEntry | undefined; onClose?: () => void }) {
  if (!entry) {
    return (
      <div className="flex h-full items-center justify-center p-6 text-center text-[11px] text-muted-foreground">
        Select a log entry to inspect its normalized context.
      </div>
    );
  }
  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <header className="flex shrink-0 items-start gap-2 border-b border-border px-3 py-3">
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2"><LogLevelPill level={entry.level} /><span className="text-[10px] text-muted-foreground">{entry.source}</span>{entry.redacted && <span className="inline-flex items-center gap-1 rounded-full border border-amber-500/30 bg-amber-500/10 px-1.5 py-0.5 text-[9px] text-amber-700 dark:text-amber-300"><ShieldCheck className="size-3" />redacted</span>}</div>
          <h2 className="mt-1.5 break-words text-[13px] font-semibold leading-5">{entry.message}</h2>
        </div>
        {onClose && <Button variant="ghost" size="icon-xs" onClick={onClose} aria-label="Close log details" title="Close log details"><X className="size-3.5" /></Button>}
      </header>
      <div className="min-h-0 flex-1 overflow-y-auto px-3 py-3">
        <dl className="grid grid-cols-[auto_minmax(0,1fr)] gap-x-3 gap-y-2 text-[11px]">
          <dt className="text-muted-foreground">Timestamp</dt><dd className="break-all font-mono text-[10px]">{formatFullTimestamp(entry.timestamp)}</dd>
          <dt className="text-muted-foreground">Source</dt><dd>{entry.source}{entry.category ? ` · ${entry.category}` : ""}</dd>
          <dt className="text-muted-foreground">Entry ID</dt><dd className="break-all font-mono text-[10px]">{entry.id}</dd>
          {entry.missionId && <><dt className="text-muted-foreground">Mission</dt><dd className="break-all">{entry.missionId}</dd></>}
          {entry.taskId && <><dt className="text-muted-foreground">Task</dt><dd className="break-all">{entry.taskId}</dd></>}
          {entry.workerId && <><dt className="text-muted-foreground">Worker</dt><dd className="break-all">{entry.workerId}{entry.workerRole ? ` · ${entry.workerRole}` : ""}</dd></>}
          {entry.provider && <><dt className="text-muted-foreground">Provider</dt><dd className="break-words">{entry.provider}</dd></>}
          {entry.model && <><dt className="text-muted-foreground">Model</dt><dd className="break-words">{entry.model}</dd></>}
          {entry.correlationId && <><dt className="text-muted-foreground">Correlation</dt><dd className="break-all font-mono text-[10px]">{entry.correlationId}</dd></>}
          {entry.sessionId && <><dt className="text-muted-foreground">Session</dt><dd className="break-all font-mono text-[10px]">{entry.sessionId}</dd></>}
          {entry.invocationId && <><dt className="text-muted-foreground">Invocation</dt><dd className="break-all font-mono text-[10px]">{entry.invocationId}</dd></>}
        </dl>
        <section className="mt-5" aria-label="Structured fields">
          <div className="mb-1.5 flex items-center justify-between gap-2"><h3 className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">Structured fields</h3>{entry.redacted && <span className="text-[10px] text-muted-foreground">Sensitive values removed</span>}</div>
          <FieldList fields={entry.fields ?? {}} />
        </section>
        <details className="mt-4 rounded-md border border-border px-2.5 py-2 text-[10px]">
          <summary className="cursor-pointer select-none font-medium">Technical view</summary>
          <pre className="mt-2 max-w-full overflow-x-auto whitespace-pre-wrap break-words text-muted-foreground">{JSON.stringify({ ...entry, fields: entry.fields ?? {} }, null, 2)}</pre>
        </details>
      </div>
    </div>
  );
}

export function LogsSurface({ snapshot, projectId }: { snapshot: RuntimeSnapshot; projectId?: ProjectId }) {
  const baseEntries = useMemo(
    () => {
      const fixtureEntries = snapshot.scenario === "logs-live"
        ? createLogsLiveFixture(projectId)
        : deriveRuntimeLogEntries(snapshot, snapshot.sessions[0]?.id ?? "workspace");
      const byId = new Map(fixtureEntries.map((entry) => [entry.id, entry]));
      for (const entry of snapshot.logs ?? []) byId.set(entry.id, entry);
      return [...byId.values()];
    },
    [projectId, snapshot],
  );
  const live = snapshot.scenario === "logs-live";
  const runtimeEntries = snapshot.logs ?? EMPTY_LOG_ENTRIES;
  const nextIndex = useRef(live ? INITIAL_LIVE_ENTRIES : baseEntries.length);
  const [storedEntries, setStoredEntries] = useState<LogEntry[]>(() => boundLogEntries(baseEntries.slice(0, live ? INITIAL_LIVE_ENTRIES : baseEntries.length), HISTORY_LIMIT));
  const entries = useMemo(() => {
    const incoming = live ? runtimeEntries : baseEntries;
    const existing = new Set(storedEntries.map((entry) => entry.id));
    const additions = incoming.filter((entry) => !existing.has(entry.id));
    return additions.length === 0 ? storedEntries : boundLogEntries([...storedEntries, ...additions], HISTORY_LIMIT);
  }, [baseEntries, live, runtimeEntries, storedEntries]);
  const [selectedId, setSelectedId] = useState<string | undefined>(entries[0]?.id);
  const [filters, setFilters] = useState<LogFilters>({});
  const [following, setFollowing] = useState(true);
  const [atBottom, setAtBottom] = useState(true);
  const [lastSeenCount, setLastSeenCount] = useState(entries.length);
  const streamRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!live) return;
    const timer = window.setInterval(() => {
      const next = baseEntries[nextIndex.current];
      if (!next) return;
      nextIndex.current += 1;
      setStoredEntries((current) => boundLogEntries([...current, next], HISTORY_LIMIT));
    }, 900);
    return () => window.clearInterval(timer);
  }, [baseEntries, live]);

  useEffect(() => {
    if (!following || !atBottom || !streamRef.current) return;
    streamRef.current.scrollTop = streamRef.current.scrollHeight;
    setLastSeenCount(entries.length);
  }, [entries.length, following, atBottom]);

  const sources = useMemo(() => [...new Set(entries.map((entry) => entry.source))].sort(), [entries]);
  const missions = useMemo(() => [...new Set(entries.map((entry) => entry.missionId).filter(Boolean))] as string[], [entries]);
  const workers = useMemo(() => [...new Set(entries.map((entry) => entry.workerId).filter(Boolean))] as string[], [entries]);
  const workerRoles = useMemo(() => [...new Set(entries.map((entry) => entry.workerRole).filter(Boolean))] as string[], [entries]);
  const providers = useMemo(() => [...new Set(entries.map((entry) => entry.provider).filter(Boolean))] as string[], [entries]);
  const models = useMemo(() => [...new Set(entries.map((entry) => entry.model).filter(Boolean))] as string[], [entries]);
  const filteredEntries = useMemo(() => filterLogEntries(entries, filters), [entries, filters]);
  const selected = entries.find((entry) => entry.id === selectedId) ?? filteredEntries[0];
  const unseen = newLogCount(entries.length, lastSeenCount, { following, atBottom });

  function updateFilter<K extends keyof LogFilters>(key: K, value: LogFilters[K]) {
    setFilters((current) => ({ ...current, [key]: value === "all" ? undefined : value }));
  }

  function scrollToLatest() {
    setFollowing(true);
    setAtBottom(true);
    setLastSeenCount(entries.length);
    requestAnimationFrame(() => {
      if (streamRef.current) streamRef.current.scrollTop = streamRef.current.scrollHeight;
    });
  }

  return (
    <div className="flex min-h-0 flex-1 flex-col bg-background">
      <header className="shrink-0 border-b border-border px-3 py-3 sm:px-4">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="min-w-0"><div className="flex items-center gap-2"><span className="rounded border border-border bg-muted/50 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">Diagnostics</span>{projectId && <span className="rounded border border-border bg-muted/50 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">{selectProject(projectId).name}</span>}{live && <span className="text-[10px] text-muted-foreground">deterministic stream</span>}</div><h1 className="mt-1 text-[16px] font-semibold tracking-tight">Logs</h1><p className="mt-0.5 text-[11px] text-muted-foreground">Normalized operational events · secrets are never shown</p></div>
          <div className="flex shrink-0 items-center gap-2">
            <span className="text-[10px] tabular-nums text-muted-foreground">{filteredEntries.length} of {entries.length} · bounded {HISTORY_LIMIT}</span>
            <Button variant={following ? "secondary" : "outline"} size="xs" onClick={() => following ? setFollowing(false) : scrollToLatest()} aria-pressed={following} title={following ? "Pause auto-follow" : "Resume auto-follow"}>
              {following ? <Pause className="size-3" /> : <Play className="size-3" />}{following ? "Following" : "Paused"}
            </Button>
          </div>
        </div>
        <div className="mt-3 grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-10">
          <label className="col-span-2 flex min-w-0 items-center gap-2 rounded-md border border-border bg-background px-2 sm:col-span-3 lg:col-span-2 xl:col-span-2"><Search className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" /><Input value={filters.text ?? ""} onChange={(event) => updateFilter("text", event.target.value)} placeholder="Search message, context…" aria-label="Search logs" className="h-8 border-0 px-0 text-[11px] focus-visible:border-0" /></label>
          <FilterSelect label="Level" value={filters.level ?? "all"} onChange={(value) => updateFilter("level", value as LogFilters["level"])}><option value="all">All levels</option>{(["trace", "debug", "info", "warn", "error"] as LogLevel[]).map((level) => <option key={level} value={level}>{level}</option>)}</FilterSelect>
          <FilterSelect label="Source" value={filters.source ?? "all"} onChange={(value) => updateFilter("source", value)}><option value="all">All sources</option>{sources.map((source) => <option key={source} value={source}>{source}</option>)}</FilterSelect>
          <FilterSelect label="Mission" value={filters.missionId ?? "all"} onChange={(value) => updateFilter("missionId", value)}><option value="all">All missions</option>{missions.map((mission) => <option key={mission} value={mission}>{mission}</option>)}</FilterSelect>
          <FilterSelect label="Worker" value={filters.workerId ?? "all"} onChange={(value) => updateFilter("workerId", value)}><option value="all">All workers</option>{workers.map((worker) => <option key={worker} value={worker}>{worker}</option>)}</FilterSelect>
          <FilterSelect label="Role" value={filters.workerRole ?? "all"} onChange={(value) => updateFilter("workerRole", value)}><option value="all">All roles</option>{workerRoles.map((role) => <option key={role} value={role}>{role}</option>)}</FilterSelect>
          <FilterSelect label="Provider" value={filters.provider ?? "all"} onChange={(value) => updateFilter("provider", value)}><option value="all">All providers</option>{providers.map((provider) => <option key={provider} value={provider}>{provider}</option>)}</FilterSelect>
          <FilterSelect label="Model" value={filters.model ?? "all"} onChange={(value) => updateFilter("model", value)}><option value="all">All models</option>{models.map((model) => <option key={model} value={model}>{model}</option>)}</FilterSelect>
          <FilterSelect label="Time window" value={filters.timeWindow ?? "all"} onChange={(value) => updateFilter("timeWindow", value as LogTimeWindow)}><option value="all">All time</option><option value="last-5-minutes">Last 5 minutes</option><option value="last-hour">Last hour</option><option value="last-day">Last day</option></FilterSelect>
        </div>
      </header>
      <div className="grid min-h-0 flex-1 grid-rows-[minmax(240px,1fr)_minmax(260px,auto)] lg:grid-cols-[minmax(0,1.2fr)_minmax(320px,0.8fr)] lg:grid-rows-1">
        <section className="relative min-h-0 overflow-hidden border-b border-border lg:border-r lg:border-b-0" aria-label="Log stream">
          <div className="grid grid-cols-[68px_52px_minmax(0,1fr)] gap-2 border-b border-border bg-muted/20 px-3 py-2 text-[9px] font-semibold uppercase tracking-wider text-muted-foreground md:grid-cols-[86px_58px_110px_minmax(0,1fr)_minmax(120px,0.6fr)]"><span>Time</span><span>Level</span><span className="hidden md:block">Source</span><span>Message</span><span className="hidden md:block">Context</span></div>
          <div ref={streamRef} onScroll={(event) => { const node = event.currentTarget; const bottom = node.scrollHeight - node.scrollTop - node.clientHeight < 24; setAtBottom(bottom); if (bottom) setLastSeenCount(entries.length); }} className="h-full overflow-y-auto" role="log" aria-live="polite" aria-label="Normalized log entries">
            {filteredEntries.length === 0 ? <div className="p-6 text-center text-[11px] text-muted-foreground">No log entries match these filters.</div> : <ul>{filteredEntries.map((entry) => <li key={entry.id} className="border-b border-border/60 last:border-b-0"><button type="button" onClick={() => setSelectedId(entry.id)} className={cn("grid w-full grid-cols-[68px_52px_minmax(0,1fr)] gap-2 px-3 py-2.5 text-left transition-colors hover:bg-muted/40 md:grid-cols-[86px_58px_110px_minmax(0,1fr)_minmax(120px,0.6fr)]", selected?.id === entry.id && "bg-muted/60") } aria-current={selected?.id === entry.id ? "true" : undefined}><time dateTime={entry.timestamp} className="pt-0.5 font-mono text-[10px] tabular-nums text-muted-foreground">{formatTimestamp(entry.timestamp)}</time><span className="pt-0.5"><LogLevelPill level={entry.level} /></span><span className="hidden min-w-0 truncate pt-0.5 text-[10px] text-muted-foreground md:block">{entry.source}</span><span className="min-w-0"><span className="block truncate text-[11px] leading-4">{entry.message}</span><span className="mt-0.5 block truncate text-[10px] text-muted-foreground md:hidden">{entry.source}{entry.category ? ` · ${entry.category}` : ""}</span></span><span className="hidden min-w-0 truncate pt-0.5 text-[10px] text-muted-foreground md:block">{entry.workerId ?? entry.missionId ?? entry.provider ?? "—"}</span></button></li>)}</ul>}
          </div>
          {unseen > 0 && <Button variant="secondary" size="xs" onClick={scrollToLatest} className="absolute bottom-3 left-1/2 -translate-x-1/2 shadow-sm">New logs ↓ <span className="tabular-nums">{unseen}</span></Button>}
        </section>
        <aside className="flex min-h-0 min-w-0 overflow-hidden bg-muted/5" aria-label="Log detail"><LogDetail entry={selected} onClose={() => setSelectedId(undefined)} /></aside>
      </div>
    </div>
  );
}
