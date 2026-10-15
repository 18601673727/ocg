"use client";

import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { Pause, Play, Search, ShieldCheck, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import {
  EmptyState,
  FilterOption,
  FilterSelect,
  KeyValue,
  KeyValueList,
  LOG_LEVEL,
  Pill,
  StatusDot,
  TEXT_TONE,
} from "@/components/ocg/primitives";
import { formatIsoTimestamp, formatLocalClock } from "@/lib/format";
import { cn } from "@/lib/utils";
import {
  filterLogEntries,
  LOG_LEVELS,
  type LogEntry,
  type LogFilters,
  type LogTimeWindow,
} from "./log-entries";
import { useI18n } from "../i18n";

const PROVENANCE_KEYS = {
  demo: "logs.demoEntry",
  derived: "logs.derivedEntry",
  snapshot: "logs.snapshotEntry",
} as const;

/** Level chip in the detail sheet: the shared pill, still reading as an uppercase level. */
function LogLevelPill({ level }: { level: LogEntry["level"] }) {
  const visual = LOG_LEVEL[level];
  return (
    <Pill tone={visual.tone} dot pulse={visual.pulse} className="uppercase">
      {level}
    </Pill>
  );
}

/** The dense stream row keeps the dot-and-word form, in the shared level tone. */
function LogLevelText({ level }: { level: LogEntry["level"] }) {
  const visual = LOG_LEVEL[level];
  return (
    <span className={cn("inline-flex items-center gap-1 text-xs font-semibold uppercase", TEXT_TONE[visual.tone])}>
      <StatusDot tone={visual.tone} pulse={visual.pulse} />
      {level}
    </span>
  );
}

function FieldList({ fields }: { fields: NonNullable<LogEntry["fields"]> }) {
  const { t } = useI18n();
  const entries = Object.entries(fields);
  if (entries.length === 0) return <p className="text-xs text-muted-foreground">{t("logs.noFields")}</p>;
  return (
    <KeyValueList>
      {entries.map(([key, value]) => (
        <KeyValue key={key} label={key} mono>
          {typeof value === "string" ? value : JSON.stringify(value)}
        </KeyValue>
      ))}
    </KeyValueList>
  );
}

function LogDetail({ entry, onClose }: { entry: LogEntry | undefined; onClose?: () => void }) {
  const { t } = useI18n();
  if (!entry) {
    return (
      <div className="flex h-full items-center justify-center p-6">
        <EmptyState className="text-center">{t("logs.selectEntry")}</EmptyState>
      </div>
    );
  }
  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <header className="flex max-h-[45%] shrink-0 items-start gap-2 overflow-y-auto border-b border-border px-3 py-3">
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2"><LogLevelPill level={entry.level} /><span className="text-xs text-muted-foreground">{entry.source}{entry.provenance ? ` · ${t(PROVENANCE_KEYS[entry.provenance])}` : ""}</span>{entry.redacted && <Pill tone="amber" className="text-[9px] normal-case"><ShieldCheck className="size-3" />{t("logs.redacted")}</Pill>}</div>
          <h2 className="mt-1.5 break-words text-[13px] font-semibold leading-5">{entry.message}</h2>
        </div>
        {onClose && <Tooltip><TooltipTrigger render={<Button variant="ghost" size="icon-xs" onClick={onClose} aria-label={t("logs.closeDetails")}><X className="size-3.5" /></Button>} /><TooltipContent>{t("logs.closeDetails")}</TooltipContent></Tooltip>}
      </header>
      <div className="min-h-0 flex-1 overflow-y-auto px-3 py-3">
        <dl className="grid grid-cols-[auto_minmax(0,1fr)] gap-x-3 gap-y-2 text-xs">
          <dt className="text-muted-foreground">{t("logs.timestamp")}</dt><dd className="break-all font-mono text-xs">{formatIsoTimestamp(entry.timestamp)}</dd>
          <dt className="text-muted-foreground">{t("logs.source")}</dt><dd>{entry.source}{entry.provenance ? ` · ${t(PROVENANCE_KEYS[entry.provenance])}` : ""}{entry.category ? ` · ${entry.category}` : ""}</dd>
          <dt className="text-muted-foreground">{t("logs.entryId")}</dt><dd className="break-all font-mono text-xs">{entry.id}</dd>
          {entry.jobId && <><dt className="text-muted-foreground">{t("logs.job")}</dt><dd className="break-all">{entry.jobId}</dd></>}
          {entry.taskId && <><dt className="text-muted-foreground">{t("logs.task")}</dt><dd className="break-all">{entry.taskId}</dd></>}
          {entry.workerId && <><dt className="text-muted-foreground">{t("logs.worker")}</dt><dd className="break-all">{entry.workerId}{entry.workerRole ? ` · ${entry.workerRole}` : ""}</dd></>}
          {entry.provider && <><dt className="text-muted-foreground">{t("logs.provider")}</dt><dd className="break-words">{entry.provider}</dd></>}
          {entry.model && <><dt className="text-muted-foreground">{t("logs.model")}</dt><dd className="break-words">{entry.model}</dd></>}
          {entry.correlationId && <><dt className="text-muted-foreground">{t("logs.correlation")}</dt><dd className="break-all font-mono text-xs">{entry.correlationId}</dd></>}
          {entry.sessionId && <><dt className="text-muted-foreground">{t("logs.session")}</dt><dd className="break-all font-mono text-xs">{entry.sessionId}</dd></>}
          {entry.invocationId && <><dt className="text-muted-foreground">{t("logs.invocation")}</dt><dd className="break-all font-mono text-xs">{entry.invocationId}</dd></>}
        </dl>
        <section className="mt-5" aria-label={t("logs.fieldsLabel")}>
          <div className="mb-1.5 flex items-center justify-between gap-2"><h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">{t("logs.structuredFields")}</h3>{entry.redacted && <span className="text-xs text-muted-foreground">{t("logs.sensitiveRemoved")}</span>}</div>
          <FieldList fields={entry.fields ?? {}} />
        </section>
        <details className="mt-4 rounded-md border border-border px-2.5 py-2 text-xs">
          <summary className="cursor-pointer select-none font-medium">{t("logs.technicalView")}</summary>
          <pre className="mt-2 max-w-full overflow-x-auto whitespace-pre-wrap break-words text-muted-foreground">{JSON.stringify(entry, null, 2)}</pre>
        </details>
      </div>
    </div>
  );
}

export type LogsViewProps = {
  entries: readonly LogEntry[];
  loading?: boolean;
  error?: string | null;
  unavailable?: boolean;
  projectName?: string;
  notice?: ReactNode;
  historyLimit?: number;
};

export function LogsView({ entries, loading = false, error = null, unavailable = false, projectName, notice, historyLimit = entries.length }: LogsViewProps) {
  const { t } = useI18n();
  const [selectedId, setSelectedId] = useState<string | undefined>(entries[0]?.id);
  const [filters, setFilters] = useState<LogFilters>({});
  const [following, setFollowing] = useState(true);
  const [atBottom, setAtBottom] = useState(true);
  const [lastSeenIds, setLastSeenIds] = useState(() => new Set(entries.map(entry => entry.id)));
  const streamRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!following || !atBottom || !streamRef.current) return;
    streamRef.current.scrollTop = streamRef.current.scrollHeight;
    setLastSeenIds(new Set(entries.map(entry => entry.id)));
  }, [entries, following, atBottom]);

  const sources = useMemo(() => [...new Set(entries.map((entry) => entry.source))].sort(), [entries]);
  const jobs = useMemo(() => [...new Set(entries.map((entry) => entry.jobId).filter(Boolean))] as string[], [entries]);
  const workers = useMemo(() => [...new Set(entries.map((entry) => entry.workerId).filter(Boolean))] as string[], [entries]);
  const workerRoles = useMemo(() => [...new Set(entries.map((entry) => entry.workerRole).filter(Boolean))] as string[], [entries]);
  const providers = useMemo(() => [...new Set(entries.map((entry) => entry.provider).filter(Boolean))] as string[], [entries]);
  const models = useMemo(() => [...new Set(entries.map((entry) => entry.model).filter(Boolean))] as string[], [entries]);
  const filteredEntries = useMemo(() => filterLogEntries(entries, filters), [entries, filters]);
  const selected = entries.find((entry) => entry.id === selectedId) ?? filteredEntries[0];
  const unseen = following && atBottom ? 0 : entries.filter(entry => !lastSeenIds.has(entry.id)).length;

  function updateFilter<K extends keyof LogFilters>(key: K, value: LogFilters[K]) {
    setFilters((current) => ({ ...current, [key]: value === "all" ? undefined : value }));
  }

  function scrollToLatest() {
    setFollowing(true);
    setAtBottom(true);
    setLastSeenIds(new Set(entries.map(entry => entry.id)));
    requestAnimationFrame(() => {
      if (streamRef.current) streamRef.current.scrollTop = streamRef.current.scrollHeight;
    });
  }

  return (
    <div className="flex min-h-0 flex-1 flex-col bg-background">
      <header className="shrink-0 border-b border-border px-4 sm:px-6 lg:px-8 py-3 sm:py-4 lg:py-5">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="min-w-0"><div className="flex items-center gap-2"><span className="rounded border border-border bg-muted/50 px-1.5 py-0.5 text-xs font-semibold uppercase tracking-wider text-muted-foreground">{t("logs.diagnostics")}</span>{projectName && <span className="rounded border border-border bg-muted/50 px-1.5 py-0.5 text-xs font-semibold uppercase tracking-wider text-muted-foreground">{projectName}</span>}</div><h1 className="mt-1 text-base font-semibold tracking-tight">{t("logs.title")}</h1><p className="mt-0.5 text-xs text-muted-foreground">{t("logs.subtitle")}</p></div>
          <div className="flex shrink-0 items-center gap-2">
            <span className="text-xs tabular-nums text-muted-foreground">{t("logs.count", { filtered: filteredEntries.length, total: entries.length, limit: historyLimit })}</span>
            <Tooltip><TooltipTrigger render={<Button variant={following ? "secondary" : "outline"} size="xs" onClick={() => following ? setFollowing(false) : scrollToLatest()} aria-pressed={following}>{following ? <Pause className="size-3" /> : <Play className="size-3" />}{following ? t("logs.follow") : t("logs.paused")}</Button>} /><TooltipContent>{following ? t("logs.pauseFollow") : t("logs.resumeFollow")}</TooltipContent></Tooltip>
          </div>
        </div>
        {notice}
        {unavailable ? <p role="status" className="mt-2 text-xs text-muted-foreground">{t("usage.unavailable")}</p> : error ? <p role="alert" className="mt-2 text-xs text-destructive">{error}</p> : loading ? <p role="status" className="mt-2 text-xs text-muted-foreground">{t("common.loading")}</p> : null}
        <div className="mt-3 grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-10">
          <label className="col-span-2 flex min-w-0 items-center gap-2 rounded-md border border-border bg-background px-2 sm:col-span-3 lg:col-span-2 xl:col-span-2"><Search className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" /><Input value={filters.text ?? ""} onChange={(event) => updateFilter("text", event.target.value)} placeholder={t("logs.search")} aria-label={t("logs.searchLabel")} className="h-8 border-0 px-0 text-xs focus-visible:border-0" /></label>
          <FilterSelect label={t("logs.level")} value={filters.level ?? "all"} onChange={(value) => updateFilter("level", value as LogFilters["level"])}><FilterOption value="all" label={t("logs.allLevels")} />{LOG_LEVELS.map((level) => <FilterOption key={level} value={level} label={level} />)}</FilterSelect>
          <FilterSelect label={t("logs.source")} value={filters.source ?? "all"} onChange={(value) => updateFilter("source", value)}><FilterOption value="all" label={t("logs.allSources")} />{sources.map((source) => <FilterOption key={source} value={source} label={source} />)}</FilterSelect>
          <FilterSelect label={t("logs.job")} value={filters.jobId ?? "all"} onChange={(value) => updateFilter("jobId", value)}><FilterOption value="all" label={t("logs.allJobs")} />{jobs.map((job) => <FilterOption key={job} value={job} label={job} />)}</FilterSelect>
          <FilterSelect label={t("logs.worker")} value={filters.workerId ?? "all"} onChange={(value) => updateFilter("workerId", value)}><FilterOption value="all" label={t("logs.allWorkers")} />{workers.map((worker) => <FilterOption key={worker} value={worker} label={worker} />)}</FilterSelect>
          <FilterSelect label={t("logs.role")} value={filters.workerRole ?? "all"} onChange={(value) => updateFilter("workerRole", value)}><FilterOption value="all" label={t("logs.allRoles")} />{workerRoles.map((role) => <FilterOption key={role} value={role} label={role} />)}</FilterSelect>
          <FilterSelect label={t("logs.provider")} value={filters.provider ?? "all"} onChange={(value) => updateFilter("provider", value)}><FilterOption value="all" label={t("logs.allProviders")} />{providers.map((provider) => <FilterOption key={provider} value={provider} label={provider} />)}</FilterSelect>
          <FilterSelect label={t("logs.model")} value={filters.model ?? "all"} onChange={(value) => updateFilter("model", value)}><FilterOption value="all" label={t("logs.allModels")} />{models.map((model) => <FilterOption key={model} value={model} label={model} />)}</FilterSelect>
          <FilterSelect label={t("logs.timeWindow")} value={filters.timeWindow ?? "all"} onChange={(value) => updateFilter("timeWindow", value as LogTimeWindow)}><FilterOption value="all" label={t("logs.allTime")} /><FilterOption value="last-5-minutes" label={t("logs.last5Min")} /><FilterOption value="last-hour" label={t("logs.lastHour")} /><FilterOption value="last-day" label={t("logs.lastDay")} /></FilterSelect>
        </div>
      </header>
      <div className="grid min-h-0 flex-1 grid-rows-[minmax(120px,1fr)_minmax(160px,1fr)] sm:grid-rows-[minmax(240px,1fr)_minmax(260px,auto)] lg:grid-cols-[minmax(0,1.2fr)_minmax(320px,0.8fr)] lg:grid-rows-1">
        <section className="relative flex min-h-0 flex-col overflow-hidden border-b border-border lg:border-r lg:border-b-0" aria-label={t("logs.streamLabel")}>
          <div className="grid grid-cols-[68px_52px_minmax(0,1fr)] gap-2 border-b border-border bg-muted/20 px-3 py-2 text-[9px] font-semibold uppercase tracking-wider text-muted-foreground md:grid-cols-[86px_58px_110px_minmax(0,1fr)_minmax(120px,0.6fr)]"><span>{t("logs.time")}</span><span>{t("logs.level")}</span><span className="hidden md:block">{t("logs.source")}</span><span>{t("logs.message")}</span><span className="hidden md:block">{t("logs.context")}</span></div>
          <div ref={streamRef} onScroll={(event) => { const node = event.currentTarget; const bottom = node.scrollHeight - node.scrollTop - node.clientHeight < 24; setAtBottom(bottom); if (bottom) setLastSeenIds(new Set(entries.map(entry => entry.id))); }} className="min-h-0 flex-1 overflow-y-auto" role="log" aria-live="polite" aria-label={t("logs.entriesLabel")}>
            {filteredEntries.length === 0 && !loading && !error && !unavailable ? <EmptyState className="m-6 text-center">{t(entries.length ? "logs.noMatch" : "logs.empty")}</EmptyState> : <ul>{filteredEntries.map((entry) => <li key={entry.id} className="border-b border-border/60 last:border-b-0"><button type="button" onClick={() => setSelectedId(entry.id)} className={cn("grid w-full grid-cols-[68px_52px_minmax(0,1fr)] gap-2 px-3 py-2.5 text-left transition-colors hover:bg-muted/40 md:grid-cols-[86px_58px_110px_minmax(0,1fr)_minmax(120px,0.6fr)]", selected?.id === entry.id && "bg-muted/60") } aria-current={selected?.id === entry.id ? "true" : undefined}><time dateTime={entry.timestamp} className="pt-0.5 font-mono text-xs tabular-nums text-muted-foreground">{formatLocalClock(entry.timestamp)}</time><span className="pt-0.5"><LogLevelText level={entry.level} /></span><span title={[entry.source, entry.provenance ? t(PROVENANCE_KEYS[entry.provenance]) : undefined].filter(Boolean).join(" · ")} className="hidden min-w-0 truncate pt-0.5 text-xs text-muted-foreground md:block">{entry.source}{entry.provenance ? ` · ${t(PROVENANCE_KEYS[entry.provenance])}` : ""}</span><span className="min-w-0"><span className="block truncate text-xs leading-4">{entry.message}</span><span className="mt-0.5 block truncate text-xs text-muted-foreground md:hidden">{entry.source}{entry.provenance ? ` · ${t(PROVENANCE_KEYS[entry.provenance])}` : ""}{entry.category ? ` · ${entry.category}` : ""}</span></span><span className="hidden min-w-0 truncate pt-0.5 text-xs text-muted-foreground md:block">{entry.workerId ?? entry.jobId ?? entry.provider ?? "—"}</span></button></li>)}</ul>}
          </div>
          {unseen > 0 && <Button variant="secondary" size="xs" onClick={scrollToLatest} className="absolute bottom-3 left-1/2 -translate-x-1/2 shadow-sm">{t("logs.newLogs")} <span className="tabular-nums">{unseen}</span></Button>}
        </section>
        <aside className="flex min-h-0 min-w-0 overflow-hidden bg-muted/5" aria-label={t("logs.detailLabel")}><LogDetail entry={selected} onClose={() => setSelectedId(undefined)} /></aside>
      </div>
    </div>
  );
}
