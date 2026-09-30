"use client";

import { useMemo } from "react";
import {
  ArrowRight,
  Clock3,
  Database,
  LayoutDashboard,
  MapPin,
  Rocket,
  Search,
  ShieldCheck,
  Sparkles,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import {
  selectHomeAttention,
  selectHomeActiveJobs,
  selectRecentWork,
  selectResourceHealthSummary,
  selectHomeUsageSummary,
  selectRecentProductActivity,
} from "./selectors";
import type {
  AttentionDestination,
  AttentionItem,
  ActiveJobProjection,
  ContinueWorkingEntry,
  RecentActivityItem,
  ResourceHealthSummary,
  UsageSummary,
} from "./domain";
import { formatCostMicros, formatPercent, formatTokens } from "@/lib/format";
import {
  DOT_TONE,
  EmptyPanel,
  SectionHeading,
  SURFACE_TONE,
  TEXT_TONE,
  type Tone,
} from "@/components/ocg/primitives";
import type { RuntimeSnapshot } from "../runtime/runtime-types";
import type { WorkspaceView } from "../layout/view-domain";

type HomeSurfaceProps = {
  snapshot: RuntimeSnapshot;
  /** Opens a workspace; the shell owns the address and the close-back-to-chat. */
  onNavigate: (view: WorkspaceView) => void;
};

/** Where a home attention row sends the operator. Onboarding lives in Settings. */
const DESTINATION_VIEWS: Record<AttentionDestination, WorkspaceView> = {
  "job-execution": "job-execution",
  "control-center": "control-center",
  "resource-ledger": "ledger",
  logs: "logs",
  settings: "settings",
  onboarding: "settings",
};

/** Home's severity scale is its own; only the colours are shared. */
const SEVERITY_TONE: Record<AttentionItem["severity"], Tone> = {
  critical: "red",
  warning: "amber",
  attention: "violet",
  info: "sky",
};

const KIND_LABELS: Record<AttentionItem["kind"], string> = {
  approvalRequired: "Approval",
  budgetGate: "Budget gate",
  blockedTask: "Blocked task",
  providerUnavailable: "Provider unavailable",
  runtimeFailure: "Runtime failure",
  verificationFailure: "Verification failure",
  configurationIssue: "Configuration",
  authenticationRequired: "Authentication",
  degradedResource: "Degraded resource",
};

const STATUS_LABELS: Record<ActiveJobProjection["status"], string> = {
  running: "Running",
  pending: "Pending",
};

export function HomeSurface(props: HomeSurfaceProps) {
  const { snapshot } = props;
  const attention = useMemo(() => selectHomeAttention(snapshot), [snapshot]);
  const jobs = useMemo(() => selectHomeActiveJobs(snapshot), [snapshot]);
  const recentWork = useMemo(() => selectRecentWork(snapshot.sessions), [snapshot.sessions]);
  const resourceHealth = useMemo(() => selectResourceHealthSummary(snapshot.bootstrap), [snapshot.bootstrap]);
  const usage = useMemo(() => selectHomeUsageSummary(snapshot.resourceLedger), [snapshot.resourceLedger]);
  const activity = useMemo(() => selectRecentProductActivity(snapshot), [snapshot]);

  return (
    <div className="flex min-h-0 flex-1 overflow-y-auto">
      <div className="flex w-full flex-col gap-5 p-4 sm:p-6 lg:p-8">
        {/* Hero entry */}
        <section className="flex items-start justify-between gap-4">
          <div className="min-w-0">
            <h1 className="text-[22px] font-semibold tracking-tight">Welcome back</h1>
            <p className="mt-1 text-sm text-muted-foreground">
              {attention.length > 0
                ? `${attention.length} item${attention.length > 1 ? "s" : ""} need${attention.length > 1 ? "" : "s"} your attention`
                : "Everything looks operational"}
              {resourceHealth.hasDegradedOrAuthRequired ? " — some resources need review" : ""}
            </p>
          </div>
          <Button variant="default" size="sm" onClick={() => props.onNavigate("chat")} className="shrink-0">
            <Sparkles className="mr-1.5 size-3.5" />
            Start with OCG
          </Button>
        </section>

        {/* Attention summary — View all navigates to the Attention Center. */}
        <AttentionSection items={attention} onNavigate={props.onNavigate} />

        {/* Main grid */}
        <div className="grid gap-5 lg:grid-cols-[1fr_320px] xl:grid-cols-[1fr_380px]">
          {/* Left column */}
          <div className="flex flex-col gap-5">
            <ActiveJobsSection jobs={jobs} onNavigate={props.onNavigate} />
            <ContinueWorkingSection entries={recentWork} />
            <RecentActivitySection items={activity} />
          </div>

          {/* Right column */}
          <div className="flex flex-col gap-5">
            <ResourceHealthSection health={resourceHealth} onNavigate={props.onNavigate} />
            <UsageSummarySection usage={usage} onNavigate={props.onNavigate} />
          </div>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Attention section
// ---------------------------------------------------------------------------

function AttentionSection({
  items,
  onNavigate,
}: {
  items: AttentionItem[];
  onNavigate: (view: WorkspaceView) => void;
}) {
  if (items.length === 0) {
    return (
      <section aria-label="Attention">
        <div className="flex items-center gap-2 rounded-lg border border-border bg-muted/20 px-4 py-3">
          <ShieldCheck className="size-4 text-emerald-500" aria-hidden="true" />
          <span className="text-sm font-medium">No action required</span>
          <span className="ml-auto text-xs text-muted-foreground">Workspace is healthy</span>
        </div>
      </section>
    );
  }

  return (
    <section aria-label="Attention items">
      <SectionHeading
        title="Attention"
        action={<Button variant="ghost" size="xs" onClick={() => onNavigate("attention")}>View all <ArrowRight className="ml-1 size-3" /></Button>}
      />
      <div className="flex flex-col gap-2">
        {items.map((item) => {
          const tone = SEVERITY_TONE[item.severity];
          return (
            <button
              key={item.id}
              type="button"
              onClick={() => onNavigate(DESTINATION_VIEWS[item.destination])}
              className={cn(
                "flex w-full items-start gap-3 rounded-lg border p-3 text-left transition-colors hover:opacity-90",
                SURFACE_TONE[tone],
              )}
              aria-label={`${item.severity} severity: ${item.title}`}
            >
              <span className={cn("size-2 shrink-0 rounded-full mt-1.5", DOT_TONE[tone])} aria-hidden="true" />
              <div className="min-w-0 flex-1">
                <div className="flex items-center gap-2">
                  <span className="text-[13px] font-semibold">{item.title}</span>
                  <span className={cn("rounded-full px-1.5 py-0.5 text-[10px] font-medium", SURFACE_TONE[tone], TEXT_TONE[tone])}>
                    {KIND_LABELS[item.kind]}
                  </span>
                </div>
                <p className="mt-0.5 text-[12px] text-muted-foreground">{item.summary}</p>
              </div>
            </button>
          );
        })}
      </div>
    </section>
  );
}

// ---------------------------------------------------------------------------
// Active jobs section
// ---------------------------------------------------------------------------

function ActiveJobsSection({
  jobs,
  onNavigate,
}: {
  jobs: ActiveJobProjection[];
  onNavigate: (view: WorkspaceView) => void;
}) {
  const openJobExecution = () => onNavigate("job-execution");
  if (jobs.length === 0) {
    return (
      <section aria-label="Active jobs">
        <SectionHeading title="Active Jobs" />
        <EmptyPanel icon={Rocket} title="No active jobs" hint="Start a Job to see it here" className="mt-2" />
      </section>
    );
  }

  return (
    <section aria-label="Active jobs">
      <SectionHeading
        title="Active Jobs"
        action={<Button variant="ghost" size="xs" onClick={openJobExecution}>Job Execution <ArrowRight className="ml-1 size-3" /></Button>}
      />
      <div className="flex flex-col gap-2">
        {jobs.map((job) => (
          <JobCard key={job.id} job={job} onClick={openJobExecution} />
        ))}
      </div>
    </section>
  );
}

function JobCard({ job, onClick }: { job: ActiveJobProjection; onClick: () => void }) {
  const statusLabel = STATUS_LABELS[job.status] ?? job.status;
  const waveInfo = job.currentWave && job.totalWaves ? `Wave ${job.currentWave}/${job.totalWaves}` : null;
  const budgetText = job.budgetSpent !== undefined && job.budgetLimit ? `$${job.budgetSpent.toFixed(2)} / $${job.budgetLimit.toFixed(2)}` : null;

  return (
    <button
      type="button"
      onClick={onClick}
      className="flex w-full items-start gap-3 rounded-lg border border-border bg-card p-3 text-left transition-colors hover:bg-muted/30"
      aria-label={`${job.title} · ${statusLabel} · ${job.completed} of ${job.total} calls`}
    >
      <div className="flex min-w-0 flex-1 flex-col gap-1.5">
        <div className="flex items-center gap-2">
          <span className="truncate text-[13px] font-semibold">{job.title}</span>
          <span className="shrink-0 rounded-full px-1.5 py-0.5 text-[10px] font-medium capitalize bg-muted text-muted-foreground">
            {statusLabel}
          </span>
        </div>
        <div className="flex items-center gap-3 text-[12px] text-muted-foreground">
          <span>{job.completed} / {job.total} calls</span>
          {job.activeWorkers > 0 && <span>{job.activeWorkers} active</span>}
          {job.blockedWorkers > 0 && <span className="text-amber-600 dark:text-amber-400">{job.blockedWorkers} blocked</span>}
          {waveInfo && <span>{waveInfo}</span>}
          <span className="ml-auto flex items-center gap-1"><Clock3 className="size-3" />{job.elapsed}</span>
        </div>
        {budgetText && (
          <div className="flex items-center gap-2 text-[12px]">
            <span className="text-muted-foreground">{budgetText}</span>
            <span className="text-[11px] text-muted-foreground">({job.progress}%)</span>
          </div>
        )}
      </div>
      <ArrowRight className="size-4 shrink-0 text-muted-foreground mt-1" aria-hidden="true" />
    </button>
  );
}

// ---------------------------------------------------------------------------
// Continue working section
// ---------------------------------------------------------------------------

function ContinueWorkingSection({ entries }: { entries: ContinueWorkingEntry[] }) {
  if (entries.length === 0) return null;

  return (
    <section aria-label="Continue working">
      <SectionHeading title="Continue Working" />
      <div className="flex flex-col gap-1">
        {entries.map((entry) => (
          <button
            key={entry.id}
            type="button"
            className="flex w-full items-center gap-3 rounded-md px-3 py-2 text-left transition-colors hover:bg-muted/40"
          >
            <div className="flex size-8 shrink-0 items-center justify-center rounded-md bg-muted">
              {entry.kind === "job" ? (
                <LayoutDashboard className="size-3.5 text-muted-foreground" />
              ) : entry.kind === "diagnostics" ? (
                <Search className="size-3.5 text-muted-foreground" />
              ) : (
                <MapPin className="size-3.5 text-muted-foreground" />
              )}
            </div>
            <div className="min-w-0 flex-1">
              <p className="truncate text-[13px] font-medium">{entry.title}</p>
              <p className="text-[11px] text-muted-foreground">{entry.subtitle} · {entry.timeAgo}</p>
            </div>
            <ArrowRight className="size-3 shrink-0 text-muted-foreground" aria-hidden="true" />
          </button>
        ))}
      </div>
    </section>
  );
}

// ---------------------------------------------------------------------------
// Resource health section
// ---------------------------------------------------------------------------

function ResourceHealthSection({
  health,
  onNavigate,
}: {
  health: ResourceHealthSummary;
  onNavigate: (view: WorkspaceView) => void;
}) {
  return (
    <section aria-label="Resource health">
      <SectionHeading
        title="Resources"
        action={<Button variant="ghost" size="xs" onClick={() => onNavigate("control-center")}>Details <ArrowRight className="ml-1 size-3" /></Button>}
      />
      <div className="mt-2 space-y-2.5 rounded-lg border border-border bg-card p-3">
        <div className="flex items-center justify-between">
          <span className="text-[12px] text-muted-foreground">{health.providerCount} providers</span>
          <span className="text-[12px] font-medium">{health.healthyProviders} healthy</span>
        </div>
        {health.degradedProviders > 0 && (
          <div className="flex items-center justify-between">
            <span className="text-[12px] text-amber-600 dark:text-amber-400">{health.degradedProviders} degraded</span>
            <span className="text-[11px] text-muted-foreground">needs attention</span>
          </div>
        )}
        {health.authRequiredProviders > 0 && (
          <div className="flex items-center justify-between">
            <span className="text-[12px] text-violet-600 dark:text-violet-400">{health.authRequiredProviders} auth required</span>
            <span className="text-[11px] text-muted-foreground">action needed</span>
          </div>
        )}
        {health.unavailableProviders > 0 && (
          <div className="flex items-center justify-between">
            <span className="text-[12px] text-red-600 dark:text-red-400">{health.unavailableProviders} unavailable</span>
            <span className="text-[11px] text-muted-foreground">offline</span>
          </div>
        )}
        <div className="mt-1 h-px bg-border" aria-hidden="true" />
        <div className="flex items-center justify-between">
          <span className="text-[12px] text-muted-foreground">Active profile</span>
          <span className="text-[12px] font-medium">{health.activeProfileLabel}</span>
        </div>
        <div className="flex items-center justify-between">
          <span className="text-[12px] text-muted-foreground">Models</span>
          <span className="text-[12px] font-medium">{health.availableModels} / {health.modelCount} available</span>
        </div>
      </div>
    </section>
  );
}

// ---------------------------------------------------------------------------
// Usage summary section
// ---------------------------------------------------------------------------

function UsageSummarySection({
  usage,
  onNavigate,
}: {
  usage: UsageSummary;
  onNavigate: (view: WorkspaceView) => void;
}) {
  return (
    <section aria-label="Usage snapshot">
      <SectionHeading
        title="Usage"
        action={<Button variant="ghost" size="xs" onClick={() => onNavigate("ledger")}>Ledger <ArrowRight className="ml-1 size-3" /></Button>}
      />
      <div className="mt-2 space-y-2.5 rounded-lg border border-border bg-card p-3">
        {usage.available ? (
          <>
            <div className="flex items-center justify-between">
              <span className="text-[12px] text-muted-foreground">Cost</span>
              <span className="text-[12px] font-medium">{formatCostMicros(usage.costMicros)}</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-[12px] text-muted-foreground">Tokens</span>
              <span className="text-[12px] font-medium tabular-nums">{formatTokens(usage.totalTokens)}</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-[12px] text-muted-foreground">Cache read</span>
              <span className="text-[12px] font-medium tabular-nums">{formatTokens(usage.cacheRead)}</span>
            </div>
            {usage.cacheShare !== null && (
              <div className="flex items-center justify-between">
                <span className="text-[12px] text-muted-foreground">Cache share</span>
                <span className="text-[12px] font-medium tabular-nums">{formatPercent(usage.cacheShare)}</span>
              </div>
            )}
            {usage.cacheLeverage !== null && (
              <div className="flex items-center justify-between">
                <span className="text-[12px] text-muted-foreground">Cache leverage</span>
                <span className="text-[12px] font-medium tabular-nums">{formatPercent(usage.cacheLeverage)}</span>
              </div>
            )}
            <div className="mt-1 h-px bg-border" aria-hidden="true" />
            <div className="flex items-center justify-between text-[11px] text-muted-foreground">
              <span>{usage.entryCount} entries</span>
              <span>{usage.costProvenance}</span>
            </div>
          </>
        ) : (
          <div className="flex items-center justify-center py-2 text-[12px] text-muted-foreground">
            <Database className="mr-1.5 size-3" /> No resource ledger data available
          </div>
        )}
      </div>
    </section>
  );
}

// ---------------------------------------------------------------------------
// Recent activity section
// ---------------------------------------------------------------------------

function RecentActivitySection({ items }: { items: RecentActivityItem[] }) {
  if (items.length === 0) {
    return (
      <section aria-label="Recent activity">
        <SectionHeading title="Recent Activity" />
        <EmptyPanel icon={Clock3} title="No recent activity to report" />
      </section>
    );
  }

  return (
    <section aria-label="Recent activity">
      <SectionHeading title="Recent Activity" />
      <div className="flex flex-col gap-1">
        {items.map((item) => (
          <div key={item.id} className="flex items-start gap-3 rounded-md px-3 py-2">
            <span className={cn("size-1.5 shrink-0 rounded-full mt-1.5", DOT_TONE[item.tone])} aria-hidden="true" />
            <div className="min-w-0 flex-1">
              <p className="text-[12px] leading-snug text-foreground">{item.summary}</p>
              <p className="text-[11px] text-muted-foreground">{item.timeAgo}</p>
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}
