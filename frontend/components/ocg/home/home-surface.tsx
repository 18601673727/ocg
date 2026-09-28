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
  selectHomeActiveMissions,
  selectRecentWork,
  selectResourceHealthSummary,
  selectHomeUsageSummary,
  selectRecentProductActivity,
} from "./selectors";
import type { AttentionItem, ActiveMissionProjection, ContinueWorkingEntry, ResourceHealthSummary, UsageSummary, RecentActivityItem } from "./domain";
import { formatCostMicros, formatPercent, formatTokens } from "../resource-ledger/format";
import type { RuntimeSnapshot } from "../runtime/runtime-types";

type HomeSurfaceProps = {
  snapshot: RuntimeSnapshot;
  onOpenChat: () => void;
  onOpenAttention: () => void;
  onOpenMissionControl: () => void;
  onOpenControlCenter: () => void;
  onOpenLedger: () => void;
  onOpenLogs: () => void;
  onOpenSettings: () => void;
};

const SEVERITY_STYLES: Record<AttentionItem["severity"], { dot: string; border: string; bg: string; text: string }> = {
  critical: { dot: "bg-red-500", border: "border-red-500/30", bg: "bg-red-500/10", text: "text-red-700 dark:text-red-400" },
  warning: { dot: "bg-amber-500", border: "border-amber-500/30", bg: "bg-amber-500/10", text: "text-amber-700 dark:text-amber-400" },
  attention: { dot: "bg-violet-500", border: "border-violet-500/30", bg: "bg-violet-500/10", text: "text-violet-700 dark:text-violet-400" },
  info: { dot: "bg-sky-500", border: "border-sky-500/30", bg: "bg-sky-500/10", text: "text-sky-700 dark:text-sky-400" },
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

const STATUS_LABELS: Record<ActiveMissionProjection["status"], string> = {
  planning: "Planning",
  running: "Running",
  paused: "Paused",
  completed: "Completed",
  failed: "Failed",
  "budget-exhausted": "Budget exhausted",
};

export function HomeSurface(props: HomeSurfaceProps) {
  const { snapshot } = props;
  const attention = useMemo(() => selectHomeAttention(snapshot), [snapshot]);
  const missions = useMemo(() => selectHomeActiveMissions(snapshot), [snapshot]);
  const recentWork = useMemo(() => selectRecentWork(snapshot.sessions), [snapshot.sessions]);
  const resourceHealth = useMemo(() => selectResourceHealthSummary(snapshot.bootstrap), [snapshot.bootstrap]);
  const usage = useMemo(() => selectHomeUsageSummary(snapshot.resourceLedger), [snapshot.resourceLedger]);
  const activity = useMemo(() => selectRecentProductActivity(snapshot), [snapshot]);
  const attentionHandlers: Record<AttentionItem["destination"], () => void> = {
    "mission-control": props.onOpenMissionControl,
    "control-center": props.onOpenControlCenter,
    "resource-ledger": props.onOpenLedger,
    logs: props.onOpenLogs,
    settings: props.onOpenSettings,
    onboarding: props.onOpenSettings,
  };

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
          <Button variant="default" size="sm" onClick={props.onOpenChat} className="shrink-0">
            <Sparkles className="mr-1.5 size-3.5" />
            Start with OCG
          </Button>
        </section>

        {/* Attention summary — View all navigates to the Attention Center. */}
        <AttentionSection items={attention} onNavigate={attentionHandlers} onOpenAttention={props.onOpenAttention} />

        {/* Main grid */}
        <div className="grid gap-5 lg:grid-cols-[1fr_320px] xl:grid-cols-[1fr_380px]">
          {/* Left column */}
          <div className="flex flex-col gap-5">
            <ActiveMissionsSection missions={missions} onOpenMissionControl={props.onOpenMissionControl} />
            <ContinueWorkingSection entries={recentWork} />
            <RecentActivitySection items={activity} />
          </div>

          {/* Right column */}
          <div className="flex flex-col gap-5">
            <ResourceHealthSection health={resourceHealth} onOpenControlCenter={props.onOpenControlCenter} />
            <UsageSummarySection usage={usage} onOpenLedger={props.onOpenLedger} />
          </div>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Attention section
// ---------------------------------------------------------------------------

function AttentionSection({ items, onNavigate, onOpenAttention }: { items: AttentionItem[]; onNavigate: Record<AttentionItem["destination"], () => void>; onOpenAttention: () => void }) {
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
      <div className="mb-2 flex items-center justify-between">
        <h2 className="text-[13px] font-semibold tracking-wider uppercase text-muted-foreground">Attention</h2>
        <Button variant="ghost" size="xs" onClick={onOpenAttention}>
          View all <ArrowRight className="ml-1 size-3" />
        </Button>
      </div>
      <div className="flex flex-col gap-2">
        {items.map((item) => {
          const style = SEVERITY_STYLES[item.severity];
          const onNavigateItem = onNavigate[item.destination];
          return (
            <button
              key={item.id}
              type="button"
              onClick={onNavigateItem}
              className={cn(
                "flex w-full items-start gap-3 rounded-lg border p-3 text-left transition-colors hover:opacity-90",
                style.border,
                style.bg,
              )}
              aria-label={`${item.severity} severity: ${item.title}`}
            >
              <span className={cn("size-2 shrink-0 rounded-full mt-1.5", style.dot)} aria-hidden="true" />
              <div className="min-w-0 flex-1">
                <div className="flex items-center gap-2">
                  <span className="text-[13px] font-semibold">{item.title}</span>
                  <span className={cn("rounded-full px-1.5 py-0.5 text-[10px] font-medium", style.bg, style.text)}>
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
// Active missions section
// ---------------------------------------------------------------------------

function ActiveMissionsSection({ missions, onOpenMissionControl }: { missions: ActiveMissionProjection[]; onOpenMissionControl: () => void }) {
  if (missions.length === 0) {
    return (
      <section aria-label="Active missions">
        <div className="flex items-center justify-between">
          <h2 className="text-[13px] font-semibold tracking-wider uppercase text-muted-foreground">Active Missions</h2>
        </div>
        <div className="mt-2 flex flex-col items-center justify-center rounded-lg border border-dashed border-border bg-muted/10 py-8">
          <Rocket className="mb-2 size-6 text-muted-foreground" aria-hidden="true" />
          <p className="text-sm text-muted-foreground">No active missions</p>
          <p className="text-[12px] text-muted-foreground">Start a Mission to see it here</p>
        </div>
      </section>
    );
  }

  return (
    <section aria-label="Active missions">
      <div className="mb-2 flex items-center justify-between">
        <h2 className="text-[13px] font-semibold tracking-wider uppercase text-muted-foreground">Active Missions</h2>
        <Button variant="ghost" size="xs" onClick={onOpenMissionControl}>
          Mission Control <ArrowRight className="ml-1 size-3" />
        </Button>
      </div>
      <div className="flex flex-col gap-2">
        {missions.map((mission) => (
          <MissionCard key={mission.id} mission={mission} onClick={onOpenMissionControl} />
        ))}
      </div>
    </section>
  );
}

function MissionCard({ mission, onClick }: { mission: ActiveMissionProjection; onClick: () => void }) {
  const statusLabel = STATUS_LABELS[mission.status] ?? mission.status;
  const waveInfo = mission.currentWave && mission.totalWaves ? `Wave ${mission.currentWave}/${mission.totalWaves}` : null;
  const budgetText = mission.budgetSpent !== undefined && mission.budgetLimit ? `$${mission.budgetSpent.toFixed(2)} / $${mission.budgetLimit.toFixed(2)}` : null;

  return (
    <button
      type="button"
      onClick={onClick}
      className="flex w-full items-start gap-3 rounded-lg border border-border bg-card p-3 text-left transition-colors hover:bg-muted/30"
      aria-label={`${mission.title} · ${statusLabel} · ${mission.completed} of ${mission.total} tasks`}
    >
      <div className="flex min-w-0 flex-1 flex-col gap-1.5">
        <div className="flex items-center gap-2">
          <span className="truncate text-[13px] font-semibold">{mission.title}</span>
          <span className="shrink-0 rounded-full px-1.5 py-0.5 text-[10px] font-medium capitalize bg-muted text-muted-foreground">
            {statusLabel}
          </span>
        </div>
        <div className="flex items-center gap-3 text-[12px] text-muted-foreground">
          <span>{mission.completed} / {mission.total} tasks</span>
          {mission.activeWorkers > 0 && <span>{mission.activeWorkers} active</span>}
          {mission.blockedWorkers > 0 && <span className="text-amber-600 dark:text-amber-400">{mission.blockedWorkers} blocked</span>}
          {waveInfo && <span>{waveInfo}</span>}
          <span className="ml-auto flex items-center gap-1"><Clock3 className="size-3" />{mission.elapsed}</span>
        </div>
        {budgetText && (
          <div className="flex items-center gap-2 text-[12px]">
            <span className="text-muted-foreground">{budgetText}</span>
            <span className="text-[11px] text-muted-foreground">({mission.progress}%)</span>
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
      <h2 className="mb-2 text-[13px] font-semibold tracking-wider uppercase text-muted-foreground">Continue Working</h2>
      <div className="flex flex-col gap-1">
        {entries.map((entry) => (
          <button
            key={entry.id}
            type="button"
            className="flex w-full items-center gap-3 rounded-md px-3 py-2 text-left transition-colors hover:bg-muted/40"
          >
            <div className="flex size-8 shrink-0 items-center justify-center rounded-md bg-muted">
              {entry.kind === "mission" ? (
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

function ResourceHealthSection({ health, onOpenControlCenter }: { health: ResourceHealthSummary; onOpenControlCenter: () => void }) {
  return (
    <section aria-label="Resource health">
      <div className="flex items-center justify-between">
        <h2 className="text-[13px] font-semibold tracking-wider uppercase text-muted-foreground">Resources</h2>
        <Button variant="ghost" size="xs" onClick={onOpenControlCenter}>
          Details <ArrowRight className="ml-1 size-3" />
        </Button>
      </div>
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

function UsageSummarySection({ usage, onOpenLedger }: { usage: UsageSummary; onOpenLedger: () => void }) {
  return (
    <section aria-label="Usage snapshot">
      <div className="flex items-center justify-between">
        <h2 className="text-[13px] font-semibold tracking-wider uppercase text-muted-foreground">Usage</h2>
        <Button variant="ghost" size="xs" onClick={onOpenLedger}>
          Ledger <ArrowRight className="ml-1 size-3" />
        </Button>
      </div>
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
        <h2 className="mb-2 text-[13px] font-semibold tracking-wider uppercase text-muted-foreground">Recent Activity</h2>
        <div className="flex flex-col items-center justify-center rounded-lg border border-dashed border-border bg-muted/10 py-6">
          <Clock3 className="mb-2 size-5 text-muted-foreground" aria-hidden="true" />
          <p className="text-[12px] text-muted-foreground">No recent activity to report</p>
        </div>
      </section>
    );
  }

  return (
    <section aria-label="Recent activity">
      <h2 className="mb-2 text-[13px] font-semibold tracking-wider uppercase text-muted-foreground">Recent Activity</h2>
      <div className="flex flex-col gap-1">
        {items.map((item) => (
          <div key={item.id} className="flex items-start gap-3 rounded-md px-3 py-2">
            <span className={cn("size-1.5 shrink-0 rounded-full mt-1.5", TONE_COLORS[item.tone])} aria-hidden="true" />
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

const TONE_COLORS: Record<RecentActivityItem["tone"], string> = {
  emerald: "bg-emerald-500",
  amber: "bg-amber-500",
  red: "bg-red-500",
  violet: "bg-violet-500",
  slate: "bg-slate-400",
};
