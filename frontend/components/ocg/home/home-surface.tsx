"use client";

import { useMemo, ViewTransition } from "react";
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
  PageSurface,
  SectionHeading,
  JOB_STATE,
  Pill,
  SURFACE_TONE,
  TEXT_TONE,
  type Tone,
} from "@/components/ocg/primitives";
import type { RuntimeSnapshot } from "../runtime/runtime-types";
import type { WorkspaceView } from "../layout/view-domain";
import { runtimeStateLabel, useI18n } from "../i18n";

type HomeSurfaceProps = {
  snapshot: RuntimeSnapshot;
  /** Opens a workspace; the shell owns the address and the close-back-to-chat. */
  onNavigate: (view: WorkspaceView) => void;
  onSelectSession?: (sessionId: string) => void;
  /** Opens a Project-owned Job. Keyed by jobId, not by Chat session. */
  onSelectJob?: (jobId: string) => void;
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

/** English fallback retained for non-React callers; UI uses i18n keys. */
export const HOME_KIND_LABELS: Record<AttentionItem["kind"], string> = {
  approvalRequired: "Approval",
  budgetGate: "Budget gate",
  blockedTask: "Blocked task",
  providerUnavailable: "Provider unavailable",
  modelUnavailable: "Model unavailable",
  runtimeFailure: "Runtime failure",
  verificationFailure: "Verification failure",
  configurationIssue: "Configuration",
  authenticationRequired: "Authentication",
  degradedResource: "Degraded resource",
};

export const HOME_STATUS_LABELS: Record<ActiveJobProjection["status"], string> = {
  unknown: "Unknown",
  running: "Running",
  eligible: "Eligible",
  cancelling: "Cancelling",
  pending: "Pending",
  completed: "Completed",
  failed: "Failed",
  cancelled: "Cancelled",
  orphaned: "Orphaned",
};

export function HomeSurface(props: HomeSurfaceProps) {
  const { t } = useI18n();
  const { snapshot } = props;
  const canonical = snapshot.authority === "canonical";
  const attention = useMemo(() => selectHomeAttention(snapshot, t), [snapshot, t]);
  const jobs = useMemo(() => selectHomeActiveJobs(snapshot), [snapshot]);
  const recentWork = useMemo(() => selectRecentWork(snapshot.sessions, t), [snapshot.sessions, t]);
  const resourceHealth = useMemo(() => selectResourceHealthSummary(snapshot.bootstrap, snapshot.status), [snapshot.bootstrap, snapshot.status]);
  const resourcesReportedHealthy =
    resourceHealth.providerCount !== null &&
    resourceHealth.providerCount > 0 &&
    resourceHealth.connectedProviders !== null &&
    resourceHealth.connectedProviders > 0 &&
    resourceHealth.availableModels !== null &&
    resourceHealth.availableModels > 0 &&
    !resourceHealth.hasUnreportedResourceState &&
    !resourceHealth.hasProviderIssues &&
    resourceHealth.unavailableModels === 0;
  const usage = useMemo(() => selectHomeUsageSummary(snapshot.resourceLedger), [snapshot.resourceLedger]);
  const activity = useMemo(() => selectRecentProductActivity(snapshot, t), [snapshot, t]);

  return (
    <PageSurface className="flex flex-col gap-5">
      {/* Hero entry */}
      <section className="flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0">
          <h1 className="text-[22px] font-semibold tracking-tight">{t("home.welcome")}</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            {attention.length > 0
              ? t(attention.length === 1 ? "home.attention.one" : "home.attention.other", { count: attention.length })
              : t("home.realWelcome")}
            {resourceHealth.hasProviderIssues ? t("home.resourcesNeedReview") : ""}
          </p>
        </div>
        <Button variant="default" size="sm" onClick={() => props.onNavigate("chat")} className="shrink-0">
          <Sparkles className="mr-1.5 size-3.5" />
          {t("home.start")}
        </Button>
      </section>

      {/* Attention summary — the shell owns selection and drill-down. */}
      <AttentionSection items={attention} onNavigate={props.onNavigate} onSelectJob={props.onSelectJob} statusKnownAndHealthy={resourcesReportedHealthy} />

      {/* Main grid */}
      <div className={cn("grid gap-5", !canonical && "lg:grid-cols-[1fr_320px] xl:grid-cols-[1fr_380px]")}>
        {/* Left column */}
        <div className="flex flex-col gap-5">
          <ActiveJobsSection jobs={jobs} onNavigate={props.onNavigate} onSelectJob={props.onSelectJob} />
          <ContinueWorkingSection entries={recentWork} onSelectSession={props.onSelectSession ?? (() => props.onNavigate("chat"))} />
          <RecentActivitySection items={activity} />
        </div>

        {/* Right column */}
        {!canonical && <div className="flex flex-col gap-5">
          <ResourceHealthSection health={resourceHealth} onNavigate={props.onNavigate} />
          <UsageSummarySection usage={usage} onNavigate={props.onNavigate} />
        </div>}
      </div>
    </PageSurface>
  );
}

// ---------------------------------------------------------------------------
// Attention section
// ---------------------------------------------------------------------------

function AttentionSection({
  items,
  onNavigate,
  onSelectJob,
  statusKnownAndHealthy,
}: {
  items: AttentionItem[];
  onNavigate: (view: WorkspaceView) => void;
  onSelectJob?: (sessionId: string) => void;
  statusKnownAndHealthy: boolean;
}) {
  const { t } = useI18n();
  const kindLabel = (kind: AttentionItem["kind"]) =>
    t(
      kind === "approvalRequired"
        ? "home.kind.approval"
        : kind === "budgetGate"
          ? "home.kind.budget"
          : kind === "blockedTask"
            ? "home.kind.blocked"
            : kind === "providerUnavailable"
              ? "home.kind.provider"
              : kind === "modelUnavailable"
                ? "home.kind.model"
              : kind === "runtimeFailure"
                ? "home.kind.runtime"
                : kind === "verificationFailure"
                  ? "home.kind.verification"
                  : kind === "configurationIssue"
                    ? "home.kind.configuration"
                    : kind === "authenticationRequired"
                      ? "home.kind.authentication"
                      : "home.kind.degraded",
    );
  if (items.length === 0) {
    return (
      <section aria-label={t("attention.title")}>
        <div className="flex items-center gap-2 rounded-lg border border-border bg-muted/20 px-4 py-3">
          {statusKnownAndHealthy ? (
            <ShieldCheck className="size-4 text-emerald-500" aria-hidden="true" />
          ) : (
            <Database className="size-4 text-muted-foreground" aria-hidden="true" />
          )}
          <span className="text-sm font-medium">{t(statusKnownAndHealthy ? "home.noAction" : "home.noAttentionRecorded")}</span>
          <span className="ml-auto text-xs text-muted-foreground">
            {t(statusKnownAndHealthy ? "home.workspaceHealthy" : "home.statusNotFullyReported")}
          </span>
        </div>
      </section>
    );
  }

  return (
    <section aria-label="Attention items">
      <SectionHeading
        title={t("home.attentionTitle")}
        action={<Button variant="ghost" size="xs" onClick={() => onNavigate("attention")}>{t("home.viewAll")} <ArrowRight className="ml-1 size-3" /></Button>}
      />
      <div className="flex flex-col gap-2">
        {items.map((item) => {
          const tone = SEVERITY_TONE[item.severity];
          return (
            <button
              key={item.id}
              type="button"
              onClick={() => {
                if (item.destination === "job-execution" && item.sessionId && onSelectJob) {
                  onSelectJob(item.sessionId);
                } else {
                  onNavigate(DESTINATION_VIEWS[item.destination]);
                }
              }}
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
                    {kindLabel(item.kind)}
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
  onSelectJob,
}: {
  jobs: ActiveJobProjection[];
  onNavigate: (view: WorkspaceView) => void;
  /** Opens a Project-owned Job. Keyed by jobId, not by Chat session. */
  onSelectJob?: (jobId: string) => void;
}) {
  const { t } = useI18n();
  const openJob = (job: ActiveJobProjection) => {
    if (onSelectJob) onSelectJob(job.jobId);
    else onNavigate("job-execution");
  };
  if (jobs.length === 0) {
    return (
      <section aria-label="Active jobs">
        <SectionHeading title={t("home.activeJobs")} />
        <EmptyPanel icon={Rocket} title={t("home.noActiveJobs")} hint={t("home.startJobHint")} className="mt-2" />
      </section>
    );
  }

  return (
    <section aria-label="Active jobs">
      <SectionHeading
        title={t("home.activeJobs")}
        action={<Button variant="ghost" size="xs" onClick={() => openJob(jobs[0])}>{t("home.jobExecution")} <ArrowRight className="ml-1 size-3" /></Button>}
      />
      <div className="flex flex-col gap-2">
        {jobs.map((job) => (
          <JobCard key={job.id} job={job} onClick={() => openJob(job)} />
        ))}
      </div>
    </section>
  );
}

function JobCard({ job, onClick }: { job: ActiveJobProjection; onClick: () => void }) {
  const { t } = useI18n();
  const statusLabel = runtimeStateLabel(t, job.status);
  const status = JOB_STATE[job.status];

  return (
    <button
      type="button"
      onClick={onClick}
      className="flex w-full items-start gap-3 rounded-lg border border-border bg-card p-3 text-left transition-colors hover:bg-muted/30"
      aria-label={`${job.title} · ${statusLabel}`}
    >
      <div className="flex min-w-0 flex-1 flex-col gap-1.5">
        <div className="flex items-center gap-2">
          <ViewTransition name={`job-${job.jobId}`} share="vt-shared" default="none">
            <span className="truncate text-[13px] font-semibold">{job.title}</span>
          </ViewTransition>
          <Pill tone={status.tone} dot pulse={status.pulse}>
            {statusLabel}
          </Pill>
        </div>
        <div className="flex items-center gap-3 text-[12px] text-muted-foreground">
          <span>{t("home.projectLabel", { project: job.projectId })}</span>
        </div>
      </div>
      <ArrowRight className="size-4 shrink-0 text-muted-foreground mt-1" aria-hidden="true" />
    </button>
  );
}

// ---------------------------------------------------------------------------
// Continue working section
// ---------------------------------------------------------------------------

function ContinueWorkingSection({ entries, onSelectSession }: { entries: ContinueWorkingEntry[]; onSelectSession: (sessionId: string) => void }) {
  const { t } = useI18n();
  if (entries.length === 0) return null;

  return (
    <section aria-label="Continue working">
      <SectionHeading title={t("home.continueWorking")} />
      <div className="flex flex-col gap-1">
        {entries.map((entry) => (
          <button
            key={entry.id}
            type="button"
            onClick={() => { if (entry.sessionId) onSelectSession(entry.sessionId); }}
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
  const { t } = useI18n();
  return (
    <section aria-label="Resource health">
      <SectionHeading
        title={t("home.resources")}
        action={<Button variant="ghost" size="xs" onClick={() => onNavigate("control-center")}>{t("common.details")} <ArrowRight className="ml-1 size-3" /></Button>}
      />
      <div className="mt-2 space-y-2.5 rounded-lg border border-border bg-card p-3">
        <div className="flex items-center justify-between">
          <span className="text-[12px] text-muted-foreground">Providers</span>
          <span className="text-[12px] font-medium">
            {health.providerCount === null ? t("common.notReported") : `${health.providerCount} reported`}
          </span>
        </div>
        {health.connectedProviders !== null && health.connectedProviders > 0 && (
          <div className="flex items-center justify-between">
            <span className="text-[12px] text-muted-foreground">Connected</span>
            <span className="text-[12px] font-medium">{health.connectedProviders}</span>
          </div>
        )}
        {health.degradedProviders !== null && health.degradedProviders > 0 && (
          <div className="flex items-center justify-between">
            <span className="text-[12px] text-amber-600 dark:text-amber-400">{health.degradedProviders} degraded</span>
            <span className="text-[11px] text-muted-foreground">needs attention</span>
          </div>
        )}
        {health.authRequiredProviders !== null && health.authRequiredProviders > 0 && (
          <div className="flex items-center justify-between">
            <span className="text-[12px] text-violet-600 dark:text-violet-400">{health.authRequiredProviders} auth required</span>
            <span className="text-[11px] text-muted-foreground">action needed</span>
          </div>
        )}
        {health.unavailableProviders !== null && health.unavailableProviders > 0 && (
          <div className="flex items-center justify-between">
            <span className="text-[12px] text-red-600 dark:text-red-400">{health.unavailableProviders} unavailable</span>
            <span className="text-[11px] text-muted-foreground">runtime reported</span>
          </div>
        )}
        {health.unknownProviders !== null && health.unknownProviders > 0 && (
          <div className="flex items-center justify-between">
            <span className="text-[12px] text-muted-foreground">{health.unknownProviders} unknown</span>
            <span className="text-[11px] text-muted-foreground">state not reported</span>
          </div>
        )}
        <div className="mt-1 h-px bg-border" aria-hidden="true" />
        <div className="flex items-center justify-between">
          <span className="text-[12px] text-muted-foreground">{t("home.activeProfile")}</span>
          <span className="text-[12px] font-medium">{health.activeProfileLabel ?? t("common.notReported")}</span>
        </div>
        <div className="flex items-center justify-between">
          <span className="text-[12px] text-muted-foreground">{t("home.models")}</span>
          <span className="text-[12px] font-medium">
            {health.modelCount === null || health.availableModels === null
              ? t("common.notReported")
              : `${health.availableModels} available / ${health.modelCount} reported`}
          </span>
        </div>
        {health.pendingModels !== null && health.pendingModels > 0 && <p className="text-[11px] text-muted-foreground">{health.pendingModels} pending</p>}
        {health.unavailableModels !== null && health.unavailableModels > 0 && <p className="text-[11px] text-red-600 dark:text-red-400">{health.unavailableModels} unavailable</p>}
        {health.unknownModels !== null && health.unknownModels > 0 && <p className="text-[11px] text-muted-foreground">{health.unknownModels} unknown</p>}
        <div className="flex items-center justify-between">
          <span className="text-[12px] text-muted-foreground">Runtime</span>
          <span className="text-[12px] font-medium">{runtimeStateLabel(t, health.runtimeState)}</span>
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
  const { t } = useI18n();
  return (
    <section aria-label="Usage snapshot">
      <SectionHeading
        title={t("home.usage")}
        action={<Button variant="ghost" size="xs" onClick={() => onNavigate("ledger")}>{t("ledger.title")} <ArrowRight className="ml-1 size-3" /></Button>}
      />
      <div className="mt-2 space-y-2.5 rounded-lg border border-border bg-card p-3">
        {usage.available ? (
          <>
            <div className="flex items-center justify-between">
              <span className="text-[12px] text-muted-foreground">{t("home.cost")}</span>
              <span className="text-[12px] font-medium">{formatCostMicros(usage.costMicros)}</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-[12px] text-muted-foreground">{t("home.tokens")}</span>
              <span className="text-[12px] font-medium tabular-nums">{formatTokens(usage.totalTokens)}</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-[12px] text-muted-foreground">{t("home.cacheRead")}</span>
              <span className="text-[12px] font-medium tabular-nums">{formatTokens(usage.cacheRead)}</span>
            </div>
            {usage.cacheShare !== null && (
              <div className="flex items-center justify-between">
                <span className="text-[12px] text-muted-foreground">{t("home.cacheShare")}</span>
                <span className="text-[12px] font-medium tabular-nums">{formatPercent(usage.cacheShare)}</span>
              </div>
            )}
            {usage.cacheLeverage !== null && (
              <div className="flex items-center justify-between">
                <span className="text-[12px] text-muted-foreground">{t("home.cacheLeverage")}</span>
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
            <Database className="mr-1.5 size-3" /> {t("home.noLedger")}
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
  const { t } = useI18n();
  if (items.length === 0) {
    return (
      <section aria-label="Recent activity">
        <SectionHeading title={t("home.recentActivity")} />
        <EmptyPanel icon={Clock3} title={t("home.noActivity")} />
      </section>
    );
  }

  return (
    <section aria-label="Recent activity">
      <SectionHeading title={t("home.recentActivity")} />
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
