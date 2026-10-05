/**
 * Pure Home selectors.
 *
 * Every Home projection is derived from the existing normalized runtime
 * snapshot. No new state is introduced here and no values are invented.
 */

import type { I18nKey, TranslateFn } from "../i18n";
import { runtimeStateLabel } from "../i18n";
import type { AttentionItem, AttentionSeverity, ActiveJobProjection, ContinueWorkingEntry, ResourceHealthSummary as HomeResourceHealthSummary, UsageSummary, RecentActivityItem } from "./domain";
import type { BootstrapState } from "../bootstrap/types";
import type { JobExecution } from "../execution/domain";
import type { JobAccounting } from "../execution/accounting";
import type { ResourceLedger } from "../resource-ledger/types";
import { sumCostMicros } from "../resource-ledger/selectors";

// ---------------------------------------------------------------------------
// Attention
// ---------------------------------------------------------------------------

export function selectHomeAttention(snapshot: {
  bootstrap: BootstrapState;
  executionsByProject?: Record<string, Record<string, JobExecution>>;
  executionBySession: Record<string, JobExecution | null>;
  accountingBySession: Record<string, JobAccounting | null>;
}, t?: TranslateFn): AttentionItem[] {
  const items: AttentionItem[] = [];
  const { bootstrap } = snapshot;

  // Degraded provider
  const providers = bootstrap.providers ?? [];
  const degradedProvider = providers.find((p) => p.state === "degraded");
  if (degradedProvider) {
    items.push({
      id: `attention-provider-degraded-${degradedProvider.id}`,
      severity: "warning",
      kind: "degradedResource",
      title: `${degradedProvider.label} is degraded`,
      summary: degradedProvider.detail ?? "Provider reporting elevated latency or reduced capacity.",
      provider: degradedProvider.label,
      createdAt: degradedProvider.lastCheckedAt ?? "now",
      status: "open",
      destination: "control-center",
    });
  }

  // Auth-required provider
  const authProvider = providers.find((p) => p.state === "auth-required");
  if (authProvider) {
    items.push({
      id: `attention-provider-auth-${authProvider.id}`,
      severity: "attention",
      kind: "authenticationRequired",
      title: `${authProvider.label} requires authentication`,
      summary: authProvider.detail ?? "Authentication is delegated to the provider runtime.",
      provider: authProvider.label,
      createdAt: authProvider.lastCheckedAt ?? "now",
      status: "open",
      destination: "control-center",
    });
  }

  // Unavailable provider
  const unavailableProvider = providers.find((p) => p.state === "unavailable");
  if (unavailableProvider) {
    items.push({
      id: `attention-provider-unavailable-${unavailableProvider.id}`,
      severity: "critical",
      kind: "providerUnavailable",
      title: `${unavailableProvider.label} is unavailable`,
      summary: unavailableProvider.detail ?? "Provider is explicitly unavailable and not selected for routing.",
      provider: unavailableProvider.label,
      createdAt: unavailableProvider.lastCheckedAt ?? "now",
      status: "open",
      destination: "control-center",
    });
  }

  // Failed Jobs are Project work, whether or not a Chat session presents them.
  for (const execution of executionsIn(snapshot)) {
    if (!execution) continue;
    if (execution.state === "failed") {
      items.push({
        id: `attention-execution-failed-${execution.jobId}`,
        severity: "warning",
        kind: "runtimeFailure",
        title: t ? t("home.failedJob", { id: execution.jobId }) : `Job ${execution.jobId} failed`,
        summary: t ? t("home.failedSummary") : "Job execution failed.",
        jobId: execution.jobId,
        createdAt: "now",
        status: "open",
        destination: "job-execution",
      });
    }
  }

  // Sort: critical first, then attention, warning, info
  const severityOrder: Record<AttentionSeverity, number> = { critical: 0, attention: 1, warning: 2, info: 3 };
  items.sort((a, b) => severityOrder[a.severity] - severityOrder[b.severity]);

  return items.slice(0, 6);
}

// ---------------------------------------------------------------------------
// Active jobs
// ---------------------------------------------------------------------------

export type JobSummary = {
  sessionId: string;
  jobId: string;
  projectId: string;
  title: string;
  status: "running" | "completed" | "failed" | "pending";
  updatedAt: string;
};

export function selectHomeActiveJobs(snapshot: {
  executionsByProject?: Record<string, Record<string, JobExecution>>;
  executionBySession: Record<string, JobExecution | null>;
}): ActiveJobProjection[] {
  const results: ActiveJobProjection[] = [];
  const chatSessionByJob = new Map(Object.entries(snapshot.executionBySession)
    .flatMap(([sessionId, execution]) => execution ? [[execution.jobId, sessionId] as const] : []));
  for (const execution of executionsIn(snapshot)) {
    if ("state" in execution && (execution.state === "running" || execution.state === "pending")) {
      results.push({
        sessionId: chatSessionByJob.get(execution.jobId),
        jobId: execution.jobId,
        projectId: execution.projectId,
        title: execution.jobId,
        id: execution.jobId,
        completed: execution.progress?.settled ?? 0,
        total: execution.progress?.total ?? 0,
        activeWorkers: execution.executors.filter((executor) => executor.status === "running").length,
        blockedWorkers: 0,
        waitingWorkers: execution.executors.filter((executor) => executor.status === "queued").length,
        elapsed: "ongoing",
        progress: execution.progress?.percent ?? 0,
        destination: "job-execution",
        status: execution.state === "running" ? "running" : "pending",
        updatedAt: new Date(execution.updatedAt * 1000).toISOString(),
      });
    }
  }
  return results.sort((a, b) => (b.updatedAt ?? "").localeCompare(a.updatedAt ?? "") || a.jobId!.localeCompare(b.jobId!));
}

function executionsIn(snapshot: {
  executionsByProject?: Record<string, Record<string, JobExecution>>;
  executionBySession: Record<string, JobExecution | null>;
}): JobExecution[] {
  if (snapshot.executionsByProject !== undefined) {
    return Object.values(snapshot.executionsByProject).flatMap((jobs) => Object.values(jobs));
  }
  return [...new Map(Object.values(snapshot.executionBySession)
    .filter((execution): execution is JobExecution => execution !== null)
    .map((execution) => [execution.jobId, execution])).values()];
}

// ---------------------------------------------------------------------------
// Recent work
// ---------------------------------------------------------------------------

export function selectRecentWork(sessions: { id: string; title: string; updatedAt: string; workType?: string }[], t?: TranslateFn): ContinueWorkingEntry[] {
  return sessions
    .slice()
    .sort((a, b) => (a.updatedAt < b.updatedAt ? 1 : a.updatedAt > b.updatedAt ? -1 : 0))
    .map((session) => ({
       id: `continue-${session.id}`,
       sessionId: session.id,
       title: session.title,
       subtitle: t ? t("home.sessionType", {
         type: WORK_TYPE_KEYS[session.workType ?? "coding"]
           ? t(WORK_TYPE_KEYS[session.workType ?? "coding"])
           : session.workType ?? "coding",
       }) : `Session · ${session.workType ?? "coding"}`,
       timeAgo: session.updatedAt,
       kind: "chat" as const,
       updatedAt: session.updatedAt,
       destination: "chat",
     }));
}

// ---------------------------------------------------------------------------
// Resource health
// ---------------------------------------------------------------------------

export type ResourceHealthItem = {
  id: string;
  label: string;
  state: "healthy" | "degraded" | "auth-required" | "unavailable";
  detail: string | null;
};

export function selectResourceHealthSummary(
  bootstrap: BootstrapState,
): HomeResourceHealthSummary {
  const items: ResourceHealthItem[] = [];
  const providers = bootstrap.providers ?? [];
  for (const provider of providers) {
    if (provider.state !== "connected") {
      items.push({
        id: provider.id,
        label: provider.label,
        state: provider.state === "unknown" ? "unavailable" : provider.state,
        detail: provider.detail ?? null,
      });
    }
  }
  return {
    providerCount: providers.length,
    healthyProviders: providers.filter((provider) => provider.state === "connected").length,
    degradedProviders: providers.filter((provider) => provider.state === "degraded").length,
    unavailableProviders: providers.filter((provider) => provider.state === "unavailable").length,
    authRequiredProviders: providers.filter((provider) => provider.state === "auth-required").length,
    unknownProviders: 0,
    modelCount: 0,
    availableModels: 0,
    unavailableModels: 0,
    activeProfileLabel: "Default",
    runtimeState: "connected",
    hasDegradedOrAuthRequired: items.some((item) => item.state === "degraded" || item.state === "auth-required"),
  };
}

// ---------------------------------------------------------------------------
// Usage summary
// ---------------------------------------------------------------------------

export function selectHomeUsageSummary(
  resourceLedger: ResourceLedger | null,
): UsageSummary {
  if (!resourceLedger || resourceLedger.entries.length === 0) {
    return { costMicros: null, costProvenance: "unavailable", totalTokens: null, freshInput: null, cacheRead: null, cacheShare: null, cacheLeverage: null, entryCount: 0, available: false, totalCost: null, hasCost: false };
  }
  const cost = sumCostMicros(resourceLedger.entries);
  const tokens = null;
  return {
    costMicros: cost === null ? null : Number(cost),
    costProvenance: "reported",
    totalTokens: tokens,
    hasCost: cost !== null,
    freshInput: null,
    cacheRead: null,
    cacheShare: null,
    cacheLeverage: null,
    entryCount: resourceLedger.entries.length,
    available: true,
  };
}

// ---------------------------------------------------------------------------
// Product activity
// ---------------------------------------------------------------------------

export function selectRecentProductActivity(snapshot: {
  sessions: { id: string; title: string; workType: string; updatedAt?: string }[];
  executionsByProject?: Record<string, Record<string, JobExecution>>;
  executionBySession: Record<string, JobExecution | null>;
}, t?: TranslateFn): RecentActivityItem[] {
  const items: RecentActivityItem[] = [];
  for (const session of snapshot.sessions.slice(0, 5)) {
    items.push({
      id: `activity-${session.id}`,
      summary: session.title,
      subtitle: t ? t("home.sessionType", {
        type: WORK_TYPE_KEYS[session.workType] ? t(WORK_TYPE_KEYS[session.workType]) : session.workType,
      }) : `Session · ${session.workType}`,
      timestamp: session.updatedAt,
      kind: "job" as const,
      tone: "slate" as const,
      timeAgo: session.updatedAt ?? "now",
    });
  }
  for (const execution of executionsIn(snapshot)) {
    items.push({
      id: `execution-${execution.projectId}-${execution.jobId}`,
      summary: t ? t("home.executionActivity", {
        id: execution.jobId, state: runtimeStateLabel(t, execution.state), count: execution.calls.length,
      }) : `Job ${execution.jobId} · ${execution.state} · ${execution.calls.length} calls`,
      kind: "job" as const,
      tone: execution.state === "failed" ? "red" as const : "emerald" as const,
      timeAgo: new Date(execution.updatedAt * 1000).toISOString(),
    });
  }
  return items.slice(0, 6);
}

const WORK_TYPE_KEYS: Record<string, I18nKey> = {
  coding: "sidebar.workType.coding", research: "sidebar.workType.research",
  design: "sidebar.workType.design", devops: "sidebar.workType.devops",
} as const;
