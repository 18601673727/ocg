/**
 * Pure Home selectors.
 *
 * Every Home projection is derived from the existing normalized runtime
 * snapshot. No new state is introduced here and no values are invented.
 */

import type { I18nKey, TranslateFn } from "../i18n";
import { runtimeStateLabel } from "../i18n";
import { JOB_STATE } from "../primitives";
import type { AttentionItem, AttentionSeverity, ActiveJobProjection, ContinueWorkingEntry, ResourceHealthSummary as HomeResourceHealthSummary, UsageSummary, RecentActivityItem } from "./domain";
import type { BootstrapState } from "../bootstrap/types";
import type { JobExecution } from "../execution/domain";
import type { JobAccounting } from "../execution/accounting";
import type { ResourceLedger } from "../resource-ledger/types";
import type { RuntimeStatus } from "../types";
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

  // Provider state is canonical and may be absent entirely. Preserve each
  // reported Provider state; unknown is not the same fact as unavailable.
  for (const provider of bootstrap.providers ?? []) {
    const createdAt = provider.lastCheckedAt;
    const common = {
      provider: provider.label,
      ...(createdAt ? { createdAt } : {}),
      status: "open" as const,
      destination: "control-center" as const,
    };
    if (provider.state === "degraded") {
      items.push({
        ...common,
        id: `attention-provider-degraded-${provider.id}`,
        severity: "warning",
        kind: "degradedResource",
        title: `${provider.label} is degraded`,
        summary: provider.detail ?? "Runtime reports this Provider as degraded.",
      });
    } else if (provider.state === "auth-required") {
      items.push({
        ...common,
        id: `attention-provider-auth-${provider.id}`,
        severity: "attention",
        kind: "authenticationRequired",
        title: `${provider.label} requires authentication`,
        summary: provider.detail ?? "Runtime reports that authentication is required.",
      });
    } else if (provider.state === "unavailable") {
      items.push({
        ...common,
        id: `attention-provider-unavailable-${provider.id}`,
        severity: "critical",
        kind: "providerUnavailable",
        title: `${provider.label} is unavailable`,
        summary: provider.detail ?? "Runtime reports this Provider as unavailable.",
      });
    }
  }

  // The model catalogue has a distinct unavailable state. Unknown and pending
  // remain visible in resource summaries without being promoted to failures.
  for (const model of bootstrap.models) {
    if (model.status !== "unavailable") continue;
    const provider = bootstrap.providers?.find((entry) => entry.id === model.providerId || entry.label === model.provider);
    items.push({
      id: `attention-model-unavailable-${model.id}`,
      severity: "warning",
      kind: "modelUnavailable",
      title: `${model.displayName ?? model.model} is unavailable`,
      summary: model.provenanceNote ?? "Runtime reports this Model as unavailable.",
      provider: provider?.label ?? model.provider,
      destination: "control-center",
      status: "open",
    });
  }

  // Failed Jobs are Project work, whether or not a Chat session presents them.
  // A Chat session is recorded alongside an attention item only as an optional
  // conversational route, never as the reason the Job exists.
  const chatSessionByJob = chatSessionsByJobId(snapshot.executionBySession);
  for (const execution of executionsIn(snapshot)) {
    if (!execution) continue;
    if (execution.state === "failed" || execution.state === "orphaned") {
      const orphaned = execution.state === "orphaned";
      items.push({
        id: `attention-execution-failed-${execution.jobId}`,
        severity: "warning",
        kind: "runtimeFailure",
        title: t
          ? t(orphaned ? "home.orphanedJob" : "home.failedJob", { id: execution.jobId })
          : `Job ${execution.jobId} ${orphaned ? "is orphaned" : "failed"}`,
        summary: t ? t(orphaned ? "home.orphanedSummary" : "home.failedSummary") : `Runtime reports this Job as ${orphaned ? "orphaned" : "failed"}.`,
        jobId: execution.jobId,
        ...(chatSessionByJob.get(execution.jobId) ? { sessionId: chatSessionByJob.get(execution.jobId)! } : {}),
        createdAt: new Date(execution.updatedAt * 1000).toISOString(),
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
  const chatSessionByJob = chatSessionsByJobId(snapshot.executionBySession);
  const seenJobIds = new Set<string>();
  // Project-owned Jobs are the collection. A Chat session only annotates the
  // projection when one happens to present the same Job.
  for (const execution of executionsIn(snapshot)) {
    if (["running", "cancelling", "eligible", "pending"].includes(execution.state) && !seenJobIds.has(execution.jobId)) {
      seenJobIds.add(execution.jobId);
      results.push({
        sessionId: chatSessionByJob.get(execution.jobId),
        jobId: execution.jobId,
        projectId: execution.projectId,
        title: execution.jobId,
        id: execution.jobId,
        destination: "job-execution",
        status: execution.state,
        updatedAt: new Date(execution.updatedAt * 1000).toISOString(),
      });
    }
  }
  return results.sort((a, b) => (b.updatedAt ?? "").localeCompare(a.updatedAt ?? "") || a.jobId!.localeCompare(b.jobId!));
}

/**
 * Chat sessions keyed by the Job they present. This is a conversational
 * annotation only: a Job with no Chat session is still a Project-owned Job.
 */
function chatSessionsByJobId(
  executionBySession: Record<string, JobExecution | null>,
): Map<string, string> {
  return new Map(Object.entries(executionBySession)
    .flatMap(([sessionId, execution]) => execution ? [[execution.jobId, sessionId] as const] : []));
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

export function selectResourceHealthSummary(
  bootstrap: BootstrapState,
  runtimeStatus: RuntimeStatus,
): HomeResourceHealthSummary {
  const providers = bootstrap.providers;
  const items: NonNullable<HomeResourceHealthSummary["items"]> = (providers ?? [])
    .filter((provider) => provider.state !== "connected")
    .map((provider) => ({
      id: provider.id,
      label: provider.label,
      state: provider.state,
      detail: provider.detail ?? null,
    }));
  const modelCountsKnown = bootstrap.ready;
  const activeProfile = bootstrap.ready
    ? bootstrap.profiles.find((profile) => profile.id === bootstrap.activeProfileId) ?? null
    : null;
  const hasProviderIssues = (providers ?? []).some((provider) =>
    provider.state === "degraded" || provider.state === "auth-required" || provider.state === "unavailable",
  );
  const hasUnreportedResourceState =
    !bootstrap.ready ||
    providers === undefined ||
    providers.some((provider) => provider.state === "unknown") ||
    bootstrap.models.some((model) => model.status === "unknown" || model.status === "pending") ||
    runtimeStatus.state !== "connected";

  return {
    providerCount: providers?.length ?? null,
    connectedProviders: providers?.filter((provider) => provider.state === "connected").length ?? null,
    degradedProviders: providers?.filter((provider) => provider.state === "degraded").length ?? null,
    unavailableProviders: providers?.filter((provider) => provider.state === "unavailable").length ?? null,
    authRequiredProviders: providers?.filter((provider) => provider.state === "auth-required").length ?? null,
    unknownProviders: providers?.filter((provider) => provider.state === "unknown").length ?? null,
    modelCount: modelCountsKnown ? bootstrap.models.length : null,
    availableModels: modelCountsKnown ? bootstrap.models.filter((model) => model.status === "available").length : null,
    pendingModels: modelCountsKnown ? bootstrap.models.filter((model) => model.status === "pending").length : null,
    unavailableModels: modelCountsKnown ? bootstrap.models.filter((model) => model.status === "unavailable").length : null,
    unknownModels: modelCountsKnown ? bootstrap.models.filter((model) => model.status === "unknown").length : null,
    activeProfileLabel: activeProfile?.label ?? null,
    runtimeState: runtimeStatus.state,
    hasProviderIssues,
    hasUnreportedResourceState,
    items,
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
      kind: "chat" as const,
      tone: "slate" as const,
      timeAgo: session.updatedAt ?? "now",
    });
  }
  for (const execution of executionsIn(snapshot)) {
    items.push({
      id: `execution-${execution.projectId}-${execution.jobId}`,
      summary: t ? t("home.executionActivity", {
        id: execution.jobId, state: runtimeStateLabel(t, execution.state),
      }) : `Job ${execution.jobId} · ${execution.state}`,
      kind: "job" as const,
      tone: JOB_STATE[execution.state].tone,
      timeAgo: new Date(execution.updatedAt * 1000).toISOString(),
    });
  }
  return items.slice(0, 6);
}

const WORK_TYPE_KEYS: Record<string, I18nKey> = {
  coding: "sidebar.workType.coding", research: "sidebar.workType.research",
  design: "sidebar.workType.design", devops: "sidebar.workType.devops",
} as const;
