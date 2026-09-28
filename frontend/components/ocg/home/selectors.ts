/**
 * Pure Home selectors.
 *
 * Every Home projection is derived from the existing normalized
 * runtime snapshot. No new state is introduced here and no values
 * are invented.
 */

import type { AttentionItem, AttentionSeverity, ActiveMissionProjection, ContinueWorkingEntry, ResourceHealthSummary, UsageSummary, RecentActivityItem } from "./domain";
import type { BootstrapState } from "../bootstrap/types";
import type { Mission } from "../types";
import type { MissionExecution } from "../execution/domain";
import type { ResourceLedger, ResourceLedgerEntry } from "../resource-ledger/types";
import { sumCostMicros, componentTraffic, cacheShare, cacheLeverage } from "../resource-ledger/selectors";

// ---------------------------------------------------------------------------
// Attention
// ---------------------------------------------------------------------------

export function selectHomeAttention(snapshot: {
  bootstrap: BootstrapState;
  missionsBySession: Record<string, Mission | null>;
  executionBySession: Record<string, MissionExecution | null>;
}): AttentionItem[] {
  const items: AttentionItem[] = [];
  const { bootstrap, missionsBySession } = snapshot;

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

  // Failed mission tasks (MissionTaskStatus has no "blocked"; execution-domain
  // blocked workers are projected separately below).
  for (const [sessionId, mission] of Object.entries(missionsBySession)) {
    if (!mission) continue;
    const blockedTasks = mission.tasks?.filter((t) => t.status === "failed");
    if (blockedTasks && blockedTasks.length > 0) {
      items.push({
        id: `attention-blocked-${sessionId}`,
        severity: "warning",
        kind: "blockedTask",
        title: `${mission.title} has blocked tasks`,
        summary: `${blockedTasks.length} task(s) are blocked or failed in this Mission.`,
        missionId: sessionId,
        createdAt: "now",
        status: "open",
        destination: "mission-control",
      });
    }
  }

  // Budget-exhausted mission
  for (const [sessionId, mission] of Object.entries(missionsBySession)) {
    if (!mission) continue;
    if (mission.status === "budget-exhausted") {
      items.push({
        id: `attention-budget-${sessionId}`,
        severity: "critical",
        kind: "budgetGate",
        title: `${mission.title} hit budget limit`,
        summary: "No additional work will be started until the budget is adjusted.",
        missionId: sessionId,
        createdAt: "now",
        status: "open",
        destination: "mission-control",
      });
    }
  }

  // Sort: critical first, then attention, warning, info
  const severityOrder: Record<AttentionSeverity, number> = { critical: 0, attention: 1, warning: 2, info: 3 };
  items.sort((a, b) => severityOrder[a.severity] - severityOrder[b.severity]);

  return items.slice(0, 6);
}

// ---------------------------------------------------------------------------
// Active missions
// ---------------------------------------------------------------------------

export function selectHomeActiveMissions(snapshot: {
  missionsBySession: Record<string, Mission | null>;
  executionBySession: Record<string, MissionExecution | null>;
}): ActiveMissionProjection[] {
  const missions: ActiveMissionProjection[] = [];
  const { missionsBySession, executionBySession } = snapshot;

  for (const [sessionId, mission] of Object.entries(missionsBySession)) {
    if (!mission) continue;
    const status = mission.status;
    if (status !== "running" && status !== "paused" && status !== "planning" && status !== "failed" && status !== "budget-exhausted") continue;

    const execution = executionBySession[sessionId] ?? null;
    const workers = mission.workers ?? [];
    const activeWorkers = workers.filter((w) => w.status === "active" || w.status === "starting").length;
    const blockedWorkers = workers.filter((w) => w.status === "waiting" || w.status === "idle" || w.status === "queued").length;
    const total = mission.total ?? 0;
    const completed = mission.completed ?? 0;
    const progress = total > 0 ? Math.round((completed / total) * 100) : 0;

    const waves = execution?.waves ?? [];
    const activeWave = waves.find((w) => w.status === "active");
    const currentWave = activeWave?.index ?? mission.current;
    const totalWaves = execution?.totalWaves ?? waves.length;

    missions.push({
      id: sessionId,
      title: mission.title,
      status,
      completed,
      total,
      currentWave: typeof currentWave === "number" ? currentWave : undefined,
      totalWaves: totalWaves || undefined,
      activeWorkers,
      blockedWorkers,
      waitingWorkers: workers.filter((w) => w.status === "waiting").length,
      elapsed: mission.elapsed ?? "0m",
      budgetSpent: mission.budget?.spent,
      budgetLimit: mission.budget?.limit,
      progress,
      destination: "mission-control",
    });
  }

  // Prioritize: running first, then paused/waiting, then recently completed
  const priorityOrder: Record<string, number> = { running: 0, paused: 1, planning: 2, failed: 3, "budget-exhausted": 3 };
  missions.sort((a, b) => (priorityOrder[a.status] ?? 4) - (priorityOrder[b.status] ?? 4));

  return missions.slice(0, 4);
}

// ---------------------------------------------------------------------------
// Continue working
// ---------------------------------------------------------------------------

export function selectRecentWork(sessions: Array<{ id: string; title: string; workType: string; updatedAt: string }>, limit = 4): ContinueWorkingEntry[] {
  const timeValue = (s: string): number => {
    const match = s.match(/^(\d+)(m|h|d)?\s*ago$/);
    if (!match) return 0;
    const num = parseInt(match[1], 10);
    switch (match[2]) {
      case "d": return num * 24 * 60;
      case "h": return num * 60;
      case "m": return num;
      default: return 0;
    }
  };
  return [...sessions]
    .sort((a, b) => timeValue(a.updatedAt) - timeValue(b.updatedAt))
    .slice(0, limit)
    .map((session) => ({
      id: session.id,
      title: session.title,
      subtitle: session.workType.charAt(0).toUpperCase() + session.workType.slice(1),
      timeAgo: session.updatedAt,
      kind: session.workType === "research" ? "chat" as const : session.workType === "coding" ? "chat" as const : session.workType === "design" ? "chat" as const : "chat" as const,
      destination: "/",
      sessionId: session.id,
    }));
}

// ---------------------------------------------------------------------------
// Resource health
// ---------------------------------------------------------------------------

export function selectResourceHealthSummary(bootstrap: BootstrapState): ResourceHealthSummary {
  const providers = bootstrap.providers ?? [];
  const models = bootstrap.models ?? [];
  const activeProfile = bootstrap.profiles.find((p) => p.id === bootstrap.activeProfileId) ?? bootstrap.profiles.find((p) => p.recommended) ?? null;

  const healthyProviders = providers.filter((p) => p.state === "connected").length;
  const degradedProviders = providers.filter((p) => p.state === "degraded").length;
  const unavailableProviders = providers.filter((p) => p.state === "unavailable").length;
  const authRequiredProviders = providers.filter((p) => p.state === "auth-required").length;
  const unknownProviders = providers.filter((p) => p.state === "unknown").length;
  const availableModels = models.filter((m) => m.status === "available").length;
  const unavailableModels = models.filter((m) => m.status === "unavailable" || m.status === "unknown").length;

  return {
    providerCount: providers.length,
    healthyProviders,
    degradedProviders,
    unavailableProviders,
    authRequiredProviders,
    unknownProviders,
    modelCount: models.length,
    availableModels,
    unavailableModels,
    activeProfileLabel: activeProfile?.label ?? "—",
    runtimeState: bootstrap.ready ? "connected" : "connecting",
    hasDegradedOrAuthRequired: degradedProviders > 0 || authRequiredProviders > 0 || unavailableProviders > 0,
  };
}

// ---------------------------------------------------------------------------
// Usage summary
// ---------------------------------------------------------------------------

export function selectHomeUsageSummary(ledger: ResourceLedger | null, timeWindowMs?: number): UsageSummary {
  if (!ledger) {
    return { costMicros: null, costProvenance: "unavailable", totalTokens: null, freshInput: null, cacheRead: null, cacheShare: null, cacheLeverage: null, entryCount: 0, available: false };
  }

  const entries = timeWindowMs ? filterEntriesByWindow(ledger.entries, timeWindowMs) : ledger.entries;
  const cost = sumCostMicros(entries);
  const traffic = componentTraffic(entries);
  const cs = cacheShare(traffic);
  const cl = cacheLeverage(traffic);

  return {
    costMicros: cost.micros,
    costProvenance: cost.provenance,
    totalTokens: traffic.total,
    freshInput: traffic.freshInput,
    cacheRead: traffic.cacheRead,
    cacheShare: cs,
    cacheLeverage: cl,
    entryCount: entries.length,
    available: true,
  };
}

function filterEntriesByWindow(entries: ResourceLedgerEntry[], windowMs: number): ResourceLedgerEntry[] {
  const nowMs = latestTimestampMs(entries) ?? Date.now();
  return entries.filter((entry) => {
    const ms = Date.parse(entry.timestamp);
    return Number.isFinite(ms) && ms >= nowMs - windowMs;
  });
}

function latestTimestampMs(entries: ResourceLedgerEntry[]): number | null {
  let latest: number | null = null;
  for (const entry of entries) {
    const ms = Date.parse(entry.timestamp);
    if (Number.isFinite(ms) && (latest === null || ms > latest)) latest = ms;
  }
  return latest;
}

// ---------------------------------------------------------------------------
// Recent activity
// ---------------------------------------------------------------------------

export function selectRecentProductActivity(snapshot: {
  missionsBySession: Record<string, Mission | null>;
  executionBySession: Record<string, MissionExecution | null>;
  bootstrap: BootstrapState;
  resourceLedger: ResourceLedger | null;
}): RecentActivityItem[] {
  const items: RecentActivityItem[] = [];
  const { missionsBySession, executionBySession, bootstrap, resourceLedger } = snapshot;

  // Mission completions and failures
  for (const [, mission] of Object.entries(missionsBySession)) {
    if (!mission) continue;
    if (mission.status === "completed") {
      items.push({
        id: `activity-mission-complete-${mission.title}`,
        timeAgo: mission.elapsed ?? "just now",
        summary: `Mission "${mission.title}" completed`,
        kind: "mission",
        tone: "emerald",
      });
    } else if (mission.status === "failed" || mission.status === "budget-exhausted") {
      items.push({
        id: `activity-mission-fail-${mission.title}`,
        timeAgo: mission.elapsed ?? "just now",
        summary: `Mission "${mission.title}" ${mission.status === "budget-exhausted" ? "exhausted budget" : "failed"}`,
        kind: "mission",
        tone: "red",
      });
    }
  }

  // Worker state changes from execution
  for (const [, execution] of Object.entries(executionBySession)) {
    if (!execution) continue;
    const blockedWorkers = execution.workers.filter((w) => w.status === "blocked");
    if (blockedWorkers.length > 0) {
      items.push({
        id: `activity-blocked-${execution.missionId}`,
        timeAgo: "recent",
        summary: `${blockedWorkers.length} worker(s) blocked in "${execution.title}"`,
        kind: "worker",
        tone: "amber",
      });
    }
    const verifying = execution.tasks.filter((t) => t.status === "verifying");
    if (verifying && verifying.length > 0) {
      items.push({
        id: `activity-verifying-${execution.missionId}`,
        timeAgo: "recent",
        summary: `Verification in progress for "${execution.title}"`,
        kind: "verification",
        tone: "violet",
      });
    }
  }

  // Provider state changes
  const providers = bootstrap.providers ?? [];
  const unavailable = providers.filter((p) => p.state === "unavailable");
  if (unavailable.length > 0) {
    items.push({
      id: "activity-provider-down",
      timeAgo: "recent",
      summary: `${unavailable.length} provider(s) unavailable`,
      kind: "provider",
      tone: "red",
    });
  }
  const degraded = providers.filter((p) => p.state === "degraded");
  if (degraded.length > 0) {
    items.push({
      id: "activity-provider-degraded",
      timeAgo: "recent",
      summary: `${degraded.length} provider(s) degraded`,
      kind: "provider",
      tone: "amber",
    });
  }

  // Resource ledger signals
  if (resourceLedger && resourceLedger.entries.length > 0) {
    const mismatchEntries = resourceLedger.entries.filter((e) => e.reconciliation === "mismatch");
    if (mismatchEntries.length > 0) {
      items.push({
        id: "activity-reconciliation-mismatch",
        timeAgo: "recent",
        summary: `${mismatchEntries.length} ledger entry(ies) with reconciliation mismatch`,
        kind: "resource",
        tone: "amber",
      });
    }
  }

  // Sort newest first
  const toneOrder = { emerald: 0, violet: 1, amber: 2, red: 3, slate: 4 };
  items.sort((a, b) => toneOrder[a.tone] - toneOrder[b.tone]);

  return items.slice(0, 8);
}
