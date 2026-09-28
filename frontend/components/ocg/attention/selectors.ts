/**
 * Pure selectors for the Attention / Approvals Center.
 *
 * Items are derived from the normalized runtime snapshot plus the
 * scenario-level fixture queue (explicit approvals / policy gates that no
 * existing domain models). Derived helpers never mutate and never widen
 * the Home-owned domain; fixture semantics live in ./fixtures.
 */

import type { RuntimeSnapshot } from "../runtime/runtime-types";
import type { Mission } from "../types";
import type {
  AttentionItem,
  AttentionKind,
  AttentionLifecycle,
  AttentionSeverity,
  AttentionSource,
  AttentionTab,
} from "./domain";
import { isApprovalItem, isBlockedItem, isUnresolved } from "./domain";

export type AttentionFilters = {
  tab: AttentionTab;
  query: string;
  kind: AttentionKind | "all";
  severity: AttentionSeverity | "all";
  source: AttentionSource | "all";
};

export const DEFAULT_ATTENTION_FILTERS: AttentionFilters = {
  tab: "overview",
  query: "",
  kind: "all",
  severity: "all",
  source: "all",
};

export const ATTENTION_RESULT_LIMIT = 60;
export const ATTENTION_HISTORY_LIMIT = 25;

export const URGENCY_ORDER: Record<AttentionSeverity, number> = {
  critical: 0,
  high: 1,
  warning: 2,
  info: 3,
};

const SEVERITY_PRIORITY: Record<AttentionLifecycle, number> = {
  pending: 0,
  acknowledged: 1,
  approved: 2,
  rejected: 2,
  resolved: 2,
  expired: 2,
  superseded: 2,
};

/** Sort unresolved items by practical urgency: severity, then lifecycle, then age. */
export function sortAttentionByUrgency(items: AttentionItem[]): AttentionItem[] {
  return [...items].sort((a, b) => {
    const severity = URGENCY_ORDER[a.severity] - URGENCY_ORDER[b.severity];
    if (severity !== 0) return severity;
    const lifecycle = SEVERITY_PRIORITY[a.status] - SEVERITY_PRIORITY[b.status];
    if (lifecycle !== 0) return lifecycle;
    if (a.createdAt < b.createdAt) return -1;
    if (a.createdAt > b.createdAt) return 1;
    return a.id < b.id ? -1 : 1;
  });
}

export function selectTabItems(items: AttentionItem[], tab: AttentionTab): AttentionItem[] {
  switch (tab) {
    case "approvals":
      return items.filter((item) => isUnresolved(item) && isApprovalItem(item));
    case "blocked":
      return items.filter((item) => isUnresolved(item) && isBlockedItem(item));
    case "resolved":
      return [...items.filter((item) => !isUnresolved(item))]
        .sort((a, b) => (a.updatedAt < b.updatedAt ? 1 : a.updatedAt > b.updatedAt ? -1 : 0))
        .slice(0, ATTENTION_HISTORY_LIMIT);
    case "overview":
    default:
      return sortAttentionByUrgency(items.filter(isUnresolved));
  }
}

function matchesQuery(item: AttentionItem, query: string): boolean {
  const normalized = query.trim().toLowerCase();
  if (!normalized) return true;
  const haystack = [
    item.title,
    item.summary,
    item.missionTitle,
    item.taskTitle,
    item.providerLabel,
    item.model,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
  return normalized.split(/\s+/).every((token) => haystack.includes(token));
}

export function filterAttentionItems(items: AttentionItem[], filters: AttentionFilters): AttentionItem[] {
  const scoped = selectTabItems(items, filters.tab);
  return scoped
    .filter((item) => (filters.kind === "all" ? true : item.kind === filters.kind))
    .filter((item) => (filters.severity === "all" ? true : item.severity === filters.severity))
    .filter((item) => (filters.source === "all" ? true : item.source === filters.source))
    .filter((item) => matchesQuery(item, filters.query))
    .slice(0, ATTENTION_RESULT_LIMIT);
}

export type AttentionSummary = {
  needsAction: number;
  awaitingApproval: number;
  blocked: number;
  critical: number;
  high: number;
  resolved: number;
};

/** Compact header metrics. Resolved stays separate from unresolved work. */
export function selectAttentionSummary(items: AttentionItem[]): AttentionSummary {
  const unresolved = items.filter(isUnresolved);
  return {
    needsAction: unresolved.length,
    awaitingApproval: unresolved.filter(isApprovalItem).length,
    blocked: unresolved.filter(isBlockedItem).length,
    critical: unresolved.filter((item) => item.severity === "critical").length,
    high: unresolved.filter((item) => item.severity === "high").length,
    resolved: items.length - unresolved.length,
  };
}

export function selectSeverityCounts(items: AttentionItem[]): Record<AttentionSeverity, number> {
  const counts: Record<AttentionSeverity, number> = { info: 0, warning: 0, high: 0, critical: 0 };
  for (const item of items.filter(isUnresolved)) counts[item.severity] += 1;
  return counts;
}

export function selectKindCounts(items: AttentionItem[]): Record<AttentionKind, number> {
  const counts: Record<AttentionKind, number> = {
    approval: 0,
    budget: 0,
    policy: 0,
    permission: 0,
    blocked: 0,
    "runtime-failure": 0,
    "resource-degraded": 0,
    configuration: 0,
    retry: 0,
    escalation: 0,
  };
  for (const item of items.filter(isUnresolved)) counts[item.kind] += 1;
  return counts;
}

function stableId(...parts: Array<string | undefined>): string {
  return parts.filter(Boolean).join(":");
}

/** Ids are derived from stable source keys only — never from display titles. */
export function attentionId(source: string, ...parts: Array<string | undefined>): string {
  return `attention-${stableId(source, ...parts)}`;
}

// ---------------------------------------------------------------------------
// Snapshot derivation
// ---------------------------------------------------------------------------

export type AttentionQueue = {
  approvals: AttentionItem[];
  history: AttentionItem[];
};

/**
 * Derive Attention Center items from the shared runtime snapshot plus the
 * scenario fixture queue. Snapshot-derived items reuse stable source keys
 * (session id, provider id, mission/execution ids); fixture items model
 * explicit approval gates that no existing normalized domain owns.
 */
export function selectAttentionItems(
  snapshot: {
    scenario?: RuntimeSnapshot["scenario"];
    missionsBySession: Record<string, Mission | null>;
    executionBySession: RuntimeSnapshot["executionBySession"];
    bootstrap: RuntimeSnapshot["bootstrap"];
    resourceLedger?: RuntimeSnapshot["resourceLedger"];
  },
  queue?: AttentionQueue,
): AttentionItem[] {
  const items: AttentionItem[] = [];
  const seen = new Set<string>();
  const push = (item: AttentionItem) => {
    if (seen.has(item.id)) return;
    seen.add(item.id);
    items.push(item);
  };

  const providers = snapshot.bootstrap.providers ?? [];
  const degraded = providers.find((p) => p.state === "degraded");
  if (degraded) {
    push({
      id: attentionId("provider-degraded", degraded.id),
      kind: "resource-degraded",
      status: "pending",
      severity: "high",
      title: `${degraded.label} is degraded`,
      summary: degraded.detail ?? "Provider is reachable but reporting elevated latency.",
      whatHappened: `${degraded.label} remains reachable but reports elevated latency. Affected routes note the fallback in the fixture.`,
      whyNeeded: "Decide whether to keep routing through the degraded provider, switch to the fallback, or wait.",
      inactionConsequence: "Affected work keeps using the fallback route; latency warnings remain until the provider recovers.",
      createdAt: degraded.lastCheckedAt ?? "2026-09-25T08:55:00Z",
      updatedAt: degraded.lastCheckedAt ?? "2026-09-25T08:55:00Z",
      source: "provider",
      destination: "control-center",
      providerId: degraded.id,
      providerLabel: degraded.label,
      approval: null,
      blocked: null,
      resolution: null,
    });
  }

  const authProvider = providers.find((p) => p.state === "auth-required");
  if (authProvider) {
    push({
      id: attentionId("provider-auth", authProvider.id),
      kind: "permission",
      status: "pending",
      severity: "warning",
      title: `${authProvider.label} needs authentication`,
      summary: authProvider.detail ?? "Authentication is delegated to the provider runtime.",
      whatHappened: `${authProvider.label} was discovered but is not authorized for this workspace yet.`,
      whyNeeded: "Authorize the provider before any route can select it, or leave it unassigned.",
      inactionConsequence: "Routes assigned to this provider stay on fallback or remain unassigned.",
      createdAt: authProvider.lastCheckedAt ?? "2026-09-25T08:50:00Z",
      updatedAt: authProvider.lastCheckedAt ?? "2026-09-25T08:50:00Z",
      source: "provider",
      destination: "control-center",
      providerId: authProvider.id,
      providerLabel: authProvider.label,
      approval: null,
      blocked: null,
      resolution: null,
    });
  }

  const unavailable = providers.find((p) => p.state === "unavailable");
  if (unavailable) {
    push({
      id: attentionId("provider-unavailable", unavailable.id),
      kind: "resource-degraded",
      status: "pending",
      severity: "critical",
      title: `${unavailable.label} is unavailable`,
      summary: unavailable.detail ?? "Provider is explicitly unavailable and not selected for routing.",
      whatHappened: `${unavailable.label} reported unavailable and was removed from routing.`,
      whyNeeded: "Confirm the outage is known and decide whether any assigned work must be rerouted manually.",
      inactionConsequence: "Assigned work stays parked on fallback routes; the provider keeps reporting unavailable.",
      createdAt: unavailable.lastCheckedAt ?? "2026-09-25T09:00:00Z",
      updatedAt: unavailable.lastCheckedAt ?? "2026-09-25T09:00:00Z",
      source: "provider",
      destination: "control-center",
      providerId: unavailable.id,
      providerLabel: unavailable.label,
      approval: null,
      blocked: null,
      resolution: null,
    });
  }

  // Auth-required connection (first-class auth surface state, distinct from providers).
  const authConnection = snapshot.bootstrap.connections.find((c) => c.state === "auth-required");
  if (authConnection) {
    push({
      id: attentionId("connection-auth", authConnection.id),
      kind: "permission",
      status: "pending",
      severity: "warning",
      title: `${authConnection.label} needs authentication`,
      summary: authConnection.detail ?? "Authentication is delegated to the provider runtime.",
      whatHappened: `The ${authConnection.label} connection requires an auth step before setup can continue.`,
      whyNeeded: "Complete the delegated auth step or keep this connection optional.",
      inactionConsequence: "The connection stays unauthenticated; dependent setup stays paused.",
      createdAt: "2026-09-25T08:40:00Z",
      updatedAt: "2026-09-25T08:40:00Z",
      source: "configuration",
      destination: "settings",
      approval: null,
      blocked: null,
      resolution: null,
    });
  }

  // Budget-exhausted missions need a spend decision, not a fake approval.
  for (const [sessionId, mission] of Object.entries(snapshot.missionsBySession)) {
    if (!mission || mission.status !== "budget-exhausted") continue;
    push({
      id: attentionId("budget", sessionId),
      kind: "budget",
      status: "pending",
      severity: "critical",
      title: `"${mission.title}" hit its budget limit`,
      summary: "No additional work will start until the budget is adjusted.",
      whatHappened: `Mission "${mission.title}" reached $${mission.budget.spent.toFixed(2)} of $${mission.budget.limit.toFixed(2)}. Execution is held.`,
      whyNeeded: "Raise the limit, close the Mission, or leave it parked until spend is reviewed in the ledger.",
      inactionConsequence: "The Mission stays parked; queued tasks never start.",
      createdAt: "2026-09-25T09:05:00Z",
      updatedAt: "2026-09-25T09:05:00Z",
      source: "mission",
      destination: "mission-control",
      missionId: sessionId,
      missionTitle: mission.title,
      approval: null,
      blocked: null,
      resolution: null,
    });
  }

  // Failed mission tasks: unresolved failures requiring acknowledgement.
  for (const [sessionId, mission] of Object.entries(snapshot.missionsBySession)) {
    if (!mission) continue;
    const failed = mission.tasks.filter((t) => t.status === "failed");
    if (failed.length === 0) continue;
    push({
      id: attentionId("mission-failed", sessionId),
      kind: "blocked",
      status: "pending",
      severity: "high",
      title: `"${mission.title}" has ${failed.length} failed task${failed.length > 1 ? "s" : ""}`,
      summary: `${failed.map((t) => t.title).slice(0, 2).join("; ")}${failed.length > 2 ? ` (+${failed.length - 2} more)` : ""} need inspection.`,
      whatHappened: `${failed.length} task(s) in "${mission.title}" reported failed. Execution holds dependent work.`,
      whyNeeded: "Acknowledge the failure, inspect it in Mission Control, or resolve it once handled.",
      inactionConsequence: "Dependent work stays held and the failure keeps surfacing as unresolved.",
      createdAt: "2026-09-25T09:02:00Z",
      updatedAt: "2026-09-25T09:02:00Z",
      source: "mission",
      destination: "mission-control",
      missionId: sessionId,
      missionTitle: mission.title,
      taskId: failed[0]?.id,
      taskTitle: failed[0]?.title,
      approval: null,
      blocked: {
        missionId: sessionId,
        missionTitle: mission.title,
        taskId: failed[0]?.id,
        taskTitle: failed[0]?.title,
        reason: "Mission task reported failed; dependents cannot proceed until it is handled.",
        unblocksWhen: "The failed task is retried, reassigned, or explicitly resolved.",
      },
      resolution: null,
    });
  }

  // Execution-domain blocked tasks/workers are genuinely separate from
  // MissionTaskStatus (which has no "blocked"). Only real blocked state
  // produces blocked items — never a fake approval.
  for (const [sessionId, execution] of Object.entries(snapshot.executionBySession)) {
    if (!execution) continue;
    const blockedTasks = execution.tasks.filter((t) => t.status === "blocked");
    const blockedWorkers = execution.workers.filter((w) => w.status === "blocked");
    if (blockedTasks.length === 0 && blockedWorkers.length === 0) continue;
    const task = blockedTasks[0] ?? null;
    const worker = blockedWorkers[0] ?? null;
    push({
      id: attentionId("execution-blocked", execution.missionId),
      kind: "blocked",
      status: "pending",
      severity: "high",
      title: `"${execution.title}" is blocked`,
      summary: task?.blockedReason ?? "Work cannot progress until the blocking condition changes.",
      whatHappened: task
        ? `Task "${task.title}" is blocked: ${task.blockedReason ?? "a blocking condition holds."}`
        : `Worker "${worker?.label}" is blocked and cannot take new work.`,
      whyNeeded: "Release the blocking condition, reassign the work, or acknowledge the hold.",
      inactionConsequence: "Downstream waves stay queued behind the blocked work.",
      createdAt: "2026-09-25T09:03:00Z",
      updatedAt: "2026-09-25T09:03:00Z",
      source: "execution",
      destination: "mission-control",
      missionId: sessionId,
      missionTitle: execution.title,
      taskId: task?.id,
      taskTitle: task?.title,
      approval: null,
      blocked: {
        missionId: execution.missionId,
        missionTitle: execution.title,
        taskId: task?.id,
        taskTitle: task?.title,
        workerId: worker?.id,
        workerLabel: worker?.label,
        reason: task?.blockedReason ?? "Execution reports blocked work.",
        unblocksWhen: task
          ? `Task "${task.title}" is unblocked or its dependency completes.`
          : "The blocked worker is reassigned or recovers.",
      },
      resolution: null,
    });
  }

  // Execution retry/failure signals surface as a retry decision, not an approval.
  for (const [sessionId, execution] of Object.entries(snapshot.executionBySession)) {
    if (!execution) continue;
    const retrying = execution.tasks.filter((t) => t.status === "retrying");
    if (retrying.length === 0) continue;
    const task = retrying[0]!;
    push({
      id: attentionId("execution-retry", execution.missionId, task.id),
      kind: "retry",
      status: "acknowledged",
      severity: "warning",
      title: `"${task.title}" is retrying`,
      summary: "A retry is already scheduled; confirm it or intervene.",
      whatHappened: `Task "${task.title}" failed once and a retry is scheduled${task.attempt && task.maxAttempts ? ` (attempt ${task.attempt} of ${task.maxAttempts})` : ""}.`,
      whyNeeded: "Confirm the automatic retry is acceptable, or escalate before it burns more budget.",
      inactionConsequence: "The retry proceeds automatically; further failures surface again.",
      createdAt: "2026-09-25T09:04:00Z",
      updatedAt: "2026-09-25T09:04:00Z",
      source: "execution",
      destination: "mission-control",
      missionId: sessionId,
      missionTitle: execution.title,
      taskId: task.id,
      taskTitle: task.title,
      approval: null,
      blocked: null,
      resolution: null,
    });
  }

  // Runtime connectivity failures require intervention, not acknowledgement theater.
  if (snapshot.scenario === "runtime-failed") {
    push({
      id: attentionId("runtime", "failed"),
      kind: "runtime-failure",
      status: "pending",
      severity: "critical",
      title: "Local runtime failed to start",
      summary: "The mock runtime reported a startup failure; work cannot proceed.",
      whatHappened: "The runtime snapshot reports a failed state before any Mission work started.",
      whyNeeded: "Restart or reconfigure the runtime, then confirm the workspace recovers.",
      inactionConsequence: "All Mission work stays unavailable until the runtime recovers.",
      createdAt: "2026-09-25T09:00:00Z",
      updatedAt: "2026-09-25T09:00:00Z",
      source: "runtime",
      destination: "logs",
      approval: null,
      blocked: null,
      resolution: null,
    });
  }

  // Ledger reconciliation mismatches are evidence-backed, bounded to two items.
  const mismatch = snapshot.resourceLedger?.entries.filter((e) => e.reconciliation === "mismatch") ?? [];
  for (const entry of mismatch.slice(0, 2)) {
    push({
      id: attentionId("ledger-mismatch", entry.id),
      kind: "configuration",
      status: "pending",
      severity: "warning",
      title: `Ledger entry ${entry.id} needs review`,
      summary: entry.note ?? "Usage was billed but the reconciliation check mismatched.",
      whatHappened: `Ledger entry ${entry.id} ("${entry.taskLabel}") reconciled as a mismatch with ${entry.costProvenance} cost provenance.`,
      whyNeeded: "Confirm the mismatch is understood or correct the attribution in the ledger.",
      inactionConsequence: "The entry keeps reporting mismatched until it is reconciled.",
      createdAt: entry.timestamp,
      updatedAt: entry.timestamp,
      source: "resource",
      destination: "resource-ledger",
      missionId: entry.missionId,
      missionTitle: entry.missionLabel,
      taskId: entry.taskId,
      taskTitle: entry.taskLabel,
      approval: null,
      blocked: null,
      resolution: null,
    });
  }

  // Scenario fixture queue: explicit decision gates only. Blocked work is
  // never represented as an approval; those items arrive without `approval`.
  for (const item of queue?.approvals ?? []) push(item);
  // Resolved history is bounded by the tab selector; the queue just supplies it.
  for (const item of queue?.history ?? []) push(item);

  return sortAttentionByUrgency(items.filter(isUnresolved)).concat(
    [...items.filter((item) => !isUnresolved(item))].sort((a, b) =>
      a.updatedAt < b.updatedAt ? 1 : a.updatedAt > b.updatedAt ? -1 : 0,
    ),
  );
}

/** Local fixture-driven decision transition. Rejected and resolved stay distinct. */
export function applyAttentionDecision(
  items: AttentionItem[],
  id: string,
  decision: "approved" | "rejected",
  at: string,
): AttentionItem[] {
  return items.map((item) => {
    if (item.id !== id || !item.approval) return item;
    if (item.approval.decision !== "pending") return item;
    const status: AttentionLifecycle = decision === "approved" ? "approved" : "rejected";
    return {
      ...item,
      status,
      updatedAt: at,
      approval: { ...item.approval, decision },
      resolution: { outcome: status, at },
    };
  });
}

/** Local acknowledgement. Only pending items transition; history is untouched. */
export function acknowledgeAttentionItem(items: AttentionItem[], id: string, at: string): AttentionItem[] {
  return items.map((item) =>
    item.id === id && item.status === "pending"
      ? { ...item, status: "acknowledged" as const, updatedAt: at }
      : item,
  );
}

/** Local resolution for non-approval items. Approvals resolve via decision. */
export function resolveAttentionItem(items: AttentionItem[], id: string, at: string): AttentionItem[] {
  return items.map((item) =>
    item.id === id && isUnresolved(item) && !item.approval
      ? { ...item, status: "resolved" as const, updatedAt: at, resolution: { outcome: "resolved" as const, at } }
      : item,
  );
}
