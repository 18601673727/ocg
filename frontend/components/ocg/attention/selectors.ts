/**
 * Pure selectors for the Attention / Approvals Center.
 *
 * Items are derived from the normalized runtime snapshot plus the
 * scenario-level fixture queue (explicit approvals / policy gates that no
 * existing domain models). Derived helpers never mutate and never widen
 * the Home-owned domain; fixture semantics live in ./fixtures.
 */

import type { RuntimeSnapshot } from "../runtime/runtime-types";
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
    item.jobTitle,
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

// ---------------------------------------------------------------------------
// Derived attention
// ---------------------------------------------------------------------------

export function selectDerivedAttention(snapshot: RuntimeSnapshot, fixtureQueue: {
  approvals: AttentionItem[];
  history: AttentionItem[];
}): AttentionItem[] {
  const items: AttentionItem[] = [...fixtureQueue.approvals, ...fixtureQueue.history];
  for (const [, execution] of Object.entries(snapshot.executionBySession)) {
    if (!execution) continue;
    if ("state" in execution && execution.state === "failed") {
      items.push({
        id: `derived-failed-${execution.jobId}`,
        severity: "warning",
        kind: "runtime-failure",
        title: `Job ${execution.jobId} failed`,
        summary: `Job execution failed.`,
        whatHappened: `The execution reported a failed state.`,
        whyNeeded: "Review the failed Job before continuing.",
        inactionConsequence: "The Job will remain failed until it is retried or investigated.",
        createdAt: "now",
        updatedAt: "now",
        status: "pending",
        destination: "job-execution",
        source: "runtime",
        approval: null,
        blocked: null,
        resolution: null,
      });
    }
  }
  return items;
}

export type AttentionQueue = {
  approvals: AttentionItem[];
  history: AttentionItem[];
};

export function selectAttentionItems(
  snapshot: RuntimeSnapshot,
  queue: AttentionQueue,
): AttentionItem[] {
  return selectDerivedAttention(snapshot, queue);
}

export function acknowledgeAttentionItem(items: AttentionItem[], id: string, at: string): AttentionItem[] {
  return items.map((item) => item.id === id && item.status === "pending" ? { ...item, status: "acknowledged", updatedAt: at } : item);
}

export function resolveAttentionItem(items: AttentionItem[], id: string, at: string): AttentionItem[] {
  return items.map((item) => item.id === id && isUnresolved(item) && !item.approval
    ? { ...item, status: "resolved", updatedAt: at, resolution: { outcome: "resolved", at } }
    : item);
}

export function applyAttentionDecision(items: AttentionItem[], id: string, decision: "approved" | "rejected", at: string): AttentionItem[] {
  return items.map((item) => item.id === id && isApprovalItem(item)
    ? { ...item, status: decision, updatedAt: at }
    : item);
}
