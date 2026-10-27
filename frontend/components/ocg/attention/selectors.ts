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

export type AttentionSummary = {
  needsAction: number;
  awaitingApproval: number;
  blocked: number;
  critical: number;
  high: number;
  resolved: number;
};

// ---------------------------------------------------------------------------
// Derived attention
// ---------------------------------------------------------------------------

export function selectDerivedAttention(
  snapshot: RuntimeSnapshot,
  fixtureQueue: {
    approvals: AttentionItem[];
    history: AttentionItem[];
  },
): AttentionItem[] {
  const items: AttentionItem[] = [...fixtureQueue.approvals, ...fixtureQueue.history];
  const executions =
    snapshot.executionsByProject !== undefined
      ? Object.values(snapshot.executionsByProject).flatMap((jobs) => Object.values(jobs))
      : [
          ...new Map(
            Object.values(snapshot.executionBySession)
              .filter((execution): execution is NonNullable<typeof execution> => execution !== null)
              .map((execution) => [execution.jobId, execution]),
          ).values(),
        ];
  for (const execution of executions) {
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
        jobId: execution.jobId,
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
