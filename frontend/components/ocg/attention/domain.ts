/**
 * Attention / Approvals Center domain.
 *
 * OCG-owned normalized frontend model. Attention is everything requiring
 * meaningful human intervention; Approval is an explicit decision gate;
 * Blocked is work unable to progress; Failure is an execution/runtime
 * problem. These concepts stay separate and are never merged into a
 * generic "notification" model.
 *
 * Fixture-driven only. No backend contract is implied.
 */

import type { ProjectId } from "../project/domain";

export type AttentionKind =
  | "approval"
  | "budget"
  | "policy"
  | "permission"
  | "blocked"
  | "runtime-failure"
  | "resource-degraded"
  | "configuration"
  | "retry"
  | "escalation";

export const ATTENTION_KINDS: readonly AttentionKind[] = [
  "approval",
  "budget",
  "policy",
  "permission",
  "blocked",
  "runtime-failure",
  "resource-degraded",
  "configuration",
  "retry",
  "escalation",
];

export type AttentionLifecycle =
  | "pending"
  | "acknowledged"
  | "approved"
  | "rejected"
  | "resolved"
  | "expired"
  | "superseded";

export type AttentionSeverity = "info" | "warning" | "high" | "critical";

export type AttentionSource =
  | "mission"
  | "execution"
  | "runtime"
  | "provider"
  | "resource"
  | "policy"
  | "configuration";

export type AttentionDestination =
  | "mission-control"
  | "control-center"
  | "resource-ledger"
  | "logs"
  | "settings"
  | "chat";

export type ApprovalType =
  | "mission-launch"
  | "spend-increase"
  | "provider-use"
  | "external-action"
  | "retry-escalation"
  | "policy-exception";

export type ApprovalDecision = "pending" | "approved" | "rejected";

export type AttentionApproval = {
  id: string;
  type: ApprovalType;
  requestedAction: string;
  reason: string;
  requester: string;
  missionId?: string;
  missionTitle?: string;
  taskId?: string;
  taskTitle?: string;
  requestedAt: string;
  expiresAt?: string;
  estimatedImpact?: string;
  requestedSpendMicros?: number;
  requestedProvider?: string;
  requestedModel?: string;
  approveConsequence: string;
  rejectConsequence: string;
  decision: ApprovalDecision;
};

export type BlockedContext = {
  missionId: string;
  missionTitle: string;
  taskId?: string;
  taskTitle?: string;
  workerId?: string;
  workerLabel?: string;
  reason: string;
  unblocksWhen: string;
};

export type AttentionResolution = {
  outcome: Extract<AttentionLifecycle, "approved" | "rejected" | "resolved" | "expired" | "superseded">;
  at: string;
  note?: string;
};

export type AttentionItem = {
  id: string;
  kind: AttentionKind;
  status: AttentionLifecycle;
  severity: AttentionSeverity;
  title: string;
  /** Concise reason shown in the list row. */
  summary: string;
  /** What happened (inspector Q1). */
  whatHappened: string;
  /** Why OCG needs the operator (inspector Q2). */
  whyNeeded: string;
  /** What happens if the operator does nothing (inspector Q5). */
  inactionConsequence: string;
  createdAt: string;
  updatedAt: string;
  source: AttentionSource;
  destination: AttentionDestination;
  /**
   * Owning project for fixture-owned attention items. Snapshot-derived items
   * leave this unset and are scoped by the project snapshot instead.
   */
  projectId?: ProjectId;
  missionId?: string;
  missionTitle?: string;
  taskId?: string;
  taskTitle?: string;
  providerId?: string;
  providerLabel?: string;
  model?: string;
  /** Present only for explicit decision gates. Never faked for blocked work. */
  approval: AttentionApproval | null;
  /** Present only for work unable to progress. */
  blocked: BlockedContext | null;
  resolution: AttentionResolution | null;
};

export type AttentionTab = "overview" | "approvals" | "blocked" | "resolved";

export const ATTENTION_TABS: readonly AttentionTab[] = [
  "overview",
  "approvals",
  "blocked",
  "resolved",
];

const UNRESOLVED: readonly AttentionLifecycle[] = ["pending", "acknowledged"];

export function isUnresolved(item: AttentionItem): boolean {
  return UNRESOLVED.includes(item.status);
}

export function isApprovalItem(item: AttentionItem): boolean {
  return item.approval !== null;
}

export function isBlockedItem(item: AttentionItem): boolean {
  return item.kind === "blocked" || item.blocked !== null;
}

export function isResolvedHistory(item: AttentionItem): boolean {
  return !isUnresolved(item);
}

/** Display labels kept next to the domain so list + inspector agree. */
export const ATTENTION_KIND_LABELS: Record<AttentionKind, string> = {
  approval: "Approval",
  budget: "Budget",
  policy: "Policy",
  permission: "Permission",
  blocked: "Blocked",
  "runtime-failure": "Runtime failure",
  "resource-degraded": "Resource degraded",
  configuration: "Configuration",
  retry: "Retry",
  escalation: "Escalation",
};

export const ATTENTION_SEVERITY_LABELS: Record<AttentionSeverity, string> = {
  info: "Info",
  warning: "Warning",
  high: "High",
  critical: "Critical",
};

export const ATTENTION_STATUS_LABELS: Record<AttentionLifecycle, string> = {
  pending: "Pending",
  acknowledged: "Acknowledged",
  approved: "Approved",
  rejected: "Rejected",
  resolved: "Resolved",
  expired: "Expired",
  superseded: "Superseded",
};

export const ATTENTION_SOURCE_LABELS: Record<AttentionSource, string> = {
  mission: "Mission",
  execution: "Execution",
  runtime: "Runtime",
  provider: "Provider",
  resource: "Resource",
  policy: "Policy",
  configuration: "Configuration",
};

export const APPROVAL_TYPE_LABELS: Record<ApprovalType, string> = {
  "mission-launch": "Mission launch",
  "spend-increase": "Spend increase",
  "provider-use": "Provider use",
  "external-action": "External action",
  "retry-escalation": "Retry escalation",
  "policy-exception": "Policy exception",
};
