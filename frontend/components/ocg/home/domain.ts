/**
 * Home domain types and pure helpers.
 *
 * Home is a projection over existing normalized domains (bootstrap,
 * job execution, sessions, resource ledger). It does not own new backend
 * state and never invent values.
 */

import type { Tone } from "../primitives";
import type { RuntimeStatus } from "../types";
import type { CanonicalJobState } from "../contracts";

export type AttentionSeverity = "info" | "attention" | "warning" | "critical";

export type AttentionKind =
  | "approvalRequired"
  | "budgetGate"
  | "blockedTask"
  | "providerUnavailable"
  | "modelUnavailable"
  | "runtimeFailure"
  | "verificationFailure"
  | "configurationIssue"
  | "authenticationRequired"
  | "degradedResource";

export type AttentionDestination =
  | "job-execution"
  | "control-center"
  | "resource-ledger"
  | "logs"
  | "settings"
  | "onboarding";

export type AttentionItem = {
  id: string;
  severity: AttentionSeverity;
  kind: AttentionKind;
  title: string;
  summary: string;
  jobId?: string;
  sessionId?: string;
  taskId?: string;
  workerId?: string;
  provider?: string;
  model?: string;
  createdAt?: string;
  status: "open" | "investigating" | "resolved";
  destination: AttentionDestination;
};

export type ActiveJobProjection = {
  id: string;
  title: string;
  status: CanonicalJobState;
  destination: "job-execution";
  sessionId: string;
  jobId: string;
  projectId: string;
  updatedAt?: string;
};

export type ContinueWorkingEntry = {
  id: string;
  title: string;
  subtitle: string;
  timeAgo: string;
  kind: "chat" | "job" | "diagnostics" | "configuration";
  destination: string;
  sessionId?: string;
  updatedAt?: string;
  workType?: string;
};

export type ResourceHealthSummary = {
  providerCount: number | null;
  connectedProviders: number | null;
  degradedProviders: number | null;
  unavailableProviders: number | null;
  authRequiredProviders: number | null;
  unknownProviders: number | null;
  modelCount: number | null;
  availableModels: number | null;
  pendingModels: number | null;
  unavailableModels: number | null;
  unknownModels: number | null;
  activeProfileLabel: string | null;
  runtimeState: RuntimeStatus["state"];
  hasProviderIssues: boolean;
  hasUnreportedResourceState: boolean;
  items?: ResourceHealthItem[];
};

export type ResourceHealthItem = { id: string; label: string; state: "connected" | "degraded" | "auth-required" | "unavailable" | "unknown"; detail: string | null };

export type UsageSummary = {
  costMicros: number | null;
  costProvenance: string;
  totalTokens: number | null;
  freshInput: number | null;
  cacheRead: number | null;
  cacheShare: number | null;
  cacheLeverage: number | null;
  entryCount: number;
  available: boolean;
  totalCost?: number | null;
  hasCost?: boolean;
};

/** Subset of the shared tone scale the activity feed ranks itself by. */
export type RecentActivityTone = Extract<Tone, "emerald" | "sky" | "amber" | "red" | "slate" | "violet">;

export type RecentActivityItem = {
  id: string;
  timeAgo: string;
  summary: string;
  kind: "chat" | "job" | "worker" | "provider" | "model" | "budget" | "resource" | "verification";
  tone: RecentActivityTone;
  title?: string;
  subtitle?: string;
  timestamp?: string;
  destination?: string;
};
