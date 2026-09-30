/**
 * Home domain types and pure helpers.
 *
 * Home is a projection over existing normalized domains (bootstrap,
 * missions, sessions, resource ledger). It does not own new backend
 * state and never invent values.
 */

import type { MissionStatus } from "../types";

import type { Tone } from "../primitives";

export type AttentionSeverity = "info" | "attention" | "warning" | "critical";

export type AttentionKind =
  | "approvalRequired"
  | "budgetGate"
  | "blockedTask"
  | "providerUnavailable"
  | "runtimeFailure"
  | "verificationFailure"
  | "configurationIssue"
  | "authenticationRequired"
  | "degradedResource";

export type AttentionDestination =
  | "mission-control"
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
  missionId?: string;
  taskId?: string;
  workerId?: string;
  provider?: string;
  model?: string;
  createdAt: string;
  status: "open" | "investigating" | "resolved";
  destination: AttentionDestination;
};

export type ActiveMissionProjection = {
  id: string;
  title: string;
  status: MissionStatus | "pending";
  completed: number;
  total: number;
  currentWave?: number;
  totalWaves?: number;
  activeWorkers: number;
  blockedWorkers: number;
  waitingWorkers: number;
  elapsed: string;
  budgetSpent?: number;
  budgetLimit?: number;
  progress: number;
  destination: "mission-control";
  sessionId?: string;
  jobId?: string;
  projectId?: string;
  updatedAt?: string;
};

export type ContinueWorkingEntry = {
  id: string;
  title: string;
  subtitle: string;
  timeAgo: string;
  kind: "chat" | "mission" | "diagnostics" | "configuration";
  destination: string;
  sessionId?: string;
  updatedAt?: string;
  workType?: string;
};

export type ResourceHealthSummary = {
  providerCount: number;
  healthyProviders: number;
  degradedProviders: number;
  unavailableProviders: number;
  authRequiredProviders: number;
  unknownProviders: number;
  modelCount: number;
  availableModels: number;
  unavailableModels: number;
  activeProfileLabel: string;
  runtimeState: string;
  hasDegradedOrAuthRequired: boolean;
  items?: ResourceHealthItem[];
};

export type ResourceHealthItem = { id: string; label: string; state: "healthy" | "degraded" | "auth-required" | "unavailable"; detail: string | null };

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
export type RecentActivityTone = Extract<Tone, "emerald" | "violet" | "amber" | "red" | "slate">;

export type RecentActivityItem = {
  id: string;
  timeAgo: string;
  summary: string;
  kind: "mission" | "worker" | "provider" | "model" | "budget" | "resource" | "verification";
  tone: RecentActivityTone;
  title?: string;
  subtitle?: string;
  timestamp?: string;
  destination?: string;
};
