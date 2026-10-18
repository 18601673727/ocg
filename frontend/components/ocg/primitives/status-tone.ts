/**
 * Status semantics for the statuses that more than one surface renders.
 *
 * Each map turns a domain status into a `Tone` plus whether it is in flight
 * (`pulse`). Keeping them here is what stops the sidebar, topbar, chat and
 * inspector from drifting apart on what "disconnected" or "retrying" looks
 * like. Statuses that exactly one surface draws stay in that surface's domain.
 */

import type { LogLevel } from "../logs/domain";
import type { CanonicalJobState } from "../contracts";
import type { RuntimeSyncStatus } from "../runtime/reconciler";
import type { RuntimeAuthority } from "../runtime/runtime-types";
import type {
  RuntimeConnectionState,
  ToolActivityStatus,
  WorkerStatus,
} from "../types";
import type { Tone } from "./tone";

export type StatusVisual = { tone: Tone; pulse?: boolean };

export const RUNTIME_CONNECTION: Record<RuntimeConnectionState, StatusVisual> = {
  connected: { tone: "emerald" },
  connecting: { tone: "amber", pulse: true },
  disconnected: { tone: "slate" },
  failed: { tone: "red" },
};

/** Short word for the connection state; "connected" reads as "ready" to users. */
export const RUNTIME_CONNECTION_LABEL: Record<RuntimeConnectionState, string> = {
  connected: "ready",
  connecting: "connecting",
  disconnected: "disconnected",
  failed: "failed",
};

/**
 * Which runtime actually answers Chat. The surfaces say this instead of
 * naming a fixture, so a product session can never be read as the mock.
 */
export const RUNTIME_AUTHORITY_LABEL: Record<RuntimeAuthority, string> = {
  canonical: "OCG backend",
  mock: "mock runtime",
};

export const SYNC_STATUS: Record<RuntimeSyncStatus, StatusVisual> = {
  uninitialized: { tone: "slate" },
  "loading-snapshot": { tone: "amber", pulse: true },
  live: { tone: "emerald" },
  reconnecting: { tone: "amber", pulse: true },
  stale: { tone: "amber", pulse: true },
  error: { tone: "red" },
};

/** Copy for the degraded sync states the topbar surfaces. */
export function syncStatusLabel(status: RuntimeSyncStatus): string {
  if (status === "stale") return "resync needed";
  if (status === "error") return "sync error";
  return "syncing";
}

export const WORKER_STATUS: Record<WorkerStatus, StatusVisual> = {
  queued: { tone: "slate" },
  starting: { tone: "sky" },
  active: { tone: "amber", pulse: true },
  waiting: { tone: "violet" },
  idle: { tone: "slate" },
  completed: { tone: "emerald" },
  failed: { tone: "red" },
  cancelled: { tone: "slate" },
};

export const JOB_STATE: Record<CanonicalJobState, StatusVisual> = {
  pending: { tone: "slate" },
  eligible: { tone: "sky" },
  running: { tone: "amber", pulse: true },
  cancelling: { tone: "amber", pulse: true },
  completed: { tone: "emerald" },
  failed: { tone: "red" },
  cancelled: { tone: "slate" },
  unknown: { tone: "slate" },
  orphaned: { tone: "red" },
};

export const TOOL_STATUS: Record<ToolActivityStatus, StatusVisual> = {
  pending: { tone: "slate" },
  running: { tone: "amber", pulse: true },
  success: { tone: "emerald" },
  failure: { tone: "red" },
  retrying: { tone: "sky", pulse: true },
  "waiting-approval": { tone: "violet" },
};

export const LOG_LEVEL: Record<LogLevel, StatusVisual> = {
  trace: { tone: "slate" },
  debug: { tone: "violet" },
  info: { tone: "sky" },
  warn: { tone: "amber" },
  error: { tone: "red" },
};
