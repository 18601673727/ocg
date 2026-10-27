/**
 * Normalized resource-ledger domain.
 *
 * This module is deliberately presentation-free. It owns the shape of a single
 * provider call as it appears in the Resource Ledger Inspector: usage split into
 * explicit components, cost with an explicit provenance, how usage was
 * authorized, how confidently the call was attributed to a Job/task, and
 * whether the call reconciled against the durable ledger.
 *
 * Missing data is always `null` (rendered as `—`), never a fabricated zero.
 * Token counts are integers; cost is stored as integer micro-units of USD.
 */

export type UsageComponent = "freshInput" | "cacheRead" | "cacheWrite" | "output" | "reasoning";

/** Where a cost number came from. `unavailable` means no cost was observed. */
export type CostProvenance = "reported" | "stored" | "estimated" | "unavailable";

/** How the usage numbers were authorized for this call. */
export type UsageAuthority = "reportedCall" | "reconciled" | "fallback" | "estimated";

/** Confidence that this call belongs to the Job/task it is grouped under. */
export type AttributionConfidence = "exact" | "inferred" | "fallback" | "unknown";

/** State of the reconciliation between observed calls and the durable ledger. */
export type ReconciliationStatus =
  | "reconciled"
  | "partial"
  | "pending"
  | "mismatch"
  | "notApplicable"
  | "unknown";

export type LedgerCallStatus = "success" | "failure" | "retrying";

export type LedgerRole = "lead" | "worker";

/** Token counts per component. A missing key means the component was not observed. */
export type LedgerUsage = Partial<Record<UsageComponent, number>>;

export type ResourceLedgerEntry = {
  id: string;
  /** ISO-8601 timestamp. */
  timestamp: string;
  startedAt: string | null;
  finishedAt: string | null;
  sessionId: string | null;
  invocationId: string | null;
  elapsedMs: number;
  durationMs: number | null;
  latencyMs: number | null;
  ttftMs: number | null;
  jobId: string;
  jobLabel: string;
  taskId: string;
  taskLabel: string;
  workerId: string;
  workerLabel: string;
  role: LedgerRole;
  provider: string;
  model: string;
  variant: string | null;
  /** 1-based sequence of this call within its worker/task. */
  callIndex: number;
  /** 1-based attempt; values greater than 1 are retries. */
  attempt: number;
  status: LedgerCallStatus;
  usage: LedgerUsage | null;
  usageAuthority: UsageAuthority;
  /** Integer micro-units of USD (1 USD = 1_000_000 micros). `null` is unavailable, not zero. */
  costMicros: number | null;
  costProvenance: CostProvenance;
  attributionConfidence: AttributionConfidence;
  attributedJobId: string | null;
  attributedTaskId: string | null;
  reconciliation: ReconciliationStatus;
  note: string | null;
};

/** The full normalized ledger for a workspace. */
export type ResourceLedger = {
  generatedAt: string;
  entries: ResourceLedgerEntry[];
};
