/**
 * Placement projection for one canonical Job.
 *
 * Placement is the backend's `Policy → Placement → Dispatch` decision path. This
 * module reads the facts the backend *recorded* and groups them for display. It
 * does not choose, rank, score, filter, or re-evaluate anything: the Provider
 * and Model below are the ones the backend froze on a DispatchIntent before the
 * Provider ran, and a Job with no such record simply has no target here.
 *
 * What is deliberately absent is as important as what is present. The backend's
 * candidate list, its per-candidate health/budget/capacity verdicts, and its
 * ranking order live inside one transient `placement::choose` call
 * (`src/orchestration/placement.rs`) and are never persisted. So there is no
 * candidate table to render, and this file does not synthesize one from the
 * current Profile — doing so would show today's configuration as though it were
 * a past decision.
 *
 * Placement is recorded per Attempt, not per Job: each admission freezes its own
 * target, so a Job with several Attempts can legitimately carry several
 * different targets. They are listed per Attempt instead of collapsed, because
 * a replacement Attempt's target is a separate decision from the one it replaced.
 */

import type { Failure } from "../contracts";
import { terminalProducingAttempt, type JobExecution } from "./domain";

/**
 * The reason codes `placement::choose` records when it returns no target, plus
 * the Profile-empty rejection written on the launch path. These are the exact
 * codes the backend writes today; an unrecognized code is left to render as a
 * plain Job termination reason rather than being claimed as placement evidence.
 */
export const PLACEMENT_FAILURE_CODES = [
  "placement_health_unknown",
  "placement_unavailable",
  "placement_incompatible",
  "placement_capacity",
] as const;

export type PlacementFailureCode = (typeof PLACEMENT_FAILURE_CODES)[number];

/** Whether the backend recorded a placement failure as this Job's termination reason. */
export function isPlacementFailure(failure: Failure | null): failure is Failure {
  return failure !== null && (PLACEMENT_FAILURE_CODES as readonly string[]).includes(failure.code);
}

/**
 * How complete the placement evidence is for this Job.
 *
 * - `recorded`: at least one dispatch froze an execution target.
 * - `rejected`: the backend recorded a placement failure and no target followed.
 * - `pending`: the backend has not produced either, which is not the same claim
 *   as "placement chose nothing".
 */
export type PlacementOutcome = "recorded" | "rejected" | "pending";

/** One execution target, as frozen by the backend for one Attempt. */
export type PlacementTarget = {
  attemptId: string;
  generation: number;
  /** Whether this is the Attempt the backend currently calls authoritative. */
  authoritative: boolean;
  providerKey: string;
  model: string;
  upstreamModelId: string | null;
  /**
   * Every dispatch carrying this same frozen target. Placement decides once per
   * admission, so one Attempt usually has several.
   */
  dispatchIntentIds: readonly string[];
  /** The dispatch that froze this target, in canonical creation order. */
  firstDispatchIntentId: string;
  createdAt: number;
};

/**
 * The primary placement target, qualified by the Job's lifecycle position.
 *
 * - `current`: the Job names a live authoritative Attempt, and this is that
 *   Attempt's frozen target.
 * - `final`: the Job is terminal. Terminalization cleared
 *   `authoritative_attempt_id`, so there is no current authority; this is
 *   the frozen target of the Attempt whose generation the Job settled at —
 *   the target that produced the terminal outcome, not a historical one.
 */
export type SelectedPlacementTarget = {
  kind: "current" | "final";
  target: PlacementTarget;
};

export type PlacementProjection = {
  outcome: PlacementOutcome;
  /** Targets in canonical order, oldest Attempt generation first. */
  targets: readonly PlacementTarget[];
  /** The recorded placement failure, when the backend produced one. */
  failure: Failure | null;
  /**
   * The Job's primary target: the frozen target belonging to the Job's
   * authority reference, and nothing else. The reference is the live
   * authoritative Attempt while the Job names one, or the producing Attempt
   * of a terminal Job, whose settled `generation` is the only correlation
   * terminalization leaves behind.
   *
   * `null` whenever the referenced Attempt has not frozen a target yet, or
   * when no Attempt can be referenced. An older Attempt's target is real
   * history and stays in `history`, but it is never promoted here: the
   * backend can mint a new Attempt and Executor before the first provider
   * DispatchIntent has its configuration frozen, and reading that window as
   * "the selected target" would state a decision the current Attempt has
   * not made.
   */
  selected: SelectedPlacementTarget | null;
  /** Recorded targets belonging to Attempts other than the selected one. */
  history: readonly PlacementTarget[];
};

/** Stable grouping key for the dispatches that froze one target. */
function targetKey(attemptId: string, providerKey: string, model: string, upstreamModelId: string | null): string {
  return JSON.stringify([attemptId, providerKey, model, upstreamModelId]);
}

/**
 * Read one Job's placement facts.
 *
 * Only canonical fields are consulted: the frozen target columns on the Job's
 * DispatchIntents, and the Job's own termination reason. Provider health,
 * budget headroom, and concurrency are never consulted here.
 */
export function placementOf(execution: JobExecution): PlacementProjection {
  const failure = isPlacementFailure(execution.terminationReason) ? execution.terminationReason : null;

  // Group the dispatches that froze the same target under the same Attempt.
  // Ordering is presentational over records that already exist; nothing is
  // ranked, and no target is preferred over another on the backend's behalf.
  const grouped = new Map<string, PlacementTarget>();
  const dispatches = [...execution.dispatchIntents].sort(
    (left, right) => left.createdAt - right.createdAt || left.dispatchIntentId.localeCompare(right.dispatchIntentId),
  );
  for (const intent of dispatches) {
    // A native tool Call carries no Provider target; placement never decided one.
    if (intent.providerKey === null || intent.model === null) continue;
    const key = targetKey(intent.attemptId, intent.providerKey, intent.model, intent.upstreamModelId);
    const existing = grouped.get(key);
    if (existing) {
      existing.dispatchIntentIds = [...existing.dispatchIntentIds, intent.dispatchIntentId];
      continue;
    }
    grouped.set(key, {
      attemptId: intent.attemptId,
      generation: intent.generation,
      authoritative: execution.authoritativeAttemptId === intent.attemptId,
      providerKey: intent.providerKey,
      model: intent.model,
      upstreamModelId: intent.upstreamModelId,
      dispatchIntentIds: [intent.dispatchIntentId],
      firstDispatchIntentId: intent.dispatchIntentId,
      createdAt: intent.createdAt,
    });
  }

  const targets = [...grouped.values()].sort(
    (left, right) => left.generation - right.generation || left.createdAt - right.createdAt,
  );

  // The primary target is scoped to the Job's authority reference alone: the
  // live authoritative Attempt while the Job names one, or — once
  // terminalization has cleared that pointer — the Attempt whose generation
  // the terminal Job settled at. An older Attempt's target is never promoted:
  // a live replacement Attempt that has not frozen a target yet means no
  // current target, and a terminal Job whose producing Attempt froze nothing
  // (or cannot be identified) means no final target. If the referenced
  // Attempt somehow recorded more than one distinct target group, the
  // earliest group in canonical order is taken; no preference is expressed,
  // because deciding between two targets of one Attempt would be scheduler
  // semantics the backend never exposed.
  const authoritative = execution.authoritativeAttemptId;
  const producing = authoritative === null
    ? terminalProducingAttempt(execution.state, execution.generation, execution.attempts)
    : null;
  const referenceAttemptId = authoritative ?? producing?.attemptId ?? null;
  const referenceTarget = referenceAttemptId === null
    ? null
    : targets.find((target) => target.attemptId === referenceAttemptId) ?? null;
  const selected: SelectedPlacementTarget | null = referenceTarget === null
    ? null
    : { kind: authoritative !== null ? "current" : "final", target: referenceTarget };
  const history = referenceTarget === null
    ? targets
    : targets.filter((target) => target.attemptId !== referenceTarget.attemptId);

  // `recorded` keeps meaning "the backend recorded placement evidence for this
  // Job", so history alone still counts as recorded. The current target and the
  // existence of history are separate facts.
  const outcome: PlacementOutcome = targets.length > 0 ? "recorded" : failure !== null ? "rejected" : "pending";
  return { outcome, targets, failure, selected, history };
}
