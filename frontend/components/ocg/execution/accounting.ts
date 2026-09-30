/**
 * Canonical accounting context for one Job.
 *
 * The execution snapshot carries no accounting. What the control plane does
 * expose is a *ceiling*: a hard budget recorded in global configuration, in a
 * Project's defaults, or in a Job's own pre-run configuration, which is the
 * configuration a DispatchIntent is admitted against. That is the only budget
 * fact this module states.
 *
 * Consumption is deliberately absent. No control route reports what a Job has
 * spent, so there is no number here to render, and every surface that shows a
 * budget says plainly that consumption is not reported rather than modelling,
 * estimating, or borrowing one from another surface.
 */

import type { GlobalConfiguration, JsonValue } from "../contracts";

export type BudgetUnit = string;

/** Which canonical configuration recorded the ceiling, in resolution order. */
export type BudgetCeilingSource = "job-configuration" | "project-default" | "global-default";

export type BudgetCeiling = {
  /** Whole currency units, as `ResourceBudget.hard_limit` stores them. */
  amount: number;
  unit: BudgetUnit;
  source: BudgetCeilingSource;
};

export type JobAccounting = {
  /** The hard limit that applies to this Job, or `null` when none is recorded. */
  ceiling: BudgetCeiling | null;
  /**
   * A placeholder for the consumption fact the control plane does not expose
   * yet. It is typed `null` so no surface can render a modelled spend by
   * accident; it becomes a real field when a Job-scoped measurement route
   * exists, not before.
   */
  consumption: null;
};

const CEILING_SOURCES: readonly BudgetCeilingSource[] = [
  "job-configuration",
  "project-default",
  "global-default",
];

export function isBudgetCeilingSource(value: unknown): value is BudgetCeilingSource {
  return typeof value === "string" && CEILING_SOURCES.includes(value as BudgetCeilingSource);
}

function positiveAmount(value: JsonValue | unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) && value > 0 ? value : null;
}

/**
 * Reads the hard budget a Job's own pre-run configuration froze.
 *
 * The configuration is an unconstrained JSON object in the substrate, so the
 * field is checked rather than trusted: a missing or non-positive value yields
 * `null` rather than a ceiling of nothing.
 */
export function ceilingFromJobConfiguration(
  configuration: unknown,
  fallbackUnit: BudgetUnit = "USD",
): BudgetCeiling | null {
  if (typeof configuration !== "object" || configuration === null || Array.isArray(configuration)) {
    return null;
  }
  const amount = positiveAmount((configuration as Record<string, unknown>).hard_budget);
  return amount === null ? null : { amount, unit: fallbackUnit, source: "job-configuration" };
}

/** The Project default recorded in the configuration view, if any. */
export function ceilingFromProjectDefaults(
  defaults: unknown,
  fallbackUnit: BudgetUnit = "USD",
): BudgetCeiling | null {
  if (typeof defaults !== "object" || defaults === null || Array.isArray(defaults)) return null;
  const amount = positiveAmount((defaults as Record<string, unknown>).hard_budget);
  return amount === null ? null : { amount, unit: fallbackUnit, source: "project-default" };
}

/** The global hard limit, which is a typed struct in the contract. */
export function ceilingFromGlobalConfiguration(
  global: GlobalConfiguration | null,
): BudgetCeiling | null {
  const budget = global?.resource_budget ?? null;
  if (budget === null) return null;
  // Zero records "no explicit hard limit", not "a budget of nothing".
  const amount = positiveAmount(budget.hard_limit);
  return amount === null ? null : { amount, unit: budget.unit, source: "global-default" };
}

/**
 * Resolves the ceiling that applies to a Job: its own configuration wins over
 * the Project default, which wins over the global default. The first candidate
 * present is the one in force, which is why callers pass them in that order.
 */
export function resolveCeiling(
  candidates: readonly (BudgetCeiling | null | undefined)[],
): BudgetCeiling | null {
  for (const candidate of candidates) {
    if (candidate) return candidate;
  }
  return null;
}

export function emptyAccounting(): JobAccounting {
  return { ceiling: null, consumption: null };
}

/**
 * The Job's own elapsed wall time, from the two canonical Job instants.
 *
 * This is the only duration the execution snapshot supports: it is a difference
 * between two timestamps the substrate wrote, not a running counter.
 */
export function jobElapsedMs(input: { createdAt: number; updatedAt: number }): number | null {
  if (!Number.isFinite(input.createdAt) || !Number.isFinite(input.updatedAt)) return null;
  const ms = (input.updatedAt - input.createdAt) * 1000;
  return ms >= 0 ? ms : null;
}
