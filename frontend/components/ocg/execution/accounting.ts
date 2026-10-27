/** Canonical Job ceiling and backend-projected settlement consumption. */

import type { UsageCost } from "../contracts";

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
  consumption: UsageCost | null;
};
