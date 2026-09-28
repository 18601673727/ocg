/**
 * Pure Mission draft domain.
 *
 * A MissionDraft is the operator-owned, frontend-only record of an in-progress
 * launch. It never calls a runtime and never persists to a backend. The
 * lifecycle is a single field (not a pile of booleans) and every transition is
 * a pure reducer step.
 */

import { PROJECT_IDS, type ProjectId } from "../project/domain";

/** A single lifecycle field replaces scattered `isSubmitting`/`hasError` flags. */
export type MissionDraftLifecycle =
  | "drafting"
  | "invalid"
  | "ready"
  | "launching"
  | "launched"
  | "launch-failed";

export type HardBudgetSource = "fixture-recommended" | "user";

export type MissionDraftTextField = "objective" | "successCriteria" | "constraints";

export type MissionDraftField =
  | MissionDraftTextField
  | "hardBudgetMicros"
  | "hardBudgetSource"
  | "resourceCommitment"
  | "lifecycle"
  | "projectId"
  | "sessionId";

export type MissionDraftIssueCode =
  | "objective-required"
  | "objective-too-short"
  | "success-criteria-required"
  | "budget-required"
  | "budget-not-integer"
  | "budget-not-safe-integer"
  | "budget-below-minimum"
  | "resource-commitment-out-of-range"
  | "project-invalid"
  | "session-required"
  | "launched-without-mission"
  | "launch-failed-without-message";

export type MissionDraftIssue = {
  code: MissionDraftIssueCode;
  field: MissionDraftField;
  message: string;
  severity: "error" | "warning";
};

export type MissionDraft = {
  /** Stable per (project, session) scope. */
  id: string;
  projectId: ProjectId;
  sessionId: string;
  objective: string;
  successCriteria: string;
  constraints: string;
  hardBudgetMicros: number | null;
  hardBudgetSource: HardBudgetSource;
  /** Normalized 0..1 share of available capacity. */
  resourceCommitment: number;
  lifecycle: MissionDraftLifecycle;
  issues: MissionDraftIssue[];
  missionId?: string;
  launchMessage?: string;
};

/**
 * The existing fixture Mission cap is $25. This is explicitly a fixture
 * recommendation for the local mock runtime, not a production policy.
 */
export const FIXTURE_RECOMMENDED_BUDGET_MICROS = 25_000_000;
export const FIXTURE_RECOMMENDED_BUDGET_USD = 25;
export const FIXTURE_RECOMMENDED_BUDGET_NOTE =
  "Fixture-recommended $25.00 hard cap — mock local policy, not a production default.";

/** Smallest positive amount representable by the existing USD micro-unit model. */
export const HARD_BUDGET_MIN_MICROS = 1;
export const MIN_OBJECTIVE_LENGTH = 12;
export const DEFAULT_RESOURCE_COMMITMENT = 0.5;

export type MissionLaunchCommand = {
  /** Stable command identity; identical content reuses the recorded result. */
  commandId: string;
  draftId: string;
  projectId: ProjectId;
  sessionId: string;
  objective: string;
  successCriteria: string;
  constraints: string;
  hardBudgetMicros: number;
  resourceCommitment: number;
};

/* -------------------------------------------------------------------------- */
/* Money helpers                                                              */
/* -------------------------------------------------------------------------- */

/** Convert integer micros to USD without introducing fractional cents. */
export function microsToUsd(micros: number): number {
  return Math.round(micros) / 1_000_000;
}

/** Convert a USD amount to integer micros, or null when not a finite number. */
export function usdToMicros(dollars: number): number | null {
  if (!Number.isFinite(dollars)) return null;
  return Math.round(dollars * 1_000_000);
}

/* -------------------------------------------------------------------------- */
/* Identity                                                                   */
/* -------------------------------------------------------------------------- */

export function missionDraftId(projectId: ProjectId, sessionId: string): string {
  return `mission-draft:${projectId}:${sessionId}`;
}

/** Pure scope key used to store one draft per Project + session. */
export function draftScopeKey(projectId: ProjectId, sessionId: string): string {
  return `${projectId}\u0000${sessionId}`;
}

/* -------------------------------------------------------------------------- */
/* Creation                                                                    */
/* -------------------------------------------------------------------------- */

export function createMissionDraft(input: {
  projectId: ProjectId;
  sessionId: string;
  objective?: string;
  successCriteria?: string;
  constraints?: string;
  hardBudgetMicros?: number | null;
  hardBudgetSource?: HardBudgetSource;
  resourceCommitment?: number;
  id?: string;
}): MissionDraft {
  const seededBudget = input.hardBudgetMicros === undefined
    ? FIXTURE_RECOMMENDED_BUDGET_MICROS
    : input.hardBudgetMicros;
  const draft: MissionDraft = {
    id: input.id ?? missionDraftId(input.projectId, input.sessionId),
    projectId: input.projectId,
    sessionId: input.sessionId,
    objective: input.objective?.trim() ?? "",
    successCriteria: input.successCriteria?.trim() ?? "",
    constraints: input.constraints?.trim() ?? "",
    hardBudgetMicros: seededBudget,
    hardBudgetSource: input.hardBudgetSource ?? (input.hardBudgetMicros === undefined ? "fixture-recommended" : "user"),
    resourceCommitment: input.resourceCommitment ?? DEFAULT_RESOURCE_COMMITMENT,
    lifecycle: "drafting",
    issues: [],
  };
  return { ...draft, issues: validateMissionDraft(draft) };
}

/* -------------------------------------------------------------------------- */
/* Validation                                                                  */
/* -------------------------------------------------------------------------- */

export function missionDraftHasErrors(issues: readonly MissionDraftIssue[]): boolean {
  return issues.some((issue) => issue.severity === "error");
}

export function validateMissionDraft(
  draft: MissionDraft,
  validProjectIds: readonly string[] = PROJECT_IDS,
): MissionDraftIssue[] {
  const issues: MissionDraftIssue[] = [];
  const objective = draft.objective.trim();

  if (objective.length === 0) {
    issues.push({
      code: "objective-required",
      field: "objective",
      message: "Add an objective before launching a Mission.",
      severity: "error",
    });
  } else if (objective.length < MIN_OBJECTIVE_LENGTH) {
    issues.push({
      code: "objective-too-short",
      field: "objective",
      message: `Describe the objective in at least ${MIN_OBJECTIVE_LENGTH} characters.`,
      severity: "error",
    });
  }

  if (draft.successCriteria.trim().length === 0) {
    issues.push({
      code: "success-criteria-required",
      field: "successCriteria",
      message: "Add at least one success criterion so success can be verified.",
      severity: "error",
    });
  }

  if (!validProjectIds.includes(draft.projectId)) {
    issues.push({
      code: "project-invalid",
      field: "projectId",
      message: `Project "${draft.projectId}" is not a known Project.`,
      severity: "error",
    });
  }

  if (draft.sessionId.trim().length === 0) {
    issues.push({
      code: "session-required",
      field: "sessionId",
      message: "A Mission draft must belong to a session.",
      severity: "error",
    });
  }

  const budget = draft.hardBudgetMicros;
  if (budget === null) {
    issues.push({
      code: "budget-required",
      field: "hardBudgetMicros",
      message: "Set a hard budget before launching.",
      severity: "error",
    });
  } else if (!Number.isInteger(budget)) {
    issues.push({
      code: "budget-not-integer",
      field: "hardBudgetMicros",
      message: "Hard budget must be a whole number of micros.",
      severity: "error",
    });
  } else if (!Number.isSafeInteger(budget)) {
    issues.push({
      code: "budget-not-safe-integer",
      field: "hardBudgetMicros",
      message: "Hard budget is outside the safely representable range.",
      severity: "error",
    });
  } else if (budget < HARD_BUDGET_MIN_MICROS) {
    issues.push({
      code: "budget-below-minimum",
      field: "hardBudgetMicros",
      message: `Hard budget must be at least $${microsToUsd(HARD_BUDGET_MIN_MICROS).toFixed(6)}.`,
      severity: "error",
    });
  }

  const commitment = draft.resourceCommitment;
  if (!Number.isFinite(commitment) || commitment < 0 || commitment > 1) {
    issues.push({
      code: "resource-commitment-out-of-range",
      field: "resourceCommitment",
      message: "Resource commitment must be between 0 and 1.",
      severity: "error",
    });
  }

  if (draft.lifecycle === "launched" && !draft.missionId) {
    issues.push({
      code: "launched-without-mission",
      field: "lifecycle",
      message: "A launched draft must reference the created Mission.",
      severity: "error",
    });
  }

  if (draft.lifecycle === "launch-failed" && !draft.launchMessage) {
    issues.push({
      code: "launch-failed-without-message",
      field: "lifecycle",
      message: "A failed launch must record why it failed.",
      severity: "error",
    });
  }

  return issues;
}

/* -------------------------------------------------------------------------- */
/* Reducer                                                                     */
/* -------------------------------------------------------------------------- */

export type MissionDraftAction =
  | { type: "update-field"; field: MissionDraftTextField; value: string }
  | { type: "set-hard-budget"; micros: number | null; source: HardBudgetSource }
  | { type: "set-resource-commitment"; value: number }
  | { type: "validate" }
  | { type: "start-launch" }
  | { type: "launch-succeeded"; missionId: string; message?: string }
  | { type: "launch-failed"; message: string };

function withLiveIssues(draft: MissionDraft): MissionDraft {
  return { ...draft, issues: validateMissionDraft(draft) };
}

function isSettling(lifecycle: MissionDraftLifecycle): boolean {
  return lifecycle === "launching" || lifecycle === "launched";
}

export function missionDraftReducer(draft: MissionDraft, action: MissionDraftAction): MissionDraft {
  switch (action.type) {
    case "update-field": {
      if (isSettling(draft.lifecycle)) return draft;
      const next: MissionDraft = {
        ...draft,
        [action.field]: action.value,
        lifecycle: "drafting",
        launchMessage: undefined,
      };
      return withLiveIssues(next);
    }

    case "set-hard-budget": {
      if (isSettling(draft.lifecycle)) return draft;
      const next: MissionDraft = {
        ...draft,
        hardBudgetMicros: action.micros,
        hardBudgetSource: action.source,
        lifecycle: "drafting",
        launchMessage: undefined,
      };
      return withLiveIssues(next);
    }

    case "set-resource-commitment": {
      if (isSettling(draft.lifecycle)) return draft;
      const next: MissionDraft = {
        ...draft,
        resourceCommitment: action.value,
        lifecycle: "drafting",
        launchMessage: undefined,
      };
      return withLiveIssues(next);
    }

    case "validate": {
      if (isSettling(draft.lifecycle)) return draft;
      const issues = validateMissionDraft(draft);
      return {
        ...draft,
        issues,
        lifecycle: missionDraftHasErrors(issues) ? "invalid" : "ready",
      };
    }

    case "start-launch": {
      if (isSettling(draft.lifecycle)) return draft;
      const issues = validateMissionDraft(draft);
      if (missionDraftHasErrors(issues)) {
        return { ...draft, issues, lifecycle: "invalid" };
      }
      return { ...draft, issues, lifecycle: "launching", launchMessage: undefined };
    }

    case "launch-succeeded": {
      if (draft.lifecycle !== "launching") return draft;
      return {
        ...draft,
        lifecycle: "launched",
        missionId: action.missionId,
        launchMessage: action.message,
        issues: [],
      };
    }

    case "launch-failed": {
      if (draft.lifecycle !== "launching") return draft;
      return { ...draft, lifecycle: "launch-failed", launchMessage: action.message };
    }
  }
}

/* -------------------------------------------------------------------------- */
/* Command conversion                                                          */
/* -------------------------------------------------------------------------- */

function missionLaunchContentKey(draft: MissionDraft): string {
  return [
    draft.projectId,
    draft.sessionId,
    draft.objective.trim(),
    draft.successCriteria.trim(),
    draft.constraints.trim(),
    String(draft.hardBudgetMicros),
    draft.resourceCommitment.toFixed(4),
  ].join("|");
}

/**
 * Converts a valid draft into the runtime boundary command. Returns null when
 * the draft is not launchable, so callers cannot send a half-formed command.
 */
export function toMissionLaunchCommand(draft: MissionDraft): MissionLaunchCommand | null {
  const issues = validateMissionDraft(draft);
  if (missionDraftHasErrors(issues) || draft.hardBudgetMicros === null) return null;

  return {
    commandId: `mission-launch:${draft.id}:${missionLaunchContentKey(draft)}`,
    draftId: draft.id,
    projectId: draft.projectId,
    sessionId: draft.sessionId,
    objective: draft.objective.trim(),
    successCriteria: draft.successCriteria.trim(),
    constraints: draft.constraints.trim(),
    hardBudgetMicros: draft.hardBudgetMicros,
    resourceCommitment: draft.resourceCommitment,
  };
}
