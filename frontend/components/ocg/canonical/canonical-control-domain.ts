/**
 * Pure view model for the backend-backed OCG control surface.
 *
 * The PWA is a projection/control surface, never an execution authority. This
 * module makes that boundary explicit in types: the only mutations are
 * Project registration, global/project defaults, and pre-run Mission
 * configuration, and each one carries a stable command identity. A dispatched
 * Run's frozen executor contract is read-only here by construction.
 */

import type {
  CanonicalConfigurationView,
  CanonicalGlobalConfiguration,
  CanonicalProjectRecord,
} from "../runtime/canonical-client";
import {
  canonicalCommandId,
  isCanonicalRejection,
  type CanonicalRejection,
} from "../runtime/canonical-client";
import type { CanonicalState } from "../runtime/canonical-store";

export type Acknowledgement = { ok: true; commandId: string } | CanonicalRejection;

export const PROFILES = ["fast", "careful", "balanced"] as const;
export const ROUTING_MODES = ["direct", "balanced", "review"] as const;
export type ProfileName = (typeof PROFILES)[number];
export type RoutingMode = (typeof ROUTING_MODES)[number];

export type ConfigurationDraft = {
  provider: string;
  model: string;
  profile: ProfileName;
  routing: RoutingMode;
  hardBudget: number;
};

export type DraftIssueCode =
  | "provider-required"
  | "model-required"
  | "budget-invalid"
  | "budget-below-minimum"
  | "profile-invalid"
  | "routing-invalid"
  | "root-required"
  | "mission-required"
  | "mission-dispatched";

export type DraftIssue = { code: DraftIssueCode; message: string; field: string };

export const MIN_HARD_BUDGET = 1;

export const DEFAULT_CONFIGURATION_DRAFT: ConfigurationDraft = {
  provider: "",
  model: "",
  profile: "careful",
  routing: "balanced",
  hardBudget: 25,
};

export function isProfileName(value: unknown): value is ProfileName {
  return typeof value === "string" && (PROFILES as readonly string[]).includes(value);
}

export function isRoutingMode(value: unknown): value is RoutingMode {
  return typeof value === "string" && (ROUTING_MODES as readonly string[]).includes(value);
}

/** Project import validates a real repository root, never a display name. */
export function validateImportRoot(root: string): DraftIssue[] {
  const trimmed = root.trim();
  if (trimmed.length === 0) {
    return [{ code: "root-required", field: "root", message: "Enter an existing repository path." }];
  }
  if (!trimmed.startsWith("/") && !trimmed.startsWith(".")) {
    return [
      {
        code: "root-required",
        field: "root",
        message: "Use an absolute path or an explicit ./relative path.",
      },
    ];
  }
  return [];
}

export function validateConfigurationDraft(draft: ConfigurationDraft): DraftIssue[] {
  const issues: DraftIssue[] = [];
  if (draft.provider.trim().length === 0) {
    issues.push({ code: "provider-required", field: "provider", message: "Provider is required." });
  }
  if (draft.model.trim().length === 0) {
    issues.push({ code: "model-required", field: "model", message: "Model is required." });
  }
  if (!isProfileName(draft.profile)) {
    issues.push({ code: "profile-invalid", field: "profile", message: "Unknown execution profile." });
  }
  if (!isRoutingMode(draft.routing)) {
    issues.push({ code: "routing-invalid", field: "routing", message: "Unknown routing mode." });
  }
  if (!Number.isFinite(draft.hardBudget)) {
    issues.push({ code: "budget-invalid", field: "hardBudget", message: "Hard budget must be a number." });
  } else if (draft.hardBudget < MIN_HARD_BUDGET) {
    issues.push({
      code: "budget-below-minimum",
      field: "hardBudget",
      message: `Hard budget must be at least ${MIN_HARD_BUDGET}.`,
    });
  }
  return issues;
}

export function toGlobalConfiguration(draft: ConfigurationDraft): CanonicalGlobalConfiguration {
  return {
    provider: draft.provider.trim(),
    model: draft.model.trim(),
    profile: draft.profile,
    routing: draft.routing,
    // The runtime is backend-owned and not part of the PWA draft. It is stated
    // explicitly as null rather than left out, so the draft this builds is the
    // same struct the backend returns rather than a partial guess at it.
    runtime: null,
    resource_budget: { hard_limit: draft.hardBudget, unit: "USD" },
  };
}

export function draftFromConfiguration(view: CanonicalConfigurationView | null): ConfigurationDraft {
  if (!view) return { ...DEFAULT_CONFIGURATION_DRAFT };
  const budget = view.global.resource_budget;
  return {
    provider: view.global.provider ?? "",
    model: view.global.model ?? "",
    profile: isProfileName(view.global.profile) ? view.global.profile : DEFAULT_CONFIGURATION_DRAFT.profile,
    routing: isRoutingMode(view.global.routing) ? view.global.routing : DEFAULT_CONFIGURATION_DRAFT.routing,
    // The decoder has already checked the budget, so a stored hard limit of
    // zero means "none recorded" and falls back to the draft default rather
    // than presenting an unusable zero to the operator.
    hardBudget: budget && budget.hard_limit > 0 ? budget.hard_limit : DEFAULT_CONFIGURATION_DRAFT.hardBudget,
  };
}

/**
 * Pre-run Job configuration is only editable while the Job has no
 * dispatched Attempt. Once one exists the surface reports it as frozen rather
 * than offering a control that would silently do nothing.
 */
export function preRunConfigurationState(state: CanonicalState): {
  jobId: string | null;
  editable: boolean;
  reason: string | null;
} {
  const jobId = state.jobId;
  if (jobId === null) {
    return { jobId: null, editable: false, reason: "No canonical Job is selected." };
  }
  if (state.projection === null) {
    return { jobId, editable: false, reason: "Canonical state has not been loaded." };
  }
  if (state.projection.attempts.length > 0) {
    return {
      jobId,
      editable: false,
      reason:
        "An Attempt is already dispatched. Its frozen executor/model/role contract cannot be edited; changes apply to future dispatches or a replacement Job.",
    };
  }
  return { jobId, editable: true, reason: null };
}

export function globalConfigurationCommandId(revision: number): string {
  return canonicalCommandId("global-config", String(revision));
}

export function projectDefaultsCommandId(projectId: string, revision: number): string {
  return canonicalCommandId("project-defaults", `${projectId}-${revision}`);
}

export function projectImportCommandId(root: string): string {
  // The command identity is derived from the boundary, not from a label, so a
  // repeated import of the same repository reuses the same identity.
  return canonicalCommandId("project-import", root.replace(/[^A-Za-z0-9]+/g, "-").slice(-48));
}

/** Human-readable acknowledgement for one command result. */
export function describeAcknowledgement(result: Acknowledgement | null): string {
  if (result === null) return "";
  if (isCanonicalRejection(result)) return `${result.commandId} rejected: ${result.message}`;
  return `${result.commandId} accepted`;
}

export function selectRegisteredProjects(
  projects: readonly CanonicalProjectRecord[],
  activeProjectId: string | null,
): Array<CanonicalProjectRecord & { active: boolean }> {
  return projects.map((project) => ({ ...project, active: project.project_id === activeProjectId }));
}
