/**
 * Pure Workspace view resolution for the root route.
 *
 * An explicit `view` search param wins so shell navigation can preserve the
 * active runtime scenario (and therefore the in-memory mock instance). When no
 * explicit view is present, the existing scenario-derived default is used.
 */

import type { ScenarioId } from "../runtime/runtime-types";

export type WorkspaceView =
  | "chat"
  | "home"
  | "attention"
  | "ledger"
  | "control-center"
  | "mission-control"
  | "logs"
  | "settings"
  | "canonical";

export const WORKSPACE_VIEWS: readonly WorkspaceView[] = [
  "chat",
  "home",
  "attention",
  "ledger",
  "control-center",
  "mission-control",
  "logs",
  "settings",
  "canonical",
];

export function isWorkspaceView(value: unknown): value is WorkspaceView {
  return typeof value === "string" && (WORKSPACE_VIEWS as readonly string[]).includes(value);
}

/** Existing scenario-derived defaults, unchanged. */
export function deriveScenarioWorkspaceView(scenario: ScenarioId): WorkspaceView {
  switch (scenario) {
    case "home-overview":
    case "home-calm":
      return "home";
    case "attention-overview":
    case "attention-calm":
      return "attention";
    case "profiles-models":
      return "control-center";
    case "resource-ledger":
      return "ledger";
    case "mission-control":
      return "mission-control";
    case "logs-live":
      return "logs";
    default:
      return "chat";
  }
}

export function resolveWorkspaceView(scenario: ScenarioId, requestedView: unknown): WorkspaceView {
  return isWorkspaceView(requestedView) ? requestedView : deriveScenarioWorkspaceView(scenario);
}
