/**
 * Pure Workspace view resolution for the root route.
 *
 * An explicit `view` search param wins, which is how shell navigation moves
 * between surfaces without renaming the scenario behind it. When no explicit
 * view is present, the scenario-derived default is used.
 */

import type { ScenarioId } from "../runtime/runtime-types";

export type WorkspaceView =
  | "chat"
  | "home"
  | "attention"
  | "ledger"
  | "control-center"
  | "job-execution"
  | "logs"
  | "settings"
  | "canonical";

export const WORKSPACE_VIEWS: readonly WorkspaceView[] = [
  "chat",
  "home",
  "attention",
  "ledger",
  "control-center",
  "job-execution",
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
    case "job-execution":
      return "job-execution";
    case "logs-live":
      return "logs";
    default:
      return "chat";
  }
}

export function resolveWorkspaceView(scenario: ScenarioId, requestedView: unknown): WorkspaceView {
  return isWorkspaceView(requestedView) ? requestedView : deriveScenarioWorkspaceView(scenario);
}

/** How a workspace is opened from the shell's navigation controls. */
export type WorkspaceTarget = {
  /** Address the view is opened at, relative to the app root. */
  readonly href: string;
  /**
   * Whether selecting a view that is already on screen closes back to the chat
   * root. Surfaces on their own route and the scenario surfaces do; the chat,
   * home and attention views are states you are either in or not.
   */
  readonly toggles: boolean;
};

/**
 * Where each workspace lives.
 *
 * Every root-route view is opened with `?view=` rather than `?scenario=`, so
 * navigating inside the workspace keeps the active scenario — and therefore the
 * one in-memory runtime instance and its authority — untouched. Naming a
 * scenario selects a fixture, so it is left to whoever opened the URL to do it
 * deliberately. The ledger, settings and OCG control surfaces have their own
 * route.
 */
const WORKSPACE_TARGETS: Record<WorkspaceView, WorkspaceTarget> = {
  chat: { href: "/", toggles: false },
  home: { href: "/?view=home", toggles: false },
  attention: { href: "/?view=attention", toggles: false },
  "control-center": { href: "/?view=control-center", toggles: true },
  ledger: { href: "/resource-ledger", toggles: true },
  "job-execution": { href: "/?view=job-execution", toggles: true },
  logs: { href: "/?view=logs", toggles: true },
  settings: { href: "/settings", toggles: true },
  canonical: { href: "/canonical", toggles: false },
};

/**
 * The address a navigation resolves to, or `null` when it would not change
 * anything on screen. Callers push it with the active Project parameter
 * attached.
 */
export function workspaceViewHref(current: WorkspaceView, target: WorkspaceView): string | null {
  const destination = WORKSPACE_TARGETS[target];
  if (current === target) return destination.toggles ? WORKSPACE_TARGETS.chat.href : null;
  return destination.href;
}
