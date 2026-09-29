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
 * Home, attention and the mission surfaces share the root route and are chosen
 * by `scenario`, which keeps one in-memory runtime instance alive across the
 * switch. The ledger, settings and OCG control surfaces have their own route.
 */
const WORKSPACE_TARGETS: Record<WorkspaceView, WorkspaceTarget> = {
  chat: { href: "/", toggles: false },
  home: { href: "/?scenario=home-overview", toggles: false },
  attention: { href: "/?scenario=attention-overview", toggles: false },
  "control-center": { href: "/?scenario=profiles-models", toggles: true },
  ledger: { href: "/resource-ledger", toggles: true },
  "mission-control": { href: "/?scenario=mission-control", toggles: true },
  logs: { href: "/?scenario=logs-live", toggles: true },
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
