import type {
  BootstrapAccessState,
  BootstrapMode,
  BootstrapResourceStatus,
  BootstrapConnectionState,
  BootstrapModelStatus,
  OnboardingStageId,
} from "./types";

/** Presentation copy only. The domain and selectors never depend on this. */

export const ONBOARDING_STAGE_LABEL: Record<OnboardingStageId, string> = {
  welcome: "Welcome",
  resources: "Add Resources",
  connections: "Credentials & Connections",
  discovery: "Discover Models & Capabilities",
  overview: "Resource Overview",
  profile: "Recommended Profile",
  ready: "Ready",
};

export const ONBOARDING_STAGE_SUMMARY: Record<OnboardingStageId, string> = {
  welcome: "Confirm what this workspace already knows before changing anything.",
  resources: "Choose the normalized resources this workspace should use.",
  connections: "Review reachability for each service. No credentials are entered here.",
  discovery: "Look at which models and capabilities the runtime actually reports.",
  overview: "Review the normalized inventory before choosing defaults.",
  profile: "Pick the starting profile. The recommendation is a suggestion, not a lock.",
  ready: "Confirm the setup. The workspace opens in the app shell afterward.",
};

export const BOOTSTRAP_MODE_LABEL: Record<BootstrapMode, string> = {
  firstRun: "First-run setup",
  resume: "Resume setup",
  migrate: "Migrate existing setup",
  recover: "Recover setup",
  reconfigure: "Reconfigure setup",
};

export const BOOTSTRAP_MODE_DESCRIPTION: Record<BootstrapMode, string> = {
  firstRun: "Set up this workspace for the first time. Nothing is written until you confirm.",
  resume: "Continue from the saved point instead of starting over.",
  migrate: "Bring settings from the previous setup into the normalized workspace.",
  recover: "Restore a setup that did not finish cleanly.",
  reconfigure: "Review the current setup and adjust the resources it uses.",
};

type AccessCopy = {
  title: string;
  detail: string;
  button: string;
};

export const ACCESS_STATE_COPY: Record<BootstrapAccessState, AccessCopy> = {
  local: {
    title: "Local workspace",
    detail: "This workspace runs locally and does not require an access handoff.",
    button: "Continue",
  },
  authenticated: {
    title: "Workspace access granted",
    detail: "The access handoff completed. Setup can continue.",
    button: "Continue",
  },
  unauthenticated: {
    title: "Workspace access required",
    detail: "This remote workspace requires a browser access handoff before setup can continue.",
    button: "Start access handoff",
  },
  "session-expired": {
    title: "Access session expired",
    detail: "The access session is no longer valid. Refresh the handoff to continue.",
    button: "Refresh access handoff",
  },
  denied: {
    title: "Access denied",
    detail: "This workspace is not available to the current access session.",
    button: "Retry access check",
  },
  "auth-required": {
    title: "Access required to continue",
    detail: "Setup is paused until the browser access handoff completes.",
    button: "Start access handoff",
  },
};

export function titleCaseStatus(value: string): string {
  return value.replace(/-/g, " ");
}

export const RESOURCE_STATUS_TONE: Record<BootstrapResourceStatus, string> = {
  available: "text-emerald-600 dark:text-emerald-400",
  pending: "text-amber-600 dark:text-amber-400",
  unavailable: "text-red-600 dark:text-red-400",
  denied: "text-red-600 dark:text-red-400",
  unknown: "text-muted-foreground",
};

export const CONNECTION_STATE_TONE: Record<BootstrapConnectionState, string> = {
  connected: "text-emerald-600 dark:text-emerald-400",
  pending: "text-amber-600 dark:text-amber-400",
  unconfigured: "text-muted-foreground",
  "auth-required": "text-amber-600 dark:text-amber-400",
  failed: "text-red-600 dark:text-red-400",
  denied: "text-red-600 dark:text-red-400",
  unknown: "text-muted-foreground",
};

export const MODEL_STATUS_TONE: Record<BootstrapModelStatus, string> = {
  available: "text-emerald-600 dark:text-emerald-400",
  pending: "text-amber-600 dark:text-amber-400",
  unavailable: "text-red-600 dark:text-red-400",
  unknown: "text-muted-foreground",
};
