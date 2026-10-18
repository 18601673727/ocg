import {
  BOOTSTRAP_MODES,
  ONBOARDING_STAGES,
  type BootstrapAccess,
  type BootstrapEntry,
  type BootstrapFailure,
  type BootstrapMode,
  type BootstrapModel,
  type BootstrapOnboarding,
  type BootstrapProfile,
  type BootstrapResource,
  type BootstrapState,
  type OnboardingStageId,
  type StageGate,
} from "./types";

const ACCESS_REQUIRED_STATES: readonly BootstrapAccess["state"][] = [
  "unauthenticated",
  "session-expired",
  "denied",
  "auth-required",
];

export function isOnboardingStageId(value: unknown): value is OnboardingStageId {
  return typeof value === "string" && (ONBOARDING_STAGES as readonly string[]).includes(value);
}

export function normalizeOnboardingMode(value: unknown): BootstrapMode {
  return typeof value === "string" && (BOOTSTRAP_MODES as readonly string[]).includes(value)
    ? (value as BootstrapMode)
    : "firstRun";
}

/** Remote access is required before the shell or onboarding can be trusted. */
export function isAccessRequired(access: BootstrapAccess | undefined): boolean {
  if (!access) return true;
  return ACCESS_REQUIRED_STATES.includes(access.state);
}

/** The access session can own setup work: local, or proven authenticated. */
export function isAccessUsable(access: BootstrapAccess | undefined): boolean {
  if (!access) return false;
  return access.state === "local" || access.state === "authenticated";
}

/** Onboarding is complete when the runtime says ready or there is nothing to set up. */
export function isOnboardingComplete(state: BootstrapState): boolean {
  if (state.ready) return true;
  if (!state.onboarding) return true;
  return state.onboarding.completedStages.includes("ready");
}

export function selectBootstrapEntry(state: BootstrapState): BootstrapEntry {
  if (isAccessRequired(state.access)) return "login";
  if (state.onboarding && !isOnboardingComplete(state)) return "onboarding";
  return "app";
}

/** Unknown or missing stage values degrade to the first stage instead of guessing. */
export function selectActiveOnboardingStage(state: BootstrapState): OnboardingStageId {
  const stage = state.onboarding?.stage as unknown;
  return isOnboardingStageId(stage) ? stage : "welcome";
}

export function selectStageIndex(stage: OnboardingStageId): number {
  const index = ONBOARDING_STAGES.indexOf(stage);
  return index < 0 ? 0 : index;
}

export function selectNextStage(stage: OnboardingStageId): OnboardingStageId | null {
  return ONBOARDING_STAGES[selectStageIndex(stage) + 1] ?? null;
}

export function selectPreviousStage(stage: OnboardingStageId): OnboardingStageId | null {
  const index = selectStageIndex(stage);
  return index > 0 ? ONBOARDING_STAGES[index - 1] : null;
}

export function selectRecommendedProfile(state: BootstrapState): BootstrapProfile | null {
  return state.profiles.find((profile) => profile.recommended) ?? null;
}

export function selectAvailableModels(state: BootstrapState): BootstrapModel[] {
  return state.models.filter((model) => model.status === "available");
}

export function selectAvailableResources(state: BootstrapState): BootstrapResource[] {
  return state.resources.filter((resource) => resource.status === "available");
}

export function selectBlockingFailure(state: BootstrapState): BootstrapFailure | null {
  return state.onboarding?.failure ?? null;
}

/**
 * A stage can advance only when its own prerequisites are met and no failure is
 * pending. Unknown statuses are never treated as satisfied.
 */
export function selectStageGate(
  state: BootstrapState,
  stage: OnboardingStageId = selectActiveOnboardingStage(state),
): StageGate {
  const blockers: string[] = [];
  const failure = selectBlockingFailure(state);
  if (failure) blockers.push(failure.code);

  const availableResources = selectAvailableResources(state);
  const availableModels = selectAvailableModels(state);
  const connected = state.connections.filter((connection) => connection.state === "connected");

  switch (stage) {
    case "welcome":
      break;
    case "resources":
      if (availableResources.length === 0) blockers.push("no-available-resource");
      break;
    case "connections":
      if (connected.length === 0) blockers.push("no-connected-service");
      if (state.connections.some((connection) => connection.required && connection.state !== "connected")) {
        blockers.push("required-connection-incomplete");
      }
      break;
    case "discovery":
      if (availableModels.length === 0) blockers.push("no-discovered-model");
      break;
    case "overview":
      if (availableResources.length === 0) blockers.push("no-available-resource");
      if (availableModels.length === 0) blockers.push("no-discovered-model");
      break;
    case "profile":
      if (!selectRecommendedProfile(state)) blockers.push("no-recommended-profile");
      break;
    case "ready":
      break;
  }

  return { stage, canAdvance: blockers.length === 0, blockers };
}

export type OnboardingProgress = {
  current: number;
  total: number;
  completed: number;
  percent: number;
};

export function selectOnboardingProgress(state: BootstrapState): OnboardingProgress {
  const total = ONBOARDING_STAGES.length;
  const completed = ONBOARDING_STAGES.filter((stage) =>
    state.onboarding?.completedStages.includes(stage),
  ).length;
  const current = selectStageIndex(selectActiveOnboardingStage(state)) + 1;
  return { current, total, completed, percent: Math.round((completed / total) * 100) };
}

/**
 * Resume is offered only when a usable access session has saved, unfinished
 * progress. Access-gated and untouched setups are not "resumable".
 */
export function canResumeOnboarding(state: BootstrapState): boolean {
  if (!state.onboarding) return false;
  if (isOnboardingComplete(state)) return false;
  if (!isAccessUsable(state.access)) return false;
  return state.onboarding.completedStages.length > 0;
}

function withStageCompleted(onboarding: BootstrapOnboarding, stage: OnboardingStageId): OnboardingStageId[] {
  return onboarding.completedStages.includes(stage)
    ? onboarding.completedStages
    : [...onboarding.completedStages, stage];
}

/**
 * Pure progression reducer. Marks the active stage complete and returns the
 * next stage, or marks setup ready once the final stage is confirmed.
 */
export function advanceOnboarding(state: BootstrapState): BootstrapState {
  const onboarding = state.onboarding;
  if (!onboarding) return state;
  const stage = selectActiveOnboardingStage(state);
  if (!selectStageGate(state, stage).canAdvance) return state;

  const completedStages = withStageCompleted(onboarding, stage);
  const next = selectNextStage(stage);
  if (!next) return { ...state, ready: true, onboarding: { ...onboarding, completedStages } };
  return { ...state, onboarding: { ...onboarding, stage: next, completedStages } };
}

/**
 * Deterministic mock recovery for an actionable failure: clears the failure and
 * promotes connections that were waiting or had failed. Unknown and
 * unavailable facts stay unknown.
 */
export function resolveBootstrapRetry(state: BootstrapState): BootstrapState {
  return {
    ...state,
    connections: state.connections.map((connection) =>
      connection.state === "failed" || connection.state === "pending"
        ? { ...connection, state: "connected" as const }
        : connection,
    ),
    onboarding: state.onboarding ? { ...state.onboarding, failure: undefined } : state.onboarding,
  };
}

/** The access session that results from a successful handoff. */
export function resolveAccessHandoff(access: BootstrapAccess): BootstrapAccess {
  // A denied identity is an authorization decision, not an authentication
  // failure. Retrying the same mock handoff must not turn it into a session or
  // create a login loop.
  if (!isAccessRequired(access) || access.state === "denied") return access;
  return {
    ...access,
    state: "authenticated",
    handoffState: "complete",
    detail: "Access handoff granted (mock).",
  };
}
