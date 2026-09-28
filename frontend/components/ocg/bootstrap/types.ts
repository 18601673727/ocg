/**
 * Normalized bootstrap domain for the mock runtime.
 *
 * This module is deliberately presentation-free and secret-free. It owns the
 * setup facts the shell needs before it can show the workspace: how access is
 * established, whether onboarding is pending, and the resource-first inventory
 * used by the wizard. Connections describe reachability only; no credential
 * material is ever represented here.
 */

export type BootstrapEntry = "app" | "login" | "onboarding";

export type BootstrapMode = "firstRun" | "resume" | "migrate" | "recover" | "reconfigure";

export const BOOTSTRAP_MODES: readonly BootstrapMode[] = [
  "firstRun",
  "resume",
  "migrate",
  "recover",
  "reconfigure",
];

export type BootstrapAccessState =
  | "local"
  | "authenticated"
  | "unauthenticated"
  | "session-expired"
  | "denied"
  | "auth-required";

export type BootstrapHandoffState = "unavailable" | "idle" | "pending" | "complete" | "failed";

export type BootstrapAccess = {
  state: BootstrapAccessState;
  remote: boolean;
  detail?: string;
  handoffState: BootstrapHandoffState;
};

export type OnboardingStageId =
  | "welcome"
  | "resources"
  | "connections"
  | "discovery"
  | "overview"
  | "profile"
  | "ready";

/** Canonical stage order for the seven-stage wizard. */
export const ONBOARDING_STAGES: readonly OnboardingStageId[] = [
  "welcome",
  "resources",
  "connections",
  "discovery",
  "overview",
  "profile",
  "ready",
];

export type BootstrapFailureCode =
  | "auth-required"
  | "connection-failed"
  | "discovery-failed"
  | "invalid-configuration"
  | "migration-failed"
  | "recovery-failed"
  | "unknown";

/** How the operator is expected to resolve a failure. */
export type BootstrapFailureAction =
  | "handoff"
  | "retry"
  | "resume"
  | "restore"
  | "reconfigure"
  | "continue";

export type BootstrapFailure = {
  code: BootstrapFailureCode;
  summary: string;
  /** Optional, display-safe technical context. Never secret material. */
  detail?: string;
  action: BootstrapFailureAction;
  actionLabel: string;
  retryable: boolean;
};

export type BootstrapResourceKind =
  | "workspace"
  | "project"
  | "repository"
  | "directory"
  | "endpoint"
  | "unknown";

export type BootstrapEconomicsKind = "free" | "subscription" | "metered" | "unknown";

export type BootstrapResourceEconomics = {
  kind: BootstrapEconomicsKind;
  detail?: string;
};

export type BootstrapResourceCapacity = {
  status: "known" | "unknown";
  detail?: string;
};

export type BootstrapResourceStatus = "available" | "pending" | "unavailable" | "denied" | "unknown";

export type BootstrapResource = {
  id: string;
  label: string;
  kind: BootstrapResourceKind;
  source: "local" | "remote" | "imported";
  status: BootstrapResourceStatus;
  detail?: string;
  provider?: string;
  plan?: string;
  economics: BootstrapResourceEconomics;
  capacity: BootstrapResourceCapacity;
  required: boolean;
};

export type BootstrapConnectionKind =
  | "runtime"
  | "model-provider"
  | "source-control"
  | "workspace"
  | "unknown";

export type BootstrapConnectionState =
  | "connected"
  | "pending"
  | "unconfigured"
  | "auth-required"
  | "failed"
  | "denied"
  | "unknown";

export type BootstrapConnection = {
  id: string;
  label: string;
  kind: BootstrapConnectionKind;
  state: BootstrapConnectionState;
  detail?: string;
  /** True when completing the connection requires a browser access handoff. */
  requiresHandoff: boolean;
  required: boolean;
  advanced?: boolean;
};

export type BootstrapModelStatus = "available" | "pending" | "unavailable" | "unknown";

/**
 * Explicit per-model capability support. `unknown` means the runtime did not
 * report the capability; `unsupported` means the runtime explicitly reported
 * that the model cannot do it. They are never interchangeable.
 */
export type BootstrapCapabilitySupport = "supported" | "unsupported" | "unknown";

export type BootstrapModel = {
  id: string;
  /** Display label of the owning provider. */
  provider: string;
  /** Optional stable provider id. Preferred when present for filtering. */
  providerId?: string;
  model: string;
  status: BootstrapModelStatus;
  /** Capability ids the runtime reported as available for this model. */
  capabilities: string[];
  /** Provider-declared variants (for example "standard", "deep"). */
  variants?: string[];
  /**
   * Explicit capability support keyed by capability id. A capability that is in
   * neither this map nor `capabilities` stays unknown.
   */
  capabilitySupport?: Record<string, BootstrapCapabilitySupport>;
  /** Display-safe note about how the model was reported. Never secret material. */
  provenanceNote?: string;
  /** Optional resource metadata; absent values remain unreported in the UI. */
  displayName?: string;
  contextWindow?: number;
  economics?: BootstrapResourceEconomics;
  latencyObservation?: { status: "known" | "unknown"; detail?: string };
};

export type BootstrapCapabilityStatus = "available" | "unknown";

export type BootstrapCapability = {
  id: string;
  label: string;
  status: BootstrapCapabilityStatus;
};

export type BootstrapProfileTier = "economy" | "balanced" | "quality";

/** How a profile came to exist: runtime recommendation or operator customization. */
export type BootstrapProfileSource = "recommended" | "customized";

/**
 * Route role kind. `lead` is the fixed coordination tier; `worker` covers an
 * open-ended set of future roles, so `roleId` is a plain string rather than a
 * closed union.
 */
export type BootstrapRouteRoleKind = "lead" | "worker";

/**
 * A normalized assignment of one role to a model. `modelId: null` is an
 * explicit unassigned route, never a fallback to some other model.
 */
export type BootstrapProfileRoute = {
  id: string;
  roleKind: BootstrapRouteRoleKind;
  /** Dynamic role id. Worker roles are open-ended; "lead" is the fixed lead id. */
  roleId: string;
  roleLabel: string;
  /** Lead tier. Present for lead routes, absent for worker routes. */
  tier?: BootstrapProfileTier;
  modelId: string | null;
  variant?: string | null;
  /** Model used when the primary assignment is unavailable. */
  fallbackModelId?: string | null;
  /** True when this route is presented as currently using a fallback. */
  fallback?: boolean;
  /** True when the operator overrode the profile default for this role. */
  overridden?: boolean;
  /** Display-safe warning copy. Never credential or endpoint secret material. */
  warning?: string;
  /** Unassigned required routes make a profile incomplete. Defaults to true. */
  required?: boolean;
};

export type BootstrapProfile = {
  id: string;
  label: string;
  tier: BootstrapProfileTier;
  rationale: string;
  modelIds: string[];
  recommended: boolean;
  advanced?: boolean;
  /** Runtime recommendation or operator customization. */
  source?: BootstrapProfileSource;
  /** Per-role routing used by the Control Center. */
  routes?: BootstrapProfileRoute[];
};

/**
 * Provider reachability/auth domain. This models whether a provider can be
 * reached and whether it needs an auth step. Credential contents are never
 * represented anywhere in this domain.
 */
export type BootstrapProviderState =
  | "connected"
  | "auth-required"
  | "degraded"
  | "unavailable"
  | "unknown";

export type BootstrapProvider = {
  id: string;
  label: string;
  state: BootstrapProviderState;
  detail?: string;
  /** True when using this provider requires a browser auth handoff first. */
  authRequired: boolean;
  /** Display-safe endpoint identity. Never a URL that embeds credentials. */
  endpointLabel?: string;
  endpointType?: string;
  plan?: string;
  economics?: BootstrapResourceEconomics;
  capacity?: BootstrapResourceCapacity;
  discoveredModelCount?: number;
  capabilities?: string[];
  lastCheckedAt?: string;
  warnings?: string[];
};

export type BootstrapOnboarding = {
  mode: BootstrapMode;
  stage: OnboardingStageId;
  completedStages: OnboardingStageId[];
  canResume: boolean;
  failure?: BootstrapFailure;
};

export type BootstrapState = {
  access: BootstrapAccess;
  onboarding: BootstrapOnboarding | null;
  resources: BootstrapResource[];
  connections: BootstrapConnection[];
  models: BootstrapModel[];
  capabilities: BootstrapCapability[];
  profiles: BootstrapProfile[];
  /** Providers reported for the workspace. Absent for fixtures predating the Control Center. */
  providers?: BootstrapProvider[];
  /** Active profile selection. Absent falls back to the recommended profile. */
  activeProfileId?: string;
  ready: boolean;
};

export type StageGate = {
  stage: OnboardingStageId;
  canAdvance: boolean;
  /** Stable codes explaining why advancement is blocked. */
  blockers: string[];
};
