/**
 * Normalized Control Center domain and pure selectors.
 *
 * This module is presentation-free and secret-free. It reads the shared
 * `BootstrapState` (providers, models, profiles, routes) and derives the facts
 * the Profiles / Providers / Models surfaces render. It deliberately introduces
 * no provider or model catalogue: every fact comes from the normalized state.
 *
 * Two distinctions are load-bearing and must never collapse:
 * - `unknown` means the runtime did not report a fact; it is not offline or
 *   unavailable.
 * - capability `unknown` (not reported) is not capability `unsupported`
 *   (explicitly reported as impossible).
 */

import {
  type BootstrapCapabilitySupport,
  type BootstrapModel,
  type BootstrapModelStatus,
  type BootstrapProfile,
  type BootstrapProfileRoute,
  type BootstrapProvider,
  type BootstrapProviderState,
  type BootstrapState,
} from "../bootstrap/types";

export type ControlCenterView = "profiles" | "providers" | "models";

export const CONTROL_CENTER_VIEWS: readonly ControlCenterView[] = [
  "profiles",
  "providers",
  "models",
];

/** Unknown or missing view values degrade to the Profiles surface. */
export function resolveControlCenterView(value: unknown): ControlCenterView {
  return typeof value === "string" && (CONTROL_CENTER_VIEWS as readonly string[]).includes(value)
    ? (value as ControlCenterView)
    : "profiles";
}

export type ProfileHealth = "healthy" | "degraded" | "unavailable" | "incomplete" | "unknown";

export const PROFILE_HEALTHS: readonly ProfileHealth[] = [
  "healthy",
  "degraded",
  "unavailable",
  "incomplete",
  "unknown",
];

export type RouteStatus =
  | "ready"
  | "degraded"
  | "fallback"
  | "unavailable"
  | "pending"
  | "unknown"
  | "unassigned"
  | "auth-required";

export const ROUTE_STATUSES: readonly RouteStatus[] = [
  "ready",
  "degraded",
  "fallback",
  "unavailable",
  "pending",
  "unknown",
  "unassigned",
  "auth-required",
];

export type ResolvedRoute = {
  route: BootstrapProfileRoute;
  model: BootstrapModel | null;
  provider: BootstrapProvider | null;
  status: RouteStatus;
};

/** A provider is unusable only when the runtime explicitly reported it so. */
export function isUnavailableProvider(provider: BootstrapProvider | null | undefined): boolean {
  return provider?.state === "unavailable";
}

/** An unknown provider was not reported; it is not offline. */
export function isUnknownProvider(provider: BootstrapProvider | null | undefined): boolean {
  return provider === null || provider === undefined || provider.state === "unknown";
}

export function isUnavailableModel(model: BootstrapModel | null | undefined): boolean {
  return model?.status === "unavailable";
}

/** An unknown model was not reported; it is not unavailable. */
export function isUnknownModel(model: BootstrapModel | null | undefined): boolean {
  return model === null || model === undefined || model.status === "unknown";
}

/** Provider-distinct model key so the same model name on two providers stays distinct. */
export function controlModelKey(provider: string, model: string): string {
  return `${provider}\u0000${model}`;
}

export function selectProviders(state: BootstrapState): BootstrapProvider[] {
  return state.providers ?? [];
}

export function selectProvider(state: BootstrapState, providerId: string | null | undefined): BootstrapProvider | null {
  if (!providerId) return null;
  return selectProviders(state).find((provider) => provider.id === providerId) ?? null;
}

export function selectModel(state: BootstrapState, modelId: string | null | undefined): BootstrapModel | null {
  if (!modelId) return null;
  return state.models.find((model) => model.id === modelId) ?? null;
}

/** Resolves the provider entity for a model by stable id, then by display label. */
export function selectModelProvider(state: BootstrapState, model: BootstrapModel | null | undefined): BootstrapProvider | null {
  if (!model) return null;
  const providers = selectProviders(state);
  return (
    providers.find((provider) => provider.id === model.providerId) ??
    providers.find((provider) => provider.label === model.provider) ??
    null
  );
}

export function selectProviderModels(state: BootstrapState, provider: BootstrapProvider): BootstrapModel[] {
  return state.models.filter((model) =>
    model.providerId === provider.id || model.provider === provider.label,
  );
}

export type RouteAssignment = {
  profileId: string;
  profileLabel: string;
  roleId: string;
  roleLabel: string;
  overridden: boolean;
};

/** Roles are derived from profile routes, keeping provider/model inventory distinct from routing policy. */
export function selectModelAssignments(state: BootstrapState, modelId: string): RouteAssignment[] {
  return state.profiles.flatMap((profile) =>
    (profile.routes ?? [])
      .filter((route) => route.modelId === modelId)
      .map((route) => ({
        profileId: profile.id,
        profileLabel: profile.label,
        roleId: route.roleId,
        roleLabel: route.roleLabel,
        overridden: route.overridden === true,
      })),
  );
}

export function selectProviderAssignments(state: BootstrapState, provider: BootstrapProvider): RouteAssignment[] {
  const modelIds = new Set(selectProviderModels(state, provider).map((model) => model.id));
  return state.profiles.flatMap((profile) =>
    (profile.routes ?? [])
      .filter((route) => route.modelId !== null && modelIds.has(route.modelId))
      .map((route) => ({
        profileId: profile.id,
        profileLabel: profile.label,
        roleId: route.roleId,
        roleLabel: route.roleLabel,
        overridden: route.overridden === true,
      })),
  );
}

export function selectModelStatusCounts(state: BootstrapState): Record<BootstrapModelStatus, number> {
  const counts: Record<BootstrapModelStatus, number> = {
    available: 0,
    pending: 0,
    unavailable: 0,
    unknown: 0,
  };
  for (const model of state.models) counts[model.status] += 1;
  return counts;
}

export function selectProviderStateCounts(state: BootstrapState): Record<BootstrapProviderState, number> {
  const counts: Record<BootstrapProviderState, number> = {
    connected: 0,
    "auth-required": 0,
    degraded: 0,
    unavailable: 0,
    unknown: 0,
  };
  for (const provider of selectProviders(state)) counts[provider.state] += 1;
  return counts;
}

/**
 * The active profile. An explicit `activeProfileId` wins; otherwise the
 * recommended profile; otherwise the first profile; otherwise none.
 */
export function selectActiveProfile(state: BootstrapState): BootstrapProfile | null {
  if (state.activeProfileId) {
    const active = state.profiles.find((profile) => profile.id === state.activeProfileId);
    if (active) return active;
  }
  return state.profiles.find((profile) => profile.recommended) ?? state.profiles[0] ?? null;
}

export function selectProfileById(state: BootstrapState, profileId: string | null | undefined): BootstrapProfile | null {
  if (!profileId) return null;
  return state.profiles.find((profile) => profile.id === profileId) ?? null;
}

/**
 * Whether a profile is a runtime recommendation or an operator customization.
 * Absent source values are derived from the recommended flag so older fixtures
 * stay meaningful without inventing new metadata.
 */
export function selectProfileSource(profile: BootstrapProfile): "recommended" | "customized" {
  if (profile.source) return profile.source;
  return profile.recommended ? "recommended" : "customized";
}

export function selectRouteStatus(state: BootstrapState, route: BootstrapProfileRoute): RouteStatus {
  if (route.modelId === null) return "unassigned";
  const model = selectModel(state, route.modelId);
  if (!model) return "unknown";
  const provider = selectModelProvider(state, model);

  if (isUnavailableModel(model) || isUnavailableProvider(provider)) {
    return route.fallback ? "fallback" : "unavailable";
  }
  if (provider?.state === "auth-required") return "auth-required";
  if (model.status === "pending") return "pending";
  if (model.status === "unknown" || !provider || provider.state === "unknown") return "unknown";
  if (route.fallback) return "fallback";
  if (provider.state === "degraded") return "degraded";
  return "ready";
}

export function selectProfileRoutes(state: BootstrapState, profile: BootstrapProfile | null): ResolvedRoute[] {
  if (!profile?.routes) return [];
  return profile.routes.map((route) => {
    const model = selectModel(state, route.modelId);
    return {
      route,
      model,
      provider: selectModelProvider(state, model),
      status: selectRouteStatus(state, route),
    };
  });
}

export function selectActiveProfileRoutes(state: BootstrapState): ResolvedRoute[] {
  return selectProfileRoutes(state, selectActiveProfile(state));
}

export function selectLeadRoutes(routes: readonly ResolvedRoute[]): ResolvedRoute[] {
  return routes.filter((resolved) => resolved.route.roleKind === "lead");
}

/** Worker roles are open-ended, so this returns every worker route in order. */
export function selectWorkerRoutes(routes: readonly ResolvedRoute[]): ResolvedRoute[] {
  return routes.filter((resolved) => resolved.route.roleKind === "worker");
}

export type WorkerRoleDescriptor = { roleId: string; roleLabel: string };

/** Unique dynamic worker roles, preserving first-seen order. Never a closed list. */
export function selectWorkerRoles(routes: readonly ResolvedRoute[]): WorkerRoleDescriptor[] {
  const seen = new Map<string, string>();
  for (const resolved of selectWorkerRoutes(routes)) {
    if (!seen.has(resolved.route.roleId)) seen.set(resolved.route.roleId, resolved.route.roleLabel);
  }
  return [...seen.entries()].map(([roleId, roleLabel]) => ({ roleId, roleLabel }));
}

export function selectRoutesForRole(routes: readonly ResolvedRoute[], roleId: string): ResolvedRoute[] {
  return routes.filter((resolved) => resolved.route.roleId === roleId);
}

/**
 * Profile health. Precedence: incomplete (unassigned required route) >
 * unavailable > degraded (fallback/degraded/auth-required/pending) > unknown >
 * healthy. Unknown is never folded into unavailable.
 */
export function selectProfileHealth(state: BootstrapState, profile: BootstrapProfile | null): ProfileHealth {
  if (!profile) return "unknown";
  const routes = selectProfileRoutes(state, profile);
  if (routes.length === 0) return "unknown";

  if (routes.some((resolved) => resolved.route.required !== false && resolved.route.modelId === null)) {
    return "incomplete";
  }

  const statuses = routes.map((resolved) => resolved.status);
  if (statuses.includes("unavailable")) return "unavailable";
  if (statuses.some((status) => status === "fallback" || status === "degraded" || status === "auth-required" || status === "pending")) {
    return "degraded";
  }
  if (statuses.some((status) => status === "unknown" || status === "unassigned")) return "unknown";
  if (statuses.every((status) => status === "ready")) return "healthy";
  return "unknown";
}

export function selectRouteWarnings(state: BootstrapState, resolved: ResolvedRoute): string[] {
  const { route } = resolved;
  const warnings: string[] = [];
  if (route.warning) warnings.push(route.warning);

  switch (resolved.status) {
    case "unassigned":
      warnings.push(`${route.roleLabel} has no model assigned.`);
      break;
    case "unavailable":
      warnings.push(`${route.roleLabel} points at a model or provider reported as unavailable.`);
      break;
    case "fallback":
      warnings.push(`${route.roleLabel} is presented as using its fallback model.`);
      break;
    case "degraded":
      warnings.push(`${route.roleLabel} provider is degraded.`);
      break;
    case "auth-required":
      warnings.push(`${route.roleLabel} provider requires an authentication step.`);
      break;
    case "pending":
      warnings.push(`${route.roleLabel} assignment is still pending.`);
      break;
    case "unknown":
      warnings.push(`${route.roleLabel} status is unknown; it is not treated as unavailable.`);
      break;
    case "ready":
      break;
  }

  if (route.overridden) warnings.push(`${route.roleLabel} was overridden from the profile default.`);
  return [...new Set(warnings)];
}

export function selectProfileWarnings(state: BootstrapState, profile: BootstrapProfile | null): string[] {
  const routes = selectProfileRoutes(state, profile);
  return routes.flatMap((resolved) => selectRouteWarnings(state, resolved));
}

/** Explicit capability support: unsupported and unknown are distinct. */
export function selectModelCapabilitySupport(
  model: BootstrapModel | null | undefined,
  capabilityId: string,
): BootstrapCapabilitySupport {
  if (!model) return "unknown";
  const explicit = model.capabilitySupport?.[capabilityId];
  if (explicit) return explicit;
  if (model.capabilities?.includes(capabilityId)) return "supported";
  return "unknown";
}

export type ModelCapabilityEntry = { id: string; support: BootstrapCapabilitySupport };

/** Every capability the model declares or explicitly maps, in stable order. */
export function selectModelCapabilities(model: BootstrapModel | null | undefined): ModelCapabilityEntry[] {
  if (!model) return [];
  const ids = new Set<string>([...(model.capabilities ?? []), ...Object.keys(model.capabilitySupport ?? {})]);
  return [...ids].map((id) => ({ id, support: selectModelCapabilitySupport(model, id) }));
}

export function selectModelVariants(model: BootstrapModel | null | undefined): string[] {
  return model?.variants ? [...model.variants] : [];
}

export function filterProviders(
  providers: readonly BootstrapProvider[],
  query: string,
): BootstrapProvider[] {
  const needle = query.trim().toLowerCase();
  if (!needle) return [...providers];
  return providers.filter((provider) =>
    [provider.label, provider.id, provider.detail ?? "", provider.endpointLabel ?? ""]
      .some((value) => value.toLowerCase().includes(needle)),
  );
}

export function selectFilteredProviders(state: BootstrapState, query: string): BootstrapProvider[] {
  return filterProviders(selectProviders(state), query);
}

export type ModelFilter = {
  query?: string;
  /** Matches a provider id or display label. */
  providerId?: string | null;
  status?: BootstrapModelStatus | "all";
  assignment?: "assigned" | "unassigned" | "all";
  capability?: string | null;
  assignedModelIds?: ReadonlySet<string>;
};

export function filterModels(
  models: readonly BootstrapModel[],
  filter: ModelFilter = {},
): BootstrapModel[] {
  const needle = (filter.query ?? "").trim().toLowerCase();
  return models.filter((model) => {
    if (filter.providerId && model.providerId !== filter.providerId && model.provider !== filter.providerId) {
      return false;
    }
    if (filter.status && filter.status !== "all" && model.status !== filter.status) return false;
    if (filter.assignment && filter.assignment !== "all" && filter.assignedModelIds) {
      const assigned = filter.assignedModelIds.has(model.id);
      if (filter.assignment === "assigned" && !assigned) return false;
      if (filter.assignment === "unassigned" && assigned) return false;
    }
    if (filter.capability && filter.capability !== "all" &&
      !model.capabilities.includes(filter.capability) &&
      !Object.prototype.hasOwnProperty.call(model.capabilitySupport ?? {}, filter.capability)) {
      return false;
    }
    if (!needle) return true;
    return [
      model.model,
      model.provider,
      model.providerId ?? "",
      ...(model.variants ?? []),
      ...(model.capabilities ?? []),
      ...Object.keys(model.capabilitySupport ?? {}),
    ]
      .join(" ")
      .toLowerCase()
      .includes(needle);
  });
}

export function selectFilteredModels(state: BootstrapState, filter: ModelFilter = {}): BootstrapModel[] {
  const assignedModelIds = new Set(
    state.profiles.flatMap((profile) => (profile.routes ?? [])
      .filter((route) => route.modelId !== null)
      .map((route) => route.modelId as string)),
  );
  return filterModels(state.models, { ...filter, assignedModelIds: filter.assignedModelIds ?? assignedModelIds });
}

export type ControlCenterSummary = {
  providerCount: number;
  profileCount: number;
  modelCount: number;
  availableModelCount: number;
  unavailableModelCount: number;
  unknownModelCount: number;
  authRequiredProviderCount: number;
  degradedProviderCount: number;
  unavailableProviderCount: number;
  unknownProviderCount: number;
  activeProfileId: string | null;
  activeProfileLabel: string | null;
};

/** Single source of truth for the Control Center header counts. */
export function selectControlCenterSummary(state: BootstrapState): ControlCenterSummary {
  const providers = selectProviders(state);
  const active = selectActiveProfile(state);
  const counts = selectModelStatusCounts(state);
  return {
    providerCount: providers.length,
    profileCount: state.profiles.length,
    modelCount: state.models.length,
    availableModelCount: counts.available,
    unavailableModelCount: counts.unavailable,
    unknownModelCount: counts.unknown,
    authRequiredProviderCount: providers.filter((provider) => provider.state === "auth-required").length,
    degradedProviderCount: providers.filter((provider) => provider.state === "degraded").length,
    unavailableProviderCount: providers.filter((provider) => provider.state === "unavailable").length,
    unknownProviderCount: providers.filter((provider) => provider.state === "unknown").length,
    activeProfileId: active?.id ?? null,
    activeProfileLabel: active?.label ?? null,
  };
}
