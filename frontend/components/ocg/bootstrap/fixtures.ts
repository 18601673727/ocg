import type { ScenarioId } from "../runtime/runtime-types";
import type {
  BootstrapAccess,
  BootstrapAccessState,
  BootstrapCapability,
  BootstrapConnection,
  BootstrapFailure,
  BootstrapHandoffState,
  BootstrapMode,
  BootstrapModel,
  BootstrapOnboarding,
  BootstrapProfile,
  BootstrapProvider,
  BootstrapResource,
  BootstrapState,
  OnboardingStageId,
} from "./types";

/**
 * Deterministic bootstrap fixtures. Every builder returns fresh objects so the
 * mock client can clone and mutate them without sharing state across scenarios.
 *
 * Only reachability and display metadata appear here. No credential material is
 * modeled, stored, or implied.
 */

function localAccess(): BootstrapAccess {
  return {
    state: "local",
    remote: false,
    handoffState: "unavailable",
    detail: "Local workspace.",
  };
}

function remoteAccess(
  state: BootstrapAccessState,
  handoffState: BootstrapHandoffState,
  detail: string,
): BootstrapAccess {
  return { state, remote: true, handoffState, detail };
}

function baseResources(): BootstrapResource[] {
  return [
    {
      id: "workspace-primary",
      label: "OCG workspace",
      kind: "workspace",
      source: "local",
      status: "available",
      detail: "Primary workspace detected.",
      economics: { kind: "unknown", detail: "Workspace accounting is not applicable." },
      capacity: { status: "unknown", detail: "Not reported by the local runtime." },
      required: true,
    },
    {
      id: "repo-runtime",
      label: "Durable job runtime",
      kind: "repository",
      source: "local",
      status: "available",
      detail: "Repository detected in the workspace.",
      economics: { kind: "unknown", detail: "No provider billing data." },
      capacity: { status: "unknown", detail: "Not reported by the local runtime." },
      required: false,
    },
    {
      id: "endpoint-broker",
      label: "Resource broker endpoint",
      kind: "endpoint",
      source: "remote",
      status: "unknown",
      detail: "Not exposed by the current runtime.",
      provider: "Future provider",
      economics: { kind: "unknown", detail: "Pricing not reported." },
      capacity: { status: "unknown", detail: "Capacity not reported." },
      required: false,
    },
    {
      id: "openai-subscription",
      label: "OpenAI subscription",
      kind: "endpoint",
      source: "remote",
      status: "available",
      provider: "OpenAI",
      plan: "Subscription resource",
      detail: "Detected from the connected runtime.",
      economics: { kind: "subscription", detail: "Subscription; per-request price is unknown." },
      capacity: { status: "unknown", detail: "Quota was not reported." },
      required: false,
    },
    {
      id: "example-provider-resource",
      label: "Provider",
      kind: "endpoint",
      source: "remote",
      status: "available",
      provider: "Provider",
      plan: "Connected account",
      detail: "Available through the runtime auth mechanism.",
      economics: { kind: "subscription", detail: "Plan economics are provider-owned." },
      capacity: { status: "known", detail: "Shared capacity; exact quota is not shown." },
      required: false,
    },
    {
      id: "example-fallback-resource",
      label: "Fallback Provider",
      kind: "endpoint",
      source: "remote",
      status: "available",
      provider: "Fallback Provider",
      plan: "Free / low-cost resource",
      detail: "A lower-cost fallback is available.",
      economics: { kind: "free", detail: "Free or included usage; provider limits apply." },
      capacity: { status: "known", detail: "Best-effort capacity." },
      required: false,
    },
    {
      id: "openai-compatible-endpoint",
      label: "OpenAI-compatible endpoint",
      kind: "endpoint",
      source: "imported",
      status: "available",
      provider: "OpenAI-compatible",
      plan: "Imported endpoint",
      detail: "Endpoint shape detected; provider metadata is limited.",
      economics: { kind: "unknown", detail: "Pricing not reported." },
      capacity: { status: "unknown", detail: "Capacity not reported." },
      required: false,
    },
    {
      id: "temporarily-unavailable-provider",
      label: "Additional provider resource",
      kind: "endpoint",
      source: "remote",
      status: "unavailable",
      provider: "Unreported provider",
      plan: "Unknown plan",
      detail: "Temporarily unavailable; it will not be used for the recommendation.",
      economics: { kind: "unknown", detail: "Pricing not reported." },
      capacity: { status: "unknown", detail: "Capacity not reported." },
      required: false,
    },
  ];
}

function baseConnections(): BootstrapConnection[] {
  return [
    {
      id: "runtime-local",
      label: "Local runtime",
      kind: "runtime",
      state: "connected",
      detail: "Local mock runtime.",
      requiresHandoff: false,
      required: true,
    },
    {
      id: "provider-local",
      label: "Model provider",
      kind: "model-provider",
      state: "connected",
      detail: "Connected for this workspace.",
      requiresHandoff: false,
      required: true,
    },
    {
      id: "source-control",
      label: "Source control",
      kind: "source-control",
      state: "unconfigured",
      detail: "Optional in this phase.",
      requiresHandoff: false,
      required: false,
      advanced: true,
    },
    {
      id: "provider-secondary",
      label: "Additional provider account",
      kind: "model-provider",
      state: "auth-required",
      detail: "Authentication is delegated to the provider runtime.",
      requiresHandoff: false,
      required: false,
    },
  ];
}

function baseModels(): BootstrapModel[] {
  return [
    {
      id: "zen-muse",
      provider: "Catalog Provider",
      model: "example-model-free",
      status: "available",
      capabilities: ["streaming", "tools"],
    },
    {
      id: "command-muse",
      provider: "Provider",
      model: "example-model",
      status: "available",
      capabilities: ["streaming", "tools", "reasoning"],
    },
  ];
}

function baseCapabilities(): BootstrapCapability[] {
  return [
    { id: "streaming", label: "Streaming responses", status: "available" },
    { id: "tools", label: "Tool activity", status: "available" },
    { id: "workers", label: "Parallel workers", status: "available" },
    { id: "vision", label: "Image input", status: "unknown" },
  ];
}

function baseProfiles(): BootstrapProfile[] {
  return [
    {
      id: "economy",
      label: "Lean",
      tier: "economy",
      rationale: "Lowest expected spend for routine threads.",
      modelIds: ["zen-muse"],
      recommended: false,
    },
    {
      id: "balanced",
      label: "Balanced",
      tier: "balanced",
      rationale: "Balances capability and spend for most work.",
      modelIds: ["command-muse", "zen-muse"],
      recommended: true,
    },
    {
      id: "quality",
      label: "Quality-first",
      tier: "quality",
      rationale: "Prefers the stronger available model per task.",
      modelIds: ["command-muse"],
      recommended: false,
    },
  ];
}

/**
 * Deterministic Control Center fixture. It exercises provider reachability,
 * explicit unknown vs unavailable, per-model capability support (unknown is not
 * unsupported), variants, the same model name on two providers, assigned and
 * unassigned routes, overrides, fallbacks, warnings, and a future worker role.
 * No credential material is represented.
 */
function controlProviders(): BootstrapProvider[] {
  return [
    {
      id: "example-catalog-provider",
      label: "Catalog Provider",
      state: "connected",
      detail: "Reported ready for this workspace.",
      authRequired: false,
      endpointLabel: "zen",
      endpointType: "hosted",
      plan: "Included resource",
      economics: { kind: "subscription", detail: "Included with the workspace resource." },
      capacity: { status: "known", detail: "Best-effort shared capacity." },
      discoveredModelCount: 1,
      capabilities: ["streaming", "tools"],
      lastCheckedAt: "2026-09-25T09:00:00Z",
    },
    {
      id: "example-provider",
      label: "Provider",
      state: "connected",
      detail: "Reported ready for this workspace.",
      authRequired: false,
      endpointLabel: "command",
      endpointType: "hosted",
      plan: "Contributor account",
      economics: { kind: "subscription", detail: "Provider-owned plan economics." },
      capacity: { status: "unknown", detail: "Quota was not reported." },
      discoveredModelCount: 2,
      capabilities: ["streaming", "tools", "reasoning"],
      lastCheckedAt: "2026-09-25T08:58:00Z",
    },
    {
      id: "example-fallback-provider",
      label: "Fallback Provider",
      state: "degraded",
      detail: "Reachable but reporting elevated latency.",
      authRequired: false,
      endpointLabel: "go",
      endpointType: "hosted",
      plan: "Free / low-cost resource",
      economics: { kind: "free", detail: "Included usage; provider limits apply." },
      capacity: { status: "known", detail: "Elevated latency observed." },
      discoveredModelCount: 2,
      capabilities: ["streaming"],
      lastCheckedAt: "2026-09-25T08:55:00Z",
      warnings: ["Latency is elevated; fallback coverage is recommended."],
    },
    {
      id: "future-provider",
      label: "Future Provider",
      state: "auth-required",
      detail: "Authentication is delegated to the provider runtime.",
      authRequired: true,
      endpointLabel: "future",
      endpointType: "hosted",
      plan: "Not reported",
      economics: { kind: "unknown", detail: "Pricing not reported." },
      capacity: { status: "unknown", detail: "Capacity not reported." },
      discoveredModelCount: 1,
      lastCheckedAt: "2026-09-25T08:50:00Z",
    },
    {
      id: "unreported-provider",
      label: "Unreported Provider",
      state: "unknown",
      detail: "Provider state was not reported; it is not treated as offline.",
      authRequired: false,
      endpointType: "unknown",
      capacity: { status: "unknown", detail: "Last check did not report capacity." },
      discoveredModelCount: 1,
    },
    {
      id: "offline-provider",
      label: "Offline Provider",
      state: "unavailable",
      detail: "Reported unavailable for this workspace.",
      authRequired: false,
      endpointType: "hosted",
      plan: "Not reported",
      economics: { kind: "unknown", detail: "Pricing not reported." },
      capacity: { status: "unknown", detail: "Provider is unavailable." },
      discoveredModelCount: 1,
      warnings: ["This provider is explicitly unavailable and is not selected for routing."],
    },
  ];
}

function controlModels(): BootstrapModel[] {
  return [
    {
      id: "zen-muse",
      provider: "Catalog Provider",
      providerId: "example-catalog-provider",
      model: "example-model-free",
      status: "available",
      capabilities: ["streaming", "tools"],
      variants: ["free", "standard"],
      capabilitySupport: {
        streaming: "supported",
        tools: "supported",
        reasoning: "unsupported",
        vision: "unknown",
      },
      provenanceNote: "Reported by the local runtime.",
      displayName: "example-model-free",
      contextWindow: 128000,
      economics: { kind: "subscription", detail: "Included resource; unit price not reported." },
      latencyObservation: { status: "known", detail: "Typical response observed." },
    },
    {
      id: "command-muse",
      provider: "Provider",
      providerId: "example-provider",
      model: "example-model",
      status: "available",
      capabilities: ["streaming", "tools", "reasoning"],
      variants: ["mid", "high"],
      capabilitySupport: {
        streaming: "supported",
        tools: "supported",
        reasoning: "supported",
        vision: "unknown",
        audio: "unsupported",
      },
      provenanceNote: "Reported by the local runtime.",
      displayName: "example-model",
      contextWindow: 200000,
      economics: { kind: "subscription", detail: "Provider-owned plan economics." },
      latencyObservation: { status: "known", detail: "Typical response observed." },
    },
    {
      id: "command-deepseek",
      provider: "Provider",
      providerId: "example-provider",
      model: "DeepSeek V4.1 Flash",
      status: "available",
      capabilities: ["streaming", "tools"],
      variants: ["standard", "deep"],
      capabilitySupport: {
        streaming: "supported",
        tools: "supported",
        vision: "unknown",
      },
      contextWindow: 128000,
      latencyObservation: { status: "unknown" },
    },
    {
      id: "go-deepseek",
      provider: "Fallback Provider",
      providerId: "example-fallback-provider",
      model: "DeepSeek V4.1 Flash",
      status: "available",
      capabilities: ["streaming"],
      variants: ["standard"],
      capabilitySupport: {
        streaming: "supported",
        tools: "unknown",
      },
      contextWindow: 128000,
      latencyObservation: { status: "known", detail: "Elevated latency from degraded provider." },
    },
    {
      id: "fallback-example-model",
      provider: "Fallback Provider",
      providerId: "example-fallback-provider",
      model: "fallback-example-model",
      status: "unavailable",
      capabilities: [],
      variants: ["standard", "deep"],
      capabilitySupport: { streaming: "unknown" },
      provenanceNote: "Reported unavailable by the provider.",
      latencyObservation: { status: "unknown", detail: "Provider did not provide a latency observation." },
    },
    {
      id: "future-model",
      provider: "Future Provider",
      providerId: "future-provider",
      model: "Future Reasoner Preview",
      status: "pending",
      capabilities: [],
      variants: [],
      capabilitySupport: {},
      provenanceNote: "Discovered but not yet authorized.",
    },
    {
      id: "unreported-model",
      provider: "Unreported Provider",
      providerId: "unreported-provider",
      model: "Unreported Model",
      status: "unknown",
      capabilities: [],
      variants: [],
      capabilitySupport: { tools: "unknown", vision: "unsupported" },
      provenanceNote: "Only the model name was reported.",
    },
    {
      id: "offline-model",
      provider: "Offline Provider",
      providerId: "offline-provider",
      model: "Retired Model",
      status: "unavailable",
      capabilities: [],
      variants: [],
      capabilitySupport: {},
      provenanceNote: "Reported unavailable by the provider.",
    },
  ];
}

function controlCapabilities(): BootstrapCapability[] {
  return [
    { id: "streaming", label: "Streaming responses", status: "available" },
    { id: "tools", label: "Tool activity", status: "available" },
    { id: "workers", label: "Parallel workers", status: "available" },
    { id: "vision", label: "Image input", status: "unknown" },
    { id: "audio", label: "Audio input", status: "unknown" },
  ];
}

function controlProfiles(): BootstrapProfile[] {
  return [
    {
      id: "balanced",
      label: "Balanced",
      tier: "balanced",
      rationale: "Balances capability and spend for most work. Explore falls back while its provider is degraded.",
      modelIds: ["command-muse", "command-deepseek", "zen-muse"],
      recommended: true,
      source: "recommended",
      routes: [
        { id: "balanced-lead", roleKind: "lead", roleId: "lead", roleLabel: "Lead", tier: "balanced", modelId: "command-muse", variant: "mid" },
        {
          id: "balanced-explore",
          roleKind: "worker",
          roleId: "explore",
          roleLabel: "Explore",
          modelId: "go-deepseek",
          variant: "standard",
          fallback: true,
          fallbackModelId: "zen-muse",
          warning: "Primary provider is degraded; Explore is using the fallback model.",
        },
        { id: "balanced-verify", roleKind: "worker", roleId: "verify", roleLabel: "Verify", modelId: "zen-muse", variant: "standard" },
        { id: "balanced-docs", roleKind: "worker", roleId: "docs", roleLabel: "Docs", modelId: "command-deepseek" },
      ],
    },
    {
      id: "lean-local",
      label: "Lean local",
      tier: "economy",
      rationale: "Uses only the free local model for every role.",
      modelIds: ["zen-muse"],
      recommended: false,
      source: "customized",
      routes: [
        { id: "lean-lead", roleKind: "lead", roleId: "lead", roleLabel: "Lead", tier: "economy", modelId: "zen-muse", variant: "free" },
        { id: "lean-explore", roleKind: "worker", roleId: "explore", roleLabel: "Explore", modelId: "zen-muse" },
        { id: "lean-docs", roleKind: "worker", roleId: "docs", roleLabel: "Docs", modelId: "zen-muse" },
      ],
    },
    {
      id: "custom-quality",
      label: "Custom quality",
      tier: "quality",
      rationale: "Prefers the stronger model per role. Explore is intentionally unassigned.",
      modelIds: ["command-muse", "command-deepseek", "zen-muse"],
      recommended: false,
      advanced: true,
      source: "customized",
      routes: [
        { id: "custom-lead", roleKind: "lead", roleId: "lead", roleLabel: "Lead", tier: "quality", modelId: "command-muse", variant: "high", overridden: true },
        { id: "custom-explore", roleKind: "worker", roleId: "explore", roleLabel: "Explore", modelId: null, overridden: true },
        { id: "custom-build", roleKind: "worker", roleId: "build", roleLabel: "Build", modelId: "command-deepseek", variant: "deep", overridden: true },
        {
          id: "custom-verify",
          roleKind: "worker",
          roleId: "verify",
          roleLabel: "Verify",
          modelId: "fallback-example-model",
          fallback: true,
          fallbackModelId: "zen-muse",
        },
        { id: "custom-synthesize", roleKind: "worker", roleId: "synthesize", roleLabel: "Synthesize", modelId: "future-model" },
      ],
    },
    {
      id: "strict-quality",
      label: "Strict quality",
      tier: "quality",
      rationale: "Pins the lead to a single model and refuses to fall back.",
      modelIds: ["fallback-example-model", "command-muse"],
      recommended: false,
      source: "customized",
      routes: [
        { id: "strict-lead", roleKind: "lead", roleId: "lead", roleLabel: "Lead", tier: "quality", modelId: "fallback-example-model", variant: "deep" },
        { id: "strict-verify", roleKind: "worker", roleId: "verify", roleLabel: "Verify", modelId: "command-muse" },
      ],
    },
    {
      id: "exploratory",
      label: "Exploratory",
      tier: "balanced",
      rationale: "Tries an unreported provider; every fact stays unknown until the runtime reports it.",
      modelIds: ["unreported-model"],
      recommended: false,
      source: "customized",
      routes: [
        { id: "explore-lead", roleKind: "lead", roleId: "lead", roleLabel: "Lead", tier: "balanced", modelId: "unreported-model" },
        { id: "explore-worker", roleKind: "worker", roleId: "explore", roleLabel: "Explore", modelId: "unreported-model" },
      ],
    },
  ];
}

/** Deterministic Control Center state reusing the shared bootstrap inventory. */
function profilesModels(): BootstrapState {
  return {
    ...localReady(),
    providers: controlProviders(),
    capabilities: controlCapabilities(),
    models: controlModels(),
    profiles: controlProfiles(),
    activeProfileId: "balanced",
  };
}

function localReady(): BootstrapState {
  return {
    access: localAccess(),
    onboarding: null,
    resources: baseResources(),
    connections: baseConnections(),
    models: baseModels(),
    capabilities: baseCapabilities(),
    profiles: baseProfiles(),
    ready: true,
  };
}

function onboardingState(
  mode: BootstrapMode,
  stage: OnboardingStageId,
  completedStages: OnboardingStageId[],
  options: { canResume?: boolean; failure?: BootstrapFailure } = {},
): BootstrapOnboarding {
  return {
    mode,
    stage,
    completedStages,
    canResume: options.canResume ?? false,
    ...(options.failure ? { failure: options.failure } : {}),
  };
}

function configured(access: BootstrapAccess, onboarding: BootstrapOnboarding | null): BootstrapState {
  return {
    access,
    onboarding,
    resources: baseResources(),
    connections: baseConnections(),
    models: baseModels(),
    capabilities: baseCapabilities(),
    profiles: baseProfiles(),
    ready: false,
  };
}

function remoteReady(access: BootstrapAccess): BootstrapState {
  const state = localReady();
  return { ...state, access };
}

function connectionFailure(): BootstrapFailure {
  return {
    code: "connection-failed",
    summary: "The model provider connection could not be established.",
    detail: "The last connection attempt did not complete. No request left this browser.",
    action: "retry",
    actionLabel: "Retry connection",
    retryable: true,
  };
}

function recoveryFailure(): BootstrapFailure {
  return {
    code: "recovery-failed",
    summary: "The previous setup could not be restored.",
    detail: "A saved setup checkpoint is available for the mock restore action.",
    action: "restore",
    actionLabel: "Restore saved setup",
    retryable: true,
  };
}

function invalidConfigurationFailure(): BootstrapFailure {
  return {
    code: "invalid-configuration",
    summary: "The saved setup is not valid for this runtime.",
    detail: "Review the detected resources and choose a new recommendation before continuing.",
    action: "reconfigure",
    actionLabel: "Review setup",
    retryable: false,
  };
}

function onboardingResume(): BootstrapState {
  const connections = baseConnections().map((connection) =>
    connection.id === "source-control" ? { ...connection, state: "pending" as const } : connection,
  );
  return {
    ...configured(
      localAccess(),
      onboardingState("resume", "connections", ["welcome", "resources"], { canResume: true }),
    ),
    connections,
  };
}

function onboardingMigration(): BootstrapState {
  const resources = baseResources().map((resource) =>
    resource.id === "repo-runtime"
      ? { ...resource, source: "imported" as const, status: "pending" as const, detail: "Imported from the previous setup." }
      : resource,
  );
  return {
    ...configured(localAccess(), onboardingState("migrate", "resources", ["welcome"], { canResume: true })),
    resources,
  };
}

function onboardingRecovery(): BootstrapState {
  return configured(localAccess(), onboardingState("recover", "welcome", [], { failure: recoveryFailure() }));
}

function onboardingInvalidConfiguration(): BootstrapState {
  return configured(
    localAccess(),
    onboardingState("reconfigure", "welcome", [], { failure: invalidConfigurationFailure() }),
  );
}

function onboardingAuthRequired(): BootstrapState {
  return configured(
    remoteAccess("auth-required", "idle", "Access is required before setup can continue."),
    onboardingState("firstRun", "welcome", []),
  );
}

function onboardingConnectionFailure(): BootstrapState {
  const connections = baseConnections().map((connection) =>
    connection.id === "provider-local"
      ? { ...connection, state: "failed" as const, detail: "The last attempt did not complete." }
      : connection,
  );
  return {
    ...configured(
      localAccess(),
      onboardingState("reconfigure", "connections", ["welcome", "resources"], { failure: connectionFailure() }),
    ),
    connections,
  };
}

function onboardingDiscovery(): BootstrapState {
  const models: BootstrapModel[] = [
    ...baseModels().slice(0, 1),
    {
      id: "unknown-provider-model",
      provider: "Unconfigured provider",
      model: "Not reported",
      status: "unknown",
      capabilities: [],
    },
  ];
  const capabilities: BootstrapCapability[] = baseCapabilities().map((capability) => ({
    ...capability,
    status: "unknown" as const,
  }));
  return {
    ...configured(localAccess(), onboardingState("firstRun", "discovery", ["welcome", "resources", "connections"])),
    models,
    capabilities,
  };
}

function onboardingReady(): BootstrapState {
  return configured(
    localAccess(),
    onboardingState(
      "firstRun",
      "ready",
      ["welcome", "resources", "connections", "discovery", "overview", "profile"],
      { canResume: true },
    ),
  );
}

export function createBootstrapFixture(scenario: ScenarioId): BootstrapState {
  switch (scenario) {
    case "local-ready":
      return localReady();
    case "local-first-run":
      return configured(localAccess(), onboardingState("firstRun", "welcome", []));
    case "remote-unauthenticated":
      return remoteReady(remoteAccess("unauthenticated", "idle", "Access handoff is required for this remote workspace."));
    case "remote-session-expired":
      return remoteReady(remoteAccess("session-expired", "idle", "The access session expired. Refresh the handoff to continue."));
    case "remote-denied":
      return remoteReady(remoteAccess("denied", "failed", "Access to this workspace was denied."));
    case "remote-authenticated-ready":
      return remoteReady(remoteAccess("authenticated", "complete", "Access handoff granted (mock)."));
    case "remote-authenticated-first-run":
      return configured(remoteAccess("authenticated", "complete", "Access handoff granted (mock)."), onboardingState("firstRun", "welcome", []));
    case "onboarding-resume":
      return onboardingResume();
    case "onboarding-migration":
      return onboardingMigration();
    case "onboarding-recovery":
      return onboardingRecovery();
    case "onboarding-invalid-configuration":
      return onboardingInvalidConfiguration();
    case "onboarding-auth-required":
      return onboardingAuthRequired();
    case "onboarding-connection-failure":
      return onboardingConnectionFailure();
    case "onboarding-discovery":
      return onboardingDiscovery();
    case "onboarding-ready":
      return onboardingReady();
    case "profiles-models":
      return profilesModels();
    default:
      // Every pre-existing scenario predates remote access and stays local-ready.
      return localReady();
  }
}
