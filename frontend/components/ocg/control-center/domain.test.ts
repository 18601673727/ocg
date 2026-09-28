import assert from "node:assert/strict";
import { test } from "node:test";
import { createBootstrapFixture } from "../bootstrap/fixtures";
import { MockOcgRuntimeClient } from "../runtime/mock-client";
import { resolveScenario } from "../runtime/scenarios";
import { selectAvailableModels } from "../bootstrap/selectors";
import type { BootstrapState } from "../bootstrap/types";
import {
  controlModelKey,
  filterModels,
  filterProviders,
  isUnavailableModel,
  isUnavailableProvider,
  isUnknownModel,
  isUnknownProvider,
  resolveControlCenterView,
  selectActiveProfile,
  selectControlCenterSummary,
  selectFilteredModels,
  selectLeadRoutes,
  selectModelCapabilities,
  selectModelCapabilitySupport,
  selectModelAssignments,
  selectModelProvider,
  selectProfileHealth,
  selectProfileRoutes,
  selectProfileSource,
  selectProfileWarnings,
  selectProviderModels,
  selectProviderAssignments,
  selectProviders,
  selectRoutesForRole,
  selectWorkerRoles,
  selectWorkerRoutes,
} from "./domain";

const SCENARIO = "profiles-models";

function fixture(): BootstrapState {
  return createBootstrapFixture(SCENARIO);
}

test("control center view resolution degrades to profiles", () => {
  assert.equal(resolveControlCenterView("providers"), "providers");
  assert.equal(resolveControlCenterView("models"), "models");
  assert.equal(resolveControlCenterView("nonsense"), "profiles");
  assert.equal(resolveControlCenterView(undefined), "profiles");
  assert.equal(resolveScenario(SCENARIO), SCENARIO);
});

test("active profile prefers explicit selection, then recommendation, then first", () => {
  const state = fixture();
  assert.equal(selectActiveProfile(state)?.id, "balanced");

  const explicit = { ...state, activeProfileId: "lean-local" };
  assert.equal(selectActiveProfile(explicit)?.id, "lean-local");

  const dangling = { ...state, activeProfileId: "does-not-exist" };
  assert.equal(selectActiveProfile(dangling)?.id, "balanced");

  const noRecommendation = {
    ...state,
    profiles: state.profiles.map((profile) => ({ ...profile, recommended: false })),
    activeProfileId: undefined,
  };
  assert.equal(selectActiveProfile(noRecommendation)?.id, "balanced");
  assert.equal(selectActiveProfile({ ...state, profiles: [] }), null);
});

test("profile source distinguishes recommendation from customization", () => {
  const state = fixture();
  const recommended = state.profiles.find((profile) => profile.id === "balanced")!;
  const customized = state.profiles.find((profile) => profile.id === "lean-local")!;
  assert.equal(selectProfileSource(recommended), "recommended");
  assert.equal(selectProfileSource(customized), "customized");

  // Absent source is derived from the recommended flag without inventing metadata.
  const derived = { ...customized, source: undefined, recommended: true };
  assert.equal(selectProfileSource(derived), "recommended");
});

test("lead routes and dynamic future worker roles are resolved separately", () => {
  const state = fixture();
  const balanced = state.profiles.find((profile) => profile.id === "balanced")!;
  const routes = selectProfileRoutes(state, balanced);

  const lead = selectLeadRoutes(routes);
  // One Lead per routing profile: fixed execution tiers are not routes.
  assert.equal(lead.length, 1);
  assert.deepEqual(lead.map((resolved) => resolved.route.tier), ["balanced"]);
  assert.equal(lead[0].route.roleId, "lead");

  const workerRoleIds = selectWorkerRoles(routes).map((role) => role.roleId);
  assert.deepEqual(workerRoleIds, ["explore", "verify", "docs"]);
  assert.equal(selectWorkerRoutes(routes).length, 3);

  // A future worker role is data-driven: no closed union blocks it.
  const custom = state.profiles.find((profile) => profile.id === "custom-quality")!;
  const customRoutes = selectProfileRoutes(state, custom);
  assert.ok(selectWorkerRoles(customRoutes).some((role) => role.roleId === "synthesize"));
  const synthesize = selectRoutesForRole(customRoutes, "synthesize");
  assert.equal(synthesize.length, 1);
  assert.equal(synthesize[0].route.roleKind, "worker");
});

test("provider state keeps unknown distinct from unavailable", () => {
  const state = fixture();
  const providers = selectProviders(state);
  const unknown = providers.find((provider) => provider.id === "unreported-provider")!;
  const unavailable = providers.find((provider) => provider.id === "offline-provider")!;
  const authRequired = providers.find((provider) => provider.id === "future-provider")!;
  const degraded = providers.find((provider) => provider.id === "opencode-go")!;

  assert.equal(isUnknownProvider(unknown), true);
  assert.equal(isUnavailableProvider(unknown), false);

  assert.equal(isUnavailableProvider(unavailable), true);
  assert.equal(isUnknownProvider(unavailable), false);

  assert.equal(isUnknownProvider(null), true);
  assert.equal(isUnavailableProvider(null), false);

  assert.equal(authRequired.authRequired, true);
  assert.equal(authRequired.state, "auth-required");
  assert.equal(degraded.state, "degraded");
});

test("same model name on different providers stays provider-distinct", () => {
  const state = fixture();
  const deepseek = selectFilteredModels(state, { query: "DeepSeek" });
  assert.equal(deepseek.length, 2);
  assert.equal(new Set(deepseek.map((model) => model.provider)).size, 2);
  assert.notEqual(
    controlModelKey(deepseek[0].provider, deepseek[0].model),
    controlModelKey(deepseek[1].provider, deepseek[1].model),
  );
  // The model name itself is identical; only the provider distinguishes them.
  assert.equal(deepseek[0].model, deepseek[1].model);

  const commandOnly = filterModels(state.models, { providerId: "command-code" });
  assert.ok(commandOnly.some((model) => model.id === "command-deepseek"));
  assert.ok(!commandOnly.some((model) => model.id === "go-deepseek"));
});

test("capability unknown is distinct from unsupported", () => {
  const state = fixture();
  const commandMuse = state.models.find((model) => model.id === "command-muse")!;
  const zenMuse = state.models.find((model) => model.id === "zen-muse")!;
  const unreported = state.models.find((model) => model.id === "unreported-model")!;

  assert.equal(selectModelCapabilitySupport(commandMuse, "vision"), "unknown");
  assert.equal(selectModelCapabilitySupport(commandMuse, "audio"), "unsupported");
  assert.equal(selectModelCapabilitySupport(commandMuse, "streaming"), "supported");

  // Not listed at all stays unknown, not unsupported.
  assert.equal(selectModelCapabilitySupport(zenMuse, "workers"), "unknown");
  assert.equal(selectModelCapabilitySupport(zenMuse, "reasoning"), "unsupported");

  assert.equal(selectModelCapabilitySupport(unreported, "tools"), "unknown");
  assert.equal(selectModelCapabilitySupport(unreported, "vision"), "unsupported");
  assert.equal(selectModelCapabilitySupport(null, "tools"), "unknown");

  const entries = selectModelCapabilities(commandMuse);
  assert.equal(entries.find((entry) => entry.id === "audio")?.support, "unsupported");
  assert.equal(entries.find((entry) => entry.id === "vision")?.support, "unknown");
});

test("profile health covers healthy, degraded, unavailable, incomplete, and unknown", () => {
  const state = fixture();
  const health = (id: string) =>
    selectProfileHealth(state, state.profiles.find((profile) => profile.id === id)!);

  assert.equal(health("balanced"), "degraded");
  assert.equal(health("lean-local"), "healthy");
  assert.equal(health("strict-quality"), "unavailable");
  assert.equal(health("custom-quality"), "incomplete");
  assert.equal(health("exploratory"), "unknown");
  assert.equal(selectProfileHealth(state, null), "unknown");

  // Route statuses prove the health derivation is not accidental.
  const balancedRoutes = selectProfileRoutes(state, state.profiles.find((p) => p.id === "balanced")!);
  assert.equal(balancedRoutes.find((resolved) => resolved.route.roleId === "explore")?.status, "fallback");

  const strictRoutes = selectProfileRoutes(state, state.profiles.find((p) => p.id === "strict-quality")!);
  assert.equal(selectLeadRoutes(strictRoutes)[0].status, "unavailable");

  const customRoutes = selectProfileRoutes(state, state.profiles.find((p) => p.id === "custom-quality")!);
  assert.equal(selectRoutesForRole(customRoutes, "explore")[0].status, "unassigned");

  const exploratoryRoutes = selectProfileRoutes(state, state.profiles.find((p) => p.id === "exploratory")!);
  assert.equal(selectLeadRoutes(exploratoryRoutes)[0].status, "unknown");

  // Unknown models/providers are never treated as unavailable.
  const unknownModel = state.models.find((model) => model.id === "unreported-model")!;
  assert.equal(isUnknownModel(unknownModel), true);
  assert.equal(isUnavailableModel(unknownModel), false);
  const unavailableModel = state.models.find((model) => model.id === "go-spacebunny")!;
  assert.equal(isUnavailableModel(unavailableModel), true);
  assert.equal(isUnknownModel(unavailableModel), false);
});

test("overrides, fallbacks, and warnings are surfaced", () => {
  const state = fixture();
  const custom = state.profiles.find((profile) => profile.id === "custom-quality")!;
  const customRoutes = selectProfileRoutes(state, custom);
  const overridden = customRoutes.filter((resolved) => resolved.route.overridden);
  assert.deepEqual(overridden.map((resolved) => resolved.route.roleId).sort(), ["build", "explore", "lead"]);

  const warnings = selectProfileWarnings(state, custom);
  assert.ok(warnings.some((warning) => warning.toLowerCase().includes("overridden")));
  assert.ok(warnings.some((warning) => warning.toLowerCase().includes("no model assigned")));

  const balanced = state.profiles.find((profile) => profile.id === "balanced")!;
  const balancedRoutes = selectProfileRoutes(state, balanced);
  const explore = balancedRoutes.find((resolved) => resolved.route.roleId === "explore")!;
  assert.equal(explore.route.fallback, true);
  assert.equal(explore.route.fallbackModelId, "zen-muse");
  assert.ok(selectProfileWarnings(state, balanced).some((warning) => warning.includes("fallback") || warning.includes("degraded")));

  assert.deepEqual(selectProfileWarnings(state, state.profiles.find((p) => p.id === "lean-local")!), []);
});

test("provider and model filtering is compact and dataset-derived", () => {
  const state = fixture();
  assert.deepEqual(filterProviders(selectProviders(state), "opencode").map((provider) => provider.id).sort(), [
    "opencode-go",
    "opencode-zen",
  ]);
  assert.equal(filterProviders(selectProviders(state), "nothing-matches").length, 0);
  assert.equal(filterProviders(selectProviders(state), "").length, 6);

  const muse = selectFilteredModels(state, { query: "muse" });
  assert.deepEqual(muse.map((model) => model.id).sort(), ["command-muse", "zen-muse"]);

  const goModels = filterModels(state.models, { providerId: "opencode-go" });
  assert.deepEqual(goModels.map((model) => model.id).sort(), ["go-deepseek", "go-spacebunny"]);
  assert.equal(selectProviderModels(state, selectProviders(state).find((p) => p.id === "opencode-go")!).length, 2);
  assert.equal(selectModelProvider(state, state.models.find((model) => model.id === "zen-muse")!)?.id, "opencode-zen");

  assert.deepEqual(
    selectFilteredModels(state, { assignment: "unassigned" }).map((model) => model.id).sort(),
    ["offline-model"],
  );
  assert.deepEqual(selectFilteredModels(state, { status: "unavailable" }).map((model) => model.id).sort(), ["go-spacebunny", "offline-model"]);
  assert.deepEqual(selectFilteredModels(state, { capability: "tools" }).map((model) => model.id).sort(), ["command-deepseek", "command-muse", "go-deepseek", "unreported-model", "zen-muse"]);
});

test("provider and model route assignments remain inspectable without merging domains", () => {
  const state = fixture();
  const command = selectProviders(state).find((provider) => provider.id === "command-code")!;
  assert.ok(selectProviderAssignments(state, command).some((assignment) => assignment.roleLabel === "Lead"));
  assert.ok(selectModelAssignments(state, "command-muse").some((assignment) => assignment.profileLabel === "Balanced"));
  assert.ok(selectModelAssignments(state, "future-model").some((assignment) => assignment.roleLabel === "Synthesize"));
});

test("summary counts come from one normalized source", () => {
  const summary = selectControlCenterSummary(fixture());
  assert.equal(summary.providerCount, 6);
  assert.equal(summary.profileCount, 5);
  assert.equal(summary.modelCount, 8);
  assert.equal(summary.authRequiredProviderCount, 1);
  assert.equal(summary.degradedProviderCount, 1);
  assert.equal(summary.unavailableProviderCount, 1);
  assert.equal(summary.unknownProviderCount, 1);
  assert.equal(summary.unavailableModelCount, 2);
  assert.equal(summary.unknownModelCount, 1);
  assert.equal(summary.activeProfileLabel, "Balanced");
});

test("scenario carries the full control-center shape and does not break other fixtures", () => {
  const state = fixture();
  assert.ok(state.profiles.length >= 2);
  assert.ok(state.providers && state.providers.some((provider) => provider.state === "auth-required"));
  assert.ok(state.providers.some((provider) => provider.state === "degraded"));
  assert.ok(state.models.some((model) => model.status === "unavailable"));
  assert.ok(state.models.some((model) => model.status === "unknown"));
  assert.ok(state.models.some((model) => (model.variants?.length ?? 0) > 1));
  assert.ok(
    state.models.some((model) =>
      Object.values(model.capabilitySupport ?? {}).some((support) => support === "unknown"),
    ),
  );
  assert.ok(
    state.models.some((model) =>
      Object.values(model.capabilitySupport ?? {}).some((support) => support === "unsupported"),
    ),
  );
  assert.ok(state.profiles.some((profile) => profile.routes?.some((route) => route.modelId === null)));
  assert.ok(state.profiles.some((profile) => profile.routes?.some((route) => route.overridden)));
  assert.ok(state.profiles.some((profile) => profile.routes?.some((route) => route.fallback)));
  assert.ok(state.profiles.some((profile) => profile.routes?.some((route) => Boolean(route.warning))));

  // Other scenarios keep their existing shape and stay free of the new fields.
  const local = createBootstrapFixture("local-ready");
  assert.equal(local.providers, undefined);
  assert.equal(local.activeProfileId, undefined);
  assert.equal(selectAvailableModels(local).length, 2);
});

test("fixtures are deterministic and contain no credential material", () => {
  assert.deepEqual(fixture(), fixture());

  const forbiddenKey = /(password|passwd|secret|api[_-]?key|token|credential)/i;
  const forbiddenValue = /(password|bearer\s|api[_-]?key|secret)/i;

  const walk = (value: unknown, visit: (key: string | null, value: unknown) => void, key: string | null = null): void => {
    visit(key, value);
    if (Array.isArray(value)) {
      value.forEach((item) => walk(item, visit, null));
      return;
    }
    if (value && typeof value === "object") {
      for (const [childKey, childValue] of Object.entries(value)) walk(childValue, visit, childKey);
    }
  };

  walk(fixture(), (key, value) => {
    if (key !== null) assert.doesNotMatch(key, forbiddenKey, "control center exposes a forbidden field");
    if (typeof value === "string") assert.doesNotMatch(value, forbiddenValue, "control center exposes a forbidden value");
  });
});

test("mock profile switching updates the snapshot without persistence", async () => {
  const client = new MockOcgRuntimeClient(SCENARIO);
  const events: string[] = [];
  client.subscribe((event) => events.push(event.type));

  assert.equal(client.getSnapshot().bootstrap.activeProfileId, "balanced");
  await client.setActiveProfile("lean-local");
  assert.equal(client.getSnapshot().bootstrap.activeProfileId, "lean-local");
  assert.equal((await client.getBootstrap()).activeProfileId, "lean-local");
  assert.ok(events.includes("bootstrap.updated"));

  // Unknown ids are ignored rather than creating a phantom active profile.
  const before = events.length;
  await client.setActiveProfile("does-not-exist");
  assert.equal(client.getSnapshot().bootstrap.activeProfileId, "lean-local");
  assert.equal(events.length, before);

  // A fresh client starts from the deterministic fixture; nothing was persisted.
  const fresh = new MockOcgRuntimeClient(SCENARIO);
  assert.equal(fresh.getSnapshot().bootstrap.activeProfileId, "balanced");
});
