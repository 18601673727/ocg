import type { BootstrapState } from "../bootstrap/types";
import type { RuntimeStatus } from "../types";
import {
  DEFAULT_APPEARANCE,
  type AppearancePreferences,
} from "../appearance/theme-domain";

export type SettingsSectionId = "general" | "runtime" | "resources" | "access" | "appearance" | "diagnostics" | "advanced";
export type SettingOwnership = "frontend-only" | "backend-authoritative" | "runtime-derived";

export type NormalizedSetting = {
  id: string;
  label: string;
  value: string | number | boolean;
  description: string;
  ownership: SettingOwnership;
  readOnly: boolean;
  action?: "reconfigure";
};

export type NormalizedSettingsState = {
  sections: Record<SettingsSectionId, NormalizedSetting[]>;
};

export type SettingsSection = {
  id: SettingsSectionId;
  title: string;
  description: string;
  items: NormalizedSetting[];
};

export const SETTINGS_SECTION_ORDER: readonly SettingsSectionId[] = [
  "general",
  "runtime",
  "resources",
  "access",
  "appearance",
  "diagnostics",
  "advanced",
];

export const SETTINGS_SECTION_COPY: Record<SettingsSectionId, { title: string; description: string }> = {
  general: { title: "General", description: "Workspace presentation and startup context." },
  runtime: { title: "Runtime", description: "Effective runtime facts reported by the current frontend fixture." },
  resources: { title: "Resources", description: "Re-enter the existing resource-first setup flow when configuration needs review." },
  access: { title: "Access", description: "Display-only local and remote access state; no browser credential management." },
  appearance: { title: "Appearance", description: "Frontend-only presentation preferences for this browser." },
  diagnostics: { title: "Diagnostics", description: "Safe diagnostic collection and verbosity facts; backend mutation is not connected." },
  advanced: { title: "Advanced", description: "Read-only projections useful when debugging the frontend boundary." },
};

export const RECONFIGURE_PATH = "/onboarding?scenario=onboarding-invalid-configuration";

function accessLabel(state: BootstrapState["access"]["state"]): string {
  return state === "local" ? "Local" : state === "authenticated" ? "Remote · authorized" : `Remote · ${state}`;
}

export function createSettingsState(
  snapshot: { status: RuntimeStatus; bootstrap: BootstrapState },
  appearance: AppearancePreferences = DEFAULT_APPEARANCE,
): NormalizedSettingsState {
  const { bootstrap, status } = snapshot;
  const activeProfile = bootstrap.profiles.find((profile) => profile.id === bootstrap.activeProfileId)
    ?? bootstrap.profiles.find((profile) => profile.recommended);
  const connectedConnections = bootstrap.connections.filter((connection) => connection.state === "connected").length;
  const availableModels = bootstrap.models.filter((model) => model.status === "available").length;

  return {
    sections: {
      general: [
        { id: "general.workspace-name", label: "Workspace display name", value: "OCG Workspace", description: "A frontend label for the current workspace shell.", ownership: "frontend-only", readOnly: true },
        { id: "general.startup-destination", label: "Startup destination", value: "Chat", description: "The default destination remains the existing workspace entry point.", ownership: "frontend-only", readOnly: true },
        { id: "general.confirmations", label: "Confirmation preferences", value: "Runtime defaults", description: "No backend action is changed by this fixture surface.", ownership: "runtime-derived", readOnly: true },
      ],
      runtime: [
        { id: "runtime.ocg-version", label: "OCG version", value: "0.1.0 · frontend fixture", description: "Build metadata is informational in this phase.", ownership: "runtime-derived", readOnly: true },
        { id: "runtime.engine", label: "OpenCode/runtime", value: "Mock OcgRuntimeClient", description: "A future adapter can replace the client without changing this read model.", ownership: "runtime-derived", readOnly: true },
        { id: "runtime.mode", label: "Runtime mode", value: bootstrap.access.remote ? "Remote" : "Local", description: "Derived from normalized bootstrap access state.", ownership: "runtime-derived", readOnly: true },
        { id: "runtime.profile", label: "Active profile", value: activeProfile?.label ?? "Unknown", description: "Profile selection is read from the current in-memory bootstrap snapshot.", ownership: "runtime-derived", readOnly: true },
        { id: "runtime.model-choice", label: "Selected model", value: "Configured by the OCG Profile", description: "The OCG Profile is the configuration authority; it is edited in Configuration, not here.", ownership: "backend-authoritative", readOnly: true },
        { id: "runtime.health", label: "Runtime health", value: status.state, description: status.detail ?? "No additional runtime detail reported.", ownership: "runtime-derived", readOnly: true },
      ],
      resources: [
        { id: "resources.setup", label: "Resources & Setup", value: `${connectedConnections} connected · ${availableModels} available models`, description: "Uses the existing onboarding/reconfigure architecture. No second setup wizard is created.", ownership: "backend-authoritative", readOnly: true, action: "reconfigure" },
      ],
      access: [
        { id: "access.mode", label: "Access mode", value: accessLabel(bootstrap.access.state), description: bootstrap.access.detail ?? "Access state is reported by bootstrap.", ownership: "backend-authoritative", readOnly: true },
        { id: "access.handoff", label: "Authorization session", value: bootstrap.access.handoffState, description: "Only normalized handoff status is displayed; no identity material is exposed.", ownership: "backend-authoritative", readOnly: true },
        { id: "access.remote-status", label: "Remote configuration", value: bootstrap.access.remote ? "Configured by runtime" : "Not configured", description: "Cloudflare Access management is intentionally outside this browser surface.", ownership: "backend-authoritative", readOnly: true },
      ],
      appearance: [
        { id: "appearance.theme", label: "Theme", value: appearance.theme, description: "System, light, or dark presentation preference.", ownership: "frontend-only", readOnly: false },
        { id: "appearance.density", label: "Density", value: appearance.density, description: "A restrained comfortable or compact layout preference.", ownership: "frontend-only", readOnly: false },
        { id: "appearance.accent", label: "Accent", value: appearance.accent, description: "A predefined technical accent; arbitrary CSS is not accepted.", ownership: "frontend-only", readOnly: false },
      ],
      diagnostics: [
        { id: "diagnostics.telemetry", label: "Telemetry", value: "Not connected", description: "This frontend does not claim to enable or disable backend telemetry.", ownership: "backend-authoritative", readOnly: true },
        { id: "diagnostics.local-collection", label: "Local diagnostic collection", value: "Fixture only", description: "The Logs surface uses bounded in-memory fixtures; no files are read.", ownership: "frontend-only", readOnly: true },
        { id: "diagnostics.verbosity", label: "Log verbosity", value: "info · fixture", description: "Displayed as normalized context; a backend verbosity write is not implemented.", ownership: "backend-authoritative", readOnly: true },
      ],
      advanced: [
        { id: "advanced.effective-config", label: "Effective configuration", value: "Read-only projection", description: "Only display-safe normalized facts are available in this phase.", ownership: "runtime-derived", readOnly: true },
        { id: "advanced.feature-flags", label: "Feature flags", value: "Frontend defaults", description: "No YAML editor or raw secret projection is exposed.", ownership: "runtime-derived", readOnly: true },
      ],
    },
  };
}

export function selectSettingsSections(state: NormalizedSettingsState): SettingsSection[] {
  return SETTINGS_SECTION_ORDER
    .map((id) => ({ id, ...SETTINGS_SECTION_COPY[id], items: state.sections[id] ?? [] }))
    .filter((section) => section.items.length > 0);
}

export function isBackendOwned(setting: NormalizedSetting): boolean {
  return setting.ownership === "backend-authoritative";
}
