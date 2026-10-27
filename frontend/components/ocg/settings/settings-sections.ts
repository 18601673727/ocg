export const SETTINGS_SECTIONS = [
  { id: "models", label: "settings.nav.models", group: "profiles" },
  { id: "agents", label: "settings.nav.agents", group: "profiles" },
  { id: "decisions", label: "decision.title", group: "profiles" },
  { id: "projects", label: "settings.nav.projects", group: "workspace" },
  { id: "execution", label: "settings.nav.execution", group: "workspace" },
  { id: "appearance", label: "settings.section.appearance", group: "preferences" },
  { id: "session", label: "settings.session.title", group: "preferences" },
  { id: "system", label: "settings.nav.system", group: "preferences" },
] as const;

export type SettingsSection = (typeof SETTINGS_SECTIONS)[number]["id"];

export function isSettingsSection(value: string): value is SettingsSection {
  return SETTINGS_SECTIONS.some((section) => section.id === value);
}

export function settingsPath(section: SettingsSection): string {
  return `/settings/${section}`;
}

export function parseSettingsPath(pathname: string): SettingsSection | null {
  const path = pathname.replace(/\.html$/, "").replace(/\/$/, "");
  if (path === "/settings") return "models";
  const section = path.startsWith("/settings/") ? path.slice("/settings/".length) : "";
  return isSettingsSection(section) ? section : null;
}
