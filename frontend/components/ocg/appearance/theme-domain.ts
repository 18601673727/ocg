export type ThemePreference = "light" | "dark";
export type AccentPreference = "ochre" | "slate" | "teal";

export type AppearancePreferences = {
  theme: ThemePreference;
  accent: AccentPreference;
};

export const APPEARANCE_STORAGE_KEY = "ocg.appearance.v1";

export const DEFAULT_APPEARANCE: AppearancePreferences = {
  theme: "dark",
  accent: "teal",
};

function resolveThemePreference(value: unknown): ThemePreference {
  return value === "light" || value === "dark" ? value : DEFAULT_APPEARANCE.theme;
}

function resolveAccentPreference(value: unknown): AccentPreference {
  return value === "ochre" || value === "slate" || value === "teal"
    ? value
    : DEFAULT_APPEARANCE.accent;
}

function parseAppearancePreferences(raw: string | null | undefined): AppearancePreferences {
  if (!raw) return { ...DEFAULT_APPEARANCE };
  try {
    const value: unknown = JSON.parse(raw);
    if (!value || typeof value !== "object") return { ...DEFAULT_APPEARANCE };
    const record = value as Record<string, unknown>;
    return {
      theme: resolveThemePreference(record.theme),
      accent: resolveAccentPreference(record.accent),
    };
  } catch {
    return { ...DEFAULT_APPEARANCE };
  }
}

export function readAppearancePreferences(): AppearancePreferences {
  try {
    return parseAppearancePreferences(window.localStorage.getItem(APPEARANCE_STORAGE_KEY));
  } catch {
    return { ...DEFAULT_APPEARANCE };
  }
}

export function serializeAppearancePreferences(preferences: AppearancePreferences): string {
  return JSON.stringify(preferences);
}
