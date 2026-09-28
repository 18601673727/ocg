export type ThemePreference = "system" | "light" | "dark";
export type ResolvedTheme = "light" | "dark";
export type DensityPreference = "comfortable" | "compact";
export type AccentPreference = "ochre" | "slate" | "teal";

export type AppearancePreferences = {
  theme: ThemePreference;
  density: DensityPreference;
  accent: AccentPreference;
};

export const APPEARANCE_STORAGE_KEY = "ocg.appearance.v1";

export const DEFAULT_APPEARANCE: AppearancePreferences = {
  theme: "system",
  density: "comfortable",
  accent: "ochre",
};

export function resolveThemePreference(value: unknown): ThemePreference {
  return value === "light" || value === "dark" || value === "system" ? value : DEFAULT_APPEARANCE.theme;
}

export function resolveDensityPreference(value: unknown): DensityPreference {
  return value === "compact" || value === "comfortable" ? value : DEFAULT_APPEARANCE.density;
}

export function resolveAccentPreference(value: unknown): AccentPreference {
  return value === "ochre" || value === "slate" || value === "teal" ? value : DEFAULT_APPEARANCE.accent;
}

export function resolveEffectiveTheme(preference: ThemePreference, systemTheme: ResolvedTheme): ResolvedTheme {
  return preference === "system" ? systemTheme : preference;
}

export function parseAppearancePreferences(raw: string | null | undefined): AppearancePreferences {
  if (!raw) return { ...DEFAULT_APPEARANCE };
  try {
    const value: unknown = JSON.parse(raw);
    if (!value || typeof value !== "object") return { ...DEFAULT_APPEARANCE };
    const record = value as Record<string, unknown>;
    return {
      theme: resolveThemePreference(record.theme),
      density: resolveDensityPreference(record.density),
      accent: resolveAccentPreference(record.accent),
    };
  } catch {
    return { ...DEFAULT_APPEARANCE };
  }
}

/** Browser persistence is only for presentation preferences, never backend configuration. */
export function readAppearancePreferences(storage: Pick<Storage, "getItem"> | null | undefined): AppearancePreferences {
  try {
    return parseAppearancePreferences(storage?.getItem(APPEARANCE_STORAGE_KEY));
  } catch {
    return { ...DEFAULT_APPEARANCE };
  }
}

export function serializeAppearancePreferences(preferences: AppearancePreferences): string {
  return JSON.stringify(preferences);
}

export type AppearanceRootState = {
  className: "light" | "dark";
  theme: ResolvedTheme;
  density: DensityPreference;
  accent: AccentPreference;
};

export function appearanceRootState(
  preferences: AppearancePreferences,
  systemTheme: ResolvedTheme,
): AppearanceRootState {
  const theme = resolveEffectiveTheme(preferences.theme, systemTheme);
  return { className: theme, theme, density: preferences.density, accent: preferences.accent };
}
