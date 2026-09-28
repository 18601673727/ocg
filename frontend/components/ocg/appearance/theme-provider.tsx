"use client";

import { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import {
  APPEARANCE_STORAGE_KEY,
  appearanceRootState,
  DEFAULT_APPEARANCE,
  readAppearancePreferences,
  resolveEffectiveTheme,
  serializeAppearancePreferences,
  type AppearancePreferences,
  type AccentPreference,
  type DensityPreference,
  type ResolvedTheme,
  type ThemePreference,
} from "./theme-domain";

type ThemeContextValue = {
  preferences: AppearancePreferences;
  theme: ThemePreference;
  resolvedTheme: ResolvedTheme;
  density: DensityPreference;
  accent: AccentPreference;
  hydrated: boolean;
  setTheme: (theme: ThemePreference) => void;
  setDensity: (density: DensityPreference) => void;
  setAccent: (accent: AccentPreference) => void;
  resetAppearance: () => void;
};

const ThemeContext = createContext<ThemeContextValue | null>(null);

function applyRootAppearance(preferences: AppearancePreferences, systemTheme: ResolvedTheme) {
  const root = document.documentElement;
  const state = appearanceRootState(preferences, systemTheme);
  root.classList.toggle("dark", state.theme === "dark");
  root.dataset.theme = state.theme;
  root.dataset.density = state.density;
  root.dataset.accent = state.accent;
}

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [preferences, setPreferences] = useState<AppearancePreferences>(() => ({ ...DEFAULT_APPEARANCE }));
  const [systemTheme, setSystemTheme] = useState<ResolvedTheme>("light");
  const [hydrated, setHydrated] = useState(false);

  useEffect(() => {
    const media = window.matchMedia("(prefers-color-scheme: dark)");
    const stored = readAppearancePreferences(window.localStorage);
    const system = media.matches ? "dark" : "light";
    const loadTimer = window.setTimeout(() => {
      setPreferences(stored);
      setSystemTheme(system);
      applyRootAppearance(stored, system);
      setHydrated(true);
    }, 0);

    const onSystemThemeChange = (event: MediaQueryListEvent) => {
      const next = event.matches ? "dark" : "light";
      setSystemTheme(next);
      setPreferences((current) => {
        applyRootAppearance(current, next);
        return current;
      });
    };
    media.addEventListener?.("change", onSystemThemeChange);
    return () => {
      window.clearTimeout(loadTimer);
      media.removeEventListener?.("change", onSystemThemeChange);
    };
  }, []);

  useEffect(() => {
    if (!hydrated) return;
    applyRootAppearance(preferences, systemTheme);
    try {
      // Only frontend presentation preferences are persisted here. Backend-
      // authoritative OCG configuration remains owned by the runtime boundary.
      window.localStorage.setItem(APPEARANCE_STORAGE_KEY, serializeAppearancePreferences(preferences));
    } catch {
      // Private browsing and denied storage should not block appearance changes.
    }
  }, [hydrated, preferences, systemTheme]);

  const setTheme = useCallback((theme: ThemePreference) => setPreferences((current) => ({ ...current, theme })), []);
  const setDensity = useCallback((density: DensityPreference) => setPreferences((current) => ({ ...current, density })), []);
  const setAccent = useCallback((accent: AccentPreference) => setPreferences((current) => ({ ...current, accent })), []);
  const resetAppearance = useCallback(() => setPreferences({ ...DEFAULT_APPEARANCE }), []);
  const value = useMemo(() => ({
    preferences,
    theme: preferences.theme,
    resolvedTheme: resolveEffectiveTheme(preferences.theme, systemTheme),
    density: preferences.density,
    accent: preferences.accent,
    hydrated,
    setTheme,
    setDensity,
    setAccent,
    resetAppearance,
  }), [hydrated, preferences, resetAppearance, setAccent, setDensity, setTheme, systemTheme]);

  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>;
}

export function useTheme(): ThemeContextValue {
  const value = useContext(ThemeContext);
  if (!value) throw new Error("useTheme must be used inside ThemeProvider");
  return value;
}
