"use client";

import { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import {
  APPEARANCE_STORAGE_KEY,
  DEFAULT_APPEARANCE,
  readAppearancePreferences,
  serializeAppearancePreferences,
  type AppearancePreferences,
  type AccentPreference,
  type ThemePreference,
} from "./theme-domain";

type ThemeContextValue = {
  preferences: AppearancePreferences;
  hydrated: boolean;
  setTheme: (theme: ThemePreference) => void;
  setAccent: (accent: AccentPreference) => void;
  resetAppearance: () => void;
};

const ThemeContext = createContext<ThemeContextValue | null>(null);

function applyRootAppearance({ theme, accent }: AppearancePreferences) {
  const root = document.documentElement;
  root.classList.toggle("dark", theme === "dark");
  root.dataset.theme = theme;
  root.dataset.accent = accent;
}

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [preferences, setPreferences] = useState<AppearancePreferences>(() => ({
    ...DEFAULT_APPEARANCE,
  }));
  const [hydrated, setHydrated] = useState(false);

  useEffect(() => {
    const loadTimer = window.setTimeout(() => {
      const stored = readAppearancePreferences();
      setPreferences(stored);
      setHydrated(true);
    }, 0);
    return () => window.clearTimeout(loadTimer);
  }, []);

  useEffect(() => {
    if (!hydrated) return;
    applyRootAppearance(preferences);
    try {
      window.localStorage.setItem(
        APPEARANCE_STORAGE_KEY,
        serializeAppearancePreferences(preferences),
      );
    } catch {}
  }, [hydrated, preferences]);

  const setTheme = useCallback(
    (theme: ThemePreference) => setPreferences((current) => ({ ...current, theme })),
    [],
  );
  const setAccent = useCallback(
    (accent: AccentPreference) => setPreferences((current) => ({ ...current, accent })),
    [],
  );
  const resetAppearance = useCallback(() => setPreferences({ ...DEFAULT_APPEARANCE }), []);
  const value = useMemo(
    () => ({
      preferences,
      hydrated,
      setTheme,
      setAccent,
      resetAppearance,
    }),
    [hydrated, preferences, resetAppearance, setAccent, setTheme],
  );

  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>;
}

export function useTheme(): ThemeContextValue {
  const value = useContext(ThemeContext);
  if (!value) throw new Error("useTheme must be used inside ThemeProvider");
  return value;
}
