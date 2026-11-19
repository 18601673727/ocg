import type { JSX } from "@solidjs/web";
import { createContext, createEffect, createSignal, useContext } from "solid-js";
import {
  type AccentPreference,
  APPEARANCE_STORAGE_KEY,
  type AppearancePreferences,
  DEFAULT_APPEARANCE,
  readAppearancePreferences,
  serializeAppearancePreferences,
  type ThemePreference,
} from "./theme-domain";

const ThemeContext = createContext<{
  preferences: AppearancePreferences;
  hydrated: boolean;
  setTheme: (theme: ThemePreference) => void;
  setAccent: (accent: AccentPreference) => void;
  resetAppearance: () => void;
}>();

export function ThemeProvider(props: { children: JSX.Element }) {
  const [preferences, setPreferences] = createSignal(readAppearancePreferences());
  createEffect(preferences, (appearance) => {
    const root = document.documentElement;
    root.classList.toggle("dark", appearance.theme === "dark");
    root.dataset.theme = appearance.theme;
    root.dataset.accent = appearance.accent;
    try {
      localStorage.setItem(APPEARANCE_STORAGE_KEY, serializeAppearancePreferences(appearance));
    } catch {}
  });
  return (
    <ThemeContext
      value={{
        get preferences() {
          return preferences();
        },
        hydrated: true,
        setTheme: (theme) => setPreferences((previous) => ({ ...previous, theme })),
        setAccent: (accent) => setPreferences((previous) => ({ ...previous, accent })),
        resetAppearance: () => setPreferences({ ...DEFAULT_APPEARANCE }),
      }}
    >
      {props.children}
    </ThemeContext>
  );
}

export function useTheme() {
  const theme = useContext(ThemeContext);
  if (!theme) throw new Error("useTheme must be used inside ThemeProvider");
  return theme;
}
