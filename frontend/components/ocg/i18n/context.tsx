"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import {
  DEFAULT_LOCALE,
  detectLocale,
  isLocale,
  LOCALE_STORAGE_KEY,
  type Locale,
} from "./locale";
import {
  dictionaries,
  formatTemplate,
  type I18nKey,
} from "./dictionaries";

export type TranslateVars = Record<string, string | number>;

export type TranslateFn = (key: I18nKey, vars?: TranslateVars) => string;

type I18nContextValue = {
  locale: Locale;
  setLocale: (locale: Locale) => void;
  t: TranslateFn;
  hydrated: boolean;
};

const I18nContext = createContext<I18nContextValue | null>(null);

function translate(locale: Locale, key: I18nKey, vars?: TranslateVars): string {
  const table = dictionaries[locale] ?? dictionaries[DEFAULT_LOCALE];
  const fallback = dictionaries[DEFAULT_LOCALE];
  const template = table[key] ?? fallback[key] ?? key;
  return formatTemplate(template, vars);
}

function applyDocumentLocale(locale: Locale) {
  if (typeof document === "undefined") return;
  document.documentElement.lang = locale;
}

export function I18nProvider({ children }: { children: ReactNode }) {
  const [locale, setLocaleState] = useState<Locale>(() => DEFAULT_LOCALE);
  const [hydrated, setHydrated] = useState(false);

  useEffect(() => {
    // Defer hydration like ThemeProvider so the initial paint stays
    // deterministic and eslint's set-state-in-effect rule stays satisfied.
    const loadTimer = window.setTimeout(() => {
      const detected = detectLocale(window.localStorage, window.navigator.language);
      setLocaleState(detected);
      applyDocumentLocale(detected);
      setHydrated(true);
    }, 0);
    return () => window.clearTimeout(loadTimer);
  }, []);

  const setLocale = useCallback((next: Locale) => {
    if (!isLocale(next)) return;
    setLocaleState(next);
    applyDocumentLocale(next);
    try {
      window.localStorage.setItem(LOCALE_STORAGE_KEY, next);
    } catch {
      // Private browsing should not block a language change.
    }
  }, []);

  const t = useCallback<TranslateFn>(
    (key, vars) => translate(locale, key, vars),
    [locale],
  );

  const value = useMemo(
    () => ({ locale, setLocale, t, hydrated }),
    [locale, setLocale, t, hydrated],
  );

  return <I18nContext.Provider value={value}>{children}</I18nContext.Provider>;
}

export function useI18n(): I18nContextValue {
  const value = useContext(I18nContext);
  if (!value) throw new Error("useI18n must be used inside I18nProvider");
  return value;
}

/** Shorthand for components that only need the translate function. */
export function useT(): TranslateFn {
  return useI18n().t;
}
