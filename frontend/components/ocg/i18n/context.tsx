import type { JSX } from "@solidjs/web";
import { createContext, createSignal, useContext } from "solid-js";
import { dictionaries, formatTemplate, type TranslateFn } from "./dictionaries";
import { DEFAULT_LOCALE, detectLocale, isLocale, LOCALE_STORAGE_KEY, type Locale } from "./locale";

export type { TranslateFn } from "./dictionaries";

type I18nContextValue = {
  locale: Locale;
  setLocale: (locale: Locale) => void;
  t: TranslateFn;
  hydrated: boolean;
};

const I18nContext = createContext<I18nContextValue>();

export function I18nProvider(props: { children: JSX.Element }) {
  const [locale, setLocale] = createSignal(
    detectLocale(window.localStorage, window.navigator.language),
  );
  const value: I18nContextValue = {
    get locale() {
      return locale();
    },
    hydrated: true,
    t: (key, vars) =>
      formatTemplate(dictionaries[locale()][key] ?? dictionaries[DEFAULT_LOCALE][key] ?? key, vars),
    setLocale(next) {
      if (!isLocale(next)) return;
      setLocale(next);
      document.documentElement.lang = next;
      try {
        window.localStorage.setItem(LOCALE_STORAGE_KEY, next);
      } catch {}
    },
  };
  return <I18nContext value={value}>{props.children}</I18nContext>;
}

export function useI18n() {
  const context = useContext(I18nContext);
  if (!context) throw new Error("useI18n must be used inside I18nProvider");
  return context;
}
