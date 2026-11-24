import type { JSX } from "@solidjs/web";
import { createContext, createSignal, Show, useContext } from "solid-js";
import { type Dictionary, en, formatTemplate, type TranslateFn } from "./dictionaries";
import { DEFAULT_LOCALE, detectLocale, isLocale, LOCALE_STORAGE_KEY, type Locale } from "./locale";

export type { TranslateFn } from "./dictionaries";

type I18nContextValue = {
  locale: Locale;
  setLocale: (locale: Locale) => void;
  t: TranslateFn;
  hydrated: boolean;
};

const I18nContext = createContext<I18nContextValue>();

export function I18nProvider(props: { children: JSX.Element; fallback?: JSX.Element }) {
  const [locale, setLocale] = createSignal(
    detectLocale(window.localStorage, window.navigator.language),
  );
  const [chinese, setChinese] = createSignal<Dictionary>();
  const [ready, setReady] = createSignal(locale() !== "zh-CN");
  function loadChinese() {
    void import("./zh")
      .then(({ zh }) => setChinese(zh))
      .catch((error) => {
        console.warn("Could not load the Chinese dictionary.", error);
        setLocale(DEFAULT_LOCALE);
        document.documentElement.lang = "en";
      })
      .finally(() => setReady(true));
  }
  if (locale() === "zh-CN") loadChinese();
  const value: I18nContextValue = {
    get locale() {
      return locale();
    },
    hydrated: true,
    t: (key, vars) =>
      formatTemplate((locale() === "zh-CN" ? chinese()?.[key] : undefined) ?? en[key], vars),
    setLocale(next) {
      if (!isLocale(next)) return;
      if (next === "zh-CN" && !chinese()) loadChinese();
      setLocale(next);
      document.documentElement.lang = next;
      try {
        window.localStorage.setItem(LOCALE_STORAGE_KEY, next);
      } catch {}
    },
  };
  return (
    <Show when={ready()} fallback={props.fallback}>
      <I18nContext value={value}>{props.children}</I18nContext>
    </Show>
  );
}

export function useI18n() {
  const context = useContext(I18nContext);
  if (!context) throw new Error("useI18n must be used inside I18nProvider");
  return context;
}
