export { I18nProvider, useI18n, useT, type TranslateFn } from "./context";
export {
  DEFAULT_LOCALE,
  LOCALES,
  LOCALE_LABEL,
  LOCALE_SHORT_LABEL,
  LOCALE_STORAGE_KEY,
  detectLocale,
  isLocale,
  readStoredLocale,
  resolveLocale,
  type Locale,
} from "./locale";
export {
  dictionaries,
  en,
  zh,
  formatTemplate,
  type Dictionary,
  type I18nKey,
} from "./dictionaries";
export { LanguageSwitcher } from "./language-switcher";
export { LOCALE_BOOTSTRAP_SCRIPT } from "./locale-bootstrap";
