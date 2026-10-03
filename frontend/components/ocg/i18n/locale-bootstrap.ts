import { DEFAULT_LOCALE, LOCALE_STORAGE_KEY } from "./locale";

/**
 * Applies the stored locale to <html lang> before first paint, mirroring the
 * theme bootstrap. Plain JS inlined into HTML; resolvers stay in locale.ts.
 */
export const LOCALE_BOOTSTRAP_SCRIPT = `(function () {
  try {
    var stored = ${JSON.stringify(LOCALE_STORAGE_KEY)};
    var fallback = ${JSON.stringify(DEFAULT_LOCALE)};
    var raw = window.localStorage.getItem(stored);
    var value = raw ? raw.replace(/^"|"$/g, "") : "";
    var normalized = String(value || "").replace("_", "-").toLowerCase();
    var locale = fallback;
    if (normalized === "zh-cn" || normalized === "zh-hans" || normalized === "zh" || normalized.indexOf("zh-") === 0) {
      locale = "zh-CN";
    } else if (normalized.indexOf("en") === 0) {
      locale = "en-US";
    } else if (!value) {
      var nav = (window.navigator.language || "").replace("_", "-").toLowerCase();
      if (nav === "zh-cn" || nav === "zh-hans" || nav === "zh" || nav.indexOf("zh-") === 0) locale = "zh-CN";
      else locale = "en-US";
    }
    document.documentElement.lang = locale;
  } catch (error) {
    document.documentElement.lang = "en-US";
  }
})();`;
