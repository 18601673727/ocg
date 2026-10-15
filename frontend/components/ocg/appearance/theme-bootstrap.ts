import { APPEARANCE_STORAGE_KEY, DEFAULT_APPEARANCE } from "./theme-domain";

/**
 * The script that applies stored appearance to the document before hydration.
 *
 * It has to be plain JavaScript inlined into the HTML, so it cannot call the
 * resolvers in `theme-domain` at runtime. Keeping it here instead of as a blob
 * inside `app/layout.tsx` means the storage key and the defaults it falls back to
 * are interpolated from the domain rather than typed out a second time, and the
 * script reads as code.
 */
export const THEME_BOOTSTRAP_SCRIPT = `(function () {
  try {
    var stored = ${JSON.stringify(APPEARANCE_STORAGE_KEY)};
    var fallback = ${JSON.stringify(DEFAULT_APPEARANCE)};
    var raw = window.localStorage.getItem(stored);
    var saved = raw ? JSON.parse(raw) : null;
    var mode = saved && (saved.theme === "light" || saved.theme === "dark" || saved.theme === "system")
      ? saved.theme
      : fallback.theme;
    var dark = mode === "dark" || (mode === "system" && window.matchMedia("(prefers-color-scheme: dark)").matches);
    var root = document.documentElement;
    root.classList.toggle("dark", dark);
    root.dataset.theme = dark ? "dark" : "light";
    if (saved) {
      root.dataset.density = saved.density || fallback.density;
      root.dataset.accent = saved.accent || fallback.accent;
    }
  } catch (error) {
    document.documentElement.dataset.theme = "light";
  }
})();`;
