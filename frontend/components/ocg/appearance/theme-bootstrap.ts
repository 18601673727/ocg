import { APPEARANCE_STORAGE_KEY, DEFAULT_APPEARANCE } from "./theme-domain";

// Apply stored appearance before rendering with the defaults shared by the theme provider.
export const THEME_BOOTSTRAP_SCRIPT = `(function () {
  var fallback = ${JSON.stringify(DEFAULT_APPEARANCE)};
  var theme = fallback.theme;
  var accent = fallback.accent;
  try {
    var raw = window.localStorage.getItem(${JSON.stringify(APPEARANCE_STORAGE_KEY)});
    var saved = raw ? JSON.parse(raw) : null;
    if (saved) {
      if (saved.theme === "light" || saved.theme === "dark") theme = saved.theme;
      if (saved.accent === "ochre" || saved.accent === "slate" || saved.accent === "teal") accent = saved.accent;
    }
  } catch {}
  var root = document.documentElement;
  root.classList.toggle("dark", theme === "dark");
  root.dataset.theme = theme;
  root.dataset.accent = accent;
})();`;
