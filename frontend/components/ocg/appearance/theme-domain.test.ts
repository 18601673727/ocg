import assert from "node:assert/strict";
import { test } from "node:test";
import {
  APPEARANCE_STORAGE_KEY,
  appearanceRootState,
  DEFAULT_APPEARANCE,
  parseAppearancePreferences,
  readAppearancePreferences,
  resolveEffectiveTheme,
  resolveThemePreference,
  serializeAppearancePreferences,
  type AppearancePreferences,
} from "./theme-domain";

test("theme selection resolves explicit and system themes", () => {
  assert.equal(resolveThemePreference("dark"), "dark");
  assert.equal(resolveThemePreference("unexpected"), "system");
  assert.equal(resolveEffectiveTheme("system", "dark"), "dark");
  assert.equal(resolveEffectiveTheme("system", "light"), "light");
  assert.equal(resolveEffectiveTheme("light", "dark"), "light");
});

test("appearance storage is versioned and presentation-only", () => {
  const stored: AppearancePreferences = { theme: "dark", density: "compact", accent: "teal" };
  const storage = { getItem: (key: string) => key === APPEARANCE_STORAGE_KEY ? JSON.stringify(stored) : null };
  assert.deepEqual(readAppearancePreferences(storage), stored);
  assert.deepEqual(parseAppearancePreferences("not-json"), DEFAULT_APPEARANCE);
  assert.equal(JSON.parse(serializeAppearancePreferences(stored)).theme, "dark");
});

test("root state expresses light and dark classes without changing backend state", () => {
  const dark = appearanceRootState({ ...DEFAULT_APPEARANCE, theme: "dark" }, "light");
  const light = appearanceRootState(DEFAULT_APPEARANCE, "light");
  assert.deepEqual(dark, { className: "dark", theme: "dark", density: "comfortable", accent: "ochre" });
  assert.equal(light.className, "light");
});
