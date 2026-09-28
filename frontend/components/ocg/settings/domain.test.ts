import assert from "node:assert/strict";
import { test } from "node:test";
import { createBootstrapFixture } from "../bootstrap/fixtures";
import { createSettingsState, isBackendOwned, RECONFIGURE_PATH, selectSettingsSections } from "./domain";

const snapshot = {
  status: { state: "connected" as const, detail: "local mock runtime" },
  bootstrap: createBootstrapFixture("local-ready"),
};

test("normalized settings expose useful sections without inventing empty categories", () => {
  const sections = selectSettingsSections(createSettingsState(snapshot));
  assert.deepEqual(sections.map((section) => section.id), ["general", "runtime", "resources", "access", "appearance", "diagnostics", "advanced"]);
  assert.ok(sections.find((section) => section.id === "resources")?.items.some((item) => item.action === "reconfigure"));
});

test("resource setup reuses onboarding and backend-owned facts stay read-only", () => {
  assert.match(RECONFIGURE_PATH, /^\/onboarding\?/);
  const state = createSettingsState(snapshot);
  const access = state.sections.access[0];
  const setup = state.sections.resources[0];
  assert.equal(isBackendOwned(access), true);
  assert.equal(access.readOnly, true);
  assert.equal(setup.action, "reconfigure");
  assert.equal(setup.readOnly, true);
  assert.equal(state.sections.appearance[0].readOnly, false);
});
