import assert from "node:assert/strict";
import { test } from "node:test";
import { deriveScenarioWorkspaceView, resolveWorkspaceView } from "./view-domain";

test("explicit workspace views preserve the current runtime scenario", () => {
  assert.equal(resolveWorkspaceView("normal-chat", "mission-control"), "mission-control");
  assert.equal(resolveWorkspaceView("normal-chat", "not-a-view"), "chat");
});

test("scenario defaults remain unchanged when no view is requested", () => {
  assert.equal(deriveScenarioWorkspaceView("home-overview"), "home");
  assert.equal(deriveScenarioWorkspaceView("attention-calm"), "attention");
  assert.equal(deriveScenarioWorkspaceView("profiles-models"), "control-center");
  assert.equal(deriveScenarioWorkspaceView("resource-ledger"), "ledger");
  assert.equal(deriveScenarioWorkspaceView("logs-live"), "logs");
});
