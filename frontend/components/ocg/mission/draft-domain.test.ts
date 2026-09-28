import assert from "node:assert/strict";
import { test } from "node:test";
import {
  DEFAULT_RESOURCE_COMMITMENT,
  FIXTURE_RECOMMENDED_BUDGET_MICROS,
  HARD_BUDGET_MIN_MICROS,
  createMissionDraft,
  draftScopeKey,
  microsToUsd,
  missionDraftHasErrors,
  missionDraftId,
  missionDraftReducer,
  toMissionLaunchCommand,
  usdToMicros,
  validateMissionDraft,
  type MissionDraft,
} from "./draft-domain";

function validDraft(projectId = "zhuju", sessionId = "design-pwa-shell"): MissionDraft {
  return createMissionDraft({
    projectId,
    sessionId,
    objective: "Ship the P0 Mission draft vertical slice",
    successCriteria: "Chat opens a draft",
  });
}

test("creates a stable default draft with an honest fixture budget recommendation", () => {
  const draft = createMissionDraft({ projectId: "zhuju", sessionId: "design-pwa-shell" });

  assert.equal(draft.id, missionDraftId("zhuju", "design-pwa-shell"));
  assert.equal(draft.projectId, "zhuju");
  assert.equal(draft.sessionId, "design-pwa-shell");
  assert.equal(draft.lifecycle, "drafting");
  assert.equal(draft.hardBudgetMicros, FIXTURE_RECOMMENDED_BUDGET_MICROS);
  assert.equal(draft.hardBudgetSource, "fixture-recommended");
  assert.equal(draft.resourceCommitment, DEFAULT_RESOURCE_COMMITMENT);

  // Default budget is the existing $25 fixture cap, explicitly fixture-sourced.
  assert.equal(microsToUsd(draft.hardBudgetMicros!), 25);
  assert.equal(Number.isInteger(microsToUsd(draft.hardBudgetMicros!)), true);

  // Scope keys isolate Projects deterministically.
  assert.equal(draftScopeKey("zhuju", "design-pwa-shell"), "zhuju\u0000design-pwa-shell");
  assert.notEqual(draftScopeKey("zhuju", "design-pwa-shell"), draftScopeKey("route-lace", "design-pwa-shell"));
});

test("seeds trimmed objective text and converts dollars to micros carefully", () => {
  const seeded = createMissionDraft({
    projectId: "zhuju",
    sessionId: "design-pwa-shell",
    objective: "  Verify the mission draft  ",
  });
  assert.equal(seeded.objective, "Verify the mission draft");

  assert.equal(usdToMicros(25), 25_000_000);
  assert.equal(usdToMicros(24.5), 24_500_000);
  assert.equal(usdToMicros(0.01), 10_000);
  assert.equal(usdToMicros(Number.NaN), null);
  assert.equal(microsToUsd(HARD_BUDGET_MIN_MICROS), 0.000001);
});

test("validation reports objective, criteria, project, and session issues", () => {
  const empty = createMissionDraft({ projectId: "zhuju", sessionId: "design-pwa-shell" });
  const codes = validateMissionDraft(empty).map((issue) => issue.code);
  assert.ok(codes.includes("objective-required"));
  assert.ok(codes.includes("success-criteria-required"));

  const short = createMissionDraft({ projectId: "zhuju", sessionId: "design-pwa-shell", objective: "short", successCriteria: "ok" });
  assert.ok(validateMissionDraft(short).some((issue) => issue.code === "objective-too-short"));

  const badProject = createMissionDraft({ projectId: "not-a-project", sessionId: "design-pwa-shell", objective: "A sufficiently long objective", successCriteria: "ok" });
  assert.ok(validateMissionDraft(badProject).some((issue) => issue.code === "project-invalid"));

  const emptySession = createMissionDraft({ projectId: "zhuju", sessionId: "   ", objective: "A sufficiently long objective", successCriteria: "ok" });
  assert.ok(validateMissionDraft(emptySession).some((issue) => issue.code === "session-required"));

  assert.deepEqual(validateMissionDraft(validDraft()), []);
});

test("validates monetary values and resource commitment bounds", () => {
  const nonInteger = createMissionDraft({ projectId: "zhuju", sessionId: "design-pwa-shell", objective: "A sufficiently long objective", successCriteria: "ok", hardBudgetMicros: 1_500_000.5 });
  assert.ok(validateMissionDraft(nonInteger).some((issue) => issue.code === "budget-not-integer"));

  const unsafe = createMissionDraft({ projectId: "zhuju", sessionId: "design-pwa-shell", objective: "A sufficiently long objective", successCriteria: "ok", hardBudgetMicros: Number.MAX_SAFE_INTEGER + 2 });
  assert.ok(validateMissionDraft(unsafe).some((issue) => issue.code === "budget-not-safe-integer"));

  const nullBudget = createMissionDraft({ projectId: "zhuju", sessionId: "design-pwa-shell", objective: "A sufficiently long objective", successCriteria: "ok", hardBudgetMicros: null });
  assert.ok(validateMissionDraft(nullBudget).some((issue) => issue.code === "budget-required"));

  const tooLow = createMissionDraft({ projectId: "zhuju", sessionId: "design-pwa-shell", objective: "A sufficiently long objective", successCriteria: "ok", hardBudgetMicros: HARD_BUDGET_MIN_MICROS - 1 });
  assert.ok(validateMissionDraft(tooLow).some((issue) => issue.code === "budget-below-minimum"));

  const negativeCommitment = missionDraftReducer(validDraft(), { type: "set-resource-commitment", value: -0.1 });
  assert.ok(negativeCommitment.issues.some((issue) => issue.code === "resource-commitment-out-of-range"));

  const overCommitment = missionDraftReducer(validDraft(), { type: "set-resource-commitment", value: 1.1 });
  assert.ok(overCommitment.issues.some((issue) => issue.code === "resource-commitment-out-of-range"));

  const atBounds = missionDraftReducer(validDraft(), { type: "set-resource-commitment", value: 0 });
  assert.ok(!atBounds.issues.some((issue) => issue.code === "resource-commitment-out-of-range"));
  const atOne = missionDraftReducer(validDraft(), { type: "set-resource-commitment", value: 1 });
  assert.ok(!atOne.issues.some((issue) => issue.code === "resource-commitment-out-of-range"));
});

test("reducer moves through validate, start-launch, success, and failure", () => {
  const draft = validDraft();

  const ready = missionDraftReducer(draft, { type: "validate" });
  assert.equal(ready.lifecycle, "ready");
  assert.equal(missionDraftHasErrors(ready.issues), false);

  const launching = missionDraftReducer(ready, { type: "start-launch" });
  assert.equal(launching.lifecycle, "launching");
  assert.equal(launching.launchMessage, undefined);

  const launched = missionDraftReducer(launching, { type: "launch-succeeded", missionId: "mission-1", message: "accepted" });
  assert.equal(launched.lifecycle, "launched");
  assert.equal(launched.missionId, "mission-1");
  assert.deepEqual(launched.issues, []);

  const failed = missionDraftReducer(launching, { type: "launch-failed", message: "runtime offline" });
  assert.equal(failed.lifecycle, "launch-failed");
  assert.equal(failed.launchMessage, "runtime offline");
  assert.equal(failed.objective, draft.objective);
});

test("invalid drafts cannot start a launch and stay invalid", () => {
  const invalid = createMissionDraft({ projectId: "zhuju", sessionId: "design-pwa-shell" });
  const attempted = missionDraftReducer(invalid, { type: "start-launch" });
  assert.equal(attempted.lifecycle, "invalid");
  assert.equal(missionDraftHasErrors(attempted.issues), true);
  assert.equal(toMissionLaunchCommand(invalid), null);
});

test("editing after a failed launch returns to drafting and clears the message", () => {
  const failed = missionDraftReducer(
    missionDraftReducer(validDraft(), { type: "start-launch" }),
    { type: "launch-failed", message: "disconnected" },
  );
  assert.equal(failed.lifecycle, "launch-failed");

  const edited = missionDraftReducer(failed, { type: "update-field", field: "objective", value: "A revised objective for retry" });
  assert.equal(edited.lifecycle, "drafting");
  assert.equal(edited.launchMessage, undefined);
  assert.equal(edited.objective, "A revised objective for retry");
});

test("settled and in-flight transitions are idempotent", () => {
  const launching = missionDraftReducer(validDraft(), { type: "start-launch" });
  const failed = missionDraftReducer(launching, { type: "launch-failed", message: "disconnected" });

  assert.equal(missionDraftReducer(failed, { type: "launch-failed", message: "again" }), failed);
  assert.equal(missionDraftReducer(launching, { type: "update-field", field: "objective", value: "changed while launching" }), launching);
  assert.equal(missionDraftReducer(failed, { type: "launch-succeeded", missionId: "late" }), failed);
});

test("lifecycle consistency issues are explicit", () => {
  const launchedWithoutMission: MissionDraft = { ...validDraft(), lifecycle: "launched" };
  assert.ok(validateMissionDraft(launchedWithoutMission).some((issue) => issue.code === "launched-without-mission"));

  const failedWithoutMessage: MissionDraft = { ...validDraft(), lifecycle: "launch-failed" };
  assert.ok(validateMissionDraft(failedWithoutMessage).some((issue) => issue.code === "launch-failed-without-message"));
});

test("command conversion is scoped by Project and stable for identical content", () => {
  const zhuju = validDraft("zhuju", "design-pwa-shell");
  const routeLace = validDraft("route-lace", "research-space-bunny");

  const zhujuCommand = toMissionLaunchCommand(zhuju);
  const routeCommand = toMissionLaunchCommand(routeLace);
  assert.ok(zhujuCommand && routeCommand);
  assert.equal(zhujuCommand.commandId, toMissionLaunchCommand(validDraft("zhuju", "design-pwa-shell"))!.commandId);
  assert.notEqual(zhujuCommand.commandId, routeCommand.commandId);
  assert.equal(zhujuCommand.draftId, zhuju.id);
  assert.equal(routeCommand.projectId, "route-lace");

  // Same draft, same identity; edited content produces a new command identity.
  const edited = missionDraftReducer(zhuju, { type: "update-field", field: "objective", value: "A different enough objective" });
  const editedCommand = toMissionLaunchCommand(edited);
  assert.ok(editedCommand);
  assert.equal(editedCommand.draftId, zhujuCommand.draftId);
  assert.notEqual(editedCommand.commandId, zhujuCommand.commandId);
});

test("command conversion trims fields and refuses invalid drafts", () => {
  const draft = createMissionDraft({
    projectId: "zhuju",
    sessionId: "design-pwa-shell",
    objective: "  Ship the P0 Mission draft vertical slice  ",
    successCriteria: "  Chat opens a draft  ",
    constraints: "  No backend changes  ",
  });
  const command = toMissionLaunchCommand(draft);
  assert.ok(command);
  assert.equal(command.objective, "Ship the P0 Mission draft vertical slice");
  assert.equal(command.successCriteria, "Chat opens a draft");
  assert.equal(command.constraints, "No backend changes");

  assert.equal(toMissionLaunchCommand(createMissionDraft({ projectId: "zhuju", sessionId: "design-pwa-shell" })), null);
});
