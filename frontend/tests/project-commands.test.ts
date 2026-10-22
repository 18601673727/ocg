import test from "node:test";
import assert from "node:assert/strict";
import {
  nextCommandDraftId,
  toCommandDrafts,
  toProjectCommands,
} from "../components/ocg/settings/command-drafts";

test("loaded commands receive stable identities in load order", () => {
  const drafts = toCommandDrafts([
    { name: "lint", argv: ["pnpm", "lint"] },
    { name: "build", argv: ["pnpm", "build"] },
  ]);
  assert.deepEqual(drafts.map((draft) => draft.id), ["command-1", "command-2"]);
  assert.equal(drafts[0].argvText, "pnpm\nlint");
});

test("added commands never collide with a live identity", () => {
  const drafts = toCommandDrafts([{ name: "a", argv: [] }]);
  assert.equal(nextCommandDraftId(drafts), "command-2");
  // Only the removed row is gone: its identity stays available, the live
  // row's identity is never reused.
  assert.equal(nextCommandDraftId([{ id: "command-2" }]), "command-3");
  assert.equal(nextCommandDraftId([{ id: "command-2" }, { id: "command-3" }]), "command-4");
});

test("removing an earlier row preserves the identity of the rest", () => {
  const drafts = toCommandDrafts([
    { name: "one", argv: ["1"] },
    { name: "two", argv: ["2"] },
    { name: "three", argv: ["3"] },
  ]);
  const remaining = drafts.filter((draft) => draft.id !== "command-1");
  assert.deepEqual(remaining.map((draft) => draft.id), ["command-2", "command-3"]);
  assert.deepEqual(remaining.map((draft) => draft.name), ["two", "three"]);
});

test("saving strips draft identities and preserves argv whitespace", () => {
  const saved = toProjectCommands([
    { id: "command-9", name: "  lint  ", argvText: "pnpm  --frozen\n\n  lint  \r\n   " },
  ]);
  assert.deepEqual(saved, [{ name: "lint", argv: ["pnpm  --frozen", "  lint  "] }]);
  assert.ok(!("id" in saved[0]));
});
