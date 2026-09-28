import assert from "node:assert/strict";
import { test } from "node:test";
import {
  COMPOSER_SUGGESTIONS,
  applyComposerSuggestion,
  dispatchComposerIntent,
  matchComposerSuggestions,
  moveComposerSuggestionIndex,
  parseComposerIntent,
  parseComposerSuggestionQuery,
  resolveComposerIntent,
} from "./domain";

test("normal text stays a chat intent", () => {
  const intent = parseComposerIntent("  Summarize the open decisions  ");
  assert.equal(intent.kind, "chat");
  if (intent.kind !== "chat") return;
  assert.equal(intent.text, "Summarize the open decisions");
  assert.equal(intent.source, "plain-text");
});

test("parseComposerIntent recognizes /mission create with and without a seed", () => {
  const bare = parseComposerIntent("/mission create");
  assert.equal(bare.kind, "mission.create");
  if (bare.kind !== "mission.create") return;
  assert.equal(bare.seed, undefined);

  const seeded = parseComposerIntent("/mission create ship the P0 slice\nwith care");
  assert.equal(seeded.kind, "mission.create");
  if (seeded.kind !== "mission.create") return;
  assert.equal(seeded.seed, "ship the P0 slice\nwith care");

  const commandCase = parseComposerIntent("/Mission Create Fix the composer");
  assert.equal(commandCase.kind, "mission.create");
  if (commandCase.kind !== "mission.create") return;
  assert.equal(commandCase.seed, "Fix the composer");
});

test("malformed slash commands are explicit unknown-command results", () => {
  const incomplete = parseComposerIntent("/mission");
  assert.equal(incomplete.kind, "unknown-command");
  if (incomplete.kind !== "unknown-command") return;
  assert.equal(incomplete.command, "/mission");
  assert.match(incomplete.reason, /insufficient|incomplete|create/i);

  const unknown = parseComposerIntent("/deploy now");
  assert.equal(unknown.kind, "unknown-command");
  if (unknown.kind !== "unknown-command") return;
  assert.equal(unknown.command, "/deploy");
  assert.match(unknown.reason, /Unknown command \/deploy/);
});

test("suggestion queries are active only for a single slash token", () => {
  assert.deepEqual(parseComposerSuggestionQuery("hello"), { active: false, query: "" });
  assert.deepEqual(parseComposerSuggestionQuery("/"), { active: true, query: "" });
  assert.deepEqual(parseComposerSuggestionQuery("/mission"), { active: true, query: "mission" });
  assert.deepEqual(parseComposerSuggestionQuery("/mission create foo"), { active: true, query: "mission create foo" });
  assert.equal(parseComposerSuggestionQuery("/mission\ncreate").active, false);
});

test("'/' and '/mission' return Mission, Create Mission, and /mission create", () => {
  const fromSlash = matchComposerSuggestions("/").map((item) => item.label);
  const fromMission = matchComposerSuggestions("/mission").map((item) => item.label);
  assert.deepEqual(fromSlash, ["Mission", "Create Mission", "/mission create"]);
  assert.deepEqual(fromMission, ["Mission", "Create Mission", "/mission create"]);
  assert.equal(COMPOSER_SUGGESTIONS.length, 3);
});

test("matching narrows deterministically and never returns unrelated commands", () => {
  const createOnly = matchComposerSuggestions("/mission create");
  assert.ok(createOnly.length >= 1);
  assert.ok(createOnly.every((item) => item.action === "create-mission"));
  assert.deepEqual(matchComposerSuggestions("/zzz"), []);
  assert.deepEqual(matchComposerSuggestions("plain text"), []);
  assert.deepEqual(matchComposerSuggestions("/mission", 1).map((item) => item.label), ["Mission"]);
});

test("keyboard navigation wraps and stays empty for no suggestions", () => {
  assert.equal(moveComposerSuggestionIndex(-1, 1, 3), 0);
  assert.equal(moveComposerSuggestionIndex(-1, -1, 3), 2);
  assert.equal(moveComposerSuggestionIndex(2, 1, 3), 0);
  assert.equal(moveComposerSuggestionIndex(0, -1, 3), 2);
  assert.equal(moveComposerSuggestionIndex(0, 1, 0), -1);
});

test("applying a suggestion exposes either insert text or a create action", () => {
  const [mission, create, createCommand] = COMPOSER_SUGGESTIONS;
  assert.deepEqual(applyComposerSuggestion(mission), { action: "insert", text: "/mission " });
  assert.deepEqual(applyComposerSuggestion(create), { action: "create-mission", text: "/mission create" });
  assert.deepEqual(applyComposerSuggestion(createCommand), { action: "create-mission", text: "/mission create" });
});

test("resolveComposerIntent uses an optional resolver before the deterministic fallback", () => {
  const fallback = resolveComposerIntent("hello");
  assert.equal(fallback.kind, "chat");

  const resolved = resolveComposerIntent("do the thing", () => ({
    kind: "mission.create",
    seed: "do the thing",
    raw: "do the thing",
    source: "resolver",
  }));
  assert.equal(resolved.kind, "mission.create");
  if (resolved.kind !== "mission.create") return;
  assert.equal(resolved.source, "resolver");
});

test("dispatches intents through the command handler boundary", () => {
  const seen: string[] = [];
  dispatchComposerIntent(parseComposerIntent("hello"), {
    chat: (intent) => seen.push(`chat:${intent.text}`),
    "mission.create": (intent) => seen.push(`mission:${intent.seed ?? ""}`),
  });
  dispatchComposerIntent(parseComposerIntent("/mission create inspect the runtime"), {
    chat: (intent) => seen.push(`chat:${intent.text}`),
    "mission.create": (intent) => seen.push(`mission:${intent.seed ?? ""}`),
  });
  assert.deepEqual(seen, ["chat:hello", "mission:inspect the runtime"]);
});
