import assert from "node:assert/strict";
import { pageTurns } from "../components/ocg/runtime/history-page";

const turns = [
  { job: { id: "00000003-0000-7001-8000-000000000003" } },
  { job: { id: "00000002-0000-7001-8000-000000000002" } },
  { job: { id: "00000001-0000-7001-8000-000000000001" } },
];
const page = { turns, has_more: true, next_before: "00000001-0000-7001-8000-000000000001" };
assert.deepEqual(pageTurns(new Set(), undefined, page), { add: [...turns].reverse(), next: "00000001-0000-7001-8000-000000000001" });
assert.deepEqual(page.turns.map(turn => turn.job.id), ["00000003-0000-7001-8000-000000000003", "00000002-0000-7001-8000-000000000002", "00000001-0000-7001-8000-000000000001"]);

const tip = { job: { id: "00000004-0000-7001-8000-000000000004" } };
assert.deepEqual(pageTurns(new Set([tip.job.id]), "00000003-0000-7001-8000-000000000003", { ...page, turns: [tip, ...turns] }).add, [...turns].reverse());
const held = new Set(["00000003-0000-7001-8000-000000000003", "00000002-0000-7001-8000-000000000002"]);
assert.deepEqual(pageTurns(held, "00000002-0000-7001-8000-000000000002", page), { add: [turns[2]], next: "00000001-0000-7001-8000-000000000001" });

assert.equal(pageTurns(new Set(), "00000004-0000-7001-8000-000000000004", { ...page, has_more: false }).next, null);

assert.throws(() => pageTurns(new Set(), undefined, { ...page, next_before: null }), { message: "Conversation turns returned no advancing cursor." });
assert.throws(() => pageTurns(new Set(), "00000001-0000-7001-8000-000000000001", page), { message: "Conversation turns returned no advancing cursor." });

console.log("history paging: 4 tests passed");
