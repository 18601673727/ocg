import assert from "node:assert/strict";
import { pageTurns } from "../components/ocg/runtime/history-page";

const turns = [3, 2, 1].map(id => ({ job: { id: `job:${id}` } }));
const page = { turns, has_more: true, next_before: "job:1" };
assert.deepEqual(pageTurns(new Set(), undefined, page), { add: [...turns].reverse(), next: "job:1" });
assert.deepEqual(page.turns.map(turn => turn.job.id), ["job:3", "job:2", "job:1"]);

const tip = { job: { id: "job:4" } };
assert.deepEqual(pageTurns(new Set([tip.job.id]), "job:3", { ...page, turns: [tip, ...turns] }).add, [...turns].reverse());
const held = new Set(["job:3", "job:2"]);
assert.deepEqual(pageTurns(held, "job:2", page), { add: [turns[2]], next: "job:1" });

assert.equal(pageTurns(new Set(), "job:4", { ...page, has_more: false }).next, null);

assert.throws(() => pageTurns(new Set(), undefined, { ...page, next_before: null }), { message: "Conversation turns returned no advancing cursor." });
assert.throws(() => pageTurns(new Set(), "job:1", page), { message: "Conversation turns returned no advancing cursor." });

console.log("history paging: 4 tests passed");
