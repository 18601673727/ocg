import assert from "node:assert/strict";
import { decodeChildren, decodeCards, decodeAdmission, decodeConversationTurns, decodeConversations, decodeStop } from "../components/ocg/contracts/control-plane";

const admission = decodeAdmission({ api_version: 1, admission: { status: "admitted", job: "job:3", conversation: "job:1", seq: 2, joined: true, model: "frozen", kept: false } });
assert.equal(admission.conversation, "job:1");
assert.equal(admission.joined, true);
assert.equal(admission.seq, 2);
assert.equal(admission.model, "frozen");
assert.equal(decodeStop({ api_version: 1, conversation: "job:1", status: "requested", record_seq: 1 }, "job:1").record_seq, 1);
assert.equal(decodeConversationTurns({ api_version: 1, conversation: "job:1", turns: [], has_more: false, next_before: null }, "job:1").turns.length, 0);
const list = decodeConversations({ api_version: 1, conversations: [{ root_job: "job:1", tip_job: "job:3", agent: null, title: "chat", state: "open", on_hold: { by: { kind: "user" } }, project: null, tip_settlement_state: "unsettled", tip_created_at_us: 1 }], has_more: false, next_before: null });
assert.equal(list.conversations[0].on_hold?.by.kind, "user");
assert.equal(list.conversations[0].state, "open");
assert.throws(() => decodeConversationTurns({ api_version: 1, conversation: "job:2", turns: [], has_more: false, next_before: null }, "job:1"));
const children = decodeChildren({ api_version: 1, conversation: "job:1", children: [{ id: "job:4", title: "Recurring child", agent: "profile:2", state: "open", on_hold: { by: { kind: "loop" } }, keep_open: true, recurrence: { interval_s: 60, phase: "waiting_due", due_us: 123 } }], has_more: true, next_before: "job:4" }, "job:1");
assert.equal(children.children[0].recurrence?.interval_s, 60);
assert.equal(children.children[0].on_hold?.by.kind, "loop");
assert.equal(children.next_before, "job:4");
assert.throws(() => decodeChildren({ api_version: 1, conversation: "job:2", children: [], has_more: false, next_before: null }, "job:1"));
assert.throws(() => decodeChildren({ api_version: 1, conversation: "job:1", children: [{ ...children.children[0], recurrence: { interval_s: 1, phase: "active", due_us: null } }], has_more: false, next_before: null }, "job:1"));

const cards = decodeCards({ api_version: 1, conversation: "job:1", cards: [
  { kind: "approval", conversation: "job:5", parent: "job:3", approval: [6, 1, 0], tool: "workspace_run_command", arguments: "{}" },
  { kind: "question", conversation: "job:5", parent: "job:3", approval: [6, 1, 1], tool: "ask_user", question: "Which branch?" },
  { kind: "loop_hold", conversation: "job:3", parent: "job:1" },
  { kind: "quarantine", conversation: "job:5", parent: "job:3", turn: "job:8" },
] }, "job:1");
assert.equal(cards.cards.length, 4);
assert.equal(cards.cards[0].parent, "job:3");
assert.throws(() => decodeCards({ api_version: 1, conversation: "job:2", cards: [] }, "job:1"));
assert.throws(() => decodeCards({ api_version: 1, conversation: "job:1", cards: [{ kind: "approval", conversation: "job:5", parent: null, approval: [6, 0, 0], tool: "x", arguments: "{}" }] }, "job:1"));

console.log("conversation contract: 3 tests passed");
