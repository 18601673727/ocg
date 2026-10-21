import assert from "node:assert/strict";
import { decodeChildren, decodeCards, decodeAdmission, decodeConversationTurns, decodeConversations, decodeJobTree, decodeStop } from "../components/ocg/contracts/control-plane";

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

const tree = decodeJobTree({ api_version: 1, job: "job:1", ancestors: ["job:0"], root: {
  id: "job:1", kind: "conversation", parent: null, title: "root", state: "open", created_at_us: 1,
  agent: "profile:2", agent_name: "dispatcher", model: { llm_profile: "profile:3", model: "gpt" },
  on_hold: { by: { kind: "user" } }, origin: "agent", keep_open: true, active_turn: "job:2",
  recurrence: { interval_s: 60, phase: "active", due_us: null },
  conversation: null, previous: null, depends_on: [], children: [{
    id: "job:2", kind: "turn", parent: "job:1", title: "answer", state: "waiting", created_at_us: 2,
    agent: "profile:2", agent_name: "dispatcher", model: { llm_profile: "profile:3", model: "gpt" },
    on_hold: null, origin: null, keep_open: null, active_turn: null, recurrence: null,
    conversation: "job:1", previous: null,
    depends_on: [{ job: "job:4", condition: "settled", state: "waiting" }],
    children: null, has_more_children: false, next_after: null,
  }, {
    id: "job:5", kind: "conversation", parent: "job:1", title: "child", state: "open", created_at_us: 3,
    agent: "profile:2", agent_name: "dispatch target", model: null,
    on_hold: null, origin: "agent", keep_open: false, active_turn: null, recurrence: null,
    conversation: null, previous: null, depends_on: [],
    children: null, has_more_children: true, next_after: null,
  }], has_more_children: false, next_after: null,
} }, "job:1");
assert.equal(tree.root.children?.[0].depends_on[0].job, "job:4");
assert.equal(tree.root.children?.[0].depends_on[0].state, "waiting");
assert.equal(tree.root.children?.[0].state, "waiting");
assert.equal(tree.root.children?.[1].kind, "conversation");
assert.equal(tree.root.children?.[1].children, null);
assert.equal(tree.root.children?.[1].has_more_children, true);
assert.equal(tree.ancestors[0], "job:0");
assert.throws(() => decodeJobTree({ api_version: 1, job: "job:2", ancestors: [], root: tree.root }, "job:1"));

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
