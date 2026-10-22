import assert from "node:assert/strict";
import { decodeChildren, decodeCards, decodeAdmission, decodeConversationTurns, decodeConversations, decodeJobTree, decodeStop } from "../components/ocg/contracts/control-plane";

const admission = decodeAdmission({ api_version: 1, admission: { status: "admitted", job: "00000003-0000-7001-8000-000000000003", conversation: "00000001-0000-7001-8000-000000000001", seq: 2, joined: true, model: "frozen", kept: false } });
assert.equal(admission.conversation, "00000001-0000-7001-8000-000000000001");
assert.equal(admission.joined, true);
assert.equal(admission.seq, 2);
assert.equal(admission.model, "frozen");
assert.equal(decodeStop({ api_version: 1, conversation: "00000001-0000-7001-8000-000000000001", status: "requested", record_seq: 1 }, "00000001-0000-7001-8000-000000000001").record_seq, 1);
assert.equal(decodeConversationTurns({ api_version: 1, conversation: "00000001-0000-7001-8000-000000000001", turns: [], has_more: false, next_before: null }, "00000001-0000-7001-8000-000000000001").turns.length, 0);
const list = decodeConversations({ api_version: 1, conversations: [{ root_job: "00000001-0000-7001-8000-000000000001", tip_job: "00000003-0000-7001-8000-000000000003", agent: null, title: "chat", state: "open", on_hold: { by: { kind: "user" } }, project: null, tip_settlement_state: "unsettled", tip_created_at_us: 1 }], has_more: false, next_before: null });
assert.equal(list.conversations[0].on_hold?.by.kind, "user");
assert.equal(list.conversations[0].state, "open");
assert.throws(() => decodeConversationTurns({ api_version: 1, conversation: "00000002-0000-7001-8000-000000000002", turns: [], has_more: false, next_before: null }, "00000001-0000-7001-8000-000000000001"));
const children = decodeChildren({ api_version: 1, conversation: "00000001-0000-7001-8000-000000000001", children: [{ id: "00000004-0000-7001-8000-000000000004", title: "Recurring child", agent: "00000002-0000-7002-8000-000000000002", state: "open", on_hold: { by: { kind: "loop" } }, keep_open: true, recurrence: { interval_s: 60, phase: "waiting_due", due_us: 123 } }], has_more: true, next_before: "00000004-0000-7001-8000-000000000004" }, "00000001-0000-7001-8000-000000000001");
assert.equal(children.children[0].recurrence?.interval_s, 60);
assert.equal(children.children[0].on_hold?.by.kind, "loop");
assert.equal(children.next_before, "00000004-0000-7001-8000-000000000004");
assert.throws(() => decodeChildren({ api_version: 1, conversation: "00000002-0000-7001-8000-000000000002", children: [], has_more: false, next_before: null }, "00000001-0000-7001-8000-000000000001"));
assert.throws(() => decodeChildren({ api_version: 1, conversation: "00000001-0000-7001-8000-000000000001", children: [{ ...children.children[0], recurrence: { interval_s: 1, phase: "active", due_us: null } }], has_more: false, next_before: null }, "00000001-0000-7001-8000-000000000001"));

const tree = decodeJobTree({ api_version: 1, job: "00000001-0000-7001-8000-000000000001", ancestors: ["00000000-0000-7001-8000-000000000000"], root: {
  id: "00000001-0000-7001-8000-000000000001", kind: "conversation", parent: null, title: "root", state: "open", created_at_us: 1,
  agent: "00000002-0000-7002-8000-000000000002", agent_name: "dispatcher", model: { llm_profile: "00000003-0000-7002-8000-000000000003", model: "gpt" },
  on_hold: { by: { kind: "user" } }, origin: "agent", keep_open: true, active_turn: "00000002-0000-7001-8000-000000000002",
  recurrence: { interval_s: 60, phase: "active", due_us: null },
  conversation: null, previous: null, depends_on: [], children: [{
    id: "00000002-0000-7001-8000-000000000002", kind: "turn", parent: "00000001-0000-7001-8000-000000000001", title: "answer", state: "waiting", created_at_us: 2,
    agent: "00000002-0000-7002-8000-000000000002", agent_name: "dispatcher", model: { llm_profile: "00000003-0000-7002-8000-000000000003", model: "gpt" },
    on_hold: null, origin: null, keep_open: null, active_turn: null, recurrence: null,
    conversation: "00000001-0000-7001-8000-000000000001", previous: null,
    depends_on: [{ job: "00000004-0000-7001-8000-000000000004", condition: "settled", state: "waiting" }],
    children: null, has_more_children: false, next_after: null,
  }, {
    id: "00000005-0000-7001-8000-000000000005", kind: "conversation", parent: "00000001-0000-7001-8000-000000000001", title: "child", state: "open", created_at_us: 3,
    agent: "00000002-0000-7002-8000-000000000002", agent_name: "dispatch target", model: null,
    on_hold: null, origin: "agent", keep_open: false, active_turn: null, recurrence: null,
    conversation: null, previous: null, depends_on: [],
    children: null, has_more_children: true, next_after: null,
  }], has_more_children: false, next_after: null,
} }, "00000001-0000-7001-8000-000000000001");
assert.equal(tree.root.children?.[0].depends_on[0].job, "00000004-0000-7001-8000-000000000004");
assert.equal(tree.root.children?.[0].depends_on[0].state, "waiting");
assert.equal(tree.root.children?.[0].state, "waiting");
assert.equal(tree.root.children?.[1].kind, "conversation");
assert.equal(tree.root.children?.[1].children, null);
assert.equal(tree.root.children?.[1].has_more_children, true);
assert.equal(tree.ancestors[0], "00000000-0000-7001-8000-000000000000");
assert.throws(() => decodeJobTree({ api_version: 1, job: "00000002-0000-7001-8000-000000000002", ancestors: [], root: tree.root }, "00000001-0000-7001-8000-000000000001"));

const cards = decodeCards({ api_version: 1, conversation: "00000001-0000-7001-8000-000000000001", cards: [
  { kind: "approval", conversation: "00000005-0000-7001-8000-000000000005", parent: "00000003-0000-7001-8000-000000000003", approval: ["00000006-0000-7001-8000-000000000006", 1, 0], tool: "workspace_run_command", arguments: "{}" },
  { kind: "question", conversation: "00000005-0000-7001-8000-000000000005", parent: "00000003-0000-7001-8000-000000000003", approval: ["00000006-0000-7001-8000-000000000006", 1, 1], tool: "ask_user", question: "Which branch?" },
  { kind: "loop_hold", conversation: "00000003-0000-7001-8000-000000000003", parent: "00000001-0000-7001-8000-000000000001" },
  { kind: "quarantine", conversation: "00000005-0000-7001-8000-000000000005", parent: "00000003-0000-7001-8000-000000000003", turn: "00000008-0000-7001-8000-000000000008" },
] }, "00000001-0000-7001-8000-000000000001");
assert.equal(cards.cards.length, 4);
assert.equal(cards.cards[0].parent, "00000003-0000-7001-8000-000000000003");
assert.throws(() => decodeCards({ api_version: 1, conversation: "00000002-0000-7001-8000-000000000002", cards: [] }, "00000001-0000-7001-8000-000000000001"));
assert.throws(() => decodeCards({ api_version: 1, conversation: "00000001-0000-7001-8000-000000000001", cards: [{ kind: "approval", conversation: "00000005-0000-7001-8000-000000000005", parent: null, approval: ["00000006-0000-7001-8000-000000000006", 0, 0], tool: "x", arguments: "{}" }] }, "00000001-0000-7001-8000-000000000001"));

console.log("conversation contract: 3 tests passed");
