import assert from "node:assert/strict";
import { decodeAdmission, decodeConversationTurns, decodeConversations, decodeStop } from "../components/ocg/contracts/control-plane";

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
console.log("conversation contract: 1 test passed");
