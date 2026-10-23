import test from "node:test";
import assert from "node:assert/strict";
import { decodeOutputsPage, decodeJobEvent, decodeConversationTurns } from "../components/ocg/contracts/control-plane";

const job = "00000001-0000-7000-8000-000000000001";

test("decision and summarization outputs preserve their kinds in pages", () => {
  for (const kind of ["decision", "summarization"] as const) {
    const output = { id: "00000002-0000-7000-8000-000000000002", seq: 1, kind, summary: null, code: null, goal_reached: null, value: { schema_version: 1 }, created_at_us: 1 };
    const page = decodeOutputsPage({ api_version: 1, job, after_seq: null, outputs: [output], next_after_seq: 1, has_more: false }, job);
    assert.equal(page.outputs[0].kind, kind);
    assert.deepEqual(page.outputs[0].value, { schema_version: 1 });
  }
});

test("structured output kinds survive live output events", () => {
  for (const kind of ["decision", "summarization"] as const) {
    const output = { id: job, seq: 1, kind, summary: null, code: null, goal_reached: null, value: { schema_version: 1 }, created_at_us: 1 };
    const event = decodeJobEvent("output", { api_version: 1, job, output }, job, "1");
    assert.equal(event.type, "output");
    if (event.type === "output") assert.equal(event.output.kind, kind);
  }
});

test("internal context metadata never becomes a user objective", () => {
  const wireJob = { id: job, parent: job, name: null, title: null, is_chat: true, execution_mode: "once", input: job, previous: null, kind: "turn", conversation: job, assigned_executor: null, created_at_us: 1, updated_at_us: 1, started_at_us: null, paused_at_us: null, sterilized_at_us: null, settlement: { state: "unsettled", settled_at_us: null }, stop_confirmed_at_us: null };
  const decoded = decodeConversationTurns({ api_version: 1, conversation: job, turns: [{ job: wireJob, configuration: null, outputs: [], outputs_truncated: false, entries: [
    { seq: 1, at_us: 1, kind: "user_message", text: "current task", attachments: [] },
    { seq: 2, at_us: 1, kind: "context_preparation", job, policy_revision: "1" },
    { seq: 3, at_us: 1, kind: "context_use", artifact: job },
  ] }], has_more: false, next_before: null }, job);
  assert.equal(decoded.turns[0].input.objective, "current task");
  assert.deepEqual(decoded.turns[0].entries.map(entry => entry.kind), ["user_message", "context_preparation", "context_use"]);
});
