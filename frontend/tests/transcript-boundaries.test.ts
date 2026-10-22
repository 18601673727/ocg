import assert from "node:assert/strict";
import test from "node:test";
import { jobTranscript } from "../components/ocg/chat/job-transcript-model";
import { ContractError, decodeProviderResponses, type ControlOutput, type ControlProviderResponse, type ControlToolCall } from "../components/ocg/contracts";
import { createHttpCanonicalControlClient } from "../components/ocg/runtime/control-client";

function output(seq: number, summary: string, kind: ControlOutput["kind"] = "progress"): ControlOutput {
  return { id: `output:${seq}`, seq, kind, summary, code: null, goal_reached: null, value: null, created_at_us: seq * 1000 };
}
function reply(request: number, text: string, requests_tools = true): ControlProviderResponse {
  return { request, text, requests_tools, created_at_us: request * 10000 };
}
function tool(request: number, outcome_kind: ControlToolCall["outcome_kind"] = "completed"): ControlToolCall {
  return { request, index: 0, tool_call_id: `call-${request}`, name: "workspace_read_file", decision: "admitted", outcome_kind, arguments: "{}", arguments_cut: false, result: outcome_kind ? "contents" : null, result_cut: false };
}
const first = "I'll inspect the project.";
const final = "The change is ready.";
const chunks = [output(1, "I'll inspect "), output(2, "the project."), output(3, "Thinking separately.", "reasoning"), output(4, final), output(5, final, "result")];
const responses = [reply(1, first), reply(2, ""), reply(3, final, false)];

test("reload preserves each assistant reply, tool-only rounds and the final answer exactly once", () => {
  const messages = jobTranscript("job:1", "Fix it", chunks, { state: "completed" }, false, undefined, [tool(1), tool(2)], false, responses);
  assert.deepEqual(messages.map(m => m.content), ["Fix it", "", first, "", final]);
  assert.deepEqual(messages.map(m => m.tools?.map(t => t.request) ?? []), [[], [], [1], [2], []]);
  assert.equal(messages[1].reasoning, "Thinking separately.");
  assert.equal(messages.at(-1)?.intermediate, false);
  assert.equal(messages.at(-1)?.id, "job:1:answer");
  assert.equal(messages.filter(m => m.role === "assistant" && !m.intermediate).length, 1);
});

test("streaming chunks form only the current tail while completed tool rounds keep their identity", () => {
  const live = jobTranscript("job:1", null, [...chunks.slice(0, 3), output(4, "The change")], { state: "unsettled" }, true, undefined, [tool(1), tool(2, null)], false, responses.slice(0, 2));
  assert.equal(live.at(-1)?.content, "The change");
  assert.equal(live.at(-1)?.status, "streaming");
  assert.equal(live.find(m => m.tools?.[0]?.request === 2)?.status, "streaming");
  const settled = jobTranscript("job:1", null, chunks, { state: "completed" }, false, undefined, [tool(1), tool(2)], false, responses);
  assert.equal(live.find(m => m.content === first)?.id, settled.find(m => m.content === first)?.id);
  assert.equal(live.at(-1)?.id, settled.at(-1)?.id);
  // Received evidence may arrive before the corresponding output snapshot.
  const ahead = jobTranscript("job:1", null, chunks.slice(0, 2), { state: "completed" }, false, undefined, [tool(1)], false, responses);
  assert.deepEqual(ahead.filter(m => m.content).map(m => m.content), [first, final]);
});

test("failed, cancelled and unknown turns retain earlier progress and interrupted text", () => {
  for (const state of ["failed", "cancelled", "unknown"] as const) {
    const messages = jobTranscript("job:1", null, [output(1, first), output(2, "Partial reply"), output(3, "Stopped", "result")], { state }, false, undefined, [tool(1)], false, [reply(1, first)]);
    assert.deepEqual(messages.map(m => m.content), [first, "Partial reply"]);
    assert.equal(messages[0].status, "completed");
    assert.equal(messages[0].detail, null);
    assert.equal(messages.at(-1)?.status, state);
    assert.equal(messages.at(-1)?.detail, "Stopped");
  }
});

test("settlement before the final receipt preserves the answer without repeating the previous reply", () => {
  const messages = jobTranscript("job:1", null, chunks, { state: "completed" }, false, undefined, [tool(1), tool(2)], false, responses.slice(0, 2));
  assert.deepEqual(messages.filter(m => m.content).map(m => m.content), [first, final]);
  const title = "<ocg-title>Project repair</ocg-title>\n";
  const named = jobTranscript("job:1", null, [output(1, title + first + final), output(2, final, "result")], { state: "completed" }, false, undefined, [tool(1)], true, [reply(1, title + first), reply(2, final, false)]);
  assert.deepEqual(named.map(m => m.content), [first, final]);
});

test("missing receipt evidence preserves raw text and tools instead of inventing a boundary", () => {
  const messages = jobTranscript("job:1", null, [output(1, "Unrecorded reply." + final)], { state: "failed" }, false, undefined, [tool(1)], false, [reply(2, final, false)]);
  assert.equal(messages.map(m => m.content).join(""), "Unrecorded reply." + final);
  assert.equal(messages.flatMap(m => m.tools ?? []).length, 1);
});

function page(rows: ControlProviderResponse[], has_more = false, next_after: number | null = null) {
  return { api_version: 1, job: "job:1", responses: rows, has_more, next_after };
}

test("response pages validate identity, order and the advancing cursor", () => {
  assert.deepEqual(decodeProviderResponses(page(responses), "job:1").responses, responses);
  for (const invalid of [page([reply(0, "")]), page([reply(2, ""), reply(1, "")]), page([reply(1, ""), reply(1, "")]), page([], true, 1), page([reply(1, "")], true, 2), page([reply(1, "")], false, 1), { ...page([]), job: "job:2" }]) {
    assert.throws(() => decodeProviderResponses(invalid, "job:1"), ContractError);
  }
});

test("client follows response pages and rejects a repeated request across pages", async () => {
  const urls: string[] = [];
  const client = createHttpCanonicalControlClient({ baseUrl: "http://localhost:7840", fetch: async url => {
    urls.push(String(url));
    return Response.json(urls.length === 1 ? page([responses[0]], true, 1) : page(responses.slice(1)));
  } });
  assert.deepEqual(await client.readAllProviderResponses("job:1"), responses);
  assert.deepEqual(urls.map(url => new URL(url).search), ["?limit=100", "?limit=100&after=1"]);
  client.close();
  const bad = createHttpCanonicalControlClient({ baseUrl: "http://localhost:7840", fetch: async () => Response.json(page([responses[0]], true, 1)) });
  await assert.rejects(bad.readAllProviderResponses("job:1"), ContractError);
  bad.close();
});
