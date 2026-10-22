import assert from "node:assert/strict";
import test from "node:test";
import { toolActivitySummary, toolCategory } from "../components/ocg/chat/tool-activity-summary";
import { conversationAncestors, conversationPath, parseConversationPath } from "../components/ocg/layout/conversation-path";
import { decodeConversationLimits } from "../components/ocg/contracts/control-plane";
import { jobTranscript } from "../components/ocg/chat/job-transcript-model";
import { ContractError, decodeProviderResponses, type ControlOutput, type ControlProviderResponse, type ControlToolCall } from "../components/ocg/contracts";
import { ControlTransportError, readTranscriptEvidence, createHttpCanonicalControlClient } from "../components/ocg/runtime/control-client";

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
  const messages = jobTranscript("00000001-0000-7001-8000-000000000001", "Fix it", chunks, { state: "completed" }, false, undefined, [tool(1), tool(2)], false, responses);
  assert.deepEqual(messages.map(m => m.content), ["Fix it", "", first, final]);
  assert.deepEqual(messages.map(m => m.tools?.map(t => t.request) ?? []), [[], [], [1, 2], []]);
  assert.equal(messages[1].reasoning, "Thinking separately.");
  assert.equal(messages.at(-1)?.intermediate, false);
  assert.equal(messages.at(-1)?.id, "00000001-0000-7001-8000-000000000001:answer");
  assert.equal(messages.filter(m => m.role === "assistant" && !m.intermediate).length, 1);
});

test("streaming chunks form only the current tail while completed tool rounds keep their identity", () => {
  const live = jobTranscript("00000001-0000-7001-8000-000000000001", null, [...chunks.slice(0, 3), output(4, "The change")], { state: "unsettled" }, true, undefined, [tool(1), tool(2, null)], false, responses.slice(0, 2));
  assert.equal(live.at(-1)?.content, "The change");
  assert.equal(live.at(-1)?.status, "streaming");
  assert.equal(live.find(m => m.tools?.some(call => call.request === 2))?.status, "streaming");
  const settled = jobTranscript("00000001-0000-7001-8000-000000000001", null, chunks, { state: "completed" }, false, undefined, [tool(1), tool(2)], false, responses);
  assert.equal(live.find(m => m.content === first)?.id, settled.find(m => m.content === first)?.id);
  assert.equal(live.at(-1)?.id, settled.at(-1)?.id);
  // Received evidence may arrive before the corresponding output snapshot.
  const ahead = jobTranscript("00000001-0000-7001-8000-000000000001", null, chunks.slice(0, 2), { state: "completed" }, false, undefined, [tool(1)], false, responses);
  assert.deepEqual(ahead.filter(m => m.content).map(m => m.content), [first, final]);
});

test("failed, cancelled and unknown turns retain earlier progress and interrupted text", () => {
  for (const state of ["failed", "cancelled", "unknown"] as const) {
    const messages = jobTranscript("00000001-0000-7001-8000-000000000001", null, [output(1, first), output(2, "Partial reply"), output(3, "Stopped", "result")], { state }, false, undefined, [tool(1)], false, [reply(1, first)]);
    assert.deepEqual(messages.map(m => m.content), [first, "Partial reply"]);
    assert.equal(messages[0].status, "completed");
    assert.equal(messages[0].detail, null);
    assert.equal(messages.at(-1)?.status, state);
    assert.equal(messages.at(-1)?.detail, "Stopped");
  }
});

test("settlement before the final receipt preserves the answer without repeating the previous reply", () => {
  const messages = jobTranscript("00000001-0000-7001-8000-000000000001", null, chunks, { state: "completed" }, false, undefined, [tool(1), tool(2)], false, responses.slice(0, 2));
  assert.deepEqual(messages.filter(m => m.content).map(m => m.content), [first, final]);
  const title = "<ocg-title>Project repair</ocg-title>\n";
  const named = jobTranscript("00000001-0000-7001-8000-000000000001", null, [output(1, title + first + final), output(2, final, "result")], { state: "completed" }, false, undefined, [tool(1)], true, [reply(1, title + first), reply(2, final, false)]);
  assert.deepEqual(named.map(m => m.content), [first, final]);
});

test("missing receipt evidence preserves raw text and tools instead of inventing a boundary", () => {
  const messages = jobTranscript("00000001-0000-7001-8000-000000000001", null, [output(1, "Unrecorded reply." + final)], { state: "failed" }, false, undefined, [tool(1)], false, [reply(2, final, false)]);
  assert.equal(messages.map(m => m.content).join(""), "Unrecorded reply." + final);
  assert.equal(messages.flatMap(m => m.tools ?? []).length, 1);
});

function page(rows: ControlProviderResponse[], has_more = false, next_after: number | null = null) {
  return { api_version: 1, job: "00000001-0000-7001-8000-000000000001", responses: rows.map(row => ({...row, text_offset: 0, text_complete: true})), has_more, next_after, next_text_offset: has_more ? 0 : null };
}

test("response pages validate identity, order and the advancing cursor", () => {
  assert.deepEqual(decodeProviderResponses(page(responses), "00000001-0000-7001-8000-000000000001").responses.map(({ text_offset, text_complete, ...response }) => { assert.equal(text_offset, 0); assert.equal(text_complete, true); return response; }), responses);
  for (const invalid of [page([reply(0, "")]), page([reply(2, ""), reply(1, "")]), page([reply(1, ""), reply(1, "")]), page([], true, 1), page([reply(1, "")], true, 2), page([reply(1, "")], false, 1), { ...page([]), job: "00000002-0000-7001-8000-000000000002" }]) {
    assert.throws(() => decodeProviderResponses(invalid, "00000001-0000-7001-8000-000000000001"), ContractError);
  }
});

test("client follows response pages and rejects a repeated request across pages", async () => {
  const urls: string[] = [];
  const client = createHttpCanonicalControlClient({ baseUrl: "http://localhost:7840", fetch: async url => {
    urls.push(String(url));
    return Response.json(urls.length === 1 ? page([responses[0]], true, 1) : page(responses.slice(1)));
  } });
  assert.deepEqual(await client.readAllProviderResponses("00000001-0000-7001-8000-000000000001"), responses);
  assert.deepEqual(urls.map(url => new URL(url).search), ["?limit=100", "?limit=100&after=1"]);
  client.close();
  const bad = createHttpCanonicalControlClient({ baseUrl: "http://localhost:7840", fetch: async () => Response.json(page([responses[0]], true, 1)) });
  await assert.rejects(bad.readAllProviderResponses("00000001-0000-7001-8000-000000000001"), ContractError);
  bad.close();
});


test("a final answer repeating the previous reply's prefix is preserved before its receipt", () => {
  const text = "Done.\nDetails of what changed.";
  const messages = jobTranscript("00000001-0000-7001-8000-000000000001", null, [output(1, "Done."), output(2, text), output(3, text, "result")], { state: "completed" }, false, undefined, [], false, [reply(1, "Done.")]);
  assert.equal(messages.at(-1)?.content, text);
  const awaiting = jobTranscript("00000001-0000-7001-8000-000000000001", null, [output(1, "Need permission."), output(2, "Need permission.\nAwaiting approval: git push", "result")], { state: "completed" }, false, undefined, [], false, [reply(1, "Need permission.")]);
  assert.equal(awaiting.at(-1)?.content, "Awaiting approval: git push");
});

test("unavailable or mismatched reply evidence retains raw narration and the stored final answer", () => {
  for (const replies of [[], [reply(2, "Different receipt", false)]]) {
    const messages = jobTranscript("00000001-0000-7001-8000-000000000001", null, [output(1, first + final), output(2, final, "result")], { state: "completed" }, false, undefined, [tool(1)], false, replies);
    assert.deepEqual(messages.map(message => message.content), [first, final]);
    assert.equal(messages.flatMap(message => message.tools ?? []).length, 1);
  }
});

test("an oversized reply does not discard tools or block the stored transcript", async () => {
  const client = {
    readAllJobTools: async () => [tool(1)],
    readAllProviderResponses: async () => { throw new ControlTransportError("output_too_large", 422, false); },
  };
  const result = await readTranscriptEvidence(client, "00000001-0000-7001-8000-000000000001");
  assert.equal(result.responses, null);
  assert.deepEqual(result.tools, [tool(1)]);
  assert.match(result.error ?? "", /output_too_large/);
  const messages = jobTranscript("00000001-0000-7001-8000-000000000001", null, [output(1, first + final), output(2, final, "result")], { state: "completed" }, false, undefined, result.tools ?? [], false, result.responses ?? []);
  assert.deepEqual(messages.map(message => message.content), [first, final]);
});

test("settled evidence catches a late final receipt without duplicating earlier replies", async () => {
  let reads = 0;
  const cursors: (number | undefined)[] = [];
  const client = {
    readAllJobTools: async () => [tool(1)],
    readAllProviderResponses: async (_job: string, _signal?: AbortSignal, after?: number) => {
      cursors.push(after);
      return ++reads === 1 ? [] : [reply(2, final, false)];
    },
  };
  const result = await readTranscriptEvidence(client, "00000001-0000-7001-8000-000000000001", { previous: [reply(1, first)], settled: true });
  assert.deepEqual(result.responses?.map(response => response.request), [1, 2]);
  assert.deepEqual(cursors, [1, 1]);
  assert.equal(result.error, undefined);
});

test("closing a view cancels settled-reply reconciliation", async () => {
  const controller = new AbortController();
  const client = { readAllJobTools: async () => [], readAllProviderResponses: async () => [] };
  const pending = readTranscriptEvidence(client, "00000001-0000-7001-8000-000000000001", { signal: controller.signal, settled: true });
  controller.abort();
  await assert.rejects(pending, { name: "AbortError" });
});


test("an endlessly advancing reply feed stops at 50 pages and falls back explicitly", async () => {
  let requests = 0;
  const client = createHttpCanonicalControlClient({ baseUrl: "http://localhost:7840", fetch: async url => {
    if (String(url).includes("/tools")) return Response.json({ api_version: 1, job: "00000001-0000-7001-8000-000000000001", tools: [], has_more: false, next_after: null });
    requests += 1;
    return Response.json(page([reply(requests, "progress")], true, requests));
  } });
  const result = await readTranscriptEvidence(client, "00000001-0000-7001-8000-000000000001");
  assert.equal(requests, 50);
  assert.equal(result.responses, null);
  assert.match(result.error ?? "", /within 50 pages/);
  client.close();
});


test("UTF-8 reply chunks reassemble before a later tool-only reply and final answer", async () => {
  const text = "é中\"\\\n".repeat(1000);
  const cut = "é中";
  const offset = new TextEncoder().encode(cut).length;
  let reads = 0;
  const urls: string[] = [];
  const client = createHttpCanonicalControlClient({ baseUrl: "http://localhost:7840", fetch: async url => {
    urls.push(String(url));
    if (++reads === 1) return Response.json({ api_version: 1, job: "00000001-0000-7001-8000-000000000001", responses: [{...reply(1, cut),text_offset:0,text_complete:false}],has_more:true,next_after:1,next_text_offset:offset });
    return Response.json({...page([reply(2, ""), reply(3, final, false)]),responses:[{...reply(1,text.slice(cut.length)),text_offset:offset,text_complete:true},...page([reply(2,""),reply(3,final,false)]).responses]});
  } });
  const result = await client.readAllProviderResponses("00000001-0000-7001-8000-000000000001");
  assert.equal(result[0].text, text);
  assert.deepEqual(result.map(reply => reply.request), [1, 2, 3]);
  assert.match(urls[1], /after=1&text_offset=5/);
  client.close();
});

test("awaiting tool calls are pending while unsettled without changing a settled Job", () => {
  for (const decision of ["awaiting_approval", "awaiting_answer"]) {
    const call = {...tool(1, null), decision};
    const live = jobTranscript("00000001-0000-7001-8000-000000000001", null, [], {state:"unsettled"}, true, undefined, [call], false, [reply(1, "")]);
    assert.equal(live.find(message => message.tools?.length)?.status, "pending");
    const settled = jobTranscript("00000001-0000-7001-8000-000000000001", null, [output(1,"Awaiting approval: action","result")], {state:"completed"}, false, undefined, [call], false, [reply(1, "")]);
    assert.equal(settled.at(-1)?.status, "completed");
    assert.equal(settled.flatMap(message => message.tools ?? []).length, 1);
  }
});


test("consecutive tool-only requests fold into one group without crossing narration", () => {
  const replies = [reply(1, first), reply(2, ""), reply(3, "  "), reply(4, "Check results."), reply(5, ""), reply(6, final, false)];
  const calls = [1,2,3,4,5].map(n => tool(n));
  const progress = replies.map(row => row.text).join("");
  const messages = jobTranscript("00000001-0000-7001-8000-000000000001", null, [output(1, progress), output(2, final, "result")], {state:"completed"}, false, undefined, calls, false, replies);
  assert.deepEqual(messages.map(row => row.content), [first, "Check results.", final]);
  assert.deepEqual(messages.map(row => row.tools?.map(call => call.request) ?? []), [[1,2,3],[4,5],[]]);
  assert.equal(messages[0].id, "00000001-0000-7001-8000-000000000001:answer:request:1");
  assert.equal(messages.flatMap(row => row.tools ?? []).length, 5);
});


test("activity categories follow workflow order and combine search strategies", () => {
  const summary = toolActivitySummary(["workspace_run_command", "workspace_fts_search", "workspace_code", "workspace_read_file", "workspace_search", "unknown"].map(name => ({name})));
  assert.deepEqual(summary.categories.map(row => [row.category,row.count]), [["read",1],["search",2],["code",1],["command",1],["other",1]]);
  assert.equal(toolCategory("job_assign"),"agent");
  assert.equal(summary.durationMs,null);
});

test("activity totals estimate UTF-8 payloads and mark missing durations and truncated text", () => {
  const summary = toolActivitySummary([{name:"workspace_read_file",arguments:"中",result:"é",durationMs:25,truncated:true}, {name:"workspace_code",arguments:"{}",result:"abc"}]);
  assert.equal(summary.tokens,Math.ceil(10/3));
  assert.equal(summary.durationMs,25);
  assert.equal(summary.durationPartial,true);
  assert.equal(summary.truncated,true);
  assert.equal(toolActivitySummary([{name:"x",durationMs:0}]).durationMs,0);
});

test("nested conversation routes round-trip and reject malformed or cyclic paths", () => {
  const ids=["00000001-0000-7001-8000-000000000001","00000003-0000-7001-8000-000000000003","00000005-0000-7001-8000-000000000005"];
  assert.deepEqual(parseConversationPath(conversationPath(ids)),ids);
  for (const invalid of ["/jobs", "/jobs/00000001-0000-7001-8000-000000000001/jobs", "/jobs/00000001-0000-7001-8000-000000000001/jobs/00000001-0000-7001-8000-000000000001", "/jobs/job%3A1", "/jobs/00000001-0000-7001-8000-000000000001/other/00000002-0000-7001-8000-000000000002"]) assert.equal(parseConversationPath(invalid),null);
  assert.throws(()=>conversationPath(["00000001-0000-7001-8000-000000000001","bad"]));
});

test("nested conversation ancestry follows stored parents, not the visible URL", async () => {
  const records = new Map([
    ["00000001-0000-7001-8000-000000000001",{id:"00000001-0000-7001-8000-000000000001",kind:"conversation" as const,conversation:null}],
    ["00000003-0000-7001-8000-000000000003",{id:"00000003-0000-7001-8000-000000000003",kind:"conversation" as const,conversation:null,parent:"00000001-0000-7001-8000-000000000001"}],
    ["00000004-0000-7001-8000-000000000004",{id:"00000004-0000-7001-8000-000000000004",kind:"turn" as const,conversation:"00000003-0000-7001-8000-000000000003"}],
  ]);
  const read=async (id:string)=>{const record=records.get(id);if(!record)throw new Error("missing");return record;};
  assert.deepEqual(await conversationAncestors(read,"00000004-0000-7001-8000-000000000004"),["00000001-0000-7001-8000-000000000001","00000003-0000-7001-8000-000000000003"]);
  records.set("00000001-0000-7001-8000-000000000001",{id:"00000001-0000-7001-8000-000000000001",kind:"conversation",conversation:null,parent:"00000003-0000-7001-8000-000000000003"});
  await assert.rejects(conversationAncestors(read,"00000003-0000-7001-8000-000000000003"),/ancestry/);
});

test("conversation limit decoder checks bounds and derived remaining depth", () => {
  const valid={api_version:1,conversation:"00000001-0000-7001-8000-000000000001",depth:2,remaining_depth:1,max_depth:3,max_auto_turns:8,max_iterations:100};
  assert.equal(decodeConversationLimits(valid,"00000001-0000-7001-8000-000000000001").max_iterations,100);
  for(const change of [{remaining_depth:2},{max_depth:17},{max_auto_turns:256},{max_iterations:-1}])assert.throws(()=>decodeConversationLimits({...valid,...change},"00000001-0000-7001-8000-000000000001"),ContractError);
});
