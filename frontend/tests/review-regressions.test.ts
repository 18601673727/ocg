import assert from "node:assert/strict";
import test from "node:test";
import { decodeChildren, decodeJobTree, type ControlTreeNode } from "../components/ocg/contracts/control-plane";
import { mergeChildren } from "../components/ocg/job/job-tree";
import { sanitizeUrl } from "../components/ocg/markdown/markdown-pipeline";
import { readModelPreference, writeModelPreference } from "../components/ocg/chat/model-preference";
import { createHttpCanonicalControlClient } from "../components/ocg/runtime/control-client";
import { convertOcgMessage } from "../components/ocg/runtime/assistant-ui-adapter";
import { zh } from "../components/ocg/i18n/dictionaries";

function uuid7(value: number): string {
  return `00000000-0000-7000-8000-${value.toString().padStart(12, "0")}`;
}

function node(id = "00000001-0000-7001-8000-000000000001"): ControlTreeNode {
  return { id, kind: "conversation", parent: null, title: "root", state: "open", created_at_us: 1,
    agent: null, agent_name: null, model: null, on_hold: null, origin: "user", keep_open: true,
    active_turn: null, recurrence: null, conversation: null, previous: null, depends_on: [],
    children: null, has_more_children: false, next_after: null };
}

test("tree depth counts four edges below the root and keeps the holding Agent", () => {
  const root = node();
  root.on_hold = { by: { kind: "agent", conversation: "00000009-0000-7001-8000-000000000009" } };
  let leaf = root;
  for (let depth = 1; depth <= 4; depth++) {
    leaf.children = [node(uuid7(depth + 1))];
    leaf = leaf.children[0];
  }
  const envelope = { api_version: 1, job: root.id, ancestors: [], root };
  assert.deepEqual(decodeJobTree(envelope, root.id).root.on_hold, root.on_hold);
  assert.deepEqual(decodeChildren({ api_version: 1, conversation: "00000009-0000-7001-8000-000000000009", children: [root], has_more: false, next_before: null }, "00000009-0000-7001-8000-000000000009").children[0].on_hold, root.on_hold);
  leaf.children = [node("00000006-0000-7001-8000-000000000006")];
  assert.throws(() => decodeJobTree(envelope, root.id), /at most 4 edges/);
  assert.throws(() => decodeJobTree({ ...envelope, root: { ...node(), on_hold: { by: { kind: "agent" } } } }, root.id), /conversation/);
});

test("overlapping tree pages refresh facts without dropping expanded descendants", () => {
  const previous = { ...node("00000002-0000-7001-8000-000000000002"), children: [node("00000003-0000-7001-8000-000000000003")], has_more_children: true, next_after: "00000003-0000-7001-8000-000000000003" };
  const incoming = { ...node("00000002-0000-7001-8000-000000000002"), state: "closing" as const, active_turn: "00000004-0000-7001-8000-000000000004",
    on_hold: { by: { kind: "user" as const } } };
  const merged = mergeChildren([previous], [incoming, node("00000005-0000-7001-8000-000000000005")]);
  assert.deepEqual(merged.map((child) => child.id), ["00000002-0000-7001-8000-000000000002", "00000005-0000-7001-8000-000000000005"]);
  assert.equal(merged[0].state, "closing");
  assert.equal(merged[0].active_turn, "00000004-0000-7001-8000-000000000004");
  assert.deepEqual(merged[0].on_hold, incoming.on_hold);
  assert.deepEqual(merged[0].children, previous.children);
  assert.equal(merged[0].next_after, "00000003-0000-7001-8000-000000000003");
  const extended = mergeChildren(merged, [{ ...incoming, children: [node("00000003-0000-7001-8000-000000000003"), node("00000006-0000-7001-8000-000000000006")], has_more_children: true, next_after: "00000006-0000-7001-8000-000000000006" }]);
  const overlap = mergeChildren(extended, [{ ...incoming, children: [node("00000003-0000-7001-8000-000000000003")], has_more_children: true, next_after: "00000003-0000-7001-8000-000000000003" }]);
  assert.deepEqual(overlap[0].children?.map((child) => child.id), ["00000003-0000-7001-8000-000000000003", "00000006-0000-7001-8000-000000000006"]);
  assert.equal(overlap[0].next_after, "00000006-0000-7001-8000-000000000006");
  const complete = mergeChildren(overlap, [{ ...incoming, children: [node("00000003-0000-7001-8000-000000000003"), node("00000006-0000-7001-8000-000000000006")], has_more_children: false, next_after: null }]);
  assert.equal(complete[0].has_more_children, false);
  assert.equal(complete[0].next_after, null);
});

test("Markdown URLs reject browser-normalized script and external relative targets", () => {
  for (const url of ["java\tscript:alert(1)", "java\nscript:alert(1)", "javascript:alert(1)", "//evil.example", "\\\\evil.example", "/\\evil.example", "data:text/html,x"]) {
    assert.equal(sanitizeUrl(url), null, JSON.stringify(url));
  }
  for (const url of ["https://example.com/a%20b", "/settings", "#section", "./image.png", "docs/my report.md", "mailto:a@example.com"]) {
    assert.equal(sanitizeUrl(url), url);
  }
});

test("model preferences restore the full selection for chats without a Project", () => {
  const values = new Map<string, string>();
  const storage = { getItem: (key: string) => values.get(key) ?? null, setItem: (key: string, value: string) => { values.set(key, value); } };
  const selection = { model: "model", effort: null, agentProfile: "00000001-0000-7002-8000-000000000001", llmProfile: "00000002-0000-7002-8000-000000000002", executor: "00000003-0000-7003-8000-000000000003", project: "00000004-0000-7004-8000-000000000004" };
  writeModelPreference(undefined, "00000001-0000-7001-8000-000000000001", selection, storage);
  assert.deepEqual(readModelPreference(undefined, "00000001-0000-7001-8000-000000000001", storage), selection);
  assert.equal(readModelPreference(undefined, "00000002-0000-7001-8000-000000000002", storage), undefined);
  writeModelPreference(undefined, "00000001-0000-7001-8000-000000000001", { ...selection, executor: "invalid" }, storage);
  assert.equal(readModelPreference(undefined, "00000001-0000-7001-8000-000000000001", storage), undefined);
});

test("browser retry carries the session CSRF signature", async () => {
  let requests = 0;
  const client = createHttpCanonicalControlClient({ baseUrl: "http://localhost:3000", csrfToken: () => "test-csrf",
    fetch: async (url, settings) => {
      requests++;
      assert.equal(String(url), "http://localhost:3000/api/v1/conversations/00000001-0000-7001-8000-000000000001/retry");
      assert.equal(new Headers(settings?.headers).get("X-OCG-CSRF"), "test-csrf");
      assert.equal(settings?.body, JSON.stringify({ turn: "00000002-0000-7001-8000-000000000002" }));
      return Response.json({ api_version: 1, conversation: "00000001-0000-7001-8000-000000000001", turn: "00000003-0000-7001-8000-000000000003", seq: 2, joined: false });
    } });
  assert.equal((await client.retryConversation("00000001-0000-7001-8000-000000000001", "00000002-0000-7001-8000-000000000002")).turn, "00000003-0000-7001-8000-000000000003");
  assert.equal(requests, 1);
  client.close();
});

test("each Job stream resumes and acknowledges independently, including on reopen", async () => {
  const requested: (string | null)[] = [];
  const client = createHttpCanonicalControlClient({ baseUrl: "http://localhost:3000", fetch: async (_url, settings) => {
    const after = new Headers(settings?.headers).get("Last-Event-ID");
    requested.push(after);
    const output = { id: uuid7(1001), seq: 1, kind: "result", summary: null, code: null, goal_reached: null, value: "answer", created_at_us: 1 };
    const data = (after === "1" ? "" : `event: output\nid: 1\ndata: ${JSON.stringify({ api_version: 1, job: "00000001-0000-7001-8000-000000000001", output })}\n\n`) +
      `event: settlement\ndata: ${JSON.stringify({ api_version: 1, job: "00000001-0000-7001-8000-000000000001", settlement: { state: "completed", settled_at_us: 2 }, stop_confirmed_at_us: null })}\n\n`;
    return new Response(data, { headers: { "Content-Type": "text/event-stream" } });
  } });
  const first: number[] = [], second: number[] = [];
  const a = client.subscribeJobEvents("00000001-0000-7001-8000-000000000001", (event) => { if (event.type === "output") first.push(event.output.seq); }, { afterSeq: 0 });
  const b = client.subscribeJobEvents("00000001-0000-7001-8000-000000000001", (event) => { if (event.type === "output") second.push(event.output.seq); }, { afterSeq: 1 });
  assert.deepEqual(await Promise.all([a.closed, b.closed]), ["settled", "settled"]);
  assert.deepEqual(first, [1]);
  assert.deepEqual(second, []);
  const reopened: number[] = [];
  await client.subscribeJobEvents("00000001-0000-7001-8000-000000000001", (event) => { if (event.type === "output") reopened.push(event.output.seq); }, { afterSeq: 0 }).closed;
  assert.deepEqual(reopened, [1]);
  assert.deepEqual(requested, ["0", "1", "0"]);
  client.close();
});

test("refused tools use the same localized label as the transcript", () => {
  const message = { id: "m", role: "assistant" as const, content: "", status: "completed" as const, createdAt: new Date(0).toISOString(),
    toolCalls: [{ tool_call_id: "call", request: 1, index: 0, name: "tool", decision: "unauthorized", outcome_kind: null,
      arguments: "{}", arguments_cut: false, result: null, result_cut: false }] };
  const converted = convertOcgMessage(message, 0, [message], null, (key) => zh[key]);
  assert.ok(Array.isArray(converted.content));
  const part = converted.content[0];
  assert.equal(part.type, "tool-call");
  if (part.type === "tool-call") {
    assert.equal(part.result, `${zh["transcript.outcome.refused"]}: unauthorized`);
    assert.equal(part.isError, true);
  }
});

test("a reconnect resumes after the output this subscriber processed", async () => {
  const requested: (string | null)[] = [], processed: number[] = [];
  const client = createHttpCanonicalControlClient({ baseUrl: "http://localhost:3000", fetch: async (_url, settings) => {
    requested.push(new Headers(settings?.headers).get("Last-Event-ID"));
    const seq = requested.length;
    const output = { id: uuid7(1000 + seq), seq, kind: "result", summary: null, code: null, goal_reached: null, value: "answer", created_at_us: seq };
     let data = `event: output\nid: ${seq}\ndata: ${JSON.stringify({ api_version: 1, job: "00000001-0000-7001-8000-000000000001", output })}\n\n`;
    if (seq === 2) data += `event: settlement\ndata: ${JSON.stringify({ api_version: 1, job: "00000001-0000-7001-8000-000000000001", settlement: { state: "completed", settled_at_us: 3 }, stop_confirmed_at_us: null })}\n\n`;
    return new Response(data, { headers: { "Content-Type": "text/event-stream" } });
  } });
  const subscription = client.subscribeJobEvents("00000001-0000-7001-8000-000000000001", (event) => { if (event.type === "output") processed.push(event.output.seq); }, { afterSeq: 0 });
  assert.equal(await subscription.closed, "settled");
  assert.deepEqual(requested, ["0", "1"]);
  assert.deepEqual(processed, [1, 2]);
  client.close();
});

test("profile paging joins executor compatibility across empty and separate pages", async () => {
  const protocol = "openai_chat_completions";
  const model = { id: uuid7(30), name: "Model", model: "model", context_window: null, protocol,
    endpoint: "https://provider.example", credential_ref: null, has_credential: false,
    declared_capabilities: ["chat"], parameters: { temperature: null, top_p: null, max_tokens: 100,
      reasoning_effort: null, max_completion_tokens: null, presence_penalty: null, frequency_penalty: null },
    editable: true, blocking_jobs: [], more_blocking_jobs: false, created_at_us: 1, updated_at_us: 1 };
  const provider = { protocol, endpoint: model.endpoint, credential_ref: null, has_credential: false,
    profile_ids: [model.id], models: [model.model] };
  const empty = { api_version: 1, llm_profiles: [], agent_profiles: [], providers: [], executor_protocols: [], has_more: true };
  const requested: (string | null)[] = [];
  const pages = [
    { ...empty, llm_profiles: [model], providers: [provider], next_before: uuid7(30) },
    { ...empty, next_before: uuid7(20) },
    { ...empty, executor_protocols: [{ id: uuid7(10), protocol }], has_more: false, next_before: null },
  ];
  const client = createHttpCanonicalControlClient({ baseUrl: "http://localhost:3000", fetch: async (input) => {
    requested.push(new URL(String(input)).searchParams.get("before"));
    return Response.json(pages[requested.length - 1]);
  } });
  const profiles = await client.readProfiles();
  assert.deepEqual(requested, [null, uuid7(30), uuid7(20)]);
  assert.deepEqual(profiles.llm_profiles[0].executors, [uuid7(10)]);
  assert.deepEqual(profiles.providers, [provider]);
  client.close();

  let calls = 0;
  const regressing = createHttpCanonicalControlClient({ baseUrl: "http://localhost:3000", fetch: async () => {
    calls++;
    return Response.json({ ...empty, next_before: uuid7(calls === 1 ? 20 : 30) });
  } });
  await assert.rejects(regressing.readProfiles(), /advancing Profile cursor/);
  assert.equal(calls, 2);
  regressing.close();
});
