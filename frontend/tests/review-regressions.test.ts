import assert from "node:assert/strict";
import test from "node:test";
import { decodeChildren, decodeJobTree, type ControlTreeNode } from "../components/ocg/contracts/control-plane";
import { mergeChildren } from "../components/ocg/job/job-tree";
import { sanitizeUrl } from "../components/ocg/markdown/markdown-pipeline";
import { readModelPreference, writeModelPreference } from "../components/ocg/chat/model-preference";
import { createHttpCanonicalControlClient } from "../components/ocg/runtime/control-client";
import { convertOcgMessage } from "../components/ocg/runtime/assistant-ui-adapter";
import { zh } from "../components/ocg/i18n/dictionaries";

function node(id = "job:1"): ControlTreeNode {
  return { id, kind: "conversation", parent: null, title: "root", state: "open", created_at_us: 1,
    agent: null, agent_name: null, model: null, on_hold: null, origin: "user", keep_open: true,
    active_turn: null, recurrence: null, conversation: null, previous: null, depends_on: [],
    children: null, has_more_children: false, next_after: null };
}

test("tree depth counts four edges below the root and keeps the holding Agent", () => {
  const root = node();
  root.on_hold = { by: { kind: "agent", conversation: "job:9" } };
  let leaf = root;
  for (let depth = 1; depth <= 4; depth++) {
    leaf.children = [node(`job:${depth + 1}`)];
    leaf = leaf.children[0];
  }
  const envelope = { api_version: 1, job: root.id, ancestors: [], root };
  assert.deepEqual(decodeJobTree(envelope, root.id).root.on_hold, root.on_hold);
  assert.deepEqual(decodeChildren({ api_version: 1, conversation: "job:9", children: [root], has_more: false, next_before: null }, "job:9").children[0].on_hold, root.on_hold);
  leaf.children = [node("job:6")];
  assert.throws(() => decodeJobTree(envelope, root.id), /at most 4 edges/);
  assert.throws(() => decodeJobTree({ ...envelope, root: { ...node(), on_hold: { by: { kind: "agent" } } } }, root.id), /conversation/);
});

test("overlapping tree pages refresh facts without dropping expanded descendants", () => {
  const previous = { ...node("job:2"), children: [node("job:3")], has_more_children: true, next_after: "job:3" };
  const incoming = { ...node("job:2"), state: "closing" as const, active_turn: "job:4",
    on_hold: { by: { kind: "user" as const } } };
  const merged = mergeChildren([previous], [incoming, node("job:5")]);
  assert.deepEqual(merged.map((child) => child.id), ["job:2", "job:5"]);
  assert.equal(merged[0].state, "closing");
  assert.equal(merged[0].active_turn, "job:4");
  assert.deepEqual(merged[0].on_hold, incoming.on_hold);
  assert.deepEqual(merged[0].children, previous.children);
  assert.equal(merged[0].next_after, "job:3");
  const extended = mergeChildren(merged, [{ ...incoming, children: [node("job:3"), node("job:6")], has_more_children: true, next_after: "job:6" }]);
  const overlap = mergeChildren(extended, [{ ...incoming, children: [node("job:3")], has_more_children: true, next_after: "job:3" }]);
  assert.deepEqual(overlap[0].children?.map((child) => child.id), ["job:3", "job:6"]);
  assert.equal(overlap[0].next_after, "job:6");
  const complete = mergeChildren(overlap, [{ ...incoming, children: [node("job:3"), node("job:6")], has_more_children: false, next_after: null }]);
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
  const selection = { model: "model", effort: null, agentProfile: "profile:1", llmProfile: "profile:2", executor: "executor:3", project: "project:4" };
  writeModelPreference(undefined, "job:1", selection, storage);
  assert.deepEqual(readModelPreference(undefined, "job:1", storage), selection);
  assert.equal(readModelPreference(undefined, "job:2", storage), undefined);
  writeModelPreference(undefined, "job:1", { ...selection, executor: "invalid" }, storage);
  assert.equal(readModelPreference(undefined, "job:1", storage), undefined);
});

test("browser retry carries the session CSRF signature", async () => {
  let requests = 0;
  const client = createHttpCanonicalControlClient({ baseUrl: "http://localhost:3000", csrfToken: () => "test-csrf",
    fetch: async (url, settings) => {
      requests++;
      assert.equal(String(url), "http://localhost:3000/api/v1/conversations/job:1/retry");
      assert.equal(new Headers(settings?.headers).get("X-OCG-CSRF"), "test-csrf");
      assert.equal(settings?.body, JSON.stringify({ turn: "job:2" }));
      return Response.json({ api_version: 1, conversation: "job:1", turn: "job:3", seq: 2, joined: false });
    } });
  assert.equal((await client.retryConversation("job:1", "job:2")).turn, "job:3");
  assert.equal(requests, 1);
  client.close();
});

test("each Job stream resumes and acknowledges independently, including on reopen", async () => {
  const requested: (string | null)[] = [];
  const client = createHttpCanonicalControlClient({ baseUrl: "http://localhost:3000", fetch: async (_url, settings) => {
    const after = new Headers(settings?.headers).get("Last-Event-ID");
    requested.push(after);
    const output = { id: "output:1", seq: 1, kind: "result", summary: null, code: null, goal_reached: null, value: "answer", created_at_us: 1 };
    const data = (after === "1" ? "" : `event: output\nid: 1\ndata: ${JSON.stringify({ api_version: 1, job: "job:1", output })}\n\n`) +
      `event: settlement\ndata: ${JSON.stringify({ api_version: 1, job: "job:1", settlement: { state: "completed", settled_at_us: 2 }, stop_confirmed_at_us: null })}\n\n`;
    return new Response(data, { headers: { "Content-Type": "text/event-stream" } });
  } });
  const first: number[] = [], second: number[] = [];
  const a = client.subscribeJobEvents("job:1", (event) => { if (event.type === "output") first.push(event.output.seq); }, { afterSeq: 0 });
  const b = client.subscribeJobEvents("job:1", (event) => { if (event.type === "output") second.push(event.output.seq); }, { afterSeq: 1 });
  assert.deepEqual(await Promise.all([a.closed, b.closed]), ["settled", "settled"]);
  assert.deepEqual(first, [1]);
  assert.deepEqual(second, []);
  const reopened: number[] = [];
  await client.subscribeJobEvents("job:1", (event) => { if (event.type === "output") reopened.push(event.output.seq); }, { afterSeq: 0 }).closed;
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
    assert.equal(part.result, zh["transcript.outcome.refused"]);
    assert.equal(part.isError, true);
  }
});

test("a reconnect resumes after the output this subscriber processed", async () => {
  const requested: (string | null)[] = [], processed: number[] = [];
  const client = createHttpCanonicalControlClient({ baseUrl: "http://localhost:3000", fetch: async (_url, settings) => {
    requested.push(new Headers(settings?.headers).get("Last-Event-ID"));
    const seq = requested.length;
    const output = { id: `output:${seq}`, seq, kind: "result", summary: null, code: null, goal_reached: null, value: "answer", created_at_us: seq };
    let data = `event: output\nid: ${seq}\ndata: ${JSON.stringify({ api_version: 1, job: "job:1", output })}\n\n`;
    if (seq === 2) data += `event: settlement\ndata: ${JSON.stringify({ api_version: 1, job: "job:1", settlement: { state: "completed", settled_at_us: 3 }, stop_confirmed_at_us: null })}\n\n`;
    return new Response(data, { headers: { "Content-Type": "text/event-stream" } });
  } });
  const subscription = client.subscribeJobEvents("job:1", (event) => { if (event.type === "output") processed.push(event.output.seq); }, { afterSeq: 0 });
  assert.equal(await subscription.closed, "settled");
  assert.deepEqual(requested, ["0", "1"]);
  assert.deepEqual(processed, [1, 2]);
  client.close();
});
