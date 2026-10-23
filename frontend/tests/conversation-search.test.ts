import assert from "node:assert/strict";
import test from "node:test";
import { ContractError } from "../components/ocg/contracts";
import { createHttpCanonicalControlClient } from "../components/ocg/runtime/control-client";

const cursor = "00000001-0000-7001-8000-000000000001";

test("conversation search preserves quoted phrases and continuation across empty scan pages", async () => {
  const urls: URL[] = [];
  const client = createHttpCanonicalControlClient({ baseUrl: "http://localhost:7841", fetch: async (url) => {
    urls.push(new URL(String(url)));
    return Response.json({ api_version: 1, conversations: [], has_more: urls.length === 1, next_before: urls.length === 1 ? cursor : null });
  } });
  const first = await client.searchConversations({ query: '"connection reset" 代理', limit: 1 });
  assert.equal(first.has_more, true);
  await client.searchConversations({ query: '"connection reset" 代理', before: first.next_before ?? undefined, limit: 1 });
  assert.equal(urls[0].pathname, "/api/v1/conversations/search");
  assert.equal(urls[0].searchParams.get("query"), '"connection reset" 代理');
  assert.equal(urls[1].searchParams.get("before"), cursor);
  client.close();
});

test("conversation search bounds UTF-8 bytes before sending and forwards cancellation", async () => {
  let calls = 0;
  const controller = new AbortController();
  const client = createHttpCanonicalControlClient({ baseUrl: "http://localhost:7841", fetch: async (_url, init) => {
    calls++;
    assert.ok(init?.signal);
    assert.equal(init.signal.aborted, controller.signal.aborted);
    return Response.json({ api_version: 1, conversations: [], has_more: false, next_before: null });
  } });
  await assert.rejects(client.searchConversations({ query: "中".repeat(167) }), ContractError);
  assert.equal(calls, 0);
  await client.searchConversations({ query: "old reply", signal: controller.signal });
  assert.equal(calls, 1);
  client.close();
});
