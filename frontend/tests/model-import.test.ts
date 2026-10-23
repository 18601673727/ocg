import assert from "node:assert/strict";
import test from "node:test";
import { ContractError, decodeProviderModels } from "../components/ocg/contracts";
import { createHttpCanonicalControlClient } from "../components/ocg/runtime/control-client";

const id = "00000001-0000-7001-8000-000000000001";

test("model discovery validates metadata without fabricating capabilities", () => {
  assert.deepEqual(decodeProviderModels({ api_version: 1, models: [{ id: "one", context_window: 128000 }, { id: "two", context_window: null }] }).models,
    [{ id: "one", context_window: 128000 }, { id: "two", context_window: null }]);
  assert.throws(() => decodeProviderModels({ api_version: 1, models: [{ id: "one", context_window: 1.5 }] }), ContractError);
});

test("model discovery, bulk import, and deletion use authenticated signed routes", async () => {
  const calls: { path: string; method: string; body: unknown }[] = [];
  let changes = 0;
  const client = createHttpCanonicalControlClient({ baseUrl: "http://localhost:7841", csrfToken: () => "test-csrf", fetch: async (url, init) => {
    const path = new URL(String(url)).pathname;
    const method = init?.method ?? "GET";
    if (method !== "GET") assert.equal(new Headers(init?.headers).get("X-OCG-CSRF"), "test-csrf");
    const body = init?.body ? JSON.parse(String(init.body)) : null;
    calls.push({ path, method, body });
    if (path === "/api/v1/provider-models") return Response.json({ api_version: 1, models: [{ id: "new", context_window: 65536 }] });
    if (path === "/api/v1/profiles/import") return Response.json({ api_version: 1, profile_ids: [id], created: 1, existing: 0 });
    return Response.json({ api_version: 1, llm_profiles: [], agent_profiles: [], executor_protocols: [], providers: [], has_more: false, next_before: null });
  } });
  client.onProfilesChanged(() => changes++);
  const models = await client.discoverProviderModels({ endpoint: "https://example.com/v1", credential_ref: "SAVED_KEY" });
  assert.equal((await client.importModels(id, models.models, ["chat"])).created, 1);
  await client.deleteLlmProfile(id);
  assert.equal(changes, 2);
  assert.deepEqual(calls[1].body, { source_profile_id: id, models: [{ id: "new", context_window: 65536 }], declared_capabilities: ["chat"] });
  assert.equal(calls.find((call) => call.method === "DELETE")?.path, `/api/v1/profiles/${id}`);
  client.close();
});
