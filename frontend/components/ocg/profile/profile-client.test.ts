import assert from "node:assert/strict";
import test from "node:test";
import { createProfileClient, isLoopbackControlUrl, runnableChoices, type Candidate, type Profile } from "./profile-client";

const placeholder: Profile = {
  origin: "new",
  providers: { placeholder: { label: "Configure a provider", placeholder: true } },
  models: { placeholder: { provider: "placeholder", id: "placeholder", placeholder: true } },
};

test("placeholder is visible but never a runnable choice", () => {
  assert.deepEqual(runnableChoices(placeholder), []);
  const configured = structuredClone(placeholder);
  configured.providers.extra = { label: "Extra", placeholder: false };
  for (let i = 0; i < 17; i++) configured.models[`model-${i}`] = { provider: "extra", id: `model-${i}`, placeholder: false };
  assert.equal(runnableChoices(configured).length, 17);
});

test("Profile client sends explicit candidate identity and never merges sources", async () => {
  const calls: Array<{ url: string; body: Record<string, unknown> }> = [];
  const fetcher = (async (url: string, options?: RequestInit) => {
    calls.push({ url, body: JSON.parse(String(options?.body ?? "{}")) as Record<string, unknown> });
    return { ok: true, json: async () => ({ api_version: "ocg.profile.v1", profile: placeholder, revision: "revision", candidates: [] }) } as Response;
  }) as typeof fetch;
  const client = createProfileClient("http://127.0.0.1:8765", fetcher);
  const global: Candidate = { source: "opencode", scope: "global", location: "/global/opencode.json", sha256: "hash-global", provider_names: ["g"], model_ids: ["g/m"], variants: {}, importable_fields: ["model"], ignored_fields: ["apiKey"] };
  await client.importCandidate(global);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, "http://127.0.0.1:8765/api/v1/profile/bootstrap");
  assert.deepEqual(calls[0].body, { choice: "import", location: global.location, sha256: global.sha256 });
  assert.ok(!JSON.stringify(calls).includes("apiKey"));
  await client.createNew();
  assert.deepEqual(calls[1].body, { choice: "new" });
});

test("only local loopback control URLs are accepted", () => {
  assert.equal(isLoopbackControlUrl("http://localhost:8765"), true);
  for (const url of ["https://localhost:8765", "http://example.com:8765", "http://localhost.evil:8765", "http://127.0.0.1:8765/path", "http://user:pass@localhost:8765"]) {
    assert.equal(isLoopbackControlUrl(url), false, url);
  }
});
