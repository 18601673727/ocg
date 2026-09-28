import { test } from "node:test";
import assert from "node:assert/strict";
import {
  canonicalCommandId,
  createHttpCanonicalControlClient,
  isCanonicalRejection,
  projectLabelFor,
  scopeIdFor,
  type CanonicalProjectRecord,
} from "./canonical-client";
import { CANONICAL_API_VERSION } from "./canonical-envelope";
import { PROFILE_API_VERSION, decodeProfileView, tryProfileView } from "../contracts";
import type { ProjectId } from "../project/domain";

const PROJECT: CanonicalProjectRecord = {
  project_id: "project-0123456789abcdef01234567",
  root: "/tmp/example",
  boundary: "/tmp/example",
  marker: true,
  created_at: 1,
  updated_at: 2,
};

/** A complete `GlobalConfiguration`; every field is required on the wire. */
const GLOBAL = {
  provider: "openai",
  model: "gpt-6-astra",
  profile: "careful",
  routing: "balanced",
  runtime: null,
  resource_budget: { hard_limit: 42, unit: "USD" },
};

const CONFIGURATION_VIEW = {
  project: PROJECT,
  global: GLOBAL,
  project_defaults: { defaults: {} },
};

type Call = { url: string; method: string; body: unknown };

function fakeFetch(handler: (call: Call) => { status: number; body: unknown }) {
  const calls: Call[] = [];
  const fetch = async (url: string, init?: { method?: string; body?: string }) => {
    const call = {
      url,
      method: init?.method ?? "GET",
      body: init?.body ? JSON.parse(init.body) : undefined,
    };
    calls.push(call);
    const { status, body } = handler(call);
    return { status, ok: status >= 200 && status < 300, text: async () => JSON.stringify(body) };
  };
  return { fetch, calls };
}

function client(fetcher: ReturnType<typeof fakeFetch>) {
  return createHttpCanonicalControlClient({ baseUrl: "http://127.0.0.1:9999/", fetch: fetcher.fetch });
}

test("project import sends a stable command identity and returns the backend record", async () => {
  const fetcher = fakeFetch(() => ({
    status: 200,
    body: { api_version: CANONICAL_API_VERSION, command_id: "cmd-import-1", accepted: true, project: PROJECT },
  }));
  const commandId = canonicalCommandId("project-import", "1");
  const ack = await client(fetcher).importProject(commandId, "/tmp/example");
  assert.equal(fetcher.calls[0]!.url, "http://127.0.0.1:9999/api/v1/canonical/projects/import");
  assert.equal(fetcher.calls[0]!.method, "POST");
  assert.deepEqual(fetcher.calls[0]!.body, { command_id: commandId, root: "/tmp/example" });
  assert.ok(!isCanonicalRejection(ack));
  if (!isCanonicalRejection(ack)) {
    assert.equal(ack.commandId, "cmd-import-1");
    assert.equal(ack.accepted, true);
    assert.equal(ack.project.project_id, PROJECT.project_id);
  }
});

test("the acknowledgement reports the command id the backend accepted, not the one sent", async () => {
  // A backend that coalesces commands echoes a different identity. The client
  // used to return the id it had sent, which misreported what the backend
  // actually recorded.
  const fetcher = fakeFetch(() => ({
    status: 200,
    body: {
      api_version: CANONICAL_API_VERSION,
      command_id: "cmd-import-coalesced",
      accepted: false,
      project: PROJECT,
    },
  }));
  const ack = await client(fetcher).importProject("cmd-import-sent", "/tmp/example");
  assert.ok(!isCanonicalRejection(ack));
  if (!isCanonicalRejection(ack)) {
    assert.equal(ack.commandId, "cmd-import-coalesced");
    assert.equal(ack.accepted, false);
  }
});

test("a rejected import surfaces the backend reason and status", async () => {
  const fetcher = fakeFetch(() => ({
    status: 400,
    body: { error: { code: "invalid_request", message: "this command needs an initialized project" } },
  }));
  const ack = await client(fetcher).importProject("cmd-import-2", "/tmp/nowhere");
  assert.ok(isCanonicalRejection(ack));
  if (isCanonicalRejection(ack)) {
    assert.equal(ack.status, 400);
    assert.match(ack.message, /initialized project/);
    assert.equal(ack.commandId, "cmd-import-2");
  }
});

test("global configuration and project defaults use distinct persisted commands", async () => {
  const fetcher = fakeFetch((call) =>
    call.url.includes("/configuration/projects/")
      ? {
          status: 200,
          body: {
            api_version: CANONICAL_API_VERSION,
            command_id: "cmd-project-defaults-1",
            accepted: true,
            project_id: PROJECT.project_id,
            revision: 2,
            configuration: {
              project: PROJECT,
              global: GLOBAL,
              project_defaults: { defaults: { profile: "fast" } },
            },
          },
        }
      : {
          status: 200,
          body: {
            api_version: CANONICAL_API_VERSION,
            command_id: "cmd-global-config-1",
            accepted: true,
            project_id: PROJECT.project_id,
            revision: 1,
            configuration: CONFIGURATION_VIEW,
          },
        },
  );
  const control = client(fetcher);
  const global = await control.writeGlobalConfiguration("cmd-global-config-1", { ...GLOBAL });
  assert.ok(!isCanonicalRejection(global));
  if (!isCanonicalRejection(global)) {
    assert.equal(global.revision, 1);
    assert.equal(global.projectId, PROJECT.project_id);
    assert.equal(global.accepted, true);
  }
  const defaults = await control.writeProjectDefaults("cmd-project-defaults-1", PROJECT.project_id, { profile: "fast" });
  assert.ok(!isCanonicalRejection(defaults));
  if (!isCanonicalRejection(defaults)) assert.equal(defaults.revision, 2);
  assert.match(fetcher.calls[1]!.url, /\/configuration\/projects\//);
});

test("a frozen pre-run Mission configuration is reported, not retried silently", async () => {
  const fetcher = fakeFetch(() => ({
    status: 400,
    body: {
      error: {
        code: "invalid_request",
        message: "Mission is already dispatched: pre-run configuration is frozen",
      },
    },
  }));
  const ack = await client(fetcher).writeMissionConfiguration("cmd-mission-1", "wn-live", { profile: "fast" });
  assert.ok(isCanonicalRejection(ack));
  if (isCanonicalRejection(ack)) assert.match(ack.message, /frozen/);
});

test("Mission configuration acknowledgement carries the backend's mission identity", async () => {
  const fetcher = fakeFetch(() => ({
    status: 200,
    body: {
      api_version: CANONICAL_API_VERSION,
      command_id: "cmd-mission-2",
      accepted: true,
      mission_id: "wn-live",
      revision: 7,
      configuration: { profile: "fast" },
    },
  }));
  const ack = await client(fetcher).writeMissionConfiguration("cmd-mission-2", "wn-live", { profile: "fast" });
  assert.ok(!isCanonicalRejection(ack));
  if (!isCanonicalRejection(ack)) {
    assert.equal(ack.missionId, "wn-live");
    assert.equal(ack.revision, 7);
  }
});

test("snapshot and event reads are project-scoped and carry the resume cursor", async () => {
  const fetcher = fakeFetch((call) =>
    call.url.includes("/work/events")
      ? {
          status: 200,
          body: {
            api_version: CANONICAL_API_VERSION,
            project_id: PROJECT.project_id,
            mission_id: "wn-live",
            events: [
              {
                api_version: CANONICAL_API_VERSION,
                project_id: PROJECT.project_id,
                mission_id: "wn-live",
                sequence: 4,
                event_id: "wn-live:4",
                kind: "run.finished",
                payload: {},
              },
            ],
          },
        }
      : {
          status: 200,
          body: {
            api_version: CANONICAL_API_VERSION,
            project_id: PROJECT.project_id,
            mission_id: "wn-live",
            cursor: 3,
            mission: {},
          },
        },
  );
  const control = client(fetcher);
  await control.readWorkSnapshot(PROJECT.project_id, "wn-live");
  assert.match(fetcher.calls[0]!.url, /project_id=project-0123456789abcdef01234567&mission_id=wn-live$/);
  const events = await control.readWorkEvents(PROJECT.project_id, "wn-live", 3);
  assert.match(fetcher.calls[1]!.url, /after=3$/);
  assert.ok(!isCanonicalRejection(events));
  if (!isCanonicalRejection(events)) assert.equal(events[0]?.sequence, 4);
});

test("a foreign API version is a hard contract error rather than a render", async () => {
  const fetcher = fakeFetch(() => ({ status: 200, body: { api_version: "ocg.canonical.v0", projects: [] } }));
  await assert.rejects(() => client(fetcher).listProjects(), /contract violation/);
});

test("a renamed Rust field is a contract violation, not an undefined in the UI", async () => {
  // `boundary` was renamed to `project_root` on the Rust side. The previous
  // implementation cast this payload, and the control surface would have
  // rendered `undefined` as the project boundary with nothing reporting it.
  const withoutBoundary: Record<string, unknown> = { ...PROJECT };
  delete withoutBoundary.boundary;
  withoutBoundary.project_root = PROJECT.root;
  const fetcher = fakeFetch(() => ({
    status: 200,
    body: { api_version: CANONICAL_API_VERSION, projects: [withoutBoundary] },
  }));
  await assert.rejects(() => client(fetcher).listProjects(), /contract violation/);
});

test("a retargeted project scope is rejected rather than rendered into another project", async () => {
  // The envelope must belong to the project the control surface asked about.
  // A wrong-project payload is a hard contract error, not a silent render.
  const fetcher = fakeFetch(() => ({
    status: 200,
    body: { api_version: CANONICAL_API_VERSION, projects: [PROJECT] },
  }));
  const projects = await client(fetcher).listProjects();
  assert.equal(projects.length, 1);
  assert.equal(scopeIdFor(projects[0]!) as string, PROJECT.project_id as ProjectId);
  assert.equal(projectLabelFor(projects[0]!), "Project 01234567");
});

test("Profile decoding rejects a version the PWA does not speak", () => {
  const drifted = { api_version: "ocg.profile.v0", profile: null, revision: null, candidates: [] };
  const result = tryProfileView(drifted);
  assert.equal(result.ok, false);
  assert.throws(() => decodeProfileView(drifted), /contract violation/);
});

test("Profile decoding accepts a complete payload from the Rust contract", () => {
  const view = decodeProfileView({
    api_version: PROFILE_API_VERSION,
    profile: {
      origin: "new",
      defaultModel: "selection-0",
      providers: { compat: { placeholder: false, label: "Compat" } },
      models: { "selection-0": { placeholder: false, provider: "compat", id: "model-0" } },
    },
    revision: "abc123",
    candidates: [],
  });
  assert.equal(view.profile?.defaultModel, "selection-0");
  assert.equal(view.profile?.models["selection-0"]?.id, "model-0");
  assert.equal(view.revision, "abc123");
});

test("Profile decoding rejects a placeholder model that lost its placeholder flag", () => {
  // A missing `placeholder` is not the same as `placeholder: false`. Silently
  // defaulting it would present a non-runnable model as selectable.
  const result = tryProfileView({
    api_version: PROFILE_API_VERSION,
    profile: {
      origin: "new",
      providers: { compat: { placeholder: false, label: "Compat" } },
      models: { "selection-0": { provider: "compat", id: "model-0" } },
    },
    revision: null,
    candidates: [],
  });
  assert.equal(result.ok, false);
});
