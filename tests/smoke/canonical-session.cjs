const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { createRequire } = require("node:module");

const frontend = path.resolve(__dirname, "../../frontend");
const ts = createRequire(path.join(frontend, "package.json"))("typescript");

// Exercise the production TypeScript modules with the existing compiler and alias.
require.extensions[".ts"] = (module, filename) => {
  const load = module.require.bind(module);
  module.require = (name) => load(name.startsWith("@/") ? path.join(frontend, name.slice(2)) : name);
  const source = ts.transpileModule(fs.readFileSync(filename, "utf8"), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
    fileName: filename,
  }).outputText;
  module._compile(source, filename);
};

const { CanonicalOcgRuntimeClient } = require(path.join(frontend, "components/ocg/runtime/canonical-launch-client.ts"));
const { RuntimeEnvelopeFactory, validateRuntimeEnvelope } = require(path.join(frontend, "components/ocg/runtime/runtime-envelope.ts"));
const { selectProjectSnapshot } = require(path.join(frontend, "components/ocg/project/selectors.ts"));
const { retryContent } = require(path.join(frontend, "components/ocg/chat/retry.ts"));

async function main() {
  const [controlUrl, projectId, retrySessionId, retryMessageId] = process.argv.slice(2);
  const client = CanonicalOcgRuntimeClient.connect("local-ready", controlUrl, fetch);
  await client.hydrateProject(projectId);
  if (retrySessionId) {
    const session = client.getSnapshot().sessions.find(item => item.sessionId === retrySessionId);
    assert.ok(session);
    const messages = client.getSnapshot().messagesBySession[session.id];
    const failed = messages.find(item => item.id === retryMessageId);
    assert.equal(failed.status, "failed");
    assert.ok(retryContent(messages.map(item => item.id === failed.id ? { ...item, status: "cancelled" } : item), failed.id));
    await assert.rejects(client.retryMessage(session.id, messages.find(item => item.role === "user").id));
    assert.ok(failed.jobId);
    const streams = [];
    global.EventSource = class {
      static CONNECTING = 0;
      static CLOSED = 2;
      constructor(url) { this.url = new URL(url); streams.push(this); }
      close() {}
    };
    // Whatever the failed execution left on screen must not survive Retry.
    client.emit({ type: "conversation.message-started", sessionId: session.id, message: { ...failed, content: "stale output", images: [{ id: "img", name: "old.png", media_type: "image/png", url: "http://127.0.0.1/old.png" }], failureReason: "boom" } }, { projectId });
    await client.retryMessage(session.id, failed.id);
    assert.equal(streams.length, 1);
    assert.equal(streams[0].url.searchParams.get("job_id"), failed.jobId);
    const after = client.getSnapshot().messagesBySession[session.id];
    assert.deepEqual(after.map(item => item.id), messages.map(item => item.id), "Retry must not add a turn or Message");
    const retried = after.find(item => item.id === failed.id);
    assert.equal(retried.status, "streaming");
    assert.equal(retried.content, "");
    assert.equal(retried.images, undefined);
    assert.equal(retried.failureReason, undefined);
    assert.equal(retried.jobId, failed.jobId);
    assert.equal(retried.commandId, failed.commandId);
    // HA-NET-01 boundaries on the Retry stream start from the cleared Message.
    const deliver = record => streams[0].onmessage({ data: JSON.stringify(record) });
    deliver({ round_begin: true });
    deliver({ delta: "provisional" });
    deliver({ round_reset: true });
    deliver({ delta: "replacement" });
    const streamed = client.getSnapshot().messagesBySession[session.id].find(item => item.id === failed.id);
    assert.equal(streamed.content, "replacement");
    assert.equal(streamed.status, "streaming");
    process.stdout.write(JSON.stringify({ job_id: streams[0].url.searchParams.get("job_id") }));
    return;
  }
  const session = await client.createSession({ projectId, title: "", workType: "coding" });
  assert.equal(session.title, "");
  assert.equal(client.getSyncState().status, "live");
  assert.deepEqual(client.getSyncState().diagnostics, []);
  const snapshot = client.getSnapshot();
  assert.equal(snapshot.authority, "canonical");
  assert.deepEqual(snapshot.sessions.find(item => item.id === session.id), session);
  assert.deepEqual(selectProjectSnapshot(snapshot, projectId).sessions, [session]);
  assert.deepEqual(snapshot.messagesBySession[session.id], []);

  const envelopes = new RuntimeEnvelopeFactory("untitled-session-smoke", 1);
  for (const type of ["conversation.session-created", "conversation.session-updated"]) {
    assert.equal(validateRuntimeEnvelope(envelopes.envelope(type, { session }, { projectId })).ok, true);
    for (const title of [undefined, null, 123, {}]) {
      const result = validateRuntimeEnvelope(envelopes.envelope(type, {
        session: { ...session, title },
      }, { projectId }));
      assert.equal(result.ok, false);
      assert.equal(result.diagnostic.code, "schema-invalid");
    }
  }
  assert.ok(session.sessionId);
  process.stdout.write(JSON.stringify({ session_id: session.sessionId }));
}

main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
