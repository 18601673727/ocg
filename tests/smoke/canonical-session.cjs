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

async function main() {
  const [controlUrl, projectId] = process.argv.slice(2);
  const client = CanonicalOcgRuntimeClient.connect("local-ready", controlUrl, fetch);
  await client.hydrateProject(projectId);
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
