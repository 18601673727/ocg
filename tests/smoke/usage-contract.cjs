const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { createRequire } = require("node:module");
const frontend = path.resolve(__dirname, "../../frontend");
const ts = createRequire(path.join(frontend, "package.json"))("typescript");
require.extensions[".ts"] = (module, filename) => {
  const load = module.require.bind(module);
  module.require = name => load(name.startsWith("@/") ? path.join(frontend, name.slice(2)) : name);
  module._compile(ts.transpileModule(fs.readFileSync(filename, "utf8"), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 }, fileName: filename,
  }).outputText, filename);
};
const { createHttpCanonicalControlClient, isCanonicalRejection } = require(path.join(frontend, "components/ocg/runtime/canonical-client.ts"));
const { decodeProjectUsageResponse, ContractError } = require(path.join(frontend, "components/ocg/contracts/index.ts"));
async function main() {
  const [baseUrl, project, session] = process.argv.slice(2);
  const client = createHttpCanonicalControlClient({ baseUrl, fetch });
  const [p, c] = await Promise.all([client.readProjectUsage(project, "all"), client.readConversationUsage(project, session)]);
  assert.equal(isCanonicalRejection(p), false);
  assert.equal(isCanonicalRejection(c), false);
  assert.equal(c.totals.turns, 3);
  assert.equal(c.totals.provider_requests.value, 4);
  assert.equal(c.totals.native_calls, 1);
  assert.deepEqual(c.totals, p.totals);
  assert.equal(c.totals.tokens.cache_write.value, null);
  assert.equal(c.totals.cost.actual_micros, null);
  const invalid = structuredClone(p);
  delete invalid.totals.tokens.input.value;
  assert.throws(() => decodeProjectUsageResponse(invalid), ContractError);
  const badEnum = structuredClone(p);
  badEnum.totals.cost.completeness = "zero";
  assert.throws(() => decodeProjectUsageResponse(badEnum), ContractError);
  const unsafeInteger = structuredClone(p);
  unsafeInteger.totals.tokens.input.value = Number.MAX_SAFE_INTEGER + 1;
  assert.throws(() => decodeProjectUsageResponse(unsafeInteger), ContractError);
  assert.equal(decodeProjectUsageResponse(p).totals.context_costs.schema_bytes_saved.value, 0);
}
main().catch(error => { console.error(error); process.exitCode = 1; });
