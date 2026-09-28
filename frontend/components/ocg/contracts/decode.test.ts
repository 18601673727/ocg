import { test } from "node:test";
import assert from "node:assert/strict";
import { ContractError, array, nullable, number, opt, record, req, string, stringMap } from "./decode";
import {
  decodeConfigurationView,
  decodeProjectsResponse,
  decodeWitness,
  tryEventsEnvelope,
} from "./canonical-decode";
import { CANONICAL_API_VERSION } from "./generated";

test("record rejects a non-object and says what it wanted", () => {
  const result = record([1, 2], "configuration", "an object");
  assert.equal(result.ok, false);
  if (result.ok) assert.fail("an array is not a record");
  assert.equal(result.error.path, "configuration");
  assert.match(result.error.message, /expected an object/);
});

test("a nested record reports the path to the offending leaf", () => {
  const decoder = stringMap(string);
  const result = decoder({ alpha: "ok", beta: 7 }, "configuration.global.providers");
  assert.equal(result.ok, false);
  if (!result.ok) {
    assert.equal(result.error.path, "configuration.global.providers.beta");
  }
});

test("an array element reports its index", () => {
  const result = array(number)([1, 2, "three"], "events");
  assert.equal(result.ok, false);
  if (!result.ok) assert.equal(result.error.path, "events[2]");
});

test("a non-finite number is rejected rather than carried into a comparison", () => {
  // `NaN` and `Infinity` are not JSON-representable, so a payload containing
  // one did not come from this backend.
  assert.equal(number(Number.NaN, "revision").ok, false);
  assert.equal(number(Number.POSITIVE_INFINITY, "revision").ok, false);
  assert.equal(number(0, "revision").ok, true);
  assert.equal(number(-1, "revision").ok, true);
});

test("null and absent are both 'not set' for an optional field", () => {
  const absent = opt({}, "variant", string, "model");
  const explicitNull = opt({ variant: null }, "variant", string, "model");
  assert.equal(absent.ok, true);
  assert.equal(explicitNull.ok, true);
  if (absent.ok && explicitNull.ok) {
    assert.equal(absent.value, undefined);
    assert.equal(explicitNull.value, undefined);
  }
});

test("a present optional field is still validated", () => {
  const result = opt({ variant: 7 }, "variant", string, "model");
  assert.equal(result.ok, false);
});

test("nullable accepts null and delegates everything else", () => {
  assert.equal(nullable(string)(null, "profile").ok, true);
  assert.equal(nullable(string)("ok", "profile").ok, true);
  assert.equal(nullable(string)(7, "profile").ok, false);
});

test("a ContractError is a real Error and names its path", () => {
  const error = new ContractError("a.b", "a string");
  assert.ok(error instanceof Error);
  assert.equal(error.name, "ContractError");
  assert.equal(error.path, "a.b");
  assert.match(error.message, /a\.b: expected a string/);
});

test("req reads a named field and reports the full path on failure", () => {
  const okResult = req({ n: 3 }, "n", number, "config.global");
  assert.equal(okResult.ok, true);
  const badResult = req({}, "n", number, "config.global");
  assert.equal(badResult.ok, false);
  if (badResult.ok) assert.fail("a missing field is not a contract value");
  assert.equal(badResult.error.path, "config.global.n");
});

test("opt threads the parent path so a violation is locatable", () => {
  const badResult = opt({ variant: 7 }, "variant", string, "profile.models.selection-0");
  assert.equal(badResult.ok, false);
  if (badResult.ok) assert.fail("a numeric variant is not a string");
  assert.equal(badResult.error.path, "profile.models.selection-0.variant");
});

const PROJECT_RECORD = {
  project_id: "p",
  root: "/r",
  boundary: "/r",
  marker: true,
  created_at: 0,
  updated_at: 0,
};

function configurationWith(budget: unknown) {
  return {
    project: PROJECT_RECORD,
    global: {
      provider: null,
      model: null,
      profile: null,
      routing: null,
      runtime: null,
      resource_budget: budget,
    },
    project_defaults: { defaults: {} },
  };
}

test("a complete resource budget decodes with its unit from the wire", () => {
  const view = decodeConfigurationView(configurationWith({ hard_limit: 42, unit: "USD" }));
  assert.deepEqual(view.global.resource_budget, { hard_limit: 42, unit: "USD" });
});

test("a resource budget missing its unit is a contract violation", () => {
  // The backend fills the unit in before serialising, so a unit-less budget on
  // the wire did not come from this backend. The PWA must not guess one.
  assert.throws(
    () => decodeConfigurationView(configurationWith({ hard_limit: 42 })),
    /resource_budget\.unit: expected a string/,
  );
});

test("a configuration field with a non-numeric budget is rejected", () => {
  assert.throws(
    () => decodeConfigurationView(configurationWith({ hard_limit: "42", unit: "USD" })),
    /resource_budget\.hard_limit: expected a finite number/,
  );
});

test("a null budget is a valid 'unset' state", () => {
  const view = decodeConfigurationView(configurationWith(null));
  assert.equal(view.global.resource_budget, null);
});

const WITNESS = {
  mission_id: "mission-1",
  work_node_id: 0,
  run_id: 3,
  run_generation: 1,
  runtime_execution_id: "sess-1",
  dispatch_id: "dispatch-1",
};

test("a complete dispatch witness is read as the authority record it is", () => {
  const witness = decodeWitness(WITNESS);
  assert.ok(witness);
  assert.equal(witness?.dispatch_id, "dispatch-1");
  assert.equal(witness?.run_generation, 1);
});

test("a witness missing a field is refused rather than partially populated", () => {
  const withoutDispatchId: Record<string, unknown> = { ...WITNESS };
  delete withoutDispatchId.dispatch_id;
  assert.equal(decodeWitness(withoutDispatchId), null);
});

test("a witness with a negative or fractional index is refused", () => {
  assert.equal(decodeWitness({ ...WITNESS, run_id: -1 }), null);
  assert.equal(decodeWitness({ ...WITNESS, work_node_id: 1.5 }), null);
});

test("a witness with an empty identity string is refused", () => {
  // Rust's `String` accepts `""`, but an empty id is not an identity.
  // `DispatchWitness::from_json` rejects it, so the PWA does too.
  assert.equal(decodeWitness({ ...WITNESS, dispatch_id: "" }), null);
  assert.equal(decodeWitness({ ...WITNESS, mission_id: "" }), null);
  assert.equal(decodeWitness({ ...WITNESS, runtime_execution_id: "" }), null);
});

test("a Run generation starts at 1, matching DispatchWitness::from_json", () => {
  assert.equal(decodeWitness({ ...WITNESS, run_generation: 0 }), null);
  assert.equal(decodeWitness({ ...WITNESS, run_generation: 1 })?.run_generation, 1);
});

test("a witness that is not an object is refused", () => {
  assert.equal(decodeWitness(null), null);
  assert.equal(decodeWitness("not a witness"), null);
  assert.equal(decodeWitness([WITNESS]), null);
  assert.equal(decodeWitness(7), null);
});

test("an unknown optional field from a newer backend is ignored, not fatal", () => {
  // Forward compatibility is deliberate: a newer backend may add an optional
  // field, and the PWA must not break on it or start guessing at its meaning.
  // The decoded value simply does not carry it.
  const view = decodeProjectsResponse({
    api_version: CANONICAL_API_VERSION,
    projects: [{ ...PROJECT_RECORD, imported_at: 1_700_000_000 }],
  });
  assert.equal(view.projects[0]?.project_id, "p");
  assert.equal(
    Object.hasOwn(view.projects[0] as object, "imported_at"),
    false,
    "an unknown field must not be passed through to the PWA",
  );
});

test("a missing required field is still fatal", () => {
  // The counterpart to the rule above: a field the contract requires is not
  // something a backend may omit.
  assert.throws(
    () => decodeProjectsResponse({ api_version: CANONICAL_API_VERSION, projects: [{ ...PROJECT_RECORD, marker: undefined }] }),
    /projects\[0\]\.marker/,
  );
});

test("a work event for another project still decodes; scope is the caller's check", () => {
  // The decoder validates shape, not identity. Project isolation is enforced
  // where it is decided, in the runtime store, not by guessing here.
  const result = tryEventsEnvelope({
    api_version: CANONICAL_API_VERSION,
    project_id: "project-other",
    mission_id: "wn-1",
    events: [
      {
        api_version: CANONICAL_API_VERSION,
        project_id: "project-other",
        mission_id: "wn-1",
        sequence: 1,
        event_id: "e1",
        kind: "run.dispatched",
        payload: { any: ["shape", 1, true, null] },
      },
    ],
  });
  assert.equal(result.ok, true);
});

test("an event missing a required field names the field", () => {
  const result = tryEventsEnvelope({
    api_version: CANONICAL_API_VERSION,
    project_id: "project-1",
    mission_id: "wn-1",
    events: [
      {
        api_version: CANONICAL_API_VERSION,
        project_id: "project-1",
        mission_id: "wn-1",
        sequence: 1,
        event_id: "e1",
        payload: {},
      },
    ],
  });
  assert.equal(result.ok, false);
  if (result.ok) assert.fail("an event without a kind is not a contract payload");
  assert.equal(result.error.path, "events[0].kind");
});

test("a well-formed envelope still decodes after the budget is typed", () => {
  const result = tryEventsEnvelope({
    api_version: CANONICAL_API_VERSION,
    project_id: "project-1",
    mission_id: "wn-1",
    events: [],
  });
  assert.equal(result.ok, true);
});
