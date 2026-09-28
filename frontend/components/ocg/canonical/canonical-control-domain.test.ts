import { test } from "node:test";
import assert from "node:assert/strict";
import {
  DEFAULT_CONFIGURATION_DRAFT,
  describeAcknowledgement,
  draftFromConfiguration,
  globalConfigurationCommandId,
  preRunConfigurationState,
  projectDefaultsCommandId,
  projectImportCommandId,
  selectRegisteredProjects,
  toGlobalConfiguration,
  validateConfigurationDraft,
  validateImportRoot,
} from "./canonical-control-domain";
import { createCanonicalState, applyCanonicalSnapshot, type CanonicalState } from "../runtime/canonical-store";
import { CANONICAL_API_VERSION } from "../runtime/canonical-envelope";
import type { CanonicalProjectRecord } from "../runtime/canonical-client";
import type { ProjectId } from "../project/domain";

const PROJECT: CanonicalProjectRecord = {
  project_id: "project-aaaa",
  root: "/tmp/repo",
  boundary: "/tmp/repo",
  marker: true,
  created_at: 1,
  updated_at: 1,
};

function canonicalState(runs: number): CanonicalState {
  const missionRuns = Array.from({ length: runs }, (_, index) => ({
    run_id: index,
    node_id: index,
    generation: 1,
    state: "active",
    contract: { executor: "ocg-build", model: "openai/m", role: "build" },
    runtime_execution_id: `session-${index}`,
    host_session_id: null,
    result: null,
    witness: {
      mission_id: "wn-x",
      work_node_id: index,
      run_id: index,
      run_generation: 1,
      runtime_execution_id: `session-${index}`,
      dispatch_id: `d-${index}`,
    },
  }));
  return applyCanonicalSnapshot(createCanonicalState(), {
    projectId: "zhuju" as ProjectId,
    generation: 1,
    payload: {
      api_version: CANONICAL_API_VERSION,
      project_id: "zhuju",
      cursor: 1,
      mission: {
        mission_id: "wn-x",
        root_node_id: 0,
        work_nodes: [
          { node_id: 0, parent_node_id: null, spawned_by_run_id: null, state: "ready", generation: 0, active_run_id: null, payload: "root" },
        ],
        dependencies: [],
        runs: missionRuns,
        events: [{ seq: 1, kind: "mission_created", payload: "{}", by_run_id: null }],
        verifications: [],
        late_results: [],
      },
    },
  });
}

test("project import validates a real repository root", () => {
  assert.deepEqual(validateImportRoot(""), [{ code: "root-required", field: "root", message: "Enter an existing repository path." }]);
  assert.equal(validateImportRoot("relative/path").length, 1);
  assert.deepEqual(validateImportRoot("/tmp/repo"), []);
  assert.deepEqual(validateImportRoot("./subdir"), []);
});

test("the import command identity is derived from the boundary, not a label", () => {
  assert.equal(projectImportCommandId("/tmp/repo"), projectImportCommandId("/tmp/repo"));
  assert.notEqual(projectImportCommandId("/tmp/repo"), projectImportCommandId("/tmp/other"));
  assert.match(projectImportCommandId("/tmp/repo"), /^cmd-project-import-/);
});

test("configuration drafts validate the supported global settings", () => {
  assert.equal(validateConfigurationDraft({ ...DEFAULT_CONFIGURATION_DRAFT, provider: "openai", model: "gpt" }).length, 0);
  assert.equal(validateConfigurationDraft({ ...DEFAULT_CONFIGURATION_DRAFT }).length, 2);
  const badBudget = validateConfigurationDraft({ ...DEFAULT_CONFIGURATION_DRAFT, provider: "p", model: "m", hardBudget: 0 });
  assert.ok(badBudget.some((issue) => issue.code === "budget-below-minimum"));
  const badProfile = validateConfigurationDraft({
    ...DEFAULT_CONFIGURATION_DRAFT,
    provider: "p",
    model: "m",
    profile: "reckless" as never,
  });
  assert.ok(badProfile.some((issue) => issue.code === "profile-invalid"));
});

test("the configuration draft round-trips through the backend global view", () => {
  const draft = { ...DEFAULT_CONFIGURATION_DRAFT, provider: "openai", model: "gpt-6-astra", hardBudget: 42 };
  const global = toGlobalConfiguration(draft);
  assert.equal(global.provider, "openai");
  assert.deepEqual(global.resource_budget, { hard_limit: 42, unit: "USD" });
  const restored = draftFromConfiguration({ project: PROJECT, global, project_defaults: { defaults: {} } });
  assert.deepEqual(restored, draft);
});

test("pre-run Mission configuration is frozen once a Run is dispatched", () => {
  const undispatched = preRunConfigurationState(canonicalState(0));
  assert.equal(undispatched.editable, true);
  assert.equal(undispatched.missionId, "wn-x");
  const dispatched = preRunConfigurationState(canonicalState(1));
  assert.equal(dispatched.editable, false);
  assert.match(dispatched.reason ?? "", /frozen executor\/model\/role contract/);
  const none = preRunConfigurationState(createCanonicalState());
  assert.equal(none.editable, false);
  assert.match(none.reason ?? "", /No canonical Mission/);
});

test("command identities are distinct per configuration scope", () => {
  assert.equal(globalConfigurationCommandId(2), globalConfigurationCommandId(2));
  assert.notEqual(globalConfigurationCommandId(2), globalConfigurationCommandId(3));
  assert.notEqual(projectDefaultsCommandId("project-a", 1), globalConfigurationCommandId(1));
});

test("acknowledgements report backend acceptance and rejection honestly", () => {
  assert.equal(describeAcknowledgement(null), "");
  assert.equal(
    describeAcknowledgement({ ok: true, commandId: "cmd-1" } as never),
    "cmd-1 accepted",
  );
  assert.equal(
    describeAcknowledgement({ ok: false, commandId: "cmd-2", status: 400, message: "frozen" } as never),
    "cmd-2 rejected: frozen",
  );
});

test("exactly one registered project is active for the projection scope", () => {
  const other: CanonicalProjectRecord = { ...PROJECT, project_id: "project-bbbb" };
  const listed = selectRegisteredProjects([PROJECT, other], "project-bbbb");
  assert.equal(listed.length, 2);
  assert.equal(listed.filter((item) => item.active).length, 1);
  assert.equal(listed.find((item) => item.active)?.project_id, "project-bbbb");
});
