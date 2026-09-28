/**
 * Runtime decoders for the canonical control contract.
 *
 * The shapes below are the generated projection of `src/contracts.rs`. These
 * replace the unchecked `as` casts the canonical client used: a payload whose
 * shape has drifted from the Rust definition now raises a contract violation
 * naming the offending path instead of reaching a component as `undefined`.
 */

import type {
  CanonicalConfigurationEnvelope,
  CanonicalConfigurationResponse,
  CanonicalDashboardResponse,
  CanonicalEventsEnvelope,
  CanonicalMissionConfigEnvelope,
  CanonicalMissionResponse,
  CanonicalProjectResponse,
  CanonicalProjectsResponse,
  CanonicalWorkEvent,
  CanonicalWorkSnapshot,
  DispatchWitness,
  GlobalConfiguration,
  ProjectConfiguration,
  ProjectConfigurationView,
  ProjectRecord,
  ResourceBudget,
} from "./generated";
import { CANONICAL_API_VERSION } from "./generated";
import {
  array,
  atLeast,
  boolean,
  decode,
  identity,
  index,
  jsonValue,
  literal,
  nullable,
  number,
  record,
  req,
  string,
  yes,
  type DecodeResult,
  type Decoder,
} from "./decode";

const projectRecord: Decoder<ProjectRecord> = (input, path) => {
  const rec = record(input, path, "a ProjectRecord");
  if (!rec.ok) return rec;
  const projectId = req(rec.value, "project_id", string, path);
  if (!projectId.ok) return projectId;
  const root = req(rec.value, "root", string, path);
  if (!root.ok) return root;
  const boundary = req(rec.value, "boundary", string, path);
  if (!boundary.ok) return boundary;
  const marker = req(rec.value, "marker", boolean, path);
  if (!marker.ok) return marker;
  const createdAt = req(rec.value, "created_at", number, path);
  if (!createdAt.ok) return createdAt;
  const updatedAt = req(rec.value, "updated_at", number, path);
  if (!updatedAt.ok) return updatedAt;
  return yes({
    project_id: projectId.value,
    root: root.value,
    boundary: boundary.value,
    marker: marker.value,
    created_at: createdAt.value,
    updated_at: updatedAt.value,
  });
};

/**
 * A dispatch witness read out of a canonical projection.
 *
 * The field rules mirror `DispatchWitness::from_json` in
 * `src/orchestration/substrate.rs`, which is the authority: identity strings
 * are non-empty, indices are non-negative integers, and a Run generation starts
 * at 1. Those refinements are not expressible in a TypeScript type, so they are
 * checked here against the same rules rather than invented separately.
 */
export const dispatchWitness: Decoder<DispatchWitness> = (input, path) => {
  const rec = record(input, path, "a DispatchWitness");
  if (!rec.ok) return rec;
  const missionId = req(rec.value, "mission_id", identity, path);
  if (!missionId.ok) return missionId;
  const workNodeId = req(rec.value, "work_node_id", index, path);
  if (!workNodeId.ok) return workNodeId;
  const runId = req(rec.value, "run_id", index, path);
  if (!runId.ok) return runId;
  const runGeneration = req(rec.value, "run_generation", atLeast(1), path);
  if (!runGeneration.ok) return runGeneration;
  const runtimeExecutionId = req(rec.value, "runtime_execution_id", identity, path);
  if (!runtimeExecutionId.ok) return runtimeExecutionId;
  const dispatchId = req(rec.value, "dispatch_id", identity, path);
  if (!dispatchId.ok) return dispatchId;
  return yes({
    mission_id: missionId.value,
    work_node_id: workNodeId.value,
    run_id: runId.value,
    run_generation: runGeneration.value,
    runtime_execution_id: runtimeExecutionId.value,
    dispatch_id: dispatchId.value,
  });
};

const resourceBudget: Decoder<ResourceBudget> = (input, path) => {
  const rec = record(input, path, "a ResourceBudget");
  if (!rec.ok) return rec;
  const hardLimit = req(rec.value, "hard_limit", number, path);
  if (!hardLimit.ok) return hardLimit;
  const unit = req(rec.value, "unit", string, path);
  if (!unit.ok) return unit;
  return yes({ hard_limit: hardLimit.value, unit: unit.value });
};

const globalConfiguration: Decoder<GlobalConfiguration> = (input, path) => {
  const rec = record(input, path, "a GlobalConfiguration");
  if (!rec.ok) return rec;
  const provider = req(rec.value, "provider", nullable(string), path);
  if (!provider.ok) return provider;
  const model = req(rec.value, "model", nullable(string), path);
  if (!model.ok) return model;
  const profileName = req(rec.value, "profile", nullable(string), path);
  if (!profileName.ok) return profileName;
  const routing = req(rec.value, "routing", nullable(string), path);
  if (!routing.ok) return routing;
  const runtime = req(rec.value, "runtime", nullable(string), path);
  if (!runtime.ok) return runtime;
  const budget = req(rec.value, "resource_budget", nullable(resourceBudget), path);
  if (!budget.ok) return budget;
  return yes({
    provider: provider.value,
    model: model.value,
    profile: profileName.value,
    routing: routing.value,
    runtime: runtime.value,
    resource_budget: budget.value,
  });
};

const projectConfiguration: Decoder<ProjectConfiguration> = (input, path) => {
  const rec = record(input, path, "a ProjectConfiguration");
  if (!rec.ok) return rec;
  const defaults = req(rec.value, "defaults", jsonValue, path);
  if (!defaults.ok) return defaults;
  return yes({ defaults: defaults.value });
};

const configurationView: Decoder<ProjectConfigurationView> = (input, path) => {
  const rec = record(input, path, "a ProjectConfigurationView");
  if (!rec.ok) return rec;
  const project = req(rec.value, "project", projectRecord, path);
  if (!project.ok) return project;
  const global = req(rec.value, "global", globalConfiguration, path);
  if (!global.ok) return global;
  const projectDefaults = req(rec.value, "project_defaults", projectConfiguration, path);
  if (!projectDefaults.ok) return projectDefaults;
  return yes({
    project: project.value,
    global: global.value,
    project_defaults: projectDefaults.value,
  });
};

const workEvent: Decoder<CanonicalWorkEvent> = (input, path) => {
  const rec = record(input, path, "a CanonicalWorkEvent");
  if (!rec.ok) return rec;
  const apiVersion = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
  if (!apiVersion.ok) return apiVersion;
  const projectId = req(rec.value, "project_id", string, path);
  if (!projectId.ok) return projectId;
  const missionId = req(rec.value, "mission_id", string, path);
  if (!missionId.ok) return missionId;
  const sequence = req(rec.value, "sequence", number, path);
  if (!sequence.ok) return sequence;
  const eventId = req(rec.value, "event_id", string, path);
  if (!eventId.ok) return eventId;
  const kind = req(rec.value, "kind", string, path);
  if (!kind.ok) return kind;
  const payload = req(rec.value, "payload", jsonValue, path);
  if (!payload.ok) return payload;
  return yes({
    api_version: apiVersion.value,
    project_id: projectId.value,
    mission_id: missionId.value,
    sequence: sequence.value,
    event_id: eventId.value,
    kind: kind.value,
    payload: payload.value,
  });
};

const workSnapshot: Decoder<CanonicalWorkSnapshot> = (input, path) => {
  const rec = record(input, path, "a CanonicalWorkSnapshot");
  if (!rec.ok) return rec;
  const apiVersion = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
  if (!apiVersion.ok) return apiVersion;
  const projectId = req(rec.value, "project_id", string, path);
  if (!projectId.ok) return projectId;
  const mission = req(rec.value, "mission", jsonValue, path);
  if (!mission.ok) return mission;
  const cursor = req(rec.value, "cursor", number, path);
  if (!cursor.ok) return cursor;
  return yes({
    api_version: apiVersion.value,
    project_id: projectId.value,
    mission: mission.value,
    cursor: cursor.value,
  });
};

const projectsResponse: Decoder<CanonicalProjectsResponse> = (input, path) => {
  const rec = record(input, path, "a CanonicalProjectsResponse");
  if (!rec.ok) return rec;
  const apiVersion = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
  if (!apiVersion.ok) return apiVersion;
  const projects = req(rec.value, "projects", array(projectRecord), path);
  if (!projects.ok) return projects;
  return yes({ api_version: apiVersion.value, projects: projects.value });
};

const projectResponse: Decoder<CanonicalProjectResponse> = (input, path) => {
  const rec = record(input, path, "a CanonicalProjectResponse");
  if (!rec.ok) return rec;
  const apiVersion = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
  if (!apiVersion.ok) return apiVersion;
  const commandId = req(rec.value, "command_id", string, path);
  if (!commandId.ok) return commandId;
  const accepted = req(rec.value, "accepted", boolean, path);
  if (!accepted.ok) return accepted;
  const project = req(rec.value, "project", projectRecord, path);
  if (!project.ok) return project;
  return yes({
    api_version: apiVersion.value,
    command_id: commandId.value,
    accepted: accepted.value,
    project: project.value,
  });
};

const configurationEnvelope: Decoder<CanonicalConfigurationEnvelope> = (input, path) => {
  const rec = record(input, path, "a CanonicalConfigurationEnvelope");
  if (!rec.ok) return rec;
  const apiVersion = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
  if (!apiVersion.ok) return apiVersion;
  const configuration = req(rec.value, "configuration", configurationView, path);
  if (!configuration.ok) return configuration;
  return yes({ api_version: apiVersion.value, configuration: configuration.value });
};

const missionConfigEnvelope: Decoder<CanonicalMissionConfigEnvelope> = (input, path) => {
  const rec = record(input, path, "a CanonicalMissionConfigEnvelope");
  if (!rec.ok) return rec;
  const apiVersion = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
  if (!apiVersion.ok) return apiVersion;
  const missionId = req(rec.value, "mission_id", string, path);
  if (!missionId.ok) return missionId;
  const configuration = req(rec.value, "configuration", jsonValue, path);
  if (!configuration.ok) return configuration;
  const revision = req(rec.value, "revision", number, path);
  if (!revision.ok) return revision;
  return yes({
    api_version: apiVersion.value,
    mission_id: missionId.value,
    configuration: configuration.value,
    revision: revision.value,
  });
};

const eventsEnvelope: Decoder<CanonicalEventsEnvelope> = (input, path) => {
  const rec = record(input, path, "a CanonicalEventsEnvelope");
  if (!rec.ok) return rec;
  const apiVersion = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
  if (!apiVersion.ok) return apiVersion;
  const projectId = req(rec.value, "project_id", string, path);
  if (!projectId.ok) return projectId;
  const missionId = req(rec.value, "mission_id", string, path);
  if (!missionId.ok) return missionId;
  const events = req(rec.value, "events", array(workEvent), path);
  if (!events.ok) return events;
  return yes({
    api_version: apiVersion.value,
    project_id: projectId.value,
    mission_id: missionId.value,
    events: events.value,
  });
};

const dashboardResponse: Decoder<CanonicalDashboardResponse> = (input, path) => {
  const rec = record(input, path, "a CanonicalDashboardResponse");
  if (!rec.ok) return rec;
  const apiVersion = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
  if (!apiVersion.ok) return apiVersion;
  const projectId = req(rec.value, "project_id", string, path);
  if (!projectId.ok) return projectId;
  const missions = req(rec.value, "missions", array(jsonValue), path);
  if (!missions.ok) return missions;
  const selectedMission = req(rec.value, "selected_mission", nullable(workSnapshot), path);
  if (!selectedMission.ok) return selectedMission;
  return yes({
    api_version: apiVersion.value,
    project_id: projectId.value,
    missions: missions.value,
    selected_mission: selectedMission.value,
  });
};

/**
 * The acknowledgement shape shared by the three configuration writes.
 *
 * The backend returns `CanonicalConfigurationResponse`, which carries the
 * command identity it actually accepted. Decoding it here is what lets the
 * client report the server's `command_id` and `accepted` flag instead of
 * echoing back the id it hoped would be honoured.
 */
export type ConfigurationAck = CanonicalConfigurationResponse;

const configurationAck: Decoder<ConfigurationAck> = (input, path) => {
  const rec = record(input, path, "a CanonicalConfigurationResponse");
  if (!rec.ok) return rec;
  const apiVersion = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
  if (!apiVersion.ok) return apiVersion;
  const commandId = req(rec.value, "command_id", string, path);
  if (!commandId.ok) return commandId;
  const accepted = req(rec.value, "accepted", boolean, path);
  if (!accepted.ok) return accepted;
  const projectId = req(rec.value, "project_id", string, path);
  if (!projectId.ok) return projectId;
  const revision = req(rec.value, "revision", number, path);
  if (!revision.ok) return revision;
  const configuration = req(rec.value, "configuration", configurationView, path);
  if (!configuration.ok) return configuration;
  return yes({
    api_version: apiVersion.value,
    command_id: commandId.value,
    accepted: accepted.value,
    project_id: projectId.value,
    revision: revision.value,
    configuration: configuration.value,
  });
};

const missionResponse: Decoder<CanonicalMissionResponse> = (input, path) => {
  const rec = record(input, path, "a CanonicalMissionResponse");
  if (!rec.ok) return rec;
  const apiVersion = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
  if (!apiVersion.ok) return apiVersion;
  const commandId = req(rec.value, "command_id", string, path);
  if (!commandId.ok) return commandId;
  const accepted = req(rec.value, "accepted", boolean, path);
  if (!accepted.ok) return accepted;
  const missionId = req(rec.value, "mission_id", string, path);
  if (!missionId.ok) return missionId;
  const revision = req(rec.value, "revision", number, path);
  if (!revision.ok) return revision;
  const configuration = req(rec.value, "configuration", jsonValue, path);
  if (!configuration.ok) return configuration;
  return yes({
    api_version: apiVersion.value,
    command_id: commandId.value,
    accepted: accepted.value,
    mission_id: missionId.value,
    revision: revision.value,
    configuration: configuration.value,
  });
};

export const decodeProjectRecord = (input: unknown, path = "project"): ProjectRecord =>
  decode((value) => projectRecord(value, path), input);

export const decodeConfigurationView = (
  input: unknown,
  path = "configuration",
): ProjectConfigurationView => decode((value) => configurationView(value, path), input);

export const decodeProjectsResponse = (input: unknown): CanonicalProjectsResponse =>
  decode((value) => projectsResponse(value, ""), input);

export const decodeProjectResponse = (input: unknown): CanonicalProjectResponse =>
  decode((value) => projectResponse(value, ""), input);

export const decodeConfigurationEnvelope = (input: unknown): CanonicalConfigurationEnvelope =>
  decode((value) => configurationEnvelope(value, ""), input);

export const decodeConfigurationAck = (input: unknown): ConfigurationAck =>
  decode((value) => configurationAck(value, ""), input);

export const decodeMissionConfigEnvelope = (input: unknown): CanonicalMissionConfigEnvelope =>
  decode((value) => missionConfigEnvelope(value, ""), input);

export const decodeMissionResponse = (input: unknown): CanonicalMissionResponse =>
  decode((value) => missionResponse(value, ""), input);

export const decodeEventsEnvelope = (input: unknown): CanonicalEventsEnvelope =>
  decode((value) => eventsEnvelope(value, ""), input);

export const decodeDashboardResponse = (input: unknown): CanonicalDashboardResponse =>
  decode((value) => dashboardResponse(value, ""), input);

export const decodeWorkSnapshot = (input: unknown): CanonicalWorkSnapshot =>
  decode((value) => workSnapshot(value, ""), input);

export const tryEventsEnvelope = (input: unknown): DecodeResult<CanonicalEventsEnvelope> =>
  eventsEnvelope(input, "");

/**
 * Read a dispatch witness out of a canonical projection.
 *
 * The witness travels inside a `JsonValue` payload, so it is checked against
 * the Rust definition here rather than assembled field by field in the
 * projection. A witness that does not match is `null`, never a partially
 * populated stand-in: the projection refuses to show authority it cannot
 * verify. This is read-only. The PWA never mints, edits, or completes a
 * witness.
 */
export function decodeWitness(value: unknown, path = "witness"): DispatchWitness | null {
  const result = dispatchWitness(value, path);
  return result.ok ? result.value : null;
}
