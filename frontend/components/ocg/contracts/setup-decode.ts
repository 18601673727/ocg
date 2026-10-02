/**
 * Decoders for the setup / first-run control contract.
 *
 * Follows the same structural-decoder pattern as canonical-decode.ts:
 * every result is built explicitly from checked parts, never cast.
 */

import { decode, record, req, nullable, string, array, boolean, literal, yes } from "./decode";
import type { Decoder, DecodeResult } from "./decode";
import { PROFILE_API_VERSION, CANONICAL_API_VERSION } from "./generated";
import type { SetupModel, SetupConnectResponse, SetupModelsResponse, SetupDirectoryEntry, SetupBrowseResponse, SetupProjectResponse } from "./generated";
import { modelMetadata } from "./profile-decode";
export type { SetupModel, SetupConnectResponse, SetupModelsResponse, SetupDirectoryEntry, SetupBrowseResponse, SetupProjectResponse } from "./generated";

// -- decoders ----------------------------------------------------------------

const setupModel: Decoder<SetupModel> = (input, path) => {
  const rec = record(input, path, "a SetupModel");
  if (!rec.ok) return rec;
  const key = req(rec.value, "key", string, path);
  if (!key.ok) return key;
  const id = req(rec.value, "id", string, path);
  if (!id.ok) return id;
  const label = req(rec.value, "label", string, path);
  if (!label.ok) return label;
  const metadata = req(rec.value, "metadata", modelMetadata, path);
  if (!metadata.ok) return metadata;
  return yes({ key: key.value, id: id.value, label: label.value, metadata: metadata.value });
};

const setupConnectResponse: Decoder<SetupConnectResponse> = (input, path) => {
  const rec = record(input, path, "a SetupConnectResponse");
  if (!rec.ok) return rec;
  const api_version = req(rec.value, "api_version", literal(PROFILE_API_VERSION), path);
  if (!api_version.ok) return api_version;
  const provider_key = req(rec.value, "provider_key", string, path);
  if (!provider_key.ok) return provider_key;
  const models = req(rec.value, "models", array(setupModel), path);
  if (!models.ok) return models;
  const revision = req(rec.value, "revision", string, path);
  if (!revision.ok) return revision;
  return yes({
    api_version: api_version.value,
    provider_key: provider_key.value,
    models: models.value,
    revision: revision.value,
  });
};

const setupModelsResponse: Decoder<SetupModelsResponse> = (input, path) => {
  const rec = record(input, path, "a SetupModelsResponse");
  if (!rec.ok) return rec;
  const api_version = req(rec.value, "api_version", literal(PROFILE_API_VERSION), path);
  if (!api_version.ok) return api_version;
  const selected_models = req(rec.value, "selected_models", array(string), path);
  if (!selected_models.ok) return selected_models;
  const default_model = req(rec.value, "default_model", string, path);
  if (!default_model.ok) return default_model;
  const runnable_choices = req(rec.value, "runnable_choices", array(string), path);
  if (!runnable_choices.ok) return runnable_choices;
  const revision = req(rec.value, "revision", string, path);
  if (!revision.ok) return revision;
  return yes({
    api_version: api_version.value,
    selected_models: selected_models.value,
    default_model: default_model.value,
    runnable_choices: runnable_choices.value,
    revision: revision.value,
  });
};

const setupDirectoryEntry: Decoder<SetupDirectoryEntry> = (input, path) => {
  const rec = record(input, path, "a SetupDirectoryEntry");
  if (!rec.ok) return rec;
  const name = req(rec.value, "name", string, path);
  if (!name.ok) return name;
  const entryPath = req(rec.value, "path", string, path);
  if (!entryPath.ok) return entryPath;
  const is_dir = req(rec.value, "is_dir", boolean, path);
  if (!is_dir.ok) return is_dir;
  return yes({ name: name.value, path: entryPath.value, is_dir: is_dir.value });
};

const setupBrowseResponse: Decoder<SetupBrowseResponse> = (input, path) => {
  const rec = record(input, path, "a SetupBrowseResponse");
  if (!rec.ok) return rec;
  const current = req(rec.value, "current", string, path);
  if (!current.ok) return current;
  const parent = req(rec.value, "parent", nullable(string), path);
  if (!parent.ok) return parent;
  const entries = req(rec.value, "entries", array(setupDirectoryEntry), path);
  if (!entries.ok) return entries;
  return yes({ current: current.value, parent: parent.value, entries: entries.value });
};

const setupProjectResponse: Decoder<SetupProjectResponse> = (input, path) => {
  const rec = record(input, path, "a SetupProjectResponse");
  if (!rec.ok) return rec;
  const api_version = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
  if (!api_version.ok) return api_version;
  const project_id = req(rec.value, "project_id", string, path);
  if (!project_id.ok) return project_id;
  const name = req(rec.value, "name", string, path);
  if (!name.ok) return name;
  const root = req(rec.value, "root", string, path);
  if (!root.ok) return root;
  return yes({
    api_version: api_version.value,
    project_id: project_id.value,
    name: name.value,
    root: root.value,
  });
};

// -- public decode wrappers --------------------------------------------------

export const decodeSetupConnectResponse = (input: unknown): SetupConnectResponse =>
  decode((value) => setupConnectResponse(value, ""), input);

export const decodeSetupModelsResponse = (input: unknown): SetupModelsResponse =>
  decode((value) => setupModelsResponse(value, ""), input);

export const decodeSetupBrowseResponse = (input: unknown): SetupBrowseResponse =>
  decode((value) => setupBrowseResponse(value, ""), input);

export const decodeSetupProjectResponse = (input: unknown): SetupProjectResponse =>
  decode((value) => setupProjectResponse(value, ""), input);

export const trySetupConnectResponse = (input: unknown): DecodeResult<SetupConnectResponse> =>
  setupConnectResponse(input, "");

export const trySetupModelsResponse = (input: unknown): DecodeResult<SetupModelsResponse> =>
  setupModelsResponse(input, "");

export const trySetupBrowseResponse = (input: unknown): DecodeResult<SetupBrowseResponse> =>
  setupBrowseResponse(input, "");

export const trySetupProjectResponse = (input: unknown): DecodeResult<SetupProjectResponse> =>
  setupProjectResponse(input, "");
