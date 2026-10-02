/**
 * Decoders for the setup / first-run control contract.
 *
 * Follows the same structural-decoder pattern as canonical-decode.ts:
 * every result is built explicitly from checked parts, never cast.
 */

import { decode, record, req, opt, string, array, boolean, jsonValue, yes } from "./decode";
import type { Decoder, DecodeResult } from "./decode";
import type { JsonValue } from "./generated";

// -- types -------------------------------------------------------------------

export interface SetupModel {
  key: string;
  id: string;
  label: string;
  metadata: JsonValue;
}

export interface SetupConnectResponse {
  api_version: string;
  provider_key: string;
  credential_ref: string;
  models: SetupModel[];
  revision: string;
}

export interface SetupModelsResponse {
  api_version: string;
  runnable_choices: string[];
  revision: string;
}

export interface SetupDirectoryEntry {
  name: string;
  path: string;
  is_dir: boolean;
}

export interface SetupBrowseResponse {
  current: string;
  parent: string | undefined;
  entries: SetupDirectoryEntry[];
}

export interface SetupProjectResponse {
  api_version: string;
  project_id: string;
  name: string;
  root: string;
}

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
  const metadata = req(rec.value, "metadata", jsonValue, path);
  if (!metadata.ok) return metadata;
  return yes({ key: key.value, id: id.value, label: label.value, metadata: metadata.value });
};

const setupConnectResponse: Decoder<SetupConnectResponse> = (input, path) => {
  const rec = record(input, path, "a SetupConnectResponse");
  if (!rec.ok) return rec;
  const api_version = req(rec.value, "api_version", string, path);
  if (!api_version.ok) return api_version;
  const provider_key = req(rec.value, "provider_key", string, path);
  if (!provider_key.ok) return provider_key;
  const credential_ref = req(rec.value, "credential_ref", string, path);
  if (!credential_ref.ok) return credential_ref;
  const models = req(rec.value, "models", array(setupModel), path);
  if (!models.ok) return models;
  const revision = req(rec.value, "revision", string, path);
  if (!revision.ok) return revision;
  return yes({
    api_version: api_version.value,
    provider_key: provider_key.value,
    credential_ref: credential_ref.value,
    models: models.value,
    revision: revision.value,
  });
};

const setupModelsResponse: Decoder<SetupModelsResponse> = (input, path) => {
  const rec = record(input, path, "a SetupModelsResponse");
  if (!rec.ok) return rec;
  const api_version = req(rec.value, "api_version", string, path);
  if (!api_version.ok) return api_version;
  const runnable_choices = req(rec.value, "runnable_choices", array(string), path);
  if (!runnable_choices.ok) return runnable_choices;
  const revision = req(rec.value, "revision", string, path);
  if (!revision.ok) return revision;
  return yes({
    api_version: api_version.value,
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
  const parent = opt(rec.value, "parent", string, path);
  if (!parent.ok) return parent;
  const entries = req(rec.value, "entries", array(setupDirectoryEntry), path);
  if (!entries.ok) return entries;
  return yes({ current: current.value, parent: parent.value, entries: entries.value });
};

const setupProjectResponse: Decoder<SetupProjectResponse> = (input, path) => {
  const rec = record(input, path, "a SetupProjectResponse");
  if (!rec.ok) return rec;
  const api_version = req(rec.value, "api_version", string, path);
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
