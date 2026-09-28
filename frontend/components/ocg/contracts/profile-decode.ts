/**
 * Runtime decoders for the Profile control contract.
 *
 * The shapes below are the generated projection of `src/contracts.rs`. Each
 * decoder builds its value explicitly from checked parts, so a **required**
 * Rust field that is added, renamed or retyped fails `tsc` here rather than
 * reaching a component as `undefined`. A newly optional field is ignored on
 * purpose; see `decode.ts` for the rule.
 */

import type { Candidate, Model, Origin, Profile, ProfileView, Provider } from "./generated";
import { PROFILE_API_VERSION } from "./generated";
import {
  array,
  boolean,
  decode,
  literal,
  nullable,
  opt,
  record,
  req,
  string,
  stringMap,
  wrapped,
  yes,
  type DecodeResult,
  type Decoder,
} from "./decode";

type ImportedOrigin = { imported: { source: string; scope: string; location: string; sha256: string } };

const provider: Decoder<Provider> = (input, path) => {
  const rec = record(input, path, "a Provider");
  if (!rec.ok) return rec;
  const placeholder = req(rec.value, "placeholder", boolean, path);
  if (!placeholder.ok) return placeholder;
  const label = req(rec.value, "label", string, path);
  if (!label.ok) return label;
  return yes({ placeholder: placeholder.value, label: label.value });
};

const model: Decoder<Model> = (input, path) => {
  const rec = record(input, path, "a Model");
  if (!rec.ok) return rec;
  const placeholder = req(rec.value, "placeholder", boolean, path);
  if (!placeholder.ok) return placeholder;
  const owner = req(rec.value, "provider", string, path);
  if (!owner.ok) return owner;
  const id = req(rec.value, "id", string, path);
  if (!id.ok) return id;
  // `variant` and `variants` are `skip_serializing_if`, so absence is normal.
  const variant = opt(rec.value, "variant", string, path);
  if (!variant.ok) return variant;
  const variants = opt(rec.value, "variants", array(string), path);
  if (!variants.ok) return variants;
  return yes({
    placeholder: placeholder.value,
    provider: owner.value,
    id: id.value,
    variant: variant.value,
    variants: variants.value,
  });
};

const importedOrigin: Decoder<ImportedOrigin["imported"]> = (input, path) => {
  const rec = record(input, path, "an imported Origin payload");
  if (!rec.ok) return rec;
  const source = req(rec.value, "source", string, path);
  if (!source.ok) return source;
  const scope = req(rec.value, "scope", string, path);
  if (!scope.ok) return scope;
  const location = req(rec.value, "location", string, path);
  if (!location.ok) return location;
  const sha256 = req(rec.value, "sha256", string, path);
  if (!sha256.ok) return sha256;
  return yes({
    source: source.value,
    scope: scope.value,
    location: location.value,
    sha256: sha256.value,
  });
};

const origin: Decoder<Origin> = (input, path) => {
  // serde's externally tagged union: a bare string, or a single-key object.
  if (input === "new") return yes("new");
  return wrapped("imported", importedOrigin)(input, path);
};

const candidate: Decoder<Candidate> = (input, path) => {
  const rec = record(input, path, "a Candidate");
  if (!rec.ok) return rec;
  const source = req(rec.value, "source", string, path);
  if (!source.ok) return source;
  const scope = req(rec.value, "scope", string, path);
  if (!scope.ok) return scope;
  const location = req(rec.value, "location", string, path);
  if (!location.ok) return location;
  const sha256 = req(rec.value, "sha256", string, path);
  if (!sha256.ok) return sha256;
  const providerNames = req(rec.value, "provider_names", array(string), path);
  if (!providerNames.ok) return providerNames;
  const modelIds = req(rec.value, "model_ids", array(string), path);
  if (!modelIds.ok) return modelIds;
  const variants = req(rec.value, "variants", stringMap(array(string)), path);
  if (!variants.ok) return variants;
  const importable = req(rec.value, "importable_fields", array(string), path);
  if (!importable.ok) return importable;
  const ignored = req(rec.value, "ignored_fields", array(string), path);
  if (!ignored.ok) return ignored;
  return yes({
    source: source.value,
    scope: scope.value,
    location: location.value,
    sha256: sha256.value,
    provider_names: providerNames.value,
    model_ids: modelIds.value,
    variants: variants.value,
    importable_fields: importable.value,
    ignored_fields: ignored.value,
  });
};

const profile: Decoder<Profile> = (input, path) => {
  const rec = record(input, path, "a Profile");
  if (!rec.ok) return rec;
  const profileOrigin = req(rec.value, "origin", origin, path);
  if (!profileOrigin.ok) return profileOrigin;
  const defaultModel = opt(rec.value, "defaultModel", string, path);
  if (!defaultModel.ok) return defaultModel;
  const providers = req(rec.value, "providers", stringMap(provider), path);
  if (!providers.ok) return providers;
  const models = req(rec.value, "models", stringMap(model), path);
  if (!models.ok) return models;
  return yes({
    origin: profileOrigin.value,
    defaultModel: defaultModel.value,
    providers: providers.value,
    models: models.value,
  });
};

const profileView: Decoder<ProfileView> = (input, path) => {
  const rec = record(input, path, "a ProfileView");
  if (!rec.ok) return rec;
  // Checking the version first is what stops the PWA from rendering a payload
  // from a backend whose protocol it does not understand.
  const apiVersion = req(rec.value, "api_version", literal(PROFILE_API_VERSION), path);
  if (!apiVersion.ok) return apiVersion;
  const value = req(rec.value, "profile", nullable(profile), path);
  if (!value.ok) return value;
  const revision = req(rec.value, "revision", nullable(string), path);
  if (!revision.ok) return revision;
  const candidates = req(rec.value, "candidates", array(candidate), path);
  if (!candidates.ok) return candidates;
  return yes({
    api_version: apiVersion.value,
    profile: value.value,
    revision: revision.value,
    candidates: candidates.value,
  });
};

export const decodeProvider = (input: unknown, path = "provider"): Provider =>
  decode((value) => provider(value, path), input);

export const decodeProfile = (input: unknown, path = "profile"): Profile =>
  decode((value) => profile(value, path), input);

export const decodeCandidate = (input: unknown, path = "candidate"): Candidate =>
  decode((value) => candidate(value, path), input);

export const decodeProfileView = (input: unknown): ProfileView =>
  decode((value) => profileView(value, ""), input);

/** Non-throwing variants, for callers that want to inspect rather than raise. */
export const tryProfileView = (input: unknown): DecodeResult<ProfileView> => profileView(input, "");
