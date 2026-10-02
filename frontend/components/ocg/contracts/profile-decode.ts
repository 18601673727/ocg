/**
 * Runtime decoders for the Profile control contract.
 *
 * The shapes below are the generated projection of `src/contracts.rs`. Each
 * decoder builds its value explicitly from checked parts, so a **required**
 * Rust field that is added, renamed or retyped fails `tsc` here rather than
 * reaching a component as `undefined`. A newly optional field is ignored on
 * purpose; see `decode.ts` for the rule.
 */

import type { Model, Origin, Profile, ProfileView, Provider } from "./generated";
import { PROFILE_API_VERSION } from "./generated";
import {
  array,
  bad,
  boolean,
  decode,
  literal,
  nullable,
  opt,
  record,
  req,
  string,
  stringMap,
  yes,
  type DecodeResult,
  type Decoder,
} from "./decode";

const provider: Decoder<Provider> = (input, path) => {
  const rec = record(input, path, "a Provider");
  if (!rec.ok) return rec;
  const placeholder = req(rec.value, "placeholder", boolean, path);
  if (!placeholder.ok) return placeholder;
  const label = req(rec.value, "label", string, path);
  if (!label.ok) return label;
  // `endpoint` and `credential_ref` are `skip_serializing_if`, so absence is
  // normal. They must be read (not dropped) so an edit round-trip preserves
  // the provider's execution wiring.
  const endpoint = opt(rec.value, "endpoint", string, path);
  if (!endpoint.ok) return endpoint;
  const credentialRef = opt(rec.value, "credential_ref", string, path);
  if (!credentialRef.ok) return credentialRef;
  return yes({
    placeholder: placeholder.value,
    label: label.value,
    endpoint: endpoint.value,
    credential_ref: credentialRef.value,
  });
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

const origin: Decoder<Origin> = (input, path) => {
  if (input === "new") return yes("new");
  return bad(path || "origin", '"new"');
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
  // Backend-computed execution readiness. Required: the PWA must decide
  // onboarding vs workspace from the backend authority, never re-derived.
  const runnableChoices = req(rec.value, "runnable_choices", array(string), path);
  if (!runnableChoices.ok) return runnableChoices;
  return yes({
    api_version: apiVersion.value,
    profile: value.value,
    revision: revision.value,
    runnable_choices: runnableChoices.value,
  });
};

export const decodeProvider = (input: unknown, path = "provider"): Provider =>
  decode((value) => provider(value, path), input);

export const decodeProfile = (input: unknown, path = "profile"): Profile =>
  decode((value) => profile(value, path), input);

export const decodeProfileView = (input: unknown): ProfileView =>
  decode((value) => profileView(value, ""), input);

/** Non-throwing variants, for callers that want to inspect rather than raise. */
export const tryProfileView = (input: unknown): DecodeResult<ProfileView> => profileView(input, "");
