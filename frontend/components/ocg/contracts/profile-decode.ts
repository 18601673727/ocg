/**
 * Runtime decoders for the Profile control contract.
 *
 * The shapes below are the generated projection of `src/contracts.rs`. Each
 * decoder builds its value explicitly from checked parts, so a **required**
 * Rust field that is added, renamed or retyped fails `tsc` here rather than
 * reaching a component as `undefined`. A newly optional field is ignored on
 * purpose; see `decode.ts` for the rule.
 */

import type { Model, ModelMetadata, CatalogModel, ProviderCatalog, Origin, Profile, ProfileView, Provider } from "./generated";
import { PROFILE_API_VERSION } from "./generated";
import {
  array,
  bad,
  boolean,
  index,
  jsonValue,
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

export const modelMetadata: Decoder<ModelMetadata> = (input, path) => {
  const rec = record(input, path, "model metadata");
  if (!rec.ok) return rec;
  const variant = req(rec.value, "variant", nullable(string), path);
  if (!variant.ok) return variant;
  const variants = req(rec.value, "variants", nullable(array(string)), path);
  if (!variants.ok) return variants;
  const label = opt(rec.value, "label", string, path);
  if (!label.ok) return label;
  const metadata = opt(rec.value, "metadata", modelMetadata, path);
  if (!metadata.ok) return metadata;
  const effort = req(rec.value, "effort", nullable(string), path);
  if (!effort.ok) return effort;
  const efforts = req(rec.value, "efforts", nullable(array(string)), path);
  if (!efforts.ok) return efforts;
  const reasoning = req(rec.value, "reasoning", nullable(boolean), path);
  if (!reasoning.ok) return reasoning;
  const fast_mode = req(rec.value, "fast_mode", nullable(boolean), path);
  if (!fast_mode.ok) return fast_mode;
  const context_window = req(rec.value, "context_window", nullable(index), path);
  if (!context_window.ok) return context_window;
  const tools = req(rec.value, "tools", nullable(boolean), path);
  if (!tools.ok) return tools;
  const images = req(rec.value, "images", nullable(boolean), path);
  if (!images.ok) return images;
  const multimodal = req(rec.value, "multimodal", nullable(boolean), path);
  if (!multimodal.ok) return multimodal;
  const pricing = req(rec.value, "pricing", nullable(jsonValue), path);
  if (!pricing.ok) return pricing;
  return yes({
    variant: variant.value,
    variants: variants.value,
    label: label.value,
    metadata: metadata.value,
    effort: effort.value,
    efforts: efforts.value,
    reasoning: reasoning.value,
    fast_mode: fast_mode.value,
    context_window: context_window.value,
    tools: tools.value,
    images: images.value,
    multimodal: multimodal.value,
    pricing: pricing.value,
  });
};

const catalogModel: Decoder<CatalogModel> = (input, path) => {
  const rec = record(input, path, "a catalog model");
  if (!rec.ok) return rec;
  const id = req(rec.value, "id", string, path);
  if (!id.ok) return id;
  const label = req(rec.value, "label", string, path);
  if (!label.ok) return label;
  const metadata = req(rec.value, "metadata", modelMetadata, path);
  if (!metadata.ok) return metadata;
  const raw = req(rec.value, "raw", jsonValue, path);
  if (!raw.ok) return raw;
  return yes({ id: id.value, label: label.value, metadata: metadata.value, raw: raw.value });
};

const catalog: Decoder<ProviderCatalog> = (input, path) => {
  const rec = record(input, path, "a provider catalog");
  if (!rec.ok) return rec;
  const discoveredAt = req(rec.value, "discovered_at", index, path);
  if (!discoveredAt.ok) return discoveredAt;
  const models = req(rec.value, "models", array(catalogModel), path);
  if (!models.ok) return models;
  return yes({ discovered_at: discoveredAt.value, models: models.value });
};

const provider: Decoder<Provider> = (input, path) => {
  const rec = record(input, path, "a Provider");
  if (!rec.ok) return rec;
  const label = req(rec.value, "label", string, path);
  if (!label.ok) return label;
  // `endpoint` and `credential_ref` are `skip_serializing_if`, so absence is
  // normal. They must be read (not dropped) so an edit round-trip preserves
  // the provider's execution wiring.
  const endpoint = opt(rec.value, "endpoint", string, path);
  if (!endpoint.ok) return endpoint;
  const credentialRef = opt(rec.value, "credential_ref", string, path);
  if (!credentialRef.ok) return credentialRef;
  const observedCatalog = opt(rec.value, "catalog", catalog, path);
  if (!observedCatalog.ok) return observedCatalog;
  return yes({
    label: label.value,
    endpoint: endpoint.value,
    credential_ref: credentialRef.value,
    catalog: observedCatalog.value,
  });
};

const model: Decoder<Model> = (input, path) => {
  const rec = record(input, path, "a Model");
  if (!rec.ok) return rec;
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
