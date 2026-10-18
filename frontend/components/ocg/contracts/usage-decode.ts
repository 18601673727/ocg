import type { UsageQuantity, UsageTokenTotals, UsageCurrencyCost, UsageCost, ContextCostTotals, UsageTotals, UsageBreakdown, UsageConversationRow, ProjectUsageResponse, ConversationUsageResponse, JobUsageResponse, UsageCompleteness, UsageWindow } from "./generated";
import { CANONICAL_API_VERSION } from "./generated";
import { array, boolean, decode, identity, bad, string, literal, nullable, oneOf, record, req, yes, type Decoder } from "./decode";

const index: Decoder<number> = (input, path) => typeof input === "number" && Number.isSafeInteger(input) && input >= 0 ? yes(input) : bad(path, "a safe non-negative integer");

const usageQuantity: Decoder<UsageQuantity> = (payload, path) => {
  const rec = record(payload, path, "UsageQuantity");
  if (!rec.ok) return rec;
  const value = req(rec.value, "value", nullable(index), path);
  if (!value.ok) return value;
  const completeness = req(rec.value, "completeness", oneOf<UsageCompleteness>(["complete", "partial", "unavailable"]), path);
  if (!completeness.ok) return completeness;
  return yes({
    value: value.value,
    completeness: completeness.value,
  });
};

const usageTokenTotals: Decoder<UsageTokenTotals> = (payload, path) => {
  const rec = record(payload, path, "UsageTokenTotals");
  if (!rec.ok) return rec;
  const input = req(rec.value, "input", usageQuantity, path);
  if (!input.ok) return input;
  const output = req(rec.value, "output", usageQuantity, path);
  if (!output.ok) return output;
  const reasoning = req(rec.value, "reasoning", usageQuantity, path);
  if (!reasoning.ok) return reasoning;
  const cache_read = req(rec.value, "cache_read", usageQuantity, path);
  if (!cache_read.ok) return cache_read;
  const cache_write = req(rec.value, "cache_write", usageQuantity, path);
  if (!cache_write.ok) return cache_write;
  const total = req(rec.value, "total", usageQuantity, path);
  if (!total.ok) return total;
  return yes({
    input: input.value,
    output: output.value,
    reasoning: reasoning.value,
    cache_read: cache_read.value,
    cache_write: cache_write.value,
    total: total.value,
  });
};

const usageCurrencyCost: Decoder<UsageCurrencyCost> = (payload, path) => {
  const rec = record(payload, path, "UsageCurrencyCost");
  if (!rec.ok) return rec;
  const currency = req(rec.value, "currency", string, path);
  if (!currency.ok) return currency;
  const actual_micros = req(rec.value, "actual_micros", index, path);
  if (!actual_micros.ok) return actual_micros;
  return yes({
    currency: currency.value,
    actual_micros: actual_micros.value,
  });
};

const usageCost: Decoder<UsageCost> = (payload, path) => {
  const rec = record(payload, path, "UsageCost");
  if (!rec.ok) return rec;
  const source = req(rec.value, "source", literal("canonical_settlement"), path);
  if (!source.ok) return source;
  const actual_micros = req(rec.value, "actual_micros", nullable(index), path);
  if (!actual_micros.ok) return actual_micros;
  const currency = req(rec.value, "currency", nullable(string), path);
  if (!currency.ok) return currency;
  const currencies = req(rec.value, "currencies", array(usageCurrencyCost), path);
  if (!currencies.ok) return currencies;
  const completeness = req(rec.value, "completeness", oneOf<UsageCompleteness>(["complete", "partial", "unavailable"]), path);
  if (!completeness.ok) return completeness;
  const settled_calls = req(rec.value, "settled_calls", index, path);
  if (!settled_calls.ok) return settled_calls;
  const unresolved_calls = req(rec.value, "unresolved_calls", index, path);
  if (!unresolved_calls.ok) return unresolved_calls;
  const unavailable_calls = req(rec.value, "unavailable_calls", index, path);
  if (!unavailable_calls.ok) return unavailable_calls;
  return yes({
    source: source.value,
    actual_micros: actual_micros.value,
    currency: currency.value,
    currencies: currencies.value,
    completeness: completeness.value,
    settled_calls: settled_calls.value,
    unresolved_calls: unresolved_calls.value,
    unavailable_calls: unavailable_calls.value,
  });
};

const contextCostTotals: Decoder<ContextCostTotals> = (payload, path) => {
  const rec = record(payload, path, "ContextCostTotals");
  if (!rec.ok) return rec;
  const canonical_message_bytes = req(rec.value, "canonical_message_bytes", usageQuantity, path);
  if (!canonical_message_bytes.ok) return canonical_message_bytes;
  const tool_schema_bytes = req(rec.value, "tool_schema_bytes", usageQuantity, path);
  if (!tool_schema_bytes.ok) return tool_schema_bytes;
  const full_schema_baseline_bytes = req(rec.value, "full_schema_baseline_bytes", usageQuantity, path);
  if (!full_schema_baseline_bytes.ok) return full_schema_baseline_bytes;
  const schema_bytes_saved = req(rec.value, "schema_bytes_saved", usageQuantity, path);
  if (!schema_bytes_saved.ok) return schema_bytes_saved;
  const wire_bytes = req(rec.value, "wire_bytes", usageQuantity, path);
  if (!wire_bytes.ok) return wire_bytes;
  const capsule_bytes = req(rec.value, "capsule_bytes", usageQuantity, path);
  if (!capsule_bytes.ok) return capsule_bytes;
  const capsule_injected_requests = req(rec.value, "capsule_injected_requests", usageQuantity, path);
  if (!capsule_injected_requests.ok) return capsule_injected_requests;
  const tool_result_bytes = req(rec.value, "tool_result_bytes", usageQuantity, path);
  if (!tool_result_bytes.ok) return tool_result_bytes;
  return yes({
    canonical_message_bytes: canonical_message_bytes.value,
    tool_schema_bytes: tool_schema_bytes.value,
    full_schema_baseline_bytes: full_schema_baseline_bytes.value,
    schema_bytes_saved: schema_bytes_saved.value,
    wire_bytes: wire_bytes.value,
    capsule_bytes: capsule_bytes.value,
    capsule_injected_requests: capsule_injected_requests.value,
    tool_result_bytes: tool_result_bytes.value,
  });
};

const usageTotals: Decoder<UsageTotals> = (payload, path) => {
  const rec = record(payload, path, "UsageTotals");
  if (!rec.ok) return rec;
  const turns = req(rec.value, "turns", index, path);
  if (!turns.ok) return turns;
  const jobs = req(rec.value, "jobs", index, path);
  if (!jobs.ok) return jobs;
  const attempts = req(rec.value, "attempts", index, path);
  if (!attempts.ok) return attempts;
  const provider_calls = req(rec.value, "provider_calls", index, path);
  if (!provider_calls.ok) return provider_calls;
  const native_calls = req(rec.value, "native_calls", index, path);
  if (!native_calls.ok) return native_calls;
  const provider_requests = req(rec.value, "provider_requests", usageQuantity, path);
  if (!provider_requests.ok) return provider_requests;
  const provider_rounds = req(rec.value, "provider_rounds", usageQuantity, path);
  if (!provider_rounds.ok) return provider_rounds;
  const tokens = req(rec.value, "tokens", usageTokenTotals, path);
  if (!tokens.ok) return tokens;
  const cost = req(rec.value, "cost", usageCost, path);
  if (!cost.ok) return cost;
  const context_costs = req(rec.value, "context_costs", contextCostTotals, path);
  if (!context_costs.ok) return context_costs;
  const first_activity_at = req(rec.value, "first_activity_at", nullable(index), path);
  if (!first_activity_at.ok) return first_activity_at;
  const last_activity_at = req(rec.value, "last_activity_at", nullable(index), path);
  if (!last_activity_at.ok) return last_activity_at;
  return yes({
    turns: turns.value,
    jobs: jobs.value,
    attempts: attempts.value,
    provider_calls: provider_calls.value,
    native_calls: native_calls.value,
    provider_requests: provider_requests.value,
    provider_rounds: provider_rounds.value,
    tokens: tokens.value,
    cost: cost.value,
    context_costs: context_costs.value,
    first_activity_at: first_activity_at.value,
    last_activity_at: last_activity_at.value,
  });
};

const usageBreakdown: Decoder<UsageBreakdown> = (payload, path) => {
  const rec = record(payload, path, "UsageBreakdown");
  if (!rec.ok) return rec;
  const provider = req(rec.value, "provider", nullable(string), path);
  if (!provider.ok) return provider;
  const model = req(rec.value, "model", nullable(string), path);
  if (!model.ok) return model;
  const totals = req(rec.value, "totals", usageTotals, path);
  if (!totals.ok) return totals;
  return yes({
    provider: provider.value,
    model: model.value,
    totals: totals.value,
  });
};

const usageConversationRow: Decoder<UsageConversationRow> = (payload, path) => {
  const rec = record(payload, path, "UsageConversationRow");
  if (!rec.ok) return rec;
  const conversation_id = req(rec.value, "conversation_id", identity, path);
  if (!conversation_id.ok) return conversation_id;
  const session_id = req(rec.value, "session_id", identity, path);
  if (!session_id.ok) return session_id;
  const title = req(rec.value, "title", nullable(string), path);
  if (!title.ok) return title;
  const updated_at = req(rec.value, "updated_at", string, path);
  if (!updated_at.ok) return updated_at;
  const totals = req(rec.value, "totals", usageTotals, path);
  if (!totals.ok) return totals;
  const providers = req(rec.value, "providers", array(string), path);
  if (!providers.ok) return providers;
  const models = req(rec.value, "models", array(string), path);
  if (!models.ok) return models;
  return yes({
    conversation_id: conversation_id.value,
    session_id: session_id.value,
    title: title.value,
    updated_at: updated_at.value,
    totals: totals.value,
    providers: providers.value,
    models: models.value,
  });
};

const projectUsageResponse: Decoder<ProjectUsageResponse> = (payload, path) => {
  const rec = record(payload, path, "ProjectUsageResponse");
  if (!rec.ok) return rec;
  const api_version = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
  if (!api_version.ok) return api_version;
  const project_id = req(rec.value, "project_id", identity, path);
  if (!project_id.ok) return project_id;
  const window = req(rec.value, "window", oneOf<UsageWindow>(["all", "today", "7d", "30d"]), path);
  if (!window.ok) return window;
  const window_started_at = req(rec.value, "window_started_at", nullable(index), path);
  if (!window_started_at.ok) return window_started_at;
  const generated_at = req(rec.value, "generated_at", index, path);
  if (!generated_at.ok) return generated_at;
  const conversations = req(rec.value, "conversations", index, path);
  if (!conversations.ok) return conversations;
  const totals = req(rec.value, "totals", usageTotals, path);
  if (!totals.ok) return totals;
  const providers = req(rec.value, "providers", array(usageBreakdown), path);
  if (!providers.ok) return providers;
  const models = req(rec.value, "models", array(usageBreakdown), path);
  if (!models.ok) return models;
  const conversation_rows = req(rec.value, "conversation_rows", array(usageConversationRow), path);
  if (!conversation_rows.ok) return conversation_rows;
  const truncated = req(rec.value, "truncated", boolean, path);
  if (!truncated.ok) return truncated;
  return yes({
    api_version: api_version.value,
    project_id: project_id.value,
    window: window.value,
    window_started_at: window_started_at.value,
    generated_at: generated_at.value,
    conversations: conversations.value,
    totals: totals.value,
    providers: providers.value,
    models: models.value,
    conversation_rows: conversation_rows.value,
    truncated: truncated.value,
  });
};

const conversationUsageResponse: Decoder<ConversationUsageResponse> = (payload, path) => {
  const rec = record(payload, path, "ConversationUsageResponse");
  if (!rec.ok) return rec;
  const api_version = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
  if (!api_version.ok) return api_version;
  const project_id = req(rec.value, "project_id", identity, path);
  if (!project_id.ok) return project_id;
  const conversation_id = req(rec.value, "conversation_id", identity, path);
  if (!conversation_id.ok) return conversation_id;
  const session_id = req(rec.value, "session_id", identity, path);
  if (!session_id.ok) return session_id;
  const title = req(rec.value, "title", nullable(string), path);
  if (!title.ok) return title;
  const created_at = req(rec.value, "created_at", string, path);
  if (!created_at.ok) return created_at;
  const updated_at = req(rec.value, "updated_at", string, path);
  if (!updated_at.ok) return updated_at;
  const latest_job_state = req(rec.value, "latest_job_state", nullable(string), path);
  if (!latest_job_state.ok) return latest_job_state;
  const generated_at = req(rec.value, "generated_at", index, path);
  if (!generated_at.ok) return generated_at;
  const totals = req(rec.value, "totals", usageTotals, path);
  if (!totals.ok) return totals;
  const providers = req(rec.value, "providers", array(usageBreakdown), path);
  if (!providers.ok) return providers;
  const models = req(rec.value, "models", array(usageBreakdown), path);
  if (!models.ok) return models;
  const truncated = req(rec.value, "truncated", boolean, path);
  if (!truncated.ok) return truncated;
  return yes({
    api_version: api_version.value,
    project_id: project_id.value,
    conversation_id: conversation_id.value,
    session_id: session_id.value,
    title: title.value,
    created_at: created_at.value,
    updated_at: updated_at.value,
    latest_job_state: latest_job_state.value,
    generated_at: generated_at.value,
    totals: totals.value,
    providers: providers.value,
    models: models.value,
    truncated: truncated.value,
  });
};

const jobUsageResponse: Decoder<JobUsageResponse> = (payload, path) => {
  const rec = record(payload, path, "JobUsageResponse");
  if (!rec.ok) return rec;
  const api_version = req(rec.value, "api_version", literal(CANONICAL_API_VERSION), path);
  if (!api_version.ok) return api_version;
  const project_id = req(rec.value, "project_id", identity, path);
  if (!project_id.ok) return project_id;
  const job_id = req(rec.value, "job_id", identity, path);
  if (!job_id.ok) return job_id;
  const generated_at = req(rec.value, "generated_at", index, path);
  if (!generated_at.ok) return generated_at;
  const totals = req(rec.value, "totals", usageTotals, path);
  if (!totals.ok) return totals;
  const providers = req(rec.value, "providers", array(usageBreakdown), path);
  if (!providers.ok) return providers;
  const models = req(rec.value, "models", array(usageBreakdown), path);
  if (!models.ok) return models;
  const truncated = req(rec.value, "truncated", boolean, path);
  if (!truncated.ok) return truncated;
  return yes({
    api_version: api_version.value,
    project_id: project_id.value,
    job_id: job_id.value,
    generated_at: generated_at.value,
    totals: totals.value,
    providers: providers.value,
    models: models.value,
    truncated: truncated.value,
  });
};

export const decodeProjectUsageResponse = (input: unknown): ProjectUsageResponse => decode(value => projectUsageResponse(value, ""), input);
export const decodeConversationUsageResponse = (input: unknown): ConversationUsageResponse => decode(value => conversationUsageResponse(value, ""), input);
export const decodeJobUsageResponse = (input: unknown): JobUsageResponse => decode(value => jobUsageResponse(value, ""), input);
