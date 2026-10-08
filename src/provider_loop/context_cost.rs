use super::{ProviderClient, ProviderRound};
use crate::error::{OcgError, Result};
use crate::http::{BoxFuture, ChunkSink, HttpResponse, HttpTransport};
use crate::native_tools::projection::ToolProjectionFacts;
use crate::openai_compatible::NormalizedUsage;
use crate::orchestration::domain::DomainRepository;
use crate::orchestration::execution_dispatch::ExecutionEnvelope;
use crate::provider_protocol::ProviderProtocol;
use serde::Serialize;
use serde_json::Value;
use std::io::{self, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

const MAX_REQUESTS: usize = super::MAX_PROVIDER_ROUNDS * 2;

#[derive(Clone, Serialize)]
pub(crate) struct ContextCosts {
    version: u8,
    requests: Vec<RequestCost>,
    truncated: bool,
}

impl Default for ContextCosts {
    fn default() -> Self {
        Self {
            version: 1,
            requests: Vec::new(),
            truncated: false,
        }
    }
}

#[derive(Clone, Serialize)]
struct RequestCost {
    request_sequence: usize,
    round: Option<usize>,
    purpose: &'static str,
    provider: String,
    model: String,
    protocol: ProviderProtocol,
    tool_projection: Option<ToolProjectionFacts>,
    canonical: CanonicalCost,
    wire: WireCost,
    reported_usage: ReportedUsage,
    provider_elapsed_ms: Option<u64>,
    outcome: &'static str,
}

#[derive(Clone, Default, Serialize)]
struct CanonicalCost {
    system_context_bytes: usize,
    user_message_bytes: usize,
    assistant_history_bytes: usize,
    tool_result_bytes: usize,
    other_message_bytes: usize,
    message_framing_bytes: usize,
    total_message_bytes: usize,
    user_input_bytes: usize,
    prior_user_message_bytes: usize,
    handoff_capsule_bytes: usize,
    handoff_capsule_content_bytes: usize,
    tool_schema_bytes: usize,
    tool_count: usize,
    tools: Vec<ToolCost>,
    total_request_json_bytes: usize,
    classification_us: u64,
    accounting_us: u64,
    tool_schema_breakdown_us: u64,
}

#[derive(Clone, Default, Serialize)]
struct WireCost {
    request_bytes: Option<usize>,
    tool_schema_bytes: usize,
    tool_count: usize,
    tools: Vec<ToolCost>,
    serialization_us: Option<u64>,
    tool_schema_breakdown_us: u64,
    size_observation_us: Option<u64>,
}

#[derive(Clone, Serialize)]
struct ToolCost {
    name: String,
    bytes: usize,
}

#[derive(Clone, Default, Serialize)]
struct ReportedUsage {
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    total_tokens: Option<u64>,
    cache_read_tokens: Option<u64>,
    cache_write_tokens: Option<u64>,
    reasoning_tokens: Option<u64>,
    cache_read_is_subset_of_input: bool,
}

struct ByteCounter(usize);

impl Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 += bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn json_bytes(value: &impl Serialize) -> Result<usize> {
    let mut counter = ByteCounter(0);
    serde_json::to_writer(&mut counter, value)
        .map_err(|error| OcgError::config(format!("context byte accounting: {error}")))?;
    Ok(counter.0)
}

fn micros(started: Instant) -> u64 {
    started.elapsed().as_micros().min(u64::MAX as u128) as u64
}

fn tool_costs(request: &Value) -> Result<(usize, Vec<ToolCost>)> {
    let Some(value) = request.get("tools") else {
        return Ok((0, Vec::new()));
    };
    let Some(tools) = value.as_array() else {
        return Ok((json_bytes(value)?, Vec::new()));
    };
    let mut total = 2 + tools.len().saturating_sub(1);
    let mut costs = Vec::with_capacity(tools.len());
    for tool in tools {
        let bytes = json_bytes(tool)?;
        total += bytes;
        let name = tool["function"]["name"]
            .as_str()
            .or_else(|| tool["name"].as_str())
            .unwrap_or("");
        costs.push(ToolCost {
            name: name.chars().take(128).collect(),
            bytes,
        });
    }
    costs.sort_by(|a, b| a.name.cmp(&b.name).then(a.bytes.cmp(&b.bytes)));
    Ok((total, costs))
}

fn canonical_cost(request: &Value) -> Result<CanonicalCost> {
    let started = Instant::now();
    let mut cost = CanonicalCost::default();
    let messages = request.get("messages").and_then(Value::as_array);
    if let Some(messages) = messages {
        let last_user = messages
            .iter()
            .rposition(|message| message["role"] == "user");
        cost.message_framing_bytes = 2 + messages.len().saturating_sub(1);
        cost.total_message_bytes = cost.message_framing_bytes;
        for (index, message) in messages.iter().enumerate() {
            let bytes = json_bytes(message)?;
            cost.total_message_bytes += bytes;
            match message["role"].as_str() {
                Some("system" | "developer") => cost.system_context_bytes += bytes,
                Some("user") => {
                    cost.user_message_bytes += bytes;
                    if Some(index) == last_user {
                        cost.user_input_bytes += bytes;
                    } else {
                        cost.prior_user_message_bytes += bytes;
                    }
                }
                Some("assistant") => {
                    cost.assistant_history_bytes += bytes;
                }
                Some("tool") => cost.tool_result_bytes += bytes,
                _ => cost.other_message_bytes += bytes,
            }
            if super::is_handoff_message(message) {
                cost.handoff_capsule_bytes += bytes;
                cost.handoff_capsule_content_bytes +=
                    message["content"].as_str().map_or(0, str::len);
            }
        }
    }
    cost.classification_us = micros(started);
    let tools_started = Instant::now();
    (cost.tool_schema_bytes, cost.tools) = tool_costs(request)?;
    cost.tool_count = cost.tools.len();
    cost.tool_schema_breakdown_us = micros(tools_started);
    // Reuse the classified sizes rather than serializing messages a second time.
    if let Some(object) = request.as_object() {
        cost.total_request_json_bytes = 2 + object.len().saturating_sub(1);
        for (key, value) in object {
            let bytes = match key.as_str() {
                "messages" if messages.is_some() => cost.total_message_bytes,
                "tools" => cost.tool_schema_bytes,
                _ => json_bytes(value)?,
            };
            cost.total_request_json_bytes += json_bytes(key)? + 1 + bytes;
        }
    } else {
        cost.total_request_json_bytes = json_bytes(request)?;
    }
    cost.accounting_us = micros(started);
    Ok(cost)
}

fn reported_usage(usage: &NormalizedUsage, protocol: ProviderProtocol) -> ReportedUsage {
    let raw = usage.raw.as_ref().unwrap_or(&Value::Null);
    let counter = |key: &str| raw.get(key).and_then(Value::as_u64);
    let anthropic = protocol == ProviderProtocol::Anthropic;
    ReportedUsage {
        input_tokens: counter(if anthropic {
            "input_tokens"
        } else {
            "prompt_tokens"
        }),
        output_tokens: counter(if anthropic {
            "output_tokens"
        } else {
            "completion_tokens"
        }),
        total_tokens: counter("total_tokens"),
        cache_read_tokens: if anthropic {
            counter("cache_read_input_tokens")
        } else {
            raw["prompt_tokens_details"]["cached_tokens"].as_u64()
        },
        cache_write_tokens: if anthropic {
            counter("cache_creation_input_tokens")
        } else {
            None
        },
        reasoning_tokens: raw["completion_tokens_details"]["reasoning_tokens"].as_u64(),
        cache_read_is_subset_of_input: !anthropic,
    }
}

#[derive(Clone, Default)]
pub(super) struct CostLog(Arc<Mutex<CostState>>);

#[derive(Default)]
struct CostState {
    data: ContextCosts,
    active: bool,
}

impl CostLog {
    pub(super) fn snapshot(&self) -> ContextCosts {
        match self.0.lock() {
            Ok(state) => state.data.clone(),
            Err(_) => ContextCosts {
                truncated: true,
                ..ContextCosts::default()
            },
        }
    }

    pub(super) fn persist(&self, root: &Path, envelope: &ExecutionEnvelope) {
        let started = Instant::now();
        if let Err(error) = DomainRepository::open(root).and_then(|mut domain| {
            domain.record_provider_context_costs(
                &envelope.call_id,
                &envelope.attempt_id,
                envelope.generation,
                &self.snapshot(),
            )
        }) {
            tracing::warn!(%error, "provider context accounting projection unavailable");
        }
        tracing::debug!(
            projection_us = micros(started),
            "provider context accounting persisted"
        );
    }
}

pub(super) struct AccountingTransport<'a> {
    pub inner: &'a dyn HttpTransport,
    pub costs: CostLog,
}

impl HttpTransport for AccountingTransport<'_> {
    fn get(&self, url: &str) -> Result<Vec<u8>> {
        self.inner.get(url)
    }

    fn post_json_stream_in_runtime(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: &Value,
        on_chunk: ChunkSink,
    ) -> BoxFuture<'static, Result<HttpResponse>> {
        let started = Instant::now();
        let tools = tool_costs(body);
        if let Ok(mut state) = self.costs.0.lock() {
            if state.active {
                if let (Some(request), Ok((bytes, tools))) = (state.data.requests.last_mut(), tools)
                {
                    request.wire.tool_schema_bytes = bytes;
                    request.wire.tool_count = tools.len();
                    request.wire.tools = tools;
                    request.wire.tool_schema_breakdown_us = micros(started);
                }
            }
        }
        let costs = self.costs.clone();
        self.inner.post_json_stream_observed_in_runtime(
            url,
            headers,
            body,
            on_chunk,
            Box::new(move |bytes, serialization_us| {
                let started = Instant::now();
                if let Ok(mut state) = costs.0.lock() {
                    if state.active {
                        if let Some(request) = state.data.requests.last_mut() {
                            request.wire.request_bytes = Some(bytes);
                            request.wire.serialization_us = Some(serialization_us);
                            request.wire.size_observation_us = Some(micros(started));
                        }
                    }
                }
            }),
        )
    }
}

pub(super) struct AccountingProvider<'a> {
    pub inner: Box<dyn ProviderClient + 'a>,
    pub costs: CostLog,
    pub protocol: ProviderProtocol,
    pub provider: &'a str,
    pub model: &'a str,
    pub root: &'a Path,
    pub envelope: &'a ExecutionEnvelope,
    pub projection: &'a ToolProjectionFacts,
}

impl AccountingProvider<'_> {
    fn complete_observed<'a>(
        &'a self,
        request: &Value,
        purpose: &'static str,
    ) -> BoxFuture<'a, Result<ProviderRound>> {
        let canonical = canonical_cost(request);
        let mut recorded = false;
        if let Ok(mut state) = self.costs.0.lock() {
            state.active = false;
            let costs = &mut state.data;
            if costs.requests.len() < MAX_REQUESTS {
                if let Ok(canonical) = canonical {
                    let round = (purpose == "execution").then(|| {
                        costs
                            .requests
                            .iter()
                            .filter(|r| r.purpose == "execution")
                            .count()
                            + 1
                    });
                    let request_sequence = costs.requests.len() + 1;
                    costs.requests.push(RequestCost {
                        request_sequence,
                        round,
                        purpose,
                        provider: self.provider.chars().take(128).collect(),
                        model: self.model.chars().take(256).collect(),
                        protocol: self.protocol,
                        tool_projection: (purpose == "execution").then(|| self.projection.clone()),
                        canonical,
                        wire: WireCost::default(),
                        reported_usage: ReportedUsage {
                            cache_read_is_subset_of_input: self.protocol
                                != ProviderProtocol::Anthropic,
                            ..ReportedUsage::default()
                        },
                        provider_elapsed_ms: None,
                        outcome: "in_flight",
                    });
                    recorded = true;
                } else {
                    costs.truncated = true;
                }
            } else {
                costs.truncated = true;
            }
            state.active = recorded;
        }
        let provider_started = Instant::now();
        let future = self.inner.complete(request);
        Box::pin(async move {
            let result = future.await;
            let provider_elapsed_ms =
                provider_started.elapsed().as_millis().min(u64::MAX as u128) as u64;
            if recorded {
                if let Ok(mut state) = self.costs.0.lock() {
                    state.active = false;
                    if let Some(request) = state.data.requests.last_mut() {
                        request.provider_elapsed_ms = Some(provider_elapsed_ms);
                        match &result {
                            Ok(round) => {
                                request.reported_usage =
                                    reported_usage(&round.summary.usage, self.protocol);
                                request.outcome = "completed";
                            }
                            Err(_) => {
                                request.outcome = if self.envelope.cancelled.is_cancelled() {
                                    "cancelled"
                                } else {
                                    "failed"
                                };
                            }
                        }
                    }
                }
                self.costs.persist(self.root, self.envelope);
            }
            result
        })
    }
}

impl ProviderClient for AccountingProvider<'_> {
    fn complete(&self, request: &Value) -> BoxFuture<'_, Result<ProviderRound>> {
        self.complete_observed(request, "execution")
    }

    fn complete_context_summary(&self, request: &Value) -> BoxFuture<'_, Result<ProviderRound>> {
        self.complete_observed(request, "context_summary")
    }
}
