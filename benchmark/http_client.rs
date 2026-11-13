use std::collections::VecDeque;
use std::time::{Duration, Instant};

use ntex::client::{Client, ClientResponse};
use ntex::http::Payload;
use serde_json::{Value, json};

const MAX_JSON_BODY: usize = 8 * 1024 * 1024;
const MAX_SSE_BUFFER: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(super) struct FixtureIdentities {
    pub(super) executor: String,
    pub(super) pool: String,
    pub(super) llm_profile: String,
    pub(super) agent_profile: String,
    pub(super) project: String,
}

pub(super) async fn client(timeout: Duration) -> Result<Client, String> {
    let connector = ntex::service::fn_factory_with_config(move |cfg: ntex::SharedCfg| async move {
        Ok::<_, std::io::Error>(ntex::service::fn_service(
            move |target: ntex::connect::Connect<ntex::http::Uri>| {
                let cfg = cfg.clone();
                async move {
                    let ip = target
                        .host()
                        .parse()
                        .map_err(|_| ntex::connect::ConnectError::InvalidInput)?;
                    let address = std::net::SocketAddr::new(ip, target.port());
                    if !address.ip().is_loopback() {
                        return Err(ntex::connect::ConnectError::InvalidInput);
                    }
                    let stream = ntex::rt::spawn_blocking(move || {
                        let stream = std::net::TcpStream::connect_timeout(&address, timeout)?;
                        stream.set_nodelay(true)?;
                        Ok::<_, std::io::Error>(stream)
                    })
                    .await
                    .map_err(|error| {
                        ntex::connect::ConnectError::Io(std::io::Error::other(error))
                    })??;
                    ntex::rt::from_tcp_stream(stream, cfg).map_err(ntex::connect::ConnectError::Io)
                }
            },
        ))
    });
    Client::builder()
        .connector::<()>(ntex::client::Connector::new().connector(connector))
        .response_timeout(ntex::time::Millis(
            timeout.as_millis().min(u128::from(u32::MAX)) as u32,
        ))
        .build(ntex::SharedCfg::default())
        .await
        .map_err(|error| format!("failed to create benchmark HTTP client: {error}"))
}

#[derive(Clone, Debug)]
pub(super) struct AdmissionConfirmation {
    pub job_id: String,
}

#[derive(Clone, Debug)]
pub(super) struct ReferenceValidation {
    pub model_requests: usize,
    pub tool_calls: usize,
    pub completions: usize,
}

#[derive(Clone, Debug)]
pub(super) struct SseEvent {
    pub name: String,
    pub id: Option<String>,
    pub data: Value,
    pub observed_at: Instant,
}

#[derive(Default)]
struct SseDecoder {
    buffered: Vec<u8>,
    ready: VecDeque<SseEvent>,
}

impl SseDecoder {
    fn push(&mut self, chunk: &[u8], observed_at: Instant) -> Result<(), String> {
        if self.buffered.len().saturating_add(chunk.len()) > MAX_SSE_BUFFER {
            return Err("SSE buffering exceeded the benchmark limit".into());
        }
        self.buffered.extend_from_slice(chunk);
        while let Some(end) = self
            .buffered
            .windows(2)
            .position(|window| window == b"\n\n")
        {
            let frame: Vec<u8> = self.buffered.drain(..end + 2).collect();
            if let Some(event) = decode_event(&frame, observed_at)? {
                self.ready.push_back(event);
            }
        }
        Ok(())
    }
}

fn decode_event(frame: &[u8], observed_at: Instant) -> Result<Option<SseEvent>, String> {
    let text = std::str::from_utf8(frame).map_err(|error| format!("SSE is not UTF-8: {error}"))?;
    let mut name = "message".to_owned();
    let mut id = None;
    let mut data = Vec::new();
    for line in text.lines() {
        if line.starts_with(':') || line.is_empty() {
            continue;
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "event" => name = value.to_owned(),
            "id" => id = Some(value.to_owned()),
            "data" => data.push(value),
            _ => {}
        }
    }
    if data.is_empty() {
        return Ok(None);
    }
    let data = serde_json::from_str(&data.join("\n"))
        .map_err(|error| format!("malformed SSE JSON event {name}: {error}"))?;
    Ok(Some(SseEvent {
        name,
        id,
        data,
        observed_at,
    }))
}

async fn next_event(
    payload: &mut Payload,
    decoder: &mut SseDecoder,
    deadline: Instant,
) -> Result<SseEvent, String> {
    loop {
        if let Some(event) = decoder.ready.pop_front() {
            return Ok(event);
        }
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| "SSE deadline exceeded".to_owned())?;
        let received = ntex::time::timeout(remaining, payload.recv())
            .await
            .map_err(|_| "SSE body read timed out".to_owned())?;
        match received {
            Some(Ok(chunk)) => {
                let observed_at = Instant::now();
                decoder.push(&chunk, observed_at)?;
            }
            Some(Err(error)) => return Err(format!("SSE body read failed: {error}")),
            None => return Err("SSE ended before a terminal settlement".into()),
        }
    }
}

pub(super) struct BenchmarkClient {
    address: String,
    token: String,
    timeout: Duration,
    client: Client,
}

impl BenchmarkClient {
    pub(super) async fn new(
        address: String,
        token: String,
        timeout: Duration,
    ) -> Result<Self, String> {
        let client = client(timeout).await?;
        Ok(Self {
            address,
            token,
            timeout,
            client,
        })
    }

    fn authorized(&self, request: ntex::client::ClientRequest) -> ntex::client::ClientRequest {
        request.set_header("Authorization", format!("Bearer {}", self.token))
    }

    async fn response_json(&self, route: &str, response: ClientResponse) -> Result<Value, String> {
        let status = response.status();
        let body = ntex::time::timeout(self.timeout, response.body().limit(MAX_JSON_BODY))
            .await
            .map_err(|_| format!("{route}: response-body deadline exceeded"))?
            .map_err(|error| format!("{route}: failed to read response body: {error}"))?;
        let context = String::from_utf8_lossy(&body);
        if !status.is_success() {
            return Err(format!(
                "{route}: HTTP {status}; response body: {}",
                context.chars().take(1_024).collect::<String>()
            ));
        }
        serde_json::from_slice(&body).map_err(|error| {
            format!(
                "{route}: malformed JSON response: {error}; body: {}",
                context.chars().take(1_024).collect::<String>()
            )
        })
    }

    pub(super) async fn get_json(&self, route: &str) -> Result<Value, String> {
        ntex::time::timeout(self.timeout, self.get_json_inner(route))
            .await
            .map_err(|_| format!("{route}: request deadline exceeded"))?
    }

    async fn get_json_inner(&self, route: &str) -> Result<Value, String> {
        let response = self
            .authorized(self.client.get(format!("{}{route}", self.address)))
            .send()
            .await
            .map_err(|error| format!("{route}: HTTP GET failed: {error}"))?;
        self.response_json(route, response).await
    }

    pub(super) async fn post_json(&self, route: &str, body: &Value) -> Result<Value, String> {
        ntex::time::timeout(self.timeout, self.post_json_inner(route, body))
            .await
            .map_err(|_| format!("{route}: request deadline exceeded"))?
    }

    async fn post_json_inner(&self, route: &str, body: &Value) -> Result<Value, String> {
        let response = self
            .authorized(self.client.post(format!("{}{route}", self.address)))
            .set_header("Content-Type", "application/json")
            .send_json(body)
            .await
            .map_err(|error| format!("{route}: HTTP POST failed: {error}"))?;
        self.response_json(route, response).await
    }

    pub(super) async fn prepare_fixture_identities(
        &self,
        provider_endpoint: &str,
        workspace: &std::path::Path,
        concurrency: usize,
    ) -> Result<FixtureIdentities, String> {
        let pool = uuid::Uuid::now_v7().to_string();
        self.post_json(
            "/api/v1/pools",
            &json!({
                "id": pool,
                "name": "benchmark-pool",
                "capacity": concurrency,
                "enabled": true,
            }),
        )
        .await?;
        let executor = uuid::Uuid::now_v7().to_string();
        self.post_json(
            "/api/v1/executors",
            &json!({
                "id": executor,
                "pool": pool,
                "name": "benchmark-executor",
                "enabled": true,
                "capabilities": [{"operation": "llm_request", "protocol": "openai_chat_completions"}],
            }),
        )
        .await?;
        let endpoint = if provider_endpoint.ends_with("/chat/completions") {
            provider_endpoint.to_owned()
        } else {
            format!("{provider_endpoint}/chat/completions")
        };
        let llm = self
            .post_json(
                "/api/v1/profiles",
                &json!({
                    "kind": "llm",
                    "name": "benchmark-llm",
                    "model": "benchmark-mock",
                    "protocol": "openai_chat_completions",
                    "endpoint": endpoint,
                    "credential_ref": "BENCH_SYNTH",
                    "credential": format!("synthetic-{}", uuid::Uuid::now_v7()),
                    "declared_capabilities": ["chat", "tools"],
                    "context_window": 32_768,
                    "parameters": {"max_tokens": 256},
                }),
            )
            .await?;
        let llm_profile = llm
            .get("profile_id")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("/api/v1/profiles: answer names no profile: {llm}"))?
            .to_owned();
        let agent = self
            .post_json(
                "/api/v1/profiles",
                &json!({
                    "kind": "agent",
                    "name": "benchmark-agent",
                    "capabilities": ["chat", "coding"],
                    "llm_targets": [llm_profile],
                }),
            )
            .await?;
        let agent_profile = agent
            .get("profile_id")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("/api/v1/profiles: answer names no profile: {agent}"))?
            .to_owned();
        let root = workspace
            .to_str()
            .ok_or("the benchmark workspace path is not UTF-8")?;
        let project = self
            .post_json(
                "/api/v1/projects",
                &json!({"name": "benchmark-project", "root": root}),
            )
            .await?;
        let project = project
            .get("project")
            .and_then(|project| project.get("id"))
            .and_then(Value::as_str)
            .ok_or_else(|| format!("/api/v1/projects: answer names no Project: {project}"))?
            .to_owned();
        Ok(FixtureIdentities {
            executor,
            pool,
            llm_profile,
            agent_profile,
            project,
        })
    }

    pub(super) async fn readiness(&self) -> Result<(), String> {
        let health = self.get_json("/api/v1/health").await?;
        if !health.is_object() {
            return Err("readiness health response is not an object".into());
        }
        let history = self.history_page(None, 1).await?;
        if !history["jobs"].is_array() || !history["has_more"].is_boolean() {
            return Err(format!("readiness history response is invalid: {history}"));
        }
        Ok(())
    }

    pub(super) async fn submit_job(
        &self,
        llm_profile: &str,
        agent_profile: &str,
        project: &str,
        objective: &str,
    ) -> Result<AdmissionConfirmation, String> {
        ntex::time::timeout(
            self.timeout,
            self.submit_job_inner(llm_profile, agent_profile, project, objective),
        )
        .await
        .map_err(|_| "/api/v1/jobs: admission deadline exceeded".to_owned())?
    }

    async fn post_job_json(
        &self,
        route: &str,
        context: &str,
        body: &Value,
    ) -> Result<Value, String> {
        let response = self
            .authorized(self.client.post(format!("{}{route}", self.address)))
            .set_header("Idempotency-Key", uuid::Uuid::now_v7().to_string())
            .set_header("X-OCG-Composer-Context", context)
            .set_header("Content-Type", "application/json")
            .send_json(body)
            .await
            .map_err(|error| format!("{route}: HTTP POST failed: {error}"))?;
        self.response_json(route, response).await
    }

    async fn submit_job_inner(
        &self,
        llm_profile: &str,
        agent_profile: &str,
        project: &str,
        objective: &str,
    ) -> Result<AdmissionConfirmation, String> {
        let context = uuid::Uuid::now_v7().to_string();
        let route = "/api/v1/jobs";
        let created = self
            .post_job_json(
                route,
                &context,
                &json!({
                    "kind": "generative",
                    "text": objective,
                    "project": project,
                    "configuration": {
                        "llm_profile": llm_profile,
                        "agent_profile": agent_profile,
                    },
                }),
            )
            .await?;
        let (job_id, revision) = parse_mutable_job(&created, "unsubmitted")?;
        let route = format!("/api/v1/jobs/{job_id}/submit");
        let submitted = self
            .post_job_json(&route, &context, &json!({ "revision": revision }))
            .await?;
        let (submitted_id, _) = parse_mutable_job(&submitted, "queued")?;
        if submitted_id != job_id {
            return Err(format!("{route}: submit answered another Job: {submitted}"));
        }
        Ok(AdmissionConfirmation { job_id })
    }

    async fn open_events(&self, job_id: &str) -> Result<ClientResponse, String> {
        let route = format!("/api/v1/jobs/{job_id}/events");
        let response = self
            .authorized(self.client.get(format!("{}{route}", self.address)))
            .set_header("Accept", "text/event-stream")
            .send()
            .await
            .map_err(|error| format!("{route}: HTTP GET failed: {error}"))?;
        if response.status().is_success() {
            return Ok(response);
        }
        let status = response.status();
        let body = ntex::time::timeout(self.timeout, response.body().limit(MAX_JSON_BODY))
            .await
            .map_err(|_| format!("{route}: error-body deadline exceeded"))?
            .map_err(|error| format!("{route}: failed to read error body: {error}"))?;
        Err(format!(
            "{route}: HTTP {status}; body: {}",
            String::from_utf8_lossy(&body)
        ))
    }

    pub(super) async fn consume_events<F, G>(
        &self,
        job_id: &str,
        timeout: Duration,
        on_open: G,
        on_event: F,
    ) -> Result<(), String>
    where
        F: FnMut(&SseEvent) -> Result<(), String>,
        G: FnOnce() -> Result<(), String>,
    {
        ntex::time::timeout(
            timeout,
            self.consume_events_inner(job_id, timeout, on_open, on_event),
        )
        .await
        .map_err(|_| format!("job {job_id} SSE deadline exceeded"))?
    }

    async fn consume_events_inner<F, G>(
        &self,
        job_id: &str,
        timeout: Duration,
        on_open: G,
        mut on_event: F,
    ) -> Result<(), String>
    where
        F: FnMut(&SseEvent) -> Result<(), String>,
        G: FnOnce() -> Result<(), String>,
    {
        let response = self.open_events(job_id).await?;
        on_open()?;
        let deadline = Instant::now() + timeout;
        let mut payload = response.take_payload();
        let mut decoder = SseDecoder::default();
        loop {
            let event = next_event(&mut payload, &mut decoder, deadline).await?;
            match event.name.as_str() {
                "job" | "output" => on_event(&event)?,
                "stream_error" => {
                    return Err(format!("backend event stream failed: {}", event.data));
                }
                "settlement" => {
                    let state = event.data["settlement"]["state"]
                        .as_str()
                        .ok_or_else(|| format!("settlement omitted its state: {}", event.data))?;
                    if event.data["job"].as_str() != Some(job_id) {
                        return Err(format!("settlement named another job: {}", event.data));
                    }
                    on_event(&event)?;
                    if state != "completed" {
                        return Err(format!(
                            "job {job_id} settled as {state}, not completed: {}",
                            event.data
                        ));
                    }
                    return Ok(());
                }
                other => return Err(format!("unexpected SSE event {other}: {}", event.data)),
            }
        }
    }

    pub(super) async fn history_page(
        &self,
        before: Option<&str>,
        limit: usize,
    ) -> Result<Value, String> {
        let mut route = format!("/api/v1/job-history?limit={limit}");
        if let Some(before) = before {
            route.push_str("&before=");
            route.push_str(before);
        }
        self.get_json(&route).await
    }

    pub(super) async fn validate_reference_job(
        &self,
        job_id: &str,
        job_key: &str,
        stream_chunks: usize,
    ) -> Result<ReferenceValidation, String> {
        if stream_chunks == 0 {
            return Err("a reference job needs at least one content marker".into());
        }
        let details = self.get_json(&format!("/api/v1/jobs/{job_id}")).await?;
        if details["job"]["settlement"]["state"] != "completed" {
            return Err(format!(
                "job {job_id} has no completed settlement: {details}"
            ));
        }
        let result = &details["result"];
        if result["code"] == "partial_result" || result["goal_reached"] == false {
            return Err(format!("job {job_id} completed only partially: {details}"));
        }
        let text = result["value"]["text"]
            .as_str()
            .ok_or_else(|| format!("job {job_id} omitted its final text: {details}"))?;
        let final_marker = format!("OCGMARK-{job_key}-{:04}", stream_chunks - 1);
        if !text.contains(job_key) || !text.contains(&final_marker) {
            return Err(format!(
                "job {job_id} returned an unexpected final result; wanted {final_marker}"
            ));
        }

        let tools = self
            .get_json(&format!("/api/v1/jobs/{job_id}/tools?limit=100"))
            .await?;
        let calls = tools["tools"]
            .as_array()
            .ok_or_else(|| format!("job {job_id} omitted tool evidence: {tools}"))?;
        if calls.len() != 2 {
            return Err(format!(
                "job {job_id} executed {} tool calls instead of 2: {tools}",
                calls.len()
            ));
        }
        for (index, call) in calls.iter().enumerate() {
            let outcome = call["outcome_kind"].as_str().unwrap_or_default();
            if call["name"] != "workspace_read_file"
                || call["decision"] != "admitted"
                || !matches!(outcome, "completed" | "completed_elided")
                || call["request"].as_u64() != Some(index as u64 + 1)
                || !call["arguments"]
                    .as_str()
                    .is_some_and(|arguments| arguments.contains("probe.txt"))
            {
                return Err(format!(
                    "job {job_id} has invalid real workspace tool evidence at {index}: {call}"
                ));
            }
        }

        for request in 1..=3 {
            let deadline = Instant::now() + self.timeout;
            let value = loop {
                let value = self
                    .get_json(&format!("/api/v1/jobs/{job_id}/requests/{request}"))
                    .await?;
                if value["messages"].is_array() && !value["response"].is_null() {
                    break value;
                }
                if Instant::now() >= deadline {
                    return Err(format!(
                        "job {job_id} request {request} did not reconstruct a provider exchange before the deadline: {value}"
                    ));
                }
                ntex::time::sleep(Duration::from_millis(5)).await;
            };
            if request > 1 {
                let seen = value["messages"].as_array().is_some_and(|messages| {
                    messages.iter().any(|message| {
                        message["role"] == "tool"
                            && message["content"]
                                .as_str()
                                .is_some_and(|content| content.contains("rarebeacon"))
                    })
                });
                if !seen {
                    return Err(format!(
                        "job {job_id} request {request} carries no workspace read result: {value}"
                    ));
                }
            }
        }
        match self
            .get_json(&format!("/api/v1/jobs/{job_id}/requests/4"))
            .await
        {
            Ok(value) => {
                return Err(format!(
                    "job {job_id} executed an unexpected fourth model request: {value}"
                ));
            }
            Err(error) if error.contains("HTTP 404") => {}
            Err(error) => {
                return Err(format!(
                    "job {job_id} fourth-request check failed unexpectedly: {error}"
                ));
            }
        }
        Ok(ReferenceValidation {
            model_requests: 3,
            tool_calls: 2,
            completions: 1,
        })
    }
}

fn parse_mutable_job(value: &Value, phase: &str) -> Result<(String, u64), String> {
    let job = value
        .get("job")
        .ok_or_else(|| format!("response omitted the job: {value}"))?;
    if job["phase"] != phase {
        return Err(format!("the Job is not {phase}: {value}"));
    }
    let id = job["id"]
        .as_str()
        .filter(|id| !id.is_empty())
        .ok_or_else(|| format!("the Job omitted its ID: {value}"))?;
    let revision = job["revision"]
        .as_u64()
        .ok_or_else(|| format!("the Job omitted its revision: {value}"))?;
    Ok((id.to_owned(), revision))
}
