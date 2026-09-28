//! Gated real OpenCode V2 rollover recovery smoke.
//!
//! The ordinary test suite does not require a provider or a local OpenCode
//! server. Set `OCG_REAL_V2_ROLLOVER_SMOKE=1` to run the disposable topology.
//! The phase-one child aborts at the first continuation resume call, which is
//! after the durable target cutover and before acknowledgement; phase two is a
//! fresh test process that must recover from disk.

use ocg::capabilities::CapabilityConfig;
use ocg::clock::FixedClock;
use ocg::context::ContextConfig;
use ocg::error::Result as OcgResult;
use ocg::orchestration::config::OrchestrationConfig;
use ocg::orchestration::context_governor::{ContextObservation, TelemetryProvenance};
use ocg::orchestration::controller::Controller;
use ocg::orchestration::handoff::Role;
use ocg::orchestration::mission::{self, Mission, MissionRolloverStatus};
use ocg::orchestration::rollover::{self, RolloverStatus};
use ocg::process::{FakeCaptureRunner, FakeGitHost};
use ocg::proxy::ChildProxyEnv;
use ocg::runtime::compat::v2_client::{ServiceRegistration, V2SessionClient};
use ocg::runtime::compat::v2_server::OwnedV2Server;
use ocg::runtime::compat::{
    select_session_lead, LeadSelection, SessionClient, SessionLifecycleClient,
};
use ocg::runtime::lifecycle::{
    RuntimeAdapter, RuntimeCapabilities, RuntimeContextEvent, RuntimeContextObservation,
    RuntimeContinuation, RuntimeError, RuntimeErrorKind, RuntimeExecution, RuntimeExecutionId,
    RuntimeIdentity, RuntimeModelMetadata, RuntimeProfile, RuntimeResult,
};
use ocg::verification::config::VerificationConfig;
use reqwest::blocking::Client as HttpClient;
use serde_json::{json, Value};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::thread;
use std::time::{Duration, Instant};

const GATE: &str = "OCG_REAL_V2_ROLLOVER_SMOKE";
const PHASE: &str = "OCG_ROLLOVER_SMOKE_PHASE";
const ROOT: &str = "OCG_ROLLOVER_SMOKE_ROOT";
const URL: &str = "OCG_ROLLOVER_SMOKE_URL";
const PASSWORD: &str = "OCG_ROLLOVER_SMOKE_PASSWORD";
const TARGET: &str = "OCG_ROLLOVER_SMOKE_TARGET";
const MISSION: &str = "OCG_ROLLOVER_SMOKE_MISSION";
const TASK: &str = "real V2 rollover recovery smoke";
const MARKER: &str = "ocg-rollover-cutover-marker.json";
const BASELINE: &str = "ocg-rollover-baseline.json";
const FAILURE: &str = "ocg-rollover-failure.json";
const PROVIDER: &str = "opencode";
const MODEL: &str = "space-bunny-free";
const AGENT: &str = "lead-high";
const CONFIG: &str = r#"{"agent":{"lead-high":{"mode":"primary","model":"opencode/space-bunny-free","steps":1,"prompt":"Complete the durable continuation without using tools."}}}"#;

type PhaseResult<T> = std::result::Result<T, SmokeFailure>;

#[derive(Debug)]
struct SmokeFailure {
    classification: &'static str,
    detail: String,
}

impl SmokeFailure {
    fn new(classification: &'static str, detail: impl Into<String>) -> Self {
        Self {
            classification,
            detail: detail.into(),
        }
    }
}

fn lead() -> LeadSelection {
    LeadSelection {
        level: "high".to_string(),
        agent: AGENT.to_string(),
        provider_id: PROVIDER.to_string(),
        model_id: MODEL.to_string(),
        variant: None,
    }
}

fn profile() -> RuntimeProfile {
    lead().runtime_profile()
}

fn controller<'a>(root: &Path, git: &'a FakeGitHost, clock: &'a FixedClock) -> Controller<'a> {
    Controller::new(
        root,
        OrchestrationConfig::default(),
        ContextConfig::default(),
        CapabilityConfig::default(),
        VerificationConfig::default(),
        git,
        clock,
    )
}

fn connect(
    url: &str,
    password: &str,
    directory: &Path,
) -> std::result::Result<V2SessionClient, String> {
    let registration = ServiceRegistration::new(url, password);
    V2SessionClient::connect(&registration, directory.to_string_lossy().as_ref())
        .map_err(|error| SmokeFailure::new("v2_api_failure", error.to_string()).detail)
}

fn observation(session: &str, event: &str, safe: bool, used: u64) -> ContextObservation {
    ContextObservation {
        session_id: session.to_string(),
        event_id: event.to_string(),
        observed_at: 100,
        safe_boundary: safe,
        used_tokens: Some(used),
        limit_tokens: Some(100),
        usage_provenance: TelemetryProvenance::Exact,
        context_provenance: TelemetryProvenance::Exact,
        ..ContextObservation::default()
    }
}

/// A test-only runtime wrapper. The abort is intentionally at the first
/// `resume_continuation` call: the controller has already persisted Mission
/// cutover, target binding, staged continuation, and the active rollover
/// artifact at that point.
struct KillAfterCutoverRuntime {
    inner: V2SessionClient,
    root: PathBuf,
    mission_id: String,
}

impl KillAfterCutoverRuntime {
    fn new(inner: V2SessionClient, root: PathBuf, mission_id: String) -> Self {
        Self {
            inner,
            root,
            mission_id,
        }
    }
}

impl SessionClient for KillAfterCutoverRuntime {
    fn resolve_session(&mut self) -> OcgResult<String> {
        self.inner.resolve_session()
    }

    fn select_agent(&mut self, session: &str, agent: &str) -> OcgResult<()> {
        self.inner.select_agent(session, agent)
    }

    fn select_model(
        &mut self,
        session: &str,
        provider_id: &str,
        model_id: &str,
        variant: Option<&str>,
    ) -> OcgResult<()> {
        self.inner
            .select_model(session, provider_id, model_id, variant)
    }

    fn effective_lead(&self, session: &str) -> OcgResult<ocg::runtime::compat::EffectiveLead> {
        self.inner.effective_lead(session)
    }
}

impl SessionLifecycleClient for KillAfterCutoverRuntime {
    fn create_fresh_session(&self) -> OcgResult<String> {
        V2SessionClient::create_fresh_session(&self.inner)
    }

    fn context_messages(&self, session: &str) -> OcgResult<Vec<Value>> {
        V2SessionClient::context_messages(&self.inner, session)
    }

    fn session_info(&self, session: &str) -> OcgResult<Value> {
        V2SessionClient::session_info(&self.inner, session)
    }

    fn model_metadata(
        &self,
        provider_id: Option<&str>,
        model_id: Option<&str>,
    ) -> OcgResult<RuntimeModelMetadata> {
        self.inner.model_metadata(provider_id, model_id)
    }

    fn inject_continuation(
        &self,
        session: &str,
        message_id: &str,
        text: &str,
        description: &str,
        metadata: &Value,
    ) -> OcgResult<()> {
        self.inner
            .inject_continuation(session, message_id, text, description, metadata)
    }

    fn stage_continuation(
        &self,
        session: &str,
        message_id: &str,
        text: &str,
        description: &str,
        metadata: &Value,
    ) -> OcgResult<()> {
        SessionLifecycleClient::stage_continuation(
            &self.inner,
            session,
            message_id,
            text,
            description,
            metadata,
        )
    }

    fn resume_continuation(
        &self,
        _session: &str,
        _message_id: &str,
        _text: &str,
        _description: &str,
        _metadata: &Value,
    ) -> OcgResult<()> {
        let artifact = rollover::latest_for_mission(&self.root, &self.mission_id)
            .ok()
            .flatten();
        let marker = json!({
            "mission_id": self.mission_id,
            "artifact_id": artifact.as_ref().map(|value| value.artifact_id.clone()),
            "artifact_status": artifact.as_ref().map(|value| value.status),
            "source_session_id": artifact.as_ref().map(|value| value.source_session_id.clone()),
            "target_session_id": artifact.as_ref().and_then(|value| value.target_session_id.clone()),
            "staged": true,
        });
        let _ = fs::write(
            self.root.join(MARKER),
            serde_json::to_vec_pretty(&marker).unwrap_or_default(),
        );
        // This is the deterministic failpoint. No timing or polling is
        // involved: the controller calls resume only after cutover is durable.
        std::process::abort();
    }
}

impl RuntimeAdapter for KillAfterCutoverRuntime {
    fn identity(&self) -> RuntimeIdentity {
        RuntimeIdentity::new("test", "opencode-v2", "rollover-smoke")
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities::OPENCODE_V2
    }

    fn resolve_execution(&mut self) -> RuntimeResult<RuntimeExecutionId> {
        RuntimeAdapter::resolve_execution(&mut self.inner)
    }

    fn create_execution(&mut self) -> RuntimeResult<RuntimeExecutionId> {
        RuntimeAdapter::create_execution(&mut self.inner)
    }

    fn inspect_execution(
        &self,
        execution_id: &RuntimeExecutionId,
    ) -> RuntimeResult<RuntimeExecution> {
        RuntimeAdapter::inspect_execution(&self.inner, execution_id)
    }

    fn prepare_execution(
        &mut self,
        execution_id: &RuntimeExecutionId,
        profile: &RuntimeProfile,
    ) -> RuntimeResult<RuntimeExecution> {
        RuntimeAdapter::prepare_execution(&mut self.inner, execution_id, profile)
    }

    fn observe_context(
        &self,
        event: &RuntimeContextEvent,
    ) -> RuntimeResult<RuntimeContextObservation> {
        RuntimeAdapter::observe_context(&self.inner, event)
    }

    fn stage_runtime_continuation(
        &self,
        execution_id: &RuntimeExecutionId,
        continuation: &RuntimeContinuation,
    ) -> RuntimeResult<()> {
        RuntimeAdapter::stage_runtime_continuation(&self.inner, execution_id, continuation)
    }

    fn resume_runtime_continuation(
        &self,
        execution_id: &RuntimeExecutionId,
        continuation: &RuntimeContinuation,
    ) -> RuntimeResult<()> {
        // Reuse the failpoint implementation above so the kill remains exactly
        // before the real resume request.
        <Self as SessionLifecycleClient>::resume_continuation(
            self,
            execution_id.as_str(),
            &continuation.id,
            &continuation.text,
            &continuation.description,
            &continuation.metadata,
        )
        .map_err(|error| RuntimeError::new(RuntimeErrorKind::ProviderCompletion, error.to_string()))
    }
}

fn write_baseline(root: &Path, mission: &Mission) -> std::result::Result<(), String> {
    let value = json!({
        "generation": mission.generation,
        "attempts": mission.attempts,
        "findings": mission.findings,
        "checkpoints": mission.checkpoints,
        "history_len": mission.history.len(),
    });
    fs::write(
        root.join(BASELINE),
        serde_json::to_vec_pretty(&value).unwrap(),
    )
    .map_err(|error| error.to_string())
}

fn redacted_detail(detail: impl std::fmt::Display, url: &str, password: &str) -> String {
    let detail = detail.to_string();
    let detail = detail
        .replace(url, "<redacted-url>")
        .replace(password, "<redacted-password>");
    if detail.contains("http://") || detail.contains("https://") {
        "<redacted-url>".to_string()
    } else {
        detail
    }
}

fn api_get(url: &str, password: &str, path: &str) -> PhaseResult<Value> {
    let endpoint = format!("{url}{path}");
    let response = HttpClient::builder()
        .no_proxy()
        .build()
        .map_err(|error| {
            SmokeFailure::new("v2_api_failure", redacted_detail(error, url, password))
        })?
        .get(&endpoint)
        .basic_auth("opencode", Some(password))
        .send()
        .map_err(|error| {
            SmokeFailure::new("v2_api_failure", redacted_detail(error, url, password))
        })?;
    let status = response.status();
    if !status.is_success() {
        return Err(SmokeFailure::new(
            "v2_api_failure",
            format!("local V2 endpoint returned HTTP {}", status.as_u16()),
        ));
    }
    let body = response.text().map_err(|error| {
        SmokeFailure::new("v2_api_failure", redacted_detail(error, url, password))
    })?;
    serde_json::from_str(&body)
        .map_err(|error| SmokeFailure::new("v2_api_failure", redacted_detail(error, url, password)))
}

fn api_session_info(url: &str, password: &str, session: &str) -> PhaseResult<Value> {
    api_get(url, password, &format!("/api/session/{session}"))
}

fn api_context(url: &str, password: &str, session: &str) -> PhaseResult<Vec<Value>> {
    let value = api_get(url, password, &format!("/api/session/{session}/context"))?;
    value
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .ok_or_else(|| SmokeFailure::new("v2_api_failure", "context response had no data array"))
}

fn list_session_ids(url: &str, password: &str, directory: &Path) -> PhaseResult<Vec<String>> {
    let mut endpoint = reqwest::Url::parse(url).map_err(|error| {
        SmokeFailure::new("v2_api_failure", redacted_detail(error, url, password))
    })?;
    endpoint.set_path("/api/session");
    endpoint
        .query_pairs_mut()
        .append_pair("directory", &directory.to_string_lossy());
    endpoint.query_pairs_mut().append_pair("limit", "100");
    let response = HttpClient::builder()
        .no_proxy()
        .build()
        .map_err(|error| {
            SmokeFailure::new("v2_api_failure", redacted_detail(error, url, password))
        })?
        .get(endpoint)
        .basic_auth("opencode", Some(password))
        .send()
        .map_err(|error| {
            SmokeFailure::new("v2_api_failure", redacted_detail(error, url, password))
        })?;
    if !response.status().is_success() {
        return Err(SmokeFailure::new(
            "v2_api_failure",
            format!(
                "session listing returned HTTP {}",
                response.status().as_u16()
            ),
        ));
    }
    let body = response.text().map_err(|error| {
        SmokeFailure::new("v2_api_failure", redacted_detail(error, url, password))
    })?;
    let value: Value = serde_json::from_str(&body).map_err(|error| {
        SmokeFailure::new("v2_api_failure", redacted_detail(error, url, password))
    })?;
    let mut ids = value
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| SmokeFailure::new("v2_api_failure", "session listing had no data array"))?
        .iter()
        .filter_map(|item| item.get("id").and_then(Value::as_str).map(str::to_string))
        .collect::<Vec<_>>();
    ids.sort();
    Ok(ids)
}

fn assert_lead_on_real_session(url: &str, password: &str, session: &str) -> PhaseResult<()> {
    let info = api_session_info(url, password, session)?;
    let data = info.get("data").ok_or_else(|| {
        SmokeFailure::new("v2_api_failure", "session response had no data object")
    })?;
    if data.get("id").and_then(Value::as_str) != Some(session) {
        return Err(SmokeFailure::new(
            "ocg_recovery_failure",
            "V2 returned a different session identity",
        ));
    }
    if data.get("agent").and_then(Value::as_str) != Some(AGENT) {
        return Err(SmokeFailure::new(
            "ocg_recovery_failure",
            "V2 session did not report the verified Lead agent",
        ));
    }
    let model = data.get("model").ok_or_else(|| {
        SmokeFailure::new("ocg_recovery_failure", "V2 session did not report a model")
    })?;
    if model.get("providerID").and_then(Value::as_str) != Some(PROVIDER)
        || model.get("id").and_then(Value::as_str) != Some(MODEL)
    {
        return Err(SmokeFailure::new(
            "ocg_recovery_failure",
            "V2 session reported a different Lead model",
        ));
    }
    Ok(())
}

fn context_summary(messages: &[Value], continuation_id: &str) -> String {
    let synthetic_count = messages
        .iter()
        .filter(|message| {
            message.get("type").and_then(Value::as_str) == Some("synthetic")
                && message.get("id").and_then(Value::as_str) == Some(continuation_id)
        })
        .count();
    let tail = messages
        .iter()
        .rev()
        .take(6)
        .map(|message| {
            let kind = message
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let finish = message.get("finish").and_then(Value::as_str).unwrap_or("-");
            let agent = message.get("agent").and_then(Value::as_str).unwrap_or("-");
            let error = message
                .get("error")
                .and_then(Value::as_object)
                .and_then(|object| object.get("name").or_else(|| object.get("type")))
                .and_then(Value::as_str)
                .unwrap_or("-");
            format!("{kind}/{finish}/{agent}/{error}")
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("synthetic={synthetic_count}; tail={tail}")
}

fn wait_for_provider_completion(
    client: &V2SessionClient,
    target: &str,
    continuation_id: &str,
) -> PhaseResult<()> {
    let timeout_seconds = env::var("OCG_ROLLOVER_SMOKE_TIMEOUT")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(180);
    let deadline = Instant::now() + Duration::from_secs(timeout_seconds);
    let mut saw_stop = false;
    loop {
        let messages = client
            .context_messages(target)
            .map_err(|error| SmokeFailure::new("v2_api_failure", error.to_string()))?;
        let last_summary = context_summary(&messages, continuation_id);
        let synthetic_count = messages
            .iter()
            .filter(|message| {
                message.get("type").and_then(Value::as_str) == Some("synthetic")
                    && message.get("id").and_then(Value::as_str) == Some(continuation_id)
            })
            .count();
        if synthetic_count > 1 {
            return Err(SmokeFailure::new(
                "ocg_recovery_failure",
                "the staged continuation appeared more than once",
            ));
        }
        if let Some(assistant) = messages
            .iter()
            .rev()
            .find(|message| message.get("type").and_then(Value::as_str) == Some("assistant"))
        {
            match assistant.get("finish").and_then(Value::as_str) {
                Some("stop") if synthetic_count == 1 => return Ok(()),
                Some("stop") => saw_stop = true,
                Some("error") => {
                    return Err(SmokeFailure::new(
                        "provider_completion_failure",
                        "provider returned an assistant error after recovery",
                    ));
                }
                _ => {}
            }
        }
        if Instant::now() >= deadline {
            return Err(SmokeFailure::new(
                if saw_stop {
                    "ocg_recovery_failure"
                } else {
                    "provider_completion_failure"
                },
                format!(
                    "{}; last_context={last_summary}",
                    if saw_stop {
                        "provider stopped but the staged continuation was not durably visible"
                    } else {
                        "provider-backed assistant completion timed out after recovery"
                    }
                ),
            ));
        }
        thread::sleep(Duration::from_millis(200));
    }
}

fn run_kill_phase() -> PhaseResult<()> {
    let root =
        PathBuf::from(env::var(ROOT).map_err(|_| {
            SmokeFailure::new("environment_failure", "smoke root was not provided")
        })?);
    let url = env::var(URL).map_err(|_| {
        SmokeFailure::new("environment_failure", "smoke server URL was not provided")
    })?;
    let password = env::var(PASSWORD).map_err(|_| {
        SmokeFailure::new(
            "environment_failure",
            "smoke server password was not provided",
        )
    })?;
    let client = connect(&url, &password, &root)
        .map_err(|detail| SmokeFailure::new("v2_api_failure", detail))?;
    let mut client = client;
    let source = client
        .resolve_session()
        .map_err(|error| SmokeFailure::new("v2_api_failure", error.to_string()))?;
    select_session_lead(&mut client, &lead())
        .map_err(|error| SmokeFailure::new("v2_api_failure", error.to_string()))?;
    assert_lead_on_real_session(&url, &password, &source)?;

    let git = FakeGitHost::new();
    let clock = FixedClock::new(100);
    let ocg = controller(&root, &git, &clock);
    let mission_id = ocg
        .admit_user_task(&source, TASK)
        .map_err(|error| SmokeFailure::new("ocg_recovery_failure", error.to_string()))?
        .task_id;
    ocg.prepare_handoff(&source, Role::Explore, "record committed smoke progress")
        .map_err(|error| SmokeFailure::new("ocg_recovery_failure", error.to_string()))?;
    ocg.consume_explore_result(
        &source,
        &json!({
            "goal": "preserve durable rollover progress",
            "constraints": ["keep Mission identity and generation"],
            "files": ["src/main.rs"],
            "symbols": [{"name": "main"}],
            "findings": [{"summary": "the disposable project is ready for rollover", "severity": "info"}]
        })
        .to_string(),
    )
    .map_err(|error| SmokeFailure::new("ocg_recovery_failure", error.to_string()))?;
    ocg.prepare_handoff(&source, Role::Build, "record a committed build attempt")
        .map_err(|error| SmokeFailure::new("ocg_recovery_failure", error.to_string()))?;
    ocg.after_build(
        &source,
        &FakeCaptureRunner::new().with_success("true", &[], "smoke progress\n"),
        None,
    )
    .map_err(|error| SmokeFailure::new("ocg_recovery_failure", error.to_string()))?;
    let mission = mission::load(&root, &mission_id)
        .map_err(|error| SmokeFailure::new("ocg_recovery_failure", error.to_string()))?
        .ok_or_else(|| SmokeFailure::new("ocg_recovery_failure", "Mission was not persisted"))?;
    write_baseline(&root, &mission)
        .map_err(|error| SmokeFailure::new("environment_failure", error))?;

    let mut runtime = KillAfterCutoverRuntime::new(client, root, mission_id);
    let _ = ocg.observe_context(
        &source,
        observation(&source, "real-v2-cutover-kill", true, 90),
        &mut runtime,
        &profile(),
    );
    Err(SmokeFailure::new(
        "ocg_recovery_failure",
        "rollover returned before the deterministic cutover failpoint",
    ))
}

fn run_recover_phase() -> PhaseResult<()> {
    let root =
        PathBuf::from(env::var(ROOT).map_err(|_| {
            SmokeFailure::new("environment_failure", "smoke root was not provided")
        })?);
    let url = env::var(URL).map_err(|_| {
        SmokeFailure::new("environment_failure", "smoke server URL was not provided")
    })?;
    let password = env::var(PASSWORD).map_err(|_| {
        SmokeFailure::new(
            "environment_failure",
            "smoke server password was not provided",
        )
    })?;
    let target = env::var(TARGET).map_err(|_| {
        SmokeFailure::new(
            "environment_failure",
            "smoke target session was not provided",
        )
    })?;
    let mission_id = env::var(MISSION).map_err(|_| {
        SmokeFailure::new("environment_failure", "smoke Mission id was not provided")
    })?;
    let client = connect(&url, &password, &root)
        .map_err(|detail| SmokeFailure::new("v2_api_failure", detail))?;
    let git = FakeGitHost::new();
    let clock = FixedClock::new(200);
    let ocg = controller(&root, &git, &clock);
    let before = mission::load(&root, &mission_id)
        .map_err(|error| SmokeFailure::new("ocg_recovery_failure", error.to_string()))?
        .ok_or_else(|| {
            SmokeFailure::new("ocg_recovery_failure", "Mission disappeared on restart")
        })?;
    if before.session_id.as_deref() != Some(target.as_str()) {
        return Err(SmokeFailure::new(
            "ocg_recovery_failure",
            "restart did not load the cutover target as Mission owner",
        ));
    }
    if before.rollover.status != MissionRolloverStatus::Active {
        return Err(SmokeFailure::new(
            "ocg_recovery_failure",
            "restart did not load the incomplete active rollover",
        ));
    }
    let artifact = rollover::latest_for_mission(&root, &mission_id)
        .map_err(|error| SmokeFailure::new("ocg_recovery_failure", error.to_string()))?
        .ok_or_else(|| {
            SmokeFailure::new("ocg_recovery_failure", "rollover artifact disappeared")
        })?;
    let continuation_id = artifact.prompt_id();

    let mut runtime = client;
    let result = ocg
        .observe_context(
            &target,
            observation(&target, "real-v2-recover-after-kill", true, 10),
            &mut runtime,
            &profile(),
        )
        .map_err(|error| SmokeFailure::new("ocg_recovery_failure", error.to_string()))?;
    if result.rollover_status != Some(MissionRolloverStatus::Applied) {
        return Err(SmokeFailure::new(
            "ocg_recovery_failure",
            "recovery did not acknowledge the active rollover",
        ));
    }
    wait_for_provider_completion(&runtime, &target, &continuation_id)?;

    let after = mission::load(&root, &mission_id)
        .map_err(|error| SmokeFailure::new("ocg_recovery_failure", error.to_string()))?
        .ok_or_else(|| {
            SmokeFailure::new("ocg_recovery_failure", "Mission disappeared after recovery")
        })?;
    if after.mission_id != before.mission_id
        || after.generation != before.generation
        || after.session_id.as_deref() != Some(target.as_str())
        || after.rollover.artifact_id.as_deref() != Some(artifact.artifact_id.as_str())
    {
        return Err(SmokeFailure::new(
            "ocg_recovery_failure",
            "recovery changed Mission identity, generation, owner, or rollover identity",
        ));
    }
    if after.attempts != before.attempts
        || after.findings != before.findings
        || after.checkpoints != before.checkpoints
    {
        return Err(SmokeFailure::new(
            "ocg_recovery_failure",
            "recovery changed committed Mission progress",
        ));
    }
    let artifact_after = rollover::latest_for_mission(&root, &mission_id)
        .map_err(|error| SmokeFailure::new("ocg_recovery_failure", error.to_string()))?
        .ok_or_else(|| {
            SmokeFailure::new(
                "ocg_recovery_failure",
                "rollover artifact disappeared after recovery",
            )
        })?;
    if artifact_after.status != RolloverStatus::Applied
        || artifact_after.artifact_id != artifact.artifact_id
    {
        return Err(SmokeFailure::new(
            "ocg_recovery_failure",
            "rollover artifact did not reach Applied with the same identity",
        ));
    }
    let applied_events = after
        .history
        .iter()
        .filter(|event| {
            event.kind == ocg::orchestration::mission::MissionEventKind::RolloverApplied
        })
        .count();
    if applied_events != 1 {
        return Err(SmokeFailure::new(
            "ocg_recovery_failure",
            "recovery did not record exactly one applied transition",
        ));
    }

    // Repeating the observation after acknowledgement is deliberately a
    // no-op: no second target, no second resume, and no additional history.
    let history_before_replay = after.history.len();
    let _ = ocg
        .observe_context(
            &target,
            observation(&target, "real-v2-recover-replay", true, 10),
            &mut runtime,
            &profile(),
        )
        .map_err(|error| SmokeFailure::new("ocg_recovery_failure", error.to_string()))?;
    let after_replay = mission::load(&root, &mission_id)
        .map_err(|error| SmokeFailure::new("ocg_recovery_failure", error.to_string()))?
        .ok_or_else(|| {
            SmokeFailure::new("ocg_recovery_failure", "Mission disappeared after replay")
        })?;
    if after_replay.history.len() != history_before_replay
        || after_replay.rollover.artifact_id.as_deref() != Some(artifact.artifact_id.as_str())
    {
        return Err(SmokeFailure::new(
            "ocg_recovery_failure",
            "replaying recovery after Applied was not a no-op",
        ));
    }
    Ok(())
}

fn run_phase(phase: &str) -> PhaseResult<()> {
    match phase {
        "kill" => run_kill_phase(),
        "recover" => run_recover_phase(),
        _ => Err(SmokeFailure::new(
            "environment_failure",
            "unknown smoke phase",
        )),
    }
}

fn child_command(
    phase: &str,
    root: &Path,
    server: &OwnedV2Server,
    target: Option<&str>,
    mission: Option<&str>,
) -> Output {
    let executable = env::current_exe().expect("current test executable");
    let mut command = Command::new(executable);
    command
        .args(["--exact", "real_v2_rollover_recovery_smoke", "--nocapture"])
        .current_dir(root)
        .env(GATE, "1")
        .env(PHASE, phase)
        .env(ROOT, root)
        .env(URL, server.url())
        .env(PASSWORD, server.password())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    if let Some(target) = target {
        command.env(TARGET, target);
    }
    if let Some(mission) = mission {
        command.env(MISSION, mission);
    }
    command.output().expect("smoke child process")
}

fn redact_child_output(output: &Output, url: &str, password: &str) -> String {
    let combined = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    combined
        .replace(url, "<redacted-url>")
        .replace(password, "<redacted-password>")
        .lines()
        .map(|line| {
            if line.contains("http://") || line.contains("https://") {
                "<redacted-url-line>".to_string()
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
        .chars()
        .take(2000)
        .collect()
}

fn run_parent_smoke() {
    let root = tempfile::tempdir().expect("disposable smoke project");
    let root_path = root.path().to_path_buf();
    fs::write(
        root_path.join("Cargo.toml"),
        "[package]\nname = \"ocg-v2-rollover-smoke\"\nversion = \"0.0.0\"\nedition = \"2021\"\n",
    )
    .expect("fixture manifest");
    fs::create_dir_all(root_path.join("src")).expect("fixture source directory");
    fs::write(root_path.join("src/main.rs"), "fn main() {}\n").expect("fixture source");

    let server = OwnedV2Server::start(
        Path::new("opencode"),
        CONFIG,
        &[],
        &ChildProxyEnv::default(),
    )
    .expect("start disposable OpenCode V2 server");
    let url = server.url().to_string();
    let password = server.password().to_string();

    let mission_id = Controller::task_id(TASK);
    let killed = child_command("kill", &root_path, &server, None, None);
    if killed.status.success() {
        panic!("phase one unexpectedly returned before the cutover failpoint");
    }
    if !root_path.join(MARKER).is_file() {
        let detail = redact_child_output(&killed, &url, &password);
        panic!("phase one did not reach the durable cutover failpoint: {detail}");
    }

    let mission = mission::load(&root_path, &mission_id)
        .expect("load Mission after kill")
        .expect("Mission must exist after cutover kill");
    let artifact = rollover::latest_for_mission(&root_path, &mission_id)
        .expect("load rollover after kill")
        .expect("rollover artifact must exist after cutover kill");
    let continuation_id = artifact.prompt_id();
    let source = mission
        .rollover
        .source_session_id
        .clone()
        .expect("source session persisted in rollover");
    let target = mission
        .session_id
        .clone()
        .expect("target session is Mission owner");
    assert_eq!(mission.mission_id, mission_id);
    assert_eq!(mission.generation, 1);
    assert_eq!(
        mission.status,
        ocg::orchestration::mission::MissionStatus::Active
    );
    assert_eq!(mission.rollover.status, MissionRolloverStatus::Active);
    assert!(
        !mission.findings.is_empty(),
        "committed findings must exist before rollover"
    );
    assert!(
        !mission.checkpoints.is_empty(),
        "committed checkpoint must exist before rollover"
    );
    assert!(
        mission.attempts.build > 0,
        "committed attempt must exist before rollover"
    );
    assert_ne!(source, target);
    assert_eq!(artifact.status, RolloverStatus::Active);
    assert_eq!(artifact.target_session_id.as_deref(), Some(target.as_str()));
    assert!(!mission
        .history
        .iter()
        .any(|event| event.kind == ocg::orchestration::mission::MissionEventKind::RolloverApplied));
    assert!(
        rollover::continuation_path(&root_path, &artifact.artifact_id)
            .expect("continuation path")
            .is_file()
    );

    let source_info = api_session_info(&url, &password, &source).expect("source session is real");
    let target_info = api_session_info(&url, &password, &target).expect("target session is real");
    assert_eq!(source_info["data"]["id"], json!(source));
    assert_eq!(target_info["data"]["id"], json!(target));
    assert_lead_on_real_session(&url, &password, &target).expect("target Lead is verified");
    let marker: Value =
        serde_json::from_slice(&fs::read(root_path.join(MARKER)).expect("cutover marker"))
            .expect("valid cutover marker");
    assert_eq!(marker["staged"], json!(true));
    assert_eq!(marker["artifact_status"], json!("active"));

    let baseline: Value = serde_json::from_slice(
        &fs::read(root_path.join(BASELINE)).expect("phase one progress baseline"),
    )
    .expect("valid baseline");
    let sessions_before =
        list_session_ids(&url, &password, &root_path).expect("list sessions before recovery");
    assert!(sessions_before.contains(&source));
    assert!(sessions_before.contains(&target));

    let recovered = child_command(
        "recover",
        &root_path,
        &server,
        Some(&target),
        Some(&mission_id),
    );
    if !recovered.status.success() {
        let child_detail = redact_child_output(&recovered, &url, &password);
        let failure_record = fs::read_to_string(root_path.join(FAILURE))
            .ok()
            .and_then(|text| serde_json::from_str::<Value>(&text).ok());
        let classification = failure_record
            .as_ref()
            .and_then(|value| value.get("classification"))
            .and_then(Value::as_str)
            .unwrap_or("unclassified");
        let failure_detail = failure_record
            .as_ref()
            .and_then(|value| value.get("detail"))
            .and_then(Value::as_str)
            .unwrap_or("no redacted child diagnostic");
        panic!("phase two failed ({classification}): {failure_detail}\nchild: {child_detail}");
    }
    assert!(
        !root_path.join(FAILURE).exists(),
        "successful recovery wrote a failure record"
    );

    let after = mission::load(&root_path, &mission_id)
        .expect("load Mission after recovery")
        .expect("Mission remains after recovery");
    assert_eq!(after.mission_id, mission_id);
    assert_eq!(after.generation, 1);
    assert_eq!(after.session_id.as_deref(), Some(target.as_str()));
    assert_eq!(after.rollover.status, MissionRolloverStatus::Applied);
    assert_eq!(
        after.rollover.artifact_id.as_deref(),
        Some(artifact.artifact_id.as_str())
    );
    assert_eq!(
        serde_json::to_value(after.attempts).unwrap(),
        baseline["attempts"]
    );
    assert_eq!(
        serde_json::to_value(&after.findings).unwrap(),
        baseline["findings"]
    );
    assert_eq!(
        serde_json::to_value(&after.checkpoints).unwrap(),
        baseline["checkpoints"]
    );

    let sessions_after =
        list_session_ids(&url, &password, &root_path).expect("list sessions after recovery");
    assert_eq!(
        sessions_before, sessions_after,
        "recovery created or replaced a target session"
    );
    let recovered_context =
        api_context(&url, &password, &target).expect("recovered target context");
    assert_eq!(
        recovered_context
            .iter()
            .filter(|message| {
                message.get("type").and_then(Value::as_str) == Some("synthetic")
                    && message.get("id").and_then(Value::as_str) == Some(continuation_id.as_str())
            })
            .count(),
        1,
        "continuation semantic message must occur exactly once"
    );
    assert_eq!(
        recovered_context
            .iter()
            .filter(|message| {
                message.get("type").and_then(Value::as_str) == Some("assistant")
                    && message.get("finish").and_then(Value::as_str) == Some("stop")
            })
            .count(),
        1,
        "provider continuation must complete exactly once"
    );

    println!("REAL_V2_ROLLOVER_SMOKE=PASS");
    println!("REAL_V2_RECOVERY=MISSION_ID_GENERATION_TARGET_REUSED");
    println!("REAL_V2_PROVIDER_COMPLETION=PASS");
}

#[test]
fn real_v2_rollover_recovery_smoke() {
    if let Ok(phase) = env::var(PHASE) {
        if let Err(failure) = run_phase(&phase) {
            if let Ok(root) = env::var(ROOT) {
                let record = json!({
                    "classification": failure.classification,
                    "detail": failure.detail,
                });
                let _ = fs::write(
                    PathBuf::from(root).join(FAILURE),
                    serde_json::to_vec_pretty(&record).unwrap_or_default(),
                );
            }
            panic!("{}: {}", failure.classification, failure.detail);
        }
        return;
    }
    if env::var(GATE).as_deref() != Ok("1") {
        eprintln!("gated real V2 rollover smoke skipped; set {GATE}=1");
        return;
    }
    run_parent_smoke();
}
