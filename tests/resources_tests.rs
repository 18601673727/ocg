//! Focused tests for the durable, descriptive Resource Registry.
//!
//! These tests cover: identity distinctness, first-class Unknown semantics,
//! provenance precedence, factual health classification, idempotent merge,
//! corruption-isolated persistence with no secrets, the RuntimeAdapter and
//! Reconciler ingestion seams, and the read-only CLI surface. They never assert
//! that a resource is *selected* — the registry is descriptive.

mod common;

use common::{load_embedded_effective, project_profile, write_yaml, TestDir};
use ocg::orchestration::reconcile::{
    publish_resource_observations, ObservationStatus, ReconcileAction, ReconcileOutcome,
    ReconcileResult, ReconcileRun,
};
use ocg::resources::{
    configured_entries, load, registry_path, save, CapabilityFacts, CatalogueEvidence,
    ConfiguredResource, EffectiveFacts, HealthFacts, ResourceHealth, ResourceId, ResourceIdentity,
    ResourceProvenance, ResourceRegistry, RESOURCE_SCHEMA_VERSION,
};
use ocg::runtime::lifecycle::{
    RuntimeAdapter, RuntimeCapabilities, RuntimeError, RuntimeErrorKind, RuntimeExecutionId,
    RuntimeIdentity, RuntimeModelMetadata, RuntimeProvenance,
};
use serde_json::json;
use std::fs;
use std::path::Path;
use std::process::{Command, Output};

const MODEL: ResourceIdentity = ResourceIdentity {
    runtime: None,
    family: None,
    provider: None,
    model: None,
    account_profile: None,
    protocol: None,
};

fn model_identity(provider: &str, model: &str) -> ResourceIdentity {
    ResourceIdentity::for_model(provider, model)
}

fn v2_runtime() -> RuntimeIdentity {
    RuntimeIdentity::new("opencode", "v2", "invocation")
}

struct FakeAdapter {
    runtime: RuntimeIdentity,
    capabilities: RuntimeCapabilities,
}

impl RuntimeAdapter for FakeAdapter {
    fn identity(&self) -> RuntimeIdentity {
        self.runtime.clone()
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        self.capabilities
    }
}

fn run(cwd: &Path, work: &Path, args: &[&str]) -> Output {
    run_with_runtime(cwd, work, args, None)
}

/// Run `ocg` with an optional explicit OpenCode executable, so a test that
/// re-derives runtime-scoped configured facts is hermetic regardless of what
/// runtime the host happens to have on PATH.
fn run_with_runtime(cwd: &Path, work: &Path, args: &[&str], runtime: Option<&Path>) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ocg"));
    command
        .current_dir(cwd)
        .env("OCG_USER_CONFIG", work.join("no-user.yaml"))
        .env_remove("OCG_PROJECT_CONFIG")
        .env_remove("OCG_THROTTLE")
        .env_remove("OCG_HOME")
        .env_remove("OCG_OPENCODE")
        .env_remove("OCG_OPENCODE_BIN")
        .env_remove("OCG_OPENCODE_BIN")
        .args(args);
    if let Some(runtime) = runtime {
        command.env("OCG_OPENCODE", runtime);
    }
    command.output().expect("run ocg")
}

/// A fake `opencode` binary reporting a V2 version, so CLI tests that re-derive
/// runtime-scoped configured facts are deterministic on every host.
#[cfg(unix)]
fn fake_v2_runtime(dir: &TestDir) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let script = dir.join("fake-opencode-v2");
    std::fs::write(
        &script,
        "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 2.0.14; exit 0; fi\nexit 0\n",
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    script
}

// -- identity -----------------------------------------------------------------

#[test]
fn identity_is_not_model_only_and_not_an_execution_id() {
    let plain = model_identity("openai", "gpt-5.6-sol");
    let with_runtime = plain.clone().with_runtime_family("opencode", "v2");
    assert_ne!(
        ResourceId::derive(&plain),
        ResourceId::derive(&with_runtime),
        "the same provider/model on different runtimes must not be conflated"
    );

    let other_account = ResourceIdentity {
        account_profile: Some("team-a".to_string()),
        ..plain.clone()
    };
    let other_protocol = ResourceIdentity {
        protocol: Some("responses".to_string()),
        ..plain.clone()
    };
    assert_ne!(
        ResourceId::derive(&plain),
        ResourceId::derive(&other_account)
    );
    assert_ne!(
        ResourceId::derive(&plain),
        ResourceId::derive(&other_protocol)
    );

    let id = ResourceId::derive(&plain);
    assert!(id.as_str().starts_with("res-"));
    assert!(!id.as_str().contains('/'), "the id is filesystem-safe");
    let execution = RuntimeExecutionId::new("ses_abc");
    assert_ne!(
        id.as_str(),
        execution.as_str(),
        "the resource id is a distinct namespace from the execution id"
    );
    assert_ne!(id.as_str(), "mission-1");
    assert!(!id.as_str().starts_with("ses"));

    // An empty identity still yields a deterministic, distinct, safe id.
    let empty = ResourceId::derive(&MODEL);
    assert!(empty.as_str().starts_with("res-"));
    assert_eq!(empty, ResourceId::derive(&ResourceIdentity::default()));
}

#[test]
fn raw_agent_labels_are_descriptive_and_never_identity() {
    let configured = ResourceIdentity::for_model("opencode-go", "deepseek-v4.1-flash");
    let mut registry = ResourceRegistry::new(1);
    // Registering the same provider/model under two different OpenCode agent
    // labels must not create or destroy resources.
    for agent in ["build", "debug", "lead-mid"] {
        registry.register_configured(
            &configured,
            ConfiguredResource {
                role: Some(agent.to_string()),
                agent: Some(agent.to_string()),
                variant: None,
            },
            1,
        );
    }
    assert_eq!(registry.len(), 1, "agent labels never create resources");
    let record = registry.resource(&ResourceId::derive(&configured)).unwrap();
    assert_eq!(record.configured.len(), 3);
}

// -- Unknown is first-class ---------------------------------------------------

#[test]
fn unknown_is_first_class_and_never_defaulted() {
    let identity = model_identity("openai", "gpt-6-astra");
    let mut registry = ResourceRegistry::new(10);
    registry.register_configured(
        &identity,
        ConfiguredResource {
            role: Some("high".to_string()),
            agent: Some("lead-high".to_string()),
            variant: Some("high".to_string()),
        },
        10,
    );
    let record = registry.resource(&ResourceId::derive(&identity)).unwrap();
    assert_eq!(record.health.state, ResourceHealth::Unknown);
    assert!(record.context_limit.value.is_none());
    assert!(record.capacity.value.is_none());
    assert!(record.quota.value.is_none());
    assert!(record.cost.value.is_none());
    assert!(record.capabilities.value.is_none());
    assert!(record.resolved.value.is_none());
    assert_eq!(record.resolved.provenance, ResourceProvenance::Unknown);
    assert_eq!(record.updated_at, 10);
}

// -- provenance and merge -----------------------------------------------------

#[test]
fn merge_never_erases_a_known_fact_with_a_weaker_one() {
    let identity = model_identity("openai", "gpt-5.6-sol");
    let mut registry = ResourceRegistry::new(1);
    registry.observe_model_metadata(
        &identity,
        &RuntimeModelMetadata {
            provider_id: Some("openai".to_string()),
            model_id: Some("gpt-5.6-sol".to_string()),
            context_limit: Some(200_000),
            ..RuntimeModelMetadata::default()
        },
        5,
    );
    let id = ResourceId::derive(&identity);
    assert_eq!(
        registry.resource(&id).unwrap().context_limit.value,
        Some(200_000)
    );

    // A later, unknown limit must not erase the known one.
    registry.observe_model_metadata(&identity, &RuntimeModelMetadata::default(), 9);
    let record = registry.resource(&id).unwrap();
    assert_eq!(record.context_limit.value, Some(200_000));
    assert_eq!(
        record.context_limit.provenance,
        ResourceProvenance::RuntimeObserved
    );

    // A catalogue fact is weaker than a live observation and must not clobber
    // the observed effective model.
    registry.observe_effective(
        &identity,
        EffectiveFacts {
            provider: Some("openai".to_string()),
            model: Some("gpt-5.6-sol".to_string()),
            ..EffectiveFacts::default()
        },
        6,
    );
    registry.observe_effective(
        &identity,
        EffectiveFacts {
            provider: Some("wrong".to_string()),
            model: Some("wrong".to_string()),
            ..EffectiveFacts::default()
        },
        4,
    );
    assert_eq!(
        registry
            .resource(&id)
            .unwrap()
            .effective
            .value
            .unwrap()
            .model,
        Some("gpt-5.6-sol".to_string()),
        "an older equal-source observation must not overwrite a newer one"
    );
}

#[test]
fn telemetry_provenance_converts_to_a_resource_source() {
    assert_eq!(
        ResourceProvenance::from(RuntimeProvenance::Exact),
        ResourceProvenance::RuntimeObserved
    );
    assert_eq!(
        ResourceProvenance::from(RuntimeProvenance::Estimated),
        ResourceProvenance::Estimated
    );
    assert_eq!(
        ResourceProvenance::from(RuntimeProvenance::Unknown),
        ResourceProvenance::Unknown
    );
    // Stronger sources rank above weaker ones.
    assert!(ResourceProvenance::ProviderReported.rank() > ResourceProvenance::Estimated.rank());
    assert!(ResourceProvenance::StaticConfig.rank() > ResourceProvenance::RuntimeReported.rank());
}

// -- health -------------------------------------------------------------------

#[test]
fn runtime_errors_classify_to_factual_health() {
    let identity = model_identity("openai", "gpt-5.6-sol");
    let mut registry = ResourceRegistry::new(1);
    let id = ResourceId::derive(&identity);

    registry.observe_error(
        &identity,
        &RuntimeError::new(RuntimeErrorKind::Authentication, "bad credentials"),
        2,
    );
    assert_eq!(
        registry.resource(&id).unwrap().health.state,
        ResourceHealth::Unavailable
    );

    // A transient transport failure is Degraded, not permanent death.
    registry.observe_error(
        &identity,
        &RuntimeError::new(RuntimeErrorKind::Transport, "connection reset"),
        3,
    );
    assert_eq!(
        registry.resource(&id).unwrap().health.state,
        ResourceHealth::Degraded
    );

    // A later success clears it again.
    registry.observe_available(&identity, "runtime reachable", 4);
    assert_eq!(
        registry.resource(&id).unwrap().health.state,
        ResourceHealth::Available
    );

    // A missing execution means the runtime answered: still Available.
    registry.observe_error(
        &identity,
        &RuntimeError::new(RuntimeErrorKind::ExecutionMissing, "gone"),
        5,
    );
    assert_eq!(
        registry.resource(&id).unwrap().health.state,
        ResourceHealth::Available
    );

    // An unknown-health observation never erases a known state.
    registry.observe_health(
        &identity,
        HealthFacts {
            state: ResourceHealth::Unknown,
            ..HealthFacts::default()
        },
        6,
    );
    let record = registry.resource(&id).unwrap();
    assert_eq!(record.health.state, ResourceHealth::Available);
    assert_eq!(record.health.observed_at, Some(5));
}

// -- idempotence and merge scope ---------------------------------------------

#[test]
fn repeated_observations_are_idempotent_and_scoped() {
    let identity = model_identity("opencode-go", "glm-5.3-flash");
    let mut registry = ResourceRegistry::new(1);
    registry.observe_model_metadata(
        &identity,
        &RuntimeModelMetadata {
            context_limit: Some(128_000),
            ..RuntimeModelMetadata::default()
        },
        5,
    );
    registry.observe_available(&identity, "reachable", 5);
    let first = registry.clone();
    // Repeating the same observations must not change the registry.
    registry.observe_model_metadata(
        &identity,
        &RuntimeModelMetadata {
            context_limit: Some(128_000),
            ..RuntimeModelMetadata::default()
        },
        5,
    );
    registry.observe_available(&identity, "reachable", 5);
    assert_eq!(first, registry);

    // A health-only observation must not erase the context limit.
    registry.observe_available(&identity, "still reachable", 7);
    let record = registry.resource(&ResourceId::derive(&identity)).unwrap();
    assert_eq!(record.context_limit.value, Some(128_000));
    assert_eq!(record.health.observed_at, Some(7));
}

// -- RuntimeAdapter normalization --------------------------------------------

#[test]
fn adapter_capabilities_are_normalized_and_v1_v2_distinct() {
    let v2 = FakeAdapter {
        runtime: v2_runtime(),
        capabilities: RuntimeCapabilities::OPENCODE_V2,
    };
    let v1 = FakeAdapter {
        runtime: RuntimeIdentity::new("opencode", "v1", "invocation"),
        capabilities: RuntimeCapabilities::NONE,
    };
    let mut registry = ResourceRegistry::new(1);
    let v2_id = registry.observe_adapter(&v2, 5);
    let v1_id = registry.observe_adapter(&v1, 5);
    assert_ne!(v2_id, v1_id, "V1 and V2 are distinct resources");

    let v2_record = registry.resource(&v2_id).unwrap();
    let facts = v2_record.capabilities.value.unwrap();
    assert!(facts.recover_execution && facts.observe_context && facts.resume_continuation);
    assert_eq!(
        v2_record.capabilities.provenance,
        ResourceProvenance::RuntimeReported
    );
    assert_eq!(v2_record.runtime.value.unwrap().family, "v2");

    let v1_facts = registry
        .resource(&v1_id)
        .unwrap()
        .capabilities
        .value
        .unwrap();
    assert_eq!(v1_facts, CapabilityFacts::default());
    assert!(v1_facts.listed().is_empty());
}

// -- configured derivation ----------------------------------------------------

#[test]
fn configured_entries_derive_from_config_and_attach_to_the_runtime() {
    let project = TestDir::new();
    write_yaml(&project.path().join(".ocg.yaml"), &project_profile());
    let effective = load_embedded_effective(project.path());
    let without_runtime = configured_entries(&effective.data, None).unwrap();
    assert!(!without_runtime.is_empty());
    // The Lead is the selected Profile model, not a fixed tier.
    assert!(without_runtime
        .iter()
        .any(|entry| entry.configured.role.as_deref() == Some("lead")));

    let runtime = v2_runtime();
    let with_runtime = configured_entries(&effective.data, Some(&runtime)).unwrap();
    let plain = without_runtime
        .iter()
        .find(|entry| entry.configured.role.as_deref() == Some("lead"))
        .unwrap();
    let bound = with_runtime
        .iter()
        .find(|entry| entry.configured.role.as_deref() == Some("lead"))
        .unwrap();
    assert_ne!(
        ResourceId::derive(&plain.identity),
        ResourceId::derive(&bound.identity),
        "the runtime family participates in the resource identity"
    );

    // The selected Lead is one configured use; each explicit worker route is
    // another. Two tiers on one model are no longer a concept.
    let mut registry = ResourceRegistry::new(1);
    for entry in &with_runtime {
        registry.register_configured(&entry.identity, entry.configured.clone(), 1);
    }
    let lead_model = bound.identity.model.clone().unwrap();
    let resource = registry
        .list()
        .into_iter()
        .find(|record| record.identity.model.as_deref() == Some(lead_model.as_str()))
        .unwrap();
    assert_eq!(
        resource.configured,
        vec![ConfiguredResource {
            role: Some("lead".into()),
            agent: Some("lead".into()),
            variant: None
        }]
    );
}

// -- persistence and corruption ----------------------------------------------

#[test]
fn persistence_round_trips_dynamic_facts_without_static_duplication() {
    let dir = TestDir::new();
    let project = dir.path();
    let identity = model_identity("openai", "gpt-5.6-sol");
    let mut registry = ResourceRegistry::new(1);
    registry.register_configured(
        &identity,
        ConfiguredResource {
            role: Some("low".to_string()),
            agent: Some("lead-low".to_string()),
            variant: Some("low".to_string()),
        },
        1,
    );
    registry.observe_available(&identity, "reachable", 20);
    let path = save(project, &registry).unwrap();
    assert_eq!(path, registry_path(project));
    assert!(path.is_file());

    let text = fs::read_to_string(&path).unwrap();
    assert!(
        !text.contains("lead-low"),
        "configured facts must not be persisted as durable truth"
    );
    let value: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(value["schema_version"], json!(RESOURCE_SCHEMA_VERSION));

    let loaded = load(project);
    assert!(!loaded.corrupt && loaded.exists);
    assert!(loaded.issues.is_empty());
    let record = loaded
        .registry
        .resource(&ResourceId::derive(&identity))
        .unwrap();
    assert_eq!(record.health.state, ResourceHealth::Available);
    assert_eq!(record.updated_at, 20);
    assert!(
        record.configured.is_empty(),
        "configured facts are re-derived, not loaded"
    );
}

#[test]
fn a_corrupt_record_is_isolated_and_reported() {
    let dir = TestDir::new();
    let project = dir.path();
    let good = model_identity("openai", "gpt-5.6-sol");
    let bad = model_identity("opencode-go", "glm-5.3-flash");
    let mut registry = ResourceRegistry::new(1);
    registry.observe_available(&good, "reachable", 5);
    registry.observe_available(&bad, "reachable", 5);
    let path = save(project, &registry).unwrap();

    // Corrupt exactly one record, leaving the other intact.
    let mut value: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
    let key = ResourceId::derive(&bad).as_str().to_string();
    value["resources"][&key]["health"]["state"] = json!("not_a_real_state");
    fs::write(&path, serde_json::to_string(&value).unwrap()).unwrap();

    // Drop the replay authority so the legacy projection scan is exercised.
    fs::remove_dir_all(ocg::orchestration::replay_dir(project)).unwrap();
    fs::remove_file(project.join(".ocg/orchestration/replay.initialized")).unwrap();
    let loaded = load(project);
    assert!(
        !loaded.corrupt,
        "one bad record must not poison the document"
    );
    assert_eq!(loaded.issues.len(), 1);
    assert!(loaded
        .registry
        .resource(&ResourceId::derive(&good))
        .is_some());
    assert!(
        loaded
            .registry
            .resource(&ResourceId::derive(&bad))
            .is_none(),
        "the corrupt resource is surfaced, never silently reset"
    );
}

#[test]
fn whole_file_corruption_is_reported_and_never_treated_as_absent() {
    let dir = TestDir::new();
    let project = dir.path();
    let path = registry_path(project);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, "{ this is not json").unwrap();

    let loaded = load(project);
    assert!(loaded.exists && loaded.corrupt);
    assert!(!loaded.issues.is_empty());
    assert!(loaded.registry.is_empty());
}

#[test]
fn an_unsupported_schema_version_is_rejected() {
    let dir = TestDir::new();
    let project = dir.path();
    let path = registry_path(project);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(
        &path,
        serde_json::to_string(&json!({
            "schema_version": 999,
            "updated_at": 1,
            "resources": {},
        }))
        .unwrap(),
    )
    .unwrap();

    let loaded = load(project);
    assert!(loaded.corrupt);
    assert!(loaded.registry.is_empty());
}

#[test]
fn no_secret_reaches_the_registry_file() {
    let dir = TestDir::new();
    let project = dir.path();
    let identity = model_identity("openai", "gpt-5.6-sol");
    let secret = format!("{}{}", "sk-", "abcdefghijklmnopqrstuvwx");
    let mut registry = ResourceRegistry::new(1);
    registry.observe_health(
        &identity,
        HealthFacts {
            state: ResourceHealth::Unavailable,
            reason: Some(format!("token {secret} rejected")),
            provenance: ResourceProvenance::RuntimeObserved,
            observed_at: Some(3),
        },
        3,
    );
    let path = save(project, &registry).unwrap();
    let text = fs::read_to_string(&path).unwrap();
    assert!(
        !text.contains(&secret),
        "credential-shaped text must be redacted before it is written"
    );
    assert!(!text.contains("v2_server_password"));
}

// -- Reconciler bridge --------------------------------------------------------

#[test]
fn reconciler_observations_publish_without_changing_decisions() {
    let identity = model_identity("openai", "gpt-5.6-sol");
    let run = ReconcileRun {
        results: vec![
            reconcile_result("m1", ObservationStatus::Exists, ReconcileOutcome::Noop),
            reconcile_result(
                "m2",
                ObservationStatus::TransientTransportFailure,
                ReconcileOutcome::Deferred,
            ),
            reconcile_result("m3", ObservationStatus::Unbound, ReconcileOutcome::Noop),
        ],
        issues: Vec::new(),
    };
    let snapshot = run.clone();

    let mut registry = ResourceRegistry::new(1);
    publish_resource_observations(&run, &mut registry, &identity, 9);
    // The bridge is read-only over the run: decisions and receipts are intact.
    assert_eq!(run, snapshot);
    let record = registry.resource(&ResourceId::derive(&identity)).unwrap();
    assert_eq!(
        record.health.state,
        ResourceHealth::Degraded,
        "the most severe observation in the pass wins"
    );

    // All-successful passes report Available.
    let ok = ReconcileRun {
        results: vec![reconcile_result(
            "m1",
            ObservationStatus::Exists,
            ReconcileOutcome::Noop,
        )],
        issues: Vec::new(),
    };
    let mut registry = ResourceRegistry::new(1);
    publish_resource_observations(&ok, &mut registry, &identity, 10);
    assert_eq!(
        registry
            .resource(&ResourceId::derive(&identity))
            .unwrap()
            .health
            .state,
        ResourceHealth::Available
    );

    // A pass that observed nothing about reachability publishes nothing.
    let empty = ReconcileRun {
        results: vec![reconcile_result(
            "m1",
            ObservationStatus::Unbound,
            ReconcileOutcome::Noop,
        )],
        issues: Vec::new(),
    };
    let mut registry = ResourceRegistry::new(1);
    publish_resource_observations(&empty, &mut registry, &identity, 11);
    assert!(registry.is_empty());
}

fn reconcile_result(
    mission_id: &str,
    observed: ObservationStatus,
    outcome: ReconcileOutcome,
) -> ReconcileResult {
    ReconcileResult {
        mission_id: mission_id.to_string(),
        generation: 1,
        current_execution_id: Some(RuntimeExecutionId::new("ses-1")),
        observed,
        decision: ReconcileAction::Noop,
        reason: "test".to_string(),
        result: outcome,
        timestamp: 1,
        policy: None,
        budget: None,
    }
}

// -- CLI surface --------------------------------------------------------------

fn init_project(dir: &TestDir) -> std::path::PathBuf {
    let project = dir.project();
    // `ocg resources` is a read-only surface: it requires a Profile.
    write_yaml(&project.join(".ocg.yaml"), &project_profile());
    project
}

#[test]
fn resources_cli_json_is_stable_and_redacted_without_writing() {
    let dir = TestDir::new();
    let project = init_project(&dir);
    let output = run(&project, dir.path(), &["resources", "--json"]);
    assert!(output.status.success(), "ocg resources --json failed");
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["schema_version"], json!(RESOURCE_SCHEMA_VERSION));
    assert!(!value["resources"].as_array().unwrap().is_empty());
    let serialized = String::from_utf8_lossy(&output.stdout);
    assert!(!serialized.contains("sk-"));
    assert!(!serialized.contains("password"));

    // Read-only: nothing was persisted.
    assert!(
        !registry_path(&project).exists(),
        "`ocg resources` must not write the registry"
    );

    let named = run(&project, dir.path(), &["resources"]);
    assert!(named.status.success());
    let text = String::from_utf8_lossy(&named.stdout);
    assert!(text.contains("identity"));
    assert!(text.contains("health"));
    assert!(text.contains("unknown"));
}

#[test]
fn resources_cli_rejects_unknown_options() {
    let dir = TestDir::new();
    let project = init_project(&dir);
    let output = run(&project, dir.path(), &["resources", "--nope"]);
    assert!(!output.status.success());
}

#[cfg(unix)]
#[test]
fn resources_cli_honors_previously_persisted_observations() {
    let dir = TestDir::new();
    let project = init_project(&dir);
    let effective = load_embedded_effective(&project);
    let runtime = v2_runtime();
    let entries = configured_entries(&effective.data, Some(&runtime)).unwrap();
    let target = entries
        .iter()
        .find(|entry| entry.configured.role.as_deref() == Some("lead"))
        .unwrap();

    let mut registry = ResourceRegistry::new(1);
    registry.observe_health(
        &target.identity,
        HealthFacts {
            state: ResourceHealth::Unavailable,
            reason: Some("seeded failure".to_string()),
            provenance: ResourceProvenance::RuntimeObserved,
            observed_at: Some(42),
        },
        42,
    );
    save(&project, &registry).unwrap();

    // Pin an explicit V2 runtime so the CLI re-derives the same runtime-scoped
    // identity the test seeded, independent of the host's own OpenCode.
    let runtime_bin = fake_v2_runtime(&dir);
    let output = run_with_runtime(
        &project,
        dir.path(),
        &["resources", "--json"],
        Some(&runtime_bin),
    );
    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let records = value["resources"].as_array().unwrap();
    let id = ResourceId::derive(&target.identity);
    let record = records
        .iter()
        .find(|record| record["resource_id"] == json!(id.as_str()))
        .expect("persisted resource is listed");
    assert_eq!(record["health"]["state"], json!("unavailable"));
    assert_eq!(record["health"]["provenance"], json!("runtime_observed"));
    // The configured use was re-derived and attached after the reload.
    assert_eq!(
        record["configured"][0]["role"],
        json!("lead"),
        "configured facts are re-derived on load"
    );
}

#[test]
fn doctor_effective_summarizes_resource_counts() {
    let dir = TestDir::new();
    let project = init_project(&dir);
    let output = run(&project, dir.path(), &["doctor", "--effective"]);
    // The runtime may or may not be observable in CI, but the summary line must
    // never be a record dump and the command must succeed.
    assert!(output.status.success());
    let text = String::from_utf8_lossy(&output.stdout);
    if text.contains("resources") {
        assert!(!text.contains("resource_id"));
    }
}

#[test]
fn catalogue_evidence_is_reported_and_unknown_stays_unknown() {
    let identity = model_identity("volcengine-coding-plan", "kimi-k3");
    let mut registry = ResourceRegistry::new(1);
    registry.observe_resolved(
        &identity,
        CatalogueEvidence::Available,
        ResourceProvenance::ProviderReported,
        4,
    );
    let record = registry.resource(&ResourceId::derive(&identity)).unwrap();
    assert_eq!(record.resolved.value, Some(CatalogueEvidence::Available));
    assert!(record.context_limit.value.is_none());
    assert!(record.capacity.value.is_none());
}
