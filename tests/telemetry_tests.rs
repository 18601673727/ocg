//! Library-level telemetry tests: token provenance, aggregation, corruption
//! recovery and secret hygiene. Deterministic and offline.

use ocg::telemetry::task::{ContextMetrics, LogMetrics, RepoMetrics};
use ocg::telemetry::{
    Aggregate, Event, Outcome, TelemetryConfig, TelemetryStats, TelemetryStore, TokenCount,
    TokenSource,
};
use serde_json::json;
use std::fs;

fn enabled() -> TelemetryConfig {
    TelemetryConfig::from_config(&json!({"telemetry": {"enabled": true}})).unwrap()
}

fn disabled() -> TelemetryConfig {
    TelemetryConfig::from_config(&json!({"telemetry": {"enabled": false}})).unwrap()
}

fn fake_secret() -> String {
    // Assembled at runtime so the repository never contains a key shape.
    format!("{}{}", concat!("sk", "-"), "Z".repeat(48))
}

#[test]
fn disabled_config_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let store = TelemetryStore::new(dir.path(), disabled());
    assert!(!store.append(&Event::new("task-1", 1)).unwrap());
    assert!(!dir.path().join(".ocg").exists());
    assert!(!store.exists());
}

#[test]
fn reported_and_estimated_tokens_stay_distinct_through_aggregation() {
    let dir = tempfile::tempdir().unwrap();
    let store = TelemetryStore::new(dir.path(), enabled());

    let mut provider = Event::new("task-provider", 1);
    provider.input_tokens = TokenCount::provider_reported(1_000);
    provider.output_tokens = TokenCount::opencode_reported(200);
    store.append(&provider).unwrap();

    let mut estimated = Event::new("task-estimated", 2);
    estimated.input_tokens = TokenCount::estimated(64);
    store.append(&estimated).unwrap();

    let stats = TelemetryStats::collect(&store);
    assert_eq!(stats.aggregate.input_tokens.provider_reported, Some(1_000));
    assert_eq!(stats.aggregate.input_tokens.estimated, Some(64));
    assert_eq!(stats.aggregate.output_tokens.opencode_reported, Some(200));
    // One event recorded no output count; that stays explicit.
    assert_eq!(stats.aggregate.output_tokens.unknown_events, 1);
    assert_eq!(stats.aggregate.events, 2);

    let text = stats.render();
    assert!(text.contains("provider_reported: 1000 (exact)"), "{text}");
    assert!(
        text.contains("estimated:         64 (estimate only)"),
        "{text}"
    );
    assert!(!text.contains("64 (exact)"), "{text}");
}

#[test]
fn cache_hit_and_log_accounting_is_only_counted_when_applicable() {
    let dir = tempfile::tempdir().unwrap();
    let store = TelemetryStore::new(dir.path(), enabled());

    let mut hit = Event::new("task-hit", 1);
    hit.context = ContextMetrics::new(10_000, 2_500, 800);
    hit.repo = RepoMetrics {
        files: 40,
        symbols: 300,
        index_reused: 39,
        index_updated: 1,
        cache_hit: Some(true),
    };
    hit.logs = LogMetrics::new(50_000, 1_000);
    store.append(&hit).unwrap();

    // A verification-style event did not look at the context cache, so it must
    // not be counted as a miss.
    let mut verify = Event::new("task-verify", 2);
    verify.outcome = Outcome::Success;
    store.append(&verify).unwrap();

    let aggregate = Aggregate::from_events(&store.read().events);
    assert_eq!(aggregate.repo.cache_hits, 1);
    assert_eq!(aggregate.repo.cache_misses, 0);
    assert_eq!(aggregate.context.candidate_bytes, 10_000);
    assert_eq!(aggregate.context.reduction_bytes, 7_500);
    assert_eq!(aggregate.logs.raw_bytes, 50_000);
    assert_eq!(aggregate.logs.reduction_bytes, 49_000);
    assert_eq!(aggregate.logs.reduction_percent(), 98.0);
}

#[test]
fn secret_like_metadata_is_sanitized_before_it_reaches_the_store() {
    let dir = tempfile::tempdir().unwrap();
    let store = TelemetryStore::new(dir.path(), enabled());
    let secret = fake_secret();
    let mut event = Event::new("task-safe", 1);
    event.role = Some(secret.clone());
    event.provider = Some(secret.clone());
    event.model = Some(format!("primary:{secret}"));
    event.capabilities = vec![secret.clone(), "filesystem".to_string()];
    event.verification.stage = Some(secret.clone());
    store.append(&event).unwrap();

    let raw = fs::read_to_string(store.path()).unwrap();
    assert!(!raw.contains(&secret), "{raw}");
    assert!(raw.contains("[redacted]"), "{raw}");
    // The safe metadata is untouched.
    assert!(raw.contains("filesystem"), "{raw}");
    // The event still round-trips.
    let log = store.read();
    assert_eq!(log.events.len(), 1);
    assert_eq!(log.events[0].capabilities, vec!["[redacted]", "filesystem"]);
}

#[test]
fn raw_prompts_never_become_task_ids() {
    let dir = tempfile::tempdir().unwrap();
    let store = TelemetryStore::new(dir.path(), enabled());
    let prompt = "SECRET-PROMPT-TEXT fix the private parser for customer X";
    let mut event = Event::new(Event::hashed_task_id(prompt), 1);
    event.task_type = Some("context".to_string());
    store.append(&event).unwrap();

    let raw = fs::read_to_string(store.path()).unwrap();
    assert!(!raw.contains("SECRET-PROMPT-TEXT"), "{raw}");
    assert!(!raw.contains("customer X"), "{raw}");
    assert!(raw.contains("task-"), "{raw}");
}

#[test]
fn corrupt_jsonl_is_skipped_counted_and_does_not_block_later_writes() {
    let dir = tempfile::tempdir().unwrap();
    let store = TelemetryStore::new(dir.path(), enabled());
    store.append(&Event::new("task-before", 1)).unwrap();
    fs::write(
        store.path(),
        format!(
            "{}\nnot json\n{{\"schema_version\":1,\"task_id\":\"truncated\"\n",
            serde_json::to_string(&Event::new("task-existing", 2)).unwrap()
        ),
    )
    .unwrap();
    fs::create_dir_all(store.dir()).unwrap();
    // Appending after corruption still works.
    store.append(&Event::new("task-after", 3)).unwrap();

    let log = store.read();
    assert_eq!(log.events.len(), 2);
    assert_eq!(log.corrupt_lines, 2);
    assert_eq!(log.events[0].task_id, "task-existing");
    assert_eq!(log.events[1].task_id, "task-after");

    let stats = TelemetryStats::collect(&store);
    assert_eq!(stats.corrupt_lines, 2);
    assert!(stats.render().contains("corrupt:     2 line(s) skipped"));
}

#[test]
fn missing_store_reports_no_data_without_creating_state() {
    let dir = tempfile::tempdir().unwrap();
    let store = TelemetryStore::new(dir.path(), enabled());
    let stats = TelemetryStats::collect(&store);
    assert_eq!(stats.events, 0);
    assert!(!dir.path().join(".ocg").exists());
    assert!(stats.render().contains("no telemetry events recorded yet"));
}

#[test]
fn token_source_labels_are_stable() {
    assert_eq!(TokenSource::ProviderReported.label(), "provider_reported");
    assert_eq!(TokenSource::OpencodeReported.label(), "opencode_reported");
    assert_eq!(TokenSource::Estimated.label(), "estimated");
    assert_eq!(TokenSource::Unknown.label(), "unknown");
    assert!(TokenSource::ProviderReported.is_exact());
    assert!(!TokenSource::Estimated.is_exact());
}

#[test]
fn older_events_without_orchestration_still_parse() {
    // The orchestration block was added after schema version 1 was released.
    // An old line must keep parsing, with the block defaulted.
    let dir = tempfile::tempdir().unwrap();
    let store = TelemetryStore::new(dir.path(), enabled());
    fs::create_dir_all(store.dir()).unwrap();
    let old = r#"{"schema_version":1,"timestamp":5,"task_id":"task-old","task_type":"context","outcome":"success"}"#;
    fs::write(store.path(), format!("{old}\n")).unwrap();
    let log = store.read();
    assert_eq!(log.events.len(), 1);
    assert_eq!(log.corrupt_lines, 0);
    assert_eq!(log.events[0].orchestration, Default::default());
    assert_eq!(log.events[0].task_id, "task-old");
}

#[test]
fn orchestration_metrics_aggregate_and_render() {
    let dir = tempfile::tempdir().unwrap();
    let store = TelemetryStore::new(dir.path(), enabled());
    let mut event = Event::new("task-orch", 1);
    event.task_type = Some("orchestration".to_string());
    event.outcome = Outcome::Success;
    event.orchestration.phase = Some("build".to_string());
    event.orchestration.source = Some("lead".to_string());
    event.orchestration.destination = Some("build".to_string());
    event.orchestration.attempt = Some(2);
    event.orchestration.rich_capsule_bytes = 20_000;
    event.orchestration.handoff_capsule_bytes = 2_400;
    event.orchestration.selected_source_bytes = 1_700;
    event.orchestration.diff_context_bytes = 120;
    event.orchestration.verification_context_bytes = 335;
    event.orchestration.model_dynamic_context_bytes = 4_000;
    event.orchestration.cache_hits = 1;
    event.orchestration.index_hits = 118;
    store.append(&event).unwrap();

    let stats = TelemetryStats::collect(&store);
    let orchestration = &stats.aggregate.orchestration;
    assert_eq!(orchestration.transitions, 1);
    assert_eq!(orchestration.build_attempts, 1);
    assert_eq!(orchestration.retries, 1);
    assert_eq!(orchestration.rich_capsule_bytes, 20_000);
    assert_eq!(orchestration.handoff_capsule_bytes, 2_400);
    assert_eq!(orchestration.model_dynamic_context_bytes, 4_000);
    assert_eq!(orchestration.cache_hits, 1);
    assert_eq!(orchestration.index_hits, 118);
    assert_eq!(orchestration.phases.get("build"), Some(&1));
    let text = stats.render();
    assert!(text.contains("orchestration"), "{text}");
    assert!(text.contains("rich capsule:   20000"), "{text}");
    assert!(text.contains("phase:       build"), "{text}");
}
