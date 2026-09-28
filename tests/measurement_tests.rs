//! Deterministic, offline measurement fixture for the local flows.
//!
//! This test builds a representative repository in a temporary directory and
//! measures the context engine (cold, warm, incremental) and log distillation.
//! Byte counts are deterministic and asserted; timings use `Instant` and are
//! only printed, never asserted. Run it with output to record a measurement:
//!
//! ```text
//! cargo test --test measurement_tests -- --nocapture
//! ```
//!
//! The fixture is intentionally small and uniform: it is one representative
//! sample on one machine, not a benchmark and not universal.

use ocg::capabilities::CapabilityConfig;
use ocg::clock::FixedClock;
use ocg::context::{ContextConfig, ContextEngine};
use ocg::orchestration::config::OrchestrationConfig;
use ocg::orchestration::controller::Controller;
use ocg::orchestration::handoff::Role;
use ocg::process::{FakeCaptureRunner, FakeGitHost};
use ocg::verification::config::VerificationConfig;
use ocg::verification::distill::distill;
use serde_json::json;
use std::fs;
use std::path::Path;
use std::time::Instant;

const MODULES: usize = 120;

fn write(path: &Path, content: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(path, content).unwrap();
}

fn rust_module(index: usize) -> String {
    format!(
        "// module {index}\npub fn parse_{index}() -> u32 {{\n    {index}\n}}\n\npub fn helper_{index}(value: u32) -> u32 {{\n    value + {index}\n}}\n"
    )
}

fn fixture(root: &Path) {
    for index in 0..MODULES {
        write(
            &root.join(format!("src/module_{index}.rs")),
            &rust_module(index),
        );
    }
    write(&root.join("README.md"), "# Measurement fixture\n");
    write(
        &root.join("Cargo.toml"),
        "[package]\nname = \"measurement-fixture\"\nversion = \"0.0.0\"\n",
    );
}

fn noisy_log() -> String {
    let mut out = String::new();
    for index in 0..400 {
        out.push_str(&format!(
            "Compiling dependency-{} v0.1.{}\n",
            index % 20,
            index % 5
        ));
    }
    for _ in 0..60 {
        out.push_str("warning: unused variable: `value`\n");
    }
    out.push_str("error[E0308]: mismatched types at src/module_1.rs:4:5\n");
    out.push_str("error[E0308]: mismatched types at src/module_1.rs:4:5\n");
    out.push_str("test result: FAILED. 3 passed; 1 failed; 0 ignored\n");
    out
}

fn capsule_bytes(plan: &ocg::context::ranking::ContextPlan) -> u64 {
    plan.capsule
        .as_ref()
        .and_then(|capsule| serde_json::to_vec(capsule).ok())
        .map(|bytes| bytes.len() as u64)
        .unwrap_or(0)
}

fn percent(reduced: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        ((reduced as f64) * 1000.0 / (total as f64)).round() / 10.0
    }
}

#[test]
fn measurement_context_and_log_distillation() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    fixture(root);

    let config = ContextConfig::default();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(1_000);
    let engine = ContextEngine::new(root, config, &git, &clock);
    let task = "update module_1 parser";

    // Cold: build the repo map, the symbol index and the first plan.
    let started = Instant::now();
    let cold = engine.plan(task, None).unwrap();
    let cold_ms = started.elapsed().as_secs_f64() * 1_000.0;
    assert!(
        !cold.from_cache,
        "the first plan must not come from the cache"
    );
    assert!(cold.plan.capsule.is_some(), "a task capsule must be built");
    assert!(cold.plan.candidate_bytes > 0);
    assert!(cold.plan.selected_bytes > 0);
    assert!(cold.plan.selected_bytes < cold.plan.candidate_bytes);

    // Warm: the identical plan is served from the dependency-checked cache.
    let started = Instant::now();
    let warm = engine.plan(task, None).unwrap();
    let warm_ms = started.elapsed().as_secs_f64() * 1_000.0;
    assert!(
        warm.from_cache,
        "an unchanged tree must hit the context cache"
    );
    assert_eq!(warm.plan.candidate_bytes, cold.plan.candidate_bytes);

    // Incremental: one selected file changes, so the index reuses the rest and
    // the cache is invalidated for exactly this plan.
    write(&root.join("src/module_1.rs"), &rust_module(9_999));
    let started = Instant::now();
    let incremental = engine.plan(task, None).unwrap();
    let incremental_ms = started.elapsed().as_secs_f64() * 1_000.0;
    assert!(
        !incremental.from_cache,
        "a changed dependency must miss the cache"
    );
    assert!(incremental.index_report.metrics.updated >= 1);
    assert!(incremental.index_report.metrics.reused >= 1);

    // Log distillation: a noisy raw log becomes a small deterministic summary.
    let noisy = noisy_log();
    let raw_bytes = noisy.len() as u64;
    let distilled = distill(&noisy, "", false);
    let distilled_bytes = serde_json::to_vec(&distilled).unwrap().len() as u64;
    assert!(
        distilled_bytes < raw_bytes,
        "distillation must reduce a noisy log"
    );

    let context_reduction = cold
        .plan
        .candidate_bytes
        .saturating_sub(cold.plan.selected_bytes) as u64;
    let context_reduction_percent = percent(context_reduction, cold.plan.candidate_bytes as u64);
    let log_reduction = raw_bytes.saturating_sub(distilled_bytes);
    let log_reduction_percent = percent(log_reduction, raw_bytes);

    // One machine-readable block; the documentation copies these exact values.
    println!("OCG_MEASUREMENT version=1 fixture_modules={MODULES}");
    println!(
        "context candidates={} selected={} capsule={} reduction={} reduction_percent={:.1}",
        cold.plan.candidate_bytes,
        cold.plan.selected_bytes,
        capsule_bytes(&cold.plan),
        context_reduction,
        context_reduction_percent
    );
    println!(
        "log raw={} distilled={} reduction={} reduction_percent={:.1}",
        raw_bytes, distilled_bytes, log_reduction, log_reduction_percent
    );
    println!("timing cold_ms={cold_ms:.3} warm_ms={warm_ms:.3} incremental_ms={incremental_ms:.3}");
    println!("OCG_MEASUREMENT_END");
}

/// Deterministic orchestration hand-off measurement.
///
/// Drives the same 120-module fixture through Explore → Build → Verify → Debug
/// with a fixed clock and fakes, and asserts that every hand-off capsule fits
/// the absolute cap and the ratio cap while keeping the required fields.
#[test]
fn measurement_orchestration_handoffs() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    fixture(root);

    let task = "update module_1 parse_1 parser";
    let verification = VerificationConfig::from_config(&json!({
        "verification": {
            "stages": {"normal": {"commands": [{"program": "cargo", "args": ["test"]}]}},
            "defaultStage": "normal"
        }
    }))
    .unwrap();
    let git = FakeGitHost::new();
    let clock = FixedClock::new(2_000);
    // The deterministic release fixture deliberately uses the tight
    // 4096-byte / 40% regression gate, not the runtime optimization envelope.
    let fixture_config = OrchestrationConfig {
        enabled: true,
        max_handoff_bytes: 4096,
        max_handoff_ratio_percent: 40,
        ..OrchestrationConfig::default()
    };
    let controller = Controller::new(
        root,
        fixture_config,
        ContextConfig::default(),
        CapabilityConfig::default(),
        verification,
        &git,
        &clock,
    );

    // Cold context plan for the candidate/selected byte figures.
    let engine = ContextEngine::new(root, ContextConfig::default(), &git, &clock);
    let plan = engine.plan(task, None).unwrap();
    let candidate_context_bytes = plan.plan.candidate_bytes as u64;
    let selected_source_bytes = plan.plan.selected_bytes as u64;
    assert!(candidate_context_bytes > selected_source_bytes);

    controller
        .prepare_lead_context("measurement", task)
        .unwrap();
    let explore = controller
        .prepare_handoff("measurement", Role::Explore, task)
        .unwrap();
    assert!(explore.metrics.model_dynamic_context_bytes > 0);

    let explore_result = json!({
        "goal": "make parse_1 safe",
        "constraints": ["never break the public API"],
        "findings": [
            {"summary": "parse_1 is recursive", "severity": "critical", "source": "src/module_1.rs"}
        ],
        "files": ["src/module_1.rs"],
        "symbols": [{"name": "parse_1"}]
    })
    .to_string();
    controller
        .consume_explore_result("measurement", &explore_result)
        .unwrap();

    let build = controller
        .prepare_handoff("measurement", Role::Build, task)
        .unwrap();
    let rich_capsule_bytes = build.metrics.rich_capsule_bytes;
    let explore_to_build_handoff_bytes = build.capsule.measured_bytes() as u64;
    let model_dynamic_context_bytes = build.metrics.model_dynamic_context_bytes;
    // Required fields survive.
    assert_eq!(build.capsule.goal.as_deref(), Some("make parse_1 safe"));
    assert!(build
        .capsule
        .hard_constraints
        .iter()
        .any(|constraint| constraint == "never break the public API"));
    assert!(build
        .capsule
        .symbols
        .iter()
        .any(|symbol| symbol.name == "parse_1"));
    assert!(build
        .capsule
        .findings
        .iter()
        .any(|finding| finding.severity == ocg::orchestration::handoff::Severity::Critical));

    // Build → Verify: run a failing verification and capture the distilled
    // verification block that is attached to the report.
    let runner = FakeCaptureRunner::new().with_failure(
        "cargo",
        &["test"],
        101,
        "error[E0308]: mismatched types\n --> src/module_1.rs:4:5\n",
    );
    let first = controller
        .after_build("measurement", &runner, None)
        .unwrap();
    assert!(
        matches!(
            &first.decision,
            ocg::orchestration::controller::BuildDecision::RetryBuild { .. }
        ),
        "expected a bounded Build retry"
    );
    // Build→Verify is an actual typed capsule, not just the raw verification
    // block.
    let verify_handoff = first
        .verify_handoff
        .as_ref()
        .expect("a completed verification must produce a Build→Verify capsule");
    let verification_bytes = verify_handoff.measured_bytes() as u64;
    let verify_rich_bytes = first.metrics.rich_capsule_bytes.max(1);
    // Exhaust the build budget to reach the Debug recommendation.
    let second = controller
        .after_build("measurement", &runner, None)
        .unwrap();
    assert!(matches!(
        &second.decision,
        ocg::orchestration::controller::BuildDecision::RetryBuild { .. }
    ));
    let third = controller
        .after_build("measurement", &runner, None)
        .unwrap();
    let (verify_to_debug_handoff_bytes, debug_rich_bytes, failing_location) = match third.decision {
        ocg::orchestration::controller::BuildDecision::Debug { handoff, .. } => (
            handoff.capsule.measured_bytes() as u64,
            handoff.metrics.rich_capsule_bytes.max(1),
            handoff
                .capsule
                .first_failing_location()
                .map(|location| location.display())
                .unwrap_or_default(),
        ),
        other => panic!("expected debug, got {other:?}"),
    };
    assert!(
        failing_location.contains("src/module_1.rs"),
        "failing location must survive: {failing_location}"
    );

    for (label, bytes, rich) in [
        (
            "explore_to_build",
            explore_to_build_handoff_bytes,
            rich_capsule_bytes,
        ),
        ("build_to_verify", verification_bytes, verify_rich_bytes),
        (
            "verify_to_debug",
            verify_to_debug_handoff_bytes,
            debug_rich_bytes,
        ),
    ] {
        assert!(bytes <= 4096, "{label} handoff {bytes} exceeds 4096");
        assert!(
            bytes <= rich * 40 / 100,
            "{label} handoff {bytes} exceeds 40% of rich {rich}"
        );
    }

    println!("OCG_ORCHESTRATION_MEASUREMENT version=1 fixture_modules={MODULES}");
    println!(
        "candidate_context_bytes={candidate_context_bytes} selected_source_bytes={selected_source_bytes} rich_capsule_bytes={rich_capsule_bytes}"
    );
    println!(
        "explore_to_build_handoff_bytes={explore_to_build_handoff_bytes} build_to_verify_handoff_bytes={verification_bytes} verify_to_debug_handoff_bytes={verify_to_debug_handoff_bytes} model_dynamic_context_bytes={model_dynamic_context_bytes}"
    );
    println!("OCG_ORCHESTRATION_MEASUREMENT_END");
}
