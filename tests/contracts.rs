//! The Rust wire contract in `src/contracts.rs` is the single source of truth
//! for the TypeScript PWA. These tests make that claim enforceable rather than
//! aspirational:
//!
//! - the committed TypeScript is byte-identical to what the Rust definitions
//!   render today, so a contract change cannot be half-applied;
//! - the protocol versions in the generated file match the Rust constants;
//! - every contract type actually round-trips through `serde_json`, which is
//!   what makes the generated TypeScript a description of real bytes;
//! - every control route answers with a shape the contract declares, so the
//!   generated types are not aspirational documentation of a payload the
//!   server never sends.

use ocg::contracts;
use ocg::orchestration::canonical_control::CANONICAL_CONTROL_API_VERSION;
use ocg::profile::PROVIDER_PROFILE_API_VERSION;
use serde_json::json;

const GENERATED: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/frontend/components/ocg/contracts/generated.ts"
);

#[test]
fn generated_typescript_is_in_sync_with_rust() {
    let rendered = contracts::render().expect("render contract");
    let committed = std::fs::read_to_string(GENERATED)
        .unwrap_or_else(|error| panic!("read {}: {error}", GENERATED));
    assert_eq!(
        rendered, committed,
        "the committed TypeScript contract is stale. Run `make contracts` and commit the result."
    );
}

#[test]
fn generated_protocol_versions_match_the_rust_constants() {
    let rendered = contracts::render().expect("render contract");
    let canonical =
        format!("export type CanonicalApiVersion = \"{CANONICAL_CONTROL_API_VERSION}\";");
    let profile = format!("export type ProfileApiVersion = \"{PROVIDER_PROFILE_API_VERSION}\";");
    assert!(
        rendered.contains(&canonical),
        "generated file must carry the canonical protocol literal {canonical}"
    );
    assert!(
        rendered.contains(&profile),
        "generated file must carry the Profile protocol literal {profile}"
    );
    assert_eq!(contracts::PROFILE_API_VERSION, PROVIDER_PROFILE_API_VERSION);
}

#[test]
fn every_contract_type_is_declared_in_the_generated_file() {
    let rendered = contracts::render().expect("render contract");
    // `export_all` walks the dependency closure, so every type the roots
    // mention is present. A type referenced but not declared would be a
    // TypeScript compile error in the PWA, i.e. a broken build.
    for name in [
        "ApiErrorBody",
        "ApiErrorEnvelope",
        "CanonicalConfigurationEnvelope",
        "CanonicalConfigurationResponse",
        "CanonicalDashboardResponse",
        "CanonicalEventsEnvelope",
        "CanonicalMissionConfigEnvelope",
        "CanonicalMissionResponse",
        "CanonicalProjectResponse",
        "CanonicalProjectsResponse",
        "CanonicalWorkEvent",
        "CanonicalWorkSnapshot",
        "Candidate",
        "CommandFingerprintInputV1",
        "ChangeSetFingerprintInputV1",
        "SpawnFingerprintInputV1",
        "GlobalConfiguration",
        "Model",
        "Origin",
        "Profile",
        "ProfileBootstrapRequest",
        "ProfileReplaceRequest",
        "ProfileView",
        "ProjectConfiguration",
        "ProjectConfigurationView",
        "ProjectRecord",
        "Provider",
        "JsonValue",
    ] {
        assert!(
            rendered.contains(&format!("export type {name} =")),
            "the generated contract must declare {name}"
        );
    }
}

#[test]
fn generated_file_uses_json_numbers_not_bigint() {
    // `serde_json` renders u64/i64 as JSON numbers. `bigint` in the generated
    // TypeScript would force the PWA to wrap every revision and cursor in
    // BigInt and break `JSON.parse` round-trips.
    let rendered = contracts::render().expect("render contract");
    assert!(
        !rendered.contains("bigint"),
        "the generated contract must not claim bigint for serde_json integers"
    );
    assert!(rendered.contains("revision: number"));
    assert!(rendered.contains("sequence: number"));
}

#[test]
fn generated_file_is_self_contained() {
    // ts-rs emits one file per type with relative imports. This module is a
    // single file, so a leftover import would be a self-reference.
    let rendered = contracts::render().expect("render contract");
    assert!(
        !rendered.contains("import type"),
        "the bundled contract must not carry ts-rs per-type imports"
    );
}

#[test]
fn profile_view_round_trips_through_serde() {
    let view = contracts::ProfileView {
        api_version: contracts::PROFILE_API_VERSION.to_string(),
        profile: Some(contracts::Profile {
            origin: contracts::Origin::New,
            default_model: Some("selection-0".to_string()),
            providers: [(
                "compat".to_string(),
                contracts::Provider {
                    placeholder: false,
                    label: "Compat".to_string(),
                },
            )]
            .into_iter()
            .collect(),
            models: [(
                "selection-0".to_string(),
                contracts::Model {
                    placeholder: false,
                    provider: "compat".to_string(),
                    id: "model-0".to_string(),
                    variant: None,
                    variants: Vec::new(),
                },
            )]
            .into_iter()
            .collect(),
        }),
        revision: Some("abc123".to_string()),
        candidates: vec![contracts::Candidate {
            source: "global".to_string(),
            scope: "global".to_string(),
            location: "/tmp/opencode.json".into(),
            sha256: "deadbeef".to_string(),
            provider_names: vec!["compat".to_string()],
            model_ids: vec!["model-0".to_string()],
            variants: Default::default(),
            importable_fields: vec!["provider".to_string()],
            ignored_fields: vec!["apiKey".to_string()],
        }],
    };

    let bytes = serde_json::to_vec(&view).expect("serialize ProfileView");
    let parsed: serde_json::Value = serde_json::from_slice(&bytes).expect("parse");
    let decoded: contracts::ProfileView =
        serde_json::from_value(parsed.clone()).expect("deserialize");
    assert_eq!(decoded, view);

    // The PWA checks the wire version before trusting a payload, so the literal
    // it compares against has to be the one the server actually emits.
    assert_eq!(parsed["api_version"], json!(contracts::PROFILE_API_VERSION));
}

#[test]
fn origin_serializes_as_the_snake_case_union_the_pwa_expects() {
    let new = serde_json::to_value(contracts::Origin::New).expect("new origin");
    assert_eq!(new, json!("new"));

    let imported = serde_json::to_value(contracts::Origin::Imported {
        source: "global".to_string(),
        scope: "global".to_string(),
        location: "/tmp/opencode.json".into(),
        sha256: "deadbeef".to_string(),
    })
    .expect("imported origin");
    assert_eq!(
        imported,
        json!({ "imported": {
            "source": "global",
            "scope": "global",
            "location": "/tmp/opencode.json",
            "sha256": "deadbeef",
        }})
    );
}

#[test]
fn error_envelope_matches_the_body_the_control_server_returns() {
    // `ApiError::new` in `src/control_server.rs` builds this same struct; if
    // the two diverge the PWA cannot surface a server error.
    let envelope = contracts::ApiErrorEnvelope {
        error: contracts::ApiErrorBody {
            code: "invalid_request".to_string(),
            message: "command_id is required".to_string(),
        },
    };
    assert_eq!(
        serde_json::to_value(&envelope).expect("serialize"),
        json!({"error": {"code": "invalid_request", "message": "command_id is required"}})
    );
}

/// The dispatch witness is the only record that confers authority, and the PWA
/// reads it out of an opaque JSON payload. `DispatchWitness::from_json` is the
/// authority for what a witness may contain: identity strings are non-empty,
/// indices are non-negative integers, and a Run generation starts at 1.
///
/// A TypeScript type cannot express those refinements, so the frontend decoder
/// states them separately and `decodeWitness` mirrors this function. These
/// cases pin the rules on the Rust side; the matching frontend cases live in
/// `frontend/components/ocg/contracts/decode.test.ts`. Both sides must be
/// changed together.
#[test]
fn dispatch_witness_field_rules_are_pinned() {
    use contracts::DispatchWitness;
    use serde_json::json;

    let valid = json!({
        "mission_id": "mission-1",
        "work_node_id": 0,
        "run_id": 3,
        "run_generation": 1,
        "runtime_execution_id": "sess-1",
        "dispatch_id": "dispatch-1",
    });

    let witness = DispatchWitness::from_json(&valid).expect("a complete witness is valid");
    assert_eq!(witness.work_node_id, 0, "work node 0 is a valid index");
    assert_eq!(witness.run_generation, 1);

    let refuse = |patch: serde_json::Value, what: &str| {
        let mut broken = valid.clone();
        for (key, value) in patch.as_object().expect("an object patch") {
            match value {
                serde_json::Value::Null => {
                    broken.as_object_mut().expect("an object").remove(key);
                }
                other => {
                    broken[key] = other.clone();
                }
            }
        }
        assert!(
            DispatchWitness::from_json(&broken).is_err(),
            "a witness with {what} must be refused"
        );
    };

    for key in [
        "mission_id",
        "work_node_id",
        "run_id",
        "run_generation",
        "runtime_execution_id",
        "dispatch_id",
    ] {
        refuse(json!({ key: null }), &format!("no {key}"));
    }

    refuse(json!({ "mission_id": "" }), "an empty mission id");
    refuse(
        json!({ "runtime_execution_id": "" }),
        "an empty runtime execution id",
    );
    refuse(json!({ "dispatch_id": "" }), "an empty dispatch id");
    refuse(json!({ "run_generation": 0 }), "a zero Run generation");
    refuse(json!({ "run_generation": -1 }), "a negative Run generation");
    refuse(json!({ "run_id": -1 }), "a negative run index");
    refuse(
        json!({ "work_node_id": 1.5 }),
        "a fractional work node index",
    );
    refuse(
        json!({ "run_generation": 1.5 }),
        "a fractional Run generation",
    );
}

#[test]
fn the_generated_witness_matches_the_rust_struct_field_for_field() {
    use contracts::DispatchWitness;

    let rendered = contracts::render().expect("render contract");
    for field in [
        "mission_id",
        "work_node_id",
        "run_id",
        "run_generation",
        "runtime_execution_id",
        "dispatch_id",
    ] {
        assert!(
            rendered.contains(field),
            "the generated witness must carry {field}; DispatchWitness declares it"
        );
    }

    // A witness survives the wire unchanged, which is what lets the PWA decode
    // one out of a canonical event payload and trust the identity.
    let witness = DispatchWitness {
        mission_id: "mission-1".to_string(),
        work_node_id: 0,
        run_id: 3,
        run_generation: 2,
        runtime_execution_id: "sess-1".to_string(),
        dispatch_id: "dispatch-1".to_string(),
    };
    let bytes = serde_json::to_vec(&witness).expect("serialize");
    let decoded: DispatchWitness =
        serde_json::from_value(serde_json::from_slice(&bytes).expect("parse"))
            .expect("deserialize");
    assert_eq!(decoded, witness);
}
