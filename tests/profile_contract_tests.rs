//! Selected OCG Profile resources, not shipped execution tiers, define Lead
//! contracts and OpenCode compatibility configuration.
mod common;

use common::{load_embedded_effective, write_yaml, TestDir};
use ocg::{build, model, profile::Profile, validate};
use serde_json::json;

fn configured() -> (TestDir, ocg::config::Effective) {
    let dir = TestDir::new();
    let project = dir.project();
    write_yaml(
        &project.join(".ocg.yaml"),
        &json!({
            "profile": { "origin": "new", "defaultModel": "selection-0" },
            "models": { "providers": { "compat": {"label": "Compat", "placeholder": false} },
                "models": (0..17).map(|index| (format!("selection-{index}"),
                    json!({"provider": "compat", "id": format!("model-{index}"),
                           "placeholder": false}))).collect::<serde_json::Map<_, _>>() }
        }),
    );
    let effective = load_embedded_effective(&project);
    (dir, effective)
}

#[test]
fn shipped_defaults_have_no_authoritative_execution_model() {
    let dir = TestDir::new();
    let effective = load_embedded_effective(&dir.project());
    assert!(effective.data.get("throttle").is_none());
    assert!(Profile::from_ocg_config(&effective.data).is_err());
    assert!(build::build_opencode_config(&effective, "selection-0").is_err());
}

#[test]
fn selected_resource_contract_and_compatibility_config_are_open_ended() {
    let (_dir, effective) = configured();
    assert!(validate::validate(&effective).is_empty());
    let profile = Profile::from_ocg_config(&effective.data).unwrap();
    assert_eq!(profile.models.len(), 17);
    assert_eq!(profile.select(None).unwrap().0, "selection-0");
    for index in [0, 1, 9, 16, 0] {
        let choice = format!("selection-{index}");
        let full = format!("compat/model-{index}");
        let contract = model::lead_contract(&effective.data, &choice).unwrap();
        assert_eq!(contract.full_model_id(), full);
        assert_eq!(contract.variant, None);
        let compatibility = build::build_opencode_config(&effective, &choice).unwrap();
        assert_eq!(compatibility["model"], json!(full));
        assert_eq!(compatibility["agent"]["lead"]["model"], json!(full));
        assert!(compatibility["agent"]["lead"].get("variant").is_none());
        assert!(compatibility["agent"]
            .as_object()
            .unwrap()
            .keys()
            .all(|key| !key.starts_with("lead-")));
        assert_eq!(compatibility["default_agent"], json!("lead"));
    }
    assert!(build::build_opencode_config(&effective, "selection-17").is_err());
}

#[test]
fn legacy_throttle_values_cannot_change_the_selected_contract() {
    let (dir, effective) = configured();
    let path = dir.project().join(".ocg.yaml");
    let mut config = common::read_yaml(&path);
    config["throttle"] = json!({"default": "high", "levels": {"high": {"model": "unowned"}}});
    write_yaml(&path, &config);
    let reloaded = load_embedded_effective(&dir.project());
    assert_eq!(
        model::lead_contract(&effective.data, "selection-0").unwrap(),
        model::lead_contract(&reloaded.data, "selection-0").unwrap()
    );
    assert!(reloaded
        .diagnostics
        .iter()
        .any(|line| line.contains("no automatic conversion")));
    assert!(reloaded.data.get("throttle").is_none());
}
