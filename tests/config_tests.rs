//! Library-level tests for the configuration pipeline (parity with the
//! historical Python suite, expressed against the Rust API).

mod common;

use common::*;
use ocg::config::{build_effective, Effective};
use ocg::defaults::{load_defaults, OcgSource};
use ocg::{build, model, observability, prompt, validate};
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};

fn setup() -> (TestDir, PathBuf, PathBuf) {
    let dir = TestDir::new();
    let home = ocg_home(&dir);
    let project = dir.project();
    (dir, home, project)
}

fn disk(home: &Path, project: &Path) -> Effective {
    seeded_disk(home, project)
}

fn write_project(project: &Path, value: serde_json::Value) {
    write_project_yaml(&project.join(".ocg.yaml"), value);
}

fn lead_prompt(home: &Path) -> String {
    fs::read_to_string(home.join("config").join("prompts").join("lead.md")).expect("lead prompt")
}

/// Worker routing must not follow the Lead selection: the same role keeps its
/// configured model for every selected Profile resource.
#[test]
fn worker_routing_is_independent_of_the_lead_selection() {
    let (_dir, home, project) = setup();
    let effective = disk(&home, &project);
    let worker_model = |choice: &str, role: &str| {
        let config = build::build_opencode_config(&effective, choice).expect("build");
        config["agent"][model::worker_agent_id(role)]["model"]
            .as_str()
            .expect("model")
            .to_string()
    };
    for role in model::role_specs(&effective.data).unwrap().keys() {
        let baseline = worker_model("gpt-5.6-sol", role);
        for choice in ["gpt-6-astra", "kimi-k3", "gpt-5.6-sol"] {
            assert_eq!(
                worker_model(choice, role),
                baseline,
                "{role} changed when the Lead selection changed to {choice}"
            );
        }
    }
}

#[test]
fn shipped_defaults_have_no_execution_resources() {
    let (_dir, home, project) = setup();
    // Shipped defaults alone: no tier, no provider, no model, no route.
    let effective = load_disk_effective(&home, &project, None, None);
    assert!(effective.data.get("throttle").is_none());
    assert!(effective.data.get("profile").is_none());
    assert!(effective.data["models"]["models"]
        .as_object()
        .unwrap()
        .is_empty());
    assert!(effective.data["routing"]["roles"]
        .as_object()
        .unwrap()
        .is_empty());
    assert!(build::build_opencode_config(&effective, "any").is_err());
}

#[test]
fn shipped_routes_are_stable() {
    let (_dir, home, project) = setup();
    let effective = disk(&home, &project);
    let full = |role: &str| -> String {
        let key = effective.data["routing"]["roles"][role]["model"]
            .as_str()
            .expect("model key");
        model::model_full_id(&effective.data, key).unwrap().1
    };
    assert_eq!(full("explore"), "volcengine-coding-plan/kimi-k2.7-code");
    assert_eq!(full("explore-deep"), "volcengine-coding-plan/kimi-k3");
    assert_eq!(full("build"), "opencode-go/deepseek-v4.1-flash");
    assert_eq!(full("verify"), "opencode-go/glm-5.3-flash");
    assert_eq!(full("debug"), "opencode-go/glm-5.3");
    assert_eq!(full("docs"), "opencode-go/deepseek-v4.1-flash");
}

#[test]
fn provider_binding_is_deterministic() {
    let (_dir, home, project) = setup();
    let effective = disk(&home, &project);
    for (role, spec) in model::role_specs(&effective.data).unwrap() {
        let key = spec["model"].as_str().unwrap();
        let (provider, _) = model::model_full_id(&effective.data, key).unwrap();
        if role.starts_with("explore") {
            assert_eq!(provider, "volcengine-coding-plan");
        } else {
            assert_eq!(provider, "opencode-go");
        }
        assert_ne!(provider, "openai", "OpenAI must not be a worker");
    }
}

#[test]
fn worker_agents_are_hidden_and_cannot_delegate() {
    let (_dir, home, project) = setup();
    let effective = disk(&home, &project);
    let config = build::build_opencode_config(&effective, "gpt-5.6-sol").expect("build");
    for role in model::role_specs(&effective.data).unwrap().keys() {
        let agent = &config["agent"][model::worker_agent_id(role)];
        assert_eq!(agent["mode"], json!("subagent"));
        assert_eq!(agent["hidden"], json!(true));
        assert_eq!(agent["permission"]["task"], json!("deny"));
    }
}

#[test]
fn lead_can_only_task_its_own_workers() {
    let (_dir, home, project) = setup();
    let effective = disk(&home, &project);
    let config = build::build_opencode_config(&effective, "gpt-5.6-sol").expect("build");
    let task = &config["agent"]["lead"]["permission"]["task"];
    assert_eq!(task["*"], json!("deny"));
    let allowed: Vec<String> = task
        .as_object()
        .unwrap()
        .iter()
        .filter(|(_, value)| value.as_str() == Some("allow"))
        .map(|(key, _)| key.clone())
        .collect();
    let expected: Vec<String> = model::role_specs(&effective.data)
        .unwrap()
        .keys()
        .map(|role| model::worker_agent_id(role))
        .collect();
    assert_eq!(allowed.len(), expected.len());
    for agent in expected {
        assert!(allowed.contains(&agent));
    }
}

#[test]
fn enabled_providers_are_the_routing_providers() {
    let (_dir, home, project) = setup();
    let effective = disk(&home, &project);
    let config = build::build_opencode_config(&effective, "gpt-5.6-sol").expect("build");
    let providers: Vec<String> = config["enabled_providers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        providers,
        vec!["openai", "volcengine-coding-plan", "opencode-go"]
    );
}

#[test]
fn fallback_is_reported_and_validated() {
    let (_dir, home, project) = setup();
    write_project(
        &project,
        json!({"routing": {"roles": {"build": {
            "model": "deepseek-v4.1-flash",
            "variant": "high",
            "fallback": [{"model": "glm-5.3", "variant": "high"}]
        }}}}),
    );
    let effective = disk(&home, &project);
    assert_eq!(validate::validate(&effective), Vec::<String>::new());
    let block = prompt::routing_block(&effective).expect("routing block");
    assert!(block.contains("Configured fallbacks"));
    let config = build::build_opencode_config(&effective, "gpt-5.6-sol").expect("build");
    let lead = config["agent"]["lead"]["prompt"].as_str().unwrap();
    assert!(lead.contains("Configured fallbacks"));
}

#[test]
fn default_config_is_valid() {
    let (_dir, home, project) = setup();
    let effective = disk(&home, &project);
    assert_eq!(validate::validate(&effective), Vec::<String>::new());
}

#[test]
fn missing_model_key_is_reported() {
    let (_dir, home, project) = setup();
    write_project(
        &project,
        json!({"routing": {"roles": {"build": {"model": "no-such-model"}}}}),
    );
    let effective = disk(&home, &project);
    let errors = validate::validate(&effective);
    assert!(
        errors.iter().any(|error| error.contains("no-such-model")),
        "{errors:?}"
    );
}

#[test]
fn missing_provider_is_reported() {
    let (_dir, home, project) = setup();
    let mut config = project_profile();
    config["models"]["models"]["mystery"] =
        json!({"provider": "no-such-provider", "id": "mystery-1", "placeholder": false});
    write_project(
        &project,
        json!({
            "routing": {"roles": {"build": {"model": "mystery"}}},
            "profile": config["profile"].clone(),
            "models": config["models"].clone(),
        }),
    );
    let effective = disk(&home, &project);
    let errors = validate::validate(&effective);
    assert!(
        errors
            .iter()
            .any(|error| error.contains("no-such-provider")),
        "{errors:?}"
    );
}

#[test]
fn unknown_variant_is_reported() {
    let (_dir, home, project) = setup();
    write_project(
        &project,
        json!({"routing": {"roles": {"build": {
            "model": "deepseek-v4.1-flash", "variant": "impossible"
        }}}}),
    );
    let effective = disk(&home, &project);
    let errors = validate::validate(&effective);
    assert!(
        errors.iter().any(|error| error.contains("impossible")),
        "{errors:?}"
    );
}

#[test]
fn valid_variant_is_accepted() {
    let (_dir, home, project) = setup();
    write_project(
        &project,
        json!({"routing": {"roles": {"build": {
            "model": "deepseek-v4.1-flash", "variant": "max"
        }}}}),
    );
    let effective = disk(&home, &project);
    assert_eq!(validate::validate(&effective), Vec::<String>::new());
}

#[test]
fn unsupported_selected_variant_is_rejected() {
    let (_dir, home, project) = setup();
    let mut config = project_profile();
    config["models"]["models"]["gpt-5.6-sol"]["variant"] = json!("impossible");
    write_yaml(&project.join(".ocg.yaml"), &config);
    let effective = disk(&home, &project);
    let errors = validate::validate(&effective);
    assert!(
        errors.iter().any(|error| error.contains("impossible")),
        "{errors:?}"
    );
}

#[test]
fn selected_model_without_a_variant_is_valid() {
    let (_dir, home, project) = setup();
    let mut config = project_profile();
    config["profile"]["defaultModel"] = json!("kimi-k3");
    config["models"]["models"]["kimi-k3"]["variant"] = serde_json::Value::Null;
    write_yaml(&project.join(".ocg.yaml"), &config);
    let effective = disk(&home, &project);
    assert_eq!(validate::validate(&effective), Vec::<String>::new());
    let contract = model::lead_contract(&effective.data, "kimi-k3").unwrap();
    assert_eq!(contract.variant, None);
}

#[test]
fn unknown_selected_model_is_reported() {
    let (_dir, home, project) = setup();
    let mut config = project_profile();
    config["profile"]["defaultModel"] = json!("ghost");
    write_yaml(&project.join(".ocg.yaml"), &config);
    let effective = disk(&home, &project);
    assert!(validate::validate(&effective)
        .iter()
        .any(|error| error.contains("ghost")));
}

#[test]
fn missing_prompt_is_reported() {
    let (_dir, home, project) = setup();
    fs::remove_file(home.join("config").join("prompts").join("docs.md")).expect("remove prompt");
    let effective = disk(&home, &project);
    assert!(validate::validate(&effective)
        .iter()
        .any(|error| error.contains("docs")));
}

#[test]
fn require_valid_raises() {
    let (_dir, home, project) = setup();
    write_project(
        &project,
        json!({"routing": {"roles": {"build": {"model": "no-such-model"}}}}),
    );
    let effective = disk(&home, &project);
    assert!(validate::require_valid(&effective).is_err());
}

#[test]
fn missing_config_file_is_reported() {
    let (dir, home, _project) = setup();
    fs::remove_file(home.join("config").join("permissions.yaml")).expect("remove permissions");
    assert!(load_defaults(&OcgSource::Dir(home)).is_err());
    drop(dir);
}

#[test]
fn project_profile_swaps_a_model_label() {
    let (_dir, home, project) = setup();
    let mut config = project_profile();
    config["models"]["models"]["glm-5.3"]["label"] = json!("GLM-5.3 custom");
    write_yaml(&project.join(".ocg.yaml"), &config);
    let effective = disk(&home, &project);
    assert_eq!(
        model::model_label(&effective.data, "glm-5.3"),
        "GLM-5.3 custom"
    );
}

#[test]
fn project_override_beats_user_override() {
    let (dir, home, project) = setup();
    let user = dir.join("user.yaml");
    write_yaml(
        &user,
        &json!({"routing": {"roles": {"build": {"model": "glm-5.3"}}}}),
    );
    write_project(
        &project,
        json!({"routing": {"roles": {"build": {"model": "glm-5.3-flash"}}}}),
    );
    let effective = load_disk_effective(&home, &project, Some(&user), None);
    assert_eq!(
        effective.data["routing"]["roles"]["build"]["model"],
        json!("glm-5.3-flash")
    );
}

#[test]
fn user_override_is_used_when_project_is_absent() {
    let (dir, home, project) = setup();
    let user = dir.join("user.yaml");
    write_yaml(
        &user,
        &json!({"routing": {"roles": {"build": {"model": "glm-5.3"}}}}),
    );
    let effective = load_disk_effective(&home, &project, Some(&user), None);
    assert_eq!(
        effective.data["routing"]["roles"]["build"]["model"],
        json!("glm-5.3")
    );
    let names: Vec<&str> = effective.applied.iter().map(|(name, _)| *name).collect();
    assert_eq!(names, vec!["user"]);
}

#[test]
fn missing_override_files_are_ignored() {
    let (dir, home, project) = setup();
    let effective = load_disk_effective(
        &home,
        &project,
        Some(&dir.join("nope-user.yaml")),
        Some(&dir.join("nope-project.yaml")),
    );
    assert!(effective.applied.is_empty());
    // No layer was invented; only the missing Profile is reported.
    assert_eq!(
        validate::validate(&effective),
        vec!["OCG Profile is missing; create or import .ocg.yaml".to_string()]
    );
}

#[test]
fn project_override_can_replace_a_prompt() {
    let (dir, home, project) = setup();
    let prompt_path = dir.join("custom-lead.md");
    fs::write(&prompt_path, "Custom lead prompt.\n").expect("write prompt");
    write_project(
        &project,
        json!({"prompts": {"lead": prompt_path.to_string_lossy()}}),
    );
    let effective = disk(&home, &project);
    let (_, body) = prompt::read_prompt(&effective, "lead").expect("read prompt");
    assert_eq!(body, "Custom lead prompt.");
}

#[test]
fn raw_opencode_override_is_merged_last() {
    let (_dir, home, project) = setup();
    write_project(&project, json!({"opencode": {"username": "gearbox"}}));
    let effective = disk(&home, &project);
    let config = build::build_opencode_config(&effective, "gpt-5.6-sol").expect("build");
    assert_eq!(config["username"], json!("gearbox"));
}

#[test]
fn relative_prompt_path_resolves_against_project() {
    let (_dir, home, project) = setup();
    fs::write(project.join("lead.md"), "Relative lead prompt.\n").expect("write");
    write_project(&project, json!({"prompts": {"lead": "lead.md"}}));
    let effective = disk(&home, &project);
    let (_, body) = prompt::read_prompt(&effective, "lead").expect("read");
    assert_eq!(body, "Relative lead prompt.");
}

#[test]
fn append_keeps_core_prompt_and_adds_policy() {
    let (_dir, home, project) = setup();
    let policy = project.join("lead-policy.md");
    fs::write(&policy, "# Project policy\n\nNever touch production.\n").expect("write");
    write_project(
        &project,
        json!({"prompts": {"lead": {"append": [policy.to_string_lossy()]}}}),
    );
    let effective = disk(&home, &project);
    let (_, body) = prompt::read_prompt(&effective, "lead").expect("read");
    assert!(body.contains("You are the Lead in an OCG multi-model setup."));
    assert!(body.contains("Never touch production."));
    assert!(body.contains("---"));
    assert!(body.find("You are the Lead").unwrap() < body.find("Never touch production.").unwrap());
}

#[test]
fn append_only_uses_the_gear_default_not_a_replacement() {
    let (_dir, home, project) = setup();
    let policy = project.join("policy.md");
    fs::write(&policy, "Project-only clause.\n").expect("write");
    write_project(
        &project,
        json!({"prompts": {"lead": {"append": [policy.to_string_lossy()]}}}),
    );
    let effective = disk(&home, &project);
    let (_, body) = prompt::read_prompt(&effective, "lead").expect("read");
    assert!(body.starts_with(lead_prompt(&home).trim()));
    assert!(body.ends_with("Project-only clause."));
}

#[test]
fn append_accepts_inline_text() {
    let (_dir, home, project) = setup();
    write_project(
        &project,
        json!({"prompts": {"lead": {"append": [{"text": "Inline clause."}]}}}),
    );
    let effective = disk(&home, &project);
    let (_, body) = prompt::read_prompt(&effective, "lead").expect("read");
    assert!(body.contains("Inline clause."));
}

#[test]
fn path_then_append_replaces_and_extends() {
    let (_dir, home, project) = setup();
    let replacement = project.join("replacement.md");
    fs::write(&replacement, "Replacement core.\n").expect("write");
    let extra = project.join("extra.md");
    fs::write(&extra, "Extra policy.\n").expect("write");
    write_project(
        &project,
        json!({"prompts": {"lead": {
            "path": replacement.to_string_lossy(),
            "append": [extra.to_string_lossy()]
        }}}),
    );
    let effective = disk(&home, &project);
    let (_, body) = prompt::read_prompt(&effective, "lead").expect("read");
    assert!(body.starts_with("Replacement core."));
    assert!(body.ends_with("Extra policy."));
    assert!(!body.contains("You are the Lead in an OCG"));
}

#[test]
fn append_accepts_relative_project_path() {
    let (_dir, home, project) = setup();
    fs::create_dir_all(project.join("policy")).expect("mkdir");
    fs::write(
        project.join("policy").join("lead.md"),
        "Relative project policy.\n",
    )
    .expect("write");
    write_project(
        &project,
        json!({"prompts": {"lead": {"append": ["policy/lead.md"]}}}),
    );
    let effective = disk(&home, &project);
    let (_, body) = prompt::read_prompt(&effective, "lead").expect("read");
    assert!(body.ends_with("Relative project policy."));
}

#[test]
fn appended_policy_reaches_the_rendered_lead_agent() {
    let (_dir, home, project) = setup();
    let policy = project.join("lead-policy.md");
    fs::write(&policy, "Use `{{build}}` only for approved scope.\n").expect("write");
    write_project(
        &project,
        json!({"prompts": {"lead": {"append": [policy.to_string_lossy()]}}}),
    );
    let effective = disk(&home, &project);
    let config = build::build_opencode_config(&effective, "gpt-5.6-sol").expect("build");
    let text = config["agent"]["lead"]["prompt"].as_str().unwrap();
    assert!(text.contains("Use `ocg-build` only for approved scope."));
}

#[test]
fn missing_appended_file_is_reported() {
    let (_dir, home, project) = setup();
    write_project(
        &project,
        json!({"prompts": {"lead": {"append": ["missing-policy.md"]}}}),
    );
    let effective = disk(&home, &project);
    assert!(validate::validate(&effective)
        .iter()
        .any(|error| error.contains("lead")));
}

#[test]
fn append_must_be_a_list() {
    let (_dir, home, project) = setup();
    write_project(
        &project,
        json!({"prompts": {"lead": {"append": "not-a-list"}}}),
    );
    let effective = disk(&home, &project);
    assert!(validate::validate(&effective)
        .iter()
        .any(|error| error.contains("append")));
}

#[test]
fn append_does_not_leak_into_worker_prompts() {
    let (_dir, home, project) = setup();
    let policy = project.join("lead-policy.md");
    fs::write(&policy, "Lead-only clause.\n").expect("write");
    write_project(
        &project,
        json!({"prompts": {"lead": {"append": [policy.to_string_lossy()]}}}),
    );
    let effective = disk(&home, &project);
    let config = build::build_opencode_config(&effective, "gpt-5.6-sol").expect("build");
    for role in model::role_specs(&effective.data).unwrap().keys() {
        let text = config["agent"][model::worker_agent_id(role)]["prompt"]
            .as_str()
            .unwrap();
        assert!(!text.contains("Lead-only clause."));
    }
}

#[test]
fn core_lead_prompt_stays_project_agnostic() {
    let (_dir, home, _project) = setup();
    let core = lead_prompt(&home);
    for token in [
        concat!("Zh", "uju"),
        concat!("xiang", "min"),
        concat!("chun", "cheon"),
    ] {
        assert!(!core.contains(token), "core prompt leaked {token}");
    }
    assert!(!core.contains("/home/"));
    assert!(!core.contains("/Users/"));
}

#[test]
fn project_override_is_loaded_only_for_that_project() {
    let (dir, home, project) = setup();
    write_project(&project, json!({"observability": {"enabled": true}}));
    let here = disk(&home, &project);
    let other = dir.join("other-project");
    fs::create_dir_all(&other).expect("mkdir");
    let elsewhere = load_disk_effective(&home, &other, None, None);
    assert_eq!(here.data["observability"]["enabled"], json!(true));
    assert_ne!(elsewhere.data["observability"]["enabled"], json!(true));
    assert_eq!(here.applied.len(), 1);
    assert!(elsewhere.applied.is_empty());
    // A different project without a Profile gets no resources and no routing.
    assert!(elsewhere.data["routing"]["roles"]
        .as_object()
        .unwrap()
        .is_empty());
}

#[test]
fn rendered_lead_is_the_core_prompt_without_project_policy() {
    let (_dir, home, project) = setup();
    let effective = disk(&home, &project);
    let config = build::build_opencode_config(&effective, "gpt-5.6-sol").expect("build");
    let rendered = config["agent"]["lead"]["prompt"].as_str().unwrap();
    let core = lead_prompt(&home);
    for line in core.lines() {
        let stripped = line.trim();
        if !stripped.is_empty() && !stripped.contains("{{") && !stripped.contains('|') {
            assert!(rendered.contains(stripped), "missing line: {stripped}");
        }
    }
}

#[test]
fn trace_is_disabled_by_default() {
    let (_dir, home, project) = setup();
    let effective = disk(&home, &project);
    assert!(observability::trace_path(&effective, None).is_none());
    assert!(observability::record_event(&effective, "launch", "gpt-5.6-sol", None).is_none());
}

#[test]
fn trace_writes_local_jsonl_without_content() {
    let (dir, home, project) = setup();
    let trace_file = dir.join("events.jsonl");
    write_project(
        &project,
        json!({"observability": {"enabled": true, "path": trace_file.to_string_lossy()}}),
    );
    let effective = disk(&home, &project);
    assert_eq!(
        observability::record_event(&effective, "launch", "gpt-5.6-sol", None),
        Some(trace_file.clone())
    );
    let record: serde_json::Value =
        serde_json::from_str(fs::read_to_string(&trace_file).unwrap().trim()).expect("record");
    assert_eq!(record["event"], json!("launch"));
    assert_eq!(record["model_choice"], json!("gpt-5.6-sol"));
    assert_eq!(record["default_agent"], json!("lead"));
    assert_eq!(
        record["routing"]["build"],
        json!("opencode-go/deepseek-v4.1-flash")
    );
    let serialized = record.to_string().to_lowercase();
    assert!(!serialized.contains("prompt"));
    assert!(!serialized.contains("source"));
}

#[test]
fn trace_env_override_is_used() {
    let (dir, home, project) = setup();
    write_project(&project, json!({"observability": {"enabled": true}}));
    let effective = disk(&home, &project);
    let trace_file = dir.join("env-events.jsonl");
    assert_eq!(
        observability::trace_path(&effective, Some(&trace_file)),
        Some(trace_file)
    );
}

#[test]
fn arbitrary_routing_roles_are_supported() {
    let (_dir, home, project) = setup();
    write_project(
        &project,
        json!({
            "routing": {"roles": {"audit": {"model": "glm-5.3", "description": "audit role"}}},
            "prompts": {"audit": {"text": "You audit changes."}}
        }),
    );
    let effective = disk(&home, &project);
    assert_eq!(validate::validate(&effective), Vec::<String>::new());
    let config = build::build_opencode_config(&effective, "gpt-5.6-sol").expect("build");
    let agent = &config["agent"]["ocg-audit"];
    assert_eq!(agent["model"], json!("opencode-go/glm-5.3"));
    assert_eq!(agent["description"], json!("audit role"));
    assert_eq!(agent["prompt"], json!("You audit changes."));
    assert_eq!(
        config["agent"]["lead"]["permission"]["task"]["ocg-audit"],
        json!("allow")
    );
    assert!(config["enabled_providers"]
        .as_array()
        .unwrap()
        .iter()
        .any(|value| value == "opencode-go"));
}

#[test]
fn arbitrary_role_placeholders_are_substituted_in_the_lead() {
    let (_dir, home, project) = setup();
    let policy = project.join("policy.md");
    fs::write(&policy, "Delegate audits to `{{audit}}`.\n").expect("write");
    write_project(
        &project,
        json!({
            "routing": {"roles": {"audit": {"model": "glm-5.3"}}},
            "prompts": {
                "audit": {"text": "You audit."},
                "lead": {"append": [policy.to_string_lossy()]}
            }
        }),
    );
    let effective = disk(&home, &project);
    let config = build::build_opencode_config(&effective, "gpt-5.6-sol").expect("build");
    let text = config["agent"]["lead"]["prompt"].as_str().unwrap();
    assert!(text.contains("Delegate audits to `ocg-audit`."));
}

#[test]
fn embedded_defaults_match_the_disk_config() {
    let (_dir, home, project) = setup();
    let embedded = load_embedded_effective(&project);
    let disk_effective = load_disk_effective(&home, &project, None, None);
    // Prompt sources legitimately differ (embedded text vs OCG-home files);
    // every shipped registry and policy section must still be identical.
    let comparable = |effective: &Effective| {
        let mut data = effective.data.clone();
        data.as_object_mut().unwrap().remove("prompts");
        data.as_object_mut().unwrap().remove("_prompt_defaults");
        data
    };
    assert_eq!(comparable(&embedded), comparable(&disk_effective));
    assert!(embedded.data.get("throttle").is_none());
}

fn build_with(
    home: &Path,
    project: &Path,
    user_path: &Path,
    project_path: &Path,
) -> ocg::error::Result<Effective> {
    let defaults = load_defaults(&OcgSource::Dir(home.to_path_buf())).expect("defaults");
    build_effective(
        defaults,
        Some(home.to_path_buf()),
        project,
        user_path,
        project_path,
        None,
    )
}

#[test]
fn stale_project_json_is_rejected_and_named() {
    let (dir, home, project) = setup();
    write_project(&project, json!({"throttle": {"default": "mid"}}));
    let stale = project.join(".ocg.json");
    fs::write(&stale, "{\"throttle\":{\"default\":\"high\"}}\n").expect("stale json");

    let error = build_with(
        &home,
        &project,
        &dir.join("no-user.yaml"),
        &project.join(".ocg.yaml"),
    )
    .expect_err("stale project JSON must be rejected");
    let text = error.to_string();
    assert!(text.contains(stale.to_str().unwrap()), "{text}");
    assert!(
        text.contains(".ocg.yaml"),
        "the YAML target must be named: {text}"
    );
    assert!(text.to_lowercase().contains("yaml"), "{text}");
    assert!(
        !text.contains("mid") && !text.contains("high"),
        "the stale JSON must not be merged or applied: {text}"
    );
}

#[test]
fn stale_user_json_is_rejected_and_named() {
    let (dir, home, project) = setup();
    let stale = dir.join("user.yaml");
    fs::write(dir.join("user.json"), "throttle:\n  default: high\n").expect("stale json");

    let error = build_with(&home, &project, &stale, &project.join(".ocg.yaml"))
        .expect_err("stale user JSON must be rejected");
    let text = error.to_string();
    assert!(text.contains("user.json"), "{text}");
    assert!(text.contains("user.yaml"), "{text}");
    assert!(text.to_lowercase().contains("yaml"), "{text}");
}

#[test]
fn explicit_json_override_is_rejected() {
    let (dir, home, project) = setup();
    let json_path = dir.join("custom.json");
    fs::write(&json_path, "throttle:\n  default: high\n").expect("json override");

    let error = build_with(&home, &project, &json_path, &project.join(".ocg.yaml"))
        .expect_err("an explicit JSON override must be rejected");
    let text = error.to_string();
    assert!(text.contains("custom.json"), "{text}");
    assert!(text.contains("custom.yaml"), "{text}");
}

#[test]
fn comment_only_yaml_override_is_a_noop() {
    let (_dir, home, project) = setup();
    fs::write(
        project.join(".ocg.yaml"),
        "# This project has no overrides yet.\n",
    )
    .expect("write override");
    let effective = disk(&home, &project);
    assert!(effective.data.get("profile").is_none());
    assert!(validate::validate(&effective)
        .iter()
        .any(|error| error.contains("Profile")));
}

#[test]
fn missing_yaml_override_files_are_ignored() {
    let (dir, home, project) = setup();
    let effective = build_with(
        &home,
        &project,
        &dir.join("nope-user.yaml"),
        &dir.join("nope-project.yaml"),
    )
    .expect("missing YAML overrides are fine");
    assert!(effective.applied.is_empty());
    assert_eq!(
        validate::validate(&effective),
        vec!["OCG Profile is missing; create or import .ocg.yaml".to_string()]
    );
}

#[test]
fn v2_config_uses_the_2011_local_plugin_discovery_contract() {
    let (_dir, home, project) = setup();
    let effective = disk(&home, &project);

    let v1 = build::build_opencode_config(&effective, "gpt-5.6-sol").expect("v1 build");
    let v2 = build::build_opencode_config_for(
        &effective,
        "gpt-5.6-sol",
        ocg::runtime::compat::v2_adapter(),
    )
    .expect("v2 build");

    // v1 keeps the historical singular plugin array and `task` permission key.
    assert!(v1.get("plugin").is_some(), "v1 must keep `plugin`");
    assert!(v1.get("plugins").is_none());
    assert!(v1["agent"]["lead"]["permission"].get("task").is_some());
    assert_eq!(
        v1["agent"]["ocg-build"]["permission"]["task"],
        json!("deny")
    );

    // 2.0.11 discovers local plugins from OPENCODE_CONFIG_DIR/plugins. Its
    // singular `plugin` array is for npm packages, so a generated local file
    // must not be represented as a file:// package entry.
    assert!(v2.get("plugins").is_none(), "2.0.11 has no `plugins` key");
    assert!(
        v2.get("plugin").is_none(),
        "local discovery adds no package entry"
    );

    // v2 renamed the delegation tool/permission key to `subagent`.
    assert!(v2["agent"]["lead"]["permission"].get("subagent").is_some());
    assert!(v2["agent"]["lead"]["permission"].get("task").is_none());
    assert_eq!(
        v2["agent"]["ocg-build"]["permission"]["subagent"],
        json!("deny")
    );

    // Agent/role structure and the Lead model selection are unchanged.
    assert_eq!(v2["default_agent"], v1["default_agent"]);
    assert_eq!(v2["model"], v1["model"]);
    assert_eq!(v2["agent"]["lead"]["model"], v1["agent"]["lead"]["model"]);
}

#[test]
fn changed_routing_model_clears_inherited_variant() {
    let (_dir, home, project) = setup();
    // `build` ships with variant `high`; routing it to a model that declares no
    // variant must clear the inherited value rather than keep it.
    write_project(
        &project,
        json!({"routing": {"roles": {"build": {"model": "kimi-k3"}}}}),
    );
    let effective = disk(&home, &project);
    assert!(effective.data["routing"]["roles"]["build"]
        .get("variant")
        .is_none());
    // The Profile's own selected variant is separate and survives.
    assert!(effective.data["models"]["models"]["gpt-5.6-sol"]
        .get("variant")
        .is_none());
}
