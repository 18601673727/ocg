#![allow(dead_code)]

//! Shared helpers for the integration tests.

use ocg::config::{build_effective, Effective};
use ocg::defaults::{load_defaults, OcgSource};
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// A temporary directory removed on drop.
pub struct TestDir {
    path: PathBuf,
}

impl TestDir {
    pub fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("ocg-test-{}-{}", std::process::id(), id));
        fs::create_dir_all(&path).expect("create test dir");
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn join(&self, name: &str) -> PathBuf {
        self.path.join(name)
    }

    pub fn project(&self) -> PathBuf {
        let project = self.join("project");
        fs::create_dir_all(&project).expect("create project");
        project
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

pub fn repo_config_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("config")
}

pub fn copy_dir_recursive(from: &Path, to: &Path) {
    fs::create_dir_all(to).expect("create dir");
    for entry in fs::read_dir(from).expect("read dir") {
        let entry = entry.expect("dir entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_dir_recursive(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), &target).expect("copy file");
        }
    }
}

/// A OCG home with `config/` copied from the repository.
pub fn ocg_home(dir: &TestDir) -> PathBuf {
    let home = dir.join("OCG");
    copy_dir_recursive(&repo_config_dir(), &home.join("config"));
    home
}

pub fn load_disk_effective(
    home: &Path,
    project: &Path,
    user: Option<&Path>,
    project_config: Option<&Path>,
) -> Effective {
    let defaults = load_defaults(&OcgSource::Dir(home.to_path_buf())).expect("defaults");
    let user_path = user
        .map(Path::to_path_buf)
        .or_else(|| project_config.map(Path::to_path_buf))
        .unwrap_or_else(|| project.join("no-user.yaml"));
    let project_path = user_path.clone();
    build_effective(
        defaults,
        Some(home.to_path_buf()),
        project,
        &user_path,
        &project_path,
        None,
    )
    .expect("effective")
}

pub fn load_embedded_effective(project: &Path) -> Effective {
    let defaults = load_defaults(&OcgSource::Embedded).expect("defaults");
    let user_path = project.join("no-user.yaml");
    build_effective(defaults, None, project, &user_path, &user_path, None).expect("effective")
}

pub fn write_yaml(path: &Path, value: &Value) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create parent");
    }
    let text = ocg::yaml::to_yaml_string(value).expect("serialize yaml");
    fs::write(path, text).expect("write yaml");
}

pub fn read_yaml(path: &Path) -> Value {
    let text = fs::read_to_string(path).expect("read yaml");
    ocg::yaml::parse_yaml_object(&path.display().to_string(), &text).expect("parse yaml")
}

pub fn patch_yaml<F: FnOnce(&mut Value)>(path: &Path, mutate: F) {
    let mut value = read_yaml(path);
    mutate(&mut value);
    write_yaml(path, &value);
}

pub fn write_json(path: &Path, value: &Value) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create parent");
    }
    fs::write(
        path,
        format!(
            "{}\n",
            serde_json::to_string_pretty(value).expect("serialize")
        ),
    )
    .expect("write json");
}

pub fn read_json(path: &Path) -> Value {
    serde_json::from_str(&fs::read_to_string(path).expect("read json")).expect("parse json")
}

pub fn patch_json<F: FnOnce(&mut Value)>(path: &Path, mutate: F) {
    let mut value = read_json(path);
    mutate(&mut value);
    write_json(path, &value);
}

pub fn get<'a>(value: &'a Value, key: &str) -> &'a Value {
    value
        .get(key)
        .unwrap_or_else(|| panic!("missing key '{key}'"))
}

/// The OCG-owned Profile a test project needs. Provider/model resources are
/// project-owned since the fixed execution tiers were removed, so a test that
/// exercises routing, permissions or prompts must declare them explicitly.
pub fn project_profile() -> Value {
    let model = |provider: &str, id: &str, label: &str, variants: &[&str]| {
        (
            id.to_string(),
            json!({
                "provider": provider,
                "id": id,
                "label": label,
                "placeholder": false,
                "variants": variants,
            }),
        )
    };
    let models = serde_json::Map::from_iter([
        model(
            "openai",
            "gpt-5.6-sol",
            "GPT-5.6 Sol",
            &["low", "medium", "high", "xhigh", "max"],
        ),
        model(
            "openai",
            "gpt-6-astra",
            "GPT-6 Astra",
            &["low", "medium", "high", "xhigh", "max"],
        ),
        model("volcengine-coding-plan", "kimi-k2.7-code", "Kimi K2.7", &[]),
        model("volcengine-coding-plan", "kimi-k3", "Kimi K3", &[]),
        model(
            "opencode-go",
            "deepseek-v4.1-flash",
            "DeepSeek V4.1 Flash",
            &["low", "high", "max"],
        ),
        model(
            "opencode-go",
            "glm-5.3-flash",
            "GLM-5.3 Flash",
            &["low", "high", "max"],
        ),
        model("opencode-go", "glm-5.3", "GLM-5.3", &["low", "high", "max"]),
    ]);
    json!({
        "profile": {"origin": "new", "defaultModel": "gpt-5.6-sol"},
        "models": {
            "providers": {
                "openai": {"label": "OpenAI", "placeholder": false},
                "volcengine-coding-plan": {"label": "Volcano Coding Plan Pro", "placeholder": false},
                "opencode-go": {"label": "OpenCode Go", "placeholder": false}
            },
            "models": models
        },
        "routing": {
            "small_model": "deepseek-v4.1-flash",
            "roles": {
                "explore": {"model": "kimi-k2.7-code", "description": "Repository reconnaissance."},
                "explore-deep": {"model": "kimi-k3", "description": "Deep exploration."},
                "build": {"model": "deepseek-v4.1-flash", "variant": "high", "description": "Implementation."},
                "verify": {"model": "glm-5.3-flash", "variant": "high", "description": "Independent review."},
                "debug": {"model": "glm-5.3", "variant": "high", "description": "Escalation target."},
                "docs": {"model": "deepseek-v4.1-flash", "variant": "high", "description": "Closeout reports."}
            }
        }
    })
}

/// Write a global config fixture that always carries the OCG Profile, so a test may
/// override only the section it is about.
pub fn write_project_yaml(path: &Path, mut value: Value) {
    let profile = project_profile();
    for key in ["profile", "models", "routing"] {
        if value.get(key).is_none() {
            value[key] = profile[key].clone();
        }
    }
    write_yaml(path, &value);
}

/// Ensure a test project has a Profile, then load the effective configuration.
pub fn seeded_disk(home: &Path, project: &Path) -> Effective {
    let path = project.join("global.yaml");
    if !path.is_file() {
        write_yaml(&path, &project_profile());
    }
    load_disk_effective(home, project, None, None)
}
