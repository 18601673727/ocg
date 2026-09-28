//! No-paid smoke tests for the generated orchestration plugin.
//!
//! The generated config is accepted by a *fake* `opencode` executable and, when
//! a real `opencode` is on `PATH`, by `opencode debug config`. Neither path
//! calls a model, network or provider; `debug config` only resolves the config
//! and loads the plugin module (which performs no bridge call until a hook
//! fires).

mod common;

use common::{project_profile, write_yaml, TestDir};
use std::fs;
use std::path::Path;
use std::process::Command;

fn project(dir: &TestDir) -> std::path::PathBuf {
    let project = dir.project();
    fs::create_dir_all(project.join("src")).unwrap();
    fs::write(
        project.join("src/parser.rs"),
        "pub fn parse() -> u32 { 1 }\n",
    )
    .unwrap();
    // A generated compatibility config needs an OCG-owned Profile resource.
    write_yaml(&project.join(".ocg.yaml"), &project_profile());
    project
}

fn generated_config(project: &Path) -> String {
    // Materialize the V2 adapter through the exact local-discovery directory,
    // then build the V2 config. Local plugins deliberately do not appear in
    // the package-only `plugin` array.
    ocg::orchestration::plugin::materialize_v2_with(
        project,
        ocg::runtime::compat::v2_adapter().plugin_source(),
    )
    .unwrap();
    let effective = common::load_embedded_effective(project);
    let config = ocg::build::build_opencode_config_for(
        &effective,
        "gpt-5.6-sol",
        ocg::runtime::compat::v2_adapter(),
    )
    .unwrap();
    serde_json::to_string(&config).unwrap()
}

#[test]
fn fake_opencode_executable_accepts_the_generated_config() {
    let dir = TestDir::new();
    let project = project(&dir);
    let content = generated_config(&project);

    let fake = dir.join("fake-opencode");
    fs::write(
        &fake,
        "#!/bin/sh\n\
         # Fake V2 config probe: validate local plugin discovery, call no model.\n\
         case \"${OPENCODE_CONFIG_CONTENT:-}\" in\n\
           *'\"agent\"'*) test -f \"$OPENCODE_CONFIG_DIR/plugins/ocg-orchestration.js\" && printf '%s' \"$OPENCODE_CONFIG_CONTENT\" && exit 0 ;;\n\
           *) echo 'config or plugin missing' >&2; exit 1 ;;\n\
         esac\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&fake, fs::Permissions::from_mode(0o755)).unwrap();
    }

    let output = Command::new(&fake)
        .current_dir(&project)
        .env("OPENCODE_CONFIG_CONTENT", &content)
        .env(
            "OPENCODE_CONFIG_DIR",
            ocg::orchestration::plugin::v2_config_dir(&project),
        )
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "fake opencode rejected the config: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("\"agent\""));
}

#[test]
fn real_opencode_debug_config_accepts_the_plugin_when_available() {
    let dir = TestDir::new();
    let project = project(&dir);
    let content = generated_config(&project);

    // The system `opencode` may be V1 (1.x) which lacks `api --standalone`.
    // Detect the major version and only run the V2 private-server probe when
    // the installed binary is V2.
    let version_text = Command::new("opencode")
        .arg("--version")
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
            } else {
                None
            }
        });
    let version_text = match version_text {
        Some(v) => v,
        None => {
            eprintln!("skipping: opencode is not on PATH");
            return;
        }
    };
    let is_v2 = version_text
        .split_whitespace()
        .find_map(|token| {
            let token =
                token.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '.' && c != '-');
            let token = token.strip_prefix('v').unwrap_or(token);
            semver::Version::parse(token).ok()
        })
        .map(|v| v.major >= 2)
        .unwrap_or(false);
    if !is_v2 {
        eprintln!("skipping: installed opencode is V1 ({version_text}), V2 private-server probe requires V2");
        // The plugin file is still materialized for V1 regression: confirm
        // the materialization path works.
        assert!(
            ocg::orchestration::plugin::v2_config_dir(&project)
                .join("plugins/ocg-orchestration.js")
                .is_file(),
            "V2 plugin materialization failed"
        );
        return;
    }

    let config_path = dir.join("opencode.json");
    fs::write(&config_path, &content).expect("write private OpenCode config");
    let config_dir = ocg::orchestration::plugin::v2_config_dir(&project);
    let output = Command::new("opencode")
        // `api --standalone` creates a private server. Unlike `debug config`,
        // it does not silently query the existing background daemon.
        .args(["api", "--standalone", "GET", "/api/config"])
        .current_dir(&project)
        .env("OPENCODE_CONFIG", &config_path)
        .env("OPENCODE_CONFIG_DIR", &config_dir)
        .output()
        .expect("run opencode api --standalone");
    assert!(
        output.status.success(),
        "opencode api --standalone failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8_lossy(&output.stdout);
    // `/api/config` reports config sources, not the server's loaded local
    // plugin modules. Its successful private-runtime response must nevertheless
    // include the exact directory containing the materialized adapter; the
    // explicit file assertion below prevents this from becoming an ambient
    // daemon/config test.
    assert!(
        text.contains(config_dir.to_string_lossy().as_ref()),
        "private V2 config did not use the adapter config directory: {text}"
    );
    assert!(config_dir.join("plugins/ocg-orchestration.js").is_file());
}
