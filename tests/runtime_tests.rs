//! Runtime discovery, install and update tests.
//!
//! Everything runs offline: HTTP is an in-memory fixture table, the process
//! host is fake, the clock is fixed and every project is a temporary
//! directory.

mod common;

use common::TestDir;
use ocg::clock::FixedClock;
use ocg::error::OcgError;
use ocg::http::MemoryHttp;
use ocg::platform::Platform;
use ocg::process::{is_executable, FakeProcessHost};
use ocg::runtime::cache::CacheRecord;
use ocg::runtime::compat::{
    self, adapter_for, classify, detect, detect_from_host, EffectiveLead, LeadSelection,
    MemorySessionClient, Observation,
};
use ocg::runtime::hash::sha256_hex;
use ocg::runtime::install::{install_opencode, ActiveRuntime, Layout};
use ocg::runtime::policy::RuntimePolicy;
use ocg::runtime::release::parse_release;
use ocg::runtime::{RuntimeManager, RuntimeSource};
use semver::Version;
use serde_json::json;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

const API: &str = "https://api.test";
const LATEST_URL: &str = "https://api.test/repos/anomalyco/opencode/releases/latest";
const TAG_URL: &str = "https://api.test/repos/anomalyco/opencode/releases/tags/v1.18.31";
const ASSET_URL: &str = "https://dl.test/opencode-linux-x64.tar.gz";

fn platform() -> Platform {
    Platform::parse("linux", "x86_64").unwrap()
}

fn targz(binary: &[u8]) -> Vec<u8> {
    targz_named("opencode", binary)
}

fn targz_named(name: &str, body: &[u8]) -> Vec<u8> {
    let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
        Vec::new(),
        flate2::Compression::default(),
    ));
    let mut header = tar::Header::new_gnu();
    header.set_size(body.len() as u64);
    header.set_mode(0o755);
    header.set_cksum();
    builder.append_data(&mut header, name, body).unwrap();
    builder.into_inner().unwrap().finish().unwrap()
}

fn release_json(version: &str, tag: &str) -> String {
    let archive = targz(format!("#!/bin/sh\necho {version}\n").as_bytes());
    let sha = sha256_hex(&archive);
    format!(
        r#"{{
          "tag_name": "{tag}",
          "assets": [
            {{"name": "opencode-linux-x64.tar.gz",
              "browser_download_url": "{ASSET_URL}",
              "digest": "sha256:{sha}"}}
          ]
        }}"#
    )
}

fn http_with_latest(version: &str) -> MemoryHttp {
    let archive = targz(format!("#!/bin/sh\necho {version}\n").as_bytes());
    MemoryHttp::new()
        .with(
            LATEST_URL,
            release_json(version, &format!("v{version}")).into_bytes(),
        )
        .with(ASSET_URL, archive)
}

fn install_fake_managed(project: &Path, version: &str) -> PathBuf {
    let version = Version::parse(version).unwrap();
    let layout = Layout::new(project);
    let binary = layout.binary_path(&version);
    fs::create_dir_all(binary.parent().unwrap()).unwrap();
    fs::write(&binary, format!("#!/bin/sh\necho {version}\n")).unwrap();
    make_executable(&binary);
    ActiveRuntime {
        version,
        path: binary.clone(),
        installed_at: 0,
    }
    .write(project)
    .unwrap();
    binary
}

fn make_executable(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).unwrap();
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
}

fn write_executable(path: &Path, body: &str) {
    fs::write(path, body).unwrap();
    make_executable(path);
}

fn manager<'a>(
    dir: &TestDir,
    policy: RuntimePolicy,
    http: &'a MemoryHttp,
    clock: &'a FixedClock,
    process: &'a FakeProcessHost,
) -> RuntimeManager<'a> {
    RuntimeManager::new(dir.path(), policy, platform(), http, clock, process)
        .with_cache_dir(Some(dir.path().join("cache")))
        .with_api_base(API)
}

fn fresh_cache(dir: &TestDir) {
    CacheRecord {
        checked_at: 1_000,
        version: None,
        failure_reason: None,
    }
    .write(&dir.path().join("cache"))
    .unwrap();
}

#[test]
fn explicit_runtime_wins_and_broken_explicit_errors() {
    let dir = TestDir::new();
    let binary = dir.join("my-opencode");
    write_executable(&binary, "#!/bin/sh\necho 1.18.31\n");

    let http = MemoryHttp::new();
    let clock = FixedClock::new(1_000);
    let process = FakeProcessHost::new();
    let policy = RuntimePolicy::default();

    let explicit = manager(&dir, policy.clone(), &http, &clock, &process)
        .with_explicit(Some(binary.clone().into_os_string()));
    let selection = explicit.resolve_for_launch().unwrap();
    assert_eq!(selection.source, RuntimeSource::Explicit);
    assert_eq!(selection.path, binary);

    let broken = manager(&dir, policy, &http, &clock, &process)
        .with_explicit(Some(PathBuf::from("/does/not/exist").into_os_string()));
    assert!(broken.resolve_for_launch().is_err());
}

#[test]
fn compatible_system_runtime_is_used_without_installing() {
    let dir = TestDir::new();
    let system = dir.join("opencode-system");
    write_executable(&system, "#!/bin/sh\necho 1.18.31\n");

    let http = MemoryHttp::new();
    let clock = FixedClock::new(1_000);
    let process = FakeProcessHost::new()
        .with_program("opencode", system.clone())
        .with_version(system.clone(), "1.18.31");
    // A fresh cache means no optional upgrade is attempted.
    fresh_cache(&dir);

    let selection = manager(&dir, RuntimePolicy::default(), &http, &clock, &process)
        .resolve_for_launch()
        .unwrap();
    assert_eq!(selection.source, RuntimeSource::System);
    assert_eq!(selection.version, Some(Version::new(1, 18, 31)));
    assert!(!dir.join(".ocg").exists(), "must not install redundantly");
    assert!(process.upgrade_calls().is_empty());
}

/// Same-family invariant: an existing managed runtime keeps winning over a
/// system runtime from the *same* supported OpenCode family — even a newer
/// same-family system release must not displace it.
#[test]
fn existing_managed_runtime_beats_same_family_system() {
    let dir = TestDir::new();
    let managed = install_fake_managed(dir.path(), "1.18.31");
    fresh_cache(&dir);

    let system = dir.join("opencode-system");
    write_executable(&system, "#!/bin/sh\necho 1.18.40\n");
    let process = FakeProcessHost::new()
        .with_program("opencode", system)
        .with_version(dir.join("opencode-system"), "1.18.40");

    let http = MemoryHttp::new();
    let clock = FixedClock::new(1_000);
    let manager = manager(&dir, RuntimePolicy::default(), &http, &clock, &process);
    let selection = manager.resolve_for_launch().unwrap();
    assert_eq!(selection.source, RuntimeSource::Managed);
    assert_eq!(selection.path, managed);
    let report = manager.resolve_for_report();
    assert_eq!(report.source, Some(RuntimeSource::Managed));
    assert_eq!(report.version, Some(Version::new(1, 18, 31)));
}

/// Cross-family invariant: an unpinned project whose managed runtime is an
/// older supported family (V1) must resolve to a newer supported system
/// family (V2) — the managed install stays on disk but stops shadowing the
/// newer runtime. Launch and read-only reporting must agree.
#[test]
fn newer_family_system_runtime_beats_older_family_managed() {
    let dir = TestDir::new();
    install_fake_managed(dir.path(), "1.18.32");
    fresh_cache(&dir);

    let system = dir.join("opencode-system");
    write_executable(&system, "#!/bin/sh\necho 2.0.14\n");
    let process = FakeProcessHost::new()
        .with_program("opencode", system.clone())
        .with_version(system.clone(), "2.0.14");

    let http = MemoryHttp::new();
    let clock = FixedClock::new(1_000);
    let manager = manager(&dir, RuntimePolicy::default(), &http, &clock, &process);

    let selection = manager.resolve_for_launch().unwrap();
    assert_eq!(selection.source, RuntimeSource::System);
    assert_eq!(selection.path, system);
    assert_eq!(selection.version, Some(Version::new(2, 0, 14)));

    // `ocg version` / `ocg doctor` (resolve_for_report) must agree with `ocg run`.
    let report = manager.resolve_for_report();
    assert_eq!(report.source, Some(RuntimeSource::System));
    assert_eq!(report.version, Some(Version::new(2, 0, 14)));

    // The old managed runtime is never deleted or mutated.
    let active = ActiveRuntime::read(dir.path()).unwrap();
    assert_eq!(active.version, Version::new(1, 18, 32));
}

/// Same family on both sides: the managed V2 preference is preserved even
/// against a newer system V2.
#[test]
fn managed_v2_still_beats_newer_system_v2() {
    let dir = TestDir::new();
    let managed = install_fake_managed(dir.path(), "2.0.14");
    fresh_cache(&dir);

    let system = dir.join("opencode-system");
    write_executable(&system, "#!/bin/sh\necho 2.9.1\n");
    let process = FakeProcessHost::new()
        .with_program("opencode", system)
        .with_version(dir.join("opencode-system"), "2.9.1");

    let http = MemoryHttp::new();
    let clock = FixedClock::new(1_000);
    let manager = manager(&dir, RuntimePolicy::default(), &http, &clock, &process);
    let selection = manager.resolve_for_launch().unwrap();
    assert_eq!(selection.source, RuntimeSource::Managed);
    assert_eq!(selection.path, managed);
    let report = manager.resolve_for_report();
    assert_eq!(report.source, Some(RuntimeSource::Managed));
}

/// An explicit `runtime.version` pin is authoritative and managed-only: a V1
/// pin must not silently move the project to a system V2 runtime.
#[test]
fn pinned_managed_v1_beats_newer_family_system() {
    let dir = TestDir::new();
    let managed = install_fake_managed(dir.path(), "1.18.32");
    fresh_cache(&dir);

    let system = dir.join("opencode-system");
    write_executable(&system, "#!/bin/sh\necho 2.0.14\n");
    let process = FakeProcessHost::new()
        .with_program("opencode", system)
        .with_version(dir.join("opencode-system"), "2.0.14");

    let policy = RuntimePolicy {
        version: Some(Version::new(1, 18, 32)),
        ..RuntimePolicy::default()
    };
    let http = MemoryHttp::new();
    let clock = FixedClock::new(1_000);
    let manager = manager(&dir, policy, &http, &clock, &process);
    let selection = manager.resolve_for_launch().unwrap();
    assert_eq!(selection.source, RuntimeSource::Managed);
    assert_eq!(selection.path, managed);
    assert_eq!(selection.version, Some(Version::new(1, 18, 32)));
    let report = manager.resolve_for_report();
    assert_eq!(report.source, Some(RuntimeSource::Managed));
    assert_eq!(report.version, Some(Version::new(1, 18, 32)));
}

/// An unsupported system runtime — no matter how high its version number —
/// must never displace a working managed runtime.
#[test]
fn unsupported_newer_system_never_beats_managed() {
    let dir = TestDir::new();
    let managed = install_fake_managed(dir.path(), "1.18.32");
    fresh_cache(&dir);

    let system = dir.join("opencode-system");
    write_executable(&system, "#!/bin/sh\necho 9.0.0\n");
    let process = FakeProcessHost::new()
        .with_program("opencode", system)
        .with_version(dir.join("opencode-system"), "9.0.0");

    let http = MemoryHttp::new();
    let clock = FixedClock::new(1_000);
    let manager = manager(&dir, RuntimePolicy::default(), &http, &clock, &process);
    let selection = manager.resolve_for_launch().unwrap();
    assert_eq!(selection.source, RuntimeSource::Managed);
    assert_eq!(selection.path, managed);
    let report = manager.resolve_for_report();
    assert_eq!(report.source, Some(RuntimeSource::Managed));
}

/// An explicit `OCG_OPENCODE` stays authoritative even when both a
/// managed runtime and a newer-family system runtime exist.
#[test]
fn explicit_runtime_beats_managed_and_newer_family_system() {
    let dir = TestDir::new();
    install_fake_managed(dir.path(), "1.18.32");
    fresh_cache(&dir);

    let system = dir.join("opencode-system");
    write_executable(&system, "#!/bin/sh\necho 2.0.14\n");
    let explicit_bin = dir.join("my-opencode");
    write_executable(&explicit_bin, "#!/bin/sh\necho 2.0.11\n");
    let process = FakeProcessHost::new()
        .with_program("opencode", system)
        .with_version(dir.join("opencode-system"), "2.0.14")
        .with_version(explicit_bin.clone(), "2.0.11");

    let http = MemoryHttp::new();
    let clock = FixedClock::new(1_000);
    let manager = manager(&dir, RuntimePolicy::default(), &http, &clock, &process)
        .with_explicit(Some(explicit_bin.clone().into_os_string()));
    let selection = manager.resolve_for_launch().unwrap();
    assert_eq!(selection.source, RuntimeSource::Explicit);
    assert_eq!(selection.path, explicit_bin);
    let report = manager.resolve_for_report();
    assert_eq!(report.source, Some(RuntimeSource::Explicit));
}

#[test]
fn missing_runtime_bootstraps_project_local() {
    let dir = TestDir::new();
    let http = http_with_latest("1.18.31");
    let clock = FixedClock::new(1_000);
    let process = FakeProcessHost::new();

    let selection = manager(&dir, RuntimePolicy::default(), &http, &clock, &process)
        .resolve_for_launch()
        .unwrap();
    assert_eq!(selection.source, RuntimeSource::ProjectLocal);
    assert_eq!(selection.version, Some(Version::new(1, 18, 31)));
    assert!(selection.path.is_file());
    assert!(Layout::new(dir.path()).active_path().is_file());
    assert!(fs::read_to_string(dir.path().join(".gitignore"))
        .unwrap()
        .contains(".ocg/"));

    let cache = CacheRecord::read(&dir.path().join("cache")).unwrap();
    assert_eq!(cache.version, Some(Version::new(1, 18, 31)));
}

#[test]
fn due_check_upgrades_managed_latest() {
    let dir = TestDir::new();
    install_fake_managed(dir.path(), "1.18.20");
    // No cache: the check is due.
    let http = http_with_latest("1.18.31");
    let clock = FixedClock::new(1_000);
    let process = FakeProcessHost::new();

    let selection = manager(&dir, RuntimePolicy::default(), &http, &clock, &process)
        .resolve_for_launch()
        .unwrap();
    assert_eq!(selection.source, RuntimeSource::Managed);
    assert_eq!(selection.version, Some(Version::new(1, 18, 31)));
    let active = ActiveRuntime::read(dir.path()).unwrap();
    assert_eq!(active.version, Version::new(1, 18, 31));
}

#[test]
fn failed_upgrade_check_keeps_the_compatible_managed_runtime() {
    let dir = TestDir::new();
    let managed = install_fake_managed(dir.path(), "1.18.20");
    // No fixture: the update check fails.
    let http = MemoryHttp::new();
    let clock = FixedClock::new(1_000);
    let process = FakeProcessHost::new();

    let selection = manager(&dir, RuntimePolicy::default(), &http, &clock, &process)
        .resolve_for_launch()
        .unwrap();
    assert_eq!(selection.source, RuntimeSource::Managed);
    assert_eq!(selection.path, managed);
    assert_eq!(
        selection.version,
        Some(Version::new(1, 18, 20)),
        "must keep the old runtime"
    );
    assert!(!selection.warnings.is_empty(), "failure must warn");
}

#[test]
fn incompatible_system_falls_back_to_a_managed_install() {
    let dir = TestDir::new();
    let system = dir.join("opencode-system");
    write_executable(&system, "#!/bin/sh\necho 1.17.0\n");
    let process = FakeProcessHost::new()
        .with_program("opencode", system.clone())
        .with_version(system.clone(), "1.17.0")
        .with_upgrade_failure(system, "no network");

    let http = http_with_latest("1.18.31");
    let clock = FixedClock::new(1_000);
    let selection = manager(&dir, RuntimePolicy::default(), &http, &clock, &process)
        .resolve_for_launch()
        .unwrap();
    assert_eq!(selection.source, RuntimeSource::ProjectLocal);
    assert_eq!(selection.version, Some(Version::new(1, 18, 31)));
    assert_eq!(
        process.upgrade_calls().len(),
        1,
        "must try system upgrade first"
    );
}

#[test]
fn incompatible_system_without_auto_upgrade_still_bootstraps() {
    let dir = TestDir::new();
    let system = dir.join("opencode-system");
    write_executable(&system, "#!/bin/sh\necho 1.17.0\n");
    let process = FakeProcessHost::new()
        .with_program("opencode", system.clone())
        .with_version(system.clone(), "1.17.0")
        .with_upgrade_failure(system, "must not be called");

    let policy = RuntimePolicy {
        auto_upgrade: false,
        ..RuntimePolicy::default()
    };
    let http = http_with_latest("1.18.31");
    let clock = FixedClock::new(1_000);
    let selection = manager(&dir, policy, &http, &clock, &process)
        .resolve_for_launch()
        .unwrap();
    assert_eq!(selection.source, RuntimeSource::ProjectLocal);
    assert_eq!(selection.version, Some(Version::new(1, 18, 31)));
    assert!(
        process.upgrade_calls().is_empty(),
        "autoUpgrade=false must disable the optional system upgrade"
    );
}

#[test]
fn incompatible_system_install_failure_errors() {
    let dir = TestDir::new();
    let system = dir.join("opencode-system");
    write_executable(&system, "#!/bin/sh\necho 1.17.0\n");
    let process = FakeProcessHost::new()
        .with_program("opencode", system.clone())
        .with_version(system.clone(), "1.17.0")
        .with_upgrade_failure(system, "no network");

    let http = MemoryHttp::new();
    let clock = FixedClock::new(1_000);
    assert!(
        manager(&dir, RuntimePolicy::default(), &http, &clock, &process)
            .resolve_for_launch()
            .is_err()
    );
}

#[test]
fn pinned_version_does_not_silently_advance() {
    let dir = TestDir::new();
    let policy = RuntimePolicy {
        version: Some(Version::new(1, 18, 31)),
        ..RuntimePolicy::default()
    };

    // The tag release (1.18.31) is the only fixture; a "latest" fixture for
    // 1.19.0 must never be consulted while the pin is active.
    let archive = targz(b"#!/bin/sh\necho 1.18.31\n");
    let sha = sha256_hex(&archive);
    let http = MemoryHttp::new()
        .with(
            TAG_URL,
            format!(
                r#"{{"tag_name": "v1.18.31", "assets": [
                    {{"name": "opencode-linux-x64.tar.gz",
                      "browser_download_url": "{ASSET_URL}",
                      "digest": "sha256:{sha}"}}]}}"#
            )
            .into_bytes(),
        )
        .with(ASSET_URL, archive)
        .with(LATEST_URL, release_json("1.19.0", "v1.19.0").into_bytes());
    let clock = FixedClock::new(1_000);
    let process = FakeProcessHost::new();

    let first = manager(&dir, policy.clone(), &http, &clock, &process)
        .resolve_for_launch()
        .unwrap();
    assert_eq!(first.version, Some(Version::new(1, 18, 31)));

    // A second resolve must stay on the pin even though a newer latest exists.
    let second = manager(&dir, policy, &http, &clock, &process)
        .resolve_for_launch()
        .unwrap();
    assert_eq!(second.version, Some(Version::new(1, 18, 31)));
    assert_eq!(second.source, RuntimeSource::Managed);
}

#[test]
fn install_failure_cleans_staging_and_leaves_no_active_runtime() {
    let dir = TestDir::new();
    // The release advertises the asset, but the archive has no `opencode`.
    let bad_archive = targz_named("README.md", b"not the binary");
    let sha = sha256_hex(&bad_archive);
    let http = MemoryHttp::new()
        .with(
            LATEST_URL,
            format!(
                r#"{{"tag_name": "v1.18.31", "assets": [
                    {{"name": "opencode-linux-x64.tar.gz",
                      "browser_download_url": "{ASSET_URL}",
                      "digest": "sha256:{sha}"}}]}}"#
            )
            .into_bytes(),
        )
        .with(ASSET_URL, bad_archive);
    let clock = FixedClock::new(1_000);
    let process = FakeProcessHost::new();

    assert!(
        manager(&dir, RuntimePolicy::default(), &http, &clock, &process)
            .resolve_for_launch()
            .is_err()
    );
    assert!(!Layout::new(dir.path()).active_path().exists());
    assert!(!Layout::new(dir.path())
        .version_dir(&Version::new(1, 18, 31))
        .exists());
    let runtime_root = Layout::new(dir.path()).runtime_root();
    if runtime_root.exists() {
        for entry in fs::read_dir(&runtime_root).unwrap() {
            let name = entry.unwrap().file_name();
            assert!(
                !name.to_string_lossy().starts_with(".staging-"),
                "staging directory was not cleaned: {}",
                name.to_string_lossy()
            );
        }
    }
}

#[test]
fn missing_asset_is_a_hard_error_and_leaves_nothing() {
    let dir = TestDir::new();
    let http = MemoryHttp::new().with(
        LATEST_URL,
        br#"{"tag_name": "v1.18.31", "assets": []}"#.to_vec(),
    );
    let clock = FixedClock::new(1_000);
    let process = FakeProcessHost::new();
    assert!(
        manager(&dir, RuntimePolicy::default(), &http, &clock, &process)
            .resolve_for_launch()
            .is_err()
    );
    assert!(!dir.path().join(".ocg").exists());
}

#[test]
fn forced_upgrade_ignores_a_fresh_cache() {
    let dir = TestDir::new();
    install_fake_managed(dir.path(), "1.18.20");
    fresh_cache(&dir);
    let http = http_with_latest("1.18.31");
    let clock = FixedClock::new(1_000);
    let process = FakeProcessHost::new();

    let outcome = manager(&dir, RuntimePolicy::default(), &http, &clock, &process)
        .upgrade()
        .unwrap();
    assert_eq!(outcome.before.version, Some(Version::new(1, 18, 20)));
    assert_eq!(outcome.after.version, Some(Version::new(1, 18, 31)));
}

#[test]
fn forced_upgrade_preserves_a_pin() {
    let dir = TestDir::new();
    let pinned = install_fake_managed(dir.path(), "1.18.31");
    let policy = RuntimePolicy {
        version: Some(Version::new(1, 18, 31)),
        ..RuntimePolicy::default()
    };
    let http = MemoryHttp::new().with(LATEST_URL, release_json("1.19.0", "v1.19.0").into_bytes());
    let clock = FixedClock::new(1_000);
    let process = FakeProcessHost::new();

    let outcome = manager(&dir, policy, &http, &clock, &process)
        .upgrade()
        .unwrap();
    assert_eq!(outcome.after.version, Some(Version::new(1, 18, 31)));
    assert_eq!(outcome.after.path, pinned);
}

#[test]
fn report_is_non_mutating_when_nothing_is_installed() {
    let dir = TestDir::new();
    let http = MemoryHttp::new();
    let clock = FixedClock::new(1_000);
    let process = FakeProcessHost::new();

    let report =
        manager(&dir, RuntimePolicy::default(), &http, &clock, &process).resolve_for_report();
    assert!(!report.installed());
    assert!(!dir.path().join(".ocg").exists());
    assert!(!dir.path().join("cache").exists());
}

#[test]
fn report_prefers_managed_and_reports_the_path() {
    let dir = TestDir::new();
    let managed = install_fake_managed(dir.path(), "1.18.31");
    let http = MemoryHttp::new();
    let clock = FixedClock::new(1_000);
    let process = FakeProcessHost::new();

    let report =
        manager(&dir, RuntimePolicy::default(), &http, &clock, &process).resolve_for_report();
    assert!(report.installed());
    assert_eq!(report.source, Some(RuntimeSource::Managed));
    assert_eq!(report.path, Some(managed));
    assert_eq!(report.version, Some(Version::new(1, 18, 31)));
}

#[test]
fn policy_object_is_read_from_the_effective_config() {
    let policy = RuntimePolicy::from_config(&json!({
        "runtime": {"channel": "latest", "autoUpgrade": false, "checkIntervalHours": 6, "fallback": "project-local"}
    }))
    .unwrap();
    assert!(!policy.auto_upgrade);
    assert_eq!(policy.check_interval_hours, 6);
    assert!(!policy.is_pinned());
}

#[test]
fn cache_freshness_gates_network_checks() {
    let dir = TestDir::new();
    install_fake_managed(dir.path(), "1.18.20");
    fresh_cache(&dir);
    // An empty HTTP transport would fail if a check were attempted.
    let http = MemoryHttp::new();
    let clock = FixedClock::new(1_000);
    let process = FakeProcessHost::new();
    let selection = manager(&dir, RuntimePolicy::default(), &http, &clock, &process)
        .resolve_for_launch()
        .unwrap();
    assert!(
        selection.warnings.is_empty(),
        "fresh cache must not network"
    );
    assert_eq!(selection.version, Some(Version::new(1, 18, 20)));
}

#[test]
fn expired_cache_triggers_a_check_and_warns_on_failure() {
    let dir = TestDir::new();
    install_fake_managed(dir.path(), "1.18.20");
    CacheRecord {
        checked_at: 0,
        version: None,
        failure_reason: None,
    }
    .write(&dir.path().join("cache"))
    .unwrap();
    let http = MemoryHttp::new();
    let clock = FixedClock::new(1_000_000);
    let process = FakeProcessHost::new();
    let selection = manager(&dir, RuntimePolicy::default(), &http, &clock, &process)
        .resolve_for_launch()
        .unwrap();
    assert_eq!(selection.version, Some(Version::new(1, 18, 20)));
    assert!(!selection.warnings.is_empty());
}

#[test]
fn gitignore_append_is_idempotent() {
    let dir = TestDir::new();
    let gitignore = dir.path().join(".gitignore");
    let mut file = fs::File::create(&gitignore).unwrap();
    file.write_all(b"target/\n").unwrap();
    drop(file);

    let http = http_with_latest("1.18.31");
    let clock = FixedClock::new(1_000);
    let process = FakeProcessHost::new();
    let policy = RuntimePolicy::default();
    manager(&dir, policy.clone(), &http, &clock, &process)
        .resolve_for_launch()
        .unwrap();
    manager(&dir, policy, &http, &clock, &process)
        .resolve_for_launch()
        .unwrap();
    let text = fs::read_to_string(&gitignore).unwrap();
    assert_eq!(text.matches(".ocg/").count(), 1);
    assert!(text.starts_with("target/\n"));
}

// --- system runtime: OpenCode's own `upgrade` ------------------------------

fn system_host(system: &Path, version: &str) -> FakeProcessHost {
    FakeProcessHost::new()
        .with_program("opencode", system)
        .with_version(system, version)
}

#[test]
fn system_due_check_upgrades_in_place_to_the_resolved_target() {
    let dir = TestDir::new();
    let system = dir.join("opencode-system");
    write_executable(&system, "#!/bin/sh\necho 1.18.20\n");
    let process =
        system_host(&system, "1.18.20").with_upgrade_success(system.clone(), Some("1.18.31"));

    let http = http_with_latest("1.18.31");
    let clock = FixedClock::new(1_000);
    let selection = manager(&dir, RuntimePolicy::default(), &http, &clock, &process)
        .resolve_for_launch()
        .unwrap();
    assert_eq!(selection.source, RuntimeSource::System);
    assert_eq!(selection.version, Some(Version::new(1, 18, 31)));
    assert!(!dir.join(".ocg").exists());
    assert_eq!(process.upgrade_calls(), vec![system]);
    assert_eq!(
        process.upgrade_targets(),
        vec![Some(Version::new(1, 18, 31))],
        "the resolved release must be passed to `opencode upgrade`"
    );
    assert_eq!(
        CacheRecord::read(&dir.path().join("cache"))
            .unwrap()
            .version,
        Some(Version::new(1, 18, 31))
    );
}

#[test]
fn system_due_check_failure_keeps_old_and_records_a_negative_check() {
    let dir = TestDir::new();
    let system = dir.join("opencode-system");
    write_executable(&system, "#!/bin/sh\necho 1.18.20\n");
    let process =
        system_host(&system, "1.18.20").with_upgrade_failure(system.clone(), "no network");

    let http = http_with_latest("1.18.31");
    let clock = FixedClock::new(1_000);
    let selection = manager(&dir, RuntimePolicy::default(), &http, &clock, &process)
        .resolve_for_launch()
        .unwrap();
    assert_eq!(selection.source, RuntimeSource::System);
    assert_eq!(selection.version, Some(Version::new(1, 18, 20)));
    assert!(!selection.warnings.is_empty());
    assert_eq!(
        process.upgrade_targets(),
        vec![Some(Version::new(1, 18, 31))],
        "a resolved target must be attempted before failing"
    );
    assert_eq!(
        CacheRecord::read(&dir.path().join("cache"))
            .unwrap()
            .version,
        None
    );
}

#[test]
fn system_lookup_failure_keeps_old_and_records_a_negative_check() {
    let dir = TestDir::new();
    let system = dir.join("opencode-system");
    write_executable(&system, "#!/bin/sh\necho 1.18.20\n");
    let process = system_host(&system, "1.18.20");

    let http = MemoryHttp::new();
    let clock = FixedClock::new(1_000);
    let selection = manager(&dir, RuntimePolicy::default(), &http, &clock, &process)
        .resolve_for_launch()
        .unwrap();
    assert_eq!(selection.source, RuntimeSource::System);
    assert_eq!(selection.version, Some(Version::new(1, 18, 20)));
    assert!(!selection.warnings.is_empty());
    assert!(
        process.upgrade_calls().is_empty(),
        "no target can be attempted when the lookup fails"
    );
    assert_eq!(
        CacheRecord::read(&dir.path().join("cache"))
            .unwrap()
            .version,
        None
    );
}

#[test]
fn system_failed_check_is_not_retried_within_the_interval() {
    let dir = TestDir::new();
    let system = dir.join("opencode-system");
    write_executable(&system, "#!/bin/sh\necho 1.18.20\n");
    let process =
        system_host(&system, "1.18.20").with_upgrade_failure(system.clone(), "no network");

    let http = http_with_latest("1.18.31");
    let clock = FixedClock::new(1_000);
    let policy = RuntimePolicy::default();

    let first = manager(&dir, policy.clone(), &http, &clock, &process)
        .resolve_for_launch()
        .unwrap();
    assert!(!first.warnings.is_empty());
    assert_eq!(process.upgrade_calls().len(), 1);
    assert_eq!(
        http.requests().len(),
        1,
        "the release must be resolved once for the failed upgrade"
    );

    let second = manager(&dir, policy, &http, &clock, &process)
        .resolve_for_launch()
        .unwrap();
    assert!(
        second
            .warnings
            .iter()
            .any(|warning| warning.contains("last OpenCode update check failed")),
        "cached failure reason must remain visible: {:?}",
        second.warnings
    );
    assert_eq!(
        process.upgrade_calls().len(),
        1,
        "must not upgrade again within the interval"
    );
    assert_eq!(
        http.requests().len(),
        1,
        "must not resolve again within the interval"
    );
}

#[test]
fn system_rate_limited_check_is_cached() {
    let dir = TestDir::new();
    let system = dir.join("opencode-system");
    write_executable(&system, "#!/bin/sh\necho 1.18.20\n");
    let process = system_host(&system, "1.18.20");

    let http = MemoryHttp::new().with_status(
        LATEST_URL,
        403,
        &[
            ("x-ratelimit-limit", "60"),
            ("x-ratelimit-remaining", "0"),
            ("x-ratelimit-resource", "core"),
            ("retry-after", "60"),
        ],
        b"{}".to_vec(),
    );
    let clock = FixedClock::new(1_000);
    let policy = RuntimePolicy::default();

    let first = manager(&dir, policy.clone(), &http, &clock, &process)
        .resolve_for_launch()
        .unwrap();
    assert_eq!(first.source, RuntimeSource::System);
    assert_eq!(first.version, Some(Version::new(1, 18, 20)));
    assert!(
        first
            .warnings
            .iter()
            .any(|warning| warning.contains("rate limit")),
        "a rate-limited lookup must say so: {:?}",
        first.warnings
    );
    assert!(process.upgrade_calls().is_empty());

    let second = manager(&dir, policy, &http, &clock, &process)
        .resolve_for_launch()
        .unwrap();
    assert!(
        second
            .warnings
            .iter()
            .any(|warning| warning.contains("last OpenCode update check was rate-limited")),
        "cached rate-limit reason must remain visible: {:?}",
        second.warnings
    );
    assert_eq!(http.requests().len(), 1, "must not hammer the API");
}

#[test]
fn incompatible_system_upgrade_success_uses_the_system_runtime() {
    let dir = TestDir::new();
    let system = dir.join("opencode-system");
    write_executable(&system, "#!/bin/sh\necho 1.17.0\n");
    let process =
        system_host(&system, "1.17.0").with_upgrade_success(system.clone(), Some("1.18.31"));

    let http = http_with_latest("1.18.31");
    let clock = FixedClock::new(1_000);
    let selection = manager(&dir, RuntimePolicy::default(), &http, &clock, &process)
        .resolve_for_launch()
        .unwrap();
    assert_eq!(selection.source, RuntimeSource::System);
    assert_eq!(selection.version, Some(Version::new(1, 18, 31)));
    assert!(!dir.join(".ocg").exists());
    assert_eq!(process.upgrade_calls().len(), 1);
    assert_eq!(
        process.upgrade_targets(),
        vec![Some(Version::new(1, 18, 31))]
    );
}

#[test]
fn auto_upgrade_false_still_bootstraps_a_missing_runtime() {
    let dir = TestDir::new();
    let policy = RuntimePolicy {
        auto_upgrade: false,
        ..RuntimePolicy::default()
    };
    let http = http_with_latest("1.18.31");
    let clock = FixedClock::new(1_000);
    let process = FakeProcessHost::new();

    let selection = manager(&dir, policy, &http, &clock, &process)
        .resolve_for_launch()
        .unwrap();
    assert_eq!(selection.source, RuntimeSource::ProjectLocal);
    assert_eq!(selection.version, Some(Version::new(1, 18, 31)));
}

#[test]
fn auto_upgrade_false_does_not_upgrade_a_compatible_system_runtime() {
    let dir = TestDir::new();
    let system = dir.join("opencode-system");
    write_executable(&system, "#!/bin/sh\necho 1.18.20\n");
    let process = system_host(&system, "1.18.20");

    let policy = RuntimePolicy {
        auto_upgrade: false,
        ..RuntimePolicy::default()
    };
    let http = MemoryHttp::new();
    let clock = FixedClock::new(1_000);
    let selection = manager(&dir, policy, &http, &clock, &process)
        .resolve_for_launch()
        .unwrap();
    assert_eq!(selection.source, RuntimeSource::System);
    assert_eq!(selection.version, Some(Version::new(1, 18, 20)));
    assert!(process.upgrade_calls().is_empty());
}

#[test]
fn managed_failed_check_is_not_retried_within_the_interval() {
    let dir = TestDir::new();
    install_fake_managed(dir.path(), "1.18.20");
    let http = MemoryHttp::new();
    let clock = FixedClock::new(1_000);
    let process = FakeProcessHost::new();
    let policy = RuntimePolicy::default();

    let first = manager(&dir, policy.clone(), &http, &clock, &process)
        .resolve_for_launch()
        .unwrap();
    assert!(!first.warnings.is_empty());
    assert_eq!(http.requests().len(), 1);

    let second = manager(&dir, policy, &http, &clock, &process)
        .resolve_for_launch()
        .unwrap();
    assert!(
        second
            .warnings
            .iter()
            .any(|warning| warning.contains("last OpenCode update check failed")),
        "the cached failure reason must stay visible without a new check: {:?}",
        second.warnings
    );
    assert_eq!(
        http.requests().len(),
        1,
        "must not request again within the interval"
    );
}

// --- forced upgrade maintains the active source ----------------------------

#[test]
fn forced_upgrade_on_a_newer_system_runtime_never_downgrades_or_installs_managed() {
    let dir = TestDir::new();
    let system = dir.join("opencode-system");
    write_executable(&system, "#!/bin/sh\necho 1.19.0\n");
    let process = system_host(&system, "1.19.0").with_upgrade_success(system.clone(), None);

    let http = MemoryHttp::new().with(LATEST_URL, release_json("1.18.31", "v1.18.31").into_bytes());
    let clock = FixedClock::new(1_000);
    let outcome = manager(&dir, RuntimePolicy::default(), &http, &clock, &process)
        .upgrade()
        .unwrap();
    assert_eq!(outcome.after.source, RuntimeSource::System);
    assert_eq!(outcome.after.version, Some(Version::new(1, 19, 0)));
    assert!(!Layout::new(dir.path()).active_path().exists());
    assert_eq!(
        http.requests(),
        vec![LATEST_URL.to_string()],
        "the latest release must be resolved once through OCG's transport"
    );
    assert!(
        process.upgrade_calls().is_empty(),
        "a newer system runtime must not be downgraded"
    );
}

#[test]
fn forced_upgrade_never_downgrades_a_newer_managed_runtime() {
    let dir = TestDir::new();
    let managed = install_fake_managed(dir.path(), "1.19.0");
    let http = http_with_latest("1.18.31");
    let clock = FixedClock::new(1_000);
    let process = FakeProcessHost::new();

    let outcome = manager(&dir, RuntimePolicy::default(), &http, &clock, &process)
        .upgrade()
        .unwrap();
    assert_eq!(outcome.after.source, RuntimeSource::Managed);
    assert_eq!(outcome.after.version, Some(Version::new(1, 19, 0)));
    assert_eq!(outcome.after.path, managed);
    assert_eq!(
        ActiveRuntime::read(dir.path()).unwrap().version,
        Version::new(1, 19, 0)
    );
}

#[test]
fn forced_upgrade_keeps_a_compatible_system_runtime_after_failure() {
    let dir = TestDir::new();
    let system = dir.join("opencode-system");
    write_executable(&system, "#!/bin/sh\necho 1.18.20\n");
    let process = system_host(&system, "1.18.20").with_upgrade_failure(system, "offline");

    let http = MemoryHttp::new();
    let clock = FixedClock::new(1_000);
    let outcome = manager(&dir, RuntimePolicy::default(), &http, &clock, &process)
        .upgrade()
        .unwrap();
    assert_eq!(outcome.after.source, RuntimeSource::System);
    assert_eq!(outcome.after.version, Some(Version::new(1, 18, 20)));
    assert!(!outcome.warnings.is_empty());
}

#[test]
fn forced_upgrade_falls_back_to_managed_after_an_incompatible_system_failure() {
    let dir = TestDir::new();
    let system = dir.join("opencode-system");
    write_executable(&system, "#!/bin/sh\necho 1.17.0\n");
    let process = system_host(&system, "1.17.0").with_upgrade_failure(system, "offline");

    let http = http_with_latest("1.18.31");
    let clock = FixedClock::new(1_000);
    let outcome = manager(&dir, RuntimePolicy::default(), &http, &clock, &process)
        .upgrade()
        .unwrap();
    assert_eq!(outcome.after.source, RuntimeSource::ProjectLocal);
    assert_eq!(outcome.after.version, Some(Version::new(1, 18, 31)));
}

#[test]
fn forced_upgrade_uses_an_explicit_runtime_upgrade() {
    let dir = TestDir::new();
    let explicit = dir.join("explicit-opencode");
    write_executable(&explicit, "#!/bin/sh\necho 1.18.20\n");
    let process = FakeProcessHost::new()
        .with_version(explicit.clone(), "1.18.20")
        .with_upgrade_success(explicit.clone(), Some("1.18.31"));

    let http = MemoryHttp::new();
    let clock = FixedClock::new(1_000);
    let runtime = manager(&dir, RuntimePolicy::default(), &http, &clock, &process)
        .with_explicit(Some(explicit.clone().into_os_string()));
    let outcome = runtime.upgrade().unwrap();
    assert_eq!(outcome.after.source, RuntimeSource::Explicit);
    assert_eq!(outcome.after.version, Some(Version::new(1, 18, 31)));
    assert!(!Layout::new(dir.path()).active_path().exists());
    assert_eq!(process.upgrade_calls(), vec![explicit]);
}

// --- install hardening ------------------------------------------------------

#[test]
fn managed_download_without_a_digest_fails_closed() {
    let dir = TestDir::new();
    let archive = targz(b"#!/bin/sh\necho 1.18.31\n");
    // The asset deliberately has no `digest` field.
    let http = MemoryHttp::new()
        .with(
            LATEST_URL,
            format!(
                r#"{{"tag_name": "v1.18.31", "assets": [
                    {{"name": "opencode-linux-x64.tar.gz",
                      "browser_download_url": "{ASSET_URL}"}}]}}"#
            )
            .into_bytes(),
        )
        .with(ASSET_URL, archive);
    let clock = FixedClock::new(1_000);
    let process = FakeProcessHost::new();

    assert!(
        manager(&dir, RuntimePolicy::default(), &http, &clock, &process)
            .resolve_for_launch()
            .is_err()
    );
    assert!(!Layout::new(dir.path()).active_path().exists());
    assert!(!dir.path().join(".ocg").exists());
}

#[cfg(unix)]
#[test]
fn install_repairs_a_non_executable_partial_binary() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TestDir::new();
    let version = Version::new(1, 18, 31);
    let binary = Layout::new(dir.path()).binary_path(&version);
    fs::create_dir_all(binary.parent().unwrap()).unwrap();
    fs::write(&binary, b"partial").unwrap();
    let mut permissions = fs::metadata(&binary).unwrap().permissions();
    permissions.set_mode(0o644);
    fs::set_permissions(&binary, permissions).unwrap();

    let archive = targz(b"#!/bin/sh\necho 1.18.31\n");
    let sha = sha256_hex(&archive);
    let release = parse_release(&format!(
        r#"{{"tag_name": "v1.18.31", "assets": [
            {{"name": "opencode-linux-x64.tar.gz",
              "browser_download_url": "{ASSET_URL}",
              "digest": "sha256:{sha}"}}]}}"#
    ))
    .unwrap();
    let http = MemoryHttp::new().with(ASSET_URL, archive);
    let clock = FixedClock::new(1_000);

    install_opencode(dir.path(), platform(), &version, &release, &http, &clock).unwrap();
    assert!(is_executable(&binary));
    assert_eq!(fs::read(&binary).unwrap(), b"#!/bin/sh\necho 1.18.31\n");
}

// --- runtime compatibility boundary (V1 vs V2) ------------------------------

#[test]
fn v1_adapter_preserves_the_supported_118_contract() {
    let detected = detect("1.18.31").unwrap();
    let adapter = adapter_for(&detected);
    assert_eq!(adapter.major(), compat::Major::V1);
    assert_eq!(adapter.plugin_key(), "plugin");
    assert_eq!(adapter.task_key(), "task");
    assert_eq!(adapter.launch_mode(), compat::LaunchMode::Exec);
    assert_eq!(
        adapter.lead_selection(),
        compat::LeadSelectionMode::RequestMessage
    );
    // The v1 generated adapter still enforces the exact Lead request contract.
    assert!(adapter
        .plugin_source()
        .contains("output.message.agent = contract.agent"));
    assert!(adapter.is_task_tool("task"));
}

#[test]
fn a_2_0_x_runtime_selects_the_v2_adapter() {
    for raw in ["2.0.0", "2.0.10", "2.0.11", "2.1.2", "opencode v2.0.10"] {
        let detected = detect(raw).unwrap();
        assert_eq!(detected.major(), compat::Major::V2, "{raw}");
        let adapter = adapter_for(&detected);
        assert_eq!(adapter.plugin_key(), "plugin", "{raw}");
        assert_eq!(adapter.task_key(), "subagent", "{raw}");
        assert_eq!(adapter.launch_mode(), compat::LaunchMode::Daemon, "{raw}");
        assert_eq!(
            adapter.lead_selection(),
            compat::LeadSelectionMode::Session,
            "{raw}"
        );
        assert!(adapter.is_task_tool("subagent"), "{raw}");
    }
    assert_eq!(
        classify(Version::new(2, 0, 10)).unwrap().major(),
        compat::Major::V2
    );
    assert_eq!(compat::v2_verified_baseline(), Version::new(2, 0, 11));
}

#[test]
fn an_unsupported_or_incompatible_major_fails_clearly() {
    for raw in ["3.0.0", "0.1.0", "1.17.0", "garbage"] {
        let error = detect(raw).unwrap_err();
        assert!(!error.to_string().is_empty(), "{raw}");
    }
}

#[test]
fn the_runtime_major_is_detected_through_the_process_host() {
    let dir = TestDir::new();
    let binary = dir.join("opencode-v2");
    write_executable(&binary, "#!/bin/sh\necho 2.0.10\n");
    let process = FakeProcessHost::new().with_default_version("2.0.10");
    let detected = detect_from_host(&process, &binary).unwrap();
    assert_eq!(detected.major(), compat::Major::V2);
    assert_eq!(detected.version(), &Version::new(2, 0, 10));
}

fn compat_lead() -> LeadSelection {
    LeadSelection {
        level: "high".to_string(),
        agent: "lead-high".to_string(),
        provider_id: "openai".to_string(),
        model_id: "gpt-6-astra".to_string(),
        variant: Some("low".to_string()),
    }
}

#[test]
fn v2_session_level_lead_selection_is_deterministic() {
    let lead = compat_lead();
    let run = || {
        let mut client = MemorySessionClient::new().with_session_id("session-7");
        compat::select_session_lead(&mut client, &lead).unwrap()
    };
    let first = run();
    let second = run();
    assert_eq!(first, second, "session selection must be deterministic");
    assert_eq!(first.session_id, "session-7");
    assert_eq!(first.lead, lead);
}

#[test]
fn a_contradictory_active_lead_fails_selection() {
    let lead = compat_lead();
    let effective = EffectiveLead {
        agent: Some("lead-low".to_string()),
        provider_id: Some("openai".to_string()),
        model_id: Some("gpt-5.6-sol".to_string()),
        variant: Some("low".to_string()),
    };
    let mut client = MemorySessionClient::new().with_effective(effective);
    let error = compat::select_session_lead(&mut client, &lead).unwrap_err();
    assert!(error.to_string().contains("contradictory"), "{error}");

    // An unavailable effective Lead is equally a failure.
    let mut unavailable = MemorySessionClient::new().failing_effective("daemon offline");
    let error = compat::select_session_lead(&mut unavailable, &lead).unwrap_err();
    assert!(error.to_string().contains("daemon offline"), "{error}");
}

#[test]
fn optional_observations_warn_and_continue() {
    let mut warnings = Vec::new();
    let observation: Observation<u32> = compat::observe_optional(
        Err(OcgError::config("catalogue offline")),
        "catalogue",
        &mut warnings,
    );
    assert_eq!(observation.unavailable_reason(), Some("catalogue offline"));
    assert_eq!(warnings.len(), 1);
    assert!(
        warnings[0].contains("optional catalogue"),
        "{}",
        warnings[0]
    );

    let observed: Observation<u32> = compat::observe_optional(Ok(3), "catalogue", &mut warnings);
    assert_eq!(observed.observed(), Some(3));
    assert_eq!(warnings.len(), 1, "a successful probe adds no warning");
}
