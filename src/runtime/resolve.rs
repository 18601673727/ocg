//! Runtime discovery and resolution.
//!
//! Pure decision logic plus the small amount of mutation a launch needs
//! (installing or re-activating a managed runtime and recording an update
//! check). All side effects go through the injected [`HttpTransport`],
//! [`Clock`] and [`ProcessHost`], so the whole pipeline is testable offline.

use crate::clock::Clock;
use crate::error::{OcgError, Result};
use crate::http::HttpTransport;
use crate::platform::Platform;
use crate::process::{is_executable, ProcessHost};
use crate::proxy::ChildProxyEnv;
use crate::runtime::cache::{self, CacheRecord};
use crate::runtime::compat;
use crate::runtime::install::{ensure_gitignore, install_opencode, ActiveRuntime, Layout};
use crate::runtime::policy::{self, RuntimePolicy};
use crate::runtime::release::{self, Release};
use crate::runtime::{DEFAULT_API_BASE, OCG_REPO, OPENCODE_REPO};
use semver::Version;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};

/// Where the OpenCode executable came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeSource {
    /// An explicit executable from the environment.
    Explicit,
    /// An existing managed project runtime.
    Managed,
    /// A usable `opencode` found on `PATH`.
    System,
    /// A project-local runtime that was bootstrapped for this project.
    ProjectLocal,
}

impl RuntimeSource {
    pub fn label(self) -> &'static str {
        match self {
            RuntimeSource::Explicit => "explicit",
            RuntimeSource::Managed => "managed",
            RuntimeSource::System => "system PATH",
            RuntimeSource::ProjectLocal => "project-local",
        }
    }
}

/// The runtime a launch will use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeSelection {
    pub source: RuntimeSource,
    pub path: PathBuf,
    pub version: Option<Version>,
    pub warnings: Vec<String>,
}

/// A read-only runtime snapshot for `ocg version` and `ocg doctor`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RuntimeReport {
    pub source: Option<RuntimeSource>,
    pub path: Option<PathBuf>,
    pub version: Option<Version>,
    pub warnings: Vec<String>,
    pub error: Option<String>,
}

impl RuntimeReport {
    pub fn installed(&self) -> bool {
        self.source.is_some() && self.error.is_none()
    }

    fn from_selection(selection: RuntimeSelection) -> Self {
        Self {
            source: Some(selection.source),
            path: Some(selection.path),
            version: selection.version,
            warnings: selection.warnings,
            error: None,
        }
    }
}

/// Before/after of an OpenCode runtime upgrade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpgradeOutcome {
    pub before: RuntimeReport,
    pub after: RuntimeSelection,
    /// Non-fatal problems encountered while upgrading.
    pub warnings: Vec<String>,
}

/// Injected dependencies and per-project settings.
pub struct RuntimeManager<'a> {
    pub project_root: PathBuf,
    pub policy: RuntimePolicy,
    pub platform: Platform,
    pub explicit: Option<OsString>,
    pub cache_dir: Option<PathBuf>,
    pub api_base: String,
    pub opencode_repo: String,
    pub ocg_repo: String,
    pub http: &'a dyn HttpTransport,
    pub clock: &'a dyn Clock,
    pub process: &'a dyn ProcessHost,
    /// The resolved proxy policy applied to any child process this manager
    /// starts (system `opencode upgrade`).
    pub proxy_env: ChildProxyEnv,
}

impl<'a> RuntimeManager<'a> {
    pub fn new(
        project_root: impl Into<PathBuf>,
        policy: RuntimePolicy,
        platform: Platform,
        http: &'a dyn HttpTransport,
        clock: &'a dyn Clock,
        process: &'a dyn ProcessHost,
    ) -> Self {
        Self {
            project_root: project_root.into(),
            policy,
            platform,
            explicit: None,
            cache_dir: cache::platform_cache_dir(),
            api_base: DEFAULT_API_BASE.to_string(),
            opencode_repo: OPENCODE_REPO.to_string(),
            ocg_repo: OCG_REPO.to_string(),
            http,
            clock,
            process,
            proxy_env: ChildProxyEnv::default(),
        }
    }

    /// Apply the resolved proxy policy to child processes.
    pub fn with_proxy_env(mut self, proxy_env: ChildProxyEnv) -> Self {
        self.proxy_env = proxy_env;
        self
    }

    pub fn with_explicit(mut self, explicit: Option<OsString>) -> Self {
        self.explicit = explicit;
        self
    }

    pub fn with_cache_dir(mut self, cache_dir: Option<PathBuf>) -> Self {
        self.cache_dir = cache_dir;
        self
    }

    pub fn with_api_base(mut self, api_base: impl Into<String>) -> Self {
        self.api_base = api_base.into();
        self
    }

    pub fn with_repos(mut self, opencode: impl Into<String>, ocg: impl Into<String>) -> Self {
        self.opencode_repo = opencode.into();
        self.ocg_repo = ocg.into();
        self
    }

    /// Resolve the runtime for a launch, installing or upgrading when needed.
    pub fn resolve_for_launch(&self) -> Result<RuntimeSelection> {
        if let Some(explicit) = &self.explicit {
            return self.resolve_explicit(explicit);
        }

        // A pin is managed-only: the system runtime is never upgraded past it.
        if let Some(pin) = self.policy.version.clone() {
            return self.resolve_pinned(&pin);
        }

        if let Some(active) = ActiveRuntime::read(&self.project_root) {
            // Unpinned only (a pin returned above): a managed runtime from an
            // older supported OpenCode family must not shadow a system
            // runtime from a newer supported family. The managed install is
            // left on disk untouched — it simply stops winning resolution.
            if let Some(system) = self.system_selection() {
                if system_is_newer_supported_family(&active.version, system.version.as_ref()) {
                    return self.resolve_system(system);
                }
            }
            return self.resolve_managed(active);
        }

        if let Some(system) = self.system_selection() {
            return self.resolve_system(system);
        }

        self.bootstrap(Vec::new())
    }

    /// Non-mutating runtime snapshot. Never downloads, installs or upgrades.
    pub fn resolve_for_report(&self) -> RuntimeReport {
        if let Some(explicit) = &self.explicit {
            return match self.resolve_explicit(explicit) {
                Ok(selection) => RuntimeReport::from_selection(selection),
                Err(error) => RuntimeReport {
                    source: Some(RuntimeSource::Explicit),
                    path: Some(PathBuf::from(explicit)),
                    version: None,
                    warnings: Vec::new(),
                    error: Some(error.to_string()),
                },
            };
        }

        if let Some(active) = ActiveRuntime::read(&self.project_root) {
            let mut warnings = Vec::new();
            if let Some(pin) = &self.policy.version {
                if active.version != *pin {
                    warnings.push(format!(
                        "runtime.version pins {pin}, but the managed runtime is {}",
                        active.version
                    ));
                }
            } else if let Some(system) = self.system_selection() {
                // Read-only reporting resolves exactly like a launch: an
                // unpinned older-family managed runtime must not shadow a
                // newer-family system runtime here either, otherwise
                // `ocg version`/`ocg doctor` would disagree with `ocg run`.
                if system_is_newer_supported_family(&active.version, system.version.as_ref()) {
                    return RuntimeReport::from_selection(system);
                }
            }
            return RuntimeReport {
                source: Some(RuntimeSource::Managed),
                path: Some(active.path),
                version: Some(active.version),
                warnings,
                error: None,
            };
        }

        if let Some(system) = self.system_selection() {
            return RuntimeReport::from_selection(system);
        }

        RuntimeReport::default()
    }

    /// Force an upgrade of whatever runtime is active.
    ///
    /// The active source is maintained, not replaced: a system runtime uses
    /// OpenCode's own `upgrade`, an explicit runtime is authoritative, and a
    /// managed runtime uses a direct managed release install. A missing
    /// runtime bootstraps.
    pub fn upgrade(&self) -> Result<UpgradeOutcome> {
        let before = self.resolve_for_report();
        let mut warnings = Vec::new();

        let after = if let Some(error) = &before.error {
            // A broken explicit executable is authoritative and must error.
            return Err(OcgError::config(error.clone()));
        } else if let Some(source) = before.source {
            match source {
                RuntimeSource::Explicit => self.force_upgrade_explicit(&before, &mut warnings)?,
                RuntimeSource::Managed | RuntimeSource::ProjectLocal => {
                    self.force_upgrade_managed(&before, &mut warnings)?
                }
                RuntimeSource::System => self.force_upgrade_system(&before, &mut warnings)?,
            }
        } else {
            self.bootstrap(Vec::new())?
        };

        Ok(UpgradeOutcome {
            before,
            after,
            warnings,
        })
    }

    fn resolve_explicit(&self, raw: &OsStr) -> Result<RuntimeSelection> {
        let candidate = PathBuf::from(raw);
        let path = if is_bare_name(&candidate) {
            self.process
                .find_in_path(&raw.to_string_lossy())
                .ok_or_else(|| {
                    OcgError::config(format!(
                        "explicit OpenCode '{}' was not found on PATH",
                        raw.to_string_lossy()
                    ))
                })?
        } else if is_executable(&candidate) {
            candidate
        } else {
            return Err(OcgError::config(format!(
                "explicit OpenCode '{}' is not an executable file",
                candidate.display()
            )));
        };
        let version = self.probe_version(&path);
        Ok(RuntimeSelection {
            source: RuntimeSource::Explicit,
            path,
            version,
            warnings: Vec::new(),
        })
    }

    fn resolve_pinned(&self, pin: &Version) -> Result<RuntimeSelection> {
        if let Some(active) = ActiveRuntime::read(&self.project_root) {
            if active.version == *pin {
                return Ok(RuntimeSelection {
                    source: RuntimeSource::Managed,
                    path: active.path,
                    version: Some(active.version),
                    warnings: Vec::new(),
                });
            }
        }

        let binary = Layout::new(&self.project_root).binary_path(pin);
        if is_executable(&binary) {
            ensure_gitignore(&self.project_root)?;
            ActiveRuntime {
                version: pin.clone(),
                path: binary.clone(),
                installed_at: self.clock.now_unix(),
            }
            .write(&self.project_root)?;
            return Ok(RuntimeSelection {
                source: RuntimeSource::Managed,
                path: binary,
                version: Some(pin.clone()),
                warnings: Vec::new(),
            });
        }

        let release =
            release::fetch_release_by_version(self.http, &self.api_base, &self.opencode_repo, pin)?;
        self.install_release(&release, RuntimeSource::ProjectLocal)
    }

    fn resolve_managed(&self, active: ActiveRuntime) -> Result<RuntimeSelection> {
        let mut warnings = Vec::new();
        self.append_cached_check_warning(&mut warnings);

        if self.policy.allows_auto_upgrade() && self.check_due() {
            match self.check_and_upgrade(&active.version) {
                Ok(Some(selection)) => return Ok(selection),
                Ok(None) => {}
                Err(error) => {
                    // Record the failure so a compatible runtime does not
                    // retry the check on every launch.
                    self.record_failed_check(&error);
                    warnings.push(format!("could not check for an OpenCode update: {error}"));
                }
            }
        }

        if active.version < policy::min_opencode_version() {
            if self.policy.allows_auto_upgrade() {
                return self.bootstrap(warnings);
            }
            return Err(OcgError::config(format!(
                "the managed OpenCode {} is below the required {} floor",
                active.version,
                policy::min_opencode_version()
            )));
        }

        Ok(RuntimeSelection {
            source: RuntimeSource::Managed,
            path: active.path,
            version: Some(active.version),
            warnings,
        })
    }

    /// Decide what to do with a system `opencode` found on `PATH`.
    ///
    /// OCG resolves the latest OpenCode release through its own
    /// transport and then asks the system runtime to move to that exact
    /// version. A compatible runtime is always kept if the lookup or the
    /// upgrade fails, and a failed check is cached so a launch does not hammer
    /// the network (including when the failure is a rate limit).
    fn resolve_system(&self, mut system: RuntimeSelection) -> Result<RuntimeSelection> {
        let mut warnings = std::mem::take(&mut system.warnings);
        self.append_cached_check_warning(&mut warnings);
        let compatible = is_compatible(&system.version);
        let auto = self.policy.allows_auto_upgrade();

        if compatible {
            if auto && self.check_due() {
                let release = match release::fetch_latest_release(
                    self.http,
                    &self.api_base,
                    &self.opencode_repo,
                ) {
                    Ok(release) => release,
                    Err(error) => {
                        // Negative check: do not retry on every launch.
                        self.record_failed_check(&error);
                        warnings.push(format!(
                            "could not check for an OpenCode update; continuing with {}: {error}",
                            describe_version(&system.version)
                        ));
                        return Ok(keep_system(system, warnings));
                    }
                };
                if system
                    .version
                    .as_ref()
                    .map(|current| release.version <= *current)
                    .unwrap_or(false)
                {
                    self.record_check(Some(release.version));
                    return Ok(keep_system(system, warnings));
                }
                return self.upgrade_compatible_system(system, &release, warnings);
            }
            return Ok(keep_system(system, warnings));
        }

        // Unusable or incompatible system runtime.
        if auto {
            match release::fetch_latest_release(self.http, &self.api_base, &self.opencode_repo) {
                Ok(release) => {
                    match self.process.upgrade(
                        &system.path,
                        Some(&release.version),
                        &self.proxy_env,
                    ) {
                        Ok(_) => {
                            let version = self.probe_version(&system.path);
                            if is_compatible(&version) {
                                self.record_check(version.clone());
                                return Ok(RuntimeSelection {
                                    source: RuntimeSource::System,
                                    path: system.path,
                                    version,
                                    warnings,
                                });
                            }
                            warnings.push(format!(
                                "the system OpenCode at {} is still not compatible after `opencode upgrade {}`",
                                system.path.display(),
                                release.version
                            ));
                        }
                        Err(error) => {
                            warnings.push(format!(
                                "the system OpenCode at {} is not compatible and `opencode upgrade {}` failed: {error}",
                                system.path.display(),
                                release.version
                            ));
                        }
                    }
                    return self.bootstrap_release(&release, warnings);
                }
                Err(error) => {
                    warnings.push(format!(
                        "the system OpenCode at {} is not compatible and the latest release could not be resolved: {error}",
                        system.path.display()
                    ));
                }
            }
        } else {
            warnings.push(format!(
                "the system OpenCode at {} is not compatible (version {}); automatic upgrades are disabled",
                system.path.display(),
                describe_version(&system.version)
            ));
        }

        // The project-local fallback is required even when autoUpgrade is off.
        self.bootstrap(warnings)
    }

    /// Upgrade a compatible system runtime to an already resolved release.
    fn upgrade_compatible_system(
        &self,
        system: RuntimeSelection,
        release: &Release,
        mut warnings: Vec<String>,
    ) -> Result<RuntimeSelection> {
        match self
            .process
            .upgrade(&system.path, Some(&release.version), &self.proxy_env)
        {
            Ok(_) => {
                let version = self.probe_version(&system.path);
                self.record_check(version.clone());
                if is_compatible(&version) {
                    return Ok(RuntimeSelection {
                        source: RuntimeSource::System,
                        path: system.path,
                        version,
                        warnings,
                    });
                }
                warnings.push(format!(
                    "the system OpenCode at {} is still not compatible after `opencode upgrade {}`",
                    system.path.display(),
                    release.version
                ));
                self.bootstrap_release(release, warnings)
            }
            Err(error) => {
                // Negative check: do not retry on every launch.
                self.record_failed_check(&error);
                warnings.push(format!(
                    "could not upgrade the system OpenCode at {}; continuing with {}: {error}",
                    system.path.display(),
                    describe_version(&system.version)
                ));
                Ok(keep_system(system, warnings))
            }
        }
    }

    fn system_selection(&self) -> Option<RuntimeSelection> {
        let path = self.process.find_in_path("opencode")?;
        let version = self.probe_version(&path);
        let mut warnings = Vec::new();
        if version.is_none() {
            warnings.push(format!(
                "could not determine the version of the system OpenCode at {}",
                path.display()
            ));
        }
        Some(RuntimeSelection {
            source: RuntimeSource::System,
            path,
            version,
            warnings,
        })
    }

    fn check_and_upgrade(&self, current: &Version) -> Result<Option<RuntimeSelection>> {
        let release =
            release::fetch_latest_release(self.http, &self.api_base, &self.opencode_repo)?;
        if release.version <= *current {
            self.record_check(Some(release.version.clone()));
            return Ok(None);
        }
        let selection = self.install_release(&release, RuntimeSource::Managed)?;
        self.record_check(Some(release.version.clone()));
        Ok(Some(selection))
    }

    fn force_upgrade_explicit(
        &self,
        before: &RuntimeReport,
        warnings: &mut Vec<String>,
    ) -> Result<RuntimeSelection> {
        let path = before
            .path
            .clone()
            .ok_or_else(|| OcgError::config("the explicit OpenCode has no path"))?;
        match self.process.upgrade(&path, None, &self.proxy_env) {
            Ok(_) => {
                let version = self.probe_version(&path);
                Ok(RuntimeSelection {
                    source: RuntimeSource::Explicit,
                    path,
                    version,
                    warnings: Vec::new(),
                })
            }
            Err(error) => {
                // Explicit stays authoritative even when its upgrade fails.
                warnings.push(format!(
                    "explicit OpenCode `upgrade` failed; keeping it: {error}"
                ));
                Ok(RuntimeSelection {
                    source: RuntimeSource::Explicit,
                    path,
                    version: before.version.clone(),
                    warnings: Vec::new(),
                })
            }
        }
    }

    fn force_upgrade_managed(
        &self,
        before: &RuntimeReport,
        warnings: &mut Vec<String>,
    ) -> Result<RuntimeSelection> {
        if let Some(pin) = self.policy.version.clone() {
            return self.resolve_pinned(&pin);
        }

        let release =
            release::fetch_latest_release(self.http, &self.api_base, &self.opencode_repo)?;
        if before
            .version
            .as_ref()
            .map(|current| release.version <= *current)
            .unwrap_or(false)
        {
            self.record_check(Some(release.version));
            return Ok(RuntimeSelection {
                source: RuntimeSource::Managed,
                path: before
                    .path
                    .clone()
                    .ok_or_else(|| OcgError::config("the managed OpenCode has no path"))?,
                version: before.version.clone(),
                warnings: Vec::new(),
            });
        }
        match self.install_release(&release, RuntimeSource::Managed) {
            Ok(selection) => {
                self.record_check(Some(release.version.clone()));
                Ok(selection)
            }
            Err(error) => {
                let usable = is_compatible(&before.version)
                    && before.path.as_deref().map(is_executable).unwrap_or(false);
                if usable {
                    self.record_failed_check(&error);
                    warnings.push(format!(
                        "managed OpenCode update failed; keeping {}: {error}",
                        describe_version(&before.version)
                    ));
                    let path = before.path.clone().ok_or_else(|| {
                        OcgError::config("the managed OpenCode has no usable path")
                    })?;
                    Ok(RuntimeSelection {
                        source: RuntimeSource::Managed,
                        path,
                        version: before.version.clone(),
                        warnings: Vec::new(),
                    })
                } else {
                    Err(error)
                }
            }
        }
    }

    fn force_upgrade_system(
        &self,
        before: &RuntimeReport,
        warnings: &mut Vec<String>,
    ) -> Result<RuntimeSelection> {
        let path = before
            .path
            .clone()
            .ok_or_else(|| OcgError::config("the system OpenCode has no path"))?;
        let compatible = is_compatible(&before.version);

        // Resolve the latest release once and use it as the explicit target.
        let release = match release::fetch_latest_release(
            self.http,
            &self.api_base,
            &self.opencode_repo,
        ) {
            Ok(release) => release,
            Err(error) => {
                if compatible {
                    self.record_failed_check(&error);
                    warnings.push(format!(
                        "could not resolve the latest OpenCode release; keeping {}: {error}",
                        describe_version(&before.version)
                    ));
                    return Ok(RuntimeSelection {
                        source: RuntimeSource::System,
                        path,
                        version: before.version.clone(),
                        warnings: Vec::new(),
                    });
                }
                warnings.push(format!(
                        "the system OpenCode at {} is not compatible and the latest release could not be resolved: {error}",
                        path.display()
                    ));
                return self.bootstrap(warnings.clone());
            }
        };

        if before
            .version
            .as_ref()
            .map(|current| release.version <= *current)
            .unwrap_or(false)
        {
            self.record_check(Some(release.version));
            return Ok(RuntimeSelection {
                source: RuntimeSource::System,
                path,
                version: before.version.clone(),
                warnings: Vec::new(),
            });
        }

        match self
            .process
            .upgrade(&path, Some(&release.version), &self.proxy_env)
        {
            Ok(_) => {
                let version = self.probe_version(&path);
                if is_compatible(&version) {
                    self.record_check(version.clone());
                    Ok(RuntimeSelection {
                        source: RuntimeSource::System,
                        path,
                        version,
                        warnings: Vec::new(),
                    })
                } else {
                    warnings.push(format!(
                        "the system OpenCode at {} is still not compatible after `opencode upgrade {}`",
                        path.display(),
                        release.version
                    ));
                    self.bootstrap_release(&release, warnings.clone())
                }
            }
            Err(error) => {
                if compatible {
                    self.record_failed_check(&error);
                    warnings.push(format!(
                        "system OpenCode `upgrade` failed; keeping {}: {error}",
                        describe_version(&before.version)
                    ));
                    Ok(RuntimeSelection {
                        source: RuntimeSource::System,
                        path,
                        version: before.version.clone(),
                        warnings: Vec::new(),
                    })
                } else {
                    warnings.push(format!(
                        "the system OpenCode at {} is not compatible and `opencode upgrade` failed: {error}",
                        path.display()
                    ));
                    self.bootstrap_release(&release, warnings.clone())
                }
            }
        }
    }

    fn bootstrap(&self, warnings: Vec<String>) -> Result<RuntimeSelection> {
        let release = match &self.policy.version {
            Some(pin) => release::fetch_release_by_version(
                self.http,
                &self.api_base,
                &self.opencode_repo,
                pin,
            )?,
            None => release::fetch_latest_release(self.http, &self.api_base, &self.opencode_repo)?,
        };
        self.bootstrap_release(&release, warnings)
    }

    /// Install an already resolved release and record the check.
    fn bootstrap_release(
        &self,
        release: &Release,
        warnings: Vec<String>,
    ) -> Result<RuntimeSelection> {
        let mut selection = self.install_release(release, RuntimeSource::ProjectLocal)?;
        if self.policy.version.is_none() {
            self.record_check(Some(release.version.clone()));
        }
        selection.warnings = warnings;
        Ok(selection)
    }

    fn install_release(
        &self,
        release: &Release,
        source: RuntimeSource,
    ) -> Result<RuntimeSelection> {
        let version = release.version.clone();
        let path = install_opencode(
            &self.project_root,
            self.platform,
            &version,
            release,
            self.http,
            self.clock,
        )?;
        Ok(RuntimeSelection {
            source,
            path,
            version: Some(version),
            warnings: Vec::new(),
        })
    }

    fn probe_version(&self, path: &Path) -> Option<Version> {
        self.process
            .version(path)
            .ok()
            .and_then(|output| release::parse_version_output(&output))
    }

    /// Whether an optional update check is due.
    fn check_due(&self) -> bool {
        cache::is_due(
            self.cache_dir.as_deref(),
            self.clock,
            self.policy.check_interval_hours,
            false,
        )
    }

    fn record_check(&self, version: Option<Version>) {
        if let Some(dir) = &self.cache_dir {
            let _ = CacheRecord {
                checked_at: self.clock.now_unix(),
                version,
                failure_reason: None,
            }
            .write(dir);
        }
    }

    fn record_failed_check(&self, error: &OcgError) {
        let message = error.to_string().to_ascii_lowercase();
        let failure_reason = if message.contains("rate limit") || message.contains("throttled") {
            "rate_limited"
        } else {
            "failed"
        };
        if let Some(dir) = &self.cache_dir {
            let _ = CacheRecord {
                checked_at: self.clock.now_unix(),
                version: None,
                failure_reason: Some(failure_reason.to_string()),
            }
            .write(dir);
        }
    }

    fn append_cached_check_warning(&self, warnings: &mut Vec<String>) {
        let Some(dir) = &self.cache_dir else {
            return;
        };
        let Some(record) = CacheRecord::read(dir) else {
            return;
        };
        if record.is_due(self.clock.now_unix(), self.policy.check_interval_hours) {
            return;
        }
        match record.failure_reason.as_deref() {
            Some("rate_limited") => warnings.push(
                "the last OpenCode update check was rate-limited and is cached until the check interval expires"
                    .to_string(),
            ),
            Some("failed") => warnings.push(
                "the last OpenCode update check failed and is cached until the check interval expires"
                    .to_string(),
            ),
            _ => {}
        }
    }
}

fn is_bare_name(path: &Path) -> bool {
    !path.is_absolute() && path.components().count() == 1
}

fn is_compatible(version: &Option<Version>) -> bool {
    version
        .as_ref()
        .map(|version| *version >= policy::min_opencode_version())
        .unwrap_or(false)
}

/// Whether an unpinned project should resolve to the system runtime instead of
/// its existing managed one: the managed runtime belongs to an older
/// *supported* OpenCode family and the system runtime to a newer one (managed
/// V1 vs system V2).
///
/// Both sides go through the compat classifier rather than a raw number
/// comparison, so a system runtime whose version cannot be probed or
/// classified — or which belongs to an unsupported major, no matter how high
/// its number is — never wins, and the managed fallback is preserved. A
/// same-family system runtime never wins either: the managed preference is
/// kept.
fn system_is_newer_supported_family(managed: &Version, system: Option<&Version>) -> bool {
    let Some(system) = system else {
        return false;
    };
    let (Ok(managed), Ok(system)) = (
        compat::classify(managed.clone()),
        compat::classify(system.clone()),
    ) else {
        return false;
    };
    system.major() > managed.major()
}

/// Keep the system runtime as selected, replacing only the warnings.
fn keep_system(selection: RuntimeSelection, warnings: Vec<String>) -> RuntimeSelection {
    RuntimeSelection {
        source: RuntimeSource::System,
        path: selection.path,
        version: selection.version,
        warnings,
    }
}

fn describe_version(version: &Option<Version>) -> String {
    version
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| "unknown".to_string())
}
