//! Execution Disk-Space Guard: host free-space protection for the execution substrate.
//!
//! A long-running OCG Project can amplify durable writes through Jobs,
//! Attempts, Calls, journals, Placement history, Watchdog actions, recursive
//! descendants, retries, context snapshots and indexes, result evidence and
//! usage records. Bounded recursion alone does not protect the host: the
//! execution layer needs a hard safety boundary that preserves a configurable
//! amount of host free disk space and stops creating additional
//! write-amplifying execution when that reserve is threatened.
//!
//! The Guard is the single authoritative API for execution-layer disk safety.
//! Callers never compare free bytes against constants themselves. They ask one
//! of:
//!
//! * [`DiskGuard::check_execution_admission`] — may new provider execution
//!   begin (new Attempt, retry, readmission, exact probe dispatch)?
//! * [`DiskGuard::check_amplifying_expansion`] — may execution fan out (new
//!   recursive child, exact diagnostic target)?
//!
//! Essential settlement (terminal state, fencing, cancellation, ownership
//! release and the minimal result/failure fact that carries it) is always
//! allowed and never consults the Guard, because refusing the writes that
//! settle already-active execution would leave corrupt or ambiguous state.
//!
//! Threshold model with hysteresis:
//!
//! * `Healthy`: enough free space for normal execution.
//! * `Pressure`: free space is approaching the reserve. New recursive/diag-
//!   nostic expansion is deferred; ordinary admission continues so active work
//!   can converge to safe canonical boundaries.
//! * `Critical`: the reserve has been crossed. New write-amplifying execution
//!   is deferred; only essential settlement proceeds.
//! * `Unknown`: no successful filesystem observation exists yet (or at all).
//!   New expansion is deferred rather than assuming infinite disk.
//!
//! Entry uses `minimum_free_bytes` / `pressure_free_bytes`; recovery requires
//! free space at or above `resume_free_bytes`, which must sit above the
//! critical entry threshold so the Guard cannot oscillate on a few freed
//! blocks. A transient observation failure keeps serving the last known valid
//! state; only a Guard that never observed successfully reports `Unknown`.
//!
//! The Guard itself writes nothing durably. State is in-memory per
//! [`DiskGuard`] (one per execution runtime/Project root) and is re-derived
//! from the filesystem on every process start, so a stale guard can never
//! survive actual recovery. Hot paths read a cached observation: the
//! filesystem is only re-measured on a bounded cadence, or synchronously when
//! the cached observation is too old for a safety-critical decision.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::error::{OcgError, Result};

/// Absolute free-space reserve below which new execution is deferred.
pub const DEFAULT_MINIMUM_FREE_BYTES: u64 = 256 * 1024 * 1024;
/// Free-space level below which recursive/diagnostic expansion is deferred.
pub const DEFAULT_PRESSURE_FREE_BYTES: u64 = 1024 * 1024 * 1024;
/// Free-space level that must be reached before guarded execution resumes.
pub const DEFAULT_RESUME_FREE_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// How often a cached observation may be refreshed by background loops.
pub const OBSERVATION_CADENCE: Duration = Duration::from_secs(5);
/// Oldest cached observation a safety-critical decision will trust before
/// measuring synchronously.
pub const MAX_OBSERVATION_AGE: Duration = Duration::from_secs(30);

fn invalid(message: impl Into<String>) -> OcgError {
    OcgError::config(message.into())
}

/// Threshold policy for one Guard. An absolute byte reserve is the core
/// requirement; OCG cannot accurately predict per-Job byte cost, so no
/// per-Job reservation is attempted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(default)]
pub struct DiskGuardConfig {
    /// Entering `Critical` when free space drops below this reserve.
    pub minimum_free_bytes: u64,
    /// Entering `Pressure` when free space drops below this level.
    pub pressure_free_bytes: u64,
    /// Guarded execution resumes only at or above this level (hysteresis).
    pub resume_free_bytes: u64,
}

impl Default for DiskGuardConfig {
    fn default() -> Self {
        Self {
            minimum_free_bytes: DEFAULT_MINIMUM_FREE_BYTES,
            pressure_free_bytes: DEFAULT_PRESSURE_FREE_BYTES,
            resume_free_bytes: DEFAULT_RESUME_FREE_BYTES,
        }
    }
}

impl DiskGuardConfig {
    /// Reject threshold combinations that cannot protect the host: an empty
    /// reserve, an inverted pressure band, or a recovery level that does not
    /// sit above the critical entry threshold (which would oscillate).
    pub fn validate(&self) -> Result<()> {
        if self.minimum_free_bytes == 0 {
            return Err(invalid("disk guard minimum_free_bytes must be positive"));
        }
        if self.pressure_free_bytes < self.minimum_free_bytes {
            return Err(invalid(
                "disk guard pressure_free_bytes must cover minimum_free_bytes",
            ));
        }
        if self.resume_free_bytes < self.pressure_free_bytes {
            return Err(invalid(
                "disk guard resume_free_bytes must cover pressure_free_bytes",
            ));
        }
        if self.resume_free_bytes <= self.minimum_free_bytes {
            return Err(invalid(
                "disk guard resume_free_bytes must sit above minimum_free_bytes",
            ));
        }
        Ok(())
    }
}

/// The Guard's current safety range. The names are deliberately coarse: this
/// is an execution gate, not a storage dashboard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "snake_case")]
pub enum DiskState {
    Healthy,
    Pressure,
    Critical,
    Unknown,
}

impl DiskState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Pressure => "pressure",
            Self::Critical => "critical",
            Self::Unknown => "unknown",
        }
    }

    /// Whether ordinary new execution expansion must wait.
    pub fn defers_new_execution(self) -> bool {
        matches!(self, Self::Critical | Self::Unknown)
    }

    /// Whether recursive/diagnostic fan-out must wait. Fan-out multiplies
    /// future writes, so it stops a full range earlier than plain admission.
    pub fn defers_expansion(self) -> bool {
        matches!(self, Self::Pressure | Self::Critical | Self::Unknown)
    }
}

/// One filesystem measurement: the authoritative reserve check. Database file
/// sizes, Job counts and descendant counts are diagnostics, never this.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilesystemObservation {
    pub available_bytes: u64,
    pub total_bytes: u64,
    pub root: String,
    pub observed_at: i64,
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Measure free space on the filesystem containing `path` with the operating
/// system's own accounting (`statvfs`). Never shells out, never estimates.
#[cfg(unix)]
pub(crate) fn measure(path: &Path) -> Result<FilesystemObservation> {
    let stat = rustix::fs::statvfs(path).map_err(|error| {
        OcgError::io(
            format!("measure free disk space at {}", path.display()),
            error.into(),
        )
    })?;
    Ok(FilesystemObservation {
        available_bytes: stat.f_bavail.saturating_mul(stat.f_frsize),
        total_bytes: stat.f_blocks.saturating_mul(stat.f_frsize),
        root: path.display().to_string(),
        observed_at: now_unix(),
    })
}

#[cfg(not(unix))]
pub(crate) fn measure(path: &Path) -> Result<FilesystemObservation> {
    stat_filesystem(path)
}

#[cfg(not(unix))]
fn stat_filesystem(path: &Path) -> Result<FilesystemObservation> {
    Err(invalid(format!(
        "disk guard free-space measurement is not supported on this platform ({})",
        path.display()
    )))
}

/// Why a disk decision deferred an operation. Carries the live bytes for
/// operator responses without letting callers parse message strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskPressureKind {
    Pressure,
    Critical,
    Unknown,
}

impl DiskPressureKind {
    fn code(self) -> &'static str {
        match self {
            Self::Pressure => "disk_pressure_deferred",
            Self::Critical => "disk_critical_deferred",
            Self::Unknown => "disk_observation_failed",
        }
    }
}

/// A deferred operation. The persisted [`Failure`] is intentionally stable
/// across observations (no byte counts, no timestamps) so repeated admission
/// scans under sustained pressure dedupe to one durable record instead of one
/// identical event per cycle. Live bytes travel in [`DiskDeferral::message`],
/// which is per-request and never persisted.
#[derive(Debug, Clone)]
pub struct DiskDeferral {
    pub kind: DiskPressureKind,
    pub available_bytes: Option<u64>,
    pub reserve_bytes: u64,
    pub observed_at: Option<i64>,
}

impl DiskDeferral {
    /// The stable canonical failure recorded for a deferred Job. Temporary by
    /// construction: disk pressure never fails work permanently.
    pub fn failure(&self) -> crate::core_contract::Failure {
        let (code, class, message) = match self.kind {
            DiskPressureKind::Pressure | DiskPressureKind::Critical => (
                self.kind.code(),
                crate::core_contract::FailureClass::ResourceLimit,
                "execution deferred: local storage reserve protection",
            ),
            DiskPressureKind::Unknown => (
                self.kind.code(),
                crate::core_contract::FailureClass::Unknown,
                "execution deferred: local storage observation failed",
            ),
        };
        crate::orchestration::domain::job_failure(code, class, message, true)
    }

    /// The operator-facing refusal. Carries the observed bytes; never parsed
    /// by callers and never persisted.
    pub fn message(&self) -> String {
        match (self.available_bytes, self.observed_at) {
            (Some(available), _) => format!(
                "{}: execution deferred: local storage reserve protection (available {} bytes, reserve {} bytes)",
                self.kind.code(),
                available,
                self.reserve_bytes
            ),
            (None, _) => format!(
                "{}: execution deferred: local storage reserve protection (reserve {} bytes, no successful observation)",
                self.kind.code(),
                self.reserve_bytes
            ),
        }
    }
}

/// The answer to "can this execution operation safely create additional
/// durable state?"
#[derive(Debug, Clone)]
pub enum DiskDecision {
    Allow,
    Defer(DiskDeferral),
}

impl DiskDecision {
    pub fn deferral(self) -> Option<DiskDeferral> {
        match self {
            Self::Allow => None,
            Self::Defer(deferral) => Some(deferral),
        }
    }

    pub fn allowed(&self) -> bool {
        matches!(self, Self::Allow)
    }
}

/// The smallest canonical read projection for operators: current state, live
/// bytes, configured reserve, last observation, measured root, and whether
/// new execution is currently being deferred.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
pub struct DiskGuardStatus {
    pub state: DiskState,
    pub available_bytes: u64,
    pub total_bytes: u64,
    pub reserve_bytes: u64,
    pub pressure_bytes: u64,
    pub resume_bytes: u64,
    pub observed_at: i64,
    pub root: String,
    pub defers_new_execution: bool,
    pub defers_expansion: bool,
}

#[derive(Debug)]
struct GuardInner {
    root: PathBuf,
    config: DiskGuardConfig,
    state: DiskState,
    cached: Option<FilesystemObservation>,
    last_attempt: Option<Instant>,
    last_error: Option<String>,
}

/// The execution-layer disk safety authority. One per Project root, owned by
/// the execution runtime and shared by reference. Bounded overhead regardless
/// of Job count: no per-Job timers, no per-Attempt threads.
#[derive(Clone, Debug)]
pub struct DiskGuard {
    inner: Arc<Mutex<GuardInner>>,
}

impl DiskGuard {
    /// Guard the filesystem containing `root` (normally the Project root,
    /// below which all write-heavy OCG state lives). Starts unobserved:
    /// callers must [`DiskGuard::refresh`] (or let a check observe) before
    /// new execution begins, so work never starts under an assumption of
    /// infinite free space.
    pub fn for_root(root: &Path, config: DiskGuardConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            inner: Arc::new(Mutex::new(GuardInner {
                root: root.to_path_buf(),
                config,
                state: DiskState::Unknown,
                cached: None,
                last_attempt: None,
                last_error: None,
            })),
        })
    }

    /// Apply new thresholds without restart. Invalid thresholds are rejected
    /// and the previous policy stays in force.
    pub fn apply_config(&self, config: &DiskGuardConfig) -> Result<()> {
        config.validate()?;
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| invalid("disk guard lock poisoned"))?;
        inner.config = *config;
        // Re-evaluate the cached observation against the new policy so a
        // tightened reserve takes effect without waiting for the next sample.
        if let Some(observation) = inner.cached.clone() {
            inner.state = classify(inner.state, observation.available_bytes, &inner.config);
        }
        Ok(())
    }

    fn locked(&self) -> Result<std::sync::MutexGuard<'_, GuardInner>> {
        self.inner
            .lock()
            .map_err(|_| invalid("disk guard lock poisoned"))
    }

    /// Measure now and advance the hysteresis state machine. Records no
    /// durable state: transitions live in memory and in the returned state.
    pub fn refresh(&self) -> Result<DiskState> {
        let root = self.locked()?.root.clone();
        match measure(&root) {
            Ok(observation) => {
                let mut inner = self.locked()?;
                inner.state = classify(inner.state, observation.available_bytes, &inner.config);
                inner.cached = Some(observation);
                inner.last_attempt = Some(Instant::now());
                inner.last_error = None;
                Ok(inner.state)
            }
            Err(error) => {
                let mut inner = self.locked()?;
                inner.last_attempt = Some(Instant::now());
                inner.last_error = Some(error.to_string());
                // A transient failure keeps serving the last valid state. Only
                // a Guard that never observed successfully reports Unknown.
                if inner.cached.is_none() {
                    inner.state = DiskState::Unknown;
                }
                Err(error)
            }
        }
    }

    /// Refresh only when the last attempt is older than the observation
    /// cadence. Hot loops (the 100ms admission worker, the 5s Watchdog pass)
    /// call this: most iterations are a cached read, not a syscall.
    pub fn refresh_if_stale(&self) {
        let stale = self
            .locked()
            .map(|inner| {
                inner
                    .last_attempt
                    .is_none_or(|attempt| attempt.elapsed() >= OBSERVATION_CADENCE)
            })
            .unwrap_or(true);
        if stale {
            let _ = self.refresh();
        }
    }

    /// Ensure the cached observation is fresh enough for a safety-critical
    /// decision, measuring synchronously when it is not.
    fn ensure_fresh(&self) {
        let stale = self
            .locked()
            .map(|inner| {
                inner.cached.is_none()
                    || inner
                        .last_attempt
                        .is_none_or(|attempt| attempt.elapsed() >= MAX_OBSERVATION_AGE)
            })
            .unwrap_or(true);
        if stale {
            let _ = self.refresh();
        }
    }

    fn decide(&self, expand: bool) -> DiskDecision {
        self.ensure_fresh();
        let inner = match self.locked() {
            Ok(inner) => inner,
            Err(_) => {
                return DiskDecision::Defer(DiskDeferral {
                    kind: DiskPressureKind::Unknown,
                    available_bytes: None,
                    reserve_bytes: DEFAULT_MINIMUM_FREE_BYTES,
                    observed_at: None,
                });
            }
        };
        let guarded = if expand {
            inner.state.defers_expansion()
        } else {
            inner.state.defers_new_execution()
        };
        if !guarded {
            return DiskDecision::Allow;
        }
        let kind = match inner.state {
            DiskState::Pressure => DiskPressureKind::Pressure,
            DiskState::Critical => DiskPressureKind::Critical,
            DiskState::Healthy | DiskState::Unknown => DiskPressureKind::Unknown,
        };
        DiskDecision::Defer(DiskDeferral {
            kind,
            available_bytes: inner.cached.as_ref().map(|item| item.available_bytes),
            reserve_bytes: inner.config.minimum_free_bytes,
            observed_at: inner.cached.as_ref().map(|item| item.observed_at),
        })
    }

    /// May new provider execution begin (new Attempt, retry, readmission,
    /// exact dispatch)? Deferred only under `Critical` (or unobserved).
    pub fn check_execution_admission(&self) -> DiskDecision {
        self.decide(false)
    }

    /// May execution fan out (new recursive child, exact diagnostic target)?
    /// Fan-out multiplies future writes, so it already defers under
    /// `Pressure`.
    pub fn check_amplifying_expansion(&self) -> DiskDecision {
        self.decide(true)
    }

    /// Cached read projection. Performs no filesystem I/O, so status
    /// endpoints stay cheap under load.
    pub fn status(&self) -> DiskGuardStatus {
        match self.locked() {
            Ok(inner) => {
                let (available, total, observed_at, root) = inner
                    .cached
                    .as_ref()
                    .map(|item| {
                        (
                            item.available_bytes,
                            item.total_bytes,
                            item.observed_at,
                            item.root.clone(),
                        )
                    })
                    .unwrap_or((0, 0, 0, inner.root.display().to_string()));
                DiskGuardStatus {
                    state: inner.state,
                    available_bytes: available,
                    total_bytes: total,
                    reserve_bytes: inner.config.minimum_free_bytes,
                    pressure_bytes: inner.config.pressure_free_bytes,
                    resume_bytes: inner.config.resume_free_bytes,
                    observed_at,
                    root,
                    defers_new_execution: inner.state.defers_new_execution(),
                    defers_expansion: inner.state.defers_expansion(),
                }
            }
            Err(_) => DiskGuardStatus {
                state: DiskState::Unknown,
                available_bytes: 0,
                total_bytes: 0,
                reserve_bytes: DEFAULT_MINIMUM_FREE_BYTES,
                pressure_bytes: DEFAULT_PRESSURE_FREE_BYTES,
                resume_bytes: DEFAULT_RESUME_FREE_BYTES,
                observed_at: 0,
                root: String::new(),
                defers_new_execution: true,
                defers_expansion: true,
            },
        }
    }

    /// Last filesystem error, if any. Diagnostic only: callers act on the
    /// decision, never on this string.
    pub fn last_error(&self) -> Option<String> {
        self.locked().ok()?.last_error.clone()
    }
}

/// Hysteresis state machine. Fresh (`Healthy`, or a first observation after
/// `Unknown`) applies entry thresholds; guarded states recover only at or
/// above the resume reserve, so freeing a few blocks cannot resume execution
/// just to cross the entry threshold again.
fn classify(current: DiskState, free_bytes: u64, config: &DiskGuardConfig) -> DiskState {
    match current {
        DiskState::Pressure | DiskState::Critical => {
            if free_bytes >= config.resume_free_bytes {
                DiskState::Healthy
            } else if free_bytes < config.minimum_free_bytes {
                DiskState::Critical
            } else {
                DiskState::Pressure
            }
        }
        DiskState::Healthy | DiskState::Unknown => {
            if free_bytes < config.minimum_free_bytes {
                DiskState::Critical
            } else if free_bytes < config.pressure_free_bytes {
                DiskState::Pressure
            } else {
                DiskState::Healthy
            }
        }
    }
}
