//! The runtime gate of autonomous loopback scheduling: an explicit, per-run,
//! per-data-directory operator authorization that a compiled
//! [`ExecutionControl::AutonomousScheduling`] needs before it may act.
//!
//! The compiled control says what the build *can* do; nothing in it says
//! that this host, over this database, *may*. A host that merely runs with
//! the control compiled in starts without autonomous authority. Authority
//! exists only as an [`ArmedActivation`], which only [`arm`] produces, only
//! inside the host that opened the database (so under its exclusive lock,
//! before its execution runtime starts and before any listener can submit),
//! and only once every check of the activation preflight passed:
//!
//! - the build compiles the control and no external, redispatch or legacy
//!   authority;
//! - the operator named exactly this data directory and exactly this
//!   database (its Genesis Job identity), and confirmed the state this
//!   process just read of it by returning the challenge a preflight of that
//!   state printed; see "What this does not prove" below;
//! - the database is open, holds no Allocation, no queued work and no loop
//!   that could create work: the first restricted activation never consumes,
//!   and never lets begin, work that existed before it.
//!
//! A preflight that cannot be read, or whose counts, entries and recurrence
//! index disagree, is refused, not guessed at. Nothing here clears, cancels,
//! archives, reserves or executes anything: a refusal leaves the database
//! exactly as it was, and the operator decides what to do with the work it
//! reported.
//!
//! # Two readings, two purposes
//!
//! [`preflight`] reads an existing database **without opening it for
//! writing**: it takes the host lock shared, opens the file read-only (or a
//! private unlinked copy when redb would have to repair it), reads one
//! consistent snapshot, and verifies afterwards that the file's bytes, mode,
//! modification time, device and inode are exactly what they were. It never
//! increments `open_epoch`, never touches `clean_shutdown`, never creates a
//! directory, lock file or log, and never rebuilds a derived index. Its
//! report is advisory: it says what a first activation would decide, and what
//! the operator must confirm.
//!
//! [`arm`] decides again, on the database the host has actually opened, under
//! its exclusive lock, immediately before the execution runtime starts. That
//! second reading is the authoritative one, and it is the only one that can
//! produce authority. It also checks that the state it sees is the state the
//! operator confirmed, so a confirmation cannot outlive the state it named.
//!
//! The token is not an environment variable, a request, a stored row or a
//! Job configuration: no HTTP client and no stored state can produce one.
//! Process-scoped hosts and the other commands never ask for one, so they
//! never hold one. Dropping the authority (stopping the armed host) stops
//! new claims through the execution runtime's ordinary shutdown; it does not
//! and cannot release ownership an execution already holds.
//!
//! # What this does not prove
//!
//! The challenge binds a confirmation to one opening of one database state,
//! and it is the only thing that does. It is not an identity check, and it is
//! no evidence that a person is present:
//!
//! - It stops a *static* challenge from being replayed. `open_epoch` advances
//!   on every opening, so a unit file, alias or script that carries a
//!   challenge printed earlier is refused after any opening, its own
//!   included.
//! - It does not stop a local process from confirming. `preflight` prints the
//!   challenge for the state it reads, so any process that can run these two
//!   commands over this directory can obtain a fresh challenge and pass it to
//!   `serve`; a wrapper around both re-arms as often as it is run. A
//!   noninteractive caller may equally read the expected challenge out of a
//!   refusal and retry with it.
//! - A terminal is where an operator usually is, not evidence that one is
//!   there: reading standard input proves only that something wrote to it.
//!
//! What the gate therefore authorizes is a *local command-execution
//! authority*: whoever can run `serve` against this data directory with a
//! challenge for its current state could already run arbitrary commands as
//! this user over these files. The gate decides when autonomous loopback
//! scheduling may act; it does not decide who the operator is.

use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use crate::db::DatabaseHandle;
use crate::db::redb::{ActivationSnapshot, GENESIS_JOB, OfflineActivation, RecurrenceAudit};
use crate::system::data_directory::{DATA_DIRECTORY_VARIABLE, DataDirectory};
use crate::system::execution_controls::{ControlState, ExecutionControl};
use crate::system::native_scheduler::loopback_eligibility;

/// Ready entries a preflight lists with their eligibility. The listing is
/// bounded; the counts it is compared against are exact, and any non-zero
/// count refuses activation on its own, so no decision depends on the
/// listing.
pub(crate) const READY_PREVIEW: usize = 32;

/// What the operator named for one activation: the data directory, the
/// database identity the armed host must be running over, and the state of
/// that database they confirmed.
#[derive(Clone, Debug)]
pub(crate) struct ActivationRequest {
    /// The data directory as the operator named it, before resolution.
    pub(crate) data_directory: PathBuf,
    /// The identity of the database file the operator means.
    pub(crate) database: DatabaseIdentity,
    /// What the operator confirmed, in this process; see [`Confirmation`].
    pub(crate) confirmation: Confirmation,
}

/// The identity of one database file on this machine: the device and inode
/// the file lives at, as `<device>:<inode>`. The database stores no instance
/// identity (its Genesis Job is Job 0 everywhere) and this gate adds no
/// stored state, so the file's own identity is what an activation binds to:
/// it is read from the stopped database by `preflight`, it survives nothing
/// that replaces the file (a restore, a migration, a copy to another
/// directory), and it names exactly one file on this filesystem. On a
/// platform without file identities no activation is possible.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DatabaseIdentity {
    device: u64,
    inode: u64,
}

impl DatabaseIdentity {
    /// Parses `<device>:<inode>` as `preflight` prints it.
    pub(crate) fn parse(text: &str) -> Option<Self> {
        let (device, inode) = text.split_once(':')?;
        Some(Self {
            device: device.parse().ok()?,
            inode: inode.parse().ok()?,
        })
    }

    /// The identity of the database file in `root`, when the platform has
    /// one and the file is a regular file.
    pub(crate) fn of(root: &Path) -> Result<Self, String> {
        let path = root.join(crate::db::DATABASE_FILE_NAME);
        let metadata = std::fs::symlink_metadata(&path).map_err(|error| {
            format!(
                "the database file {} cannot be read: {error}",
                path.display()
            )
        })?;
        if !metadata.is_file() {
            return Err(format!(
                "the database path {} is not a regular file",
                path.display()
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Ok(Self {
                device: metadata.dev(),
                inode: metadata.ino(),
            })
        }
        #[cfg(not(unix))]
        {
            Err(
                "this platform exposes no database file identity; activation is not possible"
                    .to_owned(),
            )
        }
    }
}

impl std::fmt::Display for DatabaseIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.device, self.inode)
    }
}

/// What the operator confirmed before the host opened anything: the state
/// digest a preflight printed, the stored `open_epoch` it read, and the
/// challenge it printed for exactly that pair.
///
/// The challenge is single-use by construction and stores nothing. It covers
/// `open_epoch`, which every opening increments, so the challenge an earlier
/// run printed cannot name the state a later run starts from: a service
/// manager's unit file, however often it restarts, cannot carry a challenge
/// that still matches, because every restart is another opening. It does not
/// stop a process from asking `preflight` for a fresh challenge itself, and it
/// is no evidence that a person rather than a program confirmed; see "What
/// this does not prove" in the module documentation. The host checks both that
/// it is the next opening (`open_epoch` advanced by exactly one) and that the
/// queue and recurrence state it reads is the state the digest named.
#[derive(Clone, Debug)]
pub(crate) struct Confirmation {
    /// The challenge as printed: `ocg1-<16 hex digits>`.
    pub(crate) challenge: String,
    /// The stored `open_epoch` the challenge was computed over.
    pub(crate) open_epoch: u64,
    /// The `clean_shutdown` marker the challenge was computed over.
    pub(crate) clean_shutdown: bool,
    /// The digest of the state the challenge was computed over.
    pub(crate) state: String,
}

/// Proof that this host run passed [`arm`]. It cannot be constructed
/// anywhere else, is not `Clone`, and is consumed by the one execution
/// runtime it arms.
pub(crate) struct ArmedActivation {
    _private: (),
}

/// Why an activation was refused. Nothing was changed by the refusal itself.
#[derive(Debug)]
pub(crate) struct ActivationRefusal {
    pub(crate) code: &'static str,
    pub(crate) detail: String,
}

impl std::fmt::Display for ActivationRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "autonomous loopback activation refused ({}): {}",
            self.code, self.detail
        )
    }
}

impl std::error::Error for ActivationRefusal {}

/// The outcome of one preflight: its report, and the refusal if it did not
/// pass. A preflight without a request reports and never arms.
pub(crate) struct Preflight {
    pub(crate) report: Value,
    pub(crate) refusal: Option<ActivationRefusal>,
}

/// The controls a loopback-only activation refuses to run beside: each would
/// let the armed scheduler, or something next to it, reach past loopback or
/// adopt work.
const INCOMPATIBLE_CONTROLS: [ExecutionControl; 4] = [
    ExecutionControl::ExternalProviderTransport,
    ExecutionControl::ScheduledExternalTransport,
    ExecutionControl::RecoveryAndRedispatch,
    ExecutionControl::LegacyProviderExecution,
];

/// Everything one reading of the database established, and how it read it.
/// The decision below is made from this alone, so the advisory and the
/// authoritative reading judge exactly the same facts.
struct Findings {
    /// The canonical data directory the reading came from.
    root: PathBuf,
    identity: Result<DatabaseIdentity, String>,
    /// The stored `open_epoch` this reading saw, when it could read one.
    epoch: Option<u64>,
    clean_shutdown: Option<bool>,
    /// The snapshot, or why it could not be read.
    snapshot: Result<ActivationSnapshot, (&'static str, String)>,
    /// Whether this reading came from the host's own open database.
    in_host: bool,
    /// `in_place_read_only` or `private_unlinked_copy`; `open` in a host.
    access: &'static str,
    /// Whether the file was copied to be read, which means redb repaired it.
    copied: bool,
    /// The file as a read-only reading verified it: its size and mode. Absent
    /// when this reading opened the file as a host, which writes it.
    file_bytes: Option<u64>,
    file_mode: Option<u32>,
}

/// Who is asking, and therefore what a passing decision can mean. A read-only
/// reading can only advise; only the host's own gate, holding the database
/// exclusively before its execution runtime starts, produces authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Gate {
    /// A read-only reading of a stopped database: it reports, and arms nothing.
    Advisory,
    /// The host's own reading under its exclusive lock: a pass arms this run.
    Authoritative,
}

/// What one activation report describes. It is computed from the decision that
/// same report publishes and from what the reading that made it was allowed to
/// do, so the outcome and the report cannot disagree. Nothing reconstructs it
/// from a confirmation string, a flag or the report's own text, and reporting
/// reads it without creating, extending or revoking any authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ActivationOutcome {
    /// A read-only preflight that was not asked to activate.
    NotRequested,
    /// A read-only preflight of a requested activation: it advises, and arms
    /// nothing.
    Advisory,
    /// The host's own authoritative reading refused: nothing was armed.
    Refused,
    /// The host's own authoritative reading passed, and this run holds the
    /// authority that decision produced.
    Armed,
}

impl ActivationOutcome {
    /// The report's verdict. A successful arm is the only outcome that says
    /// `armed`, and it exists only where the token exists.
    const fn verdict(self) -> &'static str {
        match self {
            Self::NotRequested => "not_requested",
            Self::Advisory => "pass",
            Self::Refused => "refused",
            Self::Armed => "armed",
        }
    }
}

/// How a reading reached the database, and what that left behind. The report
/// describes exactly this, and never claims a wider scope than it verified.
pub(crate) struct Changes {
    /// What this reading was allowed to do; see [`Gate`].
    gate: Gate,
    /// Whether this process opened the database as a host.
    pub(crate) host_started: bool,
    pub(crate) open_epoch_before: Option<u64>,
    pub(crate) open_epoch_after: Option<u64>,
    pub(crate) clean_shutdown_before: Option<bool>,
    pub(crate) clean_shutdown_after: Option<bool>,
    /// Whether the queue and recurrence state was read again after the refusal
    /// and agreed with the state read before it, where it was re-read.
    pub(crate) state_unchanged: Option<bool>,
    /// What a read-only reading verified about the file it read.
    pub(crate) file_note: &'static str,
}

impl Changes {
    /// What a reading that never opened the file for writing changed.
    fn read_only(file_note: &'static str) -> Self {
        Self {
            gate: Gate::Advisory,
            host_started: false,
            open_epoch_before: None,
            open_epoch_after: None,
            clean_shutdown_before: None,
            clean_shutdown_after: None,
            state_unchanged: None,
            file_note,
        }
    }

    fn changed(&self) -> bool {
        self.host_started
    }

    /// The outcome this report describes, computed from the decision it
    /// publishes and from what this reading was allowed to do. It is the only
    /// input the report's authority and verification fields read, so a report
    /// cannot describe an authority the decision did not produce, and a
    /// successful arm cannot describe a refusal.
    fn outcome(&self, refused: bool, requested: bool) -> ActivationOutcome {
        match (self.gate, refused, requested) {
            (Gate::Authoritative, false, true) => ActivationOutcome::Armed,
            (_, true, _) => ActivationOutcome::Refused,
            (Gate::Authoritative, false, false) => ActivationOutcome::NotRequested,
            (Gate::Advisory, false, true) => ActivationOutcome::Advisory,
            (Gate::Advisory, false, false) => ActivationOutcome::NotRequested,
        }
    }

    /// The report. `outcome` is the decision the report publishes, and
    /// `state_confirmed` says whether the authoritative state check actually
    /// ran and passed: a field is reported verified only when it did.
    fn to_json(&self, outcome: ActivationOutcome, state_confirmed: bool) -> Value {
        let startup = match (self.host_started, outcome) {
            (true, ActivationOutcome::Armed) => json!({
                "changed": true,
                "open_epoch": {"before": self.open_epoch_before, "at_decision": self.open_epoch_after},
                "clean_shutdown": {
                    "before": self.clean_shutdown_before,
                    "at_decision": self.clean_shutdown_after,
                    "after_the_host_closed": Value::Null,
                },
                "detail": "the host's startup wrote the database metadata before the decision: it incremented open_epoch and set clean_shutdown = 0. This host is serving now; what it writes when it closes is not assessed in this report",
            }),
            (true, _) => json!({
                "changed": true,
                "open_epoch": {"before": self.open_epoch_before, "at_decision": self.open_epoch_after},
                "clean_shutdown": {
                    "before": self.clean_shutdown_before,
                    "at_decision": self.clean_shutdown_after,
                    "after_the_host_closed": Value::Null,
                },
                "detail": "the host's startup wrote the database metadata before the decision: it incremented open_epoch and set clean_shutdown = 0. Whether closing it wrote clean_shutdown = 1 again is read afterwards and recorded in clean_shutdown.after_the_host_closed",
            }),
            (false, ActivationOutcome::Refused) => json!({
                "changed": false,
                "detail": "open_epoch and clean_shutdown were read and never written: this refusal was decided before the process opened the database, so no host, no execution runtime and no startup metadata write exists",
            }),
            (false, _) => json!({
                "changed": false,
                "detail": "open_epoch and clean_shutdown were read and never written; the file was not opened for writing, so no startup, no shutdown and no execution runtime ran",
            }),
        };
        let job_and_allocation_state = match outcome {
            ActivationOutcome::Armed if state_confirmed => json!({
                "changed": false,
                "verified": true,
                "detail": "verified at the decision: the queue counts and the recurrence state this host read under its exclusive lock are exactly the state the operator's confirmation named, and the activation gate itself changed none of them; this host is still serving, so the queue after it stops is not assessed here",
            }),
            ActivationOutcome::Armed => json!({
                "changed": false,
                "verified": false,
                "detail": "not verified: no state check ran for this reading, so this report claims none",
            }),
            ActivationOutcome::Refused if self.host_started => json!({
                "changed": self.state_unchanged.is_some_and(|unchanged| !unchanged),
                "verified": self.state_unchanged.is_some(),
                "detail": match self.state_unchanged {
                    Some(true) => "the queue counts and the recurrence state read after the refusal are identical to the ones read before it",
                    Some(false) => "the host's startup changed the queue or recurrence state (recovery of work an earlier run abandoned); the report below describes the state before it",
                    None => "not verified: this report was written before any second reading of the database was made, so it claims none",
                },
            }),
            ActivationOutcome::Refused => json!({
                "changed": false,
                "verified": true,
                "detail": "the refusal was decided before this process opened the database: it was only read, its bytes were verified unchanged, so no startup, shutdown or execution runtime could change its Job or Allocation state",
            }),
            ActivationOutcome::NotRequested | ActivationOutcome::Advisory => json!({
                "changed": false,
                "verified": true,
                "detail": "the database was never opened as a host, so no startup or shutdown could change its Job or Allocation state; the read-only reading verified the file unchanged",
            }),
        };
        let execution_authority = match outcome {
            ActivationOutcome::Armed => json!({
                "granted": true,
                "scope": "this host run",
                "active_after_the_run": false,
                "detail": "this host run holds the ArmedActivation its own preflight produced under the exclusive database lock, and the execution runtime was armed with it. The authority belongs to this run only: it is not stored, not cloneable and consumed by the one runtime, so no activation authority outlives it. This report records a decision already taken and grants nothing itself",
            }),
            ActivationOutcome::Refused if self.host_started => json!({
                "granted": false,
                "scope": "this host run",
                "active_after_the_run": false,
                "detail": "the authoritative preflight refused, so no ArmedActivation was produced: the execution runtime this host would have started was never armed, and every runtime this process starts is unarmed",
            }),
            ActivationOutcome::Refused => json!({
                "granted": false,
                "scope": "no host run",
                "active_after_the_run": false,
                "detail": "the refusal was decided from a read-only reading before any host or execution runtime existed, so no ArmedActivation was produced and nothing was armed",
            }),
            ActivationOutcome::NotRequested | ActivationOutcome::Advisory => json!({
                "granted": false,
                "scope": "no host run",
                "active_after_the_run": false,
                "detail": "a read-only preflight arms nothing: no host, no execution runtime and no ArmedActivation exists in this process",
            }),
        };
        json!({
            "activation": {
                "changed": false,
                "detail": "the gate itself never writes: no Job, recurrence or Allocation is mutated to make its decision come out either way",
            },
            "job_and_allocation_state": job_and_allocation_state,
            "database_file": {"changed": self.host_started, "detail": self.file_note},
            "startup_metadata": startup,
            "execution_authority": execution_authority,
        })
    }
}

/// Reads an existing database without opening it for writing and judges what
/// a first activation would decide; see [`preflight`].
pub(crate) fn preflight(
    data_directory: &DataDirectory,
    request: Option<&ActivationRequest>,
) -> Preflight {
    let root = data_directory.root();
    let findings = read_offline(root);
    let changes = Changes::read_only(match findings.access {
        "in_place_read_only" => {
            "unchanged: the file was read through a read-only handle, and its bytes, mode, \
             modification time, device and inode were verified identical afterwards"
        }
        _ => {
            "unchanged: redb had to repair this database, so a private unlinked copy was read and \
             the original was never opened for writing; its bytes, mode, modification time, \
             device and inode were verified identical afterwards"
        }
    });
    decide(&findings, request, &changes)
}

/// Reads `root` without opening it for writing; the one implementation both
/// the advisory preflight and a refused `serve` use.
fn read_offline(root: &Path) -> Findings {
    let identity = DatabaseIdentity::of(root);
    let path = root.join(crate::db::DATABASE_FILE_NAME);
    // An absent or empty file is a database this command refuses rather than
    // creates. Reading it read-only would fail anyway, but naming the reason
    // is what tells the operator the difference between a typo and a
    // database that was never initialised.
    if let Ok(metadata) = std::fs::symlink_metadata(&path)
        && metadata.is_file()
        && metadata.len() == 0
    {
        return Findings {
            root: root.to_path_buf(),
            identity,
            epoch: None,
            clean_shutdown: None,
            snapshot: Err((
                "database_uninitialized",
                format!(
                    "{} is empty; loopback-scheduling never initialises a database, so activate over one a host has already created",
                    path.display()
                ),
            )),
            in_host: false,
            access: "refused",
            copied: false,
            file_bytes: Some(0),
            file_mode: None,
        };
    }
    let read: Result<OfflineActivation, _> =
        crate::db::redb::inspect_activation_offline(root, READY_PREVIEW);
    match read {
        Ok(read) => Findings {
            root: root.to_path_buf(),
            identity,
            epoch: Some(read.snapshot.open_epoch),
            clean_shutdown: Some(read.snapshot.clean_shutdown),
            snapshot: Ok(read.snapshot),
            in_host: false,
            access: read.access,
            copied: read.copied,
            file_bytes: Some(read.file.bytes),
            file_mode: Some(read.file.mode),
        },
        Err(error) => Findings {
            root: root.to_path_buf(),
            identity,
            epoch: None,
            clean_shutdown: None,
            snapshot: Err(classify_read_error(&error)),
            in_host: false,
            access: "refused",
            copied: false,
            file_bytes: None,
            file_mode: None,
        },
    }
}

/// Why a read-only reading could not read the database, as a refusal code.
/// A database that exists but was never initialised is distinguished from one
/// that could not be read, because the first is a database this command must
/// refuse rather than create; see `crate::db::redb::inspect_activation_offline`.
fn classify_read_error(error: &crate::db::StorageError) -> (&'static str, String) {
    let detail = error.detail.clone();
    if detail.contains("unmarked database") || detail.contains("database is not a regular file") {
        ("database_uninitialized", detail)
    } else {
        ("preflight_read", detail)
    }
}

/// Reads what an activation decides on and judges it. `request` is the
/// operator's activation when there is one; without it the report says the
/// activation was not requested and the host stays unarmed.
fn decide(
    findings: &Findings,
    request: Option<&ActivationRequest>,
    changes: &Changes,
) -> Preflight {
    let mut checks = Vec::new();
    let mut refusal: Option<ActivationRefusal> = None;
    let mut refuse = |code: &'static str, detail: String, checks: &mut Vec<Value>| {
        checks.push(json!({"check": code, "pass": false, "detail": detail}));
        if refusal.is_none() {
            refusal = Some(ActivationRefusal { code, detail });
        }
    };
    let mut pass = |code: &'static str, detail: String, checks: &mut Vec<Value>| {
        checks.push(json!({"check": code, "pass": true, "detail": detail}));
    };

    // 1. The build.
    if ExecutionControl::AutonomousScheduling.is_enabled() {
        pass(
            "control_compiled",
            "autonomous_scheduling is compiled in".to_owned(),
            &mut checks,
        );
    } else {
        refuse(
            "control_compiled",
            ExecutionControl::AutonomousScheduling.refusal(),
            &mut checks,
        );
    }
    let incompatible: Vec<&str> = INCOMPATIBLE_CONTROLS
        .iter()
        .filter(|control| control.is_enabled())
        .map(|control| control.name())
        .collect();
    if incompatible.is_empty() {
        pass(
            "loopback_only_build",
            "no external, redispatch or legacy execution control is compiled in".to_owned(),
            &mut checks,
        );
    } else {
        refuse(
            "loopback_only_build",
            format!(
                "a loopback-only activation refuses a build with {} enabled",
                incompatible.join(", ")
            ),
            &mut checks,
        );
    }

    // 2. Identity: the directory and the database the operator named.
    let root = findings.root.as_path();
    match request {
        // Not a refusal: a preflight without a request only reports.
        None => checks.push(json!({
            "check": "activation_requested",
            "pass": false,
            "detail": "no activation was requested for this run",
        })),
        Some(request) => {
            pass(
                "activation_requested",
                "the operator requested a first loopback activation".to_owned(),
                &mut checks,
            );
            match same_directory(&request.data_directory, root) {
                Ok(()) => pass(
                    "data_directory_identity",
                    format!("the named data directory resolves to {}", root.display()),
                    &mut checks,
                ),
                Err(detail) => refuse("data_directory_identity", detail, &mut checks),
            }
            match environment_agrees(root) {
                Ok(()) => pass(
                    "data_directory_environment",
                    format!("{DATA_DIRECTORY_VARIABLE} is unset or names the same directory"),
                    &mut checks,
                ),
                Err(detail) => refuse("data_directory_environment", detail, &mut checks),
            }
            match &findings.identity {
                Ok(identity) if *identity == request.database => pass(
                    "database_identity",
                    format!("the database file's identity is {identity}"),
                    &mut checks,
                ),
                Ok(identity) => refuse(
                    "database_identity",
                    format!(
                        "the operator named database {} but the database file here is {identity}",
                        request.database
                    ),
                    &mut checks,
                ),
                Err(detail) => refuse("database_identity", detail.clone(), &mut checks),
            }
        }
    }

    // 3. The database, its queues and its loops.
    let snapshot = match &findings.snapshot {
        Err((code, detail)) => {
            refuse(code, detail.clone(), &mut checks);
            None
        }
        Ok(snapshot) => {
            if findings.in_host {
                pass(
                    "database_open",
                    "the database is open under this host's exclusive lock".to_owned(),
                    &mut checks,
                );
            } else {
                pass(
                    "database_read_only",
                    format!(
                        "the database was read without being opened for writing ({})",
                        findings.access
                    ),
                    &mut checks,
                );
            }
            if findings.clean_shutdown == Some(false) {
                checks.push(json!({
                    "check": "clean_shutdown",
                    "pass": true,
                    "detail": "the database was not closed cleanly, so this reading is of recovered state; the host's own startup may change it again",
                }));
            }
            Some(snapshot)
        }
    };

    let mut queues = Value::Null;
    let mut ready_jobs = Vec::new();
    if let Some(snapshot) = snapshot {
        let counts = &snapshot.counts;
        queues = json!({
            "pending": counts.pending,
            "ready": counts.ready,
            "delayed": counts.delayed,
            "running": counts.running,
            "held": counts.held,
            "ready_listed": snapshot.ready.len(),
            "ready_truncated": snapshot.truncated,
        });
        for entry in &snapshot.ready {
            let eligibility = loopback_eligibility(entry.frozen.as_ref());
            ready_jobs.push(json!({
                "job": entry.job.to_string(),
                "priority": entry.priority,
                "submitted_at_us": entry.submitted_at_us,
                "phase": format!("{:?}", entry.phase).to_ascii_lowercase(),
                "generative": entry.generative,
                "autonomous_loopback_eligibility": eligibility.to_json(),
            }));
        }
        let listed = snapshot.ready.len() as u64;
        let consistent = if snapshot.truncated {
            listed == READY_PREVIEW as u64 && counts.ready > listed
        } else {
            listed == counts.ready
        };
        if consistent {
            pass(
                "preflight_consistent",
                "the ready count and the ready entries agree".to_owned(),
                &mut checks,
            );
        } else {
            refuse(
                "preflight_consistent",
                format!(
                    "the ready index counts {} entries but {listed} were read (truncated: {})",
                    counts.ready, snapshot.truncated
                ),
                &mut checks,
            );
        }
        if counts.held == 0 && counts.running == 0 {
            pass(
                "no_held_allocations",
                "no Allocation holds capacity".to_owned(),
                &mut checks,
            );
        } else {
            refuse(
                "no_held_allocations",
                format!(
                    "{} Allocation(s) hold capacity ({} running); a first activation never adopts them; run `ocg inspect-held` once the host has stopped",
                    counts.held, counts.running
                ),
                &mut checks,
            );
        }
        if counts.ready == 0 && counts.pending == 0 && counts.delayed == 0 {
            pass(
                "no_queued_work",
                "no Job is Pending, Ready or Delayed".to_owned(),
                &mut checks,
            );
        } else {
            refuse(
                "no_queued_work",
                format!(
                    "the database already holds queued work ({} ready, {} pending, {} delayed; a queued native Job is counted in both the ready and the pending queue); a first activation refuses to arm over it and changes none of it; cancel it or run it through its own authorized workflow, or activate over an empty diagnostic database",
                    counts.ready, counts.pending, counts.delayed
                ),
                &mut checks,
            );
        }
        audit_recurrences(&snapshot.recurrences, &mut refuse, &mut pass, &mut checks);
    }

    // 4. The confirmation, when this reading is the authoritative one.
    let mut state_confirmed = false;
    if let (Some(request), Some(snapshot), true) = (request, snapshot, findings.in_host) {
        let expected_state = state_digest(root, &findings.identity, snapshot);
        let expected_challenge = challenge_of(
            &expected_state,
            request.confirmation.open_epoch,
            request.confirmation.clean_shutdown,
        );
        if request.confirmation.challenge != expected_challenge {
            refuse(
                "activation_confirmation_invalid",
                "the challenge does not describe the database identity, state and open_epoch this host read; it was not produced for this database, so it is not an authorization for it".to_owned(),
                &mut checks,
            );
        } else if request.confirmation.state != expected_state {
            refuse(
                "activation_state_changed",
                "the database's queue and recurrence state is not the state the operator confirmed; \
                 the host read a different one, so the confirmation no longer names it; run \
                 `ocg loopback-scheduling preflight` again and confirm the state it prints"
                    .to_owned(),
                &mut checks,
            );
        } else if snapshot.open_epoch != request.confirmation.open_epoch.saturating_add(1) {
            refuse(
                "activation_confirmation_stale",
                format!(
                    "the operator confirmed the state at open_epoch {} but this opening is open_epoch {}; \
                     the database was opened by something else in between, so the confirmation does \
                     not name this run; run `ocg loopback-scheduling preflight` again",
                    request.confirmation.open_epoch, snapshot.open_epoch
                ),
                &mut checks,
            );
        } else {
            state_confirmed = true;
            pass(
                "activation_confirmation",
                "the operator confirmed this state, at this open_epoch, in this process".to_owned(),
                &mut checks,
            );
        }
    }

    let controls: serde_json::Map<String, Value> = ExecutionControl::ALL
        .iter()
        .map(|control| {
            (
                control.name().to_owned(),
                Value::String(
                    match control.state() {
                        ControlState::Enabled => "enabled",
                        ControlState::Disabled => "disabled",
                    }
                    .to_owned(),
                ),
            )
        })
        .collect();
    // The decision's outcome, and the only thing the report's verdict,
    // authority and verification fields read. Reporting it cannot change it.
    let outcome = changes.outcome(refusal.is_some(), request.is_some());
    let confirmation = snapshot.map(|snapshot| {
        let state = state_digest(root, &findings.identity, snapshot);
        (
            challenge_of(&state, snapshot.open_epoch, snapshot.clean_shutdown),
            state,
        )
    });
    let report = json!({
        "kind": "autonomous_loopback_activation_preflight",
        "verdict": outcome.verdict(),
        "refusal": refusal.as_ref().map(|refusal| json!({"code": refusal.code, "detail": refusal.detail})),
        "data_directory": root.display().to_string(),
        "database_identity": findings.identity.as_ref().ok().map(ToString::to_string),
        "database_identity_kind": "file device:inode of ocg.redb on this machine",
        "database_identity_error": findings.identity.as_ref().err(),
        "genesis_job": GENESIS_JOB.to_string(),
        "database_epoch": findings.epoch,
        "clean_shutdown": findings.clean_shutdown,
        "read": {
            "access": findings.access,
            "opened_for_writing": findings.in_host,
            "copied_to_be_read": findings.copied,
            "scope": "one read transaction: the queue counts, the first ready entries, every recurrence and every row of their due index",
        },
        "file": {
            "bytes": findings.file_bytes,
            "mode": findings.file_mode.map(|mode| format!("{mode:04o}")),
            "device_inode": findings.identity.as_ref().ok().map(ToString::to_string),
            "unchanged": !findings.in_host && findings.snapshot.is_ok(),
            "verified": if findings.in_host {
                json!([])
            } else {
                json!(["bytes", "mode", "modification time", "device", "inode", "the name still reaching that file"])
            },
            "detail": if findings.in_host {
                "opened as a host, which wrote it: this reading cannot claim the file is unchanged"
            } else {
                "read without being opened for writing; every field above was compared before and after the read and is identical"
            },
        },
        "activation_requested": request.is_some(),
        "requested": request.map(|request| {
            let mut requested = json!({
                "data_directory": request.data_directory.display().to_string(),
                "database_identity": request.database.to_string(),
            });
            // Only the authoritative reading has a confirmation to report.
            if findings.in_host {
                requested["confirmed_open_epoch"] = json!(request.confirmation.open_epoch);
            }
            requested
        }),
        "confirmation_challenge": confirmation.as_ref().map(|(challenge, _)| challenge.clone()),
        "confirmation_state": confirmation.as_ref().map(|(_, state)| state.clone()),
        "execution_controls": controls,
        "queues": queues,
        "recurrences": recurrences_json(&findings.snapshot),
        "ready_jobs": ready_jobs,
        "checks": checks,
        "destination_policy": "literal http://127.0.0.1:<port> only; localhost, ::1, DNS names and external destinations are never scheduled",
        "changed": changes.changed(),
        "changes": changes.to_json(outcome, state_confirmed),
    });
    Preflight { report, refusal }
}

/// The loop checks: an existing loop that can create work refuses, and so
/// does a recurrence index that does not describe the recurrences it belongs
/// to.
fn audit_recurrences(
    audit: &RecurrenceAudit,
    refuse: &mut impl FnMut(&'static str, String, &mut Vec<Value>),
    pass: &mut impl FnMut(&'static str, String, &mut Vec<Value>),
    checks: &mut Vec<Value>,
) {
    match audit.inconsistency.as_ref() {
        None => pass(
            "recurrence_index_consistent",
            format!(
                "the {} recurrence(s) and the {} row(s) of their due index describe the same state",
                audit.total, audit.due_index_rows
            ),
            checks,
        ),
        Some(detail) => refuse(
            "recurrence_index_inconsistent",
            format!(
                "{detail}; a first activation refuses a database whose loops cannot be read reliably"
            ),
            checks,
        ),
    }
    match audit.blocking() {
        None => pass(
            "no_existing_recurrences",
            format!(
                "no loop can create work: {} recurrence(s) are Halted and none is Active or WaitingDue",
                audit.halted
            ),
            checks,
        ),
        Some(detail) => refuse(
            "no_existing_recurrences",
            format!(
                "{detail}; a first activation never lets work that already existed begin, whether its time has come or not"
            ),
            checks,
        ),
    }
}

fn recurrences_json(snapshot: &Result<ActivationSnapshot, (&'static str, String)>) -> Value {
    match snapshot {
        Err(_) => Value::Null,
        Ok(snapshot) => {
            let audit = &snapshot.recurrences;
            json!({
                "total": audit.total,
                "active": audit.active,
                "waiting_due": audit.waiting_due,
                "halted": audit.halted,
                "due_now": audit.due_now,
                "future": audit.waiting_due - audit.due_now,
                "due_index_rows": audit.due_index_rows,
                "dependency_waiters": audit.dependency_waiters,
                "consistent": audit.inconsistency.is_none(),
                "inconsistency": audit.inconsistency,
            })
        }
    }
}

/// A digest of everything about a database that a first activation refuses
/// on, and nothing that a host's ordinary opening changes. The operator
/// confirms this digest; the authoritative reading recomputes it, so a
/// confirmation cannot outlive the state it named.
fn state_digest(
    root: &Path,
    identity: &Result<DatabaseIdentity, String>,
    snapshot: &ActivationSnapshot,
) -> String {
    use aws_lc_rs::digest::{Context, SHA256};
    /// Length-prefixes `bytes`, so two different field splits cannot collide.
    fn field(context: &mut Context, bytes: &[u8]) {
        context.update(&(bytes.len() as u64).to_le_bytes());
        context.update(bytes);
    }
    let mut context = Context::new(&SHA256);
    field(&mut context, root.to_string_lossy().as_bytes());
    field(
        &mut context,
        if identity.is_ok() { "file" } else { "none" }.as_bytes(),
    );
    if let Ok(identity) = identity {
        context.update(&identity.device.to_le_bytes());
        context.update(&identity.inode.to_le_bytes());
    }
    let counts = &snapshot.counts;
    for value in [
        counts.pending,
        counts.ready,
        counts.delayed,
        counts.running,
        counts.held,
    ] {
        context.update(&value.to_le_bytes());
    }
    let audit = &snapshot.recurrences;
    for value in [
        audit.total,
        audit.active,
        audit.waiting_due,
        audit.halted,
        audit.due_index_rows,
        audit.dependency_waiters,
    ] {
        context.update(&value.to_le_bytes());
    }
    field(
        &mut context,
        audit
            .inconsistency
            .as_deref()
            .unwrap_or("consistent")
            .as_bytes(),
    );
    hex(context.finish().as_ref())
}

/// The challenge the operator copies from `preflight` and the host checks:
/// `ocg1-` and the first 16 hex digits of a digest over everything the
/// confirmation names. Because it covers `open_epoch`, which every opening
/// increments, a challenge names one opening of one database state, and the
/// opening after it can never reuse it.
fn challenge_of(state: &str, open_epoch: u64, clean_shutdown: bool) -> String {
    use aws_lc_rs::digest::{Context, SHA256};
    let mut context = Context::new(&SHA256);
    context.update(b"ocg-first-loopback-activation-v1");
    context.update(&(state.len() as u64).to_le_bytes());
    context.update(state.as_bytes());
    context.update(&open_epoch.to_le_bytes());
    context.update(&[u8::from(clean_shutdown)]);
    let digest = hex(context.finish().as_ref());
    format!("ocg1-{}", &digest[..16])
}

fn hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push_str(&format!("{byte:02x}"));
    }
    text
}

/// Runs the preflight for `request` and arms the run when it passes. The
/// report is returned either way so the caller can publish it; a refusal
/// leaves the database untouched.
pub(crate) async fn arm(
    database: &DatabaseHandle,
    data_directory: &DataDirectory,
    request: &ActivationRequest,
) -> (Value, Result<ArmedActivation, ActivationRefusal>) {
    let root = data_directory.root();
    let identity = DatabaseIdentity::of(root);
    let findings = match database.activation_snapshot(READY_PREVIEW).await {
        Ok(snapshot) => Findings {
            root: root.to_path_buf(),
            identity,
            epoch: Some(snapshot.open_epoch),
            clean_shutdown: Some(snapshot.clean_shutdown),
            snapshot: Ok(snapshot),
            in_host: true,
            access: "open",
            copied: false,
            file_bytes: None,
            file_mode: None,
        },
        Err(error) => Findings {
            root: root.to_path_buf(),
            identity,
            epoch: None,
            clean_shutdown: None,
            snapshot: Err((
                "preflight_read",
                format!("the queues could not be read: {error}"),
            )),
            in_host: true,
            access: "open",
            copied: false,
            file_bytes: None,
            file_mode: None,
        },
    };
    let changes = Changes {
        gate: Gate::Authoritative,
        host_started: true,
        open_epoch_before: Some(request.confirmation.open_epoch),
        open_epoch_after: findings.epoch,
        clean_shutdown_before: Some(request.confirmation.clean_shutdown),
        clean_shutdown_after: findings.clean_shutdown,
        state_unchanged: None,
        file_note: "written by this host's own startup and shutdown; the activation decision itself wrote nothing, and the file's identity (device and inode) is unchanged",
    };
    let Preflight { report, refusal } = decide(&findings, Some(request), &changes);
    // This is the authoritative gate, so a decision without a refusal is the
    // one that arms, and the report already says so from that same outcome.
    // The token is constructed on this path, immediately, and the caller
    // publishes the report only after receiving both: the report and the
    // authority it describes cannot disagree, and neither is derived from the
    // other's text.
    match refusal {
        Some(refusal) => (report, Err(refusal)),
        None => (report, Ok(ArmedActivation { _private: () })),
    }
}

/// Records, on a refusal report, what a second read-only reading of the same
/// database found after the host closed it. The caller reads the database
/// again once nothing owns it; until then the report says the scope was not
/// verified, and never claims it was. A report that is not a refusal is left
/// untouched: an armed host publishes its report while it still serves, so no
/// second reading of a database it owns describes it, and no refusal text may
/// reach the report of an authority that was granted.
pub(crate) fn record_state_reread(
    report: &mut Value,
    after: Option<&Value>,
    unchanged: Option<bool>,
) {
    if report.get("verdict").and_then(Value::as_str) != Some("refused") {
        return;
    }
    if let Some(after) = after {
        report["changes"]["startup_metadata"]["open_epoch"]["after_the_host_closed"] =
            after.get("database_epoch").cloned().unwrap_or(Value::Null);
        report["changes"]["startup_metadata"]["clean_shutdown"]["after_the_host_closed"] =
            after.get("clean_shutdown").cloned().unwrap_or(Value::Null);
    }
    let job_and_allocation = json!({
        "changed": unchanged.is_some_and(|unchanged| !unchanged),
        "verified": unchanged.is_some(),
        "detail": match unchanged {
            Some(true) => "the queue counts and the recurrence state read after the refusal are identical to the ones read before it",
            Some(false) => "the host's startup changed the queue or recurrence state (recovery of work an earlier run abandoned); the report describes the state before it",
            None => "not verified: the database has not been read again since the refusal, so this report claims nothing about what it holds now",
        },
    });
    report["changes"]["job_and_allocation_state"] = job_and_allocation;
}

/// Whether `named` resolves to exactly `root`, the directory the host opened.
fn same_directory(named: &Path, root: &Path) -> Result<(), String> {
    let resolved = named.canonicalize().map_err(|error| {
        format!(
            "the named data directory {} cannot be resolved: {error}",
            named.display()
        )
    })?;
    if resolved != root {
        return Err(format!(
            "the named data directory {} resolves to {}, not the opened {}",
            named.display(),
            resolved.display(),
            root.display()
        ));
    }
    Ok(())
}

/// Whether the environment, when it names a data directory, names `root`:
/// an activation over one directory while the environment points elsewhere
/// is ambiguous and refused.
fn environment_agrees(root: &Path) -> Result<(), String> {
    let Some(value) = std::env::var_os(DATA_DIRECTORY_VARIABLE).filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let named = PathBuf::from(value);
    match named.canonicalize() {
        Ok(resolved) if resolved == root => Ok(()),
        Ok(resolved) => Err(format!(
            "{DATA_DIRECTORY_VARIABLE} resolves to {}, not the activated {}",
            resolved.display(),
            root.display()
        )),
        Err(error) => Err(format!(
            "{DATA_DIRECTORY_VARIABLE} names {} which cannot be resolved: {error}",
            named.display()
        )),
    }
}
