use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use crate::db::DatabaseHandle;
use crate::db::entities::JobId;
use crate::db::redb::{ActivationSnapshot, GENESIS_JOB, OfflineActivation, RecurrenceAudit};
use crate::system::data_directory::{DATA_DIRECTORY_VARIABLE, DataDirectory};
use crate::system::execution_controls::{ControlState, ExecutionControl};
use crate::system::native_scheduler::{NOT_ASSESSED, loopback_eligibility};

pub(crate) const READY_PREVIEW: usize = 32;

#[derive(Clone, Debug)]
pub(crate) struct ActivationRequest {
    pub(crate) data_directory: PathBuf,
    pub(crate) database: DatabaseIdentity,
    pub(crate) confirmation: Confirmation,
    pub(crate) mode: ActivationMode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ActivationMode {
    FirstActivation,
    QueuedWork(QueuedAllowlist),
}

impl ActivationMode {
    const fn name(&self) -> &'static str {
        match self {
            Self::FirstActivation => "first_activation",
            Self::QueuedWork(_) => "queued_work",
        }
    }

    fn allowlist(&self) -> Option<&QueuedAllowlist> {
        match self {
            Self::FirstActivation => None,
            Self::QueuedWork(allowlist) => Some(allowlist),
        }
    }

    pub(crate) const fn subject(&self) -> &'static str {
        match self {
            Self::FirstActivation => "a first activation",
            Self::QueuedWork(_) => "a queued-work activation",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct QueuedAllowlist(Vec<JobId>);

impl QueuedAllowlist {
    pub(crate) const MAX: usize = 16;

    pub(crate) fn parse(items: &[String]) -> Result<Self, String> {
        if items.is_empty() {
            return Err("a queued-work activation names at least one Job".to_owned());
        }
        if items.len() > Self::MAX {
            return Err(format!(
                "a queued-work activation names at most {} Jobs, not {}",
                Self::MAX,
                items.len()
            ));
        }
        let mut jobs = Vec::with_capacity(items.len());
        for item in items {
            let job: JobId = item
                .parse()
                .map_err(|_| format!("{item:?} is not a canonical Job identity"))?;
            if jobs.contains(&job) {
                return Err(format!("Job {job} is named more than once"));
            }
            jobs.push(job);
        }
        jobs.sort();
        Ok(Self(jobs))
    }

    fn contains(&self, job: JobId) -> bool {
        self.0.binary_search(&job).is_ok()
    }

    fn to_json(&self) -> Value {
        Value::Array(
            self.0
                .iter()
                .map(|job| Value::String(job.to_string()))
                .collect(),
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DatabaseIdentity {
    device: u64,
    inode: u64,
}

impl DatabaseIdentity {
    pub(crate) fn parse(text: &str) -> Option<Self> {
        let (device, inode) = text.split_once(':')?;
        Some(Self {
            device: device.parse().ok()?,
            inode: inode.parse().ok()?,
        })
    }

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

#[derive(Clone, Debug)]
pub(crate) struct Confirmation {
    pub(crate) challenge: String,
    pub(crate) open_epoch: u64,
    pub(crate) clean_shutdown: bool,
    pub(crate) state: String,
}

pub(crate) struct ArmedActivation {
    _private: (),
}

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

pub(crate) struct Preflight {
    pub(crate) report: Value,
    pub(crate) refusal: Option<ActivationRefusal>,
}

const INCOMPATIBLE_CONTROLS: [ExecutionControl; 4] = [
    ExecutionControl::ExternalProviderTransport,
    ExecutionControl::ScheduledExternalTransport,
    ExecutionControl::RecoveryAndRedispatch,
    ExecutionControl::LegacyProviderExecution,
];

struct Findings {
    root: PathBuf,
    identity: Result<DatabaseIdentity, String>,
    epoch: Option<u64>,
    clean_shutdown: Option<bool>,
    snapshot: Result<ActivationSnapshot, (&'static str, String)>,
    in_host: bool,
    access: &'static str,
    copied: bool,
    file_bytes: Option<u64>,
    file_mode: Option<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Gate {
    Advisory,
    Authoritative,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ActivationOutcome {
    NotRequested,
    Advisory,
    Refused,
    Armed,
}

impl ActivationOutcome {
    const fn verdict(self) -> &'static str {
        match self {
            Self::NotRequested => "not_requested",
            Self::Advisory => "pass",
            Self::Refused => "refused",
            Self::Armed => "armed",
        }
    }
}

pub(crate) struct Changes {
    gate: Gate,
    pub(crate) host_started: bool,
    pub(crate) open_epoch_before: Option<u64>,
    pub(crate) open_epoch_after: Option<u64>,
    pub(crate) clean_shutdown_before: Option<bool>,
    pub(crate) clean_shutdown_after: Option<bool>,
    pub(crate) state_unchanged: Option<bool>,
    pub(crate) file_note: &'static str,
}

impl Changes {
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

    fn outcome(&self, refused: bool, requested: bool) -> ActivationOutcome {
        match (self.gate, refused, requested) {
            (Gate::Authoritative, false, true) => ActivationOutcome::Armed,
            (_, true, _) => ActivationOutcome::Refused,
            (Gate::Authoritative, false, false) => ActivationOutcome::NotRequested,
            (Gate::Advisory, false, true) => ActivationOutcome::Advisory,
            (Gate::Advisory, false, false) => ActivationOutcome::NotRequested,
        }
    }

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

fn read_offline(root: &Path) -> Findings {
    let identity = DatabaseIdentity::of(root);
    let path = root.join(crate::db::DATABASE_FILE_NAME);
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

fn classify_read_error(error: &crate::db::StorageError) -> (&'static str, String) {
    let detail = error.detail.clone();
    if detail.contains("unmarked database") || detail.contains("database is not a regular file") {
        ("database_uninitialized", detail)
    } else {
        ("preflight_read", detail)
    }
}

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

    let root = findings.root.as_path();
    match request {
        None => checks.push(json!({
            "check": "activation_requested",
            "pass": false,
            "detail": "no activation was requested for this run",
        })),
        Some(request) => {
            pass(
                "activation_requested",
                match request.mode.allowlist() {
                    None => "the operator requested a first loopback activation".to_owned(),
                    Some(allowlist) => format!(
                        "the operator requested a queued-work loopback activation for {} named Job(s)",
                        allowlist.0.len()
                    ),
                },
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

    let queued = request.and_then(|request| request.mode.allowlist());
    let mode = request.map_or(&ActivationMode::FirstActivation, |request| &request.mode);
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
            let mut listed = json!({
                "job": entry.job.to_string(),
                "priority": entry.priority,
                "submitted_at_us": entry.submitted_at_us,
                "phase": format!("{:?}", entry.phase).to_ascii_lowercase(),
                "generative": entry.generative,
                "autonomous_loopback_eligibility": eligibility.to_json(),
            });
            if let Some(allowlist) = queued {
                listed["revision"] = json!(entry.revision);
                listed["job_row_sha256"] = json!(hex(&entry.row_digest));
                listed["allowlisted"] = json!(allowlist.contains(entry.job));
                listed["never_started_evidence"] = json!(entry.evidence);
            }
            ready_jobs.push(listed);
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
                    "{} Allocation(s) hold capacity ({} running); {} never adopts them; run `ocg inspect-held` once the host has stopped",
                    counts.held,
                    counts.running,
                    mode.subject()
                ),
                &mut checks,
            );
        }
        match queued {
            None => {
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
            }
            Some(allowlist) => {
                audit_queued_work(snapshot, allowlist, &mut refuse, &mut pass, &mut checks);
            }
        }
        audit_recurrences(
            &snapshot.recurrences,
            mode.subject(),
            &mut refuse,
            &mut pass,
            &mut checks,
        );
    }

    let mut state_confirmed = false;
    if let (Some(request), Some(snapshot), true) = (request, snapshot, findings.in_host) {
        let expected_state = state_digest(root, &findings.identity, snapshot, &request.mode);
        let expected_challenge = challenge_of(
            &expected_state,
            request.confirmation.open_epoch,
            request.confirmation.clean_shutdown,
            &request.mode,
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
    let outcome = changes.outcome(refusal.is_some(), request.is_some());
    let confirmation = snapshot.map(|snapshot| {
        let state = state_digest(root, &findings.identity, snapshot, mode);
        (
            challenge_of(&state, snapshot.open_epoch, snapshot.clean_shutdown, mode),
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
        "activation_mode": request.map(|request| request.mode.name()),
        "queued_work": queued.zip(snapshot).map(|(allowlist, snapshot)| json!({
            "allowlist": allowlist.to_json(),
            "authorized_jobs": snapshot
                .ready
                .iter()
                .filter(|entry| allowlist.contains(entry.job))
                .map(|entry| json!({
                    "job": entry.job.to_string(),
                    "revision": entry.revision,
                    "job_row_sha256": hex(&entry.row_digest),
                }))
                .collect::<Vec<_>>(),
            "scope": "these Jobs, exactly as the confirmation names them, and nothing that is not listed: every other Pending, Ready or Delayed Job, every Allocation and every loop refuses the activation",
            "not_assessed": NOT_ASSESSED,
        })),
        "requested": request.map(|request| {
            let mut requested = json!({
                "data_directory": request.data_directory.display().to_string(),
                "database_identity": request.database.to_string(),
                "activation_mode": request.mode.name(),
            });
            if let Some(allowlist) = request.mode.allowlist() {
                requested["allowed_jobs"] = allowlist.to_json();
            }
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

fn audit_queued_work(
    snapshot: &ActivationSnapshot,
    allowlist: &QueuedAllowlist,
    refuse: &mut impl FnMut(&'static str, String, &mut Vec<Value>),
    pass: &mut impl FnMut(&'static str, String, &mut Vec<Value>),
    checks: &mut Vec<Value>,
) {
    let counts = &snapshot.counts;
    if counts.delayed == 0 {
        pass("no_delayed_work", "no Job is Delayed".to_owned(), checks);
    } else {
        refuse(
            "no_delayed_work",
            format!(
                "{} Job(s) are Delayed; a Delayed Job is promoted into the ready queue by the scheduler without any submission, so it could begin outside the allowlist",
                counts.delayed
            ),
            checks,
        );
    }
    let bounded = !snapshot.truncated
        && counts.ready <= QueuedAllowlist::MAX as u64
        && counts.pending == counts.ready
        && snapshot.ready.len() as u64 == counts.ready;
    if bounded {
        pass(
            "queued_work_bounded",
            format!(
                "the whole queue ({} Job(s), the pending and ready indexes alike) was read and fits within the {}-Job authorization scope",
                counts.ready,
                QueuedAllowlist::MAX
            ),
            checks,
        );
    } else {
        refuse(
            "queued_work_bounded",
            format!(
                "the queue ({} ready, {} pending, {} listed, truncated: {}) is not one that {} Jobs can authorize in full: the pending and ready indexes must hold the same Jobs, and at most {} of them; a queued Job outside the listing could not be confirmed, so nothing is guessed",
                counts.ready,
                counts.pending,
                snapshot.ready.len(),
                snapshot.truncated,
                QueuedAllowlist::MAX,
                QueuedAllowlist::MAX
            ),
            checks,
        );
        return;
    }

    let mut queued: Vec<JobId> = snapshot.ready.iter().map(|entry| entry.job).collect();
    queued.sort();
    let distinct = queued.windows(2).all(|pair| pair[0] != pair[1]);
    let extra: Vec<String> = queued
        .iter()
        .filter(|job| !allowlist.contains(**job))
        .map(ToString::to_string)
        .collect();
    let missing: Vec<String> = allowlist
        .0
        .iter()
        .filter(|job| queued.binary_search(job).is_err())
        .map(ToString::to_string)
        .collect();
    if distinct && extra.is_empty() && missing.is_empty() {
        pass(
            "queued_work_allowlist_exact",
            format!(
                "the {} queued Job(s) are exactly the {} the operator named",
                queued.len(),
                allowlist.0.len()
            ),
            checks,
        );
    } else {
        refuse(
            "queued_work_allowlist_exact",
            format!(
                "the queue and the allowlist differ: queued but not allowlisted [{}]; allowlisted but not queued (never submitted, already started, settled or stale) [{}]; a ready entry listed twice: {}; no Job is authorized by being queued",
                extra.join(", "),
                missing.join(", "),
                !distinct
            ),
            checks,
        );
        return;
    }

    let mut ineligible = Vec::new();
    for entry in &snapshot.ready {
        let mut why: Vec<String> = Vec::new();
        if entry.phase != crate::db::entities::MutableJobPhase::Queued {
            why.push(format!("phase {:?}, not Queued", entry.phase));
        }
        if !entry.generative {
            why.push("not a Generative Job".to_owned());
        }
        let eligibility = loopback_eligibility(entry.frozen.as_ref());
        if !eligibility.eligible() {
            why.push(format!(
                "not eligible for autonomous loopback ({}: {})",
                eligibility.code, eligibility.detail
            ));
        }
        if !entry.key_agrees {
            why.push("its ready key disagrees with its Job row".to_owned());
        }
        if !entry.in_pending {
            why.push("it is in the ready index but not the pending index".to_owned());
        }
        if !entry.evidence.is_empty() {
            why.push(format!(
                "it has execution or ownership evidence: {}",
                entry.evidence.join(", ")
            ));
        }
        if !why.is_empty() {
            ineligible.push(format!("{}: {}", entry.job, why.join("; ")));
        }
    }
    if ineligible.is_empty() {
        pass(
            "queued_work_eligible",
            format!(
                "each of the {} named Job(s) is submitted, never started, free of Allocation, claim, request and ownership evidence, and frozen for the strict autonomous loopback endpoint policy; the scheduler's own admission is not assessed here",
                snapshot.ready.len()
            ),
            checks,
        );
    } else {
        refuse(
            "queued_work_eligible",
            format!(
                "{} named Job(s) are not eligible and are left untouched: {}",
                ineligible.len(),
                ineligible.join(" | ")
            ),
            checks,
        );
    }
}

fn audit_recurrences(
    audit: &RecurrenceAudit,
    subject: &str,
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
            format!("{detail}; {subject} refuses a database whose loops cannot be read reliably"),
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
                "{detail}; {subject} never lets work that already existed begin, whether its time has come or not"
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

fn state_digest(
    root: &Path,
    identity: &Result<DatabaseIdentity, String>,
    snapshot: &ActivationSnapshot,
    mode: &ActivationMode,
) -> String {
    use aws_lc_rs::digest::{Context, SHA256};
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
    if let Some(allowlist) = mode.allowlist() {
        field(&mut context, b"queued-work-v1");
        context.update(&(allowlist.0.len() as u64).to_le_bytes());
        for job in &allowlist.0 {
            context.update(&job.get().to_le_bytes());
        }
        context.update(&(snapshot.ready.len() as u64).to_le_bytes());
        context.update(&[u8::from(snapshot.truncated)]);
        for entry in &snapshot.ready {
            context.update(&entry.job.get().to_le_bytes());
            context.update(&[entry.priority]);
            context.update(&entry.submitted_at_us.to_le_bytes());
            context.update(&entry.revision.to_le_bytes());
            context.update(&entry.requirement.to_le_bytes());
            context.update(&entry.row_digest);
            field(&mut context, format!("{:?}", entry.phase).as_bytes());
            context.update(&[
                u8::from(entry.generative),
                u8::from(entry.key_agrees),
                u8::from(entry.in_pending),
            ]);
            field(
                &mut context,
                loopback_eligibility(entry.frozen.as_ref()).code.as_bytes(),
            );
            field(&mut context, entry.evidence.join(",").as_bytes());
        }
    }
    hex(context.finish().as_ref())
}

fn challenge_of(
    state: &str,
    open_epoch: u64,
    clean_shutdown: bool,
    mode: &ActivationMode,
) -> String {
    use aws_lc_rs::digest::{Context, SHA256};
    let mut context = Context::new(&SHA256);
    context.update(match mode {
        ActivationMode::FirstActivation => b"ocg-first-loopback-activation-v1".as_slice(),
        ActivationMode::QueuedWork(_) => b"ocg-queued-loopback-activation-v1".as_slice(),
    });
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
    match refusal {
        Some(refusal) => (report, Err(refusal)),
        None => (report, Ok(ArmedActivation { _private: () })),
    }
}

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
