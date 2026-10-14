//! Job submission, admission lookup and stop for the Control Plane.
//!
//! A submission is idempotent under the client's `Idempotency-Key`. The key
//! is recorded, as a SHA-256 digest with the fingerprint of its normalized
//! request, by the transaction that creates and locally starts its Job; see
//! [`DatabaseHandle::create_submitted_local_job`]. That record is the only
//! authority on whether a key created a Job: a key names at most one Job,
//! and only a committed one, in this process and after any restart.
//!
//! What only this process knows is kept in a [`Registry`] owned by the
//! listener, and dies with it:
//!
//! - a key being admitted, from its first request until its admission ends:
//!   every other request with the key waits for that one admission and is
//!   answered from it, so a key is never submitted twice at once;
//! - a key whose creation may have committed, but did not say so: it is
//!   never submitted again while that is possible. It is settled by the
//!   record, once a read finds it, or by a reopening of the database, after
//!   which a creation of the earlier opening can no longer commit;
//! - the stop of each Job this listener admitted, until its execution has
//!   returned.
//!
//! An admission belongs to a task of the listener's runtime, never to the
//! request that started it: a client that disconnects, or whose wait ends,
//! leaves it to run, and the Job to the execution runtime. A stop request
//! cancels the one Job's own execution and says so; how the Job settles is
//! recorded by its execution, as for any other stop.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use aws_lc_rs::digest::{Context, SHA256};
use ntex::http::StatusCode;
use ntex::http::header::HeaderMap;
use serde::Deserialize;
use serde_json::{Value, json};

use super::{
    API_VERSION, DATABASE_BUSY, DATABASE_UNAVAILABLE, INTERNAL, JOB_NOT_FOUND, Refusal, Reply,
    SHUTTING_DOWN, Shared, decimal,
};
use crate::database::{DatabaseError, DatabaseHandle};
use crate::orchestration::execution_runtime::{
    AdmissionError, ExecutionRuntimeError, JobStop, ProviderExecutionPlan,
};
use crate::persistence::{
    ExecutionConfiguration, ExecutionMode, ExecutorId, JobId, JobInputDraft, JobRecord, NewJob,
    NewSubmission, ProfileId, RequestFingerprint, SubmissionKey, SubmissionRecord, Violation,
};

/// The request header that names a submission.
pub(super) const IDEMPOTENCY_KEY: &str = "idempotency-key";
/// The fewest and most characters an idempotency key may have.
pub(super) const MIN_KEY_CHARS: usize = 16;
pub(super) const MAX_KEY_CHARS: usize = 128;
/// The longest objective and Job name, in bytes.
pub(super) const MAX_OBJECTIVE_BYTES: usize = 32 * 1024;
pub(super) const MAX_NAME_BYTES: usize = 128;
/// The most keys this process may hold unresolved at once.
const MAX_UNRESOLVED: usize = 256;

const KEY_DOMAIN: &[u8] = b"ocg.control-plane.idempotency-key.v1\n";
const REQUEST_DOMAIN: &[u8] = b"ocg.control-plane.job-submission.v1\n";

pub(super) const KEY_REQUIRED: Refusal = Refusal::new(
    StatusCode::BAD_REQUEST,
    "idempotency_key_required",
    "the request must carry one Idempotency-Key header",
);
pub(super) const INVALID_KEY: Refusal = Refusal::new(
    StatusCode::BAD_REQUEST,
    "invalid_idempotency_key",
    "the Idempotency-Key must be 16 to 128 characters of A-Z, a-z, 0-9, '-', '.', '_', '~', ':', '+', '/' or '=', given once",
);
pub(super) const INVALID_BODY: Refusal = Refusal::new(
    StatusCode::BAD_REQUEST,
    "invalid_body",
    "the body is not a Job submission this API accepts",
);
const KEY_CONFLICT: Refusal = Refusal::new(
    StatusCode::CONFLICT,
    "idempotency_key_conflict",
    "the Idempotency-Key was used for a different request; nothing was submitted",
);
const UNCERTAIN: Refusal = Refusal::new(
    StatusCode::INTERNAL_SERVER_ERROR,
    "admission_uncertain",
    "whether the Job was admitted is unknown; it is not submitted again; look the key up later",
);
const TOO_MANY_UNRESOLVED: Refusal = Refusal::new(
    StatusCode::SERVICE_UNAVAILABLE,
    "too_many_submissions",
    "too many submissions are unresolved; nothing was submitted; retry later",
);
const PROFILE_NOT_FOUND: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "llm_profile_not_found",
    "no such LLM Profile; nothing was submitted",
);
const EXECUTOR_NOT_FOUND: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "executor_not_found",
    "no such Executor; nothing was submitted",
);
const EXECUTOR_INCOMPATIBLE: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "executor_incompatible",
    "the Executor cannot run the LLM Profile; nothing was submitted",
);
const CONFIGURATION_REFUSED: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "configuration_refused",
    "the selected configuration cannot run this Job; nothing was admitted",
);
const RUNTIME_BUSY: Refusal = Refusal::new(
    StatusCode::SERVICE_UNAVAILABLE,
    "runtime_busy",
    "OCG has too much work queued; nothing was admitted; retry later",
);
const STORAGE_REFUSED: Refusal = Refusal::new(
    StatusCode::SERVICE_UNAVAILABLE,
    "storage_refused",
    "storage refused the Job; nothing was admitted",
);
const NOT_STOPPABLE: Refusal = Refusal::new(
    StatusCode::CONFLICT,
    "not_stoppable",
    "the Job has no execution that this process can stop",
);

/// A validated submission: an existing LLM Profile and Executor, by their
/// persisted identities, an objective and, optionally, a Job name. Nothing
/// else can be submitted: the Provider, its credential and the model come
/// from the Profile, and no tool is offered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct JobRequest {
    llm_profile: ProfileId,
    executor: ExecutorId,
    objective: String,
    name: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRequest {
    llm_profile: String,
    executor: String,
    objective: String,
    #[serde(default)]
    name: Option<String>,
}

/// Text that may be stored: no control character but tab, carriage return
/// and line feed, and not only white space.
fn text(value: &str, max_bytes: usize) -> bool {
    !value.trim().is_empty()
        && value.len() <= max_bytes
        && !value
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\t' | '\n' | '\r'))
}

fn identity(text: &str, prefix: &str) -> Option<u64> {
    text.strip_prefix(prefix).and_then(decimal::<u64>)
}

impl JobRequest {
    /// Parses a submission body strictly: one JSON object of the known
    /// fields, none repeated, each within its bounds.
    pub(super) fn parse(body: &[u8]) -> Result<Self, Refusal> {
        let wire: WireRequest = serde_json::from_slice(body).map_err(|_| INVALID_BODY)?;
        let llm_profile = identity(&wire.llm_profile, "profile:").ok_or(INVALID_BODY)?;
        let executor = identity(&wire.executor, "executor:").ok_or(INVALID_BODY)?;
        if !text(&wire.objective, MAX_OBJECTIVE_BYTES)
            || wire
                .name
                .as_deref()
                .is_some_and(|name| !text(name, MAX_NAME_BYTES))
        {
            return Err(INVALID_BODY);
        }
        Ok(Self {
            llm_profile: ProfileId::new(llm_profile),
            executor: ExecutorId::new(executor),
            objective: wire.objective,
            name: wire.name,
        })
    }

    /// The digest of the request as parsed, whatever its JSON spelling.
    fn fingerprint(&self) -> RequestFingerprint {
        let mut context = Context::new(&SHA256);
        context.update(REQUEST_DOMAIN);
        context.update(&self.llm_profile.get().to_le_bytes());
        context.update(&self.executor.get().to_le_bytes());
        let mut field = |value: &str| {
            context.update(&(value.len() as u64).to_le_bytes());
            context.update(value.as_bytes());
        };
        field(&self.objective);
        match &self.name {
            None => context.update(&[0]),
            Some(name) => {
                context.update(&[1]);
                context.update(&(name.len() as u64).to_le_bytes());
                context.update(name.as_bytes());
            }
        }
        RequestFingerprint::from_digest(sha256(context))
    }
}

fn sha256(context: Context) -> [u8; 32] {
    context
        .finish()
        .as_ref()
        .try_into()
        .expect("SHA-256 has 32 bytes")
}

/// The request's one `Idempotency-Key`, checked, as it is recorded.
pub(super) fn idempotency_key(headers: &HeaderMap) -> Result<SubmissionKey, Refusal> {
    let mut values = headers.get_all(IDEMPOTENCY_KEY);
    let value = values.next().ok_or(KEY_REQUIRED)?;
    let key = value.as_bytes();
    if values.next().is_some()
        || !(MIN_KEY_CHARS..=MAX_KEY_CHARS).contains(&key.len())
        || !key
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-._~:+/=".contains(byte))
    {
        return Err(INVALID_KEY);
    }
    let mut context = Context::new(&SHA256);
    context.update(KEY_DOMAIN);
    context.update(key);
    Ok(SubmissionKey::from_digest(sha256(context)))
}

/// How one admission ended, for every request waiting on it.
#[derive(Clone, Copy, Debug)]
enum Outcome {
    /// The key names this Job; `created` when this admission created it.
    Admitted { job: JobId, created: bool },
    /// The key names a Job of another request.
    Conflict,
    /// Nothing was admitted, for this reason; the key is free again.
    Refused(Refusal),
    /// The Job may exist; the key stays unresolved.
    Uncertain,
}

/// A key this process has not resolved.
struct Unresolved {
    fingerprint: RequestFingerprint,
    /// Tells this entry apart from a later one of the same key.
    generation: u64,
    phase: Phase,
}

enum Phase {
    /// Its admission runs; these wait for its outcome.
    Admitting(Vec<flume::Sender<Outcome>>),
    /// Its creation may have committed in the opening of `epoch` or before.
    Uncertain { epoch: u64 },
}

#[derive(Default)]
struct State {
    unresolved: HashMap<SubmissionKey, Unresolved>,
    /// The stop of each Job admitted here whose execution has not returned.
    running: HashMap<JobId, JobStop>,
    next_generation: u64,
}

/// What this listener knows that the database does not, see the module
/// documentation. One lock covers it, never held across an await.
#[derive(Default)]
pub(super) struct Registry {
    state: Mutex<State>,
}

/// What a request finds for its key.
enum Claim {
    /// It owns the key's admission and waits for it like any other.
    Owner {
        generation: u64,
        outcome: flume::Receiver<Outcome>,
    },
    /// Another request's admission runs; it waits for that one.
    Waiting(flume::Receiver<Outcome>),
    Conflict,
    Uncertain {
        generation: u64,
        epoch: u64,
    },
    TooMany,
}

/// What this process knows of a key, for a lookup.
enum Local {
    Admitting,
    Uncertain { generation: u64, epoch: u64 },
}

impl Registry {
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn claim(&self, key: SubmissionKey, fingerprint: RequestFingerprint) -> Claim {
        let mut state = self.state();
        if let Some(entry) = state.unresolved.get_mut(&key) {
            if entry.fingerprint != fingerprint {
                return Claim::Conflict;
            }
            return match &mut entry.phase {
                Phase::Admitting(waiters) => {
                    // Requests that stopped waiting are forgotten, so a
                    // client retrying a long admission adds no garbage.
                    waiters.retain(|waiter| !waiter.is_disconnected());
                    let (sender, outcome) = flume::bounded(1);
                    waiters.push(sender);
                    Claim::Waiting(outcome)
                }
                Phase::Uncertain { epoch } => Claim::Uncertain {
                    generation: entry.generation,
                    epoch: *epoch,
                },
            };
        }
        if state.unresolved.len() >= MAX_UNRESOLVED {
            return Claim::TooMany;
        }
        state.next_generation += 1;
        let generation = state.next_generation;
        let (sender, outcome) = flume::bounded(1);
        state.unresolved.insert(
            key,
            Unresolved {
                fingerprint,
                generation,
                phase: Phase::Admitting(vec![sender]),
            },
        );
        Claim::Owner {
            generation,
            outcome,
        }
    }

    fn local(&self, key: SubmissionKey) -> Option<Local> {
        self.state()
            .unresolved
            .get(&key)
            .map(|entry| match entry.phase {
                Phase::Admitting(_) => Local::Admitting,
                Phase::Uncertain { epoch } => Local::Uncertain {
                    generation: entry.generation,
                    epoch,
                },
            })
    }

    /// Ends the admission of `generation`: the key is resolved, by the
    /// database or as refused, and every waiting request is told how.
    fn finish(&self, key: SubmissionKey, generation: u64, outcome: Outcome) {
        let mut state = self.state();
        if state
            .unresolved
            .get(&key)
            .is_some_and(|entry| entry.generation == generation)
            && let Some(entry) = state.unresolved.remove(&key)
            && let Phase::Admitting(waiters) = entry.phase
        {
            for waiter in waiters {
                waiter.try_send(outcome).ok();
            }
        }
    }

    /// Keeps the key of `generation` unresolved: its creation may have
    /// committed in the opening of `epoch` or before.
    fn uncertain(&self, key: SubmissionKey, generation: u64, epoch: u64) {
        let mut state = self.state();
        if let Some(entry) = state.unresolved.get_mut(&key)
            && entry.generation == generation
        {
            let phase = std::mem::replace(&mut entry.phase, Phase::Uncertain { epoch });
            if let Phase::Admitting(waiters) = phase {
                for waiter in waiters {
                    waiter.try_send(Outcome::Uncertain).ok();
                }
            }
        }
    }

    /// Forgets the uncertain key of `generation`, which the database has
    /// resolved, unless another entry replaced it meanwhile.
    fn forget(&self, key: SubmissionKey, generation: u64) {
        let mut state = self.state();
        if state.unresolved.get(&key).is_some_and(|entry| {
            entry.generation == generation && matches!(entry.phase, Phase::Uncertain { .. })
        }) {
            state.unresolved.remove(&key);
        }
    }

    fn run(&self, stop: JobStop) {
        self.state().running.insert(stop.job(), stop);
    }

    fn returned(&self, job: JobId) {
        self.state().running.remove(&job);
    }

    fn stop_of(&self, job: JobId) -> Option<JobStop> {
        self.state()
            .running
            .get(&job)
            .filter(|stop| stop.job() == job)
            .cloned()
    }

    #[cfg(test)]
    pub(super) fn running(&self) -> usize {
        self.state().running.len()
    }

    #[cfg(test)]
    pub(super) fn unresolved(&self) -> usize {
        self.state().unresolved.len()
    }
}

fn admission(status: &str, job: Option<JobId>) -> Value {
    json!({
        "api_version": API_VERSION,
        "admission": {
            "status": status,
            "job": job.map(|job| job.to_string()),
        },
    })
}

/// Submits `request` once under `key`, see the module documentation, and
/// waits at most the configured time for its admission: an admitted Job is
/// `201 Created` when this request created it and `200 OK` when the key
/// already named it; an admission still running is `202 Accepted`, pending,
/// and runs on without the request.
pub(super) async fn submit(
    shared: &Arc<Shared>,
    key: SubmissionKey,
    request: JobRequest,
) -> Result<Reply, Refusal> {
    let fingerprint = request.fingerprint();
    // At most one uncertain entry is resolved before the key is claimed.
    for _ in 0..2 {
        let (outcome, owner) = match shared.submissions.claim(key, fingerprint) {
            Claim::Conflict => return Err(KEY_CONFLICT),
            Claim::TooMany => return Err(TOO_MANY_UNRESOLVED),
            Claim::Uncertain { generation, epoch } => {
                match reconcile(shared, key, generation, epoch).await {
                    Reconciled::Recorded(record) if record.fingerprint == fingerprint => {
                        return Ok(Reply::new(
                            StatusCode::OK,
                            admission("admitted", Some(record.job)),
                        ));
                    }
                    Reconciled::Recorded(_) => return Err(KEY_CONFLICT),
                    Reconciled::Absent => continue,
                    Reconciled::Unresolved => return Err(UNCERTAIN),
                }
            }
            Claim::Waiting(outcome) => (outcome, false),
            Claim::Owner {
                generation,
                outcome,
            } => {
                // Spawned before anything is awaited: the admission is the
                // listener's, whatever becomes of this request.
                ntex::rt::spawn(admit(shared.clone(), key, generation, fingerprint, request));
                (outcome, true)
            }
        };
        let waited = ntex::time::timeout(shared.admission_wait, outcome.recv_async()).await;
        return match waited {
            Ok(Ok(Outcome::Admitted { job, created })) => Ok(Reply::new(
                if created && owner {
                    StatusCode::CREATED
                } else {
                    StatusCode::OK
                },
                admission("admitted", Some(job)),
            )),
            Ok(Ok(Outcome::Conflict)) => Err(KEY_CONFLICT),
            Ok(Ok(Outcome::Refused(refusal))) => Err(refusal),
            // The admission's task was dropped with the listener, so how it
            // ended is not known here.
            Ok(Ok(Outcome::Uncertain) | Err(_)) => Err(UNCERTAIN),
            Err(()) => Ok(Reply::new(StatusCode::ACCEPTED, admission("pending", None))),
        };
    }
    Err(UNCERTAIN)
}

/// Answers whether `key` created a Job: `admitted`, with the Job; `pending`
/// while this process admits it; `uncertain` while its creation may have
/// committed; `not_found` when no Job was created with it and none can be
/// any more by an earlier attempt. A database that cannot be read is an
/// error, never `not_found`.
pub(super) async fn lookup(shared: &Shared, key: SubmissionKey) -> Result<Reply, Refusal> {
    let found = match shared.submissions.local(key) {
        Some(Local::Admitting) => return Ok(Reply::ok(admission("pending", None))),
        Some(Local::Uncertain { generation, epoch }) => {
            match reconcile(shared, key, generation, epoch).await {
                Reconciled::Recorded(record) => Some(record),
                Reconciled::Absent => None,
                Reconciled::Unresolved => return Ok(Reply::ok(admission("uncertain", None))),
            }
        }
        None => shared.database.submission(key).await?,
    };
    Ok(Reply::ok(match found {
        Some(record) => admission("admitted", Some(record.job)),
        None => admission("not_found", None),
    }))
}

enum Reconciled {
    /// The key created this Job.
    Recorded(SubmissionRecord),
    /// The key created no Job, and no earlier attempt can create one now.
    Absent,
    Unresolved,
}

/// Resolves an uncertain key from the database. A missing record settles
/// it only when the database was reopened, in a later epoch, before the
/// read began: a creation of the earlier opening can no longer commit.
async fn reconcile(shared: &Shared, key: SubmissionKey, generation: u64, epoch: u64) -> Reconciled {
    let reading_in = shared.database.epoch();
    match shared.database.submission(key).await {
        Ok(Some(record)) => {
            shared.submissions.forget(key, generation);
            Reconciled::Recorded(record)
        }
        Ok(None) if reading_in > epoch => {
            shared.submissions.forget(key, generation);
            Reconciled::Absent
        }
        Ok(None) | Err(_) => Reconciled::Unresolved,
    }
}

/// The admission of one key, owned by the listener's runtime: checks the
/// record, then the configuration, then admits the Job with its submission.
/// Once admitted, the Job's stop is registered before anyone is told, and
/// released once its execution returns.
async fn admit(
    shared: Arc<Shared>,
    key: SubmissionKey,
    generation: u64,
    fingerprint: RequestFingerprint,
    request: JobRequest,
) {
    let registry = &shared.submissions;
    let finish = |outcome| registry.finish(key, generation, outcome);
    match shared.database.submission(key).await {
        Ok(Some(record)) => {
            return finish(if record.fingerprint == fingerprint {
                Outcome::Admitted {
                    job: record.job,
                    created: false,
                }
            } else {
                Outcome::Conflict
            });
        }
        Ok(None) => {}
        Err(error) => return finish(Outcome::Refused(error.into())),
    }
    if let Err(refusal) = check_configuration(&shared.database, &request).await {
        return finish(Outcome::Refused(refusal));
    }
    let plan = plan(&shared.database, request);
    let admitted = shared
        .scheduler
        .admit_submitted_provider(plan, NewSubmission { key, fingerprint })
        .await;
    match admitted {
        Ok(admitted) => {
            let job = admitted.job();
            registry.run(admitted.stop_handle());
            log::info!("control plane admitted {job}");
            finish(Outcome::Admitted { job, created: true });
            // The record is the settlement's authority; this only releases
            // the stop once nothing is left to stop.
            admitted.settled().await.ok();
            registry.returned(job);
        }
        Err(AdmissionError::NotAdmitted(ExecutionRuntimeError::Database(
            DatabaseError::Rejected(Violation::SubmissionKeyUsed { job, same_request }),
        ))) => finish(if same_request {
            Outcome::Admitted {
                job,
                created: false,
            }
        } else {
            Outcome::Conflict
        }),
        Err(AdmissionError::NotAdmitted(error)) => finish(Outcome::Refused(not_admitted(&error))),
        Err(AdmissionError::Uncertain(error)) => {
            // The creation's own epoch when it is known, else this one: an
            // attempt of this admission was issued in it or before.
            let epoch = match &error {
                ExecutionRuntimeError::Database(DatabaseError::OutcomeUnknown {
                    operation,
                    ..
                }) => operation.epoch(),
                _ => shared.database.epoch(),
            };
            log::warn!("whether a control plane submission created its Job is unknown: {error}");
            registry.uncertain(key, generation, epoch);
        }
    }
}

/// Refuses a Profile that is not an existing LLM Profile, and an Executor
/// that does not exist or cannot run it now. The creation checks both again
/// in its own transaction.
async fn check_configuration(
    database: &DatabaseHandle,
    request: &JobRequest,
) -> Result<(), Refusal> {
    match database.llm_profile(request.llm_profile).await {
        Ok(Some(_)) => {}
        Ok(None) | Err(DatabaseError::WrongProfileKind(_)) => return Err(PROFILE_NOT_FOUND),
        Err(error) => return Err(error.into()),
    }
    if database.executor(request.executor).await?.is_none() {
        return Err(EXECUTOR_NOT_FOUND);
    }
    if !database
        .compatible_executors(request.llm_profile)
        .await?
        .contains(&request.executor)
    {
        return Err(EXECUTOR_INCOMPATIBLE);
    }
    Ok(())
}

/// The no-tool Provider plan of a checked request, a child of the Genesis
/// Job, with the credential resolved as the Profile's Provider names it.
fn plan(database: &DatabaseHandle, request: JobRequest) -> ProviderExecutionPlan {
    ProviderExecutionPlan::environment_credentials(
        request.executor,
        JobInputDraft {
            objective: Some(request.objective),
            ..JobInputDraft::default()
        },
        NewJob {
            name: request.name,
            execution_mode: ExecutionMode::Once,
            eligible_at: None,
            scheduled_at: None,
            next_due_at: None,
            next_iteration_no: None,
            iteration_no: None,
            parent: database.genesis(),
            projects: Vec::new(),
            dependencies: Vec::new(),
            configuration: Some(ExecutionConfiguration {
                agent_profile: None,
                llm_profile: request.llm_profile,
            }),
            assigned_executor: None,
            previous: None,
            replaces: None,
        },
    )
}

/// Why a plan known not to have created its Job was refused, without what
/// failed inside.
fn not_admitted(error: &ExecutionRuntimeError) -> Refusal {
    match error {
        ExecutionRuntimeError::SchedulerQueueFull | ExecutionRuntimeError::ExecutorQueueFull => {
            RUNTIME_BUSY
        }
        ExecutionRuntimeError::ShuttingDown
        | ExecutionRuntimeError::SchedulerStopped
        | ExecutionRuntimeError::ExecutorStopped => SHUTTING_DOWN,
        ExecutionRuntimeError::ExecutorNotFound(_) => EXECUTOR_NOT_FOUND,
        ExecutionRuntimeError::Database(error) => match error {
            DatabaseError::Rejected(_) => CONFIGURATION_REFUSED,
            DatabaseError::ShuttingDown => SHUTTING_DOWN,
            DatabaseError::QueueFull | DatabaseError::ReadQueueFull => DATABASE_BUSY,
            DatabaseError::DiskProtected(_)
            | DatabaseError::RefusedByDiskProtection { .. }
            | DatabaseError::InsufficientSpace { .. }
            | DatabaseError::RolledBack(_)
            | DatabaseError::NeverExecuted { .. } => STORAGE_REFUSED,
            DatabaseError::ReadsClosed
            | DatabaseError::ReadsStorageFailed(_)
            | DatabaseError::ReadInterrupted
            | DatabaseError::ReadPoolStopped
            | DatabaseError::Read(_) => DATABASE_UNAVAILABLE,
            _ => INTERNAL,
        },
        _ => INTERNAL,
    }
}

fn settled(record: &JobRecord) -> bool {
    record.completed_at.is_some()
        || record.failed_at.is_some()
        || record.cancelled_at.is_some()
        || record.unknown_at.is_some()
}

fn stop_reply(status: StatusCode, job: JobId, result: &str) -> Reply {
    Reply::new(
        status,
        json!({
            "api_version": API_VERSION,
            "stop": {"job": job.to_string(), "status": result},
        }),
    )
}

/// Requests the stop of `job`'s execution, if this listener admitted it
/// and it has not returned: `202 Accepted`, `requested`, says the request
/// reached that execution, never how the Job settles. A Job whose outcome is
/// recorded is `already_settled`; any other has no execution here to stop.
pub(super) async fn stop(shared: &Shared, job: JobId) -> Result<Reply, Refusal> {
    let record = shared.database.job(job).await?.ok_or(JOB_NOT_FOUND)?;
    if settled(&record) {
        return Ok(stop_reply(StatusCode::OK, job, "already_settled"));
    }
    if let Some(stop) = shared.submissions.stop_of(job) {
        stop.request();
        log::info!("control plane requested the stop of {job}");
        return Ok(stop_reply(StatusCode::ACCEPTED, job, "requested"));
    }
    // Its execution may have returned since the first read.
    let record = shared.database.job(job).await?.ok_or(JOB_NOT_FOUND)?;
    if settled(&record) {
        Ok(stop_reply(StatusCode::OK, job, "already_settled"))
    } else {
        Err(NOT_STOPPABLE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(values: &[&str]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for value in values {
            headers.append(
                ntex::http::header::HeaderName::from_static(IDEMPOTENCY_KEY),
                ntex::http::header::HeaderValue::from_str(value).expect("a header value"),
            );
        }
        headers
    }

    #[test]
    fn keys_are_checked_and_recorded_only_as_digests() {
        let key = "client-0123456789abcdef";
        let digest = idempotency_key(&headers(&[key])).expect("a valid key");
        assert_eq!(digest, idempotency_key(&headers(&[key])).expect("again"));
        assert_ne!(
            digest,
            idempotency_key(&headers(&["client-0123456789abcdeF"])).expect("another")
        );
        assert!(!format!("{digest:?}").contains(key));
        assert_eq!(idempotency_key(&headers(&[])).err(), Some(KEY_REQUIRED));
        let long = "k".repeat(MAX_KEY_CHARS + 1);
        for refused in [
            &["short-key"][..],
            &["client 0123456789abcdef"],
            &["client-0123456789abcdé"],
            &[long.as_str()],
            &[key, key],
        ] {
            assert_eq!(
                idempotency_key(&headers(refused)).err(),
                Some(INVALID_KEY),
                "{refused:?}"
            );
        }
    }

    #[test]
    fn requests_are_parsed_strictly_and_fingerprinted_as_parsed() {
        let request = JobRequest::parse(
            br#"{"llm_profile":"profile:3","executor":"executor:2","objective":"Say hi"}"#,
        )
        .expect("valid");
        let respelled = JobRequest::parse(
            br#" { "objective" : "Say hi", "executor":"executor:2", "name": null, "llm_profile":"profile:3" } "#,
        )
        .expect("valid");
        assert_eq!(request, respelled);
        assert_eq!(request.fingerprint(), respelled.fingerprint());
        for other in [
            br#"{"llm_profile":"profile:3","executor":"executor:2","objective":"Say hi!"}"#
                .as_slice(),
            br#"{"llm_profile":"profile:4","executor":"executor:2","objective":"Say hi"}"#,
            br#"{"llm_profile":"profile:3","executor":"executor:3","objective":"Say hi"}"#,
            br#"{"llm_profile":"profile:3","executor":"executor:2","objective":"Say hi","name":"n"}"#,
        ] {
            let other = JobRequest::parse(other).expect("valid");
            assert_ne!(request.fingerprint(), other.fingerprint(), "{other:?}");
        }
        let long_objective = format!(
            r#"{{"llm_profile":"profile:3","executor":"executor:2","objective":"{}"}}"#,
            "a".repeat(MAX_OBJECTIVE_BYTES + 1)
        );
        let long_name = format!(
            r#"{{"llm_profile":"profile:3","executor":"executor:2","objective":"o","name":"{}"}}"#,
            "n".repeat(MAX_NAME_BYTES + 1)
        );
        for refused in [
            b"".as_slice(),
            b"[]",
            b"null",
            br#"{"llm_profile":"profile:3","executor":"executor:2"}"#,
            br#"{"llm_profile":"profile:3","executor":"executor:2","objective":"   "}"#,
            br#"{"llm_profile":"profile:3","executor":"executor:2","objective":"a\u0000b"}"#,
            br#"{"llm_profile":"3","executor":"executor:2","objective":"o"}"#,
            br#"{"llm_profile":"profile:03","executor":"executor:2","objective":"o"}"#,
            br#"{"llm_profile":"profile:3","executor":"job:2","objective":"o"}"#,
            br#"{"llm_profile":"profile:3","executor":"executor:2","objective":"o","tools":[]}"#,
            br#"{"llm_profile":"profile:3","executor":"executor:2","objective":"o","endpoint":"https://x"}"#,
            br#"{"llm_profile":"profile:3","executor":"executor:2","objective":"o","objective":"p"}"#,
            br#"{"llm_profile":"profile:3","executor":"executor:2","objective":"o","name":""}"#,
            br#"{"llm_profile":"profile:3","executor":"executor:2","objective":"o"} trailing"#,
            long_objective.as_bytes(),
            long_name.as_bytes(),
        ] {
            assert_eq!(
                JobRequest::parse(refused).err(),
                Some(INVALID_BODY),
                "{}",
                String::from_utf8_lossy(&refused[..refused.len().min(120)])
            );
        }
    }
}
