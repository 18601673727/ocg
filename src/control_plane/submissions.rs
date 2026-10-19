//! Job submission, admission lookup and stop for the Control Plane.
//!
//! A submission is idempotent under the client's `Idempotency-Key`. The key
//! is recorded, as a SHA-256 digest with the fingerprint of its normalized
//! request, by the transaction that creates and locally starts its Job; see
//! [`DatabaseHandle::create_submitted_job`]. That record is the only
//! authority on whether a key created a Job: a key names at most one Job,
//! and only a committed one, in this process and after any restart.
//!
//! What only this process knows is kept in a [`Registry`] owned by the
//! listener, and dies with it:
//!
//! - a key being resolved, from its first request until its record is
//!   read: it reserves the key without deciding anything, so every other
//!   request with the key, whatever its request, waits for that read and is
//!   answered from the record; a committed key is never displaced, even for
//!   a moment, by a request that has not been checked against it;
//! - a key being admitted, once its record was found missing, until its
//!   admission ends: every other request with the key waits for that one
//!   admission and is answered from it, so a key is never submitted twice
//!   at once;
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
use crate::attachments::AttachmentError;
use crate::database::{DatabaseError, DatabaseHandle};
use crate::openai_compatible::image::MAX_REQUEST_IMAGES;
use crate::orchestration::execution_runtime::{
    AdmissionError, AttachmentImageError, ExecutionRuntimeError, JobStop, ProviderExecutionPlan,
    attachment_images,
};
use crate::persistence::{
    AttachmentId, AttachmentOrigin, ConfigBody, ConfigRecord, ExecutionConfiguration, ExecutorId,
    JobId, JobInputDraft, JobRecord, ModelCapability, NewJob, NewSubmission, ProfileId,
    RequestFingerprint, SubmissionKey, SubmissionRecord, Violation,
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
const AGENT_PROFILE_NOT_FOUND: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "agent_profile_not_found",
    "no such Agent Profile; nothing was submitted",
);
const AGENT_TARGET_MISMATCH: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "llm_profile_not_targeted",
    "the Agent Profile is not configured to run with that LLM Profile; nothing was submitted",
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
const WORKSPACE_NOT_CONFIGURED: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "workspace_not_configured",
    "the selected Agent has Workspace access, but the host has no authorized Workspace; configure OCG_WORKSPACE_ROOT and restart the host; nothing was submitted",
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
const PENDING_LIMIT_REACHED: Refusal = Refusal::new(
    StatusCode::SERVICE_UNAVAILABLE,
    "pending_limit_reached",
    "OCG holds as many pending Jobs as it admits at once; nothing was admitted; retry after pending work drains",
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
const PREVIOUS_JOB_NOT_FOUND: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "previous_job_not_found",
    "no such previous Job; nothing was submitted",
);
const INVALID_PREVIOUS_JOB: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_previous_job",
    "the previous Job cannot be used as conversation context; nothing was submitted",
);
const PREVIOUS_JOB_NOT_COMPLETED: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "previous_job_not_completed",
    "the previous turn has not finished",
);
const CONVERSATION_TOO_LONG: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "conversation_too_long",
    "the conversation exceeds the maximum number of turns; nothing was submitted",
);
const VISION_NOT_SUPPORTED: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "vision_not_supported",
    "the selected LLM Profile does not declare the vision capability, but the conversation carries images; nothing was submitted",
);
const TOOLS_NOT_SUPPORTED: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "tools_not_supported",
    "the selected LLM Profile does not declare the tools capability, but the Agent has a Workspace tool grant; nothing was submitted",
);
const TOO_MANY_IMAGES: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "too_many_images",
    "a conversation may carry at most 8 images; nothing was submitted",
);
const IMAGES_TOO_LARGE: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "images_too_large",
    "the conversation's images exceed 16 MiB together, or one exceeds 4 MiB; nothing was submitted",
);
const ATTACHMENT_NOT_AN_IMAGE: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "attachment_not_an_image",
    "an Attachment is not a PNG, JPEG, GIF or WEBP image that can be sent; nothing was submitted",
);
const ATTACHMENT_NOT_FOUND: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "attachment_not_found",
    "no such uploaded image Attachment; nothing was submitted",
);
const ATTACHMENT_UNAVAILABLE: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "attachment_unavailable",
    "an Attachment's content is no longer available; nothing was submitted",
);

/// The most turns a Job-linked conversation may traverse.
pub(crate) const MAX_CONVERSATION_TURNS: usize = 64;

/// A validated submission: an existing LLM Profile and Executor, by their
/// persisted identities, an objective and, optionally, an Agent Profile
/// that targets the LLM Profile, a Job name, and a completed predecessor Job.
/// Nothing else can be submitted: the Provider, its credential and the model
/// come from the Profile; Workspace tools come from the frozen Agent grant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct JobRequest {
    agent_profile: Option<ProfileId>,
    llm_profile: ProfileId,
    executor: ExecutorId,
    objective: String,
    name: Option<String>,
    previous: Option<JobId>,
    /// Image Attachments sent with the objective, in order.
    attachments: Vec<AttachmentId>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRequest {
    #[serde(default)]
    agent_profile: Option<String>,
    llm_profile: String,
    executor: String,
    objective: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    previous: Option<String>,
    #[serde(default)]
    attachments: Vec<String>,
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
        let agent_profile = wire
            .agent_profile
            .as_deref()
            .map(|agent| identity(agent, "profile:").ok_or(INVALID_BODY))
            .transpose()?
            .map(ProfileId::new);
        let previous = wire
            .previous
            .as_deref()
            .map(|prev| identity(prev, "job:").ok_or(INVALID_BODY))
            .transpose()?
            .map(JobId::new);
        if wire.attachments.len() > MAX_REQUEST_IMAGES {
            return Err(TOO_MANY_IMAGES);
        }
        let mut attachments = Vec::with_capacity(wire.attachments.len());
        for attachment in &wire.attachments {
            let attachment =
                AttachmentId::new(identity(attachment, "attachment:").ok_or(INVALID_BODY)?);
            if attachments.contains(&attachment) {
                return Err(INVALID_BODY);
            }
            attachments.push(attachment);
        }
        if !text(&wire.objective, MAX_OBJECTIVE_BYTES)
            || wire
                .name
                .as_deref()
                .is_some_and(|name| !text(name, MAX_NAME_BYTES))
        {
            return Err(INVALID_BODY);
        }
        Ok(Self {
            agent_profile,
            llm_profile: ProfileId::new(llm_profile),
            executor: ExecutorId::new(executor),
            objective: wire.objective,
            name: wire.name,
            previous,
            attachments,
        })
    }

    /// The digest of the request as parsed, whatever its JSON spelling.
    pub(super) fn fingerprint(&self) -> RequestFingerprint {
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
        // Appended only when named, so the fingerprint of a request without
        // an Agent Profile is what it was before one could be named.
        if let Some(agent) = self.agent_profile {
            context.update(&[2]);
            context.update(&agent.get().to_le_bytes());
        }
        if let Some(previous) = self.previous {
            context.update(&[3]);
            context.update(&previous.get().to_le_bytes());
        }
        if !self.attachments.is_empty() {
            context.update(&[4]);
            context.update(&(self.attachments.len() as u64).to_le_bytes());
            for attachment in &self.attachments {
                context.update(&attachment.get().to_le_bytes());
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
    Ok(digest_key(key))
}

pub(super) fn digest_key(key: &[u8]) -> SubmissionKey {
    let mut context = Context::new(&SHA256);
    context.update(KEY_DOMAIN);
    context.update(key);
    SubmissionKey::from_digest(sha256(context))
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
    /// The request of the claim that owns it; authoritative only once the
    /// key is known not to be recorded, from [`Phase::Admitting`] on.
    fingerprint: RequestFingerprint,
    /// Tells this entry apart from a later one of the same key.
    generation: u64,
    phase: Phase,
}

/// A request waiting for how a key is resolved, answered by its own request.
struct Waiter {
    fingerprint: RequestFingerprint,
    outcome: flume::Sender<Outcome>,
}

enum Phase {
    /// Its record is being read: whether, and by which request, the key is
    /// bound is not known yet, so these wait for the read, whatever their
    /// request.
    Resolving(Vec<Waiter>),
    /// It is not recorded, and its owner's admission runs; these, all of
    /// the owner's request, wait for its outcome.
    Admitting(Vec<Waiter>),
    /// Its creation may have committed in the opening of `epoch` or before.
    Uncertain { epoch: u64 },
}

/// Adds a waiter of `fingerprint` to `waiters`, forgetting those that
/// stopped waiting, so a client retrying a long admission adds no garbage.
fn join(waiters: &mut Vec<Waiter>, fingerprint: RequestFingerprint) -> flume::Receiver<Outcome> {
    waiters.retain(|waiter| !waiter.outcome.is_disconnected());
    let (sender, outcome) = flume::bounded(1);
    waiters.push(Waiter {
        fingerprint,
        outcome: sender,
    });
    outcome
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
    /// It owns the key's resolution and admission, and waits for them like
    /// any other.
    Owner {
        generation: u64,
        outcome: flume::Receiver<Outcome>,
    },
    /// Another request resolves or admits the key; it waits for that one.
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
    Resolving,
    Admitting,
    Uncertain { generation: u64, epoch: u64 },
}

impl Registry {
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// What `fingerprint` finds for `key`. The first claim reserves the key
    /// in [`Phase::Resolving`], under this lock, before its record is read,
    /// so no other request of this process can admit the key until that
    /// read has ended.
    fn claim(&self, key: SubmissionKey, fingerprint: RequestFingerprint) -> Claim {
        let mut state = self.state();
        if let Some(entry) = state.unresolved.get_mut(&key) {
            return match &mut entry.phase {
                // Nothing is known yet that could refuse it.
                Phase::Resolving(waiters) => Claim::Waiting(join(waiters, fingerprint)),
                _ if entry.fingerprint != fingerprint => Claim::Conflict,
                Phase::Admitting(waiters) => Claim::Waiting(join(waiters, fingerprint)),
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
        let mut waiters = Vec::new();
        let outcome = join(&mut waiters, fingerprint);
        state.unresolved.insert(
            key,
            Unresolved {
                fingerprint,
                generation,
                phase: Phase::Resolving(waiters),
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
                Phase::Resolving(_) => Local::Resolving,
                Phase::Admitting(_) => Local::Admitting,
                Phase::Uncertain { epoch } => Local::Uncertain {
                    generation: entry.generation,
                    epoch,
                },
            })
    }

    /// Ends the resolution or admission of `generation`: the key is
    /// resolved, by the database or as refused, and every waiting request is
    /// told how, for its own request.
    fn finish(
        &self,
        key: SubmissionKey,
        generation: u64,
        outcome: impl Fn(RequestFingerprint) -> Outcome,
    ) {
        let mut state = self.state();
        if state
            .unresolved
            .get(&key)
            .is_some_and(|entry| entry.generation == generation)
            && let Some(entry) = state.unresolved.remove(&key)
            && let Phase::Resolving(waiters) | Phase::Admitting(waiters) = entry.phase
        {
            for waiter in waiters {
                waiter.outcome.try_send(outcome(waiter.fingerprint)).ok();
            }
        }
    }

    /// Moves the key of `generation` from [`Phase::Resolving`] to
    /// [`Phase::Admitting`] once its record was found missing: its owner's
    /// request now holds it, and every waiter of another request is refused.
    /// Returns whether it did.
    fn admitting(&self, key: SubmissionKey, generation: u64) -> bool {
        let mut state = self.state();
        let Some(entry) = state.unresolved.get_mut(&key) else {
            return false;
        };
        if entry.generation != generation {
            return false;
        }
        let Phase::Resolving(waiters) = &mut entry.phase else {
            return false;
        };
        let owner = entry.fingerprint;
        let (kept, refused): (Vec<_>, Vec<_>) = std::mem::take(waiters)
            .into_iter()
            .partition(|waiter| waiter.fingerprint == owner);
        for waiter in refused {
            waiter.outcome.try_send(Outcome::Conflict).ok();
        }
        entry.phase = Phase::Admitting(kept);
        true
    }

    /// Keeps the key of `generation` unresolved: its creation may have
    /// committed in the opening of `epoch` or before.
    fn uncertain(&self, key: SubmissionKey, generation: u64, epoch: u64) {
        let mut state = self.state();
        if let Some(entry) = state.unresolved.get_mut(&key)
            && entry.generation == generation
        {
            let phase = std::mem::replace(&mut entry.phase, Phase::Uncertain { epoch });
            if let Phase::Resolving(waiters) | Phase::Admitting(waiters) = phase {
                for waiter in waiters {
                    waiter.outcome.try_send(Outcome::Uncertain).ok();
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

    /// How many requests still wait on `key`, its owner's included.
    #[cfg(test)]
    fn waiting(&self, key: SubmissionKey) -> usize {
        self.state()
            .unresolved
            .get(&key)
            .map_or(0, |entry| match &entry.phase {
                Phase::Resolving(waiters) | Phase::Admitting(waiters) => waiters
                    .iter()
                    .filter(|waiter| !waiter.outcome.is_disconnected())
                    .count(),
                Phase::Uncertain { .. } => 0,
            })
    }
}

#[cfg(test)]
impl super::ControlPlane {
    /// How many requests wait on the key a client sends as `key`.
    pub(crate) fn waiting(&self, key: &str) -> usize {
        self.shared.submissions.waiting(digest_key(key.as_bytes()))
    }
}

/// A test hold of a key's resolution, once its record was read and before
/// anything is decided from it. Keyed by the client's key, so tests running
/// at once do not see each other's.
#[cfg(test)]
pub(crate) mod resolution_seam {
    use std::collections::BTreeMap;
    use std::sync::{Mutex, PoisonError};

    use crate::persistence::SubmissionKey;

    type Hold = (flume::Sender<()>, flume::Receiver<()>);

    static HOLDS: Mutex<BTreeMap<SubmissionKey, Hold>> = Mutex::new(BTreeMap::new());

    /// The digest the client's `key` is recorded under.
    pub(crate) fn key(key: &str) -> SubmissionKey {
        super::digest_key(key.as_bytes())
    }

    /// Holds the next resolution of `key` once its record was read: the
    /// first receiver is told when it holds, and it goes on once the second
    /// sender sends or is dropped. The listener's runtime is not blocked.
    pub(crate) fn hold(key: &str) -> (flume::Receiver<()>, flume::Sender<()>) {
        let (reached, held) = flume::bounded(1);
        let (release, released) = flume::bounded(1);
        HOLDS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(self::key(key), (reached, released));
        (held, release)
    }

    pub(super) async fn reach(key: SubmissionKey) {
        let hold = HOLDS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&key);
        if let Some((reached, released)) = hold {
            reached.send(()).ok();
            released.recv_async().await.ok();
        }
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
        // Its owner's read has not ended; the record answers for itself,
        // and its absence means the owner may still admit it.
        Some(Local::Resolving) => match shared.database.submission(key).await? {
            Some(record) => Some(record),
            None => return Ok(Reply::ok(admission("pending", None))),
        },
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

/// The resolution and admission of one key, owned by the listener's
/// runtime: reads the record, which answers every waiting request by its
/// own request; only when it is missing does the key become its owner's,
/// then the configuration is checked and the Job admitted with its
/// submission. Once admitted, the Job's stop is registered before anyone is
/// told, and released once its execution returns.
async fn admit(
    shared: Arc<Shared>,
    key: SubmissionKey,
    generation: u64,
    fingerprint: RequestFingerprint,
    request: JobRequest,
) {
    let registry = &shared.submissions;
    // Every waiter of an admission that ran is of the owner's request.
    let finish = |outcome: Outcome| registry.finish(key, generation, |_| outcome);
    let recorded = shared.database.submission(key).await;
    #[cfg(test)]
    resolution_seam::reach(key).await;
    match recorded {
        Ok(Some(record)) => {
            return registry.finish(key, generation, |request| {
                if request == record.fingerprint {
                    Outcome::Admitted {
                        job: record.job,
                        created: false,
                    }
                } else {
                    Outcome::Conflict
                }
            });
        }
        // Reserved since before the read, so no admission of this process
        // can have recorded the key since.
        Ok(None) => {
            if !registry.admitting(key, generation) {
                return;
            }
        }
        Err(error) => {
            let refusal = Refusal::from(error);
            return finish(Outcome::Refused(refusal));
        }
    }
    if let Err(refusal) = check_configuration(&shared.database, &request).await {
        return finish(Outcome::Refused(refusal));
    }
    let plan = plan(&shared.database, request);
    let admitted = shared
        .scheduler
        .admit_workspace_provider(
            plan,
            NewSubmission { key, fingerprint },
            shared.workspace_tools.clone(),
        )
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

/// Refuses a Profile that is not an existing LLM Profile, an Agent Profile
/// that does not exist or does not target it, and an Executor
/// that does not exist or cannot run it now. The creation checks both again
/// in its own transaction.
pub(super) async fn check_configuration(
    database: &DatabaseHandle,
    request: &JobRequest,
) -> Result<(), Refusal> {
    let llm = match database.config(request.llm_profile.into()).await? {
        Some(ConfigRecord {
            body: ConfigBody::Model(model),
            ..
        }) => model,
        _ => return Err(PROFILE_NOT_FOUND),
    };
    let mut chat = false;
    let mut tool_grant = false;
    if let Some(agent_profile) = request.agent_profile {
        match database.config(agent_profile.into()).await? {
            Some(ConfigRecord {
                body: ConfigBody::Agent(agent),
                ..
            }) => {
                if !agent.models.contains(&request.llm_profile.into()) {
                    return Err(AGENT_TARGET_MISMATCH);
                }
                chat = agent.behavior == crate::persistence::AgentBehavior::Chat;
                tool_grant = agent.tool_grant.is_some();
            }
            _ => return Err(AGENT_PROFILE_NOT_FOUND),
        }
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
    // Every image the conversation's one Provider request will carry: the
    // earlier turns' first, then this one's.
    let mut images: Vec<AttachmentId> = Vec::new();
    if let Some(previous) = request.previous {
        // A conversation continues with the Agent of its earlier turn, and
        // that Agent holds conversations.
        if !chat {
            return Err(INVALID_PREVIOUS_JOB);
        }
        let genesis = database.genesis();
        let mut visited = std::collections::BTreeSet::new();
        let mut cursor = Some(previous);
        let mut prior_turns = 0usize;
        while let Some(current) = cursor {
            prior_turns += 1;
            if prior_turns >= MAX_CONVERSATION_TURNS {
                return Err(CONVERSATION_TOO_LONG);
            }
            if current == genesis || !visited.insert(current) {
                return Err(INVALID_PREVIOUS_JOB);
            }
            let Some(record) = database.job(current).await? else {
                return Err(if current == previous {
                    PREVIOUS_JOB_NOT_FOUND
                } else {
                    INVALID_PREVIOUS_JOB
                });
            };
            if record.parent != genesis
                || database.job_configuration(current).await?.is_none()
                || record.agent_id.map(Into::into) != request.agent_profile
            {
                return Err(INVALID_PREVIOUS_JOB);
            }
            if !record.settled() {
                return Err(PREVIOUS_JOB_NOT_COMPLETED);
            }
            if record.completed_at.is_some()
                && let Some(input) = database.job_input(record.input).await?
            {
                images.extend(input.attachments);
            }
            cursor = record.previous;
        }
    }
    if tool_grant && !llm.declared_capabilities.contains(&ModelCapability::Tools) {
        return Err(TOOLS_NOT_SUPPORTED);
    }
    images.extend(request.attachments.iter().copied());
    if images.is_empty() {
        return Ok(());
    }
    if !llm.declared_capabilities.contains(&ModelCapability::Vision) {
        return Err(VISION_NOT_SUPPORTED);
    }
    for &attachment in &request.attachments {
        match database.attachment(attachment).await? {
            Some(record) if record.origin == AttachmentOrigin::UserProvided => {}
            _ => return Err(ATTACHMENT_NOT_FOUND),
        }
    }
    match attachment_images(database, &images).await {
        Ok(_) => Ok(()),
        Err(AttachmentImageError::TooMany) => Err(TOO_MANY_IMAGES),
        Err(AttachmentImageError::TooLarge) => Err(IMAGES_TOO_LARGE),
        Err(AttachmentImageError::NotAnImage { .. }) => Err(ATTACHMENT_NOT_AN_IMAGE),
        Err(AttachmentImageError::Attachment(error)) => Err(match error {
            AttachmentError::Database(error) => error.into(),
            AttachmentError::NotFound(_) => ATTACHMENT_NOT_FOUND,
            AttachmentError::CleanedUp { .. } | AttachmentError::Content { .. } => {
                ATTACHMENT_UNAVAILABLE
            }
            AttachmentError::NotStored(_) => INTERNAL,
        }),
    }
}

/// The Provider plan of a checked request, a child of the Genesis
/// Job, with the credential resolved as the Profile's Provider names it.
/// Its images are referenced by Attachment identity; their bytes stay in the
/// attachment store. Workspace tools are selected after T1 from the frozen
/// grant, so an Agent edit racing admission cannot change the Job's access.
pub(super) fn plan(database: &DatabaseHandle, request: JobRequest) -> ProviderExecutionPlan {
    let mut plan = ProviderExecutionPlan::environment_credentials(
        request.executor,
        JobInputDraft {
            objective: Some(request.objective),
            attachments: request.attachments,
            ..JobInputDraft::default()
        },
        NewJob {
            name: request.name,
            eligible_at: None,
            parent: database.genesis(),
            projects: Vec::new(),
            dependencies: Vec::new(),
            input: JobInputDraft::default(),
            configuration: Some(ExecutionConfiguration {
                agent_profile: request.agent_profile,
                llm_profile: request.llm_profile,
            }),
            previous: request.previous,
        },
    );
    plan.credential_resolver = database.credential_resolver();
    plan
}

/// Why a plan known not to have created its Job was refused, without what
/// failed inside.
fn not_admitted(error: &ExecutionRuntimeError) -> Refusal {
    match error {
        ExecutionRuntimeError::SchedulerQueueFull => RUNTIME_BUSY,
        ExecutionRuntimeError::ShuttingDown | ExecutionRuntimeError::SchedulerStopped => {
            SHUTTING_DOWN
        }
        ExecutionRuntimeError::ExecutorNotFound(_) => EXECUTOR_NOT_FOUND,
        ExecutionRuntimeError::Database(error) => match error {
            DatabaseError::Rejected(Violation::ToolsNotSupported) => TOOLS_NOT_SUPPORTED,
            DatabaseError::Rejected(Violation::WorkspaceNotConfigured) => WORKSPACE_NOT_CONFIGURED,
            DatabaseError::Rejected(Violation::PendingJobsLimit { .. }) => PENDING_LIMIT_REACHED,
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
            br#"{"agent_profile":"profile:2","llm_profile":"profile:3","executor":"executor:2","objective":"Say hi"}"#,
            br#"{"llm_profile":"profile:3","executor":"executor:2","objective":"Say hi","previous":"job:2"}"#,
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
            br#"{"agent_profile":"2","llm_profile":"profile:3","executor":"executor:2","objective":"o"}"#,
            br#"{"agent_profile":"profile:02","llm_profile":"profile:3","executor":"executor:2","objective":"o"}"#,
            br#"{"agent_profile":"executor:2","llm_profile":"profile:3","executor":"executor:2","objective":"o"}"#,
            br#"{"llm_profile":"profile:03","executor":"executor:2","objective":"o"}"#,
            br#"{"llm_profile":"profile:3","executor":"job:2","objective":"o"}"#,
            br#"{"llm_profile":"profile:3","executor":"executor:2","objective":"o","previous":"2"}"#,
            br#"{"llm_profile":"profile:3","executor":"executor:2","objective":"o","previous":"job:02"}"#,
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

    #[test]
    fn tools_capability_is_required_at_preflight_and_t1() {
        use crate::database::DatabaseWorker;
        use crate::persistence::{AgentBehavior, AgentConfig, NewConfig, NewExecutor};
        use crate::test_support::{data_directory, execution_configuration, wait};

        let (_root, directory) = data_directory("submissions", "tools-capability");
        let worker = DatabaseWorker::start(&directory).expect("start");
        let handle = worker.handle();
        let configuration = execution_configuration(&handle);
        let model_id = configuration.llm_profile.into();
        let model = wait(handle.config(model_id)).expect("read").expect("model");
        let ConfigBody::Model(mut body) = model.body else {
            panic!("model")
        };
        body.declared_capabilities = [ModelCapability::Chat, ModelCapability::Tools].into();
        wait(handle.update_config(
            model_id,
            NewConfig {
                name: model.name.clone(),
                body: ConfigBody::Model(body.clone()),
            },
        ))
        .expect("enable tools");
        let agent = wait(handle.create_config(NewConfig {
            name: "chat".into(),
            body: ConfigBody::Agent(AgentConfig {
                behavior: AgentBehavior::Chat,
                models: vec![model_id],
                tool_grant: Some("read".into()),
                ..Default::default()
            }),
        }))
        .expect("agent");
        let executor = wait(handle.create_executor(NewExecutor {
            name: "local".into(),
            concurrency_limit: std::num::NonZeroU32::MIN,
            llm_transports: [crate::persistence::LlmTransport::OpenAiChatCompletions].into(),
        }))
        .expect("executor");
        let request = JobRequest::parse(
            &serde_json::to_vec(&json!({
                "agent_profile": ProfileId::from(agent).to_string(),
                "llm_profile": configuration.llm_profile.to_string(),
                "executor": executor.to_string(), "objective": "read a file",
            }))
            .expect("json"),
        )
        .expect("request");
        wait(check_configuration(&handle, &request)).expect("tools supported");
        body.declared_capabilities = [ModelCapability::Chat].into();
        wait(handle.update_config(
            model_id,
            NewConfig {
                name: model.name,
                body: ConfigBody::Model(body),
            },
        ))
        .expect("remove tools after preflight");
        assert_eq!(
            wait(check_configuration(&handle, &request)),
            Err(TOOLS_NOT_SUPPORTED)
        );
        let mut plan = plan(&handle, request.clone());
        plan.job.input = plan.input;
        let key = digest_key(b"tools-capability-test");
        let before = wait(handle.jobs(None, 100)).expect("jobs").len();
        let error = wait(handle.create_workspace_submitted_job(
            plan.job,
            NewSubmission {
                key,
                fingerprint: request.fingerprint(),
            },
            true,
        ))
        .expect_err("T1 refuses the changed profile");
        assert!(matches!(
            error,
            DatabaseError::Rejected(Violation::ToolsNotSupported)
        ));
        assert_eq!(
            not_admitted(&ExecutionRuntimeError::Database(error)),
            TOOLS_NOT_SUPPORTED
        );
        assert!(wait(handle.submission(key)).expect("submission").is_none());
        assert_eq!(wait(handle.jobs(None, 100)).expect("jobs").len(), before);
        let mut without_grant = request;
        without_grant.agent_profile = None;
        wait(check_configuration(&handle, &without_grant)).expect("no grant needs no Tools");
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn multi_turn_chat_jobs_reconstruct_context_and_conversation_projection() {
        use std::collections::BTreeSet;
        use std::num::NonZeroU32;

        use crate::control_plane::{catalog, profiles};
        use crate::database::DatabaseWorker;
        use crate::orchestration::execution_runtime::{
            ExecutionRuntime,
            provider_seam::{self, Response},
        };
        use crate::persistence::{LlmTransport, NewExecutor};
        use crate::provider_execution::scripted::Step;
        use crate::test_support::{data_directory, wait};

        let (_root, directory) = data_directory("submissions", "multi-turn-chat");
        let worker = DatabaseWorker::start(&directory).expect("start database");
        let handle = worker.handle();
        let runtime = ExecutionRuntime::start(handle.clone()).expect("start runtime");
        let scheduler = runtime.scheduler();

        let executor = wait(handle.create_executor(NewExecutor {
            name: "local-llm".to_owned(),
            concurrency_limit: NonZeroU32::MIN,
            llm_transports: BTreeSet::from([LlmTransport::OpenAiChatCompletions]),
        }))
        .expect("register executor");

        let created_llm = wait(profiles::save_profile(
            &handle,
            None,
            serde_json::to_vec(&json!({
                "kind": "llm",
                "name": "Chat GPT",
                "endpoint": "https://multi-turn.invalid/v1",
                "credential_ref": "MULTI_TURN_KEY",
                "credential": "sk-test-secret",
                "model": "gpt-4o-mini",
                "declared_capabilities": ["chat"],
            }))
            .expect("json")
            .as_slice(),
        ))
        .expect("create llm profile");
        let llm_id: ProfileId = created_llm.body["profile_id"]
            .as_str()
            .and_then(|s| s.strip_prefix("profile:"))
            .and_then(|s| s.parse().ok())
            .map(ProfileId::new)
            .expect("llm profile id");

        let created_agent = wait(profiles::save_profile(
            &handle,
            None,
            serde_json::to_vec(&json!({
                "kind": "agent",
                "name": "Helpful Assistant",
                "instructions": "Be concise and remember context.",
                "llm_targets": [llm_id.to_string()],
            }))
            .expect("json")
            .as_slice(),
        ))
        .expect("create agent profile");
        let agent_id: ProfileId = created_agent.body["profile_id"]
            .as_str()
            .and_then(|s| s.strip_prefix("profile:"))
            .and_then(|s| s.parse().ok())
            .map(ProfileId::new)
            .expect("agent profile id");

        let seen_conversations = Arc::new(Mutex::new(Vec::new()));
        let recorded = seen_conversations.clone();
        let mut replies = vec![
            "data: {\"choices\":[{\"delta\":{\"content\":\"Reply 1: Remembered cobalt.\"}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"Reply 2: Your word is cobalt.\"}}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"Reply 3: tlaboc.\"}}]}\n\n",
        ]
        .into_iter();
        provider_seam::install("https://multi-turn.invalid/v1", move |conversation| {
            recorded
                .lock()
                .expect("lock")
                .push(serde_json::to_value(conversation).expect("json"));
            let chunk = replies.next().expect("reply chunk");
            Response {
                gate: None,
                steps: vec![
                    Step::Chunk(chunk),
                    Step::Chunk(
                        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                    ),
                    Step::Chunk(
                        "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5,\"total_tokens\":15}}\n\n",
                    ),
                    Step::Chunk("data: [DONE]\n\n"),
                ],
            }
        });

        let run_turn = |key: &str, objective: &str, previous: Option<JobId>| -> JobId {
            let body = json!({
                "agent_profile": agent_id.to_string(),
                "llm_profile": llm_id.to_string(),
                "executor": executor.to_string(),
                "objective": objective,
                "previous": previous.map(|id| id.to_string()),
            });
            let request = JobRequest::parse(&serde_json::to_vec(&body).expect("json"))
                .expect("parse request");
            wait(check_configuration(&handle, &request)).expect("valid configuration");
            let fingerprint = request.fingerprint();
            let sub_key = digest_key(key.as_bytes());
            let plan = plan(&handle, request);
            let admitted = wait(scheduler.admit_submitted_provider(
                plan,
                NewSubmission {
                    key: sub_key,
                    fingerprint,
                },
            ))
            .expect("admit provider job");
            let job = admitted.job();
            wait(admitted.settled()).expect("job settled");
            job
        };

        let job1 = run_turn("turn-1-00000000000001", "My secret word is cobalt.", None);
        let job2 = run_turn(
            "turn-2-00000000000002",
            "What is my secret word?",
            Some(job1),
        );
        let job3 = run_turn("turn-3-00000000000003", "Spell it backwards.", Some(job2));

        let sent = seen_conversations.lock().expect("lock").clone();
        assert_eq!(sent.len(), 3);
        assert_eq!(sent[0]["messages"].as_array().expect("msgs").len(), 2);
        assert_eq!(sent[1]["messages"].as_array().expect("msgs").len(), 4);
        assert_eq!(
            sent[1]["messages"][1]["content"],
            "My secret word is cobalt."
        );
        assert_eq!(
            sent[1]["messages"][2]["content"],
            "Reply 1: Remembered cobalt."
        );
        assert_eq!(sent[1]["messages"][3]["content"], "What is my secret word?");
        assert_eq!(sent[2]["messages"].as_array().expect("msgs").len(), 6);
        assert_eq!(
            sent[2]["messages"][4]["content"],
            "Reply 2: Your word is cobalt."
        );
        assert_eq!(sent[2]["messages"][5]["content"], "Spell it backwards.");

        let conversation = wait(catalog::job_conversation(&handle, job3)).expect("conversation");
        let turns = conversation.body["turns"].as_array().expect("turns");
        assert_eq!(turns.len(), 3);
        assert_eq!(turns[0]["job"]["id"], job1.to_string());
        assert_eq!(turns[0]["job"]["previous"], Value::Null);
        assert_eq!(turns[1]["job"]["id"], job2.to_string());
        assert_eq!(turns[1]["job"]["previous"], job1.to_string());
        assert_eq!(turns[2]["job"]["id"], job3.to_string());
        assert_eq!(turns[2]["job"]["previous"], job2.to_string());

        // Linking to Genesis or a non-existent Job is refused.
        let genesis_req = JobRequest::parse(
            serde_json::to_vec(&json!({
                "agent_profile": agent_id.to_string(),
                "llm_profile": llm_id.to_string(),
                "executor": executor.to_string(),
                "objective": "Bad link",
                "previous": handle.genesis().to_string(),
            }))
            .expect("json")
            .as_slice(),
        )
        .expect("parse");
        assert_eq!(
            wait(check_configuration(&handle, &genesis_req)).err(),
            Some(INVALID_PREVIOUS_JOB)
        );
        let missing_req = JobRequest::parse(
            serde_json::to_vec(&json!({
                "agent_profile": agent_id.to_string(),
                "llm_profile": llm_id.to_string(),
                "executor": executor.to_string(),
                "objective": "Missing link",
                "previous": "job:999999",
            }))
            .expect("json")
            .as_slice(),
        )
        .expect("parse");
        assert_eq!(
            wait(check_configuration(&handle, &missing_req)).err(),
            Some(PREVIOUS_JOB_NOT_FOUND)
        );

        wait(runtime.shutdown()).expect("shutdown runtime");
        worker.shutdown().expect("shutdown worker");
    }
}
