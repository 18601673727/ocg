//! Job submission, admission lookup and stop for the Control Plane.
//!
//! A submission is idempotent under the client's `Idempotency-Key`. The key
//! is recorded, as a SHA-256 digest with the fingerprint of its normalized
//! request, by the transaction that opens a conversation or appends an entry.
//! That record is the only authority on whether a key appended input: a key names one (turn, seq),
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
    API_VERSION, DATABASE_BUSY, DATABASE_UNAVAILABLE, INTERNAL, Refusal, Reply, SHUTTING_DOWN,
    Shared, decimal,
};
use crate::attachments::AttachmentError;
use crate::database::{DatabaseError, DatabaseHandle};
use crate::openai_compatible::image::MAX_REQUEST_IMAGES;
use crate::orchestration::execution_runtime::{
    AttachmentImageError, ExecutionRuntimeError, attachment_images,
};
use crate::persistence::{
    AttachmentId, AttachmentOrigin, ConfigBody, ConfigRecord, ExecutionConfiguration, ExecutorId,
    JobId, ModelCapability, NewSubmission, ProfileId, ProjectId, RequestFingerprint, SubmissionKey,
    SubmissionRecord, Violation,
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
const PROJECT_NOT_FOUND: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "project_not_found",
    "no such Project; nothing was submitted",
);
const PROJECT_REQUIRED: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "project_required",
    "the selected Agent has Workspace access, which needs a Project; nothing was submitted",
);
const PROJECT_MISMATCH: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "project_mismatch",
    "a conversation keeps the Project of its first turn; nothing was submitted",
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

/// A validated submission: an existing LLM Profile and Executor, by their
/// persisted identities, an objective and, optionally, an Agent Profile
/// that targets the LLM Profile, a Job name, a Project, and a completed
/// predecessor Job. Nothing else can be submitted: the Provider, its
/// credential and the model come from the Profile; Workspace tools come from
/// the frozen Agent grant and the Project.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct JobRequest {
    agent_profile: Option<ProfileId>,
    llm_profile: ProfileId,
    executor: ExecutorId,
    objective: String,
    name: Option<String>,
    conversation: Option<JobId>,
    /// The Project the Job works in. A follow-up that names none takes its
    /// previous turn's.
    project: Option<ProjectId>,
    /// Image Attachments sent with the objective, in order.
    attachments: Vec<AttachmentId>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRequest {
    #[serde(default)]
    agent_profile: Option<String>,
    #[serde(default)]
    llm_profile: Option<String>,
    executor: String,
    objective: String,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    conversation: Option<String>,
    #[serde(default)]
    project: Option<String>,
    #[serde(default)]
    attachments: Vec<String>,
}

/// Text that may be stored: no control character but tab, carriage return
/// and line feed, and not only white space.
pub(super) fn text(value: &str, max_bytes: usize) -> bool {
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
        let llm_profile = wire
            .llm_profile
            .as_deref()
            .map(|model| identity(model, "profile:").ok_or(INVALID_BODY))
            .transpose()?
            .unwrap_or(0);
        let executor = identity(&wire.executor, "executor:").ok_or(INVALID_BODY)?;
        let agent_profile = wire
            .agent_profile
            .as_deref()
            .map(|agent| identity(agent, "profile:").ok_or(INVALID_BODY))
            .transpose()?
            .map(ProfileId::new);
        let conversation = wire
            .conversation
            .as_deref()
            .map(|prev| identity(prev, "job:").ok_or(INVALID_BODY))
            .transpose()?
            .map(JobId::new);
        let project = wire
            .project
            .as_deref()
            .map(|project| identity(project, "project:").ok_or(INVALID_BODY))
            .transpose()?
            .map(ProjectId::new);
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
            conversation,
            project,
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
        if let Some(previous) = self.conversation {
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
        if let Some(project) = self.project {
            context.update(&[5]);
            context.update(&project.get().to_le_bytes());
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
                            admitted_value(shared, key, record.job).await?,
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
                admitted_value(shared, key, job).await?,
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
        Some(record) => admitted_value(shared, key, record.job).await?,
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
    let mut request = request;
    if let Err(refusal) = normalize(&shared.database, &mut request).await {
        return finish(Outcome::Refused(refusal));
    }
    let project = match check_configuration(&shared.database, &request).await {
        Ok(project) => project,
        Err(refusal) => return finish(Outcome::Refused(refusal)),
    };
    let configuration = ExecutionConfiguration {
        agent_profile: request.agent_profile,
        llm_profile: request.llm_profile,
    };
    let first = crate::persistence::Entry::UserMessage {
        text: request.objective,
        attachments: request.attachments,
    };
    let submission = Some(NewSubmission { key, fingerprint });
    #[cfg(test)]
    crate::orchestration::execution_runtime::provider_seam::before_creation(
        request.name.as_deref(),
    );
    let admitted = match request.conversation {
        Some(conversation) => shared
            .database
            .append(conversation, first, Some(configuration), submission)
            .await
            .map(|appended| appended.turn),
        None => shared
            .database
            .open_conversation(
                None,
                crate::persistence::ConversationSpec {
                    name: request.name.clone(),
                    agent: request.agent_profile.map(Into::into),
                    project,
                    origin: crate::persistence::Origin::User,
                    keep_open: true,
                },
                first,
                configuration,
                submission,
            )
            .await
            .map(|(_, appended)| appended.turn),
    };
    match admitted {
        Ok(job) => finish(Outcome::Admitted { job, created: true }),
        Err(DatabaseError::Rejected(Violation::SubmissionKeyUsed { job, same_request })) => {
            finish(if same_request {
                Outcome::Admitted {
                    job,
                    created: false,
                }
            } else {
                Outcome::Conflict
            })
        }
        Err(DatabaseError::OutcomeUnknown { operation, .. }) => {
            registry.uncertain(key, generation, operation.epoch())
        }
        Err(DatabaseError::Rejected(Violation::InvalidValue("vision_not_supported", _))) => {
            finish(Outcome::Refused(VISION_NOT_SUPPORTED))
        }
        Err(DatabaseError::Rejected(Violation::InvalidValue("too_many_images", _))) => {
            finish(Outcome::Refused(TOO_MANY_IMAGES))
        }
        Err(error) => finish(Outcome::Refused(not_admitted(
            &ExecutionRuntimeError::Database(error),
        ))),
    }
}

/// Refuses a Profile that is not an existing LLM Profile, an Agent Profile
/// that does not exist or does not target it, an Executor
/// that does not exist or cannot run it now, and a Project that does not
/// exist, is missing for an Agent with a grant, or is not the Project of the
/// previous turn. The creation checks them again in its own transaction.
/// Answers the Project the Job works in: the request's, else its previous
/// turn's.
pub(super) async fn check_configuration(
    database: &DatabaseHandle,
    request: &JobRequest,
) -> Result<Option<ProjectId>, Refusal> {
    let llm = match database.config(request.llm_profile.into()).await? {
        Some(ConfigRecord {
            body: ConfigBody::Model(model),
            ..
        }) => model,
        _ => return Err(PROFILE_NOT_FOUND),
    };
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
    let mut images: Vec<AttachmentId> = Vec::new();
    let mut capabilities = llm.declared_capabilities.clone();
    if let Some(conversation) = request.conversation
        && let Some(tip) = database
            .conversation_turns(conversation, None, 1)
            .await?
            .first()
        && tip.claim.is_none()
        && tip.outcome.is_none()
    {
        capabilities = database
            .turn_execution(tip.id)
            .await?
            .0
            .llm
            .config
            .declared_capabilities;
        for (_, entry) in database.turn_entries(tip.id).await? {
            if let crate::persistence::Entry::UserMessage { attachments, .. } = entry.entry {
                images.extend(attachments);
            }
        }
    }
    let project = request.project;
    match project {
        Some(project) => {
            if database.project(project).await?.is_none() {
                return Err(PROJECT_NOT_FOUND);
            }
        }
        None if tool_grant => return Err(PROJECT_REQUIRED),
        None => {}
    }
    if tool_grant && !llm.declared_capabilities.contains(&ModelCapability::Tools) {
        return Err(TOOLS_NOT_SUPPORTED);
    }
    images.extend(request.attachments.iter().copied());
    if images.is_empty() {
        return Ok(project);
    }
    if !capabilities.contains(&ModelCapability::Vision) {
        return Err(VISION_NOT_SUPPORTED);
    }
    for &attachment in &request.attachments {
        match database.attachment(attachment).await? {
            Some(record) if record.origin == AttachmentOrigin::UserProvided => {}
            _ => return Err(ATTACHMENT_NOT_FOUND),
        }
    }
    match attachment_images(database, &images).await {
        Ok(_) => Ok(project),
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
            DatabaseError::Rejected(Violation::ProjectRequired) => PROJECT_REQUIRED,
            DatabaseError::Rejected(Violation::ProjectMismatch) => PROJECT_MISMATCH,
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

fn conversation_refusal(code: &'static str) -> Refusal {
    Refusal::new(
        if matches!(code, "conversation_not_found" | "approval_not_found") {
            StatusCode::NOT_FOUND
        } else {
            StatusCode::UNPROCESSABLE_ENTITY
        },
        code,
        "conversation action refused",
    )
}

async fn normalize(database: &DatabaseHandle, request: &mut JobRequest) -> Result<(), Refusal> {
    if let Some(conversation) = request.conversation {
        if request.project.is_some() {
            return Err(conversation_refusal("project_fixed"));
        }
        let row = database
            .job(conversation)
            .await?
            .ok_or(conversation_refusal("conversation_not_found"))?;
        let crate::persistence::JobKind::Conversation {
            agent,
            project,
            origin,
            ..
        } = row.kind
        else {
            return Err(conversation_refusal("conversation_not_found"));
        };
        if matches!(origin, crate::persistence::Origin::System) {
            return Err(conversation_refusal("system_conversation"));
        }
        if row.outcome.is_some() {
            return Err(conversation_refusal("conversation_closed"));
        }
        if row.children_closed_at.is_some() {
            return Err(conversation_refusal("conversation_closing"));
        }
        if request.agent_profile.is_some() && request.agent_profile.map(Into::into) != agent {
            return Err(conversation_refusal("agent_mismatch"));
        }
        request.agent_profile = agent.map(Into::into);
        request.project = project;
        if request.llm_profile.get() == 0 {
            let tip = database
                .conversation_turns(conversation, None, 1)
                .await?
                .into_iter()
                .next()
                .ok_or(INTERNAL)?;
            request.llm_profile = database
                .job_configuration(tip.id)
                .await?
                .ok_or(INTERNAL)?
                .llm_profile;
        }
    } else if request.llm_profile.get() == 0 {
        return Err(INVALID_BODY);
    }
    Ok(())
}

pub(super) async fn action(
    shared: &Shared,
    conversation: JobId,
    action: &str,
    body: &[u8],
) -> Result<Reply, Refusal> {
    use crate::persistence::{Actor, StopCause};
    let value: Value = serde_json::from_slice(body).map_err(|_| INVALID_BODY)?;
    let object = value.as_object().ok_or(INVALID_BODY)?;
    if action == "retry" {
        if object.len() != 1 {
            return Err(INVALID_BODY);
        }
        let turn = super::routing::job_id(
            object
                .get("turn")
                .and_then(Value::as_str)
                .ok_or(INVALID_BODY)?,
        )?;
        let appended = shared
            .database
            .retry(conversation, turn, Actor::User)
            .await
            .map_err(action_error)?;
        return Ok(Reply::ok(
            json!({ "api_version": API_VERSION, "conversation": conversation.to_string(), "turn": appended.turn.to_string(), "seq": appended.seq, "joined": appended.joined }),
        ));
    }
    if !object.is_empty() {
        return Err(INVALID_BODY);
    }
    let result = match action {
        "stop" => shared.database.stop_turn(conversation, Actor::User).await,
        "close" => shared.database.close(conversation, Actor::User).await,
        "hold" => shared.database.hold(conversation, Actor::User).await,
        "unhold" => shared.database.unhold(conversation, Actor::User).await,
        _ => return Err(INVALID_BODY),
    }
    .map_err(action_error)?;
    for running in &result.running {
        shared.scheduler.stop(
            *running,
            if action == "close" {
                StopCause::Closed
            } else {
                StopCause::Stopped(result.record_seq.ok_or(INTERNAL)?)
            },
        );
    }
    if result.already {
        let settled = shared
            .database
            .job(conversation)
            .await?
            .ok_or(INTERNAL)?
            .outcome
            .is_some();
        return Ok(Reply::ok(json!({
            "api_version": API_VERSION,
            "conversation": conversation.to_string(),
            "status": if settled { "already_settled" } else { "already_closing" },
            "record_seq": result.record_seq,
        })));
    }
    let changed = action != "stop"
        || !result.running.is_empty()
        || if let Some(record) = result.record_seq {
            shared
                .database
                .conversation_turns(conversation, None, 1)
                .await?
                .first()
                .and_then(|turn| turn.outcome.as_ref())
                .is_some_and(|outcome| {
                    outcome.cause == Some(crate::persistence::OutcomeCause::Stopped { record })
                })
        } else {
            action != "stop"
        };
    Ok(Reply::new(
        if changed {
            StatusCode::ACCEPTED
        } else {
            StatusCode::OK
        },
        json!({ "api_version": API_VERSION, "conversation": conversation.to_string(), "status": if changed { "requested" } else { "already_settled" }, "record_seq": result.record_seq }),
    ))
}
fn action_error(error: DatabaseError) -> Refusal {
    match error {
        DatabaseError::Rejected(Violation::InvalidValue(code, _)) => conversation_refusal(code),
        DatabaseError::Rejected(Violation::NotFound(_)) => {
            conversation_refusal("conversation_not_found")
        }
        error => error.into(),
    }
}

async fn admitted_value(shared: &Shared, key: SubmissionKey, job: JobId) -> Result<Value, Refusal> {
    let record = shared.database.submission(key).await?.ok_or(INTERNAL)?;
    let turn = shared.database.job(job).await?.ok_or(INTERNAL)?;
    let frozen = shared
        .database
        .job_configuration(job)
        .await?
        .ok_or(INTERNAL)?;
    Ok(
        json!({ "api_version": API_VERSION, "admission": { "status": "admitted", "job": job.to_string(), "conversation": turn.conversation.map(|conversation| conversation.to_string()), "seq": record.seq, "joined": record.seq > 1, "model": frozen.frozen.llm.config.model, "kept": turn.cancelled_at.is_some() } }),
    )
}

#[cfg(test)]
pub(super) mod tests {
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

    pub(in crate::control_plane) fn store_fixture(
        handle: &DatabaseHandle,
        mut request: JobRequest,
        submission: NewSubmission,
    ) -> JobId {
        use crate::persistence::{ConversationSpec, Entry, Origin};
        crate::test_support::wait(normalize(handle, &mut request)).expect("normalize");
        let project = crate::test_support::wait(check_configuration(handle, &request))
            .expect("configuration");
        let configuration = ExecutionConfiguration {
            agent_profile: request.agent_profile,
            llm_profile: request.llm_profile,
        };
        let entry = Entry::UserMessage {
            text: request.objective,
            attachments: request.attachments,
        };
        match request.conversation {
            Some(conversation) => {
                crate::test_support::wait(handle.append(
                    conversation,
                    entry,
                    Some(configuration),
                    Some(submission),
                ))
                .expect("append")
                .turn
            }
            None => {
                crate::test_support::wait(handle.open_conversation(
                    None,
                    ConversationSpec {
                        name: request.name.clone(),
                        agent: request.agent_profile.map(Into::into),
                        project,
                        origin: Origin::User,
                        keep_open: true,
                    },
                    entry,
                    configuration,
                    Some(submission),
                ))
                .expect("open")
                .1
                .turn
            }
        }
    }
    pub(in crate::control_plane) fn await_fixture(handle: &DatabaseHandle, job: JobId) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !crate::test_support::wait(handle.job(job))
            .expect("read")
            .expect("job")
            .any_terminal()
        {
            assert!(std::time::Instant::now() < deadline, "turn did not finish");
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
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
            br#"{"llm_profile":"profile:3","executor":"executor:2","objective":"Say hi","conversation":"job:2"}"#,
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
            br#"{"llm_profile":"profile:3","executor":"executor:2","objective":"o","conversation":"2"}"#,
            br#"{"llm_profile":"profile:3","executor":"executor:2","objective":"o","conversation":"job:02"}"#,
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
                dispatch_to: Vec::new(),
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
        let project = wait(handle.create_project(crate::persistence::NewProject {
            name: "tools".into(),
            description: None,
            root: "/ocg-test/tools".into(),
            commands: Vec::new(),
        }))
        .expect("project");
        let request = JobRequest::parse(
            &serde_json::to_vec(&json!({
                "agent_profile": ProfileId::from(agent).to_string(),
                "llm_profile": configuration.llm_profile.to_string(),
                "executor": executor.to_string(), "objective": "read a file",
                "project": project.to_string(),
            }))
            .expect("json"),
        )
        .expect("request");
        assert_eq!(
            wait(check_configuration(&handle, &request)),
            Ok(Some(project)),
            "tools supported"
        );
        let mut elsewhere = request.clone();
        elsewhere.project = None;
        assert_eq!(
            wait(check_configuration(&handle, &elsewhere)),
            Err(PROJECT_REQUIRED)
        );
        elsewhere.project = Some(ProjectId::new(999_999));
        assert_eq!(
            wait(check_configuration(&handle, &elsewhere)),
            Err(PROJECT_NOT_FOUND)
        );
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
        let key = digest_key(b"tools-capability-test");
        let before = wait(handle.jobs(None, 100)).expect("jobs").len();
        let error = wait(handle.open_conversation(
            None,
            crate::persistence::ConversationSpec {
                name: request.name.clone(),
                agent: request.agent_profile.map(Into::into),
                project: Some(project),
                origin: crate::persistence::Origin::User,
                keep_open: true,
            },
            crate::persistence::Entry::UserMessage {
                text: request.objective.clone(),
                attachments: request.attachments.clone(),
            },
            ExecutionConfiguration {
                agent_profile: request.agent_profile,
                llm_profile: request.llm_profile,
            },
            Some(NewSubmission {
                key,
                fingerprint: request.fingerprint(),
            }),
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
        let runtime = ExecutionRuntime::start(
            handle.clone(),
            crate::orchestration::execution_runtime::ClaimScope::Stored,
        )
        .expect("start runtime");

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
                "conversation": previous.and_then(|id| wait(handle.job(id)).expect("read").expect("turn").conversation).map(|id| id.to_string()),
            });
            let request = JobRequest::parse(&serde_json::to_vec(&body).expect("json"))
                .expect("parse request");
            wait(check_configuration(&handle, &request)).expect("valid configuration");
            let fingerprint = request.fingerprint();
            let sub_key = digest_key(key.as_bytes());
            let job = store_fixture(
                &handle,
                request,
                NewSubmission {
                    key: sub_key,
                    fingerprint,
                },
            );
            await_fixture(&handle, job);
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

        let root = wait(handle.job(job3))
            .expect("read")
            .expect("turn")
            .conversation
            .expect("conversation");
        let conversation =
            wait(catalog::conversation_turns(&handle, root, None, 20)).expect("conversation");
        let mut turns = conversation.body["turns"]
            .as_array()
            .expect("turns")
            .clone();
        turns.reverse();
        assert_eq!(turns.len(), 3);
        assert_eq!(turns[0]["job"]["id"], job1.to_string());
        assert_eq!(turns[0]["job"]["previous"], Value::Null);
        assert_eq!(turns[1]["job"]["id"], job2.to_string());
        assert_eq!(turns[1]["job"]["previous"], job1.to_string());
        assert_eq!(turns[2]["job"]["id"], job3.to_string());
        assert_eq!(turns[2]["job"]["previous"], job2.to_string());

        wait(runtime.shutdown()).expect("shutdown runtime");
        worker.shutdown().expect("shutdown worker");
    }
}

pub(super) async fn decide(
    shared: &Shared,
    approval: crate::persistence::ToolCallRef,
    body: &[u8],
) -> Result<Reply, Refusal> {
    use crate::persistence::ApprovalDecision;
    let value: Value = serde_json::from_slice(body).map_err(|_| INVALID_BODY)?;
    let object = value
        .as_object()
        .filter(|object| object.len() == 1)
        .ok_or(INVALID_BODY)?;
    let (decision, name) = if let Some(answer) = object.get("answer") {
        (
            ApprovalDecision::Answered {
                text: answer.as_str().ok_or(INVALID_BODY)?.to_owned(),
            },
            "answered",
        )
    } else {
        match object.get("decision").and_then(Value::as_str) {
            Some("approve") => (ApprovalDecision::Approved, "approved"),
            Some("reject") => (ApprovalDecision::Rejected, "rejected"),
            _ => return Err(INVALID_BODY),
        }
    };
    let appended = shared
        .database
        .decide_approval(approval, decision)
        .await
        .map_err(action_error)?;
    Ok(Reply::ok(
        json!({"api_version":API_VERSION, "approval":[approval.job.get(),approval.request.get(),approval.index], "decision":name, "turn":appended.turn.to_string(), "seq":appended.seq, "joined":appended.joined}),
    ))
}

pub(super) async fn cards(shared: &Shared, conversation: JobId) -> Result<Reply, Refusal> {
    if !shared
        .database
        .job(conversation)
        .await?
        .is_some_and(|job| job.kind.is_conversation())
    {
        return Err(conversation_refusal("conversation_not_found"));
    }
    let cards = shared.database.conversation_cards(conversation).await?;
    Ok(Reply::ok(
        json!({"api_version":API_VERSION, "conversation":conversation.to_string(), "cards":cards}),
    ))
}

pub(super) async fn release_slot(
    database: &DatabaseHandle,
    job: JobId,
    body: &[u8],
) -> Result<Reply, Refusal> {
    use crate::orchestration::blocking::{self, JobProof};
    use crate::persistence::StopProof;
    let value: Value = serde_json::from_slice(body).map_err(|_| INVALID_BODY)?;
    let object = value.as_object().ok_or(INVALID_BODY)?;
    if object.keys().any(|key| key != "confirm")
        || object
            .get("confirm")
            .is_some_and(|v| v != &Value::Bool(true))
    {
        return Err(INVALID_BODY);
    }
    let record = database.job(job).await?.ok_or(super::JOB_NOT_FOUND)?;
    if record.unknown_at.is_none() {
        return Err(conversation_refusal("not_quarantined"));
    }
    let released = || Reply::ok(json!({"api_version":API_VERSION,"status":"already_released"}));
    if record.stop_confirmed_at.is_some() {
        return Ok(released());
    }
    let proof = ntex::rt::spawn_blocking(move || blocking::prove_job(job))
        .await
        .map_err(|_| INTERNAL)?;
    let by = match proof {
        JobProof::Proven => StopProof::Proof,
        JobProof::Live { group } => {
            let refusal = conversation_refusal("slot_live");
            return Ok(Reply::new(
                refusal.status,
                json!({"api_version":API_VERSION, "error":{"code":refusal.code,"message":format!("process group {group} is still live"),"group":group}}),
            ));
        }
        JobProof::Untracked if object.get("confirm") == Some(&Value::Bool(true)) => StopProof::User,
        JobProof::Untracked => return Err(conversation_refusal("confirm_required")),
    };
    match database.confirm_job_stopped(job, by).await {
        Ok(()) => Ok(Reply::ok(
            json!({"api_version":API_VERSION,"status":"released"}),
        )),
        Err(DatabaseError::Rejected(Violation::StopAlreadyConfirmed(_))) => Ok(released()),
        Err(error) => Err(action_error(error)),
    }
}

#[cfg(test)]
mod release_tests {
    use super::*;
    use crate::database::DatabaseWorker;
    use crate::persistence::*;
    use crate::test_support::{data_directory, wait};
    #[test]
    fn inherited_quarantine_requires_confirmation_and_records_user_proof() {
        let (_root, directory) = data_directory("approvals", "release-slot");
        let worker = DatabaseWorker::start(&directory).unwrap();
        let db = worker.handle();
        let configuration = crate::test_support::execution_configuration(&db);
        let pool = wait(db.create_executor(NewExecutor {
            name: "test".into(),
            concurrency_limit: std::num::NonZeroU32::new(1).unwrap(),
            llm_transports: std::collections::BTreeSet::from([LlmTransport::OpenAiChatCompletions]),
        }))
        .unwrap();
        let job = wait(db.create_claimed_fixture(
            NewJob {
                process_grant: None,
                name: None,
                eligible_at: None,
                parent: db.genesis(),
                project: None,
                input: Default::default(),
                configuration: Some(configuration),
                previous: None,
            },
            pool,
        ))
        .unwrap();
        wait(db.record_metrics(NewMetrics {
            job,
            request: RequestNumber::FIRST,
            observed_at: Timestamp::now(),
            measurements: Default::default(),
            token: Some(crate::token_usage::TokenAnnotation::eligible(
                crate::token_usage::UsageFinality::Cumulative,
            )),
        }))
        .unwrap();
        wait(db.record_tool_decision(NewToolEvidence {
            call: ToolCallRef {
                job,
                request: RequestNumber::FIRST,
                index: 0,
            },
            approval: None,
            tool_call_id: "spawn".into(),
            name: "workspace_run_command".into(),
            spawns: true,
            decision: ToolDecision::Admitted(ToolAdmission {
                handler_version: "workspace_run_command-6".into(),
                effect: ToolEffect::NonIdempotent,
                parameters: "{}".into(),
                arguments: ToolPayload::new("{}"),
                authorization: AuthorizationBasis::RegistryGrant,
            }),
        }))
        .unwrap();
        worker.shutdown().unwrap();
        let worker = DatabaseWorker::start(&directory).unwrap();
        let db = worker.handle();
        crate::test_support::on_system(move || async move {
            assert!(
                matches!(release_slot(&db, job, b"{}").await, Err(refusal) if refusal.code == "confirm_required")
            );
            assert_eq!(
                release_slot(&db, job, br#"{"confirm":true}"#)
                    .await
                    .unwrap()
                    .body["status"],
                "released"
            );
            assert_eq!(
                db.job(job).await.unwrap().unwrap().stop_confirmed_by,
                Some(StopProof::User)
            );
            assert_eq!(
                release_slot(&db, job, b"{}").await.unwrap().body["status"],
                "already_released"
            );
        });
        worker.shutdown().unwrap();
    }
}
