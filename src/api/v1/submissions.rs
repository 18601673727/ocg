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

use super::contracts::*;
use aws_lc_rs::digest::{Context, SHA256};
use ntex::http::StatusCode;
use ntex::http::header::HeaderMap;
use serde::Deserialize;
use serde_json::Value;

use super::{DATABASE_BUSY, DATABASE_UNAVAILABLE, INTERNAL, Refusal, Reply, SHUTTING_DOWN, Shared};
use crate::db::attachments::AttachmentError;
use crate::db::attachments::{AttachmentImageError, attachment_images};
use crate::db::entities::ResponseFormat;
use crate::db::entities::{
    AttachmentId, AttachmentOrigin, ConfigBody, ConfigId, ConfigRecord, ExecutionConfiguration,
    ExecutorId, JobId, ModelCapability, ProfileId, ProjectId, RequestFingerprint, Submission,
    SubmissionKey, SubmissionRecord, Violation,
};
use crate::db::{DatabaseError, DatabaseHandle};
use crate::service::ai::providers::openai_compatible::ResponseFormatWire;
use crate::service::ai::providers::openai_compatible::image::MAX_REQUEST_IMAGES;
use crate::system::executor::execution_runtime::ExecutionRuntimeError;

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
const DECISION_REQUEST_DOMAIN: &[u8] = b"ocg.control-plane.decision-submission.v1\n";

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
const INVALID_ROUTE_BODY: Refusal = Refusal::new(
    StatusCode::BAD_REQUEST,
    "invalid_body",
    "the body is not a request this route accepts",
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
    "a turn may carry at most 8 images; nothing was submitted",
);
const IMAGES_TOO_LARGE: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "images_too_large",
    "the turn's images exceed 16 MiB together, or one exceeds 4 MiB; nothing was submitted",
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
    reasoning_effort: Option<crate::db::entities::ReasoningEffort>,
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
    response_format: Option<ResponseFormat>,
    keep_open: bool,
    steer: Option<bool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubmissionInput {
    reasoning_effort: Option<String>,
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
    #[serde(default)]
    response_format: Option<ResponseFormatWire>,
    #[serde(default)]
    keep_open: Option<bool>,
    #[serde(default)]
    mode: Option<String>,
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

fn identity(text: &str, _prefix: &str) -> Option<u128> {
    crate::db::entities::parse_entity_id(text)
}

impl JobRequest {
    /// Parses a submission body strictly: one JSON object of the known
    /// fields, none repeated, each within its bounds.
    pub(super) fn parse(body: &[u8]) -> Result<Self, Refusal> {
        let wire: SubmissionInput = serde_json::from_slice(body).map_err(|_| INVALID_BODY)?;
        let shape: Value = serde_json::from_slice(body).map_err(|_| INVALID_BODY)?;
        if shape
            .pointer("/response_format/json_schema/description")
            .is_some()
            || (wire.conversation.is_some() && shape.get("keep_open").is_some())
        {
            return Err(INVALID_BODY);
        }
        let llm_profile = wire
            .llm_profile
            .as_deref()
            .map(|model| identity(model, "profile:").ok_or(INVALID_BODY))
            .transpose()?
            .map(ProfileId::new)
            .unwrap_or_default();
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
        let response_format = wire
            .response_format
            .map(|format| {
                format.validate().map_err(|_| INVALID_BODY)?;
                match format {
                    ResponseFormatWire::JsonObject {} => Ok(ResponseFormat::JsonObject),
                    ResponseFormatWire::JsonSchema { json_schema } => {
                        let name = json_schema.name.ok_or(INVALID_BODY)?;
                        if name.is_empty()
                            || name.len() > 64
                            || !name
                                .bytes()
                                .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
                        {
                            return Err(INVALID_BODY);
                        }
                        Ok(ResponseFormat::JsonSchema {
                            name,
                            schema: json_schema.schema.ok_or(INVALID_BODY)?,
                            strict: json_schema.strict,
                        })
                    }
                    ResponseFormatWire::Text {} => Err(INVALID_BODY),
                }
            })
            .transpose()?;
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
        if (wire.objective.trim().is_empty() && attachments.is_empty())
            || wire.objective.len() > MAX_OBJECTIVE_BYTES
            || wire
                .objective
                .chars()
                .any(|character| character.is_control() && !matches!(character, '\t' | '\n' | '\r'))
            || wire
                .name
                .as_deref()
                .is_some_and(|name| !text(name, MAX_NAME_BYTES))
        {
            return Err(INVALID_BODY);
        }
        let steer = match wire.mode.as_deref() {
            None => None,
            Some("queue") => Some(false),
            Some("steer") if conversation.is_some() => Some(true),
            Some(_) => return Err(INVALID_BODY),
        };
        let reasoning_effort = wire
            .reasoning_effort
            .as_deref()
            .map(|value| crate::db::entities::ReasoningEffort::parse(value).ok_or(INVALID_BODY))
            .transpose()?;
        Ok(Self {
            reasoning_effort,
            steer,
            agent_profile,
            llm_profile,
            executor: ExecutorId::new(executor),
            objective: wire.objective,
            name: wire.name,
            conversation,
            project,
            attachments,
            response_format,
            keep_open: wire.keep_open.unwrap_or(true),
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
        if let Some(format) = &self.response_format {
            context.update(&[6]);
            context.update(&crate::db::redb::encode_response_format(format));
        }
        if !self.keep_open {
            context.update(&[7]);
        }
        match self.steer {
            Some(true) => context.update(&[8]),
            None if self.conversation.is_some() => context.update(&[9]),
            None | Some(false) => {}
        }
        if let Some(effort) = self.reasoning_effort {
            context.update(&[10]);
            context.update(effort.as_str().as_bytes());
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
#[derive(Clone, Debug)]
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
}

#[derive(serde::Serialize, ts_rs::TS)]
#[serde(tag = "status", rename_all = "snake_case")]
#[ts(rename = "Admission")]
pub(crate) enum AdmissionView {
    Admitted {
        job: String,
        conversation: Option<String>,
        seq: u32,
        joined: bool,
        model: String,
        kept: bool,
    },
    Pending {
        job: Option<String>,
    },
    Uncertain {
        job: Option<String>,
    },
    NotFound {
        job: Option<String>,
    },
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "AdmissionResponse")]
pub(crate) struct AdmissionResponseView {
    admission: AdmissionView,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Append")]
pub(crate) struct AppendView {
    conversation: String,
    turn: String,
    seq: u32,
    joined: bool,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Stop")]
pub(crate) struct StopView {
    conversation: String,
    status: String,
    record_seq: Option<u32>,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Compaction")]
pub(crate) struct CompactionView {
    conversation: String,
    status: String,
    job: Option<String>,
    system_conversation: Option<String>,
    through: Option<String>,
    subsequent: bool,
    reason: Option<String>,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "ApprovalDecision")]
pub(crate) struct ApprovalDecisionView {
    approval: (String, u32, u32),
    decision: String,
    turn: String,
    seq: u32,
    joined: bool,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "ReleaseSlot")]
pub(crate) struct ReleaseSlotView {
    status: String,
}

#[derive(serde::Deserialize, serde::Serialize, ts_rs::TS)]
#[serde(rename_all = "snake_case")]
#[ts(rename = "ApprovalAssessmentState")]
pub(crate) enum ApprovalAssessmentStateView {
    Pending,
    Running,
    Completed,
    Failed,
    Unavailable,
}

#[derive(serde::Deserialize, serde::Serialize, ts_rs::TS)]
#[serde(deny_unknown_fields)]
#[ts(rename = "ApprovalAssessment")]
pub(crate) struct ApprovalAssessmentView {
    state: ApprovalAssessmentStateView,
    confidence: Option<f64>,
    minimum_confidence: f64,
}

#[derive(serde::Deserialize, serde::Serialize, ts_rs::TS)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[ts(rename = "Card")]
pub(crate) enum CardView {
    Dispatch {
        conversation: String,
        parent: Option<String>,
        approval: (String, u32, u32),
        tool: String,
        arguments: String,
    },
    Approval {
        conversation: String,
        parent: Option<String>,
        approval: (String, u32, u32),
        tool: String,
        arguments: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[ts(optional)]
        assessment: Option<ApprovalAssessmentView>,
    },
    Question {
        conversation: String,
        parent: Option<String>,
        approval: (String, u32, u32),
        tool: String,
        question: String,
    },
    LoopHold {
        conversation: String,
        parent: Option<String>,
    },
    Quarantine {
        conversation: String,
        parent: Option<String>,
        turn: String,
    },
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Cards")]
pub(crate) struct CardsView {
    conversation: String,
    cards: Vec<CardView>,
}

fn admission(admission: AdmissionView) -> Value {
    envelope(serde_json::json!(AdmissionResponseView { admission }))
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
    submit_plan(shared, key, Plan::Job(request)).await
}

/// Submits one typed decision under `key`, with the same admission
/// guarantees as an ordinary Job submission.
pub(super) async fn submit_decision(
    shared: &Arc<Shared>,
    key: SubmissionKey,
    request: DecisionJobRequest,
) -> Result<Reply, Refusal> {
    submit_plan(shared, key, Plan::Decision(request)).await
}

/// What a submission asks for. Both kinds are admitted once under one key.
enum Plan {
    Job(JobRequest),
    Decision(DecisionJobRequest),
}

impl Plan {
    fn fingerprint(&self) -> RequestFingerprint {
        match self {
            Self::Job(request) => request.fingerprint(),
            Self::Decision(request) => request.fingerprint(),
        }
    }
}

async fn submit_plan(
    shared: &Arc<Shared>,
    key: SubmissionKey,
    plan: Plan,
) -> Result<Reply, Refusal> {
    let fingerprint = plan.fingerprint();
    let decision = matches!(plan, Plan::Decision(_));
    let admission_value = move |job: JobId| async move {
        if decision {
            admitted_decision_value(shared, key, job).await
        } else {
            admitted_value(shared, key, job).await
        }
    };
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
                            admission_value(record.job).await?,
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
                ntex::rt::spawn(admit(shared.clone(), key, generation, fingerprint, plan));
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
                admission_value(job).await?,
            )),
            Ok(Ok(Outcome::Conflict)) => Err(KEY_CONFLICT),
            Ok(Ok(Outcome::Refused(refusal))) => Err(refusal),
            // The admission's task was dropped with the listener, so how it
            // ended is not known here.
            Ok(Ok(Outcome::Uncertain) | Err(_)) => Err(UNCERTAIN),
            Err(()) => Ok(Reply::new(
                StatusCode::ACCEPTED,
                admission(AdmissionView::Pending { job: None }),
            )),
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
            None => {
                return Ok(Reply::ok(admission(AdmissionView::Pending { job: None })));
            }
        },
        Some(Local::Admitting) => {
            return Ok(Reply::ok(admission(AdmissionView::Pending { job: None })));
        }
        Some(Local::Uncertain { generation, epoch }) => {
            match reconcile(shared, key, generation, epoch).await {
                Reconciled::Recorded(record) => Some(record),
                Reconciled::Absent => None,
                Reconciled::Unresolved => {
                    return Ok(Reply::ok(admission(AdmissionView::Uncertain { job: None })));
                }
            }
        }
        None => shared.database.submission(key).await?,
    };
    Ok(Reply::ok(match found {
        Some(record) => admitted_value(shared, key, record.job).await?,
        None => admission(AdmissionView::NotFound { job: None }),
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
    plan: Plan,
) {
    let registry = &shared.submissions;
    // Every waiter of an admission that ran is of the owner's request.
    let finish = |outcome: Outcome| registry.finish(key, generation, |_| outcome.clone());
    let recorded = shared.database.submission(key).await;

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
    match plan {
        Plan::Job(request) => {
            admit_job(&shared, key, generation, fingerprint, request, finish).await
        }
        Plan::Decision(request) => {
            admit_decision(&shared, key, generation, fingerprint, request, finish).await
        }
    }
}

/// The admission of an ordinary Job: its configuration is checked, then the
/// Job is opened with its submission.
async fn admit_job(
    shared: &Arc<Shared>,
    key: SubmissionKey,
    generation: u64,
    fingerprint: RequestFingerprint,
    request: JobRequest,
    finish: impl Fn(Outcome),
) {
    let mut request = request;
    if let Err(refusal) = normalize(&shared.database, &mut request).await {
        return finish(Outcome::Refused(refusal));
    }
    let project = match check_configuration(&shared.database, &request).await {
        Ok(project) => project,
        Err(refusal) => return finish(Outcome::Refused(refusal)),
    };
    let configuration = ExecutionConfiguration {
        reasoning_effort: request.reasoning_effort,
        agent_profile: request.agent_profile,
        llm_profile: request.llm_profile,
    };
    let first = crate::db::entities::Entry::UserMessage {
        text: request.objective,
        attachments: request.attachments,
    };
    let submission = Some(Submission { key, fingerprint });

    let admitted = match request.conversation {
        Some(conversation) if request.steer != Some(false) => match shared
            .database
            .steer_with_format(
                conversation,
                first,
                Some(configuration),
                submission,
                request.response_format,
                request.steer.is_none(),
            )
            .await
        {
            Ok((appended, running)) => {
                for (job, cause) in running {
                    shared.scheduler.stop(job, cause).await;
                }
                Ok(appended.turn)
            }
            Err(error) => Err(error),
        },
        Some(conversation) => shared
            .database
            .append_with_format(
                conversation,
                first,
                Some(configuration),
                submission,
                request.response_format,
            )
            .await
            .map(|appended| appended.turn),
        None => shared
            .database
            .open_group_with_format(
                None,
                crate::db::entities::JobGroup {
                    name: request.name.clone(),
                    agent: request.agent_profile.map(Into::into),
                    project,
                    origin: crate::db::entities::Origin::User,
                    keep_open: request.keep_open,
                },
                first,
                configuration,
                submission,
                request.response_format,
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
            shared
                .submissions
                .uncertain(key, generation, operation.epoch())
        }
        Err(DatabaseError::Rejected(Violation::InvalidValue("vision_not_supported", _))) => {
            finish(Outcome::Refused(VISION_NOT_SUPPORTED))
        }
        Err(DatabaseError::Rejected(Violation::InvalidValue("too_many_images", _))) => {
            finish(Outcome::Refused(TOO_MANY_IMAGES))
        }
        Err(DatabaseError::Rejected(Violation::InvalidValue("compaction_in_progress", _))) => {
            finish(Outcome::Refused(Refusal::new(
                StatusCode::CONFLICT,
                "compaction_in_progress",
                "rule-based or manual compaction only accepts queued prompts",
            )))
        }
        Err(error) => finish(Outcome::Refused(not_admitted(
            &ExecutionRuntimeError::Database(error),
        ))),
    }
}

// ---- decision submissions --------------------------------------------------------

const DECISION_PROFILE_NOT_FOUND: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "decision_profile_not_found",
    "no such Decision Profile; nothing was submitted",
);
const DECISION_MODEL_NOT_FOUND: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "decision_model_not_found",
    "no such Decision Model Profile; nothing was submitted",
);
const DECISION_MODEL_NOT_TARGETED: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "decision_model_not_targeted",
    "the Decision Profile does not select that Decision Model Profile; nothing was submitted",
);
const DECISION_EXECUTOR_NOT_FOUND: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "executor_not_found",
    "no such Executor; nothing was submitted",
);
const DECISION_EXECUTOR_INCOMPATIBLE: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "decision_executor_incompatible",
    "the Executor does not offer decision work; nothing was submitted",
);
const INVALID_DECISION_REQUEST: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "invalid_decision_request",
    "the request is not a typed decision this API accepts",
);

/// A validated decision submission: an existing Decision Profile and
/// Decision Model Profile, an Executor that offers decision work, and one
/// typed decision request. Nothing else can be submitted: the endpoint, the
/// credential and the model come from the Decision Model Profile.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct DecisionJobRequest {
    reasoning_effort: Option<crate::db::entities::ReasoningEffort>,
    decision_agent: ProfileId,
    decision_model: ProfileId,
    executor: ExecutorId,
    request: crate::db::entities::DecisionRequest,
    name: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DecisionSubmissionInput {
    reasoning_effort: Option<String>,
    decision_agent: String,
    decision_model: String,
    executor: String,
    request: DecisionRequestInput,
    #[serde(default)]
    name: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DecisionRequestInput {
    #[serde(default)]
    schema_version: Option<u32>,
    state: serde_json::Value,
    question: DecisionQuestionInput,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum DecisionQuestionInput {
    Choice {
        prompt: String,
        options: Vec<ChoiceOptionInput>,
    },
    BinaryProbability {
        prompt: String,
    },
    OrdinalScore {
        prompt: String,
        levels: Vec<ScoreLevelInput>,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChoiceOptionInput {
    id: String,
    description: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScoreLevelInput {
    value: i32,
    description: String,
}

impl DecisionJobRequest {
    /// Parses a decision submission body strictly: one JSON object of the
    /// known fields, none repeated, each within its bounds.
    pub(super) fn parse(body: &[u8]) -> Result<Self, Refusal> {
        let wire: DecisionSubmissionInput =
            serde_json::from_slice(body).map_err(|_| INVALID_DECISION_REQUEST)?;
        let decision_agent = identity(&wire.decision_agent, "profile:")
            .map(ProfileId::new)
            .ok_or(INVALID_DECISION_REQUEST)?;
        let decision_model = identity(&wire.decision_model, "profile:")
            .map(ProfileId::new)
            .ok_or(INVALID_DECISION_REQUEST)?;
        let executor = identity(&wire.executor, "executor:").ok_or(INVALID_DECISION_REQUEST)?;
        if wire
            .name
            .as_deref()
            .is_some_and(|name| !text(name, MAX_NAME_BYTES))
        {
            return Err(INVALID_DECISION_REQUEST);
        }
        let request = crate::db::entities::DecisionRequest {
            schema_version: wire
                .request
                .schema_version
                .unwrap_or(crate::db::entities::DECISION_SCHEMA_VERSION),
            state: wire.request.state,
            question: match wire.request.question {
                DecisionQuestionInput::Choice { prompt, options } => {
                    crate::db::entities::DecisionQuestion::Choice {
                        prompt,
                        options: options
                            .into_iter()
                            .map(|option| crate::db::entities::ChoiceOption {
                                id: option.id,
                                description: option.description,
                            })
                            .collect(),
                    }
                }
                DecisionQuestionInput::BinaryProbability { prompt } => {
                    crate::db::entities::DecisionQuestion::BinaryProbability { prompt }
                }
                DecisionQuestionInput::OrdinalScore { prompt, levels } => {
                    crate::db::entities::DecisionQuestion::OrdinalScore {
                        prompt,
                        levels: levels
                            .into_iter()
                            .map(|level| crate::db::entities::ScoreLevel {
                                value: level.value,
                                description: level.description,
                            })
                            .collect(),
                    }
                }
            },
        };
        // The structure of the request is checked here; the primitive the
        // question needs is checked against the Decision Model Profile when
        // the request is admitted.
        if request.schema_version != crate::db::entities::DECISION_SCHEMA_VERSION
            || serde_json::to_vec(&request)
                .map(|bytes| bytes.len() > crate::db::entities::MAX_DECISION_REQUEST_BYTES)
                .unwrap_or(true)
        {
            return Err(INVALID_DECISION_REQUEST);
        }
        let reasoning_effort = wire
            .reasoning_effort
            .as_deref()
            .map(|value| {
                crate::db::entities::ReasoningEffort::parse(value).ok_or(INVALID_DECISION_REQUEST)
            })
            .transpose()?;
        Ok(Self {
            reasoning_effort,
            decision_agent,
            decision_model,
            executor: ExecutorId::new(executor),
            request,
            name: wire.name,
        })
    }

    /// The digest of the request as parsed, whatever its JSON spelling.
    fn fingerprint(&self) -> RequestFingerprint {
        let mut context = Context::new(&SHA256);
        context.update(DECISION_REQUEST_DOMAIN);
        context.update(&self.decision_agent.get().to_le_bytes());
        context.update(&self.decision_model.get().to_le_bytes());
        context.update(&self.executor.get().to_le_bytes());
        match &self.name {
            None => context.update(&[0]),
            Some(name) => {
                context.update(&[1]);
                context.update(&(name.len() as u64).to_le_bytes());
                context.update(name.as_bytes());
            }
        }
        let request = serde_json::to_vec(&self.request).unwrap_or_default();
        context.update(&(request.len() as u64).to_le_bytes());
        context.update(&request);
        if let Some(effort) = self.reasoning_effort {
            context.update(&[2]);
            context.update(effort.as_str().as_bytes());
        }
        RequestFingerprint::from_digest(sha256(context))
    }
}

/// The admission of one typed decision: the Decision Profile, the Decision
/// Model Profile and the Executor are checked, then a system conversation
/// with one decision turn is opened with the request.
async fn admit_decision(
    shared: &Arc<Shared>,
    key: SubmissionKey,
    generation: u64,
    fingerprint: RequestFingerprint,
    request: DecisionJobRequest,
    finish: impl Fn(Outcome),
) {
    if let Err(refusal) = check_decision_configuration(&shared.database, &request).await {
        return finish(Outcome::Refused(refusal));
    }
    let submission = Some(Submission { key, fingerprint });
    let configuration = ExecutionConfiguration {
        reasoning_effort: request.reasoning_effort,
        agent_profile: Some(request.decision_agent),
        llm_profile: request.decision_model,
    };
    let first = crate::db::entities::Entry::UserMessage {
        text: match &request.request.question {
            crate::db::entities::DecisionQuestion::Choice { prompt, .. }
            | crate::db::entities::DecisionQuestion::BinaryProbability { prompt }
            | crate::db::entities::DecisionQuestion::OrdinalScore { prompt, .. } => prompt.clone(),
        },
        attachments: Vec::new(),
    };
    let admitted = shared
        .database
        .submit_decision(
            None,
            crate::db::entities::JobGroup {
                name: request.name.clone(),
                agent: Some(request.decision_agent.into()),
                project: None,
                origin: crate::db::entities::Origin::System,
                keep_open: false,
            },
            first,
            configuration,
            request.request.clone(),
            submission,
        )
        .await;
    match admitted {
        Ok((_, appended)) => finish(Outcome::Admitted {
            job: appended.turn,
            created: true,
        }),
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
            shared
                .submissions
                .uncertain(key, generation, operation.epoch())
        }
        Err(error) => finish(Outcome::Refused(not_admitted(
            &ExecutionRuntimeError::Database(error),
        ))),
    }
}

/// Refuses a Decision Profile that does not exist, a Decision Model Profile
/// that does not exist or is not selected by the Decision Profile, and an
/// Executor that does not exist or does not offer decision work. The
/// creation checks them again in its own transaction.
async fn check_decision_configuration(
    database: &DatabaseHandle,
    request: &DecisionJobRequest,
) -> Result<(), Refusal> {
    let agent = match database.config(request.decision_agent.into()).await? {
        Some(ConfigRecord {
            body: ConfigBody::DecisionAgent(agent),
            archived_at: None,
            ..
        }) => agent.models,
        Some(ConfigRecord {
            body: ConfigBody::Agent(agent),
            archived_at: None,
            ..
        }) if agent
            .capabilities
            .contains(&crate::db::entities::AgentCapability::Decision) =>
        {
            agent.models
        }
        _ => return Err(DECISION_PROFILE_NOT_FOUND),
    };
    let (capabilities, executors) = match database.config(request.decision_model.into()).await? {
        Some(ConfigRecord {
            body: ConfigBody::DecisionModel(model),
            archived_at: None,
            ..
        }) => {
            if request.reasoning_effort.is_some() {
                return Err(Refusal::new(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "reasoning_effort_unsupported",
                    "native decision models do not accept generative reasoning effort",
                ));
            }
            (
                model.declared_capabilities,
                database.decision_executors().await?,
            )
        }
        Some(ConfigRecord {
            body: ConfigBody::Model(model),
            archived_at: None,
            model_enabled: true,
            ..
        }) if model
            .declared_capabilities
            .contains(&crate::db::entities::ModelCapability::Chat) =>
        {
            if request.reasoning_effort.is_some_and(|effort| {
                model
                    .reasoning_efforts
                    .as_ref()
                    .is_some_and(|values| !values.iter().any(|value| value == effort.as_str()))
            }) {
                return Err(Refusal::new(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "reasoning_effort_unsupported",
                    "the selected model does not support the requested reasoning effort",
                ));
            }
            (
                std::collections::BTreeSet::from([
                    crate::db::entities::DecisionCapability::Choice,
                    crate::db::entities::DecisionCapability::BinaryProbability,
                    crate::db::entities::DecisionCapability::OrdinalScore,
                ]),
                database
                    .compatible_executors(request.decision_model)
                    .await?,
            )
        }
        _ => return Err(DECISION_MODEL_NOT_FOUND),
    };
    if !agent.contains(&request.decision_model.into()) {
        return Err(DECISION_MODEL_NOT_TARGETED);
    }
    request
        .request
        .validate(&capabilities)
        .map_err(|_| INVALID_DECISION_REQUEST)?;
    if database.executor(request.executor).await?.is_none() {
        return Err(DECISION_EXECUTOR_NOT_FOUND);
    }
    if !executors.contains(&request.executor) {
        return Err(DECISION_EXECUTOR_INCOMPATIBLE);
    }
    Ok(())
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
            archived_at: None,
            model_enabled: true,
            ..
        }) => model,
        _ => return Err(PROFILE_NOT_FOUND),
    };
    if request.reasoning_effort.is_some_and(|effort| {
        llm.reasoning_efforts
            .as_ref()
            .is_some_and(|values| !values.iter().any(|value| value == effort.as_str()))
    }) {
        return Err(Refusal::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "reasoning_effort_unsupported",
            "the selected model does not support the requested reasoning effort",
        ));
    }
    let mut tool_grant = false;
    if let Some(agent_profile) = request.agent_profile {
        match database.config(agent_profile.into()).await? {
            Some(ConfigRecord {
                body: ConfigBody::Agent(agent),
                ..
            }) => {
                if !agent.models.contains(&request.llm_profile.into())
                    || !agent.supports_model(&ConfigBody::Model(llm.clone()))
                    || (!agent.manages_jobs() && agent.workspace_grant().is_none())
                {
                    return Err(AGENT_TARGET_MISMATCH);
                }
                tool_grant = agent.workspace_grant().is_some();
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
            .job_execution(tip.id)
            .await?
            .0
            .llm
            .config
            .declared_capabilities;
        if let Some(input) = database.job_input_for_job(tip.id).await? {
            images.extend(input.attachments);
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
    check_user_images(database, &images).await?;
    Ok(project)
}

async fn check_user_images(
    database: &DatabaseHandle,
    images: &[AttachmentId],
) -> Result<(), Refusal> {
    for &attachment in images {
        match database.attachment(attachment).await? {
            Some(record) if record.origin == AttachmentOrigin::UserProvided => {}
            _ => return Err(ATTACHMENT_NOT_FOUND),
        }
    }
    match attachment_images(database, images).await {
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
            DatabaseError::Rejected(Violation::InvalidValue("dependencies", _)) => Refusal::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "job_dependencies_refused",
                "the new turn cannot depend on the selected Jobs; nothing was admitted",
            ),
            DatabaseError::Rejected(Violation::PendingJobsLimit { .. }) => PENDING_LIMIT_REACHED,
            DatabaseError::Rejected(violation) => match violation {
                Violation::InvalidValue(field, reason) => {
                    Refusal::new(StatusCode::UNPROCESSABLE_ENTITY, field, reason)
                }
                Violation::NotFound(_) => Refusal::new(
                    StatusCode::NOT_FOUND,
                    "record_not_found",
                    "a referenced record was not found",
                )
                .with_reason(violation.to_string()),
                Violation::WrongConfigKind { .. } | Violation::NotAConfiguredTarget { .. } => {
                    CONFIGURATION_REFUSED.with_reason(violation.to_string())
                }
                violation => Refusal::new(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "submission_refused",
                    "the Job submission was refused",
                )
                .with_reason(violation.to_string()),
            },
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
        let crate::db::entities::JobKind::Group {
            agent,
            project,
            origin,
            ..
        } = row.kind
        else {
            return Err(conversation_refusal("conversation_not_found"));
        };
        if matches!(origin, crate::db::entities::Origin::System) {
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
        let agent = request.agent_profile.ok_or(INVALID_BODY)?;
        let agent = match database.config(agent.into()).await? {
            Some(ConfigRecord {
                body: ConfigBody::Agent(agent),
                ..
            }) => agent,
            _ => return Err(AGENT_PROFILE_NOT_FOUND),
        };
        request.llm_profile = agent.default_model().ok_or(INVALID_BODY)?.into();
    }
    Ok(())
}

/// `POST /api/v1/conversations/{id}/compact`. The body names the generative
/// model. A repeated idempotency key returns the same preparation. A source
/// that will not fit is an explicit refusal, not a truncated job.
pub(super) async fn compact(
    shared: &Shared,
    conversation: JobId,
    key: SubmissionKey,
    body: &[u8],
) -> Result<Reply, Refusal> {
    let value: Value = serde_json::from_slice(body).map_err(|_| INVALID_ROUTE_BODY)?;
    let object = value.as_object().ok_or(INVALID_ROUTE_BODY)?;
    if object.len() != 1 {
        return Err(INVALID_ROUTE_BODY);
    }
    let model = object
        .get("model")
        .and_then(Value::as_str)
        .ok_or(INVALID_ROUTE_BODY)?;
    let model = identity(model, "profile:").ok_or(INVALID_ROUTE_BODY)?;
    let fingerprint = RequestFingerprint::from_digest({
        let mut context = Context::new(&SHA256);
        context.update(conversation.to_string().as_bytes());
        context.update(&[0]);
        context.update(&model.to_le_bytes());
        sha256(context)
    });
    let result = shared
        .database
        .request_compaction(
            conversation,
            ConfigId::new(model),
            Submission { key, fingerprint },
        )
        .await
        .map_err(action_error)?;
    let wire = match result {
        crate::db::entities::CompactionRequest::Prepared {
            job,
            conversation: child,
            through,
            subsequent,
            already,
        } => CompactionView {
            conversation: conversation.to_string(),
            status: if already { "already" } else { "queued" }.to_owned(),
            job: Some(job.to_string()),
            system_conversation: Some(child.to_string()),
            through: Some(through.to_string()),
            subsequent,
            reason: None,
        },
        crate::db::entities::CompactionRequest::Noop { reason } => CompactionView {
            conversation: conversation.to_string(),
            status: "noop".to_owned(),
            job: None,
            system_conversation: None,
            through: None,
            subsequent: false,
            reason: Some(reason.to_owned()),
        },
    };
    Ok(Reply::ok(envelope(serde_json::json!(wire))))
}

pub(super) async fn action(
    shared: &Shared,
    conversation: JobId,
    action: &str,
    body: &[u8],
) -> Result<Reply, Refusal> {
    use crate::db::entities::{Actor, StopCause};
    let value: Value = serde_json::from_slice(body).map_err(|_| INVALID_ROUTE_BODY)?;
    let object = value.as_object().ok_or(INVALID_ROUTE_BODY)?;
    if action == "queue" {
        #[derive(Deserialize)]
        #[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
        enum Amendment {
            Edit {
                turn: String,
                message: u32,
                text: String,
                attachments: Vec<String>,
            },
            Withdraw {
                turn: String,
                message: u32,
            },
        }
        let amendment: Amendment = serde_json::from_slice(body).map_err(|_| INVALID_ROUTE_BODY)?;
        let (turn, entry) = match amendment {
            Amendment::Edit {
                turn,
                message,
                text: content,
                attachments,
            } => {
                if !text(&content, MAX_OBJECTIVE_BYTES) || attachments.len() > MAX_REQUEST_IMAGES {
                    return Err(INVALID_ROUTE_BODY);
                }
                let attachments = attachments
                    .iter()
                    .map(|id| {
                        identity(id, "attachment:")
                            .map(AttachmentId::new)
                            .ok_or(INVALID_ROUTE_BODY)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if attachments
                    .iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    != attachments.len()
                {
                    return Err(INVALID_ROUTE_BODY);
                }
                (
                    turn,
                    crate::db::entities::Entry::UserMessageEdit {
                        message,
                        text: content,
                        attachments,
                    },
                )
            }
            Amendment::Withdraw { turn, message } => (
                turn,
                crate::db::entities::Entry::UserMessageWithdraw { message },
            ),
        };
        let turn = super::routing::job_id(&turn)?;
        if let crate::db::entities::Entry::UserMessageEdit { attachments, .. } = &entry {
            check_user_images(&shared.database, attachments).await?;
        }
        let appended = shared
            .database
            .amend_queued_input(conversation, turn, entry)
            .await
            .map_err(action_error)?;
        return Ok(Reply::ok(envelope(serde_json::json!(AppendView {
            conversation: conversation.to_string(),
            turn: appended.turn.to_string(),
            seq: appended.seq,
            joined: true,
        }))));
    }
    if action == "retry" {
        if object.len() != 1 {
            return Err(INVALID_ROUTE_BODY);
        }
        let turn = super::routing::job_id(
            object
                .get("turn")
                .and_then(Value::as_str)
                .ok_or(INVALID_ROUTE_BODY)?,
        )?;
        let appended = shared
            .database
            .retry(conversation, turn, Actor::User)
            .await
            .map_err(action_error)?;
        return Ok(Reply::ok(envelope(serde_json::json!(AppendView {
            conversation: conversation.to_string(),
            turn: appended.turn.to_string(),
            seq: appended.seq,
            joined: appended.joined,
        }))));
    }
    if !object.is_empty() {
        return Err(INVALID_ROUTE_BODY);
    }
    let result = match action {
        "stop" => shared.database.stop_turn(conversation, Actor::User).await,
        "close" => shared.database.close(conversation, Actor::User).await,
        "hold" => shared.database.hold(conversation, Actor::User).await,
        "unhold" => shared.database.unhold(conversation, Actor::User).await,
        _ => return Err(INVALID_ROUTE_BODY),
    }
    .map_err(action_error)?;
    for running in &result.running {
        shared
            .scheduler
            .stop(
                *running,
                if action == "close" {
                    StopCause::Closed
                } else {
                    StopCause::Stopped(result.record_seq.ok_or(INTERNAL)?)
                },
            )
            .await;
    }
    if result.already {
        let settled = shared
            .database
            .job(conversation)
            .await?
            .ok_or(INTERNAL)?
            .outcome
            .is_some();
        return Ok(Reply::ok(envelope(serde_json::json!(StopView {
            conversation: conversation.to_string(),
            status: if settled {
                "already_settled"
            } else {
                "already_closing"
            }
            .to_owned(),
            record_seq: result.record_seq,
        }))));
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
                    outcome.cause == Some(crate::db::entities::OutcomeCause::Stopped { record })
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
        envelope(serde_json::json!(StopView {
            conversation: conversation.to_string(),
            status: if changed {
                "requested"
            } else {
                "already_settled"
            }
            .to_owned(),
            record_seq: result.record_seq,
        })),
    ))
}
fn action_error(error: DatabaseError) -> Refusal {
    match error {
        DatabaseError::Rejected(Violation::NotFound(
            crate::db::entities::RecordRef::Attachment(_),
        )) => ATTACHMENT_NOT_FOUND,
        DatabaseError::Rejected(Violation::AttachmentCleanedUp(_)) => ATTACHMENT_UNAVAILABLE,
        DatabaseError::Rejected(Violation::InvalidValue("turn_input" | "queued_message", _)) => {
            Refusal::new(
                StatusCode::CONFLICT,
                "queued_message_unavailable",
                "the queued message has started, settled, or been withdrawn; refresh the conversation",
            )
        }
        DatabaseError::Rejected(Violation::InvalidValue(code, reason)) => {
            conversation_refusal(code).with_message(reason)
        }
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
    Ok(admission(AdmissionView::Admitted {
        job: job.to_string(),
        conversation: turn.group.map(|id| id.to_string()),
        seq: record.seq,
        joined: record.seq > 1,
        model: frozen.frozen.llm.config.model,
        kept: turn.cancelled_at.is_some(),
    }))
}

/// The admission of a decision Job. Its model comes from the Decision Model
/// Profile it froze, not from a generative configuration.
async fn admitted_decision_value(
    shared: &Shared,
    key: SubmissionKey,
    job: JobId,
) -> Result<Value, Refusal> {
    let record = shared.database.submission(key).await?.ok_or(INTERNAL)?;
    let turn = shared.database.job(job).await?.ok_or(INTERNAL)?;
    let work = shared.database.decision_work(job).await?.ok_or(INTERNAL)?;
    Ok(admission(AdmissionView::Admitted {
        job: job.to_string(),
        conversation: turn.group.map(|id| id.to_string()),
        seq: record.seq,
        joined: record.seq > 1,
        model: work.frozen.model_name().to_owned(),
        kept: turn.cancelled_at.is_some(),
    }))
}

pub(super) async fn decide(
    shared: &Shared,
    approval: crate::db::entities::ToolCallRef,
    body: &[u8],
) -> Result<Reply, Refusal> {
    use crate::db::entities::ApprovalDecision;
    let value: Value = serde_json::from_slice(body).map_err(|_| INVALID_ROUTE_BODY)?;
    let object = value
        .as_object()
        .filter(|object| object.len() == 1)
        .ok_or(INVALID_ROUTE_BODY)?;
    let (decision, name) = if let Some(answer) = object.get("answer") {
        (
            ApprovalDecision::Answered {
                text: answer.as_str().ok_or(INVALID_ROUTE_BODY)?.to_owned(),
            },
            "answered",
        )
    } else {
        match object.get("decision").and_then(Value::as_str) {
            Some("approve") => (ApprovalDecision::Approved, "approved"),
            Some("reject") => (ApprovalDecision::Rejected, "rejected"),
            _ => return Err(INVALID_ROUTE_BODY),
        }
    };
    let appended = shared
        .database
        .decide_approval(approval, decision)
        .await
        .map_err(action_error)?;
    Ok(Reply::ok(envelope(serde_json::json!(
        ApprovalDecisionView {
            approval: (
                approval.job.to_string(),
                approval.request.get(),
                approval.index,
            ),
            decision: name.to_owned(),
            turn: appended.turn.to_string(),
            seq: appended.seq,
            joined: appended.joined,
        }
    ))))
}

pub(super) async fn cards(shared: &Shared, conversation: JobId) -> Result<Reply, Refusal> {
    if !shared
        .database
        .job(conversation)
        .await?
        .is_some_and(|job| job.kind.is_group())
    {
        return Err(conversation_refusal("conversation_not_found"));
    }
    let cards = shared
        .database
        .conversation_cards(conversation)
        .await?
        .into_iter()
        .map(serde_json::from_value)
        .collect::<Result<Vec<CardView>, _>>()
        .map_err(|_| INTERNAL)?;
    Ok(Reply::ok(envelope(serde_json::json!(CardsView {
        conversation: conversation.to_string(),
        cards,
    }))))
}

pub(super) async fn release_slot(
    database: &DatabaseHandle,
    job: JobId,
    body: &[u8],
) -> Result<Reply, Refusal> {
    use crate::db::entities::StopProof;
    use crate::system::executor::blocking::{self, JobProof};
    let value: Value = serde_json::from_slice(body).map_err(|_| INVALID_ROUTE_BODY)?;
    let object = value.as_object().ok_or(INVALID_ROUTE_BODY)?;
    if object.keys().any(|key| key != "confirm")
        || object
            .get("confirm")
            .is_some_and(|v| v != &Value::Bool(true))
    {
        return Err(INVALID_ROUTE_BODY);
    }
    let record = database.job(job).await?.ok_or(super::JOB_NOT_FOUND)?;
    if record.unknown_at.is_none() {
        return Err(conversation_refusal("not_quarantined"));
    }
    let released = || {
        Reply::ok(envelope(serde_json::json!(ReleaseSlotView {
            status: "already_released".to_owned(),
        })))
    };
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
                envelope(serde_json::json!({
                    "error": serde_json::json!({
                        "code": refusal.code.to_owned(),
                        "message": format!("process group {group} is still live"),
                        "group": group,
                    }),
                })),
            ));
        }
        JobProof::Untracked if object.get("confirm") == Some(&Value::Bool(true)) => StopProof::User,
        JobProof::Untracked => return Err(conversation_refusal("confirm_required")),
    };
    match database.confirm_job_stopped(job, by).await {
        Ok(()) => Ok(Reply::ok(envelope(serde_json::json!(ReleaseSlotView {
            status: "released".to_owned(),
        })))),
        Err(DatabaseError::Rejected(Violation::StopAlreadyConfirmed(_))) => Ok(released()),
        Err(error) => Err(action_error(error)),
    }
}
