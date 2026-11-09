use super::commands::{
    byte_count, command_bytes, compaction_bytes, config_bytes, debug_bytes, entry_bytes,
    json_bytes, output_bytes, search_index_bytes, text_bytes,
};
use super::*;

/// An owned request prepared before start. Only this module constructs it;
/// database checks still have to succeed in the start transaction.
pub(crate) struct PreparedDevelopmentHandoff {
    candidate: redb::DevelopmentCandidate,
    images: Vec<(
        AttachmentId,
        crate::service::ai::providers::openai_compatible::ImageInput,
    )>,
    request: serde_json::Value,
    conversation: Option<crate::service::ai::providers::openai_compatible::ChatConversation>,
    system_policy: SystemPolicy,
    tool_policy: DevelopmentToolPolicy,
    credentials: DevelopmentHandoffCredentials,
}

/// The credentials a Job's requests use: the Provider credential and the
/// secret values of its custom headers. Each is read once, with the stored row
/// kept as change evidence for the start transaction.
#[derive(Default)]
struct DevelopmentHandoffCredentials {
    items: Vec<DevelopmentHandoffCredential>,
    /// The encodings of every secret, built once, that leak checks look for.
    spellings: Option<crate::system::secrets::Spellings>,
}

struct DevelopmentHandoffCredential {
    reference: String,
    encrypted_row: Vec<u8>,
    // CredentialSecret redacts Debug; never include it in the request body.
    secret: redb::CredentialSecret,
}

impl DevelopmentHandoffCredentials {
    fn push(&mut self, reference: String, encrypted_row: Vec<u8>, secret: redb::CredentialSecret) {
        match &mut self.spellings {
            Some(spellings) => spellings.extend(secret.as_str()),
            None => self.spellings = Some(crate::system::secrets::Spellings::new(secret.as_str())),
        }
        self.items.push(DevelopmentHandoffCredential {
            reference,
            encrypted_row,
            secret,
        });
    }
}

enum DevelopmentToolPolicy {
    NoToolsOrWorkspace,
}

impl PreparedDevelopmentHandoff {
    /// The exact request body the first Provider request will carry.
    pub(crate) fn request(&self) -> &serde_json::Value {
        &self.request
    }

    /// The frozen Generative configuration this handoff executes, if it is one.
    pub(crate) fn generative_configuration(&self) -> Option<&entities::FrozenExecutionConfig> {
        match &self.candidate {
            redb::DevelopmentCandidate::Generative(context) => Some(&context.configuration),
            redb::DevelopmentCandidate::Decision(_) => None,
        }
    }

    /// Every credential the frozen configuration names; see the private reader.
    pub(crate) fn credential_references(&self) -> Vec<&str> {
        self.expected_credential_references()
    }

    pub(super) fn candidate(&self) -> &redb::DevelopmentCandidate {
        &self.candidate
    }

    /// The stored credential rows read for this handoff.
    pub(super) fn credential_evidence(&self) -> impl Iterator<Item = (&str, &[u8])> {
        self.credentials
            .items
            .iter()
            .map(|item| (item.reference.as_str(), item.encrypted_row.as_slice()))
    }

    fn provider_credential_reference(&self) -> Option<&str> {
        match &self.candidate {
            redb::DevelopmentCandidate::Generative(context) => {
                context.configuration.llm.provider.credential_ref.as_deref()
            }
            redb::DevelopmentCandidate::Decision(context) => {
                context.configuration.credential_ref.as_deref()
            }
        }
    }

    fn header_secret_references(&self) -> Vec<&str> {
        let headers = match &self.candidate {
            redb::DevelopmentCandidate::Generative(context) => {
                &context.configuration.llm.provider.headers
            }
            redb::DevelopmentCandidate::Decision(context) => &context.configuration.headers,
        };
        headers.secret_refs().collect()
    }

    /// Every credential the frozen configuration names, each once: the
    /// Provider credential, then the secret header values.
    pub(super) fn expected_credential_references(&self) -> Vec<&str> {
        let mut references: Vec<&str> = self.provider_credential_reference().into_iter().collect();
        for reference in self.header_secret_references() {
            if !references.contains(&reference) {
                references.push(reference);
            }
        }
        references
    }

    pub(super) fn has_bounded_tool_free_inputs(&self) -> bool {
        if !matches!(self.tool_policy, DevelopmentToolPolicy::NoToolsOrWorkspace)
            || !self.request.is_object()
        {
            return false;
        }
        match &self.candidate {
            redb::DevelopmentCandidate::Decision(_) => self.images.is_empty(),
            redb::DevelopmentCandidate::Generative(context) => {
                let expected = context
                    .covered_inputs
                    .iter()
                    .flat_map(|turn| &turn.attachments)
                    .chain(context.history.iter().flat_map(|turn| &turn.attachments))
                    .chain(&context.attachments)
                    .map(|attachment| attachment.id);
                expected.eq(self.images.iter().map(|(id, _)| *id))
            }
        }
    }
}

/// A single-use, in-memory result returned only after durable start commit.
#[allow(dead_code)]
pub(crate) struct DevelopmentHandoff {
    prepared: PreparedDevelopmentHandoff,
    started: entities::MutableJob,
    owner: Arc<Shared>,
    output_busy: AtomicBool,
    output_uncertain: AtomicBool,
    usage_checkpoint: std::sync::Mutex<DevelopmentUsageCheckpoint>,
}

impl DevelopmentHandoff {
    pub(crate) fn contains_secret(&self, value: &str) -> bool {
        self.prepared
            .credentials
            .spellings
            .as_ref()
            .is_some_and(|spellings| spellings.found_in(value))
    }
    pub(crate) fn generative_inputs(
        &self,
    ) -> Option<(
        &entities::FrozenExecutionConfig,
        &crate::service::ai::providers::openai_compatible::ChatConversation,
        &serde_json::Value,
        entities::AllocationToken,
    )> {
        let redb::DevelopmentCandidate::Generative(context) = &self.prepared.candidate else {
            return None;
        };
        Some((
            &context.configuration,
            self.prepared.conversation.as_ref()?,
            &self.prepared.request,
            context.allocation.token,
        ))
    }

    pub(crate) fn decision_inputs(
        &self,
    ) -> Option<(
        &entities::NativeDecisionConfig,
        &entities::DecisionRequest,
        &serde_json::Value,
        entities::AllocationToken,
    )> {
        let redb::DevelopmentCandidate::Decision(context) = &self.prepared.candidate else {
            return None;
        };
        let entities::MutableJobContent::Decision { request, .. } = &context.job.input.content
        else {
            return None;
        };
        Some((
            &context.configuration,
            request,
            &self.prepared.request,
            context.allocation.token,
        ))
    }
}

impl crate::service::ai::providers::CredentialResolver for DevelopmentHandoff {
    fn resolve<'a>(
        &'a self,
        reference: Option<&'a str>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        Option<String>,
                        crate::service::ai::providers::ProviderExecutionError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let found = reference.and_then(|reference| {
                self.prepared
                    .credentials
                    .items
                    .iter()
                    .find(|item| item.reference == reference)
            });
            match (reference, found) {
                (None, _) if self.prepared.provider_credential_reference().is_none() => Ok(None),
                (Some(_), Some(credential)) => Ok(Some(credential.secret.as_str().to_owned())),
                _ => Err(
                    crate::service::ai::providers::ProviderExecutionError::Build {
                        message: "handoff credential reference mismatch".to_owned(),
                    },
                ),
            }
        })
    }
}

/// Derived only from confirmed writes belonging to this handoff. The start
/// transaction refuses existing evidence and the writer checks the durable
/// sequence before using it; an uncertain write makes the handoff unusable.
#[derive(Clone, Default)]
pub(super) struct DevelopmentUsageCheckpoint {
    last_sequence: u32,
    evidence: entities::TokenEvidence,
}

impl DevelopmentUsageCheckpoint {
    pub(super) fn last_sequence(&self) -> u32 {
        self.last_sequence
    }

    pub(super) fn evidence(&self) -> entities::TokenEvidence {
        self.evidence.clone()
    }

    pub(super) fn advanced(last_sequence: u32, evidence: entities::TokenEvidence) -> Self {
        Self {
            last_sequence,
            evidence,
        }
    }
}

struct DevelopmentOutputGuard<'a> {
    handoff: &'a DevelopmentHandoff,
    known: bool,
}

impl Drop for DevelopmentOutputGuard<'_> {
    fn drop(&mut self) {
        if !self.known {
            self.handoff.output_uncertain.store(true, Ordering::SeqCst);
        }
        self.handoff.output_busy.store(false, Ordering::SeqCst);
    }
}

fn development_contains_secret(
    value: &serde_json::Value,
    secret: &crate::system::secrets::Spellings,
) -> bool {
    match value {
        serde_json::Value::String(text) => secret.found_in(text),
        serde_json::Value::Array(values) => values
            .iter()
            .any(|value| development_contains_secret(value, secret)),
        serde_json::Value::Object(values) => values
            .iter()
            .any(|(key, value)| secret.found_in(key) || development_contains_secret(value, secret)),
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {
            false
        }
    }
}

/// Only a committed handoff can create this in-process write proof.
pub(crate) struct DevelopmentWriteProof {
    token: entities::AllocationToken,
    revision: u64,
}

impl DevelopmentWriteProof {
    pub(super) fn token(&self) -> entities::AllocationToken {
        self.token
    }
    pub(super) fn revision(&self) -> u64 {
        self.revision
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum DevelopmentHandoffError {
    /// The handoff failed before any start was submitted, or the checked
    /// start definitely refused it. No execution authority exists and nothing
    /// was sent; the Allocation is still only Reserved and may be ended with
    /// [`DatabaseHandle::fail_unstarted_development_reservation`].
    NotStarted {
        cause: entities::UnstartedCause,
        error: Box<DatabaseError>,
    },
    /// The failure does not prove that nothing started or that the failure is
    /// permanent (storage, shutdown, queue pressure); ownership stays held.
    Database(Box<DatabaseError>),
    /// Reconciliation is evidence only: no in-memory permit is returned.
    OutcomeUnknown {
        error: Box<DatabaseError>,
        reconciliation: Result<OperationStatus, Box<DatabaseError>>,
    },
}

/// Why a preview of a start did not complete. Neither case changed anything.
#[derive(Debug)]
pub(crate) enum DevelopmentPreviewError {
    /// The caller's guard refused the exact prepared request.
    Refused(&'static str),
    /// The request could not be prepared: the Job, its context or a stored
    /// credential is unavailable, or the read failed.
    Unavailable(DatabaseError),
}

/// A handoff failure that a retry cannot change and that happened before any
/// start was committed, with the reason to record. Storage, shutdown and queue
/// failures are not decisions about the Job, so they are not classified.
fn classify_unstarted(error: DatabaseError) -> DevelopmentHandoffError {
    let cause = match &error {
        DatabaseError::Rejected(Violation::InvalidValue(field, reason))
            if *field == "credential" || reason.contains("credential") =>
        {
            Some(entities::UnstartedCause::FrozenSecretUnavailable)
        }
        DatabaseError::Rejected(_) => Some(entities::UnstartedCause::StartRefused),
        DatabaseError::Read(storage)
            if storage.kind == StorageErrorKind::Other && storage.os_error.is_none() =>
        {
            Some(entities::UnstartedCause::InputUnavailable)
        }
        _ => None,
    };
    match cause {
        Some(cause) => DevelopmentHandoffError::NotStarted {
            cause,
            error: Box::new(error),
        },
        None => DevelopmentHandoffError::Database(Box::new(error)),
    }
}

fn prepare_development_handoff(
    resolved: redb::DevelopmentResolvedCandidate,
    system_policy: SystemPolicy,
) -> Result<PreparedDevelopmentHandoff, DatabaseError> {
    use crate::service::ai::compact::{
        ContextLimits, ContextNotices, estimate_request_tokens, fit_with_notices,
    };
    use crate::service::ai::providers::openai_compatible::image::ImageInput;
    let unavailable = || {
        DatabaseError::Rejected(Violation::InvalidValue(
            "handoff",
            "execution context or policy cannot be safely prepared",
        ))
    };
    match resolved {
        redb::DevelopmentResolvedCandidate::Decision(context) => {
            if context.conversation.parent_id.is_some() {
                return Err(unavailable());
            }
            let entities::MutableJobContent::Decision { request, .. } = &context.job.input.content
            else {
                return Err(unavailable());
            };
            let request = crate::service::ai::decisive_model::request_body(
                &entities::FrozenDecisionConfig::Native(context.configuration.clone()),
                request,
            )
            .map_err(|_| unavailable())?;
            Ok(PreparedDevelopmentHandoff {
                candidate: redb::DevelopmentCandidate::Decision(context),
                images: Vec::new(),
                request,
                conversation: None,
                system_policy,
                tool_policy: DevelopmentToolPolicy::NoToolsOrWorkspace,
                credentials: DevelopmentHandoffCredentials::default(),
            })
        }
        redb::DevelopmentResolvedCandidate::Generative(context) => {
            let snapshot = &context.snapshot;
            let frozen = &snapshot.configuration;
            if snapshot.conversation.parent_id.is_some()
                || frozen.single_request_refusal().is_some()
            {
                return Err(unavailable());
            }
            let mut image_cursor = context.images.iter();
            let mut images_for =
                |records: &[entities::AttachmentRecord]| -> Result<Vec<ImageInput>, DatabaseError> {
                    records
                        .iter()
                        .map(|record| {
                            let (id, image) = image_cursor.next().ok_or_else(unavailable)?;
                            if *id != record.id {
                                return Err(unavailable());
                            }
                            Ok(image.clone())
                        })
                        .collect()
                };
            let mut prior = Vec::new();
            if let Some(summary) = &snapshot.adopted_summary {
                prior.push((Some(format!("Generated context summary (advisory, not an original user instruction). Originals remain available:\n{}", summary.summary.context_text())), Vec::new(), None));
            }
            for input in &snapshot.covered_inputs {
                prior.push((
                    Some(input.text.clone()),
                    images_for(&input.attachments)?,
                    None,
                ));
            }
            for turn in &snapshot.history {
                let answer = if turn
                    .record
                    .outcome
                    .as_ref()
                    .is_some_and(|outcome| outcome.kind == entities::OutcomeKind::Cancelled)
                {
                    None
                } else {
                    turn.outputs
                        .last()
                        .and_then(entities::JobOutputRecord::display_text)
                };
                prior.push((
                    Some(turn.record.job.input.text().to_owned()),
                    images_for(&turn.attachments)?,
                    answer,
                ));
            }
            let current_images = images_for(&snapshot.attachments)?;
            if image_cursor.next().is_some() {
                return Err(unavailable());
            }
            let limits = ContextLimits::for_profile(&frozen.llm.config);
            let mut conversation = fit_with_notices(
                frozen,
                prior,
                Some(snapshot.job.input.text()),
                &current_images,
                snapshot.project.as_ref(),
                None,
                None,
                Vec::new(),
                limits,
                ContextNotices::default(),
            )
            .ok_or_else(unavailable)?;
            if let Some(project) = &snapshot.project {
                let first = conversation.messages.first_mut().ok_or_else(unavailable)?;
                *first = crate::service::ai::providers::openai_compatible::ChatMessage::system(
                    format!(
                        "You work in the Project {:?}. Its workspace root is {}. No workspace access or tools are available for this execution; do not claim to read or change project files.",
                        project.name, project.root
                    ),
                );
            }
            if conversation.dropped_turns != 0
                || !conversation.tools.is_empty()
                || estimate_request_tokens(&conversation, limits.max_tokens)
                    > u64::from(limits.context_window.ok_or_else(unavailable)?)
            {
                return Err(unavailable());
            }
            conversation.validate().map_err(|_| unavailable())?;
            let request = crate::service::ai::generative_model::request_body(
                &frozen.llm.config.model,
                &frozen.llm.config.parameters,
                &conversation,
            )
            .map_err(|_| unavailable())?;
            let mut verified = context.images.iter();
            let messages = request
                .get("messages")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(unavailable)?;
            for (index, message) in messages.iter().enumerate() {
                let mut ids = Vec::new();
                if let Some(parts) = message.get("content").and_then(serde_json::Value::as_array) {
                    for part in parts.iter().filter(|part| {
                        part.get("type").and_then(serde_json::Value::as_str) == Some("image_url")
                    }) {
                        let (id, image) = verified.next().ok_or_else(unavailable)?;
                        if serde_json::to_value(image.part()).map_err(|_| unavailable())? != *part {
                            return Err(unavailable());
                        }
                        ids.push(*id);
                    }
                }
                if !ids.is_empty() {
                    conversation.image_attachments.insert(index, ids);
                }
            }
            if verified.next().is_some() {
                return Err(unavailable());
            }
            Ok(PreparedDevelopmentHandoff {
                candidate: redb::DevelopmentCandidate::Generative(context.snapshot),
                images: context.images,
                request,
                conversation: Some(conversation),
                system_policy,
                tool_policy: DevelopmentToolPolicy::NoToolsOrWorkspace,
                credentials: DevelopmentHandoffCredentials::default(),
            })
        }
    }
}

struct AttachmentPermit(flume::Sender<()>);

pub(crate) struct UpdatePause(Arc<Shared>);

impl Drop for UpdatePause {
    fn drop(&mut self) {
        self.0.update_paused.store(false, Ordering::SeqCst);
        self.0.scheduler_wake.publish(true, true);
    }
}

impl Drop for AttachmentPermit {
    fn drop(&mut self) {
        let _ = self.0.try_send(());
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Vitals {
    pub database_bytes: Option<u64>,
    pub available_bytes: Option<u64>,
    pub durable_commits: u64,
    pub commit_us_mean: Option<u64>,
    pub commit_us_max: u64,
    pub claims_count: u64,
    pub claim_us_mean: Option<u64>,
    pub claim_us_max: u64,
    pub queue_pending: Option<u64>,
    pub queue_ready: Option<u64>,
    pub queue_delayed: Option<u64>,
    pub queue_running: Option<u64>,
    pub commands_waiting: u64,
    pub rss_bytes: Option<u64>,
    pub peak_rss_bytes: Option<u64>,
}

/// The database file's size and the space free beside it. Both readings are
/// optional: a probe that fails stays unknown rather than becoming zero.
fn filesystem_vitals(directory: &std::path::Path) -> (Option<u64>, Option<u64>) {
    let db_file = crate::db::redb::database_file(directory);
    (
        std::fs::metadata(&db_file)
            .map(|metadata| metadata.len())
            .ok(),
        fs4::available_space(directory).ok(),
    )
}

fn model_profile_save_bytes(save: &mutations::ModelProfileSave) -> u64 {
    let credential_ref_bytes = match &save.credential_ref {
        mutations::ProfileCredentialRef::Preserve | mutations::ProfileCredentialRef::Clear => 0,
        mutations::ProfileCredentialRef::Explicit(reference)
        | mutations::ProfileCredentialRef::PreserveOrGenerate(reference) => {
            byte_count(reference.len())
        }
    };
    let definition_bytes = match &save.definition {
        mutations::ModelProfileDefinition::Llm {
            model,
            declared_capabilities,
            ..
        } => byte_count(model.len())
            .saturating_add(byte_count(declared_capabilities.len()).saturating_mul(8))
            .saturating_add(512),
        mutations::ModelProfileDefinition::Decision {
            model,
            declared_capabilities,
            ..
        } => byte_count(model.len())
            .saturating_add(byte_count(declared_capabilities.len()).saturating_mul(8))
            .saturating_add(2048),
    };
    let secret_bytes = save
        .secret
        .as_ref()
        .map_or(0, |secret| byte_count(secret.as_str().len()));
    byte_count(save.name.len())
        .saturating_add(byte_count(save.connection_name.len()))
        .saturating_add(
            save.endpoint
                .as_ref()
                .map_or(0, |endpoint| byte_count(endpoint.len())),
        )
        .saturating_add(credential_ref_bytes)
        .saturating_add(secret_bytes)
        .saturating_add(header_change_bytes(&save.headers))
        .saturating_add(definition_bytes)
        .saturating_add(4096)
}

/// What the header fields of a save add to its command size.
fn header_change_bytes(change: &mutations::HeaderChange) -> u64 {
    match change {
        mutations::HeaderChange::Preserve => 0,
        mutations::HeaderChange::Replace(edits) => edits.iter().fold(0, |bytes, edit| {
            let value = match &edit.value {
                mutations::HeaderEditValue::Plain(value) => value.len(),
                mutations::HeaderEditValue::Secret(payload) => payload
                    .as_ref()
                    .map_or(0, |payload| payload.secret.as_str().len()),
            };
            bytes
                .saturating_add(byte_count(edit.name.len()))
                .saturating_add(byte_count(value))
                .saturating_add(256)
        }),
    }
}

/// Reads [`filesystem_vitals`] on a blocking worker, so a slow filesystem
/// never holds an async worker.
async fn filesystem_vitals_async(directory: std::path::PathBuf) -> (Option<u64>, Option<u64>) {
    ntex::rt::spawn_blocking(move || filesystem_vitals(&directory))
        .await
        .unwrap_or((None, None))
}

fn process_memory() -> (Option<u64>, Option<u64>) {
    let mut peak = None;
    #[cfg(unix)]
    {
        let mut rusage: libc::rusage = unsafe { std::mem::zeroed() };
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut rusage) } == 0 {
            #[cfg(target_os = "macos")]
            {
                peak = Some(rusage.ru_maxrss.max(0) as u64);
            }
            #[cfg(not(target_os = "macos"))]
            {
                peak = Some((rusage.ru_maxrss.max(0) as u64).saturating_mul(1024));
            }
        }
    }

    #[allow(unused_mut)]
    let mut current = None;
    #[cfg(target_os = "linux")]
    {
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if let Some(rest) = line.strip_prefix("VmRSS:") {
                    let parts: Vec<&str> = rest.split_whitespace().collect();
                    if let Some(kb) = parts.first().and_then(|s| s.parse::<u64>().ok()) {
                        current = Some(kb.saturating_mul(1024));
                    }
                }
                if let Some(rest) = line.strip_prefix("VmHWM:") {
                    let parts: Vec<&str> = rest.split_whitespace().collect();
                    if let Some(kb) = parts.first().and_then(|s| s.parse::<u64>().ok()) {
                        peak = Some(kb.saturating_mul(1024));
                    }
                }
            }
        }
    }

    (current, peak)
}

impl DatabaseHandle {
    #[allow(dead_code)]
    pub(crate) async fn decision_availability(
        &self,
        job: JobId,
        after: Option<AvailabilityStatId>,
        limit: usize,
    ) -> Result<Vec<redb::DecisionAvailabilityRecord>, DatabaseError> {
        self.read(move |database| redb::read_decision_availability(database, job, after, limit))
            .await
    }
    async fn attachment_permit(&self) -> Result<AttachmentPermit, DatabaseError> {
        self.shared
            .attachment_slots
            .1
            .recv_async()
            .await
            .map_err(|_| DatabaseError::ShuttingDown)?;
        Ok(AttachmentPermit(self.shared.attachment_slots.0.clone()))
    }

    pub(crate) async fn terminal_jobs(
        &self,
        jobs: Vec<JobId>,
    ) -> Result<Vec<JobId>, DatabaseError> {
        self.read(move |database| redb::terminal_jobs(database, &jobs))
            .await
    }

    /// Where each of `jobs` stands toward the confirmation of its stop, as
    /// durably stored now.
    pub(crate) async fn stop_standings(
        &self,
        jobs: Vec<JobId>,
    ) -> Result<Vec<(JobId, redb::StopStanding)>, DatabaseError> {
        self.read(move |database| redb::stop_standings(database, &jobs))
            .await
    }

    /// Successful durable commits of command groups since this worker started.
    /// A flushed group counts once; refused or declined commands and failed
    /// commits do not count. Recovery and maintenance commits are excluded.
    pub fn durable_commits(&self) -> u64 {
        self.shared.durable_commits.load(Ordering::SeqCst)
    }

    pub async fn vitals(&self) -> Vitals {
        let directory = self.shared.database_directory.clone();
        let (database_bytes, available_bytes) = filesystem_vitals_async(directory).await;

        let queue_counts = match self
            .read(|database| redb::queue_counts(database, false))
            .await
        {
            Ok(counts) => (
                Some(counts.0),
                Some(counts.1),
                Some(counts.2),
                Some(counts.3),
            ),
            Err(error) => {
                log::debug!("failed to read database vitals queue counts: {error}");
                (None, None, None, None)
            }
        };

        let commands_waiting = self.sender.len() as u64;
        let (rss_bytes, peak_rss_bytes) = process_memory();

        let durable_commits = self.shared.durable_commits.load(Ordering::Relaxed);
        let commit_us_total = self.shared.commit_us_total.load(Ordering::Relaxed);
        let commit_us_max = self.shared.commit_us_max.load(Ordering::Relaxed);
        let commit_us_mean = commit_us_total.checked_div(durable_commits);

        let claims_count = self.shared.claims.load(Ordering::Relaxed);
        let claim_us_total = self.shared.claim_us_total.load(Ordering::Relaxed);
        let claim_us_max = self.shared.claim_us_max.load(Ordering::Relaxed);
        let claim_us_mean = claim_us_total.checked_div(claims_count);

        Vitals {
            database_bytes,
            available_bytes,
            durable_commits,
            commit_us_mean,
            commit_us_max,
            claims_count,
            claim_us_mean,
            claim_us_max,
            queue_pending: queue_counts.0,
            queue_ready: queue_counts.1,
            queue_delayed: queue_counts.2,
            queue_running: queue_counts.3,
            commands_waiting,
            rss_bytes,
            peak_rss_bytes,
        }
    }

    pub async fn tool_stats(
        &self,
    ) -> Result<Vec<crate::db::entities::ToolStatRecord>, DatabaseError> {
        self.read(redb::read_tool_stats).await
    }

    pub async fn usage_summary(
        &self,
        filter: crate::db::entities::UsageSummaryFilter,
        after: Option<crate::db::entities::UsageCursor>,
    ) -> Result<crate::db::entities::UsageSummaryResponse, DatabaseError> {
        self.read(move |database| redb::read_usage_summary(database, &filter, after))
            .await
    }

    pub async fn usage_buckets(
        &self,
        filter: crate::db::entities::UsageSummaryFilter,
        bucketing: crate::db::entities::UsageBucketing,
        after: Option<crate::db::entities::UsageCursor>,
    ) -> Result<crate::db::entities::UsageBucketResponse, DatabaseError> {
        self.read(move |database| redb::read_usage_buckets(database, &filter, &bucketing, after))
            .await
    }

    pub(crate) async fn usage_retries(
        &self,
        filter: crate::db::entities::UsageSummaryFilter,
        bucketing: crate::db::entities::UsageBucketing,
        after: Option<(u128, u128)>,
        limit: usize,
    ) -> Result<redb::UsageRetriesPage, DatabaseError> {
        self.read(move |database| {
            redb::read_usage_retries(database, &filter, bucketing, after, limit)
        })
        .await
    }

    pub(crate) fn take_scheduler_wake(&self) -> Option<SchedulerWakeSubscription> {
        self.shared
            .scheduler_subscription
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    pub(crate) async fn next_scheduled_delay(&self) -> Result<Option<Duration>, DatabaseError> {
        self.read(redb::next_scheduled_delay).await
    }

    pub(crate) fn subscribe_catalog(&self) -> crate::system::notification::Subscription {
        self.shared.catalog_changes.subscribe()
    }

    pub(crate) fn subscribe_observations(&self) -> crate::system::notification::Subscription {
        self.shared.observation_changes.subscribe()
    }

    pub(crate) fn observation_cursor(&self) -> crate::system::notification::JournalCursor {
        self.shared.observation_changes.cursor()
    }

    pub(crate) fn observation_changes(
        &self,
        cursor: crate::system::notification::JournalCursor,
    ) -> Result<
        Vec<(
            crate::system::notification::JournalCursor,
            ObservationChange,
        )>,
        (),
    > {
        self.shared.observation_changes.after(cursor)
    }

    pub(crate) fn subscribe_outputs(
        &self,
        job: JobId,
    ) -> crate::system::notification::Subscription {
        self.shared.output_changes.subscribe(job.get())
    }

    pub(crate) fn subscribe_logs(&self) -> crate::system::notification::Subscription {
        self.shared.log_changes.subscribe()
    }

    pub(crate) fn logs_changed(&self) {
        self.shared.log_changes.publish();
    }

    pub(crate) fn log_files(&self) -> Arc<crate::system::logger::LogFiles> {
        self.shared.log_files.clone()
    }

    pub(crate) fn cleanup_worker(&self) -> Arc<AtomicBool> {
        self.shared.cleanup_worker.clone()
    }

    pub(crate) fn data_directory(&self) -> DataDirectory {
        self.shared.data_directory.clone()
    }

    pub(crate) fn log_directory(&self) -> &std::path::Path {
        &self.shared.log_directory
    }

    pub(crate) fn logging_protection_state(&self) -> (bool, u64) {
        self.shared.protection.logging_state()
    }

    pub(crate) fn activate_filesystem_protection(&self, reason: ProtectionReason) -> bool {
        let (reply, response) = flume::bounded(1);
        if self
            .control_sender
            .send(Control::FilesystemFailure { reason, reply })
            .is_err()
        {
            return false;
        }
        response.recv().unwrap_or(false)
    }

    pub fn active_system_policy(&self) -> SystemPolicy {
        self.shared.active_policy
    }

    pub(crate) fn pause_for_update(&self) -> Result<Option<UpdatePause>, DatabaseError> {
        let _staging = self
            .shared
            .staging
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        let database = self
            .shared
            .database
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let admission = self
            .shared
            .admission
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        admitted(&admission)?;
        if self.shared.update_paused.load(Ordering::SeqCst) {
            return Err(DatabaseError::ShuttingDown);
        }
        let opened = database.as_ref().ok_or(DatabaseError::ReadsClosed)?;
        let (pending, ready, delayed, running) =
            redb::queue_counts(&opened.database, true).map_err(DatabaseError::Read)?;
        if pending != 0
            || ready != 0
            || delayed != 0
            || running != 0
            || !self.sender.is_empty()
            || self.shared.in_flight_commands.load(Ordering::SeqCst) != 0
        {
            return Ok(None);
        }
        for (job, standing) in redb::stop_standings(
            &opened.database,
            &crate::system::executor::blocking::unconfirmed_stops(),
        )
        .map_err(DatabaseError::Read)?
        {
            match standing {
                redb::StopStanding::Settled => {
                    crate::system::executor::blocking::stop_settled(job);
                }
                redb::StopStanding::Open | redb::StopStanding::Unconfirmed => {}
            }
        }
        if crate::system::executor::blocking::groups_tracked() {
            return Ok(None);
        }
        self.shared.update_paused.store(true, Ordering::SeqCst);
        Ok(Some(UpdatePause(self.shared.clone())))
    }

    async fn submit<T>(
        &self,
        command: impl FnOnce(Reply<T>) -> Command,
    ) -> Result<T, DatabaseError> {
        let (reply, response) = flume::bounded(1);
        let started = Arc::new(AtomicBool::new(false));
        let operation = {
            let admission = self
                .shared
                .admission
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            admitted(&admission)?;
            if self.shared.update_paused.load(Ordering::SeqCst) {
                return Err(DatabaseError::ShuttingDown);
            }
            let epoch = self.shared.epoch.load(Ordering::SeqCst);
            let operation = OperationId::new(
                epoch,
                self.shared.next_operation.fetch_add(1, Ordering::Relaxed),
            );
            let envelope = Envelope {
                operation,
                started: started.clone(),
                command: command(reply),
                in_flight: InFlightCommand::new(self.shared.clone()),
            };
            self.sender
                .try_send(Message::Run(Box::new(envelope)))
                .map_err(|error| match error {
                    flume::TrySendError::Full(_) => DatabaseError::QueueFull,
                    flume::TrySendError::Disconnected(_) => DatabaseError::ShuttingDown,
                })?;
            operation
        };
        // Every command that reaches the worker is answered, so a closed
        // reply channel means the worker stopped. Its execution mark tells
        // whether the command began; worker termination alone says nothing
        // about a begun command's commit.
        response.recv_async().await.map_err(|_| {
            if started.load(Ordering::SeqCst) {
                DatabaseError::OutcomeUnknown {
                    operation,
                    cause: UnknownCause::WorkerStopped,
                }
            } else {
                DatabaseError::NeverExecuted { operation }
            }
        })?
    }

    pub(super) async fn read<T: Send + 'static>(
        &self,
        read: impl FnOnce(&Database) -> Result<T, StorageError> + Send + 'static,
    ) -> Result<T, DatabaseError> {
        self.read_opened(move |opened| read(&opened.database)).await
    }

    /// Reads stay available under disk protection unless storage failed.
    async fn read_opened<T: Send + 'static>(
        &self,
        read: impl FnOnce(&Opened) -> Result<T, StorageError> + Send + 'static,
    ) -> Result<T, DatabaseError> {
        let (reply, response) = flume::bounded(1);
        let job = {
            let admission = self
                .shared
                .admission
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            admission.check_read()?;
            let stopped = self
                .shared
                .reads_stopped
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            if *stopped {
                return Err(DatabaseError::ReadPoolStopped);
            }
            let job: ReadJob = Box::new(move |opened| {
                let result = opened.and_then(|opened| read(opened).map_err(DatabaseError::Read));
                reply.send(result).ok();
            });
            ReadMessage::Run(job)
        };
        self.read_sender
            .send_async(job)
            .await
            .map_err(|_| DatabaseError::ReadPoolStopped)?;
        response
            .recv_async()
            .await
            .map_err(|_| DatabaseError::ReadInterrupted)?
    }

    /// The Jobs that this worker's start found abandoned by an earlier
    /// opening of the database, a process that died or a worker that was
    /// shut down while they ran, and settled as unknown with their stop
    /// confirmed. Empty when nothing was abandoned.
    pub fn abandoned_at_open(&self) -> &[JobId] {
        &self.shared.abandoned_at_open
    }

    /// The content files this worker's start found that committed cleanups
    /// authorized it to reclaim, those of cleaned-up Attachments whose
    /// content no available Attachment names, and what became of each:
    /// [`Reclamation::Removed`], or why it is still there. Such a file is
    /// left over by a reclamation that failed or was interrupted. Empty
    /// when there was none; a file already gone is not listed. At most 1,000
    /// details are retained; see [`Self::reclamation_omitted_at_open`].
    pub fn reclaimed_at_open(&self) -> &[(ContentDigest, Result<Reclamation, StoreError>)] {
        &self.shared.reclaimed_at_open
    }

    /// Reclamation details omitted after the first 1,000 entries at startup.
    pub fn reclamation_omitted_at_open(&self) -> u64 {
        self.shared.reclamation_omitted_at_open
    }

    pub fn status(&self) -> DatabaseStatus {
        match self.shared.admission() {
            Admission::Open => DatabaseStatus::Open,
            Admission::Protected(_) => DatabaseStatus::Protected,
            Admission::WorkerStopped => DatabaseStatus::WorkerStopped,
            Admission::ShuttingDown => {
                if self.shared.released.load(Ordering::SeqCst) {
                    DatabaseStatus::Closed
                } else {
                    DatabaseStatus::ShuttingDown
                }
            }
        }
    }

    /// The operation epoch of the current opening. A recovery that reopens
    /// the database moves it forward.
    pub fn epoch(&self) -> u64 {
        self.shared.epoch.load(Ordering::SeqCst)
    }

    /// The current disk protection status. Never waits for storage.
    pub fn disk_protection(&self) -> DiskProtectionStatus {
        self.shared.protection.status()
    }

    /// Notifies of every later disk protection change until the database
    /// worker stops.
    pub fn watch_disk_protection(&self) -> ProtectionWatch {
        self.shared.protection.watch()
    }

    /// Compacts the database file when the policy says it is worth it: the
    /// file is large, a large share of it is free, and no attempt was made
    /// recently. Otherwise reports why it did not. Nothing calls this
    /// automatically, because redb's compaction cannot be interrupted and
    /// holds off every read and command for as long as it runs; the caller
    /// chooses a quiet moment. Commands and reads that arrive meanwhile wait
    /// in their queues and fail with a full-queue error beyond their limits,
    /// and [`DatabaseWorker::shutdown`] waits for it.
    ///
    /// Every earlier command is already durable and acknowledged by the time
    /// it runs, and a failed compaction leaves the file consistent.
    pub async fn compact_storage(&self) -> Result<CompactionOutcome, MaintenanceError> {
        let (reply, response) = flume::bounded(1);
        {
            let admission = self
                .shared
                .admission
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            admission.check_maintenance()?;
            self.control_sender
                .try_send(Control::CompactStorage(reply))
                .map_err(|error| match error {
                    flume::TrySendError::Full(_) => MaintenanceError::ControlQueueFull,
                    flume::TrySendError::Disconnected(_) => MaintenanceError::WorkerStopped,
                })?;
        }
        response
            .recv_async()
            .await
            .unwrap_or(Err(MaintenanceError::WorkerStopped))
    }

    /// Clears disk protection once every protected filesystem has the resume
    /// threshold available, a failed database has been released and
    /// reopened, and every operation whose outcome was unknown has been
    /// reconciled against its marker. Otherwise the lock stays and the
    /// failed step is returned. Concurrent requests share one attempt.
    ///
    /// Commands refused while locked are not replayed, operations with an
    /// unknown outcome are not retried, and no Job is resumed.
    pub async fn recover_storage(&self) -> Result<RecoveryReport, RecoveryError> {
        let response = self
            .shared
            .recovery
            .join(|| self.request(Control::Recover))?;
        response
            .recv_async()
            .await
            .unwrap_or(Err(RecoveryError::WorkerStopped))
    }

    /// Queues a control request unless the worker is stopping or stopped.
    /// Callers hold the request's lock, and the worker closes admission
    /// before answering pending requests on exit, so none is left unanswered.
    fn request(&self, control: Control) -> Result<(), RecoveryError> {
        let admission = self
            .shared
            .admission
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        admission.check_recovery_enqueue()?;
        self.control_sender
            .try_send(control)
            .map_err(|error| match error {
                flume::TrySendError::Full(_) => RecoveryError::ControlQueueFull,
                flume::TrySendError::Disconnected(_) => RecoveryError::WorkerStopped,
            })
    }

    /// The instance's Genesis Job: the root of every parent chain, and the
    /// parent to name for top-level work.
    pub fn genesis(&self) -> JobId {
        self.shared.genesis
    }

    pub(crate) async fn job_execution(
        &self,
        turn: JobId,
    ) -> Result<
        (
            crate::db::entities::FrozenExecutionConfig,
            Option<ProjectId>,
            Option<ConfigId>,
        ),
        DatabaseError,
    > {
        self.read(move |database| redb::read_turn_execution(database, turn))
            .await
    }
    pub(crate) async fn read_turn_context(
        &self,
        turn: JobId,
        budget: usize,
    ) -> Result<redb::TurnContext, DatabaseError> {
        self.read(move |database| redb::read_turn_context(database, turn, budget))
            .await
    }
    pub async fn conversation_children(
        &self,
        conversation: JobId,
        before: Option<JobId>,
        limit: usize,
    ) -> Result<Vec<redb::ChildConversationEntry>, DatabaseError> {
        self.read(move |database| {
            redb::read_conversation_children(database, conversation, before, limit)
        })
        .await
    }

    pub async fn group_limits(
        &self,
        conversation: JobId,
    ) -> Result<Option<(crate::db::entities::JobLimits, u8)>, DatabaseError> {
        self.read(move |database| redb::read_conversation_limits(database, conversation))
            .await
    }
    pub async fn conversation_turns(
        &self,
        conversation: JobId,
        before: Option<JobId>,
        limit: usize,
    ) -> Result<Vec<JobRecord>, DatabaseError> {
        self.read(move |database| {
            redb::read_conversation_turns(database, conversation, before, limit)
        })
        .await
    }
    pub(crate) async fn mutable_conversation_turn_page(
        &self,
        conversation: JobId,
        before: Option<JobId>,
        limit: usize,
    ) -> Result<Option<redb::MutableConversationPage>, DatabaseError> {
        self.read(move |database| {
            redb::read_mutable_conversation_turn_page(database, conversation, before, limit)
        })
        .await
    }

    pub(crate) async fn mutable_job_details(
        &self,
        job: JobId,
        context: Option<entities::ComposerContextId>,
    ) -> Result<redb::MutableJobDetailsLookup, DatabaseError> {
        self.read(move |database| redb::read_mutable_job_details(database, job, context))
            .await
    }
    pub async fn notices(
        &self,
        conversation: JobId,
        after: u32,
        limit: usize,
    ) -> Result<Vec<(u32, crate::db::entities::Notice)>, DatabaseError> {
        self.read(move |database| redb::read_notices(database, conversation, after, limit))
            .await
    }

    pub async fn job(&self, job: JobId) -> Result<Option<JobRecord>, DatabaseError> {
        self.read(move |database| redb::read_job(database, job))
            .await
    }

    /// Newest Jobs first, strictly before `before` when given; at most
    /// 1,000 per call.
    pub async fn jobs(
        &self,
        before: Option<JobId>,
        limit: usize,
    ) -> Result<Vec<JobRecord>, DatabaseError> {
        self.read(move |database| redb::read_jobs(database, before, limit))
            .await
    }

    /// One submitted Job with its Allocation evidence, or none.
    pub(crate) async fn native_job(
        &self,
        job: JobId,
    ) -> Result<Option<redb::NativeJobItem>, DatabaseError> {
        self.read(move |database| redb::read_native_job(database, job))
            .await
    }

    /// Newest submitted Jobs whose identity or input text contains every term,
    /// with cursor pagination.
    pub(crate) async fn search_jobs(
        &self,
        terms: Vec<String>,
        before: Option<JobId>,
        limit: usize,
    ) -> Result<redb::JobSearchPage, DatabaseError> {
        self.read(move |database| redb::read_job_search(database, &terms, before, limit))
            .await
    }

    /// One page of conversations by immutable conversation ID, newest first.
    pub async fn conversations(
        &self,
        before: Option<JobId>,
        limit: usize,
    ) -> Result<redb::ConversationsPage, DatabaseError> {
        self.read(move |database| redb::read_conversations(database, before, limit))
            .await
    }

    /// Conversations by immutable ID, newest first, whose title or message and result text holds
    /// every term. `terms` are already lowercased query terms. `before` and
    /// `limit` page like [`Self::conversations`].
    pub async fn search_conversations(
        &self,
        terms: Vec<String>,
        before: Option<JobId>,
        limit: usize,
    ) -> Result<redb::ConversationsPage, DatabaseError> {
        self.read(move |database| redb::read_conversation_search(database, &terms, before, limit))
            .await
    }

    pub async fn claim_execution(
        &self,
        request: ClaimRequest,
    ) -> Result<Result<ClaimGrant, ClaimRefusal>, DatabaseError> {
        self.submit(|reply| {
            Command::custom(
                (request,),
                reply,
                |(request,)| {
                    byte_count(request.allowed_pools.len())
                        .saturating_mul(8)
                        .saturating_add(4096 * 512)
                },
                |(request,), reply, database, operation, commit| {
                    use OperationKind as Kind;
                    {
                        let mut refusal = ClaimRefusal::RecipeInvalid;
                        let outcome = redb::run_batch_declinable(
                            database,
                            operation.epoch,
                            commit,
                            |batch| match mutations::claim_execution(batch, request)? {
                                Ok(grant) => Ok(Some(grant)),
                                Err(reason) => {
                                    refusal = reason;
                                    Ok(None)
                                }
                            },
                        );
                        let declined = matches!(outcome.result, Ok(None));
                        let outcome = redb::BatchOutcome {
                            result: outcome.result.map(|grant| match grant {
                                Some(grant) => Ok(grant),
                                None => Err(refusal),
                            }),
                            storage_failure: outcome.storage_failure,
                            durable_commit_us: outcome.durable_commit_us,
                            scheduling_wake: outcome.scheduling_wake,
                            capacity_freed: outcome.capacity_freed,
                            catalog_changed: outcome.catalog_changed,
                            probe: outcome.probe,
                        };
                        settle(
                            operation,
                            commit,
                            Kind::ClaimExecution,
                            reply,
                            outcome,
                            declined,
                        )
                    }
                },
            )
        })
        .await
    }

    /// CLAIM_BATCH (section 8.3): one write transaction that promotes the due
    /// Jobs and claims ready Jobs, smallest Job id first, while a global slot
    /// and a compatible Pool with room are free. The Jobs it returns are
    /// claimed once this returns; execution starts only after the commit.
    pub async fn claim_batch(&self) -> Result<Vec<JobId>, DatabaseError> {
        let started = Instant::now();
        let jobs = self
            .submit(|reply| {
                Command::new(
                    (),
                    reply,
                    |()| 2 * 4096 * 512,
                    OperationKind::ClaimExecution,
                    |(), batch| batch.claim_batch(),
                )
            })
            .await?;
        if !jobs.is_empty() {
            let duration_us = started.elapsed().as_micros() as u64;
            self.shared.claims.fetch_add(1, Ordering::Relaxed);
            self.shared
                .claim_us_total
                .fetch_add(duration_us, Ordering::Relaxed);
            self.shared
                .claim_us_max
                .fetch_max(duration_us, Ordering::Relaxed);
        }
        Ok(jobs)
    }

    pub async fn frozen_pool_binding(
        &self,
        job: JobId,
    ) -> Result<Option<FrozenPoolBinding>, DatabaseError> {
        self.read(move |database| redb::read_pool_binding(database, job))
            .await
    }

    /// Every execution whose external work OCG never credibly confirmed
    /// stopped, and the capacity each one still holds. Read-only and never a
    /// route to release any of it: see
    /// [`mutations::confirm_job_stopped`](crate::db::mutations::confirm_job_stopped).
    pub async fn unresolved_executions(
        &self,
    ) -> Result<Vec<(JobId, crate::db::redb::UnresolvedExecution)>, DatabaseError> {
        self.read(redb::read_unresolved_executions).await
    }
    pub async fn occupancy(&self) -> Result<Vec<(ExecutionPoolId, JobId)>, DatabaseError> {
        self.read(redb::read_occupancy).await
    }

    /// A Job's ordinary children in ID order, strictly after `after` when
    /// given; at most 1,000 per call. The Genesis Job is not its own child.
    pub async fn children(
        &self,
        parent: JobId,
        after: Option<JobId>,
        limit: usize,
    ) -> Result<Vec<JobId>, DatabaseError> {
        self.read(move |database| redb::read_children(database, parent, after, limit))
            .await
    }

    /// The tree of the work below a Job: the ancestors above it and the
    /// nodes below it, the root's own children in one page. `None` when the
    /// root has no Job of its own.
    pub async fn job_tree(
        &self,
        job: JobId,
        after: Option<JobId>,
        limit: usize,
        depth: u8,
    ) -> Result<Option<crate::db::redb::JobTree>, DatabaseError> {
        self.read(move |database| redb::read_job_tree(database, job, after, limit, depth))
            .await
    }

    /// What a submitted LLM execution child was configured to execute, as
    /// frozen in its input, with the Profiles it was submitted with as
    /// provenance. Later Profile edits do not change it. `None` for a Job
    /// submitted without a configuration.
    pub(crate) async fn job_scope(
        &self,
        job: JobId,
    ) -> Result<Option<redb::JobScope>, DatabaseError> {
        self.read(move |database| redb::read_job_scope(database, job))
            .await
    }

    pub async fn job_configuration(
        &self,
        job: JobId,
    ) -> Result<Option<entities::SubmittedConfiguration>, DatabaseError> {
        self.read(move |database| redb::read_job_configuration(database, job))
            .await
    }

    /// The typed work of a decision turn, or `None` for every other Job. A
    /// caller that receives `Some` must run the decision runtime, not the
    /// generative one.
    pub async fn decision_work(
        &self,
        job: JobId,
    ) -> Result<Option<entities::DecisionWork>, DatabaseError> {
        self.read(move |database| redb::read_decision_work(database, job))
            .await
    }

    /// The Executors that support the transport of an LLM Profile's Provider
    /// as configured now, oldest first, at most 1,000. This lists candidates
    /// for new work only; it neither ranks nor reserves them, and running on
    /// any of them never changes the Provider, model or parameters.
    pub async fn compatible_executors(
        &self,
        llm_profile: crate::db::entities::ProfileId,
    ) -> Result<Vec<ExecutorId>, DatabaseError> {
        self.read(move |database| redb::read_compatible_executors(database, llm_profile))
            .await
    }

    pub(crate) async fn candidate_compatible(
        &self,
        candidate: AdmissionCandidate,
    ) -> Result<bool, DatabaseError> {
        self.read(move |database| redb::read_candidate_compatible(database, candidate))
            .await
    }

    /// The Executors that offer typed decision work, oldest first, at most
    /// 1,000. These are the candidates for new decision Jobs; running on any
    /// of them never changes the Decision Model or its request.
    pub async fn decision_executors(&self) -> Result<Vec<ExecutorId>, DatabaseError> {
        self.read(redb::read_decision_executors).await
    }

    /// The Jobs that explicitly retry `original`, oldest first.
    pub async fn retries(
        &self,
        original: JobId,
        after: Option<JobId>,
        limit: usize,
    ) -> Result<redb::CursorPage<JobId, JobId>, DatabaseError> {
        self.read(move |database| redb::read_retries(database, original, after, limit))
            .await
    }

    /// A Job's outputs in seq order, strictly after `after_seq` when given;
    /// at most 1,000 per call.
    pub async fn job_outputs(
        &self,
        job: JobId,
        after_seq: Option<i64>,
        limit: usize,
    ) -> Result<Vec<JobOutputRecord>, DatabaseError> {
        self.read(move |database| redb::read_outputs(database, job, after_seq, limit))
            .await
    }

    pub async fn latest_output(
        &self,
        job: JobId,
    ) -> Result<Option<JobOutputRecord>, DatabaseError> {
        self.read(move |database| redb::read_latest_output(database, job))
            .await
    }

    pub async fn job_input(
        &self,
        id: JobInputId,
    ) -> Result<Option<entities::JobInputRecord>, DatabaseError> {
        self.read(move |database| redb::read_job_input(database, id))
            .await
    }

    pub async fn job_input_for_job(
        &self,
        job: JobId,
    ) -> Result<Option<entities::JobInputRecord>, DatabaseError> {
        self.read(move |database| redb::read_job_input_for_job(database, job))
            .await
    }

    pub async fn executor(
        &self,
        id: ExecutorId,
    ) -> Result<Option<entities::ExecutorRecord>, DatabaseError> {
        self.read(move |database| redb::read_executor(database, id))
            .await
    }

    /// Executors in reverse ID order, newest first. Strictly before `before` when given;
    /// at most 1,000 per call.
    pub async fn executors(
        &self,
        before: Option<ExecutorId>,
        limit: usize,
    ) -> Result<Vec<entities::ExecutorRecord>, DatabaseError> {
        self.read(move |database| redb::read_executors(database, before, limit))
            .await
    }

    pub async fn project(&self, id: ProjectId) -> Result<Option<ProjectRecord>, DatabaseError> {
        self.read(move |database| redb::read_project(database, id))
            .await
    }

    /// Projects newest first, strictly before `before` when given; at most
    /// 1,000 per call.
    pub async fn projects(
        &self,
        before: Option<ProjectId>,
        limit: usize,
    ) -> Result<Vec<ProjectRecord>, DatabaseError> {
        self.read(move |database| redb::read_projects(database, before, limit))
            .await
    }

    /// A Job's Metrics in recording order, strictly after `after` when
    /// given; at most 1,000 per call.
    pub async fn job_metrics(
        &self,
        job: JobId,
        after: Option<MetricsId>,
        limit: usize,
    ) -> Result<Vec<MetricsRecord>, DatabaseError> {
        self.read(move |database| redb::read_job_metrics(database, job, after, limit))
            .await
    }

    /// A Job's AvailabilityStats in recording order, strictly after `after`
    /// when given; at most 1,000 per call.
    pub async fn job_availability(
        &self,
        job: JobId,
        after: Option<AvailabilityStatId>,
        limit: usize,
    ) -> Result<Vec<AvailabilityStatRecord>, DatabaseError> {
        self.read(move |database| redb::read_job_availability(database, job, after, limit))
            .await
    }

    /// One page of the Metrics `filter` selects, in observation order
    /// (`observed_at`, then record ID), strictly after `after` when given.
    /// Pass the returned `next` back as `after` to continue; a page can be
    /// empty while `next` is set. At most `limit` records (clamped to
    /// 1..=1,000) per page, and at most [`MAX_OBSERVATION_SCAN`] index
    /// entries read, matching or not. Each record names the target its Job
    /// froze, not a Profile's current values.
    pub async fn metrics(
        &self,
        filter: ObservationFilter,
        after: Option<ObservationCursor>,
        limit: usize,
    ) -> Result<ObservationPage<MetricsRecord>, DatabaseError> {
        self.read(move |database| {
            redb::read_metrics(database, &filter, after, limit, MAX_OBSERVATION_SCAN)
        })
        .await
    }

    /// One page of the AvailabilityStats `filter` selects, paged like
    /// [`Self::metrics`]. These are raw observations; no availability
    /// verdict or rate is derived from them here.
    pub async fn availability_stats(
        &self,
        filter: ObservationFilter,
        after: Option<ObservationCursor>,
        limit: usize,
    ) -> Result<ObservationPage<AvailabilityStatRecord>, DatabaseError> {
        self.read(move |database| {
            redb::read_availability_stats(database, &filter, after, limit, MAX_OBSERVATION_SCAN)
        })
        .await
    }

    /// The submission recorded under `key`: the Job it created and the
    /// fingerprint of its request. `None` when no committed Job was created
    /// with it. A read never sees a creation whose commit is not durable.
    pub async fn submission(
        &self,
        key: SubmissionKey,
    ) -> Result<Option<SubmissionRecord>, DatabaseError> {
        self.read(move |database| redb::read_submission(database, key))
            .await
    }

    /// Resolves a command reported as [`DatabaseError::OutcomeUnknown`] by
    /// reading the natural key it writes (section 11.2). Call it after
    /// reopening: for an operation of an earlier opening, a missing key proves
    /// the command did not commit. A command that writes no key to read is
    /// [`OperationStatus::Unresolvable`].
    pub async fn reconcile_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationStatus, DatabaseError> {
        if operation.epoch() > self.epoch() {
            return Err(DatabaseError::UnknownOperation(operation));
        }
        let probe = self
            .shared
            .probes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&operation)
            .copied()
            .flatten();
        self.read_opened(move |opened| {
            errors::operation_status(&opened.database, operation, probe, opened.epoch)
        })
        .await
    }

    /// Manually select and reserve one development-model Job. This does not
    /// start execution. `New` is only for an identity never attempted before.
    /// After an unknown outcome, callers must use `Replay`, which never creates
    /// an Allocation when attribution is unresolved. Request IDs are not
    /// authorization credentials; this interface is restricted to the trusted
    /// internal allocator.
    #[allow(dead_code)]
    pub(crate) async fn reserve_next_development_job(
        &self,
        request: entities::DevelopmentReservationRequest,
        attempt: entities::DevelopmentReservationAttempt,
    ) -> Result<entities::DevelopmentReservation, DevelopmentReservationError> {
        self.reserve_development_job(request, attempt, entities::ReadySelection::Any)
            .await
    }

    /// Like [`Self::reserve_next_development_job`]; with
    /// [`entities::ReadySelection::Only`], a ready entry of any other Job is
    /// never reserved, so a manual trial reserves the one Job it names or
    /// none. The selection is not stored: the Allocation is the same row
    /// either way.
    #[allow(dead_code)]
    pub(crate) async fn reserve_development_job(
        &self,
        request: entities::DevelopmentReservationRequest,
        attempt: entities::DevelopmentReservationAttempt,
        selection: entities::ReadySelection,
    ) -> Result<entities::DevelopmentReservation, DevelopmentReservationError> {
        let fresh = match attempt {
            entities::DevelopmentReservationAttempt::New(fresh) => {
                if !fresh.matches(request) {
                    return Err(DevelopmentReservationError::DefinitelyNotCommitted {
                        error: Box::new(DatabaseError::Rejected(Violation::InvalidValue(
                            "reservation_request",
                            "first-attempt permit has different parameters",
                        ))),
                        fresh,
                    });
                }
                Some(fresh)
            }
            entities::DevelopmentReservationAttempt::Replay => None,
        };
        let is_new = fresh.is_some();
        let result = self
            .submit(|reply| {
                Command::new(
                    (request, is_new, selection),
                    reply,
                    |_| 4096,
                    OperationKind::AssignJobExecutor,
                    |(request, is_new, selection), batch| {
                        batch.reserve_next_development_job(request, is_new, selection)
                    },
                )
            })
            .await;
        match (result, fresh) {
            (Ok(result), _) => Ok(result),
            (Err(error), Some(fresh))
                if matches!(
                    error.command_outcome(),
                    Some(CommandOutcome::NotAccepted | CommandOutcome::NotCommitted)
                ) =>
            {
                Err(DevelopmentReservationError::DefinitelyNotCommitted {
                    error: Box::new(error),
                    fresh,
                })
            }
            (Err(error), _) => Err(DevelopmentReservationError::Other(error)),
        }
    }

    /// One reservation pass of the native scheduler, under
    /// [`entities::ReadySelection::SingleRequestGenerative`]: the selection
    /// and the reservation are one writer transaction, as for every other
    /// pass. A pass that reserves nothing declines to commit, so it neither
    /// writes nor wakes the scheduler that asked for it; only a reservation
    /// commits. A first attempt only: a scheduler never replays a request to
    /// adopt the Allocation an uncertain reply may have left.
    pub(crate) async fn reserve_scheduled_job(
        &self,
        fresh: entities::DevelopmentFreshReservation,
        policy: fn(&entities::FrozenExecutionConfig) -> bool,
    ) -> Result<entities::DevelopmentReservation, DevelopmentReservationError> {
        let request = fresh.request();
        let result = self
            .submit(|reply| {
                Command::custom(
                    (request, policy),
                    reply,
                    |_| 4096,
                    |(request, policy), reply, database, operation, commit| {
                        let mut passed = entities::DevelopmentReservation::Exhausted;
                        let outcome = redb::run_batch_declinable(
                            database,
                            operation.epoch,
                            commit,
                            |batch| match batch.reserve_next_development_job(
                                request,
                                true,
                                entities::ReadySelection::SingleRequestGenerative { policy },
                            )? {
                                reserved @ entities::DevelopmentReservation::Reserved(_) => {
                                    Ok(Some(reserved))
                                }
                                other => {
                                    passed = other;
                                    Ok(None)
                                }
                            },
                        );
                        let declined = matches!(outcome.result, Ok(None));
                        let outcome = redb::BatchOutcome {
                            result: outcome.result.map(|reserved| reserved.unwrap_or(passed)),
                            storage_failure: outcome.storage_failure,
                            durable_commit_us: outcome.durable_commit_us,
                            scheduling_wake: outcome.scheduling_wake,
                            capacity_freed: outcome.capacity_freed,
                            catalog_changed: outcome.catalog_changed,
                            probe: outcome.probe,
                        };
                        settle(
                            operation,
                            commit,
                            OperationKind::AssignJobExecutor,
                            reply,
                            outcome,
                            declined,
                        )
                    },
                )
            })
            .await;
        match result {
            Ok(result) => Ok(result),
            Err(error)
                if matches!(
                    error.command_outcome(),
                    Some(CommandOutcome::NotAccepted | CommandOutcome::NotCommitted)
                ) =>
            {
                Err(DevelopmentReservationError::DefinitelyNotCommitted {
                    error: Box::new(error),
                    fresh,
                })
            }
            Err(error) => Err(DevelopmentReservationError::Other(error)),
        }
    }

    /// Records the stop of one native Job of `context` (a Job of another
    /// context is not found); see [`redb::Batch::request_native_stop`]. A
    /// stop already requested writes nothing. A reply lost after commit is
    /// `OutcomeUnknown`: read the Job before asking again.
    pub(crate) async fn request_native_stop(
        &self,
        context: entities::ComposerContextId,
        job: entities::JobId,
    ) -> Result<entities::NativeStopRequest, DatabaseError> {
        self.submit(|reply| {
            Command::custom(
                (context, job),
                reply,
                |_| 8192,
                |(context, job), reply, database, operation, commit| {
                    let mut repeated = None;
                    let outcome =
                        redb::run_batch_declinable(database, operation.epoch, commit, |batch| {
                            batch.require_mutable_job_context(context, job)?;
                            match batch.request_native_stop(job)? {
                                entities::NativeStopRequest::AlreadyRequested(token) => {
                                    repeated = Some(token);
                                    Ok(None)
                                }
                                written => Ok(Some(written)),
                            }
                        });
                    let declined = matches!(outcome.result, Ok(None));
                    let outcome = redb::BatchOutcome {
                        result: outcome.result.and_then(|written| {
                            written
                                .or(repeated.map(entities::NativeStopRequest::AlreadyRequested))
                                .ok_or(redb::BatchFailure::Rejected(Violation::InvalidValue(
                                    "stop",
                                    "the stop had no outcome",
                                )))
                        }),
                        storage_failure: outcome.storage_failure,
                        durable_commit_us: outcome.durable_commit_us,
                        scheduling_wake: outcome.scheduling_wake,
                        capacity_freed: outcome.capacity_freed,
                        catalog_changed: outcome.catalog_changed,
                        probe: outcome.probe,
                    };
                    settle(
                        operation,
                        commit,
                        OperationKind::StopTurn,
                        reply,
                        outcome,
                        declined,
                    )
                },
            )
        })
        .await
    }

    /// Read the Executor's durable owner after a lost reply. This is a writer
    /// barrier for commands already enqueued; concurrent callers can still own
    /// the returned allocation. Use operation reconciliation when an operation
    /// ID is available. If storage is unavailable, do not retry until resolved.
    #[allow(dead_code)]
    pub(crate) async fn development_executor_allocation(
        &self,
        executor: entities::ExecutorId,
    ) -> Result<Option<entities::PoolAllocation>, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (executor,),
                reply,
                |_| 0,
                OperationKind::Open,
                |(executor,), batch| batch.development_executor_allocation(executor),
            )
        })
        .await
    }

    /// The stored Job a manual trial names and the Allocation it already has,
    /// if any. Evidence only.
    #[allow(dead_code)]
    pub(crate) async fn development_trial_job(
        &self,
        job: entities::JobId,
    ) -> Result<Option<(entities::MutableJobRecord, Option<entities::PoolAllocation>)>, DatabaseError>
    {
        self.read(move |database| redb::read_trial_job(database, job))
            .await
    }

    /// Builds the request of a resolved candidate and reads the stored
    /// credential rows it names, each once. Reads only.
    async fn prepare_with_credentials(
        &self,
        resolved: redb::DevelopmentResolvedCandidate,
    ) -> Result<PreparedDevelopmentHandoff, DevelopmentHandoffError> {
        let mut prepared = prepare_development_handoff(resolved, self.shared.active_policy)
            .map_err(classify_unstarted)?;
        let references: Vec<String> = prepared
            .expected_credential_references()
            .into_iter()
            .map(str::to_owned)
            .collect();
        for reference in references {
            if !redb::valid_credential_ref(&reference) {
                return Err(classify_unstarted(DatabaseError::Rejected(
                    Violation::InvalidValue("credential", "invalid credential reference"),
                )));
            }
            let key = self.shared.credential_key();
            let lookup = reference.clone();
            let resolved = self
                .read(move |database| {
                    redb::read_handoff_credential(database, &lookup, key.as_deref())
                })
                .await
                .map_err(|error| match &error {
                    // A stored secret that cannot be read or decrypted is the
                    // same fact to the Job as one that was deleted.
                    DatabaseError::Read(storage)
                        if storage.kind == StorageErrorKind::Other
                            && storage.os_error.is_none() =>
                    {
                        DevelopmentHandoffError::NotStarted {
                            cause: entities::UnstartedCause::FrozenSecretUnavailable,
                            error: Box::new(error),
                        }
                    }
                    _ => classify_unstarted(error),
                })?;
            let Some((secret, encrypted_row)) = resolved else {
                return Err(classify_unstarted(DatabaseError::Rejected(
                    Violation::InvalidValue("credential", "stored credential is unavailable"),
                )));
            };
            prepared.credentials.push(reference, encrypted_row, secret);
        }
        if prepared.system_policy != self.shared.active_policy {
            return Err(classify_unstarted(DatabaseError::Rejected(
                Violation::InvalidValue("handoff", "system policy changed"),
            )));
        }
        Ok(prepared)
    }

    /// Builds, without reserving anything, the exact request a start of `job`
    /// on `executor` would prepare, and lets `guard` look at it. Reads only:
    /// nothing is reserved, started, recorded or sent, and the prepared request
    /// and the credential values it read are dropped when this returns, so a
    /// refusal leaves the Job exactly as submitted.
    ///
    /// This is a preflight, not an authorization. The start transaction
    /// revalidates every fact against the real reservation, and a Job that
    /// changes in between is refused there.
    #[allow(dead_code)]
    pub(crate) async fn preview_development_handoff(
        &self,
        job: entities::JobId,
        pool: entities::ExecutionPoolId,
        executor: entities::ExecutorId,
        guard: impl FnOnce(&PreparedDevelopmentHandoff) -> Result<(), &'static str>,
    ) -> Result<(), DevelopmentPreviewError> {
        let snapshot = self
            .read(move |database| {
                redb::read_development_execution_preview(database, job, pool, executor)
            })
            .await
            .map_err(DevelopmentPreviewError::Unavailable)?;
        let resolved = self
            .resolve_development_candidate(snapshot)
            .await
            .map_err(DevelopmentPreviewError::Unavailable)?;
        let prepared =
            self.prepare_with_credentials(resolved)
                .await
                .map_err(|error| match error {
                    DevelopmentHandoffError::NotStarted { error, .. } => {
                        DevelopmentPreviewError::Unavailable(*error)
                    }
                    DevelopmentHandoffError::Database(error) => {
                        DevelopmentPreviewError::Unavailable(*error)
                    }
                    DevelopmentHandoffError::OutcomeUnknown { error, .. } => {
                        DevelopmentPreviewError::Unavailable(*error)
                    }
                })?;
        guard(&prepared).map_err(DevelopmentPreviewError::Refused)
    }

    /// Prepare locally, then return the single-use result only after the Worker
    /// confirms the checked start transaction is durable. This does not dispatch.
    #[allow(dead_code)]
    pub(crate) async fn checked_development_handoff(
        &self,
        token: entities::AllocationToken,
    ) -> Result<DevelopmentHandoff, DevelopmentHandoffError> {
        self.checked_development_handoff_guarded(token, |_| Ok(()))
            .await
    }

    /// Like [`Self::checked_development_handoff`], and `guard` sees the exact
    /// prepared request before any start is submitted. A refusal is a
    /// definite pre-start failure: no authority exists and the Allocation is
    /// still only Reserved.
    #[allow(dead_code)]
    pub(crate) async fn checked_development_handoff_guarded(
        &self,
        token: entities::AllocationToken,
        guard: impl FnOnce(&PreparedDevelopmentHandoff) -> Result<(), &'static str>,
    ) -> Result<DevelopmentHandoff, DevelopmentHandoffError> {
        let resolved = self
            .development_execution_context(token)
            .await
            .map_err(classify_unstarted)?;
        let prepared = self.prepare_with_credentials(resolved).await?;
        if let Err(reason) = guard(&prepared) {
            return Err(classify_unstarted(DatabaseError::Rejected(
                Violation::InvalidValue("handoff", reason),
            )));
        }
        let owner = self.shared.clone();
        let result = self
            .submit(|reply| {
                Command::new(
                    (prepared, owner),
                    reply,
                    |(prepared, _)| {
                        byte_count(
                            crate::db::serialized_len(&prepared.request).unwrap_or(usize::MAX),
                        )
                        .saturating_add(64 * 1024)
                    },
                    OperationKind::ClaimExecution,
                    |(prepared, owner), batch| {
                        let started = batch.start_mutable_job(&prepared)?;
                        Ok(DevelopmentHandoff {
                            prepared,
                            started,
                            owner,
                            output_busy: AtomicBool::new(false),
                            output_uncertain: AtomicBool::new(false),
                            usage_checkpoint: std::sync::Mutex::new(
                                DevelopmentUsageCheckpoint::default(),
                            ),
                        })
                    },
                )
            })
            .await;
        match result {
            Ok(handoff) => Ok(handoff),
            Err(error) => match &error {
                DatabaseError::OutcomeUnknown { operation, .. } => {
                    let reconciliation =
                        self.reconcile_operation(*operation).await.map_err(Box::new);
                    Err(DevelopmentHandoffError::OutcomeUnknown {
                        error: Box::new(error),
                        reconciliation,
                    })
                }
                _ => Err(classify_unstarted(error)),
            },
        }
    }

    /// A sequence is a caller-supplied output identity, not execution authority.
    /// An uncertain result requires read-only reconciliation; do not retry blindly.
    #[allow(dead_code)]
    pub(crate) async fn append_development_output(
        &self,
        handoff: &DevelopmentHandoff,
        expected_seq: u32,
        output: JobOutput,
    ) -> Result<AppendedOutput, DatabaseError> {
        if !Arc::ptr_eq(&self.shared, &handoff.owner) {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "handoff",
                "different database worker",
            )));
        }
        if handoff.output_uncertain.load(Ordering::SeqCst)
            || handoff
                .output_busy
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
        {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "output",
                "output write is pending or requires reconciliation",
            )));
        }
        let mut guard = DevelopmentOutputGuard {
            handoff,
            known: false,
        };
        if handoff.output_uncertain.load(Ordering::SeqCst) {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "output",
                "output write requires reconciliation",
            )));
        }
        let token = match &handoff.prepared.candidate {
            redb::DevelopmentCandidate::Generative(context) => context.allocation.token,
            redb::DevelopmentCandidate::Decision(_) => {
                return Err(DatabaseError::Rejected(Violation::InvalidValue(
                    "handoff",
                    "generative handoff required",
                )));
            }
        };
        let proof = DevelopmentWriteProof {
            token,
            revision: handoff.started.revision,
        };
        let result = self
            .submit(|reply| {
                Command::custom(
                    (proof, expected_seq, output),
                    reply,
                    |(_, _, output)| {
                        output_bytes(
                            output.summary.as_deref(),
                            output.code.as_deref(),
                            output.value.as_ref(),
                        )
                    },
                    |(proof, expected_seq, output), reply, database, operation, commit| {
                        let mut result = run(
                            database,
                            operation,
                            commit,
                            OperationKind::AppendJobOutput,
                            reply,
                            |batch| batch.append_development_output(&proof, expected_seq, output),
                        );
                        if result.committed {
                            result.output_changed = Some(proof.token.job);
                        }
                        result
                    },
                )
            })
            .await;
        guard.known = !matches!(result, Err(DatabaseError::OutcomeUnknown { .. }));
        result
    }

    /// Record evidence of the first provider request, using the exact body
    /// prepared by the checked handoff. This method does not send a request.
    #[allow(dead_code)]
    pub(crate) async fn record_development_sent_request(
        &self,
        handoff: &DevelopmentHandoff,
        observed_at: entities::Timestamp,
    ) -> Result<(), DatabaseError> {
        if !Arc::ptr_eq(&self.shared, &handoff.owner) {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "handoff",
                "different database worker",
            )));
        }
        let (token, decision) = match &handoff.prepared.candidate {
            redb::DevelopmentCandidate::Generative(context) => (context.allocation.token, false),
            redb::DevelopmentCandidate::Decision(context) => (context.allocation.token, true),
        };
        if handoff
            .prepared
            .credentials
            .spellings
            .as_ref()
            .is_some_and(|secret| development_contains_secret(&handoff.prepared.request, secret))
        {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "request",
                "request evidence contains credential material",
            )));
        }
        let mut sent = if decision {
            let body = entities::MessageBody::new(
                &serde_json::json!({"decision": &handoff.prepared.request}),
            )
            .map_err(|_| {
                DatabaseError::Rejected(Violation::InvalidValue(
                    "request",
                    "prepared decision cannot be recorded",
                ))
            })?;
            let digest = body.digest();
            entities::SentRequest {
                job: token.job,
                request: RequestNumber::FIRST,
                observed_at,
                messages: vec![digest],
                tools: None,
                parameters: digest,
                dropped_turns: None,
                bodies: vec![body],
                required_messages: Vec::new(),
            }
        } else {
            let mut recorder = crate::service::ai::providers::RequestRecorder::default();
            recorder.image_attachments = handoff
                .prepared
                .conversation
                .as_ref()
                .ok_or(DatabaseError::Rejected(Violation::InvalidValue(
                    "request",
                    "prepared conversation missing",
                )))?
                .image_attachments
                .clone();
            recorder
                .prepare(
                    token.job,
                    RequestNumber::FIRST,
                    &handoff.prepared.request,
                    0,
                )
                .map_err(|_| {
                    DatabaseError::Rejected(Violation::InvalidValue(
                        "request",
                        "prepared request cannot be recorded",
                    ))
                })?
                .0
        };
        sent.observed_at = observed_at;
        if handoff.output_uncertain.load(Ordering::SeqCst)
            || handoff
                .output_busy
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
        {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "execution",
                "write is pending or requires reconciliation",
            )));
        }
        let mut guard = DevelopmentOutputGuard {
            handoff,
            known: false,
        };
        if handoff.output_uncertain.load(Ordering::SeqCst) {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "execution",
                "write requires reconciliation",
            )));
        }
        let proof = DevelopmentWriteProof {
            token,
            revision: handoff.started.revision,
        };
        let attachments = match &handoff.prepared.candidate {
            redb::DevelopmentCandidate::Generative(context) => context
                .covered_inputs
                .iter()
                .flat_map(|input| input.attachments.iter())
                .chain(
                    context
                        .history
                        .iter()
                        .flat_map(|turn| turn.attachments.iter()),
                )
                .chain(context.attachments.iter())
                .cloned()
                .collect::<Vec<_>>(),
            redb::DevelopmentCandidate::Decision(_) => Vec::new(),
        };
        let result = self
            .submit(|reply| {
                Command::new(
                    (proof, sent, attachments),
                    reply,
                    |(_, sent, _)| {
                        sent.bodies.iter().fold(0u64, |size, body| {
                            size.saturating_add(byte_count(body.byte_length()))
                        })
                    },
                    OperationKind::RecordSentRequest,
                    |(proof, sent, attachments), batch| {
                        batch.record_development_sent_request(&proof, sent, &attachments)
                    },
                )
            })
            .await;
        guard.known = !matches!(result, Err(DatabaseError::OutcomeUnknown { .. }));
        result
    }

    /// Records an observed answer; this operation does not contact a provider.
    #[allow(dead_code)]
    pub(crate) async fn record_development_provider_answer(
        &self,
        handoff: &DevelopmentHandoff,
        answer: ProviderAnswer,
    ) -> Result<(), DatabaseError> {
        if !Arc::ptr_eq(&self.shared, &handoff.owner) {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "handoff",
                "different database worker",
            )));
        }
        let token = match &handoff.prepared.candidate {
            redb::DevelopmentCandidate::Generative(context) => context.allocation.token,
            redb::DevelopmentCandidate::Decision(context) => context.allocation.token,
        };
        let response: serde_json::Value =
            serde_json::from_slice(&answer.response.bytes).map_err(|_| {
                DatabaseError::Rejected(Violation::InvalidValue(
                    "response",
                    "invalid response JSON",
                ))
            })?;
        if let Some(secret) = &handoff.prepared.credentials.spellings {
            let observation = &answer.stat.observation;
            let failure_contains_secret = observation.failure.as_ref().is_some_and(|failure| {
                secret.found_in(&failure.message)
                    || failure
                        .provider_code
                        .as_deref()
                        .is_some_and(|s| secret.found_in(s))
                    || failure
                        .response_body
                        .as_deref()
                        .is_some_and(|s| secret.found_in(s))
                    || failure
                        .request_id
                        .as_deref()
                        .is_some_and(|s| secret.found_in(s))
            });
            if development_contains_secret(&response, secret)
                || answer
                    .measurements
                    .usage
                    .raw
                    .as_ref()
                    .is_some_and(|value| development_contains_secret(value, secret))
                || observation
                    .raw_finish_reason
                    .as_deref()
                    .is_some_and(|s| secret.found_in(s))
                || failure_contains_secret
            {
                return Err(DatabaseError::Rejected(Violation::InvalidValue(
                    "answer",
                    "answer evidence contains credential material",
                )));
            }
        }
        if handoff.output_uncertain.load(Ordering::SeqCst)
            || handoff
                .output_busy
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
        {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "execution",
                "write is pending or requires reconciliation",
            )));
        }
        let mut guard = DevelopmentOutputGuard {
            handoff,
            known: false,
        };
        if handoff.output_uncertain.load(Ordering::SeqCst) {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "execution",
                "write requires reconciliation",
            )));
        }
        let proof = DevelopmentWriteProof {
            token,
            revision: handoff.started.revision,
        };
        let result = self
            .submit(|reply| {
                Command::custom(
                    (proof, answer),
                    reply,
                    |(_, answer)| {
                        byte_count(answer.response.byte_length())
                            .saturating_add(json_bytes(answer.measurements.usage.raw.as_ref()))
                    },
                    |(proof, answer), reply, database, operation, commit| {
                        let ids = std::cell::Cell::new(None);
                        let mut result = run(
                            database,
                            operation,
                            commit,
                            OperationKind::RecordProviderAnswer,
                            reply,
                            |batch| {
                                let value =
                                    batch.record_development_provider_answer(&proof, answer)?;
                                ids.set(Some(value));
                                Ok(())
                            },
                        );
                        if result.committed
                            && let Some((metrics, availability)) = ids.get()
                        {
                            result
                                .observations
                                .push(ObservationChange::Metrics(proof.token.job, metrics));
                            result.observations.push(ObservationChange::Availability(
                                proof.token.job,
                                availability,
                            ));
                        }
                        result
                    },
                )
            })
            .await;
        guard.known = !matches!(result, Err(DatabaseError::OutcomeUnknown { .. }));
        result
    }

    /// Records one explicitly annotated Token observation. `expected_seq` is
    /// the existing request-event key retained by the caller for replay.
    #[allow(dead_code)]
    pub(crate) async fn record_development_usage(
        &self,
        handoff: &DevelopmentHandoff,
        expected_seq: u32,
        metrics: Metrics,
    ) -> Result<MetricsId, DatabaseError> {
        if !Arc::ptr_eq(&self.shared, &handoff.owner) {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "handoff",
                "different database worker",
            )));
        }
        let token = match &handoff.prepared.candidate {
            redb::DevelopmentCandidate::Generative(context) => context.allocation.token,
            redb::DevelopmentCandidate::Decision(_) => {
                return Err(DatabaseError::Rejected(Violation::InvalidValue(
                    "handoff",
                    "generative handoff required",
                )));
            }
        };
        if handoff
            .prepared
            .credentials
            .spellings
            .as_ref()
            .is_some_and(|secret| {
                metrics
                    .measurements
                    .usage
                    .raw
                    .as_ref()
                    .is_some_and(|raw| development_contains_secret(raw, secret))
            })
        {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "usage",
                "usage evidence contains credential material",
            )));
        }
        if handoff.output_uncertain.load(Ordering::SeqCst)
            || handoff
                .output_busy
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
        {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "execution",
                "write is pending or requires reconciliation",
            )));
        }
        let mut guard = DevelopmentOutputGuard {
            handoff,
            known: false,
        };
        if handoff.output_uncertain.load(Ordering::SeqCst) {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "execution",
                "write requires reconciliation",
            )));
        }
        let proof = DevelopmentWriteProof {
            token,
            revision: handoff.started.revision,
        };
        let checkpoint = handoff
            .usage_checkpoint
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let result = self
            .submit(|reply| {
                Command::custom(
                    (proof, expected_seq, metrics, checkpoint),
                    reply,
                    |(_, _, metrics, _)| json_bytes(metrics.measurements.usage.raw.as_ref()),
                    |(proof, expected_seq, metrics, checkpoint),
                     reply,
                     database,
                     operation,
                     commit| {
                        let id = std::cell::Cell::new(None);
                        let mut result = run(
                            database,
                            operation,
                            commit,
                            OperationKind::RecordMetrics,
                            reply,
                            |batch| {
                                let value = batch.record_development_usage(
                                    &proof,
                                    expected_seq,
                                    metrics,
                                    checkpoint,
                                )?;
                                id.set(Some(value.0));
                                Ok(value)
                            },
                        );
                        if result.committed
                            && let Some(id) = id.get()
                        {
                            result
                                .observations
                                .push(ObservationChange::Metrics(proof.token.job, id));
                        }
                        result
                    },
                )
            })
            .await;
        guard.known = !matches!(result, Err(DatabaseError::OutcomeUnknown { .. }));
        result.map(|(id, checkpoint)| {
            *handoff
                .usage_checkpoint
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = checkpoint;
            id
        })
    }

    /// Finalizes only after the supervising runtime observed an in-process return.
    #[allow(dead_code)]
    pub(crate) async fn finalize_development_job(
        &self,
        returned: crate::system::executor::execution_runtime::DevelopmentExecutionReturn<'_>,
        expected_seq: u32,
        finalization: JobFinalization,
    ) -> Result<AppendedOutput, DatabaseError> {
        let handoff = returned.handoff();
        if !Arc::ptr_eq(&self.shared, &handoff.owner) {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "handoff",
                "different database worker",
            )));
        }
        let token = match &handoff.prepared.candidate {
            redb::DevelopmentCandidate::Generative(context) => context.allocation.token,
            redb::DevelopmentCandidate::Decision(_) => {
                return Err(DatabaseError::Rejected(Violation::InvalidValue(
                    "handoff",
                    "generative handoff required",
                )));
            }
        };
        if let Some(secret) = &handoff.prepared.credentials.spellings {
            let result = match &finalization {
                JobFinalization::Completed(result)
                | JobFinalization::Failed(result)
                | JobFinalization::Cancelled(result) => Some(result),
                JobFinalization::Unknown => None,
            };
            if result.is_some_and(|result| {
                result
                    .summary
                    .as_deref()
                    .is_some_and(|s| secret.found_in(s))
                    || result.code.as_deref().is_some_and(|s| secret.found_in(s))
                    || result
                        .value
                        .as_ref()
                        .is_some_and(|v| development_contains_secret(v, secret))
            }) {
                return Err(DatabaseError::Rejected(Violation::InvalidValue(
                    "outcome",
                    "result contains credential material",
                )));
            }
        }
        if handoff.output_uncertain.load(Ordering::SeqCst)
            || handoff
                .output_busy
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
        {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "execution",
                "write is pending or requires reconciliation",
            )));
        }
        let mut guard = DevelopmentOutputGuard {
            handoff,
            known: false,
        };
        if handoff.output_uncertain.load(Ordering::SeqCst) {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "execution",
                "write requires reconciliation",
            )));
        }
        let proof = DevelopmentWriteProof {
            token,
            revision: handoff.started.revision,
        };
        let result = self
            .submit(|reply| {
                Command::custom(
                    (proof, expected_seq, finalization),
                    reply,
                    |(_, _, value)| match value {
                        JobFinalization::Completed(result)
                        | JobFinalization::Failed(result)
                        | JobFinalization::Cancelled(result) => output_bytes(
                            result.summary.as_deref(),
                            result.code.as_deref(),
                            result.value.as_ref(),
                        ),
                        JobFinalization::Unknown => 0,
                    },
                    |(proof, expected_seq, finalization), reply, database, operation, commit| {
                        let mut result = run(
                            database,
                            operation,
                            commit,
                            OperationKind::FinalizeJob,
                            reply,
                            |batch| {
                                batch.finalize_development_job(&proof, expected_seq, finalization)
                            },
                        );
                        if result.committed {
                            result.output_changed = Some(proof.token.job);
                        }
                        result
                    },
                )
            })
            .await;
        guard.known = !matches!(result, Err(DatabaseError::OutcomeUnknown { .. }));
        result
    }

    /// Finalizes a typed native Decision after the local executor has returned.
    #[allow(dead_code)]
    pub(crate) async fn finalize_development_decision(
        &self,
        returned: crate::system::executor::execution_runtime::DevelopmentExecutionReturn<'_>,
        expected_seq: u32,
        result: entities::DecisionResult,
    ) -> Result<AppendedOutput, DatabaseError> {
        let handoff = returned.handoff();
        if !Arc::ptr_eq(&self.shared, &handoff.owner) {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "handoff",
                "different database worker",
            )));
        }
        let token = match &handoff.prepared.candidate {
            redb::DevelopmentCandidate::Decision(context) => context.allocation.token,
            redb::DevelopmentCandidate::Generative(_) => {
                return Err(DatabaseError::Rejected(Violation::InvalidValue(
                    "handoff",
                    "Decision handoff required",
                )));
            }
        };
        let value = serde_json::to_value(&result).map_err(|_| {
            DatabaseError::Rejected(Violation::InvalidValue(
                "decision",
                "result cannot be serialized",
            ))
        })?;
        if handoff
            .prepared
            .credentials
            .spellings
            .as_ref()
            .is_some_and(|secret| development_contains_secret(&value, secret))
        {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "decision",
                "result contains credential material",
            )));
        }
        if handoff.output_uncertain.load(Ordering::SeqCst)
            || handoff
                .output_busy
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
        {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "execution",
                "write is pending or requires reconciliation",
            )));
        }
        let mut guard = DevelopmentOutputGuard {
            handoff,
            known: false,
        };
        if handoff.output_uncertain.load(Ordering::SeqCst) {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "execution",
                "write requires reconciliation",
            )));
        }
        let proof = DevelopmentWriteProof {
            token,
            revision: handoff.started.revision,
        };
        let result = self
            .submit(|reply| {
                Command::custom(
                    (proof, expected_seq, result),
                    reply,
                    |(_, _, result)| debug_bytes(result),
                    |(proof, expected_seq, decision), reply, database, operation, commit| {
                        let mut result = run(
                            database,
                            operation,
                            commit,
                            OperationKind::FinalizeJob,
                            reply,
                            |batch| {
                                batch.finalize_development_decision(&proof, expected_seq, decision)
                            },
                        );
                        if result.committed {
                            result.output_changed = Some(proof.token.job);
                        }
                        result
                    },
                )
            })
            .await;
        guard.known = !matches!(result, Err(DatabaseError::OutcomeUnknown { .. }));
        result
    }

    /// Settles a Job Failed or Cancelled without an answer, for either typed
    /// handoff. A Failed settlement may carry the observation of a Provider
    /// response that rejected the request; it is committed with the terminal
    /// transition. Only the supervising runtime, having observed the exchange
    /// return, can provide the proof. Success evidence is never supplied here.
    #[allow(dead_code)]
    pub(crate) async fn finalize_development_failure(
        &self,
        returned: crate::system::executor::execution_runtime::DevelopmentExecutionReturn<'_>,
        expected_seq: u32,
        finalization: JobFinalization,
        rejection: Option<(entities::Timestamp, entities::AvailabilityObservation)>,
    ) -> Result<AppendedOutput, DatabaseError> {
        let handoff = returned.handoff();
        if !Arc::ptr_eq(&self.shared, &handoff.owner) {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "handoff",
                "different database worker",
            )));
        }
        let token = match &handoff.prepared.candidate {
            redb::DevelopmentCandidate::Generative(context) => context.allocation.token,
            redb::DevelopmentCandidate::Decision(context) => context.allocation.token,
        };
        if let Some(secret) = &handoff.prepared.credentials.spellings {
            let (JobFinalization::Completed(result)
            | JobFinalization::Failed(result)
            | JobFinalization::Cancelled(result)) = &finalization
            else {
                return Err(DatabaseError::Rejected(Violation::InvalidValue(
                    "outcome",
                    "an unanswered failure needs a result",
                )));
            };
            let failure = rejection
                .as_ref()
                .and_then(|(_, observation)| observation.failure.as_ref());
            if [
                result.summary.as_deref(),
                result.code.as_deref(),
                failure.map(|failure| failure.message.as_str()),
                failure.and_then(|failure| failure.provider_code.as_deref()),
                failure.and_then(|failure| failure.response_body.as_deref()),
                failure.and_then(|failure| failure.request_id.as_deref()),
            ]
            .into_iter()
            .flatten()
            .any(|text| secret.found_in(text))
                || result
                    .value
                    .as_ref()
                    .is_some_and(|value| development_contains_secret(value, secret))
            {
                return Err(DatabaseError::Rejected(Violation::InvalidValue(
                    "outcome",
                    "failure evidence contains credential material",
                )));
            }
        }
        if handoff.output_uncertain.load(Ordering::SeqCst)
            || handoff
                .output_busy
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
        {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "execution",
                "write is pending or requires reconciliation",
            )));
        }
        let mut guard = DevelopmentOutputGuard {
            handoff,
            known: false,
        };
        if handoff.output_uncertain.load(Ordering::SeqCst) {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "execution",
                "write requires reconciliation",
            )));
        }
        let proof = DevelopmentWriteProof {
            token,
            revision: handoff.started.revision,
        };
        let rejection = rejection.map(|(observed_at, observation)| entities::AvailabilityStat {
            job: token.job,
            request: entities::RequestNumber::FIRST,
            observed_at,
            observation,
        });
        let result = self
            .submit(|reply| {
                Command::custom(
                    (proof, expected_seq, finalization, rejection),
                    reply,
                    |(_, _, finalization, rejection)| {
                        let result = match finalization {
                            JobFinalization::Completed(result)
                            | JobFinalization::Failed(result)
                            | JobFinalization::Cancelled(result) => output_bytes(
                                result.summary.as_deref(),
                                result.code.as_deref(),
                                result.value.as_ref(),
                            ),
                            JobFinalization::Unknown => 0,
                        };
                        result.saturating_add(
                            rejection
                                .as_ref()
                                .map_or(0, |stat| debug_bytes(&stat.observation)),
                        )
                    },
                    |(proof, expected_seq, finalization, rejection),
                     reply,
                     database,
                     operation,
                     commit| {
                        let mut outcome = run(
                            database,
                            operation,
                            commit,
                            OperationKind::FinalizeJob,
                            reply,
                            |batch| {
                                batch.finalize_development_failure(
                                    &proof,
                                    expected_seq,
                                    finalization,
                                    rejection,
                                )
                            },
                        );
                        if outcome.committed {
                            outcome.output_changed = Some(proof.token.job);
                        }
                        outcome
                    },
                )
            })
            .await;
        guard.known = !matches!(result, Err(DatabaseError::OutcomeUnknown { .. }));
        result
    }

    /// Ends the Job and releases the capacity of a Reserved Allocation whose
    /// checked start never committed, after [`DevelopmentHandoffError::NotStarted`].
    /// The writer re-proves in one transaction that the reservation is
    /// untouched and holds no execution authority; anything else is refused
    /// and the Allocation stays held. Repeating the call is refused because
    /// the first one releases the Allocation.
    #[allow(dead_code)]
    pub(crate) async fn fail_unstarted_development_reservation(
        &self,
        token: entities::AllocationToken,
        cause: entities::UnstartedCause,
    ) -> Result<AppendedOutput, DatabaseError> {
        self.submit(|reply| {
            Command::custom(
                (token, cause),
                reply,
                |(_, cause)| output_bytes(Some(cause.summary()), Some(cause.code()), None),
                |(token, cause), reply, database, operation, commit| {
                    let mut outcome = run(
                        database,
                        operation,
                        commit,
                        OperationKind::FinalizeJob,
                        reply,
                        |batch| batch.fail_unstarted_reservation(token, cause),
                    );
                    if outcome.committed {
                        outcome.output_changed = Some(token.job);
                    }
                    outcome
                },
            )
        })
        .await
    }

    /// Records, with a typed reason, that a supervised exchange returned
    /// without an outcome this process can classify. The Job becomes Unknown
    /// and the Allocation Uncertain and still held; nothing is released or
    /// retried. Only the supervising runtime, having observed the exchange
    /// return, can provide the proof.
    #[allow(dead_code)]
    pub(crate) async fn record_development_uncertain(
        &self,
        returned: crate::system::executor::execution_runtime::DevelopmentExecutionReturn<'_>,
        reason: entities::UncertaintyReason,
    ) -> Result<(), DatabaseError> {
        let handoff = returned.handoff();
        if !Arc::ptr_eq(&self.shared, &handoff.owner) {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "handoff",
                "different database worker",
            )));
        }
        let token = match &handoff.prepared.candidate {
            redb::DevelopmentCandidate::Generative(context) => context.allocation.token,
            redb::DevelopmentCandidate::Decision(context) => context.allocation.token,
        };
        if handoff.output_uncertain.load(Ordering::SeqCst)
            || handoff
                .output_busy
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
        {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "execution",
                "write is pending or requires reconciliation",
            )));
        }
        let mut guard = DevelopmentOutputGuard {
            handoff,
            known: false,
        };
        let proof = DevelopmentWriteProof {
            token,
            revision: handoff.started.revision,
        };
        let result = self
            .submit(|reply| {
                Command::custom(
                    (proof, reason),
                    reply,
                    |_| 0,
                    |(proof, reason), reply, database, operation, commit| {
                        run(
                            database,
                            operation,
                            commit,
                            OperationKind::FinalizeJob,
                            reply,
                            |batch| batch.record_development_uncertain(&proof, reason),
                        )
                    },
                )
            })
            .await;
        guard.known = !matches!(result, Err(DatabaseError::OutcomeUnknown { .. }));
        result
    }

    /// Snapshot evidence only; this neither grants dispatch authority nor adopts ownership.
    #[allow(dead_code)]
    pub(crate) async fn development_execution_context(
        &self,
        token: entities::AllocationToken,
    ) -> Result<redb::DevelopmentResolvedCandidate, DatabaseError> {
        let snapshot = self
            .read(move |database| redb::read_development_execution_context(database, token))
            .await?;
        self.resolve_development_candidate(snapshot).await
    }

    /// Reads the image content a snapshot names and checks it is the content
    /// the snapshot saw.
    async fn resolve_development_candidate(
        &self,
        snapshot: redb::DevelopmentCandidate,
    ) -> Result<redb::DevelopmentResolvedCandidate, DatabaseError> {
        use crate::service::ai::providers::openai_compatible::image::{
            ImageInput, ImageMime, MAX_IMAGE_BYTES, MAX_REQUEST_IMAGE_BYTES, MAX_REQUEST_IMAGES,
        };
        let snapshot = match snapshot {
            redb::DevelopmentCandidate::Generative(snapshot) => snapshot,
            redb::DevelopmentCandidate::Decision(snapshot) => {
                return Ok(redb::DevelopmentResolvedCandidate::Decision(snapshot));
            }
        };
        let unavailable = |detail: &str| {
            DatabaseError::Read(StorageError {
                kind: StorageErrorKind::Other,
                os_error: None,
                detail: detail.to_owned(),
            })
        };
        let records: Vec<_> = snapshot
            .covered_inputs
            .iter()
            .flat_map(|turn| turn.attachments.iter())
            .chain(
                snapshot
                    .history
                    .iter()
                    .flat_map(|turn| turn.attachments.iter()),
            )
            .chain(snapshot.attachments.iter())
            .collect();
        if records.len() > MAX_REQUEST_IMAGES {
            return Err(unavailable("context has too many images"));
        }
        let mut total = 0u64;
        let mut images = Vec::with_capacity(records.len());
        for record in records {
            let mime = ImageMime::from_mime(&record.mime)
                .ok_or_else(|| unavailable("context Attachment is not a supported image"))?;
            if record.byte_length > MAX_IMAGE_BYTES as u64 {
                return Err(unavailable("context image exceeds the per-image limit"));
            }
            total = total.saturating_add(record.byte_length);
            if total > MAX_REQUEST_IMAGE_BYTES as u64 {
                return Err(unavailable("context images exceed the request limit"));
            }
            let content =
                self.attachment_content(record.id)
                    .await
                    .map_err(|error| match error {
                        AttachmentError::Database(error) => error,
                        _ => unavailable("context Attachment content is unavailable"),
                    })?;
            if content.record != *record {
                return Err(unavailable("context Attachment changed after the snapshot"));
            }
            let image = ImageInput::new(mime, content.bytes)
                .map_err(|_| unavailable("context Attachment content is not a valid image"))?;
            images.push((record.id, image));
        }
        Ok(redb::DevelopmentResolvedCandidate::Generative(
            redb::DevelopmentResolvedContext { snapshot, images },
        ))
    }

    /// Read-only provenance evidence; it does not authorize adoption or dispatch.
    #[allow(dead_code)]
    pub(crate) async fn verify_native_summary(
        &self,
        conversation: JobId,
        consumer: JobId,
        producer: JobId,
        source: entities::ContentDigest,
    ) -> Result<redb::VerifiedNativeSummary, DatabaseError> {
        self.read(move |database| {
            redb::verify_native_summary(database, conversation, consumer, producer, source)
        })
        .await
    }

    // These commands are not yet connected to the HTTP API. Their storage
    // gate refuses the current format, and their replies remain tentative
    // until the write worker has completed its durable group commit.
    #[allow(dead_code)]
    pub(crate) async fn create_mutable_job(
        &self,
        context: entities::ComposerContextId,
        input: entities::MutableJobInput,
        creation: Submission,
        conversation: Option<JobId>,
        project: Option<entities::ProjectId>,
    ) -> Result<entities::MutableJobRecord, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (context, input, creation, conversation, project),
                reply,
                |(_, input, _, _, _)| byte_count(input.size_hint()).saturating_add(8192),
                OperationKind::CreateJobWithInput,
                |(context, input, creation, conversation, project), batch| {
                    let id = batch.create_unsubmitted_job(
                        context,
                        input,
                        creation,
                        conversation,
                        project,
                    )?;
                    match batch.read_mutable_job(id)? {
                        Some(job) => Ok(job),
                        None => batch.corrupted("created mutable Job is missing".to_owned()),
                    }
                },
            )
        })
        .await
    }

    #[allow(dead_code)]
    pub(crate) async fn mutable_job_for_context(
        &self,
        context: entities::ComposerContextId,
    ) -> Result<Option<entities::MutableJobRecord>, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (context,),
                reply,
                |_| 0,
                OperationKind::Open,
                |(context,), batch| batch.mutable_job_for_context(context),
            )
        })
        .await
    }

    #[allow(dead_code)]
    pub(crate) async fn mutable_job_in_context(
        &self,
        context: entities::ComposerContextId,
        id: JobId,
    ) -> Result<Option<entities::MutableJobRecord>, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (context, id),
                reply,
                |_| 0,
                OperationKind::Open,
                |(context, id), batch| {
                    batch.require_mutable_job_context(context, id)?;
                    batch.read_mutable_job(id)
                },
            )
        })
        .await
    }

    pub(crate) async fn mutable_job_with_support(
        &self,
        context: entities::ComposerContextId,
        id: JobId,
    ) -> Result<(entities::MutableJobRecord, bool), DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (context, id),
                reply,
                |_| 0,
                OperationKind::Open,
                |(context, id), batch| {
                    batch.require_mutable_job_context(context, id)?;
                    let Some(record) = batch.read_mutable_job(id)? else {
                        return batch.corrupted("mutable Job is missing".to_owned());
                    };
                    let blocked = batch.unsupported_queued_context(&record.job)?;
                    Ok((record, blocked))
                },
            )
        })
        .await
    }

    #[allow(dead_code)]
    pub(crate) async fn update_mutable_job(
        &self,
        context: entities::ComposerContextId,
        id: JobId,
        revision: u64,
        input: entities::MutableJobInput,
    ) -> Result<entities::MutableJobRecord, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (context, id, revision, input),
                reply,
                |(_, _, _, input)| byte_count(input.size_hint()).saturating_add(40 * 1024),
                OperationKind::SaveJobInput,
                |(context, id, revision, input), batch| {
                    batch.require_mutable_job_context(context, id)?;
                    batch.update_mutable_job(id, revision, input)?;
                    match batch.read_mutable_job(id)? {
                        Some(job) => Ok(job),
                        None => batch.corrupted("updated mutable Job is missing".to_owned()),
                    }
                },
            )
        })
        .await
    }

    /// Update the context selection of a Job in its authenticated Composer
    /// context. A successful reply follows durable transaction commit.
    pub async fn set_mutable_context_selection(
        &self,
        context: entities::ComposerContextId,
        id: JobId,
        revision: u64,
        selection: entities::MutableContextSelection,
    ) -> Result<entities::MutableJobRecord, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (context, id, revision, selection),
                reply,
                |_| 40 * 1024,
                OperationKind::SaveJobInput,
                |(context, id, revision, selection), batch| {
                    batch.require_mutable_job_context(context, id)?;
                    batch.set_mutable_context_selection(id, revision, selection)?;
                    match batch.read_mutable_job(id)? {
                        Some(job) => Ok(job),
                        None => batch.corrupted("updated mutable Job is missing".to_owned()),
                    }
                },
            )
        })
        .await
    }

    #[allow(dead_code)]
    pub(crate) async fn submit_mutable_job(
        &self,
        context: entities::ComposerContextId,
        id: JobId,
        revision: u64,
        input: Option<entities::MutableJobInput>,
        submission: Submission,
    ) -> Result<entities::MutableJobRecord, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (context, id, revision, input, submission),
                reply,
                |(_, _, _, input, _)| {
                    input.as_ref().map_or(8192, |input| {
                        byte_count(input.size_hint()).saturating_add(40 * 1024)
                    })
                },
                OperationKind::Open,
                |(context, id, revision, input, submission), batch| {
                    batch.require_mutable_job_context(context, id)?;
                    match input {
                        Some(input) => {
                            batch.submit_mutable_job_with_input(id, revision, input, submission)
                        }
                        None => batch.submit_mutable_job(id, revision, submission),
                    }?;
                    match batch.read_mutable_job(id)? {
                        Some(job) => Ok(job),
                        None => batch.corrupted("submitted mutable Job is missing".to_owned()),
                    }
                },
            )
        })
        .await
    }

    #[allow(dead_code)]
    pub(crate) async fn discard_mutable_job(
        &self,
        context: entities::ComposerContextId,
        id: JobId,
        revision: u64,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (context, id, revision),
                reply,
                |_| 8192,
                OperationKind::StopTurn,
                |(context, id, revision), batch| {
                    batch.require_mutable_job_context(context, id)?;
                    batch.discard_unsubmitted_job(id, revision)
                },
            )
        })
        .await
    }

    pub(crate) async fn cancel_unsupported_queued_job(
        &self,
        context: entities::ComposerContextId,
        id: JobId,
        revision: u64,
    ) -> Result<entities::MutableJobRecord, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (context, id, revision),
                reply,
                |_| 8192,
                OperationKind::StopTurn,
                |(context, id, revision), batch| {
                    batch.require_mutable_job_context(context, id)?;
                    batch.cancel_unsupported_queued_job(id, revision)?;
                    match batch.read_mutable_job(id)? {
                        Some(job) => Ok(job),
                        None => batch.corrupted("cancelled Job is missing".to_owned()),
                    }
                },
            )
        })
        .await
    }

    /// T1: submits a Job, or the next turn of a conversation when it names a
    /// predecessor in `previous`. The Job is pending; the claim batch grants
    /// it when a compatible Pool has room.
    pub async fn create_job(&self, job: Job) -> Result<JobId, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (job,),
                reply,
                |(job,)| {
                    text_bytes(job.name.as_deref())
                        .saturating_add(text_bytes(job.input.objective.as_deref()))
                },
                OperationKind::CreateJobWithInput,
                |(job,), batch| mutations::create_job(batch, job),
            )
        })
        .await
    }

    #[doc(hidden)]
    pub async fn bench_create_dependent_turn(&self, job: Job) -> Result<JobId, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (job,),
                reply,
                |(job,)| {
                    text_bytes(job.name.as_deref())
                        .saturating_add(text_bytes(job.input.objective.as_deref()))
                },
                OperationKind::CreateJobWithInput,
                |(job,), batch| mutations::bench_create_dependent_turn(batch, job),
            )
        })
        .await
    }

    pub async fn create_submitted_job(
        &self,
        job: Job,
        submission: Submission,
    ) -> Result<JobId, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (job, submission),
                reply,
                |(job, _)| {
                    text_bytes(job.name.as_deref())
                        .saturating_add(text_bytes(job.input.objective.as_deref()))
                },
                OperationKind::CreateJobWithInput,
                |(job, submission), batch| mutations::create_submitted_job(batch, job, submission),
            )
        })
        .await
    }

    pub(crate) async fn agent_model_configs(
        &self,
        ids: Vec<ConfigId>,
    ) -> Result<Vec<redb::AgentModelConfig>, DatabaseError> {
        self.read(move |database| redb::read_agent_model_configs(database, &ids))
            .await
    }

    /// A config, whatever its kind.
    pub async fn config(&self, id: ConfigId) -> Result<Option<ConfigRecord>, DatabaseError> {
        self.read(move |database| redb::read_config(database, id))
            .await
    }

    pub(crate) async fn credential_ref_in_use(
        &self,
        reference: &str,
    ) -> Result<bool, DatabaseError> {
        let reference = reference.to_owned();
        self.read(move |database| redb::read_credential_ref_in_use(database, &reference))
            .await
    }

    /// Configs of `kind` (every kind when `None`), newest first, strictly
    /// before `before` when given; at most 1,000 rows are examined per call.
    /// Archived configs are included; an empty page can have a continuation.
    pub async fn configs(
        &self,
        kind: Option<ConfigKind>,
        before: Option<ConfigId>,
        limit: usize,
    ) -> Result<redb::CursorPage<ConfigRecord, ConfigId>, DatabaseError> {
        self.read(move |database| redb::read_configs(database, kind, before, limit))
            .await
    }

    pub(crate) async fn native_resources(
        &self,
        before: Option<ConfigId>,
        limit: usize,
        kind: redb::NativeResourceKind,
    ) -> Result<redb::CursorPage<redb::NativeResource, ConfigId>, DatabaseError> {
        self.read(move |database| redb::read_native_resources(database, before, limit, kind))
            .await
    }

    pub(crate) async fn register_native_pool(
        &self,
        pool: entities::AllocationPool,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (pool,),
                reply,
                |_| 4096,
                OperationKind::CreateExecutionPool,
                |(pool,), batch| batch.register_allocation_pool(pool),
            )
        })
        .await
    }

    pub(crate) async fn register_native_executor(
        &self,
        executor: entities::AllocationExecutor,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (executor,),
                reply,
                |_| 4096,
                OperationKind::CreateExecutor,
                |(executor,), batch| batch.register_allocation_executor(executor),
            )
        })
        .await
    }

    pub(crate) async fn update_native_pool(
        &self,
        id: ExecutionPoolId,
        capacity: Option<std::num::NonZeroU32>,
        enabled: Option<bool>,
    ) -> Result<entities::AllocationPool, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (id, capacity, enabled),
                reply,
                |_| 4096,
                OperationKind::SetExecutionPoolProfile,
                |(id, capacity, enabled), batch| batch.update_native_pool(id, capacity, enabled),
            )
        })
        .await
    }

    pub(crate) async fn find_config(
        &self,
        kind: ConfigKind,
        matches: impl Fn(&ConfigRecord) -> bool,
    ) -> Result<Option<ConfigRecord>, DatabaseError> {
        let mut before = None;
        loop {
            let page = self.configs(Some(kind), before, 1_000).await?;
            if let Some(record) = page.records.into_iter().find(&matches) {
                return Ok(Some(record));
            }
            before = page.next;
            if before.is_none() {
                return Ok(None);
            }
        }
    }

    /// One bounded page of active Profile configs (Models and Agents), newest
    /// first and strictly before `before`, with the Connections those Models
    /// name and their compatible Executors read in the same transaction.
    /// `scan` bounds the configs examined, so an empty page can still advance
    /// its `next_before`; `None` there means the configs were exhausted.
    pub(crate) async fn profile_page(
        &self,
        before: Option<ConfigId>,
        limit: usize,
        scan: usize,
    ) -> Result<redb::ProfilePageRecords, DatabaseError> {
        let key = self.shared.credential_key();
        self.read(move |database| {
            redb::read_profile_page(database, key.as_deref(), before, limit, scan)
        })
        .await
    }

    /// Creates a Connection, Model, Agent or Pool config. Its references must
    /// name configs of the kinds its body requires.
    pub async fn create_config(&self, config: Config) -> Result<ConfigId, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (config,),
                reply,
                |(config,)| config_bytes(config),
                OperationKind::CreateProfile,
                |(config,), batch| mutations::create_config(batch, config),
            )
        })
        .await
    }

    pub(crate) async fn set_config_enabled(
        &self,
        id: ConfigId,
        kind: crate::db::entities::ConfigKind,
        enabled: bool,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (id, kind, enabled),
                reply,
                |(_, _, _)| 32,
                OperationKind::UpdateProfile,
                |(id, kind, enabled), batch| batch.set_config_enabled(id, kind, enabled),
            )
        })
        .await
    }

    pub(crate) async fn remove_provider(&self, id: ConfigId) -> Result<(), DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (id,),
                reply,
                |(_,)| 16,
                OperationKind::UpdateProfile,
                |(id,), batch| batch.remove_provider(id),
            )
        })
        .await
    }

    /// Creates or edits an LLM or Decision Model Profile, resolving its
    /// Connection and any required LLM Executor in the same write batch.
    pub(crate) async fn save_model_profile(
        &self,
        mut save: mutations::ModelProfileSave,
    ) -> Result<(ConfigId, Option<String>), DatabaseError> {
        if let Some(secret) = save.secret.clone() {
            let key = self.ensure_credential_key().await?;
            save.credential = Some(mutations::CredentialPayload { secret, key });
        }
        let kind = if save.existing_id.is_some() {
            OperationKind::UpdateProfile
        } else {
            OperationKind::CreateProfile
        };
        self.submit(|reply| {
            Command::new(
                (save,),
                reply,
                |(save,)| model_profile_save_bytes(save),
                kind,
                |(save,), batch| mutations::save_model_profile(batch, save),
            )
        })
        .await
    }

    pub async fn refresh_model_metadata(
        &self,
        connection: crate::db::entities::ConnectionConfig,
        models: Vec<crate::db::entities::DiscoveredModel>,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (connection, models),
                reply,
                |(connection, models)| {
                    text_bytes(connection.endpoint.as_deref())
                        .saturating_add(text_bytes(connection.credential_ref.as_deref()))
                        .saturating_add(
                            models
                                .iter()
                                .map(|model| byte_count(model.id.len()).saturating_add(512))
                                .sum::<u64>(),
                        )
                },
                OperationKind::UpdateProfile,
                |(connection, models), batch| batch.refresh_model_metadata(connection, models),
            )
        })
        .await
    }

    /// Imports missing models and their Provider connection in one transaction.
    pub(crate) async fn import_models(
        &self,
        mut save: mutations::ImportModelsSave,
    ) -> Result<(Vec<ConfigId>, usize), DatabaseError> {
        if let Some(secret) = save.secret.clone() {
            let key = self.ensure_credential_key().await?;
            save.credential = Some(mutations::CredentialPayload { secret, key });
        }
        self.submit(|reply| {
            Command::new(
                (save,),
                reply,
                |(save,)| {
                    let ref_bytes = match &save.credential_ref {
                        mutations::ProfileCredentialRef::Preserve => match &save.connection.body {
                            ConfigBody::Connection(body) => {
                                text_bytes(body.credential_ref.as_deref())
                            }
                            ConfigBody::Model(_)
                            | ConfigBody::Agent(_)
                            | ConfigBody::Pool(_)
                            | ConfigBody::DecisionModel(_)
                            | ConfigBody::DecisionAgent(_) => 0,
                        },
                        mutations::ProfileCredentialRef::Clear => 0,
                        mutations::ProfileCredentialRef::Explicit(reference)
                        | mutations::ProfileCredentialRef::PreserveOrGenerate(reference) => {
                            byte_count(reference.len())
                        }
                    };
                    let endpoint_bytes = match &save.connection.body {
                        ConfigBody::Connection(body) => text_bytes(body.endpoint.as_deref()),
                        ConfigBody::Model(_)
                        | ConfigBody::Agent(_)
                        | ConfigBody::Pool(_)
                        | ConfigBody::DecisionModel(_)
                        | ConfigBody::DecisionAgent(_) => 0,
                    };
                    let secret_bytes = save
                        .secret
                        .as_ref()
                        .map_or(0, |secret| byte_count(secret.as_str().len()));
                    let base = endpoint_bytes
                        .saturating_add(ref_bytes)
                        .saturating_add(header_change_bytes(&save.headers))
                        .saturating_add(secret_bytes);
                    save.models.iter().fold(
                        base.saturating_add(byte_count(save.connection.name.len())),
                        |bytes, model| {
                            bytes
                                .saturating_add(byte_count(model.id.len()))
                                .saturating_add(512)
                                .saturating_add(16)
                        },
                    )
                },
                OperationKind::CreateProfile,
                |(save,), batch| mutations::import_models(batch, save),
            )
        })
        .await
    }

    /// Archives a model and removes it from Agent choices, preserving frozen Jobs.
    /// The global custom request headers.
    pub async fn global_outbound_headers(
        &self,
    ) -> Result<crate::db::entities::GlobalHeaders, DatabaseError> {
        self.read(redb::read_global_outbound_headers).await
    }

    /// Replaces the global custom request headers if `expected_revision` is
    /// still current.
    pub async fn set_global_outbound_headers(
        &self,
        expected_revision: u32,
        fields: Vec<crate::db::entities::OutboundHeader>,
    ) -> Result<crate::db::entities::GlobalHeaders, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (expected_revision, fields),
                reply,
                |(_, fields)| {
                    byte_count(fields.len())
                        .saturating_mul(512)
                        .saturating_add(4096)
                },
                OperationKind::UpdateProfile,
                |(expected_revision, fields), batch| {
                    batch.save_global_headers(expected_revision, fields)
                },
            )
        })
        .await
    }

    /// A secret wrapped with the credential key, for a save to store.
    pub(crate) async fn credential_payload(
        &self,
        secret: redb::CredentialSecret,
    ) -> Result<mutations::CredentialPayload, DatabaseError> {
        let key = self.ensure_credential_key().await?;
        Ok(mutations::CredentialPayload { secret, key })
    }

    pub async fn global_compaction_settings(
        &self,
    ) -> Result<crate::db::entities::GlobalCompactionSettings, DatabaseError> {
        self.read(redb::read_global_compaction_settings).await
    }

    pub async fn set_global_compaction_settings(
        &self,
        settings: crate::db::entities::GlobalCompactionSettings,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (settings,),
                reply,
                |(settings,)| compaction_bytes(Some(&settings.policy)).saturating_add(64),
                OperationKind::UpdateProfile,
                |(settings,), batch| batch.set_global_compaction_settings(settings),
            )
        })
        .await
    }

    pub async fn archive_model(&self, id: ConfigId) -> Result<(), DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (id,),
                reply,
                |(_,)| 16,
                OperationKind::UpdateProfile,
                |(id,), batch| mutations::archive_model(batch, id),
            )
        })
        .await
    }

    /// Edits a config in place: a new name and body of the same kind, made
    /// from `expected_edit_seq`. It raises the edit counter and is refused
    /// (`ProfileChanged`, nothing written) when the stored counter differs;
    /// Jobs that froze the old values keep them.
    pub async fn update_config(
        &self,
        id: ConfigId,
        config: Config,
        expected_edit_seq: u32,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (id, config, expected_edit_seq),
                reply,
                |(_, config, _)| config_bytes(config),
                OperationKind::UpdateProfile,
                |(id, config, expected), batch| {
                    mutations::update_config(batch, id, config, expected)
                },
            )
        })
        .await
    }

    pub async fn decide_approval(
        &self,
        approval: ToolCallRef,
        decision: entities::ApprovalDecision,
    ) -> Result<Appended, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (approval, decision),
                reply,
                |(_, decision)| match decision {
                    entities::ApprovalDecision::Answered { text } => byte_count(text.len()),
                    entities::ApprovalDecision::Approved
                    | entities::ApprovalDecision::Rejected
                    | entities::ApprovalDecision::Voided => 64,
                },
                OperationKind::DecideApproval,
                |(approval, decision), batch| batch.decide(approval, decision),
            )
        })
        .await
    }
    pub async fn has_decider(&self, conversation: Option<JobId>) -> Result<bool, DatabaseError> {
        self.read(move |database| redb::read_has_decider(database, conversation))
            .await
    }
    pub(crate) async fn delegated_workspace(&self, job: JobId) -> Result<bool, DatabaseError> {
        self.read(move |database| redb::read_delegated_workspace(database, job))
            .await
    }
    pub async fn approved_calls(
        &self,
        conversation: JobId,
    ) -> Result<Vec<ToolEvidenceRecord>, DatabaseError> {
        self.read(move |database| redb::read_approved_calls(database, conversation))
            .await
    }
    pub(crate) async fn has_pending_approvals(
        &self,
        conversation: JobId,
    ) -> Result<bool, DatabaseError> {
        self.read(move |database| redb::read_has_pending_approvals(database, conversation))
            .await
    }
    pub async fn conversation_cards(
        &self,
        conversation: JobId,
    ) -> Result<Vec<serde_json::Value>, DatabaseError> {
        self.read(move |database| redb::read_cards(database, conversation))
            .await
    }
    pub async fn apply_job_tool(
        &self,
        call: ToolCallRef,
        tool: entities::JobToolCall,
    ) -> Result<serde_json::Value, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (call, tool),
                reply,
                |(_, tool)| {
                    use entities::JobToolCall;
                    let search = match tool {
                        JobToolCall::JobPlan { steps, .. } => steps
                            .iter()
                            .map(|step| {
                                search_index_bytes(&step.objective).saturating_add(64 * 1024)
                            })
                            .sum(),
                        JobToolCall::JobAssign { objective, .. }
                        | JobToolCall::JobDispatch { objective, .. } => {
                            search_index_bytes(objective)
                        }
                        JobToolCall::JobMessage { text, .. } => search_index_bytes(text),
                        JobToolCall::JobLimits { .. }
                        | JobToolCall::JobAgents { .. }
                        | JobToolCall::JobWorkers { .. }
                        | JobToolCall::JobStop { .. }
                        | JobToolCall::JobClose { .. }
                        | JobToolCall::JobHold { .. }
                        | JobToolCall::JobUnhold { .. }
                        | JobToolCall::JobRetry { .. } => 0,
                    };
                    debug_bytes(tool)
                        .saturating_add(4096)
                        .saturating_add(search)
                        .saturating_add(match tool {
                            JobToolCall::JobAssign { objective, .. }
                            | JobToolCall::JobDispatch { objective, .. }
                                if objective.len() <= 64 * 1024 =>
                            {
                                byte_count(objective.len())
                                    .saturating_mul(6)
                                    .saturating_add(64 * 1024)
                            }
                            JobToolCall::JobMessage { text, .. } if text.len() <= 64 * 1024 => {
                                byte_count(text.len())
                                    .saturating_mul(6)
                                    .saturating_add(64 * 1024)
                            }
                            _ => 0,
                        })
                },
                OperationKind::ApplyJobTool,
                |(call, tool), batch| batch.apply_job_tool(call, tool),
            )
        })
        .await
    }
    pub async fn boundary_b(&self, clock: i64) -> Result<(), DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (clock,),
                reply,
                |(_,)| 4096,
                OperationKind::BoundaryB,
                |(clock,), batch| batch.boundary_b(clock),
            )
        })
        .await
    }
    pub(crate) async fn recurrence_due(&self, clock: i64) -> Result<bool, DatabaseError> {
        self.read(move |database| redb::recurrence_due(database, clock))
            .await
    }
    pub async fn open_group(
        &self,
        parent: Option<JobId>,
        spec: JobGroup,
        first: Entry,
        configuration: ExecutionConfiguration,
        submission: Option<Submission>,
    ) -> Result<(JobId, Appended), DatabaseError> {
        self.open_group_with_format(parent, spec, first, configuration, submission, None, None)
            .await
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn open_group_with_format(
        &self,
        parent: Option<JobId>,
        spec: JobGroup,
        first: Entry,
        configuration: ExecutionConfiguration,
        submission: Option<Submission>,
        format: Option<crate::db::entities::ResponseFormat>,
        candidate: Option<AdmissionCandidate>,
    ) -> Result<(JobId, Appended), DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (
                    parent,
                    spec,
                    first,
                    configuration,
                    submission,
                    format,
                    candidate,
                ),
                reply,
                |(_, _, first, _, _, format, _)| entry_bytes(first, format.as_ref()),
                OperationKind::Open,
                |(parent, spec, first, configuration, submission, format, candidate), batch| {
                    batch.with_admission_candidate(candidate, submission, |batch| {
                        batch.open_group_with_format(
                            parent,
                            spec,
                            first,
                            configuration,
                            submission,
                            Vec::new(),
                            format,
                        )
                    })
                },
            )
        })
        .await
    }
    pub(crate) async fn context_state(
        &self,
        conversation: JobId,
        before: Option<JobId>,
        limit: usize,
    ) -> Result<Option<redb::ContextState>, DatabaseError> {
        self.read(move |database| redb::read_context_state(database, conversation, before, limit))
            .await
    }
    pub(crate) async fn preparation_current(&self, job: JobId) -> Result<bool, DatabaseError> {
        self.read(move |database| redb::read_preparation_current(database, job))
            .await
    }
    pub(crate) async fn prediction_current(&self, job: JobId) -> Result<bool, DatabaseError> {
        self.read(move |database| redb::read_prediction_current(database, job))
            .await
    }
    pub(crate) async fn promote_prediction(
        &self,
        job: JobId,
    ) -> Result<Option<JobId>, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (job,),
                reply,
                |(_,)| 4096 * 512,
                OperationKind::Open,
                |(job,), batch| batch.promote_prediction(job),
            )
        })
        .await
    }
    pub(crate) async fn predict_context(
        &self,
        conversation: JobId,
    ) -> Result<Option<JobId>, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (conversation,),
                reply,
                |(_,)| 131072,
                OperationKind::Open,
                |(conversation,), batch| batch.predict_context(conversation),
            )
        })
        .await
    }
    pub(crate) async fn effective_compaction_policy(
        &self,
        conversation: JobId,
    ) -> Result<Option<(entities::CompactionPolicy, u64, Option<entities::ConfigRef>)>, DatabaseError>
    {
        self.read(move |database| redb::read_effective_context_policy(database, conversation))
            .await
    }
    pub(crate) async fn summarization_input(
        &self,
        job: JobId,
    ) -> Result<Option<redb::SummarizationInput>, DatabaseError> {
        self.read(move |database| redb::read_summarization_input(database, job))
            .await
    }
    /// Opens a system conversation with one typed decision turn and freezes
    /// its Decision Model and Profile in the same transaction.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn submit_decision(
        &self,
        parent: Option<JobId>,
        spec: JobGroup,
        first: Entry,
        configuration: ExecutionConfiguration,
        request: entities::DecisionRequest,
        submission: Option<Submission>,
        candidate: AdmissionCandidate,
    ) -> Result<(JobId, Appended), DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (
                    parent,
                    spec,
                    first,
                    configuration,
                    request,
                    submission,
                    candidate,
                ),
                reply,
                |(_, _, first, _, request, _, _)| {
                    debug_bytes(first).saturating_add(debug_bytes(request))
                },
                OperationKind::Open,
                |(parent, spec, first, configuration, request, submission, candidate), batch| {
                    batch.with_admission_candidate(Some(candidate), submission, |batch| {
                        batch.open_decision(parent, spec, first, configuration, request, submission)
                    })
                },
            )
        })
        .await
    }
    /// Requests one manual compaction of `conversation` through generative
    /// `model`. The submission key returns the same Job on repeat. A source
    /// that will not fit the model's window is refused, not truncated.
    pub async fn request_compaction(
        &self,
        conversation: JobId,
        model: ConfigId,
        submission: Submission,
    ) -> Result<entities::CompactionRequest, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (conversation, model, submission),
                reply,
                |(_, _, _)| 4096 * 512,
                OperationKind::Open,
                |(conversation, model, submission), batch| {
                    batch.request_compaction(conversation, model, submission)
                },
            )
        })
        .await
    }

    pub async fn append(
        &self,
        conversation: JobId,
        entry: Entry,
        freeze: Option<ExecutionConfiguration>,
        submission: Option<Submission>,
    ) -> Result<Appended, DatabaseError> {
        self.append_with_format(conversation, entry, freeze, submission, None, None)
            .await
    }
    pub(crate) async fn append_with_format(
        &self,
        conversation: JobId,
        entry: Entry,
        freeze: Option<ExecutionConfiguration>,
        submission: Option<Submission>,
        format: Option<crate::db::entities::ResponseFormat>,
        candidate: Option<AdmissionCandidate>,
    ) -> Result<Appended, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (conversation, entry, freeze, submission, format, candidate),
                reply,
                |(_, first, _, _, format, _)| entry_bytes(first, format.as_ref()),
                OperationKind::Append,
                |(conversation, entry, freeze, submission, format, candidate), batch| {
                    batch.with_admission_candidate(candidate, submission, |batch| {
                        batch.require_agent_group(conversation)?;
                        batch.append_with_format(
                            conversation,
                            entry,
                            freeze,
                            submission,
                            false,
                            format,
                        )
                    })
                },
            )
        })
        .await
    }
    pub(crate) async fn amend_queued_input(
        &self,
        conversation: JobId,
        turn: JobId,
        entry: Entry,
    ) -> Result<Appended, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (conversation, turn, entry),
                reply,
                |(_, _, entry)| {
                    debug_bytes(entry)
                        .saturating_add(match entry {
                            Entry::UserMessageEdit {
                                text, attachments, ..
                            } => search_index_bytes(text).saturating_add(
                                (attachments.len() as u64).saturating_mul(
                                    3 * 64
                                        * (crate::db::entities::MAX_ATTACHMENT_NAME_BYTES as u64
                                            + 1),
                                ),
                            ),
                            Entry::UserMessage { .. }
                            | Entry::UserMessageWithdraw { .. }
                            | Entry::AgentMessage { .. }
                            | Entry::Notice { .. }
                            | Entry::Decision { .. }
                            | Entry::RetryOf { .. }
                            | Entry::Iteration { .. }
                            | Entry::ContextPreparation { .. }
                            | Entry::ContextUse { .. }
                            | Entry::ResponseFormat { .. } => 0,
                        })
                        .saturating_add(4096)
                },
                OperationKind::Append,
                |(conversation, turn, entry), batch| {
                    batch.amend_queued_input(conversation, turn, entry)
                },
            )
        })
        .await
    }
    pub async fn stop_turn(
        &self,
        conversation: JobId,
        by: Actor,
    ) -> Result<JobAction, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (conversation, by),
                reply,
                |(_, _)| 4096,
                OperationKind::StopTurn,
                |(conversation, by), batch| batch.stop_turn(conversation, by),
            )
        })
        .await
    }
    pub async fn close(&self, conversation: JobId, by: Actor) -> Result<JobAction, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (conversation, by),
                reply,
                |(_, _)| 4096,
                OperationKind::Close,
                |(conversation, by), batch| batch.close(conversation, by),
            )
        })
        .await
    }
    pub async fn hold(&self, conversation: JobId, by: Actor) -> Result<JobAction, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (conversation, by),
                reply,
                |(_, _)| 4096,
                OperationKind::Hold,
                |(conversation, by), batch| batch.hold(conversation, by, false),
            )
        })
        .await
    }
    pub async fn unhold(&self, conversation: JobId, by: Actor) -> Result<JobAction, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (conversation, by),
                reply,
                |(_, _)| 4096,
                OperationKind::Unhold,
                |(conversation, by), batch| batch.hold(conversation, by, true),
            )
        })
        .await
    }
    pub async fn retry(
        &self,
        conversation: JobId,
        turn: JobId,
        by: Actor,
    ) -> Result<Appended, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (conversation, turn, by),
                reply,
                |(_, _, _)| 4096,
                OperationKind::Retry,
                |(conversation, turn, by), batch| batch.retry(conversation, turn, by),
            )
        })
        .await
    }

    pub async fn append_job_output(
        &self,
        job: JobId,
        output: JobOutput,
    ) -> Result<AppendedOutput, DatabaseError> {
        self.submit(|reply| {
            Command::custom(
                (job, output),
                reply,
                |(_, output)| {
                    output_bytes(
                        output.summary.as_deref(),
                        output.code.as_deref(),
                        output.value.as_ref(),
                    )
                },
                |(job, output), reply, database, operation, commit| {
                    use OperationKind as Kind;
                    {
                        let mut result = run(
                            database,
                            operation,
                            commit,
                            Kind::AppendJobOutput,
                            reply,
                            |batch| mutations::append_job_output(batch, job, output),
                        );
                        if result.committed {
                            result.output_changed = Some(job);
                        }
                        result
                    }
                },
            )
        })
        .await
    }

    pub(crate) fn with_stop_cause(
        &self,
        cause: Arc<std::sync::Mutex<Option<crate::db::entities::StopCause>>>,
    ) -> Self {
        let mut handle = self.clone();
        handle.stop_cause = Some(cause);
        handle.ignore_stop_cause = false;
        handle
    }

    /// Whether both handles reach the same database.
    pub(crate) fn same_database(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }

    pub(crate) fn without_stop_cause(&self) -> Self {
        let mut handle = self.clone();
        handle.stop_cause = None;
        handle.ignore_stop_cause = true;
        handle
    }

    pub async fn finalize_job(
        &self,
        job: JobId,
        finalization: JobFinalization,
    ) -> Result<Option<AppendedOutput>, DatabaseError> {
        self.finalize_job_with_provider_evidence(job, finalization, None)
            .await
    }

    pub(crate) async fn finalize_job_with_provider_evidence(
        &self,
        job: JobId,
        finalization: JobFinalization,
        evidence: Option<ProviderSettlementEvidence>,
    ) -> Result<Option<AppendedOutput>, DatabaseError> {
        let cause = self
            .stop_cause
            .as_ref()
            .and_then(|cause| *cause.lock().unwrap_or_else(PoisonError::into_inner))
            .map(|cause| match cause {
                crate::db::entities::StopCause::Stopped(record) => {
                    crate::db::entities::OutcomeCause::Stopped { record }
                }
                crate::db::entities::StopCause::Closed => crate::db::entities::OutcomeCause::Closed,
            });
        self.submit(|reply| {
            Command::custom(
                (job, cause, self.ignore_stop_cause, finalization, evidence),
                reply,
                |(_, _, _, finalization, evidence)| {
                    let evidence_bytes = match evidence {
                        None => 0,
                        Some(ProviderSettlementEvidence::Answer(answer)) => {
                            byte_count(answer.response.byte_length())
                                .saturating_add(json_bytes(answer.measurements.usage.raw.as_ref()))
                        }
                        Some(ProviderSettlementEvidence::Observations(stats)) => {
                            stats.iter().fold(0u64, |bytes, stat| {
                                let observation = &stat.observation;
                                let failure = observation.failure.as_ref().map_or(0, |failure| {
                                    byte_count(failure.message.len())
                                        .saturating_add(text_bytes(
                                            failure.provider_code.as_deref(),
                                        ))
                                        .saturating_add(text_bytes(
                                            failure.response_body.as_deref(),
                                        ))
                                        .saturating_add(text_bytes(failure.request_id.as_deref()))
                                });
                                bytes.saturating_add(failure).saturating_add(text_bytes(
                                    observation.raw_finish_reason.as_deref(),
                                ))
                            })
                        }
                    };
                    evidence_bytes.saturating_add(match finalization {
                        JobFinalization::Completed(result)
                        | JobFinalization::Failed(result)
                        | JobFinalization::Cancelled(result) => {
                            let text = result
                                .value
                                .as_ref()
                                .and_then(|value| value.get("text"))
                                .and_then(serde_json::Value::as_str)
                                .or(result.summary.as_deref());
                            output_bytes(
                                result.summary.as_deref(),
                                result.code.as_deref(),
                                result.value.as_ref(),
                            )
                            .saturating_add(64 * 1024)
                            .saturating_add(text.map_or(0, search_index_bytes))
                        }
                        JobFinalization::Unknown => 0,
                    })
                },
                |(job, cause, ignore_stop_cause, finalization, evidence),
                 reply,
                 database,
                 operation,
                 commit| {
                    use OperationKind as Kind;
                    {
                        let mut changes = Vec::new();
                        let mut result = run(
                            database,
                            operation,
                            commit,
                            Kind::FinalizeJob,
                            reply,
                            |batch| {
                                let provider_settlement = evidence.is_some();
                                let abandoned = provider_settlement
                                    && batch.require_job_state(job)?.outcome.is_some_and(
                                        |outcome| {
                                            outcome.cause == Some(entities::OutcomeCause::Abandoned)
                                        },
                                    );
                                let output = if abandoned {
                                    None
                                } else {
                                    mutations::finalize_job(
                                        batch,
                                        job,
                                        finalization,
                                        cause,
                                        ignore_stop_cause,
                                    )?
                                };
                                let mut probe = None;
                                match evidence {
                                    None => {}
                                    Some(ProviderSettlementEvidence::Observations(
                                        observations,
                                    )) => {
                                        for stat in observations {
                                            if stat.job != job {
                                                return batch.reject(Violation::InvalidValue(
                                                    "job",
                                                    "observation belongs to a different job",
                                                ));
                                            }
                                            let id =
                                                mutations::record_availability_stat(batch, stat)?;
                                            changes.push(ObservationChange::Availability(job, id));
                                            probe = Some(id);
                                        }
                                    }
                                    Some(ProviderSettlementEvidence::Answer(answer)) => {
                                        if answer.stat.job != job {
                                            return batch.reject(Violation::InvalidValue(
                                                "job",
                                                "answer belongs to a different job",
                                            ));
                                        }
                                        let (metrics, availability) =
                                            mutations::record_provider_answer(batch, *answer)?;
                                        changes.push(ObservationChange::Metrics(job, metrics));
                                        changes.push(ObservationChange::Availability(
                                            job,
                                            availability,
                                        ));
                                        probe = Some(availability);
                                    }
                                }
                                if provider_settlement {
                                    batch.provider_settlement_probe(job, probe)?;
                                }
                                Ok(output)
                            },
                        );
                        if result.committed {
                            result.observations = changes;
                        }
                        result
                    }
                },
            )
        })
        .await
    }

    pub async fn create_executor(&self, executor: Executor) -> Result<ExecutorId, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (executor,),
                reply,
                |(executor,)| byte_count(executor.name.len()),
                OperationKind::CreateExecutor,
                |(executor,), batch| mutations::create_executor(batch, executor),
            )
        })
        .await
    }

    pub async fn create_project(&self, project: Project) -> Result<ProjectId, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (project,),
                reply,
                |(project,)| {
                    byte_count(project.name.len())
                        .saturating_add(text_bytes(project.description.as_deref()))
                        .saturating_add(byte_count(project.root.len()))
                        .saturating_add(command_bytes(&project.commands))
                },
                OperationKind::CreateProject,
                |(project,), batch| mutations::create_project(batch, project),
            )
        })
        .await
    }

    pub(crate) async fn assess_command_grant(
        &self,
        call: ToolCallRef,
    ) -> Result<Option<JobId>, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (call,),
                reply,
                |(_,)| 128 * 1024,
                OperationKind::UpdateProject,
                |(call,), batch| batch.assess_command_grant(call),
            )
        })
        .await
    }

    pub(crate) async fn configure_project_decision(
        &self,
        id: ProjectId,
        expected: crate::db::entities::Timestamp,
        assignment: Option<entities::ProjectDecisionConfig>,
    ) -> Result<entities::ProjectRecord, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (id, expected, assignment),
                reply,
                |(_, _, _)| 32 * 1024,
                OperationKind::UpdateProject,
                |(id, expected, assignment), batch| {
                    mutations::configure_project_decision(batch, id, expected, assignment)
                },
            )
        })
        .await
    }

    /// Replaces a Project's name, description and commands. Its root never
    /// changes.
    pub async fn update_project(
        &self,
        id: ProjectId,
        name: String,
        description: Option<String>,
        commands: Vec<ProjectCommand>,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (id, name, description, commands),
                reply,
                |(_, name, description, commands)| {
                    byte_count(name.len())
                        .saturating_add(text_bytes(description.as_deref()))
                        .saturating_add(command_bytes(commands))
                },
                OperationKind::UpdateProject,
                |(id, name, description, commands), batch| {
                    mutations::update_project(batch, id, name, description, commands)
                },
            )
        })
        .await
    }

    /// Renames or hides the conversation whose root Job is `root`.
    pub async fn set_conversation(&self, root: JobId, edit: JobEdit) -> Result<(), DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (root, edit),
                reply,
                |(_, edit)| match edit {
                    JobEdit::Title(title) | JobEdit::SuggestedTitle(title) => {
                        byte_count(title.len())
                    }
                    JobEdit::Hide | JobEdit::Limits(_) => 16,
                    JobEdit::CompactionPolicy(policy) => compaction_bytes(policy.as_ref()),
                },
                OperationKind::SetConversation,
                |(root, edit), batch| mutations::set_conversation(batch, root, edit),
            )
        })
        .await
    }

    /// Hides the conversation and closes its live subtree in the same transaction.
    pub async fn hide_conversation(&self, root: JobId) -> Result<JobAction, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (root,),
                reply,
                |_| 4096,
                OperationKind::Close,
                |(root,), batch| {
                    mutations::set_conversation(batch, root, JobEdit::Hide)?;
                    batch.close(root, Actor::User)
                },
            )
        })
        .await
    }

    /// Stores `attachment` as a new user-provided Attachment. Its content is
    /// published durably first, see [`crate::db::attachments`]; only then is the
    /// record committed. When the commit fails, the published content stays,
    /// named by no Attachment, and a later publication of the same bytes
    /// reuses it. Nothing is published while commands are not admitted.
    ///
    /// The content is pinned from before it is published until the commit's
    /// outcome is known, so no cleanup reclaims it meanwhile; a returned
    /// Attachment always has its content. When the outcome is unknown, the
    /// content stays pinned until the process ends. Publishing waits, on the
    /// caller's thread like the rest of the publication, while a cleanup
    /// reclaims content.
    pub async fn create_user_attachment(
        &self,
        attachment: Attachment,
    ) -> Result<AttachmentId, AttachmentError> {
        mutations::check_new_attachment(&attachment).map_err(DatabaseError::Rejected)?;
        admitted(
            &self
                .shared
                .admission
                .read()
                .unwrap_or_else(PoisonError::into_inner),
        )?;
        let permit = self.attachment_permit().await?;
        let (handoff_tx, handoff_rx) = flume::bounded(1);
        let (settle_tx, settle_rx) = flume::bounded(1);
        let submitted = Arc::new(AtomicBool::new(false));
        let submitted_for_worker = submitted.clone();
        let shared = self.shared.clone();
        let blocking = ntex::rt::spawn_blocking(move || {
            let pin = match shared.attachments.publish(&attachment.content) {
                Ok(pin) => pin,
                Err(error) => {
                    let _ = handoff_tx.send(Err(AttachmentError::NotStored(error)));
                    drop(permit);
                    return;
                }
            };
            let published = PublishedAttachment {
                sha256: pin.digest(),
                byte_length: attachment.content.len() as u64,
                mime: attachment.mime,
                display_name: attachment.display_name,
                origin: AttachmentOrigin::UserProvided,
            };
            if handoff_tx.send(Ok(published)).is_err() {
                // The awaiting async future was cancelled while publication was in progress.
                // Database submission certainly never occurred.
                pin.release();
                drop(permit);
                return;
            }
            match settle_rx.recv() {
                Ok(true) => {
                    // Settled commit outcome (committed or known non-committed): release pin.
                    pin.release();
                }
                Ok(false) => {
                    // Unknown commit outcome: retain pin.
                }
                Err(_) => {
                    // Async caller dropped / cancelled.
                    if !submitted_for_worker.load(Ordering::Acquire) {
                        // Certainly not submitted: release pin.
                        pin.release();
                    }
                    // If submitted, outcome is unknown: retain pin without release.
                }
            }
            drop(permit);
        });
        let published = match handoff_rx.recv_async().await {
            Ok(Ok(published)) => published,
            Ok(Err(error)) => {
                let _ = blocking.await;
                return Err(error);
            }
            Err(_) => {
                let _ = blocking.await;
                return Err(DatabaseError::WorkerPanicked.into());
            }
        };
        let created = self
            .submit(|reply| {
                submitted.store(true, Ordering::Release);
                Command::new(
                    (Box::new(published),),
                    reply,
                    |(attachment,)| {
                        byte_count(attachment.mime.len())
                            .saturating_add(text_bytes(attachment.display_name.as_deref()))
                    },
                    OperationKind::CreateAttachment,
                    |(attachment,), batch| mutations::create_attachment(batch, *attachment),
                )
            })
            .await;
        // Committed, and durable, so every later read sees the record that
        // now protects the content; or certainly not committed.
        let settled = match &created {
            Ok(_) => true,
            Err(error) => matches!(
                error.command_outcome(),
                Some(CommandOutcome::NotAccepted | CommandOutcome::NotCommitted)
            ),
        };
        let _ = settle_tx.send(settled);
        let _ = blocking.await;
        Ok(created?)
    }

    pub async fn attachment(
        &self,
        id: AttachmentId,
    ) -> Result<Option<AttachmentRecord>, DatabaseError> {
        self.read(move |database| redb::read_attachment(database, id))
            .await
    }

    /// Attachments in reverse ID order; newest first, cleaned up or not.
    /// Strictly before `before` when given; at most 1,000 per call.
    pub async fn attachments(
        &self,
        before: Option<AttachmentId>,
        limit: usize,
    ) -> Result<Vec<AttachmentRecord>, DatabaseError> {
        self.read(move |database| redb::read_attachments(database, before, limit))
            .await
    }

    /// An available Attachment with its content, read from the attachment
    /// store and verified against the record's length and SHA-256. Content
    /// that is missing or does not match is refused, never replaced.
    pub async fn attachment_content(
        &self,
        id: AttachmentId,
    ) -> Result<AttachmentContent, AttachmentError> {
        let record = self
            .attachment(id)
            .await?
            .ok_or(AttachmentError::NotFound(id))?;
        if let Some(cleanup) = record.cleanup {
            return Err(AttachmentError::CleanedUp { id, cleanup });
        }
        let permit = self.attachment_permit().await?;
        let shared = self.shared.clone();
        let digest = record.sha256;
        let length = record.byte_length;
        let loaded = ntex::rt::spawn_blocking(move || {
            let result = shared.attachments.load(digest, length);
            drop(permit);
            result
        })
        .await
        .map_err(|_| DatabaseError::WorkerPanicked)?;
        let bytes = match loaded {
            Ok(bytes) => bytes,
            // Reclaimed since the record was read, by a cleanup that
            // committed meanwhile; otherwise the file is truly missing.
            Err(StoreError::Missing) => {
                return Err(match self.attachment(id).await?.and_then(|r| r.cleanup) {
                    Some(cleanup) => AttachmentError::CleanedUp { id, cleanup },
                    None => AttachmentError::Content {
                        id,
                        error: StoreError::Missing,
                    },
                });
            }
            Err(error) => return Err(AttachmentError::Content { id, error }),
        };
        Ok(AttachmentContent { record, bytes })
    }

    /// The operator's manual cleanup of an Attachment: records that it was
    /// cleaned up, [`CleanupKind::Manual`] at the database's time, in one
    /// commit, then reclaims its content file unless another available
    /// Attachment, or a publication in progress, still needs that content;
    /// see [`crate::db::attachments`]. The Attachment keeps its identity and
    /// every other field, and every input keeps naming it; from the commit
    /// on, its content is refused to every new read and to every Job whose
    /// execution has not yet read it. Bytes already read, or already sent to
    /// a Provider, are not recalled.
    ///
    /// An Attachment already cleaned up, manually or automatically, keeps
    /// its cleanup: nothing is recorded, and only the reclamation of its
    /// content is tried again. The report says which, and what became of
    /// the content file; a failed reclamation does not undo the cleanup.
    ///
    /// # Errors
    ///
    /// [`AttachmentError::NotFound`] for an unknown Attachment, or why the
    /// cleanup was not recorded.
    pub async fn cleanup_attachment(
        &self,
        id: AttachmentId,
    ) -> Result<AttachmentCleanupReport, AttachmentError> {
        let record = self
            .attachment(id)
            .await?
            .ok_or(AttachmentError::NotFound(id))?;
        let (record, recorded) = if record.is_available() {
            let kind = CleanupKind::Manual;
            match self
                .submit(|reply| {
                    Command::new(
                        (id, kind),
                        reply,
                        |(_, _)| 0,
                        OperationKind::CleanupAttachment,
                        |(id, kind), batch| mutations::cleanup_attachment(batch, id, kind),
                    )
                })
                .await
            {
                Ok(record) => (record, true),
                // Cleaned up meanwhile by another cleanup, which is kept.
                Err(DatabaseError::Rejected(Violation::AttachmentCleanedUp(_))) => (
                    self.attachment(id)
                        .await?
                        .ok_or(AttachmentError::NotFound(id))?,
                    false,
                ),
                Err(error) => return Err(error.into()),
            }
        } else {
            (record, false)
        };
        let content = self.reclaim(record.sha256, record.byte_length).await;
        Ok(AttachmentCleanupReport {
            record,
            recorded,
            content,
        })
    }

    /// Reclaims the content `digest` on a read worker, so that whether an
    /// available Attachment still names it is read from committed, durable
    /// records while the store holds its pins' lock.
    async fn reclaim(
        &self,
        digest: ContentDigest,
        length: u64,
    ) -> Result<Reclamation, ReclamationError> {
        let shared = self.shared.clone();
        self.read(move |database| {
            shared
                .attachments
                .reclaim(digest, length, || redb::content_available(database, digest))
        })
        .await
        .map_err(ReclamationError::Database)?
        .map_err(ReclamationError::Store)
    }

    /// Records that an unknown-outcome Job's execution has actually stopped,
    /// releasing the slot it holds; the outcome stays unknown. Only the
    /// trusted execution path that observed the termination may call this,
    /// so it is not part of the public API.
    pub(crate) async fn confirm_job_stopped(
        &self,
        job: JobId,
        proof: StopProof,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (job, proof),
                reply,
                |(_, _)| 0,
                OperationKind::ConfirmJobStopped,
                |(job, proof), batch| mutations::confirm_job_stopped(batch, job, proof),
            )
        })
        .await
    }

    /// Appends measurements for a Job. Their LLM is the one the Job froze.
    pub async fn record_metrics(&self, metrics: Metrics) -> Result<MetricsId, DatabaseError> {
        self.submit(|reply| {
            Command::custom(
                (Box::new(metrics),),
                reply,
                |(metrics,)| json_bytes(metrics.measurements.usage.raw.as_ref()),
                |(metrics,), reply, database, operation, commit| {
                    use OperationKind as Kind;
                    {
                        let job = metrics.job;
                        let id = std::cell::Cell::new(None);
                        let mut result = run(
                            database,
                            operation,
                            commit,
                            Kind::RecordMetrics,
                            reply,
                            |batch| {
                                let value = mutations::record_metrics(batch, *metrics)?;
                                id.set(Some(value));
                                Ok(value)
                            },
                        );
                        if let Some(id) = id.get() {
                            result
                                .observations
                                .push(ObservationChange::Metrics(job, id));
                        }
                        result
                    }
                },
            )
        })
        .await
    }

    pub async fn record_sent_request(&self, sent: SentRequest) -> Result<(), DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (Box::new(sent),),
                reply,
                |(sent,)| {
                    sent.bodies.iter().fold(
                        byte_count(sent.messages.len()).saturating_mul(32),
                        |bytes, body| bytes.saturating_add(byte_count(body.byte_length())),
                    )
                },
                OperationKind::RecordSentRequest,
                |(sent,), batch| mutations::record_sent_request(batch, *sent),
            )
        })
        .await
    }

    pub async fn record_provider_answer(
        &self,
        answer: ProviderAnswer,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| {
            Command::custom(
                (Box::new(answer),),
                reply,
                |(answer,)| {
                    byte_count(answer.response.byte_length())
                        .saturating_add(json_bytes(answer.measurements.usage.raw.as_ref()))
                },
                |(answer,), reply, database, operation, commit| {
                    use OperationKind as Kind;
                    {
                        let job = answer.stat.job;
                        let ids = std::cell::Cell::new(None);
                        let mut result = run(
                            database,
                            operation,
                            commit,
                            Kind::RecordProviderAnswer,
                            reply,
                            |batch| {
                                let value = mutations::record_provider_answer(batch, *answer)?;
                                ids.set(Some(value));
                                Ok(())
                            },
                        );
                        if let Some((metrics, availability)) = ids.get() {
                            result
                                .observations
                                .push(ObservationChange::Metrics(job, metrics));
                            result
                                .observations
                                .push(ObservationChange::Availability(job, availability));
                        }
                        result
                    }
                },
            )
        })
        .await
    }

    pub(crate) async fn provider_responses(
        &self,
        job: JobId,
        after: Option<RequestNumber>,
        limit: usize,
        max_text_bytes: usize,
    ) -> Result<redb::ProviderResponsesPage, DatabaseError> {
        self.read(move |database| {
            redb::read_provider_responses(database, job, after, limit, max_text_bytes)
        })
        .await
    }

    pub async fn provider_request(
        &self,
        job: JobId,
        request: RequestNumber,
    ) -> Result<Option<ProviderRequestRecord>, DatabaseError> {
        self.read(move |database| redb::read_provider_request(database, job, request))
            .await
    }

    /// Appends one availability observation of an LLM request a configured
    /// Job sent. Its LLM is the one the Job froze.
    pub async fn record_availability_stat(
        &self,
        stat: AvailabilityStat,
    ) -> Result<AvailabilityStatId, DatabaseError> {
        self.submit(|reply| {
            Command::custom(
                (Box::new(stat),),
                reply,
                |(stat,)| {
                    let observation = &stat.observation;
                    let failure = observation.failure.as_ref().map_or(0, |failure| {
                        byte_count(failure.message.len())
                            .saturating_add(text_bytes(failure.provider_code.as_deref()))
                            .saturating_add(text_bytes(failure.response_body.as_deref()))
                            .saturating_add(text_bytes(failure.request_id.as_deref()))
                    });
                    failure.saturating_add(text_bytes(observation.raw_finish_reason.as_deref()))
                },
                |(stat,), reply, database, operation, commit| {
                    use OperationKind as Kind;
                    {
                        let job = stat.job;
                        let id = std::cell::Cell::new(None);
                        let mut result = run(
                            database,
                            operation,
                            commit,
                            Kind::RecordAvailabilityStat,
                            reply,
                            |batch| {
                                let value = mutations::record_availability_stat(batch, *stat)?;
                                id.set(Some(value));
                                Ok(value)
                            },
                        );
                        if let Some(id) = id.get() {
                            result
                                .observations
                                .push(ObservationChange::Availability(job, id));
                        }
                        result
                    }
                },
            )
        })
        .await
    }

    /// Records OCG's refusal or admission of a tool call a Provider
    /// requested, before anything about the call can run. Only the trusted
    /// execution path of the Job, while it executes it, may record it; a
    /// handler is invoked only once its admission is durable, which is when
    /// this returns `Ok`. See [`mutations::record_tool_decision`].
    pub(crate) async fn record_tool_decision(
        &self,
        evidence: ToolEvidence,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (Box::new(evidence),),
                reply,
                |(evidence,)| {
                    let identity = byte_count(evidence.tool_call_id.len())
                        .saturating_add(byte_count(evidence.name.len()));
                    match evidence.decision.stored_admission() {
                        None => identity,
                        Some(admission) => identity
                            .saturating_add(byte_count(admission.handler_version.len()))
                            .saturating_add(byte_count(admission.parameters.len()))
                            .saturating_add(byte_count(admission.arguments.len())),
                    }
                },
                OperationKind::RecordToolDecision,
                |(evidence,), batch| mutations::record_tool_decision(batch, *evidence),
            )
        })
        .await
    }

    /// Records, once, what is known of an admitted call's execution. Only
    /// the trusted execution path of the Job, while it executes it, may
    /// record it.
    pub(crate) async fn record_tool_outcome(
        &self,
        call: ToolCallRef,
        outcome: ToolOutcome,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (call, Box::new(outcome)),
                reply,
                |(_, outcome)| match outcome.as_ref() {
                    ToolOutcome::Completed { result } => byte_count(result.len()),
                    ToolOutcome::NotApplied { message } => byte_count(message.len()),
                    ToolOutcome::Uncertain { message, .. } => text_bytes(message.as_deref()),
                    ToolOutcome::CompletedWithheld { .. } | ToolOutcome::NotInvoked { .. } => 0,
                },
                OperationKind::RecordToolOutcome,
                |(call, outcome), batch| mutations::record_tool_outcome(batch, call, *outcome),
            )
        })
        .await
    }

    /// [`Self::record_tool_decision`] for the backend benchmark, which drives
    /// the trusted execution path itself. Not part of the API.
    #[doc(hidden)]
    pub async fn bench_record_tool_decision(
        &self,
        evidence: ToolEvidence,
    ) -> Result<(), DatabaseError> {
        self.record_tool_decision(evidence).await
    }

    /// [`Self::record_tool_outcome`] for the backend benchmark, which drives
    /// the trusted execution path itself. Not part of the API.
    #[doc(hidden)]
    pub async fn bench_record_tool_outcome(
        &self,
        call: ToolCallRef,
        outcome: ToolOutcome,
    ) -> Result<(), DatabaseError> {
        self.record_tool_outcome(call, outcome).await
    }

    /// A Job's tool evidence in call order, after the call named by its
    /// request and index when given; at most 1,000 per call.
    pub async fn tool_evidence(
        &self,
        job: JobId,
        after: Option<(RequestNumber, u32)>,
        limit: usize,
    ) -> Result<Vec<ToolEvidenceRecord>, DatabaseError> {
        self.read(move |database| redb::read_tool_evidence(database, job, after, limit))
            .await
    }
}

impl DatabaseHandle {
    pub async fn create_execution_pool(
        &self,
        pool: ExecutionPool,
    ) -> Result<ExecutionPoolId, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (pool,),
                reply,
                |(pool,)| byte_count(pool.name.len()),
                OperationKind::CreateExecutionPool,
                |(pool,), batch| mutations::create_execution_pool(batch, pool),
            )
        })
        .await
    }

    pub async fn set_execution_pool_enabled(
        &self,
        id: ExecutionPoolId,
        enabled: bool,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (id, enabled),
                reply,
                |(_, _)| 64,
                OperationKind::SetExecutionPoolEnabled,
                |(id, enabled), batch| mutations::set_execution_pool_enabled(batch, id, enabled),
            )
        })
        .await
    }

    /// Replaces a Pool's capabilities and concurrency limit.
    pub async fn update_execution_pool(
        &self,
        id: ExecutionPoolId,
        config: ExecutionPoolConfig,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (id, config),
                reply,
                |(_, _)| 64,
                OperationKind::SetExecutionPoolProfile,
                |(id, config), batch| mutations::update_execution_pool(batch, id, config),
            )
        })
        .await
    }

    pub async fn execution_pool(
        &self,
        id: ExecutionPoolId,
    ) -> Result<Option<ExecutionPoolRecord>, DatabaseError> {
        self.read(move |database| redb::read_execution_pool(database, id))
            .await
    }

    pub async fn execution_pools(
        &self,
        before: Option<ExecutionPoolId>,
        limit: usize,
    ) -> Result<Vec<ExecutionPoolRecord>, DatabaseError> {
        self.read(move |database| redb::read_execution_pools(database, before, limit))
            .await
    }

    async fn ensure_credential_key(&self) -> Result<Arc<redb::CredentialKey>, DatabaseError> {
        if let Some(key) = self.shared.credential_key() {
            return Ok(key);
        }
        {
            let admission = self
                .shared
                .admission
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            admitted(&admission)?;
            if self.shared.update_paused.load(Ordering::SeqCst) {
                return Err(DatabaseError::ShuttingDown);
            }
        }
        let shared = self.shared.clone();
        ntex::rt::spawn_blocking(move || shared.ensure_credential_key())
            .await
            .map_err(|_| DatabaseError::WorkerPanicked)?
            .map_err(DatabaseError::RolledBack)
    }

    pub(crate) async fn resolve_credential(
        &self,
        reference: &str,
    ) -> Result<Option<String>, crate::service::ai::providers::ProviderExecutionError> {
        use crate::service::ai::providers::{
            CredentialResolver, EnvironmentCredentialResolver, ProviderExecutionError,
        };
        let reference = reference.trim();
        if reference.is_empty() {
            return Ok(None);
        }
        let name = reference.to_owned();
        let key = self.shared.credential_key();
        let stored = self
            .read(move |database| redb::read_credential(database, &name, key.as_deref()))
            .await
            .map_err(|error| ProviderExecutionError::Build {
                message: format!("credential store could not be read: {error}"),
            })?;
        match stored {
            redb::StoredCredential::Decrypted(secret) => Ok(Some(secret.into_inner())),
            redb::StoredCredential::Undecryptable => Err(ProviderExecutionError::Build {
                message: "stored credential cannot be decrypted; enter the key again".to_owned(),
            }),
            redb::StoredCredential::Absent => {
                EnvironmentCredentialResolver.resolve(Some(reference)).await
            }
        }
    }

    pub fn credential_resolver(
        &self,
    ) -> Arc<dyn crate::service::ai::providers::CredentialResolver> {
        Arc::new(LocalCredentialResolver {
            database: self.clone(),
        })
    }
}

struct LocalCredentialResolver {
    database: DatabaseHandle,
}

impl crate::service::ai::providers::CredentialResolver for LocalCredentialResolver {
    fn resolve<'a>(
        &'a self,
        credential_ref: Option<&'a str>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<
                        Option<String>,
                        crate::service::ai::providers::ProviderExecutionError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let Some(name) = credential_ref else {
                return Ok(None);
            };
            self.database.resolve_credential(name).await
        })
    }
}

/// The durable scheduled external policy: created disabled, updated, enabled,
/// disabled or revoked under its own operation. None of these methods can
/// enable the build's control, and a policy row alone never authorizes a
/// dispatch.
impl DatabaseHandle {
    pub(crate) async fn create_scheduled_external_policy(
        &self,
        draft: crate::system::scheduled_external::ScheduledExternalPolicyDraft,
    ) -> Result<entities::ScheduledExternalPolicyRecord, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (draft,),
                reply,
                |_| 8192,
                OperationKind::SetScheduledExternalPolicy,
                |(draft,), batch| batch.create_scheduled_external_policy(&draft),
            )
        })
        .await
    }

    pub(crate) async fn update_scheduled_external_policy(
        &self,
        id: entities::ScheduledExternalPolicyId,
        expected_revision: u32,
        draft: crate::system::scheduled_external::ScheduledExternalPolicyDraft,
    ) -> Result<entities::ScheduledExternalPolicyRecord, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (id, expected_revision, draft),
                reply,
                |_| 8192,
                OperationKind::SetScheduledExternalPolicy,
                |(id, expected_revision, draft), batch| {
                    batch.update_scheduled_external_policy(id, expected_revision, &draft)
                },
            )
        })
        .await
    }

    pub(crate) async fn set_scheduled_external_policy_enabled(
        &self,
        id: entities::ScheduledExternalPolicyId,
        expected_revision: u32,
        enabled: bool,
    ) -> Result<entities::ScheduledExternalPolicyRecord, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (id, expected_revision, enabled),
                reply,
                |_| 4096,
                OperationKind::SetScheduledExternalPolicy,
                |(id, expected_revision, enabled), batch| {
                    batch.set_scheduled_external_policy_enabled(id, expected_revision, enabled)
                },
            )
        })
        .await
    }

    pub(crate) async fn revoke_scheduled_external_policy(
        &self,
        id: entities::ScheduledExternalPolicyId,
        expected_revision: u32,
    ) -> Result<entities::ScheduledExternalPolicyRecord, DatabaseError> {
        self.submit(|reply| {
            Command::new(
                (id, expected_revision),
                reply,
                |_| 4096,
                OperationKind::SetScheduledExternalPolicy,
                |(id, expected_revision), batch| {
                    batch.revoke_scheduled_external_policy(id, expected_revision)
                },
            )
        })
        .await
    }

    pub(crate) async fn scheduled_external_policy(
        &self,
        id: entities::ScheduledExternalPolicyId,
    ) -> Result<Option<entities::ScheduledExternalPolicyRecord>, DatabaseError> {
        self.read(move |database| redb::read_scheduled_external_policy(database, id))
            .await
    }

    pub(crate) async fn scheduled_external_policies(
        &self,
        after: Option<entities::ScheduledExternalPolicyId>,
        limit: usize,
    ) -> Result<
        redb::CursorPage<
            entities::ScheduledExternalPolicyRecord,
            entities::ScheduledExternalPolicyId,
        >,
        DatabaseError,
    > {
        self.read(move |database| redb::read_scheduled_external_policies(database, after, limit))
            .await
    }

    /// The runtime authorization of one Job's current Allocation: the policy
    /// its binding names, at the revision the binding recorded. `Ok(None)` is
    /// every execution that is not a scheduled external one. A binding that
    /// names a missing, changed or refused policy is an error, never a
    /// fallback to the loopback path.
    pub(crate) async fn scheduled_external_grant(
        &self,
        job: entities::JobId,
    ) -> Result<Option<crate::system::scheduled_external::ScheduledExternalGrant>, DatabaseError>
    {
        let pair = self
            .read(move |database| redb::read_scheduled_external_binding_with_policy(database, job))
            .await?;
        let Some((binding, policy)) = pair else {
            return Ok(None);
        };
        if policy.revision != binding.policy_revision {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "scheduled_external_policy",
                "the bound policy revision changed since reservation",
            )));
        }
        crate::system::scheduled_external::ScheduledExternalGrant::from_record(&policy)
            .map(Some)
            .map_err(|refusal| {
                DatabaseError::Rejected(Violation::InvalidValue(
                    "scheduled_external_policy",
                    refusal.code,
                ))
            })
    }
}
