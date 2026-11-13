use super::commands::{
    byte_count, command_bytes, compaction_bytes, config_bytes, debug_bytes, json_bytes,
    output_bytes, search_index_bytes, text_bytes,
};
use super::*;

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

#[derive(Default)]
struct DevelopmentHandoffCredentials {
    items: Vec<DevelopmentHandoffCredential>,
    spellings: Option<crate::system::secrets::Spellings>,
}

struct DevelopmentHandoffCredential {
    reference: String,
    secret: redb::CredentialSecret,
    encrypted: Vec<u8>,
}

impl DevelopmentHandoffCredentials {
    fn push(&mut self, reference: String, secret: redb::CredentialSecret, encrypted: Vec<u8>) {
        match &mut self.spellings {
            Some(spellings) => spellings.extend(secret.as_str()),
            None => self.spellings = Some(crate::system::secrets::Spellings::new(secret.as_str())),
        }
        self.items.push(DevelopmentHandoffCredential {
            reference,
            secret,
            encrypted,
        });
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DevelopmentToolOffer {
    pub(crate) wires: Vec<crate::service::ai::providers::openai_compatible::ToolWire>,
    pub(crate) grant: Option<&'static str>,
    pub(crate) agents_md: Option<String>,
}

pub(crate) trait DevelopmentTooling {
    async fn offer(
        &self,
        frozen: &entities::FrozenExecutionConfig,
        project: Option<&entities::ProjectRecord>,
        context: DevelopmentToolContext,
    ) -> Result<Option<DevelopmentToolOffer>, &'static str>;
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct DevelopmentToolContext {
    pub(crate) decider: bool,
    pub(crate) nested: bool,
    pub(crate) plan_step: bool,
}

enum DevelopmentToolPolicy {
    ToolFree,
    Offered,
}

impl PreparedDevelopmentHandoff {
    pub(crate) fn request(&self) -> &serde_json::Value {
        &self.request
    }

    pub(crate) fn generative_configuration(&self) -> Option<&entities::FrozenExecutionConfig> {
        match &self.candidate {
            redb::DevelopmentCandidate::Generative(context) => Some(&context.configuration),
            redb::DevelopmentCandidate::Decision(_) => None,
        }
    }

    pub(crate) fn decision_configuration(&self) -> Option<&entities::FrozenDecisionConfig> {
        match &self.candidate {
            redb::DevelopmentCandidate::Generative(_) => None,
            redb::DevelopmentCandidate::Decision(context) => Some(&context.configuration),
        }
    }

    pub(crate) fn is_summary_producer(&self) -> bool {
        matches!(
            &self.candidate,
            redb::DevelopmentCandidate::Generative(context) if context.job.summary_source.is_some()
        )
    }

    pub(crate) fn credential_references(&self) -> Vec<&str> {
        self.expected_credential_references()
    }

    pub(super) fn candidate(&self) -> &redb::DevelopmentCandidate {
        &self.candidate
    }

    fn provider_credential_reference(&self) -> Option<&str> {
        match &self.candidate {
            redb::DevelopmentCandidate::Generative(context) => {
                context.configuration.llm.provider.credential_ref.as_deref()
            }
            redb::DevelopmentCandidate::Decision(context) => match &context.configuration {
                entities::FrozenDecisionConfig::Native(native) => native.credential_ref.as_deref(),
                entities::FrozenDecisionConfig::Generative(generative, _) => {
                    generative.llm.provider.credential_ref.as_deref()
                }
            },
        }
    }

    fn header_secret_references(&self) -> Vec<&str> {
        let headers = match &self.candidate {
            redb::DevelopmentCandidate::Generative(context) => {
                &context.configuration.llm.provider.headers
            }
            redb::DevelopmentCandidate::Decision(context) => match &context.configuration {
                entities::FrozenDecisionConfig::Native(native) => &native.headers,
                entities::FrozenDecisionConfig::Generative(generative, _) => {
                    &generative.llm.provider.headers
                }
            },
        };
        headers.secret_refs().collect()
    }

    pub(super) fn expected_credential_references(&self) -> Vec<&str> {
        let mut references: Vec<&str> = self.provider_credential_reference().into_iter().collect();
        for reference in self.header_secret_references() {
            if !references.contains(&reference) {
                references.push(reference);
            }
        }
        references
    }

    pub(super) fn credential_evidence(&self) -> impl Iterator<Item = (&str, &[u8])> {
        self.credentials
            .items
            .iter()
            .map(|item| (item.reference.as_str(), item.encrypted.as_slice()))
    }

    pub(super) fn has_bounded_inputs(&self) -> bool {
        if !self.request.is_object() {
            return false;
        }
        let offers_tools = self.request.get("tools").is_some();
        if offers_tools != matches!(self.tool_policy, DevelopmentToolPolicy::Offered) {
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

#[allow(dead_code)]
pub(crate) struct DevelopmentHandoff {
    prepared: PreparedDevelopmentHandoff,
    started: entities::MutableJob,
    owner: Arc<Shared>,
    output_busy: AtomicBool,
    output_uncertain: AtomicBool,
    usage_checkpoint: std::sync::Mutex<DevelopmentUsageCheckpoint>,
    recorder: std::sync::Mutex<crate::service::ai::providers::RequestRecorder>,
}

impl DevelopmentHandoff {
    pub(crate) fn job(&self) -> entities::JobId {
        match &self.prepared.candidate {
            redb::DevelopmentCandidate::Generative(context) => context.allocation.token.job,
            redb::DevelopmentCandidate::Decision(context) => context.allocation.token.job,
        }
    }

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
        &entities::FrozenDecisionConfig,
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

#[derive(Clone, Default)]
pub(super) struct DevelopmentUsageCheckpoint {
    request: Option<entities::RequestNumber>,
    evidence: entities::TokenEvidence,
}

impl DevelopmentUsageCheckpoint {
    pub(super) fn evidence(&self) -> entities::TokenEvidence {
        self.evidence.clone()
    }

    pub(super) fn advanced(_last_sequence: u32, evidence: entities::TokenEvidence) -> Self {
        Self {
            request: None,
            evidence,
        }
    }

    fn for_request(self, request: entities::RequestNumber) -> Self {
        if self.request == Some(request) {
            self
        } else {
            Self::default()
        }
    }

    fn of_request(mut self, request: entities::RequestNumber) -> Self {
        self.request = Some(request);
        self
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
    NotStarted {
        cause: entities::UnstartedCause,
        error: Box<DatabaseError>,
    },
    Database(Box<DatabaseError>),
    OutcomeUnknown {
        error: Box<DatabaseError>,
        reconciliation: Result<OperationStatus, Box<DatabaseError>>,
    },
}

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
    offer: Option<&DevelopmentToolOffer>,
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
            if context.conversation.parent_id.is_some() || offer.is_some() {
                return Err(unavailable());
            }
            let entities::MutableJobContent::Decision { request, .. } = &context.job.input.content
            else {
                return Err(unavailable());
            };
            let request =
                crate::service::ai::decisive_model::request_body(&context.configuration, request)
                    .map_err(|_| unavailable())?;
            Ok(PreparedDevelopmentHandoff {
                candidate: redb::DevelopmentCandidate::Decision(context),
                images: Vec::new(),
                request,
                conversation: None,
                system_policy,
                tool_policy: DevelopmentToolPolicy::ToolFree,
                credentials: DevelopmentHandoffCredentials::default(),
            })
        }
        redb::DevelopmentResolvedCandidate::Generative(context) => {
            let snapshot = &context.snapshot;
            let frozen = &snapshot.configuration;
            let granted = frozen
                .agent
                .as_ref()
                .is_some_and(|agent| agent.workspace_grant().is_some());
            let manages = frozen
                .agent
                .as_ref()
                .is_some_and(|agent| agent.manages_jobs());
            let is_summary = snapshot.job.summary_source.is_some();
            if is_summary {
                if frozen.provider_refusal().is_some() || offer.is_some() {
                    return Err(unavailable());
                }
            } else if frozen.native_execution_refusal().is_some()
                || (granted && offer.is_none_or(|offer| offer.grant.is_none()))
                || (!granted && offer.is_some() && !manages)
                || offer.is_some_and(|offer| offer.wires.is_empty())
            {
                return Err(unavailable());
            }
            if is_summary {
                let mut messages = vec![
                    crate::service::ai::providers::openai_compatible::ChatMessage::system(
                        "You are a context summarizer. Return ONLY valid JSON matching {\"schema_version\":1,\"source\":\"...\",\"summary\":\"...\",\"omitted_details\":[...]}. Do not use tools, Markdown fences, or extra prose.",
                    ),
                ];
                let mut combined = String::new();
                for input in &snapshot.covered_inputs {
                    combined.push_str(&input.text);
                }
                if !combined.is_empty() {
                    messages.push(
                        crate::service::ai::providers::openai_compatible::ChatMessage::user(
                            combined,
                        ),
                    );
                }
                messages.push(
                    crate::service::ai::providers::openai_compatible::ChatMessage::user(
                        snapshot.job.input.text().to_owned(),
                    ),
                );
                let conversation =
                    crate::service::ai::providers::openai_compatible::ChatConversation {
                        messages,
                        ..Default::default()
                    };
                let request = crate::service::ai::generative_model::request_body(
                    &frozen.llm.config.model,
                    &frozen.llm.config.parameters,
                    &conversation,
                )
                .map_err(|_| unavailable())?;
                return Ok(PreparedDevelopmentHandoff {
                    candidate: redb::DevelopmentCandidate::Generative(context.snapshot),
                    images: Vec::new(),
                    request,
                    conversation: Some(conversation),
                    system_policy,
                    tool_policy: DevelopmentToolPolicy::ToolFree,
                    credentials: DevelopmentHandoffCredentials::default(),
                });
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
                    || turn.record.job.phase != entities::MutableJobPhase::Executing
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
                offer.and_then(|offer| offer.grant),
                offer.and_then(|offer| offer.agents_md.as_deref()),
                offer.map(|offer| offer.wires.clone()).unwrap_or_default(),
                limits,
                ContextNotices::default(),
            )
            .ok_or_else(unavailable)?;
            if granted && snapshot.project.is_none() {
                return Err(unavailable());
            }
            if offer.is_none_or(|offer| offer.grant.is_none())
                && let Some(project) = &snapshot.project
            {
                let first = conversation.messages.first_mut().ok_or_else(unavailable)?;
                *first = crate::service::ai::providers::openai_compatible::ChatMessage::system(
                    format!(
                        "You work in the Project {:?}. Its workspace root is {}. No workspace access or tools are available for this execution; do not claim to read or change project files.",
                        project.name, project.root
                    ),
                );
            }
            if conversation.dropped_turns != 0
                || conversation.tools.len() != offer.map_or(0, |offer| offer.wires.len())
                || estimate_request_tokens(&conversation, limits.max_tokens)
                    > u64::from(limits.context_window.ok_or_else(unavailable)?)
            {
                return Err(unavailable());
            }
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
                tool_policy: if offer.is_some() {
                    DevelopmentToolPolicy::Offered
                } else {
                    DevelopmentToolPolicy::ToolFree
                },
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
    pub queue_pending: Option<u64>,
    pub queue_ready: Option<u64>,
    pub queue_delayed: Option<u64>,
    pub queue_running: Option<u64>,
    pub commands_waiting: u64,
    pub rss_bytes: Option<u64>,
    pub peak_rss_bytes: Option<u64>,
}

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
    async fn attachment_permit(&self) -> Result<AttachmentPermit, DatabaseError> {
        self.shared
            .attachment_slots
            .1
            .recv_async()
            .await
            .map_err(|_| DatabaseError::ShuttingDown)?;
        Ok(AttachmentPermit(self.shared.attachment_slots.0.clone()))
    }

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
                Some(counts.pending),
                Some(counts.ready),
                Some(counts.delayed),
                Some(counts.running),
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

        Vitals {
            database_bytes,
            available_bytes,
            durable_commits,
            commit_us_mean,
            commit_us_max,
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
        {
            let mut admission = self
                .shared
                .admission
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            if !matches!(*admission, Admission::Open | Admission::Protected(_)) {
                return false;
            }
            let lock = self.shared.protection.lock(vec![reason.clone()]);
            *admission = Admission::Protected(lock);
        }
        if let Err(error) = self
            .control_sender
            .try_send(Control::FilesystemFailure { reason })
        {
            log::debug!("failed to enqueue filesystem protection control notice: {error}");
        }
        true
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
        let counts = redb::queue_counts(&opened.database, true).map_err(DatabaseError::Read)?;
        if counts.pending != 0
            || counts.ready != 0
            || counts.delayed != 0
            || counts.running != 0
            || counts.held != 0
            || !self.sender.is_empty()
            || self.shared.in_flight_commands.load(Ordering::SeqCst) != 0
        {
            return Ok(None);
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

    async fn write<A: Send + 'static, T: Send + 'static>(
        &self,
        bytes: u64,
        kind: OperationKind,
        args: A,
        f: impl FnOnce(&mut redb::Batch, A) -> Result<T, redb::BatchError> + Send + 'static,
    ) -> Result<T, DatabaseError> {
        self.submit(|reply| Command::new(args, reply, |_| bytes, kind, move |a, b| f(b, a)))
            .await
    }

    async fn write_sized<A: Send + 'static, T: Send + 'static>(
        &self,
        args: A,
        size: impl FnOnce(&A) -> u64,
        kind: OperationKind,
        f: impl FnOnce(&mut redb::Batch, A) -> Result<T, redb::BatchError> + Send + 'static,
    ) -> Result<T, DatabaseError> {
        self.submit(|reply| Command::new(args, reply, size, kind, move |a, b| f(b, a)))
            .await
    }

    async fn write_output<A: Send + 'static, T: Send + 'static>(
        &self,
        job: JobId,
        args: A,
        size: impl FnOnce(&A) -> u64,
        kind: OperationKind,
        f: impl FnOnce(&mut redb::Batch, A) -> Result<T, redb::BatchError> + Send + 'static,
    ) -> Result<T, DatabaseError> {
        self.submit(|reply| {
            Command::custom(args, reply, size, move |args, reply, db, op, commit| {
                let mut outcome = run(db, op, commit, kind, reply, |batch| f(batch, args));
                if outcome.committed {
                    outcome.output_changed = Some(job);
                }
                outcome
            })
        })
        .await
    }

    pub(super) async fn read<T: Send + 'static>(
        &self,
        read: impl FnOnce(&Database) -> Result<T, StorageError> + Send + 'static,
    ) -> Result<T, DatabaseError> {
        self.read_opened(move |opened| read(&opened.database)).await
    }

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

    pub fn abandoned_at_open(&self) -> &[JobId] {
        &self.shared.abandoned_at_open
    }

    pub fn reclaimed_at_open(&self) -> &[(ContentDigest, Result<Reclamation, StoreError>)] {
        &self.shared.reclaimed_at_open
    }

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

    pub fn epoch(&self) -> u64 {
        self.shared.epoch.load(Ordering::SeqCst)
    }

    pub fn watch_disk_protection(&self) -> ProtectionWatch {
        self.shared.protection.watch()
    }

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

    pub fn genesis(&self) -> JobId {
        self.shared.genesis
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

    pub(crate) async fn native_job(
        &self,
        job: JobId,
    ) -> Result<Option<redb::NativeJobItem>, DatabaseError> {
        self.read(move |database| redb::read_native_job(database, job))
            .await
    }

    pub(crate) async fn search_jobs(
        &self,
        terms: Vec<String>,
        before: Option<JobId>,
        limit: usize,
    ) -> Result<redb::JobSearchPage, DatabaseError> {
        self.read(move |database| redb::read_job_search(database, &terms, before, limit))
            .await
    }

    pub async fn conversations(
        &self,
        before: Option<JobId>,
        limit: usize,
    ) -> Result<redb::ConversationsPage, DatabaseError> {
        self.read(move |database| redb::read_conversations(database, before, limit))
            .await
    }

    pub async fn search_conversations(
        &self,
        terms: Vec<String>,
        before: Option<JobId>,
        limit: usize,
    ) -> Result<redb::ConversationsPage, DatabaseError> {
        self.read(move |database| redb::read_conversation_search(database, &terms, before, limit))
            .await
    }

    pub async fn uncertain_allocations(
        &self,
    ) -> Result<Vec<crate::db::redb::UncertainAllocation>, DatabaseError> {
        self.read(redb::read_uncertain_allocations).await
    }

    pub async fn children(
        &self,
        parent: JobId,
        after: Option<JobId>,
        limit: usize,
    ) -> Result<Vec<JobId>, DatabaseError> {
        self.read(move |database| redb::read_children(database, parent, after, limit))
            .await
    }

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

    pub async fn job_configuration(
        &self,
        job: JobId,
    ) -> Result<Option<entities::SubmittedConfiguration>, DatabaseError> {
        self.read(move |database| redb::read_job_configuration(database, job))
            .await
    }

    pub async fn decision_work(
        &self,
        job: JobId,
    ) -> Result<Option<entities::DecisionWork>, DatabaseError> {
        self.read(move |database| redb::read_decision_work(database, job))
            .await
    }

    pub async fn retries(
        &self,
        original: JobId,
        after: Option<JobId>,
        limit: usize,
    ) -> Result<redb::CursorPage<JobId, JobId>, DatabaseError> {
        self.read(move |database| redb::read_retries(database, original, after, limit))
            .await
    }

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

    pub async fn project(&self, id: ProjectId) -> Result<Option<ProjectRecord>, DatabaseError> {
        self.read(move |database| redb::read_project(database, id))
            .await
    }

    pub async fn projects(
        &self,
        before: Option<ProjectId>,
        limit: usize,
    ) -> Result<Vec<ProjectRecord>, DatabaseError> {
        self.read(move |database| redb::read_projects(database, before, limit))
            .await
    }

    pub async fn job_metrics(
        &self,
        job: JobId,
        after: Option<MetricsId>,
        limit: usize,
    ) -> Result<Vec<MetricsRecord>, DatabaseError> {
        self.read(move |database| redb::read_job_metrics(database, job, after, limit))
            .await
    }

    pub async fn job_availability(
        &self,
        job: JobId,
        after: Option<AvailabilityStatId>,
        limit: usize,
    ) -> Result<Vec<AvailabilityStatRecord>, DatabaseError> {
        self.read(move |database| redb::read_job_availability(database, job, after, limit))
            .await
    }

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

    pub async fn submission(
        &self,
        key: SubmissionKey,
    ) -> Result<Option<SubmissionRecord>, DatabaseError> {
        self.read(move |database| redb::read_submission(database, key))
            .await
    }

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

    pub(crate) async fn reserve_scheduled_job(
        &self,
        fresh: entities::DevelopmentFreshReservation,
        generative_policy: fn(&entities::FrozenExecutionConfig) -> bool,
        decision_policy: fn(&entities::NativeDecisionConfig) -> bool,
    ) -> Result<ScheduledReservation, DevelopmentReservationError> {
        let request = fresh.request();
        let result = self
            .submit(|reply| {
                Command::custom(
                    (request, generative_policy, decision_policy),
                    reply,
                    |_| 4096,
                    |(request, generative_policy, decision_policy),
                     reply,
                     database,
                     operation,
                     commit| {
                        let mut passed = ScheduledReservation::Pass(Box::new(
                            entities::DevelopmentReservation::Exhausted,
                        ));
                        let outcome = redb::run_batch_declinable(
                            database,
                            operation.epoch,
                            commit,
                            |batch| match batch.reserve_next_development_job(
                                request,
                                true,
                                entities::ReadySelection::Scheduled {
                                    generative_policy,
                                    decision_policy,
                                },
                            ) {
                                Ok(reserved @ entities::DevelopmentReservation::Reserved(_)) => {
                                    Ok(Some(ScheduledReservation::Pass(Box::new(reserved))))
                                }
                                Ok(other) => {
                                    passed = ScheduledReservation::Pass(Box::new(other));
                                    Ok(None)
                                }
                                Err(redb::BatchError(redb::Failure::Rejected(violation))) => {
                                    batch.reject(violation)
                                }
                                Err(error) => Err(error),
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

    #[allow(dead_code)]
    pub(crate) async fn development_trial_job(
        &self,
        job: entities::JobId,
    ) -> Result<Option<(entities::MutableJobRecord, Option<entities::PoolAllocation>)>, DatabaseError>
    {
        self.read(move |database| redb::read_trial_job(database, job))
            .await
    }

    async fn prepare_with_credentials(
        &self,
        resolved: redb::DevelopmentResolvedCandidate,
        offer: Option<&DevelopmentToolOffer>,
    ) -> Result<PreparedDevelopmentHandoff, DevelopmentHandoffError> {
        let mut prepared = prepare_development_handoff(resolved, self.shared.active_policy, offer)
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
            let Some((secret, encrypted)) = resolved else {
                return Err(classify_unstarted(DatabaseError::Rejected(
                    Violation::InvalidValue("credential", "stored credential is unavailable"),
                )));
            };
            prepared.credentials.push(reference, secret, encrypted);
        }
        if prepared.system_policy != self.shared.active_policy {
            return Err(classify_unstarted(DatabaseError::Rejected(
                Violation::InvalidValue("handoff", "system policy changed"),
            )));
        }
        Ok(prepared)
    }

    #[allow(dead_code)]
    pub(crate) async fn checked_development_handoff_guarded(
        &self,
        token: entities::AllocationToken,
        tooling: &impl DevelopmentTooling,
        guard: impl FnOnce(&PreparedDevelopmentHandoff) -> Result<(), &'static str>,
    ) -> Result<DevelopmentHandoff, DevelopmentHandoffError> {
        let resolved = self
            .development_execution_context(token)
            .await
            .map_err(classify_unstarted)?;
        let offer = match &resolved {
            redb::DevelopmentResolvedCandidate::Generative(context)
                if context.snapshot.job.summary_source.is_none() =>
            {
                tooling
                    .offer(
                        &context.snapshot.configuration,
                        context.snapshot.project.as_ref(),
                        DevelopmentToolContext {
                            decider: context.snapshot.job.input.priority
                                == entities::JobQueuePriority::Foreground,
                            nested: context.snapshot.conversation.parent_id.is_some(),
                            plan_step: context.snapshot.plan_step,
                        },
                    )
                    .await
                    .map_err(|reason| {
                        classify_unstarted(DatabaseError::Rejected(Violation::InvalidValue(
                            "handoff", reason,
                        )))
                    })?
            }
            redb::DevelopmentResolvedCandidate::Generative(_)
            | redb::DevelopmentResolvedCandidate::Decision(_) => None,
        };
        let prepared = self
            .prepare_with_credentials(resolved, offer.as_ref())
            .await?;
        if let Err(reason) = guard(&prepared) {
            return Err(classify_unstarted(DatabaseError::Rejected(
                Violation::InvalidValue("handoff", reason),
            )));
        }
        let owner = self.shared.clone();
        let result = self
            .write_sized(
                (prepared, owner),
                |(prepared, _)| {
                    byte_count(crate::db::serialized_len(&prepared.request).unwrap_or(usize::MAX))
                        .saturating_add(64 * 1024)
                },
                OperationKind::ClaimExecution,
                |batch, (prepared, owner)| {
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
                        recorder: std::sync::Mutex::new(
                            crate::service::ai::providers::RequestRecorder::default(),
                        ),
                    })
                },
            )
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

    #[allow(dead_code)]
    pub(crate) async fn append_development_output(
        &self,
        handoff: &DevelopmentHandoff,
        expected_seq: u32,
        output: JobOutput,
    ) -> Result<AppendedOutput, DatabaseError> {
        let (mut guard, proof) = self.begin_development_write(handoff)?;
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

    #[allow(dead_code)]
    pub(crate) async fn record_development_sent_request(
        &self,
        handoff: &DevelopmentHandoff,
        observed_at: entities::Timestamp,
    ) -> Result<(), DatabaseError> {
        self.record_development_request(
            handoff,
            entities::RequestNumber::FIRST,
            &handoff.prepared.request,
            observed_at,
        )
        .await
    }

    pub(crate) async fn record_development_request(
        &self,
        handoff: &DevelopmentHandoff,
        request_number: entities::RequestNumber,
        body: &serde_json::Value,
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
            .is_some_and(|secret| development_contains_secret(body, secret))
        {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "request",
                "request evidence contains credential material",
            )));
        }
        let mut pending_state = None;
        let mut sent = if decision {
            let body = entities::MessageBody::new(&serde_json::json!({"decision": body})).map_err(
                |_| {
                    DatabaseError::Rejected(Violation::InvalidValue(
                        "request",
                        "prepared decision cannot be recorded",
                    ))
                },
            )?;
            let digest = body.digest();
            entities::SentRequest {
                job: token.job,
                request: request_number,
                observed_at,
                messages: vec![digest],
                tools: None,
                parameters: digest,
                dropped_turns: None,
                bodies: vec![body],
                required_messages: Vec::new(),
            }
        } else {
            let mut recorder = handoff
                .recorder
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if request_number == entities::RequestNumber::FIRST {
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
            }
            let (sent, state) = recorder
                .prepare(token.job, request_number, body, 0)
                .map_err(|_| {
                    DatabaseError::Rejected(Violation::InvalidValue(
                        "request",
                        "prepared request cannot be recorded",
                    ))
                })?;
            pending_state = Some(state);
            sent
        };
        sent.observed_at = observed_at;
        let (mut guard, proof) = self.begin_development_write(handoff)?;
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
            .write_sized(
                (proof, sent, attachments),
                |(_, sent, _)| {
                    sent.bodies.iter().fold(0u64, |size, body| {
                        size.saturating_add(byte_count(body.byte_length()))
                    })
                },
                OperationKind::RecordSentRequest,
                |batch, (proof, sent, attachments)| {
                    batch.record_development_sent_request(&proof, sent, &attachments)
                },
            )
            .await;
        guard.known = !matches!(result, Err(DatabaseError::OutcomeUnknown { .. }));
        if result.is_ok()
            && let Some(state) = pending_state
        {
            handoff
                .recorder
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remember_sent(state);
        }
        result
    }

    #[allow(dead_code)]
    pub(crate) async fn record_development_provider_answer(
        &self,
        handoff: &DevelopmentHandoff,
        answer: ProviderAnswer,
    ) -> Result<(), DatabaseError> {
        let (mut guard, proof) = self.begin_development_write(handoff)?;
        let response: serde_json::Value =
            serde_json::from_slice(&answer.response.bytes).map_err(|_| {
                DatabaseError::Rejected(Violation::InvalidValue(
                    "response",
                    "invalid response JSON",
                ))
            })?;
        if let Some(secret) = &handoff.prepared.credentials.spellings {
            let observation = &answer.stat.observation;
            let failure_has_secret = observation.failure.as_ref().is_some_and(|f| {
                [
                    Some(f.message.as_str()),
                    f.provider_code.as_deref(),
                    f.response_body.as_deref(),
                    f.request_id.as_deref(),
                ]
                .into_iter()
                .flatten()
                .any(|s| secret.found_in(s))
            });
            if development_contains_secret(&response, secret)
                || answer
                    .measurements
                    .usage
                    .raw
                    .as_ref()
                    .is_some_and(|v| development_contains_secret(v, secret))
                || observation
                    .raw_finish_reason
                    .as_deref()
                    .is_some_and(|s| secret.found_in(s))
                || failure_has_secret
            {
                return Err(DatabaseError::Rejected(Violation::InvalidValue(
                    "answer",
                    "answer evidence contains credential material",
                )));
            }
        }
        let response_digest = answer.response.digest();
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
        if result.is_ok() {
            handoff
                .recorder
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remember_response(response_digest);
        }
        result
    }

    #[allow(dead_code)]
    pub(crate) async fn record_development_usage(
        &self,
        handoff: &DevelopmentHandoff,
        expected_seq: u32,
        metrics: Metrics,
    ) -> Result<MetricsId, DatabaseError> {
        let (mut guard, proof) = self.begin_development_write(handoff)?;
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
        let request = metrics.request;
        let checkpoint = handoff
            .usage_checkpoint
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .for_request(request);
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
                .unwrap_or_else(PoisonError::into_inner) = checkpoint.of_request(request);
            id
        })
    }

    #[allow(dead_code)]
    pub(crate) async fn finalize_development_job(
        &self,
        returned: crate::system::executor::execution_runtime::DevelopmentExecutionReturn<'_>,
        expected_seq: u32,
        finalization: JobFinalization,
    ) -> Result<AppendedOutput, DatabaseError> {
        let handoff = returned.handoff();
        let (mut guard, proof) = self.begin_development_write(handoff)?;
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
        let result = self
            .write_output(
                proof.token.job,
                (proof, expected_seq, finalization),
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
                OperationKind::FinalizeJob,
                |batch, (proof, expected_seq, finalization)| {
                    batch.finalize_development_job(&proof, expected_seq, finalization)
                },
            )
            .await;
        guard.known = !matches!(result, Err(DatabaseError::OutcomeUnknown { .. }));
        result
    }

    #[allow(dead_code)]
    pub(crate) async fn finalize_development_decision(
        &self,
        returned: crate::system::executor::execution_runtime::DevelopmentExecutionReturn<'_>,
        expected_seq: u32,
        result: entities::DecisionResult,
    ) -> Result<AppendedOutput, DatabaseError> {
        let handoff = returned.handoff();
        if !matches!(
            &handoff.prepared.candidate,
            redb::DevelopmentCandidate::Decision(_)
        ) {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "handoff",
                "Decision handoff required",
            )));
        }
        let (mut guard, proof) = self.begin_development_write(handoff)?;
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
        let result = self
            .write_output(
                proof.token.job,
                (proof, expected_seq, result),
                |(_, _, result)| debug_bytes(result),
                OperationKind::FinalizeJob,
                |batch, (proof, expected_seq, decision)| {
                    batch.finalize_development_decision(&proof, expected_seq, decision)
                },
            )
            .await;
        guard.known = !matches!(result, Err(DatabaseError::OutcomeUnknown { .. }));
        result
    }

    #[allow(dead_code)]
    pub(crate) async fn finalize_development_failure(
        &self,
        returned: crate::system::executor::execution_runtime::DevelopmentExecutionReturn<'_>,
        expected_seq: u32,
        finalization: JobFinalization,
        rejection: Option<(
            entities::RequestNumber,
            entities::Timestamp,
            entities::AvailabilityObservation,
        )>,
    ) -> Result<AppendedOutput, DatabaseError> {
        let handoff = returned.handoff();
        let (mut guard, proof) = self.begin_development_write(handoff)?;
        let token = proof.token;
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
                .and_then(|(_, _, observation)| observation.failure.as_ref());
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
        let rejection =
            rejection.map(
                |(request, observed_at, observation)| entities::AvailabilityStat {
                    job: token.job,
                    request,
                    observed_at,
                    observation,
                },
            );
        let result = self
            .write_output(
                proof.token.job,
                (proof, expected_seq, finalization, rejection),
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
                OperationKind::FinalizeJob,
                |batch, (proof, expected_seq, finalization, rejection)| {
                    batch.finalize_development_failure(
                        &proof,
                        expected_seq,
                        finalization,
                        rejection,
                    )
                },
            )
            .await;
        guard.known = !matches!(result, Err(DatabaseError::OutcomeUnknown { .. }));
        result
    }

    #[allow(dead_code)]
    pub(crate) async fn fail_unstarted_development_reservation(
        &self,
        token: entities::AllocationToken,
        cause: entities::UnstartedCause,
    ) -> Result<AppendedOutput, DatabaseError> {
        self.write_output(
            token.job,
            (token, cause),
            |(_, cause)| output_bytes(Some(cause.summary()), Some(cause.code()), None),
            OperationKind::FinalizeJob,
            |batch, (token, cause)| batch.fail_unstarted_reservation(token, cause),
        )
        .await
    }

    #[allow(dead_code)]
    pub(crate) async fn record_development_uncertain(
        &self,
        returned: crate::system::executor::execution_runtime::DevelopmentExecutionReturn<'_>,
        reason: entities::UncertaintyReason,
    ) -> Result<(), DatabaseError> {
        let handoff = returned.handoff();
        let (mut guard, proof) = self.begin_development_write(handoff)?;
        let result = self
            .write(
                0,
                OperationKind::FinalizeJob,
                (proof, reason),
                |batch, (proof, reason)| batch.record_development_uncertain(&proof, reason),
            )
            .await;
        guard.known = !matches!(result, Err(DatabaseError::OutcomeUnknown { .. }));
        result
    }

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

    pub(crate) async fn create_mutable_job(
        &self,
        context: entities::ComposerContextId,
        input: entities::MutableJobInput,
        creation: Submission,
        conversation: Option<JobId>,
        project: Option<entities::ProjectId>,
    ) -> Result<entities::MutableJobRecord, DatabaseError> {
        self.write_sized(
            (context, input, creation, conversation, project),
            |(_, input, _, _, _)| byte_count(input.size_hint()).saturating_add(8192),
            OperationKind::CreateJobWithInput,
            |batch, (context, input, creation, conversation, project)| {
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
        .await
    }

    #[allow(dead_code)]
    pub(crate) async fn mutable_job_for_context(
        &self,
        context: entities::ComposerContextId,
    ) -> Result<Option<entities::MutableJobRecord>, DatabaseError> {
        self.write(0, OperationKind::Open, context, |batch, context| {
            batch.mutable_job_for_context(context)
        })
        .await
    }

    pub(crate) async fn mutable_job_with_support(
        &self,
        context: entities::ComposerContextId,
        id: JobId,
    ) -> Result<(entities::MutableJobRecord, bool), DatabaseError> {
        self.write(
            0,
            OperationKind::Open,
            (context, id),
            |batch, (context, id)| {
                batch.require_mutable_job_context(context, id)?;
                let Some(record) = batch.read_mutable_job(id)? else {
                    return batch.corrupted("mutable Job is missing".to_owned());
                };
                let blocked = batch.unsupported_queued_context(&record.job)?;
                Ok((record, blocked))
            },
        )
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
        self.write_sized(
            (context, id, revision, input),
            |(_, _, _, input)| byte_count(input.size_hint()).saturating_add(40 * 1024),
            OperationKind::SaveJobInput,
            |batch, (context, id, revision, input)| {
                batch.require_mutable_job_context(context, id)?;
                batch.update_mutable_job(id, revision, input)?;
                batch.read_mutable_job(id)?.ok_or_else(|| {
                    redb::BatchError(redb::Failure::Storage(redb::StorageError::corrupted(
                        "updated mutable Job is missing",
                    )))
                })
            },
        )
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
        self.write_sized(
            (context, id, revision, input, submission),
            |(_, _, _, input, _)| {
                input.as_ref().map_or(8192, |input| {
                    byte_count(input.size_hint()).saturating_add(40 * 1024)
                })
            },
            OperationKind::Open,
            |batch, (context, id, revision, input, submission)| {
                batch.require_mutable_job_context(context, id)?;
                match input {
                    Some(input) => {
                        batch.submit_mutable_job_with_input(id, revision, input, submission)
                    }
                    None => batch.submit_mutable_job(id, revision, submission),
                }?;
                batch.read_mutable_job(id)?.ok_or_else(|| {
                    redb::BatchError(redb::Failure::Storage(redb::StorageError::corrupted(
                        "submitted mutable Job is missing",
                    )))
                })
            },
        )
        .await
    }

    #[allow(dead_code)]
    pub(crate) async fn discard_mutable_job(
        &self,
        context: entities::ComposerContextId,
        id: JobId,
        revision: u64,
    ) -> Result<(), DatabaseError> {
        self.write(
            8192,
            OperationKind::StopTurn,
            (context, id, revision),
            |batch, (context, id, revision)| {
                batch.require_mutable_job_context(context, id)?;
                batch.discard_unsubmitted_job(id, revision)
            },
        )
        .await
    }

    pub(crate) async fn cancel_unsupported_queued_job(
        &self,
        context: entities::ComposerContextId,
        id: JobId,
        revision: u64,
    ) -> Result<entities::MutableJobRecord, DatabaseError> {
        self.write(
            8192,
            OperationKind::StopTurn,
            (context, id, revision),
            |batch, (context, id, revision)| {
                batch.require_mutable_job_context(context, id)?;
                batch.cancel_unsupported_queued_job(id, revision)?;
                batch.read_mutable_job(id)?.ok_or_else(|| {
                    redb::BatchError(redb::Failure::Storage(redb::StorageError::corrupted(
                        "cancelled Job is missing",
                    )))
                })
            },
        )
        .await
    }

    pub(crate) async fn agent_model_configs(
        &self,
        ids: Vec<ConfigId>,
    ) -> Result<Vec<redb::AgentModelConfig>, DatabaseError> {
        self.read(move |database| redb::read_agent_model_configs(database, &ids))
            .await
    }

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

    pub(crate) async fn pool_allocations(
        &self,
        pool: entities::ExecutionPoolId,
        after: Option<(u128, u128)>,
        limit: usize,
    ) -> Result<Option<redb::PoolAllocationPage>, DatabaseError> {
        self.read(move |database| redb::read_pool_allocations(database, pool, after, limit))
            .await
    }

    pub(crate) async fn register_native_pool(
        &self,
        pool: entities::AllocationPool,
    ) -> Result<(), DatabaseError> {
        self.write(4096, OperationKind::CreateExecutionPool, pool, |b, p| {
            b.register_allocation_pool(p)
        })
        .await
    }

    pub(crate) async fn register_native_executor(
        &self,
        executor: entities::AllocationExecutor,
    ) -> Result<(), DatabaseError> {
        self.write(4096, OperationKind::CreateExecutor, executor, |b, e| {
            b.register_allocation_executor(e)
        })
        .await
    }

    pub(crate) async fn update_native_executor(
        &self,
        id: ExecutorId,
        expected_revision: u32,
        update: entities::ExecutorUpdate,
    ) -> Result<entities::AllocationExecutor, DatabaseError> {
        self.write(
            4096,
            OperationKind::UpdateExecutor,
            (id, expected_revision, update),
            |b, (id, rev, upd)| b.update_native_executor(id, rev, upd),
        )
        .await
    }

    pub(crate) async fn archive_native_executor(
        &self,
        id: ExecutorId,
        expected_revision: u32,
    ) -> Result<entities::AllocationExecutor, DatabaseError> {
        self.write(
            4096,
            OperationKind::ArchiveExecutor,
            (id, expected_revision),
            |b, (id, rev)| b.archive_native_executor(id, rev),
        )
        .await
    }

    pub(crate) async fn archive_native_pool(
        &self,
        id: ExecutionPoolId,
        expected_revision: u32,
    ) -> Result<entities::AllocationPool, DatabaseError> {
        self.write(
            4096,
            OperationKind::ArchivePool,
            (id, expected_revision),
            |b, (id, rev)| b.archive_native_pool(id, rev),
        )
        .await
    }

    pub(crate) async fn update_native_pool(
        &self,
        id: ExecutionPoolId,
        expected_revision: u32,
        capacity: Option<std::num::NonZeroU32>,
        enabled: Option<bool>,
    ) -> Result<entities::AllocationPool, DatabaseError> {
        self.write(
            4096,
            OperationKind::SetExecutionPoolProfile,
            (id, expected_revision, capacity, enabled),
            |b, (id, rev, cap, en)| b.update_native_pool(id, rev, cap, en),
        )
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

    pub async fn create_config(&self, config: Config) -> Result<ConfigId, DatabaseError> {
        self.write_sized(
            config,
            config_bytes,
            OperationKind::CreateProfile,
            mutations::create_config,
        )
        .await
    }

    pub(crate) async fn set_config_enabled(
        &self,
        id: ConfigId,
        kind: crate::db::entities::ConfigKind,
        enabled: bool,
    ) -> Result<(), DatabaseError> {
        self.write(
            32,
            OperationKind::UpdateProfile,
            (id, kind, enabled),
            |b, (id, k, en)| b.set_config_enabled(id, k, en),
        )
        .await
    }

    pub(crate) async fn remove_provider(&self, id: ConfigId) -> Result<(), DatabaseError> {
        self.write(16, OperationKind::UpdateProfile, id, |b, id| {
            b.remove_provider(id)
        })
        .await
    }

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
        self.write_sized(save, model_profile_save_bytes, kind, |b, s| {
            mutations::save_model_profile(b, s)
        })
        .await
    }

    pub async fn refresh_model_metadata(
        &self,
        connection: crate::db::entities::ConnectionConfig,
        models: Vec<crate::db::entities::DiscoveredModel>,
    ) -> Result<(), DatabaseError> {
        self.write_sized(
            (connection, models),
            |(c, m)| {
                text_bytes(c.endpoint.as_deref())
                    .saturating_add(text_bytes(c.credential_ref.as_deref()))
                    .saturating_add(
                        m.iter()
                            .map(|model| byte_count(model.id.len()).saturating_add(512))
                            .sum::<u64>(),
                    )
            },
            OperationKind::UpdateProfile,
            |b, (c, m)| b.refresh_model_metadata(c, m),
        )
        .await
    }

    pub(crate) async fn import_models(
        &self,
        mut save: mutations::ImportModelsSave,
    ) -> Result<(Vec<ConfigId>, usize), DatabaseError> {
        if let Some(secret) = save.secret.clone() {
            let key = self.ensure_credential_key().await?;
            save.credential = Some(mutations::CredentialPayload { secret, key });
        }
        self.write_sized(
            save,
            |save| {
                let ref_bytes = match &save.credential_ref {
                    mutations::ProfileCredentialRef::Preserve => match &save.connection.body {
                        ConfigBody::Connection(body) => text_bytes(body.credential_ref.as_deref()),
                        _ => 0,
                    },
                    mutations::ProfileCredentialRef::Clear => 0,
                    mutations::ProfileCredentialRef::Explicit(r)
                    | mutations::ProfileCredentialRef::PreserveOrGenerate(r) => byte_count(r.len()),
                };
                let endpoint_bytes = match &save.connection.body {
                    ConfigBody::Connection(body) => text_bytes(body.endpoint.as_deref()),
                    _ => 0,
                };
                let secret_bytes = save
                    .secret
                    .as_ref()
                    .map_or(0, |s| byte_count(s.as_str().len()));
                let base = endpoint_bytes
                    .saturating_add(ref_bytes)
                    .saturating_add(header_change_bytes(&save.headers))
                    .saturating_add(secret_bytes)
                    .saturating_add(byte_count(save.connection.name.len()));
                save.models.iter().fold(base, |bytes, model| {
                    bytes
                        .saturating_add(byte_count(model.id.len()))
                        .saturating_add(528)
                })
            },
            OperationKind::CreateProfile,
            mutations::import_models,
        )
        .await
    }

    pub async fn global_outbound_headers(
        &self,
    ) -> Result<crate::db::entities::GlobalHeaders, DatabaseError> {
        self.read(redb::read_global_outbound_headers).await
    }

    pub async fn set_global_outbound_headers(
        &self,
        expected_revision: u32,
        fields: Vec<crate::db::entities::OutboundHeader>,
    ) -> Result<crate::db::entities::GlobalHeaders, DatabaseError> {
        self.write_sized(
            (expected_revision, fields),
            |(_, fields)| {
                byte_count(fields.len())
                    .saturating_mul(512)
                    .saturating_add(4096)
            },
            OperationKind::UpdateProfile,
            |b, (rev, fields)| b.save_global_headers(rev, fields),
        )
        .await
    }

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
        self.write_sized(
            settings,
            |s| compaction_bytes(Some(&s.policy)).saturating_add(64),
            OperationKind::UpdateProfile,
            |b, s| b.set_global_compaction_settings(s),
        )
        .await
    }

    pub async fn archive_model(&self, id: ConfigId) -> Result<(), DatabaseError> {
        self.write(16, OperationKind::UpdateProfile, id, |b, id| {
            mutations::archive_model(b, id)
        })
        .await
    }

    pub async fn update_config(
        &self,
        id: ConfigId,
        config: Config,
        expected_edit_seq: u32,
    ) -> Result<(), DatabaseError> {
        self.write_sized(
            (id, config, expected_edit_seq),
            |(_, c, _)| config_bytes(c),
            OperationKind::UpdateProfile,
            |b, (id, c, exp)| mutations::update_config(b, id, c, exp),
        )
        .await
    }

    pub async fn decide_approval(
        &self,
        approval: ToolCallRef,
        decision: entities::ApprovalDecision,
    ) -> Result<Appended, DatabaseError> {
        self.write_sized(
            (approval, decision),
            |(_, d)| match d {
                entities::ApprovalDecision::Answered { text } => byte_count(text.len()),
                _ => 64,
            },
            OperationKind::DecideApproval,
            |b, (a, d)| b.decide(a, d),
        )
        .await
    }
    pub async fn conversation_cards(
        &self,
        conversation: JobId,
    ) -> Result<Vec<serde_json::Value>, DatabaseError> {
        self.read(move |database| redb::read_cards(database, conversation))
            .await
    }
    pub(crate) async fn apply_development_job_tool(
        &self,
        handoff: &DevelopmentHandoff,
        call: ToolCallRef,
        tool: entities::JobToolCall,
    ) -> Result<redb::JobToolApplied, DatabaseError> {
        let (mut guard, proof) = self.begin_development_write(handoff)?;
        let result = self
            .write_sized(
                (proof, call, tool),
                |(_, _, tool)| {
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
                |batch, (proof, call, tool)| batch.apply_job_tool(&proof, call, tool),
            )
            .await;
        guard.known = !matches!(result, Err(DatabaseError::OutcomeUnknown { .. }));
        result
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
        self.write_sized(
            (
                parent,
                spec,
                first,
                configuration,
                request,
                submission,
                candidate,
            ),
            |(_, _, first, _, request, _, _)| {
                debug_bytes(first).saturating_add(debug_bytes(request))
            },
            OperationKind::Open,
            |batch, (parent, spec, _first, configuration, request, submission, candidate)| {
                batch.with_admission_candidate(Some(candidate), submission, |batch| {
                    batch.submit_native_decision(parent, spec, configuration, request, submission)
                })
            },
        )
        .await
    }
    pub async fn request_compaction(
        &self,
        conversation: JobId,
        model: ConfigId,
        submission: Submission,
    ) -> Result<entities::CompactionRequest, DatabaseError> {
        self.write(
            4096 * 512,
            OperationKind::Open,
            (conversation, model, submission),
            |batch, (conversation, model, submission)| {
                batch.request_compaction(conversation, model, submission)
            },
        )
        .await
    }

    pub async fn stop_turn(
        &self,
        conversation: JobId,
        by: Actor,
    ) -> Result<JobAction, DatabaseError> {
        self.write(
            4096,
            OperationKind::StopTurn,
            (conversation, by),
            |batch, (c, by)| batch.stop_turn(c, by),
        )
        .await
    }
    pub async fn close(&self, conversation: JobId, by: Actor) -> Result<JobAction, DatabaseError> {
        self.write(
            4096,
            OperationKind::Close,
            (conversation, by),
            |batch, (c, by)| batch.close(c, by),
        )
        .await
    }
    pub(crate) async fn settle_child_turns(
        &self,
        turns: Vec<JobId>,
    ) -> Result<usize, DatabaseError> {
        if turns.is_empty() {
            return Ok(0);
        }
        self.write_sized(
            turns,
            |turns| byte_count(turns.len()).saturating_mul(64 * 1024),
            OperationKind::SettleChildTurns,
            |batch, turns| batch.settle_child_turns(&turns),
        )
        .await
    }
    pub(crate) async fn settle_unreported_children(&self) -> Result<(), DatabaseError> {
        const BATCH: usize = 8;
        loop {
            let settled = self
                .write(
                    byte_count(BATCH).saturating_mul(64 * 1024),
                    OperationKind::SettleChildTurns,
                    (),
                    |batch, ()| batch.settle_unreported_children(BATCH),
                )
                .await?;
            if settled < BATCH {
                return Ok(());
            }
        }
    }
    pub async fn hold(&self, conversation: JobId, by: Actor) -> Result<JobAction, DatabaseError> {
        self.write(
            4096,
            OperationKind::Hold,
            (conversation, by),
            |batch, (c, by)| batch.hold(c, by, false),
        )
        .await
    }
    pub async fn unhold(&self, conversation: JobId, by: Actor) -> Result<JobAction, DatabaseError> {
        self.write(
            4096,
            OperationKind::Unhold,
            (conversation, by),
            |batch, (c, by)| batch.hold(c, by, true),
        )
        .await
    }
    pub async fn retry(
        &self,
        conversation: JobId,
        turn: JobId,
        by: Actor,
    ) -> Result<Appended, DatabaseError> {
        self.write(
            4096,
            OperationKind::Retry,
            (conversation, turn, by),
            |batch, (c, t, by)| batch.retry(c, t, by),
        )
        .await
    }

    pub async fn create_project(&self, project: Project) -> Result<ProjectId, DatabaseError> {
        self.write_sized(
            project,
            |project| {
                byte_count(project.name.len())
                    .saturating_add(text_bytes(project.description.as_deref()))
                    .saturating_add(byte_count(project.root.len()))
                    .saturating_add(command_bytes(&project.commands))
            },
            OperationKind::CreateProject,
            mutations::create_project,
        )
        .await
    }

    pub(crate) async fn configure_project_decision(
        &self,
        id: ProjectId,
        expected: crate::db::entities::Timestamp,
        assignment: Option<entities::ProjectDecisionConfig>,
    ) -> Result<entities::ProjectRecord, DatabaseError> {
        self.write(
            32 * 1024,
            OperationKind::UpdateProject,
            (id, expected, assignment),
            |batch, (id, expected, assignment)| {
                mutations::configure_project_decision(batch, id, expected, assignment)
            },
        )
        .await
    }

    pub async fn update_project(
        &self,
        id: ProjectId,
        name: String,
        description: Option<String>,
        commands: Vec<ProjectCommand>,
    ) -> Result<(), DatabaseError> {
        self.write_sized(
            (id, name, description, commands),
            |(_, name, description, commands)| {
                byte_count(name.len())
                    .saturating_add(text_bytes(description.as_deref()))
                    .saturating_add(command_bytes(commands))
            },
            OperationKind::UpdateProject,
            |batch, (id, name, description, commands)| {
                mutations::update_project(batch, id, name, description, commands)
            },
        )
        .await
    }

    pub async fn set_conversation(&self, root: JobId, edit: JobEdit) -> Result<(), DatabaseError> {
        self.write_sized(
            (root, edit),
            |(_, edit)| match edit {
                JobEdit::Title(title) | JobEdit::SuggestedTitle(title) => byte_count(title.len()),
                JobEdit::Hide | JobEdit::Limits(_) => 16,
                JobEdit::CompactionPolicy(policy) => compaction_bytes(policy.as_ref()),
            },
            OperationKind::SetConversation,
            |batch, (root, edit)| mutations::set_conversation(batch, root, edit),
        )
        .await
    }

    pub async fn hide_conversation(&self, root: JobId) -> Result<JobAction, DatabaseError> {
        self.write(4096, OperationKind::Close, root, |batch, root| {
            mutations::set_conversation(batch, root, JobEdit::Hide)?;
            batch.close(root, Actor::User)
        })
        .await
    }

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
                pin.release();
                drop(permit);
                return;
            }
            match settle_rx.recv() {
                Ok(true) => {
                    pin.release();
                }
                Ok(false) => {}
                Err(_) => {
                    if !submitted_for_worker.load(Ordering::Acquire) {
                        pin.release();
                    }
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
            .write_sized(
                Box::new(published),
                |attachment| {
                    byte_count(attachment.mime.len())
                        .saturating_add(text_bytes(attachment.display_name.as_deref()))
                },
                OperationKind::CreateAttachment,
                move |batch, attachment| {
                    submitted.store(true, Ordering::Release);
                    mutations::create_attachment(batch, *attachment)
                },
            )
            .await;
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

    pub async fn attachments(
        &self,
        before: Option<AttachmentId>,
        limit: usize,
    ) -> Result<Vec<AttachmentRecord>, DatabaseError> {
        self.read(move |database| redb::read_attachments(database, before, limit))
            .await
    }

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

    fn begin_development_write<'a>(
        &self,
        handoff: &'a DevelopmentHandoff,
    ) -> Result<(DevelopmentOutputGuard<'a>, DevelopmentWriteProof), DatabaseError> {
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
        let guard = DevelopmentOutputGuard {
            handoff,
            known: false,
        };
        let proof = DevelopmentWriteProof {
            token,
            revision: handoff.started.revision,
        };
        Ok((guard, proof))
    }

    pub(crate) async fn record_development_tool_decision(
        &self,
        handoff: &DevelopmentHandoff,
        evidence: ToolEvidence,
    ) -> Result<(), DatabaseError> {
        let (mut guard, proof) = self.begin_development_write(handoff)?;
        let result = self
            .write_sized(
                (proof, Box::new(evidence)),
                |(_, evidence)| {
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
                |batch, (proof, evidence)| {
                    mutations::record_tool_decision(batch, &proof, *evidence)
                },
            )
            .await;
        guard.known = !matches!(result, Err(DatabaseError::OutcomeUnknown { .. }));
        result
    }

    pub(crate) async fn record_development_tool_outcome(
        &self,
        handoff: &DevelopmentHandoff,
        call: ToolCallRef,
        outcome: ToolOutcome,
    ) -> Result<(), DatabaseError> {
        let (mut guard, proof) = self.begin_development_write(handoff)?;
        let result = self
            .write_sized(
                (proof, call, Box::new(outcome)),
                |(_, _, outcome)| match outcome.as_ref() {
                    ToolOutcome::Completed { result } => byte_count(result.len()),
                    ToolOutcome::NotApplied { message } => byte_count(message.len()),
                    ToolOutcome::Uncertain { message, .. } => text_bytes(message.as_deref()),
                    ToolOutcome::CompletedWithheld { .. }
                    | ToolOutcome::CompletedElided { .. }
                    | ToolOutcome::NotInvoked { .. } => 0,
                },
                OperationKind::RecordToolOutcome,
                |batch, (proof, call, outcome)| {
                    mutations::record_tool_outcome(batch, &proof, call, *outcome)
                },
            )
            .await;
        guard.known = !matches!(result, Err(DatabaseError::OutcomeUnknown { .. }));
        result
    }

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

impl DatabaseHandle {
    pub(crate) async fn create_scheduled_external_policy(
        &self,
        draft: crate::system::scheduled_external::ScheduledExternalPolicyDraft,
    ) -> Result<entities::ScheduledExternalPolicyRecord, DatabaseError> {
        self.write(
            8192,
            OperationKind::SetScheduledExternalPolicy,
            draft,
            |batch, draft| batch.create_scheduled_external_policy(&draft),
        )
        .await
    }

    pub(crate) async fn update_scheduled_external_policy(
        &self,
        id: entities::ScheduledExternalPolicyId,
        expected_revision: u32,
        draft: crate::system::scheduled_external::ScheduledExternalPolicyDraft,
    ) -> Result<entities::ScheduledExternalPolicyRecord, DatabaseError> {
        self.write(
            8192,
            OperationKind::SetScheduledExternalPolicy,
            (id, expected_revision, draft),
            |batch, (id, rev, draft)| batch.update_scheduled_external_policy(id, rev, &draft),
        )
        .await
    }

    pub(crate) async fn set_scheduled_external_policy_enabled(
        &self,
        id: entities::ScheduledExternalPolicyId,
        expected_revision: u32,
        enabled: bool,
    ) -> Result<entities::ScheduledExternalPolicyRecord, DatabaseError> {
        self.write(
            4096,
            OperationKind::SetScheduledExternalPolicy,
            (id, expected_revision, enabled),
            |batch, (id, rev, enabled)| {
                batch.set_scheduled_external_policy_enabled(id, rev, enabled)
            },
        )
        .await
    }

    pub(crate) async fn revoke_scheduled_external_policy(
        &self,
        id: entities::ScheduledExternalPolicyId,
        expected_revision: u32,
    ) -> Result<entities::ScheduledExternalPolicyRecord, DatabaseError> {
        self.write(
            4096,
            OperationKind::SetScheduledExternalPolicy,
            (id, expected_revision),
            |batch, (id, rev)| batch.revoke_scheduled_external_policy(id, rev),
        )
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

    pub(crate) async fn claim_due_recurrences(
        &self,
        limit: usize,
    ) -> Result<Option<std::time::Duration>, DatabaseError> {
        self.write(
            4096,
            OperationKind::ClaimExecution,
            limit,
            |batch, limit| batch.claim_due_recurrences(limit),
        )
        .await
    }
}
