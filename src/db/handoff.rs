use crate::ai::providers::openai_compatible::ChatConversation;
use crate::db::entities;
use crate::db::entities::{AttachmentId, Violation};
use crate::db::errors::{DatabaseError, OperationStatus};
use crate::db::redb::StorageErrorKind;
use crate::db::shared::Shared;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub(crate) struct PreparedDevelopmentHandoff {
    pub(super) candidate: crate::db::entities::DevelopmentCandidate,
    image_ids: Vec<AttachmentId>,
    pub(super) request: serde_json::Value,
    pub(super) conversation: Option<ChatConversation>,
    offers_tools: bool,
    pub(super) credentials: DevelopmentHandoffCredentials,
}

#[derive(Default)]
pub(super) struct DevelopmentHandoffCredentials {
    pub(super) items: Vec<DevelopmentHandoffCredential>,
    pub(super) spellings: Option<crate::support::secrets::Spellings>,
}

pub(super) struct DevelopmentHandoffCredential {
    reference: String,
    secret: crate::db::entities::CredentialSecret,
    encrypted: Vec<u8>,
}

impl DevelopmentHandoffCredentials {
    pub(super) fn push(
        &mut self,
        reference: String,
        secret: crate::db::entities::CredentialSecret,
        encrypted: Vec<u8>,
    ) {
        match &mut self.spellings {
            Some(spellings) => spellings.extend(secret.as_str()),
            None => self.spellings = Some(crate::support::secrets::Spellings::new(secret.as_str())),
        }
        self.items.push(DevelopmentHandoffCredential {
            reference,
            secret,
            encrypted,
        });
    }
}

impl PreparedDevelopmentHandoff {
    pub(crate) fn new(
        candidate: crate::db::entities::DevelopmentCandidate,
        image_ids: Vec<AttachmentId>,
        request: serde_json::Value,
        conversation: Option<ChatConversation>,
        offers_tools: bool,
    ) -> Self {
        Self {
            candidate,
            image_ids,
            request,
            conversation,
            offers_tools,
            credentials: DevelopmentHandoffCredentials::default(),
        }
    }

    pub(crate) fn request(&self) -> &serde_json::Value {
        &self.request
    }

    pub(crate) fn generative_configuration(&self) -> Option<&entities::FrozenExecutionConfig> {
        match &self.candidate {
            crate::db::entities::DevelopmentCandidate::Generative(context) => {
                Some(&context.configuration)
            }
            crate::db::entities::DevelopmentCandidate::Decision(_) => None,
        }
    }

    pub(crate) fn decision_configuration(&self) -> Option<&entities::FrozenDecisionConfig> {
        match &self.candidate {
            crate::db::entities::DevelopmentCandidate::Generative(_) => None,
            crate::db::entities::DevelopmentCandidate::Decision(context) => {
                Some(&context.configuration)
            }
        }
    }

    pub(crate) fn is_summary_producer(&self) -> bool {
        matches!(
            &self.candidate,
            crate::db::entities::DevelopmentCandidate::Generative(context) if context.is_summary_work()
        )
    }

    fn provider_credential_reference(&self) -> Option<&str> {
        match &self.candidate {
            crate::db::entities::DevelopmentCandidate::Generative(context) => {
                context.configuration.llm.provider.credential_ref.as_deref()
            }
            crate::db::entities::DevelopmentCandidate::Decision(context) => match &context
                .configuration
            {
                entities::FrozenDecisionConfig::Native(native) => native.credential_ref.as_deref(),
                entities::FrozenDecisionConfig::Generative(generative, _) => {
                    generative.llm.provider.credential_ref.as_deref()
                }
            },
        }
    }

    fn header_secret_references(&self) -> Vec<&str> {
        let headers = match &self.candidate {
            crate::db::entities::DevelopmentCandidate::Generative(context) => {
                &context.configuration.llm.provider.headers
            }
            crate::db::entities::DevelopmentCandidate::Decision(context) => {
                match &context.configuration {
                    entities::FrozenDecisionConfig::Native(native) => &native.headers,
                    entities::FrozenDecisionConfig::Generative(generative, _) => {
                        &generative.llm.provider.headers
                    }
                }
            }
        };
        headers.secret_refs().collect()
    }

    pub(crate) fn credential_references(&self) -> Vec<&str> {
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
        if offers_tools != self.offers_tools {
            return false;
        }
        match &self.candidate {
            crate::db::entities::DevelopmentCandidate::Decision(_) => self.image_ids.is_empty(),
            crate::db::entities::DevelopmentCandidate::Generative(context) => {
                let expected = context
                    .covered_inputs
                    .iter()
                    .flat_map(|turn| &turn.attachments)
                    .chain(context.history.iter().flat_map(|turn| &turn.attachments))
                    .chain(&context.attachments)
                    .map(|attachment| attachment.id);
                expected.eq(self.image_ids.iter().copied())
            }
        }
    }
}

#[allow(dead_code)]
pub(crate) struct DevelopmentHandoff {
    pub(super) prepared: PreparedDevelopmentHandoff,
    pub(super) started: entities::MutableJob,
    pub(super) owner: Arc<Shared>,
    pub(super) output_busy: AtomicBool,
    pub(super) output_uncertain: AtomicBool,
    pub(super) usage_checkpoint: std::sync::Mutex<DevelopmentUsageCheckpoint>,
    pub(super) recorder: std::sync::Mutex<crate::ai::providers::RequestRecorder>,
}

impl DevelopmentHandoff {
    pub(crate) fn job(&self) -> entities::JobId {
        self.started.id
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
        &ChatConversation,
        &serde_json::Value,
        entities::AllocationToken,
    )> {
        let crate::db::entities::DevelopmentCandidate::Generative(context) =
            &self.prepared.candidate
        else {
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
        Option<&ChatConversation>,
    )> {
        let crate::db::entities::DevelopmentCandidate::Decision(context) = &self.prepared.candidate
        else {
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
            self.prepared.conversation.as_ref(),
        ))
    }
}

impl crate::ai::providers::CredentialResolver for DevelopmentHandoff {
    fn resolve<'a>(
        &'a self,
        reference: Option<&'a str>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Option<String>, crate::ai::providers::ProviderExecutionError>,
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
                _ => Err(crate::ai::providers::ProviderExecutionError::Build {
                    message: "handoff credential reference mismatch".to_owned(),
                }),
            }
        })
    }
}

#[derive(Clone, Default)]
pub(super) struct DevelopmentUsageCheckpoint {
    pub(super) request: Option<entities::RequestNumber>,
    pub(super) evidence: entities::TokenEvidence,
    pub(super) last_sequence: u32,
}

impl DevelopmentUsageCheckpoint {
    pub(super) fn evidence(&self) -> entities::TokenEvidence {
        self.evidence.clone()
    }

    pub(super) fn advanced(last_sequence: u32, evidence: entities::TokenEvidence) -> Self {
        Self {
            request: None,
            evidence,
            last_sequence,
        }
    }

    pub(super) fn last_sequence(&self) -> u32 {
        self.last_sequence.max(1)
    }

    pub(super) fn for_request(self, request: entities::RequestNumber) -> Self {
        if self.request == Some(request) {
            self
        } else {
            Self::default()
        }
    }

    pub(super) fn of_request(mut self, request: entities::RequestNumber) -> Self {
        self.request = Some(request);
        self
    }
}

pub(super) struct DevelopmentOutputGuard<'a> {
    pub(super) handoff: &'a DevelopmentHandoff,
    pub(super) known: bool,
}

impl Drop for DevelopmentOutputGuard<'_> {
    fn drop(&mut self) {
        if !self.known {
            self.handoff.output_uncertain.store(true, Ordering::SeqCst);
        }
        self.handoff.output_busy.store(false, Ordering::SeqCst);
    }
}

pub(super) fn development_contains_secret(
    value: &serde_json::Value,
    secret: &crate::support::secrets::Spellings,
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
    pub(super) token: entities::AllocationToken,
    pub(super) revision: u64,
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
        detail: String,
    },
    Database(Box<DatabaseError>),
    OutcomeUnknown {
        error: Box<DatabaseError>,
        reconciliation: Result<OperationStatus, Box<DatabaseError>>,
    },
}

pub(crate) fn classify_unstarted(error: DatabaseError) -> DevelopmentHandoffError {
    let cause = match &error {
        DatabaseError::Rejected(Violation::InvalidValue(field, reason))
            if *field == "credential" || reason.contains("credential") =>
        {
            Some(entities::UnstartedCause::FrozenSecretUnavailable)
        }
        DatabaseError::Rejected(Violation::InvalidValue(
            "handoff",
            "execution snapshot changed",
        )) => Some(entities::UnstartedCause::ExecutorUnavailable),
        DatabaseError::Rejected(_) => Some(entities::UnstartedCause::StartRefused),
        DatabaseError::Read(storage)
            if storage.kind == StorageErrorKind::Other
                && storage.detail == "Pool or Executor configuration changed" =>
        {
            Some(entities::UnstartedCause::ExecutorUnavailable)
        }
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
            detail: error.to_string(),
        },
        None => DevelopmentHandoffError::Database(Box::new(error)),
    }
}
