use std::collections::BTreeSet;

use super::*;
use crate::openai_compatible::ChatFinishReason;
use crate::persistence::{
    AgentBehavior, AgentConfig, AttachmentCleanup, AttachmentId, AttachmentOrigin,
    AttachmentRecord, AuthorizationBasis, ClaimRecord, CleanupKind, ConfigBody, ConfigId,
    ConfigKind, ConnectionConfig, ContentDigest, DependencyCondition, EffectConclusion,
    ExecutionOperation, FrozenExecutionConfig, JobId, JobOutputId, JobOutputRecord, LlmParameters,
    LlmTransport, Measurements, ModelCapability, ModelConfig, NotInvokedReason, OutcomeCause,
    OutcomeKind, OutcomeRecord, PoolConfig, ProjectCommand, ProjectId, ProjectRecord, Recorded,
    RequestEventRecord, RequestFingerprint, RequestNumber, RequestPayload, SubmissionKey,
    SubmissionRecord, SuspensionRecord, Timestamp, ToolAdmission, ToolCallRef, ToolDecision,
    ToolEffect, ToolEvidenceRecord, ToolOutcome, ToolPayload, ToolReconciliation, UncertainCause,
    WithheldReason,
};
use crate::token_usage::{RequestEligibility, UsageFinality};

// ---- rows ------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct JobRow {
    pub(crate) id: JobId,
    pub(crate) created_at: Timestamp,
    pub(crate) eligible_at: Option<Timestamp>,
    pub(crate) name: Option<String>,
    pub(crate) parent_id: Option<JobId>,
    pub(crate) agent_id: Option<ConfigId>,
    pub(crate) execution: Option<FrozenExecutionConfig>,
    pub(crate) objective: Option<String>,
    pub(crate) attachments: Vec<AttachmentId>,
    pub(crate) dependencies: Vec<(JobId, DependencyCondition)>,
    pub(crate) max_depth: Option<u32>,
    pub(crate) max_children: Option<u32>,
    pub(crate) max_descendants: Option<u32>,
    pub(crate) retry_of: Option<JobId>,
    pub(crate) continues: Option<JobId>,
    pub(crate) submission: Option<SubmissionKey>,
    pub(crate) project: Option<ProjectId>,
}

pub(crate) fn usage_agent(row: &JobRow) -> u64 {
    row.agent_id.map_or(0, |id| id.get())
}

impl JobRow {
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_JOB);
        e.u64(self.id.get());
        e.ts(self.created_at);
        e.ots(self.eligible_at);
        e.ostr(&self.name);
        e.ou64(self.parent_id.map(|p| p.get()));
        e.ou64(self.agent_id.map(|a| a.get()));
        e.opt(&self.execution, Enc::frozen_execution);
        e.ostr(&self.objective);
        e.u32(self.attachments.len() as u32);
        for a in &self.attachments {
            e.u64(a.get());
        }
        e.u32(self.dependencies.len() as u32);
        for (d, cond) in &self.dependencies {
            e.u64(d.get());
            e.u8(match cond {
                DependencyCondition::Success => 1,
                DependencyCondition::Settled => 2,
            });
        }
        e.ou32(self.max_depth);
        e.ou32(self.max_children);
        e.ou32(self.max_descendants);
        e.ou64(self.retry_of.map(|r| r.get()));
        e.ou64(self.continues.map(|c| c.get()));
        e.opt(&self.submission, |e, s| e.0.extend_from_slice(s.as_bytes()));
        e.ou64(self.project.map(|p| p.get()));
        e.0
    }

    pub(super) fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_JOB)?;
        let id = JobId::new(d.u64()?);
        let created_at = d.ts()?;
        let eligible_at = d.ots()?;
        let name = d.ostr()?;
        let parent_id = d.ou64()?.map(JobId::new);
        let agent_id = d.ou64()?.map(ConfigId::new);
        let execution = d.opt(Dec::frozen_execution)?;
        let objective = d.ostr()?;
        let att_len = d.u32()? as usize;
        let mut attachments = Vec::with_capacity(att_len);
        for _ in 0..att_len {
            attachments.push(AttachmentId::new(d.u64()?));
        }
        let dep_len = d.u32()? as usize;
        let mut dependencies = Vec::with_capacity(dep_len);
        for _ in 0..dep_len {
            let j = JobId::new(d.u64()?);
            let c = match d.u8()? {
                1 => DependencyCondition::Success,
                2 => DependencyCondition::Settled,
                code => {
                    return Err(StorageError::corrupted(format!(
                        "invalid dependency condition code {code}"
                    )));
                }
            };
            dependencies.push((j, c));
        }
        let max_depth = d.ou32()?;
        let max_children = d.ou32()?;
        let max_descendants = d.ou32()?;
        let retry_of = d.ou64()?.map(JobId::new);
        let continues = d.ou64()?.map(JobId::new);
        let submission = d.opt(|d| {
            let bytes = d.take(32)?;
            let arr: [u8; 32] = bytes
                .try_into()
                .map_err(|_| StorageError::corrupted("invalid submission key"))?;
            Ok(SubmissionKey::from_digest(arr))
        })?;
        let project = d.ou64()?.map(ProjectId::new);
        d.end()?;
        Ok(JobRow {
            id,
            created_at,
            eligible_at,
            name,
            parent_id,
            agent_id,
            execution,
            objective,
            attachments,
            dependencies,
            max_depth,
            max_children,
            max_descendants,
            retry_of,
            continues,
            submission,
            project,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct JobStateRow {
    pub(crate) job_id: JobId,
    pub(crate) claim: Option<ClaimRecord>,
    pub(crate) suspension: Option<SuspensionRecord>,
    pub(crate) outcome: Option<OutcomeRecord>,
    pub(crate) stop_confirmed_at: Option<Timestamp>,
    pub(crate) children_closed_at: Option<Timestamp>,
}

impl JobStateRow {
    /// The state of a Job that was just submitted: nothing has happened.
    pub(crate) fn new(job_id: JobId) -> Self {
        Self {
            job_id,
            ..Self::default()
        }
    }

    pub(crate) fn locally_stopped(&self) -> bool {
        match &self.outcome {
            Some(o) => match o.kind {
                OutcomeKind::Completed | OutcomeKind::Failed | OutcomeKind::Cancelled => true,
                OutcomeKind::Unknown => self.stop_confirmed_at.is_some(),
            },
            None => false,
        }
    }

    pub(super) fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_JOB_STATE);
        e.u64(self.job_id.get());
        e.opt(&self.claim, |e, c| {
            e.ts(c.started_at);
            e.u64(c.pool_id.get());
            e.u32(c.pool_edit_seq);
            e.u32(c.capabilities.len() as u32);
            for (op, proto) in &c.capabilities {
                e.u8(op.code());
                e.opt(proto, |e, p| e.u8(p.code()));
            }
            e.u64(c.epoch);
            e.ostr(&c.host_id);
            e.ostr(&c.boot_id);
        });
        e.opt(&self.suspension, |e, s| {
            e.ts(s.since);
            e.u64(s.wait_generation);
            e.u64(s.owner_epoch);
        });
        e.opt(&self.outcome, |e, o| {
            e.u8(o.kind as u8);
            e.ts(o.at);
            e.opt(&o.cause, |e, cause| match cause {
                OutcomeCause::Code(c) => {
                    e.u8(1);
                    e.u8(*c);
                }
                OutcomeCause::Text(t) => {
                    e.u8(2);
                    e.str(t);
                }
                OutcomeCause::Abandoned => {
                    e.u8(3);
                }
                OutcomeCause::DependencyImpossible => {
                    e.u8(4);
                }
            });
        });
        e.ots(self.stop_confirmed_at);
        e.ots(self.children_closed_at);
        e.0
    }

    pub(super) fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_JOB_STATE)?;
        let job_id = JobId::new(d.u64()?);
        let claim = d.opt(|d| {
            let started_at = d.ts()?;
            let pool_id = ConfigId::new(d.u64()?);
            let pool_edit_seq = d.u32()?;
            let cap_len = d.u32()? as usize;
            let mut capabilities = BTreeSet::new();
            for _ in 0..cap_len {
                let op = d.code(ExecutionOperation::from_code)?;
                let proto = d.opt(|d| d.code(LlmTransport::from_code))?;
                capabilities.insert((op, proto));
            }
            let epoch = d.u64()?;
            let host_id = d.ostr()?;
            let boot_id = d.ostr()?;
            Ok(ClaimRecord {
                started_at,
                pool_id,
                pool_edit_seq,
                capabilities,
                epoch,
                host_id,
                boot_id,
            })
        })?;
        let suspension = d.opt(|d| {
            Ok(SuspensionRecord {
                since: d.ts()?,
                wait_generation: d.u64()?,
                owner_epoch: d.u64()?,
            })
        })?;
        let outcome = d.opt(|d| {
            let kind = match d.u8()? {
                1 => OutcomeKind::Completed,
                2 => OutcomeKind::Failed,
                3 => OutcomeKind::Cancelled,
                4 => OutcomeKind::Unknown,
                code => {
                    return Err(StorageError::corrupted(format!(
                        "invalid outcome kind {code}"
                    )));
                }
            };
            let at = d.ts()?;
            let cause = d.opt(|d| match d.u8()? {
                1 => Ok(OutcomeCause::Code(d.u8()?)),
                2 => Ok(OutcomeCause::Text(d.str()?)),
                3 => Ok(OutcomeCause::Abandoned),
                4 => Ok(OutcomeCause::DependencyImpossible),
                code => Err(StorageError::corrupted(format!(
                    "invalid outcome cause {code}"
                ))),
            })?;
            Ok(OutcomeRecord { kind, at, cause })
        })?;
        let stop_confirmed_at = d.ots()?;
        let children_closed_at = d.ots()?;
        d.end()?;
        Ok(JobStateRow {
            job_id,
            claim,
            suspension,
            outcome,
            stop_confirmed_at,
            children_closed_at,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ConfigRow {
    pub(crate) id: ConfigId,
    pub(crate) kind: ConfigKind,
    pub(crate) name: String,
    pub(crate) edit_seq: u32,
    pub(crate) created_at: Timestamp,
    pub(crate) updated_at: Timestamp,
    pub(crate) archived_at: Option<Timestamp>,
    pub(crate) body: ConfigBody,
}

impl ConfigRow {
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_CONFIG);
        e.u64(self.id.get());
        e.u8(self.kind as u8);
        e.str(&self.name);
        e.u32(self.edit_seq);
        e.ts(self.created_at);
        e.ts(self.updated_at);
        e.ots(self.archived_at);
        // CHECK: body tag equals kind
        match &self.body {
            ConfigBody::Connection(c) => {
                e.u8(1);
                e.u8(c.protocol.code());
                e.ostr(&c.endpoint);
                e.ostr(&c.credential_ref);
            }
            ConfigBody::Model(m) => {
                e.u8(2);
                e.u64(m.connection_id.get());
                e.str(&m.model);
                e.ou32(m.context_window);
                let p = &m.parameters;
                e.of64(p.temperature);
                e.of64(p.top_p);
                e.ou32(p.max_tokens);
                e.ostr(&p.reasoning_effort);
                e.ou32(p.max_completion_tokens);
                e.of64(p.presence_penalty);
                e.of64(p.frequency_penalty);
                e.u32(m.declared_capabilities.len() as u32);
                for cap in &m.declared_capabilities {
                    e.u8(cap.code());
                }
            }
            ConfigBody::Agent(a) => {
                e.u8(3);
                e.ostr(&a.instructions);
                e.u8(a.behavior as u8);
                e.u32(a.models.len() as u32);
                for m in &a.models {
                    e.u64(m.get());
                }
                e.ou64(a.default_model.map(|m| m.get()));
                e.ostr(&a.tool_grant);
            }
            ConfigBody::Pool(p) => {
                e.u8(4);
                e.u32(p.capabilities.len() as u32);
                for (op, proto) in &p.capabilities {
                    e.u8(op.code());
                    e.opt(proto, |e, pr| e.u8(pr.code()));
                }
                e.u32(p.concurrency_limit);
                e.bool(p.enabled);
            }
        }
        e.0
    }

    pub(super) fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_CONFIG)?;
        let id = ConfigId::new(d.u64()?);
        let kind = match d.u8()? {
            1 => ConfigKind::Connection,
            2 => ConfigKind::Model,
            3 => ConfigKind::Agent,
            4 => ConfigKind::Pool,
            code => {
                return Err(StorageError::corrupted(format!(
                    "invalid config kind {code}"
                )));
            }
        };
        let name = d.str()?;
        let edit_seq = d.u32()?;
        let created_at = d.ts()?;
        let updated_at = d.ts()?;
        let archived_at = d.ots()?;
        let body_tag = d.u8()?;
        if body_tag != (kind as u8) {
            return Err(StorageError::corrupted(format!(
                "config body tag {body_tag} mismatch with kind {kind:?}"
            )));
        }
        let body = match kind {
            ConfigKind::Connection => {
                let protocol = d.code(LlmTransport::from_code)?;
                let endpoint = d.ostr()?;
                let credential_ref = d.ostr()?;
                ConfigBody::Connection(ConnectionConfig {
                    protocol,
                    endpoint,
                    credential_ref,
                })
            }
            ConfigKind::Model => {
                let connection_id = ConfigId::new(d.u64()?);
                let model = d.str()?;
                let context_window = d.ou32()?;
                let parameters = LlmParameters {
                    temperature: d.of64()?,
                    top_p: d.of64()?,
                    max_tokens: d.ou32()?,
                    reasoning_effort: d.ostr()?,
                    max_completion_tokens: d.ou32()?,
                    presence_penalty: d.of64()?,
                    frequency_penalty: d.of64()?,
                };
                let cap_len = d.u32()? as usize;
                let mut declared_capabilities = BTreeSet::new();
                for _ in 0..cap_len {
                    declared_capabilities.insert(d.code(ModelCapability::from_code)?);
                }
                ConfigBody::Model(ModelConfig {
                    connection_id,
                    model,
                    context_window,
                    parameters,
                    declared_capabilities,
                })
            }
            ConfigKind::Agent => {
                let instructions = d.ostr()?;
                let behavior = match d.u8()? {
                    1 => AgentBehavior::Chat,
                    2 => AgentBehavior::General,
                    code => {
                        return Err(StorageError::corrupted(format!(
                            "invalid agent behavior {code}"
                        )));
                    }
                };
                let models_len = d.u32()? as usize;
                let mut models = Vec::with_capacity(models_len);
                for _ in 0..models_len {
                    models.push(ConfigId::new(d.u64()?));
                }
                let default_model = d.ou64()?.map(ConfigId::new);
                let tool_grant = d.ostr()?;
                ConfigBody::Agent(AgentConfig {
                    instructions,
                    behavior,
                    models,
                    default_model,
                    tool_grant,
                })
            }
            ConfigKind::Pool => {
                let cap_len = d.u32()? as usize;
                let mut capabilities = BTreeSet::new();
                for _ in 0..cap_len {
                    let op = d.code(ExecutionOperation::from_code)?;
                    let proto = d.opt(|d| d.code(LlmTransport::from_code))?;
                    capabilities.insert((op, proto));
                }
                let concurrency_limit = d.u32()?;
                let enabled = d.bool()?;
                ConfigBody::Pool(PoolConfig {
                    capabilities,
                    concurrency_limit,
                    enabled,
                })
            }
        };
        d.end()?;
        Ok(ConfigRow {
            id,
            kind,
            name,
            edit_seq,
            created_at,
            updated_at,
            archived_at,
            body,
        })
    }
}

pub(super) fn encode_output(o: &JobOutputRecord) -> Vec<u8> {
    let mut e = Enc::new(TAG_OUTPUT);
    e.u8(o.kind.code());
    e.ostr(&o.summary);
    e.ostr(&o.code);
    e.opt(&o.goal_reached, |e, v| e.bool(*v));
    e.ojson(&o.value);
    e.ts(o.created_at);
    e.0
}

pub(super) fn decode_output(
    (job_id, seq): (u64, u32),
    bytes: &[u8],
) -> DecodeResult<JobOutputRecord> {
    let mut d = Dec::new(bytes, TAG_OUTPUT)?;
    let kind = d.code(OutputKind::from_code)?;
    let summary = d.ostr()?;
    let code = d.ostr()?;
    let goal_reached = d.opt(Dec::bool)?;
    let value = d.ojson()?;
    let created_at = d.ts()?;
    d.end()?;
    Ok(JobOutputRecord {
        id: JobOutputId::new(seq as u64),
        job: JobId::new(job_id),
        seq: seq as i64,
        kind,
        summary,
        code,
        goal_reached,
        value,
        created_at,
    })
}

pub(super) fn encode_submission(record: &SubmissionRecord) -> Vec<u8> {
    let mut e = Enc::new(TAG_SUBMISSION);
    e.0.extend_from_slice(record.fingerprint.as_bytes());
    e.u64(record.job.get());
    e.ts(record.recorded_at);
    e.0
}

pub(super) fn decode_submission(key: &[u8], bytes: &[u8]) -> DecodeResult<SubmissionRecord> {
    let key = <[u8; 32]>::try_from(key)
        .map_err(|_| StorageError::corrupted("a submission key is not a 32-byte digest"))?;
    let mut d = Dec::new(bytes, TAG_SUBMISSION)?;
    let fingerprint = RequestFingerprint::from_digest(d.take(32)?.try_into().expect("32 bytes"));
    let job = JobId::new(d.u64()?);
    let recorded_at = d.ts()?;
    d.end()?;
    Ok(SubmissionRecord {
        key: SubmissionKey::from_digest(key),
        fingerprint,
        job,
        recorded_at,
    })
}

pub(super) fn encode_attachment(a: &AttachmentRecord) -> Vec<u8> {
    let mut e = Enc::new(TAG_ATTACHMENT);
    e.u64(a.id.get());
    e.0.extend_from_slice(a.sha256.as_bytes());
    e.u64(a.byte_length);
    e.str(&a.mime);
    e.ostr(&a.display_name);
    e.u8(a.origin.code());
    e.ts(a.created_at);
    e.opt(&a.cleanup, |e, cleanup| {
        e.u8(cleanup.kind.code());
        e.ts(cleanup.cleaned_at);
    });
    e.0
}

pub(super) fn decode_attachment(bytes: &[u8]) -> DecodeResult<AttachmentRecord> {
    let mut d = Dec::new(bytes, TAG_ATTACHMENT)?;
    let a = AttachmentRecord {
        id: AttachmentId::new(d.u64()?),
        sha256: ContentDigest::from_bytes(d.take(32)?.try_into().expect("32 bytes")),
        byte_length: d.u64()?,
        mime: d.str()?,
        display_name: d.ostr()?,
        origin: d.code(AttachmentOrigin::from_code)?,
        created_at: d.ts()?,
        cleanup: d.opt(|d| {
            Ok(AttachmentCleanup {
                kind: d.code(CleanupKind::from_code)?,
                cleaned_at: d.ts()?,
            })
        })?,
    };
    d.end()?;
    Ok(a)
}

pub(super) fn encode_project(p: &ProjectRecord) -> Vec<u8> {
    let mut e = Enc::new(TAG_PROJECT);
    e.u64(p.id.get());
    e.str(&p.name);
    e.ostr(&p.description);
    e.str(&p.root);
    e.u32(u32::try_from(p.commands.len()).unwrap_or(u32::MAX));
    for command in &p.commands {
        e.str(&command.name);
        e.u32(u32::try_from(command.argv.len()).unwrap_or(u32::MAX));
        for argument in &command.argv {
            e.str(argument);
        }
    }
    e.ts(p.created_at);
    e.ts(p.updated_at);
    e.0
}

pub(super) fn decode_project(bytes: &[u8]) -> DecodeResult<ProjectRecord> {
    let mut d = Dec::new(bytes, TAG_PROJECT)?;
    let id = ProjectId::new(d.u64()?);
    let name = d.str()?;
    let description = d.ostr()?;
    let root = d.str()?;
    let command_count = d.u32()? as usize;
    let mut commands = Vec::new();
    for _ in 0..command_count {
        let name = d.str()?;
        let argument_count = d.u32()? as usize;
        let mut argv = Vec::new();
        for _ in 0..argument_count {
            argv.push(d.str()?);
        }
        commands.push(ProjectCommand { name, argv });
    }
    let created_at = d.ts()?;
    let updated_at = d.ts()?;
    d.end()?;
    Ok(ProjectRecord {
        id,
        name,
        description,
        root,
        commands,
        created_at,
        updated_at,
    })
}

/// The id and root of a stored Project, without its commands.
pub(super) fn decode_project_root(bytes: &[u8]) -> DecodeResult<(ProjectId, String)> {
    let mut d = Dec::new(bytes, TAG_PROJECT)?;
    let id = ProjectId::new(d.u64()?);
    d.str()?;
    d.ostr()?;
    Ok((id, d.str()?))
}

/// What the owner changed about a conversation root.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ConversationRow {
    pub(crate) title: Option<String>,
    pub(crate) hidden_at: Option<Timestamp>,
}

impl ConversationRow {
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_CONVERSATION);
        e.ostr(&self.title);
        e.ots(self.hidden_at);
        e.0
    }

    pub(super) fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_CONVERSATION)?;
        let row = Self {
            title: d.ostr()?,
            hidden_at: d.ots()?,
        };
        d.end()?;
        Ok(row)
    }
}

pub(super) fn encode_tool_evidence(record: &ToolEvidenceRecord) -> Vec<u8> {
    let mut e = Enc::new(TAG_TOOL_CALL);
    e.str(&record.tool_call_id);
    e.str(&record.name);
    e.bool(record.spawns);
    e.ts(record.decision.at);
    match &record.decision.fact {
        ToolDecision::Refused(refusal) => {
            e.u8(1);
            e.u8(refusal.code());
        }
        ToolDecision::Admitted(admission) => {
            e.u8(2);
            e.str(&admission.handler_version);
            e.u8(admission.effect.code());
            e.str(&admission.parameters);
            e.str(admission.arguments.as_str());
            e.u8(admission.authorization.code());
        }
    }
    e.opt(&record.outcome, |e, outcome| {
        e.ts(outcome.at);
        match &outcome.fact {
            ToolOutcome::Completed { result } => {
                e.u8(1);
                e.str(result.as_str());
            }
            ToolOutcome::CompletedWithheld { reason } => {
                e.u8(2);
                e.u8(reason.code());
            }
            ToolOutcome::NotApplied { message } => {
                e.u8(3);
                e.str(message);
            }
            ToolOutcome::NotInvoked {
                reason: NotInvokedReason::Refused(refusal),
            } => {
                e.u8(4);
                e.u8(refusal.code());
            }
            ToolOutcome::NotInvoked {
                reason: NotInvokedReason::Cancelled,
            } => e.u8(6),
            ToolOutcome::Uncertain { cause, message } => {
                e.u8(5);
                e.u8(cause.code());
                e.ostr(message);
            }
        }
    });
    e.opt(&record.reconciliation, |e, reconciliation| {
        e.ts(reconciliation.at);
        let fact = &reconciliation.fact;
        e.u8(fact.conclusion.code());
        e.str(&fact.reconciler);
        e.str(&fact.evidence);
        e.str(&fact.explanation);
    });
    e.0
}

pub(super) fn decode_tool_evidence(
    call: ToolCallRef,
    bytes: &[u8],
) -> DecodeResult<ToolEvidenceRecord> {
    let mut d = Dec::new(bytes, TAG_TOOL_CALL)?;
    let tool_call_id = d.str()?;
    let name = d.str()?;
    let spawns = d.bool()?;
    let decided_at = d.ts()?;
    let decision = match d.u8()? {
        1 => ToolDecision::Refused(d.code(crate::openai_compatible::ToolCallRefusal::from_code)?),
        2 => ToolDecision::Admitted(ToolAdmission {
            handler_version: d.str()?,
            effect: d.code(ToolEffect::from_code)?,
            parameters: d.str()?,
            arguments: ToolPayload::new(d.str()?),
            authorization: d.code(AuthorizationBasis::from_code)?,
        }),
        _ => return Err(StorageError::corrupted("invalid tool decision")),
    };
    let outcome = d.opt(|d| {
        let at = d.ts()?;
        let fact = match d.u8()? {
            1 => ToolOutcome::Completed {
                result: ToolPayload::new(d.str()?),
            },
            2 => ToolOutcome::CompletedWithheld {
                reason: d.code(WithheldReason::from_code)?,
            },
            3 => ToolOutcome::NotApplied { message: d.str()? },
            4 => ToolOutcome::NotInvoked {
                reason: NotInvokedReason::Refused(
                    d.code(crate::openai_compatible::ToolCallRefusal::from_code)?,
                ),
            },
            5 => ToolOutcome::Uncertain {
                cause: d.code(UncertainCause::from_code)?,
                message: d.ostr()?,
            },
            6 => ToolOutcome::NotInvoked {
                reason: NotInvokedReason::Cancelled,
            },
            _ => return Err(StorageError::corrupted("invalid tool outcome")),
        };
        Ok(Recorded { fact, at })
    })?;
    let reconciliation = d.opt(|d| {
        let at = d.ts()?;
        let fact = ToolReconciliation {
            conclusion: d.code(EffectConclusion::from_code)?,
            reconciler: d.str()?,
            evidence: d.str()?,
            explanation: d.str()?,
        };
        Ok(Recorded { fact, at })
    })?;
    d.end()?;
    Ok(ToolEvidenceRecord {
        call,
        tool_call_id,
        name,
        spawns,
        decision: Recorded {
            fact: decision,
            at: decided_at,
        },
        outcome,
        reconciliation,
    })
}

pub(super) fn job_tool_evidence(
    table: &impl ReadableTable<(u64, u32, u32), &'static [u8]>,
    job: JobId,
) -> Result<Vec<ToolEvidenceRecord>, StorageError> {
    let mut records = Vec::new();
    for entry in table.range((job.get(), 0, 0)..=(job.get(), u32::MAX, u32::MAX))? {
        let (key, bytes) = entry?;
        records.push(decode_tool_evidence(
            tool_call_ref(key.value())?,
            bytes.value(),
        )?);
    }
    Ok(records)
}

pub(super) fn tool_call_ref((job, request, index): (u64, u32, u32)) -> DecodeResult<ToolCallRef> {
    Ok(ToolCallRef {
        job: JobId::new(job),
        request: RequestNumber::new(request)
            .ok_or_else(|| StorageError::corrupted("a stored request number is 0"))?,
        index,
    })
}

pub(super) fn tool_call_key(call: ToolCallRef) -> (u64, u32, u32) {
    (call.job.get(), call.request.get(), call.index)
}

pub(super) fn encode_request_event(r: &RequestEventRecord) -> Vec<u8> {
    let mut e = Enc::new(TAG_REQUEST_EVENT);
    e.ts(r.observed_at);
    e.ts(r.recorded_at);
    match &r.payload {
        RequestPayload::Sent {
            messages,
            tools,
            parameters,
            dropped_turns,
        } => {
            e.u8(4);
            e.u32(u32::try_from(messages.len()).expect("request messages fit u32"));
            for digest in messages {
                e.0.extend_from_slice(digest.as_bytes());
            }
            e.opt(tools, |e, digest| e.0.extend_from_slice(digest.as_bytes()));
            e.0.extend_from_slice(parameters.as_bytes());
            e.ou32(*dropped_turns);
        }
        RequestPayload::Received { digest } => {
            e.u8(5);
            e.0.extend_from_slice(digest.as_bytes());
        }
        RequestPayload::Measurement { measurements } => {
            e.u8(1);
            for ms in [
                measurements.ttfb_ms,
                measurements.reasoning_ttft_ms,
                measurements.output_ttft_ms,
                measurements.request_finished_in_ms,
            ] {
                e.ou64(ms);
            }
            e.usage(&measurements.usage);
        }
        RequestPayload::Availability {
            status_code,
            finish_reason,
            raw_finish_reason,
            failure,
        } => {
            e.u8(2);
            e.opt(status_code, |e, v| e.u16(*v));
            e.opt(finish_reason, |e, v| e.u8(v.code()));
            e.ostr(raw_finish_reason);
            e.opt(failure, Enc::provider_failure);
        }
        RequestPayload::Token {
            eligibility,
            finality,
        } => {
            e.u8(3);
            e.u8(match eligibility {
                RequestEligibility::Eligible => 1,
                RequestEligibility::Excluded => 2,
            });
            e.u8(match finality {
                UsageFinality::Cumulative => 1,
                UsageFinality::Final => 2,
            });
        }
    }
    e.0
}

pub(super) fn decode_request_event(
    job_id: JobId,
    request: RequestNumber,
    seq: u32,
    bytes: &[u8],
) -> DecodeResult<RequestEventRecord> {
    let mut d = Dec::new(bytes, TAG_REQUEST_EVENT)?;
    let observed_at = d.ts()?;
    let recorded_at = d.ts()?;
    let payload = match d.u8()? {
        4 => {
            let mut messages = Vec::new();
            for _ in 0..d.u32()? {
                messages.push(ContentDigest::from_bytes(
                    d.take(32)?.try_into().expect("32 bytes"),
                ));
            }
            let tools = d.opt(|d| {
                Ok(ContentDigest::from_bytes(
                    d.take(32)?.try_into().expect("32 bytes"),
                ))
            })?;
            let parameters = ContentDigest::from_bytes(d.take(32)?.try_into().expect("32 bytes"));
            let dropped_turns = d.ou32()?;
            RequestPayload::Sent {
                messages,
                tools,
                parameters,
                dropped_turns,
            }
        }
        5 => RequestPayload::Received {
            digest: ContentDigest::from_bytes(d.take(32)?.try_into().expect("32 bytes")),
        },
        1 => {
            let ttfb_ms = d.ou64()?;
            let reasoning_ttft_ms = d.ou64()?;
            let output_ttft_ms = d.ou64()?;
            let request_finished_in_ms = d.ou64()?;
            let usage = d.usage()?;
            RequestPayload::Measurement {
                measurements: Measurements {
                    ttfb_ms,
                    reasoning_ttft_ms,
                    output_ttft_ms,
                    request_finished_in_ms,
                    usage,
                },
            }
        }
        2 => {
            let status_code = d.opt(Dec::u16)?;
            let finish_reason = d.opt(|d| d.code(ChatFinishReason::from_code))?;
            let raw_finish_reason = d.ostr()?;
            let failure = d.opt(Dec::provider_failure)?;
            RequestPayload::Availability {
                status_code,
                finish_reason,
                raw_finish_reason,
                failure,
            }
        }
        3 => {
            let eligibility = match d.u8()? {
                1 => RequestEligibility::Eligible,
                2 => RequestEligibility::Excluded,
                code => {
                    return Err(StorageError::corrupted(format!(
                        "invalid token eligibility {code}"
                    )));
                }
            };
            let finality = match d.u8()? {
                1 => UsageFinality::Cumulative,
                2 => UsageFinality::Final,
                code => {
                    return Err(StorageError::corrupted(format!(
                        "invalid token finality {code}"
                    )));
                }
            };
            RequestPayload::Token {
                eligibility,
                finality,
            }
        }
        code => {
            return Err(StorageError::corrupted(format!(
                "invalid request payload kind {code}"
            )));
        }
    };
    d.end()?;
    Ok(RequestEventRecord {
        job_id,
        request,
        seq,
        observed_at,
        recorded_at,
        payload,
    })
}
