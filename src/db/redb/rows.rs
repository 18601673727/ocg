use std::collections::BTreeSet;

use super::codec::{TAG_JOB_INPUT, TAG_NOTICE};
use super::*;
use crate::db::entities::{
    AttachmentCleanup, AttachmentId, AttachmentOrigin, AttachmentRecord, AuthorizationBasis,
    ClaimRecord, CleanupKind, CommandOwner, CommandStatus, ConfigBody, ConfigId, ConfigKind,
    ConnectionConfig, ContentDigest, EffectConclusion, ExecutionOperation, JobClosing, JobId,
    JobOutputId, JobOutputRecord, LlmParameters, LlmTransport, Measurements, ModelCapability,
    ModelConfig, NotInvokedReason, OutcomeCause, OutcomeKind, OutcomeRecord, PoolConfig,
    ProjectCommand, ProjectDecisionConfig, ProjectId, ProjectRecord, Recorded, RequestEventRecord,
    RequestFingerprint, RequestNumber, RequestPayload, SubmissionKey, SubmissionRecord, Timestamp,
    ToolAdmission, ToolCallRef, ToolDecision, ToolEffect, ToolEvidenceRecord, ToolOutcome,
    ToolPayload, ToolReconciliation, UncertainCause, UsageFinality, WithheldReason,
};

// ---- rows ------------------------------------------------------------------------

/// Refuses a record stored under `key` whose embedded identity names another
/// record: the table key is the authority, never a duplicate inside the value.
fn check_table_key(table: &str, key: u128, embedded: u128) -> DecodeResult<()> {
    if key == embedded {
        Ok(())
    } else {
        Err(StorageError::corrupted(format!(
            "{table} row stored under key {key} names {embedded}"
        )))
    }
}

pub(crate) fn usage_agent(row: &JobSpec, parent: Option<&JobSpec>) -> u128 {
    match &row.kind {
        JobKind::Group { agent, .. } => agent.map_or(0, ConfigId::get),
        JobKind::Execution {
            execution: JobExecution::Frozen(execution),
            plan_source: PlanSource::Process { .. },
            ..
        } => execution.agent_ref.map_or(0, |agent| agent.id.get()),
        JobKind::Execution { .. } => parent.map_or(0, |parent| usage_agent(parent, None)),
    }
}

impl JobSpec {
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_JOB);
        e.u128(self.id.get());
        e.job_kind(&self.kind);
        e.ts(self.created_at);
        e.ou128(self.parent_id.map(JobId::get));
        e.opt(&self.allocation, |e, allocation| {
            e.u128(allocation.owner.get());
            e.bool(allocation.synthesis);
        });
        e.0
    }

    pub(super) fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_JOB)?;
        let id = JobId::new(d.u128()?);
        let kind = d.job_kind()?;
        let created_at = d.ts()?;
        let parent_id = d.ou128()?.map(JobId::new);
        let allocation = d.opt(|d| {
            Ok(crate::db::entities::JobAllocation {
                owner: JobId::new(d.u128()?),
                synthesis: d.bool()?,
            })
        })?;
        d.end()?;
        Ok(JobSpec {
            id,
            created_at,
            parent_id,
            allocation,
            kind,
        })
    }

    /// Decodes the `job` row stored under `key`, which must name this Job.
    pub(super) fn decode_at(key: u128, bytes: &[u8]) -> DecodeResult<Self> {
        let row = Self::decode(bytes)?;
        check_table_key("job", key, row.id.get())?;
        Ok(row)
    }
}

impl JobState {
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
        e.u128(self.job_id.get());
        e.opt(&self.claim, |e, c| {
            e.ts(c.started_at);
            e.u128(c.pool_id.get());
            e.u32(c.pool_edit_seq);
            e.u32(c.capabilities.len() as u32);
            for (op, proto) in &c.capabilities {
                e.u8(op.code());
                e.opt(proto, |e, p| e.u8(p.code()));
            }
            e.u64(c.epoch);
            e.ostr(&c.host_id);
            e.ostr(&c.boot_id);
            e.u32(c.notice_seq);
        });
        e.opt(&self.outcome, |e, o| {
            e.u8(o.kind.code());
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
                OutcomeCause::DependencyImpossible => e.u8(4),
                OutcomeCause::Stopped { record } => {
                    e.u8(5);
                    e.u32(*record);
                }
                OutcomeCause::Closed => e.u8(6),
            });
        });
        e.ots(self.stop_confirmed_at);
        e.opt(&self.stop_confirmed_by, |e, proof| e.u8(proof.code()));
        e.0
    }

    pub(super) fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_JOB_STATE)?;
        let job_id = JobId::new(d.u128()?);
        let claim = d.opt(|d| {
            let started_at = d.ts()?;
            let pool_id = ConfigId::new(d.u128()?);
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
            let notice_seq = d.u32()?;
            Ok(ClaimRecord {
                started_at,
                pool_id,
                pool_edit_seq,
                capabilities,
                epoch,
                host_id,
                boot_id,
                notice_seq,
            })
        })?;
        let outcome = d.opt(|d| {
            let kind = d.code_with(OutcomeKind::from_code, |code| {
                StorageError::corrupted(format!("invalid outcome kind {code}"))
            })?;
            let at = d.ts()?;
            let cause = d.opt(|d| match d.u8()? {
                1 => Ok(OutcomeCause::Code(d.u8()?)),
                2 => Ok(OutcomeCause::Text(d.str()?)),
                3 => Ok(OutcomeCause::Abandoned),
                4 => Ok(OutcomeCause::DependencyImpossible),
                5 => Ok(OutcomeCause::Stopped { record: d.u32()? }),
                6 => Ok(OutcomeCause::Closed),
                code => Err(StorageError::corrupted(format!(
                    "invalid outcome cause {code}"
                ))),
            })?;
            Ok(OutcomeRecord { kind, at, cause })
        })?;
        let stop_confirmed_at = d.ots()?;
        let stop_confirmed_by = d.opt(|d| {
            d.code_with(StopProof::from_code, |_| {
                StorageError::corrupted("invalid stop proof")
            })
        })?;
        d.end()?;
        Ok(JobState {
            job_id,
            claim,
            outcome,
            stop_confirmed_at,
            stop_confirmed_by,
        })
    }

    /// Decodes the `job_state` row stored under `key`, which must name this Job.
    pub(super) fn decode_at(key: u128, bytes: &[u8]) -> DecodeResult<Self> {
        let state = Self::decode(bytes)?;
        check_table_key("job_state", key, state.job_id.get())?;
        Ok(state)
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
    pub(crate) model_enabled: bool,
    pub(crate) discovery_visible: bool,
    pub(crate) body: ConfigBody,
}

impl ConfigRow {
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_CONFIG);
        e.u128(self.id.get());
        e.u8(self.kind.code());
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
                e.u128(m.connection_id.get());
                e.str(&m.model);
                e.ou32(m.context_window);
                e.opt(&m.reasoning_efforts, |e, values| {
                    e.u32(values.len() as u32);
                    for value in values {
                        e.str(value);
                    }
                });
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
                e.agent_config(a);
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
            ConfigBody::DecisionModel(model) => {
                e.u8(5);
                e.decision_model(model);
            }
            ConfigBody::DecisionAgent(agent) => {
                e.u8(6);
                e.decision_agent(agent);
            }
        }
        match self.kind {
            ConfigKind::Connection => e.bool(self.discovery_visible),
            ConfigKind::Model => e.bool(self.model_enabled),
            ConfigKind::Agent
            | ConfigKind::Pool
            | ConfigKind::DecisionModel
            | ConfigKind::DecisionAgent => {}
        }
        e.0
    }

    pub(super) fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_CONFIG)?;
        let id = ConfigId::new(d.u128()?);
        let kind = d.code_with(ConfigKind::from_code, |code| {
            StorageError::corrupted(format!("invalid config kind {code}"))
        })?;
        let name = d.str()?;
        let edit_seq = d.u32()?;
        let created_at = d.ts()?;
        let updated_at = d.ts()?;
        let archived_at = d.ots()?;
        let body_tag = d.u8()?;
        if body_tag != kind.code() {
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
                let connection_id = ConfigId::new(d.u128()?);
                let model = d.str()?;
                let context_window = d.ou32()?;
                let reasoning_efforts = d.opt(|d| {
                    let n = d.u32()?;
                    (0..n).map(|_| d.str()).collect::<Result<Vec<_>, _>>()
                })?;
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
                    reasoning_efforts,
                    connection_id,
                    model,
                    context_window,
                    parameters,
                    declared_capabilities,
                })
            }
            ConfigKind::Agent => ConfigBody::Agent(d.agent_config()?),
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
            ConfigKind::DecisionModel => ConfigBody::DecisionModel(d.decision_model()?),
            ConfigKind::DecisionAgent => ConfigBody::DecisionAgent(d.decision_agent()?),
        };
        let (model_enabled, discovery_visible) = match kind {
            ConfigKind::Connection => (true, d.bool()?),
            ConfigKind::Model => (d.bool()?, true),
            ConfigKind::Agent
            | ConfigKind::Pool
            | ConfigKind::DecisionModel
            | ConfigKind::DecisionAgent => (true, true),
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
            model_enabled,
            discovery_visible,
            body,
        })
    }

    /// Decodes the `config` row stored under `key`, which must name this Config.
    pub(super) fn decode_at(key: u128, bytes: &[u8]) -> DecodeResult<Self> {
        let row = Self::decode(bytes)?;
        check_table_key("config", key, row.id.get())?;
        Ok(row)
    }
}

pub(super) fn encode_output(o: &JobOutputRecord) -> Vec<u8> {
    let mut e = Enc::new(TAG_OUTPUT);
    e.u128(o.id.get());
    e.u8(o.kind.code());
    e.ostr(&o.summary);
    e.ostr(&o.code);
    e.opt(&o.goal_reached, |e, v| e.bool(*v));
    e.ojson(&o.value);
    e.ts(o.created_at);
    e.0
}

pub(super) fn output_kind(bytes: &[u8]) -> DecodeResult<OutputKind> {
    let mut d = Dec::new(bytes, TAG_OUTPUT)?;
    d.u128()?;
    d.code(OutputKind::from_code)
}

pub(super) fn decode_output(
    (job_id, seq): (u128, u32),
    bytes: &[u8],
) -> DecodeResult<JobOutputRecord> {
    let mut d = Dec::new(bytes, TAG_OUTPUT)?;
    let id = JobOutputId::new(d.u128()?);
    let kind = d.code(OutputKind::from_code)?;
    let summary = d.ostr()?;
    let code = d.ostr()?;
    let goal_reached = d.opt(Dec::bool)?;
    let value = d.ojson()?;
    let created_at = d.ts()?;
    d.end()?;
    Ok(JobOutputRecord {
        id,
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
    e.u128(record.job.get());
    e.u32(record.seq);
    e.ts(record.recorded_at);
    e.0
}

pub(super) fn decode_submission(key: &[u8], bytes: &[u8]) -> DecodeResult<SubmissionRecord> {
    let key = <[u8; 32]>::try_from(key)
        .map_err(|_| StorageError::corrupted("a submission key is not a 32-byte digest"))?;
    let mut d = Dec::new(bytes, TAG_SUBMISSION)?;
    let fingerprint = RequestFingerprint::from_digest(d.take_array()?);
    let job = JobId::new(d.u128()?);
    let seq = d.u32()?;
    let recorded_at = d.ts()?;
    d.end()?;
    Ok(SubmissionRecord {
        key: SubmissionKey::from_digest(key),
        fingerprint,
        job,
        seq,
        recorded_at,
    })
}

pub(super) fn encode_attachment(a: &AttachmentRecord) -> Vec<u8> {
    let mut e = Enc::new(TAG_ATTACHMENT);
    e.u128(a.id.get());
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

pub(super) fn decode_attachment(key: u128, bytes: &[u8]) -> DecodeResult<AttachmentRecord> {
    let mut d = Dec::new(bytes, TAG_ATTACHMENT)?;
    let a = AttachmentRecord {
        id: AttachmentId::new(d.u128()?),
        sha256: ContentDigest::from_bytes(d.take_array()?),
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
    check_table_key("attachment", key, a.id.get())?;
    Ok(a)
}

pub(super) fn encode_project(p: &ProjectRecord) -> Vec<u8> {
    let mut e = Enc::new(TAG_PROJECT);
    e.u128(p.id.get());
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
        e.u8(command.owner.code());
        e.u8(command.status.code());
        e.opt(&command.proposal, |e, job| e.u128(job.get()));
    }
    e.opt(&p.decision, |e, policy| {
        e.bool(policy.enabled);
        e.ou128(policy.decision_profile.map(ConfigId::get));
        e.u32(u32::from(policy.minimum_confidence_millis));
    });
    e.u64(p.decision_revision);
    e.ts(p.created_at);
    e.ts(p.updated_at);
    e.0
}

pub(super) fn decode_project(key: u128, bytes: &[u8]) -> DecodeResult<ProjectRecord> {
    let mut d = Dec::new(bytes, TAG_PROJECT)?;
    let id = ProjectId::new(d.u128()?);
    check_table_key("project", key, id.get())?;
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
        let owner = d.code_with(CommandOwner::from_code, |_| {
            StorageError::corrupted("invalid command owner")
        })?;
        let status = d.code_with(CommandStatus::from_code, |_| {
            StorageError::corrupted("invalid command status")
        })?;
        let proposal = d.opt(|d| Ok(JobId::new(d.u128()?)))?;
        commands.push(ProjectCommand {
            name,
            argv,
            owner,
            status,
            proposal,
        });
    }
    let decision = d.opt(|d| {
        Ok(ProjectDecisionConfig {
            enabled: d.bool()?,
            decision_profile: d.ou128()?.map(ConfigId::new),
            minimum_confidence_millis: u16::try_from(d.u32()?)
                .map_err(|_| StorageError::corrupted("invalid command confidence"))?,
        })
    })?;
    let decision_revision = d.u64()?;
    let created_at = d.ts()?;
    let updated_at = d.ts()?;
    d.end()?;
    Ok(ProjectRecord {
        id,
        name,
        description,
        root,
        commands,
        decision,
        decision_revision,
        created_at,
        updated_at,
    })
}

/// The id and root of a stored Project, without its commands.
pub(super) fn decode_project_root(key: u128, bytes: &[u8]) -> DecodeResult<(ProjectId, String)> {
    let mut d = Dec::new(bytes, TAG_PROJECT)?;
    let id = ProjectId::new(d.u128()?);
    check_table_key("project", key, id.get())?;
    d.str()?;
    d.ostr()?;
    Ok((id, d.str()?))
}

impl JobControl {
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_JOB_CONTROL);
        e.ostr(&self.title);
        e.ots(self.hidden_at);
        e.ou32(self.on_hold);
        e.u8(self.loop_count);
        e.opt(&self.closing, |e, closing| {
            e.ts(closing.at);
            e.actor(&closing.by);
            e.bool(closing.running);
        });
        e.opt(&self.limits, |e, limits| {
            e.u8(limits.max_depth);
            e.u8(limits.max_auto_turns);
            e.u32(limits.max_iterations);
        });
        e.opt(&self.compaction_policy, Enc::compaction_policy);
        e.u64(self.compaction_revision);
        e.0
    }

    pub(super) fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_JOB_CONTROL)?;
        let row = Self {
            title: d.ostr()?,
            hidden_at: d.ots()?,
            on_hold: d.ou32()?,
            loop_count: d.u8()?,
            closing: d.opt(|d| {
                Ok(JobClosing {
                    at: d.ts()?,
                    by: d.actor()?,
                    running: d.bool()?,
                })
            })?,
            limits: d.opt(|d| {
                Ok(JobLimits {
                    max_depth: d.u8()?,
                    max_auto_turns: d.u8()?,
                    max_iterations: d.u32()?,
                })
            })?,
            compaction_policy: d.opt(Dec::compaction_policy)?,
            compaction_revision: d.u64()?,
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
    e.opt(&record.approval, |e, call| e.call_ref(call));
    e.ts(record.decision.at);
    match &record.decision.fact {
        ToolDecision::Refused(refusal) => {
            e.u8(1);
            e.u8(refusal.code());
        }
        ToolDecision::Admitted(admission)
        | ToolDecision::AwaitingApproval(admission)
        | ToolDecision::AwaitingAnswer(admission) => {
            e.u8(match record.decision.fact {
                ToolDecision::Admitted(_) => 2,
                ToolDecision::AwaitingApproval(_) => 3,
                ToolDecision::AwaitingAnswer(_) => 4,
                ToolDecision::Refused(_) => unreachable!(),
            });
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
    let approval = d.opt(|d| d.call_ref())?;
    let decided_at = d.ts()?;
    let decision = match d.u8()? {
        1 => ToolDecision::Refused(d.code(ToolCallRefusal::from_code)?),
        tag @ 2..=4 => {
            let admission = ToolAdmission {
                handler_version: d.str()?,
                effect: d.code(ToolEffect::from_code)?,
                parameters: d.str()?,
                arguments: ToolPayload::new(d.str()?),
                authorization: d.code(AuthorizationBasis::from_code)?,
            };
            match tag {
                2 => ToolDecision::Admitted(admission),
                3 => ToolDecision::AwaitingApproval(admission),
                _ => ToolDecision::AwaitingAnswer(admission),
            }
        }
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
                reason: NotInvokedReason::Refused(d.code(ToolCallRefusal::from_code)?),
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
        approval,
        decision: Recorded {
            fact: decision,
            at: decided_at,
        },
        outcome,
        reconciliation,
    })
}

pub(super) fn job_tool_evidence(
    table: &impl ReadableTable<(u128, u32, u32), &'static [u8]>,
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

pub(super) fn tool_call_ref((job, request, index): (u128, u32, u32)) -> DecodeResult<ToolCallRef> {
    Ok(ToolCallRef {
        job: JobId::new(job),
        request: RequestNumber::new(request)
            .ok_or_else(|| StorageError::corrupted("a stored request number is 0"))?,
        index,
    })
}

pub(super) fn tool_call_key(call: ToolCallRef) -> (u128, u32, u32) {
    (call.job.get(), call.request.get(), call.index)
}

pub(super) fn encode_request_event(r: &RequestEventRecord) -> Vec<u8> {
    let mut e = Enc::new(TAG_REQUEST_EVENT);
    e.u128(r.id.get());
    e.ts(r.observed_at);
    e.ts(r.recorded_at);
    match &r.payload {
        RequestPayload::ApprovedCalls => e.u8(6),
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
            e.u8(eligibility.code());
            e.u8(finality.code());
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
    let id = crate::db::entities::RequestEventId::new(d.u128()?);
    let observed_at = d.ts()?;
    let recorded_at = d.ts()?;
    let payload = match d.u8()? {
        6 => RequestPayload::ApprovedCalls,
        4 => {
            let mut messages = Vec::new();
            for _ in 0..d.u32()? {
                messages.push(ContentDigest::from_bytes(d.take_array()?));
            }
            let tools = d.opt(|d| Ok(ContentDigest::from_bytes(d.take_array()?)))?;
            let parameters = ContentDigest::from_bytes(d.take_array()?);
            let dropped_turns = d.ou32()?;
            RequestPayload::Sent {
                messages,
                tools,
                parameters,
                dropped_turns,
            }
        }
        5 => RequestPayload::Received {
            digest: ContentDigest::from_bytes(d.take_array()?),
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
            let eligibility = d.code_with(RequestEligibility::from_code, |code| {
                StorageError::corrupted(format!("invalid token eligibility {code}"))
            })?;
            let finality = d.code_with(UsageFinality::from_code, |code| {
                StorageError::corrupted(format!("invalid token finality {code}"))
            })?;
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
        id,
        job_id,
        request,
        seq,
        observed_at,
        recorded_at,
        payload,
    })
}

impl JobInputEntry {
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_JOB_INPUT);
        e.entry(&self.entry);
        e.ts(self.at);
        e.0
    }
    pub(super) fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_JOB_INPUT)?;
        let row = Self {
            entry: d.entry()?,
            at: d.ts()?,
        };
        d.end()?;
        Ok(row)
    }
}
impl Notice {
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_NOTICE);
        e.u8(self.class.code());
        match &self.body {
            NoticeBody::Event {
                child,
                event,
                reference,
            } => {
                e.u8(0);
                e.u128(child.get());
                e.notice_event(event);
                e.u128(reference.get());
            }
            NoticeBody::Record { event, by, at } => {
                e.u8(1);
                e.u8(event.code());
                e.actor(by);
                e.ts(*at);
            }
        };
        e.0
    }
    pub(super) fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_NOTICE)?;
        let class = d.code_with(NoticeClass::from_code, |_| {
            StorageError::corrupted("invalid notice class")
        })?;
        let body = match d.u8()? {
            0 => NoticeBody::Event {
                child: JobId::new(d.u128()?),
                event: d.notice_event()?,
                reference: JobId::new(d.u128()?),
            },
            1 => NoticeBody::Record {
                event: d.code_with(RecordEvent::from_code, |_| {
                    StorageError::corrupted("invalid record event")
                })?,
                by: d.actor()?,
                at: d.ts()?,
            },
            _ => return Err(StorageError::corrupted("invalid notice body")),
        };
        d.end()?;
        Ok(Self { class, body })
    }
}
