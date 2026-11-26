use std::collections::BTreeSet;

use super::StorageError;
use crate::db::entities::{
    AttachmentCleanup, AttachmentId, AttachmentOrigin, AttachmentRecord, AuthorizationBasis,
    ChildNoticeDrop, CleanupKind, CommandOwner, CommandStatus, ConfigBody, ConfigId, ConfigKind,
    ConnectionConfig, ContentDigest, EffectConclusion, JobId, JobOutputId, JobOutputRecord,
    LlmParameters, LlmTransport, Measurements, ModelCapability, ModelConfig, NotInvokedReason,
    ProjectCommand, ProjectDecisionConfig, ProjectId, ProjectRecord, Recorded, Recurrence,
    RecurrenceHalt, RecurrencePhase, RequestEventRecord, RequestFingerprint, RequestNumber,
    RequestPayload, SubmissionKey, SubmissionRecord, Timestamp, ToolAdmission, ToolCallRef,
    ToolDecision, ToolEffect, ToolEvidenceRecord, ToolOutcome, ToolPayload, ToolReconciliation,
    UncertainCause, UsageFinality, WithheldReason,
};
use crate::db::entities::{
    ChatFinishReason, CredentialSecret, JobSpec, Origin, OutputKind, RequestEligibility,
    ToolCallRefusal, valid_credential_ref,
};
use crate::db::redb::codec;
use crate::db::redb::codec::TAG_RECURRENCE;
use crate::db::redb::codec::{
    Code, Dec, DecodeResult, Enc, TAG_ATTACHMENT, TAG_CONFIG, TAG_OUTPUT, TAG_PROJECT,
    TAG_REQUEST_EVENT, TAG_SUBMISSION, TAG_TOOL_CALL,
};
use crate::db::redb::schema::{CREDENTIAL_KEY_FILE_NAME, FORMAT_VERSION, record_version_byte};
use std::path::Path;

fn check_table_key(table: &str, key: u128, embedded: u128) -> DecodeResult<()> {
    if key == embedded {
        Ok(())
    } else {
        Err(StorageError::corrupted(format!(
            "{table} row stored under key {key} names {embedded}"
        )))
    }
}

impl JobSpec {
    const TAG: u8 = 53;

    pub(super) fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(Self::TAG);
        e.u128(self.id.get());
        e.ou128(self.agent.map(ConfigId::get));
        e.ou128(self.project.map(ProjectId::get));
        match self.origin {
            Origin::User => e.u8(0),
            Origin::Agent { call } => {
                e.u8(1);
                e.call_ref(&call);
            }
            Origin::System => e.u8(2),
        }
        e.bool(self.keep_open);
        e.ts(self.created_at);
        e.ou128(self.parent_id.map(JobId::get));
        e.0
    }

    pub(super) fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, Self::TAG)?;
        let id = JobId::new(d.u128()?);
        let agent = d.ou128()?.map(ConfigId::new);
        let project = d.ou128()?.map(ProjectId::new);
        let origin = match d.u8()? {
            0 => Origin::User,
            1 => Origin::Agent {
                call: d.call_ref()?,
            },
            2 => Origin::System,
            _ => return Err(StorageError::corrupted("invalid conversation origin")),
        };
        let keep_open = d.bool()?;
        let created_at = d.ts()?;
        let parent_id = d.ou128()?.map(JobId::new);
        d.end()?;
        let shape = match origin {
            Origin::User => keep_open && parent_id.is_none(),
            Origin::Agent { .. } => parent_id.is_some(),
            Origin::System => !keep_open,
        };
        if !shape || id.get() == 0 {
            return Err(StorageError::corrupted("invalid mutable conversation"));
        }
        Ok(JobSpec {
            id,
            created_at,
            parent_id,
            agent,
            project,
            origin,
            keep_open,
        })
    }

    pub(super) fn decode_at(key: u128, bytes: &[u8]) -> DecodeResult<Self> {
        let row = Self::decode(bytes)?;
        check_table_key("conversation", key, row.id.get())?;
        Ok(row)
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
        match &self.body {
            ConfigBody::Connection(c) => {
                e.u8(1);
                e.u8(c.protocol.code());
                e.ostr(c.endpoint.as_deref());
                e.ostr(c.credential_ref.as_deref());
                e.outbound_headers(&c.headers);
            }
            ConfigBody::Model(m) => {
                e.u8(2);
                e.u128(m.connection_id.get());
                e.str(&m.model);
                e.ou32(m.context_window);
                e.opt(m.reasoning_efforts.as_ref(), |e, values| {
                    e.u32(values.len() as u32);
                    for value in values {
                        e.str(value);
                    }
                });
                let p = &m.parameters;
                e.of64(p.temperature);
                e.of64(p.top_p);
                e.ou32(p.max_tokens);
                e.ostr(p.reasoning_effort.as_deref());
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
            ConfigKind::Agent | ConfigKind::DecisionModel | ConfigKind::DecisionAgent => {}
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
                let headers = d.outbound_headers()?;
                ConfigBody::Connection(ConnectionConfig {
                    protocol,
                    endpoint,
                    credential_ref,
                    headers,
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
            ConfigKind::DecisionModel => ConfigBody::DecisionModel(d.decision_model()?),
            ConfigKind::DecisionAgent => ConfigBody::DecisionAgent(d.decision_agent()?),
        };
        let (model_enabled, discovery_visible) = match kind {
            ConfigKind::Connection => (true, d.bool()?),
            ConfigKind::Model => (d.bool()?, true),
            ConfigKind::Agent | ConfigKind::DecisionModel | ConfigKind::DecisionAgent => {
                (true, true)
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
            model_enabled,
            discovery_visible,
            body,
        })
    }

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
    e.ostr(o.summary.as_deref());
    e.ostr(o.code.as_deref());
    e.opt(o.goal_reached.as_ref(), |e, v| e.bool(*v));
    e.ojson(o.value.as_ref());
    e.ts(o.created_at);
    e.opt(o.part.as_ref(), |e, &(request, output, part)| {
        e.u32(request);
        e.u32(output);
        e.u32(part);
    });
    e.opt(o.span.as_ref(), |e, &(offset, len)| {
        e.u32(offset);
        e.u32(len);
    });
    e.opt(o.answer_request.as_ref(), |e, &n| e.u32(n));
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
    let part = d.opt(|d| Ok((d.u32()?, d.u32()?, d.u32()?)))?;
    let span = d.opt(|d| Ok((d.u32()?, d.u32()?)))?;
    let answer_request = d.opt(Dec::u32)?;
    d.end()?;
    if part.is_some_and(|(request, _, _)| {
        request == 0 || !matches!(kind, OutputKind::Progress | OutputKind::Reasoning)
    }) || span.is_some()
        && (part.is_none()
            || summary.is_some()
            || code.is_some()
            || value.is_some()
            || goal_reached.is_some())
        || answer_request.is_some_and(|n| {
            n == 0
                || kind != OutputKind::Result
                || summary.is_some()
                || value.as_ref().is_some_and(|v| v.get("text").is_some())
        })
    {
        return Err(StorageError::corrupted("invalid output text ownership"));
    }
    Ok(JobOutputRecord {
        part,
        span,
        answer_request,
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
    e.ostr(a.display_name.as_deref());
    e.u8(a.origin.code());
    e.ts(a.created_at);
    e.opt(a.cleanup.as_ref(), |e, cleanup| {
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
    e.ostr(p.description.as_deref());
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
        e.opt(command.proposal.as_ref(), |e, job| e.u128(job.get()));
    }
    e.opt(p.decision.as_ref(), |e, policy| {
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

pub(super) fn decode_project_root(key: u128, bytes: &[u8]) -> DecodeResult<(ProjectId, String)> {
    let mut d = Dec::new(bytes, TAG_PROJECT)?;
    let id = ProjectId::new(d.u128()?);
    check_table_key("project", key, id.get())?;
    d.str()?;
    d.ostr()?;
    Ok((id, d.str()?))
}

pub(super) fn encode_tool_evidence(record: &ToolEvidenceRecord) -> Vec<u8> {
    let mut e = Enc::new(TAG_TOOL_CALL);
    e.str(&record.tool_call_id);
    e.str(&record.name);
    e.bool(record.spawns);
    e.ts(record.decision.at);
    let admission = match &record.decision.fact {
        ToolDecision::Refused(refusal) => {
            e.u8(1);
            e.u8(refusal.code());
            None
        }
        ToolDecision::Admitted(admission) => Some((2, admission)),
        ToolDecision::AwaitingApproval(admission) => Some((3, admission)),
        ToolDecision::AwaitingAnswer(admission) => Some((4, admission)),
    };
    if let Some((tag, admission)) = admission {
        e.u8(tag);
        e.str(&admission.handler_version);
        e.u8(admission.effect.code());
        e.str(&admission.parameters);
        e.str(admission.arguments.as_str());
        e.u8(admission.authorization.code());
    }
    e.opt(record.outcome.as_ref(), |e, outcome| {
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
            ToolOutcome::CompletedElided { bytes, digest } => {
                e.u8(7);
                e.u64(*bytes);
                e.0.extend_from_slice(digest.as_bytes());
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
                e.ostr(message.as_deref());
            }
        }
    });
    e.opt(record.reconciliation.as_ref(), |e, reconciliation| {
        e.ts(reconciliation.at);
        let fact = &reconciliation.fact;
        e.u8(fact.conclusion.code());
        e.str(&fact.reconciler);
        e.str(&fact.evidence);
        e.str(&fact.explanation);
    });
    e.opt(record.group.as_ref(), |e, group| {
        e.u32(group.pid);
        e.u64(group.leader_start);
        e.u64(group.sentinel_inode);
        e.opt(group.pid_namespace.as_ref(), |e, ns| e.u64(*ns));
    });
    e.opt(record.child_notice_drop.as_ref(), |e, drop| {
        e.u8(drop.code())
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
            7 => ToolOutcome::CompletedElided {
                bytes: d.u64()?,
                digest: ContentDigest::from_bytes(
                    d.take(32)?
                        .try_into()
                        .map_err(|_| StorageError::corrupted("invalid tool result digest"))?,
                ),
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
    let group = d.opt(|d| {
        Ok(crate::db::entities::CommandGroup {
            pid: d.u32()?,
            leader_start: d.u64()?,
            sentinel_inode: d.u64()?,
            pid_namespace: d.opt(|d| d.u64())?,
        })
    })?;
    if group.is_some_and(|group| {
        group.pid == 0
            || group.pid > i32::MAX as u32
            || group
                .pid_namespace
                .is_some_and(|ns| ns == 0 || group.sentinel_inode == 0)
    }) {
        return Err(StorageError::corrupted("invalid recorded command identity"));
    }
    if group.is_some() && !(spawns && matches!(decision, ToolDecision::Admitted(_))) {
        return Err(StorageError::corrupted(
            "a recorded command group requires a spawning admission",
        ));
    }
    let child_notice_drop = d.opt(|d| d.code(ChildNoticeDrop::from_code))?;
    d.end()?;
    Ok(ToolEvidenceRecord {
        group,
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
        child_notice_drop,
    })
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
        RequestPayload::Sent {
            messages,
            tools,
            parameters,
            dropped_turns,
        } => {
            e.u8(4);
            e.u32(u32::try_from(messages.len()).unwrap_or(u32::MAX));
            for digest in messages {
                e.0.extend_from_slice(digest.as_bytes());
            }
            e.opt(tools.as_ref(), |e, digest| {
                e.0.extend_from_slice(digest.as_bytes())
            });
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
            e.opt(status_code.as_ref(), |e, v| e.u16(*v));
            e.opt(finish_reason.as_ref(), |e, v| e.u8(v.code()));
            e.ostr(raw_finish_reason.as_deref());
            e.opt(failure.as_ref(), Enc::provider_failure);
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

const CREDENTIAL_AAD_PREFIX: &[u8] = b"ocg.credential.v1\0";

fn credential_aad(reference: &str) -> Vec<u8> {
    let mut aad = Vec::with_capacity(CREDENTIAL_AAD_PREFIX.len() + reference.len());
    aad.extend_from_slice(CREDENTIAL_AAD_PREFIX);
    aad.extend_from_slice(reference.as_bytes());
    aad
}

pub(crate) struct CredentialKey {
    key_id: [u8; 8],
    key: aws_lc_rs::aead::LessSafeKey,
}

impl std::fmt::Debug for CredentialKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CredentialKey").finish_non_exhaustive()
    }
}

pub(super) struct EncryptedCredential<'a> {
    pub(super) key_id: [u8; 8],
    pub(super) nonce: [u8; 12],
    pub(super) ciphertext: &'a [u8],
}

pub(super) fn decode_credential<'a>(
    reference: &str,
    bytes: &'a [u8],
) -> DecodeResult<EncryptedCredential<'a>> {
    if !valid_credential_ref(reference) {
        return Err(StorageError::corrupted("invalid credential reference"));
    }
    let Some((&[tag, version], rest)) = bytes.split_first_chunk::<2>() else {
        return Err(StorageError::corrupted("short credential record"));
    };
    if tag != codec::TAG_CREDENTIAL || u64::from(version) != FORMAT_VERSION {
        return Err(StorageError::corrupted("invalid credential record header"));
    }
    let Some((&key_id, rest)) = rest.split_first_chunk::<8>() else {
        return Err(StorageError::corrupted("short credential key id"));
    };
    let Some((&nonce, ciphertext)) = rest.split_first_chunk::<12>() else {
        return Err(StorageError::corrupted("short credential nonce"));
    };
    if !(17..=4096 + 16).contains(&ciphertext.len()) {
        return Err(StorageError::corrupted(
            "invalid credential ciphertext length",
        ));
    }
    Ok(EncryptedCredential {
        key_id,
        nonce,
        ciphertext,
    })
}

impl CredentialKey {
    pub(crate) fn from_bytes(bytes: &[u8]) -> DecodeResult<Self> {
        if bytes.len() != 32 {
            return Err(StorageError::corrupted("ocg.key must be 32 bytes"));
        }
        let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, bytes);
        let key_id: [u8; 8] = digest.as_ref()[..8]
            .try_into()
            .expect("SHA-256 digest has 32 bytes");
        let unbound = aws_lc_rs::aead::UnboundKey::new(&aws_lc_rs::aead::AES_256_GCM, bytes)
            .map_err(|_| StorageError::corrupted("invalid credential key"))?;
        Ok(Self {
            key_id,
            key: aws_lc_rs::aead::LessSafeKey::new(unbound),
        })
    }

    pub(super) fn encrypt(
        &self,
        reference: &str,
        secret: &CredentialSecret,
    ) -> DecodeResult<Vec<u8>> {
        use aws_lc_rs::rand::SecureRandom;
        let mut nonce = [0u8; 12];
        aws_lc_rs::rand::SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| StorageError::corrupted("system random source unavailable"))?;
        let aad = credential_aad(reference);
        let mut ciphertext = secret.as_str().as_bytes().to_vec();
        if self
            .key
            .seal_in_place_append_tag(
                aws_lc_rs::aead::Nonce::assume_unique_for_key(nonce),
                aws_lc_rs::aead::Aad::from(&aad),
                &mut ciphertext,
            )
            .is_err()
        {
            crate::support::secrets::wipe_vec(&mut ciphertext);
            return Err(StorageError::corrupted("credential encryption failed"));
        }
        let mut encoded = Vec::with_capacity(2 + 8 + 12 + ciphertext.len());
        encoded.push(codec::TAG_CREDENTIAL);
        encoded.push(record_version_byte());
        encoded.extend_from_slice(&self.key_id);
        encoded.extend_from_slice(&nonce);
        encoded.extend_from_slice(&ciphertext);
        Ok(encoded)
    }

    pub(super) fn holds(&self, reference: &str, bytes: &[u8]) -> DecodeResult<bool> {
        Ok(decode_credential(reference, bytes)?.key_id == self.key_id)
    }

    pub(super) fn decrypt(
        &self,
        reference: &str,
        bytes: &[u8],
    ) -> DecodeResult<Option<CredentialSecret>> {
        let row = decode_credential(reference, bytes)?;
        if row.key_id != self.key_id {
            return Ok(None);
        }
        let aad = credential_aad(reference);
        let mut buffer = row.ciphertext.to_vec();
        let Ok(plaintext) = self.key.open_in_place(
            aws_lc_rs::aead::Nonce::assume_unique_for_key(row.nonce),
            aws_lc_rs::aead::Aad::from(&aad),
            &mut buffer,
        ) else {
            crate::support::secrets::wipe_vec(&mut buffer);
            return Ok(None);
        };
        let copy = plaintext.to_vec();
        crate::support::secrets::wipe_vec(&mut buffer);
        let secret = match String::from_utf8(copy) {
            Ok(secret) => secret,
            Err(error) => {
                crate::support::secrets::wipe_vec(&mut error.into_bytes());
                return Err(StorageError::corrupted("decrypted credential is not UTF-8"));
            }
        };
        CredentialSecret::new(secret)
            .map(Some)
            .map_err(StorageError::corrupted)
    }
}

pub(crate) fn load_credential_key(
    directory: &Path,
) -> Result<Option<std::sync::Arc<CredentialKey>>, StorageError> {
    let path = directory.join(CREDENTIAL_KEY_FILE_NAME);
    let mut bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(::redb::Error::Io(error).into()),
    };
    let _ = crate::host::data_directory::restrict_file(&path);
    let key = CredentialKey::from_bytes(&bytes);
    crate::support::secrets::wipe_vec(&mut bytes);
    key.map(std::sync::Arc::new).map(Some)
}

pub(crate) fn create_credential_key(
    directory: &Path,
) -> Result<std::sync::Arc<CredentialKey>, StorageError> {
    use aws_lc_rs::rand::SecureRandom;
    use std::io::Write;
    let key_path = directory.join(CREDENTIAL_KEY_FILE_NAME);
    let temp_path = directory.join(format!(
        ".{CREDENTIAL_KEY_FILE_NAME}.{}.{}.tmp",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos())
    ));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::custom_flags(
        &mut options,
        rustix::fs::OFlags::NOFOLLOW.bits() as i32,
    );
    crate::host::data_directory::private_file(&mut options);
    let mut raw = [0u8; 32];
    let prepared = (|| -> Result<_, StorageError> {
        aws_lc_rs::rand::SystemRandom::new()
            .fill(&mut raw)
            .map_err(|_| StorageError::corrupted("system random source unavailable"))?;
        let key = CredentialKey::from_bytes(&raw)?;
        let mut file = options.open(&temp_path).map_err(::redb::Error::Io)?;
        let written = file.write_all(&raw);
        Ok((key, file, written))
    })();
    crate::support::secrets::wipe_bytes(&mut raw);
    let (key, file, written) = prepared?;
    if let Err(error) = written.and_then(|()| file.sync_all()) {
        drop(file);
        let _ = std::fs::remove_file(&temp_path);
        return Err(::redb::Error::Io(error).into());
    }
    drop(file);
    #[cfg(any(target_vendor = "apple", target_os = "linux", target_os = "android"))]
    let renamed = rustix::fs::renameat_with(
        rustix::fs::CWD,
        &temp_path,
        rustix::fs::CWD,
        &key_path,
        rustix::fs::RenameFlags::NOREPLACE,
    )
    .map_err(std::io::Error::from);
    #[cfg(not(any(target_vendor = "apple", target_os = "linux", target_os = "android")))]
    let renamed: std::io::Result<()> = Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "atomic no-replace file publication is unsupported on this platform",
    ));
    if let Err(error) = renamed {
        let _ = std::fs::remove_file(&temp_path);
        return Err(::redb::Error::Io(error).into());
    }
    std::fs::File::open(directory)
        .and_then(|dir| dir.sync_all())
        .map_err(::redb::Error::Io)?;
    let _ = crate::host::data_directory::restrict_file(&key_path);
    Ok(std::sync::Arc::new(key))
}

impl Recurrence {
    pub(super) fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_RECURRENCE)?;
        let phase = match d.u8()? {
            0 => RecurrencePhase::Active,
            1 => RecurrencePhase::WaitingDue { due: d.i64()? },
            2 => RecurrencePhase::Halted {
                reason: d.code_with(RecurrenceHalt::from_code, |_| {
                    StorageError::corrupted("invalid recurrence halt")
                })?,
            },
            _ => return Err(StorageError::corrupted("invalid recurrence phase")),
        };
        let row = Self {
            phase,
            n: d.u64()?,
            current_turn: JobId::new(d.u128()?),
            previous_turn: d.ou128()?.map(JobId::new),
            interval_s: d.u32()?,
        };
        d.end()?;
        Ok(row)
    }

    pub(super) fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_RECURRENCE);
        match self.phase {
            RecurrencePhase::Active => {
                e.u8(0);
            }
            RecurrencePhase::WaitingDue { due } => {
                e.u8(1);
                e.i64(due);
            }
            RecurrencePhase::Halted { reason } => {
                e.u8(2);
                e.u8(reason.code());
            }
        }
        e.u64(self.n);
        e.u128(self.current_turn.get());
        e.ou128(self.previous_turn.map(JobId::get));
        e.u32(self.interval_s);
        e.0
    }
}
