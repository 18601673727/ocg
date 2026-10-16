use super::*;

// ---- rows ------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct JobRow {
    pub(crate) id: JobId,
    pub(crate) name: Option<String>,
    pub(crate) execution_mode: ExecutionMode,
    pub(crate) eligible_at: Option<Timestamp>,
    pub(crate) started_at: Option<Timestamp>,
    pub(crate) paused_at: Option<Timestamp>,
    pub(crate) sterilized_at: Option<Timestamp>,
    pub(crate) completed_at: Option<Timestamp>,
    pub(crate) failed_at: Option<Timestamp>,
    pub(crate) cancelled_at: Option<Timestamp>,
    pub(crate) unknown_at: Option<Timestamp>,
    pub(crate) stop_confirmed_at: Option<Timestamp>,
    pub(crate) next_due_at: Option<Timestamp>,
    pub(crate) next_iteration_no: Option<i64>,
    pub(crate) iteration_no: Option<i64>,
    pub(crate) scheduled_at: Option<Timestamp>,
    pub(crate) created_at: Timestamp,
    pub(crate) updated_at: Timestamp,
}

impl JobRow {
    pub(crate) fn confirmed_terminal(&self) -> bool {
        self.completed_at.is_some() || self.failed_at.is_some() || self.cancelled_at.is_some()
    }

    pub(crate) fn any_terminal(&self) -> bool {
        self.confirmed_terminal() || self.unknown_at.is_some()
    }

    /// Whether the Job has neither started nor reached any outcome, unknown
    /// included. Whether it is a member of [`PENDING_JOBS`] also depends on
    /// it not being the Genesis Job, which the row does not say.
    pub(super) fn awaits_start(&self) -> bool {
        self.started_at.is_none() && !self.any_terminal()
    }

    /// The Job's [`PENDING_JOBS`] key.
    pub(super) fn pending_key(&self) -> (i64, u64) {
        (self.created_at.as_unix_micros(), self.id.get())
    }

    /// Whether the Job's own execution is over: it has a confirmed outcome,
    /// or an unknown outcome whose execution the trusted path confirmed
    /// stopped. An unknown outcome without that confirmation may still be
    /// executing. This says nothing of the Job's children; whether they are
    /// over too is its subtree settlement, read from the waiting tables.
    pub(crate) fn locally_stopped(&self) -> bool {
        self.confirmed_terminal() || (self.unknown_at.is_some() && self.stop_confirmed_at.is_some())
    }

    /// The row-only reading that predates the subtree distinction, kept so
    /// the tests that pin it still compile. Production code asks
    /// [`Self::locally_stopped`] or the graph-aware settlement instead.
    #[cfg(test)]
    pub(crate) fn settled(&self) -> bool {
        self.locally_stopped()
    }

    /// Whether this Job holds the Profiles it was submitted with: until a
    /// confirmed outcome, or, for an unknown outcome, until its execution is
    /// confirmed stopped.
    pub(crate) fn holds_profiles(&self) -> bool {
        !self.confirmed_terminal() && self.stop_confirmed_at.is_none()
    }

    pub(super) fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_JOB);
        e.u64(self.id.get());
        e.ostr(&self.name);
        e.u8(self.execution_mode.code());
        for at in [
            self.eligible_at,
            self.started_at,
            self.paused_at,
            self.sterilized_at,
            self.completed_at,
            self.failed_at,
            self.cancelled_at,
            self.unknown_at,
            self.stop_confirmed_at,
            self.next_due_at,
        ] {
            e.ots(at);
        }
        e.oi64(self.next_iteration_no);
        e.oi64(self.iteration_no);
        e.ots(self.scheduled_at);
        e.ts(self.created_at);
        e.ts(self.updated_at);
        e.0
    }

    pub(super) fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_JOB)?;
        let row = JobRow {
            id: JobId::new(d.u64()?),
            name: d.ostr()?,
            execution_mode: d.code(ExecutionMode::from_code)?,
            eligible_at: d.ots()?,
            started_at: d.ots()?,
            paused_at: d.ots()?,
            sterilized_at: d.ots()?,
            completed_at: d.ots()?,
            failed_at: d.ots()?,
            cancelled_at: d.ots()?,
            unknown_at: d.ots()?,
            stop_confirmed_at: d.ots()?,
            next_due_at: d.ots()?,
            next_iteration_no: d.oi64()?,
            iteration_no: d.oi64()?,
            scheduled_at: d.ots()?,
            created_at: d.ts()?,
            updated_at: d.ts()?,
        };
        d.end()?;
        Ok(row)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct JobInputRow {
    pub(crate) id: JobInputId,
    pub(crate) submission_id: Option<String>,
    pub(crate) objective: Option<String>,
    pub(crate) max_depth: Option<u32>,
    pub(crate) max_children_per_job: Option<u32>,
    pub(crate) max_total_descendants: Option<u32>,
    pub(crate) submitted_at: Option<Timestamp>,
    pub(crate) created_at: Timestamp,
    pub(crate) updated_at: Timestamp,
    pub(crate) configuration: Option<FrozenExecutionConfig>,
}

impl JobInputRow {
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_INPUT);
        e.u64(self.id.get());
        e.ostr(&self.submission_id);
        e.ostr(&self.objective);
        for limit in [
            self.max_depth,
            self.max_children_per_job,
            self.max_total_descendants,
        ] {
            e.ou32(limit);
        }
        e.ots(self.submitted_at);
        e.ts(self.created_at);
        e.ts(self.updated_at);
        e.opt(&self.configuration, Enc::frozen_execution);
        e.0
    }

    pub(super) fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_INPUT)?;
        let row = JobInputRow {
            id: JobInputId::new(d.u64()?),
            submission_id: d.ostr()?,
            objective: d.ostr()?,
            max_depth: d.ou32()?,
            max_children_per_job: d.ou32()?,
            max_total_descendants: d.ou32()?,
            submitted_at: d.ots()?,
            created_at: d.ts()?,
            updated_at: d.ts()?,
            configuration: d.opt(Dec::frozen_execution)?,
        };
        d.end()?;
        Ok(row)
    }
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
    // Kind and time, together or not at all.
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

pub(super) fn encode_submission(record: &SubmissionRecord) -> Vec<u8> {
    let mut e = Enc::new(TAG_SUBMISSION);
    e.u64(record.job.get());
    e.0.extend_from_slice(record.fingerprint.as_bytes());
    e.ts(record.recorded_at);
    e.0
}

/// A submission record stored under `key`, which must be a 32-byte digest.
pub(super) fn decode_submission(key: &[u8], bytes: &[u8]) -> DecodeResult<SubmissionRecord> {
    let key = <[u8; 32]>::try_from(key)
        .map_err(|_| StorageError::corrupted("a submission key is not a 32-byte digest"))?;
    let mut d = Dec::new(bytes, TAG_SUBMISSION)?;
    let record = SubmissionRecord {
        key: SubmissionKey::from_digest(key),
        job: JobId::new(d.u64()?),
        fingerprint: RequestFingerprint::from_digest(d.take(32)?.try_into().expect("32 bytes")),
        recorded_at: d.ts()?,
    };
    d.end()?;
    Ok(record)
}

pub(super) fn encode_output(o: &JobOutputRecord) -> Vec<u8> {
    let mut e = Enc::new(TAG_OUTPUT);
    e.u64(o.id.get());
    e.u64(o.job.get());
    e.i64(o.seq);
    e.u8(o.kind.code());
    e.ostr(&o.summary);
    e.ostr(&o.code);
    e.opt(&o.goal_reached, |e, v| e.bool(*v));
    e.ojson(&o.value);
    e.ts(o.created_at);
    e.0
}

pub(super) fn decode_output(bytes: &[u8]) -> DecodeResult<JobOutputRecord> {
    let mut d = Dec::new(bytes, TAG_OUTPUT)?;
    let o = JobOutputRecord {
        id: JobOutputId::new(d.u64()?),
        job: JobId::new(d.u64()?),
        seq: d.i64()?,
        kind: d.code(OutputKind::from_code)?,
        summary: d.ostr()?,
        code: d.ostr()?,
        goal_reached: d.opt(Dec::bool)?,
        value: d.ojson()?,
        created_at: d.ts()?,
    };
    d.end()?;
    Ok(o)
}

#[derive(Clone, Debug)]
pub(crate) struct ExecutorRow {
    pub(crate) id: ExecutorId,
    pub(crate) name: String,
    pub(crate) executor_type: ExecutorType,
    pub(crate) concurrency_limit: u32,
    pub(crate) latest_enabled_at: Option<Timestamp>,
    pub(crate) latest_disabled_at: Option<Timestamp>,
    pub(crate) created_at: Timestamp,
    pub(crate) updated_at: Timestamp,
    pub(crate) llm_transports: BTreeSet<LlmTransport>,
}

impl ExecutorRow {
    pub(super) fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut decoder = Dec::new(bytes, TAG_EXECUTOR)?;
        let row = Self {
            id: ExecutorId::new(decoder.u64()?),
            name: decoder.str()?,
            executor_type: decoder.code(ExecutorType::from_code)?,
            concurrency_limit: decoder.u32()?,
            latest_enabled_at: decoder.ots()?,
            latest_disabled_at: decoder.ots()?,
            created_at: decoder.ts()?,
            updated_at: decoder.ts()?,
            llm_transports: decoder.transports()?,
        };
        decoder.end()?;
        Ok(row)
    }

    pub(super) fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_EXECUTOR);
        e.u64(self.id.get());
        e.str(&self.name);
        e.u8(self.executor_type.code());
        e.u32(self.concurrency_limit);
        e.ots(self.latest_enabled_at);
        e.ots(self.latest_disabled_at);
        e.ts(self.created_at);
        e.ts(self.updated_at);
        e.transports(&self.llm_transports);
        e.0
    }
}

pub(super) fn encode_project(p: &ProjectRecord) -> Vec<u8> {
    let mut e = Enc::new(TAG_PROJECT);
    e.u64(p.id.get());
    e.str(&p.name);
    e.ostr(&p.description);
    e.ts(p.created_at);
    e.0
}

pub(super) fn decode_project(bytes: &[u8]) -> DecodeResult<ProjectRecord> {
    let mut d = Dec::new(bytes, TAG_PROJECT)?;
    let p = ProjectRecord {
        id: ProjectId::new(d.u64()?),
        name: d.str()?,
        description: d.ostr()?,
        created_at: d.ts()?,
    };
    d.end()?;
    Ok(p)
}

pub(super) fn encode_llm_provider(p: &LlmProviderRecord) -> Vec<u8> {
    let mut e = Enc::new(TAG_LLM_PROVIDER);
    e.u64(p.id.get());
    e.str(&p.name);
    e.u8(p.transport.code());
    e.ostr(&p.endpoint);
    e.ostr(&p.credential_ref);
    e.ts(p.created_at);
    e.0
}

pub(super) fn provider_snapshot(provider: LlmProviderRecord) -> ProviderSnapshot {
    ProviderSnapshot {
        id: provider.id,
        name: provider.name,
        transport: provider.transport,
        endpoint: provider.endpoint,
        credential_ref: provider.credential_ref,
    }
}

pub(super) fn decode_llm_provider(bytes: &[u8]) -> DecodeResult<LlmProviderRecord> {
    let mut d = Dec::new(bytes, TAG_LLM_PROVIDER)?;
    let p = LlmProviderRecord {
        id: LlmProviderId::new(d.u64()?),
        name: d.str()?,
        transport: d.code(LlmTransport::from_code)?,
        endpoint: d.ostr()?,
        credential_ref: d.ostr()?,
        created_at: d.ts()?,
    };
    d.end()?;
    Ok(p)
}

/// A Profile's own attributes; its Provider lives in USES_PROVIDER.
#[derive(Clone, Debug)]
pub(crate) struct ProfileRow {
    pub(crate) id: ProfileId,
    pub(crate) name: String,
    pub(crate) config: ProfileConfig,
    pub(crate) created_at: Timestamp,
    pub(crate) updated_at: Timestamp,
}

impl ProfileRow {
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_PROFILE);
        e.u64(self.id.get());
        e.str(&self.name);
        e.profile_config(&self.config);
        e.ts(self.created_at);
        e.ts(self.updated_at);
        e.0
    }

    pub(super) fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_PROFILE)?;
        let row = Self {
            id: ProfileId::new(d.u64()?),
            name: d.str()?,
            config: d.profile_config()?,
            created_at: d.ts()?,
            updated_at: d.ts()?,
        };
        d.end()?;
        Ok(row)
    }
}

/// The references that define a Profile, written with it and replaced only
/// together with its payload.
pub(crate) enum ProfileLinks {
    Agent { llm_targets: Vec<ProfileId> },
    Llm { provider: LlmProviderId },
}

/// An Agent's own attributes; its Profiles live in USES_PROFILE.
#[derive(Clone, Debug)]
pub(crate) struct AgentRow {
    pub(crate) id: AgentId,
    pub(crate) name: String,
    pub(crate) created_at: Timestamp,
}

impl AgentRow {
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_AGENT);
        e.u64(self.id.get());
        e.str(&self.name);
        e.ts(self.created_at);
        e.0
    }

    pub(super) fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_AGENT)?;
        let row = Self {
            id: AgentId::new(d.u64()?),
            name: d.str()?,
            created_at: d.ts()?,
        };
        d.end()?;
        Ok(row)
    }
}

/// A Metrics record's own attributes; its Job lives in MEASURES.
#[derive(Clone, Debug)]
pub(crate) struct MetricsRow {
    pub(crate) id: MetricsId,
    pub(crate) request: RequestNumber,
    pub(crate) observed_at: Timestamp,
    pub(crate) measurements: Measurements,
    pub(crate) token: Option<TokenAnnotation>,
    pub(crate) recorded_at: Timestamp,
}

impl MetricsRow {
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_METRICS);
        e.u64(self.id.get());
        e.u32(self.request.get());
        e.ts(self.observed_at);
        let m = &self.measurements;
        for ms in [
            m.ttfb_ms,
            m.reasoning_ttft_ms,
            m.output_ttft_ms,
            m.request_finished_in_ms,
        ] {
            e.ou64(ms);
        }
        e.usage(&m.usage);
        e.opt(&self.token, |e, token| {
            e.u8(match token.eligibility {
                RequestEligibility::Eligible => 1,
                RequestEligibility::Excluded => 2,
            });
            e.u8(match token.finality {
                UsageFinality::Cumulative => 1,
                UsageFinality::Final => 2,
            });
        });
        e.ts(self.recorded_at);
        e.0
    }

    pub(super) fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_METRICS)?;
        let row = Self {
            id: MetricsId::new(d.u64()?),
            request: d.request()?,
            observed_at: d.ts()?,
            measurements: Measurements {
                ttfb_ms: d.ou64()?,
                reasoning_ttft_ms: d.ou64()?,
                output_ttft_ms: d.ou64()?,
                request_finished_in_ms: d.ou64()?,
                usage: d.usage()?,
            },
            token: d.opt(|d| {
                let eligibility = match d.u8()? {
                    1 => RequestEligibility::Eligible,
                    2 => RequestEligibility::Excluded,
                    other => {
                        return Err(StorageError::corrupted(format!(
                            "unknown Token eligibility code {other}"
                        )));
                    }
                };
                let finality = match d.u8()? {
                    1 => UsageFinality::Cumulative,
                    2 => UsageFinality::Final,
                    other => {
                        return Err(StorageError::corrupted(format!(
                            "unknown Token finality code {other}"
                        )));
                    }
                };
                Ok(TokenAnnotation {
                    eligibility,
                    finality,
                })
            })?,
            recorded_at: d.ts()?,
        };
        d.end()?;
        Ok(row)
    }
}

/// An AvailabilityStat's own attributes; its Job lives in OBSERVES.
#[derive(Clone, Debug)]
pub(crate) struct AvailabilityStatRow {
    pub(crate) id: AvailabilityStatId,
    pub(crate) request: RequestNumber,
    pub(crate) observed_at: Timestamp,
    pub(crate) observation: AvailabilityObservation,
    pub(crate) recorded_at: Timestamp,
}

impl AvailabilityStatRow {
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_AVAILABILITY_STAT);
        e.u64(self.id.get());
        e.u32(self.request.get());
        e.ts(self.observed_at);
        let o = &self.observation;
        e.opt(&o.status_code, |e, v| e.u16(*v));
        e.opt(&o.finish_reason, |e, v| e.u8(v.code()));
        e.ostr(&o.raw_finish_reason);
        e.opt(&o.failure, Enc::provider_failure);
        e.ts(self.recorded_at);
        e.0
    }

    pub(super) fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_AVAILABILITY_STAT)?;
        let row = Self {
            id: AvailabilityStatId::new(d.u64()?),
            request: d.request()?,
            observed_at: d.ts()?,
            observation: AvailabilityObservation {
                status_code: d.opt(Dec::u16)?,
                finish_reason: d.opt(|d| d.code(ChatFinishReason::from_code))?,
                raw_finish_reason: d.ostr()?,
                failure: d.opt(Dec::provider_failure)?,
            },
            recorded_at: d.ts()?,
        };
        d.end()?;
        Ok(row)
    }
}

pub(super) fn encode_marker(m: &OperationMarker) -> Vec<u8> {
    let mut e = Enc::new(TAG_OPERATION);
    e.u8(m.kind.code());
    e.record_ref(m.subject);
    e.opt(&m.created, |e, r| e.record_ref(*r));
    e.ts(m.committed_at);
    e.0
}

/// The record without its call, which is its key.
pub(super) fn encode_tool_evidence(record: &ToolEvidenceRecord) -> Vec<u8> {
    let mut e = Enc::new(TAG_TOOL_EVIDENCE);
    e.str(&record.tool_call_id);
    e.str(&record.name);
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
    let mut d = Dec::new(bytes, TAG_TOOL_EVIDENCE)?;
    let tool_call_id = d.str()?;
    let name = d.str()?;
    let decided_at = d.ts()?;
    let decision = match d.u8()? {
        1 => ToolDecision::Refused(d.code(ToolCallRefusal::from_code)?),
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
        decision: Recorded {
            fact: decision,
            at: decided_at,
        },
        outcome,
        reconciliation,
    })
}

/// Every tool evidence record of `job` in `table`, in call order.
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

pub(super) fn decode_marker(operation: OperationId, bytes: &[u8]) -> DecodeResult<OperationMarker> {
    let mut d = Dec::new(bytes, TAG_OPERATION)?;
    let m = OperationMarker {
        operation,
        kind: d.code(OperationKind::from_code)?,
        subject: d.record_ref()?,
        created: d.opt(Dec::record_ref)?,
        committed_at: d.ts()?,
    };
    d.end()?;
    Ok(m)
}
