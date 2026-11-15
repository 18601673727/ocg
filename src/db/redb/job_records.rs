use super::rows::encode_request_event;
use super::{StorageError, jobs, observations};
use crate::db::entities::{
    AttachmentId, AttachmentRecord, ConfigId, ContentDigest, DependencyCondition,
    FrozenExecutionConfig, JobId, JobLifecycle, JobSpec, JobState, Origin, ProjectId,
    ProjectRecord, RecordRef, RequestNumber, SubmissionKey, SubmissionRecord, Timestamp,
    ToolEvidenceRecord,
};
use crate::db::entities::{
    ConfigBody, ConfigKind, ExecutionConfiguration, OutcomeKind, Submission, Violation,
};
use crate::db::entities::{RequestEventRecord, RequestPayload, ToolCallRef};
use crate::db::redb::batch::{Batch, BatchError, Failure};
use crate::db::redb::codec::Code;
use crate::db::redb::rows::{
    decode_attachment, decode_project, decode_project_root, decode_submission,
    decode_tool_evidence, encode_attachment, encode_project, encode_tool_evidence, tool_call_key,
};
use crate::db::redb::schema::{
    ATTACHMENTS, CHILDREN, CONVERSATIONS, DEPENDENCY_WAITERS, EXECUTORS, JOB_OBSERVATIONS,
    JOB_OUTPUTS, JOB_RUNTIME, JOBS, LIVE_CHILDREN, MESSAGES, META, MUTABLE_READY, P_MAX, PENDING,
    POOL_ALLOCATIONS, POOLS, PROJECTS, REQUEST_EVENTS, REQUEST_TIMELINE, SUBMISSIONS, TOOL_CALLS,
    TURNS,
};
use crate::db::redb::{allocations, indexes, reads, reservations, rows, state};
use ::redb::ReadableTable;
use ::redb::ReadableTableMetadata;

impl Batch {
    pub(crate) fn request_compaction(
        &mut self,
        conversation: JobId,
        model: ConfigId,
        submission: Submission,
    ) -> Result<crate::db::entities::CompactionRequest, BatchError> {
        self.request_compaction_for(conversation, model, submission, None)
    }
    pub(super) fn request_compaction_for(
        &mut self,
        conversation: JobId,
        model: ConfigId,
        submission: Submission,
        automatic: Option<crate::db::entities::ResolvedCompaction>,
    ) -> Result<crate::db::entities::CompactionRequest, BatchError> {
        self.request_preparation(conversation, model, submission, automatic, None)
    }
    fn request_preparation(
        &mut self,
        conversation: JobId,
        model: ConfigId,
        submission: Submission,
        automatic: Option<crate::db::entities::ResolvedCompaction>,
        prediction: Option<JobId>,
    ) -> Result<crate::db::entities::CompactionRequest, BatchError> {
        use crate::db::entities::{
            AllocationPhase, ComposerContextId, JobQueuePriority, MessageBody,
            MutableContextSelection, MutableFrozenConfiguration, MutableJob, MutableJobContent,
            MutableJobInput, MutableJobPhase, NativeSummaryManifest, SummarizationStage,
        };
        macro_rules! refuse {
            ($code:literal, $reason:literal) => {
                if automatic.is_some() {
                    return Ok(crate::db::entities::CompactionRequest::Noop { reason: $reason });
                } else {
                    return self.reject(Violation::InvalidValue($code, $reason));
                }
            };
        }
        self.require_allocation_durability()?;
        if let Some(record) = self.submission(submission.key)? {
            if record.fingerprint != submission.fingerprint {
                return self.reject(Violation::SubmissionKeyUsed {
                    job: record.job,
                    same_request: false,
                });
            }
            let Some(job) = self.mutable_job(record.job)? else {
                return self.corrupted("compaction submission references missing Job".into());
            };
            let Some(source) = job.summary_source else {
                return self.corrupted("compaction key names non-summary work".into());
            };
            if job.parent_id != conversation {
                return self.reject(Violation::SubmissionKeyUsed {
                    job: record.job,
                    same_request: false,
                });
            }
            let manifest: NativeSummaryManifest = self.step(|tx| {
                let messages = tx.open_table(MESSAGES)?;
                let bytes = messages
                    .get(source.as_bytes().as_slice())?
                    .ok_or_else(|| StorageError::corrupted("summary manifest missing"))?;
                serde_json::from_slice(bytes.value())
                    .map_err(|_| StorageError::corrupted("malformed summary manifest"))
            })?;
            let subsequent = self.active_job(conversation)?.is_some();
            return Ok(crate::db::entities::CompactionRequest::Prepared {
                job: record.job,
                conversation,
                through: manifest.through_job,
                subsequent,
                already: true,
            });
        }
        let row = self.require_mutable_conversation(conversation)?;
        if row.is_chunk_group() || row.is_standalone_decision_group() {
            refuse!(
                "compaction_unavailable",
                "this Group does not support conversation compaction"
            );
        }
        if automatic.is_none() {
            let (policy, _) =
                self.step(|tx| reads::catalog::effective_context_policy(&tx.open_table(META)?))?;
            let allowed_by_policy = policy.summarization_model == Some(model);
            let allowed_by_agent = match row.agent {
                Some(agent_id) => match self.require_config_kind(agent_id, ConfigKind::Agent)?.body
                {
                    ConfigBody::Agent(body) => body.models.contains(&model),
                    ConfigBody::Connection(_)
                    | ConfigBody::Model(_)
                    | ConfigBody::DecisionModel(_)
                    | ConfigBody::DecisionAgent(_) => false,
                },
                None => false,
            };
            if !allowed_by_policy && !allowed_by_agent {
                return self.reject(Violation::InvalidValue(
                    "summarizer_not_allowed",
                    "use the compaction setting's Model or one the conversation's Agent lists",
                ));
            }
        }
        let model_row = self.require_config_kind(model, ConfigKind::Model)?;
        let ConfigBody::Model(model_body) = model_row.body else {
            return self.corrupted(format!("{model} has a mismatched body"));
        };
        let Some(window) = model_body.context_window else {
            refuse!(
                "summarizer_window_unknown",
                "the summarizer has no context window, so its source cannot be fitted"
            );
        };
        let reserve = model_body
            .parameters
            .max_completion_tokens
            .or(model_body.parameters.max_tokens)
            .unwrap_or(crate::db::entities::SUMMARIZER_OUTPUT_RESERVE)
            .max(1);
        let Some(budget) = window.checked_sub(reserve) else {
            refuse!(
                "summarizer_window_too_small",
                "the summarizer window cannot hold its reserved summary"
            );
        };
        let (limits, depth) = self.group_limits(conversation)?;
        let root = self.root_group(conversation)?;
        if self.job_lifecycle(conversation)?.closing.is_some()
            || self.require_job_state(conversation)?.outcome.is_some()
            || depth.saturating_add(1) > limits.max_depth
            || self.live_children(conversation)?.len() >= 16
            || self.live_subtree(root)?.len() >= 64
        {
            refuse!(
                "compaction_unavailable",
                "conversation cannot admit a compaction child now"
            );
        }
        let active = self.active_job(conversation)?;
        let tip = self.group_head(conversation)?;
        let Some(mut through) = tip else {
            return Ok(crate::db::entities::CompactionRequest::Noop {
                reason: "no settled source",
            });
        };
        loop {
            let Some(candidate) = self.mutable_job(through)? else {
                return Ok(crate::db::entities::CompactionRequest::Noop {
                    reason: "no settled source",
                });
            };
            let state = self.require_job_state(through)?;
            if candidate.summary_source.is_none()
                && state
                    .outcome
                    .as_ref()
                    .is_some_and(|o| o.kind == OutcomeKind::Completed)
            {
                break;
            }
            let Some(prev) = candidate.previous else {
                return Ok(crate::db::entities::CompactionRequest::Noop {
                    reason: "no settled source",
                });
            };
            through = prev;
        }
        if let Some(recipe) = &automatic {
            for _ in 0..recipe.preserve_recent_turns {
                let Some(prev) = self.mutable_job(through)?.and_then(|job| job.previous) else {
                    return Ok(crate::db::entities::CompactionRequest::Noop {
                        reason: "recent turns are protected",
                    });
                };
                through = prev;
            }
        }
        let sealed = self.step(|tx| {
            let tables = reads::execution_context::NativeSummaryTables {
                jobs: tx.open_table(JOBS)?,
                states: tx.open_table(JOB_RUNTIME)?,
                allocations: tx.open_table(POOL_ALLOCATIONS)?,
                outputs: tx.open_table(JOB_OUTPUTS)?,
                attachments: tx.open_table(ATTACHMENTS)?,
                tools: tx.open_table(TOOL_CALLS)?,
                pools: tx.open_table(POOLS)?,
                executors: tx.open_table(EXECUTORS)?,
            };
            let mut summary_budget = reads::execution_context::SummaryBudget {
                bytes: 0,
                outputs: 0,
            };
            let mut boundaries = Vec::new();
            let mut cursor = Some(through);
            while let Some(id) = cursor {
                let (boundary, prev) = match reads::execution_context::native_source_boundary(
                    &tables,
                    id,
                    conversation,
                    &mut summary_budget,
                ) {
                    Ok(pair) => pair,
                    Err(_) => return Ok(None),
                };
                boundaries.push(boundary);
                cursor = prev;
            }
            if boundaries.is_empty() {
                return Ok(None);
            }
            boundaries.reverse();
            let mut text = String::new();
            let mut context_tokens = 0usize;
            for boundary in &boundaries {
                let Some(bytes) = tables.jobs.get(boundary.job.get())? else {
                    return Ok(None);
                };
                let job = jobs::decode_mutable_job(boundary.job.get(), bytes.value())?;
                let mut turn_text = format!("User: {}\n", job.input.text());
                for entry in tables
                    .outputs
                    .range((boundary.job.get(), 0)..=(boundary.job.get(), u32::MAX))?
                {
                    let (key, val) = entry?;
                    let out = crate::db::redb::rows::decode_output(key.value(), val.value())?;
                    if let Some(t) = out.display_text() {
                        turn_text.push_str("Assistant: ");
                        turn_text.push_str(&t);
                        turn_text.push('\n');
                    }
                }
                if text.len().saturating_add(turn_text.len()) > 7 * 256 * 1024 {
                    return Ok(None);
                }
                context_tokens =
                    context_tokens.saturating_add(turn_text.len().saturating_add(3) / 4);
                text.push_str(&turn_text);
            }
            Ok(Some((
                NativeSummaryManifest {
                    conversation,
                    through_job: through,
                    sources: boundaries,
                },
                text,
                context_tokens,
            )))
        })?;
        let Some((manifest, source_text, source_context_tokens)) = sealed else {
            return Ok(crate::db::entities::CompactionRequest::Noop {
                reason: "no eligible settled source",
            });
        };
        let manifest_body = MessageBody::new(&serde_json::to_value(&manifest).map_err(|_| {
            BatchError(Failure::Storage(StorageError::corrupted(
                "summary manifest serialization failed",
            )))
        })?)
        .map_err(|e| BatchError(Failure::Storage(StorageError::corrupted(e.to_string()))))?;
        let source_digest = manifest_body.digest();
        let existing = self.step(|tx| {
            let children = tx.open_table(CHILDREN)?;
            let jobs = tx.open_table(JOBS)?;
            let states = tx.open_table(JOB_RUNTIME)?;
            let allocations = tx.open_table(POOL_ALLOCATIONS)?;
            for entry in children
                .range((conversation.get(), 0)..=(conversation.get(), u128::MAX))?
                .rev()
            {
                let child_id = JobId::new(entry?.0.value().1);
                let Some(bytes) = jobs.get(child_id.get())? else {
                    continue;
                };
                let job = jobs::decode_mutable_job(child_id.get(), bytes.value())?;
                if job.summary_source != Some(source_digest) {
                    continue;
                }
                let state = states
                    .get(child_id.get())?
                    .map(|b| state::decode_state(child_id.get(), b.value()))
                    .transpose()?;
                let Some(state) = state else {
                    continue;
                };
                match state.outcome.as_ref().map(|o| o.kind) {
                    None => return Ok(Some(child_id)),
                    Some(OutcomeKind::Completed) => {
                        let released = allocations
                            .get(child_id.get())?
                            .map(|b| allocations::decode_pool_allocation(child_id.get(), b.value()))
                            .transpose()?
                            .is_some_and(|a| a.phase == AllocationPhase::Released);
                        if released {
                            return Ok(Some(child_id));
                        }
                    }
                    Some(OutcomeKind::Failed | OutcomeKind::Cancelled | OutcomeKind::Unknown) => {}
                }
            }
            Ok(None)
        })?;
        if let Some(existing_job) = existing {
            return Ok(crate::db::entities::CompactionRequest::Prepared {
                job: existing_job,
                conversation,
                through,
                subsequent: active.is_some(),
                already: true,
            });
        }
        let source_tokens = source_text.len().saturating_add(768).div_ceil(3);
        if automatic.is_some_and(|recipe| source_context_tokens < recipe.minimum_source as usize) {
            return Ok(crate::db::entities::CompactionRequest::Noop {
                reason: "source is below the configured minimum",
            });
        }
        let chunk_capacity = (budget as usize)
            .saturating_sub(512)
            .saturating_mul(3)
            .min(256 * 1024);
        let stages = if source_tokens <= budget as usize {
            vec![SummarizationStage::Direct]
        } else {
            let Some(stages) = crate::db::entities::summary_chunks(&source_text, chunk_capacity)
            else {
                refuse!(
                    "source_exceeds_summarizer",
                    "source cannot fit a bounded compaction tree; no source is truncated"
                );
            };
            stages
        };
        let chunked = !matches!(stages.first(), Some(SummarizationStage::Direct));
        let work_count = stages.len() + usize::from(chunked);
        if self
            .live_children(conversation)?
            .len()
            .saturating_add(work_count)
            > 16
            || self.live_subtree(root)?.len().saturating_add(work_count) > 64
            || self
                .step(|tx| Ok(tx.open_table(PENDING)?.len()?))?
                .saturating_add(work_count as u64)
                > P_MAX
        {
            refuse!(
                "compaction_unavailable",
                "conversation cannot admit this compaction tree now"
            );
        }
        let configuration = ExecutionConfiguration {
            reasoning_effort: None,
            agent_profile: None,
            llm_profile: model.into(),
        };
        let frozen = match self.freeze_system_configuration(configuration)? {
            crate::db::entities::FrozenConfiguration::Generative(frozen) => frozen,
            crate::db::entities::FrozenConfiguration::Decision(_) => {
                return self.corrupted("summarizer is not generative".into());
            }
        };
        if let Some(reason) = frozen.provider_refusal() {
            return self.reject(Violation::InvalidValue("configuration", reason));
        }
        let mut target = reserve;
        if let Some(recipe) = automatic {
            if source_context_tokens <= recipe.minimum_savings as usize {
                return Ok(crate::db::entities::CompactionRequest::Noop {
                    reason: "protected context leaves no useful summary budget",
                });
            }
            target = target.min(recipe.target);
        }
        let priority = if automatic.is_some() || prediction.is_some() {
            JobQueuePriority::Automatic
        } else {
            JobQueuePriority::Foreground
        };
        self.put_message(&manifest_body)?;
        let producer_id = JobId::new(self.next_id()?);
        let mut dependency_ids = Vec::new();
        if chunked {
            let chunk_group = JobId::new(self.next_id()?);
            let group_row = JobSpec {
                id: chunk_group,
                created_at: self.now,
                parent_id: Some(conversation),
                agent: None,
                project: None,
                origin: Origin::System,
                keep_open: false,
            };
            self.insert(
                CONVERSATIONS,
                chunk_group.get(),
                group_row.encode().as_slice(),
            )?;
            self.put_job_state(chunk_group, &JobState::new(chunk_group))?;
            self.put_job_lifecycle(chunk_group, &JobLifecycle::default())?;
            self.step(|tx| state::set_pending(tx, chunk_group, Some((0, 0))))?;
            self.insert(CHILDREN, (conversation.get(), chunk_group.get()), ())?;
            self.link_job_group(conversation, chunk_group)?;
            self.adjust_open_children(conversation, 1)?;
            for (idx, stage) in stages.iter().enumerate() {
                if let SummarizationStage::Chunk { start, end, .. } = *stage {
                    let chunk_slice = &source_text[start as usize..end as usize];
                    let chunk_msg = MessageBody::new(&serde_json::json!({
                        "source": source_digest,
                        "chunk_index": idx,
                        "text": chunk_slice,
                    }))
                    .map_err(|e| {
                        BatchError(Failure::Storage(StorageError::corrupted(e.to_string())))
                    })?;
                    self.put_message(&chunk_msg)?;
                    let chunk_id = JobId::new(self.next_id()?);
                    let chunk_prompt = format!(
                        "Summarize chunk {} (digest {}) for source {} into a JSON object matching {{\"summary\":\"...\",\"omitted_details\":[...]}}. Target <= {} tokens.\n\n{}",
                        idx + 1,
                        chunk_msg.digest(),
                        source_digest,
                        target,
                        chunk_slice
                    );
                    let chunk_job = MutableJob {
                        id: chunk_id,
                        parent_id: chunk_group,
                        previous: None,
                        retry: None,
                        composer_context: ComposerContextId::new(self.next_id()?),
                        created_at: self.now,
                        submitted_at: Some(self.now),
                        revision: 1,
                        phase: MutableJobPhase::Queued,
                        input: MutableJobInput {
                            priority,
                            content: MutableJobContent::Generative {
                                text: chunk_prompt,
                                attachments: Vec::new(),
                                configuration: Some(configuration),
                            },
                        },
                        context_selection: MutableContextSelection::FullHistory,
                        summary_source: None,
                        frozen: Some(MutableFrozenConfiguration::Generative(frozen.clone())),
                    };
                    self.insert_native_summary_job(&chunk_job, &frozen, 0, true)?;
                    self.insert(
                        DEPENDENCY_WAITERS,
                        (chunk_id.get(), producer_id.get()),
                        DependencyCondition::Success.code(),
                    )?;
                    dependency_ids.push(chunk_id);
                }
            }
        }
        let producer_prompt = if chunked {
            format!(
                "Merge {} chunk summaries for source {} into a single JSON object matching {{\"source\":\"{}\",\"summary\":\"...\",\"omitted_details\":[...]}}. Target <= {} tokens.",
                dependency_ids.len(),
                source_digest,
                source_digest,
                target
            )
        } else {
            format!(
                "Summarize conversation source {} into a single JSON object matching {{\"source\":\"{}\",\"summary\":\"...\",\"omitted_details\":[...]}}. Target <= {} tokens. Output ONLY valid JSON.",
                source_digest, source_digest, target
            )
        };
        let producer_job = MutableJob {
            id: producer_id,
            parent_id: conversation,
            previous: None,
            retry: None,
            composer_context: ComposerContextId::new(self.next_id()?),
            created_at: self.now,
            submitted_at: Some(self.now),
            revision: 1,
            phase: MutableJobPhase::Queued,
            input: MutableJobInput {
                priority,
                content: MutableJobContent::Generative {
                    text: producer_prompt,
                    attachments: Vec::new(),
                    configuration: Some(configuration),
                },
            },
            context_selection: MutableContextSelection::FullHistory,
            summary_source: Some(source_digest),
            frozen: Some(MutableFrozenConfiguration::Generative(frozen.clone())),
        };
        self.insert_native_summary_job(
            &producer_job,
            &frozen,
            dependency_ids.len() as u32,
            dependency_ids.is_empty(),
        )?;
        for prerequisite in &dependency_ids {
            self.record_execution_event(
                producer_id,
                crate::db::entities::ExecutionEventKind::DependencyAdded {
                    prerequisite: *prerequisite,
                    condition: DependencyCondition::Success,
                },
            )?;
        }
        self.record_mutable_submission(&SubmissionRecord {
            key: submission.key,
            fingerprint: submission.fingerprint,
            job: producer_id,
            seq: 1,
            recorded_at: self.now,
        })?;
        Ok(crate::db::entities::CompactionRequest::Prepared {
            job: producer_id,
            conversation,
            through,
            subsequent: active.is_some(),
            already: false,
        })
    }

    fn insert_native_summary_job(
        &mut self,
        job: &crate::db::entities::MutableJob,
        frozen: &FrozenExecutionConfig,
        unsettled_deps: u32,
        ready_now: bool,
    ) -> Result<(), BatchError> {
        let submitted = job.submitted_at.unwrap_or(job.created_at);
        let frozen_config = crate::db::entities::FrozenConfiguration::Generative(frozen.clone());
        self.store_mutable_job(job)?;
        self.put_job_state(job.id, &JobState::new(job.id))?;
        self.step(|tx| {
            if ready_now {
                tx.open_table(MUTABLE_READY)?.insert(
                    (
                        jobs::mutable_priority(job.input.priority),
                        submitted.as_unix_micros(),
                        job.id.get(),
                        reservations::mutable_configuration_requirement(&frozen_config),
                    ),
                    (),
                )?;
            }
            tx.open_table(PENDING)?.insert(job.id.get(), ())?;
            // Summary producers and parallel Chunks are children outside the Turn sequence.
            tx.open_table(CHILDREN)?
                .insert((job.parent_id.get(), job.id.get()), ())?;
            state::set_pending(tx, job.id, Some((unsettled_deps, 0)))?;
            Ok(())
        })?;
        self.adjust_open_children(job.parent_id, 1)?;
        Ok(())
    }

    pub(crate) fn group_head(&mut self, conversation: JobId) -> Result<Option<JobId>, BatchError> {
        self.step(|tx| reads::jobs::turn_head(&tx.open_table(TURNS)?, conversation))
    }
    pub(crate) fn active_job(&mut self, conversation: JobId) -> Result<Option<JobId>, BatchError> {
        self.step(|tx| Ok(reads::jobs::active_job_in(tx, conversation)?.map(|job| job.id)))
    }

    pub(crate) fn live_children(&mut self, conversation: JobId) -> Result<Vec<JobId>, BatchError> {
        self.step(|tx| {
            let table = tx.open_table(LIVE_CHILDREN)?;
            table
                .range((conversation.get(), 0)..=(conversation.get(), u128::MAX))?
                .map(|entry| Ok(JobId::new(entry?.0.value().1)))
                .collect()
        })
    }
}

impl Batch {
    pub(crate) fn submission(
        &mut self,
        key: SubmissionKey,
    ) -> Result<Option<SubmissionRecord>, BatchError> {
        self.get(SUBMISSIONS, key.as_bytes().as_slice(), |bytes| {
            decode_submission(key.as_bytes(), bytes)
        })
    }

    pub(crate) fn require_attachment(
        &mut self,
        id: AttachmentId,
    ) -> Result<AttachmentRecord, BatchError> {
        let record = self.get(ATTACHMENTS, id.get(), |bytes| {
            decode_attachment(id.get(), bytes)
        })?;
        match record {
            Some(record) => Ok(record),
            None => self.reject(Violation::NotFound(RecordRef::Attachment(id))),
        }
    }

    pub(crate) fn insert_attachment(
        &mut self,
        record: &AttachmentRecord,
    ) -> Result<(), BatchError> {
        self.step(|tx| {
            let previous = tx
                .open_table(ATTACHMENTS)?
                .get(record.id.get())?
                .map(|bytes| decode_attachment(record.id.get(), bytes.value()))
                .transpose()?;
            indexes::content_reference(tx, record, previous.as_ref())?;
            tx.open_table(ATTACHMENTS)?
                .insert(record.id.get(), encode_attachment(record).as_slice())?;
            Ok(())
        })
    }

    pub(crate) fn require_project(&mut self, id: ProjectId) -> Result<(), BatchError> {
        let exists = self.step(|tx| Ok(tx.open_table(PROJECTS)?.get(id.get())?.is_some()))?;
        if exists {
            Ok(())
        } else {
            self.reject(Violation::NotFound(RecordRef::Project(id)))
        }
    }

    pub(crate) fn insert_project(&mut self, record: &ProjectRecord) -> Result<(), BatchError> {
        self.catalog_changed = true;
        self.insert(PROJECTS, record.id.get(), encode_project(record).as_slice())
    }

    pub(crate) fn require_project_record(
        &mut self,
        id: ProjectId,
    ) -> Result<ProjectRecord, BatchError> {
        let record = self.get(PROJECTS, id.get(), |bytes| decode_project(id.get(), bytes))?;
        match record {
            Some(record) => Ok(record),
            None => self.reject(Violation::NotFound(RecordRef::Project(id))),
        }
    }

    pub(crate) fn project_roots(&mut self) -> Result<Vec<(ProjectId, String)>, BatchError> {
        self.step(|tx| {
            let mut roots = Vec::new();
            for entry in tx.open_table(PROJECTS)?.iter()? {
                let (key, bytes) = entry?;
                roots.push(decode_project_root(key.value(), bytes.value())?);
            }
            Ok(roots)
        })
    }
}

impl Batch {
    pub(crate) fn tool_evidence(
        &mut self,
        call: ToolCallRef,
    ) -> Result<Option<ToolEvidenceRecord>, BatchError> {
        self.get(TOOL_CALLS, tool_call_key(call), |bytes| {
            decode_tool_evidence(call, bytes)
        })
    }

    pub(crate) fn put_tool_evidence(
        &mut self,
        record: &ToolEvidenceRecord,
    ) -> Result<(), BatchError> {
        self.catalog_changed = true;
        self.insert(
            TOOL_CALLS,
            tool_call_key(record.call),
            encode_tool_evidence(record).as_slice(),
        )
    }

    pub(crate) fn put_message(
        &mut self,
        body: &crate::db::entities::MessageBody,
    ) -> Result<(), BatchError> {
        self.step(|tx| {
            let mut table = tx.open_table(MESSAGES)?;
            {
                if let Some(existing) = table.get(body.digest.as_bytes().as_slice())? {
                    if existing.value() != body.bytes.as_slice() {
                        return Err(StorageError::corrupted("message digest collision"));
                    }
                    return Ok(());
                }
            }
            table.insert(body.digest.as_bytes().as_slice(), body.bytes.as_slice())?;
            Ok(())
        })
    }

    pub(crate) fn require_messages(
        &mut self,
        digests: impl Iterator<Item = ContentDigest>,
    ) -> Result<(), BatchError> {
        let missing = self.step(|tx| {
            let table = tx.open_table(MESSAGES)?;
            for digest in digests {
                if table.get(digest.as_bytes().as_slice())?.is_none() {
                    return Ok(true);
                }
            }
            Ok(false)
        })?;
        if missing {
            return self.reject(Violation::InvalidValue(
                "messages",
                "request references a missing message",
            ));
        }
        Ok(())
    }

    pub(crate) fn request_observed(
        &mut self,
        job: JobId,
        request: RequestNumber,
    ) -> Result<bool, BatchError> {
        self.step(|tx| {
            let events = tx.open_table(REQUEST_EVENTS)?;
            let observed = events
                .range((job.get(), request.get(), 0)..=(job.get(), request.get(), u32::MAX))?
                .next()
                .is_some();
            Ok(observed)
        })
    }

    pub(crate) fn insert_request_event(
        &mut self,
        job: JobId,
        request: RequestNumber,
        observed_at: Timestamp,
        payload: RequestPayload,
    ) -> Result<RequestEventRecord, BatchError> {
        let identity = self.development_usage_identity(job)?;
        let seq = self.step(|tx| {
            let events = tx.open_table(REQUEST_EVENTS)?;
            let last = events
                .range((job.get(), request.get(), 0)..=(job.get(), request.get(), u32::MAX))?
                .next_back()
                .transpose()?
                .map_or(0, |(key, _)| key.value().2);
            Ok(last)
        })?;
        let Some(seq) = seq.checked_add(1) else {
            return self.reject(Violation::ValueOutOfRange("seq"));
        };
        let event = RequestEventRecord {
            id: crate::db::entities::RequestEventId::new(self.next_id()?),
            job_id: job,
            request,
            seq,
            observed_at,
            recorded_at: self.now,
            payload,
        };
        let timeline = indexes::request_timeline_key(Some(&identity), &event);
        let usage_model = indexes::usage_model_key(Some(&identity), &event);
        self.step(|tx| {
            tx.open_table(REQUEST_EVENTS)?.insert(
                (job.get(), request.get(), seq),
                encode_request_event(&event).as_slice(),
            )?;
            if let Some(kind) =
                observations::observation_kind(&tx.open_table(REQUEST_EVENTS)?, &event)?
            {
                tx.open_table(JOB_OBSERVATIONS)?
                    .insert((job.get(), kind, event.id.get()), (request.get(), seq))?;
            }
            if let Some(key) = timeline {
                tx.open_table(REQUEST_TIMELINE)?.insert(key, ())?;
            }
            if let Some(key) = usage_model {
                indexes::record_usage_model(tx, key, job)?;
            }
            Ok(())
        })?;
        self.record_request_reference(job, request, seq)?;
        Ok(event)
    }
}

impl Batch {
    pub(crate) fn previous_tool_evidence(
        &mut self,
        call: ToolCallRef,
    ) -> Result<Vec<ToolEvidenceRecord>, BatchError> {
        self.step(|tx| {
            tx.open_table(TOOL_CALLS)?
                .range((call.job.get(), 0, 0)..(call.job.get(), call.request.get(), call.index))?
                .map(|entry| {
                    let (key, bytes) = entry?;
                    rows::decode_tool_evidence(rows::tool_call_ref(key.value())?, bytes.value())
                })
                .collect()
        })
    }
}
