use super::*;
use crate::db::entities::{
    ConfigBody, ConfigKind, ExecutionConfiguration, OutcomeCause, OutcomeKind, OutcomeRecord,
    Submission, Violation,
};
use ::redb::ReadableTableMetadata;

impl Batch {
    pub(crate) fn job_execution(
        &mut self,
        turn: &JobSpec,
    ) -> Result<(FrozenExecutionConfig, Option<ProjectId>, Option<ConfigId>), BatchError> {
        self.step(|tx| reader::job_execution(&tx.open_table(JOBS)?, turn))
    }
    /// Requests one manual compaction of `conversation`. The same submission
    /// key returns the Job it already created. A source that cannot fit the
    /// summarizer's own window is refused; it is never truncated. A decision
    /// model is not a summarizer.
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
        macro_rules! refuse {
            ($code:literal, $reason:literal) => {
                if automatic.is_some() {
                    return Ok(crate::db::entities::CompactionRequest::Noop { reason: $reason });
                } else {
                    return self.reject(Violation::InvalidValue($code, $reason));
                }
            };
        }
        if let Some(record) = self.submission(submission.key)? {
            if record.fingerprint != submission.fingerprint {
                return self.reject(Violation::SubmissionKeyUsed {
                    job: record.job,
                    same_request: false,
                });
            }
            let turn = self.require_job(record.job)?;
            let Some(child) = turn.parent_id else {
                return self.corrupted("compaction submission has no conversation".into());
            };
            let owner = self.require_job(child)?;
            if owner.parent_id != Some(conversation) {
                return self.reject(Violation::SubmissionKeyUsed {
                    job: record.job,
                    same_request: false,
                });
            }
            let subsequent = self.active_job(conversation)?.is_some();
            let through = match &turn.kind {
                JobKind::Execution {
                    execution: JobExecution::Summarization(work),
                    ..
                } => {
                    self.step(|tx| {
                        reader::load_context_manifest(&tx.open_table(MESSAGES)?, work.source)
                    })?
                    .through_turn
                }
                JobKind::Execution { .. } | JobKind::Group { .. } => {
                    return self.corrupted("compaction key names incompatible work".into());
                }
            };
            return Ok(crate::db::entities::CompactionRequest::Prepared {
                job: record.job,
                conversation: child,
                through,
                subsequent,
                already: true,
            });
        }
        let row = self.require_job(conversation)?;
        if !row.kind.is_group() {
            return self.reject(Violation::InvalidValue(
                "conversation_not_found",
                "expected a conversation",
            ));
        }
        if automatic.is_none() {
            let (policy, _, _) =
                self.step(|tx| reader::effective_context_policy(&tx.open_table(META)?))?;
            let allowed_by_policy = policy.summarization_model == Some(model);
            let allowed_by_agent = match row.kind {
                JobKind::Group {
                    agent: Some(agent_id),
                    ..
                } => match self.require_config_kind(agent_id, ConfigKind::Agent)?.body {
                    ConfigBody::Agent(body) => body.models.contains(&model),
                    ConfigBody::Connection(_)
                    | ConfigBody::Model(_)
                    | ConfigBody::Pool(_)
                    | ConfigBody::DecisionModel(_)
                    | ConfigBody::DecisionAgent(_) => false,
                },
                JobKind::Group { .. } | JobKind::Execution { .. } => false,
            };
            if !allowed_by_policy && !allowed_by_agent {
                return self.reject(Violation::InvalidValue(
                    "summarizer_not_allowed",
                    "use the compaction setting's Model or one the conversation's Agent lists",
                ));
            }
        }
        let model_row = self.require_config_kind(model, ConfigKind::Model)?;
        let crate::db::entities::ConfigBody::Model(model_body) = model_row.body else {
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
        let Some(mut through) = self.compaction_boundary(conversation)? else {
            return Ok(crate::db::entities::CompactionRequest::Noop {
                reason: "no settled source",
            });
        };
        if let Some(recipe) = &automatic {
            for _ in 0..recipe.preserve_recent_turns {
                let Some(previous) = self.require_job(through)?.kind.previous() else {
                    return Ok(crate::db::entities::CompactionRequest::Noop {
                        reason: "recent turns are protected",
                    });
                };
                through = previous;
            }
        }
        let (limits, depth) = self.group_limits(conversation)?;
        let root = self.root_group(conversation)?;
        if self.job_control(conversation)?.closing.is_some()
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
        let Some(source) = self.seal_context_source(conversation, through)? else {
            return Ok(crate::db::entities::CompactionRequest::Noop {
                reason: "no settled source",
            });
        };
        let rendered = self.step(|tx| {
            let mut text = String::new();
            let mut context_tokens = 0usize;
            for boundary in &source.turns {
                let rendered = reader::source_turn(
                    &tx.open_table(JOBS)?,
                    &tx.open_table(JOB_STATES)?,
                    &tx.open_table(JOB_INPUTS)?,
                    &tx.open_table(JOB_OUTPUTS)?,
                    &tx.open_table(TOOL_CALLS)?,
                    &tx.open_table(APPROVALS)?,
                    boundary.turn,
                    Some((boundary.input_through, boundary.output_through)),
                )?
                .ok_or_else(|| StorageError::corrupted("sealed source changed"))?;
                if text.len().saturating_add(rendered.text.len()) > 7 * 256 * 1024 {
                    return Ok(None);
                }
                text.push_str(&rendered.text);
                context_tokens = context_tokens.saturating_add(rendered.context_tokens);
            }
            Ok(Some((text, context_tokens)))
        })?;
        let Some((source_text, source_context_tokens)) = rendered else {
            refuse!(
                "source_exceeds_summarizer",
                "source exceeds the bounded compaction-tree size; it is not truncated"
            );
        };
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
            vec![crate::db::entities::SummarizationStage::Direct]
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
        let chunked = !matches!(
            stages.first(),
            Some(crate::db::entities::SummarizationStage::Direct)
        );
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
        let manifest = crate::db::entities::MessageBody::new(
            &serde_json::to_value(&source).map_err(|_| {
                BatchError(Failure::Storage(StorageError::corrupted(
                    "source serialization failed",
                )))
            })?,
        )
        .map_err(|error| {
            BatchError(Failure::Storage(StorageError::corrupted(error.to_string())))
        })?;
        let (_, revision, profile) =
            self.step(|tx| reader::effective_context_policy(&tx.open_table(META)?))?;
        let mut target = reserve;
        if let Some(recipe) = automatic {
            let full_usage = match self.group_head(conversation)? {
                Some(tip) => {
                    self.step(|tx| {
                        reader::raw_context_usage(
                            &tx.open_table(JOBS)?,
                            &tx.open_table(JOB_STATES)?,
                            &tx.open_table(JOB_INPUTS)?,
                            &tx.open_table(JOB_OUTPUTS)?,
                            tip,
                        )
                    })?
                    .0
                }
                None => 0,
            };
            let reducible = self.step(|tx| reader::source_reducible(tx, &source))?;
            let room =
                (recipe.target as usize).saturating_sub(full_usage.saturating_sub(reducible));
            if room == 0 || reducible <= recipe.minimum_savings as usize {
                return Ok(crate::db::entities::CompactionRequest::Noop {
                    reason: "protected context leaves no useful summary budget",
                });
            }
            target = target.min(u32::try_from(room).unwrap_or(u32::MAX));
        }
        let part_target = if chunked {
            reserve.min(budget.saturating_sub(512) / (stages.len() as u32 + 1))
        } else {
            target
        };
        if chunked && part_target < 128 {
            refuse!(
                "source_exceeds_summarizer",
                "summarizer cannot fit both chunk summaries and the final reduction"
            );
        }
        let reserved_requests = if prediction.is_some() {
            work_count as u32
        } else {
            0
        };
        let reserved_tokens = if prediction.is_some() {
            let bound = source_text
                .len()
                .saturating_add(work_count.saturating_mul(2048))
                .saturating_add(if chunked {
                    (part_target as usize)
                        .saturating_mul(4)
                        .saturating_mul(stages.len())
                } else {
                    0
                })
                .saturating_add(target as usize);
            u32::try_from(bound).unwrap_or(u32::MAX)
        } else {
            0
        };
        if let Some(prediction) = prediction
            && !self.predictive_capacity(
                conversation,
                prediction,
                reserved_tokens,
                reserved_requests,
            )?
        {
            return Ok(crate::db::entities::CompactionRequest::Noop {
                reason: "predictive preparation queue or hourly request/token budget unavailable",
            });
        }
        self.put_message(&manifest)?;
        let mut anchor = None;
        let mut dependencies = Vec::new();
        let mut prepared = None;
        for stage in stages {
            let final_work = !chunked;
            let work = crate::db::entities::SummarizationWork {
                frozen: frozen.clone(),
                source: manifest.digest(),
                target_tokens: if final_work { target } else { part_target },
                minimum_savings_tokens: if final_work {
                    automatic.map_or(1, |recipe| recipe.minimum_savings)
                } else {
                    0
                },
                policy_revision: if anchor.is_none() {
                    automatic.map(|_| revision)
                } else {
                    None
                },
                policy_profile: if anchor.is_none() {
                    automatic.and(profile)
                } else {
                    None
                },
                stage,
                chain: anchor,
                prediction: prediction.filter(|_| anchor.is_none()),
                reserved_tokens: if anchor.is_none() { reserved_tokens } else { 0 },
                reserved_requests: if anchor.is_none() {
                    reserved_requests
                } else {
                    0
                },
            };
            let (child, appended) = self.open_summary_work(
                conversation,
                work,
                if final_work { Some(submission) } else { None },
                Vec::new(),
            )?;
            anchor.get_or_insert(appended.turn);
            dependencies.push((child, DependencyCondition::Success));
            prepared = Some((child, appended.turn));
        }
        if chunked {
            let work = crate::db::entities::SummarizationWork {
                frozen,
                source: manifest.digest(),
                target_tokens: target,
                minimum_savings_tokens: automatic.map_or(1, |recipe| recipe.minimum_savings),
                policy_revision: None,
                policy_profile: None,
                stage: crate::db::entities::SummarizationStage::Reduce,
                chain: anchor,
                prediction: None,
                reserved_tokens: 0,
                reserved_requests: 0,
            };
            let (child, appended) =
                self.open_summary_work(conversation, work, Some(submission), dependencies)?;
            prepared = Some((child, appended.turn));
        }
        let Some((child, job)) = prepared else {
            return self.corrupted("empty summary plan".into());
        };
        Ok(crate::db::entities::CompactionRequest::Prepared {
            job,
            conversation: child,
            through,
            subsequent: active.is_some(),
            already: false,
        })
    }

    fn open_summary_work(
        &mut self,
        conversation: JobId,
        mut work: crate::db::entities::SummarizationWork,
        submission: Option<Submission>,
        dependencies: Vec<(JobId, DependencyCondition)>,
    ) -> Result<(JobId, Appended), BatchError> {
        if work
            .frozen
            .llm
            .config
            .parameters
            .max_completion_tokens
            .is_some()
        {
            work.frozen.llm.config.parameters.max_completion_tokens = Some(work.target_tokens);
        } else {
            work.frozen.llm.config.parameters.max_tokens = Some(work.target_tokens);
        }
        let format = if work.frozen.llm.provider.transport == LlmTransport::AnthropicMessages {
            ResponseFormat::JsonSchema {
                name: "context_summary_v1".into(),
                schema: serde_json::json!({
                    "type":"object", "properties": {"summary":{"type":"string"}, "omitted_details":{"type":"array","items":{"type":"string"}}},
                    "required":["summary","omitted_details"], "additionalProperties":false
                }),
                strict: Some(true),
            }
        } else {
            ResponseFormat::JsonObject
        };
        let spec = JobGroup {
            name: Some(crate::db::entities::COMPACTION_JOB_NAME.into()),
            agent: None,
            project: None,
            origin: Origin::System,
            keep_open: false,
        };
        self.validate_group_parent(Some(conversation))?;
        if let Some(existing) = self.existing_group_submission(submission.as_ref())? {
            return Ok(existing);
        }
        let system = self.insert_group(Some(conversation), &spec)?;
        let appended = self.append_system_turn(
            system,
            Entry::UserMessage {
                text:
                    "Prepare a typed context summary from the sealed source supplied at execution."
                        .into(),
                attachments: Vec::new(),
            },
            JobExecution::Summarization(work),
            submission,
            dependencies,
            spec.name,
            Some(format),
        )?;
        Ok((system, appended))
    }

    /// The newest settled turn a compaction may cover. An active turn is not
    /// included; its predecessor is the boundary for a later execution.
    fn compaction_boundary(&mut self, conversation: JobId) -> Result<Option<JobId>, BatchError> {
        let mut cursor = self.group_head(conversation)?;
        let mut seen = BTreeSet::new();
        while let Some(turn) = cursor {
            if !seen.insert(turn) {
                return self.corrupted("context source cycle".into());
            }
            if self.require_job_state(turn)?.locally_stopped() {
                return Ok(Some(turn));
            }
            cursor = self.require_job(turn)?.kind.previous();
        }
        Ok(None)
    }

    pub(crate) fn group_head(&mut self, conversation: JobId) -> Result<Option<JobId>, BatchError> {
        self.step(|tx| reader::turn_head(&tx.open_table(TURNS)?, conversation))
    }
    pub(crate) fn active_job(&mut self, conversation: JobId) -> Result<Option<JobId>, BatchError> {
        self.step(|tx| {
            Ok(tx
                .open_table(ACTIVE_JOBS)?
                .get(conversation.get())?
                .map(|turn| JobId::new(turn.value())))
        })
    }
    pub(crate) fn place_waiting_job(&mut self, conversation: JobId) -> Result<(), BatchError> {
        if let Some(tip) = self.group_head(conversation)? {
            let state = self.require_job_state(tip)?;
            if state.outcome.is_none()
                && self.step(|tx| {
                    Ok(tx
                        .open_table(UNSETTLED)?
                        .get(tip.get())?
                        .is_some_and(|counts| counts.value().0 == 0))
                })?
            {
                self.place(tip)?;
            }
        }
        Ok(())
    }

    /// Creates a system conversation and its one typed decision turn. The
    /// configuration is frozen in this transaction, exactly as an ordinary
    /// turn's is, so an archived or edited target cannot be submitted.
    pub(crate) fn open_decision(
        &mut self,
        parent: Option<JobId>,
        spec: JobGroup,
        first: Entry,
        configuration: ExecutionConfiguration,
        request: crate::db::entities::DecisionRequest,
        submission: Option<Submission>,
    ) -> Result<(JobId, Appended), BatchError> {
        self.open_decision_with_prediction(
            parent,
            spec,
            first,
            configuration,
            request,
            submission,
            None,
            None,
        )
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_decision_with_prediction(
        &mut self,
        parent: Option<JobId>,
        spec: JobGroup,
        first: Entry,
        configuration: ExecutionConfiguration,
        request: crate::db::entities::DecisionRequest,
        submission: Option<Submission>,
        prediction: Option<crate::db::entities::PredictionBinding>,
        commands: Option<crate::db::entities::CommandAssessment>,
    ) -> Result<(JobId, Appended), BatchError> {
        let frozen = match self.freeze_configuration(configuration)? {
            crate::db::entities::FrozenConfiguration::Decision(frozen) => frozen,
            crate::db::entities::FrozenConfiguration::Generative(frozen) => {
                crate::db::entities::FrozenDecisionConfig::Generative(frozen, None)
            }
        };
        self.open_resolved_decision(
            parent, spec, first, frozen, request, submission, prediction, commands, None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_resolved_decision(
        &mut self,
        parent: Option<JobId>,
        spec: JobGroup,
        first: Entry,
        frozen: crate::db::entities::FrozenDecisionConfig,
        request: crate::db::entities::DecisionRequest,
        submission: Option<Submission>,
        prediction: Option<crate::db::entities::PredictionBinding>,
        commands: Option<crate::db::entities::CommandAssessment>,
        assignment: Option<crate::db::entities::DecisionAssignment>,
    ) -> Result<(JobId, Appended), BatchError> {
        if let Some(existing) = self.existing_group_submission(submission.as_ref())? {
            return Ok(existing);
        }
        self.validate_group_parent(parent)?;
        let work = crate::db::entities::DecisionWork {
            assignment,
            frozen,
            request,
            prediction,
            commands,
        };
        if let Err(message) = work.validate() {
            return self.reject(Violation::InvalidValue("execution", message));
        }
        let conversation = self.insert_group(parent, &spec)?;
        let appended = self.append_system_turn(
            conversation,
            first,
            JobExecution::Decision(work),
            submission,
            Vec::new(),
            spec.name,
            None,
        )?;
        Ok((conversation, appended))
    }

    fn validate_group_parent(&mut self, parent: Option<JobId>) -> Result<(), BatchError> {
        if let Some(parent) = parent {
            let parent_row = self.require_job(parent)?;
            if !parent_row.kind.is_group() {
                return self.reject(Violation::InvalidValue("parent", "expected a conversation"));
            }
            let (limits, parent_depth) = self.group_limits(parent)?;
            let root = self.root_group(parent)?;
            if parent_depth.saturating_add(1) > limits.max_depth
                || self.live_children(parent)?.len() >= 16
                || self.live_subtree(root)?.len() >= 64
            {
                return self.reject(Violation::InvalidValue(
                    "children",
                    "live conversation limit reached",
                ));
            }
        }
        Ok(())
    }

    fn existing_group_submission(
        &mut self,
        submission: Option<&Submission>,
    ) -> Result<Option<(JobId, Appended)>, BatchError> {
        if let Some(submission) = submission
            && let Some(record) = self.submission(submission.key)?
        {
            if record.fingerprint != submission.fingerprint {
                return self.reject(Violation::SubmissionKeyUsed {
                    job: record.job,
                    same_request: false,
                });
            }
            let turn = self.require_job(record.job)?;
            let Some(conversation) = turn.parent_id else {
                return self.corrupted("submission has no conversation".into());
            };
            return Ok(Some((
                conversation,
                Appended {
                    turn: record.job,
                    seq: record.seq,
                    joined: record.seq > 1,
                },
            )));
        }
        Ok(None)
    }

    fn insert_group(
        &mut self,
        parent: Option<JobId>,
        spec: &JobGroup,
    ) -> Result<JobId, BatchError> {
        let conversation = self.t1_submit(
            None,
            JobSpec {
                allocation: None,
                id: JobId::new(0),
                created_at: self.now(),
                parent_id: parent,
                kind: JobKind::Group {
                    agent: spec.agent,
                    project: spec.project,
                    origin: spec.origin.clone(),
                    keep_open: spec.keep_open,
                },
            },
        )?;
        self.put_job_control(conversation, &JobControl::default())?;
        if let Some(parent) = parent {
            self.link_job_group(parent, conversation)?;
        }
        Ok(conversation)
    }

    #[allow(clippy::too_many_arguments)]
    fn append_system_turn(
        &mut self,
        conversation: JobId,
        entry: Entry,
        execution: JobExecution,
        submission: Option<Submission>,
        dependencies: Vec<(JobId, DependencyCondition)>,
        name: Option<String>,
        format: Option<ResponseFormat>,
    ) -> Result<Appended, BatchError> {
        let row = self.require_job(conversation)?;
        if !row.kind.is_group() {
            return self.reject(Violation::InvalidValue(
                "conversation_not_found",
                "expected a conversation",
            ));
        }
        if self.job_control(conversation)?.closing.is_some() {
            return self.reject(Violation::InvalidValue(
                "conversation_closed",
                "no longer accepts messages",
            ));
        }
        let spec = JobSpec {
            allocation: None,
            id: JobId::new(0),
            created_at: self.now,
            parent_id: Some(conversation),
            kind: JobKind::Execution {
                name,
                execution,
                plan_source: PlanSource::Stored {
                    previous: self.group_head(conversation)?,
                    dependencies,
                },
            },
        };
        let turn = self.t1_submit(None, spec)?;
        let seq = self.insert_turn_input(turn, entry)?;
        if let Some(format) = format {
            self.insert_turn_input(turn, Entry::ResponseFormat { format })?;
        }
        if let Some(submission) = submission {
            let record = SubmissionRecord {
                key: submission.key,
                fingerprint: submission.fingerprint,
                job: turn,
                seq,
                recorded_at: self.now,
            };
            self.record_submission(&record)?;
        }
        Ok(Appended {
            turn,
            seq,
            joined: false,
        })
    }

    /// The settled prefix a later turn may summarize. Pending and running
    /// turns are excluded. `through` is the newest turn included; a turn
    /// still open for input is not a source. Manual compaction is the
    /// production caller; tests seal a prefix directly.
    pub(crate) fn seal_context_source(
        &mut self,
        conversation: JobId,
        through: JobId,
    ) -> Result<Option<crate::db::entities::ContextSourceManifest>, BatchError> {
        let row = self.require_job(conversation)?;
        if !row.kind.is_group() {
            return self.reject(Violation::InvalidValue(
                "conversation",
                "expected a conversation",
            ));
        }
        let through_row = self.require_job(through)?;
        if through_row.parent_id != Some(conversation) || !through_row.kind.is_persistent() {
            return self.reject(Violation::InvalidValue(
                "context_source",
                "the source boundary is not a turn of this conversation",
            ));
        }
        let mut chain = Vec::new();
        let mut cursor = Some(through);
        let mut seen = BTreeSet::new();
        while let Some(turn) = cursor {
            if !seen.insert(turn) || chain.len() >= 4096 {
                return self.reject(Violation::InvalidValue(
                    "context_source",
                    "the source chain is cyclic or too long",
                ));
            }
            let row = self.require_job(turn)?;
            if row.parent_id != Some(conversation) || !row.kind.is_persistent() {
                return self.reject(Violation::InvalidValue(
                    "context_source",
                    "the source chain leaves this conversation",
                ));
            }
            let state = self.require_job_state(turn)?;
            if !state.locally_stopped() || !self.is_subtree_settled(turn)? {
                return Ok(None);
            }
            if self.step(|tx| {
                rows::job_tool_evidence(&tx.open_table(TOOL_CALLS)?, turn).map(|calls| {
                    calls
                        .iter()
                        .any(crate::db::entities::ToolEvidenceRecord::is_unresolved)
                })
            })? {
                return Ok(None);
            }
            chain.push(turn);
            cursor = row.kind.previous();
        }
        chain.reverse();
        let mut boundaries = Vec::with_capacity(chain.len());
        for turn in chain {
            let source = self.step(|tx| {
                reader::source_turn(
                    &tx.open_table(JOBS)?,
                    &tx.open_table(JOB_STATES)?,
                    &tx.open_table(JOB_INPUTS)?,
                    &tx.open_table(JOB_OUTPUTS)?,
                    &tx.open_table(TOOL_CALLS)?,
                    &tx.open_table(APPROVALS)?,
                    turn,
                    None,
                )
            })?;
            let Some(source) = source else {
                return Ok(None);
            };
            boundaries.push(source.boundary);
        }
        let manifest = crate::db::entities::ContextSourceManifest {
            schema_version: 1,
            conversation,
            through_turn: through,
            turns: boundaries,
            base_artifact: None,
        };
        if let Err(message) = manifest.validate() {
            return self.reject(Violation::InvalidValue("context_source", message));
        }
        Ok(Some(manifest))
    }

    /// Confirms a sealed prefix is still the same stored bytes. A later
    /// append beyond the boundary does not change the match.
    pub(super) fn source_still_matches(
        &mut self,
        manifest: &crate::db::entities::ContextSourceManifest,
    ) -> Result<bool, BatchError> {
        for boundary in &manifest.turns {
            let source = self.step(|tx| {
                reader::source_turn(
                    &tx.open_table(JOBS)?,
                    &tx.open_table(JOB_STATES)?,
                    &tx.open_table(JOB_INPUTS)?,
                    &tx.open_table(JOB_OUTPUTS)?,
                    &tx.open_table(TOOL_CALLS)?,
                    &tx.open_table(APPROVALS)?,
                    boundary.turn,
                    Some((boundary.input_through, boundary.output_through)),
                )
            })?;
            if source.is_none_or(|source| source.boundary != *boundary) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn predictive_capacity(
        &mut self,
        conversation: JobId,
        prediction: JobId,
        tokens: u32,
        requests: u32,
    ) -> Result<bool, BatchError> {
        use crate::db::entities::*;
        let predicted = self.require_job(prediction)?;
        if !self.step(|tx| {
            reader::prediction_current(
                &tx.open_table(META)?,
                &tx.open_table(JOBS)?,
                &tx.open_table(CONFIGS)?,
                &tx.open_table(PROJECTS)?,
                &tx.open_table(JOB_CONTROLS)?,
                reader::Turns(&tx.open_table(TURNS)?),
                &predicted,
            )
        })? {
            return Ok(false);
        }
        let JobKind::Execution {
            execution: JobExecution::Decision(work),
            ..
        } = &predicted.kind
        else {
            return Ok(false);
        };
        let Some(snapshot) = work
            .prediction
            .as_ref()
            .filter(|binding| binding.conversation == conversation)
        else {
            return Ok(false);
        };
        let Some(binding) = &snapshot.policy.predictive else {
            return Ok(false);
        };
        if binding.mode != PredictionMode::Prepare
            || tokens > binding.max_preparation_tokens
            || requests > 8
        {
            return Ok(false);
        }
        for child in self.live_children(conversation)? {
            if let Some(tip) = self.group_head(child)?
                && matches!(
                    self.require_job(tip)?.kind,
                    JobKind::Execution {
                        execution: JobExecution::Summarization(_),
                        ..
                    }
                )
                && self.require_job_state(tip)?.outcome.is_none()
            {
                return Ok(false);
            }
        }
        let now = self.now;
        self.step(|tx| {
            let jobs = tx.open_table(JOBS)?;
            let mut pending = 0u32;
            for (scanned, entry) in tx.open_table(PENDING)?.iter()?.enumerate().take(129) {
                if scanned == 128 {
                    return Ok(false);
                }
                let id = entry?.0.value();
                let Some(bytes) = jobs.get(id)? else {
                    return Err(StorageError::corrupted("pending Job missing"));
                };
                let row = JobSpec::decode_at(id, bytes.value())?;
                if reader::work_priority(&jobs, &row)? == 2 {
                    pending += 1;
                } else if reader::work_priority(&jobs, &row)? == 0 {
                    return Ok(false);
                }
            }
            if pending.saturating_add(requests) > 8 {
                return Ok(false);
            }
            let mut reserved_tokens = u64::from(tokens);
            let mut reserved_requests = u64::from(requests);
            let mut preparations = 1u32;
            let children = tx.open_table(CHILD_GROUPS)?;
            let turns = tx.open_table(TURNS)?;
            let events = tx.open_table(REQUEST_EVENTS)?;
            for (scanned, entry) in children
                .range((conversation.get(), 0)..=(conversation.get(), u128::MAX))?
                .rev()
                .enumerate()
                .take(257)
            {
                if scanned == 256 {
                    return Ok(false);
                }
                let child = entry?.0.value().1;
                let Some(tip) = reader::turn_head(&turns, JobId::new(child))?.map(JobId::get)
                else {
                    continue;
                };
                let Some(bytes) = jobs.get(tip)? else {
                    return Err(StorageError::corrupted("child turn missing"));
                };
                let row = JobSpec::decode_at(tip, bytes.value())?;
                let JobKind::Execution {
                    execution: JobExecution::Summarization(work),
                    ..
                } = &row.kind
                else {
                    continue;
                };
                if work.prediction.is_none() {
                    continue;
                }
                let (charge, charged_at) = reader::preparation_charge(
                    &jobs,
                    &children,
                    reader::Turns(&turns),
                    &events,
                    &row,
                )?;
                if now
                    .as_unix_micros()
                    .saturating_sub(charged_at.as_unix_micros())
                    >= 3_600_000_000
                {
                    continue;
                }
                if let JobKind::Execution {
                    execution: JobExecution::Summarization(work),
                    ..
                } = row.kind
                    && work.prediction.is_some()
                {
                    preparations = preparations.saturating_add(1);
                    reserved_tokens = reserved_tokens.saturating_add(charge);
                    reserved_requests =
                        reserved_requests.saturating_add(u64::from(work.reserved_requests));
                }
            }
            Ok(preparations <= binding.max_preparations_per_hour
                && reserved_tokens <= u64::from(binding.max_tokens_per_hour)
                && reserved_requests <= u64::from(binding.max_requests_per_hour))
        })
    }

    pub(super) fn cancel_stale_context_work(&mut self) -> Result<(), BatchError> {
        let candidates = self.step(|tx| {
            let jobs = tx.open_table(JOBS)?;
            let mut stale = Vec::new();
            for entry in tx.open_table(PENDING)?.iter()?.take(128) {
                let id = JobId::new(entry?.0.value());
                let Some(bytes) = jobs.get(id.get())? else {
                    return Err(StorageError::corrupted("pending Job missing"));
                };
                let row = JobSpec::decode_at(id.get(), bytes.value())?;
                if !reader::prediction_current(
                    &tx.open_table(META)?,
                    &jobs,
                    &tx.open_table(CONFIGS)?,
                    &tx.open_table(PROJECTS)?,
                    &tx.open_table(JOB_CONTROLS)?,
                    reader::Turns(&tx.open_table(TURNS)?),
                    &row,
                )? || !reader::preparation_current(
                    &tx.open_table(META)?,
                    &jobs,
                    &tx.open_table(CONFIGS)?,
                    &tx.open_table(PROJECTS)?,
                    &tx.open_table(JOB_CONTROLS)?,
                    &tx.open_table(NOTICES)?,
                    &row,
                )? {
                    stale.push(id);
                }
            }
            Ok(stale)
        })?;
        for job in candidates {
            let state = self.require_job_state(job)?;
            if state.outcome.is_none() {
                self.finalize(
                    job,
                    OutcomeKind::Cancelled,
                    None,
                    Some(crate::db::entities::FinalResult {
                        summary: Some("context work no longer eligible".into()),
                        code: Some("context_stale".into()),
                        goal_reached: Some(false),
                        value: None,
                    }),
                )?;
            }
        }
        Ok(())
    }

    fn check_context_job(&mut self, consumer: &JobSpec, producer: JobId) -> Result<(), BatchError> {
        let source = self.require_job(producer)?;
        let Some(parent) = source.parent_id else {
            return self.reject(Violation::InvalidValue(
                "context_job",
                "requires a system child turn",
            ));
        };
        let owner = self.require_job(parent)?;
        if source.id == consumer.id
            || !source.kind.is_persistent()
            || owner.parent_id != consumer.parent_id
            || !matches!(
                owner.kind,
                JobKind::Group {
                    origin: Origin::System,
                    ..
                }
            )
        {
            return self.reject(Violation::InvalidValue(
                "context_job",
                "requires a system child turn of this conversation",
            ));
        }
        Ok(())
    }

    pub(crate) fn insert_turn_input(
        &mut self,
        turn: JobId,
        entry: Entry,
    ) -> Result<u32, BatchError> {
        let row = self.require_job(turn)?;
        let state = self.require_job_state(turn)?;
        if !row.kind.is_persistent() || state.outcome.is_some() {
            return self.reject(Violation::InvalidValue(
                "turn_input",
                "input is frozen by claim or outcome",
            ));
        }
        if let Entry::UserMessageEdit { message, .. } | Entry::UserMessageWithdraw { message } =
            &entry
        {
            let valid = self.step(|tx| {
                Ok(reader::effective_entries(reader::job_entries_of(
                    &tx.open_table(JOB_INPUTS)?,
                    turn,
                )?)?
                .iter()
                .any(|(seq, value)| {
                    seq == message && matches!(value.entry, Entry::UserMessage { .. })
                }))
            })?;
            if !valid {
                return self.reject(Violation::InvalidValue(
                    "queued_message",
                    "message is absent or already withdrawn",
                ));
            }
        }
        match &entry {
            Entry::ContextPreparation {
                job,
                policy_revision,
            } => {
                self.check_context_job(&row, *job)?;
                if row.parent_id.is_none() {
                    return self.corrupted("stored turn has no conversation".into());
                }
                if *policy_revision
                    != self
                        .step(|tx| reader::global_compaction_settings(&tx.open_table(META)?))?
                        .revision
                {
                    return self.reject(Violation::InvalidValue(
                        "context_preparation",
                        "policy revision has changed",
                    ));
                }
            }
            Entry::ContextUse {
                artifact: Some(artifact),
            } => {
                self.check_context_job(&row, *artifact)?;
                if !matches!(
                    self.require_job(*artifact)?.kind,
                    JobKind::Execution {
                        execution: JobExecution::Summarization(_),
                        ..
                    }
                ) {
                    return self.reject(Violation::InvalidValue(
                        "context_use",
                        "artifact must be produced by typed summarization work",
                    ));
                }
                let state = self.require_job_state(*artifact)?;
                if !state
                    .outcome
                    .is_some_and(|outcome| outcome.kind == OutcomeKind::Completed)
                {
                    return self.reject(Violation::InvalidValue(
                        "context_use",
                        "artifact Job must have completed successfully",
                    ));
                }
                let result = self.step(|tx| {
                    let outputs = tx.open_table(JOB_OUTPUTS)?;
                    let last = outputs
                        .range((artifact.get(), 0)..=(artifact.get(), u32::MAX))?
                        .next_back()
                        .transpose()?;
                    last.map(|(key, bytes)| decode_output(key.value(), bytes.value()))
                        .transpose()
                })?;
                let valid = result
                    .filter(|output| output.kind == OutputKind::Result)
                    .and_then(|output| output.value)
                    .and_then(|value| {
                        serde_json::from_value::<crate::db::entities::SummarizationResult>(value)
                            .ok()
                    })
                    .filter(|result| result.validate().is_ok());
                let valid = match valid {
                    Some(result) => {
                        Some(
                            self.step(|tx| {
                                reader::load_context_manifest(
                                    &tx.open_table(MESSAGES)?,
                                    result.source,
                                )
                            })?
                            .conversation,
                        ) == row.parent_id
                    }
                    None => false,
                };
                if !valid {
                    return self.reject(Violation::InvalidValue(
                        "context_use",
                        "requires a validated summarization result for this conversation",
                    ));
                }
            }
            Entry::ContextUse { artifact: None }
            | Entry::UserMessageEdit { .. }
            | Entry::UserMessageWithdraw { .. }
            | Entry::UserMessage { .. }
            | Entry::AgentMessage { .. }
            | Entry::Notice { .. }
            | Entry::Decision { .. }
            | Entry::RetryOf { .. }
            | Entry::Iteration { .. }
            | Entry::ResponseFormat { .. } => {}
        }
        if matches!(
            &entry,
            Entry::UserMessage { .. } | Entry::UserMessageEdit { .. }
        ) {
            let at = self.now;
            let images = self.step(|tx| {
                let mut journal = reader::job_entries_of(&tx.open_table(JOB_INPUTS)?, turn)?;
                journal.push((
                    u32::MAX,
                    JobInputEntry {
                        entry: entry.clone(),
                        at,
                    },
                ));
                Ok(reader::effective_entries(journal)?
                    .into_iter()
                    .flat_map(|(_, value)| match value.entry {
                        Entry::UserMessage { attachments, .. } => attachments,
                        Entry::UserMessageEdit { .. }
                        | Entry::UserMessageWithdraw { .. }
                        | Entry::AgentMessage { .. }
                        | Entry::Notice { .. }
                        | Entry::Decision { .. }
                        | Entry::RetryOf { .. }
                        | Entry::Iteration { .. }
                        | Entry::ResponseFormat { .. }
                        | Entry::ContextPreparation { .. }
                        | Entry::ContextUse { .. } => Vec::new(),
                    })
                    .collect::<Vec<_>>())
            })?;
            if images.len()
                > crate::service::ai::providers::openai_compatible::image::MAX_REQUEST_IMAGES
            {
                return self.reject(Violation::InvalidValue(
                    "too_many_images",
                    "the turn has too many images",
                ));
            }
            if !images.is_empty() {
                let (frozen, _, _) = self.job_execution(&row)?;
                if !frozen
                    .llm
                    .config
                    .declared_capabilities
                    .contains(&ModelCapability::Vision)
                {
                    return self.reject(Violation::InvalidValue(
                        "vision_not_supported",
                        "the turn's model does not support images",
                    ));
                }
            }
            let mut bytes = 0u64;
            for attachment in images {
                let image = self.require_attachment(attachment)?;
                if image.origin != AttachmentOrigin::UserProvided {
                    return self.reject(Violation::NotFound(RecordRef::Attachment(attachment)));
                }
                if !image.is_available() {
                    return self.reject(Violation::AttachmentCleanedUp(attachment));
                }
                bytes = bytes.saturating_add(image.byte_length);
                if image.byte_length
                    > crate::service::ai::providers::openai_compatible::image::MAX_IMAGE_BYTES as u64
                    || bytes
                        > crate::service::ai::providers::openai_compatible::image::MAX_REQUEST_IMAGE_BYTES as u64
                {
                    return self.reject(Violation::InvalidValue(
                        "images_too_large",
                        "the turn's images exceed the byte limit",
                    ));
                }
            }
        }
        let at = self.now;
        let seq = self.step(|tx| {
            let previous = match &entry {
                Entry::UserMessageEdit { message, .. } | Entry::UserMessageWithdraw { message } => {
                    let (_, previous) = reader::effective_entries(reader::job_entries_of(
                        &tx.open_table(JOB_INPUTS)?,
                        turn,
                    )?)?
                    .into_iter()
                    .find(|(seq, _)| seq == message)
                    .ok_or_else(|| StorageError::corrupted("queued amendment target missing"))?;
                    indexes::search_input_text(&tx.open_table(ATTACHMENTS)?, &previous.entry)?
                        .map(|text| indexes::search_grams(&text))
                        .unwrap_or_default()
                }
                Entry::UserMessage { .. }
                | Entry::AgentMessage { .. }
                | Entry::Notice { .. }
                | Entry::Decision { .. }
                | Entry::RetryOf { .. }
                | Entry::Iteration { .. }
                | Entry::ResponseFormat { .. }
                | Entry::ContextPreparation { .. }
                | Entry::ContextUse { .. } => BTreeSet::new(),
            };
            let mut table = tx.open_table(JOB_INPUTS)?;
            let seq = table
                .range((turn.get(), 0)..=(turn.get(), u32::MAX))?
                .next_back()
                .transpose()?
                .map_or(1, |(key, _)| key.value().1 + 1);
            table.insert(
                (turn.get(), seq),
                JobInputEntry {
                    entry: entry.clone(),
                    at,
                }
                .encode()
                .as_slice(),
            )?;
            drop(table);
            if let Some(conversation) = row.parent_id {
                match &entry {
                    Entry::UserMessageEdit { .. } | Entry::UserMessageWithdraw { .. } => {
                        indexes::amend_turn_search_index(tx, conversation, turn, &previous)?;
                    }
                    Entry::UserMessage { .. } | Entry::AgentMessage { .. } => {
                        indexes::append_input_search_index(tx, conversation, turn, &entry)?;
                    }
                    Entry::Notice { .. }
                    | Entry::Decision { .. }
                    | Entry::RetryOf { .. }
                    | Entry::Iteration { .. }
                    | Entry::ResponseFormat { .. }
                    | Entry::ContextPreparation { .. }
                    | Entry::ContextUse { .. } => {}
                }
            }
            match entry {
                Entry::UserMessage { .. } => {
                    if let Some(conversation) = row.parent_id {
                        let mut user = tx.open_table(USER_INPUT_AT)?;
                        if user.get(conversation.get())?.is_none() {
                            user.insert(conversation.get(), ())?;
                        }
                    }
                }
                Entry::RetryOf { turn: original, by } => {
                    tx.open_table(RETRIES)?.insert(
                        (original.get(), turn.get()),
                        matches!(by, Actor::Agent { .. }),
                    )?;
                }
                Entry::UserMessageEdit { .. }
                | Entry::AgentMessage { .. }
                | Entry::Notice { .. }
                | Entry::Decision { .. }
                | Entry::Iteration { .. }
                | Entry::UserMessageWithdraw { .. }
                | Entry::ContextPreparation { .. }
                | Entry::ContextUse { .. }
                | Entry::ResponseFormat { .. } => {}
            }
            Ok(seq)
        })?;
        Ok(seq)
    }

    pub(crate) fn title_target(&mut self, turn: &JobSpec) -> Result<Option<JobId>, BatchError> {
        if !turn.kind.is_persistent()
            || turn.kind.name() != Some(crate::db::entities::TITLE_JOB_NAME)
        {
            return Ok(None);
        }
        let Some(conversation) = turn.parent_id else {
            return Ok(None);
        };
        let row = self.require_job(conversation)?;
        if matches!(
            row.kind,
            JobKind::Group {
                origin: Origin::System,
                keep_open: false,
                ..
            }
        ) {
            return Ok(row.parent_id);
        }
        Ok(None)
    }

    pub(crate) fn schedule_title_job(&mut self, turn: &JobSpec) -> Result<(), BatchError> {
        if !turn.kind.is_persistent()
            || turn.kind.previous().is_some()
            || turn.kind.name().is_some()
        {
            return Ok(());
        }
        let Some(target) = turn.parent_id else {
            return Ok(());
        };
        let parent = self.require_job(target)?;
        if parent.parent_id.is_some()
            || !matches!(
                parent.kind,
                JobKind::Group {
                    origin: Origin::User,
                    keep_open: true,
                    ..
                }
            )
        {
            return Ok(());
        }
        let metadata = self.job_control(target)?;
        if metadata.title.is_some()
            || metadata.closing.is_some()
            || self.require_job_state(target)?.outcome.is_some()
        {
            return Ok(());
        }
        let (limits, depth) = self.group_limits(target)?;
        if depth >= limits.max_depth
            || self.live_children(target)?.len() >= 16
            || self.live_subtree(target)?.len() >= 64
        {
            return Ok(());
        }
        if self.step(|tx| Ok(tx.open_table(PENDING)?.len()?))? >= P_MAX {
            return Ok(());
        }
        let (mut frozen, _, _) = self.job_execution(turn)?;
        for id in [frozen.llm.model.id, frozen.llm.provider.id.into()] {
            if self.config(id)?.is_none_or(|row| row.archived_at.is_some()) {
                return Ok(());
            }
        }
        let sent = self.step(|tx| {
            for entry in tx
                .open_table(REQUEST_EVENTS)?
                .range((turn.id.get(), 0, 0)..=(turn.id.get(), u32::MAX, u32::MAX))?
            {
                let (key, bytes) = entry?;
                let (_, request, seq) = key.value();
                if let Some(request) = RequestNumber::new(request)
                    && matches!(
                        rows::decode_request_event(turn.id, request, seq, bytes.value())?.payload,
                        crate::db::entities::RequestPayload::Sent { .. }
                    )
                {
                    return Ok(true);
                }
            }
            Ok(false)
        })?;
        if !sent {
            return Ok(());
        }
        let text = self.step(|tx| {
            for (_, entry) in reader::effective_entries(reader::job_entries_of(
                &tx.open_table(JOB_INPUTS)?,
                turn.id,
            )?)? {
                if let Entry::UserMessage { text, .. } = entry.entry {
                    return Ok(Some(text));
                }
            }
            Ok(None)
        })?;
        let Some(text) = text.filter(|text| !text.trim().is_empty()) else {
            return Ok(());
        };
        frozen.agent = None;
        frozen.agent_ref = None;
        frozen.llm.config.declared_capabilities = BTreeSet::from([ModelCapability::Chat]);
        frozen.llm.config.parameters = crate::db::entities::LlmParameters {
            max_tokens: Some(match frozen.llm.provider.transport {
                LlmTransport::OpenAiChatCompletions => 256,
                LlmTransport::OpenAiResponses => 4096,
                LlmTransport::AnthropicMessages => 256,
            }),
            ..Default::default()
        };
        let conversation = self.t1_submit(
            None,
            JobSpec {
                allocation: None,
                id: JobId::new(0),
                created_at: self.now,
                parent_id: Some(target),
                kind: JobKind::Group {
                    agent: None,
                    project: None,
                    origin: Origin::System,
                    keep_open: false,
                },
            },
        )?;
        self.put_job_control(conversation, &JobControl::default())?;
        self.step(|tx| {
            tx.open_table(CHILD_GROUPS)?
                .insert((target.get(), conversation.get()), ())?;
            tx.open_table(LIVE_CHILDREN)?
                .insert((target.get(), conversation.get()), ())?;
            Ok(())
        })?;
        let format = match frozen.llm.provider.transport {
            LlmTransport::AnthropicMessages => ResponseFormat::JsonSchema {
                name: "conversation_title_v1".into(),
                schema: serde_json::json!({
                    "type": "object",
                    "properties": { "title": { "type": "string" } },
                    "required": ["title"],
                    "additionalProperties": false,
                }),
                strict: Some(true),
            },
            LlmTransport::OpenAiChatCompletions | LlmTransport::OpenAiResponses => {
                ResponseFormat::JsonObject
            }
        };
        self.append_system_turn(
            conversation,
            Entry::UserMessage {
                text: crate::db::entities::title_prompt(&text),
                attachments: Vec::new(),
            },
            JobExecution::Frozen(frozen),
            None,
            Vec::new(),
            Some(crate::db::entities::TITLE_JOB_NAME.to_owned()),
            Some(format),
        )?;
        Ok(())
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

    pub(crate) fn index_answer(
        &mut self,
        turn: &JobSpec,
        kind: OutcomeKind,
        result: bool,
    ) -> Result<(), BatchError> {
        if !turn.kind.is_persistent() {
            return Ok(());
        }
        let Some(conversation) = turn.parent_id else {
            return Ok(());
        };
        self.step(|tx| {
            let mut table = tx.open_table(GROUP_RESULTS)?;
            let mut tips = table
                .get(conversation.get())?
                .map_or((0, 0), |tips| tips.value());
            if kind != OutcomeKind::Cancelled {
                tips.0 = tips.0.max(turn.id.get());
            }
            if result {
                tips.1 = tips.1.max(turn.id.get());
            }
            if tips != (0, 0) {
                table.insert(conversation.get(), tips)?;
            }
            Ok(())
        })
    }
    pub(crate) fn settle_group(&mut self, conversation: JobId) -> Result<(), BatchError> {
        let mut state = self.require_job_state(conversation)?;
        let row = self.job_control(conversation)?;
        if row.closing.is_none() || state.outcome.is_some() {
            return Ok(());
        }
        let tip = self.group_head(conversation)?;
        let tip_closed = match tip {
            Some(tip) => self
                .require_job_state(tip)?
                .outcome
                .is_some_and(|outcome| outcome.cause == Some(OutcomeCause::Closed)),
            None => false,
        };
        let answered = self.step(|tx| {
            Ok(tx
                .open_table(GROUP_RESULTS)?
                .get(conversation.get())?
                .map(|tips| tips.value().0)
                .filter(|turn| *turn != 0))
        })?;
        let outcome = match answered {
            Some(turn) => self.require_job_state(JobId::new(turn))?.outcome,
            None => None,
        };
        let outcome = if row.closing.is_some_and(|closing| closing.running) || tip_closed {
            None
        } else {
            outcome
        };
        let kind = outcome
            .as_ref()
            .map_or(OutcomeKind::Cancelled, |outcome| outcome.kind);
        state.outcome = Some(OutcomeRecord {
            kind,
            at: self.now,
            cause: outcome
                .and_then(|outcome| outcome.cause)
                .or_else(|| (kind == OutcomeKind::Cancelled).then_some(OutcomeCause::Closed)),
        });
        if kind == OutcomeKind::Unknown {
            state.stop_confirmed_at = Some(self.now);
            state.stop_confirmed_by = Some(StopProof::Settled);
        }
        self.put_job_state(conversation, &state)
    }
}
