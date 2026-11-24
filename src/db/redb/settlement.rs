use super::{StorageError, TxRead};
use crate::db::entities::{
    AppendedOutput, DependencyCondition, DependencyState, ExecutionEventKind, FrozenConfiguration,
    FrozenExecutionConfig, JobId, JobSpec, MutableJobPhase, Origin, OutcomeCause, OutcomeKind,
    OutcomeRecord, OutputKind, ProjectId, RecordRef, ToolCallRef, ToolDecision, ToolOutcome,
    Violation,
};
use crate::db::redb::batch::{Batch, BatchError, Failure};
use crate::db::redb::codec::Code;
use crate::db::redb::jobs::{
    TurnMembership, decode_mutable_job, is_predictive_helper, mutable_priority,
};
use crate::db::redb::reads::execution_context::{
    NativeSourceError, summary_child_job, summary_history_job, summary_job_state, summary_turn_head,
};
use crate::db::redb::reservations::mutable_configuration_requirement;
use crate::db::redb::rows::decode_output;
use crate::db::redb::schema::{
    CHILDREN, CONVERSATIONS, DEPENDENCY_WAITERS, JOB_OUTPUTS, JOB_RUNTIME, JOBS, META,
    MUTABLE_READY, RECURRENCE_DUE, RECURRENCES, TURNS,
};
use crate::db::redb::{reads, state};
use ::redb::{Database, ReadableTable};
use std::ops::ControlFlow;
use std::time::Duration;

fn recurrence_delay(tx: &impl TxRead, now: i64) -> Result<Option<Duration>, StorageError> {
    let due = tx
        .table(RECURRENCE_DUE)?
        .first()?
        .map(|(key, _)| key.value().0);
    Ok(due.map(|due| Duration::from_micros(due.saturating_sub(now).max(0) as u64)))
}

pub(crate) fn read_recurrence_delay(database: &Database) -> Result<Option<Duration>, StorageError> {
    recurrence_delay(
        &reads::read(database)?,
        crate::db::entities::Timestamp::now().as_unix_micros(),
    )
}

pub(super) fn recurrence_submission(
    conv_u128: u128,
    next_n: u64,
    label: &[u8],
) -> crate::db::entities::Submission {
    let mut ctx = aws_lc_rs::digest::Context::new(&aws_lc_rs::digest::SHA256);
    ctx.update(label);
    ctx.update(&conv_u128.to_le_bytes());
    ctx.update(&next_n.to_le_bytes());
    let digest: [u8; 32] = ctx.finish().as_ref().try_into().unwrap_or([0; 32]);
    crate::db::entities::Submission {
        key: crate::db::entities::SubmissionKey::from_digest(digest),
        fingerprint: crate::db::entities::RequestFingerprint::from_digest(digest),
    }
}

impl Batch {
    pub(super) fn read_summary_source<T>(
        &mut self,
        read: impl FnOnce(&::redb::WriteTransaction) -> Result<T, NativeSourceError>,
    ) -> Result<T, BatchError> {
        let result = self.step(|tx| match read(tx) {
            Ok(value) => Ok(Ok(value)),
            Err(NativeSourceError::Storage(error)) => Err(error),
            Err(NativeSourceError::Corrupt { job, detail }) => {
                Ok(Err(Violation::SummarySourceCorrupt { job, detail }))
            }
            Err(NativeSourceError::Unavailable(reason)) => {
                Ok(Err(Violation::InvalidValue("summary_source", reason)))
            }
        })?;
        if let Err(Violation::SummarySourceCorrupt { job, detail }) = &result {
            self.note_summary_corruption(*job, detail);
        }
        result.map_err(|violation| BatchError(Failure::Rejected(violation)))
    }

    /// Records that a read proved some committed record is damaged.
    ///
    /// The caller decides what to do with the affected Job; the database must
    /// still re-verify every record before the next start trusts a clean
    /// shutdown marker.
    pub(super) fn note_summary_corruption(&mut self, producer: JobId, detail: &str) {
        self.needs_full_verification = true;
        log::error!(
            "corrupt summary source {producer}: {detail}. Full storage verification required on next open"
        );
    }

    /// Verifies one published summary, isolating damage confined to the producer.
    ///
    /// Returns `Ok(None)` when the summary is simply unusable: not yet settled,
    /// withdrawn, or inconsistent with the consumer. Source-local corruption is
    /// also isolated as `Ok(None)`, but only after recording that the next open
    /// owes a full verification. Damage to shared rows still fails the caller.
    pub(super) fn verify_summary_source<T>(
        &mut self,
        read: impl FnOnce(
            &::redb::WriteTransaction,
        )
            -> Result<Option<T>, super::reads::execution_context::SummarySourceError>,
    ) -> Result<Option<T>, BatchError> {
        use super::reads::execution_context::SummarySourceError;
        let read = self.step(|tx| match read(tx) {
            Ok(value) => Ok(Ok(value)),
            Err(SummarySourceError::Storage(error)) => Err(error),
            Err(SummarySourceError::Corrupt { producer, detail }) => Ok(Err((producer, detail))),
        })?;
        match read {
            Ok(found) => Ok(found),
            Err((producer, detail)) => {
                self.note_summary_corruption(producer, &detail);
                Ok(None)
            }
        }
    }

    pub(super) fn automatic_compaction(
        &mut self,
        body: impl FnOnce(&mut Self) -> Result<(), BatchError>,
    ) -> Result<(), BatchError> {
        let probe = self.probe;
        let result = body(self);
        // The caller's durable outcome remains the confirmation target.
        self.probe = probe;
        self.check_failure()?;
        match result {
            Err(BatchError(Failure::Rejected(reason))) => {
                log::debug!("automatic compaction declined before writes: {reason}");
                Ok(())
            }
            result => result,
        }
    }

    pub(super) fn maybe_trigger_native_compaction(
        &mut self,
        conversation: JobId,
        frozen: &FrozenExecutionConfig,
    ) -> Result<(), BatchError> {
        let Some(window) = frozen.llm.config.context_window else {
            return Ok(());
        };
        let reserve = frozen
            .llm
            .config
            .parameters
            .max_completion_tokens
            .or(frozen.llm.config.parameters.max_tokens)
            .unwrap_or(crate::db::entities::SUMMARIZER_OUTPUT_RESERVE);
        let (policy, _) =
            self.step(|tx| reads::catalog::effective_context_policy(&tx.open_table(META)?))?;
        let Ok(recipe) = policy.recipe(Some(window), reserve, 1024, 0) else {
            return Ok(());
        };
        let Some(trigger) = recipe.trigger else {
            return Ok(());
        };
        let summarizer = policy.summarization_model.unwrap_or(frozen.llm.model.id);
        let total_tokens = self.read_summary_source(|tx| {
            let Some(tip) = summary_turn_head(tx, conversation)? else {
                return Ok(0usize);
            };
            let jobs = tx.table(JOBS)?;
            let outputs = tx.table(JOB_OUTPUTS)?;
            let states = tx.table(JOB_RUNTIME)?;
            let conversations = tx.table(CONVERSATIONS)?;
            let events = tx.table(super::schema::REQUEST_EVENTS)?;
            let messages = tx.table(super::schema::MESSAGES)?;
            let mut cursor = Some(tip);
            let mut total_tokens = 0usize;
            let mut steps = 0usize;
            while let Some(turn_id) = cursor {
                steps += 1;
                if steps > 4096 {
                    break;
                }
                let turn_job = summary_history_job(&jobs, turn_id, conversation)?;
                let mut bytes = 16usize
                    .saturating_add(turn_job.input.text().len())
                    .saturating_add(turn_job.input.attachments().len().saturating_mul(4800));
                for entry in outputs.range((turn_id.get(), 0)..=(turn_id.get(), u32::MAX))? {
                    let (key, value) = entry?;
                    let out = NativeSourceError::decoded(
                        turn_id,
                        decode_output(key.value(), value.value()),
                    )?;
                    if out.kind == OutputKind::Result {
                        let answer = super::observations::answer_text_in(
                            &jobs,
                            &states,
                            &conversations,
                            &events,
                            &messages,
                            turn_id,
                            &out,
                        )?;
                        bytes = bytes.saturating_add(
                            answer
                                .or_else(|| out.display_text())
                                .as_ref()
                                .map_or(0, String::len),
                        );
                    }
                }
                total_tokens = total_tokens.saturating_add(bytes.div_ceil(3));
                cursor = turn_job.previous;
            }
            Ok(total_tokens)
        })?;
        let cooldown_micros = i64::from(recipe.cooldown_seconds).saturating_mul(1_000_000);
        let pred_interval_micros = policy.predictive.as_ref().map_or(0, |p| {
            i64::from(p.minimum_interval_seconds).saturating_mul(1_000_000)
        });
        let now_micros = self.now.as_unix_micros();
        let (summary_blocked, pred_blocked, pred_count) = self.read_summary_source(|tx| {
            let children = tx.table(CHILDREN)?;
            let jobs = tx.table(JOBS)?;
            let groups = tx.table(CONVERSATIONS)?;
            let states = tx.table(JOB_RUNTIME)?;
            let mut summary_blocked = false;
            let mut pred_blocked = false;
            let mut pred_count = 0u32;
            for entry in
                children.range((conversation.get(), 0)..=(conversation.get(), u128::MAX))?
            {
                let (entry_key, _) = entry?;
                let (_, child_id) = entry_key.value();
                let child = JobId::new(child_id);
                let Some(child_job) = summary_child_job(&jobs, &groups, conversation, child)?
                else {
                    continue;
                };
                let outcome = summary_job_state(&states, child)?.outcome;
                if child_job.summary_source.is_some() {
                    if outcome.is_none()
                        || (cooldown_micros > 0
                            && now_micros.saturating_sub(child_job.created_at.as_unix_micros())
                                < cooldown_micros)
                    {
                        summary_blocked = true;
                    }
                } else if is_predictive_helper(tx, &child_job)? {
                    pred_count = pred_count.saturating_add(1);
                    if outcome.is_none()
                        || (pred_interval_micros > 0
                            && now_micros.saturating_sub(child_job.created_at.as_unix_micros())
                                < pred_interval_micros)
                    {
                        pred_blocked = true;
                    }
                }
            }
            Ok((summary_blocked, pred_blocked, pred_count))
        })?;
        if summary_blocked {
            return Ok(());
        }
        if total_tokens >= trigger as usize {
            let mut ctx = aws_lc_rs::digest::Context::new(&aws_lc_rs::digest::SHA256);
            ctx.update(b"native-auto-compaction");
            ctx.update(&conversation.get().to_le_bytes());
            ctx.update(&self.now.as_unix_micros().to_le_bytes());
            let digest: [u8; 32] = ctx.finish().as_ref().try_into().unwrap_or([0; 32]);
            let submission = crate::db::entities::Submission {
                key: crate::db::entities::SubmissionKey::from_digest(digest),
                fingerprint: crate::db::entities::RequestFingerprint::from_digest(digest),
            };
            self.request_compaction_for(conversation, summarizer, submission, Some(recipe))?;
        } else if let Some(pred) = &policy.predictive
            && pred.mode == crate::db::entities::PredictionMode::Prepare
            && !pred_blocked
            && pred_count < pred.max_preparations
            && total_tokens >= (trigger as usize).saturating_mul(3) / 4
        {
            if self
                .require_mutable_conversation(conversation)?
                .parent_id
                .is_some()
            {
                return Ok(());
            }
            let decision_request = crate::db::entities::DecisionRequest {
                question: crate::db::entities::DecisionQuestion::BinaryProbability {
                    prompt: "Predict whether context compaction should be prepared before the next turn."
                        .to_owned(),
                },
                state: serde_json::json!({
                    "kind": "predictive_compaction",
                    "conversation": conversation.get().to_string(),
                    "tokens": total_tokens,
                    "trigger": trigger,
                }),
            };
            let decision_frozen = self.freeze_decision_profile(pred.decision_agent)?;
            if let Err(reason) = decision_request.validate(&decision_frozen.capabilities()) {
                return self.reject(Violation::InvalidValue("decision_agent", reason));
            }
            let input = crate::db::entities::MutableJobInput {
                priority: crate::db::entities::JobQueuePriority::Speculative,
                content: crate::db::entities::MutableJobContent::Decision {
                    profile: decision_frozen.profile_ref().id,
                    references: decision_frozen.references().collect(),
                    request: decision_request,
                },
            };
            let wrapped = FrozenConfiguration::Decision(decision_frozen);
            let storable = self.storable_frozen(&wrapped)?;
            self.atomic(|batch| {
                let pred_id = JobId::new(batch.next_id()?);
                let pred_job = crate::db::entities::MutableJob {
                    id: pred_id,
                    parent_id: conversation,
                    previous: None,
                    submitted_by: crate::db::entities::Actor::Auto,
                    retry: None,
                    composer_context: crate::db::entities::ComposerContextId::new(batch.next_id()?),
                    created_at: batch.now,
                    submitted_at: Some(batch.now),
                    revision: 1,
                    phase: crate::db::entities::MutableJobPhase::Queued,
                    input,
                    context_selection: crate::db::entities::MutableContextSelection::FullHistory,
                    summary_source: None,
                    frozen: Some(storable),
                };
                let req_tag = mutable_configuration_requirement(&wrapped);
                batch.enqueue_mutable_job(&pred_job, req_tag, TurnMembership::NonTurn, false, true)
            })?;
        }
        Ok(())
    }

    pub(super) fn settle_mutable_outcome(
        &mut self,
        token: crate::db::entities::AllocationToken,
        job: &crate::db::entities::MutableJob,
        kind: OutcomeKind,
        result: crate::db::entities::FinalResult,
    ) -> Result<(u32, crate::db::entities::JobOutputId), BatchError> {
        let mut state = self.require_job_state(token.job)?;
        state.outcome = Some(OutcomeRecord {
            kind,
            at: self.now,
            cause: None,
        });
        self.put_job_state(token.job, &state)?;
        let (seq, id) = self.write_output(
            token.job,
            OutputKind::Result,
            None,
            result.summary,
            result.code,
            result.goal_reached,
            result.value,
        )?;
        self.finish_job_membership(job, true)?;
        self.advance_recurrence_on_settlement(job, kind)?;
        self.settle_native_dependents(token.job, kind)?;
        Ok((seq, id))
    }

    pub(super) fn finish_job_membership(
        &mut self,
        job: &crate::db::entities::MutableJob,
        produced_result: bool,
    ) -> Result<(), BatchError> {
        self.step(|tx| {
            let mut child = state::require(tx, job.id)?;
            if child.pending.take().is_none() {
                return Err(StorageError::corrupted("native Job pending counts missing"));
            }
            state::write(tx, &child)?;
            let mut parent = state::require(tx, job.parent_id)?;
            let (dependencies, children) = parent.pending.ok_or_else(|| {
                StorageError::corrupted("native conversation pending counts missing")
            })?;
            let children = children.checked_sub(1).ok_or_else(|| {
                StorageError::corrupted("native conversation child count underflow")
            })?;
            parent.pending = Some((dependencies, children));
            if produced_result {
                parent.record_result(job.id);
            }
            state::write(tx, &parent)
        })
    }

    pub(super) fn dependency_outcome_state(
        outcome: Option<&OutcomeRecord>,
        condition: DependencyCondition,
    ) -> DependencyState {
        match outcome {
            None => DependencyState::Waiting,
            Some(outcome) => Self::dependency_outcome_kind_state(outcome.kind, condition),
        }
    }

    pub(super) fn dependency_outcome_kind_state(
        kind: OutcomeKind,
        condition: DependencyCondition,
    ) -> DependencyState {
        match (kind, condition) {
            (OutcomeKind::Unknown, _) => DependencyState::Waiting,
            (OutcomeKind::Completed, _) => DependencyState::Satisfied,
            (OutcomeKind::Failed | OutcomeKind::Cancelled, DependencyCondition::Settled) => {
                DependencyState::Satisfied
            }
            (OutcomeKind::Failed | OutcomeKind::Cancelled, DependencyCondition::Success) => {
                DependencyState::Impossible
            }
        }
    }

    fn enqueue_settled_waiter_if_eligible(
        &mut self,
        job: &crate::db::entities::MutableJob,
    ) -> Result<(), BatchError> {
        if self.pool_allocation(job.id)?.is_some()
            || self.require_job_state(job.id)?.outcome.is_some()
        {
            return Ok(());
        }
        let submitted = job.submitted_at.ok_or_else(|| {
            BatchError(Failure::Storage(StorageError::corrupted(
                "queued Job has no submission time",
            )))
        })?;
        let frozen = self.frozen_configuration(job)?;
        let req_tag = mutable_configuration_requirement(&frozen);
        let priority = mutable_priority(job.input.priority);
        let submitted_micros = submitted.as_unix_micros();
        let inserted = self.step(|tx| {
            let mut ready = tx.open_table(MUTABLE_READY)?;
            let already_ready = ready
                .range(
                    (priority, submitted_micros, job.id.get(), 0)
                        ..=(priority, submitted_micros, job.id.get(), u16::MAX),
                )?
                .next()
                .transpose()?
                .is_some();
            if already_ready {
                return Ok(false);
            }
            ready.insert((priority, submitted_micros, job.id.get(), req_tag), ())?;
            Ok(true)
        })?;
        if inserted {
            self.wake_scheduler();
        }
        Ok(())
    }

    pub(super) fn settle_native_dependents(
        &mut self,
        prerequisite: JobId,
        kind: OutcomeKind,
    ) -> Result<(), BatchError> {
        if kind == OutcomeKind::Unknown {
            return Ok(());
        }
        let waiters = self.step(|tx| {
            let table = tx.open_table(DEPENDENCY_WAITERS)?;
            let mut found = Vec::new();
            for entry in table.range((prerequisite.get(), 0)..=(prerequisite.get(), u128::MAX))? {
                let (key, value) = entry?;
                let condition = DependencyCondition::from_code(value.value())
                    .ok_or_else(|| StorageError::corrupted("invalid dependency condition code"))?;
                found.push((JobId::new(key.value().1), condition));
            }
            Ok(found)
        })?;
        for (waiter_id, condition) in waiters {
            let Some(waiter_job) = self.mutable_job(waiter_id)? else {
                continue;
            };
            let dependency_state = Self::dependency_outcome_kind_state(kind, condition);
            if dependency_state != DependencyState::Waiting {
                self.record_execution_event(
                    waiter_id,
                    ExecutionEventKind::DependencyResolved {
                        prerequisite,
                        state: dependency_state,
                    },
                )?;
            }
            let waiter_state = self.require_job_state(waiter_id)?;
            if waiter_job.phase != MutableJobPhase::Queued || waiter_state.outcome.is_some() {
                continue;
            }
            match dependency_state {
                DependencyState::Waiting => {}
                DependencyState::Impossible => {
                    self.end_queued_native_job(&waiter_job, OutcomeCause::DependencyImpossible)?;
                }
                DependencyState::Satisfied => {
                    let ready_now = self.step(|tx| {
                        let Some((dependencies, children)) = state::pending(tx, waiter_id)? else {
                            return Ok(false);
                        };
                        let dependencies = dependencies.checked_sub(1).ok_or_else(|| {
                            StorageError::corrupted("native Job dependency count underflow")
                        })?;
                        state::set_pending(tx, waiter_id, Some((dependencies, children)))?;
                        Ok(dependencies == 0 && children == 0)
                    })?;
                    if ready_now {
                        self.enqueue_settled_waiter_if_eligible(&waiter_job)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn planning_policy_for(
        &mut self,
        parent: &JobSpec,
        job: &crate::db::entities::MutableJob,
        generative: &FrozenExecutionConfig,
    ) -> Result<
        Option<(
            ProjectId,
            crate::db::entities::ProjectRecord,
            crate::db::entities::ProjectDecisionConfig,
        )>,
        BatchError,
    > {
        if job.summary_source.is_some()
            || !generative.agent.as_ref().is_some_and(|agent| {
                !agent.capabilities.is_empty() || agent.workspace_grant().is_some()
            })
        {
            return Ok(None);
        }
        let Some(project_id) = parent.project else {
            return Ok(None);
        };
        if let Origin::Agent { call: opened } = parent.origin
            && self
                .tool_evidence(opened)?
                .is_some_and(|opening| opening.name == "job_plan")
        {
            return Ok(None);
        }
        let project = self.require_project_record(project_id)?;
        let policy = project.decision_policy();
        Ok(policy.enabled.then_some((project_id, project, policy)))
    }

    fn planning_task_context(
        &mut self,
        job: &crate::db::entities::MutableJob,
    ) -> Result<String, BatchError> {
        let mut context = self.step(|tx| {
            let first = reads::jobs::turn_first(&tx.open_table(TURNS)?, job.parent_id)?;
            let jobs = tx.open_table(JOBS)?;
            let mut context = String::new();
            for (label, source, limit) in [
                ("Original task", first.filter(|id| *id != job.id), 4096),
                (
                    "Previous request",
                    job.previous
                        .filter(|id| Some(*id) != first && *id != job.id),
                    2048,
                ),
            ] {
                let Some(source) = source else { continue };
                let row = jobs
                    .get(source.get())?
                    .ok_or_else(|| StorageError::corrupted("planning context turn is missing"))?;
                let source = decode_mutable_job(source.get(), row.value())?;
                if source.parent_id != job.parent_id {
                    return Err(StorageError::corrupted(
                        "planning context crosses conversations",
                    ));
                }
                context.push_str(label);
                context.push_str(":\n");
                context.extend(source.input.text().chars().take(limit));
                context.push_str("\n\n");
            }
            Ok(context)
        })?;
        context.push_str("Current request:\n");
        context.extend(job.input.text().chars().take(4096));
        Ok(context)
    }

    pub(super) fn maybe_attach_planning_decision(
        &mut self,
        parent: &JobSpec,
        job: &crate::db::entities::MutableJob,
        frozen: &FrozenConfiguration,
    ) -> Result<bool, BatchError> {
        use crate::db::entities::{
            ChoiceOption, ComposerContextId, DecisionQuestion, DecisionRequest,
            FrozenDecisionConfig, JobQueuePriority, MutableContextSelection, MutableJob,
            MutableJobContent, MutableJobInput, MutableJobPhase,
        };
        let FrozenConfiguration::Generative(generative) = frozen else {
            return Ok(false);
        };
        let Some((project_id, project, policy)) =
            self.planning_policy_for(parent, job, generative)?
        else {
            return Ok(false);
        };
        let frozen_decision = match policy.decision_profile {
            Some(profile) => match self.freeze_decision_profile(profile) {
                Err(BatchError(Failure::Rejected(reason))) => {
                    log::warn!(
                        "project planning for {project_id} fell back to the turn configuration: {reason}"
                    );
                    FrozenDecisionConfig::Generative(generative.clone(), None)
                }
                result => result?,
            },
            None => FrozenDecisionConfig::Generative(generative.clone(), None),
        };
        let wrapped = FrozenConfiguration::Decision(frozen_decision.clone());
        let storable = self.storable_frozen(&wrapped)?;
        let task = self.planning_task_context(job)?;
        let request = DecisionRequest {
            question: DecisionQuestion::Choice {
                prompt: format!(
                    "Decide whether the current request can run directly or requires structured job_plan planning before side-effecting tools. Interpret follow-ups such as continue in the context of the original task and previous request.\n\n{task}"
                ),
                options: vec![
                    ChoiceOption {
                        id: "direct".to_owned(),
                        description: "Simple single-step task; no job_plan required.".to_owned(),
                    },
                    ChoiceOption {
                        id: "phased".to_owned(),
                        description:
                            "Multi-step task; require job_plan before side-effecting tools."
                                .to_owned(),
                    },
                    ChoiceOption {
                        id: "delegated".to_owned(),
                        description:
                            "Complex task; require job_plan delegation before side-effecting tools."
                                .to_owned(),
                    },
                ],
            },
            state: serde_json::json!({
                "kind": "project_planning_decision",
                "target_job": job.id.get().to_string(),
                "project": project_id.get().to_string(),
                "decision_revision": project.decision_revision,
            }),
        };
        let dec_id = JobId::new(self.next_id()?);
        let dec_job = MutableJob {
            id: dec_id,
            parent_id: job.parent_id,
            previous: None,
            submitted_by: crate::db::entities::Actor::Auto,
            retry: None,
            composer_context: ComposerContextId::new(self.next_id()?),
            created_at: self.now,
            submitted_at: Some(self.now),
            revision: 1,
            phase: MutableJobPhase::Queued,
            input: MutableJobInput {
                priority: JobQueuePriority::Foreground,
                content: MutableJobContent::Decision {
                    profile: frozen_decision.profile_ref().id,
                    references: frozen_decision.references().collect(),
                    request,
                },
            },
            context_selection: MutableContextSelection::FullHistory,
            summary_source: None,
            frozen: Some(storable),
        };
        let req_tag = mutable_configuration_requirement(&wrapped);
        self.enqueue_mutable_job(&dec_job, req_tag, TurnMembership::Turn, false, true)?;
        self.step(|tx| {
            tx.open_table(DEPENDENCY_WAITERS)?.insert(
                (dec_id.get(), job.id.get()),
                DependencyCondition::Success.code(),
            )?;
            Ok(())
        })?;
        self.record_execution_event(
            job.id,
            ExecutionEventKind::DependencyAdded {
                prerequisite: dec_id,
                condition: DependencyCondition::Success,
            },
        )?;
        Ok(true)
    }

    pub(super) fn require_planning_decision_satisfied(
        &mut self,
        call: ToolCallRef,
    ) -> Result<(), BatchError> {
        use crate::db::entities::{
            DecisionAnswer, DecisionOutcome, DecisionResult, MutableFrozenConfiguration,
            MutableJobContent,
        };
        let Some(job) = self.mutable_job(call.job)? else {
            return Ok(());
        };
        let Some(MutableFrozenConfiguration::Generative(generative)) = &job.frozen else {
            return Ok(());
        };
        let parent = self.require_mutable_conversation(job.parent_id)?;
        let Some((_, _, policy)) = self.planning_policy_for(&parent, &job, generative)? else {
            return Ok(());
        };
        let target_str = call.job.get().to_string();
        let dec_job_id = self.step(|tx| {
            let children = tx.open_table(CHILDREN)?;
            let jobs = tx.open_table(JOBS)?;
            for entry in
                children.range((job.parent_id.get(), 0)..=(job.parent_id.get(), u128::MAX))?
            {
                let (key, _) = entry?;
                let (_, child_id) = key.value();
                let Some(row) = jobs.get(child_id)? else {
                    continue;
                };
                let child = decode_mutable_job(child_id, row.value())?;
                if let MutableJobContent::Decision { request, .. } = &child.input.content
                    && request.state.get("kind").and_then(|v| v.as_str())
                        == Some("project_planning_decision")
                    && request.state.get("target_job").and_then(|v| v.as_str())
                        == Some(target_str.as_str())
                {
                    return Ok(Some(JobId::new(child_id)));
                }
            }
            Ok(None)
        })?;
        let Some(dec_job_id) = dec_job_id else {
            return self.reject(Violation::InvalidValue(
                "job_plan",
                "planning decision missing; side-effecting tool refused",
            ));
        };
        let dec_state = self.require_job_state(dec_job_id)?;
        if dec_state.outcome.as_ref().map(|o| o.kind) != Some(OutcomeKind::Completed) {
            return self.reject(Violation::InvalidValue(
                "job_plan",
                "planning decision failed or uncertain; side-effecting tool refused",
            ));
        }
        let decision_result: Option<DecisionResult> = self.step(|tx| {
            let outputs = tx.open_table(JOB_OUTPUTS)?;
            for entry in outputs.range((dec_job_id.get(), 0)..=(dec_job_id.get(), u32::MAX))? {
                let (key, value) = entry?;
                let out = decode_output(key.value(), value.value())?;
                if out.kind == OutputKind::Result
                    && let Some(val) = out.value.as_ref().and_then(|v| v.get("decision"))
                {
                    return Ok(serde_json::from_value::<DecisionResult>(val.clone()).ok());
                }
            }
            Ok(None)
        })?;
        let Some(DecisionResult {
            outcome:
                DecisionOutcome::Answer {
                    answer:
                        DecisionAnswer::Choice {
                            selected,
                            confidence,
                            ..
                        },
                },
            ..
        }) = decision_result
        else {
            return self.reject(Violation::InvalidValue(
                "job_plan",
                "planning decision outcome unusable; side-effecting tool refused",
            ));
        };
        let has_plan = self.previous_tool_evidence(call)?.iter().any(|prev| {
            prev.name == "job_plan"
                && matches!(prev.decision.fact, ToolDecision::Admitted(_))
                && !prev.outcome.as_ref().is_some_and(|o| {
                    matches!(
                        o.fact,
                        ToolOutcome::NotApplied { .. } | ToolOutcome::NotInvoked { .. }
                    )
                })
        });
        if has_plan {
            return Ok(());
        }
        if let Some(conf) = confidence
            && (conf.value * 1000.0) < f64::from(policy.minimum_confidence_millis)
        {
            return self.reject(Violation::PlanRequiredLowConfidence);
        }
        if selected == "direct" {
            return Ok(());
        }
        self.reject(Violation::PlanRequired)
    }

    fn advance_recurrence_on_settlement(
        &mut self,
        job: &crate::db::entities::MutableJob,
        kind: OutcomeKind,
    ) -> Result<(), BatchError> {
        use crate::db::entities::{Recurrence, RecurrenceHalt, RecurrencePhase};
        let Some(mut rec) = self.get(RECURRENCES, job.parent_id.get(), Recurrence::decode)? else {
            return Ok(());
        };
        if rec.phase != RecurrencePhase::Active || rec.current_turn != job.id {
            return Ok(());
        }
        if kind == OutcomeKind::Completed {
            let lifecycle = self.job_lifecycle(job.parent_id)?;
            let parent_settled = self.require_job_state(job.parent_id)?.outcome.is_some();
            if lifecycle.closing.is_some() || lifecycle.hidden_at.is_some() || parent_settled {
                rec.phase = RecurrencePhase::Halted {
                    reason: RecurrenceHalt::Closed,
                };
            } else {
                let (limits, _) = self.group_limits(job.parent_id)?;
                if rec.n >= u64::from(limits.max_iterations) {
                    rec.phase = RecurrencePhase::Halted {
                        reason: RecurrenceHalt::LimitReached,
                    };
                } else {
                    let due = self
                        .now
                        .as_unix_micros()
                        .saturating_add(i64::from(rec.interval_s).saturating_mul(1_000_000));
                    rec.phase = RecurrencePhase::WaitingDue { due };
                    self.step(|tx| {
                        tx.open_table(RECURRENCE_DUE)?
                            .insert((due, job.parent_id.get()), ())?;
                        Ok(())
                    })?;
                }
            }
        } else {
            let reason = if kind == OutcomeKind::Cancelled {
                RecurrenceHalt::Closed
            } else {
                RecurrenceHalt::Refused
            };
            rec.phase = RecurrencePhase::Halted { reason };
        }
        self.insert(RECURRENCES, job.parent_id.get(), rec.encode().as_slice())
    }

    pub(crate) fn claim_due_recurrences(
        &mut self,
        limit: usize,
    ) -> Result<ControlFlow<Option<Duration>, Option<Duration>>, BatchError> {
        use crate::db::entities::{Recurrence, RecurrenceHalt, RecurrencePhase};
        let now_micros = self.now.as_unix_micros();
        let due_entries = self.step(|tx| {
            let table = tx.open_table(RECURRENCE_DUE)?;
            let mut due = Vec::new();
            for entry in table.range((i64::MIN, 0)..=(now_micros, u128::MAX))? {
                if due.len() >= limit {
                    break;
                }
                let (key, _) = entry?;
                due.push(key.value());
            }
            Ok(due)
        })?;
        let changed = !due_entries.is_empty();
        for (due_micros, conv_u128) in due_entries {
            self.step(|tx| {
                tx.open_table(RECURRENCE_DUE)?
                    .remove((due_micros, conv_u128))?;
                Ok(())
            })?;
            let Some(mut rec) = self.get(RECURRENCES, conv_u128, Recurrence::decode)? else {
                continue;
            };
            if rec.phase != (RecurrencePhase::WaitingDue { due: due_micros }) {
                continue;
            }
            let conv = JobId::new(conv_u128);
            let lifecycle = self.job_lifecycle(conv)?;
            let parent_settled = self.require_job_state(conv)?.outcome.is_some();
            if lifecycle.closing.is_some() || lifecycle.hidden_at.is_some() || parent_settled {
                rec.phase = RecurrencePhase::Halted {
                    reason: RecurrenceHalt::Closed,
                };
                self.insert(RECURRENCES, conv_u128, rec.encode().as_slice())?;
                continue;
            }
            let (limits, _) = self.group_limits(conv)?;
            if rec.n >= u64::from(limits.max_iterations) {
                rec.phase = RecurrencePhase::Halted {
                    reason: RecurrenceHalt::LimitReached,
                };
                self.insert(RECURRENCES, conv_u128, rec.encode().as_slice())?;
                continue;
            }
            let Some(prev_turn) = self.mutable_job(rec.current_turn)? else {
                rec.phase = RecurrencePhase::Halted {
                    reason: RecurrenceHalt::ConfigUnavailable,
                };
                self.insert(RECURRENCES, conv_u128, rec.encode().as_slice())?;
                continue;
            };
            let objective = prev_turn.input.text().to_owned();
            let priority = prev_turn.input.priority;
            let cfg = match prev_turn.input.configuration() {
                Some(cfg) => cfg,
                None => {
                    rec.phase = RecurrencePhase::Halted {
                        reason: RecurrenceHalt::ConfigUnavailable,
                    };
                    self.insert(RECURRENCES, conv_u128, rec.encode().as_slice())?;
                    continue;
                }
            };
            let next_n = rec.n.saturating_add(1);
            match self.host_turn(
                conv,
                super::job_tree::host_input(objective, cfg, priority),
                recurrence_submission(conv_u128, next_n, b"recurrence-turn-create"),
                recurrence_submission(conv_u128, next_n, b"recurrence-turn-submit"),
                crate::db::entities::Actor::Auto,
            ) {
                Ok(next_turn) => {
                    rec.phase = RecurrencePhase::Active;
                    rec.n = next_n;
                    rec.previous_turn = Some(rec.current_turn);
                    rec.current_turn = next_turn;
                    self.insert(RECURRENCES, conv_u128, rec.encode().as_slice())?;
                }
                Err(_) => {
                    rec.phase = RecurrencePhase::Halted {
                        reason: RecurrenceHalt::ConfigUnavailable,
                    };
                    self.insert(RECURRENCES, conv_u128, rec.encode().as_slice())?;
                }
            }
        }
        let next = self.step(|tx| recurrence_delay(tx, now_micros))?;
        Ok(if changed {
            ControlFlow::Continue(next)
        } else {
            ControlFlow::Break(next)
        })
    }

    pub(crate) fn resume_recurrence(&mut self, conversation: JobId) -> Result<(), BatchError> {
        use crate::db::entities::{Recurrence, RecurrencePhase};
        let Some(mut rec) = self.get(RECURRENCES, conversation.get(), Recurrence::decode)? else {
            return self.reject(Violation::NotFound(RecordRef::Job(conversation)));
        };
        if !matches!(rec.phase, RecurrencePhase::Halted { .. }) {
            return Ok(());
        }
        let lifecycle = self.job_lifecycle(conversation)?;
        let parent_settled = self.require_job_state(conversation)?.outcome.is_some();
        if lifecycle.closing.is_some() || lifecycle.hidden_at.is_some() || parent_settled {
            return self.reject(Violation::InvalidValue(
                "recurrence",
                "cannot resume a closed recurrence",
            ));
        }
        let (limits, _) = self.group_limits(conversation)?;
        if rec.n >= u64::from(limits.max_iterations) {
            return self.reject(Violation::InvalidValue(
                "recurrence",
                "recurrence iteration limit reached",
            ));
        }
        let due = self.now.as_unix_micros();
        rec.phase = RecurrencePhase::WaitingDue { due };
        self.step(|tx| {
            tx.open_table(RECURRENCE_DUE)?
                .insert((due, conversation.get()), ())?;
            Ok(())
        })?;
        self.insert(RECURRENCES, conversation.get(), rec.encode().as_slice())?;
        self.wake_scheduler();
        Ok(())
    }

    pub(crate) fn fail_unstarted_reservation(
        &mut self,
        token: crate::db::entities::AllocationToken,
        cause: crate::db::entities::UnstartedCause,
    ) -> Result<AppendedOutput, BatchError> {
        let (job, output) = self.end_untouched_reservation(
            token,
            cause.outcome(),
            crate::db::entities::FinalResult {
                summary: Some(cause.summary().to_owned()),
                code: Some(cause.code().to_owned()),
                goal_reached: Some(false),
                value: None,
            },
        )?;
        // Offline reservation publication verifies a target-specific delta.
        // The Chunk Group is checked at startup after the result is published.
        if cause != crate::db::entities::UnstartedCause::OfflineReconciled {
            self.settle_summary_membership(&job)?;
        }
        Ok(output)
    }
}
