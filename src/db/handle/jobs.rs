use crate::db::commands::{byte_count, compaction_bytes, debug_bytes};
use crate::db::entities::{
    Actor, Appended, ConfigId, ExecutionConfiguration, JobAction, JobEdit, JobId, JobOutputRecord,
    OperationKind, Submission, SubmissionKey, SubmissionRecord, ToolCallRef,
};
use crate::db::errors::DatabaseError;
use crate::db::worker::DatabaseHandle;
use crate::db::{entities, mutations, redb};
use std::sync::PoisonError;

impl DatabaseHandle {
    pub async fn conversation_children(
        &self,
        conversation: JobId,
        before: Option<JobId>,
        limit: usize,
    ) -> Result<Option<Vec<crate::db::entities::ChildConversationEntry>>, DatabaseError> {
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
    ) -> Result<Option<crate::db::entities::MutableConversationPage>, DatabaseError> {
        self.read(move |database| {
            redb::read_mutable_conversation_turn_page(database, conversation, before, limit)
        })
        .await
    }

    pub(crate) async fn mutable_job_details(
        &self,
        job: JobId,
        context: Option<entities::ComposerContextId>,
    ) -> Result<Option<crate::db::entities::MutableJobDetails>, DatabaseError> {
        self.read(move |database| redb::read_mutable_job_details(database, job, context))
            .await
    }

    pub(crate) async fn native_job(
        &self,
        job: JobId,
    ) -> Result<Option<crate::db::entities::NativeJobItem>, DatabaseError> {
        self.read(move |database| redb::read_native_job(database, job))
            .await
    }

    pub(crate) async fn search_jobs(
        &self,
        terms: Vec<String>,
        before: Option<JobId>,
        limit: usize,
    ) -> Result<crate::db::entities::JobSearchPage, DatabaseError> {
        self.read(move |database| redb::read_job_search(database, &terms, before, limit))
            .await
    }

    pub async fn conversations(
        &self,
        before: Option<JobId>,
        limit: usize,
        archived: bool,
    ) -> Result<crate::db::entities::ConversationsPage, DatabaseError> {
        self.read(move |database| redb::read_conversations(database, before, limit, archived))
            .await
    }

    pub async fn search_conversations(
        &self,
        terms: Vec<String>,
        before: Option<JobId>,
        limit: usize,
    ) -> Result<crate::db::entities::ConversationsPage, DatabaseError> {
        self.read(move |database| redb::read_conversation_search(database, &terms, before, limit))
            .await
    }

    pub async fn job_tree(
        &self,
        job: JobId,
        after: Option<JobId>,
        limit: usize,
        depth: u8,
    ) -> Result<Option<crate::db::entities::JobTree>, DatabaseError> {
        self.read(move |database| redb::read_job_tree(database, job, after, limit, depth))
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

    pub(crate) async fn job_outputs_with_pieces(
        &self,
        job: JobId,
        after_seq: Option<i64>,
        limit: usize,
        pieces: redb::PieceReader,
    ) -> Result<(Vec<JobOutputRecord>, redb::PieceReader), DatabaseError> {
        self.read(move |database| {
            redb::read_outputs_with_pieces(database, job, after_seq, limit, pieces)
        })
        .await
    }

    pub async fn latest_output(
        &self,
        job: JobId,
    ) -> Result<Option<JobOutputRecord>, DatabaseError> {
        self.read(move |database| redb::read_latest_output(database, job))
            .await
    }

    pub async fn submission(
        &self,
        key: SubmissionKey,
    ) -> Result<Option<SubmissionRecord>, DatabaseError> {
        self.read(move |database| redb::read_submission(database, key))
            .await
    }

    pub async fn conversation_cards(
        &self,
        conversation: JobId,
    ) -> Result<Option<Vec<serde_json::Value>>, DatabaseError> {
        self.read(move |database| redb::read_cards(database, conversation))
            .await
    }

    pub(crate) async fn context_state(
        &self,
        conversation: JobId,
        before: Option<JobId>,
        limit: usize,
    ) -> Result<Option<crate::db::entities::ContextState>, DatabaseError> {
        self.read(move |database| redb::read_context_state(database, conversation, before, limit))
            .await
    }

    pub(crate) async fn execution_history(
        &self,
        job: entities::JobId,
        after: Option<u64>,
        limit: usize,
        context: Option<entities::ComposerContextId>,
    ) -> Result<Option<crate::db::entities::ExecutionHistoryPage>, DatabaseError> {
        self.read(move |database| {
            redb::read_execution_history(database, job, after, limit, context)
        })
        .await
    }

    pub(crate) async fn execution_snapshot(
        &self,
        job: entities::JobId,
        at_seq: Option<u64>,
        context: Option<entities::ComposerContextId>,
    ) -> Result<Option<entities::ExecutionSnapshot>, DatabaseError> {
        self.read(move |database| redb::read_execution_snapshot(database, job, at_seq, context))
            .await
    }

    pub(crate) async fn output_job_is_decision(
        &self,
        job: JobId,
    ) -> Result<Option<bool>, DatabaseError> {
        self.read(move |database| redb::read_output_job_is_decision(database, job))
            .await
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

    pub(crate) async fn mutable_job_for_context(
        &self,
        context: entities::ComposerContextId,
    ) -> Result<Option<entities::MutableJobRecord>, DatabaseError> {
        self.write(0, OperationKind::Open, context, |batch, context| {
            batch.mutable_job_for_context(context)
        })
        .await
    }

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
                let job = match input {
                    Some(input) => {
                        batch.submit_mutable_job_with_input(id, revision, input, submission)
                    }
                    None => batch.submit_mutable_job(id, revision, submission),
                }?;
                batch.flush_child_reports(job.parent_id)?;
                batch.read_mutable_job(id)?.ok_or_else(|| {
                    redb::BatchError(redb::Failure::Storage(redb::StorageError::corrupted(
                        "submitted mutable Job is missing",
                    )))
                })
            },
        )
        .await
    }

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

    pub(crate) async fn cancel_queued_job(
        &self,
        context: entities::ComposerContextId,
        id: JobId,
        revision: u64,
        unsupported_only: bool,
    ) -> Result<entities::MutableJobRecord, DatabaseError> {
        self.write(
            8192,
            OperationKind::StopTurn,
            (context, id, revision, unsupported_only),
            |batch, (context, id, revision, unsupported_only)| {
                batch.require_mutable_job_context(context, id)?;
                batch.cancel_queued_job(id, revision, unsupported_only)?;
                if !unsupported_only {
                    batch.settle_child_turns(&[id])?;
                }
                batch.read_mutable_job(id)?.ok_or_else(|| {
                    redb::BatchError(redb::Failure::Storage(redb::StorageError::corrupted(
                        "cancelled Job is missing",
                    )))
                })
            },
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

    pub(crate) async fn submit_decision(
        &self,
        name: Option<String>,
        configuration: ExecutionConfiguration,
        request: entities::DecisionRequest,
        submission: Option<Submission>,
    ) -> Result<(JobId, Appended), DatabaseError> {
        let capabilities = self.shared.native_resources.clone();
        self.write_sized(
            (name, configuration, request, submission),
            |(_, _, request, _)| debug_bytes(request),
            OperationKind::Open,
            move |batch, (name, configuration, request, submission)| {
                batch.submit_native_decision(
                    name,
                    configuration,
                    request,
                    submission,
                    |operation, protocol| {
                        capabilities
                            .read()
                            .unwrap_or_else(PoisonError::into_inner)
                            .has_unarchived_capability(operation, protocol)
                    },
                )
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

    pub(crate) async fn settle_unreported_children(&self) -> Result<usize, DatabaseError> {
        log::debug!("starting pending child report sweep");
        let mut cursor = None;
        let mut reported = 0;
        loop {
            let page = self
                .read(move |db| redb::read_child_report_page(db, cursor))
                .await?;
            if let Some((parent, child)) = page.candidate {
                reported += self.retry_child_report(parent, child).await?;
            }
            cursor = page.next;
            if cursor.is_none() {
                return Ok(reported);
            }
        }
    }

    async fn retry_child_report(
        &self,
        parent: JobId,
        child: JobId,
    ) -> Result<usize, DatabaseError> {
        use crate::db::commands::Command;
        use std::ops::ControlFlow::{Break, Continue};
        self.submit(|reply| {
            Command::custom(
                (parent, child),
                reply,
                |_| 64 * 1024,
                |(parent, child), reply, database, operation, commit| {
                    let (outcome, declined) =
                        redb::run_batch_declinable(database, operation.epoch, commit, |batch| {
                            let marked = batch.retry_child_report(parent, child)?;
                            Ok(if marked == 0 {
                                Break(0)
                            } else {
                                Continue(marked)
                            })
                        });
                    crate::db::worker::settle(
                        operation,
                        commit,
                        OperationKind::SettleChildTurns,
                        reply,
                        outcome,
                        declined,
                    )
                },
            )
        })
        .await
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

    pub async fn set_conversation(&self, root: JobId, edit: JobEdit) -> Result<(), DatabaseError> {
        self.write_sized(
            (root, edit),
            |(_, edit)| match edit {
                JobEdit::Title(title) => byte_count(title.len()),
                JobEdit::Hide | JobEdit::Unhide | JobEdit::Limits(_) => 16,
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
            // A fully settled conversation is only hidden, not closed, so it
            // stays reopenable after restore. Conversations with running or
            // unsettled work are closed (which stops them).
            if batch.unsettled_turns(root)?.is_empty()
                && batch.live_children(root)?.is_empty()
                && batch.unsettled_non_turn_jobs(root)?.is_empty()
            {
                return Ok(JobAction {
                    conversation: root,
                    record_seq: None,
                    running: Vec::new(),
                    ended: Vec::new(),
                    already: false,
                    settled: true,
                });
            }
            batch.close(root, Actor::User)
        })
        .await
    }
}
