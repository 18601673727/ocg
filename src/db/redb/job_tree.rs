use super::{StorageError, TxRead, approvals, jobs, reads};
use crate::db::entities::{
    Actor, Appended, ConfigId, ContentDigest, DependencyCondition, JobId, JobLimits, JobSpec,
    JobState, Origin, OutcomeKind, OutputKind, ProjectId, RecordRef, Recurrence, SubmissionKey,
};
use crate::db::entities::{
    AllocationPhase, AllocationToken, ApprovalDecision, ChildNoticeDrop, ChildNoticeKind,
    ComposerContextId, DependencyState, ExecutionConfiguration, JobAction, JobClosing,
    JobLifecycle, JobQueuePriority, JobToolCall, MutableFrozenConfiguration, MutableJob,
    MutableJobContent, MutableJobInput, NativeStopRequest, OutcomeCause, OutcomeRecord,
    RequestFingerprint, Submission, ToolCallRef, ToolEvidenceRecord, Violation,
};
use crate::db::redb::batch::{Batch, BatchError, Failure};
use crate::db::redb::schema::{
    CHILD_GROUPS, CHILDREN, CONVERSATIONS, DRAFT_CONTEXTS, JOB_OUTPUTS, JOBS, LIVE_CHILDREN,
    OPEN_APPROVALS, PENDING_CHILD_NOTICES, RECURRENCES, RETRY_JOBS, TOOL_CALLS, TURNS,
};
use crate::db::redb::{rows, state};
use ::redb::ReadableTable;
use reads::jobs::{read_conversation_row, turn_head};
use std::ops::ControlFlow;

#[derive(Default)]
pub(super) struct ChildTurnSettlement {
    head: bool,
    changed: bool,
}

fn settled_turn(tx: &impl TxRead, id: JobId) -> Result<Option<(JobSpec, OutcomeKind)>, BatchError> {
    let Some(turn) = jobs::mutable_job_in(tx, id)? else {
        return Ok(None);
    };
    let spec = read_conversation_row(tx, turn.parent_id)?
        .ok_or(Violation::NotFound(RecordRef::Job(turn.parent_id)))?;
    if spec.is_chunk_group() || turn.summary_source.is_some() {
        return Ok(None);
    }
    let runtime = state::read(tx, id)?.ok_or(Violation::NotFound(RecordRef::Job(id)))?;
    Ok(runtime.state.outcome.map(|outcome| (spec, outcome.kind)))
}

pub(crate) fn read_child_turns_need_settlement(
    database: &::redb::Database,
    turns: &[JobId],
) -> Result<bool, StorageError> {
    let tx = reads::read(database)?;
    for &id in turns {
        let spec = match settled_turn(&tx, id) {
            Ok(None) => continue,
            Ok(Some((spec, _))) => spec,
            Err(BatchError(Failure::Storage(error))) => return Err(error),
            // Let the writer return the violation against its authoritative snapshot.
            Err(BatchError(Failure::Rejected(_))) => return Ok(true),
        };
        if spec.parent_id.is_some() || state::require(&tx, spec.id)?.lifecycle.closing.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) const MAX_LIVE_CHILDREN: usize = 16;
pub(super) const MAX_LIVE_SUBTREE: usize = 64;
pub(super) const MAX_AGENT_RETRIES: usize = 2;
pub(super) const REPORT_HEADER: &str = "The conversations you opened have finished. Their answers follow; use them, and do not claim a failed or cancelled one completed.";
pub(super) const CHILD_PROGRESS_HEADER: &str = "Child task progress report";
pub(super) const CHILD_HELP_HEADER: &str = "Child task help request";
const REPORT_TITLE_BYTES: usize = 128;
const MAX_CHILD_SCAN: usize = 256;

pub(crate) const HELD_BY_USER: u32 = 1;
pub(crate) const HELD_BY_AGENT: u32 = 2;

pub(crate) fn tool_submission(call: ToolCallRef, purpose: &[u8], n: u32) -> Submission {
    let mut identity = b"ocg native job tool ".to_vec();
    identity.extend_from_slice(purpose);
    identity.extend_from_slice(&call.job.get().to_le_bytes());
    identity.extend_from_slice(&call.request.get().to_le_bytes());
    identity.extend_from_slice(&call.index.to_le_bytes());
    identity.extend_from_slice(&n.to_le_bytes());
    let digest = *ContentDigest::of(&identity).as_bytes();
    Submission {
        key: SubmissionKey::from_digest(digest),
        fingerprint: RequestFingerprint::from_digest(digest),
    }
}

struct ChildResult {
    child: JobId,
    head: JobId,
    outcome: OutcomeKind,
    title: String,
}

struct PendingChildNotice {
    child: JobId,
    call: ToolCallRef,
    kind: ChildNoticeKind,
    text: String,
}

enum ReportTurn {
    Skip,
    AtLimit,
    Permanent(Violation),
    WaitForConfiguration,
    InvalidInput(Violation),
    Post(MutableJobInput),
}

struct ChildReport {
    parent: JobId,
    call: ToolCallRef,
    results: Vec<ChildResult>,
    turn: ReportTurn,
}

pub(crate) struct ChildReportPage {
    pub(crate) next: Option<(u128, u128)>,
    pub(crate) candidate: Option<(JobId, JobId)>,
}

pub(super) fn host_input(
    text: String,
    configuration: ExecutionConfiguration,
    priority: JobQueuePriority,
) -> MutableJobInput {
    MutableJobInput {
        priority,
        content: MutableJobContent::Generative {
            text,
            attachments: Vec::new(),
            configuration: Some(configuration),
        },
    }
}

impl Batch {
    pub(crate) fn root_group(&mut self, mut root: JobId) -> Result<JobId, BatchError> {
        for _ in 0..=crate::db::entities::MAX_JOB_DEPTH {
            match self.require_job(root)?.parent_id {
                Some(parent) => root = parent,
                None => return Ok(root),
            }
        }
        self.corrupted("conversation ancestry exceeds the hard depth limit".into())
    }

    pub(crate) fn live_subtree(&mut self, root: JobId) -> Result<Vec<JobId>, BatchError> {
        let mut result = vec![root];
        let mut i = 0;
        while i < result.len() {
            let children = self.live_children(result[i])?;
            result.extend(children);
            i += 1;
            if result.len() > 4 * MAX_LIVE_SUBTREE {
                return self.corrupted("conversation tree exceeds its live limit".into());
            }
        }
        Ok(result)
    }

    pub(crate) fn group_limits(
        &mut self,
        conversation: JobId,
    ) -> Result<(JobLimits, u8), BatchError> {
        let mut current = conversation;
        let mut effective = JobLimits::CEILING;
        for depth in 0..=crate::db::entities::MAX_JOB_DEPTH {
            let row = self.require_mutable_conversation(current)?;
            let own = self.job_lifecycle(current)?.limits;
            if let Some(limits) = own {
                effective = effective.intersect(limits);
            }
            match row.parent_id {
                Some(parent) => current = parent,
                None => return Ok((effective.intersect(own.unwrap_or_default()), depth)),
            }
        }
        self.corrupted("conversation ancestry exceeds the hard depth limit".into())
    }

    pub(crate) fn set_job_limits(
        &mut self,
        conversation: JobId,
        limits: JobLimits,
        by: Actor,
    ) -> Result<(), BatchError> {
        let row = self.require_mutable_conversation(conversation)?;
        let ceiling = match row.parent_id {
            Some(parent) => self.group_limits(parent)?.0,
            None => JobLimits::CEILING,
        };
        if !limits.within(ceiling) {
            return self.reject(Violation::InvalidValue(
                "limits",
                "limits exceed an ancestor or system ceiling",
            ));
        }
        if let Actor::Agent {
            conversation: caller,
        } = by
        {
            if caller != conversation {
                self.tool_rights(caller, conversation)?;
            }
            if !limits.within(self.group_limits(conversation)?.0) {
                return self.reject(Violation::InvalidValue(
                    "limits",
                    "an Agent may only tighten the effective limits",
                ));
            }
        }
        let mut lifecycle = self.job_lifecycle(conversation)?;
        lifecycle.limits = Some(limits);
        self.put_job_lifecycle(conversation, &lifecycle)?;
        // Descendants inherit tighter limits from their ancestors.
        for parent in self.live_subtree(conversation)? {
            self.flush_child_reports(parent)?;
        }
        Ok(())
    }

    pub(crate) fn unsettled_turns(
        &mut self,
        conversation: JobId,
    ) -> Result<Vec<JobId>, BatchError> {
        let newest = self.step(|tx| {
            tx.open_table(TURNS)?
                .range((conversation.get(), 0)..=(conversation.get(), u128::MAX))?
                .rev()
                .take(MAX_CHILD_SCAN)
                .map(|entry| Ok(JobId::new(entry?.0.value().1)))
                .collect::<Result<Vec<_>, StorageError>>()
        })?;
        let mut open = Vec::new();
        for turn in newest {
            if self.require_job_state(turn)?.outcome.is_some() {
                break;
            }
            open.push(turn);
        }
        open.reverse();
        Ok(open)
    }

    pub(super) fn head_turn(
        &mut self,
        conversation: JobId,
    ) -> Result<Option<MutableJob>, BatchError> {
        match self.group_head(conversation)? {
            Some(head) => self.mutable_job(head),
            None => Ok(None),
        }
    }

    pub(super) fn conversation_configuration(
        &mut self,
        conversation: JobId,
    ) -> Result<(ExecutionConfiguration, JobQueuePriority), BatchError> {
        Ok(head_configuration(self.head_turn(conversation)?)?)
    }

    pub(super) fn host_turn(
        &mut self,
        conversation: JobId,
        input: MutableJobInput,
        create: Submission,
        submit: Submission,
        by: Actor,
    ) -> Result<JobId, BatchError> {
        let context = ComposerContextId::new(self.next_id()?);
        let id = self.create_job_as(context, input, create, Some(conversation), None, by)?;
        self.submit_mutable_job(id, 1, submit)?;
        self.wake_scheduler();
        Ok(id)
    }

    pub(super) fn agent_turn(
        &mut self,
        conversation: JobId,
        text: String,
        call: ToolCallRef,
        purpose: &[u8],
        caller: JobId,
    ) -> Result<JobId, BatchError> {
        let (configuration, priority) = self.conversation_configuration(conversation)?;
        self.host_turn(
            conversation,
            host_input(text, configuration, priority),
            tool_submission(call, purpose, 0),
            tool_submission(call, purpose, 1),
            Actor::Agent {
                conversation: caller,
            },
        )
    }

    pub(super) fn prepare_child_notice(
        &mut self,
        child: JobId,
        call: ToolCallRef,
        text: &str,
        kind: ChildNoticeKind,
    ) -> Result<(JobId, Option<MutableJobInput>), BatchError> {
        let child_row = self.require_mutable_conversation(child)?;
        let Some(parent) = child_row.parent_id else {
            return self.reject(Violation::InvalidValue(
                "parent",
                "only a child conversation may contact its immediate parent",
            ));
        };
        let text = text.trim();
        if text.is_empty() || text.chars().count() > 2000 {
            return self.reject(Violation::InvalidValue(
                "text",
                "expected between 1 and 2000 characters",
            ));
        }
        let message = child_notice_message(child, call, text, kind);
        let input = self.inspect(|tx| prepare_child_notice_in(tx, parent, message))?;
        Ok((parent, input))
    }

    pub(super) fn post_child_notice(
        &mut self,
        child: JobId,
        call: ToolCallRef,
        text: &str,
    ) -> Result<(JobId, Option<JobId>), BatchError> {
        let (parent, input) =
            self.prepare_child_notice(child, call, text, ChildNoticeKind::Progress)?;
        let turn = if let Some(input) = input {
            Some(self.deliver_child_notice(parent, child, call, input)?)
        } else {
            self.insert(PENDING_CHILD_NOTICES, approvals::open_key(parent, call), ())?;
            None
        };
        Ok((parent, turn))
    }

    fn deliver_child_notice(
        &mut self,
        parent: JobId,
        child: JobId,
        call: ToolCallRef,
        input: MutableJobInput,
    ) -> Result<JobId, BatchError> {
        self.host_turn(
            parent,
            input,
            tool_submission(call, b"child notice", 0),
            tool_submission(call, b"child notice", 1),
            Actor::Agent {
                conversation: child,
            },
        )
    }

    fn retry_child_notice(
        &mut self,
        parent: JobId,
        notice: PendingChildNotice,
    ) -> Result<(), BatchError> {
        let PendingChildNotice {
            child,
            call,
            kind,
            text,
        } = notice;
        if self.require_mutable_conversation(child)?.parent_id != Some(parent) {
            return self.corrupted("pending notice has the wrong parent".into());
        }
        match self.prepare_child_notice(child, call, &text, kind) {
            Ok((_, Some(input))) => {
                self.deliver_child_notice(parent, child, call, input)?;
                self.remove(PENDING_CHILD_NOTICES, approvals::open_key(parent, call))?;
            }
            Ok((_, None)) => {}
            Err(BatchError(Failure::Rejected(Violation::InvalidValue(
                "parent_notice_limit",
                _,
            )))) => {
                self.drop_pending_child_notice(parent, call, ChildNoticeDrop::LimitReached)?;
            }
            Err(BatchError(Failure::Rejected(Violation::ParentClosed(_)))) => {
                self.drop_pending_child_notice(parent, call, ChildNoticeDrop::ParentClosed)?;
            }
            Err(BatchError(Failure::Rejected(reason))) => {
                log::debug!("pending child notice {call} is waiting for delivery: {reason}");
            }
            Err(error @ BatchError(Failure::Storage(_))) => return Err(error),
        }
        Ok(())
    }

    pub(super) fn drop_pending_child_notice(
        &mut self,
        parent: JobId,
        call: ToolCallRef,
        reason: ChildNoticeDrop,
    ) -> Result<(), BatchError> {
        let removed = self.step(|tx| {
            Ok(tx
                .open_table(PENDING_CHILD_NOTICES)?
                .remove(approvals::open_key(parent, call))?
                .is_some())
        })?;
        if removed {
            let Some(mut record) = self.tool_evidence(call)? else {
                return self.corrupted("dropped child notice has no tool call".into());
            };
            if record.child_notice_drop.replace(reason).is_some() {
                return self.corrupted("child notice was already dropped".into());
            }
            self.put_tool_evidence(&record)?;
        }
        Ok(())
    }

    pub(super) fn settle_child_help(
        &mut self,
        child: JobId,
        turn: JobId,
        awaiting_answer: bool,
    ) -> Result<(), BatchError> {
        if let Some((call, text)) = self.step(|tx| approvals::open_child_help(tx, child, turn))? {
            if !awaiting_answer {
                self.put_approval(call, ApprovalDecision::Voided, Actor::Auto)?;
                self.remove(OPEN_APPROVALS, approvals::open_key(child, call))?;
            } else if let Some(parent) = self.require_mutable_conversation(child)?.parent_id {
                self.insert(PENDING_CHILD_NOTICES, approvals::open_key(parent, call), ())?;
                self.retry_child_notice(
                    parent,
                    PendingChildNotice {
                        child,
                        call,
                        kind: ChildNoticeKind::Help,
                        text,
                    },
                )?;
            }
        }
        Ok(())
    }

    pub(super) fn expire_child_progress(&mut self, turn: JobId) -> Result<(), BatchError> {
        let Some(job) = self.mutable_job(turn)? else {
            return Ok(());
        };
        let Some(parent) = self.require_mutable_conversation(job.parent_id)?.parent_id else {
            return Ok(());
        };
        self.for_pending_child_notice(parent, Some(turn), |batch, call| {
            if pending_child_notice_kind(&batch.require_notice_evidence(call)?)?
                == ChildNoticeKind::Progress
            {
                batch.drop_pending_child_notice(parent, call, ChildNoticeDrop::ChildTurnSettled)?;
            }
            Ok(())
        })
    }

    pub(super) fn drop_notices_to_closed_parent(
        &mut self,
        parent: JobId,
    ) -> Result<(), BatchError> {
        self.for_pending_child_notice(parent, None, |batch, call| {
            batch.drop_pending_child_notice(parent, call, ChildNoticeDrop::ParentClosed)
        })
    }

    pub(crate) fn flush_child_notices(&mut self, parent: JobId) -> Result<(), BatchError> {
        if self.step(|tx| {
            Ok(pending_child_notices(tx, parent, None, None, 1)?.is_empty()
                || has_current_draft(tx, parent)?)
        })? {
            return Ok(());
        }
        for kind in [ChildNoticeKind::Help, ChildNoticeKind::Progress] {
            self.for_pending_child_notice(parent, None, |batch, call| {
                let record = batch.require_notice_evidence(call)?;
                if pending_child_notice_kind(&record)? == kind {
                    let child = batch.turn_conversation(call.job)?;
                    let text = stored_notice_text(&record)?;
                    batch.retry_child_notice(
                        parent,
                        PendingChildNotice {
                            child,
                            call,
                            kind,
                            text,
                        },
                    )?;
                }
                Ok(())
            })?;
        }
        Ok(())
    }

    fn require_notice_evidence(
        &mut self,
        call: ToolCallRef,
    ) -> Result<ToolEvidenceRecord, BatchError> {
        let Some(record) = self.tool_evidence(call)? else {
            return self.corrupted("pending child notice has no tool call".into());
        };
        Ok(record)
    }

    fn for_pending_child_notice(
        &mut self,
        parent: JobId,
        turn: Option<JobId>,
        mut apply: impl FnMut(&mut Self, ToolCallRef) -> Result<(), BatchError>,
    ) -> Result<(), BatchError> {
        let mut after = None;
        loop {
            let page =
                self.step(|tx| pending_child_notices(tx, parent, turn, after, MAX_CHILD_SCAN))?;
            let more = page.len() == MAX_CHILD_SCAN;
            for call in page {
                apply(self, call)?;
                after = Some(call);
            }
            if !more {
                return Ok(());
            }
        }
    }

    fn require_room_for_child(&mut self, parent: JobId) -> Result<(), BatchError> {
        let row = self.require_mutable_conversation(parent)?;
        let lifecycle = self.job_lifecycle(parent)?;
        if row.is_chunk_group()
            || row.is_standalone_decision_group()
            || self.require_job_state(parent)?.outcome.is_some()
            || lifecycle.closing.is_some()
            || lifecycle.hidden_at.is_some()
        {
            return self.reject(Violation::ParentClosed(parent));
        }
        let (limits, depth) = self.group_limits(parent)?;
        let root = self.root_group(parent)?;
        if depth.saturating_add(1) > limits.max_depth
            || self.live_children(parent)?.len() >= MAX_LIVE_CHILDREN
            || self.live_subtree(root)?.len() >= MAX_LIVE_SUBTREE
        {
            return self.reject(Violation::InvalidValue(
                "children",
                "live conversation limit reached",
            ));
        }
        Ok(())
    }

    pub(crate) fn open_child_conversation(
        &mut self,
        parent: JobId,
        call: ToolCallRef,
        agent: Option<ConfigId>,
        project: Option<ProjectId>,
        keep_open: bool,
        after: Vec<(JobId, DependencyCondition)>,
    ) -> Result<JobId, BatchError> {
        self.require_room_for_child(parent)?;
        if let Some(project) = project {
            self.require_project(project)?;
        }
        let id = JobId::new(self.next_id()?);
        let row = JobSpec {
            id,
            created_at: self.now,
            parent_id: Some(parent),
            agent,
            project,
            origin: Origin::Agent { call },
            keep_open,
        };
        self.insert(CONVERSATIONS, id.get(), row.encode().as_slice())?;
        self.put_job_state(id, &JobState::new(id))?;
        self.put_job_lifecycle(
            id,
            &JobLifecycle {
                after,
                ..JobLifecycle::default()
            },
        )?;
        self.step(|tx| state::set_pending(tx, id, Some((0, 0))))?;
        self.insert(CHILDREN, (parent.get(), id.get()), ())?;
        self.adjust_open_children(parent, 1)?;
        self.link_job_group(parent, id)?;
        Ok(id)
    }

    pub(crate) fn tool_rights(&mut self, caller: JobId, target: JobId) -> Result<(), BatchError> {
        let target_row = self.require_mutable_conversation(target)?;
        let mut parent = target_row.parent_id;
        let mut below = false;
        for _ in 0..=crate::db::entities::MAX_JOB_DEPTH {
            match parent {
                Some(id) if id == caller => {
                    below = true;
                    break;
                }
                Some(id) => parent = self.require_mutable_conversation(id)?.parent_id,
                None => break,
            }
        }
        if !below {
            return self.reject(Violation::InvalidValue(
                "rights",
                "the target must be in the caller's strict subtree",
            ));
        }
        for conversation in self.live_subtree(target)? {
            if self.step(|tx| has_user_turn(tx, conversation, true))? {
                return self.reject(Violation::InvalidValue(
                    "user_turn",
                    "a queued or running user turn in the target subtree prevents agent management",
                ));
            }
        }
        Ok(())
    }

    fn prerequisite_state(
        &mut self,
        prerequisite: JobId,
        condition: DependencyCondition,
    ) -> Result<DependencyState, BatchError> {
        if self
            .require_mutable_conversation(prerequisite)?
            .is_chunk_group()
        {
            return Ok(Batch::dependency_outcome_state(
                self.require_job_state(prerequisite)?.outcome.as_ref(),
                condition,
            ));
        }
        let Some(head) = self.group_head(prerequisite)? else {
            return Ok(DependencyState::Waiting);
        };
        Ok(Batch::dependency_outcome_state(
            self.require_job_state(head)?.outcome.as_ref(),
            condition,
        ))
    }

    pub(crate) fn mutable_job_blocked(&mut self, job: &MutableJob) -> Result<bool, BatchError> {
        let mut current = Some(job.parent_id);
        let mut own = true;
        let summary = self.is_summary_work(job)?;
        for _ in 0..=crate::db::entities::MAX_JOB_DEPTH {
            let Some(conversation) = current else {
                return Ok(false);
            };
            let lifecycle = self.job_lifecycle(conversation)?;
            if lifecycle.on_hold.is_some()
                || (summary
                    && (lifecycle.closing.is_some()
                        || self.require_job_state(conversation)?.outcome.is_some()))
            {
                return Ok(true);
            }
            if own {
                for (prerequisite, condition) in &lifecycle.after {
                    if self.prerequisite_state(*prerequisite, *condition)?
                        != DependencyState::Satisfied
                    {
                        return Ok(true);
                    }
                }
            }
            own = false;
            current = self.require_mutable_conversation(conversation)?.parent_id;
        }
        self.corrupted("conversation ancestry exceeds the hard depth limit".into())
    }

    pub(super) fn is_summary_work(&mut self, job: &MutableJob) -> Result<bool, BatchError> {
        Ok(job.summary_source.is_some()
            || self
                .require_mutable_conversation(job.parent_id)?
                .is_chunk_group())
    }

    fn stop_unsettled_turns(
        &mut self,
        conversation: JobId,
        cause: &OutcomeCause,
    ) -> Result<(Vec<AllocationToken>, Vec<JobId>), BatchError> {
        let spec = self.require_mutable_conversation(conversation)?;
        let chunk_group = spec.is_chunk_group();
        let mut jobs = if chunk_group {
            self.chunk_members(conversation)?
        } else {
            self.unsettled_turns(conversation)?
        };
        if !chunk_group {
            for id in self.unsettled_non_turn_jobs(conversation)? {
                let Some(job) = self.mutable_job(id)? else {
                    return self.corrupted("conversation child is not a native Job".into());
                };
                if matches!(cause, OutcomeCause::Closed) || job.summary_source.is_none() {
                    jobs.push(id);
                }
            }
        }
        let mut running = Vec::new();
        let mut ended = Vec::new();
        for turn in jobs {
            if self.require_job_state(turn)?.outcome.is_some() {
                continue;
            }
            let Some(allocation) = self
                .pool_allocation(turn)?
                .filter(|allocation| allocation.holds_capacity())
            else {
                let Some(job) = self.mutable_job(turn)? else {
                    return self.corrupted("turn is not a native Job".into());
                };
                self.end_queued_native_job(&job, cause.clone())?;
                ended.push(turn);
                continue;
            };
            match allocation.phase {
                AllocationPhase::Reserved
                | AllocationPhase::Running
                | AllocationPhase::StopRequested => match self.request_native_stop(turn)? {
                    NativeStopRequest::EndedBeforeStart { token, .. } => {
                        running.extend(token);
                        ended.push(turn);
                    }
                    NativeStopRequest::Requested(token)
                    | NativeStopRequest::AlreadyRequested(token) => running.push(token),
                },
                AllocationPhase::Uncertain | AllocationPhase::Released => {}
            }
        }
        Ok((running, ended))
    }

    pub(crate) fn stop_turn(
        &mut self,
        conversation: JobId,
        by: Actor,
    ) -> Result<JobAction, BatchError> {
        self.require_mutable_conversation(conversation)?;
        let cause = OutcomeCause::Text(format!("stopped by {}", actor_name(&by)));
        let (running, ended) = self.stop_unsettled_turns(conversation, &cause)?;
        self.settle_closing(conversation)?;
        Ok(JobAction {
            conversation,
            record_seq: None,
            running,
            ended,
            already: false,
            settled: false,
        })
    }

    pub(crate) fn close(
        &mut self,
        conversation: JobId,
        by: Actor,
    ) -> Result<JobAction, BatchError> {
        self.require_mutable_conversation(conversation)?;
        let already = self.job_lifecycle(conversation)?.closing.is_some();
        let subtree = self.live_subtree(conversation)?;
        for current in &subtree {
            let mut lifecycle = self.job_lifecycle(*current)?;
            if lifecycle.closing.is_none() {
                lifecycle.closing = Some(JobClosing {
                    at: self.now,
                    by: by.clone(),
                    running: false,
                });
                self.put_job_lifecycle(*current, &lifecycle)?;
                self.void_approvals(*current, by.clone())?;
            }
        }
        let mut running = Vec::new();
        let mut all_ended = Vec::new();
        for current in &subtree {
            let (tokens, never_ran) = self.stop_unsettled_turns(*current, &OutcomeCause::Closed)?;
            all_ended.extend(never_ran);
            if !tokens.is_empty() {
                let mut lifecycle = self.job_lifecycle(*current)?;
                if let Some(closing) = &mut lifecycle.closing {
                    closing.running = true;
                    self.put_job_lifecycle(*current, &lifecycle)?;
                }
            }
            running.extend(tokens);
        }
        for current in subtree.into_iter().rev() {
            let was_open = self.require_job_state(current)?.outcome.is_none();
            self.settle_closing(current)?;
            if was_open
                && self.require_job_state(current)?.outcome.is_some()
                && let Some(parent) = self.require_mutable_conversation(current)?.parent_id
            {
                self.report_settled_child(parent, current)?;
            }
        }
        Ok(JobAction {
            conversation,
            record_seq: None,
            running,
            ended: all_ended,
            already,
            settled: already && self.require_job_state(conversation)?.outcome.is_some(),
        })
    }

    fn settle_closing(&mut self, conversation: JobId) -> Result<bool, BatchError> {
        let lifecycle = self.job_lifecycle(conversation)?;
        let Some(closing) = lifecycle.closing else {
            return Ok(false);
        };
        let spec = self.require_mutable_conversation(conversation)?;
        if spec.is_chunk_group() {
            return self.settle_chunk_group(&spec);
        }
        if self.require_job_state(conversation)?.outcome.is_some()
            || !self.unsettled_turns(conversation)?.is_empty()
            || !self.live_children(conversation)?.is_empty()
            || !self.unsettled_non_turn_jobs(conversation)?.is_empty()
            || self.step(|tx| super::recovery::group_holds_capacity(tx, conversation))?
        {
            return Ok(false);
        }
        let head = self.head_turn(conversation)?;
        let answered = match head {
            Some(head) => self.require_job_state(head.id)?.outcome,
            None => None,
        };
        let kept = answered.filter(|outcome| {
            !closing.running
                && outcome.kind == OutcomeKind::Completed
                && outcome.cause != Some(OutcomeCause::Closed)
        });
        self.record_conversation_outcome(
            &spec,
            kept.unwrap_or(OutcomeRecord {
                kind: OutcomeKind::Cancelled,
                at: self.now,
                cause: Some(OutcomeCause::Closed),
            }),
        )?;
        if let Some(parent) = spec.parent_id {
            self.settle_closing(parent)?;
        }
        Ok(true)
    }

    fn record_conversation_outcome(
        &mut self,
        spec: &JobSpec,
        outcome: OutcomeRecord,
    ) -> Result<(), BatchError> {
        let conversation = spec.id;
        let mut state = self.require_job_state(conversation)?;
        state.outcome = Some(outcome);
        self.put_job_state(conversation, &state)?;
        if let Some(parent) = spec.parent_id {
            self.remove(LIVE_CHILDREN, (parent.get(), conversation.get()))?;
        }
        let released = self.step(|tx| {
            if matches!(state::pending(tx, conversation)?, Some((_, 0))) {
                state::set_pending(tx, conversation, None)?;
                Ok(true)
            } else {
                Ok(false)
            }
        })?;
        if released && let Some(parent) = spec.parent_id {
            self.adjust_open_children(parent, -1)?;
        }
        Ok(())
    }

    pub(super) fn settle_standalone_decision(
        &mut self,
        job: &MutableJob,
    ) -> Result<(), BatchError> {
        let spec = self.require_mutable_conversation(job.parent_id)?;
        if !spec.is_standalone_decision_group() {
            return Ok(());
        }
        if !matches!(
            job.input.content,
            crate::db::entities::MutableJobContent::Decision { .. }
        ) {
            return self.corrupted("standalone Decision Group contains a non-Decision Job".into());
        }
        if self.require_job_state(spec.id)?.outcome.is_some() {
            return Ok(());
        }
        let Some(outcome) = self.require_job_state(job.id)?.outcome else {
            return Ok(());
        };
        let counts = self.step(|tx| state::pending(tx, spec.id))?;
        let Some(counts) = counts else {
            return self.corrupted("standalone Decision Group unsettled row missing".into());
        };
        if outcome.kind == OutcomeKind::Unknown
            || counts != (0, 0)
            || self
                .pool_allocation(job.id)?
                .is_some_and(|allocation| allocation.holds_capacity())
            || !self.unsettled_turns(spec.id)?.is_empty()
            || !self.live_children(spec.id)?.is_empty()
            || !self.unsettled_non_turn_jobs(spec.id)?.is_empty()
        {
            return Ok(());
        }
        let mut lifecycle = self.job_lifecycle(spec.id)?;
        if lifecycle.closing.is_none() {
            lifecycle.closing = Some(JobClosing {
                at: outcome.at,
                by: Actor::Auto,
                running: false,
            });
            self.put_job_lifecycle(spec.id, &lifecycle)?;
        }
        self.record_conversation_outcome(&spec, outcome.clone())?;
        self.settle_native_dependents(spec.id, outcome.kind)?;
        self.wake_scheduler();
        Ok(())
    }

    fn chunk_members(&mut self, group: JobId) -> Result<Vec<JobId>, BatchError> {
        self.step(|tx| {
            tx.open_table(CHILDREN)?
                .range((group.get(), 0)..=(group.get(), u128::MAX))?
                .map(|entry| Ok(JobId::new(entry?.0.value().1)))
                .collect()
        })
    }

    pub(crate) fn unsettled_non_turn_jobs(
        &mut self,
        conversation: JobId,
    ) -> Result<Vec<JobId>, BatchError> {
        self.step(|tx| {
            let Some(counts) = state::pending(tx, conversation)? else {
                return Ok(Vec::new());
            };
            if counts.1 == 0 {
                return Ok(Vec::new());
            }
            let children = tx.open_table(CHILDREN)?;
            let jobs = tx.open_table(JOBS)?;
            let turns = tx.open_table(TURNS)?;
            let mut unsettled = Vec::new();
            for entry in
                children.range((conversation.get(), 0)..=(conversation.get(), u128::MAX))?
            {
                let id = entry?.0.value().1;
                if turns.get((conversation.get(), id))?.is_none()
                    && jobs.get(id)?.is_some()
                    && state::pending(tx, JobId::new(id))?.is_some()
                {
                    unsettled.push(JobId::new(id));
                }
            }
            Ok(unsettled)
        })
    }

    pub(super) fn settle_summary_membership(&mut self, job: &MutableJob) -> Result<(), BatchError> {
        let spec = self.require_mutable_conversation(job.parent_id)?;
        if spec.is_chunk_group() {
            self.settle_chunk_group(&spec)?;
        } else if !self.step(|tx| {
            Ok(tx
                .open_table(TURNS)?
                .get((spec.id.get(), job.id.get()))?
                .is_some())
        })? {
            if self.job_lifecycle(spec.id)?.closing.is_some() {
                self.settle_closing(spec.id)?;
            } else {
                self.auto_close_if_done(spec.id)?;
            }
        }
        Ok(())
    }

    fn settle_chunk_group(&mut self, spec: &JobSpec) -> Result<bool, BatchError> {
        let group = spec.id;
        if self.require_job_state(group)?.outcome.is_some() {
            return Ok(false);
        }
        // Chunk Jobs are parallel members in CHILDREN. They have no sequential head in TURNS.
        let counts = self.step(|tx| state::pending(tx, group))?;
        if counts != Some((0, 0)) {
            return Ok(false);
        }
        let Some(outcome) = self.chunk_group_outcome(group)? else {
            return Ok(false);
        };
        let mut lifecycle = self.job_lifecycle(group)?;
        if lifecycle.closing.is_none() {
            lifecycle.closing = Some(JobClosing {
                at: outcome.at,
                by: Actor::Auto,
                running: false,
            });
            self.put_job_lifecycle(group, &lifecycle)?;
        }
        self.record_conversation_outcome(spec, outcome)?;
        if let Some(parent) = spec.parent_id {
            self.end_impossible_dependents(parent, group)?;
            self.settle_closing(parent)?;
        }
        self.wake_scheduler();
        Ok(true)
    }

    fn chunk_group_outcome(&mut self, group: JobId) -> Result<Option<OutcomeRecord>, BatchError> {
        let mut aggregate: Option<OutcomeRecord> = None;
        for chunk in self.chunk_members(group)? {
            let Some(outcome) = self.require_job_state(chunk)?.outcome else {
                return Ok(None);
            };
            if outcome.kind == OutcomeKind::Unknown
                || self
                    .pool_allocation(chunk)?
                    .is_some_and(|allocation| allocation.holds_capacity())
            {
                return Ok(None);
            }
            aggregate = Some(match aggregate {
                None => outcome,
                Some(previous) => {
                    // A failed member determines the Group result even if others were cancelled.
                    let at = previous.at.max(outcome.at);
                    let worst = match (previous.kind, outcome.kind) {
                        (OutcomeKind::Completed, _)
                        | (OutcomeKind::Cancelled, OutcomeKind::Failed) => outcome,
                        _ => previous,
                    };
                    OutcomeRecord { at, ..worst }
                }
            });
        }
        if aggregate.is_none() {
            return self.corrupted("Chunk Group has no members".into());
        }
        Ok(aggregate)
    }

    pub(super) fn recover_chunk_group(&mut self, spec: &JobSpec) -> Result<(), BatchError> {
        let group = spec.id;
        let Some(parent) = spec.parent_id else {
            return self.corrupted("Chunk Group has no parent".into());
        };
        let state = self.require_job_state(group)?;
        let lifecycle = self.job_lifecycle(group)?;
        let unsettled = self.step(|tx| Ok(state::pending(tx, group)?.is_some()))?;
        if state.outcome.is_some() {
            if !unsettled {
                return Ok(());
            }
            return self.corrupted("settled Chunk Group still has unsettled members".into());
        }
        let parent_closed = self.job_lifecycle(parent)?.closing.is_some()
            || self.require_job_state(parent)?.outcome.is_some();
        if lifecycle.closing.is_some() || parent_closed {
            // Startup has already refused Held Allocations. Only queued work can be ended here.
            self.stop_unsettled_turns(group, &OutcomeCause::Closed)?;
        }
        self.settle_chunk_group(spec)?;
        Ok(())
    }

    pub(crate) fn hold(
        &mut self,
        conversation: JobId,
        by: Actor,
        unhold: bool,
    ) -> Result<JobAction, BatchError> {
        self.require_mutable_conversation(conversation)?;
        let mut lifecycle = self.job_lifecycle(conversation)?;
        if matches!(by, Actor::Agent { .. }) && lifecycle.on_hold == Some(HELD_BY_USER) {
            return self.reject(Violation::InvalidValue(
                "hold",
                "actor cannot lift this hold",
            ));
        }
        lifecycle.on_hold = (!unhold).then_some(match by {
            Actor::Agent { .. } => HELD_BY_AGENT,
            Actor::User | Actor::Auto | Actor::Loop => HELD_BY_USER,
        });
        lifecycle.loop_count = 0;
        self.put_job_lifecycle(conversation, &lifecycle)?;
        self.flush_child_reports(conversation)?;
        if unhold
            && matches!(by, Actor::User)
            && let Some(rec) = self.get(RECURRENCES, conversation.get(), Recurrence::decode)?
            && matches!(
                rec.phase,
                crate::db::entities::RecurrencePhase::Halted { .. }
            )
        {
            self.resume_recurrence(conversation)?;
        }
        self.wake_scheduler();
        Ok(JobAction {
            conversation,
            record_seq: None,
            running: Vec::new(),
            ended: Vec::new(),
            already: false,
            settled: false,
        })
    }

    pub(crate) fn retry(
        &mut self,
        conversation: JobId,
        turn: JobId,
        by: Actor,
    ) -> Result<Appended, BatchError> {
        self.require_mutable_conversation(conversation)?;
        let Some(original) = self.mutable_job(turn)? else {
            return self.reject(Violation::InvalidValue("turn", "turn not found"));
        };
        if original.parent_id != conversation || original.submitted_at.is_none() {
            return self.reject(Violation::InvalidValue(
                "turn",
                "not a turn of this conversation",
            ));
        }
        if original.summary_source.is_some()
            || self.group_head(conversation)? != Some(turn)
            || !self.step(|tx| {
                Ok(tx
                    .open_table(TURNS)?
                    .get((conversation.get(), turn.get()))?
                    .is_some())
            })?
        {
            return self.reject(Violation::InvalidValue(
                "turn",
                "only the latest regular turn can be retried",
            ));
        }
        if !self
            .require_job_state(turn)?
            .outcome
            .as_ref()
            .is_some_and(|outcome| {
                matches!(outcome.kind, OutcomeKind::Failed | OutcomeKind::Cancelled)
            })
        {
            return self.reject(Violation::InvalidValue(
                "turn",
                "only a failed or cancelled turn can be retried; uncertain work must be reconciled",
            ));
        }
        if self
            .pool_allocation(turn)?
            .is_some_and(|allocation| allocation.holds_capacity())
        {
            return self.reject(Violation::InvalidValue(
                "turn",
                "the original Job still holds execution capacity",
            ));
        }
        let may_have_applied_tool = self.step(|tx| {
            use crate::db::entities::{ToolEffect, ToolOutcome};
            let calls = tx.open_table(TOOL_CALLS)?;
            for entry in calls.range((turn.get(), 0, 0)..=(turn.get(), u32::MAX, u32::MAX))? {
                let (key, value) = entry?;
                let call =
                    rows::decode_tool_evidence(rows::tool_call_ref(key.value())?, value.value())?;
                if call
                    .admission()
                    .is_some_and(|admission| admission.effect != ToolEffect::ReadOnly)
                    && !call.outcome.as_ref().is_some_and(|outcome| {
                        matches!(
                            outcome.fact,
                            ToolOutcome::NotApplied { .. } | ToolOutcome::NotInvoked { .. }
                        )
                    })
                {
                    return Ok(true);
                }
            }
            Ok(false)
        })?;
        if may_have_applied_tool {
            return self.reject(Violation::InvalidValue(
                "retry",
                "this turn may have applied a tool effect; continue the conversation using its recorded evidence",
            ));
        }
        if !self.unsettled_turns(conversation)?.is_empty() {
            return self.reject(Violation::InvalidValue(
                "retry",
                "a waiting turn already follows",
            ));
        }
        let source = original.retry.as_ref().map_or(turn, |retry| retry.original);
        if original.retry.is_some() {
            let root = self.mutable_job(source)?.ok_or_else(|| {
                BatchError(Failure::Storage(StorageError::corrupted(
                    "retry source missing",
                )))
            })?;
            jobs::validate_retry_source(&original, &root)
                .map_err(|error| BatchError(Failure::Storage(error)))?;
        }
        if matches!(by, Actor::Agent { .. }) {
            let count = self.step(|tx| {
                let jobs = tx.open_table(JOBS)?;
                let mut count = 0;
                for entry in tx
                    .open_table(RETRY_JOBS)?
                    .range((source.get(), 0)..=(source.get(), u128::MAX))?
                {
                    let id = entry?.0.value().1;
                    let row = jobs
                        .get(id)?
                        .ok_or_else(|| StorageError::corrupted("retry Job missing"))?;
                    let job = jobs::decode_mutable_job(id, row.value())?;
                    let retry = job.retry.ok_or_else(|| {
                        StorageError::corrupted("retry index names an ordinary Job")
                    })?;
                    if retry.original != source || job.parent_id != conversation {
                        return Err(StorageError::corrupted("retry index names another source"));
                    }
                    if matches!(job.submitted_by, Actor::Agent { .. }) {
                        count += 1;
                        if count == MAX_AGENT_RETRIES {
                            break;
                        }
                    }
                }
                Ok(count)
            })?;
            if count >= MAX_AGENT_RETRIES {
                return self.reject(Violation::InvalidValue(
                    "retry_limit",
                    "at most two agent retries per original turn",
                ));
            }
        }
        let MutableJobContent::Generative {
            text,
            attachments,
            configuration,
        } = original.input.content
        else {
            return self.reject(Violation::InvalidValue(
                "turn",
                "only a Generative turn can be retried",
            ));
        };
        let context = ComposerContextId::new(self.next_id()?);
        let identity = |step: &[u8]| {
            let mut identity = b"ocg retry ".to_vec();
            identity.extend_from_slice(step);
            identity.extend_from_slice(&turn.get().to_le_bytes());
            identity.extend_from_slice(&context.get().to_le_bytes());
            let digest = *ContentDigest::of(&identity).as_bytes();
            Submission {
                key: SubmissionKey::from_digest(digest),
                fingerprint: RequestFingerprint::from_digest(digest),
            }
        };
        let input = MutableJobInput {
            priority: original.input.priority,
            content: MutableJobContent::Generative {
                text,
                attachments,
                configuration,
            },
        };
        let id = self.create_job_as(
            context,
            input,
            identity(b"create"),
            Some(conversation),
            None,
            by,
        )?;
        let mut job = self.mutable_job(id)?.ok_or_else(|| {
            BatchError(Failure::Storage(StorageError::corrupted(
                "new retry Job missing",
            )))
        })?;
        job.retry = Some(crate::db::entities::JobRetry { original: source });
        self.store_mutable_job(&job)?;
        self.submit_mutable_job(id, 1, identity(b"submit"))?;
        self.wake_scheduler();
        Ok(Appended {
            turn: id,
            seq: 1,
            joined: false,
        })
    }

    pub(super) fn child_turn_settled(
        &mut self,
        job: JobId,
    ) -> Result<ChildTurnSettlement, BatchError> {
        let mut settlement = ChildTurnSettlement::default();
        let Some((spec, outcome)) = self.inspect(|tx| settled_turn(tx, job))? else {
            return Ok(settlement);
        };
        let conversation = spec.id;
        settlement.changed = self.settle_closing(conversation)?;
        let Some(parent) = spec.parent_id else {
            return Ok(settlement);
        };
        if outcome == OutcomeKind::Unknown {
            settlement.changed |= self.report_settled_child(parent, conversation)? > 0;
            return Ok(settlement);
        }
        settlement.head = self.group_head(conversation)? == Some(job);
        if settlement.head {
            let lifecycle = self.job_lifecycle(conversation)?;
            if lifecycle.closing.is_none() {
                settlement.changed |= self.auto_close_if_done(conversation)?;
            }
            settlement.changed |= self.end_impossible_dependents(parent, conversation)?;
        }
        if self.step(|tx| {
            Ok(report_segment(tx, parent, conversation, None)?.is_some_and(|s| s.head == job))
        })? {
            settlement.changed |= self.report_settled_child(parent, conversation)? > 0;
        }
        Ok(settlement)
    }

    fn auto_close_if_done(&mut self, conversation: JobId) -> Result<bool, BatchError> {
        let spec = self.require_mutable_conversation(conversation)?;
        let mut lifecycle = self.job_lifecycle(conversation)?;
        if spec.parent_id.is_none()
            || spec.is_chunk_group()
            || spec.keep_open
            || lifecycle.closing.is_some()
            || self.step(|tx| has_user_turn(tx, conversation, false))?
        {
            return Ok(false);
        }
        let Some(head) = self.group_head(conversation)? else {
            return Ok(false);
        };
        if !self
            .require_job_state(head)?
            .outcome
            .is_some_and(|outcome| outcome.kind == OutcomeKind::Completed)
            || !self.live_children(conversation)?.is_empty()
            || !self.unsettled_non_turn_jobs(conversation)?.is_empty()
            || self.step(|tx| has_open_approvals(tx, conversation))?
        {
            return Ok(false);
        }
        lifecycle.closing = Some(JobClosing {
            at: self.now,
            by: Actor::Auto,
            running: false,
        });
        self.put_job_lifecycle(conversation, &lifecycle)?;
        self.settle_closing(conversation)?;
        Ok(true)
    }

    fn end_impossible_dependents(
        &mut self,
        parent: JobId,
        settled: JobId,
    ) -> Result<bool, BatchError> {
        let mut ended = Vec::new();
        loop {
            let mut progressed = false;
            for sibling in self.live_children(parent)? {
                if sibling == settled || ended.contains(&sibling) {
                    continue;
                }
                let lifecycle = self.job_lifecycle(sibling)?;
                if lifecycle.closing.is_some() {
                    continue;
                }
                let mut impossible = false;
                for (prerequisite, condition) in &lifecycle.after {
                    if self.prerequisite_state(*prerequisite, *condition)?
                        == DependencyState::Impossible
                    {
                        impossible = true;
                        break;
                    }
                }
                if impossible {
                    let _ =
                        self.stop_unsettled_turns(sibling, &OutcomeCause::DependencyImpossible)?;
                    let mut lifecycle = lifecycle;
                    lifecycle.closing = Some(JobClosing {
                        at: self.now,
                        by: Actor::Auto,
                        running: false,
                    });
                    self.put_job_lifecycle(sibling, &lifecycle)?;
                    self.settle_closing(sibling)?;
                    ended.push(sibling);
                    progressed = true;
                }
            }
            if !progressed {
                break;
            }
        }
        if ended.is_empty() {
            return Ok(false);
        }
        self.wake_scheduler();
        for sibling in ended {
            self.report_settled_child(parent, sibling)?;
        }
        Ok(true)
    }

    fn return_child_results(&mut self, report: ChildReport) -> Result<usize, BatchError> {
        let ChildReport {
            parent,
            call,
            results,
            turn,
        } = report;
        match turn {
            ReportTurn::AtLimit
            | ReportTurn::WaitForConfiguration
            | ReportTurn::InvalidInput(_) => return Ok(0),
            ReportTurn::Post(input) => {
                if self.step(|tx| has_current_draft(tx, parent))? {
                    return Ok(0);
                }
                let marker = results
                    .iter()
                    .fold(0u32, |marker, r| marker ^ r.head.get() as u32);
                self.host_turn(
                    parent,
                    input,
                    tool_submission(call, b"results create", marker),
                    tool_submission(call, b"results submit", marker),
                    Actor::Auto,
                )?;
                let mut lifecycle = self.job_lifecycle(parent)?;
                lifecycle.loop_count = lifecycle.loop_count.saturating_add(1);
                self.put_job_lifecycle(parent, &lifecycle)?;
            }
            ReportTurn::Permanent(reason) => {
                log::warn!("child results for parent {parent} remain on the child cards: {reason}")
            }
            ReportTurn::Skip => {}
        }
        for result in &results {
            let mut lifecycle = self.job_lifecycle(result.child)?;
            lifecycle.reported = Some(result.head);
            self.put_job_lifecycle(result.child, &lifecycle)?;
        }
        Ok(results.len())
    }

    pub(crate) fn report_settled_child(
        &mut self,
        parent: JobId,
        child: JobId,
    ) -> Result<usize, BatchError> {
        let Some(report) = self.step(|tx| collect_child_report(tx, parent, child))? else {
            return Ok(0);
        };
        if let ReportTurn::InvalidInput(reason) = &report.turn {
            log::warn!("invalid child report input for child {child} of parent {parent}: {reason}");
        }
        self.return_child_results(report)
    }

    pub(crate) fn retry_child_report(
        &mut self,
        parent: JobId,
        child: JobId,
    ) -> Result<usize, BatchError> {
        match self.step(|tx| collect_child_report(tx, parent, child))? {
            Some(report) => self.return_child_results(report),
            None => Ok(0),
        }
    }

    pub(crate) fn flush_child_reports(&mut self, parent: JobId) -> Result<(), BatchError> {
        let mut before = None;
        loop {
            let (children, next) = self.step(|tx| unreported_calls(tx, parent, before))?;
            for child in children {
                self.retry_child_report(parent, child)?;
            }
            let Some(cursor) = next else { return Ok(()) };
            before = Some(cursor);
        }
    }

    pub(crate) fn settle_child_turns(
        &mut self,
        turns: &[JobId],
    ) -> Result<ControlFlow<usize, usize>, BatchError> {
        let mut settled = 0;
        let mut changed = false;
        for turn in turns {
            let settlement = self.child_turn_settled(*turn)?;
            settled += usize::from(settlement.head);
            changed |= settlement.changed;
        }
        Ok(if changed {
            ControlFlow::Continue(settled)
        } else {
            ControlFlow::Break(settled)
        })
    }

    pub(crate) fn idle_worker(&mut self, caller: JobId, worker: JobId) -> Result<bool, BatchError> {
        let row = self.require_mutable_conversation(worker)?;
        self.step(|tx| idle_worker(tx, caller, &row))
    }

    pub(crate) fn frozen_agent_manages_jobs(
        &mut self,
        job: &MutableJob,
    ) -> Result<bool, BatchError> {
        Ok(match &job.frozen {
            Some(MutableFrozenConfiguration::Generative(frozen)) => frozen
                .agent
                .as_ref()
                .is_some_and(|agent| agent.manages_jobs()),
            Some(MutableFrozenConfiguration::Decision(_)) | None => false,
        })
    }

    pub(crate) fn child_conversations(
        &mut self,
        parent: JobId,
        after: u128,
        limit: usize,
    ) -> Result<Vec<JobId>, BatchError> {
        self.step(|tx| {
            tx.open_table(CHILD_GROUPS)?
                .range((parent.get(), after.saturating_add(1))..=(parent.get(), u128::MAX))?
                .take(limit)
                .map(|entry| Ok(JobId::new(entry?.0.value().1)))
                .collect()
        })
    }
}

fn actor_name(actor: &Actor) -> &'static str {
    match actor {
        Actor::User => "the user",
        Actor::Agent { .. } => "an Agent",
        Actor::Auto => "the host",
        Actor::Loop => "the loop limit",
    }
}

fn head_configuration(
    head: Option<MutableJob>,
) -> Result<(ExecutionConfiguration, JobQueuePriority), Violation> {
    let Some(head) = head else {
        return Err(Violation::InvalidValue(
            "conversation",
            "the conversation has no turn to take its configuration from",
        ));
    };
    match head.input.content {
        MutableJobContent::Generative {
            configuration: Some(configuration),
            ..
        } => Ok((configuration, head.input.priority)),
        MutableJobContent::Generative {
            configuration: None,
            ..
        }
        | MutableJobContent::Decision { .. } => Err(Violation::InvalidValue(
            "conversation",
            "the conversation has no Agent configuration",
        )),
    }
}

fn returned_answer(tx: &impl TxRead, turn: JobId) -> Result<Option<String>, StorageError> {
    let outputs = tx.table(JOB_OUTPUTS)?;
    let mut result = None;
    for entry in outputs.range((turn.get(), 0)..=(turn.get(), u32::MAX))? {
        let (key, value) = entry?;
        if rows::output_kind(value.value())? == OutputKind::Result {
            result = Some(rows::decode_output(key.value(), value.value())?);
        }
    }
    match result {
        Some(record) => {
            Ok(super::observations::answer_text(tx, turn, &record)?
                .or_else(|| record.display_text()))
        }
        None => Ok(None),
    }
}

// Children from one call are allocated together in a single write. Their
// keys are contiguous within their parent, even after the children close.
fn call_siblings(
    tx: &impl TxRead,
    parent: JobId,
    child: JobId,
    call: ToolCallRef,
) -> Result<Vec<JobId>, StorageError> {
    let groups = tx.table(CHILD_GROUPS)?;
    let conversations = tx.table(CONVERSATIONS)?;
    let key = (parent.get(), child.get());
    let mut siblings = Vec::new();
    let mut collect = |id: u128| -> Result<bool, StorageError> {
        let row = conversations
            .get(id)?
            .ok_or_else(|| StorageError::corrupted("child conversation missing"))?;
        if JobSpec::decode_at(id, row.value())?.origin != (Origin::Agent { call }) {
            return Ok(false);
        }
        if siblings.len() == MAX_LIVE_CHILDREN {
            return Err(StorageError::corrupted("too many children from one call"));
        }
        siblings.push(JobId::new(id));
        Ok(true)
    };
    for entry in groups.range((parent.get(), 0)..key)?.rev() {
        if !collect(entry?.0.value().1)? {
            break;
        }
    }
    for entry in groups.range(key..=(parent.get(), u128::MAX))? {
        if !collect(entry?.0.value().1)? {
            break;
        }
    }
    siblings.sort_unstable();
    Ok(siblings)
}

fn has_open_approvals(tx: &impl TxRead, conversation: JobId) -> Result<bool, StorageError> {
    Ok(tx
        .table(OPEN_APPROVALS)?
        .range((conversation.get(), 0, 0, 0)..=(conversation.get(), u128::MAX, u32::MAX, u32::MAX))?
        .next()
        .transpose()?
        .is_some())
}

pub(super) fn has_user_turn(
    tx: &impl TxRead,
    conversation: JobId,
    unsettled_only: bool,
) -> Result<bool, StorageError> {
    for entry in tx
        .table(TURNS)?
        .range((conversation.get(), 0)..=(conversation.get(), u128::MAX))?
        .rev()
    {
        let id = JobId::new(entry?.0.value().1);
        if unsettled_only && state::require(tx, id)?.state.outcome.is_some() {
            return Ok(false);
        }
        if required_turn(tx, id)?.submitted_by == Actor::User {
            return Ok(true);
        }
    }
    if unsettled_only {
        return Ok(false);
    }
    has_draft(tx, conversation, |draft| draft.submitted_by == Actor::User)
}

fn required_turn(tx: &impl TxRead, id: JobId) -> Result<MutableJob, StorageError> {
    jobs::mutable_job_in(tx, id)?
        .ok_or_else(|| StorageError::corrupted(format!("turn {id} missing")))
}

fn idle_worker(tx: &impl TxRead, caller: JobId, row: &JobSpec) -> Result<bool, StorageError> {
    if row.parent_id != Some(caller)
        || !matches!(row.origin, Origin::Agent { .. })
        || !row.keep_open
    {
        return Ok(false);
    }
    let runtime = state::require(tx, row.id)?;
    if runtime.lifecycle.closing.is_some()
        || runtime.lifecycle.on_hold.is_some()
        || has_user_turn(tx, row.id, false)?
        || runtime.state.outcome.is_some()
        || has_open_approvals(tx, row.id)?
    {
        return Ok(false);
    }
    // unsettled_turns is the suffix ending at the head. A settled head has no such suffix.
    match turn_head(tx, row.id)? {
        Some(head) => Ok(state::require(tx, head)?.state.outcome.is_some()),
        None => Ok(false),
    }
}

struct ReportSegment {
    first: JobId,
    head: JobId,
}

// An unreported continuation can belong to a segment starting at or before the
// receipt. The bounded sweep flags that case for the full sibling readiness check.
fn report_segment(
    tx: &impl TxRead,
    parent: JobId,
    child: JobId,
    reported: Option<JobId>,
) -> Result<Option<ReportSegment>, StorageError> {
    use std::ops::Bound::{Excluded, Included};
    let recurrence = tx
        .table(RECURRENCES)?
        .get(child.get())?
        .map(|row| Recurrence::decode(row.value()))
        .transpose()?;
    let start = reported.map_or(Included((child.get(), 0)), |id| {
        Excluded((child.get(), id.get()))
    });
    let mut first = reported;
    let mut head = None;
    for entry in tx
        .table(TURNS)?
        .range((start, Included((child.get(), u128::MAX))))?
        .rev()
    {
        let id = JobId::new(entry?.0.value().1);
        let job = required_turn(tx, id)?;
        if reads::jobs::project_planning_target(tx, &job)?.is_some() {
            continue;
        }
        head.get_or_insert(id);
        if job.retry.is_some() {
            continue;
        }
        if recurrence
            .as_ref()
            .is_some_and(|r| r.current_turn == id || r.previous_turn == Some(id))
            || job.submitted_by
                == (Actor::Agent {
                    conversation: parent,
                })
        {
            first = Some(id);
            break;
        }
        if job.submitted_by == Actor::User {
            head = None;
        }
    }
    let Some((first, head)) = first.zip(head) else {
        return Ok(None);
    };
    if state::require(tx, head)?.state.outcome.is_none() {
        return Ok(None);
    }
    Ok(Some(ReportSegment { first, head }))
}

fn segment_waiting(
    tx: &impl TxRead,
    conversation: JobId,
    segment: &ReportSegment,
) -> Result<bool, StorageError> {
    let first = segment.first;
    let last = segment.head;
    if tx
        .table(OPEN_APPROVALS)?
        .range(
            (conversation.get(), first.get(), 0, 0)
                ..=(conversation.get(), last.get(), u32::MAX, u32::MAX),
        )?
        .next()
        .transpose()?
        .is_some()
    {
        return Ok(true);
    }
    for entry in tx
        .table(LIVE_CHILDREN)?
        .range((conversation.get(), 0)..=(conversation.get(), u128::MAX))?
    {
        let child = conversation_row(tx, JobId::new(entry?.0.value().1))?;
        if let Origin::Agent { call } = child.origin
            && (first..=last).contains(&call.job)
            && !idle_worker(tx, conversation, &child)?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

// A report must not move the head on which an unsubmitted user message is based.
pub(super) fn has_current_draft(tx: &impl TxRead, parent: JobId) -> Result<bool, StorageError> {
    let head = turn_head(tx, parent)?;
    has_draft(tx, parent, |draft| draft.previous == head)
}

fn is_child_notice(text: &str) -> bool {
    matches!(
        text.lines().next(),
        Some(CHILD_PROGRESS_HEADER | CHILD_HELP_HEADER)
    )
}

fn child_notice_message(
    child: JobId,
    call: ToolCallRef,
    text: &str,
    kind: ChildNoticeKind,
) -> String {
    let (heading, instruction) = match kind {
        ChildNoticeKind::Help => (
            CHILD_HELP_HEADER,
            format!(
                "The direct child is paused. If you can supply the requested input, call job_answer_child with turn {}, request {}, index {} and your answer. Otherwise ask the user. The child's final result will follow separately.",
                call.job, call.request.get(), call.index,
            ),
        ),
        ChildNoticeKind::Progress => (
            CHILD_PROGRESS_HEADER,
            "This is an interim update. The work is still in progress; wait for its separate final result.".to_owned(),
        ),
    };
    format!("{heading}\nFrom direct child {child}. {instruction}\n\n{text}")
}

fn child_notice_capacity(tx: &impl TxRead, spec: &JobSpec) -> Result<usize, StorageError> {
    let (limits, _) = reads::conversations::conversation_limits(tx, spec.id)?
        .ok_or_else(|| StorageError::corrupted("child notice ancestor is missing"))?;
    let mut remaining = usize::from(limits.max_auto_turns);
    let mut previous = turn_head(tx, spec.id)?;
    // Interim updates have their own allowance. They do not spend final-report turns.
    for _ in 0..MAX_CHILD_SCAN {
        let Some(id) = previous else { break };
        if remaining == 0 {
            break;
        }
        let turn = required_turn(tx, id)?;
        previous = turn.previous;
        match turn.submitted_by {
            Actor::User => break,
            Actor::Agent { conversation } if Some(conversation) == spec.parent_id => break,
            Actor::Agent { .. } if is_child_notice(turn.input.text()) => remaining -= 1,
            Actor::Agent { .. } | Actor::Auto | Actor::Loop => {}
        }
    }
    Ok(remaining)
}

fn prepare_child_notice_in(
    tx: &impl TxRead,
    parent: JobId,
    message: String,
) -> Result<Option<MutableJobInput>, BatchError> {
    let spec = conversation_row(tx, parent)?;
    jobs::require_open_turn_parent(&spec, &state::require(tx, parent)?)
        .map_err(|_| Violation::ParentClosed(parent))?;
    let previous = turn_head(tx, parent)?;
    let head = previous.map(|id| required_turn(tx, id)).transpose()?;
    let (configuration, priority) = head_configuration(head)?;
    let input = host_input(message, configuration, priority);
    jobs::validate_mutable_input(tx, &input)?;
    if has_current_draft(tx, parent)? {
        return Ok(None);
    }
    if child_notice_capacity(tx, &spec)? == 0 {
        return Err(Violation::InvalidValue(
            "parent_notice_limit",
            "the parent has reached its automatic limit for interim child notices",
        )
        .into());
    }
    jobs::require_generative_history(tx, parent, previous, None)?;
    let frozen = jobs::generative_execution(tx, &input)?;
    jobs::require_conversation_agent(&spec, frozen.agent_ref.map(|reference| reference.id))?;
    Ok(Some(input))
}

fn pending_child_notices(
    tx: &impl TxRead,
    parent: JobId,
    turn: Option<JobId>,
    after: Option<ToolCallRef>,
    limit: usize,
) -> Result<Vec<ToolCallRef>, StorageError> {
    use std::ops::Bound::{Excluded, Included};
    let start = after.map_or(
        Included((parent.get(), turn.map_or(0, JobId::get), 0, 0)),
        |call| Excluded(approvals::open_key(parent, call)),
    );
    let end = Included((
        parent.get(),
        turn.map_or(u128::MAX, JobId::get),
        u32::MAX,
        u32::MAX,
    ));
    tx.table(PENDING_CHILD_NOTICES)?
        .range((start, end))?
        .take(limit)
        .map(|entry| {
            let (_, job, request, index) = entry?.0.value();
            rows::tool_call_ref((job, request, index))
        })
        .collect()
}

fn pending_child_notice_kind(record: &ToolEvidenceRecord) -> Result<ChildNoticeKind, StorageError> {
    match record.name.as_str() {
        "job_report_progress" => Ok(ChildNoticeKind::Progress),
        "job_ask_parent" => Ok(ChildNoticeKind::Help),
        _ => Err(StorageError::corrupted(
            "pending child notice references a different tool",
        )),
    }
}

fn stored_notice_text(record: &ToolEvidenceRecord) -> Result<String, StorageError> {
    let tool = record
        .decision
        .fact
        .stored_admission()
        .and_then(|admission| JobToolCall::parse(&record.name, admission.arguments.as_str()));
    match tool {
        Some(JobToolCall::JobReportProgress { text })
        | Some(JobToolCall::JobAskParent { question: text }) => Ok(text),
        _ => Err(StorageError::corrupted(
            "pending child notice is missing its text",
        )),
    }
}

fn has_draft(
    tx: &impl TxRead,
    parent: JobId,
    matches: impl Fn(&MutableJob) -> bool,
) -> Result<bool, StorageError> {
    for entry in tx.table(DRAFT_CONTEXTS)?.iter()? {
        let draft = jobs::mutable_job_in(tx, JobId::new(entry?.1.value()))?
            .ok_or_else(|| StorageError::corrupted("draft context references a missing Job"))?;
        if draft.parent_id == parent && matches(&draft) {
            return Ok(true);
        }
    }
    Ok(false)
}

// Return one unreported representative per call, newest first. Siblings from one call are contiguous.
fn unreported_calls(
    tx: &impl TxRead,
    parent: JobId,
    before: Option<u128>,
) -> Result<(Vec<JobId>, Option<u128>), StorageError> {
    use std::ops::Bound::{Excluded, Included};
    let end = before.map_or(Included((parent.get(), u128::MAX)), |id| {
        Excluded((parent.get(), id))
    });
    let mut children = Vec::new();
    let mut last_call = None;
    let mut count = 0;
    let mut last = None;
    for entry in tx
        .table(CHILD_GROUPS)?
        .range((Included((parent.get(), 0)), end))?
        .rev()
        .take(MAX_CHILD_SCAN)
    {
        let child = JobId::new(entry?.0.value().1);
        count += 1;
        last = Some(child.get());
        let Origin::Agent { call } = conversation_row(tx, child)?.origin else {
            continue;
        };
        if last_call != Some(call)
            && report_segment(
                tx,
                parent,
                child,
                state::require(tx, child)?.lifecycle.reported,
            )?
            .is_some()
        {
            last_call = Some(call);
            children.push(child);
        }
    }
    Ok((children, last.filter(|_| count == MAX_CHILD_SCAN)))
}

pub(super) fn waiting_child_reports(
    tx: &impl TxRead,
    parent: JobId,
) -> Result<Vec<JobId>, StorageError> {
    let mut waiting = Vec::new();
    for child in unreported_calls(tx, parent, None)?.0 {
        let Some(report) = collect_child_report(tx, parent, child)? else {
            continue;
        };
        if matches!(report.turn, ReportTurn::AtLimit) {
            waiting.extend(report.results.into_iter().map(|result| result.child));
        }
    }
    Ok(waiting)
}

fn collect_child_report(
    tx: &impl TxRead,
    parent: JobId,
    child: JobId,
) -> Result<Option<ChildReport>, StorageError> {
    let Origin::Agent { call } = conversation_row(tx, child)?.origin else {
        return Ok(None);
    };
    let mut results = Vec::new();
    for child in call_siblings(tx, parent, child, call)? {
        let Some(segment) = report_segment(tx, parent, child, None)? else {
            return Ok(None);
        };
        let head = segment.head;
        let lifecycle = state::require(tx, child)?.lifecycle;
        if segment_waiting(tx, child, &segment)? {
            return Ok(None);
        }
        if lifecycle.reported.is_none_or(|reported| reported < head) {
            let outcome = state::require(tx, head)?
                .state
                .outcome
                .ok_or_else(|| StorageError::corrupted("reportable turn has no outcome"))?;
            results.push(ChildResult {
                child,
                head,
                outcome: outcome.kind,
                title: lifecycle.title.unwrap_or_else(|| child.to_string()),
            });
        }
    }
    if results.is_empty() {
        return Ok(None);
    }
    let turn = child_report_turn(tx, parent, &results)?;
    Ok(Some(ChildReport {
        parent,
        call,
        results,
        turn,
    }))
}

fn child_report_text(tx: &impl TxRead, results: &[ChildResult]) -> Result<String, StorageError> {
    let mut text = String::from(REPORT_HEADER);
    let mut available = jobs::MAX_INPUT_BYTES - text.len();
    let mut sections = Vec::new();
    for result in results {
        let child = result.child;
        let title = &result.title[..result.title.floor_char_boundary(REPORT_TITLE_BYTES)];
        let title = title.replace(jobs::invalid_input_character, " ");
        let status = match result.outcome {
            OutcomeKind::Completed => "completed",
            OutcomeKind::Failed => "failed",
            OutcomeKind::Cancelled => "cancelled",
            OutcomeKind::Unknown => "unknown",
        };
        let header = format!("\n\n[{title}] conversation {child}: {status}\n");
        let pointer = format!("\nFull answer in child conversation {child}.");
        let body = if result.outcome == OutcomeKind::Unknown {
            reads::jobs::unknown_execution_note(tx, result.head)?
        } else {
            returned_answer(tx, result.head)?.unwrap_or_default()
        };
        let answer: String = body
            .chars()
            .take(jobs::MAX_INPUT_BYTES + 1)
            .map(|c| {
                if jobs::invalid_input_character(c) {
                    ' '
                } else {
                    c
                }
            })
            .collect();
        // At most MAX_LIVE_CHILDREN were opened by the call. Reserve every
        // header plus enough for either the full answer or its pointer.
        available -= header.len() + pointer.len().min(answer.len());
        sections.push((header, answer, pointer));
    }
    for (header, answer, pointer) in sections {
        text.push_str(&header);
        available += pointer.len().min(answer.len());
        if answer.len() <= available {
            text.push_str(&answer);
            available -= answer.len();
        } else {
            let end = answer.floor_char_boundary(available - pointer.len());
            text.push_str(&answer[..end]);
            text.push_str(&pointer);
            available -= end + pointer.len();
        }
    }
    Ok(text)
}

fn conversation_row(tx: &impl TxRead, id: JobId) -> Result<JobSpec, StorageError> {
    read_conversation_row(tx, id)?.ok_or_else(|| {
        StorageError::corrupted(format!("child report conversation {id} is missing"))
    })
}

fn child_report_turn(
    tx: &impl TxRead,
    parent: JobId,
    results: &[ChildResult],
) -> Result<ReportTurn, StorageError> {
    let runtime = state::require(tx, parent)?;
    if runtime.lifecycle.closing.is_some()
        || runtime.lifecycle.hidden_at.is_some()
        || runtime.state.outcome.is_some()
    {
        return Ok(ReportTurn::Skip);
    }
    let (limits, _) = reads::conversations::conversation_limits(tx, parent)?
        .ok_or_else(|| StorageError::corrupted("child report ancestor is missing"))?;
    if runtime.lifecycle.loop_count >= limits.max_auto_turns {
        return Ok(ReportTurn::AtLimit);
    }
    let previous = turn_head(tx, parent)?;
    let head = previous
        .map(|id| jobs::mutable_job_in(tx, id))
        .transpose()?
        .flatten();
    let (configuration, priority) = match head_configuration(head) {
        Ok(configuration) => configuration,
        Err(reason) => return Ok(ReportTurn::Permanent(reason)),
    };
    let spec = conversation_row(tx, parent)?;
    if let Err(reason) = jobs::require_open_turn_parent(&spec, &runtime) {
        return Ok(ReportTurn::Permanent(reason));
    }
    match jobs::require_generative_history(tx, parent, previous, None) {
        Ok(()) => {}
        Err(BatchError(Failure::Rejected(reason))) => return Ok(ReportTurn::Permanent(reason)),
        Err(BatchError(Failure::Storage(error))) => return Err(error),
    }
    let input = host_input(child_report_text(tx, results)?, configuration, priority);
    match jobs::validate_mutable_input(tx, &input) {
        Ok(()) => {}
        Err(BatchError(Failure::Rejected(reason))) => return Ok(ReportTurn::InvalidInput(reason)),
        Err(BatchError(Failure::Storage(error))) => return Err(error),
    }
    let frozen = match jobs::generative_execution(tx, &input) {
        Ok(frozen) => frozen,
        Err(BatchError(Failure::Rejected(_))) => return Ok(ReportTurn::WaitForConfiguration),
        Err(BatchError(Failure::Storage(error))) => return Err(error),
    };
    if let Err(reason) =
        jobs::require_conversation_agent(&spec, frozen.agent_ref.map(|reference| reference.id))
    {
        return Ok(ReportTurn::Permanent(reason));
    }
    Ok(ReportTurn::Post(input))
}

pub(crate) fn read_child_report_page(
    database: &::redb::Database,
    after: Option<(u128, u128)>,
) -> Result<ChildReportPage, StorageError> {
    use std::ops::Bound::{Excluded, Unbounded};
    let tx = reads::read(database)?;
    let groups = tx.open_table(CHILD_GROUPS)?;
    let mut next = after;
    let mut scanned = 0;
    for entry in groups
        .range((after.map_or(Unbounded, Excluded), Unbounded))?
        .take(MAX_CHILD_SCAN)
    {
        let key = entry?.0.value();
        scanned += 1;
        if next.is_some_and(|cursor| key <= cursor) {
            continue;
        }
        next = Some(key);
        let (parent, child) = (JobId::new(key.0), JobId::new(key.1));
        if report_segment(
            &tx,
            parent,
            child,
            state::require(&tx, child)?.lifecycle.reported,
        )?
        .is_none()
        {
            continue;
        }
        if let Some(report) = collect_child_report(&tx, parent, child)? {
            match report.turn {
                ReportTurn::Post(_) if has_current_draft(&tx, parent)? => {
                    next = Some((parent.get(), u128::MAX))
                }
                ReportTurn::AtLimit | ReportTurn::WaitForConfiguration => {
                    next = Some((parent.get(), u128::MAX))
                }
                ReportTurn::InvalidInput(_) => {}
                ReportTurn::Skip | ReportTurn::Permanent(_) | ReportTurn::Post(_) => {
                    // Re-read the parent head and loop count after each settlement.
                    return Ok(ChildReportPage {
                        next,
                        candidate: Some((parent, child)),
                    });
                }
            }
        }
    }
    Ok(ChildReportPage {
        next: next.filter(|_| scanned == MAX_CHILD_SCAN),
        candidate: None,
    })
}
