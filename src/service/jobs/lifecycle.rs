use crate::db::entities::*;
use crate::db::{Batch, BatchError};
use std::collections::BTreeSet;

impl Batch {
    pub(crate) fn root_group(&mut self, mut root: JobId) -> Result<JobId, BatchError> {
        while let Some(parent) = self.require_job(root)?.parent_id {
            root = parent;
        }
        Ok(root)
    }

    pub(crate) fn live_subtree(&mut self, root: JobId) -> Result<Vec<JobId>, BatchError> {
        let mut result = vec![root];
        let mut i = 0;
        while i < result.len() {
            let children = self.live_children(result[i])?;
            result.extend(children);
            i += 1;
        }
        Ok(result)
    }

    pub(crate) fn is_held(&mut self, turn: &JobSpec) -> Result<bool, BatchError> {
        let mut ancestor = turn
            .kind
            .is_persistent()
            .then_some(turn.parent_id)
            .flatten();
        let mut own = true;
        while let Some(conversation) = ancestor {
            let row = self.job_control(conversation)?;
            if let Some(seq) = row.on_hold {
                let hold = self.job_notice(conversation, seq)?;
                if own
                    || !matches!(
                        hold.map(|notice| notice.body),
                        Some(NoticeBody::Record {
                            by: Actor::Loop,
                            ..
                        })
                    )
                {
                    return Ok(true);
                }
            }
            ancestor = self.require_job(conversation)?.parent_id;
            own = false;
        }
        Ok(false)
    }

    pub(crate) fn stop_turn(
        &mut self,
        conversation: JobId,
        by: Actor,
    ) -> Result<JobAction, BatchError> {
        let mut related = Vec::new();
        if matches!(by, Actor::User)
            && let Some(tip) = self.group_head(conversation)?
        {
            let row = self.require_job(tip)?;
            if let JobKind::Execution {
                execution: JobExecution::Summarization(work),
                ..
            } = row.kind
            {
                let anchor = work.chain.unwrap_or(tip);
                if let Some(anchor_conversation) = self.require_job(anchor)?.parent_id
                    && anchor_conversation != conversation
                {
                    related.push(anchor_conversation);
                }
                if let Some(parent) = self.require_job(conversation)?.parent_id {
                    for child in self.live_children(parent)? {
                        if child == conversation {
                            continue;
                        }
                        if let Some(tip) = self.group_head(child)?
                            && matches!(self.require_job(tip)?.kind,JobKind::Execution { execution:JobExecution::Summarization(work), .. } if work.chain.unwrap_or(tip) == anchor)
                        {
                            related.push(child);
                        }
                    }
                }
            }
        }
        related.sort();
        related.dedup();
        let mut action = self.stop_one_turn(conversation, by.clone())?;
        for child in related {
            action
                .running
                .extend(self.stop_one_turn(child, by.clone())?.running);
        }
        action.running.sort();
        action.running.dedup();
        Ok(action)
    }

    fn stop_one_turn(&mut self, conversation: JobId, by: Actor) -> Result<JobAction, BatchError> {
        let cancellable_work = if matches!(by, Actor::User) {
            match self.group_head(conversation)? {
                Some(tip) => matches!(
                    self.require_job(tip)?.kind,
                    JobKind::Execution {
                        execution: JobExecution::Decision(_) | JobExecution::Summarization(_),
                        ..
                    }
                ),
                None => false,
            }
        } else {
            false
        };
        if !cancellable_work {
            self.require_agent_group(conversation)?;
        }
        let record = self.put_record(conversation, RecordEvent::Stop, by)?;
        let mut running = Vec::new();
        if let Some(tip) = self.group_head(conversation)? {
            let state = self.require_job_state(tip)?;
            if state.outcome.is_none() && state.claim.is_none() {
                self.finalize(
                    tip,
                    OutcomeKind::Cancelled,
                    Some(OutcomeCause::Stopped { record }),
                    None,
                    false,
                )?;
            }
        }
        if let Some(active) = self.active_job(conversation)?
            && self.require_job_state(active)?.outcome.is_none()
        {
            running.push(active);
        }
        Ok(JobAction {
            conversation,
            record_seq: Some(record),
            running,
            already: false,
        })
    }

    pub(crate) fn close(
        &mut self,
        conversation: JobId,
        by: Actor,
    ) -> Result<JobAction, BatchError> {
        self.require_agent_group(conversation)?;
        if self.job_control(conversation)?.closing.is_some() {
            let mut running = Vec::new();
            for current in self.live_subtree(conversation)? {
                if let Some(active) = self.active_job(current)?
                    && self.require_job_state(active)?.outcome.is_none()
                {
                    running.push(active);
                }
            }
            return Ok(JobAction {
                conversation,
                record_seq: None,
                running,
                already: true,
            });
        }
        let subtree = self.live_subtree(conversation)?;
        let mut running = Vec::new();
        let mut notified = Vec::new();
        for current in &subtree {
            let mut row = self.job_control(*current)?;
            if row.closing.is_some() {
                continue;
            }
            notified.push(*current);
            let active = self.active_job(*current)?;
            let active = match active {
                Some(active) if self.require_job_state(active)?.outcome.is_none() => Some(active),
                Some(_) | None => None,
            };
            row.closing = Some(JobClosing {
                at: self.now(),
                by: by.clone(),
                running: active.is_some(),
            });
            self.put_job_control(*current, &row)?;
            if let Some(active) = active {
                running.push(active);
            }
            if let Some(tip) = self.group_head(*current)? {
                let tip_state = self.require_job_state(tip)?;
                if tip_state.claim.is_none() && tip_state.outcome.is_none() {
                    self.finalize(
                        tip,
                        OutcomeKind::Cancelled,
                        Some(OutcomeCause::Closed),
                        None,
                        false,
                    )?;
                }
            }
            self.halt_recurrence(*current, RecurrenceHalt::Closed)?;
            self.void_approvals(*current, by.clone())?;
        }
        for current in subtree.into_iter().rev() {
            self.settle(current)?;
            if notified.contains(&current) {
                let reference = self.group_head(current)?.unwrap_or(current);
                self.notify(current, NoticeEvent::ClosedAbove, reference)?;
            }
        }
        self.wake_scheduler();
        Ok(JobAction {
            conversation,
            record_seq: None,
            running,
            already: false,
        })
    }

    pub(crate) fn hold(
        &mut self,
        conversation: JobId,
        by: Actor,
        unhold: bool,
    ) -> Result<JobAction, BatchError> {
        self.require_agent_group(conversation)?;
        let mut row = self.job_control(conversation)?;
        if let Some(seq) = row.on_hold {
            let notice = self.job_notice(conversation, seq)?;
            if matches!(by, Actor::Agent { .. })
                && matches!(
                    notice.map(|notice| notice.body),
                    Some(NoticeBody::Record {
                        by: Actor::User | Actor::Loop,
                        ..
                    })
                )
            {
                return self.reject(Violation::InvalidValue(
                    "hold",
                    "actor cannot lift this hold",
                ));
            }
        }
        let record = self.put_record(
            conversation,
            if unhold {
                RecordEvent::Unhold
            } else {
                RecordEvent::Hold
            },
            by.clone(),
        )?;
        row.on_hold = (!unhold).then_some(record);
        row.loop_count = 0;
        self.put_job_control(conversation, &row)?;
        let subtree = if by == Actor::Loop {
            vec![conversation]
        } else {
            self.live_subtree(conversation)?
        };
        for current in subtree {
            if let Some(tip) = self.group_head(current)? {
                let state = self.require_job_state(tip)?;
                if state.claim.is_some() || state.outcome.is_some() {
                    continue;
                }
                let turn = self.require_job(tip)?;
                if unhold {
                    let open = self.open_job_dependencies(tip)?;
                    if open == 0 {
                        self.place(tip)?;
                    }
                } else {
                    self.remove_job_from_ready(&turn)?;
                }
            }
        }
        self.wake_scheduler();
        Ok(JobAction {
            conversation,
            record_seq: Some(record),
            running: Vec::new(),
            already: false,
        })
    }

    pub(crate) fn retry(
        &mut self,
        conversation: JobId,
        turn: JobId,
        by: Actor,
    ) -> Result<Appended, BatchError> {
        self.require_agent_group(conversation)?;
        let Some(row) = self.job(turn)? else {
            return self.reject(Violation::InvalidValue("turn", "turn not found"));
        };
        if row.parent_id != Some(conversation) || !row.kind.is_persistent() {
            return self.reject(Violation::InvalidValue(
                "turn",
                "not a turn of this conversation",
            ));
        }
        let empty = self.effective_job_inputs(turn)?.iter().all(|(_, entry)| {
            matches!(
                entry.entry,
                Entry::ResponseFormat { .. }
                    | Entry::ContextUse { .. }
                    | Entry::ContextPreparation { .. }
            )
        });
        if empty {
            return self.reject(Violation::InvalidValue(
                "queued_message",
                "all executable input was withdrawn",
            ));
        }
        let unresolved = self
            .approval_effects(turn)?
            .iter()
            .any(|record| record.is_unresolved());
        if unresolved {
            return self.reject(Violation::InvalidValue(
                "retry_unresolved_approval",
                "an approved call has no definite outcome or reconciliation",
            ));
        }
        if self.require_job_state(turn)?.outcome.is_none() {
            return self.reject(Violation::InvalidValue(
                "turn",
                "cannot retry an unfinished turn",
            ));
        }
        if matches!(by, Actor::Agent { .. }) && self.agent_retry_limit_reached(turn)? {
            return self.reject(Violation::InvalidValue(
                "retry_limit",
                "at most two agent retries per original turn",
            ));
        }
        // A waiting turn takes the entry itself; it may hold only one retry.
        if let Some(tip) = self.group_head(conversation)? {
            let state = self.require_job_state(tip)?;
            if state.claim.is_none() && state.outcome.is_none() {
                let retried = self
                    .job_inputs(tip)?
                    .iter()
                    .any(|(_, entry)| matches!(entry.entry, Entry::RetryOf { .. }));
                if retried {
                    return self.reject(Violation::InvalidValue(
                        "retry",
                        "waiting turn already has a retry",
                    ));
                }
            }
        }
        self.append(conversation, Entry::RetryOf { turn, by }, None, None, false)
    }

    pub(crate) fn auto_close_check(&mut self, conversation: JobId) -> Result<bool, BatchError> {
        let row = self.require_job(conversation)?;
        let one_shot = matches!(
            row.kind,
            JobKind::Group {
                origin: Origin::User | Origin::System,
                keep_open: false,
                ..
            }
        );
        let one_shot = one_shot || self.group_allocation(conversation)?.is_some();
        if (row.parent_id.is_none() && !one_shot)
            || !matches!(
                row.kind,
                JobKind::Group {
                    keep_open: false,
                    ..
                }
            )
            || self.job_control(conversation)?.closing.is_some()
        {
            return Ok(false);
        }
        let Some(tip) = self.group_head(conversation)? else {
            return Ok(false);
        };
        if !self
            .require_job_state(tip)?
            .outcome
            .is_some_and(|outcome| match outcome.kind {
                OutcomeKind::Completed => true,
                OutcomeKind::Failed | OutcomeKind::Cancelled => one_shot,
                OutcomeKind::Unknown => false,
            })
            || !self.live_children(conversation)?.is_empty()
            || (!one_shot && self.has_user_input(conversation)?)
        {
            return Ok(false);
        }
        if self.has_open_job_approvals(conversation)? {
            return Ok(false);
        }
        let mut row = self.job_control(conversation)?;
        row.closing = Some(JobClosing {
            at: self.now(),
            by: Actor::Auto,
            running: false,
        });
        self.put_job_control(conversation, &row)?;
        self.settle(conversation)?;
        self.notify(conversation, NoticeEvent::ClosedSelf, tip)?;
        Ok(true)
    }
}

impl Batch {
    fn agent_retry_limit_reached(&mut self, job: JobId) -> Result<bool, BatchError> {
        let mut root = job;
        let mut ancestors = BTreeSet::new();
        loop {
            if !ancestors.insert(root) {
                return self.corrupted("retry ancestry cycle".into());
            }
            let original =
                self.job_inputs(root)?
                    .into_iter()
                    .find_map(|(_, entry)| match entry.entry {
                        Entry::RetryOf { turn, .. } => Some(turn),
                        Entry::UserMessage { .. }
                        | Entry::AgentMessage { .. }
                        | Entry::Notice { .. }
                        | Entry::Decision { .. }
                        | Entry::Iteration { .. }
                        | Entry::ResponseFormat { .. }
                        | Entry::UserMessageEdit { .. }
                        | Entry::UserMessageWithdraw { .. }
                        | Entry::ContextPreparation { .. }
                        | Entry::ContextUse { .. } => None,
                    });
            match original {
                Some(original) => root = original,
                None => break,
            }
        }
        let mut pending = vec![root];
        let mut visited = BTreeSet::new();
        let mut count = 0;
        while let Some(original) = pending.pop() {
            if !visited.insert(original) {
                continue;
            }
            for (job, agent) in self.job_retries(original)? {
                if agent {
                    count += 1;
                    if count >= 2 {
                        return Ok(true);
                    }
                }
                pending.push(job);
            }
        }
        Ok(false)
    }
}

impl Batch {
    pub(crate) fn group_limits(
        &mut self,
        conversation: JobId,
    ) -> Result<(JobLimits, u8), BatchError> {
        let mut current = conversation;
        let mut effective = JobLimits::CEILING;
        for depth in 0..=crate::db::entities::MAX_JOB_DEPTH {
            let row = self.require_job(current)?;
            if !row.kind.is_group() {
                return self.reject(Violation::InvalidValue(
                    "conversation_not_found",
                    "expected a conversation",
                ));
            }
            let own = self.job_control(current)?.limits;
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
        let row = self.require_agent_group(conversation)?;
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
                self.job_tool_rights(caller, conversation)?;
            }
            if !limits.within(self.group_limits(conversation)?.0) {
                return self.reject(Violation::InvalidValue(
                    "limits",
                    "an Agent may only tighten the effective limits",
                ));
            }
        }
        let mut metadata = self.job_control(conversation)?;
        metadata.limits = Some(limits);
        self.put_job_control(conversation, &metadata)?;
        self.loop_hold_if_needed(conversation)?;
        if self
            .recurrence(conversation)?
            .is_some_and(|recurrence| recurrence.n >= u64::from(limits.max_iterations))
        {
            self.halt_recurrence(conversation, RecurrenceHalt::LimitReached)?;
        }
        Ok(())
    }

    pub(crate) fn require_agent_group(
        &mut self,
        conversation: JobId,
    ) -> Result<JobSpec, BatchError> {
        let row = self.require_job(conversation)?;
        match &row.kind {
            JobKind::Group {
                origin: Origin::System,
                ..
            } => self.reject(Violation::InvalidValue(
                "system_conversation",
                "managed in Settings",
            )),
            JobKind::Group { .. } => Ok(row),
            JobKind::Execution { .. } => self.reject(Violation::InvalidValue(
                "conversation_not_found",
                "expected a conversation",
            )),
        }
    }
}
