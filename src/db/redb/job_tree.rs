use super::*;
use crate::db::entities::{
    AllocationPhase, AllocationToken, ComposerContextId, DependencyState, ExecutionConfiguration,
    JobAction, JobClosing, JobControl, JobQueuePriority, MutableFrozenConfiguration, MutableJob,
    MutableJobContent, MutableJobInput, NativeStopRequest, OutcomeCause, OutcomeRecord,
    RequestFingerprint, Submission, ToolCallRef, Violation,
};

const MAX_LIVE_CHILDREN: usize = 16;
const MAX_LIVE_SUBTREE: usize = 64;
const MAX_AGENT_RETRIES: usize = 2;
const RETURNED_RESULT_CHARS: usize = 4000;
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
        let mut control = self.job_control(conversation)?;
        control.limits = Some(limits);
        self.put_job_control(conversation, &control)
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
        let Some(head) = self.head_turn(conversation)? else {
            return self.reject(Violation::InvalidValue(
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
            | MutableJobContent::Decision { .. } => self.reject(Violation::InvalidValue(
                "conversation",
                "the conversation has no Agent configuration",
            )),
        }
    }

    pub(crate) fn host_turn(
        &mut self,
        conversation: JobId,
        text: String,
        configuration: ExecutionConfiguration,
        priority: JobQueuePriority,
        create: Submission,
        submit: Submission,
    ) -> Result<JobId, BatchError> {
        let input = MutableJobInput {
            priority,
            content: MutableJobContent::Generative {
                text,
                attachments: Vec::new(),
                configuration: Some(configuration),
            },
        };
        let context = ComposerContextId::new(self.next_id()?);
        let id = self.create_job_as(context, input, create, Some(conversation), None, false)?;
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
    ) -> Result<JobId, BatchError> {
        let (configuration, priority) = self.conversation_configuration(conversation)?;
        self.host_turn(
            conversation,
            text,
            configuration,
            priority,
            tool_submission(call, purpose, 0),
            tool_submission(call, purpose, 1),
        )
    }

    fn require_room_for_child(&mut self, parent: JobId) -> Result<(), BatchError> {
        let row = self.require_mutable_conversation(parent)?;
        if !row.kind.is_group()
            || self.require_job_state(parent)?.outcome.is_some()
            || self.job_control(parent)?.closing.is_some()
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
            allocation: None,
            kind: JobKind::Group {
                agent,
                project,
                origin: Origin::Agent { call },
                keep_open,
            },
        };
        self.insert(JOBS, id.get(), row.encode_mutable_conversation().as_slice())?;
        self.put_job_state(id, &JobState::new(id))?;
        self.put_job_control(
            id,
            &JobControl {
                after,
                ..JobControl::default()
            },
        )?;
        self.insert(CONVERSATIONS, id.get(), ())?;
        self.insert(UNSETTLED, id.get(), (0, 0))?;
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
            if self.job_control(conversation)?.user_input
                && !self.unsettled_turns(conversation)?.is_empty()
            {
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
        let Some(head) = self.group_head(prerequisite)? else {
            return Ok(DependencyState::Waiting);
        };
        Ok(match self.require_job_state(head)?.outcome {
            None => DependencyState::Waiting,
            Some(outcome) => match (outcome.kind, condition) {
                (OutcomeKind::Completed, _) | (_, DependencyCondition::Settled)
                    if outcome.kind != OutcomeKind::Unknown =>
                {
                    DependencyState::Satisfied
                }
                (OutcomeKind::Unknown, _) => DependencyState::Waiting,
                (_, _) => DependencyState::Impossible,
            },
        })
    }

    pub(crate) fn mutable_job_blocked(&mut self, job: &MutableJob) -> Result<bool, BatchError> {
        let mut current = Some(job.parent_id);
        let mut own = true;
        for _ in 0..=crate::db::entities::MAX_JOB_DEPTH {
            let Some(conversation) = current else {
                return Ok(false);
            };
            let control = self.job_control(conversation)?;
            if control.on_hold.is_some() {
                return Ok(true);
            }
            if own {
                for (prerequisite, condition) in &control.after {
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

    fn stop_unsettled_turns(
        &mut self,
        conversation: JobId,
        cause: &OutcomeCause,
    ) -> Result<(Vec<AllocationToken>, Vec<JobId>), BatchError> {
        let mut running = Vec::new();
        let mut ended = Vec::new();
        for turn in self.unsettled_turns(conversation)? {
            let Some(allocation) = self.pool_allocation(turn)? else {
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
                    NativeStopRequest::EndedBeforeStart(token)
                    | NativeStopRequest::Requested(token)
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
        if self.job_control(conversation)?.closing.is_some() {
            let subtree = self.live_subtree(conversation)?;
            let mut running = Vec::new();
            let mut ended = Vec::new();
            for current in &subtree {
                let (tokens, never_ran) =
                    self.stop_unsettled_turns(*current, &OutcomeCause::Closed)?;
                running.extend(tokens);
                ended.extend(never_ran);
            }
            for current in subtree.into_iter().rev() {
                self.settle_closing(current)?;
            }
            return Ok(JobAction {
                conversation,
                record_seq: None,
                running,
                ended,
                already: true,
                settled: self.require_job_state(conversation)?.outcome.is_some(),
            });
        }
        let subtree = self.live_subtree(conversation)?;
        let mut running = Vec::new();
        let mut all_ended = Vec::new();
        for current in &subtree {
            let mut control = self.job_control(*current)?;
            if control.closing.is_some() {
                continue;
            }
            let (ended, never_ran) = self.stop_unsettled_turns(*current, &OutcomeCause::Closed)?;
            all_ended.extend(never_ran);
            control.closing = Some(JobClosing {
                at: self.now,
                by: by.clone(),
                running: !ended.is_empty(),
            });
            self.put_job_control(*current, &control)?;
            self.void_approvals(*current, by.clone())?;
            running.extend(ended);
        }
        for current in subtree.into_iter().rev() {
            self.settle_closing(current)?;
        }
        Ok(JobAction {
            conversation,
            record_seq: None,
            running,
            ended: all_ended,
            already: false,
            settled: false,
        })
    }

    fn settle_closing(&mut self, conversation: JobId) -> Result<(), BatchError> {
        let control = self.job_control(conversation)?;
        let mut state = self.require_job_state(conversation)?;
        let Some(closing) = control.closing else {
            return Ok(());
        };
        if state.outcome.is_some()
            || !self.unsettled_turns(conversation)?.is_empty()
            || !self.live_children(conversation)?.is_empty()
        {
            return Ok(());
        }
        let head = self.head_turn(conversation)?;
        let answered = match head {
            Some(head) => self.require_job_state(head.id)?.outcome,
            None => None,
        };
        if answered
            .as_ref()
            .is_some_and(|outcome| outcome.kind == OutcomeKind::Unknown)
        {
            return Ok(());
        }
        let kept = answered.filter(|outcome| {
            !closing.running
                && outcome.kind == OutcomeKind::Completed
                && outcome.cause != Some(OutcomeCause::Closed)
        });
        state.outcome = Some(match kept {
            Some(outcome) => outcome,
            None => OutcomeRecord {
                kind: OutcomeKind::Cancelled,
                at: self.now,
                cause: Some(OutcomeCause::Closed),
            },
        });
        self.put_job_state(conversation, &state)?;
        let parent = self.require_mutable_conversation(conversation)?.parent_id;
        if let Some(parent) = parent {
            self.remove(LIVE_CHILDREN, (parent.get(), conversation.get()))?;
        }
        let released = self.step(|tx| {
            let mut unsettled = tx.open_table(UNSETTLED)?;
            let open = unsettled.get(conversation.get())?.map(|row| row.value());
            Ok(match open {
                Some((_, 0)) => unsettled.remove(conversation.get())?.is_some(),
                Some(_) | None => false,
            })
        })?;
        if released && let Some(parent) = parent {
            self.adjust_open_children(parent, -1)?;
        }
        Ok(())
    }

    pub(crate) fn hold(
        &mut self,
        conversation: JobId,
        by: Actor,
        unhold: bool,
    ) -> Result<JobAction, BatchError> {
        self.require_mutable_conversation(conversation)?;
        let mut control = self.job_control(conversation)?;
        if matches!(by, Actor::Agent { .. }) && control.on_hold == Some(HELD_BY_USER) {
            return self.reject(Violation::InvalidValue(
                "hold",
                "actor cannot lift this hold",
            ));
        }
        control.on_hold = (!unhold).then_some(match by {
            Actor::Agent { .. } => HELD_BY_AGENT,
            Actor::User | Actor::Auto | Actor::Loop => HELD_BY_USER,
        });
        control.loop_count = 0;
        self.put_job_control(conversation, &control)?;
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
        if self.require_job_state(turn)?.outcome.is_none() {
            return self.reject(Violation::InvalidValue(
                "turn",
                "cannot retry an unfinished turn",
            ));
        }
        if !self.unsettled_turns(conversation)?.is_empty() {
            return self.reject(Violation::InvalidValue(
                "retry",
                "a waiting turn already follows",
            ));
        }
        let mut control = self.job_control(conversation)?;
        if matches!(by, Actor::Agent { .. })
            && control
                .retried
                .iter()
                .filter(|prior| **prior == turn)
                .count()
                >= MAX_AGENT_RETRIES
        {
            return self.reject(Violation::InvalidValue(
                "retry_limit",
                "at most two agent retries per original turn",
            ));
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
        let nth = u32::try_from(control.retried.len()).unwrap_or(u32::MAX);
        let identity = |step: &[u8]| {
            let mut identity = b"ocg retry ".to_vec();
            identity.extend_from_slice(step);
            identity.extend_from_slice(&turn.get().to_le_bytes());
            identity.extend_from_slice(&nth.to_le_bytes());
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
        let context = ComposerContextId::new(self.next_id()?);
        let user = matches!(by, Actor::User);
        let id = self.create_job_as(
            context,
            input,
            identity(b"create"),
            Some(conversation),
            None,
            user,
        )?;
        self.submit_mutable_job(id, 1, identity(b"submit"))?;
        control = self.job_control(conversation)?;
        control.retried.push(turn);
        self.put_job_control(conversation, &control)?;
        self.wake_scheduler();
        Ok(Appended {
            turn: id,
            seq: 1,
            joined: false,
        })
    }

    pub(crate) fn child_turn_settled(&mut self, job: JobId) -> Result<bool, BatchError> {
        let Some(turn) = self.mutable_job(job)? else {
            return Ok(false);
        };
        let conversation = turn.parent_id;
        let spec = self.require_mutable_conversation(conversation)?;
        let Some(parent) = spec.parent_id else {
            return Ok(false);
        };
        let Some(outcome) = self.require_job_state(job)?.outcome else {
            return Ok(false);
        };
        if outcome.kind == OutcomeKind::Unknown || self.group_head(conversation)? != Some(job) {
            return Ok(false);
        }
        let JobKind::Group { keep_open, .. } = spec.kind else {
            return Ok(false);
        };
        let control = self.job_control(conversation)?;
        if control.closing.is_some() {
            self.settle_closing(conversation)?;
        } else if !keep_open
            && outcome.kind == OutcomeKind::Completed
            && !control.user_input
            && self.live_children(conversation)?.is_empty()
            && !self.has_open_job_approvals(conversation)?
        {
            let mut control = control;
            control.closing = Some(JobClosing {
                at: self.now,
                by: Actor::Auto,
                running: false,
            });
            self.put_job_control(conversation, &control)?;
            self.settle_closing(conversation)?;
        }
        self.end_impossible_dependents(parent, conversation)?;
        self.return_child_results(parent, conversation)?;
        Ok(true)
    }

    fn end_impossible_dependents(
        &mut self,
        parent: JobId,
        settled: JobId,
    ) -> Result<(), BatchError> {
        for sibling in self.live_children(parent)? {
            if sibling == settled {
                continue;
            }
            let control = self.job_control(sibling)?;
            if control.closing.is_some() {
                continue;
            }
            let mut impossible = false;
            for (prerequisite, condition) in &control.after {
                if self.prerequisite_state(*prerequisite, *condition)?
                    == DependencyState::Impossible
                {
                    impossible = true;
                }
            }
            if impossible {
                let _ = self.stop_unsettled_turns(sibling, &OutcomeCause::DependencyImpossible)?;
                let mut control = control;
                control.closing = Some(JobClosing {
                    at: self.now,
                    by: Actor::Auto,
                    running: false,
                });
                self.put_job_control(sibling, &control)?;
                self.settle_closing(sibling)?;
            }
        }
        Ok(())
    }

    fn returned_answer(&mut self, turn: JobId) -> Result<Option<String>, BatchError> {
        let answer = self.step(|tx| {
            let outputs = tx.open_table(JOB_OUTPUTS)?;
            let mut result = None;
            for entry in outputs.range((turn.get(), 0)..=(turn.get(), u32::MAX))? {
                let (key, value) = entry?;
                if rows::output_kind(value.value())? == OutputKind::Result {
                    result = Some(rows::decode_output(key.value(), value.value())?);
                }
            }
            Ok(result)
        })?;
        Ok(answer
            .and_then(|record| record.display_text())
            .map(|text| text.chars().take(RETURNED_RESULT_CHARS).collect()))
    }

    fn return_child_results(&mut self, parent: JobId, child: JobId) -> Result<(), BatchError> {
        let JobKind::Group {
            origin: Origin::Agent { call },
            ..
        } = self.require_mutable_conversation(child)?.kind
        else {
            return Ok(());
        };
        let siblings = self.step(|tx| {
            tx.open_table(CHILD_GROUPS)?
                .range((parent.get(), 0)..=(parent.get(), u128::MAX))?
                .take(MAX_CHILD_SCAN)
                .map(|entry| Ok(JobId::new(entry?.0.value().1)))
                .collect::<Result<Vec<_>, StorageError>>()
        })?;
        let mut report = Vec::new();
        for sibling in siblings {
            let spec = self.require_mutable_conversation(sibling)?;
            if !matches!(spec.kind, JobKind::Group { origin: Origin::Agent { call: opened }, .. } if opened == call)
            {
                continue;
            }
            let Some(head) = self.group_head(sibling)? else {
                return Ok(());
            };
            let state = self.require_job_state(head)?;
            let Some(outcome) = state.outcome else {
                return Ok(());
            };
            if outcome.kind == OutcomeKind::Unknown {
                return Ok(());
            }
            if self.job_control(sibling)?.reported != Some(head) {
                report.push((sibling, head, outcome.kind));
            }
        }
        if report.is_empty() {
            return Ok(());
        }
        let mut lines = vec![
            "The conversations you opened have finished. Their answers follow; use them, and do not claim a failed or cancelled one completed."
                .to_owned(),
        ];
        for (sibling, head, kind) in &report {
            let title = self
                .job_control(*sibling)?
                .title
                .unwrap_or_else(|| sibling.to_string());
            let answer = self.returned_answer(*head)?.unwrap_or_default();
            lines.push(format!(
                "\n[{title}] conversation {sibling}: {}\n{answer}",
                match kind {
                    OutcomeKind::Completed => "completed",
                    OutcomeKind::Failed => "failed",
                    OutcomeKind::Cancelled => "cancelled",
                    OutcomeKind::Unknown => "unknown",
                }
            ));
        }
        for (sibling, head, _) in &report {
            let mut control = self.job_control(*sibling)?;
            control.reported = Some(*head);
            self.put_job_control(*sibling, &control)?;
        }
        let mut control = self.job_control(parent)?;
        let parent_state = self.require_job_state(parent)?;
        if control.closing.is_some()
            || parent_state.outcome.is_some()
            || control.loop_count >= self.group_limits(parent)?.0.max_auto_turns
        {
            return Ok(());
        }
        let (configuration, priority) = self.conversation_configuration(parent)?;
        let marker = report.iter().fold(0u32, |marker, (_, head, _)| {
            marker ^ u32::try_from(head.get() & u128::from(u32::MAX)).unwrap_or(0)
        });
        self.host_turn(
            parent,
            lines.join("\n"),
            configuration,
            priority,
            tool_submission(call, b"results create", marker),
            tool_submission(call, b"results submit", marker),
        )?;
        control = self.job_control(parent)?;
        control.loop_count = control.loop_count.saturating_add(1);
        self.put_job_control(parent, &control)
    }

    pub(crate) fn unreported_child_turns(&mut self) -> Result<Vec<JobId>, BatchError> {
        let children = self.step(|tx| {
            tx.open_table(CHILD_GROUPS)?
                .iter()?
                .take(4 * MAX_CHILD_SCAN)
                .map(|entry| Ok(JobId::new(entry?.0.value().1)))
                .collect::<Result<Vec<_>, StorageError>>()
        })?;
        let mut turns = Vec::new();
        for child in children {
            let Some(head) = self.group_head(child)? else {
                continue;
            };
            let settled = self
                .require_job_state(head)?
                .outcome
                .is_some_and(|outcome| outcome.kind != OutcomeKind::Unknown);
            if settled && self.job_control(child)?.reported != Some(head) {
                turns.push(head);
            }
        }
        Ok(turns)
    }

    pub(crate) fn settle_child_turns(&mut self, turns: &[JobId]) -> Result<usize, BatchError> {
        let mut settled = 0;
        for turn in turns {
            if self.child_turn_settled(*turn)? {
                settled += 1;
            }
        }
        Ok(settled)
    }

    pub(crate) fn settle_unreported_children(&mut self, limit: usize) -> Result<usize, BatchError> {
        let mut turns = self.unreported_child_turns()?;
        turns.truncate(limit);
        self.settle_child_turns(&turns)
    }

    pub(crate) fn idle_worker(&mut self, caller: JobId, worker: JobId) -> Result<bool, BatchError> {
        let row = self.require_mutable_conversation(worker)?;
        if row.parent_id != Some(caller)
            || !matches!(
                row.kind,
                JobKind::Group {
                    origin: Origin::Agent { .. },
                    keep_open: true,
                    ..
                }
            )
        {
            return Ok(false);
        }
        let control = self.job_control(worker)?;
        if control.closing.is_some()
            || control.on_hold.is_some()
            || control.user_input
            || self.require_job_state(worker)?.outcome.is_some()
            || !self.unsettled_turns(worker)?.is_empty()
        {
            return Ok(false);
        }
        match self.group_head(worker)? {
            Some(head) => Ok(self.require_job_state(head)?.outcome.is_some()),
            None => Ok(false),
        }
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
