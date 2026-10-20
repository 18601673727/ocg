use super::*;
use crate::persistence::{
    ExecutionConfiguration, NewSubmission, OutcomeCause, OutcomeKind, OutcomeRecord, Violation,
};

impl Batch {
    pub(crate) fn turn_execution(
        &mut self,
        turn: &JobRow,
    ) -> Result<(FrozenExecutionConfig, Option<ProjectId>, Option<ConfigId>), BatchError> {
        self.step(|tx| reads::turn_execution(&tx.open_table(JOBS)?, turn))
    }
    pub(crate) fn tip(&mut self, conversation: JobId) -> Result<Option<JobId>, BatchError> {
        self.step(|tx| {
            Ok(tx
                .open_table(TIPS)?
                .get(conversation.get())?
                .map(|id| JobId::new(id.value())))
        })
    }
    pub(crate) fn active_turn(&mut self, conversation: JobId) -> Result<Option<JobId>, BatchError> {
        self.step(|tx| {
            Ok(tx
                .open_table(ACTIVE_TURNS)?
                .get(conversation.get())?
                .map(|turn| JobId::new(turn.value())))
        })
    }
    pub(crate) fn place_waiting_tip(&mut self, conversation: JobId) -> Result<(), BatchError> {
        if let Some(tip) = self.tip(conversation)? {
            let state = self.require_job_state(tip)?;
            if state.claim.is_none()
                && state.outcome.is_none()
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
    pub(crate) fn require_user_conversation(
        &mut self,
        conversation: JobId,
    ) -> Result<JobRow, BatchError> {
        let row = self.require_job(conversation)?;
        match &row.kind {
            JobKind::Conversation {
                origin: Origin::System,
                ..
            } => self.reject(Violation::InvalidValue(
                "system_conversation",
                "managed in Settings",
            )),
            JobKind::Conversation { .. } => Ok(row),
            JobKind::Turn { .. } => self.reject(Violation::InvalidValue(
                "conversation_not_found",
                "expected a conversation",
            )),
        }
    }
    pub(crate) fn refresh_recent(&mut self, conversation: JobId) -> Result<(), BatchError> {
        let row = self.require_job(conversation)?;
        let state = self.conversation(conversation)?;
        if let Some(tip) = self.tip(conversation)? {
            self.step(|tx| {
                let mut table = tx.open_table(RECENT)?;
                table.remove((tip.get(), conversation.get()))?;
                if row.parent_id.is_none() && state.hidden_at.is_none() {
                    table.insert((tip.get(), conversation.get()), ())?;
                }
                Ok(())
            })?;
        }
        Ok(())
    }
    pub(crate) fn open_conversation(
        &mut self,
        parent: Option<JobId>,
        spec: ConversationSpec,
        first: Entry,
        configuration: ExecutionConfiguration,
        submission: Option<NewSubmission>,
        dependencies: Vec<(JobId, DependencyCondition)>,
    ) -> Result<(JobId, Appended), BatchError> {
        if let Some(parent) = parent {
            let parent_row = self.require_job(parent)?;
            if !parent_row.kind.is_conversation() {
                return self.reject(Violation::InvalidValue("parent", "expected a conversation"));
            }
            let mut root = parent;
            let mut depth = 1;
            while let Some(ancestor) = self.require_job(root)?.parent_id {
                root = ancestor;
                depth += 1;
            }
            if depth > 3
                || self.live_children(parent)?.len() >= 16
                || self.live_subtree(root)?.len() >= 64
            {
                return self.reject(Violation::InvalidValue(
                    "children",
                    "live conversation limit reached",
                ));
            }
        }
        if let Some(submission) = &submission
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
            return Ok((
                conversation,
                Appended {
                    turn: record.job,
                    seq: record.seq,
                    joined: record.seq > 1,
                },
            ));
        }
        let conversation = self.t1_submit(
            None,
            JobRow {
                id: JobId::new(0),
                created_at: self.now,
                eligible_at: None,
                name: None,
                parent_id: parent,
                kind: JobKind::Conversation {
                    agent: spec.agent,
                    project: spec.project,
                    origin: spec.origin,
                    keep_open: spec.keep_open,
                },
                objective: None,
                attachments: Vec::new(),
                dependencies: Vec::new(),
                previous: None,
                submission: None,
            },
        )?;
        self.insert_conversation(conversation, &ConversationRow::default())?;
        if let Some(parent) = parent {
            self.step(|tx| {
                tx.open_table(CHILD_CONVERSATIONS)?
                    .insert((parent.get(), conversation.get()), ())?;
                tx.open_table(LIVE_CHILDREN)?
                    .insert((parent.get(), conversation.get()), ())?;
                Ok(())
            })?;
        }
        let appended = self.append_named(
            conversation,
            first,
            Some(configuration),
            submission,
            false,
            spec.name,
            dependencies,
        )?;
        Ok((conversation, appended))
    }
    pub(crate) fn insert_turn_input(
        &mut self,
        turn: JobId,
        entry: Entry,
    ) -> Result<u32, BatchError> {
        let row = self.require_job(turn)?;
        let state = self.require_job_state(turn)?;
        if !row.kind.is_stored() || state.claim.is_some() || state.outcome.is_some() {
            return self.reject(Violation::InvalidValue(
                "turn_input",
                "input is frozen by claim or outcome",
            ));
        }
        if let Entry::UserMessage { attachments, .. } = &entry {
            if !attachments.is_empty() {
                let (frozen, _, _) = self.turn_execution(&row)?;
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
                let existing = self.step(|tx| {
                    let mut count = 0;
                    for entry in tx
                        .open_table(TURN_INPUTS)?
                        .range((turn.get(), 0)..=(turn.get(), u32::MAX))?
                    {
                        let (_, bytes) = entry?;
                        if let Entry::UserMessage { attachments, .. } =
                            TurnEntry::decode(bytes.value())?.entry
                        {
                            count += attachments.len();
                        }
                    }
                    Ok(count)
                })?;
                if existing + attachments.len()
                    > crate::openai_compatible::image::MAX_REQUEST_IMAGES
                {
                    return self.reject(Violation::InvalidValue(
                        "too_many_images",
                        "the turn has too many images",
                    ));
                }
            }
            for attachment in attachments {
                if !self.require_attachment(*attachment)?.is_available() {
                    return self.reject(Violation::AttachmentCleanedUp(*attachment));
                }
            }
        }
        let at = self.now;
        let (seq, first_user) = self.step(|tx| {
            let mut first_user = false;
            let mut table = tx.open_table(TURN_INPUTS)?;
            let seq = table
                .range((turn.get(), 0)..=(turn.get(), u32::MAX))?
                .next_back()
                .transpose()?
                .map_or(1, |(key, _)| key.value().1 + 1);
            table.insert(
                (turn.get(), seq),
                TurnEntry {
                    entry: entry.clone(),
                    at,
                }
                .encode()
                .as_slice(),
            )?;
            match entry {
                Entry::UserMessage { attachments, .. } => {
                    for attachment in attachments {
                        tx.open_table(ATTACHMENT_REFS)?
                            .insert((attachment.get(), turn.get()), ())?;
                    }
                    if let Some(conversation) = row.parent_id {
                        let mut user = tx.open_table(USER_ENTRY_AT)?;
                        if user.get(conversation.get())?.is_none() {
                            user.insert(conversation.get(), at.as_unix_micros())?;
                            first_user = true;
                        }
                    }
                }
                Entry::RetryOf { turn: original, by } => {
                    tx.open_table(RETRIES)?.insert(
                        (original.get(), turn.get()),
                        matches!(by, Actor::Agent { .. }),
                    )?;
                }
                Entry::AgentMessage { .. }
                | Entry::Notice { .. }
                | Entry::Decision { .. }
                | Entry::Iteration { .. } => {}
            }
            Ok((seq, first_user))
        })?;
        if first_user && let Some(conversation) = row.parent_id {
            let waiting = self.step(|tx| {
                let jobs = tx.open_table(JOBS)?;
                let mut waiting = BTreeSet::new();
                for entry in tx
                    .open_table(DEPENDENCY_WAITERS)?
                    .range((conversation.get(), 0)..=(conversation.get(), u64::MAX))?
                {
                    let waiter = JobId::new(entry?.0.value().1);
                    if let Some(bytes) = jobs.get(waiter.get())?
                        && let Some(parent) = JobRow::decode(bytes.value())?.parent_id
                    {
                        waiting.insert(parent);
                    }
                }
                Ok(waiting.into_iter().collect::<Vec<_>>())
            })?;
            if !waiting.is_empty() {
                self.notify(conversation, NoticeEvent::WaitersBlocked { waiting }, turn)?;
            }
        }
        Ok(seq)
    }
    pub(crate) fn append(
        &mut self,
        conversation: JobId,
        entry: Entry,
        freeze: Option<ExecutionConfiguration>,
        submission: Option<NewSubmission>,
        event: bool,
    ) -> Result<Appended, BatchError> {
        self.append_named(
            conversation,
            entry,
            freeze,
            submission,
            event,
            None,
            Vec::new(),
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn append_named(
        &mut self,
        conversation: JobId,
        entry: Entry,
        freeze: Option<ExecutionConfiguration>,
        submission: Option<NewSubmission>,
        event: bool,
        name: Option<String>,
        dependencies: Vec<(JobId, DependencyCondition)>,
    ) -> Result<Appended, BatchError> {
        if let Some(submission) = &submission
            && let Some(existing) = self.submission(submission.key)?
        {
            if existing.fingerprint != submission.fingerprint {
                return self.reject(Violation::SubmissionKeyUsed {
                    job: existing.job,
                    same_request: false,
                });
            }
            return Ok(Appended {
                turn: existing.job,
                seq: existing.seq,
                joined: existing.seq > 1,
            });
        }
        let row = self.require_job(conversation)?;
        if !row.kind.is_conversation() {
            return self.reject(Violation::InvalidValue(
                "conversation_not_found",
                "expected a conversation",
            ));
        }
        let state = self.require_job_state(conversation)?;
        if state.children_closed_at.is_some() {
            if event {
                return self.attention_fallback(conversation, entry);
            }
            return self.reject(Violation::InvalidValue(
                if state.outcome.is_some() {
                    "conversation_closed"
                } else {
                    "conversation_closing"
                },
                "no longer accepts messages",
            ));
        }
        let tip = self.tip(conversation)?;
        let waiting = match tip {
            Some(tip) => {
                let state = self.require_job_state(tip)?;
                state.claim.is_none() && state.outcome.is_none()
            }
            None => false,
        };
        let turn = if waiting {
            tip.expect("waiting tip")
        } else {
            let execution = match freeze {
                Some(configuration) => {
                    let frozen = (|| {
                        let frozen = self.freeze_configuration(configuration)?;
                        let agent = frozen.agent.as_ref();
                        if agent.is_some_and(|agent| {
                            agent.tool_grant.is_some() || !agent.dispatch_to.is_empty()
                        }) && !frozen
                            .llm
                            .config
                            .declared_capabilities
                            .contains(&ModelCapability::Tools)
                        {
                            return self.reject(Violation::ToolsNotSupported);
                        }
                        if agent.is_some_and(|agent| agent.tool_grant.is_some())
                            && matches!(row.kind, JobKind::Conversation { project: None, .. })
                        {
                            return self.reject(Violation::ProjectRequired);
                        }
                        Ok(frozen)
                    })();
                    let frozen = match frozen {
                        Err(BatchError(Failure::Rejected(_))) if event => {
                            self.failed = None;
                            return self.attention_fallback(conversation, entry);
                        }
                        result => result?,
                    };
                    TurnExecution::Frozen(frozen)
                }
                None => {
                    let Some(tip) = tip else {
                        return self.reject(Violation::InvalidValue(
                            "configuration",
                            "first turn requires configuration",
                        ));
                    };
                    let tip = self.require_job(tip)?;
                    match tip.kind {
                        JobKind::Turn {
                            execution: TurnExecution::Ref(owner),
                            ..
                        } => TurnExecution::Ref(owner),
                        JobKind::Turn {
                            execution: TurnExecution::Frozen(_),
                            ..
                        } => TurnExecution::Ref(tip.id),
                        JobKind::Conversation { .. } => {
                            return self.corrupted("conversation tip is not a turn".into());
                        }
                    }
                }
            };
            let spec = JobRow {
                id: JobId::new(0),
                created_at: self.now,
                eligible_at: None,
                name,
                parent_id: Some(conversation),
                kind: JobKind::Turn {
                    execution,
                    plan_source: PlanSource::Stored,
                },
                objective: None,
                attachments: Vec::new(),
                dependencies,
                previous: tip,
                submission: None,
            };
            self.event_append = event;
            let validation = if event {
                self.validate_spec(&spec)
            } else {
                Ok(())
            };
            if let Err(BatchError(Failure::Rejected(_))) = validation {
                self.failed = None;
                self.event_append = false;
                return self.attention_fallback(conversation, entry);
            }
            if let Err(error) = validation {
                self.event_append = false;
                return Err(error);
            }
            let submitted = self.t1_submit(None, spec);
            self.event_append = false;
            submitted?
        };
        let user_message = matches!(entry, Entry::UserMessage { .. });
        let attention = event && matches!(entry, Entry::Notice { .. });
        let seq = self.insert_turn_input(turn, entry)?;
        if user_message {
            let mut row = self.conversation(conversation)?;
            if row.loop_count != 0 {
                row.loop_count = 0;
                self.insert_conversation(conversation, &row)?;
            }
            if self.loop_held(conversation)? {
                self.hold(conversation, Actor::User, true)?;
            }
        } else if attention {
            self.loop_hold_if_needed(conversation)?;
        }
        if let Some(submission) = submission {
            let record = SubmissionRecord {
                key: submission.key,
                fingerprint: submission.fingerprint,
                job: turn,
                seq,
                recorded_at: self.now,
            };
            self.step(|tx| {
                tx.open_table(SUBMISSIONS)?.insert(
                    record.key.as_bytes().as_slice(),
                    encode_submission(&record).as_slice(),
                )?;
                Ok(())
            })?;
            self.probe = Some(Probe::Submission(record.key));
        }
        Ok(Appended {
            turn,
            seq,
            joined: waiting,
        })
    }
    pub(crate) fn live_children(&mut self, conversation: JobId) -> Result<Vec<JobId>, BatchError> {
        self.step(|tx| {
            let table = tx.open_table(LIVE_CHILDREN)?;
            table
                .range((conversation.get(), 0)..=(conversation.get(), u64::MAX))?
                .map(|entry| Ok(JobId::new(entry?.0.value().1)))
                .collect()
        })
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
    pub(crate) fn is_held(&mut self, turn: &JobRow) -> Result<bool, BatchError> {
        let mut ancestor = turn.kind.is_stored().then_some(turn.parent_id).flatten();
        let mut own = true;
        while let Some(conversation) = ancestor {
            let row = self.conversation(conversation)?;
            if let Some(seq) = row.on_hold {
                let hold = self.step(|tx| {
                    tx.open_table(NOTICES)?
                        .get((conversation.get(), seq))?
                        .map(|bytes| Notice::decode(bytes.value()))
                        .transpose()
                })?;
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
    ) -> Result<ConversationAction, BatchError> {
        self.require_user_conversation(conversation)?;
        let record = self.put_record(conversation, RecordEvent::Stop, by)?;
        let mut running = Vec::new();
        if let Some(tip) = self.tip(conversation)? {
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
        if let Some(active) = self.active_turn(conversation)?
            && self.require_job_state(active)?.outcome.is_none()
        {
            running.push(active);
        }
        Ok(ConversationAction {
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
    ) -> Result<ConversationAction, BatchError> {
        self.require_user_conversation(conversation)?;
        if self
            .require_job_state(conversation)?
            .children_closed_at
            .is_some()
        {
            return Ok(ConversationAction {
                conversation,
                record_seq: None,
                running: Vec::new(),
                already: true,
            });
        }
        let subtree = self.live_subtree(conversation)?;
        let mut running = Vec::new();
        let mut notified = Vec::new();
        for current in &subtree {
            let mut state = self.require_job_state(*current)?;
            if state.children_closed_at.is_some() {
                continue;
            }
            notified.push(*current);
            state.children_closed_at = Some(self.now);
            self.put_job_state(*current, &state)?;
            let mut row = self.conversation(*current)?;
            row.closed_by.get_or_insert(by.clone());
            if let Some(tip) = self.tip(*current)? {
                let tip_state = self.require_job_state(tip)?;
                if let Some(active) = self.active_turn(*current)?
                    && self.require_job_state(active)?.outcome.is_none()
                {
                    row.closed_running = true;
                    running.push(active);
                }
                self.insert_conversation(*current, &row)?;
                if tip_state.claim.is_none() && tip_state.outcome.is_none() {
                    self.finalize(
                        tip,
                        OutcomeKind::Cancelled,
                        Some(OutcomeCause::Closed),
                        None,
                        false,
                    )?;
                }
            } else {
                self.insert_conversation(*current, &row)?;
            }
            self.halt_recurrence(*current, RecurrenceHalt::Closed)?;
            // K3 voids undecided approvals and questions here.
        }
        for current in subtree.into_iter().rev() {
            self.settle(current)?;
            if notified.contains(&current) {
                let reference = self.tip(current)?.unwrap_or(current);
                self.notify(current, NoticeEvent::ClosedAbove, reference)?;
            }
        }
        self.touched_ready_or_delayed = true;
        Ok(ConversationAction {
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
    ) -> Result<ConversationAction, BatchError> {
        self.require_user_conversation(conversation)?;
        let mut row = self.conversation(conversation)?;
        if let Some(seq) = row.on_hold {
            let notice = self.step(|tx| {
                tx.open_table(NOTICES)?
                    .get((conversation.get(), seq))?
                    .map(|bytes| Notice::decode(bytes.value()))
                    .transpose()
            })?;
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
        self.insert_conversation(conversation, &row)?;
        let subtree = if by == Actor::Loop {
            vec![conversation]
        } else {
            self.live_subtree(conversation)?
        };
        for current in subtree {
            if let Some(tip) = self.tip(current)? {
                let state = self.require_job_state(tip)?;
                if state.claim.is_some() || state.outcome.is_some() {
                    continue;
                }
                let turn = self.require_job(tip)?;
                if unhold {
                    let open = self.step(|tx| {
                        Ok(tx
                            .open_table(UNSETTLED)?
                            .get(tip.get())?
                            .map_or(0, |counts| counts.value().0))
                    })?;
                    if open == 0 {
                        self.place(tip)?;
                    }
                } else {
                    let requirement =
                        transactions::execution_requirement(&self.turn_execution(&turn)?.0);
                    self.step(|tx| {
                        tx.open_table(READY)?.remove((requirement, tip.get()))?;
                        if let Some(at) = turn.eligible_at {
                            tx.open_table(DELAYED)?
                                .remove((at.as_unix_micros(), tip.get()))?;
                        }
                        Ok(())
                    })?;
                }
            }
        }
        self.touched_ready_or_delayed = true;
        Ok(ConversationAction {
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
        self.require_user_conversation(conversation)?;
        let row = self.require_job(turn)?;
        if row.parent_id != Some(conversation) || !row.kind.is_stored() {
            return self.reject(Violation::InvalidValue(
                "turn",
                "not a turn of this conversation",
            ));
        }
        // K3 refuses an approved call without a definite outcome.
        if matches!(by, Actor::Agent { .. })
            && self.step(|tx| {
                let inputs = tx.open_table(TURN_INPUTS)?;
                let mut root = turn;
                let mut ancestors = BTreeSet::new();
                loop {
                    if !ancestors.insert(root) {
                        return Err(StorageError::corrupted("retry ancestry cycle"));
                    }
                    let mut original = None;
                    for entry in inputs.range((root.get(), 0)..=(root.get(), u32::MAX))? {
                        if let Entry::RetryOf { turn, .. } =
                            TurnEntry::decode(entry?.1.value())?.entry
                        {
                            original = Some(turn);
                            break;
                        }
                    }
                    match original {
                        Some(turn) => root = turn,
                        None => break,
                    }
                }
                let retries = tx.open_table(RETRIES)?;
                let mut pending = vec![root.get()];
                let mut visited = BTreeSet::new();
                let mut count = 0;
                while let Some(original) = pending.pop() {
                    if !visited.insert(original) {
                        continue;
                    }
                    for row in retries.range((original, 0)..=(original, u64::MAX))? {
                        let (key, agent) = row?;
                        if agent.value() {
                            count += 1;
                        }
                        pending.push(key.value().1);
                    }
                }
                Ok(count >= 2)
            })?
        {
            return self.reject(Violation::InvalidValue(
                "retry_limit",
                "at most two agent retries per original turn",
            ));
        }
        self.append(conversation, Entry::RetryOf { turn, by }, None, None, false)
    }
    pub(crate) fn auto_close_check(&mut self, conversation: JobId) -> Result<bool, BatchError> {
        let row = self.require_job(conversation)?;
        if row.parent_id.is_none()
            || !matches!(
                row.kind,
                JobKind::Conversation {
                    keep_open: false,
                    ..
                }
            )
            || self
                .require_job_state(conversation)?
                .children_closed_at
                .is_some()
        {
            return Ok(false);
        }
        let Some(tip) = self.tip(conversation)? else {
            return Ok(false);
        };
        if !self
            .require_job_state(tip)?
            .outcome
            .is_some_and(|outcome| outcome.kind == OutcomeKind::Completed)
            || !self.live_children(conversation)?.is_empty()
            || self.step(|tx| {
                Ok(tx
                    .open_table(USER_ENTRY_AT)?
                    .get(conversation.get())?
                    .is_some())
            })?
        {
            return Ok(false);
        }
        // K3 checks for undecided approvals and questions here.
        let mut state = self.require_job_state(conversation)?;
        state.children_closed_at = Some(self.now);
        self.put_job_state(conversation, &state)?;
        let mut row = self.conversation(conversation)?;
        row.closed_by = Some(Actor::Auto);
        self.insert_conversation(conversation, &row)?;
        self.settle(conversation)?;
        self.notify(conversation, NoticeEvent::ClosedSelf, tip)?;
        Ok(true)
    }
    pub(crate) fn index_answer(
        &mut self,
        turn: &JobRow,
        kind: OutcomeKind,
        result: bool,
    ) -> Result<(), BatchError> {
        if !turn.kind.is_stored() {
            return Ok(());
        }
        let Some(conversation) = turn.parent_id else {
            return Ok(());
        };
        self.step(|tx| {
            let mut table = tx.open_table(ANSWER_TIPS)?;
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
    pub(crate) fn settle_conversation(&mut self, conversation: JobId) -> Result<(), BatchError> {
        let mut state = self.require_job_state(conversation)?;
        if state.children_closed_at.is_none() || state.outcome.is_some() {
            return Ok(());
        }
        let row = self.conversation(conversation)?;
        let tip = self.tip(conversation)?;
        let tip_closed = match tip {
            Some(tip) => self
                .require_job_state(tip)?
                .outcome
                .is_some_and(|outcome| outcome.cause == Some(OutcomeCause::Closed)),
            None => false,
        };
        let answered = self.step(|tx| {
            Ok(tx
                .open_table(ANSWER_TIPS)?
                .get(conversation.get())?
                .map(|tips| tips.value().0)
                .filter(|turn| *turn != 0))
        })?;
        let outcome = match answered {
            Some(turn) => self.require_job_state(JobId::new(turn))?.outcome,
            None => None,
        };
        let outcome = if row.closed_running || tip_closed {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::{DatabaseHandle, DatabaseWorker};
    use crate::persistence::{
        ExecutionConfiguration, NewExecutor, NewSubmission, RequestFingerprint, ToolCallRef,
    };
    use crate::test_support::{data_directory, execution_configuration, wait};
    use std::num::NonZeroU32;

    struct Fixture {
        _root: crate::test_support::TestRoot,
        directory: crate::data_directory::DataDirectory,
        worker: DatabaseWorker,
        database: DatabaseHandle,
        configuration: ExecutionConfiguration,
        pool: ExecutorId,
    }
    impl Fixture {
        fn new(name: &str) -> Self {
            let (root, directory) = data_directory("conversation-tree", name);
            let worker = DatabaseWorker::start(&directory).expect("database");
            let database = worker.handle();
            let configuration = execution_configuration(&database);
            let pool = wait(database.create_executor(NewExecutor {
                name: "conversation worker".into(),
                concurrency_limit: NonZeroU32::new(16).expect("limit"),
                llm_transports: BTreeSet::from([LlmTransport::OpenAiChatCompletions]),
            }))
            .expect("pool");
            Self {
                _root: root,
                directory,
                worker,
                database,
                configuration,
                pool,
            }
        }
        fn open(&self) -> (JobId, JobId) {
            let (conversation, appended) = wait(self.database.open_conversation(
                None,
                ConversationSpec {
                    name: None,
                    agent: None,
                    project: None,
                    origin: Origin::User,
                    keep_open: true,
                },
                message("first"),
                self.configuration,
                None,
            ))
            .expect("open");
            (conversation, appended.turn)
        }
        fn child(&self, parent: JobId, keep_open: bool) -> (JobId, JobId) {
            let agent = wait(self.database.create_config(crate::persistence::NewConfig {
                name: "child Agent".into(),
                body: crate::persistence::ConfigBody::Agent(AgentConfig {
                    dispatch_to: Vec::new(),
                    models: vec![self.configuration.llm_profile.into()],
                    ..Default::default()
                }),
            }))
            .expect("agent");
            let call = ToolCallRef {
                job: parent,
                request: RequestNumber::FIRST,
                index: 0,
            };
            let (conversation, appended) = wait(self.database.open_conversation(
                Some(parent),
                ConversationSpec {
                    name: None,
                    agent: Some(agent),
                    project: None,
                    origin: Origin::Agent { call },
                    keep_open,
                },
                Entry::AgentMessage {
                    text: "objective".into(),
                    call,
                },
                ExecutionConfiguration {
                    agent_profile: Some(agent.into()),
                    ..self.configuration
                },
                None,
            ))
            .expect("child");
            (conversation, appended.turn)
        }
        fn claim(&self, turn: JobId) {
            assert!(
                wait(
                    self.database
                        .claim_execution(crate::persistence::ClaimRequest {
                            job: turn,
                            allowed_pools: BTreeSet::from([self.pool.into()])
                        })
                )
                .expect("claim")
                .is_ok()
            );
        }
        fn finish(&self, turn: JobId, kind: OutcomeKind) {
            self.claim(turn);
            wait(self.database.finalize_job(
                turn,
                match kind {
                    OutcomeKind::Completed => {
                        crate::persistence::JobFinalization::Completed(result("answer"))
                    }
                    OutcomeKind::Failed => {
                        crate::persistence::JobFinalization::Failed(result("failed"))
                    }
                    OutcomeKind::Unknown => crate::persistence::JobFinalization::Unknown,
                    OutcomeKind::Cancelled => {
                        crate::persistence::JobFinalization::Cancelled(result("cancelled"))
                    }
                },
            ))
            .expect("finalize");
        }
    }
    fn message(text: &str) -> Entry {
        Entry::UserMessage {
            text: text.into(),
            attachments: Vec::new(),
        }
    }
    fn result(text: &str) -> FinalResult {
        FinalResult {
            summary: Some(text.into()),
            code: None,
            goal_reached: None,
            value: None,
        }
    }

    #[test]
    fn kinds_and_entries_round_trip_and_t1_refuses_a_reference_to_a_reference() {
        let fixture = Fixture::new("codecs");
        let (conversation, first) = fixture.open();
        fixture.finish(first, OutcomeKind::Completed);
        let second = wait(fixture.database.append(
            conversation,
            Entry::RetryOf {
                turn: first,
                by: Actor::User,
            },
            None,
            None,
        ))
        .expect("reference")
        .turn;
        let configuration = wait(fixture.database.job_configuration(first))
            .expect("configuration")
            .expect("frozen")
            .frozen;
        let handle = fixture.database.clone();
        wait(handle.with_database(move |database| {
            let kinds = [
                JobKind::Conversation {
                    agent: None,
                    project: None,
                    origin: Origin::User,
                    keep_open: true,
                },
                JobKind::Conversation {
                    agent: None,
                    project: None,
                    origin: Origin::System,
                    keep_open: false,
                },
                JobKind::Conversation {
                    agent: Some(ConfigId::new(1)),
                    project: Some(ProjectId::new(2)),
                    origin: Origin::Agent {
                        call: ToolCallRef {
                            job: first,
                            request: RequestNumber::FIRST,
                            index: 2,
                        },
                    },
                    keep_open: false,
                },
                JobKind::Turn {
                    execution: TurnExecution::Frozen(configuration),
                    plan_source: PlanSource::Process {
                        project: Some(ProjectId::new(2)),
                        grant: Some("read".into()),
                    },
                },
                JobKind::Turn {
                    execution: TurnExecution::Ref(first),
                    plan_source: PlanSource::Stored,
                },
            ];
            for kind in kinds {
                let row = JobRow {
                    id: first,
                    kind,
                    created_at: Timestamp::now(),
                    eligible_at: None,
                    name: None,
                    parent_id: Some(conversation),
                    objective: None,
                    attachments: Vec::new(),
                    dependencies: Vec::new(),
                    previous: None,
                    submission: None,
                };
                assert_eq!(JobRow::decode(&row.encode())?, row);
            }
            let call = ToolCallRef {
                job: first,
                request: RequestNumber::FIRST,
                index: 0,
            };
            for entry in [
                message("user"),
                Entry::AgentMessage {
                    text: "agent".into(),
                    call,
                },
                Entry::Notice {
                    child: first,
                    event: NoticeEvent::ClosedSelf,
                    reference: first,
                },
                Entry::Decision {
                    approval: (first, 1, 2),
                },
                Entry::RetryOf {
                    turn: first,
                    by: Actor::User,
                },
                Entry::Iteration { n: 300 },
            ] {
                let row = TurnEntry {
                    entry,
                    at: Timestamp::now(),
                };
                assert_eq!(TurnEntry::decode(&row.encode())?, row);
            }
            let outcome = run_batch_with(database, 1, Commit::Durable, |batch| {
                let mut row = batch.require_job(second)?;
                row.kind = JobKind::Turn {
                    execution: TurnExecution::Ref(second),
                    plan_source: PlanSource::Stored,
                };
                row.previous = Some(second);
                batch.t1_submit(None, row)
            });
            assert!(matches!(outcome.result, Err(BatchFailure::Rejected(_))));
            Ok(())
        }))
        .expect("codec checks");
        fixture.worker.shutdown().expect("shutdown");
    }
    #[test]
    fn hiding_a_conversation_removes_recent_but_keeps_its_turns() {
        let fixture = Fixture::new("hide");
        let (conversation, turn) = fixture.open();
        assert_eq!(
            wait(fixture.database.conversations(None, 20))
                .expect("list")
                .conversations
                .len(),
            1
        );
        wait(
            fixture
                .database
                .set_conversation(conversation, crate::persistence::ConversationEdit::Hide),
        )
        .expect("hide");
        assert!(
            wait(fixture.database.conversations(None, 20))
                .expect("list")
                .conversations
                .is_empty()
        );
        assert_eq!(
            wait(fixture.database.conversation_turns(conversation, None, 20)).expect("turns")[0].id,
            turn
        );
        fixture.worker.shutdown().expect("shutdown");
    }
    #[test]
    fn k1_round2_completed_answer_survives_stop_and_next_turn_runs() {
        use crate::persistence::{JobFinalization, StopCause};
        use std::sync::{Arc, Mutex};
        let fixture = Fixture::new("round2-stop");
        let (conversation, turn) = fixture.open();
        fixture.claim(turn);
        let stopped = wait(fixture.database.stop_turn(conversation, Actor::User)).expect("stop");
        let record = stopped.record_seq.expect("record");
        let handle = fixture
            .database
            .with_stop_cause(Arc::new(Mutex::new(Some(StopCause::Stopped(record)))));
        wait(handle.finalize_job(turn, JobFinalization::Completed(result("finished answer"))))
            .expect("complete");
        let outcome = wait(fixture.database.job(turn))
            .expect("read")
            .expect("turn")
            .outcome
            .expect("outcome");
        assert_eq!(
            (outcome.kind, outcome.cause),
            (OutcomeKind::Completed, None)
        );
        let outputs = wait(fixture.database.job_outputs(turn, None, 10)).expect("outputs");
        assert_eq!(
            outputs.last().expect("Result").summary.as_deref(),
            Some("finished answer")
        );
        assert!(
            wait(fixture.database.notices(conversation, 0, 20))
                .expect("notices")
                .iter()
                .any(|(seq, notice)| *seq == record
                    && matches!(
                        notice.body,
                        NoticeBody::Record {
                            event: RecordEvent::Stop,
                            ..
                        }
                    ))
        );
        let next = wait(
            fixture
                .database
                .append(conversation, message("next"), None, None),
        )
        .expect("next")
        .turn;
        fixture.finish(next, OutcomeKind::Completed);
        assert_eq!(
            wait(fixture.database.job(next))
                .expect("read")
                .expect("next")
                .outcome
                .expect("outcome")
                .kind,
            OutcomeKind::Completed
        );
        let cancelled = wait(fixture.database.append(
            conversation,
            message("stop before sending"),
            None,
            None,
        ))
        .expect("next")
        .turn;
        fixture.claim(cancelled);
        let record = wait(fixture.database.stop_turn(conversation, Actor::User))
            .expect("stop")
            .record_seq
            .expect("record");
        let handle = fixture
            .database
            .with_stop_cause(Arc::new(Mutex::new(Some(StopCause::Stopped(record)))));
        wait(handle.finalize_job(cancelled, JobFinalization::Failed(result("NotDelivered"))))
            .expect("cancel before send");
        let outcome = wait(fixture.database.job(cancelled))
            .expect("read")
            .expect("turn")
            .outcome
            .expect("outcome");
        assert_eq!(
            (outcome.kind, outcome.cause),
            (
                OutcomeKind::Cancelled,
                Some(OutcomeCause::Stopped { record })
            )
        );
        fixture.worker.shutdown().expect("shutdown");
    }

    #[test]
    fn k1_round2_close_notifies_a_child_only_once() {
        let fixture = Fixture::new("round2-close");
        let (parent, _) = fixture.open();
        let (child, turn) = fixture.child(parent, true);
        fixture.claim(turn);
        let first = wait(fixture.database.close(child, Actor::User)).expect("close child");
        assert!(!first.already);
        assert_eq!(first.running, [turn]);
        let second = wait(fixture.database.close(child, Actor::User)).expect("close twice");
        assert!(second.already && second.running.is_empty());
        wait(fixture.database.close(parent, Actor::User)).expect("close parent");
        let notices = wait(fixture.database.notices(parent, 0, 100)).expect("notices");
        assert_eq!(notices.iter().filter(|(_, notice)| matches!(notice.body, NoticeBody::Event { child: found, event: NoticeEvent::ClosedAbove, .. } if found == child)).count(), 1);
        wait(fixture.database.finalize_job(
            turn,
            crate::persistence::JobFinalization::Completed(result("finished")),
        ))
        .expect("finish");
        let outcome = wait(fixture.database.job(turn))
            .expect("read")
            .expect("turn")
            .outcome
            .expect("outcome");
        assert_eq!(
            (outcome.kind, outcome.cause),
            (OutcomeKind::Completed, None)
        );
        assert_eq!(
            wait(fixture.database.job(child))
                .expect("read")
                .expect("child")
                .outcome
                .expect("outcome")
                .kind,
            OutcomeKind::Cancelled
        );
        assert!(
            wait(fixture.database.close(child, Actor::User))
                .expect("settled close")
                .already
        );
        fixture.worker.shutdown().expect("shutdown");
    }

    #[test]
    fn k1_round2_unset_context_window_bounds_the_read() {
        use crate::orchestration::execution_runtime::ContextLimits;
        let fixture = Fixture::new("round2-context");
        let (conversation, mut turn) = fixture.open();
        let frozen = wait(fixture.database.turn_execution(turn))
            .expect("frozen")
            .0;
        assert_eq!(frozen.llm.config.context_window, None);
        let limits = ContextLimits::for_profile(&frozen.llm.config);
        for _ in 0..6 {
            fixture.finish(turn, OutcomeKind::Completed);
            turn = wait(fixture.database.append(
                conversation,
                message(&"x".repeat(100_000)),
                None,
                None,
            ))
            .expect("append")
            .turn;
        }
        let context = wait(
            fixture
                .database
                .read_turn_context(turn, limits.read_budget()),
        )
        .expect("context");
        assert!(context.omitted);
        assert!(context.turns.len() <= 3);
        assert_eq!(context.turns.last().expect("current").turn, turn);
        assert_eq!(limits.context_window, None);
        fixture.worker.shutdown().expect("shutdown");
    }

    #[test]
    fn k1_round2_create_job_cannot_add_a_conversation_turn() {
        let fixture = Fixture::new("round2-create");
        let (conversation, _) = fixture.open();
        let job = crate::persistence::NewJob {
            process_grant: None,
            name: None,
            eligible_at: None,
            parent: conversation,
            project: None,
            dependencies: vec![],
            input: Default::default(),
            configuration: Some(fixture.configuration),
            previous: None,
        };
        assert!(matches!(
            wait(fixture.database.create_job(job)),
            Err(crate::database::DatabaseError::Rejected(
                Violation::InvalidValue(
                    "parent",
                    "turns are created by append or open_conversation"
                )
            ))
        ));
        fixture.worker.shutdown().expect("shutdown");
    }

    #[test]
    fn k1_round2_replay_preserves_the_turn_creating_joined_flag() {
        let fixture = Fixture::new("round2-replay");
        let (conversation, first) = fixture.open();
        fixture.finish(first, OutcomeKind::Completed);
        let submission = NewSubmission {
            key: SubmissionKey::from_digest([3; 32]),
            fingerprint: RequestFingerprint::from_digest([4; 32]),
        };
        let created = wait(fixture.database.append(
            conversation,
            message("next"),
            None,
            Some(submission),
        ))
        .expect("create");
        assert!(!created.joined);
        assert_eq!(
            wait(
                fixture
                    .database
                    .append(conversation, message("next"), None, Some(submission))
            )
            .expect("replay before claim"),
            created
        );
        fixture.claim(created.turn);
        assert_eq!(
            wait(
                fixture
                    .database
                    .append(conversation, message("next"), None, Some(submission))
            )
            .expect("replay after claim"),
            created
        );
        fixture.worker.shutdown().expect("shutdown");
    }

    #[test]
    fn a_claim_freezes_input_and_a_submission_key_names_one_entry() {
        let fixture = Fixture::new("input-freeze");
        let (conversation, turn) = fixture.open();
        let submission = NewSubmission {
            key: SubmissionKey::from_digest([1; 32]),
            fingerprint: RequestFingerprint::from_digest([2; 32]),
        };
        let first = wait(fixture.database.append(
            conversation,
            message("second"),
            None,
            Some(submission),
        ))
        .expect("join");
        let resent = wait(fixture.database.append(
            conversation,
            message("second"),
            None,
            Some(submission),
        ))
        .expect("resend");
        assert_eq!(first, resent);
        assert_eq!((first.turn, first.seq, first.joined), (turn, 2, true));
        fixture.claim(turn);
        wait(fixture.database.with_database(move |database| {
            let outcome = run_batch_with(database, 1, Commit::Durable, |batch| {
                batch.insert_turn_input(turn, message("late"))
            });
            assert!(matches!(outcome.result, Err(BatchFailure::Rejected(_))));
            Ok(())
        }))
        .expect("refused insert");
        fixture.worker.shutdown().expect("shutdown");
    }
    #[test]
    fn child_failure_wakes_parent_and_agent_completion_is_progress() {
        let fixture = Fixture::new("notices");
        let (parent, waiting) = fixture.open();
        let (child, first) = fixture.child(parent, true);
        fixture.finish(first, OutcomeKind::Failed);
        assert!(wait(fixture.database.turn_entries(waiting)).expect("entries").iter().any(|(_, entry)| matches!(entry.entry, Entry::Notice { child: found, .. } if found == child)));
        let next = wait(fixture.database.append(
            child,
            Entry::RetryOf {
                turn: first,
                by: Actor::Auto,
            },
            None,
            None,
        ))
        .expect("retry")
        .turn;
        let before = wait(fixture.database.turn_entries(waiting))
            .expect("before")
            .len();
        fixture.finish(next, OutcomeKind::Completed);
        assert_eq!(
            wait(fixture.database.turn_entries(waiting))
                .expect("after")
                .len(),
            before
        );
        assert_eq!(
            wait(fixture.database.notices(parent, 0, 20))
                .expect("notice")
                .last()
                .expect("progress")
                .1
                .class,
            NoticeClass::Progress
        );
        fixture.worker.shutdown().expect("shutdown");
    }
    #[test]
    fn all_conversation_indexes_rebuild_from_facts() {
        let fixture = Fixture::new("rebuild");
        let (conversation, turn) = fixture.open();
        let _child = fixture.child(conversation, true);
        fixture.claim(turn);
        wait(fixture.database.with_database(move |database| {
            let tx = database.begin_write()?;
            tx.open_table(TIPS)?.insert(999, 999)?;
            tx.open_table(RECENT)?.insert((999, 999), ())?;
            tx.open_table(CHILD_CONVERSATIONS)?.insert((999, 999), ())?;
            tx.open_table(LIVE_CHILDREN)?.insert((999, 999), ())?;
            tx.open_table(USER_ENTRY_AT)?.insert(999, 999)?;
            tx.open_table(NOTICE_READ)?.insert(999, 999)?;
            tx.open_table(ANSWER_TIPS)?.insert(999, (999, 999))?;
            tx.open_table(ACTIVE_TURNS)?.insert(999, 999)?;
            tx.commit()?;
            let inconsistent = derived::inconsistencies(database)?;
            for name in [
                "tip",
                "recent",
                "child_conversation",
                "live_child",
                "user_entry_at",
                "notice_read",
                "answer_tip",
                "active_turn",
            ] {
                assert!(inconsistent.contains(&name), "{name}");
            }
            let tx = database.begin_write()?;
            derived::rebuild(&tx, open::promotion_clock())?;
            tx.commit()?;
            assert!(derived::inconsistencies(database)?.is_empty());
            Ok(())
        }))
        .expect("rebuild");
        fixture.worker.shutdown().expect("shutdown");
    }
    #[test]
    fn running_turn_gets_one_waiting_successor_and_messages_join_until_claim() {
        let fixture = Fixture::new("append");
        let (conversation, first) = fixture.open();
        fixture.claim(first);
        let second = wait(fixture.database.append(
            conversation,
            message("second"),
            Some(fixture.configuration),
            None,
        ))
        .expect("second");
        let joined = wait(
            fixture
                .database
                .append(conversation, message("third"), None, None),
        )
        .expect("join");
        assert_eq!(
            (joined.turn, joined.seq, joined.joined),
            (second.turn, 2, true)
        );
        wait(fixture.database.finalize_job(
            first,
            crate::persistence::JobFinalization::Completed(result("first answer")),
        ))
        .expect("first");
        fixture.claim(second.turn);
        let third = wait(
            fixture
                .database
                .append(conversation, message("fourth"), None, None),
        )
        .expect("next");
        assert_ne!(third.turn, second.turn);
        assert_eq!(
            wait(fixture.database.job(third.turn))
                .expect("read")
                .expect("turn")
                .previous,
            Some(second.turn)
        );
        fixture.worker.shutdown().expect("shutdown");
    }
    #[test]
    fn context_is_windowed_without_a_turn_limit_and_skips_cancelled_answers() {
        let fixture = Fixture::new("context-window");
        let (conversation, mut turn) = fixture.open();
        for n in 0..300 {
            fixture.finish(
                turn,
                if n == 299 {
                    OutcomeKind::Cancelled
                } else {
                    OutcomeKind::Completed
                },
            );
            turn = wait(fixture.database.append(
                conversation,
                message(&format!("message {n}")),
                None,
                None,
            ))
            .expect("append")
            .turn;
        }
        let context = wait(fixture.database.read_turn_context(turn, 60)).expect("context");
        assert!(context.omitted);
        assert!(context.turns.len() < 10);
        assert_eq!(context.turns.last().expect("current").turn, turn);
        assert!(
            context
                .turns
                .iter()
                .any(|turn| turn.text == "message 298" && turn.answer.is_none())
        );
        fixture.worker.shutdown().expect("shutdown");
    }
    #[test]
    fn stored_pending_turn_survives_reopen_and_process_pending_is_abandoned() {
        let fixture = Fixture::new("reopen");
        let (_, stored) = fixture.open();
        let process = wait(fixture.database.create_job(crate::persistence::NewJob {
            process_grant: None,
            name: None,
            eligible_at: None,
            parent: fixture.database.genesis(),
            project: None,
            dependencies: Vec::new(),
            input: Default::default(),
            configuration: Some(fixture.configuration),
            previous: None,
        }))
        .expect("process");
        fixture.worker.shutdown().expect("shutdown");
        let worker = DatabaseWorker::start(&fixture.directory).expect("reopen");
        let database = worker.handle();
        assert!(
            wait(database.job(stored))
                .expect("read")
                .expect("stored")
                .awaits_start()
        );
        assert_eq!(
            wait(database.job(process))
                .expect("read")
                .expect("process")
                .outcome
                .expect("outcome")
                .cause,
            Some(OutcomeCause::Abandoned)
        );
        assert_eq!(wait(database.claim_batch()).expect("ready"), [stored]);
        worker.shutdown().expect("shutdown");
    }
    #[test]
    fn stop_cancels_waiting_input_and_the_next_turn_reads_it() {
        let fixture = Fixture::new("stop");
        let (conversation, running) = fixture.open();
        fixture.claim(running);
        let waiting = wait(fixture.database.append(
            conversation,
            message("queued input"),
            None,
            None,
        ))
        .expect("queue")
        .turn;
        let stopped = wait(fixture.database.stop_turn(conversation, Actor::User)).expect("stop");
        assert_eq!(stopped.running, [running]);
        let record = stopped.record_seq.expect("record");
        assert_eq!(
            wait(fixture.database.job(waiting))
                .expect("read")
                .expect("waiting")
                .outcome
                .expect("outcome")
                .cause,
            Some(OutcomeCause::Stopped { record })
        );
        wait(fixture.database.finalize_job(
            running,
            crate::persistence::JobFinalization::Cancelled(result("stopped")),
        ))
        .expect("stop running");
        let next = wait(
            fixture
                .database
                .append(conversation, message("continue"), None, None),
        )
        .expect("continue")
        .turn;
        assert!(
            wait(fixture.database.read_turn_context(next, 1000))
                .expect("context")
                .turns
                .iter()
                .any(|turn| turn.text.contains("queued input"))
        );
        fixture.worker.shutdown().expect("shutdown");
    }
    #[test]
    fn auto_close_writes_only_closed_self_and_idle_close_settles_in_its_transaction() {
        let fixture = Fixture::new("close");
        let (parent, waiting) = fixture.open();
        let (child, turn) = fixture.child(parent, false);
        fixture.finish(turn, OutcomeKind::Completed);
        assert!(
            wait(fixture.database.job(child))
                .expect("read")
                .expect("child")
                .settled()
        );
        let notices = wait(fixture.database.turn_entries(waiting))
            .expect("entries")
            .into_iter()
            .filter(|(_, entry)| matches!(entry.entry, Entry::Notice { .. }))
            .collect::<Vec<_>>();
        assert_eq!(notices.len(), 1);
        assert!(matches!(
            notices[0].1.entry,
            Entry::Notice {
                event: NoticeEvent::ClosedSelf,
                ..
            }
        ));
        wait(fixture.database.close(parent, Actor::User)).expect("close");
        assert!(
            wait(fixture.database.job(parent))
                .expect("read")
                .expect("parent")
                .settled()
        );
        fixture.worker.shutdown().expect("shutdown");
    }
    #[test]
    fn a_user_hold_covers_child_turns_and_unhold_places_them() {
        let fixture = Fixture::new("hold");
        let (parent, root_turn) = fixture.open();
        let (_, child_turn) = fixture.child(parent, true);
        wait(fixture.database.hold(parent, Actor::User)).expect("hold");
        assert!(
            wait(fixture.database.claim_batch())
                .expect("held")
                .is_empty()
        );
        wait(fixture.database.unhold(parent, Actor::User)).expect("unhold");
        let claimed = wait(fixture.database.claim_batch()).expect("placed");
        assert!(claimed.contains(&root_turn) && claimed.contains(&child_turn));
        fixture.worker.shutdown().expect("shutdown");
    }
    #[test]
    fn system_conversations_refuse_every_user_mutation() {
        let fixture = Fixture::new("system");
        let agent = wait(
            fixture
                .database
                .create_config(crate::persistence::NewConfig {
                    name: "system Agent".into(),
                    body: crate::persistence::ConfigBody::Agent(AgentConfig {
                        dispatch_to: Vec::new(),
                        models: vec![fixture.configuration.llm_profile.into()],
                        ..Default::default()
                    }),
                }),
        )
        .expect("agent");
        let (conversation, appended) = wait(fixture.database.open_conversation(
            None,
            ConversationSpec {
                name: None,
                agent: Some(agent),
                project: None,
                origin: Origin::System,
                keep_open: true,
            },
            message("system"),
            ExecutionConfiguration {
                agent_profile: Some(agent.into()),
                ..fixture.configuration
            },
            None,
        ))
        .expect("system fixture");
        for result in [
            wait(fixture.database.stop_turn(conversation, Actor::User)),
            wait(fixture.database.close(conversation, Actor::User)),
            wait(fixture.database.hold(conversation, Actor::User)),
            wait(fixture.database.unhold(conversation, Actor::User)),
        ] {
            assert!(matches!(
                result,
                Err(crate::database::DatabaseError::Rejected(
                    Violation::InvalidValue("system_conversation", _)
                ))
            ));
        }
        assert!(
            wait(
                fixture
                    .database
                    .retry(conversation, appended.turn, Actor::User)
            )
            .is_err()
        );
        assert!(
            wait(
                fixture
                    .database
                    .append(conversation, message("user"), None, None)
            )
            .is_err()
        );
        fixture.worker.shutdown().expect("shutdown");
    }
    #[test]
    fn retry_is_an_entry_and_reuses_the_frozen_owner() {
        let fixture = Fixture::new("retry");
        let (conversation, first) = fixture.open();
        fixture.finish(first, OutcomeKind::Unknown);
        let retry = wait(fixture.database.retry(conversation, first, Actor::User)).expect("retry");
        assert!(
            matches!(wait(fixture.database.turn_entries(retry.turn)).expect("entry")[0].1.entry, Entry::RetryOf { turn, .. } if turn == first)
        );
        assert!(
            matches!(wait(fixture.database.job(retry.turn)).expect("read").expect("retry").kind, JobKind::Turn { execution: TurnExecution::Ref(owner), .. } if owner == first)
        );
        assert!(
            wait(fixture.database.read_turn_context(retry.turn, 1000))
                .expect("context")
                .turns
                .last()
                .expect("retry input")
                .text
                .contains("uncertain")
        );
        assert_eq!(
            wait(fixture.database.retries(first)).expect("retries"),
            [retry.turn]
        );
        fixture.worker.shutdown().expect("shutdown");
    }
    #[test]
    fn close_waits_for_both_running_children_and_unknown_conversation_has_settled_proof() {
        let fixture = Fixture::new("close-running");
        let (parent, _) = fixture.open();
        let (left, a) = fixture.child(parent, true);
        let (right, b) = fixture.child(parent, true);
        fixture.claim(a);
        fixture.claim(b);
        let closed = wait(fixture.database.close(parent, Actor::User)).expect("close");
        assert_eq!(closed.running, [a, b]);
        wait(
            fixture
                .database
                .finalize_job(a, crate::persistence::JobFinalization::Unknown),
        )
        .expect("first stop");
        assert!(
            !wait(fixture.database.job(parent))
                .expect("read")
                .expect("parent")
                .settled()
        );
        assert!(
            wait(fixture.database.job(left))
                .expect("read")
                .expect("child")
                .settled()
        );
        wait(
            fixture
                .database
                .finalize_job(b, crate::persistence::JobFinalization::Unknown),
        )
        .expect("second stop");
        assert!(
            wait(fixture.database.job(right))
                .expect("read")
                .expect("child")
                .settled()
        );
        assert!(
            wait(fixture.database.job(parent))
                .expect("read")
                .expect("parent")
                .settled()
        );
        let (unknown, turn) = fixture.open();
        fixture.finish(turn, OutcomeKind::Unknown);
        wait(fixture.database.close(unknown, Actor::User)).expect("close unknown");
        let record = wait(fixture.database.job(unknown))
            .expect("read")
            .expect("conversation");
        assert!(record.settled());
        assert_eq!(record.stop_confirmed_by, Some(StopProof::Settled));
        fixture.worker.shutdown().expect("shutdown");
    }
    #[test]
    fn quarantine_cannot_be_bypassed_through_a_cancelled_waiting_tip() {
        let fixture = Fixture::new("quarantine-queue");
        let (conversation, running) = fixture.open();
        fixture.claim(running);
        let call = ToolCallRef {
            job: running,
            request: RequestNumber::FIRST,
            index: 0,
        };
        wait(
            fixture
                .database
                .record_metrics(crate::persistence::NewMetrics {
                    job: running,
                    request: RequestNumber::FIRST,
                    measurements: Default::default(),
                    token: Some(crate::token_usage::TokenAnnotation::eligible(
                        crate::token_usage::UsageFinality::Cumulative,
                    )),
                    observed_at: Timestamp::now(),
                }),
        )
        .expect("request");
        wait(
            fixture
                .database
                .record_tool_decision(crate::persistence::NewToolEvidence {
                    call,
                    tool_call_id: "spawn".into(),
                    name: "workspace_run_command".into(),
                    decision: crate::persistence::ToolDecision::Admitted(
                        crate::persistence::ToolAdmission {
                            handler_version: "test-1".into(),
                            effect: crate::persistence::ToolEffect::NonIdempotent,
                            parameters: "{}".into(),
                            arguments: crate::persistence::ToolPayload::new("{}"),
                            authorization: crate::persistence::AuthorizationBasis::RegistryGrant,
                        },
                    ),
                    spawns: true,
                }),
        )
        .expect("started");
        wait(
            fixture
                .database
                .append(conversation, message("queued"), None, None),
        )
        .expect("waiting");
        wait(fixture.database.stop_turn(conversation, Actor::User)).expect("stop");
        wait(
            fixture
                .database
                .finalize_job(running, crate::persistence::JobFinalization::Unknown),
        )
        .expect("quarantine");
        let retry =
            wait(fixture.database.retry(conversation, running, Actor::User)).expect("retry");
        let context = wait(fixture.database.read_turn_context(retry.turn, 1000)).expect("context");
        assert!(
            context
                .turns
                .last()
                .expect("retry")
                .text
                .contains("Uncertain: request 1, index 0, tool workspace_run_command")
        );
        assert!(
            wait(fixture.database.claim_batch())
                .expect("quarantined")
                .is_empty()
        );
        wait(
            fixture
                .database
                .confirm_job_stopped(running, StopProof::Operator),
        )
        .expect("release");
        assert_eq!(
            wait(fixture.database.claim_batch()).expect("released"),
            [retry.turn]
        );
        fixture.worker.shutdown().expect("shutdown");
    }
    #[test]
    fn notice_variants_and_stop_evidence_round_trip() {
        let now = Timestamp::now();
        let job = JobId::new(5);
        for event in [
            NoticeEvent::ClosedSelf,
            NoticeEvent::ClosedAbove,
            NoticeEvent::StoppedAbove,
            NoticeEvent::Requested,
            NoticeEvent::Stalled,
            NoticeEvent::ObjectiveCompleted,
            NoticeEvent::WaitersBlocked { waiting: vec![job] },
            NoticeEvent::IterationCompleted,
            NoticeEvent::RecurrenceHalted,
            NoticeEvent::TurnEnded {
                kind: OutcomeKind::Failed,
            },
        ] {
            let notice = Notice {
                class: NoticeClass::Attention,
                body: NoticeBody::Event {
                    child: job,
                    event,
                    reference: job,
                },
            };
            assert_eq!(Notice::decode(&notice.encode()).expect("decode"), notice);
        }
        for actor in [
            Actor::Auto,
            Actor::User,
            Actor::Loop,
            Actor::Agent { conversation: job },
        ] {
            for event in [RecordEvent::Hold, RecordEvent::Unhold, RecordEvent::Stop] {
                let notice = Notice {
                    class: NoticeClass::Record,
                    body: NoticeBody::Record {
                        event,
                        by: actor.clone(),
                        at: now,
                    },
                };
                assert_eq!(Notice::decode(&notice.encode()).expect("decode"), notice);
            }
        }
        for proof in [
            StopProof::Proof,
            StopProof::Reboot,
            StopProof::Settled,
            StopProof::User,
            StopProof::Operator,
        ] {
            let mut state = JobStateRow::new(job);
            state.stop_confirmed_at = Some(now);
            state.stop_confirmed_by = Some(proof);
            assert_eq!(JobStateRow::decode(&state.encode()).expect("decode"), state);
        }
    }
    #[test]
    fn child_limits_count_live_conversations_and_free_capacity_on_close() {
        let fixture = Fixture::new("child-limits");
        let (parent, _) = fixture.open();
        let mut children = Vec::new();
        for _ in 0..16 {
            children.push(fixture.child(parent, true).0);
        }
        let spec = ConversationSpec {
            name: None,
            agent: None,
            project: None,
            origin: Origin::User,
            keep_open: true,
        };
        assert!(matches!(
            wait(fixture.database.open_conversation(
                Some(parent),
                spec.clone(),
                message("over limit"),
                fixture.configuration,
                None
            )),
            Err(crate::database::DatabaseError::Rejected(
                Violation::InvalidValue("children", _)
            ))
        ));
        wait(fixture.database.close(children[0], Actor::User)).expect("close one child");
        assert!(
            wait(fixture.database.open_conversation(
                Some(parent),
                spec,
                message("replacement"),
                fixture.configuration,
                None
            ))
            .is_ok()
        );
        fixture.worker.shutdown().expect("shutdown");
    }
    #[test]
    fn open_and_append_wake_only_when_they_place_a_turn() {
        let mut fixture = Fixture::new("placement-wake");
        let wake = fixture.worker.take_scheduler_wake().expect("wake");
        let initial = wake.dirty_generation();
        let (conversation, first) = fixture.open();
        assert!(wake.dirty_generation() > initial);
        fixture.claim(first);
        let before = wake.dirty_generation();
        let queued = wait(
            fixture
                .database
                .append(conversation, message("queued"), None, None),
        )
        .expect("append");
        wait(
            fixture
                .database
                .append(conversation, message("joined"), None, None),
        )
        .expect("join");
        assert_eq!(wake.dirty_generation(), before);
        wait(fixture.database.hold(conversation, Actor::User)).expect("hold");
        let held = wake.dirty_generation();
        assert!(held > before);
        wait(fixture.database.finalize_job(
            first,
            crate::persistence::JobFinalization::Completed(result("done")),
        ))
        .expect("finish");
        assert!(
            wait(fixture.database.claim_batch())
                .expect("held")
                .is_empty()
        );
        wait(fixture.database.unhold(conversation, Actor::User)).expect("unhold");
        assert!(wake.dirty_generation() > held);
        assert_eq!(
            wait(fixture.database.claim_batch()).expect("placed"),
            [queued.turn]
        );
        fixture.worker.shutdown().expect("shutdown");
    }
    impl Fixture {
        fn dispatch_parent(&self) -> (JobId, JobId, ConfigId) {
            use crate::persistence::{ConfigBody, NewConfig};
            let mut model = wait(self.database.config(self.configuration.llm_profile.into()))
                .unwrap()
                .unwrap();
            if let ConfigBody::Model(body) = &mut model.body {
                body.declared_capabilities.insert(ModelCapability::Tools);
            }
            wait(self.database.update_config(
                model.id,
                NewConfig {
                    name: model.name,
                    body: model.body,
                },
            ))
            .unwrap();
            let target = wait(self.database.create_config(NewConfig {
                name: "dispatch target".into(),
                body: ConfigBody::Agent(AgentConfig {
                    models: vec![self.configuration.llm_profile.into()],
                    default_model: Some(self.configuration.llm_profile.into()),
                    ..Default::default()
                }),
            }))
            .unwrap();
            let agent = wait(self.database.create_config(NewConfig {
                name: "dispatcher".into(),
                body: ConfigBody::Agent(AgentConfig {
                    dispatch_to: vec![target],
                    models: vec![self.configuration.llm_profile.into()],
                    ..Default::default()
                }),
            }))
            .unwrap();
            let (parent, appended) = wait(self.database.open_conversation(
                None,
                ConversationSpec {
                    name: None,
                    agent: Some(agent),
                    project: None,
                    origin: Origin::User,
                    keep_open: true,
                },
                message("dispatch"),
                ExecutionConfiguration {
                    agent_profile: Some(agent.into()),
                    ..self.configuration
                },
                None,
            ))
            .unwrap();
            self.claim_observed(appended.turn);
            (parent, appended.turn, target)
        }
        fn claim_observed(&self, turn: JobId) {
            self.claim(turn);
            wait(
                self.database
                    .record_metrics(crate::persistence::NewMetrics {
                        job: turn,
                        request: RequestNumber::FIRST,
                        measurements: Default::default(),
                        token: Some(crate::token_usage::TokenAnnotation::eligible(
                            crate::token_usage::UsageFinality::Cumulative,
                        )),
                        observed_at: Timestamp::now(),
                    }),
            )
            .unwrap();
        }
        fn admit(
            &self,
            turn: JobId,
            index: u32,
            tool: &crate::orchestration::job_tools::JobToolCall,
        ) -> ToolCallRef {
            use crate::persistence::*;
            let call = ToolCallRef {
                job: turn,
                request: RequestNumber::FIRST,
                index,
            };
            wait(self.database.record_tool_decision(NewToolEvidence {
                call,
                tool_call_id: format!("call-{index}"),
                name: tool.name().into(),
                decision: ToolDecision::Admitted(ToolAdmission {
                    handler_version: format!("{}-1", tool.name()),
                    effect: ToolEffect::NonIdempotent,
                    parameters: "{}".into(),
                    arguments: ToolPayload::new("{}"),
                    authorization: AuthorizationBasis::RegistryGrant,
                }),
                spawns: false,
            }))
            .unwrap();
            call
        }
        fn apply(
            &self,
            turn: JobId,
            index: u32,
            tool: crate::orchestration::job_tools::JobToolCall,
        ) -> serde_json::Value {
            let call = self.admit(turn, index, &tool);
            wait(self.database.apply_job_tool(call, tool)).unwrap()
        }
        fn dispatch(
            &self,
            turn: JobId,
            index: u32,
            agent: ConfigId,
            keep_open: bool,
            every_seconds: Option<u32>,
            depends_on: Vec<crate::orchestration::job_tools::JobDependency>,
        ) -> (JobId, JobId) {
            let value = self.apply(
                turn,
                index,
                crate::orchestration::job_tools::JobToolCall::JobDispatch {
                    agent: ProfileId::from(agent).to_string(),
                    project: None,
                    objective: "objective".into(),
                    keep_open,
                    every_seconds,
                    depends_on,
                },
            );
            (
                JobId::new(
                    value["conversation"].as_str().unwrap()[4..]
                        .parse()
                        .unwrap(),
                ),
                JobId::new(value["turn"].as_str().unwrap()[4..].parse().unwrap()),
            )
        }
        fn complete_claimed(&self, turn: JobId) {
            wait(self.database.finalize_job(
                turn,
                crate::persistence::JobFinalization::Completed(result("done")),
            ))
            .unwrap();
        }
        fn recurrence(&self, conversation: JobId) -> Recurrence {
            wait(self.database.with_database(move |database| {
                let rx = database.begin_read()?;
                Recurrence::decode(
                    rx.open_table(RECURRENCES)?
                        .get(conversation.get())?
                        .unwrap()
                        .value(),
                )
            }))
            .unwrap()
        }
    }

    #[test]
    fn dispatch_tools_refuse_self_and_stop_a_running_child_atomically() {
        use crate::orchestration::job_tools::JobToolCall;
        let fixture = Fixture::new("dispatch-rights");
        let (parent, turn, agent) = fixture.dispatch_parent();
        let tool = JobToolCall::JobStop {
            conversation: parent.to_string(),
        };
        let call = fixture.admit(turn, 0, &tool);
        let refusal = wait(fixture.database.apply_job_tool(call, tool)).unwrap_err();
        assert!(
            matches!(refusal, crate::database::DatabaseError::Rejected(crate::persistence::Violation::InvalidValue("rights", reason)) if !reason.is_empty())
        );
        wait(fixture.database.record_tool_outcome(
            call,
            crate::persistence::ToolOutcome::Completed {
                result: crate::persistence::ToolPayload::new(
                    "{\"refused\":\"rights\",\"reason\":\"strict subtree required\"}",
                ),
            },
        ))
        .unwrap();
        let (child, child_turn) = fixture.dispatch(turn, 1, agent, true, None, vec![]);
        fixture.claim(child_turn);
        let stopped = fixture.apply(
            turn,
            2,
            JobToolCall::JobStop {
                conversation: child.to_string(),
            },
        );
        assert_eq!(
            stopped["running"],
            serde_json::json!([child_turn.to_string()])
        );
        let record = stopped["record_seq"].as_u64().unwrap() as u32;
        wait(
            fixture
                .database
                .with_stop_cause(std::sync::Arc::new(std::sync::Mutex::new(Some(
                    crate::persistence::StopCause::Stopped(record),
                ))))
                .finalize_job(
                    child_turn,
                    crate::persistence::JobFinalization::Cancelled(result("stopped")),
                ),
        )
        .unwrap();
        assert_eq!(
            wait(fixture.database.job(child_turn))
                .unwrap()
                .unwrap()
                .outcome
                .unwrap()
                .cause,
            Some(crate::persistence::OutcomeCause::Stopped { record })
        );
        assert!(
            wait(fixture.database.tool_evidence(turn, None, 100))
                .unwrap()
                .iter()
                .all(|call| call.outcome.is_some())
        );
        fixture.worker.shutdown().unwrap();
    }

    #[test]
    fn dispatched_sibling_dependency_waits_for_auto_close_and_user_entry_names_waiters() {
        use crate::orchestration::job_tools::JobDependency;
        let fixture = Fixture::new("dispatch-dependency");
        let (parent, turn, agent) = fixture.dispatch_parent();
        let (child, child_turn) = fixture.dispatch(turn, 0, agent, false, None, vec![]);
        let (waiter, waiter_turn) = fixture.dispatch(
            turn,
            1,
            agent,
            false,
            None,
            vec![JobDependency {
                conversation: child.to_string(),
                condition: "settled".into(),
            }],
        );
        let grants = wait(fixture.database.claim_batch()).unwrap();
        assert!(grants.contains(&child_turn));
        assert!(!grants.contains(&waiter_turn));
        fixture.complete_claimed(child_turn);
        assert!(
            wait(fixture.database.job(child))
                .unwrap()
                .unwrap()
                .settled()
        );
        assert!(
            wait(fixture.database.claim_batch())
                .unwrap()
                .contains(&waiter_turn)
        );
        let (blocked, _) = fixture.dispatch(turn, 2, agent, false, None, vec![]);
        let (waiting, _) = fixture.dispatch(
            turn,
            3,
            agent,
            false,
            None,
            vec![JobDependency {
                conversation: blocked.to_string(),
                condition: "success".into(),
            }],
        );
        wait(
            fixture
                .database
                .append(blocked, message("use release"), None, None),
        )
        .unwrap();
        let parent_turns = wait(fixture.database.conversation_turns(parent, None, 10)).unwrap();
        assert!(parent_turns.into_iter().any(|turn| wait(fixture.database.turn_entries(turn.id)).unwrap().iter().any(|(_, entry)| matches!(&entry.entry, Entry::Notice { event: NoticeEvent::WaitersBlocked { waiting: ids }, .. } if ids == &vec![waiting]))));
        assert_eq!(
            wait(fixture.database.job(waiter))
                .unwrap()
                .unwrap()
                .parent_id,
            Some(parent)
        );
        fixture.worker.shutdown().unwrap();
    }

    #[test]
    fn agent_messages_wake_the_parent_but_user_messages_in_a_child_do_not() {
        use crate::orchestration::job_tools::JobToolCall;
        let fixture = Fixture::new("dispatch-messages");
        let (parent, turn, agent) = fixture.dispatch_parent();
        let (child, first) = fixture.dispatch(turn, 0, agent, true, None, vec![]);
        fixture.finish(first, OutcomeKind::Completed);
        let answer = fixture.apply(
            turn,
            1,
            JobToolCall::JobMessage {
                conversation: child.to_string(),
                text: "continue".into(),
            },
        );
        let next = JobId::new(answer["turn"].as_str().unwrap()[4..].parse().unwrap());
        fixture.finish(next, OutcomeKind::Completed);
        let pending = wait(fixture.database.conversation_turns(parent, None, 1)).unwrap()[0].id;
        let before = wait(fixture.database.turn_entries(pending)).unwrap().len();
        let user = wait(
            fixture
                .database
                .append(child, message("use --release"), None, None),
        )
        .unwrap();
        fixture.finish(user.turn, OutcomeKind::Completed);
        assert_eq!(
            wait(fixture.database.turn_entries(pending)).unwrap().len(),
            before
        );
        assert!(
            wait(fixture.database.job(child))
                .unwrap()
                .unwrap()
                .children_closed_at
                .is_none()
        );
        assert!(wait(fixture.database.turn_entries(pending)).unwrap().iter().any(|(_, entry)| matches!(entry.entry, Entry::Notice { reference, .. } if reference == next)));
        fixture.worker.shutdown().unwrap();
    }

    #[test]
    fn attention_actions_hold_at_eight_while_children_run_and_user_lifts_it() {
        use crate::orchestration::job_tools::JobToolCall;
        let fixture = Fixture::new("dispatch-loop");
        let (parent, first, agent) = fixture.dispatch_parent();
        let (child, child_first) = fixture.dispatch(first, 0, agent, true, None, vec![]);
        let (_, other) = fixture.dispatch(first, 1, agent, true, None, vec![]);
        fixture.claim(other);
        fixture.complete_claimed(first);
        fixture.finish(child_first, OutcomeKind::Completed);
        for _ in 0..8 {
            let current = wait(fixture.database.conversation_turns(parent, None, 1)).unwrap()[0].id;
            fixture.claim_observed(current);
            let value = fixture.apply(
                current,
                0,
                JobToolCall::JobMessage {
                    conversation: child.to_string(),
                    text: "next round".into(),
                },
            );
            fixture.complete_claimed(current);
            let next = JobId::new(value["turn"].as_str().unwrap()[4..].parse().unwrap());
            fixture.finish(next, OutcomeKind::Completed);
        }
        let parent_row = wait(fixture.database.conversations(None, 20))
            .unwrap()
            .conversations
            .into_iter()
            .find(|row| row.root_job == parent)
            .unwrap();
        assert_eq!(parent_row.on_hold, Some(Actor::Loop));
        assert!(wait(fixture.database.claim_batch()).unwrap().is_empty());
        assert!(
            wait(fixture.database.job(other))
                .unwrap()
                .unwrap()
                .outcome
                .is_none()
        );
        let joined = wait(
            fixture
                .database
                .append(parent, message("continue please"), None, None),
        )
        .unwrap();
        assert!(joined.joined);
        assert!(
            wait(fixture.database.claim_batch())
                .unwrap()
                .contains(&joined.turn)
        );
        assert_eq!(
            wait(fixture.database.conversations(None, 20))
                .unwrap()
                .conversations
                .into_iter()
                .find(|row| row.root_job == parent)
                .unwrap()
                .on_hold,
            None
        );
        fixture.worker.shutdown().unwrap();
    }

    #[test]
    fn recurring_objective_is_progress_but_a_joined_job_message_wakes_the_parent() {
        use crate::orchestration::job_tools::JobToolCall;
        let fixture = Fixture::new("dispatch-iteration-message");
        let (parent, turn, agent) = fixture.dispatch_parent();
        let (child, first) = fixture.dispatch(turn, 0, agent, false, Some(60), vec![]);
        fixture.finish(first, OutcomeKind::Completed);
        let notices = wait(fixture.database.notices(parent, 0, 100)).unwrap();
        assert!(notices.iter().any(|(_, notice)| notice.class == NoticeClass::Progress
            && matches!(notice.body, NoticeBody::Event { child: id, event: NoticeEvent::IterationCompleted, reference } if id == child && reference == first)));
        assert_eq!(
            wait(fixture.database.conversation_turns(parent, None, 10))
                .unwrap()
                .len(),
            1
        );
        let due = match fixture.recurrence(child).phase {
            RecurrencePhase::WaitingDue { due } => due,
            phase => panic!("expected due: {phase:?}"),
        };
        wait(fixture.database.boundary_b(due + 1)).unwrap();
        let iteration = fixture.recurrence(child).current_turn;
        let reply = fixture.apply(
            turn,
            1,
            JobToolCall::JobMessage {
                conversation: child.to_string(),
                text: "please reply".into(),
            },
        );
        assert_eq!(reply["turn"], iteration.to_string());
        assert_eq!(
            wait(fixture.database.turn_entries(iteration))
                .unwrap()
                .len(),
            2
        );
        fixture.complete_claimed(turn);
        fixture.finish(iteration, OutcomeKind::Completed);
        let turns = wait(fixture.database.conversation_turns(parent, None, 10)).unwrap();
        let attention = turns.iter().find(|row| wait(fixture.database.turn_entries(row.id)).unwrap().iter().any(|(_, entry)| matches!(entry.entry,
            Entry::Notice { child: id, event: NoticeEvent::IterationCompleted, reference } if id == child && reference == iteration))).expect("parent attention entry");
        assert!(
            wait(fixture.database.claim_batch())
                .unwrap()
                .contains(&attention.id)
        );
        fixture.worker.shutdown().unwrap();
    }

    #[test]
    fn recurrence_continues_after_failure_uses_bounded_context_and_halts_on_close_or_model_removal()
    {
        let fixture = Fixture::new("dispatch-recurrence");
        let (parent, turn, agent) = fixture.dispatch_parent();
        let (child, first) = fixture.dispatch(turn, 0, agent, false, Some(60), vec![]);
        fixture.finish(first, OutcomeKind::Completed);
        for n in 2..=3 {
            let due = match fixture.recurrence(child).phase {
                RecurrencePhase::WaitingDue { due } => due,
                phase => panic!("expected due: {phase:?}"),
            };
            assert!(
                wait(fixture.database.next_scheduled_delay())
                    .unwrap()
                    .unwrap()
                    .as_secs()
                    <= 60
            );
            wait(fixture.database.boundary_b(due + 1)).unwrap();
            let recurrence = fixture.recurrence(child);
            assert_eq!(recurrence.n, n);
            assert_eq!(recurrence.phase, RecurrencePhase::Active);
            let context = wait(
                fixture
                    .database
                    .read_turn_context(recurrence.current_turn, 10000),
            )
            .unwrap();
            assert!(context.turns.len() <= 3);
            assert!(context.turns[0].text.contains("objective"));
            fixture.finish(
                recurrence.current_turn,
                if n == 2 {
                    OutcomeKind::Failed
                } else {
                    OutcomeKind::Completed
                },
            );
        }
        wait(fixture.database.close(child, Actor::User)).unwrap();
        assert_eq!(
            fixture.recurrence(child).phase,
            RecurrencePhase::Halted {
                reason: RecurrenceHalt::Closed
            }
        );
        wait(fixture.database.boundary_b(i64::MAX)).unwrap();
        assert_eq!(fixture.recurrence(child).n, 3);
        let (unavailable, first) = fixture.dispatch(turn, 1, agent, true, Some(60), vec![]);
        fixture.finish(first, OutcomeKind::Completed);
        wait(fixture.database.update_config(
            agent,
            crate::persistence::NewConfig {
                name: "no models".into(),
                body: crate::persistence::ConfigBody::Agent(AgentConfig::default()),
            },
        ))
        .unwrap();
        wait(fixture.database.boundary_b(i64::MAX)).unwrap();
        wait(fixture.database.boundary_b(i64::MAX)).unwrap();
        assert_eq!(
            fixture.recurrence(unavailable).phase,
            RecurrencePhase::Halted {
                reason: RecurrenceHalt::ConfigUnavailable
            }
        );
        let rows = wait(fixture.database.conversation_turns(parent, None, 10)).unwrap();
        let notices = rows.into_iter().flat_map(|row| wait(fixture.database.turn_entries(row.id)).unwrap()).filter(|(_, entry)| matches!(entry.entry, Entry::Notice { child, event: NoticeEvent::RecurrenceHalted, .. } if child == unavailable)).count();
        assert_eq!(notices, 1);
        wait(fixture.database.with_database(|database| {
            assert!(derived::relationship_inconsistencies(database)?.is_empty());
            Ok(())
        }))
        .unwrap();
        fixture.worker.shutdown().unwrap();
    }

    #[test]
    fn agent_cannot_unhold_user_hold_and_retry_is_bounded() {
        use crate::orchestration::job_tools::JobToolCall;
        let fixture = Fixture::new("dispatch-hold");
        let (_, turn, agent) = fixture.dispatch_parent();
        let (child, first) = fixture.dispatch(turn, 0, agent, true, None, vec![]);
        wait(fixture.database.hold(child, Actor::User)).unwrap();
        let tool = JobToolCall::JobUnhold {
            conversation: child.to_string(),
        };
        let call = fixture.admit(turn, 1, &tool);
        assert!(
            matches!(wait(fixture.database.apply_job_tool(call, tool)), Err(crate::database::DatabaseError::Rejected(crate::persistence::Violation::InvalidValue("hold", reason))) if !reason.is_empty())
        );
        wait(fixture.database.unhold(child, Actor::User)).unwrap();
        fixture.finish(first, OutcomeKind::Failed);
        let mut original = first;
        for index in 2..4 {
            let value = fixture.apply(
                turn,
                index,
                JobToolCall::JobRetry {
                    turn: original.to_string(),
                },
            );
            let retry = JobId::new(value["turn"].as_str().unwrap()[4..].parse().unwrap());
            fixture.finish(retry, OutcomeKind::Failed);
            original = retry;
        }
        let tool = JobToolCall::JobRetry {
            turn: original.to_string(),
        };
        let call = fixture.admit(turn, 4, &tool);
        assert!(matches!(
            wait(fixture.database.apply_job_tool(call, tool)),
            Err(crate::database::DatabaseError::Rejected(
                crate::persistence::Violation::InvalidValue("retry_limit", _)
            ))
        ));
        fixture.worker.shutdown().unwrap();
    }
}
