use super::rows::Closing;
use super::*;
use crate::persistence::{
    ExecutionConfiguration, NewSubmission, OutcomeCause, OutcomeKind, OutcomeRecord, Violation,
};
use redb::ReadableTableMetadata;

impl Batch {
    pub(crate) fn conversation_limits(
        &mut self,
        conversation: JobId,
    ) -> Result<(ConversationLimits, u8), BatchError> {
        let mut current = conversation;
        let mut effective = ConversationLimits::CEILING;
        for depth in 0..=crate::persistence::MAX_CONVERSATION_DEPTH {
            let row = self.require_job(current)?;
            if !row.kind.is_conversation() {
                return self.reject(Violation::InvalidValue(
                    "conversation_not_found",
                    "expected a conversation",
                ));
            }
            let own = self.conversation(current)?.limits;
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

    pub(crate) fn set_conversation_limits(
        &mut self,
        conversation: JobId,
        limits: ConversationLimits,
        by: Actor,
    ) -> Result<(), BatchError> {
        let row = self.require_user_conversation(conversation)?;
        let ceiling = match row.parent_id {
            Some(parent) => self.conversation_limits(parent)?.0,
            None => ConversationLimits::CEILING,
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
            if !limits.within(self.conversation_limits(conversation)?.0) {
                return self.reject(Violation::InvalidValue(
                    "limits",
                    "an Agent may only tighten the effective limits",
                ));
            }
        }
        let mut metadata = self.conversation(conversation)?;
        metadata.limits = Some(limits);
        self.insert_conversation(conversation, &metadata)?;
        self.loop_hold_if_needed(conversation)?;
        if self
            .recurrence(conversation)?
            .is_some_and(|recurrence| recurrence.n >= u64::from(limits.max_iterations))
        {
            self.halt_recurrence(conversation, RecurrenceHalt::LimitReached)?;
        }
        Ok(())
    }
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
        self.open_conversation_with_format(
            parent,
            spec,
            first,
            configuration,
            submission,
            dependencies,
            None,
        )
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open_conversation_with_format(
        &mut self,
        parent: Option<JobId>,
        spec: ConversationSpec,
        first: Entry,
        configuration: ExecutionConfiguration,
        submission: Option<NewSubmission>,
        dependencies: Vec<(JobId, DependencyCondition)>,
        format: Option<ResponseFormat>,
    ) -> Result<(JobId, Appended), BatchError> {
        if let Some(parent) = parent {
            let parent_row = self.require_job(parent)?;
            if !parent_row.kind.is_conversation() {
                return self.reject(Violation::InvalidValue("parent", "expected a conversation"));
            }
            let (limits, parent_depth) = self.conversation_limits(parent)?;
            let mut root = parent;
            while let Some(ancestor) = self.require_job(root)?.parent_id {
                root = ancestor;
            }
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
                parent_id: parent,
                kind: JobKind::Conversation {
                    agent: spec.agent,
                    project: spec.project,
                    origin: spec.origin,
                    keep_open: spec.keep_open,
                },
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
            format,
            None,
        )?;
        Ok((conversation, appended))
    }
    fn check_context_job(&mut self, consumer: &JobRow, producer: JobId) -> Result<(), BatchError> {
        let source = self.require_job(producer)?;
        let Some(parent) = source.parent_id else {
            return self.reject(Violation::InvalidValue(
                "context_job",
                "requires a system child turn",
            ));
        };
        let owner = self.require_job(parent)?;
        if source.id == consumer.id
            || !source.kind.is_stored()
            || owner.parent_id != consumer.parent_id
            || !matches!(
                owner.kind,
                JobKind::Conversation {
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
        if !row.kind.is_stored() || state.claim.is_some() || state.outcome.is_some() {
            return self.reject(Violation::InvalidValue(
                "turn_input",
                "input is frozen by claim or outcome",
            ));
        }
        match &entry {
            Entry::ContextPreparation {
                job,
                policy_revision,
            } => {
                self.check_context_job(&row, *job)?;
                let Some(conversation) = row.parent_id else {
                    return self.corrupted("stored turn has no conversation".into());
                };
                if *policy_revision != self.conversation(conversation)?.compaction_revision {
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
                        serde_json::from_value::<crate::persistence::SummarizationResult>(value)
                            .ok()
                    })
                    .is_some_and(|result| {
                        result.validate().is_ok()
                            && Some(result.source.conversation) == row.parent_id
                    });
                if !valid {
                    return self.reject(Violation::InvalidValue(
                        "context_use",
                        "requires a validated summarization result for this conversation",
                    ));
                }
            }
            Entry::ContextUse { artifact: None }
            | Entry::UserMessage { .. }
            | Entry::AgentMessage { .. }
            | Entry::Notice { .. }
            | Entry::Decision { .. }
            | Entry::RetryOf { .. }
            | Entry::Iteration { .. }
            | Entry::ResponseFormat { .. } => {}
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
            if let Some(conversation) = row.parent_id
                && let Entry::UserMessage { text, .. } | Entry::AgentMessage { text, .. } = &entry
            {
                derived::index_search_text(tx, conversation, turn, 0, seq, text)?;
            }
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
                | Entry::Iteration { .. }
                | Entry::ContextPreparation { .. }
                | Entry::ContextUse { .. }
                | Entry::ResponseFormat { .. } => {}
            }
            Ok((seq, first_user))
        })?;
        if first_user && let Some(conversation) = row.parent_id {
            let waiting = self.step(|tx| {
                let jobs = tx.open_table(JOBS)?;
                let mut waiting = BTreeSet::new();
                for entry in tx
                    .open_table(DEPENDENCY_WAITERS)?
                    .range((conversation.get(), 0)..=(conversation.get(), u128::MAX))?
                {
                    let waiter = JobId::new(entry?.0.value().1);
                    if let Some(bytes) = jobs.get(waiter.get())?
                        && let Some(parent) =
                            JobRow::decode_at(waiter.get(), bytes.value())?.parent_id
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
        self.append_with_format(conversation, entry, freeze, submission, event, None)
    }
    pub(crate) fn append_with_format(
        &mut self,
        conversation: JobId,
        entry: Entry,
        freeze: Option<ExecutionConfiguration>,
        submission: Option<NewSubmission>,
        event: bool,
        format: Option<ResponseFormat>,
    ) -> Result<Appended, BatchError> {
        self.append_named(
            conversation,
            entry,
            freeze,
            submission,
            event,
            None,
            Vec::new(),
            format,
            None,
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
        format: Option<ResponseFormat>,
        frozen: Option<FrozenExecutionConfig>,
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
        if matches!(
            entry,
            Entry::ContextPreparation { .. } | Entry::ContextUse { .. }
        ) {
            return self.reject(Violation::InvalidValue(
                "entry",
                "context metadata must be selected on an existing unclaimed turn",
            ));
        }
        let row = self.require_job(conversation)?;
        if !row.kind.is_conversation() {
            return self.reject(Violation::InvalidValue(
                "conversation_not_found",
                "expected a conversation",
            ));
        }
        let state = self.require_job_state(conversation)?;
        if self.conversation(conversation)?.closing.is_some() {
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
            let execution = if let Some(frozen) = frozen {
                TurnExecution::Frozen(frozen)
            } else {
                match freeze {
                    Some(configuration) => {
                        let frozen = (|| {
                            let frozen = match self.freeze_configuration(configuration)? {
                                crate::persistence::FrozenConfiguration::Generative(frozen) => {
                                    frozen
                                }
                                crate::persistence::FrozenConfiguration::Decision(_) => {
                                    return self.reject(Violation::InvalidValue(
                                        "execution",
                                        "decision work requires the typed decision runtime",
                                    ));
                                }
                            };
                            let agent = frozen.agent.as_ref();
                            if tip.is_none()
                                && agent.is_some_and(|a| !a.approve.is_empty())
                                && !self.step(|tx| {
                                    reads::has_decider(&tx.open_table(JOBS)?, Some(conversation))
                                })?
                            {
                                return self.reject(Violation::InvalidValue(
                                    "approve",
                                    "no deciding user",
                                ));
                            }
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
                            JobKind::Turn {
                                execution: TurnExecution::Decision(_),
                                ..
                            } => {
                                return self.reject(Violation::InvalidValue(
                                    "execution",
                                    "decision work requires the typed decision runtime",
                                ));
                            }
                            JobKind::Conversation { .. } => {
                                return self.corrupted("conversation tip is not a turn".into());
                            }
                        }
                    }
                }
            };
            let spec = JobRow {
                id: JobId::new(0),
                created_at: self.now,
                parent_id: Some(conversation),
                kind: JobKind::Turn {
                    name,
                    execution,
                    plan_source: PlanSource::Stored {
                        previous: tip,
                        dependencies,
                    },
                },
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
        if let Some(format) = format {
            self.insert_turn_input(turn, Entry::ResponseFormat { format })?;
        }
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
    pub(crate) fn title_target(&mut self, turn: &JobRow) -> Result<Option<JobId>, BatchError> {
        if !turn.kind.is_stored() || turn.kind.name() != Some(crate::persistence::TITLE_JOB_NAME) {
            return Ok(None);
        }
        let Some(conversation) = turn.parent_id else {
            return Ok(None);
        };
        let row = self.require_job(conversation)?;
        if matches!(
            row.kind,
            JobKind::Conversation {
                origin: Origin::System,
                keep_open: false,
                ..
            }
        ) {
            return Ok(row.parent_id);
        }
        Ok(None)
    }

    pub(crate) fn schedule_title_job(&mut self, turn: &JobRow) -> Result<(), BatchError> {
        if !turn.kind.is_stored() || turn.kind.previous().is_some() || turn.kind.name().is_some() {
            return Ok(());
        }
        let Some(target) = turn.parent_id else {
            return Ok(());
        };
        let parent = self.require_job(target)?;
        if parent.parent_id.is_some()
            || !matches!(
                parent.kind,
                JobKind::Conversation {
                    origin: Origin::User,
                    keep_open: true,
                    ..
                }
            )
        {
            return Ok(());
        }
        let metadata = self.conversation(target)?;
        if metadata.title.is_some()
            || metadata.closing.is_some()
            || self.require_job_state(target)?.outcome.is_some()
        {
            return Ok(());
        }
        let (limits, depth) = self.conversation_limits(target)?;
        if depth >= limits.max_depth
            || self.live_children(target)?.len() >= 16
            || self.live_subtree(target)?.len() >= 64
        {
            return Ok(());
        }
        if self.step(|tx| Ok(tx.open_table(PENDING)?.len()?))? >= P_MAX {
            return Ok(());
        }
        let (mut frozen, _, _) = self.turn_execution(turn)?;
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
                        crate::persistence::RequestPayload::Sent { .. }
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
            for entry in tx
                .open_table(TURN_INPUTS)?
                .range((turn.id.get(), 0)..=(turn.id.get(), u32::MAX))?
            {
                let (_, bytes) = entry?;
                if let Entry::UserMessage { text, .. } = TurnEntry::decode(bytes.value())?.entry {
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
        frozen.llm.config.parameters = crate::persistence::LlmParameters {
            max_tokens: Some(match frozen.llm.provider.transport {
                LlmTransport::OpenAiChatCompletions => 256,
                LlmTransport::OpenAiResponses => 4096,
                LlmTransport::AnthropicMessages => 256,
            }),
            ..Default::default()
        };
        let conversation = self.t1_submit(
            None,
            JobRow {
                id: JobId::new(0),
                created_at: self.now,
                parent_id: Some(target),
                kind: JobKind::Conversation {
                    agent: None,
                    project: None,
                    origin: Origin::System,
                    keep_open: false,
                },
            },
        )?;
        self.insert_conversation(conversation, &ConversationRow::default())?;
        self.step(|tx| {
            tx.open_table(CHILD_CONVERSATIONS)?
                .insert((target.get(), conversation.get()), ())?;
            tx.open_table(LIVE_CHILDREN)?
                .insert((target.get(), conversation.get()), ())?;
            Ok(())
        })?;
        self.append_named(
            conversation,
            Entry::UserMessage {
                text: crate::persistence::title_prompt(&text),
                attachments: Vec::new(),
            },
            None,
            None,
            false,
            Some(crate::persistence::TITLE_JOB_NAME.to_owned()),
            Vec::new(),
            Some(ResponseFormat::JsonObject),
            Some(frozen),
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
        if self.conversation(conversation)?.closing.is_some() {
            let mut running = Vec::new();
            for current in self.live_subtree(conversation)? {
                if let Some(active) = self.active_turn(current)?
                    && self.require_job_state(active)?.outcome.is_none()
                {
                    running.push(active);
                }
            }
            return Ok(ConversationAction {
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
            let mut row = self.conversation(*current)?;
            if row.closing.is_some() {
                continue;
            }
            notified.push(*current);
            let active = self.active_turn(*current)?;
            let active = match active {
                Some(active) if self.require_job_state(active)?.outcome.is_none() => Some(active),
                Some(_) | None => None,
            };
            row.closing = Some(Closing {
                at: self.now,
                by: by.clone(),
                running: active.is_some(),
            });
            self.insert_conversation(*current, &row)?;
            if let Some(active) = active {
                running.push(active);
            }
            if let Some(tip) = self.tip(*current)? {
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
                        if let Some(at) = turn.kind.eligible_at() {
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
        let unresolved = self.step(|tx| {
            let calls = tx.open_table(TOOL_CALLS)?;
            let links = tx.open_table(APPROVAL_CALLS)?;
            for record in job_tool_evidence(&calls, turn)? {
                if record.approval.is_some() && record.is_unresolved() {
                    return Ok(true);
                }
                if let Some(executing) = links.get(tool_call_key(record.call))? {
                    let call = tool_call_ref(executing.value())?;
                    let bytes = calls
                        .get(tool_call_key(call))?
                        .ok_or_else(|| StorageError::corrupted("approval execution missing"))?;
                    if decode_tool_evidence(call, bytes.value())?.is_unresolved() {
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        })?;
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
                    for row in retries.range((original, 0)..=(original, u128::MAX))? {
                        let (key, agent) = row?;
                        if agent.value() {
                            count += 1;
                            if count >= 2 {
                                return Ok(true);
                            }
                        }
                        pending.push(key.value().1);
                    }
                }
                Ok(false)
            })?
        {
            return self.reject(Violation::InvalidValue(
                "retry_limit",
                "at most two agent retries per original turn",
            ));
        }
        // A waiting turn takes the entry itself; it may hold only one retry.
        if let Some(tip) = self.tip(conversation)? {
            let state = self.require_job_state(tip)?;
            if state.claim.is_none() && state.outcome.is_none() {
                let retried = self.step(|tx| {
                    let inputs = tx.open_table(TURN_INPUTS)?;
                    for entry in inputs.range((tip.get(), 0)..=(tip.get(), u32::MAX))? {
                        if matches!(
                            TurnEntry::decode(entry?.1.value())?.entry,
                            Entry::RetryOf { .. }
                        ) {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                })?;
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
            JobKind::Conversation {
                origin: Origin::User | Origin::System,
                keep_open: false,
                ..
            }
        );
        if (row.parent_id.is_none() && !one_shot)
            || !matches!(
                row.kind,
                JobKind::Conversation {
                    keep_open: false,
                    ..
                }
            )
            || self.conversation(conversation)?.closing.is_some()
        {
            return Ok(false);
        }
        let Some(tip) = self.tip(conversation)? else {
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
            || (!one_shot
                && self.step(|tx| {
                    Ok(tx
                        .open_table(USER_ENTRY_AT)?
                        .get(conversation.get())?
                        .is_some())
                })?)
        {
            return Ok(false);
        }
        if self.step(|tx| {
            Ok(tx
                .open_table(OPEN_APPROVALS)?
                .range(
                    (conversation.get(), 0, 0, 0)
                        ..=(conversation.get(), u128::MAX, u32::MAX, u32::MAX),
                )?
                .next()
                .transpose()?
                .is_some())
        })? {
            return Ok(false);
        }
        let mut row = self.conversation(conversation)?;
        row.closing = Some(Closing {
            at: self.now,
            by: Actor::Auto,
            running: false,
        });
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
        let row = self.conversation(conversation)?;
        if row.closing.is_none() || state.outcome.is_some() {
            return Ok(());
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::{DatabaseHandle, DatabaseWorker};
    use crate::persistence::{
        ClaimRecord, ExecutionConfiguration, NewExecutor, NewSubmission, RequestFingerprint,
        ToolCallRef, UsageBucketWidth, UsageBucketing, UsageSummaryFilter,
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
        fn agent(&self, name: &str) -> crate::persistence::ProfileId {
            wait(self.database.create_config(crate::persistence::NewConfig {
                name: name.into(),
                body: crate::persistence::ConfigBody::Agent(AgentConfig {
                    dispatch_to: Vec::new(),
                    models: vec![self.configuration.llm_profile.into()],
                    ..Default::default()
                }),
            }))
            .expect("Agent config")
            .into()
        }
        fn open_as(&self, agent: crate::persistence::ProfileId) -> (JobId, JobId) {
            let (conversation, appended) = wait(self.database.open_conversation(
                None,
                ConversationSpec {
                    name: None,
                    agent: Some(agent.into()),
                    project: None,
                    origin: Origin::User,
                    keep_open: true,
                },
                message("first"),
                ExecutionConfiguration {
                    agent_profile: Some(agent),
                    llm_profile: self.configuration.llm_profile,
                },
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
    fn full_text_search_covers_old_inputs_results_unicode_and_exact_phrases() {
        let fixture = Fixture::new("full-text-history");
        let (conversation, first) = fixture.open();
        wait(fixture.database.append(
            conversation,
            message("Archive NEEDLE connection reset ΟΣ 中文"),
            None,
            None,
        ))
        .expect("join input");
        fixture.finish(first, OutcomeKind::Completed);
        for _ in 0..101 {
            let turn = wait(
                fixture
                    .database
                    .append(conversation, message("later"), None, None),
            )
            .expect("append")
            .turn;
            fixture.finish(turn, OutcomeKind::Completed);
        }
        for terms in [
            vec!["needle"],
            vec!["connection reset", "中文"],
            vec!["ος"],
            vec!["answer"],
        ] {
            let terms = terms.into_iter().map(str::to_owned).collect();
            let page =
                wait(fixture.database.search_conversations(terms, None, 10)).expect("search");
            assert_eq!(
                page.conversations
                    .iter()
                    .map(|item| item.root_job)
                    .collect::<Vec<_>>(),
                vec![conversation]
            );
        }
        let page = wait(fixture.database.search_conversations(
            vec!["connection missing".into()],
            None,
            10,
        ))
        .expect("search");
        assert!(
            page.conversations.is_empty(),
            "gram candidates still need exact source verification"
        );
        assert!(
            wait(fixture.database.with_database(derived::inconsistencies))
                .expect("index invariants")
                .is_empty()
        );
        fixture.worker.shutdown().expect("shutdown");
    }

    #[test]
    fn search_excludes_failure_diagnostics_and_non_root_sources() {
        let fixture = Fixture::new("search-eligibility");
        let (root, first) = fixture.open();
        fixture.finish(first, OutcomeKind::Failed);
        assert!(
            wait(
                fixture
                    .database
                    .search_conversations(vec!["failed".into()], None, 10)
            )
            .unwrap()
            .conversations
            .is_empty()
        );
        let (cancelled, cancelled_turn) = fixture.open();
        fixture.finish(cancelled_turn, OutcomeKind::Cancelled);
        assert!(
            wait(
                fixture
                    .database
                    .search_conversations(vec!["cancelled".into()], None, 10)
            )
            .unwrap()
            .conversations
            .is_empty()
        );
        let (partial, partial_turn) = fixture.open();
        fixture.claim(partial_turn);
        wait(fixture.database.finalize_job(
            partial_turn,
            crate::persistence::JobFinalization::Failed(FinalResult {
                summary: Some("diagnostic-only".into()),
                code: None,
                goal_reached: None,
                value: Some(serde_json::json!({"text":"saved partial answer"})),
            }),
        ))
        .unwrap();
        assert_eq!(
            wait(
                fixture
                    .database
                    .search_conversations(vec!["partial answer".into()], None, 10)
            )
            .unwrap()
            .conversations[0]
                .root_job,
            partial
        );
        assert!(
            wait(
                fixture
                    .database
                    .search_conversations(vec!["diagnostic-only".into()], None, 10)
            )
            .unwrap()
            .conversations
            .is_empty()
        );
        assert_ne!(cancelled, partial);
        let (child, child_turn) = fixture.child(root, false);
        fixture.finish(child_turn, OutcomeKind::Completed);
        wait(fixture.database.with_database(move |database| {
            let tx = database.begin_write()?;
            assert!(
                tx.open_table(CONVERSATION_SEARCH)?
                    .range(
                        (child.get(), 0, 0, 0, 0)
                            ..=(child.get(), u128::MAX, u128::MAX, u8::MAX, u32::MAX)
                    )?
                    .next()
                    .is_none()
            );
            // Simulate old postings for a diagnostic and a non-queryable child.
            derived::index_search_text(&tx, root, first, 1, 1, "failed")?;
            tx.open_table(CONVERSATION_SEARCH)?.insert(
                (
                    child.get(),
                    derived::search_gram(&['x']),
                    child_turn.get(),
                    0,
                    1,
                ),
                (),
            )?;
            tx.commit()?;
            Ok(())
        }))
        .unwrap();
        assert!(
            wait(
                fixture
                    .database
                    .search_conversations(vec!["failed".into()], None, 10)
            )
            .unwrap()
            .conversations
            .is_empty()
        );
        wait(fixture.database.with_database(|database| {
            assert!(derived::inconsistencies(database)?.contains(&"conversation_search"));
            let tx = database.begin_write()?;
            derived::rebuild(&tx, open::promotion_clock())?;
            tx.commit()?;
            assert!(derived::inconsistencies(database)?.is_empty());
            Ok(())
        }))
        .unwrap();
        fixture.worker.shutdown().unwrap();
    }

    #[test]
    fn full_text_search_pages_without_skipping_lookahead_and_rebuilds_its_index() {
        let fixture = Fixture::new("full-text-pages");
        let mut roots = Vec::new();
        for _ in 0..3 {
            let (conversation, _) = fixture.open();
            wait(
                fixture
                    .database
                    .append(conversation, message("unique match"), None, None),
            )
            .expect("join");
            roots.push(conversation);
        }
        let mut cursor = None;
        let mut found = Vec::new();
        loop {
            let page = wait(
                fixture
                    .database
                    .search_conversations(vec!["match".into()], cursor, 1),
            )
            .expect("page");
            found.extend(page.conversations.iter().map(|item| item.root_job));
            if !page.has_more {
                break;
            }
            assert_ne!(cursor, page.next_before);
            cursor = page.next_before;
        }
        roots.reverse();
        assert_eq!(found, roots);
        wait(fixture.database.with_database(|database| {
            let tx = database.begin_write()?;
            tx.delete_table(CONVERSATION_SEARCH)?;
            tx.open_table(CONVERSATION_SEARCH)?;
            tx.commit()?;
            assert!(derived::inconsistencies(database)?.contains(&"conversation_search"));
            let tx = database.begin_write()?;
            derived::rebuild(&tx, open::promotion_clock())?;
            tx.commit()?;
            Ok(())
        }))
        .expect("rebuild");
        assert_eq!(
            wait(
                fixture
                    .database
                    .search_conversations(vec!["unique".into()], None, 10)
            )
            .expect("rebuilt search")
            .conversations
            .len(),
            3
        );
        fixture.worker.shutdown().expect("shutdown");
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
                    name: Some("process".into()),
                    execution: TurnExecution::Frozen(configuration),
                    plan_source: PlanSource::Process {
                        project: Some(ProjectId::new(2)),
                        grant: Some("read".into()),
                        eligible_at: Some(Timestamp::from_unix_micros(123)),
                        objective: Some("objective".into()),
                        attachments: vec![AttachmentId::new(4), AttachmentId::new(3)],
                    },
                },
                JobKind::Turn {
                    name: Some("stored".into()),
                    execution: TurnExecution::Ref(first),
                    plan_source: PlanSource::Stored {
                        previous: Some(first),
                        dependencies: vec![
                            (JobId::new(1), DependencyCondition::Success),
                            (JobId::new(2), DependencyCondition::Settled),
                        ],
                    },
                },
            ];
            for kind in kinds {
                let row = JobRow {
                    id: first,
                    kind,
                    created_at: Timestamp::now(),
                    parent_id: Some(conversation),
                };
                assert_eq!(JobRow::decode(&row.encode())?, row);
            }
            let at = Timestamp::from_unix_micros(456);
            let state = JobStateRow {
                job_id: first,
                claim: Some(ClaimRecord {
                    started_at: at,
                    pool_id: ConfigId::new(3),
                    pool_edit_seq: 2,
                    capabilities: BTreeSet::from([(
                        ExecutionOperation::LlmRequest,
                        Some(LlmTransport::OpenAiChatCompletions),
                    )]),
                    epoch: 7,
                    host_id: Some("host".into()),
                    boot_id: Some("boot".into()),
                    notice_seq: 5,
                }),
                outcome: Some(OutcomeRecord {
                    kind: OutcomeKind::Unknown,
                    at,
                    cause: Some(OutcomeCause::Closed),
                }),
                stop_confirmed_at: Some(at),
                stop_confirmed_by: Some(StopProof::Proof),
            };
            for state in [JobStateRow::new(first), state] {
                assert_eq!(JobStateRow::decode(&state.encode())?, state);
            }
            for closing in [
                None,
                Some(Closing {
                    at,
                    by: Actor::User,
                    running: true,
                }),
            ] {
                let row = ConversationRow {
                    limits: Some(ConversationLimits::default()),
                    title: Some("title".into()),
                    hidden_at: Some(at),
                    on_hold: Some(2),
                    loop_count: 3,
                    closing,
                    compaction_policy: None,
                    compaction_revision: 0,
                };
                assert_eq!(ConversationRow::decode(&row.encode())?, row);
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
                    name: None,
                    execution: TurnExecution::Ref(second),
                    plan_source: PlanSource::Stored {
                        previous: Some(second),
                        dependencies: Vec::new(),
                    },
                };
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
        assert!(second.already);
        assert_eq!(second.running, [turn]);
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
    fn a_conversation_without_its_row_is_corruption_not_a_default() {
        let fixture = Fixture::new("no-conversation-row");
        let (conversation, _turn) = fixture.open();
        wait(fixture.database.with_database(move |database| {
            let tx = database.begin_write()?;
            tx.open_table(CONVERSATIONS)?.remove(conversation.get())?;
            tx.commit()?;
            Ok(())
        }))
        .expect("remove row");
        // The missing row is a storage failure, so the worker protects itself
        // and only a shutdown that refuses proves it.
        assert!(
            wait(fixture.database.set_conversation(
                conversation,
                crate::persistence::ConversationEdit::Title("renamed".into())
            ))
            .is_err()
        );
        assert!(fixture.worker.shutdown().is_err());
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
    fn user_one_shots_close_automatically_after_failure() {
        let fixture = Fixture::new("user-one-shot");
        for keep_open in [false, true] {
            let (conversation, turn) = wait(fixture.database.open_conversation(
                None,
                ConversationSpec {
                    name: None,
                    agent: None,
                    project: None,
                    origin: Origin::User,
                    keep_open,
                },
                message("objective"),
                fixture.configuration,
                None,
            ))
            .expect("open");
            fixture.finish(turn.turn, OutcomeKind::Failed);
            let record = wait(fixture.database.job(conversation))
                .expect("read")
                .expect("conversation");
            assert_eq!(record.children_closed_at.is_some(), !keep_open);
            assert_eq!(record.settled(), !keep_open);
            if !keep_open {
                let entries = wait(fixture.database.with_database(move |database| {
                    let rx = reads::read(database)?;
                    let bytes = rx
                        .open_table(CONVERSATIONS)?
                        .get(conversation.get())?
                        .expect("conversation")
                        .value()
                        .to_vec();
                    Ok(ConversationRow::decode(&bytes)?.closing)
                }))
                .expect("read")
                .expect("closing");
                assert_eq!(entries.by, Actor::Auto);
            }
        }
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
    fn retried_turns_appear_in_usage_bucket_retry_rows() {
        let fixture = Fixture::new("retry-usage-buckets");
        let (conversation, first) = fixture.open();
        fixture.finish(first, OutcomeKind::Completed);
        let first_retry =
            wait(fixture.database.retry(conversation, first, Actor::User)).expect("retry");
        let now_us = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_micros(),
        )
        .expect("time fits");
        let day = 86_400_000_000;
        let filter = UsageSummaryFilter {
            from_us: Some(now_us - 2 * day),
            until_us: Some(now_us + 2 * day),
            agent: None,
        };
        let bucketing = UsageBucketing {
            width: UsageBucketWidth::Day,
            offset_minutes: 0,
        };
        let retry = wait(fixture.database.retry(conversation, first, Actor::User));
        assert!(retry.is_err(), "an unfinished retry prevents another retry");
        let page = wait(
            fixture
                .database
                .usage_retries(filter.clone(), bucketing, None, 1),
        )
        .expect("page");
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0].retries, 1);
        assert!(
            page.rows[0].model.is_some(),
            "pending retries resolve their frozen model without usage"
        );
        let retry_turn = first_retry.turn;
        fixture.finish(retry_turn, OutcomeKind::Completed);
        wait(
            fixture
                .database
                .retry(conversation, retry_turn, Actor::User),
        )
        .expect("second retry");
        let page = wait(
            fixture
                .database
                .usage_retries(filter.clone(), bucketing, None, 1),
        )
        .expect("first page");
        let cursor = page.next.expect("bounded scan");
        assert_eq!(page.rows.iter().map(|row| row.retries).sum::<u64>(), 1);
        let tail = wait(
            fixture
                .database
                .usage_retries(filter.clone(), bucketing, Some(cursor), 1),
        )
        .expect("tail");
        assert_eq!(tail.rows.iter().map(|row| row.retries).sum::<u64>(), 1);
        assert!(tail.next.is_none());
        let outside = UsageSummaryFilter {
            from_us: Some(now_us + day),
            until_us: Some(now_us + 2 * day),
            agent: None,
        };
        let empty = wait(
            fixture
                .database
                .usage_retries(outside.clone(), bucketing, None, 1),
        )
        .expect("filtered page");
        assert!(empty.rows.is_empty());
        let next = empty
            .next
            .expect("cursor advances even without matching rows");
        assert!(
            wait(
                fixture
                    .database
                    .usage_retries(outside, bucketing, Some(next), 1)
            )
            .expect("filtered tail")
            .next
            .is_none()
        );
        fixture.worker.shutdown().expect("shutdown");
    }
    #[test]
    fn retry_pages_advance_through_filtered_rows_without_duplicates() {
        let fixture = Fixture::new("retry-usage-interleaved");
        let first_agent = fixture.agent("first retry Agent");
        let other_agent = fixture.agent("other retry Agent");
        let (first_conversation, first_turn) = fixture.open_as(first_agent);
        fixture.finish(first_turn, OutcomeKind::Completed);
        let (other_conversation, other_turn) = fixture.open_as(other_agent);
        fixture.finish(other_turn, OutcomeKind::Completed);
        let (last_conversation, last_turn) = fixture.open_as(first_agent);
        fixture.finish(last_turn, OutcomeKind::Completed);
        wait(
            fixture
                .database
                .retry(first_conversation, first_turn, Actor::User),
        )
        .expect("first retry");
        wait(
            fixture
                .database
                .retry(other_conversation, other_turn, Actor::User),
        )
        .expect("filtered retry");
        wait(
            fixture
                .database
                .retry(last_conversation, last_turn, Actor::User),
        )
        .expect("last retry");

        let now_us = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_micros(),
        )
        .expect("time fits");
        let day = 86_400_000_000;
        let filter = UsageSummaryFilter {
            from_us: Some(now_us - 2 * day),
            until_us: Some(now_us + 2 * day),
            agent: Some(first_agent.get()),
        };
        let bucketing = UsageBucketing {
            width: UsageBucketWidth::Day,
            offset_minutes: 0,
        };

        let mut after = None;
        let mut pages = 0;
        let mut retries = 0;
        let mut empty_page_with_continuation = false;
        loop {
            pages += 1;
            assert!(pages < 10, "retry cursor did not reach exhaustion");
            let page = wait(
                fixture
                    .database
                    .usage_retries(filter.clone(), bucketing, after, 1),
            )
            .expect("page");
            empty_page_with_continuation |= page.rows.is_empty() && page.next.is_some();
            retries += page.rows.iter().map(|row| row.retries).sum::<u64>();
            match page.next {
                Some(next) => {
                    assert_ne!(after, Some(next), "retry cursor did not advance");
                    after = Some(next);
                }
                None => break,
            }
        }
        assert!(pages > 2);
        assert!(
            empty_page_with_continuation,
            "a filtered raw key is an empty page with a continuation"
        );
        assert_eq!(retries, 2, "only the first Agent's retries are counted");

        let complete = wait(fixture.database.usage_retries(filter, bucketing, None, 100))
            .expect("complete scan");
        assert_eq!(complete.rows.iter().map(|row| row.retries).sum::<u64>(), 2);
        assert!(complete.next.is_none());
        fixture.worker.shutdown().expect("shutdown");
    }

    #[test]
    fn a_retry_of_a_retry_still_quotes_the_root_prompt() {
        let fixture = Fixture::new("retry-of-retry");
        let (conversation, first) = fixture.open();
        fixture.finish(first, OutcomeKind::Unknown);
        let retry = wait(fixture.database.retry(conversation, first, Actor::User)).expect("retry");
        fixture.finish(retry.turn, OutcomeKind::Unknown);
        let again = wait(
            fixture
                .database
                .retry(conversation, retry.turn, Actor::User),
        )
        .expect("retry");
        assert!(
            wait(fixture.database.read_turn_context(again.turn, 1000))
                .expect("context")
                .turns
                .last()
                .expect("retry input")
                .text
                .contains("first")
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
                    approval: None,
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
                approval: None,
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
                value["conversation"].as_str().unwrap().parse().unwrap(),
                value["turn"].as_str().unwrap().parse().unwrap(),
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
    fn delegation_catalog_and_atomic_idle_worker_assignment() {
        use crate::orchestration::job_tools::JobToolCall;
        let fixture = Fixture::new("idle-worker-assignment");
        let (parent, turn, agent) = fixture.dispatch_parent();
        let catalog = fixture.apply(
            turn,
            0,
            JobToolCall::JobAgents {
                after: None,
                limit: Some(1),
            },
        );
        assert_eq!(catalog["agents"][0]["agent"], agent.to_string());
        assert_eq!(catalog["has_more"], false);
        let empty = fixture.apply(
            turn,
            1,
            JobToolCall::JobAgents {
                after: Some(agent.to_string()),
                limit: None,
            },
        );
        assert!(empty["agents"].as_array().unwrap().is_empty());
        let (worker, first) = fixture.dispatch(turn, 2, agent, true, None, vec![]);
        let busy = fixture.apply(
            turn,
            3,
            JobToolCall::JobWorkers {
                after: None,
                limit: None,
            },
        );
        assert_eq!(busy["workers"][0]["idle"], false);
        fixture.finish(first, OutcomeKind::Completed);
        let idle = fixture.apply(
            turn,
            4,
            JobToolCall::JobWorkers {
                after: None,
                limit: None,
            },
        );
        assert_eq!(idle["workers"][0]["idle"], true);
        let assigned = fixture.apply(
            turn,
            5,
            JobToolCall::JobAssign {
                conversation: worker.to_string(),
                objective: "follow-up work".into(),
            },
        );
        let next = assigned["turn"].as_str().unwrap().parse().unwrap();
        assert_ne!(next, first);
        assert_eq!(
            wait(fixture.database.job(next)).unwrap().unwrap().parent_id,
            Some(worker)
        );
        let second = JobToolCall::JobAssign {
            conversation: worker.to_string(),
            objective: "competing work".into(),
        };
        let call = fixture.admit(turn, 6, &second);
        assert!(matches!(
            wait(fixture.database.apply_job_tool(call, second)),
            Err(crate::database::DatabaseError::Rejected(
                Violation::InvalidValue("worker_not_idle", _)
            ))
        ));
        let unrelated = fixture.open().0;
        let takeover = JobToolCall::JobAssign {
            conversation: unrelated.to_string(),
            objective: "takeover".into(),
        };
        let call = fixture.admit(turn, 7, &takeover);
        assert!(matches!(
            wait(fixture.database.apply_job_tool(call, takeover)),
            Err(crate::database::DatabaseError::Rejected(
                Violation::InvalidValue("rights", _)
            ))
        ));
        assert_eq!(
            wait(fixture.database.conversation_limits(parent))
                .unwrap()
                .unwrap()
                .1,
            0
        );
        fixture.worker.shutdown().unwrap();
    }

    #[test]
    fn conversation_limits_are_inherited_and_agent_updates_cannot_escalate() {
        use crate::orchestration::job_tools::JobToolCall;
        let fixture = Fixture::new("managed-depth");
        let (parent, turn, _) = fixture.dispatch_parent();
        let read = fixture.apply(
            turn,
            0,
            JobToolCall::JobLimits {
                conversation: None,
                max_depth: None,
                max_auto_turns: None,
                max_iterations: None,
            },
        );
        assert_eq!(read["max_depth"], 3);
        assert_eq!(read["max_iterations"], 100);
        fixture.apply(
            turn,
            1,
            JobToolCall::JobLimits {
                conversation: None,
                max_depth: Some(1),
                max_auto_turns: Some(2),
                max_iterations: Some(3),
            },
        );
        let escalation = JobToolCall::JobLimits {
            conversation: None,
            max_depth: Some(4),
            max_auto_turns: None,
            max_iterations: None,
        };
        let call = fixture.admit(turn, 2, &escalation);
        assert!(matches!(
            wait(fixture.database.apply_job_tool(call, escalation)),
            Err(crate::database::DatabaseError::Rejected(
                Violation::InvalidValue("limits", _)
            ))
        ));
        wait(fixture.database.set_conversation(
            parent,
            crate::persistence::ConversationEdit::Limits(ConversationLimits {
                max_depth: 4,
                ..Default::default()
            }),
        ))
        .unwrap();
        let mut ancestor = parent;
        for depth in 1..=4 {
            ancestor = fixture.child(ancestor, true).0;
            let (limits, actual) = wait(fixture.database.conversation_limits(ancestor))
                .unwrap()
                .unwrap();
            assert_eq!(actual, depth);
            assert_eq!(limits.max_depth, 4);
        }
        let refused = wait(fixture.database.open_conversation(
            Some(ancestor),
            ConversationSpec {
                name: None,
                agent: None,
                project: None,
                origin: Origin::User,
                keep_open: true,
            },
            message("too deep"),
            fixture.configuration,
            None,
        ));
        assert!(matches!(
            refused,
            Err(crate::database::DatabaseError::Rejected(
                Violation::InvalidValue("children", _)
            ))
        ));
        wait(fixture.database.set_conversation(
            parent,
            crate::persistence::ConversationEdit::Limits(ConversationLimits {
                max_depth: 2,
                ..Default::default()
            }),
        ))
        .unwrap();
        assert_eq!(
            wait(fixture.database.conversation_limits(ancestor))
                .unwrap()
                .unwrap()
                .1,
            4
        );
        fixture.worker.shutdown().unwrap();
    }

    #[test]
    fn scheduled_iterations_stop_at_the_inherited_limit() {
        let fixture = Fixture::new("finite-recurrence");
        let (parent, turn, agent) = fixture.dispatch_parent();
        wait(fixture.database.set_conversation(
            parent,
            crate::persistence::ConversationEdit::Limits(ConversationLimits {
                max_iterations: 2,
                ..Default::default()
            }),
        ))
        .unwrap();
        let (child, first) = fixture.dispatch(turn, 0, agent, true, Some(60), vec![]);
        fixture.finish(first, OutcomeKind::Completed);
        let RecurrencePhase::WaitingDue { due } = fixture.recurrence(child).phase else {
            panic!("due")
        };
        wait(fixture.database.boundary_b(due + 1)).unwrap();
        let second = fixture.recurrence(child).current_turn;
        fixture.finish(second, OutcomeKind::Completed);
        let RecurrencePhase::WaitingDue { due } = fixture.recurrence(child).phase else {
            panic!("due")
        };
        wait(fixture.database.boundary_b(due + 1)).unwrap();
        assert_eq!(fixture.recurrence(child).n, 2);
        assert_eq!(
            fixture.recurrence(child).phase,
            RecurrencePhase::Halted {
                reason: RecurrenceHalt::LimitReached
            }
        );
        fixture.worker.shutdown().unwrap();
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
    fn job_tree_lists_children_dependencies_and_agents() {
        use crate::orchestration::job_tools::JobDependency;
        use crate::persistence::{DependencyCondition, DependencyState};
        let fixture = Fixture::new("job-tree");
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
        let tree = wait(fixture.database.job_tree(parent, None, 50, 2))
            .unwrap()
            .expect("tree");
        let root = &tree.nodes[0];
        assert_eq!(root.id, parent);
        assert_eq!(
            root.agent.as_ref().and_then(|(_, name)| name.as_deref()),
            Some("dispatcher")
        );
        match &root.facts {
            TreeFacts::Conversation {
                state, active_turn, ..
            } => {
                assert_eq!(*state, "open");
                assert_eq!(active_turn.as_ref().map(|(turn, _, _)| *turn), Some(turn));
            }
            other => panic!("the root is a conversation: {other:?}"),
        }
        let children = root.children.as_ref().expect("the root's children");
        assert_eq!(children.len(), 3);
        assert_eq!(tree.nodes[children[0]].id, turn);
        assert_eq!(tree.nodes[children[1]].id, child);
        assert_eq!(tree.nodes[children[2]].id, waiter);
        match &tree.nodes[children[0]].facts {
            TreeFacts::Turn { state, .. } => assert_eq!(*state, "running"),
            other => panic!("the first child is a turn: {other:?}"),
        }
        for (index, name) in [
            (children[1], "dispatch target"),
            (children[2], "dispatch target"),
        ] {
            assert_eq!(
                tree.nodes[index]
                    .agent
                    .as_ref()
                    .and_then(|(_, name)| name.as_deref()),
                Some(name)
            );
        }
        let child_children = tree.nodes[children[1]]
            .children
            .as_ref()
            .expect("the child's first turn");
        assert_eq!(tree.nodes[child_children[0]].id, child_turn);
        let waiter_children = tree.nodes[children[2]]
            .children
            .as_ref()
            .expect("the waiter's first turn");
        assert_eq!(tree.nodes[waiter_children[0]].id, waiter_turn);
        match &tree.nodes[waiter_children[0]].facts {
            TreeFacts::Turn {
                state, depends_on, ..
            } => {
                assert_eq!(*state, "waiting");
                assert_eq!(
                    depends_on,
                    &vec![(
                        child,
                        DependencyCondition::Settled,
                        DependencyState::Waiting
                    )]
                );
            }
            other => panic!("the waiter's first child is a turn: {other:?}"),
        }
        let page = wait(fixture.database.job_tree(parent, None, 1, 2))
            .unwrap()
            .expect("tree");
        assert!(page.nodes[0].has_more_children);
        assert_eq!(page.nodes[0].next_after, Some(turn));
        let after = wait(fixture.database.job_tree(parent, Some(turn), 1, 2))
            .unwrap()
            .expect("tree");
        let after_children = after.nodes[0].children.as_ref().expect("children");
        assert_eq!(after.nodes[after_children[0]].id, child);
        let shallow = wait(fixture.database.job_tree(parent, None, 50, 1))
            .unwrap()
            .expect("tree");
        let shallow_children = shallow.nodes[0].children.as_ref().expect("children");
        assert!(shallow.nodes[shallow_children[1]].children.is_none());
        assert!(shallow.nodes[shallow_children[1]].has_more_children);
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
        let next = answer["turn"].as_str().unwrap().parse().unwrap();
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
            let next = value["turn"].as_str().unwrap().parse().unwrap();
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
            let retry = value["turn"].as_str().unwrap().parse().unwrap();
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
    fn awaiting(fixture: &Fixture, turn: JobId) -> ToolCallRef {
        use crate::persistence::*;
        fixture.claim_observed(turn);
        let call = ToolCallRef {
            job: turn,
            request: RequestNumber::FIRST,
            index: 0,
        };
        wait(fixture.database.record_tool_decision(NewToolEvidence {
            call,
            approval: None,
            tool_call_id: "awaiting".into(),
            name: "workspace_run_command".into(),
            spawns: false,
            decision: ToolDecision::AwaitingApproval(ToolAdmission {
                handler_version: "test-1".into(),
                effect: ToolEffect::NonIdempotent,
                parameters: "{}".into(),
                arguments: ToolPayload::new("{}"),
                authorization: AuthorizationBasis::RegistryGrant,
            }),
        }))
        .unwrap();
        call
    }

    #[test]
    fn approval_indexes_follow_request_decision_and_execution_and_detect_a_missing_row() {
        use crate::persistence::*;
        let fixture = Fixture::new("approval-indexes");
        let (conversation, first) = fixture.open();
        let approval = awaiting(&fixture, first);
        wait(fixture.database.with_database(move |database| {
            assert!(derived::inconsistencies(database).unwrap().is_empty());
            let tx = database.begin_write().unwrap();
            tx.open_table(OPEN_APPROVALS)
                .unwrap()
                .remove(approvals::open_key(conversation, approval))
                .unwrap();
            tx.commit().unwrap();
            assert_eq!(
                derived::inconsistencies(database).unwrap(),
                ["open_approval"]
            );
            let tx = database.begin_write().unwrap();
            derived::rebuild(&tx, open::promotion_clock()).unwrap();
            tx.commit().unwrap();

            Ok(())
        }))
        .unwrap();
        let next = wait(
            fixture
                .database
                .decide_approval(approval, ApprovalDecision::Approved),
        )
        .unwrap()
        .turn;
        wait(fixture.database.with_database(|database| {
            assert!(derived::inconsistencies(database).unwrap().is_empty());
            Ok(())
        }))
        .unwrap();
        wait(
            fixture
                .database
                .finalize_job(first, JobFinalization::Completed(result("awaiting"))),
        )
        .unwrap();
        fixture.claim(next);
        wait(fixture.database.record_tool_decision(NewToolEvidence {
            call: ToolCallRef {
                job: next,
                request: RequestNumber::FIRST,
                index: 0,
            },
            approval: Some(approval),
            tool_call_id: "execution".into(),
            name: "workspace_run_command".into(),
            spawns: false,
            decision: ToolDecision::Refused(
                crate::openai_compatible::ToolCallRefusal::Unauthorized,
            ),
        }))
        .unwrap();
        wait(fixture.database.with_database(|database| {
            assert!(derived::inconsistencies(database).unwrap().is_empty());
            Ok(())
        }))
        .unwrap();
        fixture.worker.shutdown().unwrap();
    }

    #[test]
    fn concurrent_approval_decisions_append_exactly_once() {
        use crate::persistence::*;
        let fixture = Fixture::new("approval-race");
        let (_, first) = fixture.open();
        let approval = awaiting(&fixture, first);
        let mut threads = Vec::new();
        for _ in 0..2 {
            let db = fixture.database.clone();
            threads.push(std::thread::spawn(move || {
                wait(db.decide_approval(approval, ApprovalDecision::Approved))
            }));
        }
        let results = threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(
                    r,
                    Err(crate::database::DatabaseError::Rejected(
                        Violation::InvalidValue("approval_decided", _)
                    ))
                ))
                .count(),
            1
        );
        wait(fixture.database.with_database(|database| {
            let tx = database.begin_read().unwrap();
            assert_eq!(tx.open_table(APPROVALS).unwrap().iter().unwrap().count(), 1);
            assert_eq!(
                tx.open_table(TURN_INPUTS)
                    .unwrap()
                    .iter()
                    .unwrap()
                    .filter(|entry| matches!(
                        TurnEntry::decode(entry.as_ref().unwrap().1.value())
                            .unwrap()
                            .entry,
                        Entry::Decision { .. }
                    ))
                    .count(),
                1
            );

            Ok(())
        }))
        .unwrap();
        fixture.worker.shutdown().unwrap();
    }

    #[test]
    fn close_voids_pending_approval_and_settles_the_child() {
        use crate::persistence::*;
        let fixture = Fixture::new("approval-close");
        let (root, _) = fixture.open();
        let (child, turn) = fixture.child(root, false);
        let approval = awaiting(&fixture, turn);
        wait(
            fixture
                .database
                .finalize_job(turn, JobFinalization::Completed(result("waiting"))),
        )
        .unwrap();
        assert!(
            wait(fixture.database.job(child))
                .unwrap()
                .unwrap()
                .children_closed_at
                .is_none()
        );
        wait(fixture.database.close(child, Actor::User)).unwrap();
        assert!(matches!(
            wait(
                fixture
                    .database
                    .decide_approval(approval, ApprovalDecision::Approved)
            ),
            Err(crate::database::DatabaseError::Rejected(
                Violation::InvalidValue("approval_decided", _)
            ))
        ));
        assert!(
            wait(fixture.database.job(child))
                .unwrap()
                .unwrap()
                .subtree_settled
        );
        wait(fixture.database.with_database(move |database| {
            let tx = database.begin_read().unwrap();
            let table = tx.open_table(APPROVALS).unwrap();
            let row = table.get(tool_call_key(approval)).unwrap().unwrap();
            assert_eq!(
                ApprovalRecord::decode(row.value()).unwrap().decision,
                ApprovalDecision::Voided
            );

            Ok(())
        }))
        .unwrap();
        assert!(matches!(
            wait(fixture.database.tool_evidence(turn, None, 10)).unwrap()[0]
                .decision
                .fact,
            ToolDecision::AwaitingApproval(_)
        ));
        fixture.worker.shutdown().unwrap();
    }

    #[test]
    fn unresolved_approved_execution_blocks_retry_of_both_turns() {
        use crate::persistence::*;
        let fixture = Fixture::new("approval-retry");
        let (conversation, first) = fixture.open();
        let approval = awaiting(&fixture, first);
        let next = wait(
            fixture
                .database
                .decide_approval(approval, ApprovalDecision::Approved),
        )
        .unwrap()
        .turn;
        wait(
            fixture
                .database
                .finalize_job(first, JobFinalization::Completed(result("waiting"))),
        )
        .unwrap();
        fixture.claim(next);
        wait(fixture.database.record_tool_decision(NewToolEvidence {
            call: ToolCallRef {
                job: next,
                request: RequestNumber::FIRST,
                index: 0,
            },
            approval: Some(approval),
            tool_call_id: "execution".into(),
            name: "workspace_run_command".into(),
            spawns: true,
            decision: ToolDecision::Admitted(ToolAdmission {
                handler_version: "workspace_run_command-6".into(),
                effect: ToolEffect::NonIdempotent,
                parameters: "{}".into(),
                arguments: ToolPayload::new("{}"),
                authorization: AuthorizationBasis::RegistryGrant,
            }),
        }))
        .unwrap();
        for turn in [first, next] {
            assert!(matches!(
                wait(fixture.database.retry(conversation, turn, Actor::User)),
                Err(crate::database::DatabaseError::Rejected(
                    Violation::InvalidValue("retry_unresolved_approval", _)
                ))
            ));
        }
        fixture.worker.shutdown().unwrap();
    }

    #[test]
    fn delegated_conversations_keep_the_user_decider_beyond_default_depth() {
        let fixture = Fixture::new("deep-decider");
        let (root, _) = fixture.open();
        wait(fixture.database.set_conversation(
            root,
            crate::persistence::ConversationEdit::Limits(ConversationLimits {
                max_depth: 4,
                ..Default::default()
            }),
        ))
        .unwrap();
        let mut descendant = root;
        for _ in 0..4 {
            descendant = fixture.child(descendant, true).0;
        }
        assert_eq!(
            wait(fixture.database.conversation_limits(descendant))
                .unwrap()
                .unwrap()
                .1,
            4
        );
        assert!(wait(fixture.database.has_decider(Some(descendant))).unwrap());
        fixture.worker.shutdown().unwrap();
    }

    #[test]
    fn grandchild_card_carries_its_parent_and_disappears_after_decision() {
        use crate::persistence::ApprovalDecision;
        let fixture = Fixture::new("approval-grandchild");
        let (root, _) = fixture.open();
        let (child, _) = fixture.child(root, true);
        let (grandchild, turn) = fixture.child(child, true);
        let approval = awaiting(&fixture, turn);
        assert!(wait(fixture.database.has_decider(Some(grandchild))).unwrap());
        let cards = wait(fixture.database.conversation_cards(root)).unwrap();
        assert_eq!(cards.len(), 1);
        assert_eq!(cards[0]["parent"], child.to_string());
        assert_eq!(cards[0]["conversation"], grandchild.to_string());
        assert_eq!(
            cards[0]["approval"],
            serde_json::json!([turn.to_string(), approval.request.get(), approval.index])
        );
        wait(
            fixture
                .database
                .decide_approval(approval, ApprovalDecision::Rejected),
        )
        .unwrap();
        assert!(
            wait(fixture.database.conversation_cards(root))
                .unwrap()
                .is_empty()
        );
        fixture.worker.shutdown().unwrap();
    }
    fn send_first_request(fixture: &Fixture, turn: JobId) {
        fixture.claim(turn);
        let body =
            serde_json::json!({"model":"test", "messages":[{"role":"user","content":"first"}]});
        let (sent, _) = crate::provider_execution::RequestRecorder::default()
            .prepare(turn, RequestNumber::FIRST, &body, 0)
            .unwrap();
        wait(fixture.database.record_sent_request(sent)).unwrap();
    }

    fn finish_first_reply(fixture: &Fixture, first: JobId) {
        send_first_request(fixture, first);
        wait(fixture.database.finalize_job(
            first,
            crate::persistence::JobFinalization::Completed(result("Unchanged answer")),
        ))
        .unwrap();
    }

    fn title_turn(fixture: &Fixture, root: JobId) -> (JobId, JobId) {
        let children = wait(fixture.database.conversation_children(root, None, 10)).unwrap();
        assert_eq!(children.len(), 1);
        let child = children[0].conversation.root_job;
        let turns = wait(fixture.database.conversation_turns(child, None, 10)).unwrap();
        (child, turns[0].id)
    }

    #[test]
    fn responses_title_jobs_allow_room_for_reasoning_tokens() {
        use crate::persistence::{ConfigBody, NewConfig};

        let mut fixture = Fixture::new("responses-title-budget");
        let model = wait(
            fixture
                .database
                .config(fixture.configuration.llm_profile.into()),
        )
        .unwrap()
        .unwrap();
        let ConfigBody::Model(model) = model.body else {
            panic!("expected model");
        };
        let connection = wait(fixture.database.config(model.connection_id))
            .unwrap()
            .unwrap();
        let ConfigBody::Connection(mut config) = connection.body else {
            panic!("expected connection");
        };
        config.protocol = LlmTransport::OpenAiResponses;
        wait(fixture.database.update_config(
            model.connection_id,
            NewConfig {
                name: connection.name,
                body: ConfigBody::Connection(config),
            },
        ))
        .unwrap();
        fixture.pool = wait(fixture.database.create_executor(NewExecutor {
            name: "Responses worker".into(),
            concurrency_limit: NonZeroU32::new(16).unwrap(),
            llm_transports: BTreeSet::from([LlmTransport::OpenAiResponses]),
        }))
        .unwrap();
        let (root, first) = fixture.open();
        finish_first_reply(&fixture, first);
        let (_, title) = title_turn(&fixture, root);
        let configuration = wait(fixture.database.job_configuration(title))
            .unwrap()
            .unwrap();
        assert_eq!(
            configuration.frozen.llm.provider.transport,
            LlmTransport::OpenAiResponses
        );
        assert_eq!(
            configuration.frozen.llm.config.parameters.max_tokens,
            Some(4096)
        );
        assert_eq!(
            configuration.frozen.llm.config.declared_capabilities,
            BTreeSet::from([ModelCapability::Chat])
        );
        fixture.worker.shutdown().unwrap();
    }

    #[test]
    fn a_title_runs_in_a_separate_tool_free_job_and_keeps_the_reply_unchanged() {
        let fixture = Fixture::new("title-job");
        let agent = fixture.agent("original Agent");
        let (root, first) = fixture.open_as(agent);
        finish_first_reply(&fixture, first);
        let (child, turn) = title_turn(&fixture, root);
        let configuration = wait(fixture.database.job_configuration(turn))
            .unwrap()
            .unwrap();
        assert_eq!(configuration.frozen.agent, None);
        assert_eq!(configuration.frozen.agent_ref, None);
        assert_eq!(
            configuration.frozen.llm.config.declared_capabilities,
            BTreeSet::from([ModelCapability::Chat])
        );
        assert_eq!(
            configuration.frozen.llm.config.parameters.max_tokens,
            Some(256)
        );
        assert_eq!(
            wait(fixture.database.job(turn))
                .unwrap()
                .unwrap()
                .name
                .as_deref(),
            Some(crate::persistence::TITLE_JOB_NAME)
        );
        let next = wait(
            fixture
                .database
                .append(root, message("follow-up"), None, None),
        )
        .unwrap();
        assert!(
            wait(fixture.database.read_turn_context(next.turn, 10000))
                .unwrap()
                .tree_status
                .is_empty()
        );
        fixture.claim(turn);
        wait(fixture.database.finalize_job(
            turn,
            crate::persistence::JobFinalization::Completed(result(r#"{"title":"Topic"}"#)),
        ))
        .unwrap();
        assert!(
            wait(fixture.database.job(child))
                .unwrap()
                .unwrap()
                .outcome
                .is_some()
        );
        let summary = wait(fixture.database.conversations(None, 100))
            .unwrap()
            .conversations
            .into_iter()
            .find(|entry| entry.root_job == root)
            .unwrap();
        assert_eq!(summary.title, "Topic");
        let context = wait(fixture.database.read_turn_context(next.turn, 10000)).unwrap();
        assert_eq!(context.turns[0].answer.as_deref(), Some("Unchanged answer"));
        assert!(
            !context
                .turns
                .iter()
                .any(|turn| turn.text.contains("Generate a concise topic"))
        );
        assert!(
            wait(fixture.database.notices(root, 0, 100))
                .unwrap()
                .is_empty()
        );
        assert!(
            wait(fixture.database.with_database(derived::inconsistencies))
                .unwrap()
                .is_empty()
        );
        fixture.worker.shutdown().unwrap();
    }

    #[test]
    fn title_job_failure_is_quiet_and_a_manual_title_wins_a_late_result() {
        for failure in [true, false] {
            let fixture = Fixture::new(if failure {
                "title-failure"
            } else {
                "title-race"
            });
            let (root, first) = fixture.open();
            finish_first_reply(&fixture, first);
            let (child, turn) = title_turn(&fixture, root);
            wait(fixture.database.set_conversation(
                root,
                crate::persistence::ConversationEdit::Title("Manual title".into()),
            ))
            .unwrap();
            fixture.claim(turn);
            let settlement = if failure {
                crate::persistence::JobFinalization::Failed(result("provider failed"))
            } else {
                crate::persistence::JobFinalization::Completed(result(
                    r#"{"title":"Automatic title"}"#,
                ))
            };
            wait(fixture.database.finalize_job(turn, settlement)).unwrap();
            assert!(
                wait(fixture.database.job(child))
                    .unwrap()
                    .unwrap()
                    .outcome
                    .is_some()
            );
            let summary = wait(fixture.database.conversations(None, 100))
                .unwrap()
                .conversations
                .into_iter()
                .find(|entry| entry.root_job == root)
                .unwrap();
            assert_eq!(summary.title, "Manual title");
            assert!(
                wait(fixture.database.notices(root, 0, 100))
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(
                wait(fixture.database.conversation_turns(root, None, 10))
                    .unwrap()
                    .len(),
                1
            );
            fixture.worker.shutdown().unwrap();
        }
    }

    #[test]
    fn disabled_child_work_skips_title_generation_without_failing_the_reply() {
        let fixture = Fixture::new("title-budget");
        let (root, first) = fixture.open();
        wait(fixture.database.set_conversation(
            root,
            crate::persistence::ConversationEdit::Limits(ConversationLimits {
                max_depth: 0,
                ..Default::default()
            }),
        ))
        .unwrap();
        finish_first_reply(&fixture, first);
        assert!(
            wait(fixture.database.conversation_children(root, None, 10))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            wait(fixture.database.job(first))
                .unwrap()
                .unwrap()
                .outcome
                .unwrap()
                .kind,
            OutcomeKind::Completed
        );
        fixture.worker.shutdown().unwrap();
    }
    #[test]
    fn an_explicit_job_name_does_not_start_automatic_naming_work() {
        let fixture = Fixture::new("title-explicit-name");
        let (root, first) = wait(fixture.database.open_conversation(
            None,
            ConversationSpec {
                name: Some("User supplied name".into()),
                agent: None,
                project: None,
                origin: Origin::User,
                keep_open: true,
            },
            message("first"),
            fixture.configuration,
            None,
        ))
        .unwrap();
        finish_first_reply(&fixture, first.turn);
        assert!(
            wait(fixture.database.conversation_children(root, None, 10))
                .unwrap()
                .is_empty()
        );
        fixture.worker.shutdown().unwrap();
    }
    fn decision_configs(fixture: &Fixture) -> (ConfigId, ConfigId) {
        use crate::persistence::*;
        let gen_model = wait(
            fixture
                .database
                .config(fixture.configuration.llm_profile.into()),
        )
        .expect("read")
        .expect("model");
        let ConfigBody::Model(gen_model) = gen_model.body else {
            panic!("generative model");
        };
        let model = wait(fixture.database.create_config(NewConfig {
            name: "decision model".into(),
            body: ConfigBody::DecisionModel(DecisionModelConfig {
                connection_id: gen_model.connection_id,
                model: "decision-v1".into(),
                protocol: DecisionProtocol::TypeSafeSystemOne,
                limits: DecisionLimits {
                    max_input_tokens_per_request: Some(64000),
                    max_input_tokens_per_question: Some(32000),
                    ..Default::default()
                },
                declared_capabilities: BTreeSet::from([
                    DecisionCapability::Choice,
                    DecisionCapability::BinaryProbability,
                ]),
            }),
        }))
        .expect("decision model");
        let agent = wait(fixture.database.create_config(NewConfig {
            name: "decision agent".into(),
            body: ConfigBody::DecisionAgent(DecisionAgentConfig {
                instructions: Some("Classify supplied evidence.".into()),
                models: vec![model],
                default_model: Some(model),
                workspace_access: WorkspaceAccess::None,
            }),
        }))
        .expect("decision agent");
        (model, agent)
    }

    #[test]
    fn decision_configs_reopen_without_breaking_ordinary_claims() {
        use crate::persistence::*;
        let fixture = Fixture::new("decision-reopen");
        let (model, agent) = decision_configs(&fixture);
        assert_eq!(
            wait(
                fixture
                    .database
                    .configs(Some(ConfigKind::DecisionModel), None, 10)
            )
            .expect("list")
            .len(),
            1
        );
        assert_eq!(
            wait(
                fixture
                    .database
                    .configs(Some(ConfigKind::DecisionAgent), None, 10)
            )
            .expect("list")
            .len(),
            1
        );
        let (_, turn) = fixture.open();
        assert!(
            wait(fixture.database.claim_batch())
                .expect("claim with decision rows")
                .contains(&turn)
        );
        wait(
            fixture
                .database
                .finalize_job(turn, JobFinalization::Completed(result("answer"))),
        )
        .expect("finalize");
        let model_record = wait(fixture.database.config(model))
            .expect("read")
            .expect("model");
        let agent_record = wait(fixture.database.config(agent))
            .expect("read")
            .expect("agent");
        fixture.worker.shutdown().expect("shutdown");
        let worker = DatabaseWorker::start(&fixture.directory).expect("reopen");
        assert_eq!(
            wait(worker.handle().config(model)).expect("read"),
            Some(model_record)
        );
        assert_eq!(
            wait(worker.handle().config(agent)).expect("read"),
            Some(agent_record)
        );
        assert_eq!(
            wait(worker.handle().job(turn))
                .expect("read")
                .expect("job")
                .outcome
                .expect("outcome")
                .kind,
            OutcomeKind::Completed
        );
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn decision_admission_refuses_wrong_kinds_bounds_and_capabilities() {
        use crate::persistence::*;
        let fixture = Fixture::new("decision-admission");
        let (model, agent) = decision_configs(&fixture);
        let record = wait(fixture.database.config(model))
            .expect("read")
            .expect("model");
        let ConfigBody::DecisionModel(body) = record.body else {
            panic!("decision");
        };
        let mut invalid_models = Vec::new();
        let mut invalid = body.clone();
        invalid.declared_capabilities.clear();
        invalid_models.push(invalid);
        let mut invalid = body.clone();
        invalid.connection_id = agent;
        invalid_models.push(invalid);
        let mut invalid = body.clone();
        invalid.limits.max_input_tokens_per_question = Some(65000);
        invalid_models.push(invalid);
        let mut invalid = body.clone();
        invalid.limits.max_input_tokens_per_request = Some(0);
        invalid_models.push(invalid);
        let mut invalid = body;
        invalid.model = " ".into();
        invalid_models.push(invalid);
        for invalid in invalid_models {
            assert!(matches!(
                wait(fixture.database.create_config(NewConfig {
                    name: "invalid".into(),
                    body: ConfigBody::DecisionModel(invalid)
                })),
                Err(crate::database::DatabaseError::Rejected(_))
            ));
        }
        for invalid in [
            DecisionAgentConfig {
                models: vec![fixture.configuration.llm_profile.into()],
                ..Default::default()
            },
            DecisionAgentConfig {
                models: vec![model, model],
                ..Default::default()
            },
            DecisionAgentConfig {
                models: vec![model],
                default_model: Some(agent),
                ..Default::default()
            },
            DecisionAgentConfig {
                instructions: Some("x".repeat(65537)),
                ..Default::default()
            },
            DecisionAgentConfig {
                models: vec![model; 257],
                ..Default::default()
            },
        ] {
            assert!(matches!(
                wait(fixture.database.create_config(NewConfig {
                    name: "invalid".into(),
                    body: ConfigBody::DecisionAgent(invalid)
                })),
                Err(crate::database::DatabaseError::Rejected(_))
            ));
        }
        assert_eq!(
            wait(
                fixture
                    .database
                    .configs(Some(ConfigKind::DecisionModel), None, 10)
            )
            .expect("list")
            .len(),
            1
        );
        fixture.worker.shutdown().expect("shutdown");
    }

    #[test]
    fn decision_freeze_preserves_versions_and_refuses_text_execution_and_archived_targets() {
        use crate::persistence::*;
        let fixture = Fixture::new("decision-freeze");
        let (model, agent) = decision_configs(&fixture);
        let config = ExecutionConfiguration {
            agent_profile: Some(agent.into()),
            llm_profile: model.into(),
        };
        let snapshot = wait(fixture.database.with_database(move |database| {
            let outcome = run_batch_with(database, 1, Commit::Durable, |batch| {
                batch.freeze_configuration(config)
            });
            let FrozenConfiguration::Decision(snapshot) = outcome.result.expect("freeze") else {
                panic!("decision snapshot");
            };
            Ok(snapshot)
        }))
        .expect("snapshot");
        let frozen_work = DecisionWork {
            frozen: snapshot.clone(),
            request: DecisionRequest {
                schema_version: DECISION_SCHEMA_VERSION,
                state: serde_json::json!({"tokens": 24000}),
                question: DecisionQuestion::BinaryProbability {
                    prompt: "Will pressure increase?".into(),
                },
            },
        };
        wait(fixture.database.with_database(move |database| {
            let spec = JobRow {
                id: JobId::new(1),
                created_at: Timestamp::now(),
                parent_id: None,
                kind: JobKind::Turn {
                    name: None,
                    execution: TurnExecution::Decision(frozen_work),
                    plan_source: PlanSource::Process {
                        project: None,
                        grant: None,
                        eligible_at: None,
                        objective: None,
                        attachments: Vec::new(),
                    },
                },
            };
            assert_eq!(JobRow::decode(&spec.encode())?, spec);
            assert!(matches!(
                run_batch_with(database, 1, Commit::Durable, |batch| batch
                    .t1_submit(None, spec))
                .result,
                Err(BatchFailure::Rejected(_))
            ));
            Ok(())
        }))
        .expect("typed work codec and admission gate");
        let original = serde_json::to_value(&snapshot).expect("serialize snapshot");
        let mut changed = wait(fixture.database.config(model))
            .expect("read")
            .expect("model");
        if let ConfigBody::DecisionModel(body) = &mut changed.body {
            body.model = "decision-v2".into();
        }
        wait(fixture.database.update_config(
            model,
            NewConfig {
                name: changed.name,
                body: changed.body,
            },
        ))
        .expect("edit");
        assert_eq!(snapshot.model.model, "decision-v1");
        assert_eq!(
            serde_json::from_value::<FrozenDecisionConfig>(original).expect("decode snapshot"),
            snapshot
        );
        let refusal = wait(fixture.database.open_conversation(
            None,
            ConversationSpec {
                name: None,
                agent: None,
                project: None,
                origin: Origin::User,
                keep_open: true,
            },
            message("not generative"),
            config,
            None,
        ));
        assert!(matches!(
            refusal,
            Err(crate::database::DatabaseError::Rejected(_))
        ));
        wait(fixture.database.archive_model(model)).expect("archive decision model");
        assert!(
            wait(fixture.database.config(model))
                .expect("read")
                .expect("archived")
                .archived_at
                .is_some()
        );
        let ConfigBody::DecisionAgent(body) = wait(fixture.database.config(agent))
            .expect("read")
            .expect("agent")
            .body
        else {
            panic!("decision agent");
        };
        assert!(body.models.is_empty() && body.default_model.is_none());
        wait(fixture.database.with_database(move |database| {
            assert!(matches!(
                run_batch_with(database, 1, Commit::Durable, |batch| batch
                    .freeze_configuration(config))
                .result,
                Err(BatchFailure::Rejected(_))
            ));
            Ok(())
        }))
        .expect("archived refusal");
        assert_eq!(snapshot.model.model, "decision-v1");
        fixture.worker.shutdown().expect("shutdown");
    }

    #[test]
    fn archive_models_clears_live_policy_bindings_without_mutating_frozen_work() {
        use crate::persistence::*;
        let fixture = Fixture::new("archive-policy-bindings");
        let (decision_model, decision_agent) = decision_configs(&fixture);
        let backup_body = wait(fixture.database.config(decision_model))
            .unwrap()
            .unwrap()
            .body;
        let backup = wait(fixture.database.create_config(NewConfig {
            name: "backup decision model".into(),
            body: backup_body,
        }))
        .unwrap();
        let mut agent = wait(fixture.database.config(decision_agent))
            .unwrap()
            .unwrap();
        if let ConfigBody::DecisionAgent(body) = &mut agent.body {
            body.models.push(backup);
        }
        wait(fixture.database.update_config(
            decision_agent,
            NewConfig {
                name: agent.name,
                body: agent.body,
            },
        ))
        .unwrap();
        let generative = ConfigId::from(fixture.configuration.llm_profile);
        let ConfigBody::Model(model_body) = wait(fixture.database.config(generative))
            .unwrap()
            .unwrap()
            .body
        else {
            panic!("model");
        };
        let summarizer = wait(fixture.database.create_config(NewConfig {
            name: "summarizer".into(),
            body: ConfigBody::Model(model_body),
        }))
        .unwrap();
        let policy = CompactionPolicy {
            mode: CompactionMode::Auto {
                preset: CompactionPreset::Balanced,
            },
            summarization_model: Some(summarizer),
            predictive: Some(PredictiveCompaction {
                decision_agent,
                minimum_interval_seconds: 120,
                max_preparations: 1,
                max_prediction_tokens: 1000,
                max_preparation_tokens: 12000,
            }),
        };
        let profile = wait(fixture.database.create_config(NewConfig {
            name: "assisted".into(),
            body: ConfigBody::Agent(AgentConfig {
                models: vec![generative],
                decision_assistance: Some(decision_agent),
                compaction_policy: Some(policy),
                ..Default::default()
            }),
        }))
        .unwrap();
        let (conversation, turn) = fixture.open_as(profile.into());
        wait(fixture.database.set_conversation(
            conversation,
            ConversationEdit::CompactionPolicy(Some(policy)),
        ))
        .unwrap();
        let frozen = wait(fixture.database.with_database(move |database| {
            let rx = database.begin_read()?;
            JobRow::decode_at(
                turn.get(),
                rx.open_table(JOBS)?.get(turn.get())?.unwrap().value(),
            )
        }))
        .unwrap();
        wait(fixture.database.archive_model(summarizer)).unwrap();
        let (stored_policy, revision) = wait(
            fixture
                .database
                .conversation_compaction_policy(conversation),
        )
        .unwrap()
        .unwrap();
        assert_eq!(revision, 2);
        assert_eq!(stored_policy.unwrap().summarization_model, None);
        assert!(stored_policy.unwrap().predictive.is_some());
        wait(fixture.database.archive_model(decision_model)).unwrap();
        let (policy_with_backup, revision) = wait(
            fixture
                .database
                .conversation_compaction_policy(conversation),
        )
        .unwrap()
        .unwrap();
        assert_eq!(revision, 2);
        assert!(policy_with_backup.unwrap().predictive.is_some());
        wait(fixture.database.archive_model(backup)).unwrap();
        let (stored_policy, revision) = wait(
            fixture
                .database
                .conversation_compaction_policy(conversation),
        )
        .unwrap()
        .unwrap();
        assert_eq!(revision, 3);
        let policy_after = stored_policy.unwrap();
        assert_eq!(policy_after.mode, policy.mode);
        assert!(policy_after.summarization_model.is_none() && policy_after.predictive.is_none());
        let current = wait(fixture.database.config(profile)).unwrap().unwrap();
        let ConfigBody::Agent(body) = &current.body else {
            panic!("agent");
        };
        assert_eq!(body.compaction_policy, Some(policy_after));
        assert_eq!(body.decision_assistance, Some(decision_agent));
        wait(fixture.database.update_config(
            profile,
            NewConfig {
                name: "still editable".into(),
                body: current.body,
            },
        ))
        .unwrap();
        wait(fixture.database.with_database(move |database| {
            let rx = database.begin_read()?;
            assert_eq!(
                JobRow::decode_at(
                    turn.get(),
                    rx.open_table(JOBS)?.get(turn.get())?.unwrap().value()
                )?,
                frozen
            );
            Ok(())
        }))
        .unwrap();
        wait(fixture.database.archive_model(decision_model)).unwrap();
        assert_eq!(
            wait(
                fixture
                    .database
                    .conversation_compaction_policy(conversation)
            )
            .unwrap()
            .unwrap()
            .1,
            3
        );
        wait(fixture.database.archive_model(generative)).unwrap();
        fixture.worker.shutdown().unwrap();
    }

    #[test]
    fn compaction_policy_revision_round_trips_and_off_overrides_inheritance() {
        use crate::persistence::*;
        let fixture = Fixture::new("compaction-policy");
        let (_, agent) = decision_configs(&fixture);
        let (conversation, _) = fixture.open();
        let policy = CompactionPolicy {
            mode: CompactionMode::Custom {
                policy: CustomPolicy {
                    trigger_tokens: 24000,
                    target_tokens: 12000,
                    reserved_output_tokens: 4000,
                    minimum_source_tokens: 8000,
                    minimum_savings_tokens: 4000,
                    preserve_recent_turns: 3,
                    cooldown_seconds: 120,
                },
            },
            summarization_model: Some(fixture.configuration.llm_profile.into()),
            predictive: Some(PredictiveCompaction {
                decision_agent: agent,
                minimum_interval_seconds: 120,
                max_preparations: 1,
                max_prediction_tokens: 1000,
                max_preparation_tokens: 12000,
            }),
        };
        assert_eq!(
            wait(
                fixture
                    .database
                    .conversation_compaction_policy(conversation)
            )
            .expect("read"),
            Some((None, 0))
        );
        wait(fixture.database.set_conversation(
            conversation,
            ConversationEdit::CompactionPolicy(Some(policy)),
        ))
        .expect("set policy");
        wait(fixture.database.set_conversation(
            conversation,
            ConversationEdit::CompactionPolicy(Some(policy)),
        ))
        .expect("idempotent value");
        assert_eq!(
            wait(
                fixture
                    .database
                    .conversation_compaction_policy(conversation)
            )
            .expect("read"),
            Some((Some(policy), 1))
        );
        let off = CompactionPolicy::default();
        assert_eq!(CompactionPolicy::resolve(policy, None, Some(off)), off);
        assert!(
            CompactionPolicy::resolve(policy, None, None)
                .predictive
                .is_none()
        );
        assert_eq!(CompactionPolicy::resolve(off, Some(policy), None), policy);
        let invalid = CompactionPolicy {
            predictive: Some(PredictiveCompaction {
                max_preparations: 2,
                ..policy.predictive.expect("predictive")
            }),
            ..policy
        };
        assert!(
            wait(fixture.database.set_conversation(
                conversation,
                ConversationEdit::CompactionPolicy(Some(invalid))
            ))
            .is_err()
        );
        let invalid = CompactionPolicy {
            summarization_model: Some(agent),
            ..policy
        };
        assert!(
            wait(fixture.database.set_conversation(
                conversation,
                ConversationEdit::CompactionPolicy(Some(invalid))
            ))
            .is_err()
        );
        fixture.worker.shutdown().expect("shutdown");
        let worker = DatabaseWorker::start(&fixture.directory).expect("reopen");
        assert_eq!(
            wait(worker.handle().conversation_compaction_policy(conversation)).expect("read"),
            Some((Some(policy), 1))
        );
        wait(
            worker
                .handle()
                .set_conversation(conversation, ConversationEdit::CompactionPolicy(Some(off))),
        )
        .expect("disable");
        assert_eq!(
            wait(worker.handle().conversation_compaction_policy(conversation)).expect("read"),
            Some((Some(off), 2))
        );
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn context_metadata_is_append_only_frozen_by_claim_and_absent_from_prompt() {
        use crate::persistence::*;
        let fixture = Fixture::new("context-metadata");
        let (conversation, consumer) = fixture.open();
        let (system, appended) = wait(fixture.database.open_conversation(
            Some(conversation),
            ConversationSpec {
                name: Some("prepare".into()),
                agent: None,
                project: None,
                origin: Origin::System,
                keep_open: false,
            },
            message("summarize sealed source"),
            fixture.configuration,
            None,
        ))
        .expect("system child");
        let producer = appended.turn;
        wait(fixture.database.with_database(move |database| {
            let preparation = Entry::ContextPreparation {
                job: producer,
                policy_revision: 0,
            };
            assert_eq!(
                TurnEntry::decode(
                    &TurnEntry {
                        entry: preparation.clone(),
                        at: Timestamp::now()
                    }
                    .encode()
                )?
                .entry,
                preparation
            );
            assert!(
                run_batch_with(database, 1, Commit::Durable, |batch| batch
                    .insert_turn_input(consumer, preparation))
                .result
                .is_ok()
            );
            assert!(matches!(
                run_batch_with(database, 1, Commit::Durable, |batch| batch
                    .insert_turn_input(
                        consumer,
                        Entry::ContextUse {
                            artifact: Some(producer)
                        }
                    ))
                .result,
                Err(BatchFailure::Rejected(_))
            ));
            Ok(())
        }))
        .expect("metadata");
        let artifact = SummarizationResult {
            schema_version: 1,
            source: ContextSourceManifest {
                schema_version: 1,
                conversation,
                through_turn: consumer,
                turns: vec![SourceBoundary {
                    turn: consumer,
                    input_through: 1,
                    output_through: 0,
                    content: ContentDigest::of(b"sealed input"),
                }],
                base_artifact: None,
            },
            summary: "durable task facts".into(),
            omitted_details: vec!["incidental wording".into()],
        };
        artifact.validate().expect("artifact");
        fixture.claim(producer);
        wait(fixture.database.finalize_job(
            producer,
            JobFinalization::Completed(FinalResult {
                value: Some(serde_json::to_value(&artifact).expect("artifact")),
                ..result("summary prepared")
            }),
        ))
        .expect("finish");
        wait(fixture.database.with_database(move |database| {
            assert!(
                run_batch_with(database, 1, Commit::Durable, |batch| batch
                    .insert_turn_input(
                        consumer,
                        Entry::ContextUse {
                            artifact: Some(producer)
                        }
                    ))
                .result
                .is_ok()
            );
            Ok(())
        }))
        .expect("candidate selection");
        let context = wait(fixture.database.read_turn_context(consumer, 10000)).expect("context");
        let text = context
            .turns
            .iter()
            .map(|turn| turn.text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(!text.contains("durable task facts"));
        assert!(!text.contains(&producer.to_string()));
        let metadata_matches = wait(fixture.database.search_conversations(
            vec![producer.to_string()],
            None,
            10,
        ))
        .expect("search metadata reference");
        assert!(metadata_matches.conversations.is_empty());
        let history_matches = wait(fixture.database.search_conversations(
            vec!["first".into()],
            None,
            10,
        ))
        .expect("search original history");
        assert!(
            history_matches
                .conversations
                .iter()
                .any(|entry| entry.root_job == conversation)
        );
        assert!(
            wait(fixture.database.with_database(derived::inconsistencies))
                .expect("search index with context metadata")
                .is_empty()
        );
        let entries = wait(fixture.database.turn_entries(consumer)).expect("entries");
        assert_eq!(entries.len(), 3);
        wait(fixture.database.with_database(move |database| {
            assert!(
                run_batch_with(database, 1, Commit::Durable, |batch| batch
                    .insert_turn_input(consumer, Entry::ContextUse { artifact: None }))
                .result
                .is_ok()
            );
            Ok(())
        }))
        .expect("explicit original-context fallback");
        let entries = wait(fixture.database.turn_entries(consumer)).expect("entries");
        assert!(matches!(
            entries.last().expect("fallback").1.entry,
            Entry::ContextUse { artifact: None }
        ));
        assert!(
            wait(fixture.database.append(
                conversation,
                Entry::ContextUse {
                    artifact: Some(producer)
                },
                None,
                None
            ))
            .is_err()
        );
        fixture.claim(consumer);
        wait(fixture.database.with_database(move |database| {
            assert!(matches!(
                run_batch_with(database, 1, Commit::Durable, |batch| batch
                    .insert_turn_input(
                        consumer,
                        Entry::ContextUse {
                            artifact: Some(producer)
                        }
                    ))
                .result,
                Err(BatchFailure::Rejected(_))
            ));
            Ok(())
        }))
        .expect("claim boundary");
        assert!(wait(fixture.database.job(system)).expect("read").is_some());
        fixture.worker.shutdown().expect("shutdown");
        let worker = DatabaseWorker::start(&fixture.directory).expect("reopen");
        assert_eq!(
            wait(worker.handle().turn_entries(consumer)).expect("entries"),
            entries
        );
        worker.shutdown().expect("shutdown");
    }
}
