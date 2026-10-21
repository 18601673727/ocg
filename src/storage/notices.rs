use super::*;
use crate::persistence::OutcomeKind;

pub(crate) fn notice_class(
    event: &NoticeEvent,
    caused_by: &BTreeSet<EntryKind>,
) -> Option<NoticeClass> {
    match event {
        NoticeEvent::ClosedSelf
        | NoticeEvent::Stalled
        | NoticeEvent::RecurrenceHalted
        | NoticeEvent::WaitersBlocked { .. } => Some(NoticeClass::Attention),
        NoticeEvent::ObjectiveCompleted => {
            if caused_by == &BTreeSet::from([EntryKind::UserMessage]) {
                None
            } else {
                Some(NoticeClass::Attention)
            }
        }
        NoticeEvent::ClosedAbove | NoticeEvent::StoppedAbove | NoticeEvent::Requested => {
            Some(NoticeClass::Progress)
        }
        NoticeEvent::IterationCompleted => completed_class(caused_by),
        NoticeEvent::TurnEnded { kind } => match kind {
            OutcomeKind::Failed | OutcomeKind::Unknown | OutcomeKind::Cancelled => {
                Some(NoticeClass::Attention)
            }
            OutcomeKind::Completed => completed_class(caused_by),
        },
    }
}
fn completed_class(causes: &BTreeSet<EntryKind>) -> Option<NoticeClass> {
    let mut class = None;
    for cause in causes {
        match cause {
            EntryKind::UserMessage | EntryKind::ResponseFormat => {}
            EntryKind::AgentMessage => return Some(NoticeClass::Attention),
            EntryKind::Notice | EntryKind::Decision | EntryKind::RetryOf | EntryKind::Iteration => {
                class = Some(NoticeClass::Progress)
            }
        }
    }
    class
}
impl Batch {
    pub(crate) fn last_notice_seq(&mut self, conversation: JobId) -> Result<u32, BatchError> {
        self.step(|tx| {
            Ok(tx
                .open_table(NOTICES)?
                .range((conversation.get(), 0)..=(conversation.get(), u32::MAX))?
                .next_back()
                .transpose()?
                .map_or(0, |(key, _)| key.value().1))
        })
    }
    pub(crate) fn put_notice(
        &mut self,
        conversation: JobId,
        notice: Notice,
    ) -> Result<u32, BatchError> {
        let seq = self.last_notice_seq(conversation)? + 1;
        self.step(|tx| {
            tx.open_table(NOTICES)?
                .insert((conversation.get(), seq), notice.encode().as_slice())?;
            Ok(seq)
        })
    }
    pub(crate) fn put_record(
        &mut self,
        conversation: JobId,
        event: RecordEvent,
        by: Actor,
    ) -> Result<u32, BatchError> {
        self.put_notice(
            conversation,
            Notice {
                class: NoticeClass::Record,
                body: NoticeBody::Record {
                    event,
                    by,
                    at: self.now,
                },
            },
        )
    }
    pub(crate) fn attention_fallback(
        &mut self,
        conversation: JobId,
        entry: Entry,
    ) -> Result<Appended, BatchError> {
        if matches!(entry, Entry::Iteration { .. } | Entry::Decision { .. }) {
            return Ok(Appended {
                turn: conversation,
                seq: 0,
                joined: false,
            });
        }
        let Entry::Notice {
            child,
            event,
            reference,
        } = entry
        else {
            return self.corrupted("event append must be a notice or iteration".into());
        };
        let seq = self.put_notice(
            conversation,
            Notice {
                class: NoticeClass::Attention,
                body: NoticeBody::Event {
                    child,
                    event,
                    reference,
                },
            },
        )?;
        Ok(Appended {
            turn: conversation,
            seq,
            joined: false,
        })
    }
    pub(crate) fn notify(
        &mut self,
        child: JobId,
        event: NoticeEvent,
        reference: JobId,
    ) -> Result<(), BatchError> {
        let Some(parent) = self.require_job(child)?.parent_id else {
            return Ok(());
        };
        if !self.require_job(parent)?.kind.is_conversation() {
            return Ok(());
        }
        let first_iteration = self.recurrence(child)?.is_some()
            && self.require_job(reference)?.kind.previous().is_none();
        let causes = self.step(|tx| {
            let mut causes = BTreeSet::new();
            for entry in tx
                .open_table(TURN_INPUTS)?
                .range((reference.get(), 0)..=(reference.get(), u32::MAX))?
            {
                let (key, bytes) = entry?;
                let kind = TurnEntry::decode(bytes.value())?.entry.kind();
                if kind == EntryKind::ResponseFormat {
                    continue;
                }
                causes.insert(
                    if first_iteration && key.value().1 == 1 && kind == EntryKind::AgentMessage {
                        EntryKind::Iteration
                    } else {
                        kind
                    },
                );
            }
            Ok(causes)
        })?;
        match notice_class(&event, &causes) {
            Some(NoticeClass::Attention) => {
                self.append(
                    parent,
                    Entry::Notice {
                        child,
                        event,
                        reference,
                    },
                    None,
                    None,
                    true,
                )?;
            }
            Some(NoticeClass::Progress | NoticeClass::Record) => {
                self.put_notice(
                    parent,
                    Notice {
                        class: NoticeClass::Progress,
                        body: NoticeBody::Event {
                            child,
                            event,
                            reference,
                        },
                    },
                )?;
            }
            None => {}
        }
        Ok(())
    }
    pub(crate) fn turn_ended(&mut self, turn: JobId) -> Result<(), BatchError> {
        let row = self.require_job(turn)?;
        if !row.kind.is_stored() {
            return Ok(());
        }
        let Some(conversation) = row.parent_id else {
            return Ok(());
        };
        let state = self.require_job_state(turn)?;
        let Some(outcome) = state.outcome else {
            return Ok(());
        };
        let recurrence = self.recurrence(conversation)?;
        let iteration = recurrence
            .as_ref()
            .is_some_and(|row| row.current_turn == turn && row.phase == RecurrencePhase::Active);
        self.count_loop_turn(conversation, turn)?;
        let quarantined = outcome.kind == OutcomeKind::Unknown && state.stop_confirmed_at.is_none();
        if quarantined {
            return self.notify(conversation, NoticeEvent::Stalled, turn);
        }
        if self.auto_close_check(conversation)? {
            return Ok(());
        }
        if matches!(
            outcome.cause,
            Some(crate::persistence::OutcomeCause::Closed)
        ) {
            return Ok(());
        }
        if outcome.kind == OutcomeKind::Completed
            && self.step(|tx| {
                for entry in tx.open_table(OPEN_APPROVALS)?.range(
                    (conversation.get(), 0, 0, 0)
                        ..=(conversation.get(), u64::MAX, u32::MAX, u32::MAX),
                )? {
                    if entry?.1.value() == 0 {
                        return Ok(true);
                    }
                }
                Ok(false)
            })?
        {
            return Ok(());
        }
        let stopped = matches!(
            outcome.cause,
            Some(crate::persistence::OutcomeCause::Stopped { .. })
        );
        if iteration
            && !stopped
            && let Some(mut recurrence) = recurrence
        {
            recurrence.phase = RecurrencePhase::WaitingDue {
                due: crate::storage::promotion_clock()
                    .max(self.now().as_unix_micros())
                    .saturating_add(i64::from(recurrence.interval_s) * 1_000_000),
            };
            recurrence.previous_turn = Some(turn);
            self.put_recurrence(conversation, &recurrence, None)?;
        }
        let event = if stopped {
            NoticeEvent::StoppedAbove
        } else if outcome.kind == OutcomeKind::Completed && iteration {
            NoticeEvent::IterationCompleted
        } else if outcome.kind == OutcomeKind::Completed && row.kind.previous().is_none() {
            NoticeEvent::ObjectiveCompleted
        } else {
            NoticeEvent::TurnEnded { kind: outcome.kind }
        };
        self.notify(conversation, event, turn)
    }
    pub(crate) fn loop_held(&mut self, conversation: JobId) -> Result<bool, BatchError> {
        let hold = self.conversation(conversation)?.on_hold;
        self.step(|tx| {
            Ok(match hold {
                Some(seq) => tx
                    .open_table(NOTICES)?
                    .get((conversation.get(), seq))?
                    .map(|bytes| Notice::decode(bytes.value()))
                    .transpose()?
                    .is_some_and(|notice| {
                        matches!(
                            notice.body,
                            NoticeBody::Record {
                                by: Actor::Loop,
                                ..
                            }
                        )
                    }),
                None => false,
            })
        })
    }
    pub(crate) fn loop_hold_if_needed(&mut self, conversation: JobId) -> Result<(), BatchError> {
        let row = self.conversation(conversation)?;
        if row.loop_count < 8
            || row.on_hold.is_some()
            || row.closing.is_some()
            || matches!(
                self.require_job(conversation)?.kind,
                JobKind::Conversation {
                    origin: Origin::System,
                    ..
                }
            )
        {
            return Ok(());
        }
        if let Some(tip) = self.tip(conversation)? {
            let state = self.require_job_state(tip)?;
            if state.claim.is_none() && state.outcome.is_none() {
                self.hold(conversation, Actor::Loop, false)?;
            }
        }
        Ok(())
    }
    fn count_loop_turn(&mut self, conversation: JobId, turn: JobId) -> Result<(), BatchError> {
        if matches!(
            self.require_job(conversation)?.kind,
            JobKind::Conversation {
                origin: Origin::System,
                ..
            }
        ) {
            return Ok(());
        }
        let counts = self.step(|tx| {
            let mut attention = false;
            for entry in tx.open_table(TURN_INPUTS)?.range((turn.get(), 0)..=(turn.get(), u32::MAX))? {
                match TurnEntry::decode(entry?.1.value())?.entry {
                    Entry::UserMessage { .. } => return Ok(false),
                    Entry::Notice { .. } => attention = true,
                    _ => {}
                }
            }
            if !attention { return Ok(false); }
            for entry in tx.open_table(TOOL_CALLS)?.range((turn.get(), 0, 0)..=(turn.get(), u32::MAX, u32::MAX))? {
                let (key, bytes) = entry?;
                let record = decode_tool_evidence(tool_call_ref(key.value())?, bytes.value())?;
                if record.admission().is_some_and(|admission| admission.handler_version.starts_with("job_")) && matches!(record.outcome.map(|outcome| outcome.fact), Some(crate::persistence::ToolOutcome::Completed { result }) if serde_json::from_str::<serde_json::Value>(result.as_str()).is_ok_and(|result| result.get("refused").is_none())) { return Ok(true); }
            }
            Ok(false)
        })?;
        if counts {
            let mut row = self.conversation(conversation)?;
            row.loop_count = row.loop_count.saturating_add(1);
            self.insert_conversation(conversation, &row)?;
            self.loop_hold_if_needed(conversation)?;
        }
        Ok(())
    }
}
