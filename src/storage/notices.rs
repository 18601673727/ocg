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
        | NoticeEvent::MessageCompleted => Some(NoticeClass::Attention),
        NoticeEvent::ObjectiveCompleted => {
            if caused_by == &BTreeSet::from([EntryKind::UserMessage]) {
                None
            } else {
                Some(NoticeClass::Attention)
            }
        }
        NoticeEvent::ClosedAbove
        | NoticeEvent::StoppedAbove
        | NoticeEvent::Requested
        | NoticeEvent::IterationCompleted => Some(NoticeClass::Progress),
        NoticeEvent::TurnEnded { kind } => match kind {
            OutcomeKind::Failed | OutcomeKind::Unknown | OutcomeKind::Cancelled => {
                Some(NoticeClass::Attention)
            }
            OutcomeKind::Completed => completed_class(caused_by),
        },
        NoticeEvent::AgentTurnCompleted => completed_class(caused_by),
    }
}
fn completed_class(causes: &BTreeSet<EntryKind>) -> Option<NoticeClass> {
    let mut class = None;
    for cause in causes {
        match cause {
            EntryKind::UserMessage => {}
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
        let Entry::Notice {
            child,
            event,
            reference,
        } = entry
        else {
            return self.corrupted("K1 event append must be a notice".into());
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
    // K3 approval requests notify here; K2 BOUNDARY_B halts notify the
    // recurrence conversation's parent here, without appending an iteration.
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
        let causes = self.step(|tx| {
            let mut causes = BTreeSet::new();
            for entry in tx
                .open_table(TURN_INPUTS)?
                .range((reference.get(), 0)..=(reference.get(), u32::MAX))?
            {
                causes.insert(TurnEntry::decode(entry?.1.value())?.entry.kind());
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
        let event = if matches!(
            outcome.cause,
            Some(crate::persistence::OutcomeCause::Stopped { .. })
        ) {
            NoticeEvent::StoppedAbove
        } else if outcome.kind == OutcomeKind::Completed && row.previous.is_none() {
            NoticeEvent::ObjectiveCompleted
        } else {
            NoticeEvent::TurnEnded { kind: outcome.kind }
        };
        self.notify(conversation, event, turn)
    }
}
