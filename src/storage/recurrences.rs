use super::codec::TAG_RECURRENCE;
use super::*;
use crate::persistence::{ConfigBody, ConfigKind, ExecutionConfiguration};

impl Recurrence {
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_RECURRENCE);
        match self.phase {
            RecurrencePhase::Active => e.u8(0),
            RecurrencePhase::WaitingDue { due } => {
                e.u8(1);
                e.i64(due);
            }
            RecurrencePhase::Halted { reason } => {
                e.u8(2);
                e.u8(match reason {
                    RecurrenceHalt::Closed => 0,
                    RecurrenceHalt::ConfigUnavailable => 1,
                    RecurrenceHalt::Refused => 2,
                });
            }
        }
        e.u64(self.n);
        e.u64(self.current_turn.get());
        e.ou64(self.previous_turn.map(JobId::get));
        e.u32(self.interval_s);
        e.0
    }
    pub(super) fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_RECURRENCE)?;
        let phase = match d.u8()? {
            0 => RecurrencePhase::Active,
            1 => RecurrencePhase::WaitingDue { due: d.i64()? },
            2 => RecurrencePhase::Halted {
                reason: match d.u8()? {
                    0 => RecurrenceHalt::Closed,
                    1 => RecurrenceHalt::ConfigUnavailable,
                    2 => RecurrenceHalt::Refused,
                    _ => return Err(StorageError::corrupted("invalid recurrence halt")),
                },
            },
            _ => return Err(StorageError::corrupted("invalid recurrence phase")),
        };
        let row = Self {
            phase,
            n: d.u64()?,
            current_turn: JobId::new(d.u64()?),
            previous_turn: d.ou64()?.map(JobId::new),
            interval_s: d.u32()?,
        };
        d.end()?;
        Ok(row)
    }
}
impl Batch {
    pub(crate) fn recurrence(
        &mut self,
        conversation: JobId,
    ) -> Result<Option<Recurrence>, BatchError> {
        self.step(|tx| {
            tx.open_table(RECURRENCES)?
                .get(conversation.get())?
                .map(|bytes| Recurrence::decode(bytes.value()))
                .transpose()
        })
    }
    pub(crate) fn put_recurrence(
        &mut self,
        conversation: JobId,
        row: &Recurrence,
        previous_due: Option<i64>,
    ) -> Result<(), BatchError> {
        self.step(|tx| {
            let mut due_table = tx.open_table(RECURRENCE_DUE)?;
            if let Some(due) = previous_due {
                due_table.remove((due, conversation.get()))?;
            }
            if let RecurrencePhase::WaitingDue { due } = row.phase {
                due_table.insert((due, conversation.get()), ())?;
            }
            tx.open_table(RECURRENCES)?
                .insert(conversation.get(), row.encode().as_slice())?;
            Ok(())
        })
    }
    pub(crate) fn halt_recurrence(
        &mut self,
        conversation: JobId,
        reason: RecurrenceHalt,
    ) -> Result<(), BatchError> {
        if let Some(mut row) = self.recurrence(conversation)?
            && !matches!(row.phase, RecurrencePhase::Halted { .. })
        {
            let previous_due = match row.phase {
                RecurrencePhase::WaitingDue { due } => Some(due),
                _ => None,
            };
            row.phase = RecurrencePhase::Halted { reason };
            self.put_recurrence(conversation, &row, previous_due)?;
        }
        Ok(())
    }
    pub(crate) fn boundary_b(&mut self, clock: i64) -> Result<(), BatchError> {
        let due = self.step(|tx| {
            tx.open_table(RECURRENCE_DUE)?
                .range(..=(clock, u64::MAX))?
                .map(|entry| Ok(JobId::new(entry?.0.value().1)))
                .collect::<Result<Vec<_>, StorageError>>()
        })?;
        for conversation in due {
            let Some(mut recurrence) = self.recurrence(conversation)? else {
                return self.corrupted("recurrence due without recurrence".into());
            };
            let RecurrencePhase::WaitingDue { due } = recurrence.phase else {
                return self.corrupted("recurrence due outside WaitingDue".into());
            };
            let first = self.step(|tx| {
                tx.open_table(CHILDREN)?
                    .range((conversation.get(), 0)..=(conversation.get(), u64::MAX))?
                    .next()
                    .transpose()?
                    .map(|(key, _)| JobId::new(key.value().1))
                    .ok_or_else(|| StorageError::corrupted("recurrence has no objective"))
            })?;
            let first = self.require_job(first)?;
            let (frozen, _, agent) = self.turn_execution(&first)?;
            let model = frozen.llm.model.id;
            let available = match agent {
                Some(agent) => {
                    matches!(self.require_config_kind(agent, ConfigKind::Agent)?.body, ConfigBody::Agent(body) if body.models.contains(&model))
                }
                None => false,
            };
            let reference = recurrence.previous_turn.unwrap_or(recurrence.current_turn);
            if !available {
                self.halt_recurrence(conversation, RecurrenceHalt::ConfigUnavailable)?;
                self.notify(conversation, NoticeEvent::RecurrenceHalted, reference)?;
                continue;
            }
            let n = recurrence.n.saturating_add(1);
            let appended = self.append(
                conversation,
                Entry::Iteration { n },
                Some(ExecutionConfiguration {
                    agent_profile: agent.map(Into::into),
                    llm_profile: model.into(),
                }),
                None,
                true,
            )?;
            if appended.turn == conversation {
                self.halt_recurrence(conversation, RecurrenceHalt::Refused)?;
                self.notify(conversation, NoticeEvent::RecurrenceHalted, reference)?;
            } else {
                recurrence.phase = RecurrencePhase::Active;
                recurrence.n = n;
                recurrence.current_turn = appended.turn;
                self.put_recurrence(conversation, &recurrence, Some(due))?;
            }
        }
        Ok(())
    }
}
