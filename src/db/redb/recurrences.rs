use super::codec::TAG_RECURRENCE;
use super::*;

impl Recurrence {
    pub(super) fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_RECURRENCE)?;
        let phase = match d.u8()? {
            0 => RecurrencePhase::Active,
            1 => RecurrencePhase::WaitingDue { due: d.i64()? },
            2 => RecurrencePhase::Halted {
                reason: d.code_with(RecurrenceHalt::from_code, |_| {
                    StorageError::corrupted("invalid recurrence halt")
                })?,
            },
            _ => return Err(StorageError::corrupted("invalid recurrence phase")),
        };
        let row = Self {
            phase,
            n: d.u64()?,
            current_turn: JobId::new(d.u128()?),
            previous_turn: d.ou128()?.map(JobId::new),
            interval_s: d.u32()?,
        };
        d.end()?;
        Ok(row)
    }

    pub(super) fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_RECURRENCE);
        match self.phase {
            RecurrencePhase::Active => {
                e.u8(0);
            }
            RecurrencePhase::WaitingDue { due } => {
                e.u8(1);
                e.i64(due);
            }
            RecurrencePhase::Halted { reason } => {
                e.u8(2);
                e.u8(reason.code());
            }
        }
        e.u64(self.n);
        e.u128(self.current_turn.get());
        e.ou128(self.previous_turn.map(JobId::get));
        e.u32(self.interval_s);
        e.0
    }
}
