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
}
