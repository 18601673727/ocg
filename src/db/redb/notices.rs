use super::*;
use crate::db::entities::JobClosing;

impl Batch {
    /// A system conversation holds one turn and closes when that turn
    /// ends. Native conversations close through their own rules.
    pub(crate) fn turn_ended(&mut self, turn: JobId) -> Result<(), BatchError> {
        let row = self.require_job(turn)?;
        if !row.kind.is_persistent() {
            return Ok(());
        }
        let Some(conversation) = row.parent_id else {
            return Ok(());
        };
        self.close_system_conversation(conversation)
    }
    pub(crate) fn close_system_conversation(
        &mut self,
        conversation: JobId,
    ) -> Result<(), BatchError> {
        let row = self.require_job(conversation)?;
        if !matches!(
            row.kind,
            JobKind::Group {
                origin: Origin::System,
                keep_open: false,
                ..
            }
        ) {
            return Ok(());
        }
        let mut control = self.job_control(conversation)?;
        if control.closing.is_some() {
            return Ok(());
        }
        let Some(tip) = self.group_head(conversation)? else {
            return Ok(());
        };
        if self.require_job_state(tip)?.outcome.is_none()
            || !self.live_children(conversation)?.is_empty()
            || self.has_open_job_approvals(conversation)?
        {
            return Ok(());
        }
        control.closing = Some(JobClosing {
            at: self.now(),
            by: Actor::Auto,
            running: false,
        });
        self.put_job_control(conversation, &control)?;
        self.settle(conversation)
    }
}
