use super::codec::TAG_APPROVAL;
use super::*;
use crate::persistence::{ToolCallRef, ToolDecision, Violation};

pub(super) fn open_key(conversation: JobId, call: ToolCallRef) -> (u64, u64, u32, u32) {
    (
        conversation.get(),
        call.job.get(),
        call.request.get(),
        call.index,
    )
}

impl ApprovalRecord {
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_APPROVAL);
        match &self.decision {
            ApprovalDecision::Approved => e.u8(1),
            ApprovalDecision::Rejected => e.u8(2),
            ApprovalDecision::Answered { text } => {
                e.u8(3);
                e.str(text);
            }
            ApprovalDecision::Voided => e.u8(4),
        }
        e.actor(&self.by);
        e.ts(self.at);
        e.0
    }
    pub(super) fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let mut d = Dec::new(bytes, TAG_APPROVAL)?;
        let decision = match d.u8()? {
            1 => ApprovalDecision::Approved,
            2 => ApprovalDecision::Rejected,
            3 => ApprovalDecision::Answered { text: d.str()? },
            4 => ApprovalDecision::Voided,
            _ => return Err(StorageError::corrupted("invalid approval decision")),
        };
        let row = Self {
            decision,
            by: d.actor()?,
            at: d.ts()?,
        };
        d.end()?;
        Ok(row)
    }
}

impl Batch {
    pub(crate) fn put_approval(
        &mut self,
        call: ToolCallRef,
        decision: ApprovalDecision,
        by: Actor,
    ) -> Result<(), BatchError> {
        let row = ApprovalRecord {
            decision,
            by,
            at: self.now(),
        };
        self.step(|tx| {
            let mut table = tx.open_table(APPROVALS)?;
            if table.get(tool_call_key(call))?.is_some() {
                return Err(StorageError::corrupted("approval written twice"));
            }
            table.insert(tool_call_key(call), row.encode().as_slice())?;
            Ok(())
        })
    }
    pub(crate) fn decide(
        &mut self,
        approval: ToolCallRef,
        decision: ApprovalDecision,
    ) -> Result<Appended, BatchError> {
        let Some(call) = self.tool_evidence(approval)? else {
            return self.reject(Violation::InvalidValue(
                "approval_not_found",
                "no awaiting call",
            ));
        };
        let valid = match &call.decision.fact {
            ToolDecision::AwaitingApproval(_) => matches!(
                decision,
                ApprovalDecision::Approved | ApprovalDecision::Rejected
            ),
            ToolDecision::AwaitingAnswer(_) => {
                matches!(decision, ApprovalDecision::Answered { .. })
            }
            ToolDecision::Admitted(_) | ToolDecision::Refused(_) => {
                return self.reject(Violation::InvalidValue(
                    "approval_not_found",
                    "no awaiting call",
                ));
            }
        };
        let turn = self.require_job(approval.job)?;
        let Some(conversation) = turn.parent_id.filter(|_| turn.kind.is_stored()) else {
            return self.reject(Violation::InvalidValue(
                "approval_not_found",
                "no conversation",
            ));
        };
        let key = open_key(conversation, approval);
        if !self.step(|tx| {
            Ok(tx
                .open_table(OPEN_APPROVALS)?
                .get(key)?
                .is_some_and(|v| v.value() == 0))
        })? {
            return self.reject(Violation::InvalidValue(
                "approval_decided",
                "approval already decided",
            ));
        }
        if !valid {
            return self.reject(Violation::InvalidValue(
                "invalid_body",
                "decision does not match the awaiting call",
            ));
        }
        let approved = decision == ApprovalDecision::Approved;
        self.put_approval(approval, decision, Actor::User)?;
        self.step(|tx| {
            let mut open = tx.open_table(OPEN_APPROVALS)?;
            if approved {
                open.insert(key, 1)?;
            } else {
                open.remove(key)?;
            }
            Ok(())
        })?;
        let appended = self.append(
            conversation,
            Entry::Decision {
                approval: (approval.job, approval.request.get(), approval.index),
            },
            None,
            None,
            true,
        )?;
        self.auto_close_check(conversation)?;
        Ok(appended)
    }
    pub(crate) fn void_approvals(
        &mut self,
        conversation: JobId,
        by: Actor,
    ) -> Result<(), BatchError> {
        let calls = self.step(|tx| {
            let mut calls = Vec::new();
            for entry in tx.open_table(OPEN_APPROVALS)?.range(
                (conversation.get(), 0, 0, 0)..=(conversation.get(), u64::MAX, u32::MAX, u32::MAX),
            )? {
                let (key, value) = entry?;
                if value.value() == 0 {
                    let (_, turn, request, index) = key.value();
                    calls.push(tool_call_ref((turn, request, index))?);
                }
            }
            Ok(calls)
        })?;
        for call in calls {
            self.put_approval(call, ApprovalDecision::Voided, by.clone())?;
            self.step(|tx| {
                tx.open_table(OPEN_APPROVALS)?
                    .remove(open_key(conversation, call))?;
                Ok(())
            })?;
        }
        Ok(())
    }
}
