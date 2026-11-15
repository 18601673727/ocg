use super::codec::TAG_APPROVAL;
use super::*;
use crate::db::entities::{MessageBody, ToolCallRef, ToolDecision, Violation};

pub(super) fn open_key(conversation: JobId, call: ToolCallRef) -> (u128, u128, u32, u32) {
    (
        conversation.get(),
        call.job.get(),
        call.request.get(),
        call.index,
    )
}

pub(super) fn command_assessment_identity(
    call: ToolCallRef,
) -> Result<MessageBody, serde_json::Error> {
    MessageBody::new(&serde_json::json!({"command_assessment": {
        "job": call.job.to_string(), "request": call.request.get(), "index": call.index
    }}))
}

fn continuation_text(tool: &str, decision: &ApprovalDecision) -> String {
    match decision {
        ApprovalDecision::Answered { text } => text.clone(),
        ApprovalDecision::Approved => {
            format!("I approve the {tool} request. Please continue.")
        }
        ApprovalDecision::Rejected if tool == "ask_user" => {
            "I would rather not answer that question. Please continue without it.".to_owned()
        }
        ApprovalDecision::Rejected => {
            format!("I decline the {tool} request. Please continue without it.")
        }
        ApprovalDecision::Voided => String::new(),
    }
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

    pub(super) fn decode(bytes: &[u8]) -> DecodeResult<Self> {
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
                matches!(
                    decision,
                    ApprovalDecision::Answered { .. } | ApprovalDecision::Rejected
                )
            }
            ToolDecision::Admitted(_) | ToolDecision::Refused(_) => {
                return self.reject(Violation::InvalidValue(
                    "approval_not_found",
                    "no awaiting call",
                ));
            }
        };
        let Some(conversation) = self.turn_conversation(approval.job)? else {
            return self.reject(Violation::InvalidValue(
                "approval_not_found",
                "no conversation",
            ));
        };
        if self.require_job_state(approval.job)?.outcome.is_none() {
            return self.reject(Violation::InvalidValue(
                "approval_not_ready",
                "the Job that asked has not ended",
            ));
        }
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
        if self.job_lifecycle(conversation)?.closing.is_some() {
            return self.reject(Violation::InvalidValue(
                "conversation_closing",
                "decisions cannot continue a closing conversation",
            ));
        }
        if decision == ApprovalDecision::Approved {
            self.grant_project_command(&call, conversation)?;
        }
        let text = match self.perform_decided_job_call(approval, conversation, &call, &decision)? {
            Some(text) => text,
            None => continuation_text(&call.name, &decision),
        };
        self.put_approval(approval, decision, Actor::User)?;
        self.step(|tx| {
            tx.open_table(OPEN_APPROVALS)?.remove(key)?;
            Ok(())
        })?;
        self.continue_after_decision(approval, text)
    }

    fn continue_after_decision(
        &mut self,
        approval: ToolCallRef,
        text: String,
    ) -> Result<Appended, BatchError> {
        use crate::db::entities::{
            ComposerContextId, MutableJobContent, MutableJobInput, RequestFingerprint, Submission,
            SubmissionKey,
        };
        let Some(asked) = self.mutable_job(approval.job)? else {
            return self.reject(Violation::InvalidValue(
                "approval_not_found",
                "the Job that asked is not a native Job",
            ));
        };
        let MutableJobContent::Generative { configuration, .. } = &asked.input.content else {
            return self.reject(Violation::InvalidValue(
                "approval_not_found",
                "the Job that asked is not a Generative Job",
            ));
        };
        let input = MutableJobInput {
            priority: asked.input.priority,
            content: MutableJobContent::Generative {
                text,
                attachments: Vec::new(),
                configuration: *configuration,
            },
        };
        let submission = |step: &[u8]| {
            let mut identity = b"ocg approval continuation ".to_vec();
            identity.extend_from_slice(step);
            identity.extend_from_slice(&approval.job.get().to_le_bytes());
            identity.extend_from_slice(&approval.request.get().to_le_bytes());
            identity.extend_from_slice(&approval.index.to_le_bytes());
            let digest = *crate::db::entities::ContentDigest::of(&identity).as_bytes();
            Submission {
                key: SubmissionKey::from_digest(digest),
                fingerprint: RequestFingerprint::from_digest(digest),
            }
        };
        let context = ComposerContextId::new(self.next_id()?);
        let id = self.create_unsubmitted_job(
            context,
            input,
            submission(b"create"),
            Some(asked.parent_id),
            None,
        )?;
        let created = self.submit_mutable_job(id, 1, submission(b"submit"))?;
        Ok(Appended {
            turn: created.id,
            seq: 1,
            joined: false,
        })
    }
    fn command_project(
        &mut self,
        conversation: JobId,
    ) -> Result<Option<crate::db::entities::ProjectRecord>, BatchError> {
        let row = self.require_mutable_conversation(conversation)?;
        match row.project {
            Some(project) => self.require_project_record(project).map(Some),
            None => Ok(None),
        }
    }

    fn command_input(
        &mut self,
        call: &crate::db::entities::ToolEvidenceRecord,
        project: &crate::db::entities::ProjectRecord,
    ) -> Result<Option<crate::db::entities::CommandGrantInput>, BatchError> {
        use crate::db::entities::*;
        let Some(admission) = call.decision.fact.stored_admission() else {
            return Ok(None);
        };
        let Ok(arguments) = serde_json::from_str::<serde_json::Value>(admission.arguments.as_str())
        else {
            return Ok(None);
        };
        if call.name == "workspace_request_command" {
            let Ok(input) = serde_json::from_value::<CommandGrantInput>(
                serde_json::json!({ "command": { "name": arguments.get("name"), "argv": arguments.get("argv") }, "reason": arguments.get("reason") }),
            ) else {
                return Ok(None);
            };
            if validate_project_commands(std::slice::from_ref(&input.command)).is_err()
                || input.reason.len() > 4096
            {
                return Ok(None);
            }
            return Ok(Some(input));
        }
        if call.name != "workspace_run_command" {
            return Ok(None);
        }
        let Some(name) = arguments.get("command").and_then(serde_json::Value::as_str) else {
            return Ok(None);
        };
        let command = project
            .commands
            .iter()
            .find(|command| command.name == name && command.status == CommandStatus::Allowed)
            .cloned()
            .or_else(|| match name {
                "cargo_check" => Some(ProjectCommand {
                    name: "cargo_check".into(),
                    argv: vec!["cargo".into(), "check".into(), "--all-targets".into()],
                    ..Default::default()
                }),
                "cargo_test" => Some(ProjectCommand {
                    name: "cargo_test".into(),
                    argv: vec!["cargo".into(), "test".into(), "--all-targets".into()],
                    ..Default::default()
                }),
                _ => None,
            });
        let Some(mut command) = command else {
            return Ok(None);
        };
        if let Some(extra) = arguments
            .get("arguments")
            .and_then(serde_json::Value::as_array)
        {
            for argument in extra {
                let Some(argument) = argument.as_str() else {
                    return Ok(None);
                };
                command.argv.push(argument.to_owned());
            }
        }
        command.owner = CommandOwner::User;
        command.status = CommandStatus::Allowed;
        command.proposal = None;
        Ok(Some(CommandGrantInput {
            command,
            reason: "The generative Agent requested approval to run this exact configured command."
                .into(),
        }))
    }

    fn grant_project_command(
        &mut self,
        call: &crate::db::entities::ToolEvidenceRecord,
        conversation: JobId,
    ) -> Result<(), BatchError> {
        use crate::db::entities::*;
        let Some(mut project) = self.command_project(conversation)? else {
            return Ok(());
        };
        let Some(input) = self.command_input(call, &project)? else {
            return Ok(());
        };
        if call.name == "workspace_request_command" {
            let mut command = input.command;
            command.owner = CommandOwner::User;
            command.proposal = None;
            if let Some(index) = project
                .commands
                .iter()
                .position(|prior| prior.name == command.name)
            {
                project.commands[index] = command;
            } else {
                project.commands.push(command);
            }
        } else if let Some(command) = project
            .commands
            .iter_mut()
            .find(|command| command.name == input.command.name)
        {
            command.owner = CommandOwner::User;
        } else {
            return Ok(());
        }
        if let Err(reason) = validate_project_commands(&project.commands) {
            return self.reject(Violation::InvalidValue(
                match reason {
                    "a Project has at most 32 active commands" => "command_list_full",
                    "the commands take at most 16 KiB" => "command_list_bytes",
                    _ => "commands",
                },
                reason,
            ));
        }
        project.updated_at = self.now;
        self.insert_project(&project)
    }

    pub(crate) fn void_approvals(
        &mut self,
        conversation: JobId,
        by: Actor,
    ) -> Result<(), BatchError> {
        let calls = self.step(|tx| {
            let mut calls = Vec::new();
            for entry in tx.open_table(OPEN_APPROVALS)?.range(
                (conversation.get(), 0, 0, 0)..=(conversation.get(), u128::MAX, u32::MAX, u32::MAX),
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
            self.remove(OPEN_APPROVALS, open_key(conversation, call))?;
        }
        Ok(())
    }
}
