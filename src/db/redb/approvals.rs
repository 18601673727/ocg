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
        self.decide_with_assessment(approval, decision, None)
    }
    fn decide_with_assessment(
        &mut self,
        approval: ToolCallRef,
        decision: ApprovalDecision,
        assessment: Option<JobId>,
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
        let turn = self.require_job(approval.job)?;
        let Some(conversation) = turn.parent_id.filter(|_| turn.kind.is_persistent()) else {
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
        let selected_dispatch =
            call.name == "job_dispatch" && matches!(decision, ApprovalDecision::Answered { .. });
        if let ApprovalDecision::Answered { text } = &decision
            && selected_dispatch
        {
            let selection = serde_json::from_str::<crate::db::entities::DispatchSelection>(text)
                .map_err(|_| {
                    BatchError(Failure::Rejected(Violation::InvalidValue(
                        "dispatch_selection",
                        "select dispatch targets",
                    )))
                })?;
            self.validate_dispatch_selection(&selection)?;
            let (limits, depth) = self.group_limits(conversation)?;
            if depth >= limits.max_depth
                || self
                    .live_children(conversation)?
                    .len()
                    .saturating_add(selection.projects.len().max(1))
                    > 16
            {
                return self.reject(Violation::InvalidValue(
                    "dispatch_selection",
                    "the selected targets exceed this conversation's remaining child capacity",
                ));
            }
        }
        let approved = decision == ApprovalDecision::Approved || selected_dispatch;
        if approved && !selected_dispatch {
            self.grant_project_command(&call, assessment)?;
        }
        self.put_approval(
            approval,
            decision,
            if assessment.is_some() {
                Actor::Auto
            } else {
                Actor::User
            },
        )?;
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
        if let Some(mut recurrence) = self.recurrence(conversation)?
            && recurrence.phase == RecurrencePhase::Active
            && recurrence.current_turn == approval.job
        {
            recurrence.current_turn = appended.turn;
            self.put_recurrence(conversation, &recurrence, None)?;
        }
        self.auto_close_check(conversation)?;
        Ok(appended)
    }
    fn command_project(
        &mut self,
        job: JobId,
    ) -> Result<Option<crate::db::entities::ProjectRecord>, BatchError> {
        let mut current = Some(job);
        while let Some(job) = current {
            let row = self.require_job(job)?;
            let project = match row.kind {
                JobKind::Group { project, .. } => project,
                JobKind::Execution {
                    plan_source: PlanSource::Process { project, .. },
                    ..
                } => project,
                JobKind::Execution {
                    plan_source: PlanSource::Stored { .. },
                    ..
                } => None,
            };
            if let Some(project) = project {
                return self.require_project_record(project).map(Some);
            }
            current = row.parent_id;
        }
        Ok(None)
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
        assessment: Option<JobId>,
    ) -> Result<(), BatchError> {
        use crate::db::entities::*;
        let Some(mut project) = self.command_project(call.call.job)? else {
            return Ok(());
        };
        let Some(input) = self.command_input(call, &project)? else {
            return Ok(());
        };
        if call.name == "workspace_request_command" {
            let mut command = input.command;
            command.owner = if assessment.is_some() {
                CommandOwner::Decision
            } else {
                CommandOwner::User
            };
            command.proposal = assessment;
            if let Some(index) = project
                .commands
                .iter()
                .position(|prior| prior.name == command.name)
            {
                if assessment.is_some() && project.commands[index].owner == CommandOwner::User {
                    return Ok(());
                }
                command.proposal = command.proposal.or(project.commands[index].proposal);
                project.commands[index] = command;
            } else {
                project.commands.push(command);
            }
        } else if assessment.is_none()
            && let Some(command) = project
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

    pub(crate) fn assess_command_grant(
        &mut self,
        call: ToolCallRef,
    ) -> Result<Option<JobId>, BatchError> {
        use crate::db::entities::*;
        if self
            .require_job_state(call.job)?
            .outcome
            .is_none_or(|outcome| outcome.kind != crate::db::entities::OutcomeKind::Completed)
        {
            return Ok(None);
        }
        let Some(evidence) = self.tool_evidence(call)? else {
            return Ok(None);
        };
        if !matches!(evidence.decision.fact, ToolDecision::AwaitingApproval(_)) {
            return Ok(None);
        }
        let Some(project) = self.command_project(call.job)? else {
            return Ok(None);
        };
        let policy = project.decision_policy();
        if !policy.enabled {
            return Ok(None);
        }
        let Some(input) = self.command_input(&evidence, &project)? else {
            return Ok(None);
        };
        let identity = command_assessment_identity(call);
        let Ok(identity) = identity else {
            return Ok(None);
        };
        let key = SubmissionKey::from_digest(*identity.digest().as_bytes());
        if let Some(previous) = self.submission(key)? {
            return Ok(Some(previous.job));
        }
        let active_job = self.require_job(call.job)?;
        let active = self.job_execution(&active_job)?.0;
        let Some((frozen, assignment)) = self.project_decision(Some(project.id), &active)? else {
            return Ok(None);
        };
        let (_, appended) = self.open_resolved_decision(
            None,
            JobGroup {
                name: Some("Command grant assessment".into()),
                agent: frozen.agent_ref().map(|reference| reference.id),
                project: Some(project.id),
                origin: Origin::System,
                keep_open: false,
            },
            Entry::UserMessage {
                text: "Assess the pending command grant.".into(),
                attachments: vec![],
            },
            frozen,
            input.request(),
            Some(Submission {
                key,
                fingerprint: RequestFingerprint::from_digest(*identity.digest().as_bytes()),
            }),
            None,
            Some(CommandAssessment {
                approval: call,
                minimum_confidence_millis: policy.minimum_confidence_millis,
            }),
            assignment,
        )?;
        if evidence.name == "workspace_request_command"
            && !project.commands.iter().any(|command| {
                command.name == input.command.name
                    || (command.owner == CommandOwner::User && command.argv == input.command.argv)
            })
            && project
                .commands
                .iter()
                .filter(|command| command.status != CommandStatus::Blocked)
                .count()
                < 32
        {
            let mut project = project;
            let mut command = input.command;
            command.owner = CommandOwner::Decision;
            command.status = CommandStatus::Review;
            command.proposal = Some(appended.turn);
            project.commands.push(command);
            if validate_project_commands(&project.commands).is_ok() {
                project.updated_at = self.now;
                self.insert_project(&project)?;
            }
        }
        Ok(Some(appended.turn))
    }

    pub(crate) fn apply_command_assessment(
        &mut self,
        job: JobId,
        work: &crate::db::entities::DecisionWork,
        result: Option<&FinalResult>,
    ) -> Result<(), BatchError> {
        use crate::db::entities::*;
        let Some(binding) = &work.commands else {
            return Ok(());
        };
        let Some(assignment) = &work.assignment else {
            return Ok(());
        };
        let project = self.require_project_record(assignment.project)?;
        if !self.decision_assignment_current(work)?
            || project.decision_policy().minimum_confidence_millis
                != binding.minimum_confidence_millis
        {
            return Ok(());
        }
        let Some(evidence) = self.tool_evidence(binding.approval)? else {
            return Ok(());
        };
        let Some(conversation) = self.require_job(binding.approval.job)?.parent_id else {
            return Ok(());
        };
        if !self.step(|tx| {
            Ok(tx
                .open_table(OPEN_APPROVALS)?
                .get(open_key(conversation, binding.approval))?
                .is_some_and(|value| value.value() == 0))
        })? {
            return Ok(());
        }
        if self.job_control(conversation)?.closing.is_some() {
            return Ok(());
        }
        let Some(input) = self.command_input(&evidence, &project)? else {
            return Ok(());
        };
        if input.request() != work.request {
            return Ok(());
        }
        // Explicit user blocks and conflicting user entries cannot be replaced by advice.
        if project.commands.iter().any(|command| {
            command.owner == CommandOwner::User
                && (command.name == input.command.name || command.argv == input.command.argv)
                && (command.status != CommandStatus::Allowed
                    || (evidence.name == "workspace_request_command"
                        && command.argv != input.command.argv))
        }) {
            return Ok(());
        }
        let Some(value) = result
            .and_then(|result| result.value.as_ref())
            .and_then(|value| value.get("decision"))
        else {
            return Ok(());
        };
        let Ok(result) = serde_json::from_value::<DecisionResult>(value.clone()) else {
            return Ok(());
        };
        if result
            .validate(
                &work.request,
                &work.frozen,
                RequestRef {
                    job,
                    request: RequestNumber::FIRST,
                },
            )
            .is_err()
        {
            return Ok(());
        }
        let DecisionOutcome::Answer {
            answer:
                DecisionAnswer::Choice {
                    selected,
                    confidence: Some(confidence),
                    ..
                },
        } = result.outcome
        else {
            return Ok(());
        };
        if !matches!(selected.as_str(), "approve" | "reject")
            || confidence.semantics != work.frozen.confidence_semantics()
            || confidence.value < f64::from(binding.minimum_confidence_millis) / 1000.0
        {
            return Ok(());
        }
        if selected == "approve" && evidence.name == "workspace_request_command" {
            let mut commands = project.commands.clone();
            let mut command = input.command;
            command.owner = CommandOwner::Decision;
            command.proposal = Some(job);
            if let Some(prior) = commands.iter_mut().find(|prior| prior.name == command.name) {
                if prior.owner != CommandOwner::User {
                    *prior = command;
                }
            } else {
                commands.push(command);
            }
            if validate_project_commands(&commands).is_err() {
                return Ok(());
            }
        }
        if selected == "reject" {
            let mut project = project;
            for command in &mut project.commands {
                if command.owner == CommandOwner::Decision && command.proposal == Some(job) {
                    command.status = CommandStatus::Blocked;
                }
            }
            project.updated_at = self.now;
            self.insert_project(&project)?;
        }
        self.decide_with_assessment(
            binding.approval,
            if selected == "approve" {
                ApprovalDecision::Approved
            } else {
                ApprovalDecision::Rejected
            },
            Some(job),
        )?;
        Ok(())
    }

    pub(crate) fn resume_user_command_grants(
        &mut self,
        project: &crate::db::entities::ProjectRecord,
    ) -> Result<(), BatchError> {
        use crate::db::entities::*;
        for command in &project.commands {
            if command.owner != CommandOwner::User || command.status != CommandStatus::Allowed {
                continue;
            }
            let Some(proposal) = command.proposal else {
                continue;
            };
            let row = self.require_job(proposal)?;
            let JobKind::Execution {
                execution: JobExecution::Decision(work),
                ..
            } = row.kind
            else {
                continue;
            };
            let Some(binding) = work.commands else {
                continue;
            };
            let Some(evidence) = self.tool_evidence(binding.approval)? else {
                continue;
            };
            let Some(input) = self.command_input(&evidence, project)? else {
                continue;
            };
            if input.command.name != command.name || input.command.argv != command.argv {
                continue;
            }
            let Some(conversation) = self.require_job(binding.approval.job)?.parent_id else {
                continue;
            };
            let pending = self.step(|tx| {
                Ok(tx
                    .open_table(OPEN_APPROVALS)?
                    .get(open_key(conversation, binding.approval))?
                    .is_some_and(|value| value.value() == 0))
            })?;
            if pending && self.job_control(conversation)?.closing.is_none() {
                self.decide(binding.approval, ApprovalDecision::Approved)?;
            }
        }
        Ok(())
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
            self.step(|tx| {
                tx.open_table(OPEN_APPROVALS)?
                    .remove(open_key(conversation, call))?;
                Ok(())
            })?;
        }
        Ok(())
    }
}
