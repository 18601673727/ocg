use super::codec::TAG_APPROVAL;
use super::{StorageError, approvals};
use crate::db::entities::{
    Actor, Appended, ApprovalDecision, ApprovalRecord, JobId, Origin, OutcomeKind, ToolCallRefusal,
    ToolEffect, ToolEvidenceRecord,
};
use crate::db::entities::{
    ExecutionEventKind, MessageBody, Recorded, ToolCallRef, ToolDecision, ToolOutcome, Violation,
};
use crate::db::entities::{RequestFingerprint, Submission, SubmissionKey};
use crate::db::redb::batch::{Batch, BatchError, Failure, Probe};
use crate::db::redb::codec::{Dec, DecodeResult, Enc};
use crate::db::redb::reads::jobs::{read_conversation_row, read_state};
use crate::db::redb::reads::read;
use crate::db::redb::rows::{
    decode_output, decode_project, decode_submission, decode_tool_evidence, encode_tool_evidence,
    tool_call_key, tool_call_ref,
};
use crate::db::redb::schema::{
    APPROVALS, JOB_OUTPUTS, LIVE_CHILDREN, OPEN_APPROVALS, PROJECTS, SUBMISSIONS, TOOL_CALL_IDS,
    TOOL_CALLS, TOOL_STATS,
};
use ::redb::{Database, ReadTransaction, ReadableTable};
use std::collections::BTreeSet;

pub(super) fn continuation_submission(approval: ToolCallRef, step: &[u8]) -> Submission {
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
}

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
    MessageBody::new_exact(&serde_json::json!({"command_assessment": {
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
        let conversation = self.turn_conversation(approval.job)?;
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
        let approved_command = if decision == ApprovalDecision::Approved {
            self.grant_project_command(&call, conversation)?
        } else {
            None
        };
        let text = match self.perform_decided_job_call(approval, conversation, &call, &decision)? {
            Some(text) => text,
            None => match approved_command {
                Some(name) => format!(
                    "I approved the Project command `{name}`. Call `workspace_run_command` with command `{name}` and no extra arguments to run the approved argv, then continue with the actual result."
                ),
                None => continuation_text(&call.name, &decision),
            },
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
        use crate::db::entities::{MutableJobContent, MutableJobInput};
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
        // Answering a card continues the tool call. Only a message typed into the child
        // should keep that conversation open after the continuation finishes.
        let turn = self.host_turn(
            asked.parent_id,
            input,
            continuation_submission(approval, b"create"),
            continuation_submission(approval, b"submit"),
            Actor::Auto,
        )?;
        Ok(Appended {
            turn,
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
        use crate::db::entities::{
            CommandGrantInput, CommandOwner, CommandStatus, ProjectCommand,
            validate_project_commands,
        };
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
    ) -> Result<Option<String>, BatchError> {
        use crate::db::entities::{CommandOwner, Violation, validate_project_commands};
        let Some(mut project) = self.command_project(conversation)? else {
            return Ok(None);
        };
        let Some(input) = self.command_input(call, &project)? else {
            return Ok(None);
        };
        let registered = if call.name == "workspace_request_command" {
            let mut command = input.command;
            command.owner = CommandOwner::User;
            command.proposal = None;
            let name = command.name.clone();
            if let Some(index) = project
                .commands
                .iter()
                .position(|prior| prior.name == command.name)
            {
                project.commands[index] = command;
            } else {
                project.commands.push(command);
            }
            Some(name)
        } else if let Some(command) = project
            .commands
            .iter_mut()
            .find(|command| command.name == input.command.name)
        {
            command.owner = CommandOwner::User;
            None
        } else {
            return Ok(None);
        };
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
        self.insert_project(&project)?;
        Ok(registered)
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
                if value.value() != 0 {
                    return Err(StorageError::corrupted("invalid open approval index entry"));
                }
                let (_, turn, request, index) = key.value();
                calls.push(tool_call_ref((turn, request, index))?);
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

impl Batch {
    pub(crate) fn tool_decide(
        &mut self,
        proof: &crate::db::handoff::DevelopmentWriteProof,
        evidence: crate::db::entities::ToolEvidence,
    ) -> Result<(), BatchError> {
        let crate::db::entities::ToolEvidence {
            call,
            tool_call_id,
            name,
            decision,
            spawns,
        } = evidence;
        let (tool_call_id, name) = (tool_call_id.as_str(), name.as_str());
        self.require_development_call(proof, call)?;
        if matches!(
            &decision,
            ToolDecision::Admitted(admitted) if admitted.effect != ToolEffect::ReadOnly
        ) && name != "job_plan"
        {
            self.require_planning_decision_satisfied(call)?;
        }
        let conversation = self.turn_conversation(call.job)?;
        if !self.request_observed(call.job, call.request)? {
            return self.reject(Violation::RequestNotObserved {
                job: call.job,
                request: call.request,
            });
        }
        let key = tool_call_key(call);
        if self.tool_evidence(call)?.is_some() {
            return self.reject(Violation::DuplicateToolCall(call));
        }
        let existing = self.step(|tx| {
            Ok(tx
                .open_table(TOOL_CALL_IDS)?
                .get((call.job.get(), tool_call_id))?
                .map(|found| found.value()))
        })?;
        if let Some((request, index)) = existing {
            let existing = tool_call_ref((call.job.get(), request, index))
                .map_err(|error| BatchError(Failure::Storage(error)))?;
            return self.reject(Violation::DuplicateToolCallId { call, existing });
        }
        let current_stats = self.step(|tx| {
            Ok(tx
                .open_table(TOOL_STATS)?
                .get(name)?
                .map(|found| found.value()))
        })?;
        let mut stats_changed = false;
        let mut updated_stats = current_stats.unwrap_or((0, 0, 0, 0, 0, 0));
        match &decision {
            ToolDecision::Admitted(_) => {
                updated_stats.0 = updated_stats.0.saturating_add(1);
                stats_changed = true;
            }
            ToolDecision::Refused(ToolCallRefusal::InvalidArguments) => {
                updated_stats.1 = updated_stats.1.saturating_add(1);
                stats_changed = true;
            }
            ToolDecision::AwaitingApproval(_) | ToolDecision::AwaitingAnswer(_) => {}
            ToolDecision::Refused(
                ToolCallRefusal::Malformed
                | ToolCallRefusal::ConflictingIdentity
                | ToolCallRefusal::AmbiguousIdentity
                | ToolCallRefusal::LimitExceeded
                | ToolCallRefusal::Incomplete
                | ToolCallRefusal::Unauthorized
                | ToolCallRefusal::MissingSchema
                | ToolCallRefusal::UnsupportedSchema
                | ToolCallRefusal::NoDecider,
            ) => {}
        }
        let record = ToolEvidenceRecord {
            call,
            tool_call_id: tool_call_id.to_owned(),
            name: name.to_owned(),
            spawns: spawns && matches!(decision, ToolDecision::Admitted(_)),
            decision: Recorded {
                fact: decision,
                at: self.now,
            },
            outcome: None,
            reconciliation: None,
        };
        self.step(|tx| {
            tx.open_table(TOOL_CALLS)?
                .insert(key, encode_tool_evidence(&record).as_slice())?;
            tx.open_table(TOOL_CALL_IDS)?.insert(
                (call.job.get(), tool_call_id),
                (call.request.get(), call.index),
            )?;
            if stats_changed || current_stats.is_some() {
                tx.open_table(TOOL_STATS)?.insert(name, updated_stats)?;
            }
            Ok(())
        })?;
        self.record_execution_event(
            call.job,
            ExecutionEventKind::ToolRecorded {
                call,
                completed: false,
            },
        )?;
        if matches!(
            record.decision.fact,
            ToolDecision::AwaitingApproval(_) | ToolDecision::AwaitingAnswer(_)
        ) {
            if let Some(closing) = self.job_lifecycle(conversation)?.closing {
                self.put_approval(call, ApprovalDecision::Voided, closing.by)?;
            } else {
                self.insert(OPEN_APPROVALS, approvals::open_key(conversation, call), 0)?;
            }
        }
        self.probe = Some(Probe::ToolCall(call));
        Ok(())
    }

    pub(crate) fn tool_outcome(
        &mut self,
        proof: &crate::db::handoff::DevelopmentWriteProof,
        call: ToolCallRef,
        outcome: ToolOutcome,
    ) -> Result<(), BatchError> {
        self.require_development_call(proof, call)?;
        let Some(mut record) = self.tool_evidence(call)? else {
            return self.reject(Violation::ToolCallNotFound(call));
        };
        if record.admission().is_none() {
            return self.reject(Violation::ToolCallNotAdmitted(call));
        }
        if record.outcome.is_some() {
            return self.reject(Violation::ToolOutcomeAlreadyRecorded(call));
        }
        if matches!(outcome, ToolOutcome::CompletedElided { .. })
            && record
                .admission()
                .is_none_or(|admission| admission.effect != ToolEffect::ReadOnly)
        {
            return self.reject(Violation::InvalidValue(
                "outcome",
                "only a read-only call may keep just the size and digest of its result",
            ));
        }
        let current_stats = self.step(|tx| {
            Ok(tx
                .open_table(TOOL_STATS)?
                .get(record.name.as_str())?
                .map(|found| found.value())
                .unwrap_or((0, 0, 0, 0, 0, 0)))
        })?;
        let mut updated_stats = current_stats;
        let duration_us = self
            .now
            .as_unix_micros()
            .saturating_sub(record.decision.at.as_unix_micros())
            .max(0) as u64;
        updated_stats.4 = updated_stats.4.saturating_add(duration_us);
        updated_stats.5 = updated_stats.5.max(duration_us);
        match &outcome {
            ToolOutcome::Completed { .. }
            | ToolOutcome::CompletedWithheld { .. }
            | ToolOutcome::CompletedElided { .. } => {
                updated_stats.2 = updated_stats.2.saturating_add(1);
            }
            ToolOutcome::NotApplied { .. }
            | ToolOutcome::NotInvoked { .. }
            | ToolOutcome::Uncertain { .. } => {
                updated_stats.3 = updated_stats.3.saturating_add(1);
            }
        }
        record.outcome = Some(Recorded {
            fact: outcome,
            at: self.now,
        });
        self.put_tool_evidence(&record)?;
        self.record_execution_event(
            call.job,
            ExecutionEventKind::ToolRecorded {
                call,
                completed: true,
            },
        )?;
        self.insert(TOOL_STATS, record.name.as_str(), updated_stats)?;
        self.probe = Some(Probe::ToolOutcome(call));
        Ok(())
    }
}

fn question(arguments: &str) -> String {
    serde_json::from_str::<serde_json::Value>(arguments)
        .ok()
        .and_then(|v| {
            v.get("question")
                .and_then(|v| v.as_str())
                .map(str::to_owned)
        })
        .unwrap_or_default()
}

fn approval_assessment(
    rx: &ReadTransaction,
    call: ToolCallRef,
    project: Option<crate::db::entities::ProjectId>,
) -> Result<Option<serde_json::Value>, StorageError> {
    use crate::db::entities::{DecisionAnswer, DecisionOutcome, DecisionResult};
    let Some(project) = project else {
        return Ok(None);
    };
    let policy = rx
        .open_table(PROJECTS)?
        .get(project.get())?
        .map(|bytes| decode_project(project.get(), bytes.value()))
        .transpose()?
        .ok_or_else(|| StorageError::corrupted("approval project missing"))?
        .decision_policy();
    if !policy.enabled {
        return Ok(None);
    }
    let mut assessment = serde_json::json!({
        "state": "unavailable", "confidence": null,
        "minimum_confidence": f64::from(policy.minimum_confidence_millis) / 1000.0
    });
    let identity = approvals::command_assessment_identity(call)
        .map_err(|_| StorageError::corrupted("invalid command assessment identity"))?;
    let digest = identity.digest();
    let Some(submission) = rx
        .open_table(SUBMISSIONS)?
        .get(digest.as_bytes().as_slice())?
        .map(|bytes| decode_submission(digest.as_bytes(), bytes.value()))
        .transpose()?
    else {
        return Ok(Some(assessment));
    };
    let state = read_state(rx, submission.job)?;
    assessment["state"] = serde_json::json!(match state.outcome {
        None => "pending",
        Some(ref outcome) if outcome.kind == OutcomeKind::Completed => "completed",
        Some(_) => "failed",
    });
    if let Some((key, bytes)) = rx
        .open_table(JOB_OUTPUTS)?
        .range((submission.job.get(), 0)..=(submission.job.get(), u32::MAX))?
        .next_back()
        .transpose()?
    {
        let output = decode_output(key.value(), bytes.value())?;
        if let Some(result) = output
            .value
            .as_ref()
            .and_then(|value| value.get("decision"))
        {
            let result: DecisionResult = serde_json::from_value(result.clone())
                .map_err(|_| StorageError::corrupted("invalid command assessment result"))?;
            if let DecisionOutcome::Answer {
                answer: DecisionAnswer::Choice { confidence, .. },
            } = result.outcome
            {
                assessment["confidence"] = confidence
                    .map_or(serde_json::Value::Null, |confidence| {
                        serde_json::json!(confidence.value)
                    });
            }
        }
    }
    Ok(Some(assessment))
}

pub(crate) fn read_cards(
    database: &Database,
    root: JobId,
) -> Result<Option<Vec<serde_json::Value>>, StorageError> {
    use serde_json::json;
    let rx = read(database)?;
    let mut cards = Vec::new();
    let mut pending = vec![root];
    let mut seen = BTreeSet::new();
    while let Some(conversation) = pending.pop() {
        if !seen.insert(conversation) || seen.len() > 65 {
            return Err(StorageError::corrupted(
                "live conversation tree exceeds its bound",
            ));
        }
        let Some(row) = read_conversation_row(&rx, conversation)? else {
            if conversation == root {
                return Ok(None);
            }
            return Err(StorageError::corrupted("conversation missing"));
        };
        if conversation == root {
            super::state::require(&rx, root)?;
        }
        let (origin, project) = (row.origin, row.project);
        let parent = row.parent_id.map(|id| id.to_string());
        if origin != Origin::System {
            let waiting = super::job_tree::waiting_child_reports(&rx, conversation)?;
            if !waiting.is_empty() {
                cards.push(json!({"kind": "paused", "conversation": conversation.to_string(), "parent": parent, "waiting": waiting}));
            }
            for entry in rx.open_table(OPEN_APPROVALS)?.range(
                (conversation.get(), 0, 0, 0)..=(conversation.get(), u128::MAX, u32::MAX, u32::MAX),
            )? {
                let (key, state) = entry?;
                if state.value() != 0 {
                    return Err(StorageError::corrupted("invalid open approval index entry"));
                }
                let (_, turn, request, index) = key.value();
                let call = tool_call_ref((turn, request, index))?;
                let table = rx.open_table(TOOL_CALLS)?;
                let bytes = table
                    .get(tool_call_key(call))?
                    .ok_or_else(|| StorageError::corrupted("awaiting call missing"))?;
                let call = decode_tool_evidence(call, bytes.value())?;
                let arguments = call
                    .decision
                    .fact
                    .stored_admission()
                    .ok_or_else(|| StorageError::corrupted("awaiting admission missing"))?
                    .arguments
                    .as_str();
                let mut card = json!({"kind": if matches!(call.decision.fact, crate::db::entities::ToolDecision::AwaitingAnswer(_)) { "question" } else { "approval" }, "conversation": conversation.to_string(), "parent":parent, "approval":[JobId::new(turn).to_string(),request,index], "tool":call.name});
                if call.name == "job_dispatch" {
                    card["kind"] = json!("dispatch");
                    card["arguments"] = json!(arguments);
                } else if matches!(
                    call.decision.fact,
                    crate::db::entities::ToolDecision::AwaitingAnswer(_)
                ) {
                    card["question"] = json!(question(arguments));
                } else {
                    card["arguments"] = json!(arguments);
                    if matches!(
                        call.name.as_str(),
                        "workspace_request_command" | "workspace_run_command"
                    ) && let Some(assessment) = approval_assessment(&rx, call.call, project)?
                    {
                        card["assessment"] = assessment;
                    }
                }
                cards.push(card);
            }
        }
        for entry in rx
            .open_table(LIVE_CHILDREN)?
            .range((conversation.get(), 0)..=(conversation.get(), u128::MAX))?
        {
            pending.push(JobId::new(entry?.0.value().1));
        }
    }
    Ok(Some(cards))
}
