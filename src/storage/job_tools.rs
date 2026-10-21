use super::*;
use crate::orchestration::job_tools::JobToolCall;
use crate::persistence::{
    ConfigBody, ConfigKind, ExecutionConfiguration, ToolCallRef, ToolOutcome, ToolPayload,
    Violation,
};
use serde_json::{Value, json};

impl Batch {
    fn job_tool_id(&mut self, text: &str, prefix: &'static str) -> Result<u64, BatchError> {
        match text
            .strip_prefix(prefix)
            .filter(|text| !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()))
            .and_then(|text| text.parse::<u64>().ok())
            .filter(|id| *id != 0)
        {
            Some(id) => Ok(id),
            None => self.reject(Violation::InvalidValue(
                "invalid_target",
                "expected a stable record id",
            )),
        }
    }
    fn job_tool_rights(&mut self, caller: JobId, target: JobId) -> Result<(), BatchError> {
        let target_row = self.require_user_conversation(target)?;
        let mut parent = target_row.parent_id;
        let mut below = false;
        for _ in 0..3 {
            match parent {
                Some(id) if id == caller => {
                    below = true;
                    break;
                }
                Some(id) => {
                    self.require_user_conversation(id)?;
                    parent = self.require_job(id)?.parent_id;
                }
                None => break,
            }
        }
        if !below {
            return self.reject(Violation::InvalidValue(
                "rights",
                "the target must be in the caller's strict subtree",
            ));
        }
        for conversation in self.live_subtree(target)? {
            self.require_user_conversation(conversation)?;
            let mut turns = BTreeSet::new();
            if let Some(tip) = self.tip(conversation)? {
                let state = self.require_job_state(tip)?;
                if state.claim.is_none() && state.outcome.is_none() {
                    turns.insert(tip);
                }
            }
            if let Some(active) = self.active_turn(conversation)? {
                turns.insert(active);
            }
            for turn in turns {
                if self.step(|tx| {
                    for entry in tx
                        .open_table(TURN_INPUTS)?
                        .range((turn.get(), 0)..=(turn.get(), u32::MAX))?
                    {
                        if matches!(
                            TurnEntry::decode(entry?.1.value())?.entry,
                            Entry::UserMessage { .. }
                        ) {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                })? {
                    return self.reject(Violation::InvalidValue("user_turn", "a queued or running user turn in the target subtree prevents agent management"));
                }
            }
        }
        Ok(())
    }
    pub(crate) fn apply_job_tool(
        &mut self,
        call: ToolCallRef,
        tool: JobToolCall,
        max_result_bytes: usize,
    ) -> Result<Value, BatchError> {
        self.require_executing_here(call.job)?;
        let Some(evidence) = self.tool_evidence(call)? else {
            return self.reject(Violation::ToolCallNotFound(call));
        };
        if evidence.outcome.is_some() {
            return self.reject(Violation::ToolOutcomeAlreadyRecorded(call));
        }
        if evidence
            .admission()
            .is_none_or(|admission| admission.handler_version != format!("{}-1", tool.name()))
            || evidence.name != tool.name()
        {
            return self.reject(Violation::ToolCallNotAdmitted(call));
        }
        let caller_turn = self.require_job(call.job)?;
        if !caller_turn.kind.is_stored() {
            return self.reject(Violation::InvalidValue(
                "stored_turn_required",
                "job tools require a stored turn",
            ));
        }
        let Some(caller) = caller_turn.parent_id else {
            return self.corrupted("stored caller has no conversation".into());
        };
        let result = match tool {
            JobToolCall::JobDispatch {
                agent,
                project,
                objective,
                keep_open,
                every_seconds,
                depends_on,
            } => {
                let agent = ConfigId::new(self.job_tool_id(&agent, "profile:")?);
                let (frozen, _, _) = self.turn_execution(&caller_turn)?;
                if frozen
                    .agent
                    .as_ref()
                    .is_none_or(|body| !body.dispatch_to.contains(&agent))
                {
                    return self.reject(Violation::InvalidValue(
                        "dispatch_to",
                        "the caller's frozen Agent does not allow this dispatch target",
                    ));
                }
                let ConfigBody::Agent(body) =
                    self.require_config_kind(agent, ConfigKind::Agent)?.body
                else {
                    return self.corrupted("Agent kind mismatch".into());
                };
                let Some(model) = body.default_model else {
                    return self.reject(Violation::InvalidValue(
                        "no_default_model",
                        "the dispatched Agent has no default_model",
                    ));
                };
                if objective.trim().is_empty() {
                    return self.reject(Violation::InvalidValue("objective", "must not be empty"));
                }
                if every_seconds.is_some_and(|interval| !(60..=2_592_000).contains(&interval)) {
                    return self.reject(Violation::InvalidValue(
                        "every_seconds",
                        "must be between 60 and 2592000",
                    ));
                }
                let caller_row = self.require_job(caller)?;
                let project = match project {
                    Some(project) => Some(ProjectId::new(self.job_tool_id(&project, "project:")?)),
                    None => match caller_row.kind {
                        JobKind::Conversation { project, .. } => project,
                        _ => None,
                    },
                };
                let mut dependencies = Vec::new();
                for dependency in depends_on {
                    let id = JobId::new(self.job_tool_id(&dependency.conversation, "job:")?);
                    let row = self.require_job(id)?;
                    if !matches!(
                        row.kind,
                        JobKind::Conversation {
                            keep_open: false,
                            ..
                        }
                    ) || !self.step(|tx| {
                        Ok(tx
                            .open_table(CHILD_CONVERSATIONS)?
                            .get((caller.get(), id.get()))?
                            .is_some()
                            && tx.open_table(USER_ENTRY_AT)?.get(id.get())?.is_none())
                    })? {
                        return self.reject(Violation::InvalidValue("depends_on", "dependencies must be sibling conversations with keep_open false and no user message"));
                    }
                    let condition = match dependency.condition.as_str() {
                        "settled" => DependencyCondition::Settled,
                        "success" => DependencyCondition::Success,
                        _ => {
                            return self.reject(Violation::InvalidValue(
                                "depends_on",
                                "condition must be settled or success",
                            ));
                        }
                    };
                    dependencies.push((id, condition));
                }
                dependencies.sort_by_key(|(id, _)| *id);
                if dependencies.windows(2).any(|pair| pair[0].0 == pair[1].0) {
                    return self.reject(Violation::InvalidValue(
                        "depends_on",
                        "dependencies must be unique",
                    ));
                }
                let (conversation, appended) = self.open_conversation(
                    Some(caller),
                    ConversationSpec {
                        name: None,
                        agent: Some(agent),
                        project,
                        origin: Origin::Agent { call },
                        keep_open: keep_open || every_seconds.is_some(),
                    },
                    Entry::AgentMessage {
                        text: objective,
                        call,
                    },
                    ExecutionConfiguration {
                        agent_profile: Some(agent.into()),
                        llm_profile: model.into(),
                    },
                    None,
                    dependencies,
                )?;
                if let Some(interval_s) = every_seconds {
                    self.put_recurrence(
                        conversation,
                        &Recurrence {
                            phase: RecurrencePhase::Active,
                            n: 1,
                            current_turn: appended.turn,
                            previous_turn: None,
                            interval_s,
                        },
                        None,
                    )?;
                }
                json!({"conversation":conversation.to_string(), "turn":appended.turn.to_string()})
            }
            JobToolCall::JobRetry { turn } => {
                let turn = JobId::new(self.job_tool_id(&turn, "job:")?);
                let row = self.require_job(turn)?;
                let Some(conversation) = row.parent_id.filter(|_| row.kind.is_stored()) else {
                    return self.reject(Violation::InvalidValue("turn", "expected a stored turn"));
                };
                self.job_tool_rights(caller, conversation)?;
                let appended = self.retry(
                    conversation,
                    turn,
                    Actor::Agent {
                        conversation: caller,
                    },
                )?;
                json!({"conversation":conversation.to_string(), "turn":appended.turn.to_string()})
            }
            JobToolCall::JobMessage { conversation, text } => {
                let conversation = JobId::new(self.job_tool_id(&conversation, "job:")?);
                self.job_tool_rights(caller, conversation)?;
                let appended = self.append(
                    conversation,
                    Entry::AgentMessage { text, call },
                    None,
                    None,
                    false,
                )?;
                json!({"conversation":conversation.to_string(), "turn":appended.turn.to_string()})
            }
            tool => {
                let (conversation, action) = match tool {
                    JobToolCall::JobStop { conversation } => (conversation, "stop"),
                    JobToolCall::JobClose { conversation } => (conversation, "close"),
                    JobToolCall::JobHold { conversation } => (conversation, "hold"),
                    JobToolCall::JobUnhold { conversation } => (conversation, "unhold"),
                    _ => return self.corrupted("unexpected management tool".into()),
                };
                let conversation = JobId::new(self.job_tool_id(&conversation, "job:")?);
                self.job_tool_rights(caller, conversation)?;
                let by = Actor::Agent {
                    conversation: caller,
                };
                let effect = match action {
                    "stop" => self.stop_turn(conversation, by)?,
                    "close" => self.close(conversation, by)?,
                    "hold" => self.hold(conversation, by, false)?,
                    _ => self.hold(conversation, by, true)?,
                };
                let status = if effect.already {
                    if self.require_job_state(conversation)?.outcome.is_some() {
                        "already_settled"
                    } else {
                        "already_closing"
                    }
                } else {
                    "applied"
                };
                json!({"conversation":conversation.to_string(), "status":status, "record_seq":effect.record_seq, "running":effect.running.iter().map(ToString::to_string).collect::<Vec<_>>()})
            }
        };
        let text = result.to_string();
        let outcome = if text.len() > max_result_bytes {
            ToolOutcome::CompletedWithheld {
                reason: crate::persistence::WithheldReason::TooLarge,
            }
        } else {
            ToolOutcome::Completed {
                result: ToolPayload::new(text),
            }
        };
        self.tool_outcome(call, outcome)?;
        Ok(result)
    }
}
