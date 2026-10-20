//! Reads of what a Job was submitted with, and of what a Job can be
//! submitted with: `GET /api/v1/jobs/{job}/input` and
//! `GET /api/v1/agent-profiles`.
//!
//! Both are projections of persisted facts. The input route gives the text a
//! Job's frozen input holds, which the Job record names only by identity;
//! the Agent Profile route gives each Agent Profile with the LLM Profiles it
//! may run with and the Executors that can run each now. Neither stores,
//! copies or interprets anything, and neither says anything of a Provider's
//! endpoint or credential.

use serde_json::{Value, json};

use super::{
    API_VERSION, INCONSISTENT_RECORD, INVALID_REQUEST, JOB_NOT_FOUND, NOT_FOUND, Refusal, Reply,
    decimal, micros, profiles, routing, submissions,
};
use crate::database::{DatabaseError, DatabaseHandle};
use crate::persistence::{
    ConfigBody, ConfigKind, ConversationEdit, JobId, ModelCapability, ProfileId, RecordRef,
    Violation,
};

/// The most one page of Conversations or Projects reads, and the default.
pub(super) const MAX_CONVERSATION_PAGE: usize = 100;
const DEFAULT_CONVERSATION_PAGE: usize = 20;

/// `before`, as `parse_before` reads it, and `limit`, each at most once, and
/// nothing else.
pub(super) fn page_query<T>(
    query: &str,
    parse_before: impl Fn(&str) -> Result<T, Refusal>,
) -> Result<(Option<T>, usize), Refusal> {
    let (mut before, mut limit) = (None, None);
    if !query.is_empty() {
        for pair in query.split('&') {
            let (key, value) = pair.split_once('=').ok_or(INVALID_REQUEST)?;
            match key {
                "before" if before.is_none() => {
                    before = Some(parse_before(value)?);
                }
                "limit" if limit.is_none() => {
                    limit = Some(
                        decimal::<usize>(value)
                            .filter(|limit| (1..=MAX_CONVERSATION_PAGE).contains(limit))
                            .ok_or(INVALID_REQUEST)?,
                    );
                }
                _ => return Err(INVALID_REQUEST),
            }
        }
    }
    Ok((before, limit.unwrap_or(DEFAULT_CONVERSATION_PAGE)))
}

pub(super) fn conversations_query(query: &str) -> Result<(Option<JobId>, usize), Refusal> {
    page_query(query, routing::job_id)
}

/// The most Profiles one page of Agent Profiles reads, and the default.
pub(super) const MAX_PROFILE_PAGE: usize = 100;
const DEFAULT_PROFILE_PAGE: usize = 50;

/// `before` and `limit`, each at most once, and nothing else.
pub(super) fn agent_profile_query(query: &str) -> Result<(Option<ProfileId>, usize), Refusal> {
    let (mut before, mut limit) = (None, None);
    if !query.is_empty() {
        for pair in query.split('&') {
            let (key, value) = pair.split_once('=').ok_or(INVALID_REQUEST)?;
            match key {
                "before" if before.is_none() => {
                    before = Some(
                        value
                            .strip_prefix("profile:")
                            .and_then(decimal::<u64>)
                            .map(ProfileId::new)
                            .ok_or(INVALID_REQUEST)?,
                    );
                }
                "limit" if limit.is_none() => {
                    limit = Some(
                        decimal::<usize>(value)
                            .filter(|limit| (1..=MAX_PROFILE_PAGE).contains(limit))
                            .ok_or(INVALID_REQUEST)?,
                    );
                }
                _ => return Err(INVALID_REQUEST),
            }
        }
    }
    Ok((before, limit.unwrap_or(DEFAULT_PROFILE_PAGE)))
}

/// The Job's frozen input: its objective as text, its Attachments by
/// identity, and the configuration it was frozen with, named by Profile and
/// model alone.
pub(super) async fn job_input(database: &DatabaseHandle, job: JobId) -> Result<Reply, Refusal> {
    database.job(job).await?.ok_or(JOB_NOT_FOUND)?;
    let input = database
        .job_input_for_job(job)
        .await?
        .ok_or(INCONSISTENT_RECORD)?;
    let configuration = database.job_configuration(job).await?.map(|submitted| {
        json!({
            "agent_profile": submitted.agent_profile.map(|profile| profile.to_string()),
            "llm_profile": submitted.llm_profile.to_string(),
            "model": submitted.frozen.llm.config.model,
            "context_window": submitted.frozen.llm.config.context_window,
        })
    });
    Ok(Reply::ok(json!({
        "api_version": API_VERSION,
        "job": job.to_string(),
        "input": {
            "id": input.id.to_string(),
            "objective": input.objective,
            "submitted_at_us": micros(input.submitted_at),
            "attachments": input
                .attachments
                .iter()
                .map(|attachment| attachment.to_string())
                .collect::<Vec<_>>(),
            "configuration": configuration,
        },
    })))
}

pub(super) fn actor_json(actor: &crate::persistence::Actor) -> Value {
    use crate::persistence::Actor;
    match actor {
        Actor::Auto => json!({"kind":"auto"}),
        Actor::User => json!({"kind":"user"}),
        Actor::Loop => json!({"kind":"loop"}),
        Actor::Agent { conversation } => {
            json!({"kind":"agent", "conversation":conversation.to_string()})
        }
    }
}
pub(super) fn entry_json(seq: u32, entry: &crate::persistence::TurnEntry) -> Value {
    use crate::persistence::Entry;
    let mut value = match &entry.entry {
        Entry::UserMessage { text, attachments } => {
            json!({ "kind": "user_message", "text": text, "attachments": attachments.iter().map(ToString::to_string).collect::<Vec<_>>() })
        }
        Entry::AgentMessage { text, call } => {
            json!({ "kind": "agent_message", "text": text, "call": format!("{}:{}/{}", call.job, call.request.get(), call.index) })
        }
        Entry::Notice {
            child,
            event,
            reference,
        } => {
            json!({ "kind": "notice", "child": child.to_string(), "event": format!("{event:?}"), "reference": reference.to_string() })
        }
        Entry::Decision { approval } => {
            json!({ "kind": "decision", "approval": [approval.0.get(), approval.1 as u64, approval.2 as u64] })
        }
        Entry::RetryOf { turn, by } => {
            json!({ "kind": "retry_of", "turn": turn.to_string(), "by": actor_json(by) })
        }
        Entry::Iteration { n } => json!({ "kind": "iteration", "n": n }),
    };
    value["seq"] = json!(seq);
    value["at_us"] = json!(entry.at.as_unix_micros());
    value
}
pub(super) async fn conversation_children(
    database: &DatabaseHandle,
    conversation: JobId,
    before: Option<JobId>,
    limit: usize,
) -> Result<Reply, Refusal> {
    let row = database.job(conversation).await?.ok_or(JOB_NOT_FOUND)?;
    if !row.kind.is_conversation() {
        return Err(JOB_NOT_FOUND);
    }
    let records = database
        .conversation_children(conversation, before, limit + 1)
        .await?;
    let has_more = records.len() > limit;
    let mut children = Vec::new();
    for record in records.iter().take(limit) {
        let c = &record.conversation;
        let recurrence = record.recurrence.as_ref().map(|row| {
            use crate::persistence::RecurrencePhase;
            let (phase, due) = match row.phase {
                RecurrencePhase::Active => ("active", None),
                RecurrencePhase::WaitingDue { due } => ("waiting_due", Some(due)),
                RecurrencePhase::Halted { .. } => ("halted", None),
            };
            json!({"interval_s":row.interval_s, "phase":phase, "due_us":due})
        });
        children.push(json!({"id":c.root_job.to_string(), "title":c.title, "agent":c.agent.map(|id| ProfileId::from(id).to_string()), "state":c.state, "on_hold":c.on_hold.as_ref().map(|by| json!({"by":actor_json(by)})), "keep_open":record.keep_open, "recurrence":recurrence}));
    }
    Ok(Reply::ok(
        json!({"api_version":API_VERSION,"conversation":conversation.to_string(),"children":children,"has_more":has_more,"next_before":if has_more { records.get(limit.saturating_sub(1)).map(|record| record.conversation.root_job.to_string()) } else { None }}),
    ))
}
pub(super) async fn conversation_turns(
    database: &DatabaseHandle,
    conversation: JobId,
    before: Option<JobId>,
    limit: usize,
) -> Result<Reply, Refusal> {
    let row = database.job(conversation).await?.ok_or(Refusal::new(
        ntex::http::StatusCode::NOT_FOUND,
        "conversation_not_found",
        "conversation does not exist",
    ))?;
    if !row.kind.is_conversation() {
        return Err(Refusal::new(
            ntex::http::StatusCode::NOT_FOUND,
            "conversation_not_found",
            "conversation does not exist",
        ));
    }
    let records = database
        .conversation_turns(conversation, before, limit + 1)
        .await?;
    let has_more = records.len() > limit;
    let mut turns = Vec::new();
    for record in records.iter().take(limit) {
        let entries = database.turn_entries(record.id).await?;
        let outputs = database.job_outputs(record.id, None, 1000).await?;
        let configuration = database.job_configuration(record.id).await?.map(|submitted| json!({"agent_profile": submitted.agent_profile.map(|profile| profile.to_string()), "llm_profile": submitted.llm_profile.to_string(), "model": submitted.frozen.llm.config.model, "context_window": submitted.frozen.llm.config.context_window}));
        let mut uncertain = Vec::new();
        if record.unknown_at.is_some() {
            for call in database.tool_evidence(record.id, None, 1000).await? {
                if let Some(admission) = call.admission()
                    && call.outcome.as_ref().is_none_or(|outcome| {
                        matches!(
                            outcome.fact,
                            crate::persistence::ToolOutcome::Uncertain { .. }
                        )
                    })
                {
                    uncertain.push(json!({"request":call.call.request.get(),"index":call.call.index,"tool":call.name,"argv":admission.arguments.as_str()}));
                }
            }
        }
        turns.push(json!({"job":super::job_json(record)?, "entries":entries.iter().map(|(seq, entry)| entry_json(*seq, entry)).collect::<Vec<_>>(), "configuration": configuration, "outputs":outputs.iter().map(super::output_json).collect::<Vec<_>>(), "uncertain_calls":uncertain}));
    }
    Ok(Reply::ok(
        json!({"api_version":API_VERSION,"conversation":conversation.to_string(),"turns":turns,"has_more":has_more,"next_before":if has_more { records.get(limit.saturating_sub(1)).map(|record| record.id.to_string()) } else {None}}),
    ))
}
pub(super) async fn conversation_notices(
    database: &DatabaseHandle,
    conversation: JobId,
    after: u32,
    limit: usize,
) -> Result<Reply, Refusal> {
    if !database
        .job(conversation)
        .await?
        .is_some_and(|record| record.kind.is_conversation())
    {
        return Err(Refusal::new(
            ntex::http::StatusCode::NOT_FOUND,
            "conversation_not_found",
            "conversation does not exist",
        ));
    }
    let records = database.notices(conversation, after, limit + 1).await?;
    let notices = records.iter().take(limit).map(|(seq, notice)| {
        use crate::persistence::{NoticeClass, NoticeBody};
        let body = match &notice.body { NoticeBody::Event {child,event,reference} => json!({"child":child.to_string(),"event":format!("{event:?}"),"reference":reference.to_string()}), NoticeBody::Record {event,by,at} => json!({"event":format!("{event:?}").to_lowercase(),"by":actor_json(by),"at_us":at.as_unix_micros()}) };
        json!({"seq":seq,"class":match notice.class {NoticeClass::Attention=>"attention",NoticeClass::Progress=>"progress",NoticeClass::Record=>"record"},"body":body})
    }).collect::<Vec<_>>();
    Ok(Reply::ok(
        json!({"api_version":API_VERSION,"conversation":conversation.to_string(),"notices":notices,"has_more":records.len()>limit}),
    ))
}

fn capability(capability: ModelCapability) -> &'static str {
    match capability {
        ModelCapability::Chat => "chat",
        ModelCapability::Tools => "tools",
        ModelCapability::Vision => "vision",
        ModelCapability::Reasoning => "reasoning",
    }
}

/// One page of Agent Profiles, newest first, strictly before `before` when
/// given. An Agent Profile is an Agent config; archived ones are not listed,
/// so the page may hold fewer than `limit`; `next_before` continues from the
/// oldest one read.
pub(super) async fn agent_profiles(
    database: &DatabaseHandle,
    before: Option<ProfileId>,
    limit: usize,
) -> Result<Reply, Refusal> {
    let mut records = database
        .configs(Some(ConfigKind::Agent), before.map(Into::into), limit + 1)
        .await?;
    let has_more = records.len() > limit;
    records.truncate(limit);
    let next_before = if has_more {
        records
            .last()
            .map(|record| ProfileId::from(record.id).to_string())
    } else {
        None
    };
    let mut agent_profiles = Vec::new();
    for record in records {
        let ConfigBody::Agent(agent) = record.body else {
            return Err(INCONSISTENT_RECORD);
        };
        if record.archived_at.is_some() {
            continue;
        }
        let mut llm_targets = Vec::with_capacity(agent.models.len());
        for target in &agent.models {
            let llm = database.config(*target).await?.ok_or(INCONSISTENT_RECORD)?;
            let ConfigBody::Model(model) = llm.body else {
                return Err(INCONSISTENT_RECORD);
            };
            let executors = database.compatible_executors((*target).into()).await?;
            llm_targets.push(json!({
                "id": ProfileId::from(llm.id).to_string(),
                "name": llm.name,
                "model": model.model,
                "context_window": model.context_window,
                "parameters": {
                    "temperature": model.parameters.temperature,
                    "top_p": model.parameters.top_p,
                    "max_tokens": model.parameters.max_tokens,
                    "reasoning_effort": model.parameters.reasoning_effort,
                    "max_completion_tokens": model.parameters.max_completion_tokens,
                    "presence_penalty": model.parameters.presence_penalty,
                    "frequency_penalty": model.parameters.frequency_penalty,
                },
                "declared_capabilities": model
                    .declared_capabilities
                    .iter()
                    .map(|declared| capability(*declared))
                    .collect::<Vec<_>>(),
                "executors": executors
                    .iter()
                    .map(|executor| executor.to_string())
                    .collect::<Vec<_>>(),
            }));
        }
        agent_profiles.push(json!({
            "id": ProfileId::from(record.id).to_string(),
            "name": record.name,
            "tool_grant": agent.tool_grant,
            "llm_targets": llm_targets,
        }));
    }
    Ok(Reply::ok(page(agent_profiles, next_before)))
}

fn page(agent_profiles: Vec<Value>, next_before: Option<String>) -> Value {
    json!({
        "api_version": API_VERSION,
        "agent_profiles": agent_profiles,
        "has_more": next_before.is_some(),
        "next_before": next_before,
    })
}

/// One page of conversations, newest activity first, strictly before `before` when given.
pub(super) async fn conversations(
    database: &DatabaseHandle,
    before: Option<JobId>,
    limit: usize,
) -> Result<Reply, Refusal> {
    let page = database.conversations(before, limit).await?;
    let items = page
        .conversations
        .into_iter()
        .map(|c| {
            json!({
                "root_job": c.root_job.to_string(),
                "tip_job": c.tip_job.to_string(),
                "agent": c.agent.map(|agent| ProfileId::from(agent).to_string()),
                "title": c.title,
                "project": c.project.map(|project| project.to_string()),
                "state": c.state,
                "on_hold": c.on_hold.as_ref().map(|by| json!({"by": actor_json(by)})),
                "tip_settlement_state": c.tip_settlement_state,
                "tip_created_at_us": c.tip_created_at.as_unix_micros(),
            })
        })
        .collect::<Vec<_>>();
    Ok(Reply::ok(json!({
        "api_version": API_VERSION,
        "conversations": items,
        "has_more": page.has_more,
        "next_before": page.next_before.map(|j| j.to_string()),
    })))
}

/// Exactly one of the two fields.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct WireConversationEdit {
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    hidden: Option<bool>,
}

/// `POST /api/v1/conversations/{job}`: renames the conversation whose root
/// is `root`, or hides it for good. No Job changes.
pub(super) async fn edit_conversation(
    database: &DatabaseHandle,
    root: JobId,
    body: &[u8],
) -> Result<Reply, Refusal> {
    let wire: WireConversationEdit = serde_json::from_slice(body).map_err(|_| INVALID_REQUEST)?;
    let edit = match (wire.title, wire.hidden) {
        (Some(title), None) if submissions::text(&title, submissions::MAX_NAME_BYTES) => {
            ConversationEdit::Title(title)
        }
        (None, Some(true)) => ConversationEdit::Hide,
        _ => return Err(INVALID_REQUEST),
    };
    database
        .set_conversation(root, edit)
        .await
        .map_err(|error| match error {
            DatabaseError::Rejected(Violation::NotFound(RecordRef::Job(_))) => NOT_FOUND,
            other => profiles::map_mutation_error(other),
        })?;
    Ok(Reply::ok(
        json!({"api_version": API_VERSION, "conversation": root.to_string()}),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_profile_query_names_only_before_and_limit_once() {
        let (before, limit) = agent_profile_query("").expect("no query is the first page");
        assert_eq!((before, limit), (None, DEFAULT_PROFILE_PAGE));
        let (before, limit) =
            agent_profile_query("before=profile:7&limit=3").expect("a cursor and a limit");
        assert_eq!((before, limit), (Some(ProfileId::new(7)), 3));
        for refused in [
            "limit=0",
            "limit=101",
            "limit=03",
            "before=7",
            "before=profile:07",
            "before=profile:1&before=profile:2",
            "after=profile:1",
            "limit",
        ] {
            assert!(agent_profile_query(refused).is_err(), "{refused}");
        }
    }
}
