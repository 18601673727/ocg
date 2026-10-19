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
    API_VERSION, INCONSISTENT_RECORD, INVALID_REQUEST, JOB_NOT_FOUND, Refusal, Reply, decimal,
    micros, routing,
};
use crate::database::DatabaseHandle;
use crate::persistence::{ConfigBody, ConfigKind, JobId, ModelCapability, ProfileId};

/// The most Conversations one page of Conversations reads, and the default.
pub(super) const MAX_CONVERSATION_PAGE: usize = 100;
const DEFAULT_CONVERSATION_PAGE: usize = 50;

/// `before` and `limit`, each at most once, and nothing else.
pub(super) fn conversations_query(query: &str) -> Result<(Option<JobId>, usize), Refusal> {
    let (mut before, mut limit) = (None, None);
    if !query.is_empty() {
        for pair in query.split('&') {
            let (key, value) = pair.split_once('=').ok_or(INVALID_REQUEST)?;
            match key {
                "before" if before.is_none() => {
                    before = Some(routing::job_id(value)?);
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

/// Reconstructs the bounded conversation ending at `job` from persisted Job
/// records, their `previous` links, their frozen inputs and their stored
/// outputs, ordered from oldest turn to `job`.
pub(super) async fn job_conversation(
    database: &DatabaseHandle,
    job: JobId,
) -> Result<Reply, Refusal> {
    let genesis = database.genesis();
    let mut visited = std::collections::BTreeSet::new();
    let mut turns = Vec::new();
    let mut cursor = Some(job);
    while let Some(current) = cursor {
        if turns.len() >= super::submissions::MAX_CONVERSATION_TURNS || !visited.insert(current) {
            return Err(INCONSISTENT_RECORD);
        }
        let record = database.job(current).await?.ok_or(if current == job {
            JOB_NOT_FOUND
        } else {
            INCONSISTENT_RECORD
        })?;
        let configuration = database.job_configuration(current).await?;
        if current != job
            && (current == genesis || record.parent != genesis || configuration.is_none())
        {
            return Err(INCONSISTENT_RECORD);
        }
        let input = database
            .job_input_for_job(current)
            .await?
            .ok_or(INCONSISTENT_RECORD)?;
        let configuration_json = configuration.map(|submitted| {
            json!({
                "agent_profile": submitted.agent_profile.map(|profile| profile.to_string()),
                "llm_profile": submitted.llm_profile.to_string(),
                "model": submitted.frozen.llm.config.model,
                "context_window": submitted.frozen.llm.config.context_window,
            })
        });
        let outputs = database.job_outputs(current, None, 1_000).await?;
        turns.push(json!({
            "job": super::job_json(&record)?,
            "input": {
                "id": input.id.to_string(),
                "objective": input.objective,
                "submitted_at_us": micros(input.submitted_at),
                "attachments": input
                    .attachments
                    .iter()
                    .map(|attachment| attachment.to_string())
                    .collect::<Vec<_>>(),
                "configuration": configuration_json,
            },
            "outputs": outputs.iter().map(super::output_json).collect::<Vec<_>>(),
        }));
        cursor = record.previous;
    }
    turns.reverse();
    Ok(Reply::ok(json!({
        "api_version": API_VERSION,
        "job": job.to_string(),
        "turns": turns,
    })))
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
                "agent": ProfileId::from(c.agent).to_string(),
                "title": c.title,
                "turn_count": c.turn_count,
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
