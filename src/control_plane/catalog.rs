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
    micros,
};
use crate::database::DatabaseHandle;
use crate::persistence::{AgentConfig, JobId, ModelCapability, Profile, ProfileId};

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
/// given. The page is `limit` Profiles of either kind as they are stored, of
/// which the Agent Profiles are listed, so it may hold fewer than `limit`;
/// `next_before` continues from the oldest Profile read.
pub(super) async fn agent_profiles(
    database: &DatabaseHandle,
    before: Option<ProfileId>,
    limit: usize,
) -> Result<Reply, Refusal> {
    let mut records = database.profiles(before, limit + 1).await?;
    let has_more = records.len() > limit;
    records.truncate(limit);
    let next_before = if has_more {
        records.last().map(|record| record.id.to_string())
    } else {
        None
    };
    let mut agent_profiles = Vec::new();
    for record in records {
        let Ok(agent) = Profile::<AgentConfig>::try_from(record) else {
            continue;
        };
        let mut llm_targets = Vec::with_capacity(agent.links.len());
        for target in &agent.links {
            let llm = database
                .llm_profile(*target)
                .await?
                .ok_or(INCONSISTENT_RECORD)?;
            let executors = database.compatible_executors(*target).await?;
            llm_targets.push(json!({
                "id": llm.id.to_string(),
                "name": llm.name,
                "model": llm.config.model,
                "declared_capabilities": llm
                    .config
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
            "id": agent.id.to_string(),
            "name": agent.name,
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
