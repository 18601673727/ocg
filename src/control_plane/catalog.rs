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

use super::contracts::*;
use serde_json::Value;

use super::{
    INCONSISTENT_RECORD, INVALID_REQUEST, JOB_NOT_FOUND, NOT_FOUND, Refusal, Reply, Shared,
    dashboard, decimal, profiles, routing, submissions,
};
use crate::database::{DatabaseError, DatabaseHandle};
use crate::persistence::{
    ConfigBody, ConfigKind, ConversationEdit, DependencyCondition, DependencyState, JobId,
    ModelCapability, ProfileId, RecordRef, Recurrence, Violation,
};
use crate::storage::{JobTree, TreeFacts, TreeNode};

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

/// The most bytes a conversation search query may hold after decoding, and
/// the most terms it may hold. Both keep one read transaction short: terms
/// combine by AND, so more terms only narrow the walk, but an unbounded
/// query could still waste a scan on noise.
const MAX_SEARCH_QUERY_BYTES: usize = 500;
const MAX_SEARCH_TERMS: usize = 10;

/// `query` once, `before` and `limit` each at most once, and nothing else.
/// `query` is percent-decoded (`+` means a space, like a browser form), then
/// split into lowercased AND terms, with double quotes keeping phrases together.
pub(super) fn conversation_search_query(
    query: &str,
) -> Result<(Vec<String>, Option<JobId>, usize), Refusal> {
    let (mut raw, mut before, mut limit) = (None, None, None);
    if !query.is_empty() {
        for pair in query.split('&') {
            let (key, value) = pair.split_once('=').ok_or(INVALID_REQUEST)?;
            match key {
                "query" if raw.is_none() => {
                    raw = Some(value);
                }
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
    let raw = raw.ok_or(INVALID_REQUEST)?;
    let decoded = decode_query_value(raw).ok_or(INVALID_REQUEST)?;
    if decoded.len() > MAX_SEARCH_QUERY_BYTES {
        return Err(INVALID_REQUEST);
    }
    let mut terms = Vec::new();
    let mut term = String::new();
    let mut quoted = false;
    for ch in decoded.chars() {
        if ch == '"' {
            quoted = !quoted;
        } else if ch.is_whitespace() && !quoted {
            if !term.is_empty() {
                terms.push(std::mem::take(&mut term).to_lowercase());
            }
        } else {
            term.push(ch);
        }
    }
    if quoted {
        return Err(INVALID_REQUEST);
    }
    if !term.is_empty() {
        terms.push(term.to_lowercase());
    }
    terms.sort();
    terms.dedup();
    if terms.is_empty() || terms.len() > MAX_SEARCH_TERMS {
        return Err(INVALID_REQUEST);
    }
    if terms.iter().any(|term| term.chars().count() > 50) {
        return Err(INVALID_REQUEST);
    }
    Ok((terms, before, limit.unwrap_or(DEFAULT_CONVERSATION_PAGE)))
}

/// Percent-decodes `value`, with `+` as a space. Returns `None` when an
/// escape is truncated, non-hexadecimal, or the result is not UTF-8.
fn decode_query_value(value: &str) -> Option<String> {
    let mut decoded = Vec::with_capacity(value.len());
    let mut bytes = value.bytes();
    while let Some(byte) = bytes.next() {
        if byte == b'+' {
            decoded.push(b' ');
        } else if byte == b'%' {
            let high = bytes.next()?;
            let low = bytes.next()?;
            let digit = |byte: u8| (byte as char).to_digit(16).map(|digit| digit as u8);
            decoded.push(digit(high)? * 16 + digit(low)?);
        } else {
            decoded.push(byte);
        }
    }
    String::from_utf8(decoded).ok()
}

/// The most children one page of a tree lists, and the default.
const MAX_TREE_PAGE: usize = 100;
const DEFAULT_TREE_PAGE: usize = 50;
/// The deepest level a tree expands, and the default.
const MAX_TREE_DEPTH: u8 = 4;
const DEFAULT_TREE_DEPTH: u8 = 2;

/// `after`, `limit` and `depth`, each at most once, and nothing else.
pub(super) fn tree_query(query: &str) -> Result<(Option<JobId>, usize, u8), Refusal> {
    let (mut after, mut limit, mut depth) = (None, None, None);
    if !query.is_empty() {
        for pair in query.split('&') {
            let (key, value) = pair.split_once('=').ok_or(INVALID_REQUEST)?;
            match key {
                "after" if after.is_none() => {
                    after = Some(routing::job_id(value)?);
                }
                "limit" if limit.is_none() => {
                    limit = Some(
                        decimal::<usize>(value)
                            .filter(|limit| (1..=MAX_TREE_PAGE).contains(limit))
                            .ok_or(INVALID_REQUEST)?,
                    );
                }
                "depth" if depth.is_none() => {
                    depth = Some(
                        decimal::<u8>(value)
                            .filter(|depth| (1..=MAX_TREE_DEPTH).contains(depth))
                            .ok_or(INVALID_REQUEST)?,
                    );
                }
                _ => return Err(INVALID_REQUEST),
            }
        }
    }
    Ok((
        after,
        limit.unwrap_or(DEFAULT_TREE_PAGE),
        depth.unwrap_or(DEFAULT_TREE_DEPTH),
    ))
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
                    before = Some(super::profiles::profile_id(value)?);
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
    let configuration = database
        .job_configuration(job)
        .await?
        .map(|submitted| WireConfiguration {
            agent_profile: submitted.agent_profile.map(|id| id.to_string()),
            llm_profile: submitted.llm_profile.to_string(),
            model: submitted.frozen.llm.config.model,
            context_window: submitted.frozen.llm.config.context_window,
        });
    Ok(Reply::ok(envelope(WireJobInput {
        job: job.to_string(),
        input: WireInput {
            id: input.id.to_string(),
            objective: input.objective,
            submitted_at_us: input.submitted_at.map(|at| at.as_unix_micros()),
            attachments: input.attachments.iter().map(ToString::to_string).collect(),
            configuration,
        },
    })))
}

pub(super) fn actor_view(actor: &crate::persistence::Actor) -> WireActor {
    use crate::persistence::Actor;
    match actor {
        Actor::Auto => WireActor::Auto,
        Actor::User => WireActor::User,
        Actor::Loop => WireActor::Loop,
        Actor::Agent { conversation } => WireActor::Agent {
            conversation: conversation.to_string(),
        },
    }
}

pub(super) fn entry_json(seq: u32, entry: &crate::persistence::TurnEntry) -> WireTurnEntry {
    use crate::persistence::Entry;
    let value = match &entry.entry {
        Entry::UserMessage { text, attachments } => WireEntry::UserMessage {
            text: text.clone(),
            attachments: attachments.iter().map(ToString::to_string).collect(),
        },
        Entry::AgentMessage { text, call } => WireEntry::AgentMessage {
            text: text.clone(),
            call: format!("{}:{}/{}", call.job, call.request.get(), call.index),
        },
        Entry::Notice {
            child,
            event,
            reference,
        } => WireEntry::Notice {
            child: child.to_string(),
            event: format!("{event:?}"),
            reference: reference.to_string(),
        },
        Entry::Decision { approval } => WireEntry::Decision {
            approval: (approval.0.to_string(), approval.1, approval.2),
        },
        Entry::RetryOf { turn, by } => WireEntry::RetryOf {
            turn: turn.to_string(),
            by: actor_view(by),
        },
        Entry::ContextPreparation {
            job,
            policy_revision,
        } => WireEntry::ContextPreparation {
            job: job.to_string(),
            policy_revision: policy_revision.to_string(),
        },
        Entry::ContextUse { artifact } => WireEntry::ContextUse {
            artifact: artifact.map(|job| job.to_string()),
        },
        Entry::Iteration { n } => WireEntry::Iteration { n: *n },
        Entry::ResponseFormat { format } => WireEntry::ResponseFormat {
            format: match format {
                crate::persistence::ResponseFormat::JsonObject => WireResponseFormat::JsonObject,
                crate::persistence::ResponseFormat::JsonSchema { name, .. } => {
                    WireResponseFormat::JsonSchema { name: name.clone() }
                }
            },
        },
    };
    WireTurnEntry {
        seq,
        at_us: entry.at.as_unix_micros(),
        entry: value,
    }
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
        children.push(WireChild {
            id: c.root_job.to_string(),
            origin: Some(
                match record.origin {
                    crate::persistence::Origin::User => "user",
                    crate::persistence::Origin::Agent { .. } => "agent",
                    crate::persistence::Origin::System => "system",
                }
                .to_owned(),
            ),
            title: c.title.clone(),
            agent: c.agent.map(|id| ProfileId::from(id).to_string()),
            state: c.state.to_owned(),
            on_hold: c.on_hold.as_ref().map(|by| WireHold { by: actor_view(by) }),
            keep_open: record.keep_open,
            recurrence: record.recurrence.as_ref().map(recurrence_json),
        });
    }
    Ok(Reply::ok(envelope(WireChildren {
        conversation: conversation.to_string(),
        children,
        has_more,
        next_before: if has_more {
            records
                .get(limit.saturating_sub(1))
                .map(|record| record.conversation.root_job.to_string())
        } else {
            None
        },
    })))
}
/// The recurrence of a conversation, as the children route reports it.
fn recurrence_json(row: &Recurrence) -> WireRecurrence {
    use crate::persistence::RecurrencePhase;
    let (phase, due) = match row.phase {
        RecurrencePhase::Active => ("active", None),
        RecurrencePhase::WaitingDue { due } => ("waiting_due", Some(due)),
        RecurrencePhase::Halted { .. } => ("halted", None),
    };
    WireRecurrence {
        interval_s: row.interval_s,
        phase: phase.to_owned(),
        due_us: due,
    }
}

/// One node of the tree, as the route reports it. The children of a node
/// are the nodes the tree holds below it.
fn tree_node_json(node: &TreeNode, tree: &JobTree) -> WireTreeNode {
    let mut value = WireTreeNode {
        id: node.id.to_string(),
        kind: WireJobKind::Turn,
        parent: node.parent_id.map(|id| id.to_string()),
        title: String::new(),
        state: String::new(),
        created_at_us: node.created_at.as_unix_micros(),
        agent: node
            .agent
            .as_ref()
            .map(|(id, _)| ProfileId::from(*id).to_string()),
        agent_name: node.agent.as_ref().and_then(|(_, name)| name.clone()),
        model: None,
        on_hold: None,
        origin: None,
        keep_open: None,
        active_turn: None,
        recurrence: None,
        conversation: None,
        previous: None,
        depends_on: Vec::new(),
        children: node.children.as_ref().map(|children| {
            children
                .iter()
                .map(|index| tree_node_json(&tree.nodes[*index], tree))
                .collect()
        }),
        has_more_children: node.has_more_children,
        next_after: node.next_after.map(|id| id.to_string()),
    };
    match &node.facts {
        TreeFacts::Conversation {
            title,
            state,
            on_hold,
            origin,
            keep_open,
            active_turn,
            recurrence,
        } => {
            value.kind = WireJobKind::Conversation;
            value.title = title.clone();
            value.state = (*state).to_owned();
            value.on_hold = on_hold.as_ref().map(|by| WireHold { by: actor_view(by) });
            value.origin = Some((*origin).to_owned());
            value.keep_open = Some(*keep_open);
            value.active_turn = active_turn.as_ref().map(|(id, _, _)| id.to_string());
            value.recurrence = recurrence.as_ref().map(recurrence_json);
            value.model = active_turn.as_ref().map(|(_, llm, model)| WireModel {
                llm_profile: ProfileId::from(*llm).to_string(),
                model: model.clone(),
            });
        }
        TreeFacts::Turn {
            title,
            state,
            conversation,
            previous,
            model,
            depends_on,
        } => {
            value.title = title.clone();
            value.state = (*state).to_owned();
            value.model = Some(WireModel {
                llm_profile: ProfileId::from(model.0).to_string(),
                model: model.1.clone(),
            });
            value.conversation = conversation.map(|id| id.to_string());
            value.previous = previous.map(|id| id.to_string());
            value.depends_on = depends_on
                .iter()
                .map(|(job, condition, state)| WireDependency {
                    job: job.to_string(),
                    condition: match condition {
                        DependencyCondition::Success => "success",
                        DependencyCondition::Settled => "settled",
                    }
                    .to_owned(),
                    state: match state {
                        DependencyState::Waiting => "waiting",
                        DependencyState::Satisfied => "satisfied",
                        DependencyState::Impossible => "impossible",
                    }
                    .to_owned(),
                })
                .collect();
        }
    }
    value
}

/// The tree of the work below a Job: its ancestors, and the nodes of the
/// tree itself. Every fact is read from the row that owns it.
pub(super) async fn job_tree(
    shared: &Shared,
    job: JobId,
    after: Option<JobId>,
    limit: usize,
    depth: u8,
) -> Result<Reply, Refusal> {
    let tree = shared
        .database
        .job_tree(job, after, limit, depth)
        .await?
        .ok_or(JOB_NOT_FOUND)?;
    let root = tree_node_json(&tree.nodes[0], &tree);
    dashboard::bounded(
        shared,
        envelope(WireTree {
            job: job.to_string(),
            ancestors: tree.ancestors.iter().map(ToString::to_string).collect(),
            root,
        }),
    )
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
        let mut outputs = database.job_outputs(record.id, None, 1000).await?;
        let mut outputs_truncated = false;
        if outputs.len() == 1000
            && outputs
                .last()
                .is_some_and(|last| last.kind != crate::persistence::OutputKind::Result)
        {
            outputs_truncated = true;
            if let Some(latest) = database.latest_output(record.id).await?
                && latest.kind == crate::persistence::OutputKind::Result
            {
                outputs.push(latest);
            }
        }
        let configuration = database
            .job_configuration(record.id)
            .await?
            .map(|submitted| WireConfiguration {
                agent_profile: submitted.agent_profile.map(|id| id.to_string()),
                llm_profile: submitted.llm_profile.to_string(),
                model: submitted.frozen.llm.config.model,
                context_window: submitted.frozen.llm.config.context_window,
            });
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
                    uncertain.push(WireUncertainCall {
                        request: call.call.request.get(),
                        index: call.call.index,
                        tool: call.name.clone(),
                        argv: admission.arguments.as_str().to_owned(),
                    });
                }
            }
        }
        turns.push(WireTurn {
            job: super::projection::job_view(record)?,
            entries: entries
                .iter()
                .map(|(seq, entry)| entry_json(*seq, entry))
                .collect(),
            configuration,
            outputs: outputs.iter().map(super::projection::output_view).collect(),
            uncertain_calls: uncertain,
            outputs_truncated,
        });
    }
    Ok(Reply::ok(envelope(WireTurns {
        conversation: conversation.to_string(),
        turns,
        has_more,
        next_before: if has_more {
            records
                .get(limit.saturating_sub(1))
                .map(|record| record.id.to_string())
        } else {
            None
        },
    })))
}
pub(super) fn bound_conversation_turns(shared: &Shared, mut body: Value) -> Result<Reply, Refusal> {
    let mut has_more = body["has_more"].as_bool().ok_or(INCONSISTENT_RECORD)?;
    let records = std::mem::take(
        body.get_mut("turns")
            .and_then(Value::as_array_mut)
            .ok_or(INCONSISTENT_RECORD)?,
    );
    body["has_more"] = Value::Bool(false);
    body["next_before"] = body["conversation"].clone();
    let mut size = body.to_string().len();
    let mut turns = Vec::new();
    let mut next_before = Value::Null;
    for mut turn in records {
        let mut cost = turn.to_string().len() + 1;
        if size + cost > shared.response_bytes {
            if !turns.is_empty() {
                has_more = true;
                break;
            }
            let outputs = std::mem::take(
                turn.get_mut("outputs")
                    .and_then(Value::as_array_mut)
                    .ok_or(INCONSISTENT_RECORD)?,
            );
            turn["outputs_truncated"] = Value::Bool(true);
            cost = turn.to_string().len() + 1;
            if size + cost > shared.response_bytes {
                return Err(super::OUTPUT_TOO_LARGE);
            }
            let preview = turn
                .get_mut("outputs")
                .and_then(Value::as_array_mut)
                .ok_or(INCONSISTENT_RECORD)?;
            for output in outputs {
                let output_cost = output.to_string().len() + 1;
                if size + cost + output_cost > shared.response_bytes {
                    break;
                }
                cost += output_cost;
                preview.push(output);
            }
        }
        size += cost;
        next_before = turn["job"]["id"].clone();
        turns.push(turn);
    }
    body["turns"] = Value::Array(turns);
    body["has_more"] = Value::Bool(has_more);
    body["next_before"] = if has_more { next_before } else { Value::Null };
    dashboard::bounded(shared, body)
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
    let notices = records
        .iter()
        .take(limit)
        .map(|(seq, notice)| {
            use crate::persistence::{NoticeBody, NoticeClass};
            let body = match &notice.body {
                NoticeBody::Event {
                    child,
                    event,
                    reference,
                } => WireNoticeBody::Event {
                    child: child.to_string(),
                    event: format!("{event:?}"),
                    reference: reference.to_string(),
                },
                NoticeBody::Record { event, by, at } => WireNoticeBody::Record {
                    event: format!("{event:?}").to_lowercase(),
                    by: actor_view(by),
                    at_us: at.as_unix_micros(),
                },
            };
            WireNotice {
                seq: *seq,
                class: match notice.class {
                    NoticeClass::Attention => "attention",
                    NoticeClass::Progress => "progress",
                    NoticeClass::Record => "record",
                }
                .to_owned(),
                body,
            }
        })
        .collect::<Vec<_>>();
    Ok(Reply::ok(envelope(WireNotices {
        conversation: conversation.to_string(),
        notices,
        has_more: records.len() > limit,
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
            llm_targets.push(WireLlmTarget {
                id: ProfileId::from(llm.id).to_string(),
                name: llm.name,
                model: model.model,
                context_window: model.context_window,
                parameters: WireLlmParameters::from_parameters(&model.parameters),
                declared_capabilities: model
                    .declared_capabilities
                    .iter()
                    .map(|declared| capability(*declared).to_owned())
                    .collect(),
                executors: executors.iter().map(ToString::to_string).collect(),
            });
        }
        agent_profiles.push(WireAgentProfile {
            id: ProfileId::from(record.id).to_string(),
            name: record.name,
            tool_grant: agent.tool_grant,
            llm_targets,
        });
    }
    Ok(Reply::ok(page(agent_profiles, next_before)))
}

fn page(agent_profiles: Vec<WireAgentProfile>, next_before: Option<String>) -> Value {
    envelope(WireAgentProfiles {
        agent_profiles,
        has_more: next_before.is_some(),
        next_before,
    })
}

/// One page of conversations, newest activity first, strictly before `before` when given.
pub(super) async fn conversations(
    database: &DatabaseHandle,
    before: Option<JobId>,
    limit: usize,
) -> Result<Reply, Refusal> {
    let page = database.conversations(before, limit).await?;
    Ok(Reply::ok(envelope(conversation_page(page))))
}

/// One page of conversations newest first whose title or recent message text
/// holds every term. Paging means the same as [`conversations`].
pub(super) async fn search_conversations(
    database: &DatabaseHandle,
    terms: Vec<String>,
    before: Option<JobId>,
    limit: usize,
) -> Result<Reply, Refusal> {
    let page = database.search_conversations(terms, before, limit).await?;
    Ok(Reply::ok(envelope(conversation_page(page))))
}

fn conversation_page(page: crate::storage::ConversationsPage) -> WireConversations {
    let items = page
        .conversations
        .into_iter()
        .map(|c| WireConversation {
            root_job: c.root_job.to_string(),
            tip_job: c.tip_job.to_string(),
            agent: c.agent.map(|id| ProfileId::from(id).to_string()),
            title: c.title,
            project: c.project.map(|id| id.to_string()),
            state: c.state.to_owned(),
            on_hold: c.on_hold.as_ref().map(|by| WireHold { by: actor_view(by) }),
            tip_settlement_state: c.tip_settlement_state.to_owned(),
            tip_created_at_us: c.tip_created_at.as_unix_micros(),
        })
        .collect::<Vec<_>>();
    WireConversations {
        conversations: items,
        has_more: page.has_more,
        next_before: page.next_before.map(|j| j.to_string()),
    }
}

/// Exactly one edit operation.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct WireConversationEdit {
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    hidden: Option<bool>,
    limits: Option<crate::persistence::ConversationLimits>,
}

pub(super) async fn conversation_limits(
    database: &DatabaseHandle,
    conversation: JobId,
) -> Result<Reply, Refusal> {
    let (limits, depth) = database
        .conversation_limits(conversation)
        .await?
        .ok_or(NOT_FOUND)?;
    Ok(Reply::ok(envelope(WireConversationLimits {
        conversation: conversation.to_string(),
        depth,
        remaining_depth: limits.max_depth.saturating_sub(depth),
        max_depth: limits.max_depth,
        max_auto_turns: limits.max_auto_turns,
        max_iterations: limits.max_iterations,
    })))
}

/// `POST /api/v1/conversations/{job}`: renames the conversation whose root
/// is `root`, hides it for good, or updates its limits. No Job spec changes.
pub(super) async fn edit_conversation(
    database: &DatabaseHandle,
    root: JobId,
    body: &[u8],
) -> Result<Reply, Refusal> {
    let wire: WireConversationEdit = serde_json::from_slice(body).map_err(|_| INVALID_REQUEST)?;
    let edit = match (wire.title, wire.hidden, wire.limits) {
        (Some(title), None, None) if submissions::text(&title, submissions::MAX_NAME_BYTES) => {
            ConversationEdit::Title(title)
        }
        (None, Some(true), None) => ConversationEdit::Hide,
        (None, None, Some(limits)) => ConversationEdit::Limits(limits),
        _ => return Err(INVALID_REQUEST),
    };
    database
        .set_conversation(root, edit)
        .await
        .map_err(|error| match error {
            DatabaseError::Rejected(Violation::NotFound(RecordRef::Job(_))) => NOT_FOUND,
            other => profiles::map_mutation_error(other),
        })?;
    Ok(Reply::ok(envelope(WireConversationEdited {
        conversation: root.to_string(),
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversation_search_parses_quoted_phrases_deduplicates_terms_and_bounds_queries() {
        let (terms, before, limit) =
            conversation_search_query("query=Proxy+%22Connection+Reset%22+proxy&limit=2").unwrap();
        assert_eq!(terms, ["connection reset", "proxy"]);
        assert_eq!(before, None);
        assert_eq!(limit, 2);
        for query in [
            "query=%22unfinished",
            "query=%22%22",
            "query=%FF",
            "query=a&query=b",
            "query=a&limit=0",
        ] {
            assert!(conversation_search_query(query).is_err(), "{query}");
        }
    }

    #[test]
    fn the_profile_query_names_only_before_and_limit_once() {
        let (before, limit) = agent_profile_query("").expect("no query is the first page");
        assert_eq!((before, limit), (None, DEFAULT_PROFILE_PAGE));
        let (before, limit) =
            agent_profile_query("before=00000007-0000-7002-8000-000000000007&limit=3")
                .expect("a cursor and a limit");
        assert_eq!(
            (before, limit),
            (
                Some("00000007-0000-7002-8000-000000000007".parse().unwrap()),
                3
            )
        );
        for refused in [
            "limit=0",
            "limit=101",
            "limit=03",
            "before=7",
            "before=00000007-0000-4002-8000-000000000007",
            "before=00000001-0000-7002-8000-000000000001&before=00000002-0000-7002-8000-000000000002",
            "after=00000001-0000-7002-8000-000000000001",
            "limit",
        ] {
            assert!(agent_profile_query(refused).is_err(), "{refused}");
        }
    }

    #[test]
    fn the_tree_query_names_after_limit_and_depth_once() {
        let (after, limit, depth) = tree_query("").expect("no query is the first page");
        assert_eq!(
            (after, limit, depth),
            (None, DEFAULT_TREE_PAGE, DEFAULT_TREE_DEPTH)
        );
        let (after, limit, depth) =
            tree_query("after=00000003-0000-7001-8000-000000000003&limit=5&depth=4")
                .expect("a cursor, a limit and a depth");
        assert_eq!(
            (after, limit, depth),
            (
                Some("00000003-0000-7001-8000-000000000003".parse().unwrap()),
                5,
                4
            )
        );
        for refused in [
            "limit=0",
            "limit=101",
            "depth=0",
            "depth=5",
            "after=3",
            "after=00000001-0000-7001-8000-000000000001&after=00000002-0000-7001-8000-000000000002",
            "before=00000001-0000-7001-8000-000000000001",
            "depth",
        ] {
            assert!(tree_query(refused).is_err(), "{refused}");
        }
    }
}
