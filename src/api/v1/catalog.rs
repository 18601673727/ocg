use super::contracts::*;
use ntex::web::HttpRequest;
use serde_json::Value;

use super::{
    INCONSISTENT_RECORD, INVALID_REQUEST, JOB_NOT_FOUND, NOT_FOUND, Refusal, Reply, Shared,
    dashboard, decimal, profiles, routing, submissions,
};
use crate::db::entities::{
    ConfigBody, ConfigKind, DependencyCondition, DependencyState, JobEdit, JobId, ModelCapability,
    ProfileId, RecordRef, Recurrence, Violation,
};
use crate::db::redb::{JobTree, TreeFacts, TreeNode};
use crate::db::{DatabaseError, DatabaseHandle};

pub(super) const MAX_CONVERSATION_PAGE: usize = 100;
const DEFAULT_CONVERSATION_PAGE: usize = 20;

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

const MAX_SEARCH_QUERY_BYTES: usize = 500;
const MAX_SEARCH_TERMS: usize = 10;

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

const MAX_TREE_PAGE: usize = 100;
const DEFAULT_TREE_PAGE: usize = 50;
const MAX_TREE_DEPTH: u8 = 4;
const DEFAULT_TREE_DEPTH: u8 = 2;

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

pub(super) const MAX_PROFILE_PAGE: usize = 100;
const DEFAULT_PROFILE_PAGE: usize = 50;

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

pub(super) async fn job_input(
    database: &DatabaseHandle,
    job: JobId,
    request: &HttpRequest,
) -> Result<Reply, Refusal> {
    {
        let context = super::mutable_jobs::optional_context(request)?;
        match database.mutable_job_details(job, context).await? {
            crate::db::redb::MutableJobDetailsLookup::Found(details) => {
                let record = details.record;
                let configuration = details
                    .configuration
                    .map(|configuration| ConfigurationView {
                        agent_profile: configuration
                            .selection
                            .agent_profile
                            .map(|id| id.to_string()),
                        llm_profile: configuration.selection.llm_profile.to_string(),
                        model: configuration.model,
                        context_window: configuration.context_window,
                        reasoning_effort: configuration
                            .selection
                            .reasoning_effort
                            .map(|effort| effort.as_str().to_owned()),
                    });
                let mut body = envelope(serde_json::json!(JobInputView {
                    job: job.to_string(),
                    input: InputView {
                        id: job.to_string(),
                        objective: match &record.job.input.content {
                            crate::db::entities::MutableJobContent::Generative { text, .. } =>
                                Some(text.clone()),
                            crate::db::entities::MutableJobContent::Decision { .. } => None,
                        },
                        submitted_at_us: record.job.submitted_at.map(|time| time.as_unix_micros()),
                        attachments: record
                            .job
                            .input
                            .attachments()
                            .iter()
                            .map(ToString::to_string)
                            .collect(),
                        configuration,
                    },
                }));
                body["input"]["revision"] = serde_json::json!(record.job.revision);
                if let crate::db::entities::MutableJobContent::Decision {
                    request, profile, ..
                } = &record.job.input.content
                {
                    body["input"]["kind"] = serde_json::json!("decision");
                    body["input"]["decision_request"] = serde_json::json!(request);
                    body["input"]["decision_profile"] = serde_json::json!(profile.to_string());
                }
                body["input"]["project"] =
                    serde_json::json!(record.project.map(|project| project.to_string()));
                body["input"]["execution_requirements"] = serde_json::json!(
                    record.job.input.configuration().map(|selection| serde_json::json!({
                        "llm_profile": selection.llm_profile.to_string(),
                        "agent_profile": selection.agent_profile.map(|id| id.to_string()),
                        "reasoning_effort": selection.reasoning_effort.map(|effort| effort.as_str()),
                    }))
                );
                Ok(Reply::ok(body))
            }
            crate::db::redb::MutableJobDetailsLookup::Inaccessible
            | crate::db::redb::MutableJobDetailsLookup::NotMutable => Err(JOB_NOT_FOUND),
        }
    }
}

pub(super) fn actor_view(actor: &crate::db::entities::Actor) -> ActorView {
    use crate::db::entities::Actor;
    match actor {
        Actor::Auto => ActorView::Auto,
        Actor::User => ActorView::User,
        Actor::Loop => ActorView::Loop,
        Actor::Agent { conversation } => ActorView::Agent {
            conversation: conversation.to_string(),
        },
    }
}

#[allow(dead_code)]
#[derive(serde::Serialize, ts_rs::TS)]
#[serde(tag = "type", rename_all = "snake_case")]
#[ts(rename = "ResponseFormat")]
pub(crate) enum ResponseFormatView {
    JsonObject,
    JsonSchema { name: String },
}

#[allow(dead_code)]
#[derive(serde::Serialize, ts_rs::TS)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[ts(rename = "TurnEntryKind")]
pub(crate) enum TurnEntryKindView {
    ContextPreparation {
        job: String,
        policy_revision: String,
    },
    ContextUse {
        artifact: Option<String>,
    },
    UserMessage {
        text: String,
        attachments: Vec<String>,
    },
    UserMessageEdit {
        message: u32,
        text: String,
        attachments: Vec<String>,
    },
    UserMessageWithdraw {
        message: u32,
    },
    AgentMessage {
        text: String,
        call: String,
    },
    Notice {
        child: String,
        event: String,
        reference: String,
    },
    Decision {
        approval: (String, u32, u32),
    },
    RetryOf {
        turn: String,
        by: ActorView,
    },
    Iteration {
        n: u64,
    },
    ResponseFormat {
        #[serde(flatten)]
        format: ResponseFormatView,
    },
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "TurnEntry")]
pub(crate) struct TurnEntryView {
    seq: u32,
    at_us: i64,
    #[serde(flatten)]
    entry: TurnEntryKindView,
}

pub(super) async fn conversation_children(
    database: &DatabaseHandle,
    conversation: JobId,
    before: Option<JobId>,
    limit: usize,
) -> Result<Reply, Refusal> {
    let row = database.job(conversation).await?.ok_or(JOB_NOT_FOUND)?;
    if !row.kind.is_group() {
        return Err(JOB_NOT_FOUND);
    }
    let records = database
        .conversation_children(conversation, before, limit + 1)
        .await?;
    let has_more = records.len() > limit;
    let mut children = Vec::new();
    for record in records.iter().take(limit) {
        let c = &record.conversation;
        children.push(ChildView {
            id: c.root_job.to_string(),
            title: c.title.clone(),
            agent: c.agent.map(|id| ProfileId::from(id).to_string()),
            state: c.state.clone(),
            job_state: record.job_state.clone(),
            on_hold: c.on_hold.as_ref().map(|by| HoldView { by: actor_view(by) }),
            keep_open: record.keep_open,
            recurrence: record.recurrence.as_ref().map(recurrence_json),
            origin: match record.origin {
                crate::db::entities::Origin::User => "user",
                crate::db::entities::Origin::Agent { .. } => "agent",
                crate::db::entities::Origin::System => "system",
            }
            .to_owned(),
        });
    }
    Ok(Reply::ok(envelope(serde_json::json!(ChildrenView {
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
    }))))
}
fn recurrence_json(row: &Recurrence) -> RecurrenceView {
    use crate::db::entities::RecurrencePhase;
    let (phase, due) = match row.phase {
        RecurrencePhase::Active => ("active", None),
        RecurrencePhase::WaitingDue { due } => ("waiting_due", Some(due)),
        RecurrencePhase::Halted { .. } => ("halted", None),
    };
    RecurrenceView {
        interval_s: row.interval_s,
        phase: phase.to_owned(),
        due_us: due,
    }
}

fn dependency_views(
    depends_on: &[(JobId, DependencyCondition, DependencyState)],
) -> Vec<DependencyView> {
    depends_on
        .iter()
        .map(|(job, condition, state)| DependencyView {
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
        .collect()
}

fn tree_node_json(node: &TreeNode, tree: &JobTree) -> TreeNodeView {
    let mut value = TreeNodeView {
        id: node.id.to_string(),
        kind: super::jobs::JobKindView::Turn,
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
            depends_on,
        } => {
            value.kind = super::jobs::JobKindView::Conversation;
            value.depends_on = dependency_views(depends_on);
            value.title = title.clone();
            value.state = (*state).to_owned();
            value.on_hold = on_hold.as_ref().map(|by| HoldView { by: actor_view(by) });
            value.origin = Some((*origin).to_owned());
            value.keep_open = Some(*keep_open);
            value.active_turn = active_turn.as_ref().map(|(id, _, _)| id.to_string());
            value.recurrence = recurrence.as_ref().map(recurrence_json);
            value.model = active_turn.as_ref().map(|(_, llm, model)| ModelView {
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
            value.model = Some(ModelView {
                llm_profile: ProfileId::from(model.0).to_string(),
                model: model.1.clone(),
            });
            value.conversation = conversation.map(|id| id.to_string());
            value.previous = previous.map(|id| id.to_string());
            value.depends_on = dependency_views(depends_on);
        }
    }
    value
}

pub(super) async fn job_tree(
    shared: &Shared,
    job: JobId,
    after: Option<JobId>,
    limit: usize,
    depth: u8,
) -> Result<Reply, Refusal> {
    if let crate::db::redb::MutableJobDetailsLookup::Found(details) =
        shared.database.mutable_job_details(job, None).await?
    {
        return dashboard::bounded(shared, envelope(serde_json::json!(native_tree(&details))));
    }
    let tree = shared
        .database
        .job_tree(job, after, limit, depth)
        .await?
        .ok_or(JOB_NOT_FOUND)?;
    let root = tree_node_json(&tree.nodes[0], &tree);
    dashboard::bounded(
        shared,
        envelope(serde_json::json!(TreeView {
            job: job.to_string(),
            ancestors: tree.ancestors.iter().map(ToString::to_string).collect(),
            root,
        })),
    )
}

fn native_tree(details: &crate::db::redb::MutableJobDetails) -> TreeView {
    let record = &details.record;
    let job = &record.job;
    let title = super::jobs::mutable_title(job);
    let state = match super::mutable_jobs::phase(record) {
        "executing" => "running",
        other => other,
    };
    TreeView {
        job: job.id.to_string(),
        ancestors: vec![job.parent_id.to_string()],
        root: TreeNodeView {
            id: job.id.to_string(),
            kind: super::jobs::JobKindView::Turn,
            parent: Some(job.parent_id.to_string()),
            title,
            state: state.to_owned(),
            created_at_us: job.created_at.as_unix_micros(),
            agent: None,
            agent_name: None,
            model: details
                .configuration
                .as_ref()
                .map(|configuration| ModelView {
                    llm_profile: configuration.selection.llm_profile.to_string(),
                    model: configuration.model.clone(),
                }),
            on_hold: None,
            origin: None,
            keep_open: None,
            active_turn: None,
            recurrence: None,
            conversation: Some(job.parent_id.to_string()),
            previous: job.previous.map(|id| id.to_string()),
            depends_on: Vec::new(),
            children: Some(Vec::new()),
            has_more_children: false,
            next_after: None,
        },
    }
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "UncertainCall")]
pub(crate) struct UncertainCallView {
    request: u32,
    index: u32,
    tool: String,
    argv: String,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "CompactionUse")]
pub(crate) struct CompactionUseView {
    artifact: String,
    kind: String,
    at_us: i64,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[serde(rename_all = "snake_case")]
#[ts(rename = "TurnContentKind")]
enum TurnContentKind {
    Generative,
    Decision,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Turn")]
pub(crate) struct TurnView {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    compaction: Option<CompactionUseView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    native_decision: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    content_kind: Option<TurnContentKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    submitted_at_us: Option<i64>,
    job: super::jobs::JobView,
    entries: Vec<TurnEntryView>,
    configuration: Option<ConfigurationView>,
    outputs: Vec<super::outputs::OutputView>,
    uncertain_calls: Vec<UncertainCallView>,
    outputs_truncated: bool,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Turns")]
pub(crate) struct TurnsView {
    conversation: String,
    turns: Vec<TurnView>,
    has_more: bool,
    next_before: Option<String>,
}

pub(super) async fn conversation_turns(
    database: &DatabaseHandle,
    conversation: JobId,
    before: Option<JobId>,
    limit: usize,
) -> Result<Reply, Refusal> {
    {
        let page = database
            .mutable_conversation_turn_page(conversation, before, limit)
            .await?
            .ok_or(Refusal::new(
                ntex::http::StatusCode::NOT_FOUND,
                "conversation_not_found",
                "conversation does not exist",
            ))?;
        let next_before = page
            .turns
            .last()
            .filter(|_| page.has_more)
            .map(|turn| turn.details.record.job.id.to_string());
        let turns = page
            .turns
            .into_iter()
            .map(|item| {
                let turn = &item.details.record;
                let at = turn
                    .job
                    .submitted_at
                    .ok_or(INCONSISTENT_RECORD)?
                    .as_unix_micros();
                let decision = match &turn.job.input.content {
                    crate::db::entities::MutableJobContent::Generative { .. } => None,
                    crate::db::entities::MutableJobContent::Decision { request, .. } => {
                        let result = item.details.result.as_ref().and_then(|output| output.value.as_ref()).and_then(|value| value.get("decision")).cloned().map(|value| {
                            let result: crate::db::entities::DecisionResult = serde_json::from_value(value).map_err(|_| INCONSISTENT_RECORD)?;
                            let Some(crate::db::entities::MutableFrozenConfiguration::Decision(frozen)) = &turn.job.frozen else { return Err(INCONSISTENT_RECORD) };
                            result.validate(request, &crate::db::entities::FrozenDecisionConfig::Native(frozen.clone()), crate::db::entities::RequestRef { job: turn.job.id, request: crate::db::entities::RequestNumber::FIRST }).map_err(|_| INCONSISTENT_RECORD)?;
                            Ok::<_, Refusal>(result)
                        }).transpose()?;
                        if turn.outcome.as_ref().is_some_and(|outcome| outcome.kind == crate::db::entities::OutcomeKind::Completed) && result.is_none() { return Err(INCONSISTENT_RECORD); }
                        let failure = turn.outcome.as_ref().filter(|outcome| outcome.kind == crate::db::entities::OutcomeKind::Failed).and(item.details.result.as_ref()).and_then(|output| output.summary.as_deref());
                        Some(serde_json::json!({"kind": "decision", "request": request, "result": result, "failure": failure}))
                    }
                };
                let entry = TurnEntryView {
                    seq: 1,
                    at_us: at,
                    entry: TurnEntryKindView::UserMessage {
                        text: turn.job.input.text().to_owned(),
                        attachments: turn
                            .job
                            .input
                            .attachments()
                            .iter()
                            .map(ToString::to_string)
                            .collect(),
                    },
                };
                Ok(TurnView {
                    compaction: None,
                    content_kind: Some(if decision.is_some() { TurnContentKind::Decision } else { TurnContentKind::Generative }),
                    submitted_at_us: turn.job.submitted_at.map(|at| at.as_unix_micros()),
                    native_decision: decision.clone(),
                    job: super::jobs::mutable_job_view(
                        turn,
                        item.details.allocation.as_ref(),
                        item.details.activity.last_activity_at_us(),
                    ),
                    entries: if decision.is_some() { Vec::new() } else { vec![entry] },
                    configuration: item.details.configuration.map(|configuration| {
                        ConfigurationView {
                            agent_profile: configuration
                                .selection
                                .agent_profile
                                .map(|id| id.to_string()),
                            llm_profile: configuration.selection.llm_profile.to_string(),
                            model: configuration.model,
                            context_window: configuration.context_window,
                            reasoning_effort: configuration
                                .selection
                                .reasoning_effort
                                .map(|effort| effort.as_str().to_owned()),
                        }
                    }),
                    outputs: item
                        .outputs
                        .iter()
                        .map(super::outputs::output_view)
                        .collect(),
                    uncertain_calls: item
                        .evidence
                        .into_iter()
                        .filter_map(|call| {
                            let admission = call.admission()?;
                            if call.outcome.as_ref().is_some_and(|outcome| {
                                !matches!(
                                    outcome.fact,
                                    crate::db::entities::ToolOutcome::Uncertain { .. }
                                )
                            }) {
                                return None;
                            }
                            Some(UncertainCallView {
                                request: call.call.request.get(),
                                index: call.call.index,
                                tool: call.name.clone(),
                                argv: admission.arguments.as_str().to_owned(),
                            })
                        })
                        .collect(),
                    outputs_truncated: item.outputs_truncated,
                })
            })
            .collect::<Result<Vec<_>, Refusal>>()?;
        Ok(Reply::ok(envelope(serde_json::json!(TurnsView {
            conversation: conversation.to_string(),
            turns,
            has_more: page.has_more,
            next_before,
        }))))
    }
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
        .is_some_and(|record| record.kind.is_group())
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
            use crate::db::entities::{NoticeBody, NoticeClass};
            let body = match &notice.body {
                NoticeBody::Event {
                    child,
                    event,
                    reference,
                } => serde_json::json!({
                    "child": child.to_string(),
                    "event": format!("{event:?}"),
                    "reference": reference.to_string(),
                }),
                NoticeBody::Record { event, by, at } => serde_json::json!({
                    "event": format!("{event:?}").to_lowercase(),
                    "by": actor_view(by),
                    "at_us": at.as_unix_micros(),
                }),
            };
            serde_json::json!({
                "seq": *seq,
                "class": match notice.class {
                    NoticeClass::Attention => "attention",
                    NoticeClass::Progress => "progress",
                    NoticeClass::Record => "record",
                }
                .to_owned(),
                "body": body,
            })
        })
        .collect::<Vec<_>>();
    Ok(Reply::ok(envelope(serde_json::json!({
        "conversation": conversation.to_string(),
        "notices": notices,
        "has_more": records.len() > limit,
    }))))
}

fn capability(capability: ModelCapability) -> &'static str {
    match capability {
        ModelCapability::Chat => "chat",
        ModelCapability::Tools => "tools",
        ModelCapability::Vision => "vision",
        ModelCapability::Reasoning => "reasoning",
    }
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "LlmTarget")]
pub(crate) struct LlmTargetView {
    reasoning_efforts: Option<Vec<String>>,
    id: String,
    name: String,
    model: String,
    context_window: Option<u32>,
    parameters: crate::db::entities::LlmParameters,
    declared_capabilities: Vec<String>,
    executors: Vec<String>,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "AgentProfile")]
pub(crate) struct AgentProfileView {
    id: String,
    name: String,
    capabilities: std::collections::BTreeSet<crate::db::entities::AgentCapability>,
    llm_targets: Vec<LlmTargetView>,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "AgentProfiles")]
pub(crate) struct AgentProfilesView {
    agent_profiles: Vec<AgentProfileView>,
    has_more: bool,
    next_before: Option<String>,
}

pub(super) async fn agent_profiles(
    database: &DatabaseHandle,
    before: Option<ProfileId>,
    limit: usize,
) -> Result<Reply, Refusal> {
    let configs = database
        .configs(Some(ConfigKind::Agent), before.map(Into::into), limit)
        .await?;
    let next_before = configs.next.map(|id| ProfileId::from(id).to_string());
    let records = configs.records;
    let targets = records
        .iter()
        .filter_map(|record| match &record.body {
            ConfigBody::Agent(agent) if record.archived_at.is_none() => {
                Some(agent.models.iter().copied())
            }
            _ => None,
        })
        .flatten()
        .collect();
    let mut models = database.agent_model_configs(targets).await?.into_iter();
    let mut agent_profiles = Vec::new();
    for record in records {
        let ConfigBody::Agent(agent) = record.body else {
            return Err(INCONSISTENT_RECORD);
        };
        if record.archived_at.is_some() {
            continue;
        }
        let mut llm_targets = Vec::with_capacity(agent.models.len());
        for _ in &agent.models {
            let model_config = models.next().ok_or(INCONSISTENT_RECORD)?;
            let llm = model_config.config.ok_or(INCONSISTENT_RECORD)?;
            let executors = model_config.executors;
            if !llm.model_enabled || llm.archived_at.is_some() {
                continue;
            }
            let ConfigBody::Model(model) = llm.body else {
                continue;
            };
            llm_targets.push(LlmTargetView {
                id: ProfileId::from(llm.id).to_string(),
                name: llm.name,
                model: model.model,
                context_window: model.context_window,
                reasoning_efforts: model.reasoning_efforts,
                parameters: model.parameters,
                declared_capabilities: model
                    .declared_capabilities
                    .iter()
                    .map(|declared| capability(*declared).to_owned())
                    .collect(),
                executors: executors.iter().map(ToString::to_string).collect(),
            });
        }
        agent_profiles.push(AgentProfileView {
            id: ProfileId::from(record.id).to_string(),
            name: record.name,
            capabilities: agent.capabilities,
            llm_targets,
        });
    }
    Ok(Reply::ok(page(agent_profiles, next_before)))
}

fn page(agent_profiles: Vec<AgentProfileView>, next_before: Option<String>) -> Value {
    envelope(serde_json::json!(AgentProfilesView {
        has_more: next_before.is_some(),
        agent_profiles,
        next_before,
    }))
}

pub(super) async fn conversations(
    database: &DatabaseHandle,
    before: Option<JobId>,
    limit: usize,
) -> Result<Reply, Refusal> {
    let page = database.conversations(before, limit).await?;
    Ok(Reply::ok(envelope(conversation_page(page))))
}

pub(super) async fn search_conversations(
    database: &DatabaseHandle,
    terms: Vec<String>,
    before: Option<JobId>,
    limit: usize,
) -> Result<Reply, Refusal> {
    let page = database.search_conversations(terms, before, limit).await?;
    Ok(Reply::ok(envelope(conversation_page(page))))
}

fn conversation_page(page: crate::db::redb::ConversationsPage) -> serde_json::Value {
    let items = page
        .conversations
        .into_iter()
        .map(|c| ConversationView {
            job_state: c.job_state,
            has_children: c.has_children,
            root_job: c.root_job.to_string(),
            tip_job: c.tip_job.to_string(),
            agent: c.agent.map(|id| ProfileId::from(id).to_string()),
            title: c.title,
            project: c.project.map(|id| id.to_string()),
            state: c.state,
            on_hold: c.on_hold.as_ref().map(|by| HoldView { by: actor_view(by) }),
            tip_settlement_state: c.tip_settlement_state,
            tip_created_at_us: c.tip_created_at.as_unix_micros(),
        })
        .collect::<Vec<_>>();
    serde_json::json!(ConversationsView {
        conversations: items,
        has_more: page.has_more,
        next_before: page.next_before.map(|j| j.to_string()),
    })
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ConversationEditInput {
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    hidden: Option<bool>,
    limits: Option<crate::db::entities::JobLimits>,
    #[serde(default, deserialize_with = "profiles::optional_value")]
    compaction_policy: Option<Option<CompactionPolicyInput>>,
}

pub(super) async fn group_limits(
    database: &DatabaseHandle,
    conversation: JobId,
) -> Result<Reply, Refusal> {
    let (limits, depth) = database
        .group_limits(conversation)
        .await?
        .ok_or(NOT_FOUND)?;
    Ok(Reply::ok(envelope(serde_json::json!(
        ConversationLimitsView {
            conversation: conversation.to_string(),
            depth,
            remaining_depth: limits.max_depth.saturating_sub(depth),
            max_depth: limits.max_depth,
            max_auto_turns: limits.max_auto_turns,
            max_iterations: limits.max_iterations,
        }
    ))))
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "ContextModel")]
pub(crate) struct ContextModelView {
    profile: String,
    edit_seq: u32,
    model: String,
    capacity: Option<u32>,
    capacity_source: String,
    reserved_output_tokens: u32,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "ContextUse")]
pub(crate) struct ContextUseView {
    turn: String,
    seq: u32,
    artifact: Option<String>,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "ContextBoundary")]
pub(crate) struct ContextBoundaryView {
    turn: String,
    input_through: u32,
    output_through: u32,
    fingerprint: String,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "ContextSource")]
pub(crate) struct ContextSourceView {
    digest: String,
    first: ContextBoundaryView,
    through: ContextBoundaryView,
    turn_count: usize,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[ts(rename = "ContextWork")]
pub(crate) enum ContextWorkView {
    Summarization {
        job: String,
        conversation: String,
        state: String,
        summarizer: String,
        failure: Option<String>,
        automatic: bool,
        stage: String,
        chain: Option<String>,
        prediction: Option<String>,
        reserved_tokens: u32,
        reserved_requests: u32,
        source: ContextSourceView,
    },
    Prediction {
        job: String,
        conversation: String,
        state: String,
        predictor: String,
        model: String,
        activity_turn: String,
        policy_revision: u64,
        policy: CompactionPolicyInput,
        max_tokens: u32,
        forecast_tokens: Option<u64>,
        result: Option<Value>,
    },
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "ContextStatus")]
pub(crate) struct ContextStatusView {
    conversation: String,
    mode: String,
    summarizer: Option<String>,
    predictive: bool,
    predictive_unavailable: Option<String>,
    revision: u64,
    uncalibrated: bool,
    policy: CompactionPolicyInput,
    policy_override: Option<CompactionPolicyInput>,
    policy_source: String,
    predictor: Option<String>,
    predictor_model: Option<String>,
    prediction_mode: String,
    predictive_preparation_enabled: bool,
    foreground_model: Option<ContextModelView>,
    estimated_tokens: Option<u64>,
    estimate_method: String,
    recent_growth_tokens: u32,
    last_context_use: Option<ContextUseView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    steer_blocked_by: Option<String>,
    work: Vec<ContextWorkView>,
    has_more: bool,
    next_before: Option<String>,
}

pub(super) async fn conversation_context(
    database: &DatabaseHandle,
    conversation: JobId,
    before: Option<JobId>,
    limit: usize,
) -> Result<Reply, Refusal> {
    let state = database
        .context_state(conversation, before, limit)
        .await?
        .ok_or(NOT_FOUND)?;
    let policy_json = compaction_policy_view(state.policy.clone());
    let mode = match &state.policy.mode {
        crate::db::entities::CompactionMode::Off => "disabled",
        crate::db::entities::CompactionMode::Auto { preset } => match preset {
            crate::db::entities::CompactionPreset::Conservative => "conservative",
            crate::db::entities::CompactionPreset::Balanced => "balanced",
            crate::db::entities::CompactionPreset::Aggressive => "aggressive",
        },
        crate::db::entities::CompactionMode::Custom { .. } => "custom",
    };
    let summarizer = state
        .policy
        .summarization_model
        .or_else(|| state.model.as_ref().map(|model| model.model.id));
    let predictive_preparation_enabled =
        state.prediction_unavailable.is_none()
            && state.policy.predictive.as_ref().is_some_and(|binding| {
                binding.mode == crate::db::entities::PredictionMode::Prepare
            });
    let boundary = |source: crate::db::entities::SourceBoundary| ContextBoundaryView {
        turn: source.turn.to_string(),
        input_through: source.input_through,
        output_through: source.output_through,
        fingerprint: source.content.to_string(),
    };
    Ok(Reply::ok(envelope(serde_json::json!(ContextStatusView {
        conversation: conversation.to_string(),
        mode: mode.to_owned(),
        summarizer: summarizer.map(|id| id.to_string()),
        predictive: state.policy.predictive.is_some(),
        predictive_unavailable: state.prediction_unavailable,
        revision: state.revision,
        uncalibrated: true,
        policy: policy_json,
        policy_override: state.stored_override.map(compaction_policy_view),
        policy_source: state.policy_source.to_owned(),
        predictor: state.predictor.map(|id| id.to_string()),
        predictor_model: state.predictor_model.map(|id| id.to_string()),
        prediction_mode: state
            .policy
            .predictive
            .as_ref()
            .map_or("disabled", |binding| match binding.mode {
                crate::db::entities::PredictionMode::Shadow => "shadow",
                crate::db::entities::PredictionMode::Prepare => "prepare",
            })
            .to_owned(),
        predictive_preparation_enabled,
        foreground_model: state.model.map(|model| ContextModelView {
            profile: model.model.id.to_string(),
            edit_seq: model.model.edit_seq,
            model: model.config.model,
            capacity: model.config.context_window,
            capacity_source: if model.config.context_window.is_some() {
                "frozen_model_configuration"
            } else {
                "unknown"
            }
            .to_owned(),
            reserved_output_tokens: model
                .config
                .parameters
                .max_completion_tokens
                .or(model.config.parameters.max_tokens)
                .unwrap_or(4096),
        }),
        estimated_tokens: state.estimated_tokens,
        estimate_method: "utf8_bytes_per_three_plus_framing_and_image_allowance".to_owned(),
        recent_growth_tokens: state.growth,
        last_context_use: state.last_use.map(|(turn, seq, artifact)| ContextUseView {
            turn: turn.to_string(),
            seq,
            artifact: artifact.map(|id| id.to_string()),
        }),
        steer_blocked_by: state.steer_blocked_by.map(str::to_owned),
        work: state
            .work
            .into_iter()
            .map(|work| match work.details {
                crate::db::redb::ContextWorkDetails::Summary {
                    summarizer,
                    failure,
                    automatic,
                    stage,
                    chain,
                    prediction,
                    reserved_tokens,
                    reserved_requests,
                    source,
                    first,
                    through,
                    turn_count,
                } => ContextWorkView::Summarization {
                    job: work.job.to_string(),
                    conversation: work.conversation.to_string(),
                    state: work.state.to_owned(),
                    summarizer: summarizer.to_string(),
                    failure,
                    automatic,
                    stage: stage.to_owned(),
                    chain: chain.map(|id| id.to_string()),
                    prediction: prediction.map(|id| id.to_string()),
                    reserved_tokens,
                    reserved_requests,
                    source: ContextSourceView {
                        digest: source.to_string(),
                        first: boundary(first),
                        through: boundary(through),
                        turn_count,
                    },
                },
                crate::db::redb::ContextWorkDetails::Prediction {
                    predictor,
                    model,
                    binding,
                    forecast_tokens,
                    result,
                } => ContextWorkView::Prediction {
                    job: work.job.to_string(),
                    conversation: work.conversation.to_string(),
                    state: work.state.to_owned(),
                    predictor: predictor.to_string(),
                    model: model.to_string(),
                    activity_turn: binding.activity_turn.to_string(),
                    policy_revision: binding.policy_revision,
                    policy: compaction_policy_view(binding.policy),
                    max_tokens: binding.max_tokens,
                    forecast_tokens,
                    result: result.and_then(|result| serde_json::to_value(result).ok()),
                },
            })
            .collect(),
        has_more: state.next_before.is_some(),
        next_before: state.next_before.map(|id| id.to_string()),
    }))))
}

pub(super) async fn edit_conversation(
    shared: &Shared,
    root: JobId,
    body: &[u8],
) -> Result<Reply, Refusal> {
    let wire: ConversationEditInput = serde_json::from_slice(body).map_err(|_| INVALID_REQUEST)?;
    let edit = match (wire.title, wire.hidden, wire.limits, wire.compaction_policy) {
        (Some(title), None, None, None)
            if submissions::text(&title, submissions::MAX_NAME_BYTES) =>
        {
            JobEdit::Title(title)
        }
        (None, Some(true), None, None) => JobEdit::Hide,
        (None, None, Some(limits), None) => JobEdit::Limits(limits),
        (None, None, None, Some(_)) => {
            return Err(Refusal::new(
                ntex::http::StatusCode::UNPROCESSABLE_ENTITY,
                "global_compaction_policy",
                "change compaction through /api/v1/settings/compaction",
            ));
        }
        _ => return Err(INVALID_REQUEST),
    };
    let map_error = |error| match error {
        DatabaseError::Rejected(Violation::NotFound(RecordRef::Job(_))) => NOT_FOUND,
        other => profiles::map_mutation_error(other),
    };
    if matches!(edit, JobEdit::Hide) {
        let action = shared
            .database
            .hide_conversation(root)
            .await
            .map_err(map_error)?;
        for token in &action.running {
            shared.scheduler.native_stops().cancel(*token);
        }
        if let Err(error) = shared.database.settle_child_turns(action.ended).await {
            log::warn!("the parents of the turns hiding ended were not told: {error}");
        }
    } else {
        shared
            .database
            .set_conversation(root, edit)
            .await
            .map_err(map_error)?;
    }
    Ok(Reply::ok(envelope(serde_json::json!(
        ConversationEditedView {
            conversation: root.to_string(),
        }
    ))))
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "ConversationEdited")]
pub(crate) struct ConversationEditedView {
    conversation: String,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[ts(rename = "Actor")]
pub(crate) enum ActorView {
    Auto,
    User,
    Loop,
    Agent { conversation: String },
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Hold")]
pub(crate) struct HoldView {
    by: ActorView,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Conversation")]
pub(crate) struct ConversationView {
    job_state: String,
    has_children: bool,
    root_job: String,
    tip_job: String,
    agent: Option<String>,
    title: String,
    project: Option<String>,
    state: String,
    on_hold: Option<HoldView>,
    tip_settlement_state: String,
    tip_created_at_us: i64,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Conversations")]
pub(crate) struct ConversationsView {
    conversations: Vec<ConversationView>,
    has_more: bool,
    next_before: Option<String>,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "ConversationLimits")]
pub(crate) struct ConversationLimitsView {
    conversation: String,
    depth: u8,
    remaining_depth: u8,
    max_depth: u8,
    max_auto_turns: u8,
    max_iterations: u32,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Configuration")]
pub(crate) struct ConfigurationView {
    agent_profile: Option<String>,
    llm_profile: String,
    model: String,
    context_window: Option<u32>,
    reasoning_effort: Option<String>,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Input")]
pub(crate) struct InputView {
    id: String,
    objective: Option<String>,
    submitted_at_us: Option<i64>,
    attachments: Vec<String>,
    configuration: Option<ConfigurationView>,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "JobInput")]
pub(crate) struct JobInputView {
    job: String,
    input: InputView,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Recurrence")]
pub(crate) struct RecurrenceView {
    interval_s: u32,
    phase: String,
    due_us: Option<i64>,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Child")]
pub(crate) struct ChildView {
    job_state: String,
    id: String,
    origin: String,
    title: String,
    agent: Option<String>,
    state: String,
    on_hold: Option<HoldView>,
    keep_open: bool,
    recurrence: Option<RecurrenceView>,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Children")]
pub(crate) struct ChildrenView {
    conversation: String,
    children: Vec<ChildView>,
    has_more: bool,
    next_before: Option<String>,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Model")]
pub(crate) struct ModelView {
    llm_profile: String,
    model: String,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Dependency")]
pub(crate) struct DependencyView {
    job: String,
    condition: String,
    state: String,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "TreeNode")]
pub(crate) struct TreeNodeView {
    id: String,
    kind: super::jobs::JobKindView,
    parent: Option<String>,
    title: String,
    state: String,
    created_at_us: i64,
    agent: Option<String>,
    agent_name: Option<String>,
    model: Option<ModelView>,
    on_hold: Option<HoldView>,
    origin: Option<String>,
    keep_open: Option<bool>,
    active_turn: Option<String>,
    recurrence: Option<RecurrenceView>,
    conversation: Option<String>,
    previous: Option<String>,
    depends_on: Vec<DependencyView>,
    children: Option<Vec<TreeNodeView>>,
    has_more_children: bool,
    next_after: Option<String>,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Tree")]
pub(crate) struct TreeView {
    job: String,
    ancestors: Vec<String>,
    root: TreeNodeView,
}
