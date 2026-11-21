use super::{
    DEFAULT_OUTPUT_PAGE, HttpRequest, INVALID_REQUEST, JOB_NOT_FOUND, MAX_OUTPUT_PAGE, Refusal,
    Reply, Shared, contracts::envelope, decimal,
};
use crate::db::entities::{JobId, MutableJobPhase, OutcomeRecord, PoolAllocation};

#[derive(Clone, Copy, PartialEq, Eq, serde::Serialize, ts_rs::TS)]
#[serde(rename_all = "snake_case")]
#[ts(rename = "SettlementState")]
pub(crate) enum SettlementState {
    Unsettled,
    Completed,
    Failed,
    Cancelled,
    Unknown,
}

#[derive(Clone, serde::Serialize, ts_rs::TS)]
#[ts(rename = "Settlement")]
pub(crate) struct SettlementView {
    state: SettlementState,
    settled_at_us: Option<i64>,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[serde(rename_all = "snake_case")]
#[ts(rename = "JobKind")]
pub(crate) enum JobKindView {
    Conversation,
    Turn,
}

#[derive(Clone, Copy, PartialEq, Eq, serde::Serialize, ts_rs::TS)]
#[serde(rename_all = "snake_case")]
#[ts(rename = "ExecutionPhase")]
pub(crate) enum ExecutionPhaseView {
    Unsubmitted,
    Queued,
    Reserved,
    Running,
    StopRequested,
    Unknown,
    Completed,
    Failed,
    Cancelled,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "JobRetry")]
struct JobRetryView {
    original: String,
    by: super::catalog::ActorView,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Job")]
pub(crate) struct JobView {
    id: String,
    event_seq: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    parent: Option<String>,
    name: Option<String>,
    title: Option<String>,
    is_chat: bool,
    execution_mode: String,
    input: String,
    previous: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    retry: Option<JobRetryView>,
    kind: JobKindView,
    conversation: Option<String>,
    assigned_executor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    last_executor: Option<String>,
    created_at_us: i64,
    updated_at_us: i64,
    started_at_us: Option<i64>,
    paused_at_us: Option<i64>,
    sterilized_at_us: Option<i64>,
    settlement: SettlementView,
    execution_phase: ExecutionPhaseView,
    stop_confirmed_at_us: Option<i64>,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Jobs")]
pub(crate) struct JobsView {
    pub(super) jobs: Vec<JobView>,
    pub(super) next_before: Option<String>,
    pub(super) has_more: bool,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "JobDetails")]
pub(crate) struct JobDetailsView {
    pub(super) job: JobView,
    pub(super) result: Option<super::outputs::OutputView>,
    pub(super) usage: Option<crate::db::entities::JobUsage>,
    pub(super) activity: crate::db::entities::JobActivity,
    pub(super) task_plan: Option<crate::db::entities::JobPlanView>,
    pub(super) unknown_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub(super) allocation: Option<AllocationView>,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[serde(rename_all = "snake_case")]
#[ts(rename = "AllocationPhase")]
enum AllocationPhaseView {
    Reserved,
    Running,
    StopRequested,
    Uncertain,
    Released,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Allocation")]
pub(super) struct AllocationView {
    pool: String,
    executor: String,
    generation: u32,
    job_revision: u64,
    phase: AllocationPhaseView,
    holds_capacity: bool,
}

pub(super) fn allocation_view(allocation: &crate::db::entities::PoolAllocation) -> AllocationView {
    AllocationView {
        pool: allocation.token.pool.to_string(),
        executor: allocation.token.executor.to_string(),
        generation: allocation.token.generation.get(),
        job_revision: allocation.job_revision,
        phase: allocation_phase_view(allocation.phase),
        holds_capacity: allocation.holds_capacity(),
    }
}

fn allocation_phase_view(phase: crate::db::entities::AllocationPhase) -> AllocationPhaseView {
    use crate::db::entities::AllocationPhase;
    match phase {
        AllocationPhase::Reserved => AllocationPhaseView::Reserved,
        AllocationPhase::Running => AllocationPhaseView::Running,
        AllocationPhase::StopRequested => AllocationPhaseView::StopRequested,
        AllocationPhase::Uncertain => AllocationPhaseView::Uncertain,
        AllocationPhase::Released => AllocationPhaseView::Released,
    }
}

impl JobView {
    pub(super) fn event_seq(&self) -> &str {
        &self.event_seq
    }

    pub(super) fn lifecycle(&self) -> (String, [Option<i64>; 4]) {
        (
            self.event_seq.clone(),
            [
                Some(self.execution_phase as i64),
                Some(self.settlement.state as i64),
                self.settlement.settled_at_us,
                self.stop_confirmed_at_us,
            ],
        )
    }

    pub(super) fn concluded(&self) -> bool {
        match self.settlement.state {
            SettlementState::Completed | SettlementState::Failed | SettlementState::Cancelled => {
                true
            }
            SettlementState::Unknown | SettlementState::Unsettled => false,
        }
    }

    pub(super) fn settlement(&self) -> SettlementView {
        self.settlement.clone()
    }

    pub(super) fn stop_confirmed_at_us(&self) -> Option<i64> {
        self.stop_confirmed_at_us
    }
}

pub(super) fn jobs_page_view(page: &crate::db::entities::JobSearchPage) -> JobsView {
    JobsView {
        jobs: page
            .items
            .iter()
            .map(|item| {
                mutable_job_view(&item.record, item.allocation.as_ref(), item.updated_at_us)
            })
            .collect(),
        next_before: page.next_before.map(|id| id.to_string()),
        has_more: page.has_more,
    }
}

pub(super) fn mutable_title(job: &crate::db::entities::MutableJob) -> String {
    match &job.input.content {
        crate::db::entities::MutableJobContent::Generative { text, .. } => text.as_str(),
        crate::db::entities::MutableJobContent::Decision { request, .. } => {
            request.question.prompt()
        }
    }
    .chars()
    .take(80)
    .collect()
}

fn execution_phase(
    phase: MutableJobPhase,
    outcome: Option<&OutcomeRecord>,
    allocation: Option<&PoolAllocation>,
) -> ExecutionPhaseView {
    use crate::db::entities::{AllocationPhase, MutableJobPhase, OutcomeKind};
    if let Some(outcome) = outcome {
        return match outcome.kind {
            OutcomeKind::Completed => ExecutionPhaseView::Completed,
            OutcomeKind::Failed => ExecutionPhaseView::Failed,
            OutcomeKind::Cancelled => ExecutionPhaseView::Cancelled,
            OutcomeKind::Unknown => ExecutionPhaseView::Unknown,
        };
    }
    match allocation.map(|allocation| allocation.phase) {
        Some(AllocationPhase::Reserved) => ExecutionPhaseView::Reserved,
        Some(AllocationPhase::Running) => ExecutionPhaseView::Running,
        Some(AllocationPhase::StopRequested) => ExecutionPhaseView::StopRequested,
        Some(AllocationPhase::Uncertain) => ExecutionPhaseView::Unknown,
        Some(AllocationPhase::Released) | None => match phase {
            MutableJobPhase::Unsubmitted => ExecutionPhaseView::Unsubmitted,
            MutableJobPhase::Queued => ExecutionPhaseView::Queued,
            MutableJobPhase::Executing => ExecutionPhaseView::Running,
        },
    }
}

fn settlement_view(outcome: Option<&OutcomeRecord>) -> SettlementView {
    use crate::db::entities::OutcomeKind;
    match outcome {
        None => SettlementView {
            state: SettlementState::Unsettled,
            settled_at_us: None,
        },
        Some(outcome) => SettlementView {
            state: match outcome.kind {
                OutcomeKind::Completed => SettlementState::Completed,
                OutcomeKind::Failed => SettlementState::Failed,
                OutcomeKind::Cancelled => SettlementState::Cancelled,
                OutcomeKind::Unknown => SettlementState::Unknown,
            },
            settled_at_us: Some(outcome.at.as_unix_micros()),
        },
    }
}

pub(super) fn mutable_job_view(
    record: &crate::db::entities::MutableJobRecord,
    allocation: Option<&crate::db::entities::PoolAllocation>,
    updated_at_us: i64,
) -> JobView {
    let job = &record.job;
    JobView {
        id: job.id.to_string(),
        event_seq: record.event_seq.to_string(),
        parent: Some(job.parent_id.to_string()),
        name: None,
        title: Some(mutable_title(job)).filter(|title| !title.is_empty()),
        is_chat: true,
        execution_mode: "once".to_owned(),
        input: job.id.to_string(),
        previous: job.previous.map(|id| id.to_string()),
        retry: job.retry.as_ref().map(|retry| JobRetryView {
            original: retry.original.to_string(),
            by: super::catalog::actor_view(&job.submitted_by),
        }),
        kind: JobKindView::Turn,
        conversation: Some(job.parent_id.to_string()),
        assigned_executor: allocation
            .filter(|allocation| allocation.holds_capacity())
            .map(|allocation| allocation.token.executor.to_string()),
        last_executor: allocation
            .filter(|allocation| !allocation.holds_capacity())
            .map(|allocation| allocation.token.executor.to_string()),
        created_at_us: job.created_at.as_unix_micros(),
        updated_at_us,
        started_at_us: allocation
            .and_then(|allocation| allocation.started_at)
            .map(|time| time.as_unix_micros()),
        paused_at_us: None,
        sterilized_at_us: record
            .outcome
            .as_ref()
            .map(|outcome| outcome.at.as_unix_micros()),
        settlement: settlement_view(record.outcome.as_ref()),
        execution_phase: execution_phase(job.phase, record.outcome.as_ref(), allocation),
        stop_confirmed_at_us: allocation
            .and_then(|allocation| allocation.stop_confirmed_at)
            .map(|time| time.as_unix_micros()),
    }
}
fn execution_page_query(query: &str, cursor_name: &str) -> Result<(Option<u64>, usize), Refusal> {
    let (mut cursor, mut limit) = (None, None);
    if !query.is_empty() {
        for pair in query.split('&') {
            let (key, value) = pair.split_once('=').ok_or(INVALID_REQUEST)?;
            if key == cursor_name && cursor.is_none() {
                cursor = Some(decimal::<u64>(value).ok_or(INVALID_REQUEST)?);
            } else if key == "limit" && limit.is_none() {
                limit = Some(
                    decimal::<usize>(value)
                        .filter(|limit| (1..=MAX_OUTPUT_PAGE).contains(limit))
                        .ok_or(INVALID_REQUEST)?,
                );
            } else {
                return Err(INVALID_REQUEST);
            }
        }
    }
    Ok((cursor, limit.unwrap_or(DEFAULT_OUTPUT_PAGE)))
}

pub(super) fn history_query(query: &str) -> Result<(Option<u64>, usize), Refusal> {
    execution_page_query(query, "after")
}

pub(super) fn state_query(query: &str) -> Result<Option<u64>, Refusal> {
    if query.is_empty() {
        return Ok(None);
    }
    query
        .strip_prefix("at_seq=")
        .and_then(decimal::<u64>)
        .map(Some)
        .ok_or(INVALID_REQUEST)
}

#[derive(serde::Serialize, ts_rs::TS)]
#[serde(rename_all = "snake_case")]
#[ts(rename = "ExecutionHistoryKind")]
pub(crate) enum ExecutionHistoryKindView {
    Created,
    PhaseChanged,
    AllocationReserved,
    AllocationStarted,
    AllocationPhaseChanged,
    AllocationStopConfirmed,
    JobOutcome,
    Closing,
    ClosingRunningChanged,
    HoldChanged,
    VisibilityChanged,
    PrerequisitesChanged,
    InputRevisionRecorded,
    DependencyAdded,
    DependencyResolved,
    RequestRecorded,
    ToolRecorded,
    OutputRecorded,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "ExecutionHistoryEvent")]
pub(crate) struct ExecutionHistoryEventView {
    seq: String,
    previous_seq: String,
    transaction: String,
    committed_seq: String,
    at_us: i64,
    kind: ExecutionHistoryKindView,
    facts: serde_json::Value,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "ExecutionHistory")]
pub(crate) struct ExecutionHistoryView {
    job: String,
    event_seq: String,
    events: Vec<ExecutionHistoryEventView>,
    next_after: Option<String>,
    has_more: bool,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "ExecutionDependency")]
pub(crate) struct ExecutionDependencyView {
    prerequisite: String,
    condition: String,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "ExecutionState")]
pub(crate) struct ExecutionStateView {
    job: String,
    at_seq: Option<String>,
    event_seq: String,
    execution_phase: Option<ExecutionPhaseView>,
    input_revision: Option<String>,
    settlement: SettlementView,
    outcome_cause: Option<serde_json::Value>,
    allocation: Option<AllocationView>,
    assigned_executor: Option<String>,
    last_executor: Option<String>,
    stop_confirmed_at_us: Option<i64>,
    stop_proof: Option<String>,
    closing_at_us: Option<i64>,
    closing_running: Option<bool>,
    on_hold: Option<u32>,
    hidden_at_us: Option<i64>,
    prerequisites: Vec<ExecutionDependencyView>,
    dependencies: Vec<ExecutionDependencyView>,
}

fn stop_proof_name(proof: crate::db::entities::StopProof) -> &'static str {
    use crate::db::entities::StopProof;
    match proof {
        StopProof::Proof => "proof",
        StopProof::Reboot => "reboot",
        StopProof::Settled => "settled",
        StopProof::User => "user",
        StopProof::Operator => "operator",
    }
}

fn dependency_condition(condition: crate::db::entities::DependencyCondition) -> &'static str {
    use crate::db::entities::DependencyCondition;
    match condition {
        DependencyCondition::Success => "success",
        DependencyCondition::Settled => "settled",
    }
}

fn outcome_cause(cause: &crate::db::entities::OutcomeCause) -> serde_json::Value {
    use crate::db::entities::OutcomeCause;
    use serde_json::json;
    match cause {
        OutcomeCause::Code(code) => json!({"kind": "code", "code": code}),
        OutcomeCause::Text(text) => json!({"kind": "text", "text": text}),
        OutcomeCause::Abandoned => json!({"kind": "abandoned"}),
        OutcomeCause::DependencyImpossible => json!({"kind": "dependency_impossible"}),
        OutcomeCause::Stopped { record } => json!({"kind": "stopped", "record": record}),
        OutcomeCause::Closed => json!({"kind": "closed"}),
        OutcomeCause::Uncertain(reason) => json!({
            "kind": "uncertain", "reason": reason.as_str(), "description": reason.description()
        }),
    }
}

fn history_event_view(
    item: &crate::db::entities::ExecutionHistoryItem,
) -> ExecutionHistoryEventView {
    use crate::db::entities::{Actor, DependencyState, ExecutionEventKind};
    use serde_json::json;
    let event = &item.event;
    let (kind, facts) = match &event.kind {
        ExecutionEventKind::Created { phase } => (
            ExecutionHistoryKindView::Created,
            json!({"phase": phase.map(|phase| execution_phase(phase, None, None))}),
        ),
        ExecutionEventKind::PhaseChanged { phase } => (
            ExecutionHistoryKindView::PhaseChanged,
            json!({"phase": execution_phase(*phase, None, None)}),
        ),
        ExecutionEventKind::AllocationReserved { allocation } => (
            ExecutionHistoryKindView::AllocationReserved,
            json!({
                "allocation": allocation_view(allocation),
                "epoch": allocation.token.epoch.to_string(),
                "claimed_at_us": allocation.claimed_at.as_unix_micros(),
            }),
        ),
        ExecutionEventKind::AllocationStarted => {
            (ExecutionHistoryKindView::AllocationStarted, json!({}))
        }
        ExecutionEventKind::AllocationPhaseChanged { phase } => (
            ExecutionHistoryKindView::AllocationPhaseChanged,
            json!({"phase": allocation_phase_view(*phase)}),
        ),
        ExecutionEventKind::AllocationStopConfirmed { at, proof } => (
            ExecutionHistoryKindView::AllocationStopConfirmed,
            json!({"at_us": at.as_unix_micros(), "proof": stop_proof_name(*proof)}),
        ),
        ExecutionEventKind::JobOutcome { outcome } => (
            ExecutionHistoryKindView::JobOutcome,
            json!({"settlement": settlement_view(Some(outcome)), "cause": outcome.cause.as_ref().map(outcome_cause)}),
        ),
        ExecutionEventKind::Closing { at, by, running } => {
            let by = match by {
                Actor::Auto => json!({"kind": "auto"}),
                Actor::Agent { conversation } => {
                    json!({"kind": "agent", "conversation": conversation.to_string()})
                }
                Actor::User => json!({"kind": "user"}),
                Actor::Loop => json!({"kind": "loop"}),
            };
            (
                ExecutionHistoryKindView::Closing,
                json!({"at_us": at.as_unix_micros(), "by": by, "running": running}),
            )
        }
        ExecutionEventKind::ClosingRunningChanged { running } => (
            ExecutionHistoryKindView::ClosingRunningChanged,
            json!({"running": running}),
        ),
        ExecutionEventKind::HoldChanged { reason } => (
            ExecutionHistoryKindView::HoldChanged,
            json!({"reason": reason}),
        ),
        ExecutionEventKind::VisibilityChanged { at } => (
            ExecutionHistoryKindView::VisibilityChanged,
            json!({"hidden_at_us": at.map(|at| at.as_unix_micros())}),
        ),
        ExecutionEventKind::PrerequisitesChanged { after } => (
            ExecutionHistoryKindView::PrerequisitesChanged,
            json!({"after": after.iter().map(|(job, condition)| ExecutionDependencyView {
                prerequisite: job.to_string(),
                condition: dependency_condition(*condition).to_owned(),
            }).collect::<Vec<_>>()}),
        ),
        ExecutionEventKind::InputRevisionRecorded { revision } => (
            ExecutionHistoryKindView::InputRevisionRecorded,
            json!({"revision": revision.to_string()}),
        ),
        ExecutionEventKind::DependencyAdded {
            prerequisite,
            condition,
        } => (
            ExecutionHistoryKindView::DependencyAdded,
            json!({"prerequisite": prerequisite.to_string(), "condition": dependency_condition(*condition)}),
        ),
        ExecutionEventKind::DependencyResolved {
            prerequisite,
            state,
        } => {
            let state = match state {
                DependencyState::Waiting => "waiting",
                DependencyState::Satisfied => "satisfied",
                DependencyState::Impossible => "impossible",
            };
            (
                ExecutionHistoryKindView::DependencyResolved,
                json!({"prerequisite": prerequisite.to_string(), "state": state}),
            )
        }
        ExecutionEventKind::RequestRecorded { request, seq } => (
            ExecutionHistoryKindView::RequestRecorded,
            json!({"request": request.get(), "record_seq": seq}),
        ),
        ExecutionEventKind::ToolRecorded { call, completed } => (
            ExecutionHistoryKindView::ToolRecorded,
            json!({"job": call.job.to_string(), "request": call.request.get(), "index": call.index, "completed": completed}),
        ),
        ExecutionEventKind::OutputRecorded { seq } => (
            ExecutionHistoryKindView::OutputRecorded,
            json!({"output_seq": seq}),
        ),
    };
    ExecutionHistoryEventView {
        seq: event.seq.to_string(),
        previous_seq: event.previous_seq.to_string(),
        transaction: event.transaction.to_string(),
        committed_seq: item.committed_seq.to_string(),
        at_us: event.at.as_unix_micros(),
        kind,
        facts,
    }
}

fn execution_state_view(
    snapshot: &crate::db::entities::ExecutionSnapshot,
    at_seq: Option<u64>,
) -> ExecutionStateView {
    let allocation = snapshot.allocation.as_ref();
    let current = allocation.filter(|allocation| allocation.holds_capacity());
    let last = allocation.filter(|allocation| !allocation.holds_capacity());
    ExecutionStateView {
        job: snapshot.job.to_string(),
        at_seq: at_seq.map(|seq| seq.to_string()),
        event_seq: snapshot.seq.to_string(),
        execution_phase: snapshot
            .phase
            .map(|phase| execution_phase(phase, snapshot.outcome.as_ref(), allocation)),
        input_revision: snapshot.input_revision.map(|revision| revision.to_string()),
        settlement: settlement_view(snapshot.outcome.as_ref()),
        outcome_cause: snapshot
            .outcome
            .as_ref()
            .and_then(|outcome| outcome.cause.as_ref())
            .map(outcome_cause),
        allocation: allocation.map(allocation_view),
        assigned_executor: current.map(|allocation| allocation.token.executor.to_string()),
        last_executor: last.map(|allocation| allocation.token.executor.to_string()),
        stop_confirmed_at_us: allocation
            .and_then(|allocation| allocation.stop_confirmed_at)
            .map(|at| at.as_unix_micros()),
        stop_proof: allocation
            .and_then(|allocation| allocation.stop_proof)
            .map(|proof| stop_proof_name(proof).to_owned()),
        closing_at_us: snapshot
            .closing
            .as_ref()
            .map(|closing| closing.at.as_unix_micros()),
        closing_running: snapshot.closing.as_ref().map(|closing| closing.running),
        on_hold: snapshot.on_hold,
        hidden_at_us: snapshot.hidden_at.map(|at| at.as_unix_micros()),
        prerequisites: snapshot
            .prerequisites
            .iter()
            .map(|(job, condition)| ExecutionDependencyView {
                prerequisite: job.to_string(),
                condition: dependency_condition(*condition).to_owned(),
            })
            .collect(),
        dependencies: snapshot
            .dependencies
            .iter()
            .map(|(job, condition)| ExecutionDependencyView {
                prerequisite: job.to_string(),
                condition: dependency_condition(*condition).to_owned(),
            })
            .collect(),
    }
}

pub(super) async fn history(
    shared: &Shared,
    job: JobId,
    after: Option<u64>,
    limit: usize,
    request: &HttpRequest,
) -> Result<Reply, Refusal> {
    let context = super::mutable_jobs::optional_context(request)?;
    let page = shared
        .database
        .execution_history(job, after, limit, context)
        .await?
        .ok_or(JOB_NOT_FOUND)?;
    super::dashboard::bounded(
        shared,
        envelope(serde_json::json!(ExecutionHistoryView {
            job: job.to_string(),
            event_seq: page.head_seq.to_string(),
            events: page.events.iter().map(history_event_view).collect(),
            next_after: page.next_after.map(|seq| seq.to_string()),
            has_more: page.next_after.is_some(),
        })),
    )
}

pub(super) async fn state(
    shared: &Shared,
    job: JobId,
    at_seq: Option<u64>,
    request: &HttpRequest,
) -> Result<Reply, Refusal> {
    let context = super::mutable_jobs::optional_context(request)?;
    let snapshot = shared
        .database
        .execution_snapshot(job, at_seq, context)
        .await?
        .ok_or(JOB_NOT_FOUND)?;
    super::dashboard::bounded(
        shared,
        envelope(serde_json::json!(execution_state_view(&snapshot, at_seq))),
    )
}
