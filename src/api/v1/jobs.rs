use super::*;

#[derive(serde::Serialize, ts_rs::TS)]
#[serde(rename_all = "snake_case")]
#[ts(rename = "SettlementState")]
pub(crate) enum SettlementState {
    Unsettled,
    Completed,
    Failed,
    Cancelled,
    Unknown,
}

#[derive(serde::Serialize, ts_rs::TS)]
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

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Job")]
pub(crate) struct JobView {
    id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    parent: Option<String>,
    name: Option<String>,
    title: Option<String>,
    is_chat: bool,
    execution_mode: String,
    input: String,
    previous: Option<String>,
    kind: JobKindView,
    conversation: Option<String>,
    assigned_executor: Option<String>,
    created_at_us: i64,
    updated_at_us: i64,
    started_at_us: Option<i64>,
    paused_at_us: Option<i64>,
    sterilized_at_us: Option<i64>,
    settlement: SettlementView,
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
}

/// The Job's settlement: the one outcome its record holds, or none yet; a
/// record holding more than one is not described at all.
pub(super) fn settlement_view(record: &JobRecord) -> Result<SettlementView, Refusal> {
    let outcomes = [
        (SettlementState::Completed, record.completed_at),
        (SettlementState::Failed, record.failed_at),
        (SettlementState::Cancelled, record.cancelled_at),
        (SettlementState::Unknown, record.unknown_at),
    ]
    .into_iter()
    .filter_map(|(state, at)| at.map(|at| (state, at)))
    .collect::<Vec<_>>();
    let mut outcomes = outcomes.into_iter();
    let (state, settled_at_us) = match outcomes.next() {
        None => (SettlementState::Unsettled, None),
        Some((state, at)) => (state, Some(at.as_unix_micros())),
    };
    if outcomes.next().is_some() {
        return Err(INCONSISTENT_RECORD);
    }
    Ok(SettlementView {
        state,
        settled_at_us,
    })
}

pub(super) fn job_view(record: &JobRecord) -> Result<JobView, Refusal> {
    Ok(JobView {
        id: record.id.to_string(),
        parent: record.parent_id.map(|id| id.to_string()),
        name: record.name.clone(),
        title: record.objective.clone(),
        is_chat: record.group.is_some(),
        execution_mode: "once".to_owned(),
        input: record.input.to_string(),
        previous: record.previous.map(|id| id.to_string()),
        kind: if record.kind.is_group() {
            JobKindView::Conversation
        } else {
            JobKindView::Turn
        },
        conversation: record.group.map(|id| id.to_string()),
        assigned_executor: record.assigned_executor.map(|id| id.to_string()),
        created_at_us: record.created_at.as_unix_micros(),
        updated_at_us: record.updated_at.as_unix_micros(),
        started_at_us: record.started_at.map(|at| at.as_unix_micros()),
        paused_at_us: None,
        sterilized_at_us: record.children_closed_at.map(|at| at.as_unix_micros()),
        settlement: settlement_view(record)?,
        stop_confirmed_at_us: record.stop_confirmed_at.map(|at| at.as_unix_micros()),
    })
}
