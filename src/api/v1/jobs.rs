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

/// Where a native Job is in its execution, derived from its outcome, its
/// phase and its Allocation. `stop_requested` and `unknown` are not stopped:
/// only a confirmed stop is.
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
    use crate::db::entities::AllocationPhase;
    AllocationView {
        pool: allocation.token.pool.to_string(),
        executor: allocation.token.executor.to_string(),
        generation: allocation.token.generation.get(),
        job_revision: allocation.job_revision,
        phase: match allocation.phase {
            AllocationPhase::Reserved => AllocationPhaseView::Reserved,
            AllocationPhase::Running => AllocationPhaseView::Running,
            AllocationPhase::StopRequested => AllocationPhaseView::StopRequested,
            AllocationPhase::Uncertain => AllocationPhaseView::Uncertain,
            AllocationPhase::Released => AllocationPhaseView::Released,
        },
        holds_capacity: allocation.holds_capacity(),
    }
}

impl JobView {
    /// What an events stream announces the Job again for: its settlement and
    /// its confirmed stop.
    pub(super) fn lifecycle(&self) -> [Option<i64>; 4] {
        [
            Some(self.execution_phase as i64),
            Some(self.settlement.state as i64),
            self.settlement.settled_at_us,
            self.stop_confirmed_at_us,
        ]
    }

    /// Whether the Job concluded: an outcome that is final, or an unknown
    /// one whose local execution was confirmed stopped.
    pub(super) fn concluded(&self) -> bool {
        match self.settlement.state {
            SettlementState::Completed | SettlementState::Failed | SettlementState::Cancelled => {
                true
            }
            SettlementState::Unknown => self.stop_confirmed_at_us.is_some(),
            SettlementState::Unsettled => false,
        }
    }

    pub(super) fn settlement(&self) -> SettlementView {
        self.settlement.clone()
    }

    pub(super) fn stop_confirmed_at_us(&self) -> Option<i64> {
        self.stop_confirmed_at_us
    }
}

pub(super) fn jobs_page_view(page: &crate::db::redb::JobSearchPage) -> JobsView {
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

/// The display title of a native Job: the first 80 characters of its
/// Generative text, or of its Decision question prompt.
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

/// Derives the execution phase. A settled outcome wins; otherwise a live
/// Allocation says how far execution got, and a Job without one is where its
/// own phase says.
fn execution_phase(
    record: &crate::db::entities::MutableJobRecord,
    allocation: Option<&crate::db::entities::PoolAllocation>,
) -> ExecutionPhaseView {
    use crate::db::entities::{AllocationPhase, MutableJobPhase, OutcomeKind};
    if let Some(outcome) = &record.outcome {
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
        Some(AllocationPhase::Released) | None => match record.job.phase {
            MutableJobPhase::Unsubmitted => ExecutionPhaseView::Unsubmitted,
            MutableJobPhase::Queued => ExecutionPhaseView::Queued,
            MutableJobPhase::Executing => ExecutionPhaseView::Running,
        },
    }
}

pub(super) fn mutable_job_view(
    record: &crate::db::entities::MutableJobRecord,
    allocation: Option<&crate::db::entities::PoolAllocation>,
    updated_at_us: i64,
) -> JobView {
    use crate::db::entities::OutcomeKind;
    let job = &record.job;
    let settlement = match &record.outcome {
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
    };
    JobView {
        id: job.id.to_string(),
        parent: Some(job.parent_id.to_string()),
        name: None,
        title: Some(mutable_title(job)).filter(|title| !title.is_empty()),
        is_chat: true,
        execution_mode: "once".to_owned(),
        input: job.id.to_string(),
        previous: job.previous.map(|id| id.to_string()),
        kind: JobKindView::Turn,
        conversation: Some(job.parent_id.to_string()),
        assigned_executor: None,
        created_at_us: job.created_at.as_unix_micros(),
        updated_at_us,
        // Only a committed start is a start; a reservation or a queue time is not.
        started_at_us: allocation
            .and_then(|allocation| allocation.started_at)
            .map(|time| time.as_unix_micros()),
        paused_at_us: None,
        sterilized_at_us: record
            .outcome
            .as_ref()
            .map(|outcome| outcome.at.as_unix_micros()),
        settlement,
        execution_phase: execution_phase(record, allocation),
        stop_confirmed_at_us: allocation
            .and_then(|allocation| allocation.stop_confirmed_at)
            .map(|time| time.as_unix_micros()),
    }
}
