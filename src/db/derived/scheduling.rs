#[derive(serde::Serialize, ts_rs::TS, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub(crate) enum JobActivityPhase {
    Queued,
    Starting,
    WaitingForProvider,
    RunningTools,
    WaitingForUser,
    RateLimited,
    RunningTasks,
    Synthesizing,
    Completed,
    Failed,
    Cancelled,
    Unknown,
}

#[derive(serde::Serialize, ts_rs::TS)]
pub(crate) struct JobActivity {
    pub(in crate::db) phase: JobActivityPhase,
    pub(in crate::db) request: Option<u32>,
    pub(in crate::db) request_started_at_us: Option<i64>,
    pub(in crate::db) last_activity_at_us: i64,
    pub(in crate::db) last_output_at_us: Option<i64>,
    pub(in crate::db) last_output_seq: i64,
    pub(in crate::db) provider_status: Option<u16>,
    pub(in crate::db) tool: Option<String>,
}

#[derive(serde::Serialize, ts_rs::TS)]
pub(crate) struct JobPlanStepView {
    pub(in crate::db) conversation: String,
    pub(in crate::db) turn: String,
    pub(in crate::db) title: String,
    pub(in crate::db) agent: Option<String>,
    pub(in crate::db) model: Option<String>,
    pub(in crate::db) state: JobActivityPhase,
    pub(in crate::db) depends_on: Vec<String>,
}

#[derive(serde::Serialize, ts_rs::TS)]
pub(crate) struct JobPlanView {
    pub(in crate::db) owner: String,
    pub(in crate::db) continuation: String,
    pub(in crate::db) state: JobActivityPhase,
    pub(in crate::db) steps: Vec<JobPlanStepView>,
}
