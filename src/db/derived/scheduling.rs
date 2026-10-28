use crate::db::entities::{
    JobOutputRecord, JobRecord, OutcomeKind, RequestEventRecord, RequestNumber, RequestPayload,
    ToolDecision, ToolEvidenceRecord,
};

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

impl JobActivityPhase {
    pub(in crate::db) fn for_plan(job: &JobRecord) -> Self {
        match job.outcome.as_ref().map(|outcome| outcome.kind) {
            Some(OutcomeKind::Completed) => Self::Completed,
            Some(OutcomeKind::Failed) => Self::Failed,
            Some(OutcomeKind::Cancelled) => Self::Cancelled,
            Some(OutcomeKind::Unknown) => Self::Unknown,
            None if job.started_at.is_some() => Self::RunningTools,
            None => Self::Queued,
        }
    }
}

pub(in crate::db) struct ActivityProjection {
    activity: JobActivity,
    received: bool,
    rate_limited: bool,
    waiting_user: bool,
    latest_request: Option<RequestNumber>,
    unknown_done: bool,
    unknown_reason: Option<String>,
}

impl ActivityProjection {
    pub(in crate::db) fn new(job: &JobRecord, latest_output: Option<&JobOutputRecord>) -> Self {
        Self {
            activity: JobActivity {
                phase: JobActivityPhase::Starting,
                request: None,
                request_started_at_us: None,
                last_activity_at_us: job
                    .updated_at
                    .as_unix_micros()
                    .max(latest_output.map_or(0, |output| output.created_at.as_unix_micros())),
                last_output_at_us: latest_output.map(|output| output.created_at.as_unix_micros()),
                last_output_seq: latest_output.map_or(0, |output| output.seq),
                provider_status: None,
                tool: None,
            },
            received: false,
            rate_limited: false,
            waiting_user: false,
            latest_request: None,
            unknown_done: job.unknown_at.is_none(),
            unknown_reason: None,
        }
    }

    pub(in crate::db) fn request_event(&mut self, event: &RequestEventRecord) {
        let latest = *self.activity.request.get_or_insert(event.request.get());
        if event.request.get() == latest {
            self.activity.last_activity_at_us = self
                .activity
                .last_activity_at_us
                .max(event.recorded_at.as_unix_micros());
            match &event.payload {
                RequestPayload::Sent { .. } => {
                    self.activity.request_started_at_us = Some(event.recorded_at.as_unix_micros())
                }
                RequestPayload::Received { .. } => self.received = true,
                RequestPayload::Availability {
                    status_code,
                    failure,
                    ..
                } => {
                    if self.activity.provider_status.is_none() {
                        self.activity.provider_status = *status_code;
                        self.rate_limited = *status_code == Some(429)
                            && failure.as_ref().is_some_and(|failure| failure.retryable);
                    }
                }
                RequestPayload::ApprovedCalls
                | RequestPayload::Measurement { .. }
                | RequestPayload::Token { .. } => {}
            }
        }
        if !self.unknown_done {
            let latest = *self.latest_request.get_or_insert(event.request);
            if event.request != latest {
                self.unknown_done = true;
            } else if let RequestPayload::Availability { failure, .. } = &event.payload {
                if let Some(failure) = failure {
                    let mut reason = String::new();
                    if let Some(code) = &failure.provider_code {
                        reason.push_str("Provider code: ");
                        reason.push_str(code);
                        reason.push_str(". ");
                    }
                    if let Some(request_id) = &failure.request_id {
                        reason.push_str("Request ID: ");
                        reason.push_str(request_id);
                        reason.push_str(". ");
                    }
                    reason.push_str(&failure.message);
                    self.unknown_reason = Some(cut_unknown_reason(&reason));
                }
                self.unknown_done = true;
            }
        }
    }

    pub(in crate::db) fn tool_call(&mut self, call: ToolEvidenceRecord, awaiting_user: bool) {
        self.activity.last_activity_at_us = self
            .activity
            .last_activity_at_us
            .max(call.decision.at.as_unix_micros());
        if let Some(outcome) = &call.outcome {
            self.activity.last_activity_at_us = self
                .activity
                .last_activity_at_us
                .max(outcome.at.as_unix_micros());
        }
        match call.decision.fact {
            ToolDecision::AwaitingAnswer(_) | ToolDecision::AwaitingApproval(_) => {
                self.waiting_user |= awaiting_user
            }
            ToolDecision::Admitted(_) if call.outcome.is_none() => {
                self.activity.tool = Some(call.name)
            }
            ToolDecision::Admitted(_) | ToolDecision::Refused(_) => {}
        }
    }

    pub(in crate::db) fn finish(mut self, job: &JobRecord) -> (JobActivity, Option<String>) {
        self.activity.phase = match job.outcome.as_ref().map(|outcome| outcome.kind) {
            Some(OutcomeKind::Completed) if self.waiting_user => JobActivityPhase::WaitingForUser,
            Some(OutcomeKind::Completed) => JobActivityPhase::Completed,
            Some(OutcomeKind::Failed) => JobActivityPhase::Failed,
            Some(OutcomeKind::Cancelled) => JobActivityPhase::Cancelled,
            Some(OutcomeKind::Unknown) => JobActivityPhase::Unknown,
            None if job.started_at.is_none() => JobActivityPhase::Queued,
            None if self.activity.tool.is_some() => JobActivityPhase::RunningTools,
            None if self.rate_limited => JobActivityPhase::RateLimited,
            None if self.activity.request_started_at_us.is_some() && !self.received => {
                JobActivityPhase::WaitingForProvider
            }
            None => JobActivityPhase::Starting,
        };
        (self.activity, self.unknown_reason)
    }
}

fn cut_unknown_reason(message: &str) -> String {
    const LIMIT: usize = 2048;
    if message.len() <= LIMIT {
        return message.to_owned();
    }
    let mut boundary = LIMIT;
    while !message.is_char_boundary(boundary) {
        boundary -= 1;
    }
    format!("{}…", &message[..boundary])
}
