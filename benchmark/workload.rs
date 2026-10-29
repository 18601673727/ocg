//! The reference job workload.
//!
//! Every measurement of the backend's work uses one job shape: three model
//! requests and two tool calls, the two calls reading the fixture the run
//! prepared. The shape is stated with every measurement, because a number
//! without the work behind it describes nothing.

use std::time::Instant;

/// The reference job: three model requests and two tool calls.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReferenceJob {
    /// How many model requests the job makes.
    pub model_requests: usize,
    /// How many tool calls the job makes.
    pub tool_calls: usize,
}

impl ReferenceJob {
    pub const fn new(model_requests: usize, tool_calls: usize) -> Self {
        Self {
            model_requests,
            tool_calls,
        }
    }

    /// The shape every measurement uses.
    pub const fn standard() -> Self {
        Self {
            model_requests: 3,
            tool_calls: 2,
        }
    }
}

/// The concurrency levels the workload is measured at.
#[derive(Clone, Copy, Debug)]
pub struct ConcurrencyLevels;

impl ConcurrencyLevels {
    /// The levels every run measures.
    pub const ALL: [usize; 3] = [1, 8, 16];

    /// The level the summary table reports.
    pub const REFERENCE: usize = 8;
}

/// What one job of the reference workload does.
#[derive(Clone, Debug)]
pub enum Step {
    /// A model request, with the round it is.
    ModelRequest { round: usize },
    /// A tool call, with the round it is and the file it reads.
    ToolCall { round: usize, path: String },
}

/// The steps of the reference job, in the order the backend performs them.
///
/// The provider answers the first two rounds with a tool call and the third
/// with the answer, so a job of three model requests makes two tool calls.
pub fn reference_steps(probe_path: &str) -> Vec<Step> {
    let mut steps = Vec::new();
    for round in 0..ReferenceJob::standard().model_requests {
        steps.push(Step::ModelRequest { round });
        if round < ReferenceJob::standard().tool_calls {
            steps.push(Step::ToolCall {
                round,
                path: probe_path.to_owned(),
            });
        }
    }
    steps
}

/// The timings of one job.
#[derive(Clone, Copy, Debug)]
pub struct JobTiming {
    /// When the job was submitted.
    pub submitted_at: Instant,
    /// When the job was admitted and began to run.
    pub admitted_at: Option<Instant>,
    /// When the first token of the first answer reached the client.
    pub first_token_at: Option<Instant>,
    /// When the job finished.
    pub completed_at: Option<Instant>,
}

impl JobTiming {
    pub fn new(submitted_at: Instant) -> Self {
        Self {
            submitted_at,
            admitted_at: None,
            first_token_at: None,
            completed_at: None,
        }
    }

    pub fn record_admission(&mut self, at: Instant) {
        if self.admitted_at.is_none() {
            self.admitted_at = Some(at);
        }
    }

    pub fn record_first_token(&mut self, at: Instant) {
        if self.first_token_at.is_none() {
            self.first_token_at = Some(at);
        }
    }

    pub fn record_completion(&mut self, at: Instant) {
        self.completed_at = Some(at);
    }

    /// The time the job waited to be admitted.
    pub fn admission_latency(&self) -> Option<Duration> {
        self.admitted_at
            .map(|at| at.saturating_duration_since(self.submitted_at))
    }

    /// The time from the submission to the first token.
    pub fn time_to_first_token(&self) -> Option<Duration> {
        self.first_token_at
            .map(|at| at.saturating_duration_since(self.submitted_at))
    }

    /// The time the whole job took.
    pub fn execution_time(&self) -> Option<Duration> {
        self.completed_at
            .map(|at| at.saturating_duration_since(self.submitted_at))
    }

    /// The time from the first token to the end of the job.
    pub fn streaming_latency(&self) -> Option<Duration> {
        match (self.first_token_at, self.completed_at) {
            (Some(first), Some(end)) => Some(end.saturating_duration_since(first)),
            _ => None,
        }
    }
}

/// A duration in seconds, as the reports state it.
pub type Duration = std::time::Duration;
