//! JSONL reporting and the summary a reader of the results needs.
//!
//! Every line is one JSON object, so a run's output can be read by a tool
//! that knows nothing about the benchmark. The summary mode reads those
//! lines back and prints the table the results support, without the reader
//! having to know which run produced which number.

use serde_json::{Value, json};
use std::time::Duration;

use crate::benchmark::{OutcomeCounts, ResourceUsage, Samples};
use crate::metadata::{BenchmarkMetadata, WorkloadParameters};

/// The kinds of record a run emits.
pub enum Record {
    Environment(Box<EnvironmentRecord>),
    Measurement(Box<MeasurementRecord>),
    Fixture(FixtureRecord),
    Skip(SkipRecord),
}

impl Record {
    pub fn to_json(&self) -> Value {
        match self {
            Record::Environment(record) => record.to_json(),
            Record::Measurement(record) => record.to_json(),
            Record::Fixture(record) => record.to_json(),
            Record::Skip(record) => record.to_json(),
        }
    }

    /// Writes the record to standard output, one line each.
    pub fn emit(&self) {
        println!("{}", self.to_json());
    }
}

/// The environment record, emitted once per run.
pub struct EnvironmentRecord {
    pub metadata: BenchmarkMetadata,
    /// What the workload was: the shape of a job and how many ran at once.
    pub workload_model: String,
    /// What the run covered, and what it left out.
    pub scope: String,
    /// The options the run was asked for, and the ones it applied.
    pub requested: Value,
    pub applied: Value,
}

impl EnvironmentRecord {
    pub fn to_json(&self) -> Value {
        let mut object = self.metadata.to_json();
        if let Some(map) = object.as_object_mut() {
            map.insert("type".to_string(), json!("environment"));
            map.insert("workload_model".to_string(), json!(self.workload_model));
            map.insert("scope".to_string(), json!(self.scope));
            map.insert("requested".to_string(), self.requested.clone());
            map.insert("applied".to_string(), self.applied.clone());
        }
        object
    }
}

/// A measurement: what was measured, and what came of it.
pub struct MeasurementRecord {
    /// The name of the metric.
    pub name: String,
    /// The workload the measurement ran.
    pub parameters: WorkloadParameters,
    /// The environment the measurement ran in.
    pub metadata: BenchmarkMetadata,
    /// The samples the percentile figures come from.
    pub samples: Samples,
    /// What became of the operations that were attempted.
    pub outcomes: OutcomeCounts,
    /// The memory and CPU the run used, when it could be read.
    pub resources: Option<ResourceUsage>,
    /// The window the measurement covered.
    pub timing_window: Duration,
    /// How many operations ran before the measurement began.
    pub warm_up_iterations: usize,
    /// What the figure means, where the name alone does not say it.
    pub note: Option<String>,
}

impl MeasurementRecord {
    pub fn to_json(&self) -> Value {
        let mut object = json!({
            "type": "measurement",
            "name": self.name,
            "parameters": self.parameters.to_json(),
            "environment": self.metadata.to_json(),
            "samples": self.samples.count(),
            "warm_up_iterations": self.warm_up_iterations,
            "timing_window_s": self.timing_window.as_secs_f64(),
            "outcomes": {
                "attempted": self.outcomes.attempted,
                "successful": self.outcomes.successful,
                "rejected": self.outcomes.rejected,
                "failed": self.outcomes.failed,
                "timed_out": self.outcomes.timed_out,
                "unfinished": self.outcomes.unfinished,
            },
        });

        if let Some(map) = object.as_object_mut() {
            if self.samples.count() > 0 {
                map.insert(
                    "p50_ms".to_string(),
                    json!(self.samples.median().as_secs_f64() * 1000.0),
                );
                map.insert(
                    "p95_ms".to_string(),
                    json!(self.samples.p95().as_secs_f64() * 1000.0),
                );
                // A tail percentile over a handful of samples is a number
                // without meaning, so it is reported only when the sample
                // count can carry it.
                if self.samples.count() >= 100 {
                    map.insert(
                        "p99_ms".to_string(),
                        json!(self.samples.p99().as_secs_f64() * 1000.0),
                    );
                }
                map.insert(
                    "max_ms".to_string(),
                    json!(self.samples.max().as_secs_f64() * 1000.0),
                );
                if let Some(mean) = self.samples.mean() {
                    map.insert("mean_ms".to_string(), json!(mean.as_secs_f64() * 1000.0));
                }
                let seconds = self.timing_window.as_secs_f64();
                if seconds > 0.0 {
                    map.insert(
                        "throughput_per_second".to_string(),
                        json!(self.samples.count() as f64 / seconds),
                    );
                }
            }

            if let Some(resources) = &self.resources {
                let mut resource_object = serde_json::Map::new();
                if let Some(current) = resources.current_rss_bytes {
                    resource_object.insert("current_rss_bytes".to_string(), json!(current));
                }
                if let Some(peak) = resources.peak_rss_bytes {
                    resource_object.insert("peak_rss_bytes".to_string(), json!(peak));
                }
                if let Some(user) = resources.user_cpu_seconds {
                    resource_object.insert("user_cpu_seconds".to_string(), json!(user));
                }
                if let Some(system) = resources.system_cpu_seconds {
                    resource_object.insert("system_cpu_seconds".to_string(), json!(system));
                }
                if !resource_object.is_empty() {
                    map.insert("resources".to_string(), Value::Object(resource_object));
                }
            }

            if let Some(note) = &self.note {
                map.insert("note".to_string(), json!(note));
            }
        }

        object
    }
}

/// A fixture record: what was prepared, and what it held.
pub struct FixtureRecord {
    pub name: String,
    pub parameters: Value,
    pub preparation_time_s: f64,
}

impl FixtureRecord {
    pub fn to_json(&self) -> Value {
        json!({
            "type": "fixture",
            "name": self.name,
            "parameters": self.parameters,
            "preparation_time_s": self.preparation_time_s,
        })
    }
}

/// A skip record: a measurement that was not taken, and why.
pub struct SkipRecord {
    pub name: String,
    pub reason: String,
    pub parameters: Value,
}

impl SkipRecord {
    pub fn to_json(&self) -> Value {
        json!({
            "type": "skip",
            "name": self.name,
            "reason": self.reason,
            "parameters": self.parameters,
        })
    }
}

/// Emit a section begin marker.
pub fn emit_begin(parameters: &WorkloadParameters) {
    println!(
        "{}",
        json!({
            "type": "begin",
            "section": parameters.case_name,
        })
    );
}

/// Emit a trial result.
pub fn emit_trial_result(
    metadata: &BenchmarkMetadata,
    parameters: &WorkloadParameters,
    concurrency: usize,
    trial: usize,
    timing_window: std::time::Duration,
    durations: Vec<std::time::Duration>,
    outcomes: crate::benchmark::OutcomeCounts,
) {
    let samples = Samples::from_durations(durations);
    let mut parameters = parameters.clone().with_trial(trial);
    parameters.concurrency = concurrency;
    let record = MeasurementRecord {
        name: format!("{}.trial", parameters.case_name),
        parameters,
        metadata: metadata.clone(),
        samples,
        outcomes,
        resources: None,
        timing_window,
        warm_up_iterations: 0,
        note: Some(format!("trial {}", trial)),
    };
    Record::Measurement(Box::new(record)).emit();
}

/// Emit a section complete marker.
pub fn emit_section_complete(parameters: &WorkloadParameters) {
    println!(
        "{}",
        json!({
            "type": "complete",
            "section": parameters.case_name,
        })
    );
}
