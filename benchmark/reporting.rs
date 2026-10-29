use serde_json::{Value, json};
use std::time::Duration;

use crate::benchmark::{OutcomeCounts, ResourceUsage, Samples};
use crate::metadata::{BenchmarkMetadata, WorkloadParameters};

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

    pub fn emit(&self) {
        println!("{}", self.to_json());
    }
}

pub struct EnvironmentRecord {
    pub metadata: BenchmarkMetadata,
    pub workload_model: String,
    pub scope: String,
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

pub struct MeasurementRecord {
    pub name: String,
    pub parameters: WorkloadParameters,
    pub metadata: BenchmarkMetadata,
    pub samples: Samples,
    pub sample_count: Option<usize>,
    pub outcomes: OutcomeCounts,
    pub resources: Option<ResourceUsage>,
    pub timing_window: Duration,
    pub warm_up_iterations: usize,
    pub note: Option<String>,
    pub status: String,
    pub values: Value,
}

impl MeasurementRecord {
    pub fn to_json(&self) -> Value {
        let sample_count = self.sample_count.unwrap_or(self.samples.count());
        let parameters = self.parameters.to_json();
        let mode = parameters["mode"].as_str().unwrap_or("smoke");
        let mut object = json!({
            "type": "measurement",
            "name": self.name,
            "status": self.status,
            "parameters": parameters,
            "environment": self.metadata.to_json(),
            "samples": sample_count,
            "latency_samples": self.samples.count(),
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
                if mode == "reference" && self.samples.count() >= 100 {
                    map.insert(
                        "p95_ms".to_string(),
                        json!(self.samples.p95().as_secs_f64() * 1000.0),
                    );
                }
                if mode == "reference" && self.samples.count() >= 1_000 {
                    map.insert(
                        "p99_ms".to_string(),
                        json!(self.samples.p99().as_secs_f64() * 1000.0),
                    );
                }
                map.insert(
                    "min_ms".to_string(),
                    json!(self.samples.min().as_secs_f64() * 1000.0),
                );
                map.insert(
                    "max_ms".to_string(),
                    json!(self.samples.max().as_secs_f64() * 1000.0),
                );
                if let Some(mean) = self.samples.mean() {
                    map.insert("mean_ms".to_string(), json!(mean.as_secs_f64() * 1000.0));
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
            if let Some(values) = self.values.as_object() {
                for (key, value) in values {
                    map.insert(key.clone(), value.clone());
                }
            }
        }
        object
    }
}

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

pub fn emit_begin(parameters: &WorkloadParameters) {
    println!(
        "{}",
        json!({"type": "begin", "section": parameters.case_name, "parameters": parameters.to_json()})
    );
}

pub fn canonical_metric(section: &str) -> &str {
    match section {
        "startup" => "startup.ready",
        "memory" => "memory.footprint",
        "admission" => "admission.latency",
        "throughput" => "throughput.jobs",
        "streaming" => "streaming.delivery",
        "history" => "history.read",
        "search" => "search.latency",
        "database" => "database.footprint",
        _ => "unknown.measurement",
    }
}

pub fn emit_trial_result(
    metadata: &BenchmarkMetadata,
    parameters: &WorkloadParameters,
    concurrency: usize,
    trial: usize,
    timing_window: Duration,
    durations: Vec<Duration>,
    outcomes: OutcomeCounts,
) {
    let samples = Samples::from_durations(durations);
    let mut parameters = parameters.clone().with_trial(trial);
    parameters.concurrency = concurrency;
    let record = MeasurementRecord {
        name: canonical_metric(&parameters.case_name).to_owned(),
        parameters,
        metadata: metadata.clone(),
        samples,
        sample_count: None,
        outcomes,
        resources: None,
        timing_window,
        warm_up_iterations: 0,
        note: None,
        status: "success".into(),
        values: Value::Object(serde_json::Map::new()),
    };
    Record::Measurement(Box::new(record)).emit();
}

pub fn emit_section_complete(parameters: &WorkloadParameters) {
    println!(
        "{}",
        json!({"type": "complete", "section": parameters.case_name, "status": "success"})
    );
}
