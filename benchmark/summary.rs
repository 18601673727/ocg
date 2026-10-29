//! The summary a reader of the results needs.
//!
//! The summary reads the JSONL a run wrote and prints the table the results
//! support. It takes no measurement of its own: every figure in the table
//! comes from a record the run emitted, and a metric with no record is
//! reported as missing rather than filled in.

use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;

/// The metrics the table has a row for.
pub const METRICS: [(&str, &str); 8] = [
    ("startup", "Startup time"),
    ("memory", "Memory footprint"),
    ("admission", "Job admission latency"),
    ("throughput", "Job throughput"),
    ("streaming", "Streaming delivery latency"),
    ("history", "History read latency"),
    ("search", "Workspace search latency"),
    ("database", "Database footprint"),
];

/// One measurement the summary found.
#[derive(Clone, Debug)]
pub struct Found {
    pub name: String,
    pub trial: Option<usize>,
    pub concurrency: Option<usize>,
    pub p50_ms: Option<f64>,
    pub p95_ms: Option<f64>,
    pub p99_ms: Option<f64>,
    pub max_ms: Option<f64>,
    pub throughput_per_second: Option<f64>,
    pub bytes: Option<f64>,
    pub samples: usize,
    pub trials: usize,
    pub scope: Option<String>,
    pub note: Option<String>,
}

/// The records of one run, read from its JSONL.
#[derive(Default)]
pub struct Summary {
    measurements: Vec<Found>,
    environment: Option<Value>,
    skipped: BTreeMap<String, String>,
}

/// The measurement names each metric is read from.
///
/// The names are the ones the measurements report under, so the summary
/// does not have to guess which of them is the metric a row wants.
const METRIC_NAMES: [(&str, &[&str]); 8] = [
    ("startup", &["startup.ready"]),
    ("memory", &["memory.footprint"]),
    ("admission", &["admission.latency"]),
    ("throughput", &["throughput.jobs"]),
    ("streaming", &["streaming.delivery"]),
    ("history", &["history.read"]),
    ("search", &["search.latency"]),
    ("database", &["database.footprint"]),
];

impl Summary {
    /// Reads the JSONL of a run.
    pub fn read(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        Ok(Self::from_lines(content.lines()))
    }

    /// Reads the JSONL a run wrote to standard output.
    pub fn from_lines<'a>(lines: impl Iterator<Item = &'a str>) -> Self {
        let mut summary = Summary::default();
        for line in lines {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let Ok(value) = serde_json::from_str::<Value>(trimmed) else {
                continue;
            };
            match value["type"].as_str() {
                Some("environment") => summary.environment = Some(value),
                Some("measurement") => summary.measurements.push(found(&value)),
                Some("skip") => {
                    if let Some(name) = value["name"].as_str() {
                        let reason = value["reason"].as_str().unwrap_or("no reason given");
                        summary.skipped.insert(name.to_owned(), reason.to_owned());
                    }
                }
                _ => {}
            }
        }
        summary
    }

    /// The measurements of one metric, at the reference concurrency.
    fn metric(&self, metric: &str, reference: usize) -> Vec<&Found> {
        let names = METRIC_NAMES
            .iter()
            .find(|(key, _)| *key == metric)
            .map(|(_, names)| *names)
            .unwrap_or(&[]);
        let mut found: Vec<&Found> = self
            .measurements
            .iter()
            .filter(|measurement| names.contains(&measurement.name.as_str()))
            .filter(|measurement| match measurement.concurrency {
                Some(concurrency) => concurrency == reference,
                None => true,
            })
            .collect();
        // The reference concurrency first, then the trials in order.
        found.sort_by(|left, right| {
            left.trial
                .unwrap_or(0)
                .cmp(&right.trial.unwrap_or(0))
                .then(left.name.cmp(&right.name))
        });
        found
    }

    /// The table, as Markdown.
    pub fn to_markdown(&self, reference: usize) -> String {
        let mut table = String::new();
        table.push_str("| Metric | Value | p50 | p95 | Unit | Notes |\n");
        table.push_str("| --- | --- | --- | --- | --- | --- |\n");

        for (key, title) in METRICS {
            let measurements = self.metric(key, reference);
            if measurements.is_empty() {
                let reason = self
                    .skipped
                    .get(key)
                    .cloned()
                    .unwrap_or_else(|| "not measured".to_owned());
                table.push_str(&format!("| {title} | – | – | – | – | {reason} |\n"));
                continue;
            }

            let trials = measurements.len();
            let p50 = median_of(
                &measurements
                    .iter()
                    .filter_map(|measurement| measurement.p50_ms)
                    .collect::<Vec<_>>(),
            );
            let p95 = median_of(
                &measurements
                    .iter()
                    .filter_map(|measurement| measurement.p95_ms)
                    .collect::<Vec<_>>(),
            );
            let value = match key {
                "throughput" => median_of(
                    &measurements
                        .iter()
                        .filter_map(|measurement| measurement.throughput_per_second)
                        .collect::<Vec<_>>(),
                ),
                "memory" | "database" => median_of(
                    &measurements
                        .iter()
                        .filter_map(|measurement| measurement.bytes)
                        .collect::<Vec<_>>(),
                ),
                _ => p50,
            };
            let unit = match key {
                "throughput" => "jobs/s",
                "memory" | "database" => "MiB",
                _ => "ms",
            };
            let notes = notes_for(&measurements);

            table.push_str(&format!(
                "| {title} | {} | {} | {} | {unit} | {notes} |\n",
                format_value(value, key),
                format_value(p50, key),
                format_value(p95, key),
            ));
            let _ = trials;
        }

        table
    }

    /// The environment the run was taken in, as a line of text.
    pub fn environment_line(&self) -> String {
        let Some(environment) = &self.environment else {
            return "no environment record".to_owned();
        };
        let field = |name: &str| {
            environment
                .get(name)
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_owned()
        };
        format!(
            "commit {} ({} tree), {} on {}/{}, {} — {}/s",
            field("commit"),
            if environment["dirty_tree"].as_bool().unwrap_or(false) {
                "dirty"
            } else {
                "clean"
            },
            field("compiler"),
            field("os"),
            field("arch"),
            field("cpu_model"),
            environment["available_parallelism"]
                .as_u64()
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_owned()),
        )
    }
}

/// The notes a row carries.
fn notes_for(measurements: &[&Found]) -> String {
    let trials = measurements.len();
    let scope = measurements
        .iter()
        .find_map(|measurement| measurement.scope.clone())
        .unwrap_or_default();
    let mut notes = Vec::new();
    if trials > 1 {
        notes.push(format!("{trials} trials"));
    }
    if !scope.is_empty() {
        notes.push(scope);
    }
    if let Some(note) = measurements
        .iter()
        .find_map(|measurement| measurement.note.clone())
    {
        notes.push(note);
    }
    notes.join("; ")
}

/// The value of a cell.
fn format_value(value: Option<f64>, key: &str) -> String {
    let Some(value) = value else {
        return "–".to_owned();
    };
    match key {
        "memory" | "database" => format!("{:.1}", value / (1024.0 * 1024.0)),
        "throughput" => format!("{value:.1}"),
        _ => format!("{value:.3}"),
    }
}

/// The median of the values of the trials.
fn median_of(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    let middle = sorted.len() / 2;
    Some(if sorted.len().is_multiple_of(2) {
        (sorted[middle - 1] + sorted[middle]) / 2.0
    } else {
        sorted[middle]
    })
}

/// Reads one measurement record.
fn found(value: &Value) -> Found {
    let number = |name: &str| value.get(name).and_then(Value::as_f64);
    let parameters = value.get("parameters");
    Found {
        name: value["name"].as_str().unwrap_or_default().to_owned(),
        trial: parameters
            .and_then(|parameters| parameters.get("trial"))
            .and_then(Value::as_u64)
            .map(|value| value as usize),
        concurrency: parameters
            .and_then(|parameters| parameters.get("concurrency"))
            .and_then(Value::as_u64)
            .map(|value| value as usize),
        p50_ms: number("p50_ms"),
        p95_ms: number("p95_ms"),
        p99_ms: number("p99_ms"),
        max_ms: number("max_ms"),
        throughput_per_second: number("throughput_per_second"),
        bytes: parameters
            .and_then(|parameters| parameters.get("bytes"))
            .and_then(Value::as_f64)
            .or_else(|| number("bytes")),
        samples: value["samples"].as_u64().unwrap_or(0) as usize,
        trials: 1,
        scope: parameters
            .and_then(|parameters| parameters.get("scope"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        note: value["note"].as_str().map(str::to_owned),
    }
}

/// Read JSONL from stdin and emit Markdown table to stdout.
pub fn run_summary() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::io::{self, Read};
    let mut buffer = String::new();
    io::stdin().read_to_string(&mut buffer)?;
    let summary = Summary::from_lines(buffer.lines());
    println!("{}", summary.to_markdown(8));
    Ok(())
}
