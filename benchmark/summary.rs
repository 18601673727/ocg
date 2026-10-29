use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

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

const METRIC_NAMES: [(&str, &str); 8] = [
    ("startup", "startup.ready"),
    ("memory", "memory.footprint"),
    ("admission", "admission.latency"),
    ("throughput", "throughput.jobs"),
    ("streaming", "streaming.delivery"),
    ("history", "history.read"),
    ("search", "search.latency"),
    ("database", "database.footprint"),
];

#[derive(Clone, Debug)]
pub struct Found {
    pub name: String,
    pub status: String,
    pub trial: usize,
    pub run_id: String,
    pub mode: String,
    pub concurrency: usize,
    pub p50_ms: Option<f64>,
    pub p95_ms: Option<f64>,
    pub throughput_per_second: Option<f64>,
    pub bytes: Option<f64>,
    pub idle_rss_bytes: Option<f64>,
    pub growth_kib_per_job: Option<f64>,
    pub samples: usize,
    pub scope: Option<String>,
    pub note: Option<String>,
    pub parameters: Value,
    pub environment: Value,
    pub outcomes: Value,
}

#[derive(Default)]
pub struct Summary {
    measurements: Vec<Found>,
    environment: Option<Value>,
    skipped: BTreeMap<String, (String, String)>,
}

impl Summary {
    pub fn read(path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let content = std::fs::read_to_string(path)?;
        Self::from_lines(content.lines())
    }

    pub fn from_lines<'a>(
        lines: impl Iterator<Item = &'a str>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut summary = Summary::default();
        for (index, line) in lines.enumerate() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let value: Value = serde_json::from_str(trimmed)
                .map_err(|error| format!("malformed JSONL at line {}: {error}", index + 1))?;
            match value["type"].as_str() {
                Some("environment") => summary.environment = Some(value),
                Some("measurement") => {
                    if METRIC_NAMES.iter().any(|(_, name)| value["name"] == *name) {
                        summary.measurements.push(found(&value)?);
                    }
                }
                Some("skip") => {
                    if let Some((_, name)) = METRIC_NAMES
                        .iter()
                        .find(|(key, name)| value["name"] == *name || value["name"] == *key)
                    {
                        let reason = value["reason"]
                            .as_str()
                            .ok_or("skip record omitted its reason")?;
                        summary.skipped.insert(
                            (*name).to_owned(),
                            ("skipped".to_owned(), reason.to_owned()),
                        );
                    }
                }
                Some("measurement_unavailable") => {
                    if let Some((_, name)) = METRIC_NAMES
                        .iter()
                        .find(|(_, name)| value["name"] == *name || value["metric"] == *name)
                    {
                        let reason = value["reason"]
                            .as_str()
                            .ok_or("unavailable record omitted its reason")?;
                        summary.skipped.insert(
                            (*name).to_owned(),
                            ("unsupported".to_owned(), reason.to_owned()),
                        );
                    }
                }
                _ => {}
            }
        }
        Ok(summary)
    }

    pub fn to_markdown(
        &self,
        concurrency: usize,
        mode: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let mut table = String::new();
        table.push_str("| Metric | Value | p50 | p95 | Unit | Status | Notes |\n");
        table.push_str("| --- | --- | --- | --- | --- | --- | --- |\n");
        let mut selected_environments = BTreeSet::new();

        for (key, title) in METRICS {
            let name = METRIC_NAMES
                .iter()
                .find(|(metric, _)| *metric == key)
                .map(|(_, name)| *name)
                .ok_or("unknown summary metric")?;
            let all = self
                .measurements
                .iter()
                .filter(|measurement| measurement.name == name)
                .collect::<Vec<_>>();
            let mut selected = all
                .iter()
                .copied()
                .filter(|measurement| {
                    measurement.concurrency == concurrency && measurement.mode == mode
                })
                .collect::<Vec<_>>();
            selected.sort_by(|left, right| {
                left.run_id
                    .cmp(&right.run_id)
                    .then(left.trial.cmp(&right.trial))
            });
            if selected.is_empty() {
                if let Some((status, reason)) = self.skipped.get(name) {
                    table.push_str(&format!(
                        "| {title} | – | – | – | – | {status} | {reason} |\n"
                    ));
                    continue;
                }
                let notes = if all.is_empty() {
                    "not measured".to_owned()
                } else {
                    let available = all
                        .iter()
                        .map(|measurement| {
                            format!(
                                "{} / concurrency {}",
                                measurement.mode, measurement.concurrency
                            )
                        })
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("unselected; available: {available}")
                };
                let status = if all.is_empty() {
                    "missing"
                } else {
                    "unselected"
                };
                table.push_str(&format!(
                    "| {title} | – | – | – | – | {status} | {notes} |\n"
                ));
                continue;
            }
            if let Some(failed) = selected.iter().find(|measurement| {
                matches!(
                    measurement.status.as_str(),
                    "failed" | "unsupported" | "skipped"
                )
            }) {
                let reason = failed
                    .parameters
                    .get("error")
                    .and_then(Value::as_str)
                    .or(failed.note.as_deref())
                    .unwrap_or("measurement did not succeed");
                table.push_str(&format!(
                    "| {title} | – | – | – | – | {} | {} |\n",
                    failed.status,
                    escape_cell(reason)
                ));
                continue;
            }
            if selected
                .iter()
                .any(|measurement| measurement.status != "success")
            {
                return Err(format!("unknown status in relevant {name} record").into());
            }

            let workload = signature(selected[0], false)?;
            let environment = signature(selected[0], true)?;
            selected_environments.insert(environment.clone());
            if selected
                .iter()
                .any(|measurement| signature(measurement, false).ok() != Some(workload.clone()))
            {
                return Err(format!(
                    "incompatible workloads, fixture sizes, or run parameters for {name}"
                )
                .into());
            }
            for measurement in &selected {
                if signature(measurement, true)? != environment {
                    return Err(format!("incompatible environments for {name}").into());
                }
                validate_success(measurement, key)?;
            }

            let trials = selected.len();
            let runs = selected
                .iter()
                .map(|measurement| measurement.run_id.clone())
                .collect::<BTreeSet<_>>()
                .len();
            let samples = selected
                .iter()
                .map(|measurement| measurement.samples)
                .sum::<usize>();
            let p50 = median_of(
                &selected
                    .iter()
                    .filter_map(|measurement| measurement.p50_ms)
                    .collect::<Vec<_>>(),
            );
            let p95 = median_of(
                &selected
                    .iter()
                    .filter_map(|measurement| measurement.p95_ms)
                    .collect::<Vec<_>>(),
            );
            let value = match key {
                "throughput" => median_of(
                    &selected
                        .iter()
                        .filter_map(|measurement| measurement.throughput_per_second)
                        .collect::<Vec<_>>(),
                ),
                "memory" | "database" => median_of(
                    &selected
                        .iter()
                        .filter_map(|measurement| measurement.bytes)
                        .collect::<Vec<_>>(),
                ),
                _ => p50,
            };
            let unit = match key {
                "throughput" => "jobs/s (latency ms)",
                "memory" | "database" => "MiB",
                _ => "ms",
            };
            let mut notes = vec![
                mode.to_string(),
                format!("{trials} trials/{runs} runs/{samples} samples"),
                "median of per-trial values/p50".to_owned(),
            ];
            if p95.is_some() {
                notes.push("p95 is median of per-trial p95".to_owned());
            } else {
                notes.push("p95 suppressed/unavailable".to_owned());
            }
            if let Some(scope) = selected[0].scope.as_deref() {
                notes.push(scope.to_owned());
            }
            if key == "database" {
                let growth = median_of(
                    &selected
                        .iter()
                        .filter_map(|measurement| measurement.growth_kib_per_job)
                        .collect::<Vec<_>>(),
                );
                notes.push(format!("growth {} KiB/job", format_number(growth, 3)));
            }
            if key == "memory" {
                let idle = median_of(
                    &selected
                        .iter()
                        .filter_map(|measurement| measurement.idle_rss_bytes)
                        .collect::<Vec<_>>(),
                );
                notes.push(format!(
                    "idle RSS {} MiB; value is sampled loaded peak",
                    format_scaled(idle, "memory")
                ));
            }
            if key == "startup" {
                let durations = selected
                    .iter()
                    .filter_map(|measurement| measurement.p50_ms)
                    .collect::<Vec<_>>();
                if let (Some(min), Some(max)) = (
                    durations.iter().copied().reduce(f64::min),
                    durations.iter().copied().reduce(f64::max),
                ) {
                    notes.push(format!("trial range {min:.3}–{max:.3} ms"));
                }
            }
            if let Some(note) = selected[0].note.as_deref() {
                notes.push(note.to_owned());
            }
            table.push_str(&format!(
                "| {title} | {} | {} | {} | {unit} | success | {} |\n",
                format_scaled(value, key),
                format_scaled(p50, "latency"),
                format_scaled(p95, "latency"),
                escape_cell(&notes.join("; "))
            ));
        }
        if selected_environments.len() > 1 {
            return Err("selected rows come from incompatible environments".into());
        }
        Ok(table)
    }

    pub fn environment_line(&self) -> String {
        let environment = self
            .measurements
            .first()
            .map(|measurement| &measurement.environment)
            .or(self.environment.as_ref());
        let Some(environment) = environment else {
            return "no environment record".to_owned();
        };
        let field = |name: &str| {
            environment
                .get(name)
                .and_then(Value::as_str)
                .unwrap_or("unknown")
        };
        format!(
            "commit {} ({} tree), {} / {}, {} on {}/{}, parallelism {}",
            field("commit"),
            if environment["dirty_tree"].as_bool().unwrap_or(false) {
                "dirty"
            } else {
                "clean"
            },
            field("compiler"),
            field("build_profile"),
            field("cpu_model"),
            field("os"),
            field("arch"),
            environment["available_parallelism"]
                .as_u64()
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_owned()),
        )
    }
}

fn validate_success(
    measurement: &Found,
    key: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let outcomes = &measurement.outcomes;
    for field in [
        "attempted",
        "successful",
        "rejected",
        "failed",
        "timed_out",
        "unfinished",
    ] {
        if !outcomes[field].is_u64() {
            return Err(format!("{} omitted integer outcomes.{field}", measurement.name).into());
        }
    }
    let attempted = outcomes["attempted"].as_u64().unwrap_or(0);
    let accounted = [
        "successful",
        "rejected",
        "failed",
        "timed_out",
        "unfinished",
    ]
    .iter()
    .map(|field| outcomes[*field].as_u64().unwrap_or(0))
    .sum::<u64>();
    if attempted == 0
        || outcomes["successful"] == 0
        || attempted != accounted
        || outcomes["rejected"] != 0
        || outcomes["failed"] != 0
        || outcomes["timed_out"] != 0
        || outcomes["unfinished"] != 0
    {
        return Err(format!(
            "successful {} has invalid outcomes: {outcomes}",
            measurement.name
        )
        .into());
    }
    if measurement.samples == 0 {
        return Err(format!("successful {} has no samples", measurement.name).into());
    }
    if matches!(key, "memory" | "database") && measurement.bytes.is_none() {
        return Err(format!("successful {} omitted bytes", measurement.name).into());
    }
    if key == "memory" && measurement.idle_rss_bytes.is_none() {
        return Err("successful memory.footprint omitted idle RSS".into());
    }
    if key == "database" && measurement.growth_kib_per_job.is_none() {
        return Err("successful database.footprint omitted KiB/job growth".into());
    }
    if key == "throughput" && measurement.throughput_per_second.is_none() {
        return Err("successful throughput.jobs omitted jobs/s".into());
    }
    if !matches!(key, "memory" | "database") && measurement.p50_ms.is_none() {
        return Err(format!("successful {} omitted latency p50", measurement.name).into());
    }
    Ok(())
}

fn signature(
    measurement: &Found,
    environment: bool,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let mut value = if environment {
        measurement.environment.clone()
    } else {
        measurement.parameters.clone()
    };
    let object = value
        .as_object_mut()
        .ok_or("record signature is not an object")?;
    for key in ["storage_path", "run_id", "trial"] {
        object.remove(key);
    }
    serde_json::to_string(&value).map_err(Into::into)
}

fn found(value: &Value) -> Result<Found, Box<dyn std::error::Error + Send + Sync>> {
    let name = value["name"].as_str().ok_or("measurement omitted name")?;
    let status = value["status"]
        .as_str()
        .ok_or_else(|| format!("{name} omitted status"))?;
    if !matches!(status, "success" | "failed" | "unsupported" | "skipped") {
        return Err(format!("{name} has invalid status {status}").into());
    }
    let outcomes = value
        .get("outcomes")
        .filter(|value| value.is_object())
        .ok_or_else(|| format!("{name} omitted outcomes"))?;
    let count = |field: &str| {
        outcomes[field]
            .as_u64()
            .ok_or_else(|| format!("{name} omitted integer outcomes.{field}"))
    };
    let attempted = count("attempted")?;
    let accounted = [
        "successful",
        "rejected",
        "failed",
        "timed_out",
        "unfinished",
    ]
    .iter()
    .try_fold(0u64, |sum, field| {
        sum.checked_add(count(field)?)
            .ok_or_else(|| format!("{name} outcome count overflow"))
    })?;
    if attempted != accounted {
        return Err(format!("{name} outcome counts do not reconcile: {outcomes}").into());
    }
    let parameters = value
        .get("parameters")
        .filter(|value| value.is_object())
        .ok_or_else(|| format!("{name} omitted parameters"))?;
    let environment = value
        .get("environment")
        .filter(|value| value.is_object())
        .ok_or_else(|| format!("{name} omitted environment"))?;
    let required_u64 = |field: &str| {
        parameters[field]
            .as_u64()
            .map(|value| value as usize)
            .ok_or_else(|| -> Box<dyn std::error::Error + Send + Sync> {
                format!("{name} omitted integer parameters.{field}").into()
            })
    };
    let trial = required_u64("trial")?;
    let concurrency = required_u64("concurrency")?;
    let run_id = parameters["run_id"]
        .as_str()
        .ok_or_else(|| format!("{name} omitted parameters.run_id"))?
        .to_owned();
    let mode = parameters["mode"]
        .as_str()
        .ok_or_else(|| format!("{name} omitted parameters.mode"))?
        .to_owned();
    if !matches!(mode.as_str(), "smoke" | "reference") {
        return Err(format!("{name} has invalid mode {mode}").into());
    }
    for field in [
        "p50_ms",
        "p95_ms",
        "throughput_per_second",
        "bytes",
        "idle_rss_bytes",
        "growth_kib_per_completed_job",
    ] {
        if let Some(number) = value.get(field)
            && !number.is_null()
            && number.as_f64().is_none()
        {
            return Err(format!("{name} has invalid numeric {field}").into());
        }
    }
    let number = |field: &str| value.get(field).and_then(Value::as_f64);
    let samples = value["samples"]
        .as_u64()
        .ok_or_else(|| format!("{name} omitted integer samples"))? as usize;
    Ok(Found {
        name: name.to_owned(),
        status: status.to_owned(),
        trial,
        run_id,
        mode,
        concurrency,
        p50_ms: number("p50_ms"),
        p95_ms: number("p95_ms"),
        throughput_per_second: number("throughput_per_second"),
        bytes: number("bytes"),
        idle_rss_bytes: number("idle_rss_bytes"),
        growth_kib_per_job: number("growth_kib_per_completed_job"),
        samples,
        scope: parameters["scope"].as_str().map(str::to_owned),
        note: value["note"].as_str().map(str::to_owned),
        parameters: parameters.clone(),
        environment: environment.clone(),
        outcomes: value["outcomes"].clone(),
    })
}

fn median_of(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.total_cmp(right));
    let middle = sorted.len() / 2;
    Some(if sorted.len().is_multiple_of(2) {
        (sorted[middle - 1] + sorted[middle]) / 2.0
    } else {
        sorted[middle]
    })
}

fn format_scaled(value: Option<f64>, key: &str) -> String {
    match key {
        "memory" | "database" => format_number(value.map(|value| value / 1_048_576.0), 2),
        "throughput" => format_number(value, 3),
        _ => format_number(value, 3),
    }
}

fn format_number(value: Option<f64>, precision: usize) -> String {
    value.map_or_else(|| "–".to_owned(), |value| format!("{value:.precision$}"))
}

fn escape_cell(text: &str) -> String {
    text.replace('|', "\\|").replace('\n', " ")
}

pub fn run_summary() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::io::{self, Read};
    let mut concurrency = 8;
    let mut mode = "smoke".to_owned();
    let mut arguments = std::env::args().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "summary" | "--bench" => {}
            "--concurrency" => {
                concurrency = arguments
                    .next()
                    .ok_or("missing --concurrency value")?
                    .parse()?;
            }
            "--mode" => {
                mode = arguments.next().ok_or("missing --mode value")?;
                if !matches!(mode.as_str(), "smoke" | "reference") {
                    return Err("--mode must be smoke or reference".into());
                }
            }
            _ => return Err(format!("unknown summary argument: {argument}").into()),
        }
    }
    let mut buffer = String::new();
    io::stdin().read_to_string(&mut buffer)?;
    let summary = Summary::from_lines(buffer.lines())?;
    print!("{}", summary.to_markdown(concurrency, &mode)?);
    println!("\nEnvironment: {}", summary.environment_line());
    Ok(())
}
