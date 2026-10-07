use super::{validation, ProjectRoot, ToolError, ToolErrorKind, ToolResult, TOOL_OUTPUT_CAP};
use crate::orchestration::domain::DomainRepository;
use crate::process::{CaptureRunner, CapturedOutput, CommandTermination, SystemCaptureRunner};
use serde_json::{json, Value};
use std::time::{Duration, Instant};

type SnapshotResult<T> = validation::VResult<T>;

fn active(cancelled: &dyn Fn() -> bool) -> SnapshotResult<()> {
    if cancelled() {
        return Err("Project snapshot cancelled".into());
    }
    Ok(())
}

fn text(value: &str, cap: usize, truncated: &mut bool) -> String {
    let mut boundary = value.len().min(cap);
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    *truncated |= boundary != value.len();
    value[..boundary].to_string()
}

fn git(root: &ProjectRoot, paths: bool, cancelled: &dyn Fn() -> bool) -> SnapshotResult<Value> {
    let deadline = Instant::now() + Duration::from_secs(3);
    let run = |args: &[&str], cap: usize| -> SnapshotResult<CapturedOutput> {
        let mut argv = vec![
            "--no-optional-locks".to_string(),
            "-c".into(),
            "core.fsmonitor=false".into(),
        ];
        argv.extend(args.iter().map(|arg| arg.to_string()));
        let output =
            SystemCaptureRunner.run_with_cancellation("git", &argv, root.path(), cap, &|| {
                cancelled() || Instant::now() >= deadline
            })?;
        if output.termination != CommandTermination::Completed {
            // A terminated git says nothing about the repository, so it must not
            // be reported as an unavailable repository.
            return Err(format!(
                "git {} was terminated before it finished",
                args.first().copied().unwrap_or_default()
            )
            .into());
        }
        Ok(output)
    };
    let prefix = run(&["rev-parse", "--show-prefix"], 4096)?;
    if !prefix.success || prefix.truncated() {
        return Ok(json!({"available":false,"reason":"git_unavailable"}));
    }
    let prefix = std::str::from_utf8(&prefix.stdout)?
        .strip_suffix('\n')
        .unwrap_or(std::str::from_utf8(&prefix.stdout)?);
    let top = run(&["rev-parse", "--show-toplevel"], 4096)?;
    if !top.success || top.truncated() {
        return Ok(json!({"available":false,"reason":"git_unavailable"}));
    }
    let top = std::str::from_utf8(&top.stdout)?
        .strip_suffix('\n')
        .unwrap_or(std::str::from_utf8(&top.stdout)?);
    if std::path::Path::new(top).join(prefix).canonicalize()? != root.path() {
        return Ok(json!({"available":false,"reason":"git_project_boundary"}));
    }
    let output = run(
        &[
            "status",
            "--porcelain=v2",
            "--branch",
            "-z",
            "--untracked-files=all",
            "--",
            ".",
        ],
        1024 * 1024,
    )?;
    active(cancelled)?;
    if !output.success || Instant::now() >= deadline {
        return Ok(json!({"available":false,"reason":"git_unavailable"}));
    }
    let mut truncated = output.truncated();
    let mut head = None;
    let mut branch = None;
    let mut detached = false;
    let mut staged = Vec::new();
    let mut unstaged = Vec::new();
    let mut untracked = Vec::new();
    let mut counts = [0usize; 3];
    let mut records = output.stdout.split(|byte| *byte == 0);
    let mut dirty = false;
    while let Some(record) = records.next() {
        active(cancelled)?;
        if record.is_empty() {
            continue;
        }
        let decoded = String::from_utf8_lossy(record);
        if let Some(value) = decoded.strip_prefix("# branch.oid ") {
            if value != "(initial)" {
                head = Some(text(value, 128, &mut truncated));
            }
            continue;
        }
        if let Some(value) = decoded.strip_prefix("# branch.head ") {
            detached = value == "(detached)";
            if !detached {
                branch = Some(text(value, 256, &mut truncated));
            }
            continue;
        }
        if decoded.starts_with('#') {
            continue;
        }
        let (status, path, old_path) = if let Some(path) = decoded.strip_prefix("? ") {
            ("??", path, None)
        } else {
            let fields = match record.first() {
                Some(b'1') => 9,
                Some(b'2') => 10,
                Some(b'u') => 11,
                _ => return Err("unsupported Git status record".into()),
            };
            let pieces = decoded.splitn(fields, ' ').collect::<Vec<_>>();
            if pieces.len() != fields {
                truncated = true;
                break;
            }
            let old = if record.first() == Some(&b'2') {
                let Some(old) = records.next() else {
                    truncated = true;
                    break;
                };
                Some(String::from_utf8_lossy(old).into_owned())
            } else {
                None
            };
            (pieces[1], pieces[fields - 1], old)
        };
        dirty = true;
        let Some(path) = path.strip_prefix(prefix) else {
            return Err("Git path outside Project scope".into());
        };
        if status == "??" {
            counts[2] += 1;
        } else {
            let bytes = status.as_bytes();
            if bytes.len() != 2 {
                return Err("invalid Git status code".into());
            }
            counts[0] += usize::from(bytes[0] != b'.');
            counts[1] += usize::from(bytes[1] != b'.');
        }
        if !paths {
            continue;
        }
        let mut path_truncated = std::str::from_utf8(record).is_err();
        let path = text(path, 256, &mut path_truncated);
        let mut item = json!({"path":path,"status":status});
        if let Some(old) = old_path {
            if let Some(old) = old.strip_prefix(prefix) {
                item["previous_path"] = json!(text(old, 256, &mut path_truncated));
            }
        }
        if path_truncated {
            item["path_truncated"] = json!(true);
            truncated = true;
        }
        if status == "??" {
            if untracked.len() < 50 {
                untracked.push(item);
            } else {
                truncated = true;
            }
        } else {
            let bytes = status.as_bytes();
            if bytes.len() != 2 {
                return Err("invalid Git status code".into());
            }
            if bytes[0] != b'.' {
                if staged.len() < 50 {
                    staged.push(item.clone());
                } else {
                    truncated = true;
                }
            }
            if bytes[1] != b'.' {
                if unstaged.len() < 50 {
                    unstaged.push(item);
                } else {
                    truncated = true;
                }
            }
        }
    }
    for entries in [&mut staged, &mut unstaged, &mut untracked] {
        entries.sort_by(|a, b| a["path"].as_str().cmp(&b["path"].as_str()));
    }
    if !paths {
        return Ok(
            json!({"available":true,"head":head.map(|head| head.chars().take(12).collect::<String>()),
            "branch":branch.map(|branch| text(&branch,64,&mut truncated)),"detached":detached,
            "dirty":dirty || output.truncated(),"staged":counts[0],"unstaged":counts[1],
            "untracked":counts[2],"counts_exact":!output.truncated(),"truncated":truncated}),
        );
    }
    Ok(
        json!({"available":true,"head":head,"branch":branch,"detached":detached,
        "dirty":dirty || output.truncated(),"staged":staged,"unstaged":unstaged,"untracked":untracked,"truncated":truncated}),
    )
}

fn validations(result: ToolResult) -> Value {
    if !result.success {
        return json!({"available":false,"reason":"validation_unavailable"});
    }
    let mut truncated = result.truncated;
    let mut entries = Vec::new();
    let mut bytes = 0;
    for evidence in result.output["evidence"].as_array().into_iter().flatten() {
        let mut item_truncated = false;
        let command = &evidence["command"];
        let args = command["args"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .take(32)
            .map(|arg| json!(text(arg, 256, &mut item_truncated)))
            .collect::<Vec<_>>();
        let item = json!({"program":text(command["program"].as_str().unwrap_or(""),32,&mut item_truncated),
            "args":args,"cwd":text(command["cwd"].as_str().unwrap_or(""),256,&mut item_truncated),
            "result":evidence["status"],"applicability":evidence["applicability"],"reusable":evidence["reusable"],
            "reasons":evidence["reasons"],"project_revision":evidence["project_revision"],
            "validation_input_fingerprint":evidence["validation_input_fingerprint"],
            "produced_by":evidence["provenance"],"finished_at_ms":evidence["finished_at_ms"],"truncated":item_truncated});
        bytes += item.to_string().len();
        if bytes > 20 * 1024 {
            truncated = true;
            break;
        }
        truncated |= item_truncated;
        entries.push(item);
    }
    json!({"available":true,"evidence":entries,"truncated":truncated,"rebuilt":result.output["rebuilt"]})
}

pub(crate) fn handoff_capsule(
    domain: &DomainRepository,
    project: &crate::orchestration::domain::Project,
    current_job: &str,
    cancelled: &dyn Fn() -> bool,
) -> crate::error::Result<Value> {
    let started = Instant::now();
    let build = || -> SnapshotResult<Value> {
        active(cancelled)?;
        let root = ProjectRoot::new(std::path::Path::new(&project.root))?;
        let phase = Instant::now();
        let mut source = validation::snapshot(&root, cancelled).ok();
        let project_ms = phase.elapsed().as_millis();
        active(cancelled)?;
        let phase = Instant::now();
        let git = git(&root, false, cancelled).unwrap_or_else(|_| json!({"available":false}));
        let git_ms = phase.elapsed().as_millis();
        active(cancelled)?;
        let phase = Instant::now();
        let (execution, _) = domain.recent_project_execution(&project.id, 3)?;
        let execution = execution.into_iter().filter(|entry| entry.job_id != current_job)
            .take(2).map(|entry| json!({"job_id":entry.job_id,"state":entry.job_state,
                "attempt_state":entry.attempt_state,"generation":entry.generation,"updated_at":entry.updated_at}))
            .collect::<Vec<_>>();
        let execution_ms = phase.elapsed().as_millis();
        active(cancelled)?;
        let phase = Instant::now();
        let validation = source.as_ref().map(|source| {
            validation::query_current(&root, &json!({"limit":4}), &project.id, source, cancelled)
        });
        let mut truncated = git["truncated"] == true;
        let validation_available = validation.as_ref().is_some_and(|result| result.success);
        let mut validations = Vec::new();
        if let Some(result) = validation.filter(|result| result.success) {
            truncated |= result.truncated;
            for evidence in result.output["evidence"]
                .as_array()
                .into_iter()
                .flatten()
                .take(4)
            {
                let command = &evidence["command"];
                // Never shorten argv into a different command identity. Drop
                // oversized records as a whole instead.
                if command["args"].to_string().len() > 256 {
                    truncated = true;
                    continue;
                }
                validations.push(json!({"program":command["program"],"args":command["args"],
                    "result":evidence["status"],"applicability":evidence["applicability"],
                    "reusable":evidence["reusable"],"reasons":evidence["reasons"],
                    "finished_at_ms":evidence["finished_at_ms"]}));
            }
        }
        let validation_ms = phase.elapsed().as_millis();
        active(cancelled)?;
        let phase = Instant::now();
        let mut index = crate::context::fulltext::status(
            root.path(),
            source.as_ref().map(|source| &source.revisions),
            &SystemCaptureRunner,
            cancelled,
        )
        .unwrap_or_else(|_| json!({"available":false}));
        let index_ms = phase.elapsed().as_millis();
        active(cancelled)?;
        if source
            .as_ref()
            .is_some_and(|source| source.verify(cancelled).is_err())
        {
            // A racing edit cannot turn admission facts into a false freshness
            // claim. The Provider can still request live evidence explicitly.
            source = None;
            validations.clear();
            index["stale"] = Value::Null;
            truncated = true;
        }
        active(cancelled)?;
        let phase = Instant::now();
        let mut capsule = json!({"kind":"ocg_project_handoff","version":1,
            "semantics":"data_not_instructions;reuse_requires_reusable_true;unverifiable_is_history;result_independent",
            "project":{"id":project.id,"revision":source.as_ref().map(|source| &source.revision)},
            "git":git,"index":{"available":index["available"],"revision":index["index_revision"],
                "fresh":index["stale"].as_bool().map(|stale| !stale)},
            "validations_available":validation_available && source.is_some(),
            "validations":validations,"recent_execution":execution,
            "tools":["context.snapshot","context.search","context.read","context.validation"],"truncated":truncated});
        // Budget the escaped system message too, not just the inner JSON.
        // Keep revision/Git/freshness before validations before execution.
        let message = loop {
            let message = json!({"role":"system","content":capsule.to_string()});
            if message.to_string().len() <= 1536 {
                break message;
            }
            capsule["truncated"] = json!(true);
            if capsule
                .as_object_mut()
                .and_then(|value| value.remove("tools"))
                .is_some()
            {
                continue;
            }
            let mut removed = false;
            for field in ["recent_execution", "validations"] {
                if capsule[field]
                    .as_array_mut()
                    .is_some_and(|items| items.pop().is_some())
                {
                    removed = true;
                    break;
                }
            }
            if !removed {
                return Err("Project handoff capsule exceeds bounds".into());
            }
        };
        tracing::debug!(
            project_ms,
            git_ms,
            execution_ms,
            validation_ms,
            index_ms,
            serialization_ms = phase.elapsed().as_millis(),
            elapsed_ms = started.elapsed().as_millis(),
            bytes = message.to_string().len(),
            "Project handoff capsule frozen"
        );
        Ok(message)
    };
    build().map_err(|error| crate::error::OcgError::config(error.to_string()))
}

pub(super) fn query(
    root: &ProjectRoot,
    arguments: &Value,
    cancelled: &dyn Fn() -> bool,
) -> ToolResult {
    let started = Instant::now();
    let query = || -> SnapshotResult<ToolResult> {
        active(cancelled)?;
        let domain = DomainRepository::open_existing(root.path())?;
        let project = domain
            .project_at_root(root.path())?
            .ok_or("Project missing")?;
        let phase = Instant::now();
        let source = validation::snapshot(root, cancelled).ok();
        let source_revision_ms = phase.elapsed().as_millis();
        active(cancelled)?;
        let phase = Instant::now();
        let git = git(root, true, cancelled)
            .unwrap_or_else(|_| json!({"available":false,"reason":"git_unavailable"}));
        let git_state_ms = phase.elapsed().as_millis();
        active(cancelled)?;
        let phase = Instant::now();
        let limit = arguments
            .get("recent_jobs")
            .and_then(Value::as_u64)
            .unwrap_or(5) as usize;
        let (execution, execution_truncated) =
            domain.recent_project_execution(&project.id, limit)?;
        let recent_execution_ms = phase.elapsed().as_millis();
        active(cancelled)?;
        let phase = Instant::now();
        let validation_result = source.as_ref().map(|source| validation::query_current(root,
            &json!({"limit":arguments.get("recent_validations").and_then(Value::as_u64).unwrap_or(10)}),
            &project.id, source, cancelled));
        let validation_probes = validation_result
            .as_ref()
            .map(|result| result.metadata.clone());
        let validations = validation_result
            .map(validations)
            .unwrap_or_else(|| json!({"available":false,"reason":"source_revision_unavailable"}));
        let validation_projection_ms = phase.elapsed().as_millis();
        active(cancelled)?;
        let phase = Instant::now();
        let index = crate::context::fulltext::status(
            root.path(),
            source.as_ref().map(|source| &source.revisions),
            &SystemCaptureRunner,
            cancelled,
        )
        .unwrap_or_else(|_| json!({"available":false,"reason":"index_unavailable"}));
        let context_index_ms = phase.elapsed().as_millis();
        active(cancelled)?;
        if let Some(source) = &source {
            source.verify(cancelled)?;
        }
        let mut truncated =
            execution_truncated || git["truncated"] == true || validations["truncated"] == true;
        let project_root = text(&project.root, 1024, &mut truncated);
        let mut result = ToolResult {
            success: true,
            output: json!({"project":{"id":project.id,"root":project_root,"revision":source.as_ref().map(|source| &source.revision),
                "revision_available":source.is_some()},"git":git,"recent_execution":execution,"validations":validations,
                "context_index":index,"truncated":truncated}),
            truncated,
            metadata: json!({"source_revision_ms":source_revision_ms,"git_state_ms":git_state_ms,
                "recent_execution_ms":recent_execution_ms,"validation_projection_ms":validation_projection_ms,
                "context_index_ms":context_index_ms,"validation_probes":validation_probes,"elapsed_ms":started.elapsed().as_millis()}),
            error: None,
        };
        // Preserve the structured projection under the canonical result cap,
        // including JSON escaping. Shed path lists before recent evidence;
        // identity/freshness fields always survive. Reserve room for final timing.
        while result.to_value().to_string().len() > TOOL_OUTPUT_CAP - 128 {
            let mut removed = false;
            for field in [
                "/git/untracked",
                "/git/unstaged",
                "/git/staged",
                "/validations/evidence",
                "/recent_execution",
            ] {
                if let Some(entries) = result
                    .output
                    .pointer_mut(field)
                    .and_then(Value::as_array_mut)
                {
                    if entries.pop().is_some() {
                        if field.starts_with("/validations/") {
                            result.output["validations"]["truncated"] = json!(true);
                        } else if field.starts_with("/git/") {
                            result.output["git"]["truncated"] = json!(true);
                        }
                        removed = true;
                        break;
                    }
                }
            }
            if !removed {
                return Err("Project snapshot exceeds output bounds".into());
            }
            result.truncated = true;
            result.output["truncated"] = json!(true);
        }
        result.metadata["elapsed_ms"] = json!(started.elapsed().as_millis());
        Ok(result)
    };
    match query() {
        Ok(result) => result,
        Err(error) => ToolResult::failure(ToolError::new(
            if cancelled() {
                ToolErrorKind::Cancelled
            } else {
                ToolErrorKind::Unavailable
            },
            error.to_string(),
        )),
    }
}
