use super::contracts::*;
use super::*;

/// What the host can establish: it runs, as the listener answers only
/// while it does, and what the database says of itself now.
pub(super) async fn health(database: &DatabaseHandle) -> Value {
    let status = match database.status() {
        DatabaseStatus::Open => "open",
        DatabaseStatus::Protected => "protected",
        DatabaseStatus::ShuttingDown => "shutting_down",
        DatabaseStatus::WorkerStopped => "stopped",
        DatabaseStatus::Closed => "closed",
    };
    let readable = database.job(database.genesis()).await.is_ok();
    let vitals = database.vitals().await;
    let quarantined = match database.unresolved_executions().await {
        Ok(unresolved) => unresolved
            .into_iter()
            .map(|(job, execution)| WireQuarantined {
                job: job.to_string(),
                pool: execution.pool.map(|pool| pool.to_string()),
                since: execution.unresolved_at.as_unix_micros(),
            })
            .collect::<Vec<_>>(),
        Err(error) => {
            log::warn!("failed to read unresolved executions for health: {error}");
            Vec::new()
        }
    };
    envelope(WireHealth {
        host: "running".to_owned(),
        database: WireDatabaseHealth {
            status: status.to_owned(),
            accepting_commands: status == "open",
            readable,
            database_bytes: vitals.database_bytes,
            available_bytes: vitals.available_bytes,
            durable_commits: vitals.durable_commits,
            commit_us_mean: vitals.commit_us_mean,
            commit_us_max: vitals.commit_us_max,
        },
        claims: WireClaims {
            count: vitals.claims_count,
            mean_us: vitals.claim_us_mean,
            max_us: vitals.claim_us_max,
        },
        queue: WireQueue {
            pending: vitals.queue_pending,
            ready: vitals.queue_ready,
            delayed: vitals.queue_delayed,
            commands_waiting: vitals.commands_waiting,
        },
        process: WireProcess {
            rss_bytes: vitals.rss_bytes,
            peak_rss_bytes: vitals.peak_rss_bytes,
        },
        quarantined,
    })
}

/// The Job's settlement: the one outcome its record holds, or none yet; a
/// record holding more than one is not described at all.
pub(super) fn settlement_view(record: &JobRecord) -> Result<WireSettlement, Refusal> {
    let outcomes = [
        (WireSettlementState::Completed, record.completed_at),
        (WireSettlementState::Failed, record.failed_at),
        (WireSettlementState::Cancelled, record.cancelled_at),
        (WireSettlementState::Unknown, record.unknown_at),
    ]
    .into_iter()
    .filter_map(|(state, at)| at.map(|at| (state, at)))
    .collect::<Vec<_>>();
    let mut outcomes = outcomes.into_iter();
    let value = match outcomes.next() {
        None => WireSettlement {
            state: WireSettlementState::Unsettled,
            settled_at_us: None,
        },
        Some((state, at)) => WireSettlement {
            state,
            settled_at_us: Some(at.as_unix_micros()),
        },
    };
    if outcomes.next().is_some() {
        return Err(INCONSISTENT_RECORD);
    }
    Ok(value)
}

pub(super) fn job_view(record: &JobRecord) -> Result<WireJob, Refusal> {
    Ok(WireJob {
        id: record.id.to_string(),
        name: record.name.clone(),
        title: record.objective.clone(),
        is_chat: record.conversation.is_some(),
        execution_mode: "once".to_owned(),
        input: record.input.to_string(),
        previous: record.previous.map(|id| id.to_string()),
        kind: if record.kind.is_conversation() {
            WireJobKind::Conversation
        } else {
            WireJobKind::Turn
        },
        conversation: record.conversation.map(|id| id.to_string()),
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

pub(super) fn output_view(record: &JobOutputRecord) -> WireOutput {
    WireOutput {
        id: record.id.to_string(),
        seq: record.seq,
        kind: match record.kind {
            OutputKind::Progress => WireOutputKind::Progress,
            OutputKind::Evaluation => WireOutputKind::Evaluation,
            OutputKind::Result => WireOutputKind::Result,
            OutputKind::Reasoning => WireOutputKind::Reasoning,
        },
        summary: record.summary.clone(),
        code: record.code.clone(),
        goal_reached: record.goal_reached,
        value: record.value.clone().unwrap_or(Value::Null),
        created_at_us: record.created_at.as_unix_micros(),
    }
}

/// A page of the Job's outputs, exactly as stored and in `seq` order: as
/// many of the first `limit` after `after_seq` as fit the response, whole.
/// `next_after_seq` is the cursor of the next page, `after_seq` again when
/// none was returned; `has_more` says whether a later output was stored
/// when the page was read.
pub(super) async fn outputs(
    shared: &Shared,
    job: JobId,
    after_seq: Option<i64>,
    limit: usize,
) -> Result<Value, Refusal> {
    let database = &shared.database;
    if database.job(job).await?.is_none() {
        return Err(JOB_NOT_FOUND);
    }
    let records = database.job_outputs(job, after_seq, limit + 1).await?;
    let envelope = |outputs: Vec<WireOutput>, next: Option<i64>, more: bool| {
        envelope(WireOutputs {
            job: job.to_string(),
            after_seq,
            outputs,
            next_after_seq: next,
            has_more: more,
        })
    };
    let mut size = envelope(Vec::new(), Some(i64::MIN), true).to_string().len();
    let mut page = Vec::new();
    let mut next = after_seq;
    let mut more = records.len() > limit;
    for record in records.iter().take(limit) {
        let output = output_view(record);
        let cost = json!(output).to_string().len() + 1;
        if size + cost > shared.response_bytes {
            if page.is_empty() {
                return Err(OUTPUT_TOO_LARGE);
            }
            more = true;
            break;
        }
        size += cost;
        page.push(output);
        next = Some(record.seq);
    }
    Ok(envelope(page, next, more))
}

pub(super) const MAX_EXPOSED_TOOL_STRING_BYTES: usize = 2 * 1024;

pub(super) fn responses_query(query: &str) -> Result<(Option<RequestNumber>, usize), Refusal> {
    let (mut after, mut limit) = (None, None);
    if !query.is_empty() {
        for pair in query.split('&') {
            let (key, value) = pair.split_once('=').ok_or(INVALID_REQUEST)?;
            match key {
                "after" if after.is_none() => {
                    after = Some(
                        routing::decimal::<u32>(value)
                            .and_then(RequestNumber::new)
                            .ok_or(INVALID_REQUEST)?,
                    );
                }
                "limit" if limit.is_none() => {
                    limit = Some(
                        routing::decimal::<usize>(value)
                            .filter(|limit| (1..=100).contains(limit))
                            .ok_or(INVALID_REQUEST)?,
                    );
                }
                _ => return Err(INVALID_REQUEST),
            }
        }
    }
    Ok((after, limit.unwrap_or(50)))
}

/// Pages received assistant text without replaying each request's full context.
pub(super) async fn provider_responses(
    shared: &Shared,
    job: JobId,
    after: Option<RequestNumber>,
    limit: usize,
) -> Result<Value, Refusal> {
    if shared.database.job(job).await?.is_none() {
        return Err(JOB_NOT_FOUND);
    }
    let records = shared
        .database
        .provider_responses(job, after, limit + 1)
        .await?;
    let envelope = |responses: Vec<WireProviderResponse>, next_after, has_more| {
        envelope(WireProviderResponses {
            job: job.to_string(),
            responses,
            next_after,
            has_more,
        })
    };
    let mut size = envelope(Vec::new(), Some(u32::MAX), true).to_string().len();
    let mut page = Vec::new();
    let mut more = records.len() > limit;
    let mut next = None;
    for record in records.into_iter().take(limit) {
        let response = WireProviderResponse {
            request: record.request.get(),
            text: record.text,
            requests_tools: record.requests_tools,
            created_at_us: record.recorded_at.as_unix_micros(),
        };
        let cost = json!(response).to_string().len() + 1;
        if size + cost > shared.response_bytes {
            if page.is_empty() {
                return Err(OUTPUT_TOO_LARGE);
            }
            more = true;
            break;
        }
        size += cost;
        next = Some(response.request);
        page.push(response);
    }
    Ok(envelope(page, if more { next } else { None }, more))
}

pub(super) fn cut_tool_string(text: &str) -> (&str, bool) {
    if text.len() <= MAX_EXPOSED_TOOL_STRING_BYTES {
        (text, false)
    } else {
        let mut boundary = MAX_EXPOSED_TOOL_STRING_BYTES;
        while !text.is_char_boundary(boundary) {
            boundary -= 1;
        }
        (&text[..boundary], true)
    }
}

pub(super) fn parse_tool_cursor(text: &str) -> Result<(RequestNumber, u32), Refusal> {
    if let Some((req_str, idx_str)) = text.split_once(':') {
        let req = decimal::<u32>(req_str)
            .and_then(RequestNumber::new)
            .ok_or(INVALID_REQUEST)?;
        let idx = decimal::<u32>(idx_str).ok_or(INVALID_REQUEST)?;
        return Ok((req, idx));
    }
    Err(INVALID_REQUEST)
}

pub(super) fn tools_query(query: &str) -> Result<(Option<(RequestNumber, u32)>, usize), Refusal> {
    let (mut after, mut limit) = (None, None);
    if !query.is_empty() {
        for pair in query.split('&') {
            let (key, value) = pair.split_once('=').ok_or(INVALID_REQUEST)?;
            match key {
                "after" if after.is_none() => {
                    after = Some(parse_tool_cursor(value)?);
                }
                "limit" if limit.is_none() => {
                    limit = Some(
                        decimal::<usize>(value)
                            .filter(|limit| (1..=100).contains(limit))
                            .ok_or(INVALID_REQUEST)?,
                    );
                }
                _ => return Err(INVALID_REQUEST),
            }
        }
    }
    Ok((after, limit.unwrap_or(50)))
}

fn tool_view(record: &ToolEvidenceRecord) -> WireToolCall {
    let decision = match &record.decision.fact {
        ToolDecision::Admitted(_) => "admitted",
        ToolDecision::AwaitingApproval(_) => "awaiting_approval",
        ToolDecision::AwaitingAnswer(_) => "awaiting_answer",
        ToolDecision::Refused(refusal) => refusal.as_str(),
    };
    let (arguments, arguments_cut) = match record.decision.fact.stored_admission() {
        Some(admission) => {
            let (cut, truncated) = cut_tool_string(admission.arguments.as_str());
            (Some(cut), truncated)
        }
        None => (None, false),
    };
    let raw_result = match &record.outcome {
        Some(outcome) => match &outcome.fact {
            ToolOutcome::Completed { result } => Some(result.as_str()),
            ToolOutcome::CompletedWithheld { reason } => match reason {
                WithheldReason::TooLarge => Some("result withheld: too large"),
                WithheldReason::Unserializable => Some("result withheld: unserializable"),
            },
            ToolOutcome::NotApplied { message } => Some(message.as_str()),
            ToolOutcome::NotInvoked { reason } => match reason {
                NotInvokedReason::Refused(r) => Some(r.as_str()),
                NotInvokedReason::Cancelled => Some("cancelled before invocation"),
            },
            ToolOutcome::Uncertain { message, cause } => message.as_deref().or(match cause {
                UncertainCause::TimedOut => Some("timed out"),
                UncertainCause::Cancelled => Some("cancelled"),
                UncertainCause::HandlerReported => Some("handler reported failure"),
            }),
        },
        None => None,
    };
    let (result, result_cut) = match raw_result {
        Some(text) => {
            let (cut, truncated) = cut_tool_string(text);
            (Some(cut), truncated)
        }
        None => (None, false),
    };
    let outcome_kind = record.outcome.as_ref().map(|o| match o.fact.kind() {
        ToolOutcomeKind::Completed => WireToolOutcomeKind::Completed,
        ToolOutcomeKind::CompletedWithheld => WireToolOutcomeKind::CompletedWithheld,
        ToolOutcomeKind::NotApplied => WireToolOutcomeKind::NotApplied,
        ToolOutcomeKind::NotInvoked => WireToolOutcomeKind::NotInvoked,
        ToolOutcomeKind::Uncertain => WireToolOutcomeKind::Uncertain,
    });

    WireToolCall {
        tool_call_id: record.tool_call_id.clone(),
        request: record.call.request.get(),
        index: record.call.index,
        name: record.name.clone(),
        decision: decision.to_owned(),
        outcome_kind,
        arguments: arguments.map(str::to_owned),
        arguments_cut,
        result: result.map(str::to_owned),
        result_cut,
    }
}

#[cfg(test)]
fn tool_json(record: &ToolEvidenceRecord) -> Value {
    json!(tool_view(record))
}

pub(super) async fn tools(
    shared: &Shared,
    job: JobId,
    after: Option<(RequestNumber, u32)>,
    limit: usize,
) -> Result<Value, Refusal> {
    let database = &shared.database;
    if database.job(job).await?.is_none() {
        return Err(JOB_NOT_FOUND);
    }
    let records = database.tool_evidence(job, after, limit + 1).await?;
    let after_cursor = after.map(|(req, idx)| format!("{}:{}", req.get(), idx));
    let envelope = |page: Vec<WireToolCall>, next: Option<(RequestNumber, u32)>, more: bool| {
        envelope(WireTools {
            job: job.to_string(),
            after: after_cursor.clone(),
            tools: page,
            next_after: next.map(|(req, idx)| format!("{}:{}", req.get(), idx)),
            has_more: more,
        })
    };
    let mut size = envelope(Vec::new(), None, true).to_string().len();
    let mut page = Vec::new();
    let mut next = None;
    let mut more = records.len() > limit;
    for record in records.iter().take(limit) {
        let tool = tool_view(record);
        let cost = json!(tool).to_string().len() + 1;
        if size + cost > shared.response_bytes {
            if page.is_empty() {
                return Err(OUTPUT_TOO_LARGE);
            }
            more = true;
            break;
        }
        size += cost;
        next = Some((record.call.request, record.call.index));
        page.push(tool);
    }
    if !more {
        next = None;
    }
    Ok(envelope(page, next, more))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_names_no_workspace_root() {
        use crate::database::DatabaseWorker;
        use crate::test_support::{data_directory, wait};

        let (_data, directory) = data_directory("health", "workspace-root");
        let worker = DatabaseWorker::start(&directory).expect("start");
        let handle = worker.handle();
        assert_eq!(wait(health(&handle)).get("workspace_root"), None);
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn health_includes_vitals() {
        use crate::database::DatabaseWorker;
        use crate::test_support::{data_directory, wait};

        let (_data, directory) = data_directory("health", "vitals");
        let worker = DatabaseWorker::start(&directory).expect("start");
        let handle = worker.handle();
        let configuration = crate::test_support::execution_configuration(&handle);
        let _job = wait(handle.create_job(crate::persistence::NewJob {
            process_grant: None,
            name: None,
            eligible_at: None,
            parent: handle.genesis(),
            project: None,
            input: crate::persistence::JobInputDraft {
                objective: Some("Ready Job".to_owned()),
                ..Default::default()
            },
            configuration: Some(configuration),
            previous: None,
        }))
        .expect("create job");

        let value = wait(health(&handle));
        assert_eq!(value["api_version"], 1);
        assert_eq!(value["host"], "running");
        assert_eq!(value["database"]["status"], "open");
        assert_eq!(value["database"]["accepting_commands"], true);
        assert_eq!(value["database"]["readable"], true);
        assert!(
            value["database"]["database_bytes"]
                .as_u64()
                .expect("database_bytes")
                > 0
        );
        assert!(
            value["database"]["available_bytes"]
                .as_u64()
                .expect("available_bytes")
                > 0
        );
        assert!(
            value["database"]["durable_commits"]
                .as_u64()
                .expect("durable_commits")
                >= 1
        );
        assert!(value["database"]["commit_us_max"].is_u64());
        assert!(value["claims"]["count"].is_u64());
        assert!(value["claims"]["max_us"].is_u64());
        assert!(value["queue"]["pending"].is_u64());
        assert!(value["queue"]["ready"].as_u64().expect("queue_ready") >= 1);
        assert!(value["queue"]["delayed"].is_u64());
        assert!(value["queue"]["commands_waiting"].is_u64());
        assert!(
            value["process"]["peak_rss_bytes"]
                .as_u64()
                .expect("peak_rss_bytes")
                > 0
        );
        assert_eq!(value["quarantined"], json!([]));
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn response_query_requires_an_advancing_request_and_bounded_page() {
        assert_eq!(responses_query("").unwrap(), (None, 50));
        assert_eq!(
            responses_query("after=2&limit=1").unwrap(),
            (RequestNumber::new(2), 1)
        );
        for query in [
            "after=0",
            "after=01",
            "after=4294967296",
            "after=1&after=2",
            "limit=0",
            "limit=101",
            "limit=1&limit=2",
            "limit=2&",
            "&limit=2",
            "unknown=1",
        ] {
            assert_eq!(responses_query(query), Err(INVALID_REQUEST), "{query}");
        }
    }

    #[test]
    fn tools_query_parsing() {
        let (after, limit) = tools_query("").expect("empty query");
        assert_eq!(after, None);
        assert_eq!(limit, 50);

        let (after, limit) = tools_query("limit=10").expect("limit only");
        assert_eq!(after, None);
        assert_eq!(limit, 10);

        let (after, limit) = tools_query("after=1:0&limit=100").expect("after and limit");
        assert_eq!(after, Some((RequestNumber::new(1).unwrap(), 0)));
        assert_eq!(limit, 100);

        let (after, limit) = tools_query("after=2:15").expect("after only");
        assert_eq!(after, Some((RequestNumber::new(2).unwrap(), 15)));
        assert_eq!(limit, 50);

        for invalid in [
            "limit=0",
            "limit=101",
            "limit=05",
            "after=0:0",
            "after=1",
            "after=1:0:0",
            "after=:0",
            "after=1:",
            "unknown=1",
            "limit=10&limit=20",
        ] {
            assert!(tools_query(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn string_cutting_at_2kib() {
        let short = "hello world";
        let (cut, truncated) = cut_tool_string(short);
        assert_eq!(cut, short);
        assert!(!truncated);

        let exact = "a".repeat(2048);
        let (cut, truncated) = cut_tool_string(&exact);
        assert_eq!(cut.len(), 2048);
        assert!(!truncated);

        let long = "a".repeat(3000);
        let (cut, truncated) = cut_tool_string(&long);
        assert_eq!(cut.len(), 2048);
        assert!(truncated);

        // UTF-8 boundary safety
        let mut multibyte = "a".repeat(2047);
        multibyte.push('€'); // 3 bytes: 2047 + 3 = 2050 bytes total
        let (cut, truncated) = cut_tool_string(&multibyte);
        assert_eq!(cut.len(), 2047);
        assert!(truncated);
    }

    #[test]
    fn tool_json_formats_admitted_cut_and_refused() {
        use crate::persistence::{
            AuthorizationBasis, Recorded, Timestamp, ToolAdmission, ToolCallRef, ToolEffect,
            ToolPayload,
        };

        let call = ToolCallRef {
            job: JobId::new(1),
            request: RequestNumber::new(1).unwrap(),
            index: 0,
        };
        let admitted_record = ToolEvidenceRecord {
            approval: None,
            call,
            tool_call_id: "call_abc".into(),
            name: "workspace_read_file".into(),
            spawns: false,
            decision: Recorded {
                fact: ToolDecision::Admitted(ToolAdmission {
                    handler_version: "workspace_read_file-1".into(),
                    effect: ToolEffect::ReadOnly,
                    parameters: "{}".into(),
                    arguments: ToolPayload::new(format!("{{\"path\": \"{}\"}}", "a".repeat(3000))),
                    authorization: AuthorizationBasis::RegistryGrant,
                }),
                at: Timestamp::now(),
            },
            outcome: Some(Recorded {
                fact: ToolOutcome::Completed {
                    result: ToolPayload::new("result output".repeat(300)),
                },
                at: Timestamp::now(),
            }),
            reconciliation: None,
        };

        let json = tool_json(&admitted_record);
        assert_eq!(json["name"], "workspace_read_file");
        assert_eq!(json["decision"], "admitted");
        assert_eq!(json["outcome_kind"], "completed");
        assert_eq!(json["arguments_cut"], true);
        assert_eq!(json["result_cut"], true);
        assert!(json.get("outcome").is_none());
        assert!(json.get("arguments_truncated").is_none());
        assert!(json.get("result_truncated").is_none());
        assert_eq!(json["arguments"].as_str().unwrap().len(), 2048);
        assert_eq!(json["result"].as_str().unwrap().len(), 2048);

        let refused_record = ToolEvidenceRecord {
            approval: None,
            call: ToolCallRef {
                job: JobId::new(1),
                request: RequestNumber::new(1).unwrap(),
                index: 1,
            },
            tool_call_id: "call_refused".into(),
            name: "workspace_run_command".into(),
            spawns: false,
            decision: Recorded {
                fact: ToolDecision::Refused(
                    crate::openai_compatible::ToolCallRefusal::Unauthorized,
                ),
                at: Timestamp::now(),
            },
            outcome: None,
            reconciliation: None,
        };

        let json_refused = tool_json(&refused_record);
        assert_eq!(json_refused["name"], "workspace_run_command");
        assert_eq!(json_refused["decision"], "unauthorized");
        assert_eq!(json_refused["outcome_kind"], Value::Null);
        assert_eq!(json_refused["arguments"], Value::Null);
        assert_eq!(json_refused["arguments_cut"], false);
        assert_eq!(json_refused["result"], Value::Null);
        assert_eq!(json_refused["result_cut"], false);
    }
}
