use super::contracts::*;
use super::*;

/// A request's route, once admitted.
pub(super) enum Route {
    Health,
    Jobs {
        before: Option<JobId>,
        limit: usize,
        terms: Vec<String>,
    },
    Conversations {
        before: Option<JobId>,
        limit: usize,
    },
    ConversationSearch {
        query: Vec<String>,
        before: Option<JobId>,
        limit: usize,
    },
    Usage(dashboard::ObservationQuery),
    UsageSummary(dashboard::UsageSummaryQuery),
    UsageRetries(dashboard::UsageRetriesQuery),
    ToolStats,
    Availability(dashboard::ObservationQuery),
    Logs(dashboard::LogQuery),
    Subscription(subscriptions::Topic),
    Job(JobId),
    JobInput(JobId),
    ConversationChildren {
        conversation: JobId,
        before: Option<JobId>,
        limit: usize,
    },
    ConversationTurns {
        conversation: JobId,
        before: Option<JobId>,
        limit: usize,
    },
    JobLimits(JobId),
    ConversationContext {
        conversation: JobId,
        before: Option<JobId>,
        limit: usize,
    },
    ConversationNotices {
        conversation: JobId,
        after: u32,
        limit: usize,
    },
    ProviderRequest(JobId, RequestNumber),
    ProviderResponses {
        job: JobId,
        after: Option<RequestNumber>,
        limit: usize,
        text_offset: u32,
    },
    AgentProfiles {
        before: Option<ProfileId>,
        limit: usize,
    },
    Profiles {
        before: Option<ProfileId>,
        limit: usize,
    },
    ActiveSystemSettings,
    GlobalCompactionSettings,
    SaveGlobalCompactionSettings,
    ProfileCreate,
    ProfileImport,
    ProfileDelete(ProfileId),
    ProviderModels,
    ProfileUpdate(ProfileId),
    Projects {
        before: Option<ProjectId>,
        limit: usize,
    },
    ProjectCreate,
    ProjectUpdate(ProjectId),
    ProjectDecision(ProjectId),
    Pools,
    PoolUpdate(ExecutionPoolId),
    JobEdit(JobId),
    Outputs {
        job: JobId,
        after_seq: Option<i64>,
        limit: usize,
    },
    LatestOutput(JobId),
    Tools {
        job: JobId,
        after: Option<(RequestNumber, u32)>,
        limit: usize,
    },
    Tool {
        job: JobId,
        request: RequestNumber,
        index: u32,
    },
    JobTree {
        job: JobId,
        after: Option<JobId>,
        limit: usize,
        depth: u8,
    },
    Events {
        job: JobId,
        after_seq: Option<i64>,
    },
    Submit,
    DecisionSubmit,
    Admission,
    DecideApproval(crate::db::entities::ToolCallRef),
    ReleaseSlot(JobId),
    ConversationCards(JobId),
    JobAction(JobId, &'static str),
    ConversationCompact(JobId),
    AttachmentUpload,
    Attachment(AttachmentId),
    AttachmentContent(AttachmentId),
    SessionCreate,
    SessionStatus,
    SessionRevoke,
}

/// Whether `caller` may use `route`: the API routes a signed-in browser
/// may use are named one by one, and everything unnamed is refused to it.
/// The session routes are the browser's alone.
pub(super) fn authorize(caller: &Caller, route: Route) -> Result<Route, Refusal> {
    let allowed = match (caller, &route) {
        (Caller::Anonymous, Route::SessionCreate | Route::SessionStatus) => true,
        (
            Caller::Programmatic,
            Route::SessionCreate | Route::SessionStatus | Route::SessionRevoke,
        ) => false,
        (Caller::Programmatic, _) => true,
        (
            Caller::Operator(_),
            Route::Health
            | Route::Jobs { .. }
            | Route::Conversations { .. }
            | Route::ConversationSearch { .. }
            | Route::Usage(_)
            | Route::UsageSummary(_)
            | Route::UsageRetries(_)
            | Route::ToolStats
            | Route::Availability(_)
            | Route::Logs(_)
            | Route::Subscription(_)
            | Route::Job(_)
            | Route::JobInput(_)
            | Route::ConversationChildren { .. }
            | Route::ConversationTurns { .. }
            | Route::JobLimits(_)
            | Route::ConversationContext { .. }
            | Route::ConversationNotices { .. }
            | Route::ProviderRequest(..)
            | Route::ProviderResponses { .. }
            | Route::Tools { .. }
            | Route::Tool { .. }
            | Route::JobTree { .. }
            | Route::AgentProfiles { .. }
            | Route::Profiles { .. }
            | Route::ProviderModels
            | Route::ProfileImport
            | Route::ProfileDelete(_)
            | Route::ActiveSystemSettings
            | Route::GlobalCompactionSettings
            | Route::SaveGlobalCompactionSettings
            | Route::ProfileCreate
            | Route::ProfileUpdate(_)
            | Route::Projects { .. }
            | Route::ProjectCreate
            | Route::ProjectUpdate(_)
            | Route::ProjectDecision(_)
            | Route::Pools
            | Route::PoolUpdate(_)
            | Route::JobEdit(_)
            | Route::Outputs { .. }
            | Route::LatestOutput(_)
            | Route::Events { .. }
            | Route::Admission
            | Route::Submit
            | Route::DecisionSubmit
            | Route::DecideApproval(_)
            | Route::ReleaseSlot(_)
            | Route::ConversationCards(_)
            | Route::JobAction(..)
            | Route::ConversationCompact(_)
            | Route::AttachmentUpload
            | Route::Attachment(_)
            | Route::AttachmentContent(_)
            | Route::SessionStatus
            | Route::SessionRevoke,
        ) => true,
        (Caller::Operator(_) | Caller::Anonymous, _) => false,
    };
    if allowed { Ok(route) } else { Err(NOT_FOUND) }
}

pub(super) fn route(request: &HttpRequest) -> Result<Route, Refusal> {
    let path = request.path();
    let query = request.query_string();
    let segments = path
        .strip_prefix("/api/v1/")
        .ok_or(NOT_FOUND)?
        .split('/')
        .collect::<Vec<_>>();
    if request.method() == Method::POST {
        return match segments.as_slice() {
            ["jobs"] => no_query(query).map(|()| Route::Submit),
            ["decisions"] => no_query(query).map(|()| Route::DecisionSubmit),
            ["jobs", job, "release-slot"] => {
                no_query(query).and_then(|()| job_id(job).map(Route::ReleaseSlot))
            }
            ["approvals", job, request, index] => {
                let approval = crate::db::entities::ToolCallRef {
                    job: job_id(job)?,
                    request: decimal::<u32>(request)
                        .and_then(RequestNumber::new)
                        .ok_or(INVALID_REQUEST)?,
                    index: decimal::<u32>(index).ok_or(INVALID_REQUEST)?,
                };
                no_query(query).map(|()| Route::DecideApproval(approval))
            }
            ["conversations", conversation, "compact"] => {
                no_query(query).and_then(|()| job_id(conversation).map(Route::ConversationCompact))
            }
            ["conversations", conversation, action]
                if matches!(
                    *action,
                    "stop" | "close" | "hold" | "unhold" | "retry" | "queue"
                ) =>
            {
                let conversation = job_id(conversation)?;
                let action = match *action {
                    "stop" => "stop",
                    "close" => "close",
                    "hold" => "hold",
                    "unhold" => "unhold",
                    "retry" => "retry",
                    "queue" => "queue",
                    _ => return Err(NOT_FOUND),
                };
                no_query(query).map(|()| Route::JobAction(conversation, action))
            }
            ["settings", "compaction"] => {
                no_query(query).map(|()| Route::SaveGlobalCompactionSettings)
            }
            ["profiles"] => no_query(query).map(|()| Route::ProfileCreate),
            ["profiles", "import"] => no_query(query).map(|()| Route::ProfileImport),
            ["provider-models"] => no_query(query).map(|()| Route::ProviderModels),
            ["profiles", profile] => {
                let profile = profiles::profile_id(profile)?;
                no_query(query).map(|()| Route::ProfileUpdate(profile))
            }
            ["projects"] => no_query(query).map(|()| Route::ProjectCreate),
            ["projects", project, "decision"] => {
                let project = projects::project_id(project)?;
                no_query(query).map(|()| Route::ProjectDecision(project))
            }
            ["projects", project] => {
                let project = projects::project_id(project)?;
                no_query(query).map(|()| Route::ProjectUpdate(project))
            }
            ["pools", pool] => {
                let pool = pools::pool_id(pool)?;
                no_query(query).map(|()| Route::PoolUpdate(pool))
            }
            ["conversations", job] => {
                let job = job_id(job)?;
                no_query(query).map(|()| Route::JobEdit(job))
            }
            ["attachments"] => no_query(query).map(|()| Route::AttachmentUpload),
            ["session"] => no_query(query).map(|()| Route::SessionCreate),
            _ => Err(NOT_FOUND),
        };
    }
    if request.method() == Method::DELETE {
        return match segments.as_slice() {
            ["session"] => no_query(query).map(|()| Route::SessionRevoke),
            ["profiles", id] => {
                let id = profiles::profile_id(id)?;
                no_query(query).map(|()| Route::ProfileDelete(id))
            }
            _ => Err(NOT_FOUND),
        };
    }
    match segments.as_slice() {
        ["health"] => no_query(query).map(|()| Route::Health),
        ["job-history"] => dashboard::jobs_query(query).map(|(before, limit, terms)| Route::Jobs {
            before,
            limit,
            terms,
        }),
        ["job-history", "events"] => subscriptions::JobsQuery::parse(query)
            .map(|query| Route::Subscription(subscriptions::Topic::Jobs(query))),
        ["catalog", "events"] => {
            no_query(query).map(|()| Route::Subscription(subscriptions::Topic::Catalog))
        }
        ["observation-changes", "events"] => subscriptions::change_cursor(query)
            .map(|cursor| Route::Subscription(subscriptions::Topic::ObservationChanges(cursor))),
        ["sidebar", "events"] => subscriptions::SidebarQuery::parse(query)
            .map(|query| Route::Subscription(subscriptions::Topic::Sidebar(query))),
        ["logs", "events"] => dashboard::LogQuery::parse(query)
            .map(|query| Route::Subscription(subscriptions::Topic::Logs(query))),
        ["usage", "events"] => dashboard::ObservationQuery::parse(query)
            .map(|query| Route::Subscription(subscriptions::Topic::Usage(query))),
        ["availability", "events"] => dashboard::ObservationQuery::parse(query)
            .map(|query| Route::Subscription(subscriptions::Topic::Availability(query))),
        ["conversations"] => catalog::conversations_query(query)
            .map(|(before, limit)| Route::Conversations { before, limit }),
        ["conversations", "search"] => {
            catalog::conversation_search_query(query).map(|(query, before, limit)| {
                Route::ConversationSearch {
                    query,
                    before,
                    limit,
                }
            })
        }
        ["usage"] => dashboard::ObservationQuery::parse(query).map(Route::Usage),
        ["usage", "summary"] => dashboard::UsageSummaryQuery::parse(query).map(Route::UsageSummary),
        ["usage", "retries"] => dashboard::UsageRetriesQuery::parse(query).map(Route::UsageRetries),
        ["tool-stats"] => no_query(query).map(|()| Route::ToolStats),
        ["availability"] => dashboard::ObservationQuery::parse(query).map(Route::Availability),
        ["logs"] => dashboard::LogQuery::parse(query).map(Route::Logs),
        ["admissions"] => no_query(query).map(|()| Route::Admission),
        ["session"] => no_query(query).map(|()| Route::SessionStatus),
        ["settings", "system"] => no_query(query).map(|()| Route::ActiveSystemSettings),
        ["settings", "compaction"] => no_query(query).map(|()| Route::GlobalCompactionSettings),
        ["profiles"] => catalog::agent_profile_query(query)
            .map(|(before, limit)| Route::Profiles { before, limit }),
        ["projects"] => {
            projects::projects_query(query).map(|(before, limit)| Route::Projects { before, limit })
        }
        ["pools"] => no_query(query).map(|()| Route::Pools),
        ["attachments", attachment] => {
            let attachment = attachments::attachment_id(attachment)?;
            no_query(query).map(|()| Route::Attachment(attachment))
        }
        ["attachments", attachment, "content"] => {
            let attachment = attachments::attachment_id(attachment)?;
            no_query(query).map(|()| Route::AttachmentContent(attachment))
        }
        ["jobs", job] => {
            let job = job_id(job)?;
            no_query(query).map(|()| Route::Job(job))
        }
        ["jobs", job, "requests", number] => {
            let job = job_id(job)?;
            let request = decimal::<u32>(number)
                .and_then(RequestNumber::new)
                .ok_or(INVALID_REQUEST)?;
            no_query(query).map(|()| Route::ProviderRequest(job, request))
        }
        ["jobs", job, "responses"] => {
            let job = job_id(job)?;
            let (after, limit, text_offset) = responses::responses_query(query)?;
            Ok(Route::ProviderResponses {
                job,
                after,
                limit,
                text_offset,
            })
        }
        ["jobs", job, "input"] => {
            let job = job_id(job)?;
            no_query(query).map(|()| Route::JobInput(job))
        }
        ["conversations", conversation, "cards"] => {
            no_query(query).and_then(|()| job_id(conversation).map(Route::ConversationCards))
        }
        ["conversations", conversation, "children"] => {
            let conversation = job_id(conversation)?;
            let (before, limit) = catalog::page_query(query, job_id)?;
            Ok(Route::ConversationChildren {
                conversation,
                before,
                limit,
            })
        }
        ["conversations", conversation, "context"] => {
            let conversation = job_id(conversation)?;
            let (before, limit) = catalog::page_query(query, job_id)?;
            Ok(Route::ConversationContext {
                conversation,
                before,
                limit,
            })
        }
        ["conversations", conversation, "limits"] => {
            no_query(query).and_then(|()| job_id(conversation).map(Route::JobLimits))
        }
        ["conversations", conversation, "turns"] => {
            let conversation = job_id(conversation)?;
            let (before, limit) = catalog::page_query(query, job_id)?;
            Ok(Route::ConversationTurns {
                conversation,
                before,
                limit,
            })
        }
        ["conversations", conversation, "notices"] => {
            let conversation = job_id(conversation)?;
            let (after, limit) = output_query(query)?;
            let after = after
                .map(u32::try_from)
                .transpose()
                .map_err(|_| INVALID_REQUEST)?
                .unwrap_or(0);
            Ok(Route::ConversationNotices {
                conversation,
                after,
                limit,
            })
        }
        ["agent-profiles"] => catalog::agent_profile_query(query)
            .map(|(before, limit)| Route::AgentProfiles { before, limit }),
        ["jobs", job, "outputs", "latest"] if query.is_empty() => {
            Ok(Route::LatestOutput(job_id(job)?))
        }
        ["jobs", job, "outputs"] => {
            let job = job_id(job)?;
            let (after_seq, limit) = output_query(query)?;
            Ok(Route::Outputs {
                job,
                after_seq,
                limit,
            })
        }
        ["jobs", job, "tools"] => {
            let job = job_id(job)?;
            let (after, limit) = tools::tools_query(query)?;
            Ok(Route::Tools { job, after, limit })
        }
        ["jobs", job, "tools", request, index] if query.is_empty() => Ok(Route::Tool {
            job: job_id(job)?,
            request: decimal::<u32>(request)
                .and_then(RequestNumber::new)
                .ok_or(INVALID_REQUEST)?,
            index: decimal::<u32>(index).ok_or(INVALID_REQUEST)?,
        }),
        ["jobs", job, "tree"] => {
            let job = job_id(job)?;
            let (after, limit, depth) = catalog::tree_query(query)?;
            Ok(Route::JobTree {
                job,
                after,
                limit,
                depth,
            })
        }
        ["jobs", job, "events"] => {
            let job = job_id(job)?;
            let after_seq = events::cursor(query, request.headers())?;
            Ok(Route::Events { job, after_seq })
        }
        _ => Err(NOT_FOUND),
    }
}

fn no_query(query: &str) -> Result<(), Refusal> {
    if query.is_empty() {
        Ok(())
    } else {
        Err(INVALID_REQUEST)
    }
}

/// A non-negative decimal number without sign, leading zero or more digits
/// than `T` holds.
pub(super) fn decimal<T: std::str::FromStr>(text: &str) -> Option<T> {
    let canonical = !text.is_empty()
        && text.bytes().all(|byte| byte.is_ascii_digit())
        && (text == "0" || !text.starts_with('0'));
    canonical.then(|| text.parse().ok()).flatten()
}

/// Parses a canonical Job UUID.
pub(super) fn job_id(text: &str) -> Result<JobId, Refusal> {
    crate::db::entities::parse_entity_id(text)
        .map(JobId::new)
        .ok_or(INVALID_REQUEST)
}

/// `after_seq` and `limit`, each at most once, and nothing else.
pub(super) fn output_query(query: &str) -> Result<(Option<i64>, usize), Refusal> {
    let (mut after_seq, mut limit) = (None, None);
    if !query.is_empty() {
        for pair in query.split('&') {
            let (key, value) = pair.split_once('=').ok_or(INVALID_REQUEST)?;
            let slot = match key {
                "after_seq" if after_seq.is_none() => {
                    after_seq = Some(decimal::<i64>(value).ok_or(INVALID_REQUEST)?);
                    continue;
                }
                "limit" if limit.is_none() => &mut limit,
                _ => return Err(INVALID_REQUEST),
            };
            *slot = Some(
                decimal::<usize>(value)
                    .filter(|limit| (1..=MAX_OUTPUT_PAGE).contains(limit))
                    .ok_or(INVALID_REQUEST)?,
            );
        }
    }
    Ok((after_seq, limit.unwrap_or(DEFAULT_OUTPUT_PAGE)))
}

pub(super) async fn respond(
    route: Route,
    caller: &Caller,
    request: &HttpRequest,
    payload: web::types::Payload,
    shared: &Arc<Shared>,
) -> Result<Reply, Refusal> {
    let database = &shared.database;
    match route {
        Route::Health => Ok(Reply::ok(health(database).await)),
        Route::Jobs {
            before,
            limit,
            terms,
        } => dashboard::jobs(shared, before, limit, terms).await,
        Route::Conversations { before, limit } => {
            catalog::conversations(database, before, limit).await
        }
        Route::ConversationSearch {
            query,
            before,
            limit,
        } => catalog::search_conversations(database, query, before, limit).await,
        Route::Usage(query) => dashboard::usage(shared, query).await,
        Route::UsageSummary(query) => dashboard::usage_summary(shared, query).await,
        Route::UsageRetries(query) => dashboard::usage_retries(shared, query).await,
        Route::ToolStats => dashboard::tool_stats(shared).await,
        Route::Availability(query) => dashboard::availability(shared, query).await,
        Route::Logs(query) => dashboard::logs(shared, query).await,
        Route::Job(job) => {
            let details = database.job_details(job).await?.ok_or(JOB_NOT_FOUND)?;
            Ok(Reply::ok(envelope(serde_json::json!(
                jobs::JobDetailsView {
                    job: jobs::job_view(&details.job)?,
                    result: details.result.as_ref().map(outputs::output_view),
                    usage: details.usage,
                    activity: details.activity,
                    task_plan: details.task_plan,
                    unknown_reason: details.unknown_reason,
                }
            ))))
        }
        Route::JobInput(job) => catalog::job_input(database, job).await,
        Route::ConversationChildren {
            conversation,
            before,
            limit,
        } => catalog::conversation_children(database, conversation, before, limit).await,
        Route::JobLimits(conversation) => catalog::group_limits(database, conversation).await,
        Route::ConversationContext {
            conversation,
            before,
            limit,
        } => catalog::conversation_context(database, conversation, before, limit).await,
        Route::ConversationTurns {
            conversation,
            before,
            limit,
        } => {
            let reply = catalog::conversation_turns(database, conversation, before, limit).await?;
            catalog::bound_conversation_turns(shared, reply.body)
        }
        Route::ConversationNotices {
            conversation,
            after,
            limit,
        } => catalog::conversation_notices(database, conversation, after, limit).await,
        Route::ProviderRequest(job, request) => {
            let record = database
                .provider_request(job, request)
                .await?
                .ok_or(NOT_FOUND)?;
            Ok(Reply::ok(envelope(serde_json::json!({
                "job": job.to_string(),
                "request": request.get(),
                "messages": record.messages,
                "tools": record.tools,
                "parameters": record.parameters,
                "dropped_turns": record.dropped_turns,
                "response": record.response,
            }))))
        }
        Route::ProviderResponses {
            job,
            after,
            limit,
            text_offset,
        } => responses::provider_responses(shared, job, after, limit, text_offset)
            .await
            .map(Reply::ok),
        Route::AgentProfiles { before, limit } => {
            catalog::agent_profiles(database, before, limit).await
        }
        Route::Profiles { before, limit } => {
            profiles::list_profiles(database, before.map(Into::into), limit).await
        }
        Route::ProviderModels => {
            let body = read_body(payload, MAX_SUBMISSION_BYTES).await?;
            profiles::provider_models(database, &body).await
        }
        Route::ProfileImport => {
            let body = read_body(payload, MAX_SUBMISSION_BYTES).await?;
            profiles::import_models(database, &body).await
        }
        Route::ActiveSystemSettings => Ok(Reply::ok(envelope(serde_json::json!(
            super::system::ActiveSystemSettingsView {
                path: crate::system::policy::SystemPolicy::path(&database.data_directory())
                    .display()
                    .to_string(),
                content: database.active_system_policy().to_conf(),
            }
        )))),
        Route::GlobalCompactionSettings => {
            profiles::global_compaction_settings(database, None).await
        }
        Route::SaveGlobalCompactionSettings => {
            let body = read_body(payload, MAX_SUBMISSION_BYTES).await?;
            profiles::global_compaction_settings(database, Some(&body)).await
        }
        Route::ProfileDelete(id) => profiles::delete_model(database, id).await,
        Route::ProfileCreate => {
            let body = read_body(payload, MAX_SUBMISSION_BYTES).await?;
            profiles::save_profile(database, None, &body).await
        }
        Route::ProfileUpdate(profile) => {
            let body = read_body(payload, MAX_SUBMISSION_BYTES).await?;
            profiles::save_profile(database, Some(profile), &body).await
        }
        Route::Projects { before, limit } => projects::list(database, before, limit).await,
        Route::ProjectCreate => {
            let body = read_body(payload, MAX_SUBMISSION_BYTES).await?;
            projects::create(shared, &body).await
        }
        Route::ProjectDecision(project) => {
            let body = read_body(payload, MAX_SUBMISSION_BYTES).await?;
            projects::configure_decision(shared, project, &body).await
        }
        Route::ProjectUpdate(project) => {
            let body = read_body(payload, MAX_SUBMISSION_BYTES).await?;
            projects::update(database, project, &body).await
        }
        Route::Pools => pools::list(database).await,
        Route::PoolUpdate(pool) => {
            let body = read_body(payload, MAX_SUBMISSION_BYTES).await?;
            pools::update(database, pool, &body).await
        }
        Route::JobEdit(job) => {
            let body = read_body(payload, MAX_SUBMISSION_BYTES).await?;
            catalog::edit_conversation(database, job, &body).await
        }
        Route::Outputs {
            job,
            after_seq,
            limit,
        } => outputs(shared, job, after_seq, limit).await.map(Reply::ok),
        Route::LatestOutput(job) => outputs::latest_output(shared, job).await.map(Reply::ok),
        Route::Tools { job, after, limit } => {
            tools::tools(shared, job, after, limit).await.map(Reply::ok)
        }
        Route::Tool {
            job,
            request,
            index,
        } => tools::tool(shared, job, request, index).await,
        Route::JobTree {
            job,
            after,
            limit,
            depth,
        } => catalog::job_tree(shared, job, after, limit, depth).await,
        Route::Submit => {
            let key = submissions::idempotency_key(request.headers())?;
            let body = read_body(payload, MAX_SUBMISSION_BYTES).await?;
            let job = submissions::JobRequest::parse(&body)?;
            submissions::submit(shared, key, job).await
        }
        Route::DecisionSubmit => {
            let key = submissions::idempotency_key(request.headers())?;
            let body = read_body(payload, MAX_SUBMISSION_BYTES).await?;
            let decision = submissions::DecisionJobRequest::parse(&body)?;
            submissions::submit_decision(shared, key, decision).await
        }
        Route::Admission => {
            let key = submissions::idempotency_key(request.headers())?;
            submissions::lookup(shared, key).await
        }
        Route::DecideApproval(approval) => {
            let body = read_body(payload, MAX_SUBMISSION_BYTES).await?;
            submissions::decide(shared, approval, &body).await
        }
        Route::ReleaseSlot(job) => {
            let body = read_body(payload, MAX_SUBMISSION_BYTES).await?;
            submissions::release_slot(database, job, &body).await
        }
        Route::ConversationCards(conversation) => submissions::cards(shared, conversation).await,
        Route::ConversationCompact(conversation) => {
            let key = submissions::idempotency_key(request.headers())?;
            let body = read_body(payload, MAX_SUBMISSION_BYTES).await?;
            submissions::compact(shared, conversation, key, &body).await
        }
        Route::JobAction(conversation, action) => {
            let body = read_body(payload, MAX_SUBMISSION_BYTES).await?;
            submissions::action(shared, conversation, action, &body).await
        }
        Route::AttachmentUpload => {
            let body = read_body(payload, MAX_ATTACHMENT_UPLOAD_BYTES).await?;
            attachments::upload(database, &body).await
        }
        Route::Attachment(attachment) => attachments::metadata(database, attachment).await,
        Route::SessionCreate => {
            let body = read_body(payload, browser::MAX_LOGIN_BYTES).await?;
            let browser = shared.browser.as_ref().ok_or(NOT_FOUND)?;
            browser.login(&body)
        }
        Route::SessionStatus => match caller {
            Caller::Operator(session) => Ok(Reply::ok(browser::described(session))),
            // The served frontend signs itself in; a headless listener has
            // no page to do that from and keeps the pairing code.
            Caller::Anonymous if shared.assets.is_some() => {
                let browser = shared.browser.as_ref().ok_or(NOT_FOUND)?;
                browser.begin_session()
            }
            _ => Err(NOT_FOUND),
        },
        Route::SessionRevoke => match caller {
            Caller::Operator(session) => {
                let browser = shared.browser.as_ref().ok_or(NOT_FOUND)?;
                Ok(browser.logout(session))
            }
            _ => Err(NOT_FOUND),
        },
        // Answered as a stream, before any reply.
        Route::Events { .. } | Route::Subscription(_) => Err(INTERNAL),
        // Answered as the image itself, before any reply.
        Route::AttachmentContent(_) => Err(INTERNAL),
    }
}
