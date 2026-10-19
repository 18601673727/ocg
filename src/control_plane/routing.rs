use super::*;

/// A request's route, once admitted.
pub(super) enum Route {
    Health,
    Jobs {
        before: Option<JobId>,
        limit: usize,
    },
    Conversations {
        before: Option<JobId>,
        limit: usize,
    },
    Usage(dashboard::ObservationQuery),
    Availability(dashboard::ObservationQuery),
    Logs(dashboard::LogQuery),
    Job(JobId),
    JobInput(JobId),
    JobConversation(JobId),
    ProviderRequest(JobId, RequestNumber),
    AgentProfiles {
        before: Option<ProfileId>,
        limit: usize,
    },
    Profiles,
    ProfileCreate,
    ProviderModels,
    ProfileUpdate(ProfileId),
    Outputs {
        job: JobId,
        after_seq: Option<i64>,
        limit: usize,
    },
    Tools {
        job: JobId,
        after: Option<(RequestNumber, u32)>,
        limit: usize,
    },
    Events {
        job: JobId,
        after_seq: Option<i64>,
    },
    Submit,
    Admission,
    Stop(JobId),
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
        (Caller::Anonymous, Route::SessionCreate) => true,
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
            | Route::Usage(_)
            | Route::Availability(_)
            | Route::Logs(_)
            | Route::Job(_)
            | Route::JobInput(_)
            | Route::JobConversation(_)
            | Route::ProviderRequest(..)
            | Route::Tools { .. }
            | Route::AgentProfiles { .. }
            | Route::Profiles
            | Route::ProviderModels
            | Route::ProfileCreate
            | Route::ProfileUpdate(_)
            | Route::Outputs { .. }
            | Route::Events { .. }
            | Route::Admission
            | Route::Submit
            | Route::Stop(_)
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
            ["jobs", job, "stop"] => {
                let job = job_id(job)?;
                no_query(query).map(|()| Route::Stop(job))
            }
            ["profiles"] => no_query(query).map(|()| Route::ProfileCreate),
            ["provider-models"] => no_query(query).map(|()| Route::ProviderModels),
            ["profiles", profile] => {
                let profile = profiles::profile_id(profile)?;
                no_query(query).map(|()| Route::ProfileUpdate(profile))
            }
            ["attachments"] => no_query(query).map(|()| Route::AttachmentUpload),
            ["session"] => no_query(query).map(|()| Route::SessionCreate),
            _ => Err(NOT_FOUND),
        };
    }
    if request.method() == Method::DELETE {
        return match segments.as_slice() {
            ["session"] => no_query(query).map(|()| Route::SessionRevoke),
            _ => Err(NOT_FOUND),
        };
    }
    match segments.as_slice() {
        ["health"] => no_query(query).map(|()| Route::Health),
        ["job-history"] => {
            dashboard::jobs_query(query).map(|(before, limit)| Route::Jobs { before, limit })
        }
        ["conversations"] => catalog::conversations_query(query)
            .map(|(before, limit)| Route::Conversations { before, limit }),
        ["usage"] => dashboard::ObservationQuery::parse(query).map(Route::Usage),
        ["availability"] => dashboard::ObservationQuery::parse(query).map(Route::Availability),
        ["logs"] => dashboard::LogQuery::parse(query).map(Route::Logs),
        ["admissions"] => no_query(query).map(|()| Route::Admission),
        ["session"] => no_query(query).map(|()| Route::SessionStatus),
        ["profiles"] => no_query(query).map(|()| Route::Profiles),
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
        ["jobs", job, "input"] => {
            let job = job_id(job)?;
            no_query(query).map(|()| Route::JobInput(job))
        }
        ["jobs", job, "conversation"] => {
            let job = job_id(job)?;
            no_query(query).map(|()| Route::JobConversation(job))
        }
        ["agent-profiles"] => catalog::agent_profile_query(query)
            .map(|(before, limit)| Route::AgentProfiles { before, limit }),
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
            let (after, limit) = projection::tools_query(query)?;
            Ok(Route::Tools { job, after, limit })
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

/// A Job written as OCG writes it: `job:<n>`.
pub(super) fn job_id(text: &str) -> Result<JobId, Refusal> {
    text.strip_prefix("job:")
        .and_then(decimal::<u64>)
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
        Route::Health => Ok(Reply::ok(
            health(database, shared.workspace_tools.root()).await,
        )),
        Route::Jobs { before, limit } => dashboard::jobs(shared, before, limit).await,
        Route::Conversations { before, limit } => {
            catalog::conversations(database, before, limit).await
        }
        Route::Usage(query) => dashboard::usage(shared, query).await,
        Route::Availability(query) => dashboard::availability(shared, query).await,
        Route::Logs(query) => dashboard::logs(shared, query).await,
        Route::Job(job) => {
            let record = database.job(job).await?.ok_or(JOB_NOT_FOUND)?;
            Ok(Reply::ok(
                json!({"api_version": API_VERSION, "job": job_json(&record)?}),
            ))
        }
        Route::JobInput(job) => catalog::job_input(database, job).await,
        Route::JobConversation(job) => catalog::job_conversation(database, job).await,
        Route::ProviderRequest(job, request) => {
            let record = database
                .provider_request(job, request)
                .await?
                .ok_or(NOT_FOUND)?;
            Ok(Reply::ok(
                json!({"api_version": API_VERSION, "job": job.to_string(),
                "request": request.get(), "messages": record.messages, "tools": record.tools,
                "parameters": record.parameters, "dropped_turns": record.dropped_turns, "response": record.response}),
            ))
        }
        Route::AgentProfiles { before, limit } => {
            catalog::agent_profiles(database, before, limit).await
        }
        Route::Profiles => profiles::list_profiles(database).await,
        Route::ProviderModels => {
            let body = read_body(payload, MAX_SUBMISSION_BYTES).await?;
            profiles::provider_models(database, &body).await
        }
        Route::ProfileCreate => {
            let body = read_body(payload, MAX_SUBMISSION_BYTES).await?;
            profiles::save_profile(database, None, &body).await
        }
        Route::ProfileUpdate(profile) => {
            let body = read_body(payload, MAX_SUBMISSION_BYTES).await?;
            profiles::save_profile(database, Some(profile), &body).await
        }
        Route::Outputs {
            job,
            after_seq,
            limit,
        } => outputs(shared, job, after_seq, limit).await.map(Reply::ok),
        Route::Tools { job, after, limit } => projection::tools(shared, job, after, limit)
            .await
            .map(Reply::ok),
        Route::Submit => {
            let key = submissions::idempotency_key(request.headers())?;
            let body = read_body(payload, MAX_SUBMISSION_BYTES).await?;
            let job = submissions::JobRequest::parse(&body)?;
            submissions::submit(shared, key, job).await
        }
        Route::Admission => {
            let key = submissions::idempotency_key(request.headers())?;
            submissions::lookup(shared, key).await
        }
        Route::Stop(job) => submissions::stop(shared, job).await,
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
        Route::Events { .. } => Err(INTERNAL),
        // Answered as the image itself, before any reply.
        Route::AttachmentContent(_) => Err(INTERNAL),
    }
}
