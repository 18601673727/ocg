use super::contracts::envelope;
use super::{
    Arc, AttachmentId, Caller, DEFAULT_OUTPUT_PAGE, ExecutionPoolId, HttpRequest, INTERNAL,
    INVALID_REQUEST, JOB_NOT_FOUND, JobId, MAX_ATTACHMENT_UPLOAD_BYTES, MAX_OUTPUT_PAGE,
    MAX_SUBMISSION_BYTES, METHOD_NOT_ALLOWED, Method, NOT_FOUND, ONLY_POST, PROFILE_METHODS,
    ProfileId, ProjectId, Refusal, Reply, RequestNumber, SESSION_METHODS, Shared, attachments,
    browser, catalog, dashboard, events, health, jobs, mutable_jobs, outputs, pools, profiles,
    projects, responses, submissions, subscriptions, tools, update, wire::LimitedBody,
};

pub(super) enum Route {
    Health(Option<crate::db::entities::AllocationCursor>),
    MutableJob(mutable_jobs::Action),
    Release(JobId),
    Update(bool),
    StartUpdate,
    Jobs {
        before: Option<JobId>,
        limit: usize,
        terms: Vec<String>,
    },
    Conversations {
        page: catalog::ConversationQuery,
        archived: bool,
    },
    ConversationSearch(catalog::ConversationQuery),
    Usage(dashboard::ObservationQuery),
    UsageSummary(dashboard::UsageSummaryQuery),
    UsageRetries(dashboard::UsageRetriesQuery),
    ToolStats,
    Availability(dashboard::ObservationQuery),
    Logs(dashboard::LogQuery),
    Subscription(subscriptions::Topic),
    Job(JobId),
    JobHistory {
        job: JobId,
        after: Option<u64>,
        limit: usize,
    },
    JobState {
        job: JobId,
        at_seq: Option<u64>,
    },
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
    GlobalOutboundHeaders,
    SaveGlobalOutboundHeaders,
    ProfileCreate,
    ProfileImport,
    ProfileDelete(ProfileId),
    ProfileAvailability(ProfileId),
    ProviderDelete(ProfileId),
    ProviderModels,
    ProfileUpdate(ProfileId),
    Projects {
        before: Option<ProjectId>,
        limit: usize,
    },
    ProjectCreate,
    ProjectUpdate(ProjectId),
    ProjectDecision(ProjectId),
    Pools {
        before: Option<ExecutionPoolId>,
        limit: usize,
    },
    PoolAllocations {
        pool: ExecutionPoolId,
        after: Option<(u128, u128)>,
        limit: usize,
    },
    PoolUpdate(ExecutionPoolId),
    NativePoolArchive(ExecutionPoolId),
    NativePoolCreate,
    NativeExecutors {
        before: Option<crate::db::entities::ExecutorId>,
        limit: usize,
    },
    NativeExecutorCreate,
    NativeExecutorUpdate(crate::db::entities::ExecutorId),
    NativeExecutorArchive(crate::db::entities::ExecutorId),
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
    DecisionSubmit,
    Admission,
    DecideApproval(crate::db::entities::ToolCallRef),
    ConversationCards(JobId),
    JobAction(JobId, &'static str),
    ConversationCompact(JobId),
    AttachmentUpload,
    Attachment(AttachmentId),
    AttachmentContent(AttachmentId),
    SessionStatus,
    SessionRevoke,
}

pub(super) fn authorize(caller: &Caller, route: Route) -> Result<Route, Refusal> {
    let allowed = match (caller, &route) {
        (
            Caller::Operator(_),
            Route::NativeExecutors { .. }
            | Route::PoolAllocations { .. }
            | Route::NativeExecutorCreate
            | Route::NativeExecutorUpdate(_)
            | Route::NativeExecutorArchive(_)
            | Route::NativePoolCreate,
        ) => true,
        (Caller::Anonymous | Caller::BeginSession, Route::SessionStatus) => true,
        (Caller::Programmatic, Route::SessionStatus | Route::SessionRevoke) => false,
        (Caller::Programmatic, _) => true,
        (
            Caller::Operator(_),
            Route::Health(_)
            | Route::Release(_)
            | Route::MutableJob(_)
            | Route::Update(_)
            | Route::StartUpdate
            | Route::Jobs { .. }
            | Route::Conversations { .. }
            | Route::ConversationSearch(..)
            | Route::Usage(_)
            | Route::UsageSummary(_)
            | Route::UsageRetries(_)
            | Route::ToolStats
            | Route::Availability(_)
            | Route::Logs(_)
            | Route::Subscription(_)
            | Route::Job(_)
            | Route::JobHistory { .. }
            | Route::JobState { .. }
            | Route::JobInput(_)
            | Route::ConversationChildren { .. }
            | Route::ConversationTurns { .. }
            | Route::JobLimits(_)
            | Route::ConversationContext { .. }
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
            | Route::ProfileAvailability(_)
            | Route::ProviderDelete(_)
            | Route::ActiveSystemSettings
            | Route::GlobalCompactionSettings
            | Route::SaveGlobalCompactionSettings
            | Route::GlobalOutboundHeaders
            | Route::SaveGlobalOutboundHeaders
            | Route::ProfileCreate
            | Route::ProfileUpdate(_)
            | Route::Projects { .. }
            | Route::ProjectCreate
            | Route::ProjectUpdate(_)
            | Route::ProjectDecision(_)
            | Route::Pools { .. }
            | Route::PoolUpdate(_)
            | Route::NativePoolArchive(_)
            | Route::JobEdit(_)
            | Route::Outputs { .. }
            | Route::LatestOutput(_)
            | Route::Events { .. }
            | Route::Admission
            | Route::DecisionSubmit
            | Route::DecideApproval(_)
            | Route::ConversationCards(_)
            | Route::JobAction(..)
            | Route::ConversationCompact(_)
            | Route::AttachmentUpload
            | Route::Attachment(_)
            | Route::AttachmentContent(_)
            | Route::SessionStatus
            | Route::SessionRevoke,
        ) => true,
        (Caller::Anonymous | Caller::BeginSession, _) => false,
    };
    if allowed { Ok(route) } else { Err(NOT_FOUND) }
}

pub(super) struct ApiRequest {
    pub(super) caller: Caller,
    pub(super) route: Route,
    pub(super) body_limit: usize,
}

type ParseRoute<'a> = &'a dyn Fn() -> Result<Route, Refusal>;

#[derive(Clone, Copy, Default)]
struct Endpoint<'a> {
    get: Option<ParseRoute<'a>>,
    post: Option<(usize, ParseRoute<'a>)>,
    delete: Option<ParseRoute<'a>>,
    session: bool,
}

impl<'a> Endpoint<'a> {
    fn get(mut self, parse: ParseRoute<'a>) -> Self {
        self.get = Some(parse);
        self
    }

    fn post(mut self, limit: usize, parse: ParseRoute<'a>) -> Self {
        self.post = Some((limit, parse));
        self
    }

    fn delete(mut self, parse: ParseRoute<'a>) -> Self {
        self.delete = Some(parse);
        self
    }
}

pub(super) fn route(
    request: &HttpRequest,
    admit: impl Fn(bool, Option<usize>) -> Result<Caller, Refusal>,
) -> Result<ApiRequest, Refusal> {
    let query = request.query_string();
    let segments = request
        .path()
        .strip_prefix("/api/v1/")
        .map(|path| path.split('/').collect::<Vec<_>>())
        .unwrap_or_default();
    let endpoint = Endpoint::default();
    let resolve = |endpoint: Endpoint<'_>| {
        let (limit, parse) = match (request.method(), endpoint.post, endpoint.delete) {
            (&Method::GET, _, _) => (None, endpoint.get),
            (&Method::POST, Some((limit, parse)), _) => (Some(limit), Some(parse)),
            (&Method::DELETE, _, Some(parse)) => (None, Some(parse)),
            _ if endpoint.session => return Err(SESSION_METHODS),
            (_, Some(_), Some(_)) => return Err(PROFILE_METHODS),
            (_, Some(_), None) => return Err(ONLY_POST),
            _ => return Err(METHOD_NOT_ALLOWED),
        };
        // Authentication and body-header checks precede ID and query parsing, even for unknown GETs.
        let caller = admit(endpoint.session, limit)?;
        let route = parse.ok_or(NOT_FOUND)?()?;
        if request.method() != Method::GET {
            no_query(query)?;
        }
        Ok(ApiRequest {
            caller,
            route,
            body_limit: limit.unwrap_or(0),
        })
    };
    let conversations = |archived| {
        catalog::conversation_query(query, false, archived)
            .map(|page| Route::Conversations { page, archived })
    };
    let get = |parse: ParseRoute<'_>| resolve(endpoint.get(parse));
    let post = |limit, parse: ParseRoute<'_>| resolve(endpoint.post(limit, parse));
    match segments.as_slice() {
        ["update"] => resolve(
            endpoint
                .get(&|| match query {
                    "" => Ok(Route::Update(false)),
                    "latest=1" => Ok(Route::Update(true)),
                    _ => Err(INVALID_REQUEST),
                })
                .post(update::UPDATE_BODY_LIMIT, &|| Ok(Route::StartUpdate)),
        ),
        ["health"] => get(&|| {
            if query.is_empty() {
                return Ok(Route::Health(None));
            }
            let mut parts = query
                .strip_prefix("after=")
                .ok_or(INVALID_REQUEST)?
                .split('.');
            let mut part = || {
                parts
                    .next()
                    .and_then(|s| u128::from_str_radix(s, 16).ok())
                    .ok_or(INVALID_REQUEST)
            };
            let cursor = (part()?, part()?, part()?);
            if parts.next().is_some() {
                return Err(INVALID_REQUEST);
            }
            Ok(Route::Health(Some(cursor)))
        }),
        ["job-history"] => get(&|| {
            dashboard::jobs_query(query).map(|(before, limit, terms)| Route::Jobs {
                before,
                limit,
                terms,
            })
        }),
        ["job-history", "events"] => get(&|| {
            subscriptions::JobsQuery::parse(query)
                .map(|query| Route::Subscription(subscriptions::Topic::Jobs(query)))
        }),
        ["catalog", "events"] => {
            get(&|| no_query(query).map(|()| Route::Subscription(subscriptions::Topic::Catalog)))
        }
        ["observation-changes", "events"] => get(&|| {
            subscriptions::change_cursor(query)
                .map(|cursor| Route::Subscription(subscriptions::Topic::ObservationChanges(cursor)))
        }),
        ["sidebar", "events"] => get(&|| {
            subscriptions::SidebarQuery::parse(query)
                .map(|query| Route::Subscription(subscriptions::Topic::Sidebar(query)))
        }),
        ["logs", "events"] => get(&|| {
            dashboard::LogQuery::parse(query)
                .map(|query| Route::Subscription(subscriptions::Topic::Logs(query)))
        }),
        ["usage", "events"] => get(&|| {
            dashboard::ObservationQuery::parse(query)
                .map(|query| Route::Subscription(subscriptions::Topic::Usage(query)))
        }),
        ["availability", "events"] => get(&|| {
            dashboard::ObservationQuery::parse(query)
                .map(|query| Route::Subscription(subscriptions::Topic::Availability(query)))
        }),
        ["usage"] => get(&|| dashboard::ObservationQuery::parse(query).map(Route::Usage)),
        ["usage", "summary"] => {
            get(&|| dashboard::UsageSummaryQuery::parse(query).map(Route::UsageSummary))
        }
        ["usage", "retries"] => {
            get(&|| dashboard::UsageRetriesQuery::parse(query).map(Route::UsageRetries))
        }
        ["tool-stats"] => get(&|| no_query(query).map(|()| Route::ToolStats)),
        ["availability"] => {
            get(&|| dashboard::ObservationQuery::parse(query).map(Route::Availability))
        }
        ["logs"] => get(&|| dashboard::LogQuery::parse(query).map(Route::Logs)),
        ["admissions"] => get(&|| no_query(query).map(|()| Route::Admission)),
        ["session"] => resolve(
            Endpoint {
                session: true,
                ..endpoint
            }
            .get(&|| no_query(query).map(|()| Route::SessionStatus))
            .delete(&|| Ok(Route::SessionRevoke)),
        ),
        ["settings", "system"] => get(&|| no_query(query).map(|()| Route::ActiveSystemSettings)),
        ["settings", "compaction"] => resolve(
            endpoint
                .get(&|| no_query(query).map(|()| Route::GlobalCompactionSettings))
                .post(MAX_SUBMISSION_BYTES, &|| {
                    Ok(Route::SaveGlobalCompactionSettings)
                }),
        ),
        ["settings", "outbound-headers"] => resolve(
            endpoint
                .get(&|| no_query(query).map(|()| Route::GlobalOutboundHeaders))
                .post(MAX_SUBMISSION_BYTES, &|| {
                    Ok(Route::SaveGlobalOutboundHeaders)
                }),
        ),
        ["profiles"] => resolve(
            endpoint
                .get(&|| {
                    catalog::agent_profile_query(query)
                        .map(|(before, limit)| Route::Profiles { before, limit })
                })
                .post(MAX_SUBMISSION_BYTES, &|| Ok(Route::ProfileCreate)),
        ),
        ["projects"] => resolve(
            endpoint
                .get(&|| {
                    projects::projects_query(query)
                        .map(|(before, limit)| Route::Projects { before, limit })
                })
                .post(MAX_SUBMISSION_BYTES, &|| Ok(Route::ProjectCreate)),
        ),
        ["pools"] => resolve(
            endpoint
                .get(&|| {
                    catalog::page_query(query, pools::pool_id)
                        .map(|(before, limit)| Route::Pools { before, limit })
                })
                .post(MAX_SUBMISSION_BYTES, &|| Ok(Route::NativePoolCreate)),
        ),
        ["pools", pool, "allocations"] => get(&|| {
            let pool = pools::pool_id(pool)?;
            pools::allocations_query(query).map(|(after, limit)| Route::PoolAllocations {
                pool,
                after,
                limit,
            })
        }),
        ["executors"] => resolve(
            endpoint
                .get(&|| {
                    catalog::page_query(query, pools::executor_id)
                        .map(|(before, limit)| Route::NativeExecutors { before, limit })
                })
                .post(MAX_SUBMISSION_BYTES, &|| Ok(Route::NativeExecutorCreate)),
        ),
        ["attachments", attachment] => get(&|| {
            let attachment = attachments::attachment_id(attachment)?;
            no_query(query).map(|()| Route::Attachment(attachment))
        }),
        ["attachments", attachment, "content"] => get(&|| {
            let attachment = attachments::attachment_id(attachment)?;
            no_query(query).map(|()| Route::AttachmentContent(attachment))
        }),
        ["jobs", job] => {
            let discard = || {
                Ok(Route::MutableJob(mutable_jobs::Action::Discard(job_id(
                    job,
                )?)))
            };
            let endpoint = Endpoint {
                delete: (!job.is_empty()).then_some(&discard as ParseRoute<'_>),
                ..endpoint
            };
            resolve(
                endpoint
                    .get(&|| {
                        if *job == "current" {
                            return no_query(query)
                                .map(|()| Route::MutableJob(mutable_jobs::Action::Current));
                        }
                        let id = job_id(job)?;
                        no_query(query).map(|()| Route::Job(id))
                    })
                    .post(MAX_SUBMISSION_BYTES, &|| {
                        Ok(Route::MutableJob(mutable_jobs::Action::Edit(job_id(job)?)))
                    }),
            )
        }
        ["jobs", job, "history"] => get(&|| {
            let job = job_id(job)?;
            let (after, limit) = jobs::history_query(query)?;
            Ok(Route::JobHistory { job, after, limit })
        }),
        ["jobs", job, "state"] => get(&|| {
            let job = job_id(job)?;
            jobs::state_query(query).map(|at_seq| Route::JobState { job, at_seq })
        }),
        ["jobs", job, "requests", number] => get(&|| {
            let job = job_id(job)?;
            let request = decimal::<u32>(number)
                .and_then(RequestNumber::new)
                .ok_or(INVALID_REQUEST)?;
            no_query(query).map(|()| Route::ProviderRequest(job, request))
        }),
        ["jobs", job, "responses"] => get(&|| {
            let job = job_id(job)?;
            let (after, limit, text_offset) = responses::responses_query(query)?;
            Ok(Route::ProviderResponses {
                job,
                after,
                limit,
                text_offset,
            })
        }),
        ["jobs", job, "input"] => get(&|| {
            let job = job_id(job)?;
            no_query(query).map(|()| Route::JobInput(job))
        }),
        ["conversations", conversation, "cards"] => get(&|| {
            no_query(query).and_then(|()| job_id(conversation).map(Route::ConversationCards))
        }),
        ["conversations", conversation, "children"] => get(&|| {
            let conversation = job_id(conversation)?;
            let (before, limit) = catalog::page_query(query, job_id)?;
            Ok(Route::ConversationChildren {
                conversation,
                before,
                limit,
            })
        }),
        ["conversations", conversation, "context"] => get(&|| {
            let conversation = job_id(conversation)?;
            let (before, limit) = catalog::page_query(query, job_id)?;
            Ok(Route::ConversationContext {
                conversation,
                before,
                limit,
            })
        }),
        ["conversations", conversation, "limits"] => {
            get(&|| no_query(query).and_then(|()| job_id(conversation).map(Route::JobLimits)))
        }
        ["conversations", conversation, "turns"] => get(&|| {
            let conversation = job_id(conversation)?;
            let (before, limit) = catalog::page_query(query, job_id)?;
            Ok(Route::ConversationTurns {
                conversation,
                before,
                limit,
            })
        }),
        ["agent-profiles"] => get(&|| {
            catalog::agent_profile_query(query)
                .map(|(before, limit)| Route::AgentProfiles { before, limit })
        }),
        ["jobs", job, "outputs", "latest"] if query.is_empty() => {
            get(&|| Ok(Route::LatestOutput(job_id(job)?)))
        }
        ["jobs", job, "outputs"] => get(&|| {
            let job = job_id(job)?;
            let (after_seq, limit) = output_query(query)?;
            Ok(Route::Outputs {
                job,
                after_seq,
                limit,
            })
        }),
        ["jobs", job, "tools"] => get(&|| {
            let job = job_id(job)?;
            let (after, limit) = tools::tools_query(query)?;
            Ok(Route::Tools { job, after, limit })
        }),
        ["jobs", job, "tools", request, index] if query.is_empty() => get(&|| {
            Ok(Route::Tool {
                job: job_id(job)?,
                request: decimal::<u32>(request)
                    .and_then(RequestNumber::new)
                    .ok_or(INVALID_REQUEST)?,
                index: decimal::<u32>(index).ok_or(INVALID_REQUEST)?,
            })
        }),
        ["jobs", job, "tree"] => get(&|| {
            let job = job_id(job)?;
            let (after, limit, depth) = catalog::tree_query(query)?;
            Ok(Route::JobTree {
                job,
                after,
                limit,
                depth,
            })
        }),
        ["jobs", job, "events"] => get(&|| {
            let job = job_id(job)?;
            let after_seq = events::cursor(query, request.headers())?;
            Ok(Route::Events { job, after_seq })
        }),
        ["conversations"] => get(&|| conversations(false)),
        ["conversations", job] if !job.is_empty() => resolve(
            endpoint
                .get(&|| match *job {
                    "archived" => conversations(true),
                    "search" => catalog::conversation_query(query, true, false)
                        .map(Route::ConversationSearch),
                    _ => Err(NOT_FOUND),
                })
                .post(MAX_SUBMISSION_BYTES, &|| Ok(Route::JobEdit(job_id(job)?))),
        ),
        ["jobs", job, "withdraw"] => post(MAX_SUBMISSION_BYTES, &|| {
            Ok(Route::MutableJob(mutable_jobs::Action::Withdraw(job_id(
                job,
            )?)))
        }),
        ["jobs", job, "cancel-unsupported"] => post(MAX_SUBMISSION_BYTES, &|| {
            Ok(Route::MutableJob(mutable_jobs::Action::CancelQueued(
                job_id(job)?,
            )))
        }),
        ["jobs", job, "release"] => {
            post(MAX_SUBMISSION_BYTES, &|| Ok(Route::Release(job_id(job)?)))
        }
        ["jobs", job, "stop"] => post(MAX_SUBMISSION_BYTES, &|| {
            Ok(Route::MutableJob(mutable_jobs::Action::Stop(job_id(job)?)))
        }),
        ["jobs"] => post(MAX_SUBMISSION_BYTES, &|| {
            Ok(Route::MutableJob(mutable_jobs::Action::Create))
        }),
        ["jobs", job, "submit"] => post(MAX_SUBMISSION_BYTES, &|| {
            Ok(Route::MutableJob(mutable_jobs::Action::Submit(job_id(
                job,
            )?)))
        }),
        ["decisions"] => post(MAX_SUBMISSION_BYTES, &|| Ok(Route::DecisionSubmit)),
        ["approvals", job, request, index]
            if !job.is_empty() && !request.is_empty() && !index.is_empty() =>
        {
            post(MAX_SUBMISSION_BYTES, &|| {
                Ok(Route::DecideApproval(crate::db::entities::ToolCallRef {
                    job: job_id(job)?,
                    request: decimal::<u32>(request)
                        .and_then(RequestNumber::new)
                        .ok_or(INVALID_REQUEST)?,
                    index: decimal::<u32>(index).ok_or(INVALID_REQUEST)?,
                }))
            })
        }
        ["conversations", conversation, "compact"] if !conversation.is_empty() => {
            post(MAX_SUBMISSION_BYTES, &|| {
                Ok(Route::ConversationCompact(job_id(conversation)?))
            })
        }
        ["profiles", profile, "availability"] if !profile.is_empty() => {
            post(MAX_SUBMISSION_BYTES, &|| {
                Ok(Route::ProfileAvailability(profiles::profile_id(profile)?))
            })
        }
        ["provider-models"] => post(MAX_SUBMISSION_BYTES, &|| Ok(Route::ProviderModels)),
        ["profiles", profile] if !profile.is_empty() => resolve(
            endpoint
                .post(MAX_SUBMISSION_BYTES, &|| {
                    Ok(if *profile == "import" {
                        Route::ProfileImport
                    } else {
                        Route::ProfileUpdate(profiles::profile_id(profile)?)
                    })
                })
                .delete(&|| Ok(Route::ProfileDelete(profiles::profile_id(profile)?))),
        ),
        ["projects", project, "decision"] if !project.is_empty() => {
            post(MAX_SUBMISSION_BYTES, &|| {
                Ok(Route::ProjectDecision(projects::project_id(project)?))
            })
        }
        ["projects", project] if !project.is_empty() => post(MAX_SUBMISSION_BYTES, &|| {
            Ok(Route::ProjectUpdate(projects::project_id(project)?))
        }),
        ["pools", pool, "archive"] if !pool.is_empty() => post(MAX_SUBMISSION_BYTES, &|| {
            Ok(Route::NativePoolArchive(pools::pool_id(pool)?))
        }),
        ["pools", pool] if !pool.is_empty() => post(MAX_SUBMISSION_BYTES, &|| {
            Ok(Route::PoolUpdate(pools::pool_id(pool)?))
        }),
        ["executors", executor] if !executor.is_empty() => post(MAX_SUBMISSION_BYTES, &|| {
            Ok(Route::NativeExecutorUpdate(pools::executor_id(executor)?))
        }),
        ["executors", executor, "archive"] if !executor.is_empty() => {
            post(MAX_SUBMISSION_BYTES, &|| {
                Ok(Route::NativeExecutorArchive(pools::executor_id(executor)?))
            })
        }
        ["attachments"] => post(MAX_ATTACHMENT_UPLOAD_BYTES, &|| Ok(Route::AttachmentUpload)),
        ["conversations", conversation, action] if !conversation.is_empty() => {
            match ["stop", "close", "hold", "unhold", "retry"]
                .into_iter()
                .find(|name| name == action)
            {
                Some(name) => post(MAX_SUBMISSION_BYTES, &|| {
                    Ok(Route::JobAction(job_id(conversation)?, name))
                }),
                None => resolve(endpoint),
            }
        }
        ["providers", id] if !id.is_empty() => {
            resolve(endpoint.delete(&|| Ok(Route::ProviderDelete(profiles::profile_id(id)?))))
        }
        _ => resolve(endpoint),
    }
}

fn no_query(query: &str) -> Result<(), Refusal> {
    if query.is_empty() {
        Ok(())
    } else {
        Err(INVALID_REQUEST)
    }
}

pub(super) fn decimal<T: std::str::FromStr>(text: &str) -> Option<T> {
    let valid = !text.is_empty()
        && text.bytes().all(|byte| byte.is_ascii_digit())
        && (text == "0" || !text.starts_with('0'));
    valid.then(|| text.parse().ok()).flatten()
}

pub(super) fn job_id(text: &str) -> Result<JobId, Refusal> {
    crate::db::entities::parse_entity_id(text)
        .map(JobId::new)
        .ok_or(INVALID_REQUEST)
}

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
    payload: LimitedBody,
    shared: &Arc<Shared>,
) -> Result<Reply, Refusal> {
    let database = &shared.database;
    match route {
        Route::Health(after) => Ok(Reply::ok(health(database, after).await)),
        Route::Release(id) => mutable_jobs::release(id, payload, database).await,
        Route::MutableJob(action) => {
            mutable_jobs::respond(action, request, payload, database, &shared.native_stops).await
        }
        Route::Update(latest) => update::get(latest).await,
        Route::StartUpdate => {
            let body = payload.read().await?;
            update::post(database, &body).await
        }
        Route::Jobs {
            before,
            limit,
            terms,
        } => dashboard::jobs(shared, before, limit, terms).await,
        Route::Conversations { page, archived } => {
            catalog::conversations(database, page.before, page.limit, archived, page.scope).await
        }
        Route::ConversationSearch(page) => {
            catalog::search_conversations(database, page.scope, page.terms, page.before, page.limit)
                .await
        }
        Route::Usage(query) => dashboard::usage(shared, query).await,
        Route::UsageSummary(query) => dashboard::usage_summary(shared, query).await,
        Route::UsageRetries(query) => dashboard::usage_retries(shared, query).await,
        Route::ToolStats => dashboard::tool_stats(shared).await,
        Route::Availability(query) => dashboard::availability(shared, query).await,
        Route::Logs(query) => dashboard::logs(shared, query).await,
        Route::Job(job) => {
            let context = mutable_jobs::optional_context(request)?;
            match database.mutable_job_details(job, context).await? {
                Some(details) => {
                    let job_view = jobs::mutable_job_view(
                        &details.record,
                        details.allocation.as_ref(),
                        details.activity.last_activity_at_us(),
                    );
                    let mut body = envelope(serde_json::json!(jobs::JobDetailsView {
                        job: job_view,
                        result: details.result.as_ref().map(outputs::output_view),
                        usage: details.usage,
                        activity: details.activity,
                        task_plan: None,
                        unknown_reason: details.unknown_reason,
                        allocation: details.allocation.as_ref().map(jobs::allocation_view),
                    }));
                    body["revision"] = serde_json::json!(details.record.job.revision);
                    body["phase"] = serde_json::json!(mutable_jobs::phase(&details.record));
                    body["project"] = serde_json::json!(
                        details.record.project.map(|project| project.to_string())
                    );
                    mutable_jobs::add_editable_state(
                        &mut body,
                        &details.record,
                        details.unsupported_decision_history,
                    )?;
                    Ok(Reply::ok(body))
                }
                None => Err(JOB_NOT_FOUND),
            }
        }
        Route::JobInput(job) => catalog::job_input(database, job, request).await,
        Route::JobHistory { job, after, limit } => {
            jobs::history(shared, job, after, limit, request).await
        }
        Route::JobState { job, at_seq } => jobs::state(shared, job, at_seq, request).await,
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
            let body = payload.read().await?;
            profiles::provider_models(database, &body).await
        }
        Route::ProfileImport => {
            let body = payload.read().await?;
            profiles::import_models(database, &body).await
        }
        Route::ActiveSystemSettings => Ok(Reply::ok(envelope(serde_json::json!(
            shared.active_system_settings
        )))),
        Route::GlobalCompactionSettings => {
            profiles::global_compaction_settings(database, None).await
        }
        Route::SaveGlobalCompactionSettings => {
            let body = payload.read().await?;
            profiles::global_compaction_settings(database, Some(&body)).await
        }
        Route::GlobalOutboundHeaders => profiles::global_outbound_headers(database, None).await,
        Route::SaveGlobalOutboundHeaders => {
            let body = payload.read().await?;
            profiles::global_outbound_headers(database, Some(&body)).await
        }
        Route::ProfileDelete(id) => profiles::delete_model(database, id).await,
        Route::ProviderDelete(id) => profiles::remove_provider(database, id).await,
        Route::ProfileAvailability(id) => {
            let body = payload.read().await?;
            profiles::set_model_availability(database, id, &body).await
        }
        Route::ProfileCreate => {
            let body = payload.read().await?;
            profiles::save_profile(database, None, &body).await
        }
        Route::ProfileUpdate(profile) => {
            let body = payload.read().await?;
            profiles::save_profile(database, Some(profile), &body).await
        }
        Route::Projects { before, limit } => projects::list(database, before, limit).await,
        Route::ProjectCreate => {
            let body = payload.read().await?;
            projects::create(shared, &body).await
        }
        Route::ProjectDecision(project) => {
            let body = payload.read().await?;
            projects::configure_decision(shared, project, &body).await
        }
        Route::ProjectUpdate(project) => {
            let body = payload.read().await?;
            projects::update(database, project, &body).await
        }
        Route::Pools { before, limit } => pools::list(database, before, limit).await,
        Route::PoolAllocations { pool, after, limit } => {
            pools::list_allocations(database, pool, after, limit).await
        }
        Route::NativePoolCreate => {
            let body = payload.read().await?;
            pools::create_pool(database, &body).await
        }
        Route::PoolUpdate(pool) => {
            let body = payload.read().await?;
            pools::update(database, pool, &body).await
        }
        Route::NativePoolArchive(pool) => {
            let body = payload.read().await?;
            pools::archive_pool(database, pool, &body).await
        }
        Route::NativeExecutors { before, limit } => {
            pools::list_executors(database, before, limit).await
        }
        Route::NativeExecutorCreate => {
            let body = payload.read().await?;
            pools::create_executor(database, &body).await
        }
        Route::NativeExecutorUpdate(executor) => {
            let body = payload.read().await?;
            pools::update_executor(database, executor, &body).await
        }
        Route::NativeExecutorArchive(executor) => {
            let body = payload.read().await?;
            pools::archive_executor(database, executor, &body).await
        }
        Route::JobEdit(job) => {
            let body = payload.read().await?;
            catalog::edit_conversation(shared, job, &body).await
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
        Route::DecisionSubmit => {
            let key = submissions::idempotency_key(request.headers())?;
            let body = payload.read().await?;
            let decision = submissions::DecisionJobRequest::parse(&body)?;
            submissions::submit_decision(shared, key, decision).await
        }
        Route::Admission => {
            let key = submissions::idempotency_key(request.headers())?;
            submissions::lookup(shared, key).await
        }
        Route::DecideApproval(approval) => {
            let body = payload.read().await?;
            submissions::decide(shared, approval, &body).await
        }
        Route::ConversationCards(conversation) => submissions::cards(shared, conversation).await,
        Route::ConversationCompact(conversation) => {
            let key = submissions::idempotency_key(request.headers())?;
            let body = payload.read().await?;
            submissions::compact(shared, conversation, key, &body).await
        }
        Route::JobAction(conversation, action) => {
            let body = payload.read().await?;
            submissions::action(shared, conversation, action, &body).await
        }
        Route::AttachmentUpload => {
            let body = payload.read().await?;
            attachments::upload(database, &body).await
        }
        Route::Attachment(attachment) => attachments::metadata(database, attachment).await,
        Route::SessionStatus => match caller {
            Caller::Operator(session) => Ok(Reply::ok(browser::described(session))),
            Caller::BeginSession => shared.browser.begin_session(),
            Caller::Anonymous => Ok(browser::signed_out(!matches!(
                browser::presented(request.headers()),
                browser::Presented::None
            ))),
            Caller::Programmatic => Err(NOT_FOUND),
        },
        Route::SessionRevoke => match caller {
            Caller::Operator(session) => {
                Ok(shared.browser.logout(session, shared.token.ui_auth_mode))
            }
            Caller::Programmatic | Caller::Anonymous | Caller::BeginSession => Err(NOT_FOUND),
        },
        Route::Events { .. } | Route::Subscription(_) => Err(INTERNAL),
        Route::AttachmentContent(_) => Err(INTERNAL),
    }
}
