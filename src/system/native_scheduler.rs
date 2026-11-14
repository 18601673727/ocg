use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use crate::db::entities::{
    AllocationPhase, AllocationToken, ConfigId, DevelopmentFreshReservation,
    DevelopmentReadyCursor, DevelopmentReservation, ExecutionOperation, ExecutionPoolId,
    ExecutorId, FrozenExecutionConfig, JobId, LlmTransport, NativeDecisionConfig, OutcomeKind,
    PoolAllocation, UnstartedCause,
};
use crate::db::redb::{NativeResource, NativeResourceKind};
use crate::db::{
    DatabaseError, DatabaseHandle, DevelopmentHandoffError, DevelopmentReservationError,
    ScheduledReservation, SchedulerWakeSubscription,
};
use crate::service::ai::providers::{CancellationToken, ProviderExecutionConfig};
use crate::system::execution_controls::ExecutionControl;
use crate::system::executor::blocking::Permit;
use crate::system::executor::execution_runtime::{
    EndpointPolicy, WorkspaceTools, supervise_development_decision,
    supervise_development_generative,
};
use crate::system::executor::native_tools::NativeTooling;
use crate::system::external_approval::MAX_TIMEOUT_CEILING;
use crate::system::scheduler::{ClaimScope, DispatchBatch, Scheduler};

const EXECUTOR_PAGE: usize = 100;
const MAX_EXECUTOR_PAGES: usize = 16;
const MAX_READY_PAGES: usize = 8;
const HEAD_PAGES: usize = 2;
const DISPATCH_TIMEOUT: Duration = MAX_TIMEOUT_CEILING;
const MAX_RESPONSE_BYTES: usize = 1 << 20;
const GENERATIVE_CAPABILITY: (ExecutionOperation, Option<LlmTransport>) = (
    ExecutionOperation::LlmRequest,
    Some(LlmTransport::OpenAiChatCompletions),
);
const DECISION_CAPABILITY: (ExecutionOperation, Option<LlmTransport>) =
    (ExecutionOperation::Decision, None);

pub(crate) struct SchedulingAuthority(());

impl SchedulingAuthority {
    pub(crate) fn host_autonomous(database: &DatabaseHandle, scope: ClaimScope) -> Option<Self> {
        (scope == ClaimScope::Stored
            && ExecutionControl::AutonomousScheduling.is_enabled()
            && database.status() == crate::db::DatabaseStatus::Open)
            .then_some(Self(()))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LoopbackEligibility {
    pub(crate) code: &'static str,
    pub(crate) detail: &'static str,
    pub(crate) endpoint: Option<crate::system::http::LoopbackEndpointClass>,
}

pub(crate) const NOT_ASSESSED: [&str; 7] = [
    "conversation_start_context",
    "predecessor_settlement",
    "attachment_availability",
    "executor_capacity",
    "autonomous_scheduling_control_compiled",
    "host_armed_activation",
    "reservation_and_writer_admission",
];

impl LoopbackEligibility {
    pub(crate) fn eligible(self) -> bool {
        self.code == "eligible"
    }

    pub(crate) fn to_json(self) -> serde_json::Value {
        serde_json::json!({
            "eligible": self.eligible(),
            "code": self.code,
            "detail": self.detail,
            "endpoint_class": self.endpoint.map(crate::system::http::LoopbackEndpointClass::name),
            "scope": "autonomous_loopback_dispatch_policy",
            "not_assessed": NOT_ASSESSED,
        })
    }
}

pub(crate) fn loopback_eligibility(
    frozen: Option<&crate::db::entities::MutableFrozenConfiguration>,
) -> LoopbackEligibility {
    use crate::db::entities::MutableFrozenConfiguration;
    use crate::system::http::{LoopbackEndpointClass, classify_loopback_endpoint};
    let none = |code, detail| LoopbackEligibility {
        code,
        detail,
        endpoint: None,
    };
    let (endpoint, kind_label) = match frozen {
        None => {
            return none(
                "not_frozen",
                "the Job has no frozen configuration; only a submitted Job is scheduled",
            );
        }
        Some(MutableFrozenConfiguration::Decision(decision)) => {
            (decision.endpoint.as_deref(), "native Decision")
        }
        Some(MutableFrozenConfiguration::Generative(frozen)) => {
            if frozen.native_execution_refusal().is_some() {
                return none(
                    "not_single_request_generative",
                    "the frozen configuration cannot run as a native Generative execution (a chat or coding Agent, the OpenAI Chat Completions protocol and a declared context window are required)",
                );
            }
            (frozen.llm.provider.endpoint.as_deref(), "native Generative")
        }
    };
    let Some(endpoint) = endpoint else {
        return none("endpoint_missing", "the frozen Provider has no endpoint");
    };
    if ExecutionControl::ExternalProviderTransport.is_enabled() {
        return none(
            "external_transport_enabled",
            "this build enables external Provider transport, under which the loopback dispatch admits no endpoint",
        );
    }
    let class = classify_loopback_endpoint(endpoint);
    let detail = match class {
        LoopbackEndpointClass::LiteralDirect => {
            let detail = match kind_label {
                "native Decision" => {
                    "the frozen endpoint is a literal http://127.0.0.1:<port> endpoint no proxy intercepts, and the frozen configuration is a native Decision execution; this is not a claim that the Job will run: see not_assessed for what the scheduler also asks"
                }
                _ => {
                    "the frozen endpoint is a literal http://127.0.0.1:<port> endpoint no proxy intercepts, and the frozen configuration is a native Generative execution; this is not a claim that the Job will run: see not_assessed for what the scheduler also asks"
                }
            };
            return LoopbackEligibility {
                code: "eligible",
                detail,
                endpoint: Some(class),
            };
        }
        LoopbackEndpointClass::LiteralProxied => {
            "the frozen endpoint is literal 127.0.0.1 but the configured proxy would intercept it; exclude it with NO_PROXY=127.0.0.1 or unset the proxy for the host, then resubmit"
        }
        LoopbackEndpointClass::LiteralMalformed => {
            "the frozen endpoint names 127.0.0.1 but is not http://127.0.0.1:<explicit non-zero port> without credentials or a query; freeze the Provider endpoint in that form and resubmit"
        }
        LoopbackEndpointClass::LocalhostName => {
            "the frozen endpoint names `localhost`, which configuration accepts but autonomous loopback scheduling never resolves; freeze the Provider endpoint as http://127.0.0.1:<port> and resubmit, or run the Job through a separately authorized workflow"
        }
        LoopbackEndpointClass::Ipv6Loopback => {
            "the frozen endpoint is the IPv6 loopback `::1`, which configuration accepts but autonomous loopback scheduling never reaches; freeze the Provider endpoint as http://127.0.0.1:<port> and resubmit, or run the Job through a separately authorized workflow"
        }
        LoopbackEndpointClass::External => {
            "the frozen endpoint is not a loopback endpoint; loopback dispatch never reaches it, and an external destination is admitted only by an active scheduled external policy and grant under scheduled_external_transport, or by a manual external execution"
        }
        LoopbackEndpointClass::Unparseable => "the frozen endpoint is not a URI",
    };
    LoopbackEligibility {
        code: "endpoint_not_literal_loopback",
        detail,
        endpoint: Some(class),
    }
}

fn scheduled_policy(frozen: &FrozenExecutionConfig) -> bool {
    frozen
        .llm
        .provider
        .endpoint
        .as_deref()
        .is_some_and(crate::system::http::direct_diagnostic_loopback)
        || ExecutionControl::ScheduledExternalTransport.is_enabled()
}

fn scheduled_decision_policy(native: &NativeDecisionConfig) -> bool {
    native
        .endpoint
        .as_deref()
        .is_some_and(crate::system::http::direct_diagnostic_loopback)
}

#[derive(Clone, Default)]
pub(crate) struct NativeStops(Arc<Mutex<BTreeMap<JobId, (AllocationToken, CancellationToken)>>>);

impl std::fmt::Debug for NativeStops {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("NativeStops")
    }
}

impl NativeStops {
    fn register(&self, token: AllocationToken, cancellation: CancellationToken) -> Registration {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(token.job, (token, cancellation));
        Registration {
            stops: self.clone(),
            token,
        }
    }

    pub(crate) fn cancel(&self, token: AllocationToken) -> bool {
        match self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&token.job)
        {
            Some((registered, cancellation)) if *registered == token => {
                cancellation.cancel();
                true
            }
            Some(_) | None => false,
        }
    }
}

struct Registration {
    stops: NativeStops,
    token: AllocationToken,
}

impl Drop for Registration {
    fn drop(&mut self) {
        let mut stops = self.stops.0.lock().unwrap_or_else(PoisonError::into_inner);
        if stops
            .get(&self.token.job)
            .is_some_and(|(registered, _)| *registered == self.token)
        {
            stops.remove(&self.token.job);
        }
    }
}

pub(crate) async fn run(
    database: DatabaseHandle,
    scope: ClaimScope,
    wake: SchedulerWakeSubscription,
    shutdown: CancellationToken,
    stops: NativeStops,
    workspace_tools: Option<crate::system::executor::execution_runtime::WorkspaceTools>,
) {
    let Some(authority) = SchedulingAuthority::host_autonomous(&database, scope) else {
        shutdown.cancelled().await;
        return;
    };
    run_with(authority, database, wake, shutdown, stops, workspace_tools).await;
}

pub(crate) async fn run_with(
    authority: SchedulingAuthority,
    database: DatabaseHandle,
    wake: SchedulerWakeSubscription,
    shutdown: CancellationToken,
    stops: NativeStops,
    workspace_tools: Option<crate::system::executor::execution_runtime::WorkspaceTools>,
) {
    log::info!("native scheduler running: native Generative Jobs, loopback endpoints only");
    if let Err(error) = database.settle_unreported_children().await {
        log::warn!("native scheduler: unreported child conversations were not settled ({error})");
    }
    let scheduler = Rc::new(NativeScheduler::new(
        database.clone(),
        authority,
        shutdown.clone(),
        stops,
        workspace_tools,
    ));
    Scheduler::new(&database, wake, shutdown)
        .run(
            |database_changed| {
                let scheduler = scheduler.clone();
                async move { scheduler.claim(database_changed).await }
            },
            |allocation| {
                let scheduler = scheduler.clone();
                async move { scheduler.spawn(allocation) }
            },
        )
        .await;
    scheduler.drain().await;
    log::info!("native scheduler stopped");
}

pub(crate) enum ExecutorPass {
    Reserved(Box<PoolAllocation>),
    Nothing,
    BudgetSpent,
    Rescan,
    IndexExhausted,
    Halted,
}

#[derive(Clone, Copy, Default)]
struct Discovery {
    next: Option<ConfigId>,
    sweep: Option<Sweep>,
}

#[derive(Clone, Copy)]
struct Sweep {
    origin: Option<ConfigId>,
    wrapped: bool,
}

impl Discovery {
    fn advanced(self, reached: Option<ConfigId>) -> Self {
        let sweep = self.sweep.unwrap_or(Sweep {
            origin: self.next,
            wrapped: false,
        });
        let complete = match (reached, sweep.origin) {
            (None, origin) => origin.is_none() || sweep.wrapped,
            (Some(reached), Some(origin)) => sweep.wrapped && reached.get() <= origin.get(),
            (Some(_), None) => false,
        };
        Self {
            next: reached,
            sweep: (!complete).then_some(Sweep {
                origin: sweep.origin,
                wrapped: sweep.wrapped || reached.is_none(),
            }),
        }
    }
}

pub(crate) struct NativeScheduler {
    database: DatabaseHandle,
    _authority: SchedulingAuthority,
    shutdown: CancellationToken,
    stops: NativeStops,
    // A scan may skip Jobs that another capability set or a file permit admits.
    // Two capability bits and one permit bit bound the independent cursors.
    resume: Cell<[Option<DevelopmentReadyCursor>; 8]>,
    // A changed queue may make a Job eligible in a previously scanned segment.
    rescan: Cell<u8>,
    discovery: Cell<Discovery>,
    turns: Cell<u64>,
    workspace_tools: Option<crate::system::executor::execution_runtime::WorkspaceTools>,
    running: RefCell<Vec<ntex::rt::JoinHandle<Dispatched>>>,
    halted: Rc<Cell<bool>>,
}

impl NativeScheduler {
    pub(crate) fn new(
        database: DatabaseHandle,
        authority: SchedulingAuthority,
        shutdown: CancellationToken,
        stops: NativeStops,
        workspace_tools: Option<crate::system::executor::execution_runtime::WorkspaceTools>,
    ) -> Self {
        Self {
            workspace_tools,
            database,
            _authority: authority,
            shutdown,
            stops,
            resume: Cell::new([None; 8]),
            rescan: Cell::new(0),
            discovery: Cell::new(Discovery::default()),
            turns: Cell::new(0),
            running: RefCell::new(Vec::new()),
            halted: Rc::new(Cell::new(false)),
        }
    }

    fn halt(&self, reason: &str) {
        if !self.halted.replace(true) {
            log::error!(
                "native scheduler stopped claiming: {reason}; run `ocg inspect-held` once the host has stopped; nothing is retried, reconciled or released"
            );
        }
    }

    pub(crate) async fn claim(
        &self,
        database_changed: bool,
    ) -> Result<DispatchBatch<(PoolAllocation, Option<Permit>)>, DatabaseError> {
        if database_changed {
            let mut rescan = self.rescan.get();
            for (index, cursor) in self.resume.get().iter().enumerate() {
                if cursor.is_some() {
                    rescan |= 1 << index;
                }
            }
            self.rescan.set(rescan);
        }
        let mut grants = Vec::new();
        let (finished, running): (Vec<_>, Vec<_>) = std::mem::take(&mut *self.running.borrow_mut())
            .into_iter()
            .partition(|task| task.is_finished());
        *self.running.borrow_mut() = running;
        for task in finished {
            if task.await.is_err() {
                self.halt("a dispatch ended without a report");
            }
        }
        if crate::system::executor::blocking::groups_tracked()
            && let Err(error) =
                ntex::rt::spawn_blocking(crate::system::executor::blocking::retry_unproven_groups)
                    .await
        {
            log::warn!("failed to retry unproven groups on a blocking worker: {error}");
        }
        if self.halted.get() || self.shutdown.is_cancelled() {
            return Ok(DispatchBatch {
                grants,
                next_due: None,
                wait_for_blocking: false,
            });
        }
        let recurrence_due = self.database.claim_due_recurrences(16).await?;
        let discovery = self.discovery.get();
        let mut executors = Vec::new();
        let mut before = discovery.next;
        let mut reached = None;
        for _ in 0..MAX_EXECUTOR_PAGES {
            if self.shutdown.is_cancelled() {
                return Ok(DispatchBatch {
                    grants,
                    next_due: None,
                    wait_for_blocking: false,
                });
            }
            let page = self
                .database
                .native_resources(before, EXECUTOR_PAGE, NativeResourceKind::Executor)
                .await?;
            for resource in page.records {
                if let NativeResource::Executor(executor) = resource
                    && executor.enabled
                    && !executor.archived
                    && (executor.capabilities.contains(&GENERATIVE_CAPABILITY)
                        || executor.capabilities.contains(&DECISION_CAPABILITY))
                {
                    let capabilities =
                        usize::from(executor.capabilities.contains(&GENERATIVE_CAPABILITY))
                            | (usize::from(executor.capabilities.contains(&DECISION_CAPABILITY))
                                << 1);
                    executors.push((executor.pool, executor.id, capabilities));
                }
            }
            reached = page.next;
            match page.next {
                Some(next) => before = Some(next),
                None => break,
            }
        }
        let turn = self.turns.get();
        if !executors.is_empty() {
            let start = usize::try_from(turn % executors.len() as u64).unwrap_or(0);
            executors.rotate_left(start);
        }
        let mut rotating = false;
        let mut revisit = false;
        let mut wait_for_blocking = false;
        let mut exhausted = 0u8;
        for (pool, executor, capabilities) in executors {
            if self.halted.get() || self.shutdown.is_cancelled() {
                break;
            }
            let permit = Permit::try_file();
            let workspace_permit = permit.is_some();
            wait_for_blocking |= !workspace_permit;
            let scan = capabilities | (usize::from(workspace_permit) << 2);
            if exhausted & (1 << scan) != 0 {
                continue;
            }
            match self
                .reserve_for(pool, executor, workspace_permit, scan)
                .await
            {
                ExecutorPass::Reserved(allocation) => grants.push((*allocation, permit)),
                ExecutorPass::Nothing => {}
                ExecutorPass::BudgetSpent => rotating = true,
                ExecutorPass::Rescan => {
                    exhausted |= 1 << scan;
                    revisit = true;
                }
                ExecutorPass::IndexExhausted => exhausted |= 1 << scan,
                ExecutorPass::Halted => break,
            }
        }
        let discovery = discovery.advanced(reached);
        self.discovery.set(discovery);
        if reached.is_none() {
            self.turns.set(turn.wrapping_add(1));
        }
        let dispatch_due = ((revisit
            || rotating && self.resume.get().iter().any(Option::is_some)
            || discovery.sweep.is_some())
            && !self.halted.get())
        .then_some(Duration::ZERO);
        let next_due = match (dispatch_due, recurrence_due) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        Ok(DispatchBatch {
            grants,
            next_due,
            wait_for_blocking,
        })
    }

    pub(crate) async fn reserve_for(
        &self,
        pool: ExecutionPoolId,
        executor: ExecutorId,
        workspace_permit: bool,
        scan: usize,
    ) -> ExecutorPass {
        let resume = self.resume.get()[scan];
        let mut after: Option<DevelopmentReadyCursor> = None;
        for page in 0..MAX_READY_PAGES {
            if page == HEAD_PAGES
                && let Some(resume) = resume
                && after.is_none_or(|reached| reached.0 < resume.0)
            {
                after = Some(resume);
            }
            let fresh = DevelopmentFreshReservation::new(pool, executor, after);
            let passed = self
                .database
                .reserve_scheduled_job(
                    fresh,
                    scheduled_policy,
                    scheduled_decision_policy,
                    workspace_permit,
                )
                .await
                .map(|ScheduledReservation::Pass(passed)| *passed);
            match passed {
                Ok(DevelopmentReservation::Reserved(allocation)) => {
                    if allocation.token.executor != executor || allocation.token.pool != pool {
                        self.halt("a reservation names another Executor");
                        return ExecutorPass::Halted;
                    }
                    return ExecutorPass::Reserved(Box::new(allocation));
                }
                Ok(DevelopmentReservation::Continue(cursor)) => after = Some(cursor),
                Ok(DevelopmentReservation::Exhausted) => {
                    let mut cursors = self.resume.get();
                    cursors[scan] = None;
                    self.resume.set(cursors);
                    let rescan = self.rescan.get();
                    self.rescan.set(rescan & !(1 << scan));
                    return if rescan & (1 << scan) != 0 {
                        ExecutorPass::Rescan
                    } else {
                        ExecutorPass::IndexExhausted
                    };
                }
                Ok(DevelopmentReservation::Replayed(_) | DevelopmentReservation::Unresolved) => {
                    self.halt("a first reservation attempt was answered as a replay");
                    return ExecutorPass::Halted;
                }
                Err(DevelopmentReservationError::DefinitelyNotCommitted { error, .. }) => {
                    log::debug!("native scheduler: Executor {executor} reserved nothing: {error}");
                    return ExecutorPass::Nothing;
                }
                Err(DevelopmentReservationError::Other(error)) => {
                    self.halt(&format!(
                        "a reservation on Executor {executor} may have committed without a reply ({error})"
                    ));
                    return ExecutorPass::Halted;
                }
            }
        }
        if after.is_some_and(|reached| resume.is_none_or(|resume| reached.0 > resume.0)) {
            let mut cursors = self.resume.get();
            cursors[scan] = after;
            self.resume.set(cursors);
        }
        ExecutorPass::BudgetSpent
    }

    pub(crate) fn spawn(&self, grant: (PoolAllocation, Option<Permit>)) {
        let (allocation, permit) = grant;
        let database = self.database.clone();
        let cancellation = self.shutdown.child();
        let registration = self.stops.register(allocation.token, cancellation.clone());
        let halted = self.halted.clone();
        let workspace_tools = self.workspace_tools.clone();
        let stops = self.stops.clone();
        let task = ntex::rt::spawn(async move {
            let _registration = registration;
            let dispatched = dispatch(
                &database,
                allocation,
                &cancellation,
                workspace_tools.as_ref(),
                &stops,
                permit,
            )
            .await;
            dispatched.log();
            if !dispatched.resolved() && !halted.replace(true) {
                log::error!(
                    "native scheduler stopped claiming: Job {} is not settled or Uncertain after its dispatch; run `ocg inspect-held` once the host has stopped; nothing is retried, reconciled or released",
                    dispatched.job
                );
            }
            dispatched
        });
        self.running.borrow_mut().push(task);
    }

    pub(crate) async fn drain(&self) -> Vec<Dispatched> {
        let tasks = std::mem::take(&mut *self.running.borrow_mut());
        let mut finished = Vec::with_capacity(tasks.len());
        for task in tasks {
            match task.await {
                Ok(dispatched) => finished.push(dispatched),
                Err(_) => self.halt("a dispatch ended without a report"),
            }
        }
        finished
    }
}

#[derive(Debug)]
pub(crate) struct Dispatched {
    pub(crate) job: JobId,
    pub(crate) allocation_phase: Option<AllocationPhase>,
    pub(crate) started: bool,
    pub(crate) outcome: Option<OutcomeKind>,
    pub(crate) detail: String,
}

impl Dispatched {
    pub(crate) fn resolved(&self) -> bool {
        match self.allocation_phase {
            Some(AllocationPhase::Released) => self.outcome.is_some(),
            Some(AllocationPhase::Uncertain) => true,
            Some(
                AllocationPhase::Reserved
                | AllocationPhase::Running
                | AllocationPhase::StopRequested,
            )
            | None => false,
        }
    }

    fn log(&self) {
        let phase = self.allocation_phase.map_or("unknown", |p| match p {
            AllocationPhase::Reserved => "reserved",
            AllocationPhase::Running => "running",
            AllocationPhase::StopRequested => "stop_requested",
            AllocationPhase::Uncertain => "uncertain",
            AllocationPhase::Released => "released",
        });
        let outcome = match self.outcome {
            None => "none",
            Some(OutcomeKind::Completed) => "completed",
            Some(OutcomeKind::Failed) => "failed",
            Some(OutcomeKind::Cancelled) => "cancelled",
            Some(OutcomeKind::Unknown) => "unknown",
        };
        let started = if self.started {
            "started"
        } else {
            "never started"
        };
        match self.allocation_phase {
            Some(AllocationPhase::Released) => log::info!(
                "native scheduler: Job {} {started}, {outcome}, Allocation released ({})",
                self.job,
                self.detail
            ),
            _ => log::warn!(
                "native scheduler: Job {} {started}, {outcome}, Allocation {phase} and held ({})",
                self.job,
                self.detail
            ),
        }
    }
}

fn check_prepared_native(
    prepared: &crate::db::PreparedDevelopmentHandoff,
) -> Result<(), &'static str> {
    if let Some(decision) = prepared.decision_configuration() {
        let endpoint = match decision {
            crate::db::entities::FrozenDecisionConfig::Native(native) => native.endpoint.as_deref(),
            crate::db::entities::FrozenDecisionConfig::Generative(generative, _) => {
                generative.llm.provider.endpoint.as_deref()
            }
        };
        if !endpoint.is_some_and(crate::system::http::direct_diagnostic_loopback) {
            return Err("endpoint_not_loopback");
        }
        return Ok(());
    }
    let Some(frozen) = prepared.generative_configuration() else {
        return Err("generative_required");
    };
    let refusal = if prepared.is_summary_producer() {
        frozen.provider_refusal()
    } else {
        frozen.native_execution_refusal()
    };
    if refusal.is_some() {
        return Err("not_single_request_generative");
    }
    if !frozen
        .llm
        .provider
        .endpoint
        .as_deref()
        .is_some_and(crate::system::http::direct_diagnostic_loopback)
    {
        return Err("endpoint_not_loopback");
    }
    let request = prepared.request();
    if request.get("tool_choice").is_some()
        || request.get("stream") != Some(&serde_json::Value::Bool(true))
    {
        return Err("request_not_tool_free_stream");
    }
    Ok(())
}

async fn within_deadline<F: std::future::Future>(
    cancellation: &CancellationToken,
    timeout: Duration,
    exchange: F,
) -> F::Output {
    let done = CancellationToken::default();
    let timer = ntex::rt::spawn({
        let cancellation = cancellation.clone();
        let done = done.clone();
        async move {
            use ntex::util::{Either, select};
            if let Either::Right(()) = select(done.cancelled(), ntex::time::sleep(timeout)).await {
                cancellation.cancel();
            }
        }
    });
    let output = exchange.await;
    done.cancel();
    drop(timer);
    output
}

fn protection_refused(error: &DatabaseError) -> bool {
    matches!(
        error,
        DatabaseError::DiskProtected(_) | DatabaseError::RefusedByDiskProtection { .. }
    )
}

async fn wait_for_protection_recovery(
    database: &DatabaseHandle,
    cancellation: &CancellationToken,
) -> bool {
    use ntex::util::{Either, select};

    let protection = database.watch_disk_protection();
    loop {
        if cancellation.is_cancelled() {
            return false;
        }
        if protection.status().lock.is_none() {
            return true;
        }
        match select(cancellation.cancelled(), protection.changed()).await {
            Either::Left(()) | Either::Right(None) => return false,
            Either::Right(Some(_)) => {}
        }
    }
}

pub(crate) async fn dispatch(
    database: &DatabaseHandle,
    allocation: PoolAllocation,
    cancellation: &CancellationToken,
    workspace_tools: Option<&crate::system::executor::execution_runtime::WorkspaceTools>,
    stops: &NativeStops,
    permit: Option<Permit>,
) -> Dispatched {
    let token = allocation.token;
    let detail = if cancellation.is_cancelled() {
        match database
            .fail_unstarted_development_reservation(token, UnstartedCause::StartRefused)
            .await
        {
            Ok(_) => "cancelled before the start; the reservation was ended and nothing was sent"
                .to_owned(),
            Err(error) => {
                format!("cancelled before the start; the reservation was not ended here ({error})")
            }
        }
    } else {
        match database.scheduled_external_grant(token.job).await {
            Ok(None) => {
                let config = ProviderExecutionConfig {
                    timeout: DISPATCH_TIMEOUT,
                    first_delta_timeout: Some(DISPATCH_TIMEOUT),
                    max_output_bytes: MAX_RESPONSE_BYTES,
                    cancellation: cancellation.clone(),
                    ..ProviderExecutionConfig::default()
                };
                start_and_exchange(
                    database,
                    token,
                    allocation.operation,
                    cancellation,
                    &config,
                    EndpointPolicy::Loopback,
                    workspace_tools
                        .filter(|_| allocation.operation == ExecutionOperation::LlmRequest)
                        .map(WorkspaceTools::data_directory),
                    stops,
                    check_prepared_native,
                    permit,
                )
                .await
            }
            Ok(Some(_)) if allocation.operation == ExecutionOperation::Decision => {
                let prefix = "Decision execution is not permitted under a scheduled external grant";
                match database
                    .fail_unstarted_development_reservation(token, UnstartedCause::StartRefused)
                    .await
                {
                    Ok(_) => format!("{prefix}; the reservation was ended and nothing was sent"),
                    Err(settle) => {
                        format!("{prefix}; the reservation was not ended here ({settle})")
                    }
                }
            }
            Ok(Some(grant)) => {
                let config = grant.provider_config(cancellation.clone());
                let guard = |prepared: &crate::db::PreparedDevelopmentHandoff| {
                    grant.check_prepared(prepared).map_err(|refusal| {
                        log::warn!(
                            "native scheduler: Job {} scheduled external start refused ({}): {}",
                            token.job,
                            refusal.code,
                            refusal.detail
                        );
                        refusal.code
                    })
                };
                start_and_exchange(
                    database,
                    token,
                    allocation.operation,
                    cancellation,
                    &config,
                    EndpointPolicy::ScheduledExternal(&grant),
                    workspace_tools
                        .filter(|_| allocation.operation == ExecutionOperation::LlmRequest)
                        .map(WorkspaceTools::data_directory),
                    stops,
                    guard,
                    permit,
                )
                .await
            }
            Err(error) => {
                let prefix = format!(
                    "the scheduled external authorization was refused before the start ({error})"
                );
                match database
                    .fail_unstarted_development_reservation(token, UnstartedCause::StartRefused)
                    .await
                {
                    Ok(_) => format!("{prefix}; the reservation was ended and nothing was sent"),
                    Err(settle) => {
                        format!("{prefix}; the reservation was not ended here ({settle})")
                    }
                }
            }
        }
    };
    let dispatched = reported(database, token, detail).await;
    if dispatched.outcome.is_some()
        && let Err(error) = database.settle_child_turns(vec![token.job]).await
    {
        log::warn!(
            "native scheduler: the settling of Job {} was not passed on ({error})",
            token.job
        );
    }
    dispatched
}

#[allow(clippy::too_many_arguments)]
async fn start_and_exchange(
    database: &DatabaseHandle,
    token: AllocationToken,
    operation: ExecutionOperation,
    cancellation: &CancellationToken,
    config: &ProviderExecutionConfig,
    policy: EndpointPolicy<'_>,
    data_directory: Option<&crate::system::data_directory::DataDirectory>,
    stops: &NativeStops,
    guard: impl Fn(&crate::db::PreparedDevelopmentHandoff) -> Result<(), &'static str>,
    permit: Option<Permit>,
) -> String {
    let tooling = NativeTooling::new(data_directory, stops.clone(), permit);
    let handoff = loop {
        match database
            .checked_development_handoff_guarded(token, &tooling, &guard)
            .await
        {
            Err(DevelopmentHandoffError::Database(error)) if protection_refused(&error) => {
                if !wait_for_protection_recovery(database, cancellation).await {
                    return "protection interrupted the start; the reservation remains held".into();
                }
            }
            result => break result,
        }
    };
    match handoff {
        Ok(handoff) => {
            let tools = tooling.into_execution();
            let exchange = within_deadline(cancellation, DISPATCH_TIMEOUT, async {
                if handoff.decision_inputs().is_some() {
                    supervise_development_decision(database, handoff, config, policy).await
                } else {
                    match operation {
                        ExecutionOperation::LlmRequest => {
                            supervise_development_generative(
                                database, handoff, config, policy, tools,
                            )
                            .await
                        }
                        ExecutionOperation::Decision => {
                            supervise_development_decision(database, handoff, config, policy).await
                        }
                    }
                }
            })
            .await;
            match exchange {
                Ok(_) => "the exchange returned and was recorded".to_owned(),
                Err(error) => error.to_string(),
            }
        }
        Err(DevelopmentHandoffError::NotStarted { cause, error }) => {
            let settled = loop {
                match database
                    .fail_unstarted_development_reservation(token, cause)
                    .await
                {
                    Err(error) if protection_refused(&error) => {
                        if !wait_for_protection_recovery(database, cancellation).await {
                            return "protection interrupted settlement; the reservation remains held".into();
                        }
                    }
                    result => break result,
                }
            };
            match settled {
                Ok(_) => format!(
                    "the start was refused before any request ({error}); the reservation was ended"
                ),
                Err(settle) => format!(
                    "the start was refused before any request ({error}); the reservation was not ended here ({settle})"
                ),
            }
        }
        Err(error) => format!("the start may have committed without a reply ({error:?})"),
    }
}

async fn reported(database: &DatabaseHandle, token: AllocationToken, detail: String) -> Dispatched {
    match database.development_trial_job(token.job).await {
        Ok(Some((record, Some(allocation)))) if allocation.token == token => {
            if allocation.phase == AllocationPhase::Released {
                crate::system::executor::blocking::stop_settled(token.job);
            }
            Dispatched {
                job: token.job,
                allocation_phase: Some(allocation.phase),
                started: allocation.started_at.is_some(),
                outcome: record.outcome.as_ref().map(|outcome| outcome.kind),
                detail,
            }
        }
        Ok(_) => Dispatched {
            job: token.job,
            allocation_phase: None,
            started: false,
            outcome: None,
            detail: format!("{detail}; the Allocation could not be found afterwards"),
        },
        Err(error) => Dispatched {
            job: token.job,
            allocation_phase: None,
            started: false,
            outcome: None,
            detail: format!("{detail}; the result could not be read ({error})"),
        },
    }
}
