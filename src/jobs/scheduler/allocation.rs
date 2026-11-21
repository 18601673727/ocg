use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::Duration;

use super::dispatch::dispatch;
use super::wake::Scheduler;
use crate::ai::providers::CancellationToken;
use crate::db::NativeStops;
use crate::db::entities::{
    DevelopmentFreshReservation, DevelopmentReadyCursor, DevelopmentReservation,
    ExecutionOperation, ExecutionPoolId, ExecutorId, LlmTransport, PoolAllocation,
};
use crate::db::{
    DatabaseError, DatabaseHandle, DevelopmentReservationError, SchedulerWakeSubscription,
};
use crate::jobs::execution::runtime::WorkspaceTools;

const EXECUTOR_PAGE: usize = 100;
const MAX_EXECUTOR_PAGES: usize = 16;
const MAX_DISPATCHES: usize = 16;
const MAX_READY_PAGES: usize = 8;
const HEAD_PAGES: usize = 2;
const GENERATIVE_CAPABILITY: (ExecutionOperation, Option<LlmTransport>) = (
    ExecutionOperation::LlmRequest,
    Some(LlmTransport::OpenAiChatCompletions),
);
const RESPONSES_CAPABILITY: (ExecutionOperation, Option<LlmTransport>) = (
    ExecutionOperation::LlmRequest,
    Some(LlmTransport::OpenAiResponses),
);
const DECISION_CAPABILITY: (ExecutionOperation, Option<LlmTransport>) =
    (ExecutionOperation::Decision, None);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EndpointEligibility {
    code: &'static str,
    detail: &'static str,
    endpoint_class: Option<&'static str>,
}

const NOT_ASSESSED: [&str; 7] = [
    "agent_and_live_conversation_start_context",
    "live_attachment_availability",
    "executor_capacity",
    "host_scheduler_running",
    "optional_allocation_policy_binding",
    "live_execution_authorization",
    "reservation_and_writer_admission",
];

impl EndpointEligibility {
    pub(crate) fn to_json(self) -> serde_json::Value {
        serde_json::json!({
            "eligible": self.code == "eligible",
            "code": self.code,
            "detail": self.detail,
            "endpoint_class": self.endpoint_class,
            "scope": "frozen_provider_configuration",
            "not_assessed": NOT_ASSESSED,
        })
    }
}

pub(crate) fn endpoint_eligibility(
    frozen: Option<&crate::db::entities::MutableFrozenConfiguration>,
) -> EndpointEligibility {
    use crate::ai::providers::provider_uri;
    use crate::db::entities::MutableFrozenConfiguration;
    let rejected = |code, detail| EndpointEligibility {
        code,
        detail,
        endpoint_class: None,
    };
    let endpoint = match frozen {
        Some(MutableFrozenConfiguration::Generative(frozen)) => {
            if frozen.provider_refusal().is_some() {
                return rejected(
                    "unsupported_configuration",
                    "the frozen Provider configuration is not supported for native execution",
                );
            }
            frozen.llm.provider.endpoint.as_deref()
        }
        Some(MutableFrozenConfiguration::Decision(frozen)) => frozen.endpoint.as_deref(),
        None => {
            return rejected(
                "configuration_missing",
                "the Job has no frozen configuration",
            );
        }
    };
    let Some(endpoint) = endpoint else {
        return rejected("endpoint_missing", "the frozen Provider has no endpoint");
    };
    let Some(uri) = provider_uri(endpoint) else {
        return rejected(
            "invalid_endpoint",
            "the configured Provider endpoint is invalid",
        );
    };
    EndpointEligibility {
        code: "eligible",
        detail: "the frozen Provider configuration is supported; execution still requires live authorization",
        endpoint_class: Some(if uri.scheme_str() == Some("https") {
            "external"
        } else {
            "configured_local_http"
        }),
    }
}

fn configuration_is_eligible(
    configuration: &crate::db::entities::FrozenConfiguration,
    summary_work: bool,
) -> bool {
    use crate::ai::providers::provider_uri;
    use crate::db::entities::{FrozenConfiguration, FrozenDecisionConfig};
    let (endpoint, refusal) = match configuration {
        FrozenConfiguration::Generative(frozen) => (
            frozen.llm.provider.endpoint.as_deref(),
            if summary_work {
                frozen.provider_refusal()
            } else {
                frozen.native_execution_refusal()
            },
        ),
        FrozenConfiguration::Decision(FrozenDecisionConfig::Native(native)) => {
            (native.endpoint.as_deref(), None)
        }
        FrozenConfiguration::Decision(FrozenDecisionConfig::Generative(frozen, _)) => (
            frozen.llm.provider.endpoint.as_deref(),
            frozen.llm.provider.transport.execution_refusal(),
        ),
    };
    refusal.is_none() && endpoint.and_then(provider_uri).is_some()
}

pub(crate) async fn run(
    database: DatabaseHandle,
    wake: SchedulerWakeSubscription,
    shutdown: CancellationToken,
    stops: NativeStops,
    workspace_tools: WorkspaceTools,
) {
    if database.status() != crate::db::DatabaseStatus::Open {
        shutdown.cancelled().await;
        return;
    }
    log::info!("native scheduler running with configured Provider endpoints");
    let configs = database.subscribe_generative_configs();
    if let Err(error) = database.settle_unreported_children().await {
        log::warn!("native scheduler: unreported child conversations were not settled ({error})");
    }
    let scheduler = Allocator::new(database.clone(), shutdown.clone(), stops, workspace_tools);
    let wake = Scheduler::new(&database, wake, shutdown);
    let reports = async {
        while configs.changed().await.is_ok() {
            if let Err(error) = database.settle_unreported_children().await {
                log::warn!("native scheduler: could not return pending child results ({error})");
            }
        }
    };
    ntex::util::select(wake.run(&scheduler), reports).await;
    scheduler.drain().await;
    log::info!("native scheduler stopped");
}

enum ExecutorPass {
    Reserved(Box<PoolAllocation>),
    Nothing,
    BudgetSpent,
    Rescan,
    IndexExhausted,
    Halted,
}

#[derive(Clone, Copy, Default)]
struct Discovery {
    next: Option<ExecutorId>,
    sweep: Option<Sweep>,
}

#[derive(Clone, Copy)]
struct Sweep {
    origin: Option<ExecutorId>,
    wrapped: bool,
}

impl Discovery {
    fn advanced(self, reached: Option<ExecutorId>) -> Self {
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

pub(super) struct Allocator {
    database: DatabaseHandle,
    shutdown: CancellationToken,
    stops: NativeStops,
    // Each supported executor capability set has its own ready cursor.
    resume: Cell<[Option<DevelopmentReadyCursor>; 8]>,
    // A changed queue may make a Job eligible in a previously scanned segment.
    rescan: Cell<u8>,
    discovery: Cell<Discovery>,
    workspace_tools: WorkspaceTools,
    running: RefCell<Vec<ntex::rt::JoinHandle<()>>>,
    halted: Rc<Cell<bool>>,
}

impl Allocator {
    pub(crate) fn new(
        database: DatabaseHandle,
        shutdown: CancellationToken,
        stops: NativeStops,
        workspace_tools: WorkspaceTools,
    ) -> Self {
        Self {
            workspace_tools,
            database,
            shutdown,
            stops,
            resume: Cell::new([None; 8]),
            rescan: Cell::new(0),
            discovery: Cell::new(Discovery::default()),
            running: RefCell::new(Vec::new()),
            halted: Rc::new(Cell::new(false)),
        }
    }

    fn halt(&self, reason: &str) {
        if !self.halted.replace(true) {
            log::error!(
                "native scheduler stopped claiming: {reason}. Once the host has stopped, run `ocg inspect-held` to inspect ownership and `ocg reconcile-held` to release eligible Allocations"
            );
        }
    }

    pub(super) async fn dispatch(
        &self,
        database_changed: bool,
        recheck: bool,
    ) -> Result<Option<Duration>, DatabaseError> {
        if database_changed {
            let mut rescan = self.rescan.get();
            for (index, cursor) in self.resume.get().iter().enumerate() {
                if cursor.is_some() {
                    rescan |= 1 << index;
                }
            }
            self.rescan.set(rescan);
        }
        let (finished, running): (Vec<_>, Vec<_>) = std::mem::take(&mut *self.running.borrow_mut())
            .into_iter()
            .partition(|task| task.is_finished());
        *self.running.borrow_mut() = running;
        for task in finished {
            if task.await.is_err() {
                self.halt("a dispatch ended without a report");
            }
        }
        if recheck {
            self.database.recheck_interrupted().await?;
        }
        if crate::workspace::process::groups_tracked()
            && let Err(error) =
                ntex::rt::spawn_blocking(crate::workspace::process::retry_unproven_groups).await
        {
            log::warn!("failed to retry unproven groups on a blocking worker: {error}");
        }
        let available = MAX_DISPATCHES.saturating_sub(self.running.borrow().len());
        if self.halted.get() || self.shutdown.is_cancelled() || available == 0 {
            return Ok(None);
        }
        let recurrence_due = self.database.claim_due_recurrences(16).await?;
        let discovery = self.discovery.get();
        let mut executors = Vec::new();
        let mut before = discovery.next;
        let mut reached = None;
        for _ in 0..MAX_EXECUTOR_PAGES {
            if self.shutdown.is_cancelled() {
                return Ok(None);
            }
            let page = self
                .database
                .allocation_executors(before, EXECUTOR_PAGE)
                .await?;
            for executor in page.records {
                let capabilities = [
                    GENERATIVE_CAPABILITY,
                    DECISION_CAPABILITY,
                    RESPONSES_CAPABILITY,
                ]
                .into_iter()
                .enumerate()
                .fold(0, |mask, (bit, capability)| {
                    mask | (usize::from(executor.capabilities.contains(&capability)) << bit)
                });
                if executor.enabled && capabilities != 0 {
                    executors.push((executor.pool, executor.id, capabilities));
                }
            }
            reached = page.next;
            match page.next {
                Some(next) => before = Some(next),
                None => break,
            }
        }
        let mut rotating = false;
        let mut revisit = false;
        let mut capacity_reached = false;
        let mut dispatched = 0;
        let mut last_examined = None;
        let mut exhausted = 0u8;
        for (pool, executor, scan) in executors {
            if self.halted.get() || self.shutdown.is_cancelled() {
                break;
            }
            if dispatched == available {
                capacity_reached = true;
                break;
            }
            last_examined = Some(executor);
            if exhausted & (1 << scan) != 0 {
                continue;
            }
            match self.reserve_for(pool, executor, scan).await {
                ExecutorPass::Reserved(allocation) => {
                    // Transfer the committed claim before awaiting another reservation.
                    self.spawn(*allocation);
                    dispatched += 1;
                }
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
        let reached = if capacity_reached {
            // Resume after the last inspected candidate to keep unvisited entries eligible.
            last_examined
        } else {
            reached
        };
        let discovery = discovery.advanced(reached);
        self.discovery.set(discovery);
        let dispatch_due = ((revisit
            || rotating && self.resume.get().iter().any(Option::is_some)
            || discovery.sweep.is_some())
            && !capacity_reached
            && !self.halted.get())
        .then_some(Duration::ZERO);
        let next_due = match (dispatch_due, recurrence_due) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        Ok(next_due)
    }

    async fn reserve_for(
        &self,
        pool: ExecutionPoolId,
        executor: ExecutorId,
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
                .reserve_scheduled_job(fresh, configuration_is_eligible)
                .await;
            match passed {
                Ok(DevelopmentReservation::Reserved(allocation)) => {
                    if allocation.token.executor != executor || allocation.token.pool != pool {
                        self.halt("a reservation names another Executor");
                        return ExecutorPass::Halted;
                    }
                    return ExecutorPass::Reserved(allocation);
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
                Ok(DevelopmentReservation::Replayed) => {
                    self.halt("a first reservation attempt was answered as a replay");
                    return ExecutorPass::Halted;
                }
                Err(DevelopmentReservationError::DefinitelyNotCommitted(error)) => {
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

    fn spawn(&self, allocation: PoolAllocation) {
        let database = self.database.clone();
        let cancellation = self.shutdown.child();
        let registration = self.stops.register(allocation.token, cancellation.clone());
        let halted = self.halted.clone();
        let workspace_tools = self.workspace_tools.clone();
        let stops = self.stops.clone();
        let task = ntex::rt::spawn(async move {
            let _completion = DispatchWake(database.clone());
            let _registration = registration;
            let dispatched = dispatch(
                &database,
                allocation,
                &cancellation,
                &workspace_tools,
                &stops,
            )
            .await;
            dispatched.log();
            if !dispatched.resolved() && !halted.replace(true) {
                log::error!(
                    "native scheduler stopped claiming: Job {} is not settled or Uncertain after its dispatch. Once the host has stopped, run `ocg inspect-held` to inspect ownership and `ocg reconcile-held` to release eligible Allocations",
                    dispatched.job
                );
            }
        });
        self.running.borrow_mut().push(task);
    }

    async fn drain(&self) {
        let tasks = std::mem::take(&mut *self.running.borrow_mut());
        for task in tasks {
            if task.await.is_err() {
                self.halt("a dispatch ended without a report");
            }
        }
    }
}

struct DispatchWake(DatabaseHandle);

impl Drop for DispatchWake {
    fn drop(&mut self) {
        self.0.wake_scheduler();
    }
}
