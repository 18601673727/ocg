//! The native scheduler: picks submitted native Jobs and runs them without a
//! per-Job invocation, under [`ExecutionControl::AutonomousScheduling`] and
//! nothing else.
//!
//! It is not manual loopback execution, not an external trial, not ordinary
//! manual external execution and not recovery: it holds none of their
//! authority and none of theirs enables it. It is reached only from the host's
//! execution runtime ([`crate::system::scheduler`]), only in the host that
//! serves stored Jobs, and only when the build enables its control and does
//! not enable [`ExecutionControl::LegacyProviderExecution`], and only in a
//! run the operator armed ([`crate::system::loopback_activation`]). In a
//! build where the control is disabled, or a run that was not armed, no
//! [`SchedulingAuthority`] exists, so nothing here claims, starts or sends
//! anything.
//!
//! # Selection
//!
//! A pass offers each enabled Executor that has the single-request Generative
//! capability among those it discovered, in an order that rotates, to the
//! writer, which selects and reserves in one transaction
//! ([`crate::db::entities::ReadySelection::SingleRequestGenerative`]): the
//! first ready Job in the ready index's order (priority, then submission time,
//! then Job) whose frozen configuration can run as the checked single
//! tool-free request and whose whole frozen configuration this scheduler's
//! policy admits. Everything else is passed over untouched and stays queued:
//! Decisions, Jobs whose Agent needs tools or a Workspace, other protocols, and
//! every endpoint that is neither a literal loopback one nor bound to a live
//! durable scheduled external policy. A literal loopback endpoint remains
//! [`EndpointPolicy::Loopback`]. An external configuration is admitted only by
//! the separate scheduled external policy, under its own disabled control and
//! its own durable binding; this module never infers it from the frozen
//! configuration. A busy Executor, a full
//! Pool and the global limit are refused before any ready entry is read, and a
//! pass that reserves nothing writes nothing, so it wakes nobody.
//!
//! Eligibility does not depend on which capable Executor asks, so the scan
//! position is shared. Each Executor's search starts at the head of the index;
//! when a long run of ineligible entries keeps it from getting further, a
//! bounded search reads the head first and then resumes where the last one
//! stopped, so every ready entry is reached within a bounded number of passes.
//! The order within that reach is the index's; an entry behind the resume
//! point can be reserved before an earlier entry that became eligible while
//! the head search could not reach it.
//!
//! # Executor discovery
//!
//! A pass reads a bounded window of the configuration rows, newest first, and
//! the next pass reads on from where it stopped, so a catalog longer than one
//! window is covered over consecutive passes rather than its newest window
//! over and over. Passes follow one another without waiting for a wake until
//! they have gone once round the catalog from where they began; then the
//! scheduler waits as before. The position is process memory and a hint about
//! where to read, nothing else: it is separate from the ready index's resume
//! point, a new run starts at the newest row, and what a window showed is
//! never trusted, since the writer checks the Executor again in the
//! transaction that reserves. Being discovered is not being given a Job: that
//! still takes a ready Job this scheduler may run, a free Executor and room.
//!
//! When the writer reports that the global limit itself is reached, the pass
//! stops offering Executors and the scheduler waits for the wake a freed
//! Allocation publishes. A pass that had reserved before the limit was reached
//! moves discovery on past its window, so each freed slot gives the next window
//! its turn; one that reserved nothing leaves discovery where it was. Every
//! window is therefore read within a bounded number of passes that find
//! capacity, but a window's Executors that the limit cut off are not offered
//! again until discovery returns to it.
//!
//! # Dispatch and stop
//!
//! Each reservation runs once: the checked start, with the same request check
//! as manual loopback execution, and one bounded exchange. Its cancellation is
//! registered under its Allocation's token before the start, so a stop
//! ([`NativeStops`]) reaches exactly that execution. Nothing here retries,
//! replays, reconciles or releases. A start refused before any request ends
//! the reservation and fails the Job; an exchange that may have reached the
//! Provider leaves its Allocation held as Uncertain, and a held Executor is
//! never offered again in this run. A restart never resumes anything either:
//! while [`ExecutionControl::RecoveryAndRedispatch`] is disabled the database
//! refuses to open with any Allocation held. Once any reservation or dispatch
//! ends in a state the database does not show as settled or Uncertain, the
//! scheduler stops claiming for the rest of the run.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use crate::db::entities::{
    AllocationPhase, AllocationToken, ConfigId, DevelopmentFreshReservation,
    DevelopmentReadyCursor, DevelopmentReservation, ExecutionOperation, ExecutionPoolId,
    ExecutorId, FrozenExecutionConfig, JobId, LlmTransport, OutcomeKind, PoolAllocation,
    UnstartedCause,
};
use crate::db::redb::{NativeResource, NativeResourceKind};
use crate::db::{
    DatabaseError, DatabaseHandle, DevelopmentHandoffError, DevelopmentReservationError,
    ScheduledReservation, SchedulerWakeSubscription,
};
use crate::service::ai::providers::{CancellationToken, ProviderExecutionConfig};
use crate::system::execution_controls::ExecutionControl;
use crate::system::executor::execution_runtime::{
    EndpointPolicy, supervise_development_generative,
};
use crate::system::loopback_activation::ArmedActivation;
use crate::system::manual_execution::within_deadline;
use crate::system::manual_loopback::{MAX_TIMEOUT_CEILING, check_prepared};
use crate::system::scheduler::{DispatchBatch, Scheduler};

/// Executors one list read returns.
const EXECUTOR_PAGE: usize = 100;
/// Executor pages one pass reads.
const MAX_EXECUTOR_PAGES: usize = 16;
/// Ready-index pages (each one bounded transaction) one Executor's search may
/// read.
const MAX_READY_PAGES: usize = 8;
/// Of those, the pages read from the head of the index before a search jumps
/// to where the last one stopped.
const HEAD_PAGES: usize = 2;
/// The longest exchange: the longest an operator may allow a manual loopback
/// execution.
const DISPATCH_TIMEOUT: Duration = MAX_TIMEOUT_CEILING;
/// The response bytes the exchange reads before it gives up.
const MAX_RESPONSE_BYTES: usize = 1 << 20;
/// What an Executor must offer for the one execution this scheduler runs.
const CAPABILITY: (ExecutionOperation, Option<LlmTransport>) = (
    ExecutionOperation::LlmRequest,
    Some(LlmTransport::OpenAiChatCompletions),
);

/// The authority of one autonomous scheduling run. [`Self::autonomous`] is
/// the only way to make one, and it makes none unless the build enables
/// [`ExecutionControl::AutonomousScheduling`] *and* this host run was armed
/// by the operator ([`ArmedActivation`], which only a passed activation
/// preflight produces). The compiled control is necessary, never sufficient.
/// It grants no transport: each dispatch takes the endpoint policy its
/// reservation's durable binding names.
pub(crate) struct SchedulingAuthority(());

impl SchedulingAuthority {
    pub(crate) fn autonomous(armed: ArmedActivation) -> Option<Self> {
        let _armed = armed;
        ExecutionControl::AutonomousScheduling
            .is_enabled()
            .then_some(Self(()))
    }
}

/// Why a queued Job is, or is not, one this scheduler's loopback dispatch
/// would even consider. It is derived from the frozen configuration and the
/// process's endpoint policy, stored nowhere, and decides nothing: the
/// reservation transaction asks the same facts itself.
///
/// # What it does not say
///
/// It does not say the Job will run. Whether the scheduler selects a Job it
/// would consider is decided in the reservation transaction, which asks more
/// than this can answer without opening a write transaction: whether the
/// conversation's start context is one checked start supports
/// (`unsupported_start_context`: notices, child work, approvals and tool
/// evidence), whether its predecessors are settled, whether its attachments
/// are available, and whether an Executor with the required capability has
/// capacity. Only the scheduler's own transaction is authoritative for that,
/// and this report never stands in for it: no field here grants, and none is
/// read as, execution authority.
///
/// So a Job that is not eligible here stays queued untouched; it may belong
/// to a separately authorized workflow (a manual loopback execution, a manual
/// external execution, or a scheduled external policy under its own control).
/// A Job that is eligible here may still never be selected, and says why in
/// the fields this report does not cover.
///
/// # What it says about authority
///
/// Nothing, and it cannot: this is derived from one Job's frozen
/// configuration, so it reads no Host and never reports whether one is armed.
/// Actual execution also needs the build to compile
/// [`ExecutionControl::AutonomousScheduling`] enabled, the current Host to hold
/// an [`ArmedActivation`] for this database, and the reservation and writer
/// checks to admit the Job. `not_assessed` names all three, so policy
/// eligibility is never read as immediate schedulability.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LoopbackEligibility {
    pub(crate) code: &'static str,
    pub(crate) detail: &'static str,
    /// Set when the endpoint alone decided; see
    /// [`crate::system::http::LoopbackEndpointClass`].
    pub(crate) endpoint: Option<crate::system::http::LoopbackEndpointClass>,
}

/// What this policy answer does not cover, named so no reader can mistake it
/// for a scheduling decision. The first four are questions the scheduler
/// answers inside its own write transaction. The last three are the runtime
/// authority that must exist before any of those questions is even asked:
/// this calculation is static, sees no Host, and must not guess whether one
/// is armed.
const NOT_ASSESSED: [&str; 7] = [
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

    /// The report. `eligible` means the frozen configuration and this
    /// process's endpoint policy admit autonomous loopback dispatch, and
    /// nothing more: it is neither schedulability nor authority.
    /// `not_assessed` names everything it does not cover, including the
    /// runtime authority a Host needs before any Job can run, so a reader
    /// cannot mistake it for the scheduler's decision.
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

/// The eligibility of a Job frozen as `frozen` for this scheduler's loopback
/// dispatch; see [`LoopbackEligibility`]. The predicates are exactly the ones
/// the reservation transaction and the checked start ask about the *frozen
/// configuration itself*
/// ([`FrozenExecutionConfig::single_request_refusal`] and
/// [`crate::system::http::direct_diagnostic_loopback`]); the endpoint class
/// only explains the second. Everything the schedule also asks about the
/// Job's conversation and the pool is reported as `not_assessed`, never
/// guessed at, and the Job's Decision history is
/// `GET /api/v1/jobs/{job}`'s own `unsupported_decision_history`.
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
    let frozen = match frozen {
        None => {
            return none(
                "not_frozen",
                "the Job has no frozen configuration; only a submitted Job is scheduled",
            );
        }
        Some(MutableFrozenConfiguration::Decision(_)) => {
            return none(
                "decision_job",
                "a Decision Job is never selected by autonomous loopback scheduling",
            );
        }
        Some(MutableFrozenConfiguration::Generative(frozen)) => frozen,
    };
    if frozen.single_request_refusal().is_some() {
        return none(
            "not_single_request_generative",
            "the frozen configuration cannot run as a single tool-free Generative request (a chat Agent without a Workspace grant, the OpenAI Chat Completions protocol and a declared context window are required)",
        );
    }
    let Some(endpoint) = frozen.llm.provider.endpoint.as_deref() else {
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
            return LoopbackEligibility {
                code: "eligible",
                detail: "the frozen endpoint is a literal http://127.0.0.1:<port> endpoint no proxy intercepts, and the frozen configuration is a single tool-free Generative request; this is not a claim that the Job will run: see not_assessed for what the scheduler also asks",
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
            "the frozen endpoint is not a loopback endpoint; autonomous loopback scheduling never reaches it, and an external destination is admitted only by a live scheduled external policy under its own (disabled) control or by a manual external execution"
        }
        LoopbackEndpointClass::Unparseable => "the frozen endpoint is not a URI",
    };
    LoopbackEligibility {
        code: "endpoint_not_literal_loopback",
        detail,
        endpoint: Some(class),
    }
}

/// Whether this scheduler may select `frozen`: the whole frozen configuration
/// is offered, not just an endpoint. A literal loopback endpoint the configured
/// proxy does not intercept is admitted exactly as before, and an external
/// configuration is admitted only by the separate scheduled external policy,
/// which is unconfigured and whose control is disabled in this build. This
/// predicate only opens the door; the reservation transaction reads the live
/// policy and records the binding the start and send transactions revalidate,
/// and dispatch takes its endpoint policy from that binding.
fn scheduled_policy(frozen: &FrozenExecutionConfig) -> bool {
    frozen
        .llm
        .provider
        .endpoint
        .as_deref()
        .is_some_and(crate::system::http::direct_diagnostic_loopback)
        || ExecutionControl::ScheduledExternalTransport.is_enabled()
}

/// The cancellations of the executions this host's native scheduler runs, by
/// Job, each with the token of the Allocation it runs under. A stop reaches
/// an execution only through the token the database recorded the stop for,
/// so it never reaches another Job, another Executor's work, a later
/// Allocation or the host. Registered before the checked start and removed
/// when the execution's task ends.
#[derive(Clone, Default)]
pub(crate) struct NativeStops(Arc<Mutex<BTreeMap<JobId, (AllocationToken, CancellationToken)>>>);

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

    /// Cancels the execution registered under exactly `token`, and says
    /// whether there was one. Cancelling twice has no further effect.
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

/// Removes its execution's entry from [`NativeStops`] when dropped, however
/// the task ends.
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

/// Runs the native scheduler until `shutdown`, then waits for every dispatch
/// it started to settle what it can. Without [`SchedulingAuthority`] it only
/// waits for `shutdown`.
pub(crate) async fn run(
    database: DatabaseHandle,
    wake: SchedulerWakeSubscription,
    shutdown: CancellationToken,
    stops: NativeStops,
    armed: ArmedActivation,
) {
    let Some(authority) = SchedulingAuthority::autonomous(armed) else {
        shutdown.cancelled().await;
        return;
    };
    run_with(authority, database, wake, shutdown, stops).await;
}

/// [`run`] under an authority already held.
pub(crate) async fn run_with(
    authority: SchedulingAuthority,
    database: DatabaseHandle,
    wake: SchedulerWakeSubscription,
    shutdown: CancellationToken,
    stops: NativeStops,
) {
    log::info!("native scheduler running: single-request Generative Jobs, loopback endpoints only");
    let scheduler = Rc::new(NativeScheduler::new(
        database.clone(),
        authority,
        shutdown.clone(),
        stops,
    ));
    Scheduler::new(&database, wake, shutdown)
        .run(
            || {
                let scheduler = scheduler.clone();
                async move { scheduler.claim().await }
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

/// What one Executor's search came to.
pub(crate) enum ExecutorPass {
    Reserved(Box<PoolAllocation>),
    /// Nothing for this Executor now: it was refused, as a busy Executor or
    /// one whose Pool is full is. Nothing was written.
    Nothing,
    /// The writer read the global limit as reached: no Executor can reserve
    /// until an Allocation is released. Nothing was written.
    GlobalLimit,
    /// The search ran out of budget; the next one resumes where it stopped.
    /// Nothing was written.
    BudgetSpent,
    /// The search read to the end of the ready index without finding an
    /// eligible Job, from the head or completing a rotation that earlier
    /// searches began. The pass ends: another Executor would only begin the
    /// next rotation, and whatever makes a Job eligible wakes the scheduler.
    IndexExhausted,
    /// A reservation may have committed without a reply; the scheduler stops
    /// claiming.
    Halted,
}

/// Where Executor discovery stands between passes. It says where to read and
/// nothing about what may run.
#[derive(Clone, Copy, Default)]
struct Discovery {
    /// The configuration row the next pass reads below; `None` is the newest.
    next: Option<ConfigId>,
    /// The round of the catalog under way, when the passes so far have not
    /// completed one.
    sweep: Option<Sweep>,
}

/// One round of the catalog, over as many passes as it takes.
#[derive(Clone, Copy)]
struct Sweep {
    /// Where its first pass began reading.
    origin: Option<ConfigId>,
    /// Whether it has read to the oldest row and gone back to the newest.
    wrapped: bool,
}

impl Discovery {
    /// Where discovery stands after a pass that began here read down to
    /// `reached`, or to the oldest row when `reached` is `None`. The round is
    /// over once every row has been read since it began: at the oldest row
    /// for a round that began at the newest, and otherwise on coming back
    /// round to its origin. Rows are compared by key, so rows that appear or
    /// are archived meanwhile neither end a round early nor prolong it.
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

/// The native scheduler of one host run.
pub(crate) struct NativeScheduler {
    database: DatabaseHandle,
    _authority: SchedulingAuthority,
    shutdown: CancellationToken,
    stops: NativeStops,
    /// Where the last search that ran out of budget stopped. Process memory
    /// only: a new run starts at the head of the ready index.
    resume: Cell<Option<DevelopmentReadyCursor>>,
    /// Where Executor discovery continues. Process memory only, and never the
    /// ready index's `resume`: a new run starts at the newest row.
    discovery: Cell<Discovery>,
    /// Rotates the order a window's Executors are offered in. It moves each
    /// time discovery returns to the newest row and each time a pass leaves
    /// discovery where it was, so a window is offered in a new order every
    /// time it is read again, however many passes a round takes.
    turns: Cell<u64>,
    running: RefCell<Vec<ntex::rt::JoinHandle<Dispatched>>>,
    /// Set once anything ends in a state the database does not show as
    /// settled or Uncertain; nothing is claimed after it.
    halted: Rc<Cell<bool>>,
}

impl NativeScheduler {
    pub(crate) fn new(
        database: DatabaseHandle,
        authority: SchedulingAuthority,
        shutdown: CancellationToken,
        stops: NativeStops,
    ) -> Self {
        Self {
            database,
            _authority: authority,
            shutdown,
            stops,
            resume: Cell::new(None),
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

    /// One pass: at most one reservation per eligible Executor. Reads, and
    /// writes only the reservations it returns.
    pub(crate) async fn claim(&self) -> Result<DispatchBatch<PoolAllocation>, DatabaseError> {
        let mut grants = Vec::new();
        let (finished, running): (Vec<_>, Vec<_>) = std::mem::take(&mut *self.running.borrow_mut())
            .into_iter()
            .partition(|task| task.is_finished());
        *self.running.borrow_mut() = running;
        for task in finished {
            // A dispatch that ended without a report (it panicked) left its
            // Allocation in a state nobody read.
            if task.await.is_err() {
                self.halt("a dispatch ended without a report");
            }
        }
        if self.halted.get() || self.shutdown.is_cancelled() {
            return Ok(DispatchBatch {
                grants,
                next_due: None,
            });
        }
        // Discovery moves only once the whole window has been read: a failed
        // read or a cancellation leaves it where it was, so the next pass
        // reads the same window again and no Executor in it is skipped.
        let discovery = self.discovery.get();
        let mut executors = Vec::new();
        let mut before = discovery.next;
        let mut reached = None;
        for _ in 0..MAX_EXECUTOR_PAGES {
            if self.shutdown.is_cancelled() {
                return Ok(DispatchBatch {
                    grants,
                    next_due: None,
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
                    && executor.capabilities.contains(&CAPABILITY)
                {
                    executors.push((executor.pool, executor.id));
                }
            }
            // A page that returned no Executor still moves the read on.
            reached = page.next;
            match page.next {
                Some(next) => before = Some(next),
                None => break,
            }
        }
        let turn = self.turns.get();
        if !executors.is_empty() {
            // No Executor is always offered the head of the queue first.
            let start = usize::try_from(turn % executors.len() as u64).unwrap_or(0);
            executors.rotate_left(start);
        }
        let mut rotating = false;
        let mut global_limit = false;
        for (pool, executor) in executors {
            if self.halted.get() || self.shutdown.is_cancelled() {
                break;
            }
            match self.reserve_for(pool, executor).await {
                ExecutorPass::Reserved(allocation) => grants.push(*allocation),
                ExecutorPass::Nothing => {}
                ExecutorPass::BudgetSpent => rotating = true,
                ExecutorPass::GlobalLimit => {
                    global_limit = true;
                    break;
                }
                ExecutorPass::IndexExhausted | ExecutorPass::Halted => break,
            }
        }
        // At the global limit every other Executor would be refused the same
        // way, so the pass stops offering. What it does with discovery depends
        // on whether this window used up the capacity that was free:
        //
        // - It reserved at least once: the window has had its use of what was
        //   freed, so discovery moves on from it exactly as after any other
        //   window. Were it to stay, the newest window would take every slot a
        //   release frees and no older window would ever be read.
        // - It reserved nothing: the limit was already reached when the pass
        //   began (a wake that lost the freed slot to an earlier pass), the
        //   window has had no use of capacity and discovery, round included,
        //   stays as it was.
        //
        // Either way the pass waits for the wake a released Allocation
        // publishes and does not follow itself.
        let discovery = if global_limit && grants.is_empty() {
            discovery
        } else {
            discovery.advanced(reached)
        };
        self.discovery.set(discovery);
        // The window is offered in a new order each time it is read again:
        // when discovery wraps to the newest row, and when a pass that
        // reserved nothing at the limit leaves it in place.
        if reached.is_none() || (global_limit && grants.is_empty()) {
            self.turns.set(turn.wrapping_add(1));
        }
        // A search that ran out of budget wrote nothing, so no wake follows
        // it: the next pass is due at once, and passes continue while
        // searches advance, until one reads to the end of the index. A window
        // that did not complete a round of the catalog is followed at once by
        // the next one too, whatever it reserved and whether or not it showed
        // any Executor: a reservation's wake is not what continues discovery.
        // Only the global limit holds this back: no pass can reserve until an
        // Allocation is released, so it waits for that wake, and a round
        // under way continues from there. A pass that ends a round with every
        // Executor refused advanced nothing and waits for a wake, as a
        // settlement that frees one publishes.
        let next_due = ((rotating && self.resume.get().is_some() || discovery.sweep.is_some())
            && !global_limit
            && !self.halted.get())
        .then_some(Duration::ZERO);
        Ok(DispatchBatch { grants, next_due })
    }

    /// One Executor's search: from the head of the ready index, and, when an
    /// earlier search ran out of budget, from where it stopped after
    /// [`HEAD_PAGES`]. Every request is a first attempt; none is replayed.
    pub(crate) async fn reserve_for(
        &self,
        pool: ExecutionPoolId,
        executor: ExecutorId,
    ) -> ExecutorPass {
        let resume = self.resume.get();
        let mut after: Option<DevelopmentReadyCursor> = None;
        for page in 0..MAX_READY_PAGES {
            if page == HEAD_PAGES
                && let Some(resume) = resume
                && after.is_none_or(|reached| reached.0 < resume.0)
            {
                after = Some(resume);
            }
            let fresh = DevelopmentFreshReservation::new(pool, executor, after);
            let passed = match self
                .database
                .reserve_scheduled_job(fresh, scheduled_policy)
                .await
            {
                Ok(ScheduledReservation::GlobalLimitReached) => return ExecutorPass::GlobalLimit,
                Ok(ScheduledReservation::Pass(passed)) => Ok(*passed),
                Err(error) => Err(error),
            };
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
                    self.resume.set(None);
                    return ExecutorPass::IndexExhausted;
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
            self.resume.set(after);
        }
        ExecutorPass::BudgetSpent
    }

    /// Registers the dispatch of one reservation under its token and starts
    /// it as a task of its own.
    pub(crate) fn spawn(&self, allocation: PoolAllocation) {
        let database = self.database.clone();
        let cancellation = self.shutdown.child();
        let registration = self.stops.register(allocation.token, cancellation.clone());
        let halted = self.halted.clone();
        let task = ntex::rt::spawn(async move {
            let _registration = registration;
            let dispatched = dispatch(&database, allocation, &cancellation).await;
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

    /// Waits for every dispatch this scheduler started. Shutdown has
    /// cancelled their exchanges; each settles what it can.
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

/// What the database shows after one scheduled dispatch returned.
#[derive(Debug)]
pub(crate) struct Dispatched {
    pub(crate) job: JobId,
    pub(crate) allocation_phase: Option<AllocationPhase>,
    pub(crate) started: bool,
    pub(crate) outcome: Option<OutcomeKind>,
    pub(crate) detail: String,
}

impl Dispatched {
    /// Released with an outcome, or recorded as Uncertain and held. Anything
    /// else is ownership nobody recorded an end for.
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
        let phase = self
            .allocation_phase
            .map_or("unknown", crate::system::manual_execution::phase_name);
        let outcome = crate::system::manual_execution::outcome_name(self.outcome);
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

/// The checked start and the one exchange of `allocation`, under
/// `cancellation` (shutdown's, and the Job's own stop), then what the
/// database shows. A reservation whose start a cancellation overtook is ended
/// rather than started, unless a stop already ended it: it never reached a
/// Provider.
///
/// The endpoint policy comes from the reservation's durable binding, never
/// from the Job's frozen configuration alone. No binding means the loopback
/// path exactly as before. A binding that cannot be read, whose policy is no
/// longer live at the recorded revision, or that the disabled
/// [`ExecutionControl::ScheduledExternalTransport`] refuses ends the
/// reservation unsent: this dispatch does not fall back to loopback.
pub(crate) async fn dispatch(
    database: &DatabaseHandle,
    allocation: PoolAllocation,
    cancellation: &CancellationToken,
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
                    cancellation,
                    &config,
                    EndpointPolicy::Loopback,
                    check_prepared,
                )
                .await
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
                    cancellation,
                    &config,
                    EndpointPolicy::ScheduledExternal(&grant),
                    guard,
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
    reported(database, token, detail).await
}

/// The checked start of one reservation and the one exchange it authorizes.
async fn start_and_exchange(
    database: &DatabaseHandle,
    token: AllocationToken,
    cancellation: &CancellationToken,
    config: &ProviderExecutionConfig,
    policy: EndpointPolicy<'_>,
    guard: impl FnOnce(&crate::db::PreparedDevelopmentHandoff) -> Result<(), &'static str>,
) -> String {
    match database
        .checked_development_handoff_guarded(token, guard)
        .await
    {
        Ok(handoff) => {
            let exchange = within_deadline(
                cancellation,
                DISPATCH_TIMEOUT,
                supervise_development_generative(database, handoff, config, policy),
            )
            .await;
            match exchange {
                Ok(_) => "the exchange returned and was recorded".to_owned(),
                Err(error) => error.to_string(),
            }
        }
        Err(DevelopmentHandoffError::NotStarted { cause, error }) => {
            match database
                .fail_unstarted_development_reservation(token, cause)
                .await
            {
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

/// What the database shows after one scheduled dispatch returned.
async fn reported(database: &DatabaseHandle, token: AllocationToken, detail: String) -> Dispatched {
    match database.development_trial_job(token.job).await {
        Ok(Some((record, Some(allocation)))) if allocation.token == token => Dispatched {
            job: token.job,
            allocation_phase: Some(allocation.phase),
            started: allocation.started_at.is_some(),
            outcome: record.outcome.as_ref().map(|outcome| outcome.kind),
            detail,
        },
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
