use std::future::Future;
use std::time::{Duration, Instant};

use ntex::util::{Either, select};

use crate::db::{DatabaseError, DatabaseHandle, SchedulerWakeSubscription};
use crate::service::ai::providers::CancellationToken;
use crate::system::execution_controls::ExecutionControl;
use crate::system::executor::execution_runtime::ExecutionRuntimeError;
use crate::system::monitor::ProtectionWatch;

const BATCH_FLOOR: Duration = Duration::from_millis(5);
const RECONCILE: Duration = Duration::from_secs(30);

pub(crate) struct DispatchBatch<G> {
    pub(crate) grants: Vec<G>,
    pub(crate) next_due: Option<Duration>,
}

pub(crate) struct Scheduler {
    wake: SchedulerWakeSubscription,
    protection: ProtectionWatch,
    shutdown: CancellationToken,
}

impl Scheduler {
    pub(crate) fn new(
        database: &DatabaseHandle,
        wake: SchedulerWakeSubscription,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            wake,
            protection: database.watch_disk_protection(),
            shutdown,
        }
    }

    pub(crate) async fn run<G, Claim, ClaimFuture, Spawn, SpawnFuture>(
        self,
        mut claim: Claim,
        mut spawn: Spawn,
    ) where
        Claim: FnMut() -> ClaimFuture,
        ClaimFuture: Future<Output = Result<DispatchBatch<G>, DatabaseError>>,
        Spawn: FnMut(G) -> SpawnFuture,
        SpawnFuture: Future<Output = ()>,
    {
        let mut previous_batch = None;
        let mut next_due = None;
        loop {
            if self.shutdown.is_cancelled() {
                return;
            }
            let mut recovered = false;
            while self.protection.status().lock.is_some() {
                match select(self.shutdown.cancelled(), self.protection.changed()).await {
                    Either::Left(()) => return,
                    Either::Right(Some(_)) => recovered = true,
                    Either::Right(None) => {
                        self.shutdown.cancelled().await;
                        return;
                    }
                }
            }
            if let Some(previous) = previous_batch {
                if !recovered {
                    let timer = batch_delay(previous, next_due);
                    let waking = select(
                        self.wake.recv_async(),
                        select(self.protection.changed(), ntex::time::sleep(timer)),
                    );
                    match select(self.shutdown.cancelled(), waking).await {
                        Either::Left(()) => return,
                        Either::Right(Either::Left(Err(_))) => return,
                        Either::Right(_) => {}
                    }
                }
                let remaining = BATCH_FLOOR.saturating_sub(previous.elapsed());
                if let Either::Left(()) =
                    select(self.shutdown.cancelled(), ntex::time::sleep(remaining)).await
                {
                    return;
                }
            }
            if self.protection.status().lock.is_some() {
                continue;
            }
            while self.wake.try_recv().is_ok() {}
            let batch = claim().await;
            previous_batch = Some(Instant::now());
            match batch {
                Ok(batch) => {
                    next_due = batch.next_due;
                    for grant in batch.grants {
                        spawn(grant).await;
                    }
                }
                Err(error) => {
                    log::error!("dispatcher claim batch failed: {error}");
                    next_due = None;
                }
            }
        }
    }
}

fn batch_delay(previous: Instant, next_due: Option<Duration>) -> Duration {
    let due = next_due
        .map(|due| due.saturating_sub(previous.elapsed()))
        .unwrap_or(RECONCILE)
        .min(RECONCILE);
    due.max(BATCH_FLOOR.saturating_sub(previous.elapsed()))
}

/// A cloneable endpoint to the host's scheduler.
#[derive(Clone)]
pub struct SchedulerHandle {
    /// The cancellations of the native scheduler's running executions; a
    /// native stop reaches its one execution through them.
    native_stops: crate::system::native_scheduler::NativeStops,
}

impl SchedulerHandle {
    /// Where a native stop finds the one running execution it cancels.
    pub(crate) fn native_stops(&self) -> &crate::system::native_scheduler::NativeStops {
        &self.native_stops
    }
}

/// Owns the host's native dispatcher.
pub struct ExecutionRuntime {
    scheduler: SchedulerHandle,
    shutdown: CancellationToken,
    finished: Option<ntex::rt::JoinHandle<Result<(), ExecutionRuntimeError>>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimScope {
    Stored,
    Process,
}

impl ExecutionRuntime {
    /// A runtime without autonomous authority: whatever the build compiles,
    /// it schedules nothing. Every host and command starts one of these
    /// unless it was explicitly armed; see [`Self::start_armed`].
    pub fn start(
        database: DatabaseHandle,
        scope: ClaimScope,
    ) -> Result<Self, ExecutionRuntimeError> {
        Self::start_with(database, scope, None, None)
    }

    /// A runtime the operator armed for autonomous loopback scheduling; see
    /// [`crate::system::loopback_activation`]. The token is necessary, and
    /// the compiled [`ExecutionControl::AutonomousScheduling`] remains
    /// necessary too: without either, this schedules nothing.
    pub(crate) fn start_armed(
        database: DatabaseHandle,
        scope: ClaimScope,
        armed: crate::system::loopback_activation::ArmedActivation,
        workspace_tools: crate::system::executor::execution_runtime::WorkspaceTools,
    ) -> Result<Self, ExecutionRuntimeError> {
        Self::start_with(database, scope, Some(armed), Some(workspace_tools))
    }

    fn start_with(
        database: DatabaseHandle,
        scope: ClaimScope,
        armed: Option<crate::system::loopback_activation::ArmedActivation>,
        workspace_tools: Option<crate::system::executor::execution_runtime::WorkspaceTools>,
    ) -> Result<Self, ExecutionRuntimeError> {
        // Only a runtime that schedules takes the database's scheduler wake;
        // one that never schedules leaves it untouched. Scheduling takes the
        // compiled control and the operator's arming of this run, both.
        let armed = armed.filter(|_| ExecutionControl::AutonomousScheduling.is_enabled());
        let wake = if armed.is_some() {
            Some(
                database
                    .take_scheduler_wake()
                    .ok_or(ExecutionRuntimeError::SchedulerStopped)?,
            )
        } else {
            None
        };
        let native_stops = crate::system::native_scheduler::NativeStops::default();
        let shutdown = CancellationToken::default();
        let finished = ntex::rt::spawn(dispatch_runtime(
            database,
            scope,
            wake,
            armed,
            shutdown.clone(),
            native_stops.clone(),
            workspace_tools,
        ));
        Ok(Self {
            scheduler: SchedulerHandle { native_stops },
            shutdown,
            finished: Some(finished),
        })
    }

    pub fn scheduler(&self) -> SchedulerHandle {
        self.scheduler.clone()
    }

    pub async fn shutdown(mut self) -> Result<(), ExecutionRuntimeError> {
        self.shutdown.cancel();
        if let Some(finished) = self.finished.take() {
            finished
                .await
                .map_err(|_| ExecutionRuntimeError::WorkerPanicked)??;
        }
        Ok(())
    }
}

impl Drop for ExecutionRuntime {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

async fn dispatch_runtime(
    database: DatabaseHandle,
    scope: ClaimScope,
    wake: Option<crate::db::SchedulerWakeSubscription>,
    armed: Option<crate::system::loopback_activation::ArmedActivation>,
    shutdown: CancellationToken,
    native_stops: crate::system::native_scheduler::NativeStops,
    workspace_tools: Option<crate::system::executor::execution_runtime::WorkspaceTools>,
) -> Result<(), ExecutionRuntimeError> {
    let (Some(wake), Some(armed)) = (
        wake.filter(|_| ExecutionControl::AutonomousScheduling.is_enabled()),
        armed,
    ) else {
        // No autonomous dispatch of any kind: the control is not compiled
        // in, or this run was not armed.
        shutdown.cancelled().await;
        return Ok(());
    };
    // The native scheduler runs only in the host that serves stored Jobs,
    // never inside a one-shot command's host.
    match scope {
        ClaimScope::Stored => {
            crate::system::native_scheduler::run(
                database,
                wake,
                shutdown,
                native_stops,
                armed,
                workspace_tools,
            )
            .await;
        }
        ClaimScope::Process => shutdown.cancelled().await,
    }
    Ok(())
}
