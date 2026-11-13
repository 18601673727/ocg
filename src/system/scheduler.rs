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

#[derive(Clone)]
pub struct SchedulerHandle {
    native_stops: crate::system::native_scheduler::NativeStops,
}

impl SchedulerHandle {
    pub(crate) fn native_stops(&self) -> &crate::system::native_scheduler::NativeStops {
        &self.native_stops
    }
}

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
    pub fn start(
        database: DatabaseHandle,
        scope: ClaimScope,
    ) -> Result<Self, ExecutionRuntimeError> {
        Self::start_with(database, scope, false, None)
    }

    pub(crate) fn start_autonomous(
        database: DatabaseHandle,
        scope: ClaimScope,
        workspace_tools: crate::system::executor::execution_runtime::WorkspaceTools,
    ) -> Result<Self, ExecutionRuntimeError> {
        let autonomous =
            scope == ClaimScope::Stored && ExecutionControl::AutonomousScheduling.is_enabled();
        Self::start_with(database, scope, autonomous, Some(workspace_tools))
    }

    fn start_with(
        database: DatabaseHandle,
        scope: ClaimScope,
        autonomous: bool,
        workspace_tools: Option<crate::system::executor::execution_runtime::WorkspaceTools>,
    ) -> Result<Self, ExecutionRuntimeError> {
        let autonomous = autonomous
            && scope == ClaimScope::Stored
            && ExecutionControl::AutonomousScheduling.is_enabled();
        let wake = if autonomous {
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
            autonomous,
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
    autonomous: bool,
    shutdown: CancellationToken,
    native_stops: crate::system::native_scheduler::NativeStops,
    workspace_tools: Option<crate::system::executor::execution_runtime::WorkspaceTools>,
) -> Result<(), ExecutionRuntimeError> {
    let Some(wake) = wake.filter(|_| {
        autonomous
            && scope == ClaimScope::Stored
            && ExecutionControl::AutonomousScheduling.is_enabled()
    }) else {
        shutdown.cancelled().await;
        return Ok(());
    };
    match scope {
        ClaimScope::Stored => {
            crate::system::native_scheduler::run(
                database,
                scope,
                wake,
                shutdown,
                native_stops,
                workspace_tools,
            )
            .await;
        }
        ClaimScope::Process => shutdown.cancelled().await,
    }
    Ok(())
}
