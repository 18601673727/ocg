use std::time::{Duration, Instant};

use ntex::util::{Either, select};

use crate::db::{DatabaseHandle, SchedulerWakeSubscription};
use crate::service::ai::providers::CancellationToken;
use crate::system::execution_features::ExecutionFeature;
use crate::system::executor::execution_runtime::ExecutionRuntimeError;
use crate::system::monitor::ProtectionWatch;
use crate::system::native_scheduler::{NativeScheduler, NativeStops};

const BATCH_FLOOR: Duration = Duration::from_millis(5);
const RECONCILE: Duration = Duration::from_secs(30);

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

    pub(super) async fn run(self, scheduler: &NativeScheduler) {
        let mut previous_batch = None;
        let mut next_due = None;
        loop {
            if self.shutdown.is_cancelled() {
                return;
            }
            let mut database_changed = false;
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
                        Either::Right(Either::Left(Ok(()))) => database_changed = true,
                        Either::Right(Either::Right(_)) => {}
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
            while self.wake.try_recv().is_ok() {
                database_changed = true;
            }
            let result = scheduler.dispatch(database_changed).await;
            previous_batch = Some(Instant::now());
            match result {
                Ok(due) => next_due = due,
                Err(error) => {
                    log::error!("scheduler dispatch failed: {error}");
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

pub struct ExecutionRuntime {
    native_stops: NativeStops,
    shutdown: CancellationToken,
    finished: Option<ntex::rt::JoinHandle<()>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimScope {
    Stored,
    Process,
}

impl ExecutionRuntime {
    fn idle() -> Self {
        Self {
            native_stops: NativeStops::default(),
            shutdown: CancellationToken::default(),
            finished: None,
        }
    }

    pub(crate) fn start_host(
        database: DatabaseHandle,
        scope: ClaimScope,
        workspace_tools: &crate::system::executor::execution_runtime::WorkspaceTools,
    ) -> Result<Self, ExecutionRuntimeError> {
        let mut runtime = Self::idle();
        if scope == ClaimScope::Stored && ExecutionFeature::AutonomousScheduling.is_enabled() {
            let wake = database
                .take_scheduler_wake()
                .ok_or(ExecutionRuntimeError::SchedulerStopped)?;
            runtime.finished = Some(ntex::rt::spawn(crate::system::native_scheduler::run(
                database,
                wake,
                runtime.shutdown.clone(),
                runtime.native_stops.clone(),
                workspace_tools.clone(),
            )));
        }
        Ok(runtime)
    }

    pub(crate) fn is_autonomous(&self) -> bool {
        self.finished.is_some()
    }

    pub(super) fn native_stops(&self) -> NativeStops {
        self.native_stops.clone()
    }

    pub async fn shutdown(mut self) -> Result<(), ExecutionRuntimeError> {
        self.shutdown.cancel();
        if let Some(finished) = self.finished.take() {
            finished
                .await
                .map_err(|_| ExecutionRuntimeError::WorkerPanicked)?;
        }
        Ok(())
    }
}

impl Drop for ExecutionRuntime {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}
