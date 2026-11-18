use crate::ai::providers::CancellationToken;
use crate::db::DatabaseHandle;
use crate::db::NativeStops;
use crate::jobs::execution::runtime::{ExecutionRuntimeError, WorkspaceTools};
use crate::jobs::scheduler::allocation;
use crate::support::cancellation::CancellationReason;

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
    pub fn start(
        database: DatabaseHandle,
        _scope: ClaimScope,
    ) -> Result<Self, ExecutionRuntimeError> {
        Ok(Self::idle(&database))
    }

    fn idle(database: &DatabaseHandle) -> Self {
        Self {
            native_stops: database.native_stops(),
            shutdown: CancellationToken::default(),
            finished: None,
        }
    }

    pub(crate) fn start_host(
        database: DatabaseHandle,
        scope: ClaimScope,
        workspace_tools: &WorkspaceTools,
    ) -> Result<Self, ExecutionRuntimeError> {
        let mut runtime = Self::idle(&database);
        if scope == ClaimScope::Stored {
            let wake = database
                .take_scheduler_wake()
                .ok_or(ExecutionRuntimeError::SchedulerStopped)?;
            runtime.finished = Some(ntex::rt::spawn(allocation::run(
                database,
                wake,
                runtime.shutdown.clone(),
                runtime.native_stops.clone(),
                workspace_tools.clone(),
            )));
        }
        Ok(runtime)
    }

    pub(crate) fn scheduler_task_running(&self) -> bool {
        !self.shutdown.is_cancelled()
            && self
                .finished
                .as_ref()
                .is_some_and(|task| !task.is_finished())
    }

    pub(crate) fn native_stops(&self) -> NativeStops {
        self.native_stops.clone()
    }

    pub async fn shutdown(mut self) -> Result<(), ExecutionRuntimeError> {
        self.shutdown.cancel_for(CancellationReason::HostShutdown);
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
        self.shutdown.cancel_for(CancellationReason::HostShutdown);
    }
}
