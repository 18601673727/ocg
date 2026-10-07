//! Long-lived execution runtime ownership for provider and native tool workers.

use crate::error::{OcgError, Result};
use crate::http::HttpTransport;
use crate::native_tools::PermissionPolicy;
use crate::orchestration::disk_guard::{DiskGuard, DiskGuardConfig};
use crate::orchestration::execution_dispatch::BoundedDispatcher;
use crate::orchestration::governor::Governor;
use crate::provider_loop::{run_recovered_provider_dispatcher, ProviderHandlerConfig};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

/// Long-lived execution runtime that owns provider and native tool dispatchers.
pub struct ExecutionRuntime {
    project_root: PathBuf,
    provider_dispatcher: BoundedDispatcher,
    native_tool_dispatcher: BoundedDispatcher,
    cancelled: Arc<AtomicBool>,
    provider_in_flight_limit: Arc<std::sync::atomic::AtomicUsize>,
    governor: Governor,
    disk_guard: DiskGuard,
    provider_thread: Option<JoinHandle<Result<()>>>,
    native_tool_thread: Option<JoinHandle<Result<()>>>,
}

impl ExecutionRuntime {
    /// Start the execution runtime with provider and native tool workers.
    pub fn start(
        project_root: &Path,
        provider_capacity: usize,
        native_tool_capacity: usize,
        provider_in_flight_limit: usize,
        transport: Arc<dyn HttpTransport>,
        permission_policy: PermissionPolicy,
    ) -> Result<Self> {
        if !(1..=64).contains(&provider_in_flight_limit) {
            return Err(OcgError::config(
                "provider in-flight limit must be between 1 and 64",
            ));
        }
        let canonical_root = project_root
            .canonicalize()
            .map_err(|error| OcgError::io("resolve execution Project root", error))?;
        let project_root = canonical_root.as_path();
        let provider_dispatcher = BoundedDispatcher::new(provider_capacity)?;
        let native_tool_dispatcher = BoundedDispatcher::new(native_tool_capacity)?;
        let cancelled = Arc::new(AtomicBool::new(false));
        // Finish the recovery scan before exposing a handle for new admissions.
        let mut domain = crate::orchestration::domain::DomainRepository::open(project_root)?;
        domain.recover_startup()?;
        let recovered = domain.recover_provider_dispatches()?;

        let provider_in_flight_limit = Arc::new(std::sync::atomic::AtomicUsize::new(
            provider_in_flight_limit,
        ));
        let governor = Governor::new();
        // Measure disk before exposing a handle for new admissions: execution
        // must not begin under an uninitialized assumption of infinite free
        // space. A failed first observation leaves the Guard Unknown, which
        // defers new expansion but still allows essential settlement.
        let disk_guard = DiskGuard::for_root(project_root, DiskGuardConfig::default())?;
        if let Err(error) = disk_guard.refresh() {
            tracing::warn!(%error, "disk guard initial observation failed");
        }
        let provider_config = ProviderHandlerConfig {
            transport,
            project_root: project_root.to_path_buf(),
            permission_policy,
            cancelled: cancelled.clone(),
            native_tool_dispatcher: native_tool_dispatcher.clone(),
            in_flight_limit: provider_in_flight_limit.clone(),
            governor: governor.clone(),
        };

        let native_tool_handler = crate::native_tools::NativeToolCallHandler::new(
            project_root.to_path_buf(),
            permission_policy,
            cancelled.clone(),
        );

        // Start native tool consumer in separate thread
        let native_tool_dispatcher_clone = native_tool_dispatcher.clone();
        let native_tool_thread = std::thread::Builder::new()
            .name("native-tool-worker".to_string())
            .spawn(move || {
                crate::provider_loop::run_native_tool_worker(
                    &native_tool_dispatcher_clone,
                    &native_tool_handler,
                )
            })
            .map_err(|error| OcgError::config(format!("spawn native tool thread: {error}")))?;

        // Start provider dispatcher in separate thread (with recovery)
        let provider_dispatcher_clone = provider_dispatcher.clone();
        let project_root_clone = project_root.to_path_buf();
        let provider_thread = match std::thread::Builder::new()
            .name("provider-worker".to_string())
            .spawn(move || {
                run_recovered_provider_dispatcher(
                    &project_root_clone,
                    &provider_dispatcher_clone,
                    provider_config,
                    recovered,
                )
            }) {
            Ok(thread) => thread,
            Err(error) => {
                cancelled.store(true, Ordering::SeqCst);
                native_tool_dispatcher.shutdown();
                let _ = native_tool_thread.join();
                return Err(OcgError::config(format!("spawn provider thread: {error}")));
            }
        };

        Ok(Self {
            project_root: project_root.to_path_buf(),
            provider_dispatcher,
            native_tool_dispatcher,
            cancelled,
            provider_in_flight_limit,
            governor,
            disk_guard,
            provider_thread: Some(provider_thread),
            native_tool_thread: Some(native_tool_thread),
        })
    }

    /// Get a handle to submit work to the provider dispatcher.
    pub fn provider_dispatcher(&self) -> &BoundedDispatcher {
        &self.provider_dispatcher
    }

    /// Get a reference to the governor.
    pub fn governor(&self) -> &Governor {
        &self.governor
    }

    /// Get a reference to the execution disk-space guard.
    pub fn disk_guard(&self) -> &DiskGuard {
        &self.disk_guard
    }

    pub fn set_provider_in_flight_limit(&self, limit: usize) -> Result<()> {
        if !(1..=64).contains(&limit) {
            return Err(OcgError::config(
                "provider in-flight limit must be between 1 and 64",
            ));
        }
        self.provider_in_flight_limit.store(limit, Ordering::SeqCst);
        Ok(())
    }

    /// Check if the runtime has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Shutdown the runtime gracefully.
    pub fn shutdown(mut self) -> Result<()> {
        self.stop();
        let mut failure = None;
        for thread in [self.provider_thread.take(), self.native_tool_thread.take()]
            .into_iter()
            .flatten()
        {
            let result = thread
                .join()
                .unwrap_or_else(|_| Err(OcgError::config("execution worker panicked")));
            if let Err(error) = result {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }

    fn stop(&self) {
        // Set cancelled flag first so queued work gets fast-failed
        self.cancelled.store(true, Ordering::SeqCst);

        // Close both dispatchers to signal workers to exit
        self.provider_dispatcher.shutdown();
        self.native_tool_dispatcher.shutdown();
    }
}

impl Drop for ExecutionRuntime {
    fn drop(&mut self) {
        // Ensure threads don't outlive the runtime
        self.stop();

        if let Some(thread) = self.provider_thread.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.native_tool_thread.take() {
            let _ = thread.join();
        }
    }
}

/// A lightweight handle to submit work to an owned ExecutionRuntime.
#[derive(Clone, Debug)]
pub struct ExecutionRuntimeHandle {
    project_root: PathBuf,
    provider_dispatcher: BoundedDispatcher,
    cancelled: Arc<AtomicBool>,
    governor: Governor,
    disk_guard: DiskGuard,
}

impl ExecutionRuntimeHandle {
    pub fn new(runtime: &ExecutionRuntime) -> Self {
        Self {
            project_root: runtime.project_root.clone(),
            provider_dispatcher: runtime.provider_dispatcher.clone(),
            cancelled: runtime.cancelled.clone(),
            governor: runtime.governor.clone(),
            disk_guard: runtime.disk_guard.clone(),
        }
    }

    pub fn provider_dispatcher(&self) -> &BoundedDispatcher {
        &self.provider_dispatcher
    }

    pub fn governor(&self) -> &Governor {
        &self.governor
    }

    /// The shared execution disk-space guard for this Project root.
    pub fn disk_guard(&self) -> &DiskGuard {
        &self.disk_guard
    }

    pub fn project_root(&self) -> &Path {
        &self.project_root
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

pub struct ProjectRuntimeRegistry {
    transport: Arc<dyn HttpTransport>,
    permission_policy: PermissionPolicy,
    provider_capacity: usize,
    native_tool_capacity: usize,
    state: Mutex<ProjectRuntimeState>,
}

#[derive(Default)]
struct ProjectRuntimeState {
    runtimes: HashMap<String, ExecutionRuntime>,
    shutdown: bool,
}

impl std::fmt::Debug for ProjectRuntimeRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProjectRuntimeRegistry")
            .finish_non_exhaustive()
    }
}

impl ProjectRuntimeRegistry {
    pub fn handle(&self, project_id: &str) -> Result<Option<ExecutionRuntimeHandle>> {
        let state = self
            .state
            .lock()
            .map_err(|_| OcgError::config("Project runtime registry poisoned"))?;
        Ok(state
            .runtimes
            .get(project_id)
            .map(ExecutionRuntimeHandle::new))
    }

    pub fn new(
        transport: Arc<dyn HttpTransport>,
        permission_policy: PermissionPolicy,
        provider_capacity: usize,
        native_tool_capacity: usize,
    ) -> Self {
        Self {
            transport,
            permission_policy,
            provider_capacity,
            native_tool_capacity,
            state: Mutex::new(ProjectRuntimeState::default()),
        }
    }

    pub fn get_or_start(
        &self,
        project_id: &str,
        root: &Path,
        provider_in_flight_limit: usize,
    ) -> Result<ExecutionRuntimeHandle> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| OcgError::config("Project runtime registry poisoned"))?;
        if state.shutdown {
            return Err(OcgError::config(
                "Project runtime registry is shutting down",
            ));
        }
        if let Some(runtime) = state.runtimes.get(project_id) {
            if runtime.project_root != root {
                return Err(OcgError::config("Project runtime root changed"));
            }
            runtime.set_provider_in_flight_limit(provider_in_flight_limit)?;
            return Ok(ExecutionRuntimeHandle::new(runtime));
        }
        let runtime = ExecutionRuntime::start(
            root,
            self.provider_capacity,
            self.native_tool_capacity,
            provider_in_flight_limit,
            self.transport.clone(),
            self.permission_policy,
        )?;
        let handle = ExecutionRuntimeHandle::new(&runtime);
        state.runtimes.insert(project_id.to_string(), runtime);
        Ok(handle)
    }

    /// Stop and forget the cached runtime for one Project.
    ///
    /// The removed runtime is fully stopped here: `shutdown` cancels it, closes
    /// both dispatchers and joins both workers before returning, so once this
    /// call has returned no worker of the old root can still be running and no
    /// live-but-forgotten runtime can be left behind. A worker that reported a
    /// failure while stopping is still joined, so the error is a report about a
    /// runtime that is already dead — it never describes a runtime that
    /// outlived this call.
    ///
    /// Callers get `Err` when that shutdown failed and must not treat the
    /// Project as movable to another root on a runtime they believe is gone.
    pub fn invalidate(&self, project_id: &str) -> Result<()> {
        let runtime = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| OcgError::config("Project runtime registry poisoned"))?;
            state.runtimes.remove(project_id)
        };
        match runtime {
            // `ExecutionRuntime::shutdown` joins every worker before it
            // returns, so the removed runtime is dead whether it reports
            // success or failure.
            Some(runtime) => runtime.shutdown(),
            None => Ok(()),
        }
    }

    pub fn shutdown(&self) -> Result<()> {
        let runtimes = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| OcgError::config("Project runtime registry poisoned"))?;
            state.shutdown = true;
            std::mem::take(&mut state.runtimes)
        };
        for runtime in runtimes.values() {
            runtime.stop();
        }
        let mut failure = None;
        for runtime in runtimes.into_values() {
            if let Err(error) = runtime.shutdown() {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

impl Drop for ProjectRuntimeRegistry {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
