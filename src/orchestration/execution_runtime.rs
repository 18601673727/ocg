//! Long-lived execution runtime ownership for provider and native tool workers.

use crate::error::{OcgError, Result};
use crate::http::HttpTransport;
use crate::native_tools::PermissionPolicy;
use crate::orchestration::execution_dispatch::BoundedDispatcher;
use crate::provider_loop::{run_provider_dispatcher, ProviderHandlerConfig};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

/// Long-lived execution runtime that owns provider and native tool dispatchers.
pub struct ExecutionRuntime {
    provider_dispatcher: BoundedDispatcher,
    native_tool_dispatcher: BoundedDispatcher,
    cancelled: Arc<AtomicBool>,
    provider_thread: Option<JoinHandle<Result<()>>>,
    native_tool_thread: Option<JoinHandle<Result<()>>>,
}

impl ExecutionRuntime {
    /// Start the execution runtime with provider and native tool workers.
    pub fn start(
        project_root: &Path,
        provider_capacity: usize,
        native_tool_capacity: usize,
        transport: Arc<dyn HttpTransport>,
        permission_policy: PermissionPolicy,
    ) -> Result<Self> {
        let provider_dispatcher = BoundedDispatcher::new(provider_capacity)?;
        let native_tool_dispatcher = BoundedDispatcher::new(native_tool_capacity)?;
        let cancelled = Arc::new(AtomicBool::new(false));

        let provider_config = ProviderHandlerConfig {
            transport,
            project_root: project_root.to_path_buf(),
            permission_policy,
            cancelled: cancelled.clone(),
            native_tool_dispatcher: native_tool_dispatcher.clone(),
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
                crate::provider_loop::run_native_tool_worker(&native_tool_dispatcher_clone, &native_tool_handler)
            })
            .map_err(|error| OcgError::config(format!("spawn native tool thread: {error}")))?;

        // Start provider dispatcher in separate thread (with recovery)
        let provider_dispatcher_clone = provider_dispatcher.clone();
        let project_root_clone = project_root.to_path_buf();
        let provider_thread = std::thread::Builder::new()
            .name("provider-worker".to_string())
            .spawn(move || {
                run_provider_dispatcher(&project_root_clone, &provider_dispatcher_clone, provider_config)
            })
            .map_err(|error| OcgError::config(format!("spawn provider thread: {error}")))?;

        Ok(Self {
            provider_dispatcher,
            native_tool_dispatcher,
            cancelled,
            provider_thread: Some(provider_thread),
            native_tool_thread: Some(native_tool_thread),
        })
    }

    /// Get a handle to submit work to the provider dispatcher.
    pub fn provider_dispatcher(&self) -> &BoundedDispatcher {
        &self.provider_dispatcher
    }

    /// Check if the runtime has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Shutdown the runtime gracefully.
    pub fn shutdown(mut self) -> Result<()> {
        // Set cancelled flag first so queued work gets fast-failed
        self.cancelled.store(true, Ordering::SeqCst);

        // Close both dispatchers to signal workers to exit
        self.provider_dispatcher.shutdown();
        self.native_tool_dispatcher.shutdown();

        // Join provider thread first (it may still be submitting native tool work)
        if let Some(thread) = self.provider_thread.take() {
            match thread.join() {
                Ok(result) => result?,
                Err(panic) => std::panic::resume_unwind(panic),
            }
        }

        // Join native tool thread after provider has exited
        if let Some(thread) = self.native_tool_thread.take() {
            match thread.join() {
                Ok(result) => result?,
                Err(panic) => std::panic::resume_unwind(panic),
            }
        }

        Ok(())
    }
}

impl Drop for ExecutionRuntime {
    fn drop(&mut self) {
        // Ensure threads don't outlive the runtime
        self.cancelled.store(true, Ordering::SeqCst);
        self.provider_dispatcher.shutdown();
        self.native_tool_dispatcher.shutdown();

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
    provider_dispatcher: BoundedDispatcher,
    cancelled: Arc<AtomicBool>,
}

impl ExecutionRuntimeHandle {
    pub fn new(runtime: &ExecutionRuntime) -> Self {
        Self {
            provider_dispatcher: runtime.provider_dispatcher.clone(),
            cancelled: runtime.cancelled.clone(),
        }
    }

    pub fn provider_dispatcher(&self) -> &BoundedDispatcher {
        &self.provider_dispatcher
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}
