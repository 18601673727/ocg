//! Orchestration primitives shared by the scheduler and executors.

pub mod aggregation_service;
pub mod execution_runtime;
pub mod governor;
#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
pub(crate) mod lsp_client;
pub(crate) mod tools;
#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
pub mod workspace_command;
#[cfg(unix)]
pub mod workspace_discovery;
#[cfg(unix)]
pub mod workspace_fts;
#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
pub mod workspace_index_query;
#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
pub mod workspace_lsp;
#[cfg(unix)]
pub mod workspace_read;
#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
pub mod workspace_write;

pub use tools::ToolRegistryError;
