//! Managed OpenCode runtime and the runtime-neutral execution lifecycle seam.
//!
//! `compat` describes OpenCode family/config behavior. `lifecycle` is the
//! stateful per-invocation contract consumed by Mission orchestration; OpenCode
//! V2 is its first concrete adapter.
//!
//! The runtime layer answers one question for a launch: which `opencode`
//! executable should run? The answer is one of four sources, in a fixed
//! precedence:
//!
//! 1. an explicit executable (environment override),
//! 2. an existing managed project runtime,
//! 3. a usable system `opencode` on `PATH`,
//! 4. a project-local bootstrap install.
//!
//! Policy lives in the top-level `runtime` configuration object; the existing
//! raw `opencode` config key is never overloaded.

pub mod archive;
pub mod cache;
pub mod compat;
pub mod effective;
pub mod hash;
pub mod install;
pub mod lifecycle;
pub mod policy;
pub mod release;
pub mod resolve;
pub mod self_update;

pub use compat::{detect, detect_from_host, Major, RuntimeAdapter, RuntimeVersion, SessionClient};
pub use lifecycle::{
    resolve_execution_lineage, RuntimeAdapter as RuntimeLifecycleAdapter, RuntimeCapabilities,
    RuntimeContextEvent, RuntimeContextObservation, RuntimeContinuation, RuntimeError,
    RuntimeErrorKind, RuntimeExecution, RuntimeExecutionId, RuntimeExecutionLineage,
    RuntimeIdentity, RuntimeProfile, RuntimeProvenance, RuntimeRecoveryKey,
};
pub use policy::{Channel, Fallback, RuntimePolicy};
pub use resolve::{RuntimeManager, RuntimeReport, RuntimeSelection, RuntimeSource, UpgradeOutcome};

/// The official standalone OpenCode release source.
pub const OPENCODE_REPO: &str = "anomalyco/opencode";

/// This repository's releases are the OCG self-update source.
pub const OCG_REPO: &str = "18601673727/ocg";

/// GitHub's public API base.
pub const DEFAULT_API_BASE: &str = "https://api.github.com";
