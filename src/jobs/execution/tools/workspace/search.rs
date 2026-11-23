use std::sync::Arc;
use std::sync::atomic::AtomicBool;
#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
use std::time::Duration;

use serde_json::Value;

use super::super::registry::{
    ToolFailure, ToolFuture, ToolHandler, ToolRegistry, ToolRegistryError, register_static,
};
use crate::db::entities::ToolEffect;
use crate::workspace::blocking::{self, Kind};
use crate::workspace::files::read::Workspace;
use crate::workspace::search::fts::{self, FtsLimits, WorkspaceFts};
#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
use crate::workspace::search::lsp;

struct FtsSearch {
    fts: Arc<WorkspaceFts>,
}

impl ToolHandler for FtsSearch {
    fn version(&self) -> &str {
        "workspace_fts_search-2"
    }

    fn effect(&self) -> ToolEffect {
        ToolEffect::ReadOnly
    }

    fn call(&self, arguments: Value) -> ToolFuture<'_> {
        Box::pin(async move {
            let call = fts::call_of(&arguments)
                .map_err(|refusal| ToolFailure::not_applied(refusal.to_string()))?;
            let stop = Arc::new(AtomicBool::new(false));
            let fts = self.fts.clone();
            let worker_fts = fts.clone();
            let worker_stop = stop.clone();
            let stopping = StopOnDrop { stop, fts };
            let found = blocking::run(Kind::Cpu, move || {
                fts::run_call(&worker_fts, &call, &worker_stop)
            })
            .await
            .map_err(|_| ToolFailure::not_applied("the search ended without a result"))?;
            drop(stopping);
            found.map_err(|refusal| ToolFailure::not_applied(refusal.to_string()))
        })
    }
}

struct StopOnDrop {
    stop: Arc<AtomicBool>,
    fts: Arc<WorkspaceFts>,
}

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.fts.stop_query(&self.stop);
    }
}

impl Drop for FtsSearch {
    fn drop(&mut self) {
        self.fts.dispose();
    }
}

pub(crate) fn register_fts(
    registry: &ToolRegistry,
    workspace: Workspace,
) -> Result<(), ToolRegistryError> {
    register_static!(
        registry,
        fts::TOOL_NAME,
        Some(
            "Opt-in: find files of the authorized Workspace with an in-memory full-text index built once and reused. All `query` words must match as whole words, case-insensitively (this is word search, not substring search), and a \"quoted phrase\" or one word of several parts, like JobScheduler, must appear as adjacent words. Results are ranked by BM25 with the matching line and an excerpt; offsets are UTF-8 byte columns, and excerpt_offset locates the excerpt within that line. AND applies across each file and phrases may cross lines. files_matched counts verified matching candidates examined, not an uncapped total. The index is a snapshot: its generation, age and coverage are in every result, and `refresh` rebuilds it after edits. Known OCG writes and commands mark snapshots dirty; external edits remain unverified even after refresh. Hidden, secret-bearing, symlinked, binary and over 1 MiB files are not indexed, nor `target` or `node_modules` directories; `index.complete` is false when a bound left part of the Workspace out of the snapshot.",
        ),
        fts::parameters(),
        Arc::new(FtsSearch {
            fts: Arc::new(WorkspaceFts::new(workspace, FtsLimits::default())),
        }),
    )
}

#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
struct CodeSearch(lsp::WorkspaceCode);

#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
impl ToolHandler for CodeSearch {
    fn version(&self) -> &str {
        "workspace_code-2"
    }

    fn effect(&self) -> ToolEffect {
        ToolEffect::ReadOnly
    }

    fn call(&self, arguments: Value) -> ToolFuture<'_> {
        Box::pin(async move { self.0.call(arguments).await.map_err(super::failure) })
    }
}

#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
pub(crate) fn register_lsp(
    registry: &ToolRegistry,
    workspace: Workspace,
    timeout: Duration,
) -> Result<(), ToolRegistryError> {
    let code = lsp::WorkspaceCode::new(workspace, timeout)
        .map_err(|error| ToolRegistryError::Unregistered(error.to_string()))?;
    register_static!(
        registry,
        lsp::TOOL_NAME,
        Some(
            "Use workspace_code before reading files. Read symbols with body instead of whole files, and batch every question you already have into one call (up to 8 queries). find returns definitions/signatures by Name, Type::name or prefix*; refs returns identifier uses, optionally kind=call/type/import; body takes name or path+line, signature=true includes docs and signature; outline takes a file/folder (depth 2); errors takes path or path=changed since this Job's previous call. Every query supports a path prefix. Compact text shares max_bytes (default 6144, capped at the Job's 16 KiB result limit). Supports Rust, JavaScript/JSX, TypeScript/TSX and Python. Results are by name, not by type: macros, trait dispatch, aliases and runtime attributes may be unresolved, so confirm renames/deletions with cargo check or tests.",
        ),
        lsp::parameters(),
        Arc::new(CodeSearch(code)),
    )
}
