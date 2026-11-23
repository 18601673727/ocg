use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::{Value, json};

use super::super::registry::{
    ToolFailure, ToolFuture, ToolHandler, ToolRegistry, ToolRegistryError, register_static,
};
use crate::db::entities::{JobId, ToolCallRef, ToolEffect};
use crate::workspace::blocking::{self, Kind};
use crate::workspace::files::{discovery, read, write};
use read::Workspace;

struct ReadFile {
    workspace: Workspace,
}

impl ToolHandler for ReadFile {
    fn version(&self) -> &str {
        "workspace_read_file-3"
    }

    fn effect(&self) -> ToolEffect {
        ToolEffect::ReadOnly
    }

    fn call(&self, arguments: Value) -> ToolFuture<'_> {
        Box::pin(async move {
            let refused = |refusal: read::Refusal| ToolFailure::not_applied(refusal.to_string());
            let Some(path) = arguments.get("path").and_then(Value::as_str) else {
                return Err(refused(read::Refusal::InvalidPath));
            };
            match read::requested_range(&arguments).map_err(refused)? {
                None => {
                    let owned_path = path.to_owned();
                    let workspace = self.workspace.clone();
                    let content = blocking::run(Kind::File, move || workspace.read(&owned_path))
                        .await
                        .map_err(ToolFailure::uncertain)?
                        .map_err(refused)?;
                    Ok(json!({"path": path, "content": content}))
                }
                Some((start_line, line_count)) => {
                    let owned_path = path.to_owned();
                    let workspace = self.workspace.clone();
                    let range = blocking::run(Kind::File, move || {
                        workspace.read_range(&owned_path, start_line, line_count)
                    })
                    .await
                    .map_err(ToolFailure::uncertain)?
                    .map_err(refused)?;
                    Ok(range.to_json(path))
                }
            }
        })
    }
}

pub(crate) fn register_read(
    registry: &ToolRegistry,
    workspace: Workspace,
) -> Result<(), ToolRegistryError> {
    register_static!(
        registry,
        read::TOOL_NAME,
        Some(
            "Read one UTF-8 text file from the authorized Workspace. With only `path`, return its complete contents; a file over 32 KiB fails the call, so read larger files in ranges. With `start_line` or `line_count`, return whole lines from `start_line` (from 1) of a file of up to 16 MiB, at most `line_count` (up to 2000) and about 32 KiB of them, exactly as the file holds them, with `end_line` (null when the file ends before `start_line`), `total_lines`, and `next_start_line` to continue from (null at the end of the file); `line_truncated` says the one line returned is cut because it alone is longer than the limit.",
        ),
        read::parameters(),
        Arc::new(ReadFile { workspace }),
    )
}

struct StopOnDrop(Arc<AtomicBool>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

fn refused(refusal: impl std::fmt::Display) -> ToolFailure {
    ToolFailure::not_applied(refusal.to_string())
}

struct ListDirectory {
    workspace: Workspace,
    limits: discovery::DiscoveryLimits,
}

impl ToolHandler for ListDirectory {
    fn version(&self) -> &str {
        "workspace_list_directory-1"
    }

    fn effect(&self) -> ToolEffect {
        ToolEffect::ReadOnly
    }

    fn call(&self, arguments: Value) -> ToolFuture<'_> {
        Box::pin(async move {
            let path = discovery::directory_path(&arguments)
                .map_err(refused)?
                .to_owned();
            let workspace = self.workspace.clone();
            let limits = self.limits;
            blocking::run(Kind::File, move || {
                discovery::list(&workspace, &path, &limits)
            })
            .await
            .map_err(ToolFailure::uncertain)?
            .map_err(refused)
        })
    }
}

struct Search {
    workspace: Workspace,
    limits: discovery::DiscoveryLimits,
}

impl ToolHandler for Search {
    fn version(&self) -> &str {
        "workspace_search-2"
    }

    fn effect(&self) -> ToolEffect {
        ToolEffect::ReadOnly
    }

    fn call(&self, arguments: Value) -> ToolFuture<'_> {
        Box::pin(async move {
            let query = discovery::query(&arguments).map_err(refused)?;
            let stop = Arc::new(AtomicBool::new(false));
            let stopping = StopOnDrop(stop.clone());
            let (workspace, limits) = (self.workspace.clone(), self.limits);

            let found = blocking::run(Kind::File, move || {
                discovery::search(&workspace, &query, &limits, &stop)
            })
            .await
            .map_err(|_| ToolFailure::not_applied("the search ended without a result"))?;
            drop(stopping);
            found.map_err(refused)
        })
    }
}

pub(crate) fn register_discovery(
    registry: &ToolRegistry,
    workspace: Workspace,
) -> Result<(), ToolRegistryError> {
    let limits = discovery::DiscoveryLimits::default();
    register_static!(
        registry,
        discovery::LIST_TOOL_NAME,
        Some(
            "List one directory of the authorized Workspace: its entries sorted by name, each with its type (file, directory, symlink, other, inaccessible) and a file's size in bytes. Hidden and secret-bearing names are not shown, and symlinks are never followed. At most 200 entries; `truncated` says some were left out and `total_entries` counts them all.",
        ),
        discovery::list_parameters(),
        Arc::new(ListDirectory {
            workspace: workspace.clone(),
            limits,
        }),
    )?;
    register_static!(
        registry,
        discovery::SEARCH_TOOL_NAME,
        Some(
            "Find files below a directory of the authorized Workspace by name, by content, or both: give `name`, `text`, or both. Returns matching file paths with their sizes and, for `text`, the matching line numbers and lines (each cut to 200 bytes, at most 10 per file). Hidden, secret-bearing, symlinked, binary and over 1 MiB files are not searched, nor `target` or `node_modules` directories unless `path` names them. `complete` is false when a bound (results, entries, content_bytes, time) named by `limit` ended the search early; `skipped` counts entries that could not be searched.",
        ),
        discovery::search_parameters(),
        Arc::new(Search { workspace, limits }),
    )
}

struct WriteFile(write::WriteFile);

impl ToolHandler for WriteFile {
    fn version(&self) -> &str {
        "workspace_write_file-4"
    }

    fn effect(&self) -> ToolEffect {
        ToolEffect::NonIdempotent
    }

    fn spawns(&self) -> bool {
        true
    }

    fn call(&self, arguments: Value) -> ToolFuture<'_> {
        Box::pin(async move { self.0.write(None, arguments).await.map_err(super::failure) })
    }

    fn call_for(&self, call: ToolCallRef, arguments: Value) -> ToolFuture<'_> {
        Box::pin(async move {
            self.0
                .write(Some(call.job), arguments)
                .await
                .map_err(super::failure)
        })
    }

    fn stop(&self, job: JobId) -> Pin<Box<dyn Future<Output = Result<(), ToolFailure>> + '_>> {
        Box::pin(async move {
            self.0.stop(job).await;
            Ok(())
        })
    }
}

pub(crate) fn register_write(
    registry: &ToolRegistry,
    workspace: Workspace,
) -> Result<(), ToolRegistryError> {
    register_static!(
        registry,
        write::TOOL_NAME,
        Some(
            "Edit UTF-8 text files in the authorized Workspace. `create` {path, content} creates a new file. `update` {path, content, expected_content} replaces a whole file matching `expected_content`. Each content field is limited to 48 KiB. `replace` {path, old, new} replaces one unique occurrence of `old` in a file up to 1 MiB (up to 48 KiB per field). `multi_replace` {path, edits: [{old, new}, ...]} applies 1 to 32 edits atomically. Each `old` must occur exactly once in the original file, and matches must not overlap. All edits together may contain up to 48 KiB of old and new text. The original and resulting files may each be up to 1 MiB. `delete` {path, expected_content} deletes a file matching `expected_content` (up to 64 KiB). Directories are never created or deleted.",
        ),
        write::parameters(),
        Arc::new(WriteFile(write::WriteFile::new(workspace))),
    )
}
