use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use super::super::registry::{
    ToolFailure, ToolFuture, ToolHandler, ToolRegistry, ToolRegistryError, ToolStart,
};
use super::super::tool_loop::ToolLoopLimits;
use crate::db::entities::{JobId, ProjectCommand, ToolCallRef, ToolEffect};
use crate::workspace::command::{self, CommandEnvironment};
use crate::workspace::files::read::Workspace;
use crate::workspace::process;

struct RunCommand(command::RunCommand);

impl ToolHandler for RunCommand {
    fn version(&self) -> &str {
        "workspace_run_command-7"
    }

    fn effect(&self) -> ToolEffect {
        ToolEffect::NonIdempotent
    }

    fn spawns(&self) -> bool {
        true
    }

    fn stop(&self, job: JobId) -> Pin<Box<dyn Future<Output = Result<(), ToolFailure>> + '_>> {
        Box::pin(async move { self.0.stop(job).await.map_err(super::failure) })
    }

    fn call(&self, arguments: Value) -> ToolFuture<'_> {
        Box::pin(async move { self.0.call(arguments).await.map_err(super::failure) })
    }

    fn call_for(&self, call: ToolCallRef, arguments: Value) -> ToolFuture<'_> {
        Box::pin(async move {
            self.0
                .call_for(call, arguments, ToolStart::default())
                .await
                .map_err(super::failure)
        })
    }

    fn call_with_start(
        &self,
        call: ToolCallRef,
        arguments: Value,
        start: ToolStart,
    ) -> ToolFuture<'_> {
        Box::pin(async move {
            self.0
                .call_for(call, arguments, start)
                .await
                .map_err(super::failure)
        })
    }
}

pub(crate) fn register(
    registry: &ToolRegistry,
    workspace: Workspace,
    commands: &[ProjectCommand],
    environment: CommandEnvironment,
    timeout: Duration,
) -> Result<(), ToolRegistryError> {
    registry.register(
        command::TOOL_NAME,
        Some(
            "Run one of the Project's commands in the authorized Workspace root, optionally appending literal entries from `arguments`, and return its argv, exit code, stdout, stderr, wall_ms, cpu_user_ms, cpu_system_ms and peak_rss_bytes. A nonzero exit code, such as a failing build or test, is a normal result to read, not a failure of this tool. Output is limited to about 12 KiB; `truncated` says whether part of it was omitted. The command's executable, configured arguments, directory and environment cannot be replaced.",
        ),
        command::parameters(commands),
        Arc::new(RunCommand(command::RunCommand::new(
            workspace,
            commands,
            environment,
            timeout,
        ))),
    )
}

pub(crate) fn coding_limits(timeout: Duration) -> ToolLoopLimits {
    ToolLoopLimits {
        handler_timeout: timeout.saturating_add(process::STOP_GRACE),
        ..ToolLoopLimits::default()
    }
}
