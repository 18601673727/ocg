use std::cell::RefCell;

use super::tool_loop::{
    ASK_USER_TOOL_NAME, REQUEST_COMMAND_TOOL_NAME, ToolExecution, ToolLoopLimits,
};
use super::tools::{ToolFailure, ToolFuture, ToolHandler, ToolRegistry, ToolRegistryError};
use super::workspace_read::{self, Workspace};
use crate::db::entities::{FrozenExecutionConfig, ModelCapability, ProjectRecord, ToolEffect};
use crate::db::{DevelopmentToolContext, DevelopmentToolOffer, DevelopmentTooling};
use crate::system::execution_controls::ExecutionControl;

fn offered_grant(mutation: bool) -> &'static str {
    if mutation { "code" } else { "read" }
}

const MAX_AGENTS_MD_BYTES: usize = 32 * 1024;

pub(crate) struct NativeTooling<'a> {
    data_directory: Option<&'a crate::system::data_directory::DataDirectory>,
    stops: crate::system::native_scheduler::NativeStops,
    built: RefCell<Option<ToolExecution>>,
}

impl<'a> NativeTooling<'a> {
    pub(crate) fn new(
        data_directory: Option<&'a crate::system::data_directory::DataDirectory>,
        stops: crate::system::native_scheduler::NativeStops,
    ) -> Self {
        Self {
            data_directory,
            stops,
            built: RefCell::new(None),
        }
    }

    pub(crate) fn into_execution(self) -> Option<ToolExecution> {
        self.built.into_inner()
    }
}

impl DevelopmentTooling for NativeTooling<'_> {
    async fn offer(
        &self,
        frozen: &FrozenExecutionConfig,
        project: Option<&ProjectRecord>,
        context: DevelopmentToolContext,
    ) -> Result<Option<DevelopmentToolOffer>, &'static str> {
        let agent = frozen.agent.as_ref();
        let workspace = agent.is_some_and(|agent| agent.workspace_grant().is_some());
        let tools = frozen
            .llm
            .config
            .declared_capabilities
            .contains(&ModelCapability::Tools);
        let jobs = agent.is_some_and(|agent| agent.manages_jobs())
            && tools
            && ExecutionControl::ToolsAndChildJobs.is_enabled();
        if !workspace && !jobs {
            return Ok(None);
        }
        if workspace && !ExecutionControl::ToolsAndChildJobs.is_enabled() {
            return Err("tools_disabled");
        }
        let project = if workspace {
            let (Some(_), Some(project)) = (self.data_directory, project) else {
                return Err("workspace_unavailable");
            };
            Some(project)
        } else {
            None
        };
        let ask = context.decider && context.nested && tools;
        let wiring = Wiring {
            ask,
            decider: context.decider,
            mutation: ExecutionControl::WorkspaceMutation.is_enabled(),
            jobs,
            plan: !context.plan_step,
        };
        let (execution, offer) =
            build(self.data_directory, project, wiring, self.stops.clone()).await?;
        *self.built.borrow_mut() = Some(execution);
        Ok(Some(offer))
    }
}

struct AwaitsUser(&'static str);

impl ToolHandler for AwaitsUser {
    fn version(&self) -> &str {
        self.0
    }
    fn effect(&self) -> ToolEffect {
        ToolEffect::NonIdempotent
    }
    fn call(&self, _arguments: serde_json::Value) -> ToolFuture<'_> {
        Box::pin(async {
            Err(ToolFailure::not_applied(
                "this call is decided by the user, not run",
            ))
        })
    }
}

fn register_request_command(registry: &ToolRegistry) -> Result<(), ToolRegistryError> {
    registry.register(
        REQUEST_COMMAND_TOOL_NAME,
        Some("Request a new named command before running it. Supply the exact literal argv and a task-specific reason. The user approves or declines; an approved command joins the Project's commands but does not run. Then call workspace_run_command by name."),
        serde_json::json!({"type":"object","additionalProperties":false,"properties":{"name":{"type":"string","minLength":1,"maxLength":64,"pattern":"^[a-z][a-z0-9_]{0,63}$"},"argv":{"type":"array","minItems":1,"maxItems":64,"items":{"type":"string","minLength":1,"maxLength":4096}},"reason":{"type":"string","minLength":1,"maxLength":4096}},"required":["name","argv","reason"]}),
        std::sync::Arc::new(AwaitsUser("workspace_request_command-1")),
    )?;
    registry.authorize(REQUEST_COMMAND_TOOL_NAME)
}

fn register_ask_user(registry: &ToolRegistry) -> Result<(), ToolRegistryError> {
    registry.register(
        ASK_USER_TOOL_NAME,
        Some("Ask the deciding user a question and wait for their answer."),
        serde_json::json!({"type":"object","additionalProperties":false,"properties":{"question":{"type":"string","minLength":1}},"required":["question"]}),
        std::sync::Arc::new(AwaitsUser("ask_user-1")),
    )?;
    registry.authorize(ASK_USER_TOOL_NAME)
}

#[cfg(unix)]
async fn build(
    data_directory: Option<&crate::system::data_directory::DataDirectory>,
    project: Option<&ProjectRecord>,
    wiring: Wiring,
    stops: crate::system::native_scheduler::NativeStops,
) -> Result<(ToolExecution, DevelopmentToolOffer), &'static str> {
    use super::blocking::{self, Kind};
    let mut agents_md = None;
    let workspace = match (data_directory, project) {
        (Some(data_directory), Some(project)) => {
            let root = std::path::PathBuf::from(&project.root);
            let directory = data_directory.clone();
            let workspace =
                blocking::run(Kind::File, move || Workspace::authorize(&root, &directory))
                    .await
                    .map_err(|error| {
                        log::warn!("the Project's root could not be checked: {error}");
                        "workspace_unchecked"
                    })?
                    .map_err(|error| {
                        log::warn!("the Project's root no longer authorizes: {error}");
                        "workspace_unauthorized"
                    })?;
            if workspace.root() != std::path::Path::new(&project.root) {
                return Err("workspace_root_changed");
            }
            agents_md = read_agents_md(&workspace).await;
            Some((workspace, project.commands.as_slice()))
        }
        _ => None,
    };
    let granted = workspace.is_some();
    let mutation = wiring.mutation && granted;
    let execution = tools_of(workspace, Wiring { mutation, ..wiring }, stops).map_err(|error| {
        log::warn!("the tools cannot be offered: {error}");
        "tools_unavailable"
    })?;
    let wires = execution.offer.wires().map_err(|error| {
        log::warn!("the tools cannot be offered: {error}");
        "tools_unavailable"
    })?;
    Ok((
        execution,
        DevelopmentToolOffer {
            wires,
            grant: granted.then(|| offered_grant(mutation)),
            agents_md,
        },
    ))
}

#[cfg(not(unix))]
async fn build(
    _data_directory: Option<&crate::system::data_directory::DataDirectory>,
    _project: Option<&ProjectRecord>,
    _wiring: Wiring,
    _stops: crate::system::native_scheduler::NativeStops,
) -> Result<(ToolExecution, DevelopmentToolOffer), &'static str> {
    Err("workspace_unsupported")
}

#[derive(Clone, Copy)]
struct Wiring {
    ask: bool,
    decider: bool,
    mutation: bool,
    jobs: bool,
    plan: bool,
}

#[cfg(unix)]
fn tools_of(
    workspace: Option<(Workspace, &[crate::db::entities::ProjectCommand])>,
    wiring: Wiring,
    stops: crate::system::native_scheduler::NativeStops,
) -> Result<ToolExecution, ToolRegistryError> {
    let registry = ToolRegistry::default();
    let limits = if wiring.mutation {
        crate::service::ai::command::coding_limits(crate::service::ai::command::COMMAND_TIMEOUT)
    } else {
        ToolLoopLimits::default()
    };
    let mut names = Vec::new();
    let captured_workspace = workspace.as_ref().map(|(workspace, _)| workspace.clone());
    if let Some((workspace, commands)) = workspace {
        workspace_read::register(&registry, workspace.clone())?;
        super::workspace_discovery::register(&registry, workspace.clone())?;
        crate::service::fts::register(&registry, workspace.clone())?;
        #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
        crate::service::lsp::register(&registry, workspace.clone(), limits.handler_timeout)?;

        names.extend([
            workspace_read::TOOL_NAME,
            super::workspace_discovery::LIST_TOOL_NAME,
            super::workspace_discovery::SEARCH_TOOL_NAME,
            crate::service::fts::TOOL_NAME,
        ]);
        #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
        names.push(crate::service::lsp::TOOL_NAME);
        super::network::register(&registry)?;
        names.extend(super::network::NAMES);
        if wiring.mutation {
            super::workspace_write::register(&registry, workspace.clone())?;
            names.push(super::workspace_write::TOOL_NAME);
            crate::service::ai::command::register(
                &registry,
                workspace,
                commands,
                crate::service::ai::command::CommandEnvironment::of_process(),
                crate::service::ai::command::COMMAND_TIMEOUT,
            )?;
            names.push(crate::service::ai::command::TOOL_NAME);
            if wiring.decider {
                register_request_command(&registry)?;
                names.push(REQUEST_COMMAND_TOOL_NAME);
            }
        }
        for name in &names {
            registry.authorize(name)?;
        }
    }
    if wiring.jobs {
        names.extend(super::job_tools::register(
            &registry,
            wiring.decider,
            wiring.plan,
        )?);
    }
    if wiring.ask {
        register_ask_user(&registry)?;
        names.push(ASK_USER_TOOL_NAME);
    }
    Ok(ToolExecution {
        decider: wiring.decider,
        offer: registry.offer(&names)?,
        limits,
        stops,
        workspace: captured_workspace,
    })
}

#[cfg(unix)]
async fn read_agents_md(workspace: &Workspace) -> Option<String> {
    use super::blocking::{self, Kind};
    use super::workspace_read::{Refusal, Step};

    let workspace = workspace.clone();
    let result = blocking::run(Kind::File, move || -> Result<Option<String>, String> {
        let (parent, name) = match workspace.parent("AGENTS.md") {
            Ok(located) => located,
            Err(Refusal::NotFound) => return Ok(None),
            Err(error) => return Err(format!("failed to locate AGENTS.md: {error}")),
        };
        let file = match workspace_read::open_within(&parent, name, Step::File, MAX_AGENTS_MD_BYTES)
        {
            Ok(file) => file,
            Err(Refusal::NotFound) => return Ok(None),
            Err(error) => return Err(format!("failed to open AGENTS.md: {error}")),
        };
        let bytes = match workspace_read::read_bytes(&file, MAX_AGENTS_MD_BYTES) {
            Ok(bytes) => bytes,
            Err(Refusal::NotFound) => return Ok(None),
            Err(error) => return Err(format!("failed to read AGENTS.md: {error}")),
        };
        let text = String::from_utf8(bytes)
            .map_err(|error| format!("AGENTS.md is not valid UTF-8: {error}"))?;
        if text.contains('\0') {
            return Err("AGENTS.md contains a NUL byte".to_owned());
        }
        Ok(Some(text))
    })
    .await;
    match result {
        Ok(Ok(content)) => content,
        Ok(Err(reason)) => {
            log::warn!("skipping AGENTS.md: {reason}");
            None
        }
        Err(error) => {
            log::warn!("skipping AGENTS.md: blocking task failed: {error}");
            None
        }
    }
}
