use super::blocking;
use super::tool_loop::{
    ASK_USER_TOOL_NAME, REQUEST_COMMAND_TOOL_NAME, ToolExecution, ToolLoopLimits,
};
use super::tools::{ToolFailure, ToolFuture, ToolHandler, ToolRegistry, ToolRegistryError};
use super::workspace_read::{self, Workspace};
use crate::db::entities::{FrozenExecutionConfig, ModelCapability, ProjectRecord, ToolEffect};
use crate::service::ai::providers::CancellationToken;
use crate::service::jobs::{DevelopmentToolContext, DevelopmentToolOffer};

const MAX_AGENTS_MD_BYTES: usize = 32 * 1024;

pub(crate) struct NativeTooling<'a> {
    data_directory: &'a crate::system::data_directory::DataDirectory,
    stops: crate::system::native_scheduler::NativeStops,
    built: Option<ToolExecution>,
    cancellation: CancellationToken,
}

impl<'a> NativeTooling<'a> {
    pub(crate) fn new(
        data_directory: &'a crate::system::data_directory::DataDirectory,
        stops: crate::system::native_scheduler::NativeStops,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            data_directory,
            stops,
            built: None,
            cancellation,
        }
    }

    pub(crate) fn into_execution(self) -> Option<ToolExecution> {
        self.built
    }

    pub(crate) async fn offer(
        &mut self,
        frozen: &FrozenExecutionConfig,
        project: Option<&ProjectRecord>,
        context: DevelopmentToolContext,
    ) -> Result<Option<DevelopmentToolOffer>, &'static str> {
        let agent = frozen.agent.as_ref();
        let workspace = frozen.workspace_enabled(project.is_some())?;
        let tools = frozen
            .llm
            .config
            .declared_capabilities
            .contains(&ModelCapability::Tools);
        let jobs = agent.is_some_and(|agent| agent.manages_jobs()) && tools;
        if !workspace && !jobs {
            return Ok(None);
        }
        let project = project.filter(|_| workspace);
        let ask = context.decider && context.nested && tools;
        let wiring = Wiring {
            ask,
            decider: context.decider,
            jobs,
            plan: !context.plan_step,
        };
        use ntex::util::{Either, select};

        let (execution, offer) = match select(
            self.cancellation.cancelled(),
            build(self.data_directory, project, wiring, self.stops.clone()),
        )
        .await
        {
            Either::Left(()) => return Err("preparation_cancelled"),
            Either::Right(result) => result?,
        };
        self.built = Some(execution);
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
async fn authorize_workspace(
    root: &std::path::Path,
    data_directory: &crate::system::data_directory::DataDirectory,
) -> Result<(Workspace, Option<String>), &'static str> {
    let root = root.to_path_buf();
    let directory = data_directory.clone();
    match blocking::run(blocking::Kind::File, move || {
        let workspace = Workspace::authorize(&root, &directory)?;
        let agents_md = read_agents_md(&workspace);
        Ok::<_, workspace_read::WorkspaceError>((workspace, agents_md))
    })
    .await
    {
        Ok(Ok((workspace, agents_md))) => Ok((workspace, agents_md)),
        Ok(Err(error)) => {
            log::warn!("the Project's root no longer authorizes: {error}");
            Err("workspace_unauthorized")
        }
        Err(error) => {
            log::warn!("the Project's root could not be checked: {error}");
            Err("workspace_unchecked")
        }
    }
}

#[cfg(unix)]
async fn build(
    data_directory: &crate::system::data_directory::DataDirectory,
    project: Option<&ProjectRecord>,
    wiring: Wiring,
    stops: crate::system::native_scheduler::NativeStops,
) -> Result<(ToolExecution, DevelopmentToolOffer), &'static str> {
    let mut agents_md = None;
    let workspace = match project {
        Some(project) => {
            let root = std::path::PathBuf::from(&project.root);
            let (workspace, content) = authorize_workspace(&root, data_directory).await?;
            if workspace.root() != std::path::Path::new(&project.root) {
                return Err("workspace_root_changed");
            }
            agents_md = content;
            Some((workspace, project.commands.as_slice()))
        }
        None => None,
    };
    let granted = workspace.is_some();
    let execution = tools_of(workspace, wiring, stops).map_err(|error| {
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
            grant: granted.then_some("code"),
            agents_md,
        },
    ))
}

#[cfg(not(unix))]
async fn build(
    _data_directory: &crate::system::data_directory::DataDirectory,
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
    let limits = if workspace.is_some() {
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
fn read_agents_md(workspace: &Workspace) -> Option<String> {
    use super::workspace_read::{Refusal, Step};

    let result = (|| -> Result<Option<String>, String> {
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
    })();
    match result {
        Ok(content) => content,
        Err(reason) => {
            log::warn!("skipping AGENTS.md: {reason}");
            None
        }
    }
}
