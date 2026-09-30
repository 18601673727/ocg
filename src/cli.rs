//! Command-line interface: argument parsing, environment resolution and
//! dispatch.
//!
//! The CLI exposes the `ocg` binary and owns the `OCG_*` environment namespace.

use crate::build;
use crate::capabilities::{CapabilityConfig, CapabilityEvidence, CapabilityPlan};
use crate::clock::{Clock, SystemClock};
use crate::config;
use crate::context::{self, ContextConfig, ContextEngine};
use crate::defaults::{load_defaults, OcgSource};
use crate::error::OcgError;
use crate::http::{GithubToken, HttpTransport, NoHttp, ProcessHttpEnv, ReqwestHttp, Secret};
use crate::model;
use crate::observability;
use crate::orchestration::checkpoint::{self, Phase};
use crate::platform::Platform;
use crate::preflight::{Availability, ModelPreflight};
use crate::process::{
    ProcessHost, ProcessRunner, SystemCaptureRunner, SystemGitHost, SystemProcessHost,
    SystemStaticProxy,
};
use crate::project;
use crate::provider_gateway::{GatewayRoute, ProviderGateway};
use crate::provider_transport::ProviderTransportConfig;
use crate::proxy::{ProxyScheme, ProxySelection, ProxySource};
use crate::report;
use crate::runtime::compat::{
    self, BridgeRuntimeClient, LeadSelection, RuntimeAdapter, SessionClient,
};
use crate::runtime::effective as runtime_effective;
use crate::runtime::lifecycle::RuntimeAdapter as RuntimeLifecycleAdapter;
use crate::runtime::lifecycle::RuntimeIdentity;
use crate::runtime::policy::RuntimePolicy;
use crate::runtime::{self, install::Layout};
use crate::telemetry::{self, TelemetryConfig};
use crate::validate;
use crate::verification::runner::{execute, VerifyRequest};
use crate::verification::Config as VerificationConfig;
use semver::Version;
use serde_json::{json, Value};
use std::ffi::OsString;
use std::fmt;
use std::path::{Path, PathBuf};

/// Printed by `version` and embedded in the help header.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Process environment, resolved once so the rest of the code stays pure.
#[derive(Default, Clone)]
pub struct Env {
    pub home: Option<PathBuf>,
    pub user_config: Option<PathBuf>,
    /// Explicit OpenCode executable. Canonical `OCG_OPENCODE`, with
    /// `OCG_OPENCODE_BIN` as a compatibility alias.
    pub opencode_bin: Option<OsString>,
    pub trace: Option<PathBuf>,
    pub xdg_config_home: Option<PathBuf>,
    pub home_dir: Option<PathBuf>,
    /// Override for the GitHub API base (mirrors and tests).
    pub api_base: Option<String>,
    /// Override for the update-check cache directory.
    pub cache_dir: Option<PathBuf>,
    /// `OCG_TELEMETRY` on/off override.
    pub telemetry: Option<String>,
    /// `OCG_ORCHESTRATION` on/off escape hatch.
    pub orchestration: Option<String>,
    /// Invocation-scoped OpenCode V2 endpoint exported to the generated
    /// bridge. It is never persisted in OCG artifacts.
    pub v2_server_url: Option<String>,
    /// Invocation-scoped local service password. `Secret` keeps Debug/Display
    /// redacted while the child process receives the raw value through env.
    pub v2_server_password: Option<Secret>,
    /// The session selected by the launch preflight. It is a target identity,
    /// not a Mission identity.
    pub v2_target_session: Option<String>,
    /// Exact directory used to create the invocation's V2 sessions.
    pub v2_directory: Option<String>,
    /// Resolved Lead contract exported to the generated bridge.
    pub v2_lead: Option<LeadSelection>,
    /// `GH_TOKEN` (preferred) then `GITHUB_TOKEN` for OCG-owned GitHub calls.
    pub github_token: Option<GithubToken>,
}

impl fmt::Debug for Env {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Env")
            .field("home", &self.home)
            .field("user_config", &self.user_config)
            .field("opencode_bin", &self.opencode_bin)
            .field("trace", &self.trace)
            .field("xdg_config_home", &self.xdg_config_home)
            .field("home_dir", &self.home_dir)
            .field("api_base", &self.api_base.as_ref().map(|_| "<redacted>"))
            .field("cache_dir", &self.cache_dir)
            .field("telemetry", &self.telemetry)
            .field("orchestration", &self.orchestration)
            .field(
                "v2_server_url",
                &self.v2_server_url.as_ref().map(|_| "<redacted>"),
            )
            .field("v2_server_password", &self.v2_server_password)
            .field("v2_target_session", &self.v2_target_session)
            .field("v2_directory", &self.v2_directory)
            .field("v2_lead", &self.v2_lead)
            .field("github_token", &self.github_token)
            .finish()
    }
}

impl Env {
    pub fn from_process() -> Self {
        let home_dir = std::env::var_os("HOME")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("USERPROFILE").map(PathBuf::from));
        Self {
            home: env_path(&["OCG_HOME"]),
            user_config: env_path(&["OCG_USER_CONFIG"]),
            opencode_bin: env_os(&["OCG_OPENCODE", "OCG_OPENCODE_BIN"]),
            trace: env_path(&["OCG_TRACE"]),
            xdg_config_home: env_path(&["XDG_CONFIG_HOME"]),
            home_dir,
            api_base: env_string(&["OCG_API_BASE"]),
            cache_dir: env_path(&["OCG_CACHE_DIR"]),
            telemetry: env_string(&["OCG_TELEMETRY"]),
            orchestration: env_string(&["OCG_ORCHESTRATION"]),
            v2_server_url: env_string(&["OCG_V2_SERVER_URL"]),
            v2_server_password: env_string(&["OCG_V2_SERVER_PASSWORD"]).map(Secret::new),
            v2_target_session: env_string(&["OCG_V2_TARGET_SESSION"]),
            v2_directory: env_string(&["OCG_V2_DIRECTORY"]),
            v2_lead: env_string(&["OCG_LEAD_CONTRACT"])
                .and_then(|raw| serde_json::from_str(&raw).ok()),
            github_token: crate::http::github_token_from_env(&ProcessHttpEnv),
        }
    }
}

fn env_os(names: &[&str]) -> Option<OsString> {
    for name in names {
        if let Some(value) = std::env::var_os(name) {
            if !value.is_empty() {
                return Some(value);
            }
        }
    }
    None
}

fn env_path(names: &[&str]) -> Option<PathBuf> {
    env_os(names).map(PathBuf::from)
}

fn env_string(names: &[&str]) -> Option<String> {
    env_os(names).and_then(|value| value.into_string().ok())
}

/// A command-line usage error. Always exits with status 2.
#[derive(Debug)]
pub struct UsageError(pub String);

#[derive(Debug)]
pub enum Command {
    Launch,
    Run(Vec<OsString>),
    Models(Vec<OsString>),
    Status,
    Routing,
    Config(Vec<OsString>),
    Auth(Vec<OsString>),
    Validate,
    Layers,
    Init,
    Build,
    Trace(Option<String>),
    Context(Vec<OsString>),
    Cache(Option<String>),
    Stats(Vec<OsString>),
    Verify(Vec<OsString>),
    Tools(Vec<OsString>),
    Checkpoint(Vec<OsString>),
    /// Explicitly reconcile durable Missions once.
    Reconcile(Vec<OsString>),
    /// Read-only inspection of the descriptive Resource Registry.
    Resources(Vec<OsString>),
    /// Read-only inspection of the effective Policy and the latest admission
    /// decision per durable Mission.
    Policy(Vec<OsString>),
    /// Inspect the mandatory economic configuration and durable Mission budget,
    /// or explicitly set a hard Mission budget (the only way past a hard cap).
    Budget(Vec<OsString>),
    /// Read-only listing of durable, generation-bound approval requests.
    Approvals(Vec<OsString>),
    /// Resolve a pending approval as approved.
    Approve(Vec<OsString>),
    /// Resolve a pending approval as rejected.
    Reject(Vec<OsString>),
    /// Run the loopback-only HTTP/SSE control server.
    Serve(Vec<OsString>),
    /// Canonical Job/Attempt control: admit, configure, dispatch, deliver and
    /// inspect a canonical Mission. Every mutating operation is witness-bound.
    Work(Vec<OsString>),
    /// Run the project-scoped STDIO MCP adapter.
    Mcp(Vec<OsString>),
    /// Hidden/internal: the generated plugin's bridge. Never advertised.
    Bridge(Vec<OsString>),
    Version,
    Doctor,
    Upgrade,
    Help,
}

#[derive(Debug)]
pub struct Cli {
    /// OCG Profile model key selected for this invocation.
    pub model_choice: Option<String>,
    pub project: Option<PathBuf>,
    pub dry_run: bool,
    pub pretty: bool,
    pub user_config: Option<PathBuf>,
    /// `--disable-proxy`: never use any proxy, ambient or system.
    pub disable_proxy: bool,
    /// `--effective`: also resolve and observe the live runtime state
    /// (Configured / Resolved / Effective) for `status` and `doctor`.
    pub effective: bool,
    pub command: Command,
}

/// Parse arguments. Options may appear before or after the command.
pub fn parse<I>(args: I) -> std::result::Result<Cli, UsageError>
where
    I: IntoIterator<Item = OsString>,
{
    let args: Vec<OsString> = args.into_iter().collect();
    let mut model_choice: Option<String> = None;
    let mut project: Option<PathBuf> = None;
    let mut dry_run = false;
    let mut pretty = false;
    let mut user_config: Option<PathBuf> = None;
    let mut command_token: Option<String> = None;
    let mut disable_proxy = false;
    let mut effective = false;
    let mut rest: Vec<OsString> = Vec::new();
    let mut event: Option<String> = None;

    let mut index = 0;
    let mut passthrough = false;
    while index < args.len() {
        let text = args[index].to_string_lossy().into_owned();

        if passthrough {
            rest.push(args[index].clone());
            index += 1;
            continue;
        }

        if text == "--" {
            index += 1;
            if index < args.len() {
                command_token = Some(args[index].to_string_lossy().into_owned());
                index += 1;
                rest = args[index..].to_vec();
            }
            break;
        }
        if let Some(value) = text.strip_prefix("--model=") {
            model_choice = Some(value.to_string());
            index += 1;
            continue;
        }
        if text == "--model" {
            let value = args
                .get(index + 1)
                .ok_or_else(|| UsageError("--model needs a Profile model key".to_string()))?;
            model_choice = Some(value.to_string_lossy().into_owned());
            index += 2;
            continue;
        }
        if let Some(value) = text.strip_prefix("--project=") {
            project = Some(PathBuf::from(value));
            index += 1;
            continue;
        }
        if text == "--project" || text == "--cwd" {
            let value = args
                .get(index + 1)
                .ok_or_else(|| UsageError(format!("{text} needs a value")))?;
            project = Some(PathBuf::from(value));
            index += 2;
            continue;
        }
        if let Some(value) = text.strip_prefix("--cwd=") {
            project = Some(PathBuf::from(value));
            index += 1;
            continue;
        }
        if let Some(value) = text.strip_prefix("--user-config=") {
            user_config = Some(PathBuf::from(value));
            index += 1;
            continue;
        }
        if text == "--user-config" {
            let value = args
                .get(index + 1)
                .ok_or_else(|| UsageError("--user-config needs a value".to_string()))?;
            user_config = Some(PathBuf::from(value));
            index += 2;
            continue;
        }
        if text == "--dry-run" {
            dry_run = true;
            index += 1;
            continue;
        }
        if text == "--disable-proxy" {
            disable_proxy = true;
            index += 1;
            continue;
        }
        if text == "--pretty" {
            pretty = true;
            index += 1;
            continue;
        }
        if text == "--effective" {
            effective = true;
            index += 1;
            continue;
        }
        if let Some(value) = text.strip_prefix("--event=") {
            event = Some(value.to_string());
            index += 1;
            continue;
        }
        if text == "--event" {
            let value = args
                .get(index + 1)
                .ok_or_else(|| UsageError("--event needs a value".to_string()))?;
            event = Some(value.to_string_lossy().into_owned());
            index += 2;
            continue;
        }
        if text == "-h" || text == "--help" {
            if command_token.as_deref() == Some("config") {
                rest.push(args[index].clone());
                index += 1;
                continue;
            }
            command_token = Some("help".to_string());
            break;
        }
        if text == "--version" {
            command_token = Some("version".to_string());
            break;
        }
        if text.starts_with('-') && text != "-" {
            // Only commands that actually accept subcommand options may collect
            // an unknown option. `checkpoint save --phase ...` and
            // `config lead high --model ... --yes` do; every legacy command
            // (validate, status, doctor, version, routing, layers, build,
            // upgrade, cache, context, verify, ...) keeps the strict usage
            // error it always had.
            if matches!(
                command_token.as_deref(),
                Some("checkpoint")
                    | Some("config")
                    | Some("reconcile")
                    | Some("resources")
                    | Some("policy")
                    | Some("budget")
                    | Some("approvals")
                    | Some("approve")
                    | Some("reject")
                    | Some("serve")
                    | Some("work")
                    | Some("mcp")
            ) {
                rest.push(args[index].clone());
                index += 1;
                continue;
            }
            return Err(UsageError(format!(
                "unknown option: {text} (try 'ocg help')"
            )));
        }

        if command_token.is_none() {
            command_token = Some(text);
            index += 1;
            // `run` and `models` forward everything after them to OpenCode.
            if matches!(command_token.as_deref(), Some("run") | Some("models")) {
                passthrough = true;
            }
            continue;
        }
        // A positional argument for a reporting command (for example the
        // model key after `config model remove`). Options are still parsed.
        rest.push(args[index].clone());
        index += 1;
    }

    let command = match command_token.as_deref() {
        None => Command::Launch,
        Some("run") => Command::Run(rest),
        Some("models") => Command::Models(rest),
        Some("status") => Command::Status,
        Some("routing") | Some("routes") => Command::Routing,
        Some("config") => Command::Config(rest),
        Some("auth") => Command::Auth(rest),
        Some("validate") => Command::Validate,
        Some("layers") => Command::Layers,
        Some("init") => Command::Init,
        Some("build") => Command::Build,
        Some("dry-run") => Command::Build,
        Some("trace") => Command::Trace(event),
        Some("context") => Command::Context(rest),
        Some("cache") => Command::Cache(
            rest.first()
                .map(|value| value.to_string_lossy().into_owned()),
        ),
        Some("stats") => Command::Stats(rest),
        Some("verify") => Command::Verify(rest),
        Some("tools") => Command::Tools(rest),
        Some("checkpoint") => Command::Checkpoint(rest),
        Some("reconcile") => Command::Reconcile(rest),
        Some("resources") => Command::Resources(rest),
        Some("policy") => Command::Policy(rest),
        Some("budget") => Command::Budget(rest),
        Some("approvals") => Command::Approvals(rest),
        Some("approve") => Command::Approve(rest),
        Some("reject") => Command::Reject(rest),
        Some("serve") => Command::Serve(rest),
        Some("work") => Command::Work(rest),
        Some("mcp") => Command::Mcp(rest),
        Some("__bridge") => Command::Bridge(rest),
        Some("version") => Command::Version,
        Some("doctor") => Command::Doctor,
        Some("upgrade") => Command::Upgrade,
        Some("help") => Command::Help,
        Some(other) => {
            return Err(UsageError(format!(
                "unknown command: {other} (try 'ocg help')"
            )))
        }
    };

    Ok(Cli {
        model_choice,
        project,
        dry_run,
        pretty,
        user_config,
        disable_proxy,
        effective,
        command,
    })
}

fn usage() -> &'static str {
    r#"OCG - project-agnostic multi-model orchestration

Usage:
  ocg [--project DIR] [--model KEY] [--dry-run] [--disable-proxy] [command] [args...]

Commands:
  (none)                open the local OCG PWA (first run enters Profile onboarding)
  run <args...>         launch `opencode run` with the OCG config
  models [args...]      run `opencode models` with the OCG config
  status                show configuration and runtime status
  routing               show the worker role -> model table
  config [profile|candidates|new|import PATH SHA256]
                          inspect or explicitly bootstrap an OCG-owned Profile
  config provider add KEY LABEL | remove KEY
  config model add KEY PROVIDER ID [--default] | remove KEY
                          edit the user-global Profile (not OpenCode config)
  auth list | set ENV_NAME | remove ENV_NAME
                          store provider credentials in the encrypted user vault
  validate              validate the merged configuration
  layers                show configuration layers and trace state
  init                  create a minimal global OCG profile
  build                 print the resolved OpenCode config
  context <task...>     build a deterministic local repository context plan
  context symbols <q>   find indexed symbols by name (diagnostic)
  cache clean|stats     manage the local context cache (never the runtime)
  stats [--pretty]      read-only project telemetry aggregate (local only)
  verify [fast|normal|full]
                        run the configured trusted commands for a stage
  tools <task...>       show the capability plan / Tool Context Firewall view
  checkpoint list|show|save
                        inspect, or create, a phase checkpoint
  reconcile [--once]    reconcile canonical dispatch authority once
  resources [--json] [--observe]
                        inspect the descriptive Resource Registry (read-only)
  policy [--json]       show the effective Project policy
  budget [--json]       show the canonical Project budget
  budget set --project-id <id> --limit <micros> --currency <CUR>
                        explicitly set a hard Project budget
  approvals [--json]    list durable approval requests (read-only)
  approve <id> [--note TEXT] [--json]
                        approve a pending admission request
  reject <id> [--note TEXT] [--json]
                        reject a pending admission request
  serve [--addr 127.0.0.1:PORT]
                        run the loopback-only HTTP/SSE control server
  work create|admit|child|plan|dispatch|replace|finish|deliver|inspect|ready|recover|status|set-config|config
                        canonical Job / Attempt execution commands
  mcp                   run the local project-scoped STDIO MCP server
  version               report OCG, platform and the resolved OpenCode runtime
  doctor                diagnose layering, Lead contracts, OpenCode, proxy and runtime (read-only)
  upgrade               self-update OCG, then maintain the active OpenCode
  help                  show this help

Options:
  --project DIR         project working directory used for local state
  --model KEY           select a configured OCG Profile model for this invocation
  --dry-run             print the merged OpenCode config instead of launching
  --disable-proxy       never use a proxy (overrides env and system discovery)
  --pretty              pretty-print JSON output (build / --dry-run)
  --effective           with status/doctor, also resolve the runtime and verify the
                        live effective Lead (starts and terminates a private server)
  --user-config PATH    global OCG profile/config file
  -h, --help            show this help

Environment:
  OCG_HOME           config directory loaded instead of the embedded defaults
  OCG_FRONTEND_DIR   built frontend project for the local PWA host
  OCG_USER_CONFIG    global OCG profile/config file (default ~/.config/ocg/config.yaml)
  OCG_PROJECT_CONFIG is removed; project-local OCG config is ignored
  OCG_OPENCODE       explicit opencode binary (wins over everything)
  OCG_OPENCODE_BIN   compatibility alias for the same explicit binary
  OCG_TRACE          trace file; only read when observability is enabled
  OCG_CACHE_DIR      override the update-check cache directory
  OCG_API_BASE       override the GitHub API base (mirrors, tests)
  OCG_TELEMETRY      0/1 to force local telemetry off/on
  OCG_ORCHESTRATION  0/1 to force orchestration off/on (0 is no-hook)
  OCG_DISABLE_PROXY  1/true/on/yes disables proxy use for this process
  HTTP_PROXY / HTTPS_PROXY / ALL_PROXY / NO_PROXY
                                standard proxy variables, upper- or lower-case; used when
                                OCG_DISABLE_PROXY is not truthy
  GH_TOKEN / GITHUB_TOKEN       optional GitHub API token (GH_TOKEN wins); sent only to
                                api.github.com

OCG_OPENCODE_BIN is accepted as a compatibility alias. A broken explicit binary is authoritative and errors
instead of silently falling back to another runtime.
"#
}

#[derive(Debug)]
enum Failure {
    Usage(String),
    Ocg(OcgError),
}

impl From<OcgError> for Failure {
    fn from(error: OcgError) -> Self {
        Failure::Ocg(error)
    }
}

fn usage_failure(message: impl Into<String>) -> Failure {
    Failure::Usage(message.into())
}

/// Entry point. Returns the process exit code.
pub fn run(args: impl Iterator<Item = OsString>) -> i32 {
    observability::init_tracing();
    match run_inner(args) {
        Ok(code) => code,
        Err(Failure::Usage(message)) => {
            eprintln!("ocg: {message}");
            2
        }
        Err(Failure::Ocg(error)) => {
            eprintln!("ocg: {error}");
            2
        }
    }
}

fn run_inner(args: impl Iterator<Item = OsString>) -> std::result::Result<i32, Failure> {
    let cli = parse(args).map_err(|error| usage_failure(error.0))?;
    tracing::debug!(
        pretty = cli.pretty,
        dry_run = cli.dry_run,
        "parsed CLI invocation"
    );
    let env = Env::from_process();

    match &cli.command {
        Command::Help => {
            print!("{}", usage());
            return Ok(0);
        }
        Command::Version => {
            return version_command(&cli, &env);
        }
        _ => {}
    }

    if let Some(project) = &cli.project {
        if !project.is_dir() {
            return Err(usage_failure(format!(
                "--project is not a directory: {}",
                project.display()
            )));
        }
    }

    let invocation_dir = match &cli.project {
        Some(project) => project.clone(),
        None => std::env::current_dir()
            .map_err(|error| OcgError::io("cannot determine the current directory", error))
            .map_err(Failure::Ocg)?,
    };
    // An explicit `--project` names the workspace; otherwise the nearest
    // ancestor carrying an `.ocg` state directory wins. Both paths are canonicalized so a
    // symlinked spelling cannot create a second boundary.
    let boundary = match &cli.project {
        Some(root) => project::explicit(root),
        None => project::resolve(&invocation_dir),
    };
    let project_root = boundary.root().to_path_buf();
    let (ocg_source, ocg_home) = match env.home.clone() {
        Some(home) => (OcgSource::Dir(home.clone()), Some(home)),
        None => (OcgSource::Embedded, None),
    };
    let defaults = load_defaults(&ocg_source).map_err(Failure::Ocg)?;
    // `ocg config` builds a candidate configuration from the same defaults and
    // the same resolved layer paths, so keep them available for that command.
    let config_defaults = defaults.clone();
    let config_ocg_home = ocg_home.clone();
    let user_path = config::user_config_path(
        cli.user_config.as_deref(),
        env.user_config.as_deref(),
        env.xdg_config_home.as_deref(),
        env.home_dir.as_deref(),
    );
    // Profiles are user-global. `project_root` remains the workspace boundary
    // for orchestration state, context, verification and generated plugins.
    let project_path = user_path.clone();
    if let Command::Init = cli.command {
        return init_command(&user_path);
    }
    if matches!(
        cli.command,
        Command::Run(_)
            | Command::Serve(_)
            | Command::Mcp(_)
            | Command::Reconcile(_)
            | Command::Work(_)
            | Command::Bridge(_)
    ) && !project_path.is_file()
    {
        return Err(Failure::Ocg(OcgError::config(format!(
            "OCG Profile is required at {}; run interactive `ocg` onboarding or create the global profile before headless/serve execution",
            project_path.display()
        ))));
    }
    let effective = config::build_effective(
        defaults,
        ocg_home.clone(),
        &project_root,
        &user_path,
        &project_path,
        env.home_dir.clone(),
    )
    .map_err(Failure::Ocg)?;
    let mut effective = effective;
    if matches!(
        cli.command,
        Command::Serve(_) | Command::Mcp(_) | Command::Work(_)
    ) {
        crate::profile::Profile::from_ocg_config(&effective.data).map_err(Failure::Ocg)?;
    }
    // The orchestration escape hatch is applied to the effective config so that
    // build, dry-run, launch and the bridge all agree for one process.
    crate::orchestration::OrchestrationConfig::apply_env_override(
        &mut effective.data,
        env.orchestration.as_deref(),
    );
    let level = cli
        .model_choice
        .clone()
        .or_else(|| {
            crate::profile::Profile::from_ocg_config(&effective.data)
                .ok()
                .and_then(|profile| profile.default_model)
        })
        .unwrap_or_default();

    if cli.dry_run
        && !matches!(
            cli.command,
            Command::Reconcile(_) | Command::Serve(_) | Command::Work(_) | Command::Mcp(_)
        )
    {
        // Runtime-facing output: a dry-run must print the same contract a real
        // launch would use, so the runtime family is resolved exactly like
        // launch/doctor/models resolve it.
        let adapter = config_output_adapter(&project_root, &effective, &env)?;
        let resolved =
            build::build_opencode_config_for(&effective, &level, adapter).map_err(Failure::Ocg)?;
        print_config(&resolved, cli.pretty)?;
        return Ok(0);
    }

    match &cli.command {
        Command::Help | Command::Version => Ok(0),
        Command::Launch => {
            let profile = crate::profile::ProfileService::with_workspace(&user_path, &project_root)
                .current()
                .map_err(Failure::Ocg)?;
            crate::pwa::run(&project_root, &user_path, profile.is_some()).map_err(Failure::Ocg)?;
            Ok(0)
        }
        Command::Run(args) => {
            let forwarded = prepend_subcommand("run", args);
            launch(
                &effective,
                &invocation_dir,
                &project_root,
                &level,
                &forwarded,
                true,
                true,
                &env,
                cli.disable_proxy,
            )
        }
        Command::Models(args) => {
            let forwarded = prepend_subcommand("models", args);
            launch(
                &effective,
                &invocation_dir,
                &project_root,
                &level,
                &forwarded,
                false,
                false,
                &env,
                cli.disable_proxy,
            )
        }
        Command::Status => {
            validate::require_valid(&effective).map_err(Failure::Ocg)?;
            let text = report::status_text(&effective, &level).map_err(Failure::Ocg)?;
            println!("{text}");
            if cli.effective {
                print_runtime_state(
                    &effective,
                    &project_root,
                    &invocation_dir,
                    &level,
                    &env,
                    cli.disable_proxy,
                    true,
                )?;
            }
            Ok(0)
        }
        Command::Routing => {
            validate::require_valid(&effective).map_err(Failure::Ocg)?;
            let text = report::routing_text(&effective).map_err(Failure::Ocg)?;
            println!("{text}");
            Ok(0)
        }
        Command::Config(args) => config_command(
            args,
            config_defaults,
            config_ocg_home,
            &invocation_dir,
            &project_root,
            &user_path,
            &project_path,
            &effective,
            &level,
            &env,
            cli.disable_proxy,
        ),
        Command::Auth(args) => auth_command(args),
        Command::Validate => {
            let errors = validate::validate(&effective);
            if !errors.is_empty() {
                eprintln!("OCG configuration errors:");
                for error in &errors {
                    eprintln!("  - {error}");
                }
                return Ok(1);
            }
            // Structural validity is independent from execution readiness.
            // In particular a placeholder-only Profile is valid but cannot
            // generate a runnable compatibility config or send inference.
            println!("configuration is structurally valid");
            for diagnostic in &effective.diagnostics {
                eprintln!("ocg: migration: {diagnostic}");
            }
            Ok(0)
        }
        Command::Layers => {
            println!("{}", report::layers_text(&effective, env.trace.as_deref()));
            Ok(0)
        }
        Command::Init => init_command(&invocation_dir),
        Command::Build => {
            // Runtime-facing output, like `--dry-run`: follow the detected
            // runtime family rather than hard-coding the v1 contract.
            let adapter = config_output_adapter(&project_root, &effective, &env)?;
            let resolved = build::build_opencode_config_for(&effective, &level, adapter)
                .map_err(Failure::Ocg)?;
            print_config(&resolved, cli.pretty)?;
            Ok(0)
        }
        Command::Trace(event) => {
            let event = event.as_deref().unwrap_or("launch");
            if let Some(path) =
                observability::record_event(&effective, event, &level, env.trace.as_deref())
            {
                println!("{}", path.display());
            }
            Ok(0)
        }
        Command::Doctor => doctor_command(
            &effective,
            &project_root,
            &invocation_dir,
            &level,
            &env,
            cli.disable_proxy,
            cli.effective,
        ),
        Command::Context(args) => {
            boundary.require(&invocation_dir).map_err(Failure::Ocg)?;
            context_command(&effective, &project_root, args, &env, cli.pretty)
        }
        Command::Cache(action) => {
            boundary.require(&invocation_dir).map_err(Failure::Ocg)?;
            cache_command(&effective, &project_root, action.as_deref())
        }
        Command::Stats(args) => {
            // `stats` is read-only: it may run outside an initialized project
            // and simply report that no local telemetry exists.
            stats_command(&effective, &project_root, args, &env, cli.pretty)
        }
        Command::Verify(args) => {
            boundary.require(&invocation_dir).map_err(Failure::Ocg)?;
            verify_command(&effective, &project_root, args, &env, cli.pretty)
        }
        Command::Tools(args) => tools_command(&effective, args, cli.pretty),
        Command::Checkpoint(args) => {
            boundary.require(&invocation_dir).map_err(Failure::Ocg)?;
            checkpoint_command(&effective, &project_root, args, cli.pretty)
        }
        Command::Reconcile(args) => {
            boundary.require(&invocation_dir).map_err(Failure::Ocg)?;
            reconcile_command(
                &effective,
                &project_root,
                &invocation_dir,
                &level,
                &env,
                args,
                cli.pretty,
            )
        }
        Command::Resources(args) => {
            boundary.require(&invocation_dir).map_err(Failure::Ocg)?;
            resources_command(&effective, &project_root, &env, args, cli.pretty)
        }
        Command::Policy(args) => {
            boundary.require(&invocation_dir).map_err(Failure::Ocg)?;
            policy_command(&effective, &project_root, args, cli.pretty)
        }
        Command::Budget(args) => {
            boundary.require(&invocation_dir).map_err(Failure::Ocg)?;
            budget_command(&effective, &project_root, args, cli.pretty)
        }
        Command::Approvals(args) => {
            boundary.require(&invocation_dir).map_err(Failure::Ocg)?;
            approvals_command(&effective, &project_root, args, cli.pretty)
        }
        Command::Approve(args) => {
            boundary.require(&invocation_dir).map_err(Failure::Ocg)?;
            resolve_approval_command(
                &effective,
                &project_root,
                args,
                crate::orchestration::policy::ApprovalStatus::Approved,
                cli.pretty,
            )
        }
        Command::Reject(args) => {
            boundary.require(&invocation_dir).map_err(Failure::Ocg)?;
            resolve_approval_command(
                &effective,
                &project_root,
                args,
                crate::orchestration::policy::ApprovalStatus::Rejected,
                cli.pretty,
            )
        }
        Command::Serve(args) => {
            boundary.require(&invocation_dir).map_err(Failure::Ocg)?;
            serve_command(&project_root, &user_path, args, cli.pretty)
        }
        Command::Work(args) => {
            boundary.require(&invocation_dir).map_err(Failure::Ocg)?;
            work_command(&effective, &project_root, args, cli.pretty)
        }
        Command::Mcp(args) => {
            boundary.require(&invocation_dir).map_err(Failure::Ocg)?;
            if args.is_empty() {
                crate::mcp::serve_stdio(&project_root).map_err(Failure::Ocg)?;
                return Ok(0);
            }
            if args.first().and_then(|arg| arg.to_str()) != Some("--http") {
                return Err(usage_failure(
                    "ocg mcp accepts no arguments, or --http [--addr 127.0.0.1:PORT]",
                ));
            }
            let mut addr = "127.0.0.1:0".to_string();
            let mut index = 1;
            while index < args.len() {
                let text = args[index].to_string_lossy();
                if text == "--addr" || text.starts_with("--addr=") {
                    addr = option_value(args, "--addr", &mut index)?;
                    continue;
                }
                return Err(usage_failure(format!("unknown mcp option: {text}")));
            }
            crate::mcp::serve_http(&project_root, &addr).map_err(Failure::Ocg)?;
            Ok(0)
        }
        Command::Bridge(args) => {
            boundary.require(&invocation_dir).map_err(Failure::Ocg)?;
            bridge_command(&effective, &project_root, args, &env)
        }
        Command::Upgrade => upgrade_command(&effective, &project_root, &env, cli.disable_proxy),
    }
}

/// `ocg reconcile [--once]`: one bounded, explicit convergence pass over the
/// durable Mission store. The command does not start a daemon or a background
/// worker. A live V2 client is used only when the invocation already has a
/// runtime endpoint or OpenCode's registered service is available; otherwise
/// Missions are reported as runtime-unavailable and no replacement is made.
fn reconcile_command(
    _effective: &config::Effective,
    project_root: &Path,
    _invocation_dir: &Path,
    _level: &str,
    _env: &Env,
    args: &[OsString],
    pretty: bool,
) -> std::result::Result<i32, Failure> {
    for arg in args {
        if arg != "--once" {
            return Err(usage_failure(format!(
                "unknown reconcile option: {} (only --once is supported)",
                arg.to_string_lossy()
            )));
        }
    }
    let mut repository =
        crate::orchestration::domain::DomainRepository::open(project_root).map_err(Failure::Ocg)?;
    let result = repository.reconcile_dispatches().map_err(Failure::Ocg)?;
    print_json(&result, pretty);
    Ok(0)
}

/// `ocg resources [--json] [--observe]`: read-only inspection of the
/// descriptive Resource Registry.
///
/// It never selects, ranks, scores or routes a resource. `--observe` resolves
/// the local runtime and records its identity, lifecycle capabilities and a
/// factual availability observation; without it nothing is written and only the
/// configured + previously persisted facts are shown.
fn resources_command(
    effective: &config::Effective,
    project_root: &Path,
    env: &Env,
    args: &[OsString],
    pretty: bool,
) -> std::result::Result<i32, Failure> {
    let mut json = false;
    let mut observe = false;
    for arg in args {
        match arg.to_str() {
            Some("--json") => json = true,
            Some("--observe") => observe = true,
            _ => {
                return Err(usage_failure(format!(
                    "unknown resources option: {} (only --json and --observe are supported)",
                    arg.to_string_lossy()
                )))
            }
        }
    }
    validate::require_valid(effective).map_err(Failure::Ocg)?;

    let clock = SystemClock;
    let now = clock.now_unix();
    let process = SystemProcessHost;
    let manager = runtime_manager(project_root, effective, env, &NoHttp, &clock, &process)
        .map_err(Failure::Ocg)?;
    let report = manager.resolve_for_report();
    let resolved = report
        .version
        .clone()
        .and_then(|version| compat::classify(version).ok());
    let runtime_identity = resolved
        .as_ref()
        .map(|version| RuntimeIdentity::new("opencode", version.major().as_str(), "resolved"));

    let loaded = crate::resources::load(project_root);
    let file_corrupt = loaded.corrupt;
    let mut issues = loaded.issues;
    let mut registry = loaded.registry;

    if observe {
        if let (Some(identity), Some(version)) = (&runtime_identity, resolved.as_ref()) {
            let adapter = compat::adapter_for(version);
            registry.observe_runtime_capabilities(
                identity.clone(),
                adapter.lifecycle_capabilities(),
                now,
            );
            let runtime_scoped = crate::resources::ResourceIdentity::for_runtime(identity);
            registry.observe_available(
                &runtime_scoped,
                format!(
                    "runtime {} resolved ({})",
                    identity.family,
                    describe_runtime_version(report.version.as_ref())
                ),
                now,
            );
            if let Err(error) = crate::resources::save(project_root, &registry) {
                issues.push(crate::resources::ResourceIssue {
                    resource: crate::resources::registry_path(project_root)
                        .display()
                        .to_string(),
                    detail: format!("registry was not persisted: {error}"),
                });
            }
        }
    }

    for entry in crate::resources::configured_entries(&effective.data, runtime_identity.as_ref())
        .map_err(Failure::Ocg)?
    {
        registry.register_configured(&entry.identity, entry.configured, now);
    }

    let records = registry.list();
    if json {
        let value = json!({
            "schema_version": registry.schema_version(),
            "updated_at": registry.updated_at(),
            "corrupt": file_corrupt,
            "issues": issues
                .iter()
                .map(|issue| json!({"resource": issue.resource, "detail": issue.detail}))
                .collect::<Vec<_>>(),
            "resources": records,
        });
        println!(
            "{}",
            if pretty {
                serde_json::to_string_pretty(&value).unwrap_or_else(|_| "{}".to_string())
            } else {
                serde_json::to_string(&value).unwrap_or_else(|_| "{}".to_string())
            }
        );
    } else {
        print_resources_text(&records, now, file_corrupt, &issues);
    }
    Ok(0)
}

fn print_resources_text(
    records: &[crate::resources::ResourceRecord],
    now: i64,
    corrupt: bool,
    issues: &[crate::resources::ResourceIssue],
) {
    if corrupt {
        println!("registry: corrupt (unreadable or unsupported schema); no facts are trusted");
    }
    if records.is_empty() {
        println!("no resources known");
    }
    for record in records {
        println!("{}", record.resource_id);
        println!("  identity      {}", record.identity.describe());
        if !record.configured.is_empty() {
            let uses = record
                .configured
                .iter()
                .map(|use_| {
                    format!(
                        "{} (variant {})",
                        use_.role.as_deref().unwrap_or("?"),
                        use_.variant.as_deref().unwrap_or("default")
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            println!("  configured    {uses}");
        }
        match &record.runtime.value {
            Some(runtime) => println!(
                "  runtime       {}/{} [{}]",
                runtime.runtime,
                runtime.family,
                record.runtime.provenance.as_str()
            ),
            None => println!("  runtime       unknown"),
        }
        match &record.capabilities.value {
            Some(capabilities) => {
                let names = capabilities.listed();
                println!(
                    "  capabilities  {}",
                    if names.is_empty() {
                        "none".to_string()
                    } else {
                        names.join(", ")
                    }
                );
            }
            None => println!("  capabilities  unknown"),
        }
        println!(
            "  resolved      {} [{}]",
            record
                .resolved
                .value
                .map(|evidence| evidence.as_str())
                .unwrap_or("unknown"),
            record.resolved.provenance.as_str()
        );
        match &record.effective.value {
            Some(effective) => println!(
                "  effective     {}/{} agent {} variant {} [{}]",
                effective.provider.as_deref().unwrap_or("?"),
                effective.model.as_deref().unwrap_or("?"),
                effective.agent.as_deref().unwrap_or("?"),
                effective.variant.as_deref().unwrap_or("default"),
                record.effective.provenance.as_str()
            ),
            None => println!("  effective     unknown"),
        }
        println!(
            "  health        {}{} [{}]",
            record.health.state.as_str(),
            record
                .health
                .reason
                .as_deref()
                .map(|reason| format!(" ({reason})"))
                .unwrap_or_default(),
            record.health.provenance.as_str()
        );
        match record.context_limit.value {
            Some(limit) => println!(
                "  context limit {limit} [{}]",
                record.context_limit.provenance.as_str()
            ),
            None => println!("  context limit unknown"),
        }
        match record.capacity.value {
            Some(slots) => println!(
                "  capacity      {slots} slots [{}]",
                record.capacity.provenance.as_str()
            ),
            None => println!("  capacity      unknown"),
        }
        println!(
            "  quota         {}",
            if record.quota.is_known() {
                "known"
            } else {
                "unknown"
            }
        );
        println!(
            "  cost          {}",
            if record.cost.is_known() {
                "known"
            } else {
                "unknown"
            }
        );
        match record.observed_at() {
            Some(at) => println!(
                "  observed      {at}{}",
                if record.is_stale(now, crate::resources::DEFAULT_STALE_AFTER_SECONDS) {
                    " (stale)"
                } else {
                    ""
                }
            ),
            None => println!("  observed      never"),
        }
    }
    for issue in issues {
        println!("warning: resource {}: {}", issue.resource, issue.detail);
    }
}

/// `ocg policy [--json]`: read-only inspection of the effective admission
/// policy and the latest durable Policy decision per Mission.
///
/// It never evaluates a live action, mutates a Mission or writes an approval.
fn policy_command(
    effective: &config::Effective,
    project_root: &Path,
    args: &[OsString],
    pretty: bool,
) -> std::result::Result<i32, Failure> {
    let mut json = false;
    for arg in args {
        match arg.to_str() {
            Some("--json") => json = true,
            _ => {
                return Err(usage_failure(format!(
                    "unknown policy option: {} (only --json is supported)",
                    arg.to_string_lossy()
                )))
            }
        }
    }
    validate::require_valid(effective).map_err(Failure::Ocg)?;
    let config = crate::orchestration::policy::PolicyConfig::from_config(&effective.data)
        .map_err(Failure::Ocg)?;

    let repository =
        crate::orchestration::domain::DomainRepository::open(project_root).map_err(Failure::Ocg)?;
    let project = repository
        .ensure_project(project_root)
        .map_err(Failure::Ocg)?;
    let value = json!({"enabled":config.enabled,"require_approval_for":config.require_approval_for,
        "fingerprint":config.fingerprint(),"project_id":project.id});
    if json {
        print_json(&value, pretty);
    } else {
        println!("{value}");
    }
    Ok(0)
}

/// `ocg budget [--json]` and `ocg budget set ...`.
///
/// The read form is read-only: it shows the effective economic configuration
/// and the canonical Project's durable accounting. The `set` form is the only
/// supported way to raise or change a hard Project budget; a generic approval
/// can never do it, and no currency is ever converted.
fn budget_command(
    effective: &config::Effective,
    project_root: &Path,
    args: &[OsString],
    pretty: bool,
) -> std::result::Result<i32, Failure> {
    if args.first().and_then(|arg| arg.to_str()) == Some("set") {
        return budget_set_command(effective, project_root, &args[1..], pretty);
    }
    let mut json = false;
    for arg in args {
        match arg.to_str() {
            Some("--json") => json = true,
            _ => {
                return Err(usage_failure(format!(
                    "unknown budget option: {} (only --json, or 'set', is supported)",
                    arg.to_string_lossy()
                )))
            }
        }
    }
    validate::require_valid(effective).map_err(Failure::Ocg)?;
    let config = crate::orchestration::budget::BudgetConfig::from_config(&effective.data)
        .map_err(Failure::Ocg)?;

    let repository =
        crate::orchestration::domain::DomainRepository::open(project_root).map_err(Failure::Ocg)?;
    let project = repository
        .ensure_project(project_root)
        .map_err(Failure::Ocg)?;
    let budget = repository
        .project_budget(&project.id)
        .map_err(Failure::Ocg)?;

    if json {
        let value = json!({
            "configured": config.hard_limit_micros.is_some(),
            "currency": config.currency,
            "hard_limit_micros": config.hard_limit_micros,
            "estimated_operation_cost_micros": config.estimated_operation_cost_micros,
            "require_quota": config.require_quota,
            "fingerprint": config.fingerprint(),
            "project_id": project.id,
            "budget": {
                "status": budget.status,
                "origin": budget.origin,
                "currency": budget.currency,
                "hard_limit_micros": budget.hard_limit_micros,
                "settled_micros": budget.settled_micros,
                "reserved_micros": budget.reserved_micros,
                "unresolved_micros": budget.unresolved_micros,
                "released_micros": budget.released_micros,
                "overage_micros": budget.overage_micros,
                "reservation_count": budget.reservation_count,
                "settlement_count": budget.settlement_count,
                "unresolved_settlement_count": budget.unresolved_settlement_count,
                "reason": budget.reason,
            },
        });
        print_json(&value, pretty);
    } else {
        match (config.hard_limit_micros, config.currency.as_deref()) {
            (Some(limit), Some(currency)) => {
                println!("default hard budget: {limit} micros {currency}")
            }
            _ => println!("default hard budget: none (no economic cutoff)"),
        }
        println!(
            "estimated provider-costly operation: {}",
            match (
                config.estimated_operation_cost_micros,
                config.currency.as_deref()
            ) {
                (Some(estimate), Some(currency)) => format!("{estimate} micros {currency}"),
                _ => "unknown (a hard-budgeted provider-costly action defers)".to_string(),
            }
        );
        println!("require quota: {}", config.require_quota);
        println!(
            "configured token prices: {} (a completed dispatch with no price settles as \
             unresolved, never at a guess)",
            config.pricing.len()
        );
        println!("project {} budget", project.id);
        println!(
            "  {} origin {} currency {}",
            budget.status,
            budget.origin,
            if budget.currency.is_empty() {
                "unknown"
            } else {
                &budget.currency
            }
        );
        println!(
            "  hard limit {} settled {} reserved {} unresolved {}",
            budget
                .hard_limit_micros
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_string()),
            budget.settled_micros,
            budget.reserved_micros,
            budget.unresolved_micros,
        );
        println!(
            "  released {} overage {} ({} reservations, {} settlements, {} unresolved)",
            budget.released_micros,
            budget.overage_micros,
            budget.reservation_count,
            budget.settlement_count,
            budget.unresolved_settlement_count,
        );
        if let Some(reason) = &budget.reason {
            println!("  reason {reason}");
        }
    }
    Ok(0)
}

/// `ocg budget set --project-id <id> --limit <micros> --currency <CUR>`: the
/// only supported way past a hard cap. It is an explicit operator change to the
/// durable Project hard budget itself, never an approval. `--mission` is
/// accepted as a legacy alias for the same Project identity.
fn budget_set_command(
    effective: &config::Effective,
    project_root: &Path,
    args: &[OsString],
    pretty: bool,
) -> std::result::Result<i32, Failure> {
    // `--mission` remains a legacy alias for the same canonical Project
    // identity; normalize the flag before parsing so both the `--flag value`
    // and `--flag=value` forms behave identically.
    let normalized: Vec<OsString> = args
        .iter()
        .map(|arg| {
            let text = arg.to_string_lossy();
            if text.starts_with("--mission") {
                OsString::from(text.replacen("--mission", "--project-id", 1))
            } else {
                arg.clone()
            }
        })
        .collect();
    let mut json = false;
    let mut project_id: Option<String> = None;
    let mut limit: Option<i64> = None;
    let mut currency: Option<String> = None;
    let mut index = 0;
    while index < normalized.len() {
        let text = normalized[index].to_string_lossy().into_owned();
        match text.as_str() {
            "--json" => {
                json = true;
                index += 1;
            }
            "--project-id" => {
                project_id = Some(option_value(&normalized, "--project-id", &mut index)?)
            }
            "--limit" => {
                let value = option_value(&normalized, "--limit", &mut index)?;
                limit = Some(value.parse::<i64>().map_err(|_| {
                    usage_failure(format!("--limit must be an integer, got '{value}'"))
                })?);
            }
            "--currency" => currency = Some(option_value(&normalized, "--currency", &mut index)?),
            _ if text.starts_with("--project-id=") => {
                project_id = Some(option_value(&normalized, "--project-id", &mut index)?)
            }
            _ if text.starts_with("--limit=") => {
                let value = option_value(&normalized, "--limit", &mut index)?;
                limit = Some(value.parse::<i64>().map_err(|_| {
                    usage_failure(format!("--limit must be an integer, got '{value}'"))
                })?);
            }
            _ if text.starts_with("--currency=") => {
                currency = Some(option_value(&normalized, "--currency", &mut index)?)
            }
            _ => return Err(usage_failure(format!("unknown budget set option: {text}"))),
        }
    }
    let project_id = project_id.ok_or_else(|| usage_failure("--project-id <id> is required"))?;
    let limit = limit.ok_or_else(|| usage_failure("--limit <micros> is required"))?;
    let currency = currency.ok_or_else(|| usage_failure("--currency <CUR> is required"))?;
    validate::require_valid(effective).map_err(Failure::Ocg)?;
    let mut repository =
        crate::orchestration::domain::DomainRepository::open(project_root).map_err(Failure::Ocg)?;
    let amount = crate::orchestration::budget::Money::new(limit, currency);
    let changed = repository
        .set_project_budget(&project_id, amount)
        .map_err(Failure::Ocg)?;
    let budget = repository
        .project_budget(&project_id)
        .map_err(Failure::Ocg)?;
    if json {
        print_json(
            &serde_json::to_value(&budget).unwrap_or(serde_json::Value::Null),
            pretty,
        );
    } else {
        println!(
            "{project_id} hard budget set to {} micros {} (changed {changed})",
            budget.hard_limit_micros.unwrap_or(limit),
            budget.currency
        );
        println!("  status {} origin {}", budget.status, budget.origin);
    }
    Ok(0)
}

/// Read the value of `--name VALUE` or `--name=VALUE` and advance `index`.
fn option_value(
    args: &[OsString],
    name: &str,
    index: &mut usize,
) -> std::result::Result<String, Failure> {
    let text = args[*index].to_string_lossy().into_owned();
    let prefix = format!("{name}=");
    if let Some(value) = text.strip_prefix(prefix.as_str()) {
        *index += 1;
        if value.is_empty() {
            return Err(usage_failure(format!("{name} needs a value")));
        }
        return Ok(value.to_string());
    }
    let value = args
        .get(*index + 1)
        .ok_or_else(|| usage_failure(format!("{name} needs a value")))?;
    *index += 2;
    Ok(value.to_string_lossy().into_owned())
}

/// `ocg approvals [--json]`: read-only listing of durable approval requests.
fn approvals_command(
    effective: &config::Effective,
    project_root: &Path,
    args: &[OsString],
    pretty: bool,
) -> std::result::Result<i32, Failure> {
    let mut json = false;
    for arg in args {
        match arg.to_str() {
            Some("--json") => json = true,
            _ => {
                return Err(usage_failure(format!(
                    "unknown approvals option: {} (only --json is supported)",
                    arg.to_string_lossy()
                )))
            }
        }
    }
    validate::require_valid(effective).map_err(Failure::Ocg)?;
    let loaded = crate::orchestration::policy::list_approvals(project_root);
    if json {
        let value = json!({
            "approvals": loaded.approvals,
            "issues": loaded.issues,
        });
        print_json(&value, pretty);
    } else {
        if loaded.approvals.is_empty() {
            println!("no approvals recorded");
        }
        for record in &loaded.approvals {
            println!("{} {}", record.approval_id, record.status.as_str());
            println!(
                "  mission {} gen {} action {}",
                record.mission_id, record.generation, record.action
            );
            if let Some(execution) = &record.current_execution_id {
                println!("  execution {execution}");
            }
            println!(
                "  requested {} resolved {}",
                record.requested_at,
                record
                    .resolved_at
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "never".to_string())
            );
        }
        for issue in &loaded.issues {
            println!("warning: approval {}: {}", issue.file, issue.detail);
        }
    }
    Ok(0)
}

/// `ocg approve <id>` / `ocg reject <id>`: resolve one durable approval.
///
/// The resolution is bound to the record's exact Mission generation and action;
/// a later generation or a different action can never inherit it.
fn resolve_approval_command(
    effective: &config::Effective,
    project_root: &Path,
    args: &[OsString],
    status: crate::orchestration::policy::ApprovalStatus,
    pretty: bool,
) -> std::result::Result<i32, Failure> {
    let mut json = false;
    let mut note: Option<String> = None;
    let mut approval_id: Option<String> = None;
    let mut index = 0;
    while index < args.len() {
        let text = args[index].to_string_lossy().into_owned();
        if text == "--json" {
            json = true;
            index += 1;
            continue;
        }
        if text == "--note" {
            let value = args
                .get(index + 1)
                .ok_or_else(|| usage_failure("--note needs a value"))?;
            note = Some(value.to_string_lossy().into_owned());
            index += 2;
            continue;
        }
        if let Some(value) = text.strip_prefix("--note=") {
            note = Some(value.to_string());
            index += 1;
            continue;
        }
        if text.starts_with('-') {
            return Err(usage_failure(format!("unknown option: {text}")));
        }
        if approval_id.is_some() {
            return Err(usage_failure(format!("unexpected argument: {text}")));
        }
        approval_id = Some(text);
        index += 1;
    }
    let approval_id = approval_id.ok_or_else(|| usage_failure("an approval id is required"))?;
    validate::require_valid(effective).map_err(Failure::Ocg)?;
    let clock = SystemClock;
    let service = crate::orchestration::ControlService::open(project_root).map_err(Failure::Ocg)?;
    let (record, _cursor) = service
        .resolve_approval(&approval_id, status, note, clock.now_unix())
        .map_err(|error| Failure::Ocg(error.into_ocg_error()))?;
    if json {
        let value = serde_json::to_value(&record).unwrap_or(serde_json::Value::Null);
        print_json(&value, pretty);
    } else {
        println!("{} {}", record.approval_id, record.status.as_str());
        println!(
            "  mission {} gen {} action {}",
            record.mission_id, record.generation, record.action
        );
    }
    Ok(0)
}

fn print_json(value: &serde_json::Value, pretty: bool) {
    println!(
        "{}",
        if pretty {
            serde_json::to_string_pretty(value).unwrap_or_else(|_| "{}".to_string())
        } else {
            serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string())
        }
    );
}

/// `ocg serve [--addr 127.0.0.1:PORT]`: run the loopback-only HTTP/SSE control
/// server over the durable orchestration authority.
///
/// The server prints its bound base URL and then runs until the process is
/// terminated. Only loopback addresses are accepted, so the control plane can
/// never be reached from another host.
fn serve_command(
    project_root: &Path,
    profile_path: &Path,
    args: &[OsString],
    pretty: bool,
) -> std::result::Result<i32, Failure> {
    let mut addr = "127.0.0.1:0".to_string();
    let mut index = 0;
    while index < args.len() {
        let text = args[index].to_string_lossy().into_owned();
        if text == "--addr" || text.starts_with("--addr=") {
            addr = option_value(args, "--addr", &mut index)?;
            continue;
        }
        return Err(usage_failure(format!("unknown serve option: {text}")));
    }
    let config = crate::control_server::ServerConfig::default();
    let server = crate::control_server::ControlServer::bind_with_profile(
        &addr,
        project_root,
        profile_path,
        config,
    )
    .map_err(Failure::Ocg)?;
    let base = server.base_url();
    if pretty {
        print_json(&json!({ "listening": base }), true);
    } else {
        println!("listening on {base}");
    }
    let _ = std::io::Write::flush(&mut std::io::stdout());
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    server.serve(stop).map_err(Failure::Ocg)?;
    Ok(0)
}

/// `ocg work <subcommand>`: the canonical Job/Attempt control surface.
///
/// This is the same authority the bridge uses. Every mutating operation is
/// witness-bound: `dispatch` prints the durable dispatch witness, `deliver`
/// accepts exactly that witness, and a stale or fenced delivery is retained as
/// evidence without changing authoritative state.
fn work_command(
    _effective: &config::Effective,
    project_root: &Path,
    args: &[OsString],
    pretty: bool,
) -> std::result::Result<i32, Failure> {
    canonical_work_command(project_root, args, pretty)
}

fn canonical_work_command(
    project_root: &Path,
    args: &[OsString],
    pretty: bool,
) -> std::result::Result<i32, Failure> {
    let words = args
        .iter()
        .map(|value| value.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    let subcommand = words
        .first()
        .cloned()
        .ok_or_else(|| usage_failure("ocg work requires a subcommand"))?;
    let option = |name: &str| -> Option<String> {
        let flag = format!("--{name}");
        let prefix = format!("{flag}=");
        words.iter().enumerate().find_map(|(index, word)| {
            if word == &flag {
                words.get(index + 1).cloned()
            } else {
                word.strip_prefix(&prefix).map(str::to_string)
            }
        })
    };
    let required = |name: &str| -> std::result::Result<String, Failure> {
        option(name).ok_or_else(|| usage_failure(format!("--{name} is required")))
    };
    let mut repository =
        crate::orchestration::domain::DomainRepository::open(project_root).map_err(Failure::Ocg)?;
    let project = repository
        .ensure_project(project_root)
        .map_err(Failure::Ocg)?;
    let print = |value: serde_json::Value| {
        print_json(&value, pretty);
        Ok(0)
    };
    match subcommand.as_str() {
        "admit" | "create" => {
            let binding = option("session")
                .unwrap_or_else(|| option("binding").unwrap_or_else(|| "cli".into()));
            let payload = option("objective").unwrap_or_default();
            let kind = option("agent").unwrap_or_else(|| "lead".into());
            let admission = repository
                .admit_job(project, &binding, &payload, &kind)
                .map_err(Failure::Ocg)?;
            print(serde_json::json!({
                "project_id": admission.project.id,
                "job_id": admission.job.id,
                "attempt_id": admission.attempt.id,
                "executor_id": admission.executor.id,
                "generation": admission.attempt.generation,
            }))
        }
        "plan" | "child" => {
            let session = required("session")?;
            let parent = repository
                .authority_for_binding(&project.id, &session)
                .map_err(Failure::Ocg)?
                .ok_or_else(|| {
                    Failure::Ocg(OcgError::config("session has no canonical authority"))
                })?;
            let payload = option("objective").unwrap_or_default();
            let dependencies = option("depends-on")
                .map(|raw| {
                    raw.split(',')
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(str::to_string)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let dependency_refs = dependencies.iter().map(String::as_str).collect::<Vec<_>>();
            let job = repository
                .create_child_job(&parent, &payload, &dependency_refs)
                .map_err(Failure::Ocg)?;
            print(serde_json::json!({"project_id":job.project_id,"job_id":job.id,"job":job}))
        }
        "dispatch" => {
            let job_id = required("job")?;
            let (attempt, executor) = repository
                .dispatch_job(&job_id, &option("agent").unwrap_or_else(|| "worker".into()))
                .map_err(Failure::Ocg)?;
            print(
                serde_json::json!({"job_id":job_id,"attempt_id":attempt.id,"executor_id":executor.id,"generation":attempt.generation}),
            )
        }
        "replace" => {
            let job_id = required("job")?;
            let admission = repository
                .replace_attempt_checked(
                    &job_id,
                    &option("agent").unwrap_or_else(|| "worker".into()),
                    Some(&required("attempt")?),
                )
                .map_err(Failure::Ocg)?;
            print(
                serde_json::json!({"job_id":admission.job.id,"attempt_id":admission.attempt.id,"executor_id":admission.executor.id,"generation":admission.attempt.generation}),
            )
        }
        "finish" | "deliver" => {
            let attempt_id = required("attempt")?;
            let outcome = option("outcome").unwrap_or_else(|| "completed".into());
            if !matches!(outcome.as_str(), "completed" | "failed") {
                return Err(usage_failure(
                    "outcome must be completed or failed; cancellation requires confirmed stop",
                ));
            }
            if let Some(call_id) = option("call") {
                let generation = required("generation")?
                    .parse::<u64>()
                    .map_err(|_| usage_failure("--generation must be an integer"))?;
                let result = option("result").unwrap_or_else(|| "null".into());
                let witness = repository
                    .witness_for_call(&call_id, &attempt_id, generation)
                    .map_err(Failure::Ocg)?;
                let disposition = repository
                    .deliver_result(&witness, &result, outcome == "completed")
                    .map_err(Failure::Ocg)?;
                return print(
                    json!({"attempt_id":attempt_id,"call_id":call_id,"disposition":disposition,"applied":disposition == "authoritative"}),
                );
            } else if subcommand == "deliver" {
                return Err(usage_failure("deliver requires --call and --generation"));
            }
            repository
                .finish_attempt(&attempt_id, outcome == "completed")
                .map_err(Failure::Ocg)?;
            print(serde_json::json!({"attempt_id":attempt_id,"outcome":outcome,"applied":true}))
        }
        "inspect" => {
            let job_id = required("job")?;
            print(repository.inspect_job(&job_id).map_err(Failure::Ocg)?)
        }
        "ready" => print(
            json!({"project_id":project.id,"ready":repository.ready_jobs(&project.id).map_err(Failure::Ocg)?}),
        ),
        "recover" | "reconcile" => print(repository.reconcile_dispatches().map_err(Failure::Ocg)?),
        "status" => print(
            json!({"project":project,"jobs":repository.jobs(&project.id).map_err(Failure::Ocg)?}),
        ),
        "set-config" => {
            let job_id = required("job")?;
            let value: Value = serde_json::from_str(&required("json")?)
                .map_err(|error| usage_failure(error.to_string()))?;
            let revision = repository
                .set_job_configuration(&job_id, &value)
                .map_err(Failure::Ocg)?;
            print(json!({"job_id":job_id,"revision":revision,"configuration":value}))
        }
        "config" => {
            let job_id = required("job")?;
            print(
                json!({"job_id":job_id,"configuration":repository.job_configuration(&job_id).map_err(Failure::Ocg)?}),
            )
        }

        _ => Err(usage_failure(format!(
            "unknown canonical work subcommand: {subcommand}"
        ))),
    }
}

/// Create the user-global OCG Profile; never overwrite an existing file.
fn init_command(target: &Path) -> std::result::Result<i32, Failure> {
    let legacy = target.with_extension("json");
    if legacy.is_file() {
        return Err(Failure::Ocg(OcgError::config(format!(
            "refusing to initialize: unsupported JSON global config exists at {}\nOCG reads YAML only; convert it to {} (no migration is performed).",
            legacy.display(),
            target.display()
        ))));
    }
    if target.exists() {
        println!("global config already exists: {}", target.display());
        return Ok(0);
    }
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent).map_err(|error| {
            Failure::Ocg(OcgError::io(
                "cannot create global OCG config directory",
                error,
            ))
        })?;
    }
    crate::profile::persist_new(target, &crate::profile::Profile::new()).map_err(Failure::Ocg)?;
    println!("created {}", target.display());
    Ok(0)
}

fn print_config(config: &Value, pretty: bool) -> std::result::Result<(), Failure> {
    let text = if pretty {
        serde_json::to_string_pretty(config)
    } else {
        serde_json::to_string(config)
    }
    .map_err(|error| {
        Failure::Ocg(OcgError::config(format!(
            "cannot serialize the OpenCode config: {error}"
        )))
    })?;
    println!("{text}");
    Ok(())
}

/// `opencode run ...` / `opencode models ...` keep their subcommand name.
fn prepend_subcommand(subcommand: &str, args: &[OsString]) -> Vec<OsString> {
    let mut forwarded = Vec::with_capacity(1 + args.len());
    forwarded.push(OsString::from(subcommand));
    forwarded.extend(args.iter().cloned());
    forwarded
}

#[allow(clippy::too_many_arguments)]
fn launch(
    effective: &config::Effective,
    // Directory the child OpenCode process starts in and relative arguments
    // resolve against. This is the invocation directory, not the project root.
    invocation_dir: &Path,
    // The resolved project boundary that owns local state and the plugin.
    project_root: &Path,
    level: &str,
    args: &[OsString],
    trace: bool,
    coding_session: bool,
    env: &Env,
    disable_proxy: bool,
) -> std::result::Result<i32, Failure> {
    // Validate before any runtime resolution so an invalid configuration fails
    // fast and can never trigger an install or upgrade.
    validate::require_valid(effective).map_err(Failure::Ocg)?;
    if coding_session {
        crate::profile::Profile::from_ocg_config(&effective.data)
            .and_then(|profile| profile.select(Some(level)).map(|_| ()))
            .map_err(Failure::Ocg)?;
    }
    if trace {
        observability::record_event(effective, "launch", level, env.trace.as_deref());
    }
    let proxy = resolve_proxy(disable_proxy);
    for warning in proxy.warnings() {
        eprintln!("ocg: warning: {warning}");
    }
    let proxy_env = proxy.child_env();
    let http =
        ReqwestHttp::with_policy(proxy.plan(), env.github_token.clone()).map_err(Failure::Ocg)?;
    let clock = SystemClock;
    let process = SystemProcessHost;
    let manager = runtime_manager(project_root, effective, env, &http, &clock, &process)
        .map_err(Failure::Ocg)?
        .with_proxy_env(proxy_env.clone());
    let selection = manager.resolve_for_launch().map_err(Failure::Ocg)?;
    for warning in &selection.warnings {
        eprintln!("ocg: warning: {warning}");
    }

    // Establish the runtime family before generating config. A parseable but
    // unsupported major fails here; an unclassifiable version keeps the
    // historical v1 launch path.
    let adapter = resolve_adapter(selection.version.as_ref()).map_err(Failure::Ocg)?;

    let mut resolved =
        build::build_opencode_config_for(effective, level, adapter).map_err(Failure::Ocg)?;
    // `ocg models` is not a coding session: it must not require the
    // orchestration plugin to exist or the project directory to be writable.
    if !coding_session {
        crate::orchestration::plugin::remove_ocg_plugin_for(&mut resolved, adapter.plugin_key());
    }
    // OpenCode 2 discovers a generated local plugin through
    // OPENCODE_CONFIG_DIR/plugins, so it intentionally has no config-array
    // entry to inspect here.
    let plugin_active = coding_session
        && crate::orchestration::OrchestrationConfig::from_config(&effective.data)
            .map_err(Failure::Ocg)?
            .enabled;
    // The catalogue probe runs before local plugin materialization. Keep every
    // user plugin/config entry, but do not ask OpenCode to load OCG's generated
    // file before that file exists.
    let preflight_content = if coding_session {
        let mut probe_config = resolved.clone();
        crate::orchestration::plugin::remove_ocg_plugin_for(
            &mut probe_config,
            adapter.plugin_key(),
        );
        Some(serde_json::to_string(&probe_config).map_err(|error| {
            Failure::Ocg(OcgError::config(format!(
                "cannot serialize the OpenCode config for runtime model checks: {error}"
            )))
        })?)
    } else {
        None
    };
    let mut gateway = None;
    let mut migrated_env = None;
    if coding_session && adapter.major() == compat::Major::V2 {
        if let Some(routes) = effective
            .data
            .get("provider_transport")
            .and_then(Value::as_object)
        {
            if routes.len() != 1 {
                return Err(Failure::Ocg(OcgError::config("provider_transport requires exactly one explicitly migrated route per invocation")));
            }
            let (provider, settings) = routes.iter().next().expect("checked nonempty");
            if settings.get("ownership").and_then(Value::as_str) != Some("ocg_native") {
                return Err(Failure::Ocg(OcgError::config(
                    "provider_transport ownership must be ocg_native",
                )));
            }
            let source = resolved
                .get_mut("provider")
                .and_then(Value::as_object_mut)
                .and_then(|all| all.get_mut(provider))
                .and_then(Value::as_object_mut)
                .ok_or_else(|| {
                    Failure::Ocg(OcgError::config(
                        "migrated provider is not configured in OpenCode",
                    ))
                })?;
            let model = settings
                .get("model")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    Failure::Ocg(OcgError::config("provider_transport.model is required"))
                })?;
            let known = source
                .get("models")
                .and_then(Value::as_object)
                .is_some_and(|models| {
                    models.iter().any(|(key, spec)| {
                        spec.get("id")
                            .or_else(|| spec.get("modelID"))
                            .and_then(Value::as_str)
                            .unwrap_or(key)
                            == model
                    })
                });
            if !known {
                return Err(Failure::Ocg(OcgError::config(
                    "migrated upstream model is not configured",
                )));
            }
            let base_env = settings
                .get("base_url_env")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    Failure::Ocg(OcgError::config(
                        "provider_transport.base_url_env is required",
                    ))
                })?;
            let key_env = settings
                .get("api_key_env")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    Failure::Ocg(OcgError::config(
                        "provider_transport.api_key_env is required",
                    ))
                })?;
            let valid_env = |name: &str| {
                !name.is_empty()
                    && name.len() <= 80
                    && name
                        .bytes()
                        .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
            };
            if !valid_env(base_env) || !valid_env(key_env) {
                return Err(Failure::Ocg(OcgError::config(
                    "invalid provider_transport environment variable name",
                )));
            }
            let base = std::env::var(base_env).map_err(|_| {
                Failure::Ocg(OcgError::config(
                    "migrated upstream base URL is unavailable",
                ))
            })?;
            let credential = std::env::var(key_env).map_err(|_| {
                Failure::Ocg(OcgError::config(
                    "migrated upstream credential is unavailable",
                ))
            })?;
            if !base.starts_with("https://") && !base.starts_with("http://127.0.0.1:") {
                return Err(Failure::Ocg(OcgError::config(
                    "migrated upstream must use HTTPS or numeric loopback",
                )));
            }
            let budget = crate::orchestration::budget::BudgetConfig::from_config(&effective.data)
                .map_err(Failure::Ocg)?;
            let owned = ProviderGateway::start(
                project_root.to_path_buf(),
                invocation_dir.to_string_lossy().to_string(),
                GatewayRoute {
                    provider: provider.clone(),
                    model: model.to_string(),
                    upstream: ProviderTransportConfig::new(base, credential),
                },
                budget,
            )
            .map_err(Failure::Ocg)?;
            // Do not let provider-specific headers, original credentials, or
            // alternate endpoints escape into OpenCode's runtime state.
            source.insert(
                "options".into(),
                json!({"baseURL":owned.url(),"apiKey":owned.token()}),
            );
            gateway = Some(owned);
            migrated_env = Some((base_env.to_string(), key_env.to_string()));
        }
    }
    let content = serde_json::to_string(&resolved).map_err(|error| {
        Failure::Ocg(OcgError::config(format!(
            "cannot serialize the OpenCode config: {error}"
        )))
    })?;

    let mut v2_runtime = None;
    let mut v2_channel = None;
    let mut runtime_target_session: Option<String> = None;
    if coding_session {
        match adapter.lead_selection() {
            // v1 enforces the Lead on the mutable request message; the runtime
            // catalogue probe proves the active Lead model exists first.
            compat::LeadSelectionMode::RequestMessage => {
                let preflight = crate::preflight::probe(
                    &effective.data,
                    Some(level),
                    &process,
                    &selection.path,
                    invocation_dir,
                    preflight_content.as_deref().unwrap_or(&content),
                    &proxy_env,
                )
                .map_err(Failure::Ocg)?;
                if let Some(error) = preflight.active_lead_failure(level) {
                    return Err(Failure::Ocg(OcgError::config(error)));
                }
                match &preflight {
                    ModelPreflight::Unavailable { reason } => {
                        eprintln!(
                            "ocg: warning: {reason}; continuing because the probe is unavailable"
                        )
                    }
                    ModelPreflight::Complete { .. } => {
                        let missing = preflight.missing_non_active_count(level);
                        if missing > 0 {
                            eprintln!(
                                "ocg: warning: {missing} configured non-active model route(s) are not currently exposed by OpenCode; run `ocg doctor` for details"
                            );
                        }
                    }
                }
            }
            // v2 selects the Lead on the session. OCG starts its own
            // invocation-scoped server, hands it the generated config, applies
            // the Rust-resolved contract to a session, then reads the effective
            // state back. The runtime identity and the verified effective Lead
            // are reported so the invocation can never be mistaken for an
            // ambient daemon using another configuration.
            compat::LeadSelectionMode::Session => {
                let contract =
                    model::lead_contract(&effective.data, level).map_err(Failure::Ocg)?;
                let lead = LeadSelection::from_contract(&contract);
                let mut plugin_env = if plugin_active {
                    let mut vars = runtime_plugin_env(effective, project_root, level, adapter)
                        .map_err(Failure::Ocg)?;
                    if let Some(gateway) = &gateway {
                        vars.push((
                            OsString::from("OCG_PROVIDER_INVOCATION"),
                            OsString::from(gateway.invocation()),
                        ));
                        vars.push((
                            OsString::from("OCG_PROVIDER_ID"),
                            OsString::from(&gateway.provider_id()),
                        ));
                    }
                    if let Some((base, key)) = &migrated_env {
                        vars.push((OsString::from(base), OsString::new()));
                        vars.push((OsString::from(key), OsString::new()));
                    }
                    vars
                } else {
                    Vec::new()
                };
                if gateway.is_some() && !plugin_active {
                    return Err(Failure::Ocg(OcgError::config(
                        "migrated provider requires the V2 correlation plugin",
                    )));
                }
                // This reference must exist in the server environment *before*
                // spawn. The registration itself arrives only after handshake.
                #[cfg(unix)]
                if plugin_active {
                    let channel = compat::v2_rendezvous::InvocationChannel::new(project_root)
                        .map_err(Failure::Ocg)?;
                    plugin_env.push((
                        OsString::from(compat::v2_rendezvous::CHANNEL_ENV),
                        channel.socket().into_os_string(),
                    ));
                    plugin_env.push((
                        OsString::from(compat::v2_rendezvous::ID_ENV),
                        OsString::from(channel.identity()),
                    ));
                    v2_channel = Some(channel);
                }
                let runtime = compat::v2_server::OwnedV2Server::start(
                    &selection.path,
                    &content,
                    &plugin_env,
                    &proxy_env,
                )
                .map_err(Failure::Ocg)?;
                #[cfg(unix)]
                if let Some(channel) = &v2_channel {
                    channel.publish(&runtime).map_err(Failure::Ocg)?;
                }
                if let Some(gateway) = &gateway {
                    gateway
                        .attach_runtime(runtime.registration().clone())
                        .map_err(Failure::Ocg)?;
                }
                let mut client = compat::v2_client::V2SessionClient::connect(
                    runtime.registration(),
                    invocation_dir.to_string_lossy(),
                )
                .map_err(Failure::Ocg)?;
                let profile = lead.runtime_profile();
                // The daemon's session database is shared with other runtime
                // processes. Reusing its newest root session can hijack an
                // ambient client's live session, even though this server is
                // invocation-owned. A launch always creates its own root.
                let session_id = RuntimeLifecycleAdapter::create_execution(&mut client)
                    .map_err(|error| Failure::Ocg(OcgError::config(error.to_string())))?;
                RuntimeLifecycleAdapter::prepare_execution(&mut client, &session_id, &profile)
                    .map_err(|error| Failure::Ocg(OcgError::config(error.to_string())))?;
                let observed = SessionClient::effective_lead(&client, session_id.as_str())
                    .map_err(Failure::Ocg)?;
                eprintln!(
                    "ocg: runtime {} | session {} | effective Lead {}",
                    runtime.identity().describe(),
                    session_id,
                    crate::runtime::effective::render_effective(&observed)
                );
                runtime_target_session = Some(session_id.to_string());
                v2_runtime = Some(runtime);
            }
        }
    }
    let runner = ProcessRunner::new(selection.path.into_os_string());
    // Materialize the adapter *before* exec: if it cannot be written, the
    // generated `file://` plugin would be broken, so fail clearly instead of
    // launching an integration that cannot work. A non-coding session (for
    // example `ocg models`) skips this entirely.
    let mut extra_env = if plugin_active {
        runtime_plugin_env(effective, project_root, level, adapter).map_err(Failure::Ocg)?
    } else {
        Vec::new()
    };
    extra_env.extend(
        crate::vault::Vault::user_global()
            .and_then(|vault| vault.child_environment())
            .map_err(Failure::Ocg)?,
    );
    if let Some(gateway) = &gateway {
        extra_env.push((
            OsString::from("OCG_PROVIDER_INVOCATION"),
            OsString::from(gateway.invocation()),
        ));
        extra_env.push((
            OsString::from("OCG_PROVIDER_ID"),
            OsString::from(gateway.provider_id()),
        ));
    }
    if let Some((base, key)) = &migrated_env {
        extra_env.push((OsString::from(base), OsString::new()));
        extra_env.push((OsString::from(key), OsString::new()));
    }
    if let Some(runtime) = v2_runtime {
        let target_session = runtime_target_session.as_deref().unwrap_or_default();
        let private_args = private_server_args(args, runtime.url(), target_session);
        let mut private_env = extra_env;
        #[cfg(unix)]
        if let Some(channel) = &v2_channel {
            private_env.push((
                OsString::from(compat::v2_rendezvous::CHANNEL_ENV),
                channel.socket().into_os_string(),
            ));
            private_env.push((
                OsString::from(compat::v2_rendezvous::ID_ENV),
                OsString::from(channel.identity()),
            ));
        }
        private_env.push((
            OsString::from("OPENCODE_SERVER_PASSWORD"),
            OsString::from(runtime.password()),
        ));
        // These values exist only in this invocation's child environment. The
        // generated plugin passes them to the bridge for context observation;
        // none is written to Mission, telemetry, rollover or continuation
        // artifacts. The target id also makes the launched OpenCode client
        // select the session whose Lead was verified above.
        private_env.push((
            OsString::from("OCG_V2_SERVER_URL"),
            OsString::from(runtime.url()),
        ));
        private_env.push((
            OsString::from("OCG_V2_SERVER_PASSWORD"),
            OsString::from(runtime.password()),
        ));
        if !target_session.is_empty() {
            private_env.push((
                OsString::from("OCG_V2_TARGET_SESSION"),
                OsString::from(target_session),
            ));
        }
        private_env.push((
            OsString::from("OCG_V2_DIRECTORY"),
            invocation_dir.as_os_str().to_os_string(),
        ));
        // Keep `runtime` alive until its client exits. Its Drop implementation
        // terminates and reaps the private server on every return path.
        return runner
            .run(
                &private_args,
                invocation_dir,
                &content,
                &private_env,
                &proxy_env,
            )
            .map_err(Failure::Ocg);
    }
    runner
        .exec(args, invocation_dir, &content, &extra_env, &proxy_env)
        .map_err(Failure::Ocg)?;
    Ok(0)
}

/// `--server` is accepted by OpenCode 2.0.11's `run` command, but not before
/// the subcommand. Interactive launch has no subcommand, where it remains a
/// root flag.
fn private_server_args(args: &[OsString], url: &str, target_session: &str) -> Vec<OsString> {
    let mut result = Vec::with_capacity(args.len() + 4);
    if let Some((first, rest)) = args.split_first() {
        result.push(first.clone());
        result.push(OsString::from("--server"));
        result.push(OsString::from(url));
        if !target_session.is_empty() {
            result.push(OsString::from("--session"));
            result.push(OsString::from(target_session));
        }
        result.extend(rest.iter().cloned());
    } else {
        result.push(OsString::from("--server"));
        result.push(OsString::from(url));
        if !target_session.is_empty() {
            result.push(OsString::from("--session"));
            result.push(OsString::from(target_session));
        }
    }
    result
}

// An invocation-scoped OpenCode 2 server is owned by
// `compat::v2_server::OwnedV2Server`; launch only needs its URL and password to
// hand the private server to the launched client.

/// Resolve the compatibility adapter for a runtime selection.
///
/// A detected version is classified explicitly and an unsupported major is a
/// hard failure: OCG never guesses how to talk to an unknown runtime. A
/// runtime whose version genuinely cannot be probed keeps the historical v1
/// contract so the supported 1.18.x path is not regressed.
fn resolve_adapter(version: Option<&Version>) -> crate::error::Result<&'static dyn RuntimeAdapter> {
    match version {
        Some(version) => {
            compat::classify(version.clone()).map(|detected| compat::adapter_for(&detected))
        }
        None => {
            eprintln!(
                "ocg: warning: could not determine the OpenCode version; assuming the v1 (1.18.x) contract"
            );
            Ok(compat::v1_adapter())
        }
    }
}

/// Resolve the effective proxy for a command. Static system discovery is only
/// attempted when the environment carries nothing, so non-network commands do
/// not spawn `scutil`.
fn resolve_proxy(disable: bool) -> ProxySelection {
    crate::proxy::resolve(disable, &crate::proxy::SystemProxyEnv, &SystemStaticProxy)
}

/// Resolve the runtime family for the config-output commands (`--dry-run` and
/// `build`).
///
/// Those commands print the very config a real launch would use, so they must
/// agree with launch/doctor/models on the runtime family. Resolution is
/// read-only: it never installs, upgrades or touches the network (unlike
/// doctor, no proxy env is applied — with `NoHttp` nothing can reach a socket,
/// and `resolve_for_report` only probes a local binary). A broken
/// explicit runtime override is authoritative and fails, exactly like a launch.
/// A genuinely absent runtime keeps the historical v1 contract so the
/// deterministic v1 output is not regressed on machines without OpenCode.
fn config_output_adapter(
    project_root: &Path,
    effective: &config::Effective,
    env: &Env,
) -> std::result::Result<&'static dyn RuntimeAdapter, Failure> {
    let clock = SystemClock;
    let process = SystemProcessHost;
    let manager = runtime_manager(project_root, effective, env, &NoHttp, &clock, &process)
        .map_err(Failure::Ocg)?;
    let report = manager.resolve_for_report();
    for warning in &report.warnings {
        eprintln!("ocg: warning: {warning}");
    }
    if report.version.is_none() {
        if let Some(error) = &report.error {
            return Err(Failure::Ocg(OcgError::config(error.clone())));
        }
    }
    resolve_adapter(report.version.as_ref()).map_err(Failure::Ocg)
}

/// Materialize the generated plugin and export the exact bridge environment.
///
/// This is the only place `launch` writes local state, and it only happens when
/// orchestration is enabled. Disabled orchestration returns an empty vector, so
/// no plugin, no file and no variable reaches OpenCode. A materialization
/// failure is returned so the launch aborts rather than injecting a `file://`
/// URL that does not resolve.
fn runtime_plugin_env(
    effective: &config::Effective,
    cwd: &Path,
    level: &str,
    adapter: &dyn RuntimeAdapter,
) -> crate::error::Result<Vec<(OsString, OsString)>> {
    let path = match adapter.major() {
        compat::Major::V1 => {
            crate::orchestration::plugin::materialize_with(cwd, adapter.plugin_source())?
        }
        compat::Major::V2 => {
            crate::orchestration::plugin::materialize_v2_with(cwd, adapter.plugin_source())?
        }
    };
    let mut env = Vec::new();
    let exe = std::env::current_exe().map_err(|error| {
        OcgError::io(
            "cannot determine the orchestration bridge executable",
            error,
        )
    })?;
    env.push((OsString::from("OCG_BRIDGE"), exe.into_os_string()));
    env.push((
        OsString::from("OCG_PROJECT"),
        cwd.as_os_str().to_os_string(),
    ));
    // The plugin starts a fresh `ocg __bridge` process. Preserve the exact
    // resolved global profile path inside that process too.
    env.push((
        OsString::from("OCG_USER_CONFIG"),
        effective.user_path.as_os_str().to_os_string(),
    ));
    env.push((
        OsString::from("OCG_ORCHESTRATION_ENABLED"),
        OsString::from("1"),
    ));
    // The raw latest-Lead-output capture rides on the generated plugin. Export
    // the resolved switch so the adapter can stay inert without a bridge spawn
    // when it is disabled; the bridge re-checks the same policy.
    let reports = crate::reports::ReportsConfig::from_config(&effective.data)?;
    env.push((
        OsString::from("OCG_REPORTS_LATEST_LEAD_OUTPUT"),
        OsString::from(if reports.latest_lead_output.enabled {
            "1"
        } else {
            "0"
        }),
    ));
    let governor =
        crate::orchestration::OrchestrationConfig::from_config(&effective.data)?.context_governor;
    env.push((
        OsString::from("OCG_CONTEXT_GOVERNOR_ENABLED"),
        OsString::from(if governor.enabled { "1" } else { "0" }),
    ));
    if adapter.major() == compat::Major::V2 {
        env.push((
            OsString::from("OPENCODE_CONFIG_DIR"),
            crate::orchestration::plugin::v2_config_dir(cwd).into_os_string(),
        ));
    }
    let contract = model::lead_contract(&effective.data, level)?;
    let contract = serde_json::to_string(&contract).map_err(|error| {
        OcgError::config(format!(
            "cannot serialize the Lead runtime contract: {error}"
        ))
    })?;
    env.push((
        OsString::from("OCG_LEAD_CONTRACT"),
        OsString::from(contract),
    ));
    // Defense in depth: the exported path is the one just materialized.
    debug_assert!(path.is_file());
    Ok(env)
}

/// Build a runtime manager from the effective config and environment.
fn runtime_manager<'a>(
    project_root: &Path,
    effective: &config::Effective,
    env: &Env,
    http: &'a dyn HttpTransport,
    clock: &'a SystemClock,
    process: &'a dyn ProcessHost,
) -> crate::error::Result<runtime::RuntimeManager<'a>> {
    let policy = RuntimePolicy::from_config(&effective.data)?;
    let platform = Platform::current()?;
    let mut manager =
        runtime::RuntimeManager::new(project_root, policy, platform, http, clock, process)
            .with_explicit(env.opencode_bin.clone());
    if let Some(cache_dir) = &env.cache_dir {
        manager = manager.with_cache_dir(Some(cache_dir.clone()));
    }
    if let Some(api_base) = &env.api_base {
        manager = manager.with_api_base(api_base.clone());
    }
    Ok(manager)
}

/// `ocg version`: report OCG, platform and the resolved runtime. Never
/// installs, upgrades or writes the update cache.
fn auth_command(args: &[OsString]) -> std::result::Result<i32, Failure> {
    let vault = crate::vault::Vault::user_global().map_err(Failure::Ocg)?;
    let words = args
        .iter()
        .map(|arg| arg.to_string_lossy())
        .collect::<Vec<_>>();
    match words.as_slice() {
        [action] if action == "list" => {
            for name in vault.list().map_err(Failure::Ocg)? {
                println!("{name}");
            }
        }
        [action, name] if action == "set" => {
            let value = rpassword::prompt_password(format!("Credential for {name}: "))
                .map_err(|_| Failure::Ocg(OcgError::config("cannot read credential input")))?;
            vault.set(name, &value).map_err(Failure::Ocg)?;
            println!("stored credential {name}");
        }
        [action, name] if action == "remove" => {
            if vault.remove(name).map_err(Failure::Ocg)? {
                println!("removed credential {name}");
            } else {
                println!("credential {name} was not stored");
            }
        }
        _ => {
            return Err(Failure::Usage(
                "usage: ocg auth list | set ENV_NAME | remove ENV_NAME".into(),
            ));
        }
    }
    Ok(0)
}

fn version_command(cli: &Cli, env: &Env) -> std::result::Result<i32, Failure> {
    let project = cli
        .project
        .clone()
        .or_else(|| std::env::current_dir().ok())
        .unwrap_or_else(|| PathBuf::from("."));
    let platform = Platform::current().map_err(Failure::Ocg)?;
    let clock = SystemClock;
    let process = SystemProcessHost;
    let policy = RuntimePolicy::default();
    let manager =
        runtime::RuntimeManager::new(project, policy, platform, &NoHttp, &clock, &process)
            .with_explicit(env.opencode_bin.clone());
    let report = manager.resolve_for_report();

    println!("OCG {VERSION}");
    println!("platform:        {}", platform.slug());
    if report.installed() {
        println!(
            "opencode:        {} ({})",
            describe_runtime_version(report.version.as_ref()),
            report
                .source
                .map(|source| source.label())
                .unwrap_or("unknown")
        );
        if let Some(path) = &report.path {
            println!("runtime path:    {}", path.display());
        }
    } else {
        println!("opencode:        not installed");
        if let Some(error) = &report.error {
            println!("runtime error:   {error}");
        } else {
            println!(
                "runtime:         none; the next launch will bootstrap a project-local runtime"
            );
        }
    }
    if let Some(version) = report.version.as_ref() {
        match compat::classify(version.clone()) {
            Ok(detected) => println!(
                "opencode family: {} ({})",
                detected.major().as_str(),
                detected.version()
            ),
            Err(error) => println!("opencode family: unsupported ({error})"),
        }
    }
    for warning in &report.warnings {
        println!("warning:         {warning}");
    }
    Ok(0)
}

fn describe_runtime_version(version: Option<&Version>) -> String {
    version
        .map(ToString::to_string)
        .unwrap_or_else(|| "unknown version".to_string())
}

/// Treat "2.0.11", "v2.0.11" and "opencode v2.0.11" as equivalent for doctor
/// warnings. Genuine mismatches still produce a warning.
fn versions_equivalent(a: &str, b: &str) -> bool {
    match (
        compat::parse_version_token(a),
        compat::parse_version_token(b),
    ) {
        (Some(va), Some(vb)) => va == vb,
        _ => a.trim() == b.trim(),
    }
}

/// One logical doctor section. Rendering follows this fixed order, so the layout
/// is stable as checks are added and a check is grouped by its owner regardless
/// of the order in which diagnostics happen to run.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Section {
    Environment,
    Configuration,
    Lead,
    Workers,
    Runtime,
    Effective,
    Infrastructure,
}

impl Section {
    const ALL: [Section; 7] = [
        Section::Environment,
        Section::Configuration,
        Section::Lead,
        Section::Workers,
        Section::Runtime,
        Section::Effective,
        Section::Infrastructure,
    ];

    fn index(self) -> usize {
        match self {
            Section::Environment => 0,
            Section::Configuration => 1,
            Section::Lead => 2,
            Section::Workers => 3,
            Section::Runtime => 4,
            Section::Effective => 5,
            Section::Infrastructure => 6,
        }
    }

    /// Concise human-facing name; the section header is the only place these
    /// titles appear, so they never become a check label.
    fn title(self) -> &'static str {
        match self {
            Section::Environment => "Environment & Proxy",
            Section::Configuration => "Configuration",
            Section::Lead => "Lead",
            Section::Workers => "Workers",
            Section::Runtime => "Runtime",
            Section::Effective => "Effective State",
            Section::Infrastructure => "Project Infrastructure",
        }
    }
}

/// Severity of one check. The token is the stable, color-free status carrier.
#[derive(Clone, Copy, PartialEq, Eq)]
enum DoctorStatus {
    Pass,
    Info,
    Warn,
    Fail,
}

impl DoctorStatus {
    /// The historical string keys are the contract; unknown keys are INFO, as
    /// before.
    fn from_key(key: &str) -> Self {
        match key {
            "ok" => DoctorStatus::Pass,
            "warn" => DoctorStatus::Warn,
            "error" | "fail" => DoctorStatus::Fail,
            _ => DoctorStatus::Info,
        }
    }

    fn token(self) -> &'static str {
        match self {
            DoctorStatus::Pass => "PASS",
            DoctorStatus::Info => "INFO",
            DoctorStatus::Warn => "WARN",
            DoctorStatus::Fail => "FAIL",
        }
    }
}

/// One recorded check. `continuation` carries secondary metadata (paths,
/// provenance) that should sit on indented lines rather than stretch the
/// primary detail.
struct DoctorCheck {
    status: DoctorStatus,
    label: String,
    detail: String,
    continuation: Vec<String>,
}

#[derive(Default)]
struct DoctorCounts {
    passed: usize,
    warnings: usize,
    failures: usize,
    infos: usize,
}

/// Doctor output is collected as structured checks and rendered once, so the
/// status column never depends on label length and the summary counters derive
/// from the very checks that were rendered. Diagnostic collection and terminal
/// rendering stay separate; only FAIL affects the exit status.
struct Doctor {
    current: Section,
    sections: [Vec<DoctorCheck>; 7],
}

impl Default for Doctor {
    fn default() -> Self {
        Doctor {
            current: Section::Environment,
            sections: std::array::from_fn(|_| Vec::new()),
        }
    }
}

/// Rendered width before a detail wraps. Chosen for ordinary terminals; a single
/// unbreakable token (a path or URL) may still exceed it.
const DOCTOR_WIDTH: usize = 100;
/// Bounded label column so a long label cannot push the detail arbitrarily far.
const DOCTOR_LABEL_MAX: usize = 28;
/// Floor for the label column when few or short labels are present.
const DOCTOR_LABEL_MIN: usize = 18;

impl Doctor {
    fn section(&mut self, section: Section) {
        self.current = section;
    }

    fn line(&mut self, status: &str, label: &str, detail: &str) {
        self.push(status, label, detail, Vec::new());
    }

    /// A check whose secondary metadata belongs on indented continuation lines.
    fn line_with(&mut self, status: &str, label: &str, detail: &str, continuation: Vec<String>) {
        self.push(status, label, detail, continuation);
    }

    fn push(&mut self, status: &str, label: &str, detail: &str, continuation: Vec<String>) {
        self.sections[self.current.index()].push(DoctorCheck {
            status: DoctorStatus::from_key(status),
            label: label.to_string(),
            detail: detail.to_string(),
            continuation,
        });
    }

    fn counts(&self) -> DoctorCounts {
        let mut counts = DoctorCounts::default();
        for check in self.sections.iter().flatten() {
            match check.status {
                DoctorStatus::Pass => counts.passed += 1,
                DoctorStatus::Info => counts.infos += 1,
                DoctorStatus::Warn => counts.warnings += 1,
                DoctorStatus::Fail => counts.failures += 1,
            }
        }
        counts
    }

    fn failures(&self) -> usize {
        self.counts().failures
    }

    /// The label column is derived from the checks actually present (bounded),
    /// not from another arbitrary fixed constant.
    fn label_width(&self) -> usize {
        self.sections
            .iter()
            .flatten()
            .map(|check| check.label.chars().count())
            .max()
            .unwrap_or(DOCTOR_LABEL_MIN)
            .clamp(DOCTOR_LABEL_MIN, DOCTOR_LABEL_MAX)
    }

    fn render(&self) {
        let width = self.label_width();
        println!("OCG doctor");
        for section in Section::ALL {
            let checks = &self.sections[section.index()];
            if checks.is_empty() {
                continue;
            }
            // One blank line between logical sections.
            println!();
            println!("{}", section.title());
            for check in checks {
                render_check(check, width);
            }
        }
        let counts = self.counts();
        println!();
        println!("Summary");
        println!(
            "  {} passed · {} warnings · {} failures · {} informational",
            counts.passed, counts.warnings, counts.failures, counts.infos
        );
    }
}

/// Render one check status-first: `  [PASS] label  detail`, wrapping the detail
/// and any continuation metadata onto aligned indented lines. Status placement
/// never depends on label length.
fn render_check(check: &DoctorCheck, width: usize) {
    let prefix = format!("  [{}] ", check.status.token());
    let label = format!("{:<width$}", check.label);
    let detail_column = prefix.chars().count() + width + 2;
    let indent = " ".repeat(detail_column);
    let available = DOCTOR_WIDTH.saturating_sub(detail_column).max(24);
    let mut bodies: Vec<String> = Vec::new();
    if !check.detail.is_empty() {
        bodies.extend(wrap_lines(&check.detail, available));
    }
    for continuation in &check.continuation {
        bodies.extend(wrap_lines(continuation, available));
    }
    match bodies.split_first() {
        None => println!("{prefix}{label}"),
        Some((first, rest)) => {
            println!("{prefix}{label}  {first}");
            for line in rest {
                println!("{indent}{line}");
            }
        }
    }
}

/// Greedy whitespace wrap. An unbreakable token longer than the available width
/// (a path, URL or identifier) is kept intact rather than split, so provenance
/// is never corrupted.
fn wrap_lines(text: &str, width: usize) -> Vec<String> {
    let width = width.max(1);
    let mut lines = Vec::new();
    let mut current = String::new();
    for word in text.split_whitespace() {
        if current.is_empty() {
            current.push_str(word);
        } else if current.chars().count() + 1 + word.chars().count() <= width {
            current.push(' ');
            current.push_str(word);
        } else {
            lines.push(std::mem::take(&mut current));
            current.push_str(word);
        }
    }
    if !current.is_empty() {
        lines.push(current);
    }
    lines
}

/// Whether a standard proxy variable (either spelling) is set and non-empty.
/// Only presence is ever observed; the value is never read or rendered.
fn proxy_env_present(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    std::env::var_os(name)
        .filter(|value| !value.is_empty())
        .or_else(|| std::env::var_os(&lower).filter(|value| !value.is_empty()))
        .is_some()
}

/// `ocg doctor`: read-only environment and runtime checks. Never installs,
/// updates or writes the cache, and never prints secrets. With `effective_state`
/// it additionally starts a bounded, OCG-owned private runtime to verify the
/// live effective Lead; that server is terminated before returning and no local
/// state is written.
fn doctor_command(
    effective: &config::Effective,
    project_root: &Path,
    invocation_dir: &Path,
    level: &str,
    env: &Env,
    disable_proxy: bool,
    effective_state: bool,
) -> std::result::Result<i32, Failure> {
    let mut doctor = Doctor::default();
    let proxy = resolve_proxy(disable_proxy);
    print_proxy_diagnostics(&mut doctor, &proxy);
    print_proxy_env_diagnostics(&mut doctor, &proxy, disable_proxy);
    let proxy_env = proxy.child_env();

    let platform = match Platform::current() {
        Ok(platform) => {
            doctor.line("ok", "platform", &platform.slug());
            Some(platform)
        }
        Err(error) => {
            doctor.line("fail", "platform", &error.to_string());
            None
        }
    };

    match std::env::current_exe() {
        Ok(exe) => doctor.line("ok", "ocg", &format!("{} (OCG {VERSION})", exe.display())),
        Err(error) => {
            doctor.line(
                "fail",
                "ocg",
                &format!("cannot determine the running executable: {error}"),
            );
        }
    }

    let process = SystemProcessHost;
    match process.find_in_path("ocg") {
        Some(path) => doctor.line("ok", "ocg on PATH", &path.display().to_string()),
        None => doctor.line(
            "warn",
            "ocg on PATH",
            "not found; install with install.sh or add the install directory to PATH",
        ),
    }

    // Config layering: which layer each override came from, and whether it was
    // found. This is the section a user reads when "my override does nothing".
    doctor.section(Section::Configuration);
    match effective.ocg_home.as_ref() {
        Some(home) => doctor.line(
            "ok",
            "defaults",
            &format!("disk OCG home {}", home.display()),
        ),
        None => doctor.line("ok", "defaults", "embedded in the binary (no OCG_HOME)"),
    }
    for (name, path) in [("global config", &effective.user_path)] {
        if path.is_file() {
            doctor.line("ok", name, &path.display().to_string());
        } else {
            doctor.line("info", name, &format!("{} (not present)", path.display()));
        }
    }
    if project_root.is_dir() {
        doctor.line("ok", "project root", &project_root.display().to_string());
    } else {
        doctor.line(
            "fail",
            "project root",
            &format!("{} is not a directory", project_root.display()),
        );
    }

    // OCG-owned Profile facts; availability is not inferred from OpenCode defaults.
    doctor.section(Section::Lead);
    match crate::profile::Profile::from_ocg_config(&effective.data) {
        Ok(profile) => {
            let origin = match &profile.origin {
                crate::profile::Origin::New => "New".to_string(),
                crate::profile::Origin::Imported { source, scope, .. } => {
                    format!("Imported {scope} from {source}")
                }
            };
            doctor.line("ok", "Profile", &origin);
            doctor.line("info", "providers", &profile.providers.len().to_string());
            doctor.line("info", "models", &profile.models.len().to_string());
            doctor.line(
                "info",
                "runnable models",
                &profile.runnable_models().count().to_string(),
            );
            if profile.runnable_models().next().is_none() {
                doctor.line(
                    "warn",
                    "execution",
                    "No runnable provider/model configured; placeholders are configuration-only",
                );
            }
        }
        Err(error) => doctor.line("fail", "Profile", &error.to_string()),
    }
    // An absent variant is reported as `provider-default`, never invented.
    match model::lead_contract(&effective.data, level) {
        Ok(contract) => doctor.line(
            "ok",
            "lead model",
            &format!(
                "{} (variant {})",
                contract.full_model_id(),
                contract.variant.as_deref().unwrap_or("provider-default")
            ),
        ),
        Err(error) => doctor.line("info", "lead model", &error.to_string()),
    }
    for diagnostic in &effective.diagnostics {
        doctor.line("warn", "migration", diagnostic);
    }
    doctor.line("info", "default agent", &model::lead_agent_id(level));

    // The Worker Router, independent of the Lead selection. Only role names and
    // provider/model ids are printed; no credential ever reaches this section.
    doctor.section(Section::Workers);
    match model::routing_rows(&effective.data) {
        Ok(rows) => {
            for (role, agent, _provider, full) in rows {
                doctor.line("ok", &agent, &format!("{full} ({role})"));
            }
        }
        Err(error) => doctor.line("warn", "worker router", &error.to_string()),
    }
    if let Some(small) = effective
        .data
        .get("routing")
        .and_then(|routing| routing.get("small_model"))
        .and_then(Value::as_str)
    {
        if let Ok((_, full)) = model::model_full_id(&effective.data, small) {
            doctor.line("info", "small_model", &full);
        }
    }

    // Static validity belongs with the configuration it validates, even though
    // the runtime report below needs it; the renderer groups by section.
    doctor.section(Section::Configuration);
    let errors = validate::validate(effective);
    let static_config_valid = errors.is_empty();
    if static_config_valid {
        let roles = model::role_specs(&effective.data)
            .map(|roles| roles.len())
            .unwrap_or(0);
        doctor.line(
            "ok",
            "static config/routing",
            &format!("valid ({roles} roles)"),
        );
    } else {
        doctor.line("fail", "static config/routing", &errors.join("; "));
    }

    doctor.section(Section::Runtime);
    let clock = SystemClock;
    let policy = RuntimePolicy::from_config(&effective.data).unwrap_or_default();
    let report = match platform {
        Some(platform) => {
            let manager = runtime::RuntimeManager::new(
                project_root,
                policy.clone(),
                platform,
                &NoHttp,
                &clock,
                &process,
            )
            .with_explicit(env.opencode_bin.clone())
            .with_proxy_env(proxy_env.clone());
            manager.resolve_for_report()
        }
        None => runtime::RuntimeReport::default(),
    };

    if report.installed() {
        let source = report
            .source
            .map(|source| source.label())
            .unwrap_or("unknown");
        let detail = format!(
            "{} ({source})",
            describe_runtime_version(report.version.as_ref())
        );
        // The path is secondary provenance: keep it on an indented line rather
        // than stretching the primary runtime scan line.
        match &report.path {
            Some(path) => {
                doctor.line_with("ok", "runtime", &detail, vec![path.display().to_string()])
            }
            None => doctor.line("ok", "runtime", &detail),
        }
    } else if let Some(error) = &report.error {
        doctor.line("fail", "runtime", error);
    } else {
        doctor.line(
            "info",
            "runtime",
            &format!(
                "not installed; `ocg` or `ocg upgrade` will bootstrap {}",
                Layout::new(project_root).runtime_root().display()
            ),
        );
    }
    for warning in &report.warnings {
        doctor.line("warn", "runtime", warning);
    }

    // OpenCode installation. `report` is the runtime OCG would launch; the
    // system PATH binary is reported separately so an obvious version split
    // between a managed runtime and an ambient install is visible.
    doctor.section(Section::Runtime);
    let system_binary = process.find_in_path("opencode");
    let system_version = system_binary
        .as_ref()
        .and_then(|path| process.version(path).ok());
    match (&system_binary, &system_version) {
        (Some(path), Some(version)) => doctor.line(
            "ok",
            "opencode on PATH",
            &format!("{} ({version})", path.display()),
        ),
        (Some(path), None) => doctor.line(
            "warn",
            "opencode on PATH",
            &format!("{} exists but `--version` failed", path.display()),
        ),
        (None, _) => doctor.line(
            "info",
            "opencode on PATH",
            "not found; OCG uses the runtime above, or bootstraps one on launch",
        ),
    }
    let runtime_version = report.version.as_ref().map(ToString::to_string);
    match (report.source, &runtime_version, &system_version) {
        (Some(source), Some(runtime), Some(path)) if !versions_equivalent(runtime, path) => doctor
            .line(
                "warn",
                "opencode version",
                &format!(
                    "runtime ({}) is {runtime} but system PATH is {path}; launches use the runtime",
                    source.label()
                ),
            ),
        (Some(source), Some(runtime), _) => doctor.line(
            "ok",
            "opencode version",
            &format!("runtime ({}) {runtime}", source.label()),
        ),
        (Some(source), None, _) => doctor.line(
            "info",
            "opencode version",
            &format!("runtime ({}) version unknown", source.label()),
        ),
        _ => {}
    }

    // The runtime family selects the compatibility adapter for the optional
    // catalogue probe. An unsupported major is a doctor FAIL, never a panic.
    let adapter = match report.version.as_ref() {
        Some(version) => match compat::classify(version.clone()) {
            Ok(detected) => {
                doctor.line(
                    "ok",
                    "runtime family",
                    &format!("{} ({})", detected.major().as_str(), detected.version()),
                );
                Some(compat::adapter_for(&detected))
            }
            Err(error) => {
                doctor.line("fail", "runtime family", &error.to_string());
                None
            }
        },
        None => {
            doctor.line("info", "runtime family", "unknown; assuming v1 (1.18.x)");
            Some(compat::v1_adapter())
        }
    };

    doctor.section(Section::Runtime);
    // Captured for the optional effective-state check below so the catalogue is
    // probed at most once per doctor run.
    let mut runtime_content: Option<String> = None;
    let mut runtime_preflight: Option<ModelPreflight> = None;
    if !static_config_valid {
        doctor.line(
            "info",
            "runtime models",
            "skipped because static config/routing validation failed",
        );
    } else if let (Some(program), Some(adapter)) = (report.path.as_deref(), adapter) {
        let mut resolved =
            build::build_opencode_config_for(effective, level, adapter).map_err(Failure::Ocg)?;
        crate::orchestration::plugin::remove_ocg_plugin_for(&mut resolved, adapter.plugin_key());
        let content = serde_json::to_string(&resolved).map_err(|error| {
            Failure::Ocg(OcgError::config(format!(
                "cannot serialize the OpenCode config for runtime model checks: {error}"
            )))
        })?;
        // OpenCode 2 is a background daemon: `opencode models` would silently
        // query whatever ambient service happens to be up, ignoring the
        // generated `OPENCODE_CONFIG_CONTENT` and provider credential env. And
        // even an OCG-owned V2 runtime exposes no catalogue of config-declared
        // providers, so OCG never infers V2 availability from any endpoint:
        // it reports the catalogue evidence as unavailable (only when the
        // caller opted into runtime contact with `--effective`, because the
        // default doctor must never start a runtime) and relies on the
        // effective-state observation instead. The v1 family keeps using
        // OpenCode's supported `models` CLI surface.
        if adapter.major() == compat::Major::V2 && !effective_state {
            doctor.line(
                "info",
                "runtime models",
                "not checked (pass --effective to probe the catalogue of an OCG-owned OpenCode V2 runtime)",
            );
            runtime_content = Some(content);
        } else {
            let preflight = if adapter.major() == compat::Major::V2 {
                match probe_v2_owned_catalogue(
                    &effective.data,
                    program,
                    &content,
                    project_root,
                    &proxy_env,
                ) {
                    Some(report) => report,
                    None => ModelPreflight::Unavailable {
                        reason: "runtime model check could not be completed (private OpenCode V2 server for the catalogue probe failed to start, connect, or report a catalogue)"
                            .to_string(),
                    },
                }
            } else {
                crate::preflight::probe(
                    &effective.data,
                    Some(level),
                    &process,
                    program,
                    project_root,
                    &content,
                    &proxy_env,
                )
                .map_err(Failure::Ocg)?
            };
            match &preflight {
                ModelPreflight::Unavailable { reason } => {
                    doctor.line("warn", "runtime models", reason)
                }
                ModelPreflight::Complete { checks } => {
                    for check in checks {
                        let variant = check
                            .requirement
                            .variant
                            .as_deref()
                            .map(|variant| format!(" (variant {variant})"))
                            .unwrap_or_default();
                        match check.availability {
                            Availability::Available => doctor.line(
                                "ok",
                                &check.requirement.label,
                                &format!("{}{}", check.requirement.full_model_id, variant),
                            ),
                            Availability::MissingProvider => {
                                doctor.line(
                                    "error",
                                    &check.requirement.label,
                                    &format!(
                                        "{}{} — provider is not currently exposed by OpenCode; authenticate/configure the provider or adjust OCG configuration",
                                        check.requirement.full_model_id, variant
                                    ),
                                );
                            }
                            Availability::MissingModel => {
                                doctor.line(
                                    "error",
                                    &check.requirement.label,
                                    &format!(
                                        "{}{} — model is not currently exposed by OpenCode; authenticate/configure the provider or adjust OCG configuration",
                                        check.requirement.full_model_id, variant
                                    ),
                                );
                            }
                        }
                    }
                }
            }
            runtime_content = Some(content);
            runtime_preflight = Some(preflight);
        }
    } else {
        doctor.line(
            "info",
            "runtime models",
            "not checked because no usable, supported OpenCode runtime is installed",
        );
    }

    // Configured / Resolved / Effective. The effective state is only proven by
    // talking to a real runtime, so it is opt-in: `ocg doctor --effective`
    // starts a bounded, OCG-owned private server and terminates it again.
    doctor.section(Section::Effective);
    if effective_state {
        match build_runtime_state(
            effective,
            level,
            report.path.as_deref(),
            adapter,
            runtime_content.as_deref(),
            &proxy_env,
            invocation_dir,
            runtime_preflight.as_ref(),
            true,
        ) {
            Ok(state) => {
                let (status, detail) = configured_line(&state);
                doctor.line(status, "configured", &detail);
                if state.static_errors.is_empty() {
                    doctor.line("ok", "resolved", "accepted by OCG validation");
                } else {
                    doctor.line("fail", "resolved", &state.static_errors.join("; "));
                }
                let (status, detail) = state.model.describe(
                    &state.configured.full_model_id(),
                    state.configured.variant.as_deref(),
                );
                doctor.line(status, "resolved model", &detail);
                let (status, detail) = state.effective_line();
                doctor.line(status, "effective", &detail);
                if let Some(identity) = &state.identity {
                    doctor.line("ok", "runtime endpoint", &identity.describe());
                }
            }
            Err(Failure::Usage(message)) => doctor.line("fail", "runtime state", &message),
            Err(Failure::Ocg(error)) => doctor.line("fail", "runtime state", &error.to_string()),
        }
        // Registry summary: counts and health only, never a record dump.
        let loaded = crate::resources::load(project_root);
        if loaded.exists {
            let records = loaded.registry.list();
            let mut available = 0usize;
            let mut degraded = 0usize;
            let mut unavailable = 0usize;
            for record in &records {
                match record.health.state {
                    crate::resources::ResourceHealth::Available => available += 1,
                    crate::resources::ResourceHealth::Degraded => degraded += 1,
                    crate::resources::ResourceHealth::Unavailable => unavailable += 1,
                    crate::resources::ResourceHealth::Unknown => {}
                }
            }
            let status = if loaded.corrupt || unavailable > 0 || degraded > 0 {
                "warn"
            } else if records.is_empty() {
                "info"
            } else {
                "ok"
            };
            doctor.line(
                status,
                "resources",
                &format!(
                    "{} known ({} available, {} degraded, {} unavailable){}",
                    records.len(),
                    available,
                    degraded,
                    unavailable,
                    if loaded.corrupt { "; corrupt" } else { "" }
                ),
            );
        }
    } else {
        doctor.line(
            "info",
            "runtime state",
            "not checked (pass --effective to resolve the runtime and verify the live effective Lead)",
        );
    }

    // Local artifacts and optional subsystems: nothing above touches them, so
    // they share one scannable infrastructure block.
    doctor.section(Section::Infrastructure);
    let runtime_root = Layout::new(project_root).runtime_root();
    if is_writable_dir(&runtime_root) {
        doctor.line(
            "ok",
            "runtime dir",
            &format!("{} is writable", runtime_root.display()),
        );
    } else {
        doctor.line(
            "warn",
            "runtime dir",
            &format!("{} is not writable", runtime_root.display()),
        );
    }

    let cache_dir = env
        .cache_dir
        .clone()
        .or_else(runtime::cache::platform_cache_dir);
    match cache_dir {
        Some(dir) => match runtime::cache::CacheRecord::read(&dir) {
            Some(record) => {
                let age = (clock.now_unix() - record.checked_at).max(0);
                let fresh = !runtime::cache::due(
                    Some(&record),
                    clock.now_unix(),
                    policy.check_interval_hours,
                    false,
                );
                doctor.line(
                    "ok",
                    "update cache",
                    &format!(
                        "{} ({}, checked {age}s ago)",
                        dir.display(),
                        if fresh { "fresh" } else { "expired" }
                    ),
                );
            }
            None => doctor.line(
                "info",
                "update cache",
                &format!("{} (never checked)", dir.display()),
            ),
        },
        None => doctor.line(
            "warn",
            "update cache",
            "platform cache directory unavailable",
        ),
    }

    // Repository map / symbol index: inspect the existing artifact only. Doctor
    // never builds, updates or trims an index.
    let index_path = context::index::index_path(project_root);
    let index = if index_path.is_file() {
        match std::fs::read_to_string(&index_path)
            .ok()
            .and_then(|text| serde_json::from_str::<context::index::ContextIndex>(&text).ok())
        {
            Some(index) => {
                doctor.line(
                    "ok",
                    "repository map/index",
                    &format!(
                        "{} file(s), {} symbol(s) at {}",
                        index.metrics.files,
                        index.metrics.symbols,
                        index_path.display()
                    ),
                );
                Some(index)
            }
            None => {
                doctor.line(
                    "warn",
                    "repository map/index",
                    &format!(
                        "{} is unreadable or corrupt; the next `ocg context` rebuilds it",
                        index_path.display()
                    ),
                );
                None
            }
        }
    } else {
        doctor.line(
            "info",
            "repository map/index",
            "not built (optional; `ocg context` builds it)",
        );
        None
    };

    match &index {
        Some(index) => doctor.line(
            "ok",
            "symbol index",
            &format!(
                "{} symbol(s) across {} indexed file(s)",
                index.metrics.symbols, index.metrics.files
            ),
        ),
        None => doctor.line(
            "info",
            "symbol index",
            "not built (optional; depends on the repository index)",
        ),
    }

    // Context cache: read-only statistics.
    let cache = context::cache::ContextCache::new(project_root);
    let cache_stats = cache.stats();
    let cache_exists = Path::new(&cache_stats.dir).exists();
    if !cache_exists {
        doctor.line("info", "context cache", "not present (optional)");
    } else if cache_stats.corrupt > 0 {
        doctor.line(
            "warn",
            "context cache",
            &format!(
                "{} entr(y/ies), {} corrupt (ignored and removed on reuse)",
                cache_stats.entries, cache_stats.corrupt
            ),
        );
    } else {
        doctor.line(
            "ok",
            "context cache",
            &format!(
                "{} entr(y/ies), {} bytes",
                cache_stats.entries, cache_stats.bytes
            ),
        );
    }

    // Task checkpoints: corrupt files are counted, never printed.
    let (checkpoints, corrupt_checkpoints) = checkpoint::list(project_root);
    if corrupt_checkpoints > 0 {
        doctor.line(
            "warn",
            "task checkpoints",
            &format!(
                "{corrupt_checkpoints} corrupt checkpoint(s) ignored ({} readable)",
                checkpoints.len()
            ),
        );
    } else if checkpoints.is_empty() {
        doctor.line("info", "task checkpoints", "none saved (optional)");
    } else {
        doctor.line(
            "ok",
            "task checkpoints",
            &format!("{} checkpoint(s)", checkpoints.len()),
        );
    }

    // Verification config: parse only, never run a command.
    match VerificationConfig::from_config(&effective.data) {
        Ok(config) => doctor.line(
            if config.enabled { "ok" } else { "info" },
            "verification config",
            &format!(
                "{}; default stage '{}'; {} configured command(s)",
                if config.enabled {
                    "enabled"
                } else {
                    "disabled"
                },
                config.default_stage,
                config
                    .stages
                    .values()
                    .map(|stage| stage.commands.len())
                    .sum::<usize>()
            ),
        ),
        Err(error) => doctor.line("warn", "verification config", &error.to_string()),
    }

    // Telemetry storage: read-only, local-only, no state creation.
    let (telemetry_config, _telemetry_warnings) = telemetry_for(effective, env);
    let store = telemetry::TelemetryStore::new(project_root, telemetry_config);
    let telemetry_log = store.read();
    let telemetry_state = if !telemetry_config.enabled {
        "disabled".to_string()
    } else {
        format!("enabled, {} event(s)", telemetry_log.events.len())
    };
    let telemetry_line = format!(
        "{}; local-only; {}{}",
        telemetry_state,
        store.path().display(),
        match (telemetry_log.corrupt_lines, telemetry_log.unsupported_lines,) {
            (0, 0) => String::new(),
            (corrupt, 0) => format!("; {corrupt} corrupt line(s) skipped"),
            (0, unsupported) => {
                format!("; {unsupported} unsupported schema line(s) skipped")
            }
            (corrupt, unsupported) => format!(
                "; {corrupt} corrupt line(s) and {unsupported} unsupported schema line(s) skipped"
            ),
        }
    );
    if telemetry_log.corrupt_lines > 0 || telemetry_log.unsupported_lines > 0 {
        doctor.line("warn", "telemetry", &telemetry_line);
    } else if !telemetry_config.enabled || !store.exists() {
        doctor.line("info", "telemetry", &telemetry_line);
    } else {
        doctor.line("ok", "telemetry", &telemetry_line);
    }

    // Capability planner: advisory only.
    match CapabilityConfig::from_config(&effective.data) {
        Ok(config) => doctor.line(
            if config.enabled { "ok" } else { "info" },
            "tool capability planner",
            &format!(
                "{}; {} custom capability name(s)",
                if config.enabled {
                    "enabled"
                } else {
                    "disabled"
                },
                config.custom.len()
            ),
        ),
        Err(error) => doctor.line("warn", "tool capability planner", &error.to_string()),
    }

    // Sensitive-file exclusions: summarise the current index only.
    match &index {
        Some(index) => {
            let excluded = index.files.iter().filter(|file| file.excluded).count();
            doctor.line(
                "ok",
                "sensitive-file exclusions",
                &format!("{excluded} path(s) excluded from content in the current index"),
            );
        }
        None => doctor.line(
            "info",
            "sensitive-file exclusions",
            "applied by path during indexing; no index to summarise",
        ),
    }

    // Orchestration: read-only health of the generated integration. Doctor
    // never launches OpenCode, never runs a bridge and never mutates state.
    match crate::orchestration::OrchestrationConfig::from_config(&effective.data) {
        Ok(config) => {
            doctor.line(
                if config.enabled { "ok" } else { "info" },
                "orchestration",
                &format!(
                    "{}; build retries {}; debug retries {}; handoff <= {} bytes / {}%",
                    if config.enabled {
                        "enabled"
                    } else {
                        "disabled"
                    },
                    config.max_build_retries,
                    config.max_debug_retries,
                    config.max_handoff_bytes,
                    config.max_handoff_ratio_percent
                ),
            );
            if config.enabled {
                let (plugin, mechanism) = match adapter {
                    Some(a) if a.major() == compat::Major::V2 => {
                        let path = crate::orchestration::plugin::v2_plugin_path(project_root);
                        let mech = "local-discovery JS adapter at OPENCODE_CONFIG_DIR/plugins; hooks: prompt, context, execute.before/after";
                        (path, mech)
                    }
                    _ => {
                        let path = crate::orchestration::plugin::plugin_path(project_root);
                        let mech = "file:// JS adapter injected via config.plugin; hooks: chat.message, tool.execute.before/after";
                        (path, mech)
                    }
                };
                if plugin.is_file() {
                    doctor.line(
                        "ok",
                        "orchestration plugin",
                        &format!("{} (mechanism: {mechanism})", plugin.display()),
                    );
                } else {
                    doctor.line(
                        "info",
                        "orchestration plugin",
                        "not materialized yet (written at the next `ocg` launch)",
                    );
                }
                let loaded = crate::orchestration::state::load(project_root);
                if !loaded.exists {
                    doctor.line(
                        "info",
                        "orchestration state",
                        "not present (created by the bridge on first use)",
                    );
                } else if loaded.corrupt {
                    doctor.line(
                        "warn",
                        "orchestration state",
                        "corrupt or unsupported; the next bridge call starts from empty state",
                    );
                } else {
                    let checkpoints = loaded
                        .state
                        .sessions
                        .values()
                        .map(|session| session.checkpoints.len())
                        .sum::<usize>();
                    doctor.line(
                        "ok",
                        "orchestration state",
                        &format!(
                            "{} session(s); {} checkpoint reference(s)",
                            loaded.state.sessions.len(),
                            checkpoints
                        ),
                    );
                }
                match crate::orchestration::domain::DomainRepository::open(project_root).and_then(
                    |repository| {
                        let project = repository.ensure_project(project_root)?;
                        repository.jobs(&project.id)
                    },
                ) {
                    Ok(jobs) => doctor.line(
                        "ok",
                        "canonical execution",
                        &format!("{} Job(s)", jobs.len()),
                    ),
                    Err(error) => doctor.line("error", "canonical execution", &error.to_string()),
                }
                let limits = crate::orchestration::projection::ProjectionLimits {
                    max_bytes: config.max_handoff_bytes,
                    ratio_percent: config.max_handoff_ratio_percent,
                };
                let capsule = crate::orchestration::projection::project(
                    &crate::orchestration::handoff::ProjectionInput::default(),
                    crate::orchestration::handoff::Role::Lead,
                    crate::orchestration::handoff::Role::Build,
                    "health",
                    "health",
                    limits,
                );
                doctor.line(
                    if capsule.measured_bytes() <= limits.max_bytes {
                        "ok"
                    } else {
                        "warn"
                    },
                    "projection",
                    &format!(
                        "handoff schema reachable ({} bytes, cap {})",
                        capsule.measured_bytes(),
                        limits.max_bytes
                    ),
                );
                let context_enabled = ContextConfig::from_config(&effective.data)
                    .map(|context| context.enabled)
                    .unwrap_or(false);
                doctor.line(
                    if context_enabled { "ok" } else { "info" },
                    "context activation",
                    if context_enabled {
                        "context preparation is active for orchestration"
                    } else {
                        "context.enabled=false; orchestration runs with an empty dynamic context"
                    },
                );
                let governor = &config.context_governor;
                let artifact_count = |directory: PathBuf| {
                    std::fs::read_dir(directory)
                        .map(|entries| {
                            entries
                                .flatten()
                                .filter(|entry| {
                                    entry.path().extension().and_then(|ext| ext.to_str())
                                        == Some("json")
                                })
                                .count()
                        })
                        .unwrap_or(0)
                };
                doctor.line(
                    if governor.enabled { "ok" } else { "info" },
                    "context governor",
                    &format!(
                        "{}; warning {}%; rollover {}%; telemetry artifact(s) {}; rollover artifact(s) {}; continuation packet(s) {}",
                        if governor.enabled { "enabled" } else { "disabled" },
                        governor.approaching_percent,
                        governor.rollover_percent,
                        artifact_count(crate::orchestration::context_governor::telemetry_dir(project_root)),
                        artifact_count(crate::orchestration::context_governor::rollover_dir(project_root)),
                        artifact_count(crate::orchestration::context_governor::continuation_dir(project_root)),
                    ),
                );
                let verification_ready = VerificationConfig::from_config(&effective.data)
                    .map(|verification| {
                        verification.enabled
                            && verification.command_count(&verification.default_stage) > 0
                    })
                    .unwrap_or(false);
                doctor.line(
                    if verification_ready { "ok" } else { "info" },
                    "verification integration",
                    if verification_ready {
                        "after Build, the configured verification stage runs and feeds the retry policy"
                    } else {
                        "no trusted command in the default stage; after Build reports not-configured"
                    },
                );
            }
        }
        Err(error) => doctor.line("warn", "orchestration", &error.to_string()),
    }

    doctor.render();
    Ok(if doctor.failures() == 0 { 0 } else { 1 })
}

/// Proxy mode, endpoints and non-SOCKS notes. Values are always rendered
/// through [`crate::proxy::SecretUrl`]; only names and schemes are ever shown.
fn print_proxy_diagnostics(doctor: &mut Doctor, proxy: &ProxySelection) {
    if proxy.is_disabled() {
        doctor.line(
            "ok",
            "proxy",
            match proxy.source() {
                ProxySource::CliDisabled => "disabled by CLI",
                ProxySource::EnvDisabled => "disabled by OCG_DISABLE_PROXY",
                _ => "disabled",
            },
        );
        return;
    }
    doctor.line("ok", "proxy mode", "auto");
    let source = match proxy.source() {
        ProxySource::Environment => "environment",
        ProxySource::System => "macOS system settings",
        ProxySource::Direct => "direct (no proxy configured)",
        ProxySource::CliDisabled | ProxySource::EnvDisabled => "disabled",
    };
    doctor.line("ok", "proxy source", source);
    for endpoint in proxy.plan().endpoints() {
        let label = match endpoint.scheme() {
            ProxyScheme::Http => "HTTP proxy",
            ProxyScheme::Https => "HTTPS proxy",
            ProxyScheme::All => "all proxy",
        };
        doctor.line("ok", label, "configured");
    }
    if !proxy.plan().no_proxy().is_empty() {
        doctor.line("ok", "no-proxy", "configured");
    }
    for warning in proxy.warnings() {
        // SOCKS pass-through is rendered structurally, with its full meaning,
        // by `print_proxy_env_diagnostics`.
        if warning.contains("SOCKS scheme that OCG does not interpret") {
            continue;
        }
        doctor.line("warn", "proxy", warning);
    }
}

/// The standard proxy variables and the SOCKS pass-through contract.
///
/// Only presence is reported for the variables; values (which may embed a
/// credential) are never read here. A SOCKS `ALL_PROXY` is a WARN, not a FAIL:
/// OCG's own HTTP client does not interpret SOCKS, but the value is preserved
/// verbatim for the child OpenCode process, and no OCG network operation
/// depends on it.
fn print_proxy_env_diagnostics(doctor: &mut Doctor, proxy: &ProxySelection, disable_proxy: bool) {
    doctor.section(Section::Environment);
    for name in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"] {
        if proxy_env_present(name) {
            doctor.line("info", name, "present (value never shown)");
        } else {
            doctor.line("info", name, "not set");
        }
    }
    if disable_proxy {
        doctor.line(
            "info",
            "proxy policy",
            "disabled for this invocation; the child OpenCode process inherits no proxy variable",
        );
        return;
    }
    for (name, _url) in proxy.plan().passthrough() {
        doctor.line(
            "warn",
            name,
            "uses a SOCKS scheme OCG does not interpret; OCG's own network calls ignore it, but the value is preserved verbatim for the child OpenCode process",
        );
    }
}
/// `ocg upgrade`: self-update OCG, then force-maintain the active OpenCode.
fn upgrade_command(
    effective: &config::Effective,
    project_root: &Path,
    env: &Env,
    disable_proxy: bool,
) -> std::result::Result<i32, Failure> {
    let platform = Platform::current().map_err(Failure::Ocg)?;
    let clock = SystemClock;
    let process = SystemProcessHost;
    let proxy = resolve_proxy(disable_proxy);
    for warning in proxy.warnings() {
        eprintln!("ocg: warning: {warning}");
    }
    let proxy_env = proxy.child_env();
    let http =
        ReqwestHttp::with_policy(proxy.plan(), env.github_token.clone()).map_err(Failure::Ocg)?;
    let manager = runtime_manager(project_root, effective, env, &http, &clock, &process)
        .map_err(Failure::Ocg)?
        .with_proxy_env(proxy_env);

    let current = Version::parse(VERSION).map_err(|error| {
        Failure::Ocg(OcgError::config(format!(
            "invalid OCG version '{VERSION}': {error}"
        )))
    })?;
    match std::env::current_exe() {
        Ok(exe) => {
            match runtime::self_update::self_update(
                &http,
                &manager.api_base,
                &manager.ocg_repo,
                platform,
                &exe,
                &current,
                &process,
            ) {
                Ok(outcome) if outcome.updated => {
                    println!("OCG:        {} -> {}", outcome.from, outcome.to);
                }
                Ok(outcome) => println!("OCG:        {} (up to date)", outcome.from),
                Err(error) => println!("OCG:        self-update skipped: {error}"),
            }
        }
        Err(error) => println!(
            "OCG:        self-update skipped: cannot determine the running executable: {error}"
        ),
    }

    let outcome = manager.upgrade().map_err(Failure::Ocg)?;
    let before = match outcome.before.installed() {
        true => format!(
            "{} ({})",
            describe_runtime_version(outcome.before.version.as_ref()),
            outcome
                .before
                .source
                .map(|source| source.label())
                .unwrap_or("unknown")
        ),
        false => "not installed".to_string(),
    };
    let after = format!(
        "{} ({}) {}",
        describe_runtime_version(outcome.after.version.as_ref()),
        outcome.after.source.label(),
        outcome.after.path.display()
    );
    println!("OpenCode:  {before} -> {after}");
    for warning in outcome.after.warnings.iter().chain(outcome.warnings.iter()) {
        eprintln!("ocg: warning: {warning}");
    }
    Ok(0)
}

/// Whether a directory (or its nearest existing ancestor) is writable.
fn is_writable_dir(path: &Path) -> bool {
    let mut current = path;
    loop {
        match std::fs::metadata(current) {
            Ok(metadata) => return !metadata.permissions().readonly(),
            Err(_) => match current.parent() {
                Some(parent) if !parent.as_os_str().is_empty() => current = parent,
                _ => return false,
            },
        }
    }
}

/// Resolve the telemetry policy without letting a broken policy block the
/// command that owns it. A rejectable config (for example `localOnly=false`)
/// disables collection with a warning; `ocg validate` still reports it.
fn telemetry_for(effective: &config::Effective, env: &Env) -> (TelemetryConfig, Vec<String>) {
    match TelemetryConfig::from_config(&effective.data) {
        Ok(config) => (
            config.with_env_override(env.telemetry.as_deref()),
            Vec::new(),
        ),
        Err(error) => (
            TelemetryConfig::disabled(),
            vec![format!("telemetry disabled: {error}")],
        ),
    }
}

fn print_telemetry_warnings(warnings: &[String]) {
    for warning in warnings {
        eprintln!("ocg: warning: {warning}");
    }
}

/// `ocg context <task...>` and `ocg context symbols <query>`.
fn context_command(
    effective: &config::Effective,
    project_root: &Path,
    args: &[OsString],
    env: &Env,
    pretty: bool,
) -> std::result::Result<i32, Failure> {
    let config = ContextConfig::from_config(&effective.data).map_err(Failure::Ocg)?;
    if !config.enabled {
        // Disabled means disabled: do not read, index or cache anything.
        println!(
            "context engine is disabled (context.enabled=false); no index or cache work was performed"
        );
        return Ok(0);
    }
    let git = SystemGitHost;
    let clock = SystemClock;
    let capabilities = crate::capabilities::CapabilityConfig::from_config(&effective.data)
        .map_err(Failure::Ocg)?;
    let verification =
        crate::verification::Config::from_config(&effective.data).map_err(Failure::Ocg)?;
    let engine = ContextEngine::new(project_root, config, &git, &clock)
        .with_capabilities(capabilities)
        .with_verification(verification);
    let words: Vec<String> = args
        .iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();

    if words.first().map(String::as_str) == Some("symbols") {
        let query = words[1..].join(" ").trim().to_string();
        if query.is_empty() {
            return Err(usage_failure("context symbols needs a query"));
        }
        let hits = engine.search_symbols(&query, 100).map_err(Failure::Ocg)?;
        let definition = engine.definition(&query).map_err(Failure::Ocg)?;
        let payload = json!({
            "query": query,
            "definition": definition,
            "symbols": hits,
        });
        print_config(&payload, pretty)?;
        return Ok(0);
    }

    let task = words.join(" ").trim().to_string();
    if task.is_empty() {
        return Err(usage_failure(
            "context needs a task description (for example: ocg context fix the parser)",
        ));
    }
    let started = std::time::Instant::now();
    let outcome = engine.plan(&task, None).map_err(Failure::Ocg)?;
    let duration_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    for warning in &outcome.warnings {
        eprintln!("ocg: warning: {warning}");
    }

    // Local telemetry: byte and estimate accounting only, never the task text.
    let (telemetry_config, telemetry_warnings) = telemetry_for(effective, env);
    print_telemetry_warnings(&telemetry_warnings);
    let capsule_bytes = outcome
        .plan
        .capsule
        .as_ref()
        .and_then(|capsule| serde_json::to_vec(capsule).ok())
        .map(|bytes| bytes.len() as u64)
        .unwrap_or(0);
    let capabilities = outcome
        .plan
        .capabilities
        .capabilities
        .iter()
        .map(|entry| entry.capability.name())
        .collect();
    let event = telemetry::Event {
        task_id: telemetry::Event::hashed_task_id(&format!("context|{task}")),
        task_type: Some("context".to_string()),
        timestamp: clock.now_unix(),
        duration_ms,
        input_tokens: telemetry::TokenCount::estimated(outcome.plan.estimated_tokens as u64),
        context: telemetry::ContextMetrics::new(
            outcome.plan.candidate_bytes as u64,
            outcome.plan.selected_bytes as u64,
            capsule_bytes,
        ),
        repo: telemetry::RepoMetrics {
            files: outcome.index_report.metrics.files,
            symbols: outcome.index_report.metrics.symbols,
            index_reused: outcome.index_report.metrics.reused,
            index_updated: outcome.index_report.metrics.updated,
            cache_hit: Some(outcome.from_cache),
        },
        capabilities,
        outcome: telemetry::Outcome::Success,
        ..telemetry::Event::new(String::new(), clock.now_unix())
    };
    print_telemetry_warnings(&telemetry::record(project_root, &telemetry_config, event));
    if pretty {
        let value = serde_json::to_value(&outcome.plan).map_err(|error| {
            Failure::Ocg(OcgError::config(format!(
                "cannot serialize the context plan: {error}"
            )))
        })?;
        print_config(&value, true)?;
    } else {
        println!("{}", context::plan_text(&outcome.plan));
    }
    Ok(0)
}

/// `ocg cache clean|stats`. Never touches the managed runtime.
fn cache_command(
    effective: &config::Effective,
    project_root: &Path,
    action: Option<&str>,
) -> std::result::Result<i32, Failure> {
    let config = ContextConfig::from_config(&effective.data).map_err(Failure::Ocg)?;
    let git = SystemGitHost;
    let clock = SystemClock;
    let engine = ContextEngine::new(project_root, config, &git, &clock);
    match action {
        Some("clean") => {
            let report = engine.cache_clean().map_err(Failure::Ocg)?;
            println!(
                "removed {} context cache entr{} ({} bytes) from {}",
                report.removed_entries,
                if report.removed_entries == 1 {
                    "y"
                } else {
                    "ies"
                },
                report.removed_bytes,
                report.dir
            );
            Ok(0)
        }
        Some("stats") | None => {
            let stats = engine.cache_stats();
            println!("context cache: {}", stats.dir);
            println!(
                "  entries: {}  bytes: {}  corrupt: {}",
                stats.entries, stats.bytes, stats.corrupt
            );
            match (stats.oldest, stats.newest) {
                (Some(oldest), Some(newest)) => {
                    println!("  oldest: {oldest}  newest: {newest}");
                }
                _ => println!("  oldest: -  newest: -"),
            }
            let index = engine.load_index();
            match index {
                Some(index) => println!(
                    "index: {} indexed files, {} symbols",
                    index.metrics.files, index.metrics.symbols
                ),
                None => println!("index: not built"),
            }
            Ok(0)
        }
        Some(other) => Err(usage_failure(format!(
            "unknown cache action: {other} (try 'ocg cache stats' or 'ocg cache clean')"
        ))),
    }
}

/// `ocg stats [--pretty]`: read-only local telemetry aggregate. Offline, never
/// creates state and never reads the network.
fn stats_command(
    effective: &config::Effective,
    project_root: &Path,
    args: &[OsString],
    env: &Env,
    pretty: bool,
) -> std::result::Result<i32, Failure> {
    if !args.is_empty() {
        let joined = args
            .iter()
            .map(|arg| arg.to_string_lossy())
            .collect::<Vec<_>>()
            .join(" ");
        return Err(usage_failure(format!(
            "stats takes no arguments; got: {joined}"
        )));
    }
    let (config, warnings) = telemetry_for(effective, env);
    print_telemetry_warnings(&warnings);
    let store = telemetry::TelemetryStore::new(project_root, config);
    let stats = telemetry::TelemetryStats::collect(&store);
    if pretty {
        let value = serde_json::to_value(&stats).map_err(|error| {
            Failure::Ocg(OcgError::config(format!(
                "cannot serialize the telemetry stats: {error}"
            )))
        })?;
        print_config(&value, true)?;
    } else {
        print!("{}", stats.render());
    }
    Ok(0)
}

/// `ocg verify [fast|normal|full]`: run only configured trusted commands.
fn verify_command(
    effective: &config::Effective,
    project_root: &Path,
    args: &[OsString],
    env: &Env,
    pretty: bool,
) -> std::result::Result<i32, Failure> {
    let config = VerificationConfig::from_config(&effective.data).map_err(Failure::Ocg)?;
    let words: Vec<String> = args
        .iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    if words.len() > 1 {
        return Err(usage_failure(format!(
            "verify takes at most one stage (fast, normal or full); got: {}",
            words.join(" ")
        )));
    }
    let requested = words
        .first()
        .cloned()
        .unwrap_or_else(|| config.default_stage.clone());
    // Validate the stage name even when verification is disabled.
    config.stage(&requested).map_err(Failure::Ocg)?;

    let clock = SystemClock;
    let context_config = ContextConfig::from_config(&effective.data).map_err(Failure::Ocg)?;
    let mut extra_notes = Vec::new();

    // An advisory proposal only; it is never executed here. When context is
    // disabled nothing is indexed or read and the absence is stated explicitly.
    let proposal = if !config.enabled {
        None
    } else if !context_config.enabled {
        extra_notes.push(
            "context is disabled (context.enabled=false); targeted-test selection was skipped and no context index was created"
                .to_string(),
        );
        None
    } else if config.include_test_proposal {
        let git = SystemGitHost;
        let capabilities = CapabilityConfig::from_config(&effective.data).map_err(Failure::Ocg)?;
        let engine = ContextEngine::new(project_root, context_config, &git, &clock)
            .with_capabilities(capabilities)
            .with_verification(config.clone());
        match engine.targeted_tests() {
            Ok(proposal) => Some(proposal),
            Err(error) => {
                eprintln!("ocg: warning: targeted test proposal skipped: {error}");
                extra_notes.push(format!("targeted-test selection was skipped: {error}"));
                None
            }
        }
    } else {
        None
    };

    let runner = SystemCaptureRunner;
    let started = std::time::Instant::now();
    let mut report = execute(&VerifyRequest {
        root: project_root,
        config: &config,
        stage: requested,
        runner: &runner,
        clock: &clock,
        test_proposal: proposal,
    })
    .map_err(Failure::Ocg)?;
    let duration_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    report.notes.extend(extra_notes);

    // Local telemetry: attempt/byte accounting only, never a command string or
    // any captured output.
    let (telemetry_config, telemetry_warnings) = telemetry_for(effective, env);
    print_telemetry_warnings(&telemetry_warnings);
    let event = telemetry::Event {
        task_id: telemetry::Event::hashed_task_id(&format!("verify|{}", report.stage)),
        task_type: Some("verification".to_string()),
        timestamp: clock.now_unix(),
        duration_ms,
        verification: telemetry::verification_metrics(&report),
        logs: telemetry::log_metrics(project_root, &report),
        outcome: telemetry::verification_outcome(&report),
        ..telemetry::Event::new(String::new(), clock.now_unix())
    };
    print_telemetry_warnings(&telemetry::record(project_root, &telemetry_config, event));

    if pretty {
        let value = serde_json::to_value(&report).map_err(|error| {
            Failure::Ocg(OcgError::config(format!(
                "cannot serialize the verification report: {error}"
            )))
        })?;
        print_config(&value, true)?;
    } else {
        print_verification_report(&report);
    }
    Ok(if report.failed() { 1 } else { 0 })
}

fn print_verification_report(report: &crate::verification::VerificationReport) {
    println!(
        "verification stage: {} ({})",
        report.stage,
        report.overall().as_str()
    );
    for note in &report.notes {
        println!("note: {note}");
    }
    for result in &report.results {
        println!(
            "  [{}] {} ({} ms, {})",
            if result.success { "ok" } else { "fail" },
            result.display(),
            result.duration_ms,
            result.exit.label()
        );
        if !result.output.summary.is_empty() {
            for line in result.output.summary.iter().take(20) {
                println!("      {line}");
            }
        }
        for note in &result.output.notes {
            println!("      note: {note}");
        }
        if let Some(path) = &result.raw_log {
            println!(
                "      raw log: {path}{}",
                if result.raw_truncated {
                    " (truncated: retained prefix only)"
                } else {
                    ""
                }
            );
        }
    }
    if let Some(proposal) = &report.test_proposal {
        print!("{}", proposal.render());
    }
}

/// `ocg tools <task...>`: the capability plan / firewall diagnostic.
fn tools_command(
    effective: &config::Effective,
    args: &[OsString],
    pretty: bool,
) -> std::result::Result<i32, Failure> {
    let config = CapabilityConfig::from_config(&effective.data).map_err(Failure::Ocg)?;
    let task = args
        .iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string();
    if task.is_empty() {
        return Err(usage_failure(
            "tools needs a task description (for example: ocg tools commit the fix)",
        ));
    }
    let plan = CapabilityPlan::plan_config(
        &task,
        &CapabilityEvidence::default(),
        &config.custom,
        config.enabled,
    );
    if pretty {
        let value = serde_json::to_value(&plan).map_err(|error| {
            Failure::Ocg(OcgError::config(format!(
                "cannot serialize the capability plan: {error}"
            )))
        })?;
        print_config(&value, true)?;
    } else {
        print!("{}", plan.render());
    }
    Ok(0)
}

/// `ocg checkpoint list|show|save`.
fn checkpoint_command(
    effective: &config::Effective,
    project_root: &Path,
    args: &[OsString],
    pretty: bool,
) -> std::result::Result<i32, Failure> {
    let words: Vec<String> = args
        .iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    let git = SystemGitHost;
    match words.first().map(String::as_str) {
        None | Some("list") => {
            if words.len() > 1 {
                return Err(usage_failure(format!(
                    "checkpoint list takes no arguments; got: {}",
                    words[1..].join(" ")
                )));
            }
            let (summaries, corrupt) = checkpoint::list(project_root);
            println!(
                "checkpoints: {} ({} corrupt, ignored)",
                summaries.len(),
                corrupt
            );
            for summary in &summaries {
                println!(
                    "  {}  {}  {}",
                    summary.created_at,
                    summary.phase.as_str(),
                    summary.id
                );
                println!("      task: {}", summary.task);
            }
            Ok(0)
        }
        Some("show") => {
            if words.len() != 2 {
                return Err(usage_failure(
                    "checkpoint show needs exactly one checkpoint id (options such as --pretty may appear before or after it)",
                ));
            }
            let id = &words[1];
            if id.starts_with('-') {
                return Err(usage_failure(format!(
                    "unknown checkpoint show option: {id}"
                )));
            }
            let loaded = checkpoint::load(project_root, id, &git).map_err(Failure::Ocg)?;
            if pretty {
                let value = json!({
                    "checkpoint": loaded.checkpoint,
                    "stale": loaded.staleness.stale,
                    "reasons": loaded.staleness.reasons,
                });
                print_config(&value, true)?;
            } else {
                println!(
                    "checkpoint {} ({}) created_at {}",
                    loaded.checkpoint.id,
                    loaded.checkpoint.phase.as_str(),
                    loaded.checkpoint.created_at
                );
                println!("task:        {}", loaded.checkpoint.capsule.task);
                println!(
                    "stale:       {}{}",
                    loaded.staleness.stale,
                    if loaded.staleness.reasons.is_empty() {
                        String::new()
                    } else {
                        format!(" ({})", loaded.staleness.reasons.join("; "))
                    }
                );
            }
            Ok(0)
        }
        Some("save") => save_checkpoint(effective, project_root, &words[1..]),
        Some(other) => Err(usage_failure(format!(
            "unknown checkpoint action: {other} (try 'list', 'show' or 'save')"
        ))),
    }
}

fn save_checkpoint(
    effective: &config::Effective,
    project_root: &Path,
    args: &[String],
) -> std::result::Result<i32, Failure> {
    let mut phase: Option<Phase> = None;
    let mut task: Option<String> = None;
    let mut decisions: Vec<String> = Vec::new();
    let mut index = 0;
    while index < args.len() {
        let argument = &args[index];
        match argument.as_str() {
            "--phase" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| usage_failure("checkpoint save --phase needs a value"))?;
                phase = Phase::parse(value);
                if phase.is_none() {
                    return Err(usage_failure(format!(
                        "unknown phase '{value}' (expected explore-to-build, build-to-verify, verify-to-debug, debug-to-build, decision)"
                    )));
                }
                index += 2;
            }
            "--task" => {
                task = Some(
                    args.get(index + 1)
                        .ok_or_else(|| usage_failure("checkpoint save --task needs a value"))?
                        .clone(),
                );
                index += 2;
            }
            "--decision" => {
                decisions.push(
                    args.get(index + 1)
                        .ok_or_else(|| usage_failure("checkpoint save --decision needs a value"))?
                        .clone(),
                );
                index += 2;
            }
            other => {
                return Err(usage_failure(format!(
                    "unknown checkpoint save option: {other}"
                )))
            }
        }
    }
    let phase = phase.ok_or_else(|| usage_failure("checkpoint save needs --phase"))?;
    let task = task.unwrap_or_else(|| "checkpoint".to_string());

    let git = SystemGitHost;
    let clock = SystemClock;
    let snapshot = crate::context::gitdiff::GitSnapshot::collect(project_root, &git);
    let git_fingerprint = crate::context::gitdiff::snapshot_fingerprint(&snapshot);

    // Best-effort capsule from the current context plan; the checkpoint still
    // saves without one if context is disabled or unavailable.
    let (capsule, provenance) = {
        let context_config = ContextConfig::from_config(&effective.data).map_err(Failure::Ocg)?;
        let capabilities = CapabilityConfig::from_config(&effective.data).map_err(Failure::Ocg)?;
        let verification =
            VerificationConfig::from_config(&effective.data).map_err(Failure::Ocg)?;
        if context_config.enabled {
            let engine = ContextEngine::new(project_root, context_config, &git, &clock)
                .with_capabilities(capabilities)
                .with_verification(verification);
            match engine.plan(&task, None) {
                Ok(outcome) => (
                    outcome
                        .plan
                        .capsule
                        .clone()
                        .unwrap_or_else(|| crate::context::capsule::TaskCapsule::new(&task)),
                    outcome.plan.provenance,
                ),
                Err(error) => {
                    eprintln!("ocg: warning: checkpoint capsule built without context: {error}");
                    (
                        crate::context::capsule::TaskCapsule::new(&task),
                        crate::context::freshness::Provenance::default(),
                    )
                }
            }
        } else {
            (
                crate::context::capsule::TaskCapsule::new(&task),
                crate::context::freshness::Provenance::default(),
            )
        }
    };

    let decisions: Vec<crate::context::capsule::Decision> = decisions
        .into_iter()
        .map(|decision| crate::context::capsule::Decision {
            decision,
            rationale: None,
            date: None,
            date_unknown: true,
        })
        .collect();
    let checkpoint = checkpoint::Checkpoint::build(
        phase,
        capsule,
        snapshot.state.clone(),
        git_fingerprint,
        None,
        provenance,
        decisions,
        clock.now_unix(),
    );
    let path = checkpoint.save(project_root).map_err(Failure::Ocg)?;
    println!("checkpoint saved: {} ({})", checkpoint.id, path.display());
    Ok(0)
}

/// `ocg __bridge <event>`: the hidden JSON bridge the generated plugin calls.
///
/// It never fails the process: a bad payload, a disabled policy or a controller
/// error all become `{"ok":false,...}` on stdout, so the adapter can fail soft.
fn bridge_command(
    effective: &config::Effective,
    project_root: &Path,
    args: &[OsString],
    env: &Env,
) -> std::result::Result<i32, Failure> {
    let event = args
        .first()
        .map(|value| value.to_string_lossy().into_owned())
        .unwrap_or_default();
    let value = bridge_payload(effective, project_root, &event, env);
    println!(
        "{}",
        serde_json::to_string(&value).unwrap_or_else(|_| "{\"ok\":false}".to_string())
    );
    Ok(0)
}

/// Build the bridge reply. Fail-soft by construction.
fn bridge_payload(
    effective: &config::Effective,
    project_root: &Path,
    event: &str,
    env: &Env,
) -> Value {
    // Always consume stdin first, even on the disabled/early-return paths, so
    // the generated adapter's writer never sees a BrokenPipe. The read is
    // capped and overflow is rejected fail-soft.
    let (payload, oversized) = read_stdin_json(BRIDGE_MAX_STDIN_BYTES);
    if oversized {
        return json!({
            "ok": false,
            "error": format!("bridge payload exceeds the {BRIDGE_MAX_STDIN_BYTES} byte cap"),
        });
    }
    if event.is_empty() {
        return json!({"ok": false, "error": "missing bridge event"});
    }
    if matches!(
        event,
        "session.prompt"
            | "work.dispatch"
            | "work.replace"
            | "context.observe"
            | "session.context.observe"
            | "context-observation"
    ) {
        let readiness =
            crate::profile::Profile::from_ocg_config(&effective.data).and_then(|profile| {
                let key = env.v2_lead.as_ref().map(|lead| lead.level.as_str());
                let (key, _) = profile.select(key)?;
                if let Some(lead) = env.v2_lead.as_ref() {
                    let expected = model::lead_contract(&effective.data, key)?;
                    if lead.agent != expected.agent
                        || lead.provider_id != expected.provider_id
                        || lead.model_id != expected.model_id
                        || lead.variant != expected.variant
                    {
                        return Err(OcgError::config(
                            "invocation Lead does not match the current OCG Profile selection",
                        ));
                    }
                }
                Ok(())
            });
        if let Err(error) = readiness {
            return json!({"ok": false, "error": error.to_string()});
        }
    }
    let orchestration =
        match crate::orchestration::OrchestrationConfig::from_config(&effective.data) {
            Ok(config) => config,
            Err(error) => return json!({"ok": false, "error": error.to_string()}),
        };
    if !orchestration.enabled {
        return json!({"ok": false, "disabled": true, "context": ""});
    }
    let context = match ContextConfig::from_config(&effective.data) {
        Ok(config) => config,
        Err(error) => return json!({"ok": false, "error": error.to_string()}),
    };
    let capabilities = match CapabilityConfig::from_config(&effective.data) {
        Ok(config) => config,
        Err(error) => return json!({"ok": false, "error": error.to_string()}),
    };
    let verification = match VerificationConfig::from_config(&effective.data) {
        Ok(config) => config,
        Err(error) => return json!({"ok": false, "error": error.to_string()}),
    };
    // The bridge is a real execution path: `context.observe` can drive a
    // rollover continuation resume, which is provider-costly. It must carry the
    // same optional Policy and mandatory economic configuration as every other
    // path, so a configured hard Mission budget is enforced here too (and is not
    // silently replaced by the permissive constructor defaults).
    let policy = match crate::orchestration::policy::PolicyConfig::from_config(&effective.data) {
        Ok(config) => config,
        Err(error) => return json!({"ok": false, "error": error.to_string()}),
    };
    let budget = match crate::orchestration::budget::BudgetConfig::from_config(&effective.data) {
        Ok(config) => config,
        Err(error) => return json!({"ok": false, "error": error.to_string()}),
    };
    let git = SystemGitHost;
    let clock = SystemClock;
    let controller = crate::orchestration::controller::Controller::new(
        project_root,
        orchestration,
        context,
        capabilities,
        verification,
        &git,
        &clock,
    )
    .with_policy(policy)
    .with_budget(budget);
    let runner = SystemCaptureRunner;
    let (telemetry_config, warnings) = telemetry_for(effective, env);
    print_telemetry_warnings(&warnings);
    let reports = match crate::reports::ReportsConfig::from_config(&effective.data) {
        Ok(config) => config,
        Err(error) => return json!({"ok": false, "error": error.to_string()}),
    };
    let mut bridge =
        crate::orchestration::bridge::BridgeContext::new(&controller, &runner, telemetry_config)
            .with_reports(reports)
            // The worker routing table OCG writes into the generated agent
            // config. A canonical child Attempt freezes the model its agent will
            // actually use, so the recorded contract is not an approximation.
            .with_routing(crate::orchestration::bridge::WorkerRouting::from_config(
                &effective.data,
            ));
    // The session.prompt authority gate and context.observe require a live V2 client.
    // Ordinary bridge events remain entirely local.
    if matches!(
        event,
        "context.observe"
            | "session.context.observe"
            | "context-observation"
            | "session.prompt"
            | "work.mission.create"
    ) {
        #[cfg(unix)]
        let channel = std::env::var_os(compat::v2_rendezvous::CHANNEL_ENV);
        #[cfg(unix)]
        let registration = if let Some(path) = channel {
            let identity = std::env::var(compat::v2_rendezvous::ID_ENV).unwrap_or_default();
            match compat::v2_rendezvous::resolve(Path::new(&path), &identity) {
                Ok(registration) => Some(registration),
                Err(error) => return json!({"ok":false,"error":error.to_string()}),
            }
        } else if event == "session.prompt" {
            // Direct endpoint env (or ambient discovery) cannot establish
            // invocation-owned prompt authority.
            return json!({"ok":false,"error":"invocation channel unavailable"});
        } else {
            env.v2_server_url
                .as_deref()
                .zip(env.v2_server_password.as_ref())
                .map(|(url, password)| {
                    compat::v2_client::ServiceRegistration::new(url, password.expose())
                })
        };
        #[cfg(not(unix))]
        let registration = env
            .v2_server_url
            .as_deref()
            .zip(env.v2_server_password.as_ref())
            .map(|(url, password)| {
                compat::v2_client::ServiceRegistration::new(url, password.expose())
            });
        if let (Some(registration), Some(lead)) = (registration, env.v2_lead.clone()) {
            let directory = env
                .v2_directory
                .clone()
                .unwrap_or_else(|| project_root.to_string_lossy().into_owned());
            match compat::v2_client::V2SessionClient::connect(&registration, directory) {
                Ok(client) => {
                    let runtime: std::rc::Rc<std::cell::RefCell<Box<dyn BridgeRuntimeClient>>> =
                        std::rc::Rc::new(std::cell::RefCell::new(Box::new(client)));
                    bridge = bridge
                        .with_bridge_runtime(runtime, lead.runtime_profile())
                        .with_lead_contract(lead);
                }
                Err(_) => {
                    // The bridge will record an explicit unknown observation;
                    // do not turn a client startup failure into Mission
                    // failure or expose the credential in the reply.
                    if event == "session.prompt" {
                        return json!({"ok":false,"error":"owned runtime connection unavailable"});
                    }
                }
            }
        } else if event == "session.prompt" {
            return json!({"ok":false,"error":"invocation runtime or Lead contract unavailable"});
        }
    }
    bridge.dispatch(event, &payload)
}

/// The hard cap on a bridge payload. The generated adapter never sends anything
/// close to this; the cap protects against a hostile or broken caller and keeps
/// memory bounded.
pub const BRIDGE_MAX_STDIN_BYTES: usize = 4 * 1024 * 1024;

/// Read the bridge payload from stdin with a hard cap. Returns `(value,
/// oversized)`. Empty or invalid input is `null`, never an error. When the
/// input exceeds the cap it is drained (so the writer does not get a
/// BrokenPipe) but not retained, and `oversized` is true.
fn read_stdin_json(max_bytes: usize) -> (Value, bool) {
    use std::io::Read;
    let mut buffer = Vec::new();
    {
        let mut limited = std::io::stdin().lock().take(max_bytes as u64 + 1);
        if limited.read_to_end(&mut buffer).is_err() {
            return (Value::Null, false);
        }
    }
    // Drain the rest without retaining it, so a larger writer can complete.
    let mut sink = std::io::sink();
    let _ = std::io::copy(&mut std::io::stdin().lock(), &mut sink);
    if buffer.len() > max_bytes {
        return (Value::Null, true);
    }
    let text = String::from_utf8_lossy(&buffer);
    if text.trim().is_empty() {
        return (Value::Null, false);
    }
    match serde_json::from_str::<Value>(&text) {
        Ok(value) => (value, false),
        Err(_) => (Value::Null, false),
    }
}

/// `ocg config`: guided, safe Lead/provider configuration.
///
/// The command owns the interactive surface and the runtime probe; everything
/// else (candidate building, validation, atomic write, reporting) lives in
/// [`crate::config_command`].
#[allow(clippy::too_many_arguments)]
fn config_command(
    args: &[OsString],
    defaults: Value,
    ocg_home: Option<PathBuf>,
    invocation_dir: &Path,
    project_root: &Path,
    user_path: &Path,
    project_path: &Path,
    effective: &config::Effective,
    level: &str,
    env: &Env,
    disable_proxy: bool,
) -> std::result::Result<i32, Failure> {
    if args.first().is_some_and(|arg| arg == "lead") {
        return Err(Failure::Usage(
            "tier-based config lead is removed; configure the OCG Profile provider/model selection instead"
                .to_string(),
        ));
    }
    let request = crate::config_command::parse_request(args).map_err(Failure::Ocg)?;
    let context = crate::config_command::Context {
        defaults,
        ocg_home,
        project_root: project_root.to_path_buf(),
        invocation_dir: invocation_dir.to_path_buf(),
        user_path: user_path.to_path_buf(),
        project_path: project_path.to_path_buf(),
        env,
        level: level.to_string(),
        current: effective.clone(),
    };
    let probe = |candidate: &config::Effective, probe_level: &str| {
        probe_candidate_config(
            candidate,
            probe_level,
            project_root,
            invocation_dir,
            env,
            disable_proxy,
        )
    };
    let activate = |candidate: &config::Effective, activate_level: &str| {
        activate_candidate_config(
            candidate,
            activate_level,
            project_root,
            invocation_dir,
            env,
            disable_proxy,
        )
    };
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut input = stdin.lock();
    let mut output = stdout.lock();
    let code = crate::config_command::execute(
        &request,
        &context,
        &probe,
        &activate,
        &mut input,
        &mut output,
    )
    .map_err(Failure::Ocg)?;
    Ok(code)
}

/// Probe the runtime catalogue for a candidate configuration.
///
/// Returns `None` when no probe can be attempted at all (no resolvable runtime
/// or adapter); the caller then reports the change as runtime-unverified
/// instead of pretending the model was checked.
///
/// OpenCode 2 exposes no catalogue of config-declared providers, so for the
/// V2 family this honestly reports the catalogue evidence as unavailable (see
/// [`probe_v2_owned_catalogue`]); availability is proven by activation on an
/// OCG-owned runtime instead. The v1 family keeps using OpenCode's supported
/// `models` CLI output. Neither path reads provider credential stores.
fn probe_candidate_config(
    effective: &config::Effective,
    level: &str,
    project_root: &Path,
    invocation_dir: &Path,
    env: &Env,
    disable_proxy: bool,
) -> Option<ModelPreflight> {
    let clock = SystemClock;
    let process = SystemProcessHost;
    let manager = runtime_manager(project_root, effective, env, &NoHttp, &clock, &process).ok()?;
    let report = manager.resolve_for_report();
    let program = report.path.clone()?;
    let adapter = resolve_adapter(report.version.as_ref()).ok()?;
    let mut resolved = build::build_opencode_config_for(effective, level, adapter).ok()?;
    // The generated plugin is not materialized for a configuration check, so
    // probe with the exact config minus OCG's not-yet-existing plugin file.
    crate::orchestration::plugin::remove_ocg_plugin_for(&mut resolved, adapter.plugin_key());
    let content = serde_json::to_string(&resolved).ok()?;
    let proxy = resolve_proxy(disable_proxy);
    let proxy_env = proxy.child_env();

    if adapter.major() == compat::Major::V2 {
        return probe_candidate_v2(
            &effective.data,
            &program,
            &content,
            invocation_dir,
            &proxy_env,
        );
    }

    crate::preflight::probe(
        &effective.data,
        Some(level),
        &process,
        &program,
        invocation_dir,
        &content,
        &proxy_env,
    )
    .ok()
}

/// Report provider/model catalogue evidence for an OCG-owned OpenCode 2
/// runtime.
///
/// OpenCode 2 exposes no catalogue of config-declared providers, so no
/// private server is started and no endpoint is queried here: the only honest
/// catalogue answer is [`ModelPreflight::Unavailable`], and the real
/// availability evidence is the OCG-owned runtime accepting and applying the
/// selected Lead (see the activation/effective-state observation).
///
/// The `Option` return shape is retained for the shared call sites; this
/// implementation never returns `None` because there is no probe attempt that
/// could fail.
fn probe_v2_owned_catalogue(
    _data: &Value,
    _program: &Path,
    _config_content: &str,
    _invocation_dir: &Path,
    _proxy_env: &crate::proxy::ChildProxyEnv,
) -> Option<ModelPreflight> {
    // OpenCode 2 exposes no catalogue of config-declared providers. Verified
    // against real OpenCode 2.0.14 on linux-arm64 and darwin-arm64:
    // /api/config document entries carry an empty `info`, and /api/model +
    // /api/provider list only built-in/registry models — never configured
    // custom providers (their credentials resolve at use time). The previous
    // implementation parsed a fictional `info.providers` shape and therefore
    // reported every configured provider as "not currently exposed" while the
    // same owned runtime demonstrably served the model — a false failure.
    //
    // The only honest availability evidence is the OCG-owned runtime itself:
    // every launch applies the Lead on an owned session (hard-failing if the
    // runtime cannot honor it), and status/doctor --effective observe the
    // effective agent/provider/model on an owned session. Report Unavailable
    // with this reason rather than fabricating a negative catalogue.
    Some(ModelPreflight::Unavailable {
        reason: "OpenCode 2 exposes no model catalogue for configured providers; the active Lead is verified by the OCG-owned runtime's session observation (see the effective state) and enforced at launch".to_string(),
    })
}

/// Backwards-compatible alias used by the candidate-activation path. Identical
/// to [`probe_v2_owned_catalogue`] — kept so this refactor does not split one
/// proven helper across two names.
fn probe_candidate_v2(
    data: &Value,
    program: &Path,
    config_content: &str,
    invocation_dir: &Path,
    proxy_env: &crate::proxy::ChildProxyEnv,
) -> Option<ModelPreflight> {
    probe_v2_owned_catalogue(data, program, config_content, invocation_dir, proxy_env)
}

/// Activate a candidate configuration on an OCG-owned private runtime and read
/// the effective Lead back.
///
/// This is the last stage of a switch: after the file is written, prove that the
/// exact generated configuration is accepted by the runtime OCG would launch and
/// that its session reports the resolved agent/provider/model/variant. When no
/// session-level runtime can be resolved (a v1 runtime or none at all) the
/// caller reports the change as written-but-not-observed rather than verified.
fn activate_candidate_config(
    effective: &config::Effective,
    level: &str,
    project_root: &Path,
    invocation_dir: &Path,
    env: &Env,
    disable_proxy: bool,
) -> crate::config_command::Activation {
    use crate::config_command::Activation;

    let clock = SystemClock;
    let process = SystemProcessHost;
    let Ok(manager) = runtime_manager(project_root, effective, env, &NoHttp, &clock, &process)
    else {
        return Activation::NotAvailable("no OpenCode runtime could be resolved".to_string());
    };
    let report = manager.resolve_for_report();
    let Some(program) = report.path.clone() else {
        return Activation::NotAvailable("no OpenCode runtime could be resolved".to_string());
    };
    let adapter = match report.version.as_ref() {
        Some(version) => match compat::classify(version.clone()) {
            Ok(detected) => compat::adapter_for(&detected),
            Err(error) => return Activation::NotAvailable(error.to_string()),
        },
        None => compat::v1_adapter(),
    };
    if adapter.major() != compat::Major::V2 {
        return Activation::NotAvailable(
            "the resolved v1 (1.18.x) runtime has no session-level Lead to observe".to_string(),
        );
    }
    let contract = match model::lead_contract(&effective.data, level) {
        Ok(contract) => contract,
        Err(error) => return Activation::Failed(error.to_string()),
    };
    let lead = LeadSelection::from_contract(&contract);
    let mut resolved = match build::build_opencode_config_for(effective, level, adapter) {
        Ok(config) => config,
        Err(error) => return Activation::Failed(error.to_string()),
    };
    // The generated plugin is not materialized for a check; removing it keeps
    // the probe honest and leaves no local state behind.
    crate::orchestration::plugin::remove_ocg_plugin_for(&mut resolved, adapter.plugin_key());
    let content = match serde_json::to_string(&resolved) {
        Ok(content) => content,
        Err(error) => {
            return Activation::Failed(format!(
                "cannot serialize the OpenCode config for activation: {error}"
            ))
        }
    };
    let proxy = resolve_proxy(disable_proxy);
    let proxy_env = proxy.child_env();
    match runtime_effective::observe_owned_v2(
        &program,
        &content,
        &[],
        &proxy_env,
        &lead,
        &invocation_dir.to_string_lossy(),
    ) {
        Ok(observed) => Activation::Verified {
            endpoint: observed.identity.endpoint,
            session_id: observed.session_id,
            lead: observed.effective,
        },
        Err(error) => Activation::Failed(error.to_string()),
    }
}

/// Resolve the Configured / Resolved / Effective state for one selected model.
///
/// `content` is the serialized OpenCode config with OCG's generated plugin
/// removed: diagnostics must not materialize local state, and the removed
/// plugin never affects which Lead model the runtime selects. `preflight` is
/// the already-computed catalogue evidence when the caller has it. Either may
/// be absent; the state is then reported as unverified/not observed rather than
/// fabricated.
#[allow(clippy::too_many_arguments)]
fn build_runtime_state(
    effective: &config::Effective,
    level: &str,
    program: Option<&Path>,
    adapter: Option<&dyn RuntimeAdapter>,
    content: Option<&str>,
    proxy_env: &crate::proxy::ChildProxyEnv,
    directory: &Path,
    preflight: Option<&ModelPreflight>,
    observe: bool,
) -> std::result::Result<runtime_effective::RuntimeState, Failure> {
    let contract = model::lead_contract(&effective.data, level).map_err(Failure::Ocg)?;
    let configured = LeadSelection::from_contract(&contract);
    let static_errors = validate::validate(effective);
    let model = runtime_effective::RuntimeState::model_evidence(preflight, &configured);
    let (evidence, identity) = match (observe, program, adapter, content) {
        (true, Some(program), Some(adapter), Some(content))
            if adapter.major() == compat::Major::V2 =>
        {
            match runtime_effective::observe_owned_v2(
                program,
                content,
                &[],
                proxy_env,
                &configured,
                &directory.to_string_lossy(),
            ) {
                Ok(observed) => (
                    runtime_effective::EffectiveEvidence::Observed {
                        session_id: observed.session_id,
                        lead: observed.effective,
                    },
                    Some(observed.identity),
                ),
                Err(error) => (
                    runtime_effective::EffectiveEvidence::Unavailable(error.to_string()),
                    None,
                ),
            }
        }
        (true, _, Some(adapter), _) => (
            runtime_effective::EffectiveEvidence::NotObserved(format!(
                "the resolved {} runtime has no session-level Lead to observe",
                adapter.major().as_str()
            )),
            None,
        ),
        (true, _, _, _) => (
            runtime_effective::EffectiveEvidence::NotObserved(
                "no usable OpenCode runtime was resolved".to_string(),
            ),
            None,
        ),
        (false, _, _, _) => (
            runtime_effective::EffectiveEvidence::NotObserved(
                "effective-state verification was not requested (pass --effective)".to_string(),
            ),
            None,
        ),
    };
    Ok(runtime_effective::RuntimeState {
        level: level.to_string(),
        configured,
        static_errors,
        model,
        effective: evidence,
        identity,
    })
}

fn runtime_state_token(status: &str) -> &'static str {
    match status {
        "ok" => "PASS",
        "warn" => "WARN",
        "error" => "FAIL",
        _ => "INFO",
    }
}

/// Render one resolved [`runtime_effective::RuntimeState`] as concise lines.
fn print_runtime_state_lines(state: &runtime_effective::RuntimeState) {
    println!();
    println!("Runtime state (selected model {}):", state.level);
    let (status, detail) = configured_line(state);
    println!(
        "  {:<16} [{}] {detail}",
        "configured",
        runtime_state_token(status)
    );
    if state.static_errors.is_empty() {
        println!("  {:<16} [PASS] accepted by OCG validation", "resolved");
    } else {
        println!(
            "  {:<16} [FAIL] {}",
            "resolved",
            state.static_errors.join("; ")
        );
    }
    let (status, detail) = state.model.describe(
        &state.configured.full_model_id(),
        state.configured.variant.as_deref(),
    );
    println!(
        "  {:<16} [{}] {detail}",
        "model",
        runtime_state_token(status)
    );
    let (status, detail) = state.effective_line();
    println!(
        "  {:<16} [{}] {detail}",
        "effective",
        runtime_state_token(status)
    );
    match &state.identity {
        Some(identity) => println!("  {:<16} {}", "runtime", identity.describe()),
        None => println!(
            "  {:<16} none — no OCG-owned runtime was resolved",
            "runtime"
        ),
    }
}

fn configured_line(state: &runtime_effective::RuntimeState) -> (&'static str, String) {
    let variant = state
        .configured
        .variant
        .as_deref()
        .unwrap_or("provider-default");
    (
        "ok",
        format!(
            "{} on {} (variant {variant})",
            state.configured.agent,
            state.configured.full_model_id()
        ),
    )
}

/// `ocg status --effective`: resolve the runtime and print the three states.
fn print_runtime_state(
    effective: &config::Effective,
    project_root: &Path,
    invocation_dir: &Path,
    level: &str,
    env: &Env,
    disable_proxy: bool,
    observe: bool,
) -> std::result::Result<(), Failure> {
    let proxy = resolve_proxy(disable_proxy);
    let proxy_env = proxy.child_env();
    let clock = SystemClock;
    let process = SystemProcessHost;
    let manager = runtime_manager(project_root, effective, env, &NoHttp, &clock, &process)
        .map_err(Failure::Ocg)?;
    let report = manager.resolve_for_report();
    for warning in &report.warnings {
        eprintln!("ocg: warning: {warning}");
    }
    let adapter = match report.version.as_ref() {
        Some(version) => match compat::classify(version.clone()) {
            Ok(detected) => Some(compat::adapter_for(&detected)),
            Err(error) => {
                eprintln!("ocg: warning: {error}");
                None
            }
        },
        None => {
            eprintln!(
                "ocg: warning: could not determine the OpenCode version; assuming the v1 (1.18.x) contract"
            );
            Some(compat::v1_adapter())
        }
    };
    let program = report.path.clone();
    let content = match (program.as_deref(), adapter) {
        (Some(_), Some(adapter)) => {
            let mut resolved = build::build_opencode_config_for(effective, level, adapter)
                .map_err(Failure::Ocg)?;
            crate::orchestration::plugin::remove_ocg_plugin_for(
                &mut resolved,
                adapter.plugin_key(),
            );
            Some(serde_json::to_string(&resolved).map_err(|error| {
                Failure::Ocg(OcgError::config(format!(
                    "cannot serialize the OpenCode config for the runtime state report: {error}"
                )))
            })?)
        }
        _ => None,
    };
    let preflight = match (program.as_deref(), content.as_deref()) {
        (Some(program), Some(content)) => Some(
            if adapter
                .map(|adapter| adapter.major() == compat::Major::V2)
                .unwrap_or(false)
            {
                match probe_v2_owned_catalogue(
                    &effective.data,
                    program,
                    content,
                    invocation_dir,
                    &proxy_env,
                ) {
                    Some(report) => report,
                    None => ModelPreflight::Unavailable {
                        reason: "runtime model check could not be completed (private OpenCode V2 server for the catalogue probe failed to start, connect, or report a catalogue)"
                            .to_string(),
                    },
                }
            } else {
                crate::preflight::probe(
                    &effective.data,
                    Some(level),
                    &process,
                    program,
                    invocation_dir,
                    content,
                    &proxy_env,
                )
                .map_err(Failure::Ocg)?
            },
        ),
        _ => None,
    };
    let state = build_runtime_state(
        effective,
        level,
        program.as_deref(),
        adapter,
        content.as_deref(),
        &proxy_env,
        invocation_dir,
        preflight.as_ref(),
        observe,
    )?;
    print_runtime_state_lines(&state);
    Ok(())
}
