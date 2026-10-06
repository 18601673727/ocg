//! OCG command-line control surface.
//!
//! Commands in this module operate on OCG configuration and canonical
//! orchestration state. Provider execution is admitted through the native
//! Project / Job / Attempt / Call model; no external runtime is launched.

use crate::clock::Clock;
use crate::config;
use crate::context::{ContextConfig, ContextEngine};
use crate::defaults::{load_defaults, OcgSource};
use crate::error::OcgError;
use crate::observability;
use crate::orchestration::checkpoint::Phase;
use crate::project;
use crate::report;
use crate::validate;
use serde_json::{json, Value};
use std::ffi::OsString;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Default, Clone)]
pub struct Env {
    pub home: Option<PathBuf>,
    pub user_config: Option<PathBuf>,
    pub trace: Option<PathBuf>,
    pub xdg_config_home: Option<PathBuf>,
    pub home_dir: Option<PathBuf>,
    pub telemetry: Option<String>,
    pub orchestration: Option<String>,
}

impl fmt::Debug for Env {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Env")
            .field("home", &self.home)
            .field("user_config", &self.user_config)
            .field("trace", &self.trace)
            .field("xdg_config_home", &self.xdg_config_home)
            .field("home_dir", &self.home_dir)
            .field("telemetry", &self.telemetry)
            .field("orchestration", &self.orchestration)
            .finish()
    }
}

impl Env {
    pub fn from_process() -> Self {
        Self {
            home: env_path("OCG_HOME"),
            user_config: env_path("OCG_USER_CONFIG"),
            trace: env_path("OCG_TRACE"),
            xdg_config_home: env_path("XDG_CONFIG_HOME"),
            home_dir: env_path("HOME").or_else(|| env_path("USERPROFILE")),
            telemetry: env_string("OCG_TELEMETRY"),
            orchestration: env_string("OCG_ORCHESTRATION"),
        }
    }
}

fn env_path(name: &str) -> Option<PathBuf> {
    std::env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn env_string(name: &str) -> Option<String> {
    env_path(name).and_then(|value| value.into_os_string().into_string().ok())
}

#[derive(Debug)]
pub struct UsageError(pub String);

#[derive(Debug)]
pub enum Command {
    Launch,
    Status,
    Routing,
    Config(Vec<OsString>),
    Auth(Vec<OsString>),
    Validate,
    Layers,
    Init,
    Trace(Option<String>),
    Context(Vec<OsString>),
    Cache(Vec<OsString>),
    Stats(Vec<OsString>),
    Verify(Vec<OsString>),
    Tools(Vec<OsString>),
    Checkpoint(Vec<OsString>),
    Reconcile(Vec<OsString>),
    Resources(Vec<OsString>),
    Budget(Vec<OsString>),
    Serve(Vec<OsString>),
    Work(Vec<OsString>),
    Health(Vec<OsString>),
    Version,
    Doctor,
    Help,
}

#[derive(Debug)]
pub struct Cli {
    pub model_choice: Option<String>,
    pub project: Option<PathBuf>,
    pub pretty: bool,
    pub user_config: Option<PathBuf>,
    pub disable_proxy: bool,
    pub command: Command,
}

pub fn parse<I>(args: I) -> Result<Cli, UsageError>
where
    I: IntoIterator<Item = OsString>,
{
    let args: Vec<OsString> = args.into_iter().collect();
    let mut model_choice = None;
    let mut project = None;
    let mut pretty = false;
    let mut user_config = None;
    let mut disable_proxy = false;
    let mut command = None;
    let mut rest = Vec::new();
    let mut event = None;
    let mut index = 0;
    while index < args.len() {
        let text = args[index].to_string_lossy().into_owned();
        if let Some(value) = text.strip_prefix("--model=") {
            model_choice = Some(value.to_string());
        } else if text == "--model" {
            model_choice = Some(next_value(&args, &mut index, "--model")?);
        } else if let Some(value) = text
            .strip_prefix("--project=")
            .or_else(|| text.strip_prefix("--cwd="))
        {
            project = Some(PathBuf::from(value));
        } else if text == "--project" || text == "--cwd" {
            project = Some(PathBuf::from(next_value(&args, &mut index, &text)?));
        } else if let Some(value) = text.strip_prefix("--user-config=") {
            user_config = Some(PathBuf::from(value));
        } else if text == "--user-config" {
            user_config = Some(PathBuf::from(next_value(
                &args,
                &mut index,
                "--user-config",
            )?));
        } else if text == "--pretty" {
            pretty = true;
        } else if text == "--disable-proxy" {
            disable_proxy = true;
        } else if let Some(value) = text.strip_prefix("--event=") {
            event = Some(value.to_string());
        } else if text == "--event" {
            event = Some(next_value(&args, &mut index, "--event")?);
        } else if text == "--help" || text == "-h" {
            command = Some("help".to_string());
            break;
        } else if text == "--version" {
            command = Some("version".to_string());
            break;
        } else if text.starts_with('-') && command.is_none() {
            return Err(UsageError(format!("unknown option: {text}")));
        } else if command.is_none() {
            command = Some(text);
        } else {
            rest.push(args[index].clone());
        }
        index += 1;
    }
    let command = match command.as_deref() {
        None => Command::Launch,
        Some("help") => Command::Help,
        Some("version") => Command::Version,
        Some("status") => Command::Status,
        Some("routing") | Some("routes") => Command::Routing,
        Some("config") => Command::Config(rest),
        Some("auth") => Command::Auth(rest),
        Some("validate") => Command::Validate,
        Some("layers") => Command::Layers,
        Some("init") => Command::Init,
        Some("trace") => Command::Trace(event),
        Some("context") => Command::Context(rest),
        Some("cache") => Command::Cache(rest),
        Some("stats") => Command::Stats(rest),
        Some("verify") => Command::Verify(rest),
        Some("tools") => Command::Tools(rest),
        Some("checkpoint") => Command::Checkpoint(rest),
        Some("reconcile") => Command::Reconcile(rest),
        Some("resources") => Command::Resources(rest),
        Some("budget") => Command::Budget(rest),
        Some("serve") => Command::Serve(rest),
        Some("work") => Command::Work(rest),
        Some("health") => Command::Health(rest),
        Some("doctor") => Command::Doctor,
        Some(other) => return Err(UsageError(format!("unknown command: {other}"))),
    };
    Ok(Cli {
        model_choice,
        project,
        pretty,
        user_config,
        disable_proxy,
        command,
    })
}

fn next_value(args: &[OsString], index: &mut usize, name: &str) -> Result<String, UsageError> {
    *index += 1;
    args.get(*index)
        .map(|value| value.to_string_lossy().into_owned())
        .ok_or_else(|| UsageError(format!("{name} needs a value")))
}

fn usage() -> &'static str {
    "OCG - native project orchestration and control plane\n\nUsage: ocg [--project DIR] [command]\n\nCommands:\n  (none)       open the OCG control surface\n  status       show OCG configuration\n  routing      show configured model routing\n  config       inspect or edit the OCG Profile\n  auth         manage OCG credentials\n  validate     validate configuration\n  context      build repository context\n  verify       run configured verification\n  tools        show capability policy\n  checkpoint   inspect or save checkpoints\n  reconcile    recover durable dispatch intents\n  resources    inspect native resources\n  budget       inspect the Project budget\n  serve        run the OCG control server\n  work         operate canonical Jobs and Calls\n  health       probe or read Health Probe evidence for a Provider/Model/Effort\n  version      report the OCG version\n  doctor       run native diagnostics\n  init         create a global OCG Profile\n  help         show this help\n"
}

#[derive(Debug)]
enum Failure {
    Usage(String),
    Ocg(OcgError),
}

impl From<OcgError> for Failure {
    fn from(error: OcgError) -> Self {
        Self::Ocg(error)
    }
}

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

fn run_inner(args: impl Iterator<Item = OsString>) -> Result<i32, Failure> {
    let cli = parse(args).map_err(|error| Failure::Usage(error.0))?;
    if matches!(cli.command, Command::Help) {
        print!("{}", usage());
        return Ok(0);
    }
    if matches!(cli.command, Command::Version) {
        println!("OCG {VERSION}");
        return Ok(0);
    }
    let env = Env::from_process();
    let root = cli.project.clone().unwrap_or(
        std::env::current_dir()
            .map_err(|error| OcgError::io("cannot determine current directory", error))?,
    );
    if !root.is_dir() {
        return Err(Failure::Usage(format!(
            "project is not a directory: {}",
            root.display()
        )));
    }
    let boundary = project::resolve(&root);
    let project_root = boundary.root().to_path_buf();
    let (source, home) = match env.home.clone() {
        Some(path) => (OcgSource::Dir(path.clone()), Some(path)),
        None => (OcgSource::Embedded, None),
    };
    let defaults = load_defaults(&source)?;
    let user_path = config::user_config_path(
        cli.user_config.as_deref(),
        env.user_config.as_deref(),
        env.xdg_config_home.as_deref(),
        env.home_dir.as_deref(),
    );
    if matches!(cli.command, Command::Init) {
        return init_command(&user_path);
    }
    let mut effective = config::build_effective(
        defaults.clone(),
        home.clone(),
        &project_root,
        &user_path,
        &user_path,
        env.home_dir.clone(),
    )?;
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
    match cli.command {
        Command::Launch => crate::pwa::run(
            &project_root,
            &user_path,
            user_path.is_file(),
            cli.disable_proxy,
        )
        .map(|_| 0)
        .map_err(Into::into),
        Command::Status => {
            validate::require_valid(&effective)?;
            println!("{}", report::status_text(&effective, &level)?);
            Ok(0)
        }
        Command::Routing => {
            validate::require_valid(&effective)?;
            println!("{}", report::routing_text(&effective)?);
            Ok(0)
        }
        Command::Config(args) => native_config(&args, &user_path, &project_root, &env),
        Command::Auth(args) => auth_command(&args),
        Command::Validate => validate_command(&effective),
        Command::Layers => {
            println!("{}", report::layers_text(&effective, env.trace.as_deref()));
            Ok(0)
        }
        Command::Trace(event) => {
            let event = event.as_deref().unwrap_or("command");
            if let Some(path) =
                observability::record_event(&effective, event, &level, env.trace.as_deref())
            {
                println!("{}", path.display());
            }
            Ok(0)
        }
        Command::Context(args) => context_command(&effective, &project_root, &args, cli.pretty),
        Command::Cache(args) => native_cache(&effective, &project_root, &args),
        Command::Stats(args) => native_stats(&effective, &project_root, &args, cli.pretty),
        Command::Verify(args) => native_verify(&effective, &project_root, &args, cli.pretty),
        Command::Tools(args) => native_tools(&effective, &args, cli.pretty),
        Command::Checkpoint(args) => {
            native_checkpoint(&effective, &project_root, &args, cli.pretty)
        }
        Command::Reconcile(args) => reconcile_command(&project_root, &args, cli.pretty),
        Command::Resources(args) => native_resources(&project_root, &args, cli.pretty),
        Command::Budget(args) => native_budget(&effective, &project_root, &args, cli.pretty),
        Command::Serve(args) => serve_command(
            &project_root,
            &user_path,
            &args,
            cli.pretty,
            cli.disable_proxy,
        ),
        Command::Work(args) => work_command(&project_root, &args, cli.pretty),
        Command::Health(args) => health_command(&project_root, &user_path, &args, cli.pretty),
        Command::Doctor => doctor_command(&effective, &project_root),
        Command::Help | Command::Version | Command::Init => unreachable!(),
    }
}

fn print_json(value: &Value, pretty: bool) -> Result<(), Failure> {
    let text = if pretty {
        serde_json::to_string_pretty(value)
    } else {
        serde_json::to_string(value)
    }
    .map_err(|error| OcgError::config(format!("cannot serialize output: {error}")))?;
    println!("{text}");
    Ok(())
}

fn init_command(path: &Path) -> Result<i32, Failure> {
    if path.exists() {
        println!("global config already exists: {}", path.display());
        return Ok(0);
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| OcgError::io("cannot create config directory", error))?;
    }
    crate::profile::persist_new(path, &crate::profile::Profile::new())?;
    println!("created {}", path.display());
    Ok(0)
}

fn validate_command(effective: &config::Effective) -> Result<i32, Failure> {
    let errors = validate::validate(effective);
    if errors.is_empty() {
        println!("configuration is structurally valid");
        Ok(0)
    } else {
        for error in errors {
            eprintln!("  - {error}");
        }
        Ok(1)
    }
}

fn auth_command(args: &[OsString]) -> Result<i32, Failure> {
    let vault = crate::vault::Vault::user_global()?;
    let words: Vec<String> = args
        .iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    match words.as_slice() {
        [action] if action == "list" => {
            for name in vault.list()? {
                println!("{name}");
            }
        }
        [action, name] if action == "set" => {
            let value = rpassword::prompt_password(format!("Credential for {name}: "))
                .map_err(|_| OcgError::config("cannot read credential input"))?;
            vault.set(name, &value)?;
        }
        [action, name] if action == "remove" => {
            if vault.remove(name)? {
                println!("removed credential {name}");
            }
        }
        _ => {
            return Err(Failure::Usage(
                "usage: ocg auth list | set ENV_NAME | remove ENV_NAME".into(),
            ))
        }
    }
    Ok(0)
}

fn context_command(
    effective: &config::Effective,
    root: &Path,
    args: &[OsString],
    pretty: bool,
) -> Result<i32, Failure> {
    let context = ContextConfig::from_config(&effective.data)?;
    if !context.enabled {
        println!("context engine is disabled");
        return Ok(0);
    }
    let git = crate::process::SystemGitHost;
    let clock = crate::clock::SystemClock;
    let engine = ContextEngine::new(root, context, &git, &clock);
    let words: Vec<String> = args
        .iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    if words.first().map(String::as_str) == Some("symbols") {
        return print_json(
            &json!({"symbols": engine.search_symbols(&words[1..].join(" "), 100)?}),
            pretty,
        )
        .map(|_| 0);
    }
    let outcome = engine.plan(&words.join(" "), None)?;
    print_json(
        &serde_json::to_value(outcome.plan)
            .map_err(|error| OcgError::config(format!("cannot serialize context plan: {error}")))?,
        pretty,
    )
    .map(|_| 0)
}

fn native_cache(
    effective: &config::Effective,
    root: &Path,
    args: &[OsString],
) -> Result<i32, Failure> {
    if args.len() > 1 {
        return Err(Failure::Usage(
            "cache takes at most one action: stats or clean".into(),
        ));
    }
    let context_config = ContextConfig::from_config(&effective.data)?;
    let git = crate::process::SystemGitHost;
    let clock = crate::clock::SystemClock;
    let engine = ContextEngine::new(root, context_config, &git, &clock);
    match args.first().map(|value| value.to_string_lossy()) {
        None => {
            let stats = engine.cache_stats();
            println!("context cache: {}", stats.dir);
            println!(
                "  entries: {} bytes: {} corrupt: {}",
                stats.entries, stats.bytes, stats.corrupt
            );
        }
        Some(action) if action == "stats" => {
            let stats = engine.cache_stats();
            println!("context cache: {}", stats.dir);
            println!(
                "  entries: {} bytes: {} corrupt: {}",
                stats.entries, stats.bytes, stats.corrupt
            );
            if let Some(index) = engine.load_index() {
                println!(
                    "index: {} files, {} symbols",
                    index.metrics.files, index.metrics.symbols
                );
            } else {
                println!("index: not built");
            }
        }
        Some(action) if action == "clean" => {
            let report = engine.cache_clean()?;
            println!(
                "removed {} entries ({} bytes) from {}",
                report.removed_entries, report.removed_bytes, report.dir
            );
        }
        Some(action) => return Err(Failure::Usage(format!("unknown cache action: {action}"))),
    }
    Ok(0)
}

fn reconcile_command(root: &Path, _args: &[OsString], pretty: bool) -> Result<i32, Failure> {
    let mut repository = crate::orchestration::domain::DomainRepository::open(root)?;
    print_json(&repository.reconcile_dispatches()?, pretty).map(|_| 0)
}

fn serve_command(
    root: &Path,
    profile: &Path,
    args: &[OsString],
    pretty: bool,
    disable_proxy: bool,
) -> Result<i32, Failure> {
    let address = args
        .first()
        .map(|value| value.to_string_lossy().into_owned())
        .unwrap_or_else(|| "127.0.0.1:0".into());
    let server = crate::control_server::ControlServer::bind_with_profile(
        &address,
        root,
        profile,
        crate::control_server::ServerConfig::default(),
        disable_proxy,
    )?;
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let signal_stop = std::sync::Arc::clone(&stop);
    ctrlc::set_handler(move || {
        signal_stop.store(true, std::sync::atomic::Ordering::SeqCst);
    })
    .map_err(|error| {
        OcgError::config(format!(
            "cannot install control server shutdown handler: {error}"
        ))
    })?;
    if pretty {
        print_json(&json!({"listening": server.base_url()}), true)?;
    } else {
        println!("listening on {}", server.base_url());
    }
    server.serve(stop)?;
    Ok(0)
}

fn work_command(root: &Path, args: &[OsString], pretty: bool) -> Result<i32, Failure> {
    let words: Vec<String> = args
        .iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    let subcommand = words
        .first()
        .ok_or_else(|| OcgError::config("ocg work requires a subcommand"))?;
    let option = |name: &str| -> Option<String> {
        let flag = format!("--{name}");
        let prefix = format!("{flag}=");
        words
            .windows(2)
            .find_map(|pair| {
                if pair[0] == flag {
                    Some(pair[1].clone())
                } else {
                    None
                }
            })
            .or_else(|| {
                words
                    .iter()
                    .find_map(|word| word.strip_prefix(&prefix).map(str::to_string))
            })
    };
    let mut repository = crate::orchestration::domain::DomainRepository::open(root)?;
    let project = repository.ensure_project(root)?;
    let required = |name: &str| -> Result<String, Failure> {
        option(name).ok_or_else(|| Failure::Usage(format!("--{name} is required")))
    };
    let print = |value: Value| {
        print_json(&value, pretty)?;
        Ok(0)
    };
    match subcommand.as_str() {
        "admit" | "create" => {
            let binding = option("session")
                .or_else(|| option("binding"))
                .unwrap_or_else(|| "cli".into());
            let spec = crate::orchestration::domain::JobSpec {
                objective: Some(option("objective").unwrap_or_default()),
                ..Default::default()
            };
            let admission = repository.admit_job(
                project,
                &binding,
                spec,
                &option("agent").unwrap_or_else(|| "lead".into()),
            )?;
            print(
                json!({"project_id": admission.project.id, "job_id": admission.job.id, "attempt_id": admission.attempt.id, "executor_id": admission.executor.id, "generation": admission.attempt.generation}),
            )
        }
        "spawn" => {
            let parent = crate::orchestration::domain::AttemptAuthority {
                job_id: required("job")?,
                attempt_id: required("attempt")?,
                generation: required("generation")?
                    .parse::<u64>()
                    .map_err(|_| Failure::Usage("--generation must be an integer".into()))?,
            };
            let spec = crate::orchestration::domain::JobSpec {
                objective: Some(required("objective")?),
                ..Default::default()
            };
            let mut policy = serde_json::Map::new();
            for dimension in ["join", "cancellation", "failure"] {
                if let Some(value) = option(dimension) {
                    policy.insert(dimension.to_string(), json!(value));
                }
            }
            let policy = serde_json::from_value::<crate::orchestration::domain::ChildPolicy>(
                Value::Object(policy),
            )
            .map_err(|error| OcgError::config(error.to_string()))?;
            let dependencies = option("depends-on")
                .map(|raw| {
                    raw.split(',')
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(str::to_string)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let prerequisites = dependencies.iter().map(String::as_str).collect::<Vec<_>>();
            let spawn_key = required("spawn-key")?;
            let executor_kind = option("agent").unwrap_or_else(|| "worker".into());
            let (job, duplicate) =
                repository.spawn_child(crate::orchestration::domain::SpawnChildRequest {
                    parent_authority: &parent,
                    spawn_key: &spawn_key,
                    spec,
                    prerequisite_job_ids: &prerequisites,
                    executor_kind: &executor_kind,
                    policy: &policy,
                    call_id: None,
                })?;
            print(
                json!({"project_id": job.project_id, "job_id": job.id, "duplicate": duplicate, "job": job}),
            )
        }
        "plan" | "child" => {
            let session = required("session")?;
            let parent = repository
                .authority_for_binding(&project.id, &session)?
                .ok_or_else(|| {
                    Failure::Ocg(OcgError::config("session has no canonical authority"))
                })?;
            let spec = crate::orchestration::domain::JobSpec {
                objective: Some(option("objective").unwrap_or_default()),
                ..Default::default()
            };
            let dependencies = option("depends-on")
                .map(|raw| {
                    raw.split(',')
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(str::to_string)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let dependency_refs = dependencies
                .iter()
                .map(|value| value.as_str())
                .collect::<Vec<_>>();
            let job = repository.create_child_job(&parent, spec, &dependency_refs)?;
            print(json!({"project_id": job.project_id, "job_id": job.id, "job": job}))
        }
        "dispatch" => {
            let job_id = required("job")?;
            let (attempt, executor) = repository
                .dispatch_job(&job_id, &option("agent").unwrap_or_else(|| "worker".into()))?;
            print(
                json!({"job_id": job_id, "attempt_id": attempt.id, "executor_id": executor.id, "generation": attempt.generation}),
            )
        }
        "replace" => {
            let job_id = required("job")?;
            let admission = repository.replace_attempt_checked(
                &job_id,
                &option("agent").unwrap_or_else(|| "worker".into()),
                Some(&required("attempt")?),
            )?;
            print(
                json!({"job_id": admission.job.id, "attempt_id": admission.attempt.id, "executor_id": admission.executor.id, "generation": admission.attempt.generation}),
            )
        }
        "finish" | "deliver" => {
            let attempt_id = required("attempt")?;
            let outcome = option("outcome").unwrap_or_else(|| "completed".into());
            if !matches!(outcome.as_str(), "completed" | "failed") {
                return Err(Failure::Usage(
                    "outcome must be completed or failed; cancellation requires confirmed stop"
                        .into(),
                ));
            }
            if let Some(call_id) = option("call") {
                let generation = required("generation")?
                    .parse::<u64>()
                    .map_err(|_| Failure::Usage("--generation must be an integer".into()))?;
                let result = option("result").unwrap_or_else(|| "null".into());
                let witness = repository.witness_for_call(&call_id, &attempt_id, generation)?;
                let disposition =
                    repository.deliver_result(&witness, &result, outcome == "completed")?;
                print(
                    json!({"attempt_id": attempt_id, "call_id": call_id, "disposition": disposition, "applied": disposition == "authoritative"}),
                )
            } else if subcommand == "deliver" {
                Err(Failure::Usage(
                    "deliver requires --call and --generation".into(),
                ))
            } else {
                repository.finish_attempt(&attempt_id, outcome == "completed")?;
                print(json!({"attempt_id": attempt_id, "outcome": outcome, "applied": true}))
            }
        }
        "inspect" => print(repository.inspect_job(&required("job")?)?),
        "ready" => {
            print(json!({"project_id": project.id, "ready": repository.ready_jobs(&project.id)?}))
        }
        "recover" | "reconcile" => print(repository.reconcile_dispatches()?),
        "status" => print(json!({"project": project, "jobs": repository.jobs(&project.id)?})),
        "set-config" => {
            let job_id = required("job")?;
            let value: Value = serde_json::from_str(&required("json")?)
                .map_err(|error| Failure::Usage(error.to_string()))?;
            let revision = repository.set_job_configuration(&job_id, &value)?;
            print(json!({"job_id": job_id, "revision": revision, "configuration": value}))
        }
        "config" => {
            let job_id = required("job")?;
            print(
                json!({"job_id": job_id, "configuration": repository.job_configuration(&job_id)?}),
            )
        }
        _ => Err(Failure::Usage(format!(
            "unknown canonical work subcommand: {subcommand}"
        ))),
    }
}

/// Block until one probe Job reaches a canonical terminal state.
///
/// This waits on the single Job this command just created. It is a read of
/// existing lifecycle evidence, not a second execution path and not a recurring
/// schedule. The bound is generous because a reasoning model can legitimately
/// take a while to answer a one-word question; exceeding it reports the Job's
/// current state rather than inventing a verdict.
fn wait_for_terminal_probe(
    service: &crate::orchestration::canonical_control::CanonicalControlService,
    job_id: &str,
) -> Result<(), Failure> {
    const DEADLINE: std::time::Duration = std::time::Duration::from_secs(180);
    let start = std::time::Instant::now();
    loop {
        // Read the Job row directly rather than the snapshot: the snapshot also
        // replays the whole journal, which grows without bound, so a long wait
        // would get steadily slower for no new information.
        let state = crate::orchestration::domain::DomainRepository::open(service.root())?
            .job(job_id)?
            .map(|job| job.state);
        let terminal = matches!(
            state,
            Some(
                crate::orchestration::domain::JobState::Completed
                    | crate::orchestration::domain::JobState::Failed
                    | crate::orchestration::domain::JobState::Cancelled
                    | crate::orchestration::domain::JobState::Unknown
                    | crate::orchestration::domain::JobState::Orphaned
            )
        );
        if terminal || start.elapsed() >= DEADLINE {
            return Ok(());
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
}

/// Probe a Provider/Model/Effort tuple, or read the evidence a past probe left.
///
/// `ocg health probe <provider> <model> [--effort E]` launches one Health Probe
/// Job, waits for it to settle, and prints the verdict. `ocg health show
/// <provider> <model> [--effort E]` reads the latest canonical evidence without
/// sending anything.
///
/// The provider and model are positional. `--model` is already a global
/// routing flag parsed before the subcommand is seen, so a same-named
/// subcommand flag would silently never arrive.
///
/// Neither subcommand schedules anything. A probe runs because it was asked for.
fn health_command(
    root: &Path,
    user_path: &Path,
    args: &[OsString],
    pretty: bool,
) -> Result<i32, Failure> {
    let words: Vec<String> = args
        .iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    let subcommand = words
        .first()
        .ok_or_else(|| Failure::Usage("ocg health requires a subcommand: probe or show".into()))?;
    let option = |name: &str| -> Option<String> {
        let flag = format!("--{name}");
        let prefix = format!("{flag}=");
        words
            .windows(2)
            .find_map(|pair| {
                if pair[0] == flag {
                    Some(pair[1].clone())
                } else {
                    None
                }
            })
            .or_else(|| {
                words
                    .iter()
                    .find_map(|word| word.strip_prefix(&prefix).map(str::to_owned))
            })
    };
    let repository = crate::orchestration::domain::DomainRepository::open(root)?;
    let project = repository.ensure_project(root)?;
    // Positional arguments are the provider and the model. A value-taking flag's
    // argument is not positional, so skip it rather than mistaking a flag value
    // for the model name.
    let takes_value = ["--effort", "--command-id"];
    let mut skip = false;
    let mut positional: Vec<&String> = Vec::new();
    for word in &words[1..] {
        if takes_value.contains(&word.as_str()) {
            skip = true;
            continue;
        }
        if skip {
            skip = false;
            continue;
        }
        if !word.starts_with('-') {
            positional.push(word);
        }
    }
    const HEALTH_USAGE: &str = "usage: ocg health probe|show <provider> <model> [--effort E]";
    if positional.len() != 2 {
        return Err(Failure::Usage(HEALTH_USAGE.into()));
    }
    let target = crate::contracts::HealthProbeTarget {
        provider: positional[0].clone(),
        model: positional[1].clone(),
        effort: option("effort"),
    };
    match subcommand.as_str() {
        "show" => {
            let observation = repository
                .latest_health_probe(&project.id, &target.intent())?
                .map(crate::contracts::HealthProbeObservation::from);
            print_json(
                &json!({
                    "project_id": project.id,
                    "target": target,
                    "observation": observation,
                }),
                pretty,
            )?;
            Ok(0)
        }
        "probe" => {
            // A probe executes through the ordinary canonical provider worker,
            // so this borrows the same registry-backed runtime the control
            // server uses rather than inventing a second execution host. The
            // registry is dropped at the end of this process, which is what
            // makes the probe one-shot: nothing reschedules it.
            let service =
                crate::orchestration::canonical_control::CanonicalControlService::open_process(
                    root, user_path,
                )?;
            let selection = crate::proxy::resolve(
                true,
                &crate::proxy::SystemProxyEnv,
                &crate::process::SystemStaticProxy,
            );
            let transport = Arc::new(crate::http::NativeHttp::with_policy(
                selection.plan(),
                None,
            )?);
            let registry = Arc::new(
                crate::orchestration::execution_runtime::ProjectRuntimeRegistry::new(
                    transport,
                    crate::native_tools::PermissionPolicy::default(),
                    16,
                    16,
                ),
            );
            let service = service.with_runtime_registry(registry.clone());
            let response = service.launch_health_probe(
                crate::contracts::HealthProbeRequest {
                    command_id: option("command-id")
                        .unwrap_or_else(|| format!("health-{}", uuid::Uuid::now_v7())),
                    project_id: project.id.clone(),
                    target,
                },
                crate::clock::Clock::now_unix(&crate::clock::SystemClock),
            )?;
            // Wait for the probe Job to reach a terminal state so the CLI can
            // report the verdict rather than only that a Job exists. This is a
            // bounded wait on one Job this command created, not a scheduler.
            if let Some(job_id) = response.job_id.clone() {
                wait_for_terminal_probe(&service, &job_id)?;
            }
            let observation = repository
                .latest_health_probe(&project.id, &response.target.intent())?
                .map(crate::contracts::HealthProbeObservation::from);
            print_json(
                &json!({
                    "launch": response,
                    "observation": observation,
                }),
                pretty,
            )?;
            registry.shutdown()?;
            Ok(0)
        }
        other => Err(Failure::Usage(format!(
            "unknown health subcommand: {other}; expected probe or show"
        ))),
    }
}

fn native_stats(
    effective: &config::Effective,
    root: &Path,
    args: &[OsString],
    pretty: bool,
) -> Result<i32, Failure> {
    if !args.is_empty() {
        return Err(Failure::Usage("stats takes no arguments".into()));
    }
    let telemetry_config = crate::telemetry::TelemetryConfig::from_config(&effective.data)
        .unwrap_or_else(|_| crate::telemetry::TelemetryConfig::disabled());
    let stats = crate::telemetry::TelemetryStats::collect(&crate::telemetry::TelemetryStore::new(
        root,
        telemetry_config,
    ));
    if pretty {
        let value = serde_json::to_value(stats).map_err(|error| {
            OcgError::config(format!("cannot serialize telemetry stats: {error}"))
        })?;
        print_json(&value, true)?;
    } else {
        print!("{}", stats.render());
    }
    Ok(0)
}
fn native_verify(
    effective: &config::Effective,
    root: &Path,
    args: &[OsString],
    pretty: bool,
) -> Result<i32, Failure> {
    if args.len() > 1 {
        return Err(Failure::Usage(
            "verify takes at most one stage (fast, normal or full)".into(),
        ));
    }
    let verify_config = crate::verification::Config::from_config(&effective.data)?;
    let stage = args
        .first()
        .map(|value| value.to_string_lossy().into_owned())
        .unwrap_or_else(|| verify_config.default_stage.clone());
    verify_config.stage(&stage)?;
    let clock = crate::clock::SystemClock;
    let runner = crate::process::SystemCaptureRunner;
    let report =
        crate::verification::runner::execute(&crate::verification::runner::VerifyRequest {
            root,
            config: &verify_config,
            stage,
            runner: &runner,
            clock: &clock,
            test_proposal: None,
        })?;
    if pretty {
        let value = serde_json::to_value(&report).map_err(|error| {
            OcgError::config(format!("cannot serialize verification report: {error}"))
        })?;
        print_json(&value, true)?;
    } else {
        println!(
            "verification stage: {} ({})",
            report.stage,
            report.overall().as_str()
        );
        for result in &report.results {
            println!(
                "  [{}] {} ({})",
                if result.success { "ok" } else { "fail" },
                result.display(),
                result.duration_ms
            ); // A failing command's feedback is what the stage exists to report; for a
               // Rust compile this is the structured diagnostic delta, which is
               // already bounded and is not the re-sent compiler output. Print
               // indented so it reads as belonging to the command above it.
            if !result.success {
                for line in result.output.render().lines() {
                    println!("      {line}");
                }
            }
        }
        for note in &report.notes {
            println!("note: {note}");
        }
    }
    Ok(if report.failed() { 1 } else { 0 })
}
fn native_tools(
    effective: &config::Effective,
    args: &[OsString],
    pretty: bool,
) -> Result<i32, Failure> {
    let task = args
        .iter()
        .map(|arg| arg.to_string_lossy())
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string();
    if task.is_empty() {
        return Err(Failure::Usage("tools needs a task description".into()));
    }
    let capabilities = crate::capabilities::CapabilityConfig::from_config(&effective.data)?;
    let plan = crate::capabilities::CapabilityPlan::plan_config(
        &task,
        &crate::capabilities::CapabilityEvidence::default(),
        &capabilities.custom,
        capabilities.enabled,
    );
    if pretty {
        let value = serde_json::to_value(plan).map_err(|error| {
            OcgError::config(format!("cannot serialize capability plan: {error}"))
        })?;
        print_json(&value, true)?;
    } else {
        print!("{}", plan.render());
    }
    Ok(0)
}
fn native_checkpoint(
    effective: &config::Effective,
    root: &Path,
    args: &[OsString],
    pretty: bool,
) -> Result<i32, Failure> {
    let words = args
        .iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    let git = crate::process::SystemGitHost;
    match words.first().map(String::as_str) {
        None | Some("list") => {
            if words.len() > 1 {
                return Err(Failure::Usage("checkpoint list takes no arguments".into()));
            }
            let (items, corrupt) = crate::orchestration::checkpoint::list(root);
            let value = json!({"checkpoints": items, "corrupt": corrupt});
            print_json(&value, pretty)?;
        }
        Some("show") if words.len() == 2 => {
            let loaded = crate::orchestration::checkpoint::load(root, &words[1], &git)?;
            let value = json!({"checkpoint": loaded.checkpoint, "stale": loaded.staleness.stale, "reasons": loaded.staleness.reasons});
            print_json(&value, pretty)?;
        }
        Some("save") => {
            let mut phase = None;
            let mut task = String::from("checkpoint");
            let mut decisions = Vec::new();
            let mut index = 1;
            while index < words.len() {
                match words[index].as_str() {
                    "--phase" => {
                        index += 1;
                        phase = words.get(index).and_then(|value| Phase::parse(value));
                        if phase.is_none() {
                            return Err(Failure::Usage(
                                "checkpoint save needs a valid --phase".into(),
                            ));
                        }
                    }
                    "--task" => {
                        index += 1;
                        task = words
                            .get(index)
                            .cloned()
                            .ok_or_else(|| Failure::Usage("--task needs a value".into()))?;
                    }
                    "--decision" => {
                        index += 1;
                        decisions.push(
                            words
                                .get(index)
                                .cloned()
                                .ok_or_else(|| Failure::Usage("--decision needs a value".into()))?,
                        );
                    }
                    other => {
                        return Err(Failure::Usage(format!(
                            "unknown checkpoint save option: {other}"
                        )))
                    }
                }
                index += 1;
            }
            let phase =
                phase.ok_or_else(|| Failure::Usage("checkpoint save needs --phase".into()))?;
            let snapshot = crate::context::gitdiff::GitSnapshot::collect(root, &git);
            let fingerprint = crate::context::gitdiff::snapshot_fingerprint(&snapshot);
            let capsule = crate::context::capsule::TaskCapsule::new(&task);
            let decisions = decisions
                .into_iter()
                .map(|decision| crate::context::capsule::Decision {
                    decision,
                    rationale: None,
                    date: None,
                    date_unknown: true,
                })
                .collect();
            let checkpoint = crate::orchestration::checkpoint::Checkpoint::build(
                phase,
                capsule,
                snapshot.state,
                fingerprint,
                None,
                crate::context::freshness::Provenance::default(),
                decisions,
                crate::clock::SystemClock.now_unix(),
            );
            let path = checkpoint.save(root)?;
            print_json(&json!({"id": checkpoint.id, "path": path}), pretty)?;
        }
        Some(other) => {
            return Err(Failure::Usage(format!(
                "unknown checkpoint action: {other}"
            )))
        }
    }
    let _ = effective;
    Ok(0)
}
fn native_resources(root: &Path, _args: &[OsString], pretty: bool) -> Result<i32, Failure> {
    let loaded = crate::resources::load(root);
    print_json(&json!({"exists": loaded.exists, "corrupt": loaded.corrupt, "count": loaded.registry.list().len()}), pretty).map(|_| 0)
}
fn native_budget(
    effective: &config::Effective,
    root: &Path,
    args: &[OsString],
    pretty: bool,
) -> Result<i32, Failure> {
    let mut repository = crate::orchestration::domain::DomainRepository::open(root)?;
    let project = repository.ensure_project(root)?;
    if args.first().is_some_and(|arg| arg == "set") {
        let value = |name: &str| -> Option<String> {
            let flag = format!("--{name}");
            args.iter().enumerate().find_map(|(index, arg)| {
                let word = arg.to_string_lossy();
                if word == flag {
                    args.get(index + 1)
                        .map(|next| next.to_string_lossy().into_owned())
                } else {
                    word.strip_prefix(&format!("{flag}=")).map(str::to_string)
                }
            })
        };
        let project_id = value("project-id").unwrap_or_else(|| project.id.clone());
        let limit = value("limit")
            .ok_or_else(|| Failure::Usage("--limit is required".into()))?
            .parse::<i64>()
            .map_err(|_| Failure::Usage("--limit must be an integer".into()))?;
        let currency =
            value("currency").ok_or_else(|| Failure::Usage("--currency is required".into()))?;
        repository.set_project_budget(
            &project_id,
            crate::orchestration::budget::Money::new(limit, currency),
        )?;
    }
    let config = crate::orchestration::budget::BudgetConfig::from_config(&effective.data)?;
    let budget = repository.project_budget(&project.id)?;
    let value = json!({"configured": config.hard_limit_micros.is_some(), "currency": config.currency, "hard_limit_micros": config.hard_limit_micros, "estimated_operation_cost_micros": config.estimated_operation_cost_micros, "require_quota": config.require_quota, "project_id": project.id, "budget": budget});
    print_json(&value, pretty)?;
    Ok(0)
}
fn doctor_command(effective: &config::Effective, root: &Path) -> Result<i32, Failure> {
    let errors = validate::validate(effective);
    println!("OCG {VERSION}");
    println!("project: {}", root.display());
    println!(
        "configuration: {}",
        if errors.is_empty() {
            "valid"
        } else {
            "invalid"
        }
    );
    Ok(if errors.is_empty() { 0 } else { 1 })
}

fn native_config(
    args: &[OsString],
    user_path: &Path,
    project_root: &Path,
    env: &Env,
) -> Result<i32, Failure> {
    let request = crate::config_command::parse_request(args)?;
    let defaults = load_defaults(&OcgSource::Embedded)?;
    let current = config::build_effective(
        defaults.clone(),
        None,
        project_root,
        user_path,
        user_path,
        env.home_dir.clone(),
    )?;
    let context = crate::config_command::Context {
        defaults,
        ocg_home: None,
        project_root: project_root.to_path_buf(),
        invocation_dir: project_root.to_path_buf(),
        user_path: user_path.to_path_buf(),
        project_path: user_path.to_path_buf(),
        env,
        level: String::new(),
        current,
    };
    let probe = |_effective: &config::Effective, _level: &str| {};
    let activate = |_effective: &config::Effective, _level: &str| {};
    let mut input = std::io::BufReader::new(std::io::stdin());
    let mut output = std::io::stdout();
    crate::config_command::execute(
        &request,
        &context,
        &probe,
        &activate,
        &mut input,
        &mut output,
    )
    .map_err(Failure::Ocg)
}

#[allow(dead_code)]
fn _phase_name(phase: Phase) -> &'static str {
    phase.as_str()
}
