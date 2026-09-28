//! CLI editing of the same user-global Profile used by the PWA and headless path.
//! External runtime documents are comparison candidates, never configuration
//! layers. All mutations are explicit and revision-checked.
use crate::cli::Env;
use crate::config::Effective;
use crate::error::{OcgError, Result};
use crate::preflight::ModelPreflight;
use crate::profile::{Model, ProfileService, Provider};
use crate::runtime::compat::EffectiveLead;
use std::ffi::OsString;
use std::io::{BufRead, Write};
use std::path::PathBuf;

#[derive(Debug)]
pub enum Request {
    Help,
    Show,
    Candidates,
    New,
    Import {
        location: PathBuf,
        sha256: String,
    },
    Provider {
        key: String,
        label: String,
    },
    Model {
        key: String,
        provider: String,
        id: String,
        default: bool,
    },
    RemoveModel(String),
    RemoveProvider(String),
}

pub struct Context<'a> {
    pub defaults: serde_json::Value,
    pub ocg_home: Option<PathBuf>,
    pub project_root: PathBuf,
    pub invocation_dir: PathBuf,
    pub user_path: PathBuf,
    pub project_path: PathBuf,
    pub env: &'a Env,
    pub level: String,
    pub current: Effective,
}

pub type ProbeFn<'a> = dyn Fn(&Effective, &str) -> Option<ModelPreflight> + 'a;
pub type ActivateFn<'a> = dyn Fn(&Effective, &str) -> Activation + 'a;

/// Retained only as a compatibility observation for the existing runtime
/// diagnostics helper. Configuration editing does not activate a runtime.
#[derive(Clone, Debug)]
pub enum Activation {
    Verified {
        endpoint: String,
        session_id: String,
        lead: EffectiveLead,
    },
    Failed(String),
    NotAvailable(String),
}

pub fn parse_request(args: &[OsString]) -> Result<Request> {
    let words: Vec<_> = args
        .iter()
        .map(|part| part.to_string_lossy().into_owned())
        .collect();
    let strings: Vec<_> = words.iter().map(String::as_str).collect();
    let request = match strings.as_slice() {
        [] | ["profile"] => Request::Show,
        ["--help" | "-h"] => Request::Help,
        ["candidates"] => Request::Candidates,
        ["new"] => Request::New,
        ["import", location, sha256] => Request::Import { location: PathBuf::from(location), sha256: (*sha256).to_string() },
        ["provider", "add", key, label] => Request::Provider { key: (*key).into(), label: (*label).into() },
        ["model", "add", key, provider, id] => Request::Model { key: (*key).into(), provider: (*provider).into(), id: (*id).into(), default: false },
        ["model", "add", key, provider, id, "--default"] => Request::Model { key: (*key).into(), provider: (*provider).into(), id: (*id).into(), default: true },
        ["model", "remove", key] => Request::RemoveModel((*key).into()),
        ["provider", "remove", key] => Request::RemoveProvider((*key).into()),
        _ => return Err(OcgError::config("unsupported config command; use `ocg config --help` for OCG Profile commands (fixed execution tiers are removed)")),
    };
    Ok(request)
}

pub fn execute(
    request: &Request,
    ctx: &Context,
    _probe: &ProbeFn,
    _activate: &ActivateFn,
    _input: &mut dyn BufRead,
    output: &mut dyn Write,
) -> Result<i32> {
    let service = ProfileService::with_workspace(&ctx.user_path, &ctx.project_root);
    let xdg = ctx
        .env
        .xdg_config_home
        .clone()
        .or_else(|| ctx.env.home_dir.as_ref().map(|home| home.join(".config")))
        .ok_or_else(|| {
            OcgError::config("HOME or XDG_CONFIG_HOME is required to compare external config")
        })?;
    match request {
        Request::Help => writeln!(output, "ocg config profile|candidates|new|import PATH SHA256\nocg config provider add KEY LABEL|remove KEY\nocg config model add KEY PROVIDER ID [--default]|remove KEY\nConfiguration is stored in the user-global OCG profile. Provider credentials remain owned by OpenCode; no credentials are imported.")
            .map_err(|error| OcgError::io("cannot print config help", error))?,
        Request::Candidates => {
            let candidates = service.candidates(&xdg)?;
            writeln!(output, "{}", serde_json::to_string_pretty(&candidates).map_err(|error| OcgError::config(format!("cannot serialize comparison: {error}")))?)
                .map_err(|error| OcgError::io("cannot print candidate comparison", error))?;
        }
        Request::New => { service.bootstrap(None, &xdg)?; print_current(&service, output)?; }
        Request::Import { location, sha256 } => { service.bootstrap(Some((location, sha256)), &xdg)?; print_current(&service, output)?; }
        Request::Show => print_current(&service, output)?,
        Request::Provider { key, label } => {
            let (mut profile, revision) = service.current()?.ok_or_else(missing_profile)?;
            if key.is_empty() || profile.providers.contains_key(key) { return Err(OcgError::config("provider key must be non-empty and unique")); }
            profile.providers.insert(key.clone(), Provider { placeholder: false, label: label.clone() });
            service.replace(&revision, &profile)?;
            print_current(&service, output)?;
        }
        Request::Model { key, provider, id, default } => {
            let (mut profile, revision) = service.current()?.ok_or_else(missing_profile)?;
            if key.is_empty() || id.is_empty() || profile.models.contains_key(key) || !profile.providers.get(provider).is_some_and(|entry| !entry.placeholder) {
                return Err(OcgError::config("model key/id must be non-empty and unique; choose a configured non-placeholder provider"));
            }
            profile.models.insert(key.clone(), Model { placeholder: false, provider: provider.clone(), id: id.clone(), variant: None, variants: vec![] });
            if *default { profile.default_model = Some(key.clone()); }
            service.replace(&revision, &profile)?;
            print_current(&service, output)?;
        }
        Request::RemoveModel(key) => {
            let (mut profile, revision) = service.current()?.ok_or_else(missing_profile)?;
            if profile.models.remove(key).is_none() { return Err(OcgError::config("unknown Profile model")); }
            if profile.default_model.as_ref() == Some(key) { profile.default_model = None; }
            service.replace(&revision, &profile)?;
            print_current(&service, output)?;
        }
        Request::RemoveProvider(key) => {
            let (mut profile, revision) = service.current()?.ok_or_else(missing_profile)?;
            if profile.providers.remove(key).is_none() { return Err(OcgError::config("unknown Profile provider")); }
            profile.models.retain(|_, model| model.provider != *key);
            if profile.default_model.as_ref().is_some_and(|selected| !profile.models.contains_key(selected)) { profile.default_model = None; }
            service.replace(&revision, &profile)?;
            print_current(&service, output)?;
        }
    }
    Ok(0)
}

fn missing_profile() -> OcgError {
    OcgError::config("OCG Profile is missing; run `ocg config new` or explicit import first")
}

fn print_current(service: &ProfileService, output: &mut dyn Write) -> Result<()> {
    let current = service.current()?;
    // Only OCG-owned, allowlisted resources. No source document or secrets.
    writeln!(
        output,
        "{}",
        serde_json::to_string_pretty(&current.map(
            |(profile, revision)| serde_json::json!({"profile": profile, "revision": revision})
        ))
        .map_err(|error| OcgError::config(format!("cannot serialize Profile: {error}")))?
    )
    .map_err(|error| OcgError::io("cannot print OCG Profile", error))
}
