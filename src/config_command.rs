//! CLI editing of the Profile the PWA and headless path use, committed in the
//! Core database.
//! All mutations are explicit and revision-checked.
use crate::core_store::CoreStore;
use crate::error::{OcgError, Result};
use crate::profile::{Model, ProfileService, Provider};
use std::ffi::OsString;
use std::io::Write;

#[derive(Debug)]
pub enum Request {
    Help,
    Show,
    New,
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

pub fn parse_request(args: &[OsString]) -> Result<Request> {
    let words: Vec<_> = args
        .iter()
        .map(|part| part.to_string_lossy().into_owned())
        .collect();
    let strings: Vec<_> = words.iter().map(String::as_str).collect();
    let request = match strings.as_slice() {
        [] | ["profile"] => Request::Show,
        ["--help" | "-h"] => Request::Help,
        ["new"] => Request::New,
        ["provider", "add", key, label] => Request::Provider { key: (*key).into(), label: (*label).into() },
        ["model", "add", key, provider, id] => Request::Model { key: (*key).into(), provider: (*provider).into(), id: (*id).into(), default: false },
        ["model", "add", key, provider, id, "--default"] => Request::Model { key: (*key).into(), provider: (*provider).into(), id: (*id).into(), default: true },
        ["model", "remove", key] => Request::RemoveModel((*key).into()),
        ["provider", "remove", key] => Request::RemoveProvider((*key).into()),
        _ => return Err(OcgError::config("unsupported config command; use `ocg config --help` for OCG Profile commands (fixed execution tiers are removed)")),
    };
    Ok(request)
}

pub fn execute(request: &Request, store: &CoreStore, output: &mut dyn Write) -> Result<i32> {
    let service = ProfileService::new(store.clone());
    match request {
        Request::Help => writeln!(output, "ocg config profile|new\nocg config provider add KEY LABEL|remove KEY\nocg config model add KEY PROVIDER ID [--default]|remove KEY\nThe Profile is stored in the OCG Core database.")
            .map_err(|error| OcgError::io("cannot print config help", error))?,
        Request::New => { service.bootstrap()?; print_current(&service, output)?; }
        Request::Show => print_current(&service, output)?,
        Request::Provider { key, label } => {
            let (mut profile, revision) = service.current()?.ok_or_else(missing_profile)?;
            if key.is_empty() || profile.providers.contains_key(key) { return Err(OcgError::config("provider key must be non-empty and unique")); }
            profile.providers.insert(key.clone(), Provider { label: label.clone(), endpoint: None, credential_ref: None, protocol: None, catalog: None });
            service.replace(&revision, &profile)?;
            print_current(&service, output)?;
        }
        Request::Model { key, provider, id, default } => {
            let (mut profile, revision) = service.current()?.ok_or_else(missing_profile)?;
            if key.is_empty() || id.is_empty() || profile.models.contains_key(key) || !profile.providers.contains_key(provider) {
                return Err(OcgError::config("model key/id must be non-empty and unique; choose a configured provider"));
            }
            profile.models.insert(key.clone(), Model { provider: provider.clone(), id: id.clone(), variant: None, variants: vec![], label: None, metadata: None });
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
    OcgError::config("OCG Profile is missing; run `ocg config new` first")
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
