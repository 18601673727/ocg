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
    /// Show the effective value at a dotted key, or the whole configuration.
    Get(Option<String>),
    /// Set the value at a dotted key, computed from `expected_revision`.
    Set {
        key: String,
        value: serde_json::Value,
        expected_revision: u64,
    },
    /// The committed configuration revisions after `after`.
    Revisions(u64),
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
        ["get"] => Request::Get(None),
        ["get", key] => Request::Get(Some((*key).into())),
        ["set", key, value, "--expected-revision", revision] => Request::Set {
            key: (*key).into(),
            value: serde_json::from_str(value)
                .map_err(|error| OcgError::config(format!("value must be JSON: {error}")))?,
            expected_revision: revision
                .parse()
                .map_err(|_| OcgError::config("--expected-revision must be a number"))?,
        },
        ["revisions"] => Request::Revisions(0),
        ["revisions", "--after", after] => Request::Revisions(
            after
                .parse()
                .map_err(|_| OcgError::config("--after must be a number"))?,
        ),
        _ => return Err(OcgError::config("unsupported config command; use `ocg config --help` for OCG Profile commands (fixed execution tiers are removed)")),
    };
    Ok(request)
}

pub fn execute(request: &Request, store: &CoreStore, output: &mut dyn Write) -> Result<i32> {
    let service = ProfileService::new(store.clone());
    match request {
        Request::Help => writeln!(output, "ocg config profile|new\nocg config provider add KEY LABEL|remove KEY\nocg config model add KEY PROVIDER ID [--default]|remove KEY\nocg config get [KEY]\nocg config set KEY JSON --expected-revision N\nocg config revisions [--after N]\nConfiguration is stored in the OCG database; KEY is dotted, e.g. storage.attachments.path.")
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
        Request::Get(key) => {
            let effective = crate::config::load(store, std::path::Path::new("."), None)?;
            let value = match key {
                None => effective.data.clone(),
                Some(key) => key
                    .split('.')
                    .try_fold(&effective.data, |value, segment| value.get(segment))
                    .cloned()
                    .ok_or_else(|| OcgError::config(format!("no configuration at {key}")))?,
            };
            print_json(output, &serde_json::json!({"revision": effective.revision, "value": value}))?;
        }
        Request::Set { key, value, expected_revision } => {
            let mut segments = key.split('.');
            let section = segments
                .next()
                .filter(|section| !section.is_empty())
                .ok_or_else(|| OcgError::config("KEY must name a configuration section"))?;
            let path: Vec<&str> = segments.collect();
            let document = store.document(crate::core_store::CORE_SCOPE)?;
            let mut updated = document
                .section(section)
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}));
            set_path(&mut updated, &path, value.clone())?;
            let store_path = store.path();
            let committed = store.commit(
                crate::core_store::ConfigChange {
                    scope: crate::core_store::CORE_SCOPE,
                    command_id: &format!("config-set-{}", uuid::Uuid::now_v7()),
                    principal: "operator",
                    expected_revision: *expected_revision,
                    sections: vec![(section, Some(updated))],
                },
                |candidate| crate::config::validate_document(candidate, &store_path),
            )?;
            print_json(output, &serde_json::json!({"revision": committed.revision, "key": key}))?;
        }
        Request::Revisions(after) => {
            let revisions = store.revisions_since(*after, 256)?;
            print_json(output, &serde_json::to_value(revisions).unwrap_or_default())?;
        }
    }
    Ok(0)
}

/// Replace the value at `path` inside `target`, creating objects on the way.
fn set_path(target: &mut serde_json::Value, path: &[&str], value: serde_json::Value) -> Result<()> {
    let Some((last, parents)) = path.split_last() else {
        *target = value;
        return Ok(());
    };
    let mut current = target;
    for segment in parents {
        current = current
            .as_object_mut()
            .ok_or_else(|| OcgError::config(format!("{segment} is not inside an object")))?
            .entry(segment.to_string())
            .or_insert_with(|| serde_json::json!({}));
    }
    current
        .as_object_mut()
        .ok_or_else(|| OcgError::config(format!("{last} is not inside an object")))?
        .insert(last.to_string(), value);
    Ok(())
}

fn print_json(output: &mut dyn Write, value: &serde_json::Value) -> Result<()> {
    writeln!(
        output,
        "{}",
        serde_json::to_string_pretty(value).map_err(|error| OcgError::config(error.to_string()))?
    )
    .map_err(|error| OcgError::io("cannot print configuration", error))
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
