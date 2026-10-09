//! The effective configuration: the defaults compiled into this binary,
//! overlaid by the configuration committed to the Core database.

use crate::core_store::{ConfigDocument, CoreStore, CORE_SCOPE};
use crate::error::{OcgError, Result};
use crate::json::deep_merge;
use serde_json::Value;
use std::path::{Path, PathBuf};

/// The fully resolved configuration plus what is needed to report on it.
#[derive(Debug, Clone)]
pub struct Effective {
    /// Merged OCG registries (Profile, routing, permissions, base, prompts,
    /// observability and policies).
    pub data: Value,
    /// The Core-scope revision this configuration was read at.
    pub revision: u64,
    /// The Core database it was read from.
    pub store: PathBuf,
    /// Directory the invocation runs in.
    pub cwd: PathBuf,
    /// User home, used for `~` expansion of generated-file locations.
    pub home_dir: Option<PathBuf>,
}

/// Expand a leading `~` using the supplied home directory.
pub fn expand_tilde(raw: &str, home_dir: Option<&Path>) -> PathBuf {
    if raw == "~" {
        if let Some(home) = home_dir {
            return home.to_path_buf();
        }
        return PathBuf::from(raw);
    }
    if let Some(rest) = raw.strip_prefix("~/") {
        if let Some(home) = home_dir {
            return home.join(rest);
        }
    }
    PathBuf::from(raw)
}

/// Read the committed Core configuration and resolve it.
pub fn load(store: &CoreStore, cwd: &Path, home_dir: Option<PathBuf>) -> Result<Effective> {
    let document = store.document(CORE_SCOPE)?;
    resolve(&document, &store.path(), cwd, home_dir)
}

/// Resolve the defaults and one Core-scope document into the effective
/// configuration. Pure: commit-time validation resolves a candidate document
/// exactly as readers will resolve it once committed.
pub fn resolve(
    document: &ConfigDocument,
    store: &Path,
    cwd: &Path,
    home_dir: Option<PathBuf>,
) -> Result<Effective> {
    let defaults = crate::defaults::load_defaults()?;
    let overlay = document.to_value();
    let mut data = deep_merge(&defaults, &overlay);
    normalize_model_variant_overrides(&defaults, &mut data, &overlay);
    Ok(Effective {
        data,
        revision: document.revision,
        store: store.to_path_buf(),
        cwd: cwd.to_path_buf(),
        home_dir,
    })
}

/// Validate a candidate Core-scope document as the complete configuration it
/// would become. Used as the commit-time validator of every Core-scope change.
pub fn validate_document(document: &ConfigDocument, store: &Path) -> Result<()> {
    let effective = resolve(document, store, Path::new("."), None)?;
    let errors = crate::validate::validate(&effective);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(OcgError::config(format!(
            "configuration is invalid:\n  - {}",
            errors.join("\n  - ")
        )))
    }
}

/// A variant belongs to a model, not to a route name. Deep merge deliberately
/// preserves omitted scalars, but a model replacement must not retain the old
/// model's variant. Omitted variants still inherit when the model is unchanged;
/// an explicit null remains provider-default.
fn normalize_model_variant_overrides(prior: &Value, merged: &mut Value, overlay: &Value) {
    for (section, routes_key) in [("routing", "roles")] {
        let Some(overrides) = overlay
            .get(section)
            .and_then(|value| value.get(routes_key))
            .and_then(Value::as_object)
        else {
            continue;
        };
        let Some(routes) = merged
            .get_mut(section)
            .and_then(|value| value.get_mut(routes_key))
            .and_then(Value::as_object_mut)
        else {
            continue;
        };
        for (name, route_override) in overrides {
            let Some(route_override) = route_override.as_object() else {
                continue;
            };
            if route_override.contains_key("model") && !route_override.contains_key("variant") {
                let old_model = prior
                    .get(section)
                    .and_then(|value| value.get(routes_key))
                    .and_then(|value| value.get(name))
                    .and_then(|value| value.get("model"));
                let new_model = route_override.get("model");
                if old_model != new_model {
                    if let Some(route) = routes.get_mut(name).and_then(Value::as_object_mut) {
                        route.remove("variant");
                    }
                }
            }
        }
    }
}
