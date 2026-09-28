//! User-global configuration: defaults -> global OCG config -> CLI/environment.

use crate::error::{OcgError, Result};
use crate::json::deep_merge;
use crate::yaml::read_yaml_object;
use serde_json::Value;
use std::path::{Path, PathBuf};

/// The fully resolved configuration plus the paths and metadata needed to
/// report on it.
#[derive(Debug, Clone)]
pub struct Effective {
    /// Merged OCG registries (Profile, routing, permissions, base,
    /// prompts, observability).
    pub data: Value,
    /// The disk OCG home, when defaults were loaded from disk.
    pub ocg_home: Option<PathBuf>,
    /// Directory used to resolve project overrides and launch the child process.
    pub cwd: PathBuf,
    /// User home, used for `~` expansion and the default user config path.
    pub home_dir: Option<PathBuf>,
    /// Resolved user override path (may not exist).
    pub user_path: PathBuf,
    /// Deprecated compatibility field. It equals `user_path`; project files
    /// are never read as OCG configuration.
    pub project_path: PathBuf,
    /// Layers that were found and merged, in application order.
    pub applied: Vec<(&'static str, PathBuf)>,
    /// Non-destructive diagnostics about fields ignored during migration.
    pub diagnostics: Vec<String>,
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

fn xdg_config_home(home_dir: Option<&Path>, explicit: Option<&Path>) -> PathBuf {
    if let Some(dir) = explicit {
        return dir.to_path_buf();
    }
    match home_dir {
        Some(home) => home.join(".config"),
        None => PathBuf::from(".config"),
    }
}

/// Resolve the user override path. An explicit flag beats the environment,
/// which beats the default under `$XDG_CONFIG_HOME`.
pub fn user_config_path(
    explicit: Option<&Path>,
    env_path: Option<&Path>,
    xdg: Option<&Path>,
    home_dir: Option<&Path>,
) -> PathBuf {
    if let Some(raw) = explicit.or(env_path) {
        return expand_tilde(&raw.to_string_lossy(), home_dir);
    }
    xdg_config_home(home_dir, xdg)
        .join("ocg")
        .join("config.yaml")
}

/// Kept for source compatibility with the pre-0.5 API. OCG no longer reads a
/// project-level profile, so this resolves the same global path as
/// [`user_config_path`].
pub fn project_config_path(
    _cwd: &Path,
    explicit: Option<&Path>,
    env_path: Option<&Path>,
    home_dir: Option<&Path>,
) -> PathBuf {
    user_config_path(explicit, env_path, None, home_dir)
}

/// Reject a legacy JSON override instead of silently ignoring or migrating it.
///
/// `path` is the resolved layer path. Two shapes are rejected:
///
/// * the resolved path itself is an existing `.json` file, and
/// * an existing JSON sibling next to the YAML path (for example
///   `~/.config/ocg/config.json` next to `config.yaml`).
///
/// A non-existent explicit `.json` path is left alone so previous "missing
/// override" behavior is preserved.
pub fn reject_stale_json(path: &Path, label: &str) -> Result<()> {
    let is_json = path
        .extension()
        .map(|extension| extension.eq_ignore_ascii_case("json"))
        .unwrap_or(false);
    if is_json {
        let target = path.with_extension("yaml");
        return Err(OcgError::config(format!(
            "unsupported JSON {label} path: {}\nOCG reads and writes YAML only; use {}.",
            path.display(),
            target.display()
        )));
    }
    let legacy = path.with_extension("json");
    if legacy != path && legacy.is_file() {
        return Err(OcgError::config(format!(
            "unsupported JSON {label} file: {}\nOCG reads YAML only; remove it or convert it to {}. Existing JSON is never migrated or merged.",
            legacy.display(),
            path.display()
        )));
    }
    Ok(())
}

/// Deep-merge the global override onto the defaults. The project path remains
/// in the signature for compatibility, but is deliberately ignored.
pub fn build_effective(
    defaults: Value,
    ocg_home: Option<PathBuf>,
    cwd: &Path,
    user_path: &Path,
    project_path: &Path,
    home_dir: Option<PathBuf>,
) -> Result<Effective> {
    let user = if user_path.is_file() {
        reject_stale_json(user_path, "user")?;
        Some(read_yaml_object(user_path)?)
    } else {
        reject_stale_json(user_path, "user")?;
        None
    };
    let project = None;
    build_effective_with_overlays(
        defaults,
        ocg_home,
        cwd,
        user.as_ref(),
        user_path,
        project.as_ref(),
        project_path,
        home_dir,
    )
}

/// Deep-merge an in-memory global overlay onto the defaults. The project
/// overlay arguments remain for source compatibility and are ignored.
///
/// `ocg config` builds a candidate configuration before persisting it; this
/// variant takes the overlays as values so the exact same merging,
/// normalization and layer bookkeeping run without touching any file. A
/// missing layer is `None`, exactly like a missing file.
#[allow(clippy::too_many_arguments)]
pub fn build_effective_with_overlays(
    defaults: Value,
    ocg_home: Option<PathBuf>,
    cwd: &Path,
    user: Option<&Value>,
    user_path: &Path,
    _project: Option<&Value>,
    project_path: &Path,
    home_dir: Option<PathBuf>,
) -> Result<Effective> {
    let mut data = defaults;
    if let Some(object) = data.as_object_mut() {
        object.remove("throttle");
    }
    let mut applied = Vec::new();
    let mut diagnostics = Vec::new();
    let layers: [(&'static str, Option<&Value>, &Path); 1] = [("global", user, user_path)];
    for (name, overlay, path) in layers {
        if let Some(overlay) = overlay {
            let mut overlay = overlay.clone();
            if overlay
                .as_object_mut()
                .and_then(|object| object.remove("throttle"))
                .is_some()
            {
                diagnostics.push(format!("Legacy configuration field 'throttle' detected in {}. OCG v0.4.1 no longer uses fixed execution tiers. The field is ignored; no automatic conversion was performed. Configure provider/model resources in the OCG Profile.", path.display()));
            }
            let prior = data.clone();
            data = deep_merge(&data, &overlay);
            normalize_model_variant_overrides(&prior, &mut data, &overlay);
            applied.push((name, path.to_path_buf()));
        }
    }
    Ok(Effective {
        data,
        ocg_home,
        cwd: cwd.to_path_buf(),
        home_dir,
        user_path: user_path.to_path_buf(),
        project_path: project_path.to_path_buf(),
        applied,
        diagnostics,
    })
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
