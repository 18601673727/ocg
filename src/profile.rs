//! OCG-owned provider/model profile and explicit external configuration imports.
//!
//! External configuration is an input snapshot, never a live configuration layer.
use crate::error::{OcgError, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use ts_rs::TS;

/// The Profile control API version. Shared with the generated TypeScript, so
/// a version bump is a compile-time mismatch rather than a runtime surprise.
pub const PROVIDER_PROFILE_API_VERSION: &str = "ocg.profile.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum Origin {
    New,
    Imported {
        source: String,
        scope: String,
        location: PathBuf,
        sha256: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct Provider {
    pub placeholder: bool,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct Model {
    pub placeholder: bool,
    pub provider: String,
    pub id: String,
    /// Optional selected reasoning variant; never inferred from a fixed tier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub variants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct Profile {
    pub origin: Origin,
    #[serde(
        rename = "defaultModel",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub default_model: Option<String>,
    pub providers: BTreeMap<String, Provider>,
    pub models: BTreeMap<String, Model>,
}

impl Profile {
    pub fn new() -> Self {
        Self {
            origin: Origin::New,
            default_model: None,
            providers: BTreeMap::from([(
                "placeholder".into(),
                Provider {
                    placeholder: true,
                    label: "Configure a provider".into(),
                },
            )]),
            models: BTreeMap::from([(
                "placeholder".into(),
                Model {
                    placeholder: true,
                    provider: "placeholder".into(),
                    id: "placeholder".into(),
                    variant: None,
                    variants: vec![],
                },
            )]),
        }
    }

    /// Structural validity does not imply authorization to send an inference request.
    pub fn validate(&self) -> Result<()> {
        if self.providers.is_empty() || self.models.is_empty() {
            return Err(OcgError::config(
                "Profile requires at least one provider and one model",
            ));
        }
        for (key, model) in &self.models {
            if model.id.is_empty() || !self.providers.contains_key(&model.provider) {
                return Err(OcgError::config(format!(
                    "Profile model '{key}' needs an id and a declared provider"
                )));
            }
            if let Some(variant) = model.variant.as_deref() {
                if variant.is_empty()
                    || (!model.variants.is_empty()
                        && !model.variants.iter().any(|candidate| candidate == variant))
                {
                    return Err(OcgError::config(format!(
                        "Profile model '{key}' selects unsupported variant '{variant}'"
                    )));
                }
            }
        }
        if let Some(selected) = self.default_model.as_deref() {
            if !self.models.contains_key(selected) {
                return Err(OcgError::config(format!(
                    "profile.defaultModel references unknown model '{selected}'"
                )));
            }
        }
        Ok(())
    }

    pub fn runnable_models(&self) -> impl Iterator<Item = (&String, &Model)> {
        self.models.iter().filter(|(_, model)| {
            !model.placeholder
                && self
                    .providers
                    .get(&model.provider)
                    .is_some_and(|provider| !provider.placeholder)
        })
    }

    pub fn require_runnable(&self) -> Result<()> {
        self.validate()?;
        if self.runnable_models().next().is_none() {
            return Err(OcgError::config("No runnable provider/model configured; replace the Profile placeholders before executing a Mission"));
        }
        Ok(())
    }

    pub fn select(&self, requested: Option<&str>) -> Result<(&str, &Model)> {
        self.require_runnable()?;
        let key = requested.or(self.default_model.as_deref()).ok_or_else(|| {
            OcgError::config("No model selected; set profile.defaultModel or select a configured model explicitly")
        })?;
        let (key, model) = self
            .models
            .get_key_value(key)
            .ok_or_else(|| OcgError::config(format!("unknown Profile model '{key}'")))?;
        if model.placeholder
            || self
                .providers
                .get(&model.provider)
                .is_none_or(|provider| provider.placeholder)
        {
            return Err(OcgError::config(
                "No runnable provider/model configured; selected Profile resource is a placeholder",
            ));
        }
        Ok((key, model))
    }

    /// Read one OCG-owned YAML document, not a merged OpenCode runtime config.
    pub fn from_ocg_config(data: &Value) -> Result<Self> {
        let profile = data
            .get("profile")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                OcgError::config("OCG Profile is missing; create or import the global profile")
            })?;
        let models = data
            .get("models")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                OcgError::config("OCG Profile requires models.providers and models.models")
            })?;
        let value = json!({
            "origin": profile.get("origin").cloned().unwrap_or(json!("new")),
            "defaultModel": profile.get("defaultModel"),
            "providers": models.get("providers"), "models": models.get("models")
        });
        let parsed: Profile = serde_json::from_value(value)
            .map_err(|error| OcgError::config(format!("invalid OCG Profile: {error}")))?;
        parsed.validate()?;
        Ok(parsed)
    }
}

impl Default for Profile {
    fn default() -> Self {
        Self::new()
    }
}

/// A reusable, redacted comparison record; neither original JSON nor credentials are exposed.
///
/// Round-trippable on purpose: a comparison the client can deserialize is a
/// comparison the client can actually validate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct Candidate {
    pub source: String,
    pub scope: String,
    pub location: PathBuf,
    pub sha256: String,
    pub provider_names: Vec<String>,
    pub model_ids: Vec<String>,
    pub variants: BTreeMap<String, Vec<String>>,
    pub importable_fields: Vec<String>,
    pub ignored_fields: Vec<String>,
}

/// Discover documents independently, including project ancestors. OpenCode's
/// runtime merges its own documents, but OCG import never performs that merge:
/// every discovered file requires an explicit independent selection.
pub fn discover_opencode(project: &Path, xdg: &Path) -> Result<Vec<Candidate>> {
    let mut candidates = Vec::new();
    let mut locations = vec![("global", xdg.join("opencode"))];
    for ancestor in project.ancestors() {
        locations.push(("local", ancestor.to_path_buf()));
        locations.push(("local", ancestor.join(".opencode")));
    }
    for (scope, dir) in locations {
        for filename in ["opencode.json", "opencode.jsonc"] {
            let path = dir.join(filename);
            if path.is_file() {
                candidates.push(inspect_opencode(&path, scope)?);
            }
        }
    }
    Ok(candidates)
}

fn read_source(path: &Path) -> Result<(Vec<u8>, Value)> {
    let bytes =
        std::fs::read(path).map_err(|error| OcgError::io("cannot read external config", error))?;
    // JSONC must be handled explicitly rather than silently treating a candidate
    // as an empty document. Unsupported syntax is a diagnostic, never an import.
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| OcgError::config("external config is not UTF-8"))?;
    let normalized = strip_jsonc(text)?;
    let value: Value = serde_json::from_str(&normalized).map_err(|error| {
        OcgError::config(format!(
            "invalid external config at {}: {error}",
            path.display()
        ))
    })?;
    if !value.is_object() {
        return Err(OcgError::config("external config must be an object"));
    }
    Ok((bytes, value))
}

/// Remove JSONC comments and trailing commas without interpreting comment tokens in strings.
fn strip_jsonc(text: &str) -> Result<String> {
    let chars: Vec<char> = text.chars().collect();
    let mut output = String::new();
    let (mut i, mut quoted, mut escaped) = (0, false, false);
    while i < chars.len() {
        let c = chars[i];
        if quoted {
            output.push(c);
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                quoted = false;
            }
            i += 1;
        } else if c == '"' {
            quoted = true;
            output.push(c);
            i += 1;
        } else if c == '/' && chars.get(i + 1) == Some(&'/') {
            i += 2;
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
        } else if c == '/' && chars.get(i + 1) == Some(&'*') {
            i += 2;
            let mut closed = false;
            while i + 1 < chars.len() {
                if chars[i] == '*' && chars[i + 1] == '/' {
                    i += 2;
                    closed = true;
                    break;
                }
                i += 1;
            }
            if !closed {
                return Err(OcgError::config("unterminated JSONC comment"));
            }
            output.push(' ');
        } else if c == ',' {
            let mut next = i + 1;
            while next < chars.len() && chars[next].is_whitespace() {
                next += 1;
            }
            if !matches!(chars.get(next), Some('}') | Some(']')) {
                output.push(c);
            }
            i += 1;
        } else {
            output.push(c);
            i += 1;
        }
    }
    // Comments may occur between a final comma and a closing delimiter.
    // Remove trailing commas after comment removal, outside strings only.
    let chars: Vec<char> = output.chars().collect();
    let mut normalized = String::new();
    let (mut quoted, mut escaped) = (false, false);
    for (index, c) in chars.iter().enumerate() {
        if quoted {
            normalized.push(*c);
            if escaped {
                escaped = false;
            } else if *c == '\\' {
                escaped = true;
            } else if *c == '"' {
                quoted = false;
            }
        } else if *c == '"' {
            quoted = true;
            normalized.push(*c);
        } else if *c != ','
            || !matches!(
                chars.iter().skip(index + 1).find(|c| !c.is_whitespace()),
                Some('}') | Some(']')
            )
        {
            normalized.push(*c);
        }
    }
    Ok(normalized)
}

pub fn inspect_opencode(path: &Path, scope: &str) -> Result<Candidate> {
    let (bytes, data) = read_source(path)?;
    let mut provider_names = Vec::new();
    let mut model_ids = Vec::new();
    let mut variants = BTreeMap::new();
    if let Some(providers) = data.get("providers").and_then(Value::as_object) {
        for (name, entry) in providers {
            provider_names.push(name.clone());
            if let Some(models) = entry.get("models").and_then(Value::as_object) {
                for (id, spec) in models {
                    model_ids.push(format!("{name}/{id}"));
                    if let Some(options) = spec.get("variants").and_then(Value::as_object) {
                        variants.insert(format!("{name}/{id}"), options.keys().cloned().collect());
                    }
                }
            }
        }
    }
    if let Some(default) = data.get("model").and_then(Value::as_str) {
        if let Some((provider, _)) = default.split_once('/') {
            if !provider_names.contains(&provider.to_string()) {
                provider_names.push(provider.to_string());
            }
            if !model_ids.contains(&default.to_string()) {
                model_ids.push(default.to_string());
            }
        }
    }
    let fields = data.as_object().expect("read_source verified object");
    let mut importable_fields: Vec<String> = fields
        .keys()
        .filter(|key| matches!(key.as_str(), "model" | "providers"))
        .cloned()
        .collect();
    // Only field names are retained, never field contents (which may be secrets).
    let mut ignored_fields: Vec<String> = fields
        .keys()
        .filter(|key| !matches!(key.as_str(), "model" | "providers"))
        .cloned()
        .collect();
    importable_fields.sort();
    ignored_fields.sort();
    Ok(Candidate {
        source: "opencode".into(),
        scope: scope.into(),
        location: path.to_path_buf(),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        provider_names,
        model_ids,
        variants,
        importable_fields,
        ignored_fields,
    })
}

/// Snapshot only allowlisted identity facts. No credentials, headers, URLs or
/// unknown provider settings are copied. Never re-read after import.
pub fn import_opencode(candidate: &Candidate) -> Result<Profile> {
    if candidate.source != "opencode" {
        return Err(OcgError::config("unsupported import source"));
    }
    let (bytes, data) = read_source(&candidate.location)?;
    if format!("{:x}", Sha256::digest(&bytes)) != candidate.sha256 {
        return Err(OcgError::config("external configuration changed since comparison; inspect candidates again before importing"));
    }
    let mut profile = Profile {
        origin: Origin::Imported {
            source: candidate.source.clone(),
            scope: candidate.scope.clone(),
            location: candidate.location.clone(),
            sha256: format!("{:x}", Sha256::digest(&bytes)),
        },
        default_model: data
            .get("model")
            .and_then(Value::as_str)
            .map(str::to_string),
        providers: BTreeMap::new(),
        models: BTreeMap::new(),
    };
    if let Some(providers) = data.get("providers").and_then(Value::as_object) {
        for (provider, entry) in providers {
            if let Some(models) = entry.get("models").and_then(Value::as_object) {
                for (id, spec) in models {
                    let model_id = spec.get("modelID").and_then(Value::as_str).unwrap_or(id);
                    let variants = spec
                        .get("variants")
                        .and_then(Value::as_object)
                        .map(|v| v.keys().cloned().collect())
                        .unwrap_or_default();
                    add_model(&mut profile, provider, id, model_id, variants);
                }
            }
        }
    }
    if let Some((provider, model)) = data
        .get("model")
        .and_then(Value::as_str)
        .and_then(|s| s.split_once('/'))
    {
        if !provider.is_empty()
            && !model.is_empty()
            && !profile.models.contains_key(&format!("{provider}/{model}"))
        {
            add_model(&mut profile, provider, model, model, vec![]);
        }
    }
    if profile.models.is_empty() {
        // No explicit importable model: onboarding remains possible, inference not.
        let placeholder = Profile::new();
        profile.providers = placeholder.providers;
        profile.models = placeholder.models;
    }
    profile.validate()?;
    Ok(profile)
}

fn add_model(profile: &mut Profile, provider: &str, key: &str, id: &str, variants: Vec<String>) {
    profile
        .providers
        .entry(provider.to_string())
        .or_insert_with(|| Provider {
            placeholder: false,
            label: provider.to_string(),
        });
    profile.models.insert(
        format!("{provider}/{key}"),
        Model {
            placeholder: false,
            provider: provider.to_string(),
            id: id.to_string(),
            variant: None,
            variants,
        },
    );
}

pub fn as_ocg_config(profile: &Profile) -> Result<Value> {
    profile.validate()?;
    Ok(
        json!({ "profile": { "origin": profile.origin, "defaultModel": profile.default_model }, "models": {
            "providers": profile.providers, "models": profile.models
        }}),
    )
}

/// Reject an existing file: bootstrap and import must not overwrite user edits.
pub fn persist_new(path: &Path, profile: &Profile) -> Result<()> {
    let value = as_ocg_config(profile)?;
    let text = serde_yaml_ng::to_string(&value)
        .map_err(|error| OcgError::config(format!("cannot serialize Profile: {error}")))?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| OcgError::io("cannot create global OCG config directory", error))?;
    }
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| {
            OcgError::io(
                "cannot create OCG Profile (existing files are never overwritten)",
                error,
            )
        })?;
    use std::io::Write;
    file.write_all(text.as_bytes())
        .map_err(|error| OcgError::io("cannot write OCG Profile", error))?;
    file.sync_all()
        .map_err(|error| OcgError::io("cannot sync OCG Profile", error))
}

/// A filesystem-backed view of the user-global OCG Profile. Neither a project
/// workspace nor OpenCode owns a shadow Profile copy.
#[derive(Debug, Clone)]
pub struct ProfileService {
    config_path: PathBuf,
    state_dir: PathBuf,
    workspace: PathBuf,
}

impl ProfileService {
    pub fn new(config_path: &Path) -> Self {
        let workspace = config_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        Self::with_workspace(config_path, &workspace)
    }

    pub fn with_workspace(config_path: &Path, workspace: &Path) -> Self {
        Self {
            config_path: config_path.to_path_buf(),
            state_dir: config_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join("state"),
            workspace: workspace.to_path_buf(),
        }
    }

    fn path(&self) -> PathBuf {
        self.config_path.clone()
    }

    fn lock(&self) -> Result<std::fs::File> {
        let dir = &self.state_dir;
        std::fs::create_dir_all(dir)
            .map_err(|error| OcgError::io("cannot create Profile state directory", error))?;
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(dir.join("profile.lock"))
            .map_err(|error| OcgError::io("cannot open Profile lock", error))?;
        fs2::FileExt::lock_exclusive(&lock)
            .map_err(|error| OcgError::io("cannot lock Profile", error))?;
        Ok(lock)
    }

    pub fn candidates(&self, xdg: &Path) -> Result<Vec<Candidate>> {
        discover_opencode(&self.workspace, xdg)
    }

    pub fn current(&self) -> Result<Option<(Profile, String)>> {
        let path = self.path();
        if !path.exists() {
            return Ok(None);
        }
        let bytes =
            std::fs::read(&path).map_err(|error| OcgError::io("cannot read OCG Profile", error))?;
        let data = crate::yaml::read_yaml_object(&path)?;
        Ok(Some((
            Profile::from_ocg_config(&data)?,
            format!("{:x}", Sha256::digest(&bytes)),
        )))
    }

    /// The user chooses either New or a candidate from a freshly-discovered
    /// comparison. No candidate is inferred from file existence or ordering.
    pub fn bootstrap(&self, selection: Option<(&Path, &str)>, xdg: &Path) -> Result<Profile> {
        let _lock = self.lock()?;
        if self.path().exists() {
            return Err(OcgError::config(
                "OCG Profile already exists; bootstrap never overwrites it",
            ));
        }
        let profile = if let Some((path, hash)) = selection {
            let candidates = self.candidates(xdg)?;
            let candidate = candidates.iter().find(|candidate| candidate.location == path && candidate.sha256 == hash)
                .ok_or_else(|| OcgError::config("import candidate is missing or changed; refresh comparison before selecting"))?;
            import_opencode(candidate)?
        } else {
            Profile::new()
        };
        persist_new(&self.path(), &profile)?;
        Ok(profile)
    }

    /// Explicit editing of the OCG-owned Profile; preserve other OCG YAML
    /// sections and reject edits based on a stale backend projection.
    pub fn replace(&self, expected_sha256: &str, profile: &Profile) -> Result<String> {
        profile.validate()?;
        let _lock = self.lock()?;
        let (.., actual) = self
            .current()?
            .ok_or_else(|| OcgError::config("OCG Profile is missing; bootstrap it explicitly"))?;
        if expected_sha256 != actual {
            return Err(OcgError::config(
                "OCG Profile changed since it was read; refresh before editing",
            ));
        }
        let path = self.path();
        let mut document = crate::yaml::read_yaml_object(&path)?;
        let profile_data = as_ocg_config(profile)?;
        let object = document
            .as_object_mut()
            .ok_or_else(|| OcgError::config("OCG config must be a mapping"))?;
        object.insert("profile".into(), profile_data["profile"].clone());
        object.insert("models".into(), profile_data["models"].clone());
        let serialized = serde_yaml_ng::to_string(&document)
            .map_err(|error| OcgError::config(format!("cannot serialize OCG Profile: {error}")))?;
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let mut temp = tempfile::NamedTempFile::new_in(parent)
            .map_err(|error| OcgError::io("cannot stage OCG Profile edit", error))?;
        use std::io::Write;
        temp.write_all(serialized.as_bytes())
            .map_err(|error| OcgError::io("cannot stage OCG Profile edit", error))?;
        temp.as_file()
            .sync_all()
            .map_err(|error| OcgError::io("cannot sync OCG Profile edit", error))?;
        temp.persist(&path)
            .map_err(|error| OcgError::io("cannot persist OCG Profile edit", error.error))?;
        Ok(format!("{:x}", Sha256::digest(serialized.as_bytes())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn placeholder_is_valid_but_never_runnable() {
        let profile = Profile::new();
        profile.validate().unwrap();
        assert!(profile
            .require_runnable()
            .unwrap_err()
            .to_string()
            .contains("No runnable provider/model"));
    }

    #[test]
    fn models_have_no_fixed_upper_bound() {
        let mut profile = Profile::new();
        profile.providers.insert(
            "real".into(),
            Provider {
                placeholder: false,
                label: "Real".into(),
            },
        );
        assert_eq!(profile.runnable_models().count(), 0);
        for i in 0..17 {
            profile.models.insert(
                format!("m{i}"),
                Model {
                    placeholder: false,
                    provider: "real".into(),
                    id: format!("m{i}"),
                    variant: None,
                    variants: vec![],
                },
            );
        }
        profile.validate().unwrap();
        assert_eq!(profile.runnable_models().count(), 17);
        assert!(profile.require_runnable().is_ok());
        let config = as_ocg_config(&profile).unwrap();
        let parsed = Profile::from_ocg_config(&config).unwrap();
        assert_eq!(parsed.models.len(), 18); // 17 choices plus the explicit sentinel
        for i in 0..17 {
            let key = format!("m{i}");
            assert_eq!(parsed.select(Some(&key)).unwrap().0, key);
            let contract = crate::model::lead_contract(&config, &key).unwrap();
            assert_eq!(contract.full_model_id(), format!("real/m{i}"));
        }
        assert!(parsed.select(Some("placeholder")).is_err());
    }

    #[test]
    fn selected_variant_is_explicit_and_is_rejected_when_unsupported() {
        let mut profile = Profile::new();
        profile.providers.insert(
            "provider".into(),
            Provider {
                placeholder: false,
                label: "Provider".into(),
            },
        );
        profile.models.insert(
            "selected".into(),
            Model {
                placeholder: false,
                provider: "provider".into(),
                id: "model".into(),
                variant: Some("medium".into()),
                variants: vec!["fast".into(), "medium".into()],
            },
        );
        profile.default_model = Some("selected".into());
        let config = as_ocg_config(&profile).unwrap();
        assert_eq!(
            crate::model::lead_contract(&config, "selected")
                .unwrap()
                .variant
                .as_deref(),
            Some("medium")
        );
        profile.models.get_mut("selected").unwrap().variant = Some("unsupported".into());
        assert!(profile
            .validate()
            .unwrap_err()
            .to_string()
            .contains("unsupported variant"));
    }

    #[test]
    fn independent_candidates_snapshot_only_allowlisted_fields() {
        let dir = tempfile::tempdir().unwrap();
        let global = dir.path().join("opencode");
        std::fs::create_dir(&global).unwrap();
        let global_path = global.join("opencode.jsonc");
        std::fs::write(
            &global_path,
            r#"{ // secret: SUPERSECRET
            "model": "global/g1", "providers": {"global": {"apiKey": "SUPERSECRET",
            "models": {"g1": {"modelID": "g1", "variants": {"fast": {}, "slow": {}}}}}},
            "authorization": "SUPERSECRET",
        }"#,
        )
        .unwrap();
        let local = dir.path().join("project");
        std::fs::create_dir(&local).unwrap();
        let local_path = local.join("opencode.json");
        std::fs::write(&local_path, r#"{"model":"local/l1"}"#).unwrap();
        let candidates = discover_opencode(&local, dir.path()).unwrap();
        assert_eq!(candidates.len(), 2);
        let comparison = serde_json::to_string(&candidates).unwrap();
        assert!(comparison.contains("global/g1"));
        assert!(comparison.contains("local/l1"));
        assert!(!comparison.contains("SUPERSECRET"));
        let imported = import_opencode(&candidates[0]).unwrap();
        assert!(imported.require_runnable().is_ok());
        assert!(!serde_json::to_string(&imported)
            .unwrap()
            .contains("SUPERSECRET"));
        std::fs::write(&global_path, r#"{"model":"different/changed"}"#).unwrap();
        assert!(imported.models.contains_key("global/g1"));
        assert!(!imported.models.contains_key("different/changed"));
        assert!(import_opencode(&candidates[0])
            .unwrap_err()
            .to_string()
            .contains("changed since comparison"));
        assert!(import_opencode(&candidates[1])
            .unwrap()
            .models
            .contains_key("local/l1"));
        assert_eq!(Profile::new().origin, Origin::New);
    }

    #[test]
    fn bootstrap_never_overwrites_user_profile() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("global.yaml");
        persist_new(&path, &Profile::new()).unwrap();
        let initial = std::fs::read(&path).unwrap();
        let document: Value = serde_yaml_ng::from_slice(&initial).unwrap();
        assert_eq!(
            document["models"]["providers"]["placeholder"]["placeholder"],
            true
        );
        assert_eq!(
            document["models"]["models"]["placeholder"]["placeholder"],
            true
        );
        assert!(persist_new(&path, &Profile::new()).is_err());
        assert_eq!(initial, std::fs::read(&path).unwrap());
    }

    #[test]
    fn unsupported_external_fields_cannot_become_ocg_configuration() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("opencode.json");
        std::fs::write(
            &path,
            r#"{"providers":{"sample":{"apiKey":"DO_NOT_COPY",
          "headers":{"Authorization":"DO_NOT_COPY"},"models":{"model":{"name":"Example"}}}},
          "permissions":["allow all"],"mcp":{"servers":{"secret":"DO_NOT_COPY"}}}"#,
        )
        .unwrap();
        let candidate = inspect_opencode(&path, "local").unwrap();
        assert_eq!(candidate.ignored_fields, ["mcp", "permissions"]);
        let profile = import_opencode(&candidate).unwrap();
        let serialized = serde_json::to_string(&as_ocg_config(&profile).unwrap()).unwrap();
        assert!(!serialized.contains("DO_NOT_COPY"));
        assert!(!serialized.contains("permissions"));
        assert!(!serialized.contains("headers"));
    }

    #[test]
    fn project_ancestor_config_is_an_independent_local_candidate() {
        let dir = tempfile::tempdir().unwrap();
        let parent = dir.path().join("parent");
        let child = parent.join("child");
        std::fs::create_dir_all(&child).unwrap();
        let ancestor = parent.join("opencode.jsonc");
        let local = child.join(".opencode");
        std::fs::create_dir(&local).unwrap();
        let nested = local.join("opencode.json");
        std::fs::write(&ancestor, r#"{"model":"one/a"}"#).unwrap();
        std::fs::write(&nested, r#"{"model":"two/b"}"#).unwrap();
        let candidates = discover_opencode(&child, dir.path()).unwrap();
        assert!(candidates
            .iter()
            .any(|candidate| candidate.location == ancestor && candidate.scope == "local"));
        assert!(candidates
            .iter()
            .any(|candidate| candidate.location == nested && candidate.scope == "local"));
        assert!(import_opencode(
            candidates
                .iter()
                .find(|candidate| candidate.location == ancestor)
                .unwrap()
        )
        .unwrap()
        .models
        .contains_key("one/a"));
        assert!(import_opencode(
            candidates
                .iter()
                .find(|candidate| candidate.location == nested)
                .unwrap()
        )
        .unwrap()
        .models
        .contains_key("two/b"));
    }

    #[test]
    fn selected_import_persists_once_and_editor_uses_same_global_yaml() {
        for choice in ["global", "local", "new"] {
            let dir = tempfile::tempdir().unwrap();
            let root = dir.path().join("project");
            std::fs::create_dir(&root).unwrap();
            let global = dir.path().join("opencode");
            std::fs::create_dir(&global).unwrap();
            let global_path = global.join("opencode.json");
            let local_path = root.join("opencode.json");
            std::fs::write(&global_path, r#"{"model":"global/one"}"#).unwrap();
            std::fs::write(&local_path, r#"{"model":"local/two"}"#).unwrap();
            let profile_path = dir.path().join("global.yaml");
            let service = ProfileService::with_workspace(&profile_path, &root);
            let candidates = service.candidates(dir.path()).unwrap();
            assert_eq!(candidates.len(), 2);
            let selection = candidates
                .iter()
                .find(|candidate| candidate.scope == choice)
                .map(|candidate| (candidate.location.as_path(), candidate.sha256.as_str()));
            let chosen = service.bootstrap(selection, dir.path()).unwrap();
            let (saved, revision) = service.current().unwrap().unwrap();
            assert_eq!(saved, chosen);
            assert_eq!(saved.models.contains_key("global/one"), choice == "global");
            assert_eq!(saved.models.contains_key("local/two"), choice == "local");
            assert_eq!(saved.origin == Origin::New, choice == "new");
            assert!(service.bootstrap(None, dir.path()).is_err());
            std::fs::write(&global_path, r#"{"model":"global/changed"}"#).unwrap();
            assert_eq!(service.current().unwrap().unwrap().0, chosen);
            let mut edited = chosen;
            edited.providers.insert(
                "configured".into(),
                Provider {
                    placeholder: false,
                    label: "Configured".into(),
                },
            );
            edited.models.insert(
                "configured".into(),
                Model {
                    placeholder: false,
                    provider: "configured".into(),
                    id: "model".into(),
                    variant: None,
                    variants: vec![],
                },
            );
            edited.default_model = Some("configured".into());
            service.replace(&revision, &edited).unwrap();
            assert!(service.replace(&revision, &saved).is_err());
            assert_eq!(
                service
                    .current()
                    .unwrap()
                    .unwrap()
                    .0
                    .select(None)
                    .unwrap()
                    .0,
                "configured"
            );
        }
    }

    #[test]
    fn changed_candidate_is_rejected_before_first_profile_is_written() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("project");
        std::fs::create_dir(&root).unwrap();
        let source = root.join("opencode.json");
        std::fs::write(&source, r#"{"model":"external/one"}"#).unwrap();
        let profile_path = dir.path().join("global.yaml");
        let service = ProfileService::with_workspace(&profile_path, &root);
        let candidate = service.candidates(dir.path()).unwrap().remove(0);
        std::fs::write(&source, r#"{"model":"external/two"}"#).unwrap();
        assert!(service
            .bootstrap(Some((&candidate.location, &candidate.sha256)), dir.path())
            .is_err());
        assert!(service.current().unwrap().is_none());
    }
}
