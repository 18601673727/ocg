//! OCG-owned provider/model Profile.
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct Provider {
    pub label: String,
    /// HTTPS endpoint for provider API calls. Must not include userinfo.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// Reference to a Vault credential name. The credential value is the raw
    /// bearer token (without "Bearer " prefix); OCG constructs the Authorization
    /// header at runtime.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog: Option<ProviderCatalog>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct Model {
    pub provider: String,
    pub id: String,
    /// Optional selected reasoning variant; never inferred from a fixed tier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub variants: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ModelMetadata>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ModelMetadata {
    pub variant: Option<String>,
    pub variants: Option<Vec<String>>,
    pub effort: Option<String>,
    pub efforts: Option<Vec<String>>,
    pub reasoning: Option<bool>,
    pub fast_mode: Option<bool>,
    pub context_window: Option<u64>,
    pub tools: Option<bool>,
    pub images: Option<bool>,
    pub multimodal: Option<bool>,
    pub pricing: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct CatalogModel {
    pub id: String,
    pub label: String,
    pub metadata: ModelMetadata,
    pub raw: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ProviderCatalog {
    pub discovered_at: u64,
    pub models: Vec<CatalogModel>,
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
            providers: BTreeMap::new(),
            models: BTreeMap::new(),
        }
    }

    /// Structural validity does not imply authorization to send an inference request.
    pub fn validate(&self) -> Result<()> {
        for (provider_key, provider) in &self.providers {
            // Validate endpoint if present
            if let Some(endpoint) = &provider.endpoint {
                if endpoint.is_empty() {
                    return Err(OcgError::config(format!(
                        "Profile provider '{provider_key}' endpoint cannot be empty"
                    )));
                }
                // Cleartext HTTP stays allowed: a localhost or private-network
                // OpenAI-compatible endpoint is a legitimate provider, and the
                // transport routes it directly rather than through a proxy.
                if !endpoint.starts_with("https://") && !endpoint.starts_with("http://") {
                    return Err(OcgError::config(format!(
                        "Profile provider '{provider_key}' endpoint must use HTTP or HTTPS"
                    )));
                }
                // Reject embedded credentials (check for @ in the authority section)
                if let Some((_, authority_start)) = endpoint.split_once("://") {
                    if let Some(at_pos) = authority_start.find('@') {
                        // Check if @ comes before the path/query/fragment
                        let path_start = authority_start
                            .find(['/', '?', '#'])
                            .unwrap_or(authority_start.len());
                        if at_pos < path_start {
                            return Err(OcgError::config(format!(
                                "Profile provider '{provider_key}' endpoint must not contain userinfo (username/password)"
                            )));
                        }
                    }
                }
            }
            // Validate credential_ref if present (just non-empty check; Vault validates name format)
            if let Some(credential_ref) = &provider.credential_ref {
                if credential_ref.is_empty() {
                    return Err(OcgError::config(format!(
                        "Profile provider '{provider_key}' credential_ref cannot be empty"
                    )));
                }
            }
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
            !model.id.is_empty() && self.providers.contains_key(&model.provider)
        })
    }

    /// Model keys that can actually execute right now. This is the backend
    /// execution-readiness authority: structural selection, plus a usable
    /// endpoint, plus a credential that exists in the Vault. The secret value
    /// is never read out, only its existence is checked. A Vault failure
    /// closes the gate rather than guessing.
    pub fn executable_choices(&self, vault: &crate::vault::Vault) -> Vec<String> {
        self.models
            .iter()
            .filter(|(_, model)| {
                if model.id.is_empty() {
                    return false;
                }
                let Some(provider) = self.providers.get(&model.provider) else {
                    return false;
                };
                if !endpoint_usable(provider.endpoint.as_deref()) {
                    return false;
                }
                // A declared credential must resolve in the Vault; no declared
                // credential means an unauthenticated endpoint, which is
                // also executable.
                match provider.credential_ref.as_deref() {
                    None => true,
                    Some(reference) if !reference.is_empty() => vault
                        .get(reference)
                        .ok()
                        .flatten()
                        .is_some_and(|value| !value.is_empty()),
                    Some(_) => false,
                }
            })
            .map(|(key, _)| key.clone())
            .collect()
    }

    pub fn require_runnable(&self) -> Result<()> {
        self.validate()?;
        if self.runnable_models().next().is_none() {
            return Err(OcgError::config("No provider/model configured; connect a provider and select models before executing a Job"));
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
        Ok((key, model))
    }

    /// Read one OCG-owned YAML document.
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
        let mut value = json!({
            "origin": profile.get("origin").cloned().unwrap_or(json!("new")),
            "defaultModel": profile.get("defaultModel"),
            "providers": models.get("providers"), "models": models.get("models")
        });
        clean_legacy_resources(&mut value);
        let encoded = serde_json::to_vec(&value)
            .map_err(|error| OcgError::config(format!("invalid OCG Profile: {error}")))?;
        let mut deserializer = serde_json::Deserializer::from_slice(&encoded);
        let parsed: Profile = serde_path_to_error::deserialize(&mut deserializer)
            .map_err(|error| OcgError::config(format!("invalid OCG Profile: {error}")))?;
        parsed.validate()?;
        Ok(parsed)
    }
}

fn clean_legacy_resources(value: &mut Value) {
    let mut removed_providers = std::collections::BTreeSet::new();
    if let Some(providers) = value.get_mut("providers").and_then(Value::as_object_mut) {
        providers.retain(|key, entry| {
            let legacy = entry.get("placeholder").and_then(Value::as_bool) == Some(true)
                || (entry.get("placeholder").is_none()
                    && key == "placeholder"
                    && entry.get("label").and_then(Value::as_str) == Some("Configure a provider")
                    && entry.get("endpoint").is_none_or(Value::is_null)
                    && entry.get("credential_ref").is_none_or(Value::is_null));
            if legacy {
                removed_providers.insert(key.clone());
            }
            !legacy
        });
    }
    let legacy_provider_missing = value.get("providers").and_then(Value::as_object)
        .is_some_and(|providers| !providers.contains_key("placeholder"));
    let mut removed_models = std::collections::BTreeSet::new();
    if let Some(models) = value.get_mut("models").and_then(Value::as_object_mut) {
        models.retain(|key, entry| {
            let legacy = entry.get("placeholder").and_then(Value::as_bool) == Some(true)
                || entry
                    .get("provider")
                    .and_then(Value::as_str)
                    .is_some_and(|provider| removed_providers.contains(provider))
                || (entry.get("placeholder").is_none()
                    && legacy_provider_missing
                    && key == "placeholder"
                    && entry.get("provider").and_then(Value::as_str) == Some("placeholder")
                    && entry.get("id").and_then(Value::as_str) == Some("placeholder"));
            if legacy {
                removed_models.insert(key.clone());
            }
            !legacy
        });
    }
    if value
        .get("defaultModel")
        .and_then(Value::as_str)
        .is_some_and(|key| removed_models.contains(key))
    {
        value["defaultModel"] = Value::Null;
    }
}

impl Default for Profile {
    fn default() -> Self {
        Self::new()
    }
}

/// Whether a provider endpoint is usable for execution. Mirrors the
/// structural rules in `validate`: present, non-empty, HTTP(S), no userinfo.
fn endpoint_usable(endpoint: Option<&str>) -> bool {
    let Some(endpoint) = endpoint.filter(|value| !value.is_empty()) else {
        return false;
    };
    if !endpoint.starts_with("https://") && !endpoint.starts_with("http://") {
        return false;
    }
    let Some((_, authority)) = endpoint.split_once("://") else {
        return false;
    };
    let path_start = authority.find(['/', '?', '#']).unwrap_or(authority.len());
    !authority[..path_start].contains('@')
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

/// A filesystem-backed view of the user-global OCG Profile.
#[derive(Debug, Clone)]
pub struct ProfileService {
    config_path: PathBuf,
    state_dir: PathBuf,
}

impl ProfileService {
    pub fn new(config_path: &Path) -> Self {
        Self::with_workspace(config_path, Path::new("."))
    }

    pub fn with_workspace(config_path: &Path, workspace: &Path) -> Self {
        let _ = workspace;
        Self {
            config_path: config_path.to_path_buf(),
            state_dir: config_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join("state"),
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

    /// Backend-computed execution readiness: model keys that satisfy the
    /// same selection, endpoint, and credential rules as canonical launch.
    /// A missing Profile or an unreadable Vault yields no choices.
    pub fn runnable_choices(&self) -> Vec<String> {
        let Ok(Some((profile, _))) = self.current() else {
            return Vec::new();
        };
        let Ok(vault) = crate::vault::Vault::user_global() else {
            return Vec::new();
        };
        profile.executable_choices(&vault)
    }

    /// Create a new Profile without overwriting an existing one.
    pub fn bootstrap(&self) -> Result<Profile> {
        let _lock = self.lock()?;
        if self.path().exists() {
            return Err(OcgError::config(
                "OCG Profile already exists; bootstrap never overwrites it",
            ));
        }
        let profile = Profile::new();
        persist_new(&self.path(), &profile)?;
        Ok(profile)
    }

    /// Explicit editing of the OCG-owned Profile; preserve other OCG YAML
    /// sections and reject edits based on a stale backend projection.
    pub fn replace(&self, expected_sha256: &str, profile: &Profile) -> Result<String> {
        profile.validate()?;
        let _lock = self.lock()?;
        self.replace_locked(expected_sha256, profile)
    }

    pub fn replace_with_credential(
        &self,
        expected_sha256: &str,
        profile: &Profile,
        vault: &crate::vault::Vault,
        reference: &str,
        secret: &str,
    ) -> Result<String> {
        profile.validate()?;
        let _lock = self.lock()?;
        vault.insert_with(reference, secret, || {
            self.replace_locked(expected_sha256, profile)
        })
    }

    fn replace_locked(&self, expected_sha256: &str, profile: &Profile) -> Result<String> {
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
