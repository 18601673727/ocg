//! OCG-owned provider/model Profile.
use crate::error::{OcgError, Result};
use crate::provider_protocol::ProviderProtocol;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
#[cfg(test)]
use std::path::PathBuf;
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
    /// The wire protocol this provider speaks. `None` means the provider speaks
    /// OpenAI Chat Completions, which is what every Profile written before
    /// protocols were explicit does. A key, a label or an endpoint host never
    /// implies a protocol.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<ProviderProtocol>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog: Option<ProviderCatalog>,
}

impl Provider {
    /// The protocol this provider speaks, resolving an absent declaration to the
    /// OpenAI-compatible contract every existing Profile already speaks.
    #[must_use]
    pub fn wire_protocol(&self) -> ProviderProtocol {
        self.protocol.unwrap_or_default()
    }
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
    /// endpoint, plus a credential that is stored. `stored` is the set of
    /// stored credential names; no credential value is read here.
    pub fn executable_choices(&self, stored: &std::collections::BTreeSet<String>) -> Vec<String> {
        let mut choices = Vec::new();
        for (key, model) in &self.models {
            if model.id.is_empty() {
                continue;
            }
            let Some(provider) = self.providers.get(&model.provider) else {
                continue;
            };
            if !endpoint_usable(provider.endpoint.as_deref()) {
                continue;
            }
            // A declared credential must be stored; no declared credential
            // means an unauthenticated endpoint, which is also executable.
            let executable = match provider.credential_ref.as_deref() {
                None => true,
                Some(reference) => !reference.is_empty() && stored.contains(reference),
            };
            if executable {
                choices.push(key.clone());
            }
        }
        choices
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
    let legacy_provider_missing = value
        .get("providers")
        .and_then(Value::as_object)
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

/// The user-global OCG Profile, committed in the Core database.
///
/// The Profile is the `profile` and `models` sections of the Core
/// configuration scope. Its revision token is the scope revision it was read
/// at, and every edit commits only against the revision it was computed from.
#[derive(Debug, Clone)]
pub struct ProfileService {
    store: crate::core_store::CoreStore,
    #[cfg(test)]
    legacy_config: Option<PathBuf>,
}

impl ProfileService {
    pub fn new(store: crate::core_store::CoreStore) -> Self {
        Self {
            store,
            #[cfg(test)]
            legacy_config: None,
        }
    }

    /// The pre-database constructor retained for unit tests that provide an
    /// isolated filesystem-backed Profile fixture.
    #[cfg(test)]
    pub fn with_workspace(config: &std::path::Path, _workspace: &std::path::Path) -> Self {
        Self {
            store: crate::core_store::CoreStore,
            legacy_config: Some(config.to_path_buf()),
        }
    }

    pub fn store(&self) -> &crate::core_store::CoreStore {
        &self.store
    }

    pub fn current(&self) -> Result<Option<(Profile, String)>> {
        #[cfg(test)]
        if let Some(path) = &self.legacy_config {
            if !path.exists() {
                return Ok(None);
            }
            let text = std::fs::read_to_string(path)
                .map_err(|error| OcgError::io("cannot read test Profile", error))?;
            let value: Value = serde_yaml_ng::from_str(&text)
                .map_err(|error| OcgError::config(format!("invalid test Profile: {error}")))?;
            return Ok(Some((
                Profile::from_ocg_config(&value)?,
                "test".to_string(),
            )));
        }
        let document = self.store.document(crate::core_store::CORE_SCOPE)?;
        if document.section("profile").is_none() {
            return Ok(None);
        }
        Ok(Some((
            Profile::from_ocg_config(&document.to_value())?,
            document.revision.to_string(),
        )))
    }

    /// The revision of the configuration the Profile was read from, or `None`
    /// before a Profile exists.
    pub(crate) fn content_revision(&self) -> Result<Option<String>> {
        let document = self.store.document(crate::core_store::CORE_SCOPE)?;
        Ok(document
            .section("profile")
            .map(|_| document.revision.to_string()))
    }

    pub(crate) fn budget_config(&self) -> Result<crate::orchestration::budget::BudgetConfig> {
        let document = self.store.document(crate::core_store::CORE_SCOPE)?;
        crate::orchestration::budget::BudgetConfig::from_config(&document.to_value())
    }

    /// Backend-computed execution readiness: model keys that satisfy the
    /// same selection, endpoint, and credential rules as canonical launch.
    /// A missing Profile yields no choices.
    pub fn runnable_choices(&self) -> Result<Vec<String>> {
        let Some((profile, _)) = self.current()? else {
            return Ok(Vec::new());
        };
        Ok(profile.executable_choices(&self.store.credential_names()?))
    }

    /// Create a new, empty Profile. An existing Profile is never replaced.
    pub fn bootstrap(&self) -> Result<Profile> {
        let document = self.store.document(crate::core_store::CORE_SCOPE)?;
        if document.section("profile").is_some() {
            return Err(OcgError::config(
                "OCG Profile already exists; bootstrap never overwrites it",
            ));
        }
        let profile = Profile::new();
        self.commit(document.revision, &profile, "profile-bootstrap")?;
        Ok(profile)
    }

    /// Replace the Profile, provided the configuration is still at the
    /// revision `expected` the edit was computed from.
    pub fn replace(&self, expected: &str, profile: &Profile) -> Result<String> {
        profile.validate()?;
        let expected = expected.parse::<u64>().map_err(|_| {
            OcgError::ConfigurationConflict(
                "OCG Profile changed since it was read; refresh before editing".to_string(),
            )
        })?;
        self.commit(expected, profile, "profile-edit")
    }

    /// Store a new credential together with the Profile edit that references
    /// it, in one transaction: both commit or neither does. The edit is
    /// computed from the current Profile and commits only against the
    /// revision it was computed from. `edit` receives the stored credential
    /// names and returns the name its credential is stored under.
    pub fn edit_with_credential<T>(
        &self,
        secret: &str,
        edit: impl FnOnce(&mut Profile, &std::collections::BTreeSet<String>) -> Result<(String, T)>,
    ) -> Result<(String, T)> {
        if secret.is_empty() {
            return Err(OcgError::config("credential value cannot be empty"));
        }
        let (mut profile, revision) = self
            .current()?
            .ok_or_else(|| OcgError::config("profile must be bootstrapped first"))?;
        let stored = self.store.credential_names()?;
        let (reference, output) = edit(&mut profile, &stored)?;
        crate::core_store::validate_credential_name(&reference)?;
        profile.validate()?;
        let expected = revision.parse::<u64>().map_err(|_| {
            OcgError::ConfigurationConflict(
                "OCG Profile changed since it was read; refresh before editing".to_string(),
            )
        })?;
        let revision =
            self.commit_also(expected, &profile, "profile-credential", |transaction| {
                crate::core_store::store_credential(transaction, &reference, secret, false)
            })?;
        Ok((revision, output))
    }

    fn commit(&self, expected: u64, profile: &Profile, purpose: &str) -> Result<String> {
        self.commit_also(expected, profile, purpose, |_| Ok(()))
    }

    fn commit_also(
        &self,
        expected: u64,
        profile: &Profile,
        purpose: &str,
        also: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<()>,
    ) -> Result<String> {
        let sections = as_ocg_config(profile)?;
        let command_id = format!("{purpose}-{}", uuid::Uuid::now_v7());
        let store_path = self.store.path();
        let committed = self
            .store
            .commit_with(
                crate::core_store::ConfigChange {
                    scope: crate::core_store::CORE_SCOPE,
                    command_id: &command_id,
                    principal: "operator",
                    expected_revision: expected,
                    sections: vec![
                        ("profile", Some(sections["profile"].clone())),
                        ("models", Some(sections["models"].clone())),
                    ],
                },
                |candidate| crate::config::validate_document(candidate, &store_path),
                also,
            )
            .map_err(|error| match error {
                OcgError::ConfigurationConflict(_) => OcgError::ConfigurationConflict(
                    "OCG Profile changed since it was read; refresh before editing".to_string(),
                ),
                other => other,
            })?;
        Ok(committed.revision.to_string())
    }
}
