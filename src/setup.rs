//! First-run setup helpers: provider discovery, filesystem browsing, project initialization.

use crate::error::{OcgError, Result};
use crate::http::HttpTransport;
use crate::profile::{CatalogModel, ModelMetadata, ProviderCatalog};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::{Path, PathBuf};

/// Normalize a provider name into a stable internal key.
/// Lowercase, replace non-alphanumeric with underscore, ensure uniqueness if needed.
pub fn normalize_provider_key(name: &str, existing_keys: &[&str]) -> String {
    let base = name
        .chars()
        .map(|c| if c.is_alphanumeric() { c.to_ascii_lowercase() } else { '_' })
        .collect::<String>()
        .trim_matches('_')
        .to_string();
    
    let base = if base.is_empty() { "provider".to_string() } else { base };
    
    // Ensure uniqueness
    if !existing_keys.contains(&base.as_str()) {
        return base;
    }
    
    for i in 2..1000 {
        let candidate = format!("{}_{}", base, i);
        if !existing_keys.contains(&candidate.as_str()) {
            return candidate;
        }
    }
    
    // Fallback with timestamp
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{}_{}", base, timestamp)
}

/// Normalize a provider name into a credential reference name.
/// Uppercase, alphanumeric + underscore only.
pub fn normalize_credential_ref(provider_name: &str, existing_refs: &[&str]) -> String {
    let base = provider_name
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .map(|c| c.to_ascii_uppercase())
        .collect::<String>();
    
    let base = if base.is_empty() { "PROVIDER_KEY".to_string() } else { format!("{}_KEY", base) };
    
    // Ensure uniqueness and Vault validation rules
    if !existing_refs.contains(&base.as_str()) && is_valid_credential_name(&base) {
        return base;
    }
    
    for i in 2..1000 {
        let candidate = format!("{}_{}", base, i);
        if !existing_refs.contains(&candidate.as_str()) && is_valid_credential_name(&candidate) {
            return candidate;
        }
    }
    
    // Fallback
    format!("PROVIDER_KEY_{}", existing_refs.len() + 1)
}

fn is_valid_credential_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 128 {
        return false;
    }
    if name.starts_with("OCG_") {
        return false;
    }
    name.chars().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}

/// Construct the `/models` URL from a user-supplied provider endpoint.
///
/// Accepts either a base URL or a full chat-completions URL, because that is
/// what a human is likely to paste:
/// - `https://api.provider.com/v1/chat/completions` → `https://api.provider.com/v1/models`
/// - `https://api.provider.com/v1` → `https://api.provider.com/v1/models`
/// - `https://api.provider.com` → `https://api.provider.com/v1/models`
pub fn models_url_from_endpoint(endpoint: &str) -> Result<String> {
    Ok(format!("{}/models", api_base_url(endpoint)?))
}

/// Construct the canonical chat-completions endpoint that OCG dispatches to.
///
/// This matters because the runtime uses the Profile endpoint *verbatim* as the
/// request URL: it never appends `/chat/completions` itself. Storing a bare
/// `https://api.provider.com/v1` would therefore POST to a URL that returns
/// 404, so the Profile must hold the full path.
pub fn chat_endpoint_from_base(endpoint: &str) -> Result<String> {
    Ok(format!("{}/chat/completions", api_base_url(endpoint)?))
}

/// The API base URL shared by `/models` and `/chat/completions`.
fn api_base_url(endpoint: &str) -> Result<String> {
    let trimmed = endpoint.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err(OcgError::config("provider endpoint is empty"));
    }
    let Some((scheme, rest)) = trimmed.split_once("://") else {
        return Err(OcgError::config("provider endpoint has no scheme"));
    };
    if !scheme.eq_ignore_ascii_case("https") && !scheme.eq_ignore_ascii_case("http") {
        return Err(OcgError::config(format!(
            "provider endpoint must use http or https, got {scheme}"
        )));
    }
    if rest.split(['/', '?', '#']).next().unwrap_or("").is_empty() {
        return Err(OcgError::config("provider endpoint has no host"));
    }
    let authority = rest.split('/').next().unwrap_or_default();
    if authority.contains('@')
        || rest.contains(['?', '#'])
        || trimmed.chars().any(char::is_whitespace)
    {
        return Err(OcgError::config(
            "provider endpoint must not contain credentials, query, fragment or whitespace",
        ));
    }
    // Already a full OpenAI-compatible path: use it verbatim as the base.
    for suffix in ["/chat/completions", "/completions", "/models"] {
        if let Some(base) = trimmed.strip_suffix(suffix) {
            return Ok(base.to_string());
        }
    }
    // A versioned base such as `/v1` is already the API root.
    let last = trimmed.rsplit('/').next().unwrap_or_default();
    if last.len() >= 2 && last.starts_with('v') && last[1..].chars().all(|c| c.is_ascii_digit()) {
        return Ok(trimmed.to_string());
    }
    // Otherwise the operator gave a host or host+prefix; the OpenAI-compatible
    // API lives under `/v1`.
    Ok(format!("{}/v1", trimmed))
}

/// The stable OCG identity for a discovered model.
///
/// The upstream id is preserved verbatim; only the separator and the
/// characters that cannot appear in a Profile map key are normalized, so the
/// OCG key never becomes the thing that is sent upstream.
pub fn model_key(provider_key: &str, upstream_model_id: &str) -> String {
    let suffix = upstream_model_id
        .bytes()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':') {
                char::from(byte).to_string()
            } else {
                format!("%{byte:02X}")
            }
        })
        .collect::<String>();
    format!("{provider_key}:{suffix}")
}

/// Discover models from an explicit `/models` URL.
pub fn discover_models_url(
    transport: &dyn HttpTransport,
    models_url: &str,
    api_key: &str,
) -> Result<Vec<CatalogModel>> {
    let authorization = format!("Bearer {}", api_key.trim());
    let mut headers = vec![("Accept", "application/json")];
    if !api_key.trim().is_empty() {
        headers.push(("Authorization", authorization.as_str()));
    }
    let response = transport
        .get_with_headers(models_url, &headers)
        .map_err(|_| OcgError::config("could not reach provider model catalog"))?;

    if !response.is_success() {
        let detail = match response.status {
            401 | 403 => "the provider rejected the API key".to_string(),
            404 => "this endpoint does not expose /models".to_string(),
            other => format!("HTTP {other}"),
        };
        return Err(OcgError::config(format!(
            "could not list models from {models_url}: {detail}"
        )));
    }

    let mut value: Value = serde_json::from_slice(&response.body)
        .map_err(|_| OcgError::config("provider returned a response that is not JSON"))?;
    if value
        .get("data")
        .and_then(Value::as_array)
        .is_some_and(|entries| {
            entries.iter().any(|entry| {
                entry
                    .get("id")
                    .and_then(Value::as_str)
                    .is_some_and(|id| !api_key.trim().is_empty() && id.contains(api_key.trim()))
            })
        })
    {
        return Err(OcgError::config(
            "provider returned a credential in its model identity",
        ));
    }
    redact_catalog_secret(&mut value, api_key.trim());

    // The OpenAI-compatible shape is `{"object":"list","data":[...]}`. Anything
    // else is reported rather than guessed at, because a wrong model id only
    // fails later at dispatch time with a far worse message.
    let data = value.get("data").and_then(Value::as_array).ok_or_else(|| {
        OcgError::config(format!(
            "{models_url} did not return an OpenAI-compatible model list (expected an object with a \"data\" array)"
        ))
    })?;

    let mut models = Vec::with_capacity(data.len());
    for entry in data {
        let Some(id) = entry.get("id").and_then(Value::as_str) else {
            continue;
        };
        if id.trim().is_empty() {
            continue;
        }
        if models.iter().any(|model: &CatalogModel| model.id == id) {
            continue;
        }
        models.push(CatalogModel {
            id: id.to_string(),
            label: ["label", "display_name", "name"]
                .iter()
                .find_map(|field| {
                    entry
                        .get(field)
                        .and_then(Value::as_str)
                        .filter(|label| !label.is_empty())
                })
                .unwrap_or(id)
                .to_string(),
            metadata: normalize_model_metadata(entry),
            raw: entry.clone(),
        });
    }

    if models.is_empty() {
        return Err(OcgError::config(format!(
            "{models_url} returned no usable models"
        )));
    }
    Ok(models)
}

fn redact_catalog_secret(value: &mut Value, secret: &str) {
    if secret.is_empty() {
        return;
    }
    match value {
        Value::String(text) if !secret.is_empty() => *text = text.replace(secret, "[redacted]"),
        Value::Array(values) => {
            for value in values {
                redact_catalog_secret(value, secret);
            }
        }
        Value::Object(values) => {
            values.retain(|key, _| {
                !key.contains(secret) && !matches!(key.to_ascii_lowercase().as_str(),
                    "api_key" | "apikey" | "authorization" | "access_token" | "secret" | "token")
            });
            for value in values.values_mut() {
                redact_catalog_secret(value, secret);
            }
        }
        _ => {}
    }
}

pub fn discover_catalog(
    transport: &dyn HttpTransport,
    endpoint: &str,
    api_key: &str,
) -> Result<ProviderCatalog> {
    let models = discover_models_url(transport, &models_url_from_endpoint(endpoint)?, api_key)?;
    let discovered_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| OcgError::config("system time precedes Unix epoch"))?
        .as_secs();
    Ok(ProviderCatalog {
        discovered_at,
        models,
    })
}

fn normalize_model_metadata(entry: &Value) -> ModelMetadata {
    let field = |names: &[&str]| -> Option<&Value> {
        names.iter().find_map(|name| {
            entry
                .get(*name)
                .filter(|value| !value.is_null())
                .or_else(|| {
                    entry
                        .get("capabilities")
                        .and_then(|caps| caps.get(*name))
                        .filter(|value| !value.is_null())
                })
                .or_else(|| {
                    entry
                        .get("metadata")
                        .and_then(|meta| meta.get(*name))
                        .filter(|value| !value.is_null())
                })
        })
    };
    let strings = |names: &[&str]| -> Option<Vec<String>> {
        let values = field(names)?.as_array()?;
        values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
            })
            .collect()
    };
    let string = |names: &[&str]| {
        field(names)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    };
    let boolean = |names: &[&str]| field(names).and_then(Value::as_bool);
    let modalities = strings(&["input_modalities"]).filter(|values| !values.is_empty());
    ModelMetadata {
        variant: string(&["variant"]),
        variants: strings(&["variants"]),
        effort: string(&["effort", "reasoning_effort"]),
        efforts: strings(&["efforts", "reasoning_efforts"]),
        reasoning: boolean(&["reasoning", "supports_reasoning"]),
        fast_mode: boolean(&["fast_mode", "supports_fast_mode"]),
        context_window: field(&["context_window", "context_length", "max_context_length"])
            .and_then(Value::as_u64),
        tools: boolean(&[
            "tools",
            "tool_calling",
            "supports_tools",
            "supports_function_calling",
        ]),
        images: boolean(&["images", "vision", "supports_vision"]).or_else(|| {
            modalities
                .as_ref()
                .map(|values| values.iter().any(|value| value == "image"))
        }),
        multimodal: boolean(&["multimodal", "supports_multimodal"]).or_else(|| {
            modalities
                .as_ref()
                .map(|values| values.iter().any(|value| value != "text"))
        }),
        pricing: field(&["pricing", "price"])
            .filter(|value| value.is_object())
            .cloned(),
    }
}

pub fn catalog_models(
    provider_key: &str,
    catalog: &ProviderCatalog,
) -> Vec<crate::contracts::SetupModel> {
    catalog
        .models
        .iter()
        .map(|model| crate::contracts::SetupModel {
            key: model_key(provider_key, &model.id),
            id: model.id.clone(),
            label: model.label.clone(),
            metadata: model.metadata.clone(),
        })
        .collect()
}

/// List directories in a given path for filesystem browsing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryEntry {
    pub name: String,
    pub path: String,
    pub is_dir: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectoryListing {
    pub current: String,
    pub parent: Option<String>,
    pub entries: Vec<DirectoryEntry>,
}

/// Browse a directory, returning subdirectories only (not files).
pub fn browse_directory(path: &Path) -> Result<DirectoryListing> {
    let path = if path.as_os_str().is_empty() {
        std::env::current_dir().map_err(|e| OcgError::io("get current directory", e))?
    } else {
        path.to_path_buf()
    };
    
    let canonical = std::fs::canonicalize(&path)
        .map_err(|e| OcgError::io("canonicalize path", e))?;
    
    let parent = canonical.parent().map(|p| p.display().to_string());
    
    let mut entries = Vec::new();
    
    let read_dir = std::fs::read_dir(&canonical)
        .map_err(|e| OcgError::io("read directory", e))?;
    
    for entry in read_dir {
        let entry = entry.map_err(|e| OcgError::io("read directory entry", e))?;
        let metadata = entry.metadata().map_err(|e| OcgError::io("read entry metadata", e))?;
        
        // Only include directories
        if metadata.is_dir() {
            let name = entry.file_name().to_string_lossy().to_string();
            // Skip hidden directories on Unix
            #[cfg(unix)]
            if name.starts_with('.') {
                continue;
            }
            
            entries.push(DirectoryEntry {
                name: name.clone(),
                path: entry.path().display().to_string(),
                is_dir: true,
            });
        }
    }
    
    // Sort by name
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    
    Ok(DirectoryListing {
        current: canonical.display().to_string(),
        parent,
        entries,
    })
}

/// Initialize a project by creating the .ocg marker if it doesn't exist.
pub fn initialize_project_marker(root: &Path) -> Result<PathBuf> {
    let canonical = std::fs::canonicalize(root)
        .map_err(|e| OcgError::io("canonicalize project root", e))?;
    
    if !canonical.is_dir() {
        return Err(OcgError::config("project root is not a directory"));
    }
    
    let marker = canonical.join(crate::project::MARKER);
    
    if !marker.exists() {
        std::fs::create_dir_all(&marker)
            .map_err(|e| OcgError::io("create .ocg marker", e))?;
        
        // Create .gitignore in .ocg
        crate::install::ensure_gitignore(&canonical)?;
    }
    
    Ok(canonical)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_provider_key() {
        assert_eq!(normalize_provider_key("OpenAI", &[]), "openai");
        assert_eq!(normalize_provider_key("My Provider!", &[]), "my_provider");
        assert_eq!(normalize_provider_key("openai", &["openai"]), "openai_2");
        assert_eq!(normalize_provider_key("", &[]), "provider");
    }

    #[test]
    fn test_normalize_credential_ref() {
        assert_eq!(normalize_credential_ref("openai", &[]), "OPENAI_KEY");
        assert_eq!(normalize_credential_ref("My Provider", &[]), "MYPROVIDER_KEY");
        assert_eq!(normalize_credential_ref("openai", &["OPENAI_KEY"]), "OPENAI_KEY_2");
    }

    #[test]
    fn test_models_url_from_endpoint() {
        assert_eq!(
            models_url_from_endpoint("https://api.openai.com/v1/chat/completions").unwrap(),
            "https://api.openai.com/v1/models"
        );
        assert_eq!(
            models_url_from_endpoint("https://api.provider.com/v1").unwrap(),
            "https://api.provider.com/v1/models"
        );
        assert_eq!(
            models_url_from_endpoint("https://api.provider.com").unwrap(),
            "https://api.provider.com/v1/models"
        );
    }
}
