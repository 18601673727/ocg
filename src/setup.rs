//! First-run setup helpers: provider discovery, filesystem browsing, project initialization.

use crate::error::{OcgError, Result};
use crate::http::HttpTransport;
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
        return Err(OcgError::config(format!(
            "provider endpoint has no scheme: {endpoint}"
        )));
    };
    if !scheme.eq_ignore_ascii_case("https") && !scheme.eq_ignore_ascii_case("http") {
        return Err(OcgError::config(format!(
            "provider endpoint must use http or https, got {scheme}"
        )));
    }
    if rest.split(['/', '?', '#']).next().unwrap_or("").is_empty() {
        return Err(OcgError::config(format!(
            "provider endpoint has no host: {endpoint}"
        )));
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

/// Normalized OCG model representation from discovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredModel {
    /// Provider's model id (upstream)
    pub id: String,
    /// Display label (defaults to id if not available)
    #[serde(default)]
    pub label: Option<String>,
    /// Known metadata (don't infer capabilities)
    #[serde(default)]
    pub metadata: Value,
}

/// The stable OCG identity for a discovered model.
///
/// The upstream id is preserved verbatim; only the separator and the
/// characters that cannot appear in a Profile map key are normalized, so the
/// OCG key never becomes the thing that is sent upstream.
pub fn model_key(provider_key: &str, upstream_model_id: &str) -> String {
    let suffix = upstream_model_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':') {
                c
            } else {
                '_'
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
) -> Result<Vec<DiscoveredModel>> {
    let authorization = format!("Bearer {}", api_key.trim());
    let headers = [
        ("Accept", "application/json"),
        ("Authorization", authorization.as_str()),
    ];
    let response = transport.get_with_headers(models_url, &headers)?;

    if !response.is_success() {
        let preview = String::from_utf8_lossy(&response.body);
        let preview = preview.chars().take(200).collect::<String>();
        let detail = match response.status {
            401 | 403 => "the provider rejected the API key".to_string(),
            404 => "this endpoint does not expose /models".to_string(),
            other => format!("HTTP {other}: {preview}"),
        };
        return Err(OcgError::config(format!(
            "could not list models from {models_url}: {detail}"
        )));
    }

    let value: Value = serde_json::from_slice(&response.body).map_err(|error| {
        OcgError::config(format!("{models_url} returned a response that is not JSON: {error}"))
    })?;

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
        let id = id.trim();
        if id.is_empty() {
            continue;
        }
        // Only fields the provider actually reported are carried through.
        // Capabilities, context window and price are never inferred.
        let mut metadata = serde_json::Map::new();
        for field in ["created", "owned_by", "object", "permission"] {
            if let Some(value) = entry.get(field) {
                metadata.insert(field.to_string(), value.clone());
            }
        }
        models.push(DiscoveredModel {
            id: id.to_string(),
            label: Some(id.to_string()),
            metadata: Value::Object(metadata),
        });
    }

    if models.is_empty() {
        return Err(OcgError::config(format!(
            "{models_url} returned no usable models"
        )));
    }
    Ok(models)
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
