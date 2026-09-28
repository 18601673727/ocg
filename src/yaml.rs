//! YAML helpers used across the configuration pipeline.
//!
//! OCG 0.3 reads YAML only. Everything is still kept as
//! [`serde_json::Value`] so the layered defaults can be deep-merged exactly like
//! the historical JSON builder did; YAML is just the on-disk encoding.

use crate::error::{OcgError, Result};
use serde_json::{Map, Value};
use std::fs;
use std::path::Path;

/// Load a YAML file that must contain a mapping.
pub fn read_yaml_object(path: &Path) -> Result<Value> {
    if !path.is_file() {
        return Err(OcgError::config(format!(
            "missing configuration file: {}",
            path.display()
        )));
    }
    let text = fs::read_to_string(path).map_err(|error| OcgError::read(path, error))?;
    parse_yaml_object(&path.display().to_string(), &text)
}

/// Parse a YAML document that must contain a mapping. `label` is used in error
/// messages (for embedded defaults that is e.g. `config/models.yaml`).
///
/// An empty document, or one that contains only comments, is treated as an
/// empty mapping so a comment-only override file is valid.
pub fn parse_yaml_object(label: &str, text: &str) -> Result<Value> {
    let value: Value = serde_yaml_ng::from_str(text)
        .map_err(|error| OcgError::config(format!("{label} is not valid YAML: {error}")))?;
    match value {
        Value::Null => Ok(Value::Object(Map::new())),
        Value::Object(_) => Ok(value),
        _ => Err(OcgError::config(format!(
            "{label} must contain a YAML mapping"
        ))),
    }
}

/// Serialize a value as human-readable YAML.
pub fn to_yaml_string(value: &Value) -> Result<String> {
    serde_yaml_ng::to_string(value)
        .map_err(|error| OcgError::config(format!("cannot serialize YAML: {error}")))
}
