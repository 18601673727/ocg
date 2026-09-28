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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_a_mapping_and_allows_empty_documents() {
        assert_eq!(parse_yaml_object("x", "a: 1\n").unwrap(), json!({"a": 1}));
        assert_eq!(
            parse_yaml_object("x", "# comment only\n").unwrap(),
            json!({})
        );
        assert!(parse_yaml_object("x", "- 1\n").is_err());
        assert!(parse_yaml_object("x", "a: [\n").is_err());
    }

    #[test]
    fn accepts_json_documents_as_yaml() {
        // JSON is a subset of YAML; legacy-style inline JSON test fixtures must
        // still parse when they live in a `.yaml` file.
        let value = parse_yaml_object("x", "{\"context\": {\"enabled\": false}}").unwrap();
        assert_eq!(value, json!({"context": {"enabled": false}}));
        let value = parse_yaml_object(
            "x",
            "{\"verification\": {\"stages\": {\"fast\": {\"commands\": [\"cargo check\"]}}}}",
        )
        .unwrap();
        assert_eq!(
            value["verification"]["stages"]["fast"]["commands"][0],
            json!("cargo check")
        );
    }

    #[test]
    fn serializes_a_json_value_as_yaml() {
        let text = to_yaml_string(&json!({"profile": {"defaultModel": "selected"}})).unwrap();
        let back: Value = serde_yaml_ng::from_str(&text).unwrap();
        assert_eq!(back, json!({"profile": {"defaultModel": "selected"}}));
    }

    #[test]
    fn shipped_registries_parse_as_mappings() {
        for (label, text) in [
            ("config/base.yaml", include_str!("../config/base.yaml")),
            ("config/models.yaml", include_str!("../config/models.yaml")),
            (
                "config/routing.yaml",
                include_str!("../config/routing.yaml"),
            ),
            (
                "config/permissions.yaml",
                include_str!("../config/permissions.yaml"),
            ),
        ] {
            let value = parse_yaml_object(label, text).unwrap_or_else(|error| panic!("{error}"));
            assert!(value.is_object(), "{label} must be a mapping");
        }
    }
}
