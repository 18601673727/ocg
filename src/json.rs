//! JSON helpers used across the configuration pipeline.
//!
//! Everything is kept as [`serde_json::Value`] so the layered defaults can be
//! deep-merged exactly like the historical Python builder did. The
//! `preserve_order` feature keeps object key order stable and predictable.

use crate::error::{OcgError, Result};
use serde_json::{Map, Value};
use std::fs;
use std::path::Path;

/// Recursively merge `over` onto `base`.
///
/// Objects merge key by key; every other value (including arrays) is replaced
/// by the override. An explicit `null` override keeps an object base (it does
/// not delete it), matching the historical builder.
pub fn deep_merge(base: &Value, over: &Value) -> Value {
    match (base, over) {
        (Value::Object(base_object), Value::Object(over_object)) => {
            let mut result = base_object.clone();
            for (key, value) in over_object {
                let merged = match result.get(key) {
                    Some(existing) => deep_merge(existing, value),
                    None => deep_merge(&Value::Null, value),
                };
                result.insert(key.clone(), merged);
            }
            Value::Object(result)
        }
        (Value::Object(base_object), Value::Null) => Value::Object(base_object.clone()),
        _ => over.clone(),
    }
}

/// Python-like truthiness, used by config fields such as `hidden`, `enabled`
/// and optional descriptions.
pub fn is_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(inner) => *inner,
        Value::Number(number) => number.as_f64().map(|inner| inner != 0.0).unwrap_or(true),
        Value::String(text) => !text.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(object) => !object.is_empty(),
    }
}

/// Render a value the way Python's `str()` would for the cases the config
/// actually uses (strings pass through, everything else uses JSON).
pub fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

/// Load a JSON file that must contain an object.
pub fn read_json_object(path: &Path) -> Result<Value> {
    if !path.is_file() {
        return Err(OcgError::config(format!(
            "missing configuration file: {}",
            path.display()
        )));
    }
    let text = fs::read_to_string(path).map_err(|error| OcgError::read(path, error))?;
    parse_json_object(&path.display().to_string(), &text)
}

/// Parse a JSON document that must contain an object. `label` is used in
/// error messages. OCG's own configuration is YAML; these helpers are
/// retained for genuinely JSON protocols, bridge payloads and state files).
pub fn parse_json_object(label: &str, text: &str) -> Result<Value> {
    let value: Value = serde_json::from_str(text)
        .map_err(|error| OcgError::config(format!("{label} is not valid JSON: {error}")))?;
    if !value.is_object() {
        return Err(OcgError::config(format!(
            "{label} must contain a JSON object"
        )));
    }
    Ok(value)
}

pub fn as_object(value: &Value) -> Option<&Map<String, Value>> {
    value.as_object()
}

pub fn get_object<'a>(value: &'a Value, key: &str) -> Option<&'a Map<String, Value>> {
    value.get(key).and_then(Value::as_object)
}

pub fn get_str<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}
