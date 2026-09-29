//! Input and output contracts for executable Calls.
//!
//! These schemas are independent from MCP protocol schemas. They are built
//! and evaluated from in-memory JSON values only; no external reference
//! resolver is configured or used.

use crate::error::{OcgError, Result};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

fn invalid(message: impl Into<String>) -> OcgError {
    OcgError::config(message)
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CallInput {
    pub arguments: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CallOutput {
    pub result: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CallContract {
    pub input: CallInput,
    pub output: CallOutput,
}

pub fn schema_for<T: JsonSchema>() -> Result<Value> {
    let schema = serde_json::to_value(schemars::schema_for!(T))
        .map_err(|error| invalid(format!("serialize Call schema: {error}")))?;
    reject_external_refs(&schema)?;
    Ok(schema)
}

pub fn input_schema() -> Result<Value> {
    schema_for::<CallInput>()
}

pub fn output_schema() -> Result<Value> {
    schema_for::<CallOutput>()
}

pub fn validate_input(value: &Value) -> Result<()> {
    validate(&input_schema()?, value, "input")
}

pub fn validate_output(value: &Value) -> Result<()> {
    validate(&output_schema()?, value, "output")
}

fn validate(schema: &Value, value: &Value, direction: &str) -> Result<()> {
    let validator = jsonschema::options()
        .build(schema)
        .map_err(|error| invalid(format!("compile Call {direction} schema: {error}")))?;
    validator.validate(value).map_err(|error| {
        invalid(format!(
            "Call {direction} schema validation failed: {error}"
        ))
    })
}

fn reject_external_refs(value: &Value) -> Result<()> {
    match value {
        Value::Object(object) => {
            if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
                if !reference.starts_with('#') {
                    return Err(invalid("external Call schema references are not allowed"));
                }
            }
            for child in object.values() {
                reject_external_refs(child)?;
            }
        }
        Value::Array(array) => {
            for child in array {
                reject_external_refs(child)?;
            }
        }
        _ => {}
    }
    Ok(())
}
