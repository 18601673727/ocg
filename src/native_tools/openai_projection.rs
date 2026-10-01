//! OpenAI strict function-calling projection of the Native Tool Registry.
//!
//! The Registry owns the canonical tool name and the canonical argument
//! schema, where an optional field is simply absent from `required`. OpenAI
//! strict mode instead requires every property to be listed in `required` and
//! expresses optionality as a nullable type. This module derives that wire
//! shape from the Registry and maps provider tool calls back; it holds no tool
//! facts of its own.

use super::{NativeToolDefinition, NativeToolRegistry};
use crate::error::{OcgError, Result};
use serde_json::{json, Map, Value};

const WIRE_NAME_MAX_LEN: usize = 64;

// Projecting a keyword we do not understand could silently change what the
// provider is allowed to send, so anything outside this set fails closed.
const SUPPORTED_KEYWORDS: &[&str] = &[
    "type",
    "description",
    "properties",
    "required",
    "additionalProperties",
    "items",
    "enum",
    "minimum",
    "maximum",
];

#[derive(Debug, Clone)]
pub struct OpenAiToolProjection {
    tools: Vec<ProjectedTool>,
}

#[derive(Debug, Clone)]
pub struct ProjectedTool {
    wire_name: String,
    definition: NativeToolDefinition,
    strict_parameters: Value,
}

impl OpenAiToolProjection {
    pub fn from_registry() -> Result<Self> {
        let mut tools: Vec<ProjectedTool> = Vec::new();
        for definition in NativeToolRegistry::definitions() {
            let wire_name = wire_name_for(definition.name)?;
            if let Some(existing) = tools.iter().find(|tool| tool.wire_name == wire_name) {
                return Err(OcgError::config(format!(
                    "native tools '{}' and '{}' project to the same OpenAI wire name '{wire_name}'",
                    existing.definition.name, definition.name
                )));
            }
            let strict_parameters = strict_schema(&definition.parameters, definition.name)?;
            tools.push(ProjectedTool {
                wire_name,
                definition,
                strict_parameters,
            });
        }
        Ok(Self { tools })
    }

    pub fn tools(&self) -> Vec<Value> {
        self.tools.iter().map(ProjectedTool::function).collect()
    }

    pub fn resolve(&self, wire_name: &str) -> Result<&ProjectedTool> {
        self.tools
            .iter()
            .find(|tool| tool.wire_name == wire_name)
            .ok_or_else(|| {
                OcgError::config(format!(
                    "provider requested unknown native tool wire name '{wire_name}'"
                ))
            })
    }
}

impl ProjectedTool {
    pub fn canonical_name(&self) -> &'static str {
        self.definition.name
    }

    /// Validate wire arguments against the strict schema this projection
    /// generated. This must run before canonical normalization.
    pub fn validate_wire_arguments(&self, wire_arguments: &Value) -> Result<()> {
        let validator = jsonschema::options()
            .build(&self.strict_parameters)
            .map_err(|error| {
                OcgError::config(format!(
                    "compile strict wire schema for tool '{}': {error}",
                    self.wire_name
                ))
            })?;
        validator.validate(wire_arguments).map_err(|error| {
            OcgError::config(format!(
                "wire arguments for tool '{}' violate OpenAI strict schema: {error}",
                self.wire_name
            ))
        })
    }

    /// Undo the strict-mode nullable widening. Only a `null` this projection
    /// introduced is dropped; a `null` the canonical schema itself does not
    /// accept is kept so canonical validation rejects it.
    pub fn canonical_arguments(&self, wire_arguments: &Value) -> Value {
        canonical_value(&self.definition.parameters, wire_arguments)
    }

    fn function(&self) -> Value {
        json!({
            "type": "function",
            "function": {
                "name": self.wire_name,
                "description": self.definition.description,
                "parameters": self.strict_parameters,
                "strict": true
            }
        })
    }
}

fn wire_name_for(canonical_name: &str) -> Result<String> {
    let wire_name = canonical_name.replace('.', "_");
    let valid = !wire_name.is_empty()
        && wire_name.len() <= WIRE_NAME_MAX_LEN
        && wire_name.chars().all(|character| {
            character.is_ascii_alphanumeric() || character == '_' || character == '-'
        });
    if valid {
        Ok(wire_name)
    } else {
        Err(OcgError::config(format!(
            "native tool '{canonical_name}' has no valid OpenAI wire name"
        )))
    }
}

fn strict_schema(schema: &Value, tool: &str) -> Result<Value> {
    let object = schema
        .as_object()
        .ok_or_else(|| unsupported(tool, "schema must be an object"))?;
    if let Some(keyword) = object
        .keys()
        .find(|keyword| !SUPPORTED_KEYWORDS.contains(&keyword.as_str()))
    {
        return Err(unsupported(tool, &format!("keyword '{keyword}'")));
    }
    let mut strict = object.clone();
    if let Some(items) = object.get("items") {
        strict.insert("items".to_string(), strict_schema(items, tool)?);
    }
    let Some(properties) = object.get("properties") else {
        if type_names(object).contains(&"object") {
            return Err(unsupported(tool, "object schema without properties"));
        }
        return Ok(Value::Object(strict));
    };
    let properties = properties
        .as_object()
        .ok_or_else(|| unsupported(tool, "properties must be an object"))?;
    if object.get("additionalProperties") != Some(&Value::Bool(false)) {
        return Err(unsupported(
            tool,
            "object schema must set additionalProperties to false",
        ));
    }
    let required = required_names(object, properties, tool)?;
    let mut strict_properties = Map::new();
    for (name, property) in properties {
        let mut projected = strict_schema(property, tool)?;
        if !required.contains(&name.as_str()) {
            make_nullable(&mut projected, tool)?;
        }
        strict_properties.insert(name.clone(), projected);
    }
    strict.insert(
        "required".to_string(),
        Value::Array(properties.keys().cloned().map(Value::String).collect()),
    );
    strict.insert("properties".to_string(), Value::Object(strict_properties));
    Ok(Value::Object(strict))
}

fn required_names<'a>(
    object: &'a Map<String, Value>,
    properties: &Map<String, Value>,
    tool: &str,
) -> Result<Vec<&'a str>> {
    let Some(required) = object.get("required") else {
        return Ok(Vec::new());
    };
    let required = required
        .as_array()
        .ok_or_else(|| unsupported(tool, "required must be an array"))?;
    required
        .iter()
        .map(|name| match name.as_str() {
            Some(name) if properties.contains_key(name) => Ok(name),
            Some(name) => Err(unsupported(
                tool,
                &format!("required property '{name}' is not declared"),
            )),
            None => Err(unsupported(tool, "required entries must be strings")),
        })
        .collect()
}

fn make_nullable(schema: &mut Value, tool: &str) -> Result<()> {
    let object = schema
        .as_object_mut()
        .ok_or_else(|| unsupported(tool, "property schema must be an object"))?;
    match object.get_mut("type") {
        Some(Value::String(name)) => {
            if name != "null" {
                let name = std::mem::take(name);
                object.insert("type".to_string(), json!([name, "null"]));
            }
        }
        Some(Value::Array(names)) => {
            if !names.iter().any(|name| name == "null") {
                names.push(Value::String("null".to_string()));
            }
        }
        _ => return Err(unsupported(tool, "optional property without a type")),
    }
    if let Some(Value::Array(values)) = object.get_mut("enum") {
        if !values.iter().any(Value::is_null) {
            values.push(Value::Null);
        }
    }
    Ok(())
}

fn canonical_value(schema: &Value, value: &Value) -> Value {
    match (schema.get("properties").and_then(Value::as_object), value) {
        (Some(properties), Value::Object(fields)) => {
            let required: Vec<&str> = schema
                .get("required")
                .and_then(Value::as_array)
                .map(|names| names.iter().filter_map(Value::as_str).collect())
                .unwrap_or_default();
            let mut canonical = Map::new();
            for (name, field) in fields {
                match properties.get(name) {
                    Some(property)
                        if field.is_null()
                            && !required.contains(&name.as_str())
                            && !allows_null(property) => {}
                    Some(property) => {
                        canonical.insert(name.clone(), canonical_value(property, field));
                    }
                    None => {
                        canonical.insert(name.clone(), field.clone());
                    }
                }
            }
            Value::Object(canonical)
        }
        (None, Value::Array(elements)) => match schema.get("items") {
            Some(items) => Value::Array(
                elements
                    .iter()
                    .map(|element| canonical_value(items, element))
                    .collect(),
            ),
            None => value.clone(),
        },
        _ => value.clone(),
    }
}

fn allows_null(schema: &Value) -> bool {
    match schema.get("type") {
        Some(Value::String(name)) => name == "null",
        Some(Value::Array(names)) => names.iter().any(|name| name == "null"),
        _ => false,
    }
}

fn type_names(object: &Map<String, Value>) -> Vec<&str> {
    match object.get("type") {
        Some(Value::String(name)) => vec![name.as_str()],
        Some(Value::Array(names)) => names.iter().filter_map(Value::as_str).collect(),
        _ => Vec::new(),
    }
}

fn unsupported(tool: &str, detail: &str) -> OcgError {
    OcgError::config(format!(
        "native tool '{tool}' cannot be projected to an OpenAI strict schema: {detail}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn projection_from_registry_succeeds() {
        let projection = OpenAiToolProjection::from_registry().unwrap();
        assert!(!projection.tools().is_empty());
        for tool_value in projection.tools() {
            assert_eq!(tool_value["type"], "function");
            assert_eq!(tool_value["function"]["strict"], true);
        }
    }

    #[test]
    fn resolve_known_wire_name() {
        let projection = OpenAiToolProjection::from_registry().unwrap();
        let tool = projection.resolve("filesystem_read").unwrap();
        assert_eq!(tool.canonical_name(), "filesystem.read");
    }

    #[test]
    fn resolve_unknown_wire_name() {
        let projection = OpenAiToolProjection::from_registry().unwrap();
        assert!(projection.resolve("unknown_tool").is_err());
    }

    #[test]
    fn nullable_optional_field_removed() {
        let projection = OpenAiToolProjection::from_registry().unwrap();
        let tool = projection.resolve("filesystem_read").unwrap();
        let wire = json!({"path": "test.txt", "offset": null, "limit": null});
        let canonical = tool.canonical_arguments(&wire);
        assert_eq!(canonical, json!({"path": "test.txt"}));
    }

    #[test]
    fn non_null_optional_field_preserved() {
        let projection = OpenAiToolProjection::from_registry().unwrap();
        let tool = projection.resolve("filesystem_read").unwrap();
        let wire = json!({"path": "test.txt", "offset": 10, "limit": null});
        let canonical = tool.canonical_arguments(&wire);
        assert_eq!(canonical, json!({"path": "test.txt", "offset": 10}));
    }

    #[test]
    fn null_in_required_field_preserved_and_rejected() {
        let projection = OpenAiToolProjection::from_registry().unwrap();
        let tool = projection.resolve("filesystem_read").unwrap();
        let wire = json!({"path": null, "offset": null, "limit": null});
        let canonical = tool.canonical_arguments(&wire);
        assert_eq!(canonical, json!({"path": null}));
        assert!(
            super::super::validate_parameters(&tool.definition.parameters, &canonical).is_err()
        );
    }

    #[test]
    fn strict_schema_requires_every_property_and_widens_optional_ones() {
        let projection = OpenAiToolProjection::from_registry().unwrap();
        for tool in &projection.tools {
            let canonical = &tool.definition.parameters;
            let strict = &tool.strict_parameters;
            let canonical_properties = canonical["properties"].as_object().unwrap();
            let canonical_required: Vec<&str> = canonical
                .get("required")
                .and_then(Value::as_array)
                .map(|names| names.iter().filter_map(Value::as_str).collect())
                .unwrap_or_default();

            let strict_required: Vec<&str> = strict["required"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(Value::as_str)
                .collect();
            let property_names: Vec<&str> =
                canonical_properties.keys().map(String::as_str).collect();
            assert_eq!(strict_required, property_names, "{}", tool.wire_name);
            assert_eq!(strict["additionalProperties"], false, "{}", tool.wire_name);

            for (name, canonical_property) in canonical_properties {
                let strict_property = &strict["properties"][name];
                if canonical_required.contains(&name.as_str()) {
                    assert_eq!(
                        strict_property, canonical_property,
                        "{}.{name}",
                        tool.wire_name
                    );
                } else {
                    assert!(allows_null(strict_property), "{}.{name}", tool.wire_name);
                }
            }
        }
    }

    #[test]
    fn wire_validation_succeeds_with_complete_strict_arguments() {
        let projection = OpenAiToolProjection::from_registry().unwrap();
        let tool = projection.resolve("filesystem_read").unwrap();
        let wire = json!({"path": "test.txt", "offset": null, "limit": null});
        assert!(tool.validate_wire_arguments(&wire).is_ok());
    }

    #[test]
    fn wire_validation_succeeds_with_explicit_null_for_optional() {
        let projection = OpenAiToolProjection::from_registry().unwrap();
        let tool = projection.resolve("filesystem_read").unwrap();
        let wire = json!({"path": "test.txt", "offset": 100, "limit": null});
        assert!(tool.validate_wire_arguments(&wire).is_ok());
    }

    #[test]
    fn wire_validation_fails_when_optional_field_missing() {
        let projection = OpenAiToolProjection::from_registry().unwrap();
        let tool = projection.resolve("filesystem_read").unwrap();
        let wire = json!({"path": "test.txt"});
        assert!(tool.validate_wire_arguments(&wire).is_err());
    }

    #[test]
    fn wire_validation_fails_when_required_field_is_null() {
        let projection = OpenAiToolProjection::from_registry().unwrap();
        let tool = projection.resolve("filesystem_read").unwrap();
        let wire = json!({"path": null, "offset": null, "limit": null});
        assert!(tool.validate_wire_arguments(&wire).is_err());
    }

    #[test]
    fn wire_validation_fails_with_unknown_property() {
        let projection = OpenAiToolProjection::from_registry().unwrap();
        let tool = projection.resolve("filesystem_read").unwrap();
        let wire = json!({"path": "test.txt", "offset": null, "limit": null, "unknown": "value"});
        assert!(tool.validate_wire_arguments(&wire).is_err());
    }

    #[test]
    fn wire_validation_fails_with_wrong_scalar_type() {
        let projection = OpenAiToolProjection::from_registry().unwrap();
        let tool = projection.resolve("filesystem_read").unwrap();
        let wire = json!({"path": 123, "offset": null, "limit": null});
        assert!(tool.validate_wire_arguments(&wire).is_err());
    }

    #[test]
    fn wire_validation_fails_with_wrong_array_item_type() {
        let projection = OpenAiToolProjection::from_registry().unwrap();
        let tool = projection.resolve("process_exec").unwrap();
        let valid = json!({"program": "ls", "args": ["-la"], "cwd": null});
        assert!(tool.validate_wire_arguments(&valid).is_ok());
        let wire = json!({"program": "ls", "args": [123], "cwd": null});
        let error = tool.validate_wire_arguments(&wire).unwrap_err().to_string();
        assert!(error.contains("process_exec"), "{error}");
    }

    #[test]
    fn wire_validation_fails_with_invalid_enum() {
        let projection = OpenAiToolProjection::from_registry().unwrap();
        let tool = projection.resolve("filesystem_edit").unwrap();
        let mut wire = json!({
            "operation": "append",
            "file": "test.txt",
            "expectedRevision": null,
            "oldString": null,
            "old_string": null,
            "anchor": null,
            "newString": null,
            "content": null
        });
        assert!(tool.validate_wire_arguments(&wire).is_ok());
        wire["operation"] = json!("invalid_op");
        assert!(tool.validate_wire_arguments(&wire).is_err());
    }

    #[test]
    fn wire_validation_fails_with_violated_minimum() {
        let projection = OpenAiToolProjection::from_registry().unwrap();
        let tool = projection.resolve("filesystem_read").unwrap();
        let wire = json!({"path": "test.txt", "offset": -1, "limit": null});
        assert!(tool.validate_wire_arguments(&wire).is_err());
    }

    #[test]
    fn canonical_arguments_drops_projection_nulls_after_wire_validation() {
        let projection = OpenAiToolProjection::from_registry().unwrap();
        let tool = projection.resolve("filesystem_read").unwrap();
        let wire = json!({"path": "test.txt", "offset": null, "limit": null});
        assert!(tool.validate_wire_arguments(&wire).is_ok());
        let canonical = tool.canonical_arguments(&wire);
        assert_eq!(canonical, json!({"path": "test.txt"}));
        assert!(super::super::validate_parameters(&tool.definition.parameters, &canonical).is_ok());
    }

    #[test]
    fn wire_invalid_payload_not_rescued_by_normalization() {
        let projection = OpenAiToolProjection::from_registry().unwrap();
        let tool = projection.resolve("filesystem_read").unwrap();
        // The normalized form would pass canonical validation, which is why
        // the wire check has to run first.
        let wire = json!({"path": "test.txt"});
        assert!(tool.validate_wire_arguments(&wire).is_err());
        let canonical = tool.canonical_arguments(&wire);
        assert_eq!(canonical, json!({"path": "test.txt"}));
        assert!(super::super::validate_parameters(&tool.definition.parameters, &canonical).is_ok());
    }
}
