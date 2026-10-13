//! The argument half of admitting a Provider's tool call: the arguments of
//! a call must be one strict JSON object that satisfies the parameter schema
//! of the function its request offered under that name.
//!
//! Admission is scoped to one request. It is not permission to execute: an
//! admitted call has only been found to be one its request offered and
//! allowed, with arguments its declared schema accepts. Whatever executes a
//! call must still check it against OCG's own tool registry, policies and
//! effective permissions.

use std::fmt;

use jsonschema::Validator;
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};

use super::stream::{InvalidToolCall, ToolCallRefusal};

/// The one dialect a parameter schema is read as.
const DRAFT_2020_12: &str = "https://json-schema.org/draft/2020-12/schema";
/// Deepest nesting of subschemas a parameter schema may have.
const MAX_SCHEMA_DEPTH: usize = 32;

/// Compiles the parameter schema of `name`, offered without one when
/// `parameters` is `None`, which admits no call: arguments are never taken
/// as safe for want of a schema. The schema is read as JSON Schema 2020-12
/// and is refused unless every keyword of it is enforced as written: see
/// [`require_supported`]. Formats are asserted, an unknown one is refused,
/// and patterns use a regular-expression engine whose matching time is
/// linear in the input. Nothing is ever fetched to resolve a reference.
pub(crate) fn parameter_validator(
    index: u32,
    name: &str,
    parameters: Option<&Value>,
) -> Result<Validator, InvalidToolCall> {
    let Some(schema) = parameters else {
        return Err(InvalidToolCall::at(
            ToolCallRefusal::MissingSchema,
            index,
            format!("tool `{name}` was offered without a parameter schema"),
        ));
    };
    let unsupported = |problem: String| {
        InvalidToolCall::at(
            ToolCallRefusal::UnsupportedSchema,
            index,
            format!("the parameter schema of `{name}` {problem}"),
        )
    };
    require_supported(schema, "", 0).map_err(unsupported)?;
    jsonschema::draft202012::options()
        .should_validate_formats(true)
        .should_ignore_unknown_formats(false)
        .with_pattern_options(jsonschema::PatternOptions::regex())
        .build(schema)
        .map_err(|error| unsupported(format!("cannot be enforced: {error}")))
}

/// Refuses a schema that uses anything not enforced as written: a keyword
/// outside the 2020-12 vocabularies that are validated, including the
/// content keywords, which only annotate; a base URI of its own; a
/// reference that leaves the schema; another dialect; or nesting deeper
/// than [`MAX_SCHEMA_DEPTH`]. Returns what it found, where.
fn require_supported(schema: &Value, at: &str, depth: usize) -> Result<(), String> {
    if depth > MAX_SCHEMA_DEPTH {
        return Err(format!(
            "nests deeper than {MAX_SCHEMA_DEPTH} levels at `{at}`"
        ));
    }
    let keywords = match schema {
        Value::Bool(_) => return Ok(()),
        Value::Object(keywords) => keywords,
        _ => return Err(format!("has a schema at `{at}` that is not one")),
    };
    for (keyword, value) in keywords {
        let here = format!("{at}/{keyword}");
        match keyword.as_str() {
            "additionalProperties"
            | "contains"
            | "else"
            | "if"
            | "items"
            | "not"
            | "propertyNames"
            | "then"
            | "unevaluatedItems"
            | "unevaluatedProperties" => {
                require_supported(value, &here, depth + 1)?;
            }
            "$defs" | "dependentSchemas" | "patternProperties" | "properties" => {
                let Some(schemas) = value.as_object() else {
                    return Err(format!("has a `{here}` that is not an object"));
                };
                for (name, schema) in schemas {
                    require_supported(schema, &format!("{here}/{name}"), depth + 1)?;
                }
            }
            "allOf" | "anyOf" | "oneOf" | "prefixItems" => {
                let Some(schemas) = value.as_array() else {
                    return Err(format!("has a `{here}` that is not an array"));
                };
                for (position, schema) in schemas.iter().enumerate() {
                    require_supported(schema, &format!("{here}/{position}"), depth + 1)?;
                }
            }
            "$ref" | "$dynamicRef" => {
                if !value
                    .as_str()
                    .is_some_and(|reference| reference.starts_with('#'))
                {
                    return Err(format!(
                        "has a reference at `{here}` that leaves the schema"
                    ));
                }
            }
            "$schema" => {
                if value.as_str() != Some(DRAFT_2020_12) {
                    return Err(format!("declares a dialect other than {DRAFT_2020_12}"));
                }
            }
            // Assertions and annotations whose values are data; the
            // meta-schema checks their form when the schema is compiled.
            "$anchor" | "$comment" | "$dynamicAnchor" | "const" | "default"
            | "dependentRequired" | "deprecated" | "description" | "enum" | "examples"
            | "exclusiveMaximum" | "exclusiveMinimum" | "format" | "maxContains" | "maxItems"
            | "maxLength" | "maxProperties" | "maximum" | "minContains" | "minItems"
            | "minLength" | "minProperties" | "minimum" | "multipleOf" | "pattern" | "readOnly"
            | "required" | "title" | "type" | "uniqueItems" | "writeOnly" => {}
            _ => return Err(format!("uses `{here}`, which is not enforced")),
        }
    }
    Ok(())
}

/// Admits the arguments of the call at `index` to `name` when they are one
/// strict JSON object, see [`parse_strict`], that `parameters` accepts. The
/// arguments are only read: nothing is defaulted, removed, converted or
/// repaired, and the call keeps them exactly as they were sent. A refusal
/// names where the schema refused them, never what they held.
pub(crate) fn admit_arguments(
    index: u32,
    name: &str,
    arguments: &str,
    parameters: &Validator,
) -> Result<(), InvalidToolCall> {
    let invalid = |problem: String| {
        InvalidToolCall::at(
            ToolCallRefusal::InvalidArguments,
            index,
            format!("tool call {index} arguments {problem}"),
        )
    };
    let value = parse_strict(arguments).map_err(invalid)?;
    if !value.is_object() {
        return Err(invalid("are not one JSON object".to_owned()));
    }
    parameters.validate(&value).map_err(|error| {
        invalid(format!(
            "do not satisfy the parameter schema of `{name}` at `{}`",
            error.schema_path()
        ))
    })
}

/// Parses `text` as one I-JSON value (RFC 7493), so that what the schema
/// validates is what any reader of the text will take it to say: no object
/// has a key twice, and every number is exactly the double it is read as.
fn parse_strict(text: &str) -> Result<Value, String> {
    let mut numbers = Vec::new();
    let mut deserializer = serde_json::Deserializer::from_str(text);
    let value = Strict {
        numbers: &mut numbers,
    }
    .deserialize(&mut deserializer)
    .and_then(|value| deserializer.end().map(|()| value))
    .map_err(|error| format!("are not valid JSON: {error}"))?;
    let literals = number_literals(text);
    if literals.len() != numbers.len()
        || !literals
            .iter()
            .zip(&numbers)
            .all(|(literal, number)| is_exact(literal, number))
    {
        return Err("hold a number that is not exactly what it was read as".to_owned());
    }
    Ok(value)
}

/// Builds a [`Value`], refusing a key that one object repeats, and records
/// every number in the order of the text.
struct Strict<'a> {
    numbers: &'a mut Vec<Number>,
}

impl Strict<'_> {
    fn number(self, number: Number) -> Value {
        self.numbers.push(number.clone());
        Value::Number(number)
    }
}

impl<'de> DeserializeSeed<'de> for Strict<'_> {
    type Value = Value;

    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<Value, D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Strict<'_> {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_bool<E: de::Error>(self, value: bool) -> Result<Value, E> {
        Ok(Value::Bool(value))
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Value, E> {
        Ok(self.number(value.into()))
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Value, E> {
        Ok(self.number(value.into()))
    }

    fn visit_f64<E: de::Error>(self, value: f64) -> Result<Value, E> {
        let number = Number::from_f64(value).ok_or_else(|| E::custom("a number is not finite"))?;
        Ok(self.number(number))
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Value, E> {
        Ok(Value::String(value.to_owned()))
    }

    fn visit_string<E: de::Error>(self, value: String) -> Result<Value, E> {
        Ok(Value::String(value))
    }

    fn visit_unit<E: de::Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Value, A::Error> {
        let mut items = Vec::new();
        while let Some(item) = sequence.next_element_seed(Strict {
            numbers: &mut *self.numbers,
        })? {
            items.push(item);
        }
        Ok(Value::Array(items))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut entries: A) -> Result<Value, A::Error> {
        let mut object = Map::new();
        while let Some(key) = entries.next_key::<String>()? {
            if object.contains_key(&key) {
                return Err(de::Error::custom("an object has a key twice"));
            }
            let value = entries.next_value_seed(Strict {
                numbers: &mut *self.numbers,
            })?;
            object.insert(key, value);
        }
        Ok(Value::Object(object))
    }
}

/// The number literals of valid JSON `text`, in order.
fn number_literals(text: &str) -> Vec<&str> {
    let bytes = text.as_bytes();
    let mut literals = Vec::new();
    let mut position = 0;
    while position < bytes.len() {
        match bytes[position] {
            b'"' => {
                position += 1;
                while position < bytes.len() {
                    match bytes[position] {
                        b'\\' => position += 2,
                        b'"' => {
                            position += 1;
                            break;
                        }
                        _ => position += 1,
                    }
                }
            }
            b'-' | b'0'..=b'9' => {
                let start = position;
                while position < bytes.len()
                    && matches!(
                        bytes[position],
                        b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E'
                    )
                {
                    position += 1;
                }
                literals.push(&text[start..position]);
            }
            _ => position += 1,
        }
    }
    literals
}

/// Whether `literal` is exactly the value `number` holds. An integer read
/// as one is; a double is when the literal denotes the same decimal as the
/// double's shortest representation, so a literal the double rounded, such
/// as an integer beyond 2^53 or a fraction with more digits than a double
/// keeps, is not.
fn is_exact(literal: &str, number: &Number) -> bool {
    if number.is_i64() || number.is_u64() {
        return true;
    }
    number.as_f64().is_some_and(|double| {
        let shortest = format!("{double:e}");
        decimal(literal).is_some_and(|written| decimal(&shortest) == Some(written))
    })
}

/// A number literal as its sign, significant digits and decimal exponent,
/// `digits × 10^exponent`, with no leading or trailing zero. Zero has no
/// digits and no sign.
fn decimal(literal: &str) -> Option<(bool, String, i64)> {
    let (negative, unsigned) = match literal.strip_prefix('-') {
        Some(unsigned) => (true, unsigned),
        None => (false, literal),
    };
    let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
        Some(at) => (&unsigned[..at], unsigned[at + 1..].parse::<i64>().ok()?),
        None => (unsigned, 0),
    };
    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let mut exponent = exponent.checked_sub(i64::try_from(fraction.len()).ok()?)?;
    let mut digits = format!("{integer}{fraction}")
        .trim_start_matches('0')
        .to_owned();
    while digits.ends_with('0') {
        digits.pop();
        exponent = exponent.checked_add(1)?;
    }
    if digits.is_empty() {
        return Some((false, digits, 0));
    }
    Some((negative, digits, exponent))
}
