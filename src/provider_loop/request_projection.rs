use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{json, Map, Value};
use std::collections::BTreeSet;
use std::ops::Range;

const CONTENT_BYTES: usize = 4096;
const MIN_EXCERPT_BYTES: usize = 128;
const VERBATIM_ROUNDS: usize = 2;

pub(super) fn project(request: &Value) -> Value {
    let mut projected = request.clone();
    let Some(messages) = request.get("messages").and_then(Value::as_array) else {
        return projected;
    };
    let Some(rounds) = complete_rounds(messages) else {
        return projected;
    };
    for (assistant, results) in rounds
        .iter()
        .take(rounds.len().saturating_sub(VERBATIM_ROUNDS))
    {
        let Some(calls) = messages[*assistant]["tool_calls"].as_array() else {
            continue;
        };
        for index in results.clone() {
            let message = &messages[index];
            let Some(call) = calls
                .iter()
                .find(|call| call["id"] == message["tool_call_id"])
            else {
                continue;
            };
            if let Some(content) = project_content(call, message) {
                projected["messages"][index]["content"] = Value::String(content);
            }
        }
    }
    projected
}

fn complete_rounds(messages: &[Value]) -> Option<Vec<(usize, Range<usize>)>> {
    let mut rounds = Vec::new();
    let mut all_ids = BTreeSet::new();
    let mut index = 0;
    while let Some(message) = messages.get(index) {
        if message["role"] == "tool" {
            return None;
        }
        if let Some(calls) = message.get("tool_calls") {
            let calls = calls.as_array()?;
            if message["role"] != "assistant" {
                return None;
            }
            if calls.is_empty() {
                index += 1;
                continue;
            }
            let mut pending = BTreeSet::new();
            for call in calls {
                let id = call["id"].as_str().filter(|id| !id.is_empty())?;
                if !all_ids.insert(id) || !pending.insert(id) {
                    return None;
                }
            }
            let start = index + 1;
            let end = start.checked_add(calls.len())?;
            for result in messages.get(start..end)? {
                if result["role"] != "tool"
                    || result.get("tool_calls").is_some()
                    || !pending.remove(result["tool_call_id"].as_str()?)
                {
                    return None;
                }
            }
            // Any incomplete or orphaned exchange makes the retention boundary
            // ambiguous, so the entire request stays verbatim in that case.
            rounds.push((index, start..end));
            index = end;
        } else {
            index += 1;
        }
    }
    Some(rounds)
}

fn project_content(call: &Value, message: &Value) -> Option<String> {
    let name = call["function"]["name"].as_str()?;
    let field = match name {
        "filesystem.read" | "filesystem_read" => "content",
        "filesystem.list" | "filesystem_list" => "entries",
        "filesystem.search" | "filesystem_search" => "matches",
        _ => return None,
    };
    // Exempt tools are rejected before even parsing their arbitrary output.
    let original = message["content"].as_str()?;
    if original.len() <= CONTENT_BYTES || call["type"] != "function" {
        return None;
    }
    let arguments = parse_unambiguous(call["function"]["arguments"].as_str()?)?;
    let arguments_object = arguments.as_object()?;
    let mut result = parse_unambiguous(original)?;
    let object = result.as_object()?;
    if !only_keys(
        object,
        &["success", "output", "truncated", "metadata", "error"],
    ) || object.len() != 5
        || result["success"] != true
        || !result["error"].is_null()
        || !result["truncated"].is_boolean()
    {
        return None;
    }
    let output = result["output"].as_object()?;
    let metadata = result["metadata"].as_object()?;
    if metadata.get("remaining")?.as_bool()? != result["truncated"].as_bool()? {
        return None;
    }
    match field {
        "content" => {
            if !only_keys(output, &["path", "content"])
                || !only_keys(metadata, &["remaining", "offset", "nextOffset", "revision"])
                || !valid_path(&result["output"]["path"])
                || !valid_path(&arguments["path"])
                || !only_keys(arguments_object, &["path", "offset", "limit"])
                || !optional_unsigned(&arguments, "offset")
                || !optional_unsigned(&arguments, "limit")
            {
                return None;
            }
            match (metadata.get("offset"), metadata.get("nextOffset")) {
                (Some(offset), Some(next)) if offset.as_u64()? <= next.as_u64()? => {}
                (None, None) => {}
                _ => return None,
            }
            if let Some(revision) = metadata.get("revision") {
                let revision = revision.as_str()?;
                if result["truncated"] != false
                    || metadata.get("offset").is_some_and(|offset| offset != 0)
                    || revision.len() != 64
                    || !revision.bytes().all(|byte| byte.is_ascii_hexdigit())
                {
                    return None;
                }
            }
            let content = result["output"]["content"].as_str()?.to_owned();
            result["provider_projection"] = json!({"kind": "filesystem_observation_v1", "field": field, "content_omitted": true});
            project_text(&mut result, &content)?;
        }
        "entries" => {
            if !only_keys(output, &["path", "entries"])
                || !only_keys(metadata, &["remaining"])
                || !valid_path(&result["output"]["path"])
                || !valid_path(&arguments["path"])
                || !only_keys(arguments_object, &["path"])
            {
                return None;
            }
            let entries = result["output"]["entries"].as_array()?.clone();
            if !entries.iter().all(valid_entry) {
                return None;
            }
            project_items(&mut result, field, &entries, |_| false)?;
        }
        "matches" => {
            if !only_keys(output, &["matches", "stderr", "exit"])
                || !only_keys(metadata, &["remaining", "stderr_truncated"])
                || !result["output"]["stderr"].is_string()
                || !metadata.get("stderr_truncated")?.is_boolean()
                || !matches!(result["output"]["exit"].as_str()?, "exit 0" | "exit 1")
                || !only_keys(arguments_object, &["path", "query"])
                || !arguments["query"]
                    .as_str()
                    .is_some_and(|query| !query.is_empty())
                || arguments
                    .get("path")
                    .is_some_and(|path| !path.is_null() && !valid_path(path))
            {
                return None;
            }
            let events = result["output"]["matches"].as_array()?.clone();
            if !events.iter().all(valid_search_event) {
                return None;
            }
            // Keep all rg lifecycle/statistics events and stderr intact: only
            // complete match/context observations may be omitted.
            project_items(&mut result, field, &events, |event| {
                !matches!(event["type"].as_str(), Some("match" | "context"))
            })?;
        }
        _ => return None,
    }
    let encoded = result.to_string();
    (encoded.len() <= CONTENT_BYTES
        && encoded.len() < original.len()
        && Value::String(encoded.clone()).to_string().len()
            < Value::String(original.to_owned()).to_string().len())
    .then_some(encoded)
}

fn only_keys(object: &Map<String, Value>, keys: &[&str]) -> bool {
    object.keys().all(|key| keys.contains(&key.as_str()))
}

fn parse_unambiguous(text: &str) -> Option<Value> {
    serde_json::from_str::<UnambiguousJson>(text)
        .ok()
        .map(|value| value.0)
}

struct UnambiguousJson(Value);

impl<'de> Deserialize<'de> for UnambiguousJson {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct JsonVisitor;
        impl<'de> Visitor<'de> for JsonVisitor {
            type Value = UnambiguousJson;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("JSON without duplicate object keys")
            }

            fn visit_bool<E: de::Error>(self, value: bool) -> std::result::Result<Self::Value, E> {
                Ok(UnambiguousJson(Value::Bool(value)))
            }

            fn visit_i64<E: de::Error>(self, value: i64) -> std::result::Result<Self::Value, E> {
                Ok(UnambiguousJson(value.into()))
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> std::result::Result<Self::Value, E> {
                Ok(UnambiguousJson(value.into()))
            }

            fn visit_f64<E: de::Error>(self, value: f64) -> std::result::Result<Self::Value, E> {
                serde_json::Number::from_f64(value)
                    .map(|number| UnambiguousJson(Value::Number(number)))
                    .ok_or_else(|| E::custom("invalid JSON number"))
            }

            fn visit_str<E: de::Error>(self, value: &str) -> std::result::Result<Self::Value, E> {
                Ok(UnambiguousJson(Value::String(value.to_owned())))
            }

            fn visit_unit<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
                Ok(UnambiguousJson(Value::Null))
            }

            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(UnambiguousJson(value)) = sequence.next_element()? {
                    values.push(value);
                }
                Ok(UnambiguousJson(Value::Array(values)))
            }

            fn visit_map<A: MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut values = Map::new();
                while let Some((key, UnambiguousJson(value))) =
                    map.next_entry::<String, UnambiguousJson>()?
                {
                    if values.insert(key, value).is_some() {
                        return Err(de::Error::custom("duplicate JSON object key"));
                    }
                }
                Ok(UnambiguousJson(Value::Object(values)))
            }
        }
        deserializer.deserialize_any(JsonVisitor)
    }
}

fn valid_path(path: &Value) -> bool {
    path.as_str().is_some_and(|path| !path.is_empty())
}

fn optional_unsigned(object: &Value, key: &str) -> bool {
    object.as_object().is_some()
        && object
            .get(key)
            .is_none_or(|value| value.is_null() || value.as_u64().is_some())
}

fn valid_entry(entry: &Value) -> bool {
    entry.as_object().is_some_and(|entry| {
        only_keys(entry, &["path", "name", "kind", "accessible"])
            && entry.len() == 4
            && entry.get("path").is_some_and(valid_path)
            && entry.get("name").is_some_and(Value::is_string)
            && matches!(
                entry.get("kind").and_then(Value::as_str),
                Some("file" | "directory" | "symlink")
            )
            && entry.get("accessible").is_some_and(Value::is_boolean)
    })
}

fn valid_search_event(event: &Value) -> bool {
    let Some(object) = event.as_object() else {
        return false;
    };
    if object.len() != 2 || !only_keys(object, &["type", "data"]) || !event["data"].is_object() {
        return false;
    }
    match event["type"].as_str() {
        Some("begin") => valid_search_text(&event["data"]["path"]),
        Some("end") => {
            valid_search_text(&event["data"]["path"]) && event["data"]["stats"].is_object()
        }
        Some("summary") => event["data"]["stats"].is_object(),
        Some("match" | "context") => {
            valid_search_text(&event["data"]["path"])
                && valid_search_text(&event["data"]["lines"])
                && event["data"]["line_number"].as_u64().is_some()
                && event["data"]["absolute_offset"].as_u64().is_some()
                && event["data"]["submatches"]
                    .as_array()
                    .is_some_and(|matches| {
                        matches.iter().all(|item| {
                            valid_search_text(&item["match"])
                                && item["start"]
                                    .as_u64()
                                    .zip(item["end"].as_u64())
                                    .is_some_and(|(start, end)| start <= end)
                        })
                    })
        }
        _ => false,
    }
}

fn valid_search_text(value: &Value) -> bool {
    value.as_object().is_some_and(|object| {
        object.len() == 1
            && (object.get("text").is_some_and(Value::is_string)
                || object.get("bytes").is_some_and(Value::is_string))
    })
}

fn project_text(result: &mut Value, content: &str) -> Option<()> {
    let render = |result: &mut Value, budget: usize| {
        let mut prefix_end = (budget / 2).min(content.len());
        while !content.is_char_boundary(prefix_end) {
            prefix_end -= 1;
        }
        let mut suffix_start = content.len().saturating_sub(budget - prefix_end);
        while !content.is_char_boundary(suffix_start) {
            suffix_start += 1;
        }
        result["output"]["content"] = json!({
            "prefix": &content[..prefix_end],
            "suffix": &content[suffix_start..],
            "original_bytes": content.len(),
            "omitted_bytes": suffix_start.saturating_sub(prefix_end),
        });
        prefix_end + content.len() - suffix_start
    };
    let mut lower = 0;
    let mut upper = CONTENT_BYTES.min(content.len());
    while lower < upper {
        let middle = lower + (upper - lower).div_ceil(2);
        render(result, middle);
        if result.to_string().len() <= CONTENT_BYTES {
            lower = middle;
        } else {
            upper = middle - 1;
        }
    }
    let retained = render(result, lower);
    (retained >= MIN_EXCERPT_BYTES && retained < content.len()).then_some(())
}

fn project_items(
    result: &mut Value,
    field: &str,
    items: &[Value],
    required: impl Fn(&Value) -> bool,
) -> Option<()> {
    let observations = items.iter().filter(|item| !required(item)).count();
    let render = |result: &mut Value, retained: usize| {
        let prefix = retained.div_ceil(2);
        let suffix = retained / 2;
        let mut ordinal = 0;
        let excerpt = items
            .iter()
            .filter(|item| {
                if required(item) {
                    return true;
                }
                let keep = ordinal < prefix || ordinal >= observations.saturating_sub(suffix);
                ordinal += 1;
                keep
            })
            .cloned()
            .collect::<Vec<_>>();
        result["output"][field] = Value::Array(excerpt);
        result["provider_projection"] = json!({
            "kind": "filesystem_observation_v1",
            "field": field,
            "content_omitted": true,
            "original_items": items.len(),
            "omitted_items": observations.saturating_sub(retained),
            "prefix_observations": prefix,
            "suffix_observations": suffix,
        });
    };
    let mut lower = 0;
    let mut upper = observations;
    while lower < upper {
        let middle = lower + (upper - lower).div_ceil(2);
        render(result, middle);
        if result.to_string().len() <= CONTENT_BYTES {
            lower = middle;
        } else {
            upper = middle - 1;
        }
    }
    render(result, lower);
    (lower > 0 && lower < observations).then_some(())
}
