use std::sync::Arc;
use std::time::Duration;

use ntex::http::Uri;
use regex::Regex;
use serde::Deserialize;
use serde_json::{Value, json};

use super::tools::{ToolFailure, ToolFuture, ToolHandler, ToolRegistry, ToolRegistryError};
use crate::db::entities::ToolEffect;

pub(crate) const NAMES: [&str; 2] = ["web_fetch", "web_search"];
const TIMEOUT: Duration = Duration::from_secs(30);
const MAX_BODY_BYTES: usize = 2 * 1024 * 1024;
const MAX_REDIRECTS: usize = 10;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FetchArguments {
    url: String,
    #[serde(default)]
    start_char: usize,
    #[serde(default = "default_max_chars")]
    max_chars: usize,
    #[serde(default)]
    format: FetchFormat,
}

fn default_max_chars() -> usize {
    8000
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FetchFormat {
    #[default]
    Text,
    Html,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchArguments {
    query: String,
    #[serde(default = "default_search_limit")]
    limit: usize,
}

fn default_search_limit() -> usize {
    5
}

struct Fetch;

struct Search;

struct Response {
    url: Uri,
    status: u16,
    content_type: String,
    body: Vec<u8>,
    truncated: bool,
}

fn parse_url(url: &str) -> Result<Uri, ToolFailure> {
    let url = url.split('#').next().unwrap_or(url);
    url.parse::<Uri>()
        .ok()
        .filter(|uri| {
            matches!(uri.scheme_str(), Some("http" | "https"))
                && uri.host().is_some_and(|host| !host.is_empty())
                && uri
                    .authority()
                    .is_some_and(|authority| !authority.as_str().contains('@'))
        })
        .ok_or_else(|| {
            ToolFailure::not_applied("use an absolute HTTP or HTTPS URL without credentials")
        })
}

fn redirect_url(current: &Uri, location: &str) -> Result<Uri, ToolFailure> {
    let location = location.split('#').next().unwrap_or(location);
    if location.contains("://") {
        return parse_url(location);
    }
    let scheme = current.scheme_str().unwrap_or("https");
    let authority = current.authority().map_or("", |value| value.as_str());
    if location.starts_with("//") {
        return parse_url(&format!("{scheme}:{location}"));
    }
    let path = if location.is_empty() {
        current
            .path_and_query()
            .map_or("/", |value| value.as_str())
            .to_owned()
    } else if location.starts_with('?') {
        format!("{}{location}", current.path())
    } else if location.starts_with('/') {
        location.to_owned()
    } else {
        let directory = current
            .path()
            .rsplit_once('/')
            .map_or("", |(directory, _)| directory);
        format!("{directory}/{location}")
    };
    let (path, query) = path
        .split_once('?')
        .map_or((path.as_str(), None), |(path, query)| (path, Some(query)));
    let mut segments = Vec::new();
    for segment in path.split('/').skip(1) {
        match segment {
            "." => {}
            ".." => {
                segments.pop();
            }
            segment => segments.push(segment),
        }
    }
    if path.ends_with("/.") || path.ends_with("/..") {
        segments.push("");
    }
    let path = segments.join("/");
    let query = query.map_or_else(String::new, |query| format!("?{query}"));
    parse_url(&format!("{scheme}://{authority}/{path}{query}"))
}

async fn request(url: &str) -> Result<Response, ToolFailure> {
    let mut url = parse_url(url)?;
    ntex::time::timeout(TIMEOUT, async {
        let client = crate::system::http::provider_client(TIMEOUT)
            .await
            .map_err(|_| ToolFailure::not_applied("HTTP client could not be initialized"))?;
        for redirects in 0..=MAX_REDIRECTS {
            let response = client
                .get(url.clone())
                .set_header(
                    "Accept",
                    "text/html, application/json, application/xml, text/plain, */*",
                )
                .set_header("Accept-Encoding", "identity")
                .send()
                .await
                .map_err(|_| ToolFailure::not_applied("HTTP request failed"))?;
            let status = response.status().as_u16();
            if matches!(status, 301 | 302 | 303 | 307 | 308)
                && let Some(location) = response.header("location")
            {
                if redirects == MAX_REDIRECTS {
                    return Err(ToolFailure::not_applied("HTTP redirect limit exceeded"));
                }
                let location = location.to_str().map_err(|_| {
                    ToolFailure::not_applied("HTTP redirect has an invalid location")
                })?;
                url = redirect_url(&url, location)?;
                continue;
            }
            let content_type = response
                .header("content-type")
                .and_then(|value| value.to_str().ok())
                .unwrap_or("")
                .chars()
                .take(256)
                .collect();
            let mut payload = response.take_payload();
            let mut body = Vec::new();
            let mut truncated = false;
            while let Some(chunk) = payload.recv().await {
                let chunk = chunk.map_err(|_| {
                    ToolFailure::not_applied("HTTP response body could not be read")
                })?;
                let remaining = MAX_BODY_BYTES.saturating_sub(body.len());
                body.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
                if chunk.len() > remaining {
                    truncated = true;
                    break;
                }
            }
            return Ok(Response {
                url,
                status,
                content_type,
                body,
                truncated,
            });
        }
        Err(ToolFailure::not_applied("HTTP redirect limit exceeded"))
    })
    .await
    .map_err(|_| ToolFailure::not_applied("HTTP request timed out"))?
}

fn decode_entities(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut remaining = text;
    while let Some(start) = remaining.find('&') {
        output.push_str(&remaining[..start]);
        remaining = &remaining[start..];
        let entity = remaining
            .find(';')
            .filter(|end| *end <= 32)
            .map(|end| (&remaining[1..end], end));
        let decoded = entity.and_then(|(name, _)| match name {
            "amp" => Some('&'),
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" | "#39" => Some('\''),
            "nbsp" => Some(' '),
            name => name
                .strip_prefix("#x")
                .or_else(|| name.strip_prefix("#X"))
                .and_then(|number| u32::from_str_radix(number, 16).ok())
                .or_else(|| {
                    name.strip_prefix('#')
                        .and_then(|number| number.parse().ok())
                })
                .and_then(char::from_u32),
        });
        if let (Some(character), Some((_, end))) = (decoded, entity) {
            output.push(character);
            remaining = &remaining[end + 1..];
        } else {
            output.push('&');
            remaining = &remaining[1..];
        }
    }
    output.push_str(remaining);
    output
}

fn html_text(html: &str) -> Result<String, ToolFailure> {
    let hidden =
        Regex::new(r"(?is)<!--.*?-->|<script\b[^>]*>.*?</script\s*>|<style\b[^>]*>.*?</style\s*>")
            .map_err(|_| {
                ToolFailure::not_applied("HTML text extraction could not be initialized")
            })?;
    let tags = Regex::new(r"(?s)<[^>]*>")
        .map_err(|_| ToolFailure::not_applied("HTML text extraction could not be initialized"))?;
    let visible = hidden.replace_all(html, "");
    let text = decode_entities(&tags.replace_all(&visible, "\n"));
    Ok(text
        .lines()
        .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n"))
}

impl ToolHandler for Fetch {
    fn version(&self) -> &str {
        "web_fetch-1"
    }
    fn effect(&self) -> ToolEffect {
        ToolEffect::ReadOnly
    }
    fn call(&self, arguments: Value) -> ToolFuture<'_> {
        Box::pin(async move {
            let arguments: FetchArguments = serde_json::from_value(arguments)
                .map_err(|_| ToolFailure::not_applied("invalid web_fetch arguments"))?;
            let response = request(&arguments.url).await?;
            ntex::rt::spawn_blocking(move || {
                let source = String::from_utf8_lossy(&response.body);
                let text = match arguments.format {
                    FetchFormat::Text
                        if response.content_type.to_ascii_lowercase().contains("html") =>
                    {
                        html_text(&source)?
                    }
                    FetchFormat::Text | FetchFormat::Html => source.into_owned(),
                };
                let total_chars = text.chars().count();
                let mut result = json!({
                    "url": response.url.to_string(), "status": response.status,
                    "content_type": response.content_type, "content": "",
                    "start_char": arguments.start_char, "total_chars": total_chars,
                    "next_start": null, "body_truncated": response.truncated,
                });
                let content = text
                    .chars()
                    .skip(arguments.start_char)
                    .take(arguments.max_chars)
                    .collect::<String>();
                let next = arguments.start_char.saturating_add(content.chars().count());
                result["content"] = json!(content);
                if next < total_chars {
                    result["next_start"] = json!(next);
                }
                Ok(result)
            })
            .await
            .map_err(|_| ToolFailure::not_applied("HTTP content could not be processed"))?
        })
    }
}

fn encode_query(query: &str) -> String {
    let mut encoded = String::new();
    for byte in query.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            const HEX: &[u8] = b"0123456789ABCDEF";
            encoded.push('%');
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 15)]));
        }
    }
    encoded
}

fn xml_field(item: &str, name: &str) -> Option<String> {
    let (_, tail) = item.split_once(&format!("<{name}>"))?;
    let (value, _) = tail.split_once(&format!("</{name}>"))?;
    let value = value
        .strip_prefix("<![CDATA[")
        .and_then(|value| value.strip_suffix("]]>"))
        .unwrap_or(value);
    Some(decode_entities(value))
}

impl ToolHandler for Search {
    fn version(&self) -> &str {
        "web_search-1"
    }
    fn effect(&self) -> ToolEffect {
        ToolEffect::ReadOnly
    }
    fn call(&self, arguments: Value) -> ToolFuture<'_> {
        Box::pin(async move {
            let arguments: SearchArguments = serde_json::from_value(arguments)
                .map_err(|_| ToolFailure::not_applied("invalid web_search arguments"))?;
            if arguments.query.trim().is_empty() {
                return Err(ToolFailure::not_applied("search query must not be empty"));
            }
            let response = request(&format!(
                "https://www.bing.com/search?format=rss&q={}",
                encode_query(&arguments.query)
            ))
            .await?;
            if !(200..300).contains(&response.status) {
                return Err(ToolFailure::not_applied(format!(
                    "public search returned HTTP {}",
                    response.status
                )));
            }
            let source = String::from_utf8_lossy(&response.body);
            if response.truncated || !source.contains("<rss") || !source.contains("</rss>") {
                return Err(ToolFailure::not_applied(
                    "public search did not return a complete RSS result; try again or use web_fetch with a known URL",
                ));
            }
            let mut results = Vec::new();
            let mut truncated = false;
            for item in source.split("<item>").skip(1) {
                let item = item.split_once("</item>").map_or(item, |(item, _)| item);
                let (Some(title), Some(url)) = (xml_field(item, "title"), xml_field(item, "link"))
                else {
                    return Err(ToolFailure::not_applied(
                        "public search returned an invalid result",
                    ));
                };
                parse_url(&url)?;
                let entry = json!({"title": title, "url": url, "snippet": xml_field(item, "description").unwrap_or_default()});
                results.push(entry);
                if results.len() > arguments.limit {
                    results.pop();
                    truncated = true;
                    break;
                }
            }
            Ok(json!({"provider":"bing", "results":results, "truncated":truncated}))
        })
    }
}

pub(crate) fn register(registry: &ToolRegistry) -> Result<(), ToolRegistryError> {
    registry.register("web_fetch", Some("Access the internet: GET an HTTP or HTTPS webpage or API using OCG's configured proxy. Returns the final URL, HTTP status, content type and UTF-8 content. HTML becomes readable text by default; format=html returns source. Follow next_start with start_char to read more (the URL is fetched again). body_truncated means the downloaded body exceeded 2 MiB; use a smaller resource or the site's pagination. No JavaScript execution. Use URLs without credentials."), json!({
        "type":"object", "additionalProperties":false,
        "properties":{
            "url":{"type":"string", "minLength":1, "maxLength":2048},
            "start_char":{"type":"integer", "minimum":0, "maximum":MAX_BODY_BYTES},
            "max_chars":{"type":"integer", "minimum":1, "maximum":16000},
            "format":{"type":"string", "enum":["text", "html"]}
        }, "required":["url"]
    }), Arc::new(Fetch))?;
    registry.register("web_search", Some("Search the public web with Bing's RSS search through OCG's configured proxy; no API key is needed. Returns titles, source URLs and snippets. Use web_fetch on result URLs to read and verify sources. Public search may be unavailable or rate limited; it does not execute JavaScript."), json!({
        "type":"object", "additionalProperties":false,
        "properties":{
            "query":{"type":"string", "minLength":1, "maxLength":512},
            "limit":{"type":"integer", "minimum":1, "maximum":10}
        }, "required":["query"]
    }), Arc::new(Search))?;
    for name in NAMES {
        registry.authorize(name)?;
    }
    Ok(())
}
