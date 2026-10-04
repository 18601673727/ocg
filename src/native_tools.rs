//! OCG-owned Native Tool Plane.
//!
//! The registry in this module is the only definition of the built-in tools.
//! It owns their schemas, permission classes, capability requirements and
//! executor bindings. OpenAI-compatible function definitions are projections
//! of those definitions; they are not the canonical tool model.

mod evidence;
pub mod openai_projection;
mod validation;

use crate::edit;
use crate::error::{OcgError, Result};
use crate::orchestration::call_schema;
use crate::orchestration::domain::{AttemptAuthority, DomainRepository, EffectIntentKind};
use crate::process::{CaptureRunner, ProcessExit, SystemCaptureRunner};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub const TOOL_OUTPUT_CAP: usize = 64 * 1024;
const FILE_READ_CONTENT_CAP: usize = TOOL_OUTPUT_CAP / 8;
pub const TOOL_STDERR_CAP: usize = 32 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionClass {
    ReadOnly,
    FilesystemWrite,
    ProcessExec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeToolExecutorBinding {
    FilesystemRead,
    FilesystemList,
    FilesystemSearch,
    ContextSearch,
    ContextRead,
    ContextValidation,
    FilesystemEdit,
    ProcessExec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PermissionPolicy {
    pub read_only: bool,
    pub filesystem_write: bool,
    pub process_exec: bool,
    pub filesystem_capability: bool,
    pub process_capability: bool,
}

impl PermissionPolicy {
    pub const fn allow_all() -> Self {
        Self {
            read_only: true,
            filesystem_write: true,
            process_exec: true,
            filesystem_capability: true,
            process_capability: true,
        }
    }

    pub const fn read_only() -> Self {
        Self {
            read_only: true,
            filesystem_write: false,
            process_exec: false,
            filesystem_capability: true,
            process_capability: false,
        }
    }

    pub fn allows(self, permission: PermissionClass) -> bool {
        match permission {
            PermissionClass::ReadOnly => self.read_only,
            PermissionClass::FilesystemWrite => self.filesystem_write,
            PermissionClass::ProcessExec => self.process_exec,
        }
    }

    pub fn allows_capability(self, capability: &str) -> bool {
        match capability {
            "filesystem" => self.filesystem_capability,
            "process" => self.process_capability,
            _ => false,
        }
    }
}

impl Default for PermissionPolicy {
    fn default() -> Self {
        Self::allow_all()
    }
}

impl PermissionClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read_only",
            Self::FilesystemWrite => "filesystem_write",
            Self::ProcessExec => "process_exec",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolErrorKind {
    InvalidInput,
    PermissionDenied,
    PathEscape,
    Unavailable,
    ExecutionFailure,
    Cancelled,
    OutputLimit,
}

impl ToolErrorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidInput => "invalid_input",
            Self::PermissionDenied => "permission_denied",
            Self::PathEscape => "path_escape",
            Self::Unavailable => "unavailable",
            Self::ExecutionFailure => "execution_failure",
            Self::Cancelled => "cancelled",
            Self::OutputLimit => "output_limit",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolError {
    pub kind: ToolErrorKind,
    pub message: String,
    pub metadata: Value,
}

impl ToolError {
    fn new(kind: ToolErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            metadata: Value::Object(Map::new()),
        }
    }

    fn json(&self) -> Value {
        json!({
            "kind": self.kind.as_str(),
            "message": self.message,
            "metadata": self.metadata,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolResult {
    pub success: bool,
    pub output: Value,
    pub truncated: bool,
    pub metadata: Value,
    pub error: Option<ToolError>,
}

impl ToolResult {
    pub fn success(output: Value) -> Self {
        Self {
            success: true,
            output,
            truncated: false,
            metadata: Value::Object(Map::new()),
            error: None,
        }
    }

    fn failure(error: ToolError) -> Self {
        Self {
            success: false,
            output: Value::Null,
            truncated: false,
            metadata: Value::Object(Map::new()),
            error: Some(error),
        }
    }

    pub fn to_value(&self) -> Value {
        json!({
            "success": self.success,
            "output": self.output,
            "truncated": self.truncated,
            "metadata": self.metadata,
            "error": self.error.as_ref().map(ToolError::json),
        })
    }

    pub fn tool_message_content(&self) -> String {
        self.to_value().to_string()
    }

    pub(crate) fn validate_response(response: &str, success: bool) -> Result<Self> {
        if response.len() > TOOL_OUTPUT_CAP {
            return Err(OcgError::config("native tool result exceeds output cap"));
        }
        let result: Self = serde_json::from_str(response)
            .map_err(|error| OcgError::config(format!("invalid native tool result: {error}")))?;
        if result.success != success
            || result.success != result.error.is_none()
            || !result.metadata.is_object()
            || result.error.as_ref().is_some_and(|error| {
                error.kind == ToolErrorKind::Cancelled
                    || error.message.is_empty()
                    || !error.metadata.is_object()
            })
        {
            return Err(OcgError::config("invalid native tool result outcome"));
        }
        call_schema::validate_output(&json!({"result": result.to_value()}))?;
        Ok(result)
    }

    /// Bound the complete canonical result, including structured metadata and
    /// error details. Individual executors cap their streams too, but this
    /// final boundary prevents a combination of bounded fields from growing
    /// beyond the single result budget sent back to an LLM.
    pub fn bounded(mut self) -> Self {
        let encoded = self.to_value().to_string();
        if encoded.len() <= TOOL_OUTPUT_CAP {
            return self;
        }
        let preview_cap = TOOL_OUTPUT_CAP / 2;
        let (preview, _) = bounded_text(encoded.as_bytes(), preview_cap);
        self.output = json!({
            "preview": preview,
            "original_bytes": encoded.len(),
            "truncated": true,
            "remaining": true
        });
        self.truncated = true;
        self.metadata = json!({
            "remaining": true,
            "output_cap": TOOL_OUTPUT_CAP
        });
        self
    }
}

#[derive(Debug, Clone)]
pub struct NativeToolDefinition {
    pub name: &'static str,
    pub description: &'static str,
    pub parameters: Value,
    pub permission: PermissionClass,
    pub capability: &'static str,
    pub executor: NativeToolExecutorBinding,
    /// Other names a model plausibly emits for this tool, in addition to the
    /// canonical name and its OpenAI wire name.
    ///
    /// The registry owns this list because the registry owns every other tool
    /// fact. Recovery never learns a tool name from anywhere else, so it holds
    /// no knowledge of any specific alias and cannot drift from the registry.
    pub aliases: &'static [&'static str],
    /// The argument field naming the resource this tool acts on, when it has
    /// one.
    ///
    /// Declaring it is what makes deterministic argument recovery sound: a
    /// resource may only be recovered for a field the registry identifies as
    /// that tool's target. No other field is ever inferred.
    pub target_field: Option<&'static str>,
    /// Argument fields this tool also answers to, mapping the accepted
    /// spelling to the canonical one.
    ///
    /// Models carry tool conventions from other agents, so a model may send
    /// `path` to a tool whose schema says `file`. Declaring the accepted
    /// spelling lets the Runtime normalize it deterministically instead of
    /// spending a reasoning turn to learn a field name.
    pub argument_aliases: &'static [(&'static str, &'static str)],
}

pub struct NativeToolRegistry;

impl NativeToolRegistry {
    /// The one definition of every built-in tool.
    pub fn definitions() -> Vec<NativeToolDefinition> {
        vec![
            NativeToolDefinition {
                name: "filesystem.read",
                description: "Read a bounded UTF-8 file inside the current Project root. offset and limit count bytes, not lines. Each read returns at most 8192 bytes. Omit limit for the default bounded read; use metadata.nextOffset to continue a truncated read.",
                parameters: json!({"type":"object","additionalProperties":false,"required":["path"],"properties":{"path":{"type":"string"},"offset":{"type":"integer","minimum":0,"description":"Zero-based byte offset, not a line number. Default: 0."},"limit":{"type":"integer","minimum":1,"description":"Maximum bytes to read, not lines. Omit to use the default bounded read."}}}),
                permission: PermissionClass::ReadOnly,
                capability: "filesystem",
                executor: NativeToolExecutorBinding::FilesystemRead,
                aliases: &["read", "read_file", "view", "cat"],
                target_field: Some("path"),
                argument_aliases: &[("file", "path"), ("filePath", "path")],
            },
            NativeToolDefinition {
                name: "filesystem.list",
                description: "List bounded structured entries in a directory inside the current Project root.",
                parameters: json!({"type":"object","additionalProperties":false,"required":["path"],"properties":{"path":{"type":"string"}}}),
                permission: PermissionClass::ReadOnly,
                capability: "filesystem",
                executor: NativeToolExecutorBinding::FilesystemList,
                aliases: &["list", "ls", "list_dir", "list_directory", "readdir"],
                target_field: Some("path"),
                argument_aliases: &[("directory", "path"), ("dir", "path")],
            },
            NativeToolDefinition {
                name: "filesystem.search",
                description: "Search file contents with an rg regular expression under a Project-relative directory. path must be a directory, not a file. Use process.exec with rg argv to search one file.",
                parameters: json!({"type":"object","additionalProperties":false,"required":["query"],"properties":{"query":{"type":"string"},"path":{"type":"string"}}}),
                permission: PermissionClass::ReadOnly,
                capability: "filesystem",
                executor: NativeToolExecutorBinding::FilesystemSearch,
                aliases: &["search", "grep", "ripgrep", "find_in_files"],
                target_field: Some("path"),
                argument_aliases: &[("directory", "path"), ("dir", "path"), ("pattern", "query")],
            },
            NativeToolDefinition {
                name: "context.search",
                description: "Search reusable Project-local indexed text. Refreshes changed files on demand. query is plain text: all case-insensitive word tokens must occur in the same bounded chunk. path restricts results to a Project-relative directory. Returns line ranges, snippets, content revisions, freshness and refresh counters. Use filesystem.search for live regex search.",
                parameters: json!({"type":"object","additionalProperties":false,"required":["query"],"properties":{"query":{"type":"string","description":"Plain text word tokens, maximum 512 bytes."},"path":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":50}}}),
                permission: PermissionClass::ReadOnly,
                capability: "filesystem",
                executor: NativeToolExecutorBinding::ContextSearch,
                aliases: &[],
                target_field: Some("path"),
                argument_aliases: &[],
            },
            NativeToolDefinition {
                name: "context.read",
                description: "Read 1..16 revision-validated evidence ranges from context.search in one Call. Each item requires a Project-relative path, the search sha256 revision, and 1-based inclusive line_start/line_end (at most 400 lines). Reads and hashes each unique file once, returns ordered per-item ok/stale/missing outcomes. Stale evidence requires a new context.search. Content is bounded to 8192 bytes per item and the total result cap; inspect truncated. Use filesystem.read for reads without a revision.",
                parameters: json!({"type":"object","additionalProperties":false,"required":["items"],"properties":{"items":{"type":"array","description":"Between 1 and 16 evidence ranges.","items":{"type":"object","additionalProperties":false,"required":["path","line_start","line_end","revision"],"properties":{"path":{"type":"string"},"line_start":{"type":"integer","minimum":1,"maximum":4294967295u64},"line_end":{"type":"integer","minimum":1,"maximum":4294967295u64},"revision":{"type":"string","description":"Exact sha256: followed by 64 lowercase hexadecimal characters from context.search."}}}}}}),
                permission: PermissionClass::ReadOnly,
                capability: "filesystem",
                executor: NativeToolExecutorBinding::ContextRead,
                aliases: &[],
                target_field: None,
                argument_aliases: &[],
            },
            NativeToolDefinition {
                name: "context.validation",
                description: "Discover durable validation facts for this Project source revision. Optional program + exact args + Project-relative cwd filters cargo check/build or git diff --check; omit these to list recent evidence. Each fact includes passed/failed, applicability, source/environment fingerprints and producing Job/Attempt/Call. Stale facts do not validate current sources. Only bounded metadata probes run; validation is never executed or skipped. Inspect applicability/reusable and component fingerprints: unproven Cargo input closure and unknown inputs are conservative. Legacy facts have unknown applicability.",
                parameters: json!({"type":"object","additionalProperties":false,"properties":{"program":{"type":"string","enum":["cargo","git"]},"args":{"type":"array","items":{"type":"string"}},"cwd":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":20}}}),
                permission: PermissionClass::ReadOnly,
                capability: "filesystem",
                executor: NativeToolExecutorBinding::ContextValidation,
                aliases: &[],
                target_field: None,
                argument_aliases: &[],
            },
            NativeToolDefinition {
                name: "filesystem.edit",
                description: "Apply a transactional Robust Edit inside the current Project root.",
                parameters: json!({"type":"object","additionalProperties":false,"required":["operation","file"],"properties":{"operation":{"type":"string","enum":["replace","insertBefore","insertAfter","append"]},"file":{"type":"string"},"expectedRevision":{"type":"string"},"oldString":{"type":"string"},"old_string":{"type":"string"},"anchor":{"type":"string"},"newString":{"type":"string"},"content":{"type":"string"}}}),
                permission: PermissionClass::FilesystemWrite,
                capability: "filesystem",
                executor: NativeToolExecutorBinding::FilesystemEdit,
                aliases: &["edit", "edit_file", "patch", "write_file"],
                target_field: Some("file"),
                // `path` is the field name other coding agents use for the
                // edited file, and it is the one models most often send instead
                // of `file`.
                argument_aliases: &[("path", "file"), ("filename", "file"), ("target", "file")],
            },
            NativeToolDefinition {
                name: "process.exec",
                description: "Execute one direct executable with argv inside the current Project root. cwd must be relative (use . for the root). Shell interpreters, -c scripts, pipes, redirects and command chains are not supported. Supply each argument separately; use sed -n START,ENDp to read lines.",
                parameters: json!({"type":"object","additionalProperties":false,"required":["program"],"properties":{"program":{"type":"string"},"args":{"type":"array","items":{"type":"string"}},"cwd":{"type":"string","description":"Project-relative directory; use . for the root. Absolute paths are rejected."}}}),
                permission: PermissionClass::ProcessExec,
                capability: "process",
                executor: NativeToolExecutorBinding::ProcessExec,
                aliases: &["bash", "shell", "sh", "run", "run_command", "execute"],
                target_field: None,
                argument_aliases: &[("command", "program"), ("cmd", "program")],
            },
        ]
    }

    /// Look a tool up by its exact canonical name.
    pub fn get(name: &str) -> Option<NativeToolDefinition> {
        Self::definitions()
            .into_iter()
            .find(|tool| tool.name == name)
    }

    /// Look a tool up by its canonical name or by any registry-declared alias.
    ///
    /// This is the only name lookup that is allowed to succeed on something
    /// other than the canonical name, so the accepted surface stays owned by
    /// the registry.
    pub fn get_by_alias(name: &str) -> Option<(NativeToolDefinition, NameMatch)> {
        let definitions = Self::definitions();
        let exact = definitions
            .iter()
            .find(|tool| tool.name == name)
            .cloned();
        if let Some(definition) = exact {
            return Some((definition, NameMatch::Exact));
        }
        let alias = definitions
            .iter()
            .find(|tool| tool.aliases.contains(&name))
            .cloned();
        if let Some(definition) = alias {
            return Some((definition, NameMatch::Alias));
        }
        // A normalized match is accepted only when it is unique. Two tools that
        // fold to the same spelling are not a match at all; picking one would be
        // a guess about which tool a model meant.
        let normalized = normalize_name(name);
        let mut folded: Vec<_> = definitions
            .into_iter()
            .filter(|tool| {
                normalize_name(tool.name) == normalized
                    || tool
                        .aliases
                        .iter()
                        .any(|alias| normalize_name(alias) == normalized)
            })
            .collect();
        if folded.len() == 1 {
            return Some((folded.remove(0), NameMatch::Normalized));
        }
        None
    }

    /// Every canonical tool name, in registry order.
    pub fn names() -> Vec<&'static str> {
        Self::definitions()
            .into_iter()
            .map(|tool| tool.name)
            .collect()
    }
}

/// How confidently a requested tool name matched a registry entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameMatch {
    /// The requested name is the canonical name.
    Exact,
    /// The requested name is a registry-declared alias.
    Alias,
    /// The requested name matched only after case and separator folding.
    Normalized,
}

/// Fold case and the separators models vary on, so `FileSystem.Edit`,
/// `filesystem-edit` and `filesystem_edit` resolve to one registry entry.
///
/// Folding is symmetric and total: it never invents a name, it only refuses to
/// treat spelling variants as different tools.
fn normalize_name(name: &str) -> String {
    name.chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .map(|character| character.to_ascii_lowercase())
        .collect()
}

#[derive(Debug, Clone)]
pub struct ProjectRoot {
    root: PathBuf,
}

impl ProjectRoot {
    pub fn new(root: &Path) -> Result<Self> {
        let root = root
            .canonicalize()
            .map_err(|error| OcgError::io("cannot canonicalize Project root", error))?;
        if !root.is_dir() {
            return Err(OcgError::config("Project root is not a directory"));
        }
        Ok(Self { root })
    }

    pub fn path(&self) -> &Path {
        &self.root
    }

    pub fn resolve_existing(&self, raw: &str) -> std::result::Result<PathBuf, ToolError> {
        let relative = relative_path(raw)?;
        let path = self.root.join(relative);
        let resolved = path
            .canonicalize()
            .map_err(|_| ToolError::new(ToolErrorKind::ExecutionFailure, "path does not exist"))?;
        if !resolved.starts_with(&self.root) {
            return Err(ToolError::new(
                ToolErrorKind::PathEscape,
                "path escapes the Project root",
            ));
        }
        Ok(resolved)
    }

    pub fn resolve_for_create(&self, raw: &str) -> std::result::Result<PathBuf, ToolError> {
        let relative = relative_path(raw)?;
        let path = self.root.join(relative);
        let parent = path.parent().ok_or_else(|| {
            ToolError::new(ToolErrorKind::PathEscape, "path has no Project parent")
        })?;
        let parent = parent.canonicalize().map_err(|_| {
            ToolError::new(
                ToolErrorKind::ExecutionFailure,
                "parent directory does not exist",
            )
        })?;
        if !parent.starts_with(&self.root) {
            return Err(ToolError::new(
                ToolErrorKind::PathEscape,
                "path escapes the Project root",
            ));
        }
        if path.exists() {
            let resolved = path.canonicalize().map_err(|_| {
                ToolError::new(ToolErrorKind::PathEscape, "path cannot be resolved")
            })?;
            if !resolved.starts_with(&self.root) {
                return Err(ToolError::new(
                    ToolErrorKind::PathEscape,
                    "path escapes the Project root",
                ));
            }
        }
        Ok(path)
    }
}

fn relative_path(raw: &str) -> std::result::Result<PathBuf, ToolError> {
    if raw.trim().is_empty() {
        return Err(ToolError::new(
            ToolErrorKind::InvalidInput,
            "path must not be empty",
        ));
    }
    let path = Path::new(raw);
    if path.is_absolute() {
        return Err(ToolError::new(
            ToolErrorKind::PathEscape,
            "path must be relative and contain no '..' components",
        ));
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => normalized.push(value),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(ToolError::new(
                    ToolErrorKind::PathEscape,
                    "path must be relative and contain no '..' components",
                ));
            }
        }
    }
    if normalized.as_os_str().is_empty() {
        normalized.push(".");
    }
    Ok(normalized)
}

fn bounded_text(bytes: &[u8], cap: usize) -> (String, bool) {
    let truncated = bytes.len() > cap;
    let bytes = &bytes[..bytes.len().min(cap)];
    (String::from_utf8_lossy(bytes).into_owned(), truncated)
}

pub struct NativeToolExecutor {
    root: ProjectRoot,
    runner: Box<dyn CaptureRunner>,
}

impl NativeToolExecutor {
    pub fn new(root: &Path) -> Result<Self> {
        Ok(Self {
            root: ProjectRoot::new(root)?,
            runner: Box::new(SystemCaptureRunner),
        })
    }

    pub fn with_runner(root: &Path, runner: Box<dyn CaptureRunner>) -> Result<Self> {
        Ok(Self {
            root: ProjectRoot::new(root)?,
            runner,
        })
    }

    pub fn execute(
        &self,
        name: &str,
        arguments: &Value,
        permission: PermissionClass,
        policy: PermissionPolicy,
        cancelled: &AtomicBool,
    ) -> ToolResult {
        self.execute_cancellable(name, arguments, permission, policy, &|| {
            cancelled.load(Ordering::SeqCst)
        })
    }

    fn execute_cancellable(
        &self,
        name: &str,
        arguments: &Value,
        permission: PermissionClass,
        policy: PermissionPolicy,
        cancelled: &dyn Fn() -> bool,
    ) -> ToolResult {
        let Some(definition) = NativeToolRegistry::get(name) else {
            return ToolResult::failure(ToolError::new(
                ToolErrorKind::InvalidInput,
                "unknown native tool",
            ));
        };
        if definition.permission != permission {
            return ToolResult::failure(ToolError::new(
                ToolErrorKind::PermissionDenied,
                "tool permission class mismatch",
            ));
        }
        if !policy.allows(permission) || !policy.allows_capability(definition.capability) {
            return ToolResult::failure(ToolError::new(
                ToolErrorKind::PermissionDenied,
                format!(
                    "permission or capability '{}' is not granted",
                    definition.capability
                ),
            ));
        }
        if cancelled() {
            return ToolResult::failure(ToolError::new(
                ToolErrorKind::Cancelled,
                "Attempt was cancelled before tool execution",
            ));
        }
        if let Err(error) = validate_parameters(&definition.parameters, arguments) {
            return ToolResult::failure(ToolError::new(
                ToolErrorKind::InvalidInput,
                error.to_string(),
            ));
        }
        let result = match definition.executor {
            NativeToolExecutorBinding::FilesystemRead => self.read(arguments),
            NativeToolExecutorBinding::FilesystemList => self.list(arguments, cancelled),
            NativeToolExecutorBinding::FilesystemSearch => self.search(arguments, cancelled),
            NativeToolExecutorBinding::ContextSearch => self.context_search(arguments, cancelled),
            NativeToolExecutorBinding::ContextRead => evidence::read(&self.root, arguments, cancelled),
            NativeToolExecutorBinding::ContextValidation => {
                validation::query(&self.root, arguments, cancelled)
            }
            NativeToolExecutorBinding::FilesystemEdit => self.edit(arguments, cancelled),
            NativeToolExecutorBinding::ProcessExec => self.exec(arguments, cancelled),
        };
        if cancelled() {
            return ToolResult::failure(ToolError::new(
                ToolErrorKind::Cancelled,
                "Attempt was cancelled during tool execution",
            ));
        }
        result.bounded()
    }

    fn read(&self, arguments: &Value) -> ToolResult {
        let path = match arguments.get("path").and_then(Value::as_str) {
            Some(path) => match self.root.resolve_existing(path) {
                Ok(path) => path,
                Err(error) => return ToolResult::failure(error),
            },
            None => {
                return ToolResult::failure(ToolError::new(
                    ToolErrorKind::InvalidInput,
                    "path is required",
                ))
            }
        };
        if !path.is_file() {
            return ToolResult::failure(ToolError::new(
                ToolErrorKind::InvalidInput,
                "filesystem.read requires a file",
            ));
        }
        let offset = arguments.get("offset").and_then(Value::as_u64).unwrap_or(0) as usize;
        let limit = arguments
            .get("limit")
            .and_then(Value::as_u64)
            .map(|value| value as usize)
            .unwrap_or(FILE_READ_CONTENT_CAP);
        // Leave room for JSON escaping and pagination metadata in the result cap.
        let cap = limit.min(FILE_READ_CONTENT_CAP);
        let mut file = match fs::File::open(&path) {
            Ok(file) => file,
            Err(error) => {
                return ToolResult::failure(ToolError::new(
                    ToolErrorKind::ExecutionFailure,
                    error.to_string(),
                ))
            }
        };
        let file_len = match file.metadata() {
            Ok(metadata) => metadata.len(),
            Err(error) => {
                return ToolResult::failure(ToolError::new(
                    ToolErrorKind::ExecutionFailure,
                    error.to_string(),
                ))
            }
        };
        let start = (offset as u64).min(file_len);
        if let Err(error) = file.seek(SeekFrom::Start(start)) {
            return ToolResult::failure(ToolError::new(
                ToolErrorKind::ExecutionFailure,
                error.to_string(),
            ));
        }
        let mut bytes = Vec::with_capacity(cap.saturating_add(1));
        if let Err(error) = file.take(cap as u64 + 1).read_to_end(&mut bytes) {
            return ToolResult::failure(ToolError::new(
                ToolErrorKind::ExecutionFailure,
                error.to_string(),
            ));
        }
        let truncated = bytes.len() > cap || start.saturating_add(bytes.len() as u64) < file_len;
        let (content, _) = bounded_text(&bytes, cap);
        ToolResult {
            success: true,
            output: json!({"path": relative_display(&self.root, &path), "content": content}),
            truncated,
            metadata: json!({
                "remaining": truncated,
                "offset": start,
                "nextOffset": start.saturating_add(bytes.len().min(cap) as u64),
            }),
            error: None,
        }
    }

    fn list(&self, arguments: &Value, cancelled: &dyn Fn() -> bool) -> ToolResult {
        let raw = match arguments.get("path").and_then(Value::as_str) {
            Some(raw) => raw,
            None => {
                return ToolResult::failure(ToolError::new(
                    ToolErrorKind::InvalidInput,
                    "path is required",
                ))
            }
        };
        let path = match self.root.resolve_existing(raw) {
            Ok(path) => path,
            Err(error) => return ToolResult::failure(error),
        };
        if !path.is_dir() {
            return ToolResult::failure(ToolError::new(
                ToolErrorKind::InvalidInput,
                "filesystem.list requires a directory",
            ));
        }
        let mut entries = Vec::new();
        let iterator = match fs::read_dir(&path) {
            Ok(iterator) => iterator,
            Err(error) => {
                return ToolResult::failure(ToolError::new(
                    ToolErrorKind::ExecutionFailure,
                    error.to_string(),
                ))
            }
        };
        let mut truncated = false;
        for entry in iterator {
            if cancelled() {
                return ToolResult::failure(ToolError::new(
                    ToolErrorKind::Cancelled,
                    "Attempt was cancelled during filesystem.list",
                ));
            }
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    return ToolResult::failure(ToolError::new(
                        ToolErrorKind::ExecutionFailure,
                        error.to_string(),
                    ))
                }
            };
            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(error) => {
                    return ToolResult::failure(ToolError::new(
                        ToolErrorKind::ExecutionFailure,
                        error.to_string(),
                    ))
                }
            };
            let kind = if file_type.is_symlink() {
                "symlink"
            } else if file_type.is_dir() {
                "directory"
            } else {
                "file"
            };
            let target_within_root = if file_type.is_symlink() {
                entry
                    .path()
                    .canonicalize()
                    .map(|target| target.starts_with(self.root.path()))
                    .unwrap_or(false)
            } else {
                true
            };
            let value = json!({"path": relative_display(&self.root, &entry.path()), "name": entry.file_name().to_string_lossy(), "kind": kind, "accessible": target_within_root});
            if serde_json::to_vec(&entries)
                .map(|bytes| bytes.len())
                .unwrap_or(TOOL_OUTPUT_CAP + 1)
                + value.to_string().len()
                > TOOL_OUTPUT_CAP
            {
                truncated = true;
                break;
            }
            entries.push(value);
        }
        ToolResult {
            success: true,
            output: json!({"path": relative_display(&self.root, &path), "entries": entries}),
            truncated,
            metadata: json!({"remaining": truncated}),
            error: None,
        }
    }

    fn search(&self, arguments: &Value, cancelled: &dyn Fn() -> bool) -> ToolResult {
        let query = arguments.get("query").and_then(Value::as_str).unwrap_or("");
        if query.is_empty() {
            return ToolResult::failure(ToolError::new(
                ToolErrorKind::InvalidInput,
                "query must not be empty",
            ));
        }
        let cwd = match arguments.get("path").and_then(Value::as_str) {
            Some(raw) => match self.root.resolve_existing(raw) {
                Ok(path) => path,
                Err(error) => return ToolResult::failure(error),
            },
            None => self.root.path().to_path_buf(),
        };
        if !cwd.is_dir() {
            return ToolResult::failure(ToolError::new(
                ToolErrorKind::InvalidInput,
                "search path must be a directory",
            ));
        }
        let args = vec![
            "--json".to_string(),
            "--no-heading".to_string(),
            "--color=never".to_string(),
            query.to_string(),
            ".".to_string(),
        ];
        let output =
            match self
                .runner
                .run_with_cancellation("rg", &args, &cwd, TOOL_OUTPUT_CAP, cancelled)
            {
                Ok(output) => output,
                Err(error) => {
                    return ToolResult::failure(ToolError::new(
                        ToolErrorKind::Unavailable,
                        error.to_string(),
                    ))
                }
            };
        let (stdout, stdout_truncated) = bounded_text(&output.stdout, TOOL_OUTPUT_CAP);
        let (stderr, stderr_truncated) = bounded_text(&output.stderr, TOOL_STDERR_CAP);
        let mut matches = Vec::new();
        for line in stdout.lines() {
            if let Ok(value) = serde_json::from_str::<Value>(line) {
                matches.push(value);
            }
        }
        let truncated = output.truncated() || stdout_truncated || stderr_truncated;
        if cancelled() {
            return ToolResult::failure(ToolError::new(
                ToolErrorKind::Cancelled,
                "Attempt was cancelled during search",
            ));
        }
        let no_match = matches!(output.exit, ProcessExit::Code(1));
        let success = output.success || no_match;
        ToolResult {
            success,
            output: json!({"matches":matches,"stderr":stderr,"exit":output.exit.label()}),
            truncated,
            metadata: json!({"remaining":truncated,"stderr_truncated":stderr_truncated}),
            error: if success {
                None
            } else {
                Some(ToolError::new(
                    ToolErrorKind::ExecutionFailure,
                    "rg returned a non-zero exit status",
                ))
            },
        }
    }

    fn context_search(&self, arguments: &Value, cancelled: &dyn Fn() -> bool) -> ToolResult {
        let raw = arguments.get("path").and_then(Value::as_str).unwrap_or(".");
        let directory = match self.root.resolve_existing(raw) {
            Ok(path) if path.is_dir() => path,
            Ok(_) => {
                return ToolResult::failure(ToolError::new(
                    ToolErrorKind::InvalidInput,
                    "search path must be a directory",
                ))
            }
            Err(error) => return ToolResult::failure(error),
        };
        let prefix = directory
            .strip_prefix(self.root.path())
            .unwrap_or(Path::new(""));
        let query = arguments.get("query").and_then(Value::as_str).unwrap_or("");
        if query.len() > 512 || !query.chars().any(char::is_alphanumeric) {
            return ToolResult::failure(ToolError::new(
                ToolErrorKind::InvalidInput,
                "context query requires word tokens and at most 512 bytes",
            ));
        }
        let limit = arguments.get("limit").and_then(Value::as_u64).unwrap_or(20) as usize;
        match crate::context::fulltext::search(
            self.root.path(),
            prefix,
            query,
            limit,
            self.runner.as_ref(),
            cancelled,
        ) {
            Ok(output) => ToolResult::success(output),
            Err(error) => ToolResult::failure(ToolError::new(
                ToolErrorKind::ExecutionFailure,
                error.to_string(),
            )),
        }
    }

    fn edit(&self, arguments: &Value, cancelled: &dyn Fn() -> bool) -> ToolResult {
        let file = match arguments.get("file").and_then(Value::as_str) {
            Some(file) => file,
            None => {
                return ToolResult::failure(ToolError::new(
                    ToolErrorKind::InvalidInput,
                    "file is required",
                ))
            }
        };
        if let Err(error) = self.root.resolve_existing(file) {
            return ToolResult::failure(error);
        }
        let call = match edit::construct_call(arguments) {
            Ok(call) => call,
            Err(error) => {
                return ToolResult::failure(ToolError::new(
                    ToolErrorKind::InvalidInput,
                    format!("Robust Edit construction failed: {error:?}"),
                ))
            }
        };
        if cancelled() {
            return ToolResult::failure(ToolError::new(
                ToolErrorKind::Cancelled,
                "Attempt was cancelled before filesystem.edit persistence",
            ));
        }
        match edit::apply_call(self.root.path(), call) {
            Ok(outcome) => ToolResult::success(
                json!({"file":file,"operation":format!("{:?}", outcome.kind),"previousRevision":outcome.previous_revision,"newRevision":outcome.new_revision,"rebased":outcome.rebased,"retries":outcome.retries,"mechanicallyRepaired":outcome.mechanically_repaired}),
            ),
            Err(error) => ToolResult::failure(ToolError::new(
                ToolErrorKind::ExecutionFailure,
                format!("Robust Edit execution failed: {error:?}"),
            )),
        }
    }

    fn exec(&self, arguments: &Value, cancelled: &dyn Fn() -> bool) -> ToolResult {
        let program = match arguments.get("program").and_then(Value::as_str) {
            Some(program) if !program.is_empty() => program,
            _ => {
                return ToolResult::failure(ToolError::new(
                    ToolErrorKind::InvalidInput,
                    "program is required",
                ))
            }
        };
        if is_shell_program(program) {
            return ToolResult::failure(ToolError::new(
                ToolErrorKind::InvalidInput,
                "process.exec does not invoke a shell; provide a direct executable and argv",
            ));
        }
        let args = arguments
            .get("args")
            .and_then(Value::as_array)
            .map(|args| {
                args.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let cwd = match arguments.get("cwd").and_then(Value::as_str) {
            Some(raw) => match self.root.resolve_existing(raw) {
                Ok(path) => path,
                Err(error) => return ToolResult::failure(error),
            },
            None => self.root.path().to_path_buf(),
        };
        if !cwd.is_dir() {
            return ToolResult::failure(ToolError::new(
                ToolErrorKind::InvalidInput,
                "cwd must be a directory",
            ));
        }
        let output =
            match self
                .runner
                .run_with_cancellation(program, &args, &cwd, TOOL_OUTPUT_CAP, cancelled)
            {
                Ok(output) => output,
                Err(error) => {
                    return ToolResult::failure(ToolError::new(
                        ToolErrorKind::Unavailable,
                        error.to_string(),
                    ))
                }
            };
        if cancelled() {
            return ToolResult::failure(ToolError::new(
                ToolErrorKind::Cancelled,
                "Attempt was cancelled during process execution",
            ));
        }
        let stdout = output.stdout_lossy();
        let stderr = output.stderr_lossy();
        ToolResult {
            success: output.success,
            output: json!({"program":program,"args":args,"cwd":relative_display(&self.root, &cwd),"exit":output.exit.label(),"stdout":stdout,"stderr":stderr}),
            truncated: output.truncated(),
            metadata: json!({"remaining":output.truncated(),"durationMs":output.duration_ms,"exitSuccess":output.success}),
            error: if output.success {
                None
            } else {
                Some(ToolError::new(
                    ToolErrorKind::ExecutionFailure,
                    "process exited with a non-zero status",
                ))
            },
        }
    }
}

fn relative_display(root: &ProjectRoot, path: &Path) -> String {
    let value = path
        .strip_prefix(root.path())
        .unwrap_or(path)
        .to_string_lossy()
        .replace(std::path::MAIN_SEPARATOR, "/");
    if value.is_empty() {
        ".".to_string()
    } else {
        value
    }
}

fn is_shell_program(program: &str) -> bool {
    Path::new(program)
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| matches!(name, "sh" | "bash" | "zsh" | "fish" | "csh" | "ksh"))
}

fn validate_parameters(schema: &Value, value: &Value) -> Result<()> {
    let validator = jsonschema::options()
        .build(schema)
        .map_err(|error| OcgError::config(format!("compile native tool schema: {error}")))?;
    validator
        .validate(value)
        .map_err(|error| OcgError::config(format!("native tool schema validation failed: {error}")))
}

#[derive(Debug, Clone)]
pub struct NativeCallRequest {
    pub tool_call_id: String,
    pub name: String,
    pub arguments: Value,
    pub permission: PermissionClass,
}

/// Admit, claim, execute and complete one Native Tool Call through the
/// canonical Call substrate. Multiple requests are intentionally processed in
/// provider order; no second parallel scheduler is introduced.
pub fn execute_canonical_tool_call(
    domain: &mut DomainRepository,
    authority: &AttemptAuthority,
    executor_id: &str,
    project_root: &Path,
    request: NativeCallRequest,
    policy: PermissionPolicy,
    cancelled: &AtomicBool,
) -> Result<ToolResult> {
    let definition = NativeToolRegistry::get(&request.name);
    let permission = request.permission;
    let effect_kind = if permission == PermissionClass::ReadOnly {
        EffectIntentKind::Idempotent
    } else {
        EffectIntentKind::StrictFenced
    };
    let payload = json!({"kind":"native_tool","tool_call_id":request.tool_call_id,"name":request.name,"arguments":request.arguments}).to_string();
    let payload_value = serde_json::from_str::<Value>(&payload)
        .map_err(|error| OcgError::config(format!("serialize native Call input: {error}")))?;
    call_schema::validate_input(&json!({"arguments":payload_value}))?;
    let call = domain.create_call_with_effect(
        &authority.attempt_id,
        Some(executor_id),
        authority.generation,
        effect_kind,
        &payload,
    )?;
    domain.mark_dispatch_queued(&call.id)?;
    let mut observation = None;
    let result = if definition.is_none() {
        ToolResult::failure(ToolError::new(
            ToolErrorKind::InvalidInput,
            "unknown native tool",
        ))
    } else if definition
        .as_ref()
        .is_some_and(|definition| definition.permission != permission)
    {
        ToolResult::failure(ToolError::new(
            ToolErrorKind::PermissionDenied,
            "tool permission class mismatch",
        ))
    } else if definition.as_ref().is_some_and(|definition| {
        !policy.allows(permission) || !policy.allows_capability(definition.capability)
    }) {
        ToolResult::failure(ToolError::new(
            ToolErrorKind::PermissionDenied,
            "permission or capability is not granted",
        ))
    } else if let Some(definition) = definition.as_ref() {
        if let Err(error) = validate_parameters(&definition.parameters, &request.arguments) {
            ToolResult::failure(ToolError::new(
                ToolErrorKind::InvalidInput,
                error.to_string(),
            ))
        } else if domain.authority(&authority.attempt_id)?.as_ref() != Some(authority) {
            ToolResult::failure(ToolError::new(
                ToolErrorKind::Cancelled,
                "Attempt authority was revoked before side effect",
            ))
        } else {
            domain.start_call(&call.id, &authority.attempt_id, authority.generation)?;
            match NativeToolExecutor::new(project_root) {
                Ok(executor) => {
                    observation = validation::Observation::begin(
                        &executor.root,
                        &request.name,
                        &request.arguments,
                        &|| cancelled.load(Ordering::SeqCst),
                    );
                    executor.execute(
                        &request.name,
                        &request.arguments,
                        permission,
                        policy,
                        cancelled,
                    )
                }
                Err(error) => ToolResult::failure(ToolError::new(
                    ToolErrorKind::ExecutionFailure,
                    error.to_string(),
                )),
            }
        }
    } else {
        ToolResult::failure(ToolError::new(
            ToolErrorKind::InvalidInput,
            "unknown native tool",
        ))
    };
    let serialized = result.to_value().to_string();
    if result.success {
        domain.finish_call(
            &call.id,
            &authority.attempt_id,
            authority.generation,
            &serialized,
        )?;
    } else {
        let failure = result
            .error
            .as_ref()
            .map(|error| format!("{}: {}", error.kind.as_str(), error.message))
            .unwrap_or_else(|| "native tool failed".to_string());
        if domain
            .fail_call(
                &call.id,
                &authority.attempt_id,
                authority.generation,
                &failure,
            )
            .is_err()
        {
            domain.fence_dispatch_intent(&call.id, &failure)?;
        }
    }
    if let Some(observation) = observation {
        if let Ok(root) = ProjectRoot::new(project_root) {
            observation.finish(&root, domain, &call.id, &result, &|| {
                cancelled.load(Ordering::SeqCst)
            });
        }
    }
    Ok(result.bounded())
}

pub fn tool_permission_for(name: &str) -> Option<PermissionClass> {
    NativeToolRegistry::get(name).map(|definition| definition.permission)
}

/// Handler for Native Tool Calls running through bounded execution.
pub struct NativeToolCallHandler {
    project_root: PathBuf,
    permission_policy: PermissionPolicy,
    runtime_shutdown: Arc<AtomicBool>,
}

impl NativeToolCallHandler {
    pub(crate) fn project_root(&self) -> &Path {
        &self.project_root
    }

    pub fn new(
        project_root: PathBuf,
        permission_policy: PermissionPolicy,
        runtime_shutdown: Arc<AtomicBool>,
    ) -> Self {
        Self {
            project_root,
            permission_policy,
            runtime_shutdown,
        }
    }

    pub fn execute_validated_sync(
        &self,
        envelope: crate::orchestration::execution_dispatch::ExecutionEnvelope,
    ) -> Result<serde_json::Value> {
        let mut claimed = false;
        match self.execute_envelope(&envelope, &mut claimed) {
            Ok(output) => Ok(output),
            Err(error) => {
                self.terminalize_error(&envelope, &error.to_string(), claimed)?;
                Err(error)
            }
        }
    }

    fn execute_envelope(
        &self,
        envelope: &crate::orchestration::execution_dispatch::ExecutionEnvelope,
        claimed: &mut bool,
    ) -> Result<Value> {
        let cancelled =
            || self.runtime_shutdown.load(Ordering::SeqCst) || envelope.cancelled.is_cancelled();
        if cancelled() {
            return Err(OcgError::config("cancelled before native tool execution"));
        }

        let mut domain = DomainRepository::open(&self.project_root)?;

        // Verify authority
        let _authority = domain
            .authority(&envelope.attempt_id)?
            .filter(|authority| {
                authority.job_id == envelope.job_id && authority.generation == envelope.generation
            })
            .ok_or_else(|| OcgError::config("native tool Call has stale Attempt authority"))?;

        let call = domain.call(&envelope.call_id)?;
        let intent = domain
            .dispatch_intent(&envelope.call_id)?
            .ok_or_else(|| OcgError::config("native tool Call has no dispatch intent"))?;
        if call.attempt_id != envelope.attempt_id
            || call.generation != envelope.generation
            || call.executor_id != envelope.executor_id
            || call.request != envelope.payload
            || intent.job_id != envelope.job_id
            || intent.attempt_id != envelope.attempt_id
            || intent.generation != envelope.generation
            || intent.executor_id != envelope.executor_id
            || intent.request != envelope.payload
            || !matches!(intent.state.as_str(), "pending" | "queued")
            || intent.effect_state != crate::orchestration::domain::EffectIntentState::NotStarted
        {
            return Err(OcgError::config("native tool envelope is not executable"));
        }

        let input: Value = serde_json::from_str(&envelope.payload)
            .map_err(|error| OcgError::config(format!("invalid native tool payload: {error}")))?;
        call_schema::validate_input(&input)?;
        if input.get("kind").and_then(Value::as_str) != Some("native_tool") {
            return Err(OcgError::config("invalid native tool payload kind"));
        }

        let name = input
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| OcgError::config("native tool payload missing 'name'"))?;

        let arguments = input
            .get("arguments")
            .ok_or_else(|| OcgError::config("native tool payload missing 'arguments'"))?
            .clone();

        let definition = NativeToolRegistry::get(name)
            .ok_or_else(|| OcgError::config(format!("unknown native tool: {name}")))?;

        let permission = definition.permission;

        let executor = NativeToolExecutor::new(&self.project_root)?;
        if cancelled() {
            return Err(OcgError::config("cancelled before native tool claim"));
        }
        domain.start_call(&envelope.call_id, &envelope.attempt_id, envelope.generation)?;
        *claimed = true;
        drop(domain);
        let observation =
            validation::Observation::begin(&executor.root, name, &arguments, &cancelled);
        let result = executor.execute_cancellable(
            name,
            &arguments,
            permission,
            self.permission_policy,
            &cancelled,
        );

        if cancelled()
            || result
                .error
                .as_ref()
                .is_some_and(|error| error.kind == ToolErrorKind::Cancelled)
        {
            return Err(OcgError::config("native tool execution cancelled"));
        }
        let serialized = result.to_value().to_string();
        ToolResult::validate_response(&serialized, result.success)?;
        let output = json!({"result": result.to_value()});
        call_schema::validate_output(&output)?;
        let mut domain = DomainRepository::open(&self.project_root)?;

        if result.success {
            domain.finish_call(
                &envelope.call_id,
                &envelope.attempt_id,
                envelope.generation,
                &serialized,
            )?;
        } else {
            domain.fail_native_tool_call(
                &envelope.call_id,
                &envelope.attempt_id,
                envelope.generation,
                &serialized,
            )?;
        }
        if let Some(observation) = observation {
            observation.finish(
                &executor.root,
                &domain,
                &envelope.call_id,
                &result,
                &cancelled,
            );
        }
        Ok(output)
    }

    fn terminalize_error(
        &self,
        envelope: &crate::orchestration::execution_dispatch::ExecutionEnvelope,
        failure: &str,
        claimed: bool,
    ) -> Result<()> {
        let mut domain = DomainRepository::open(&self.project_root)?;
        let call = domain.call(&envelope.call_id)?;
        if call.attempt_id != envelope.attempt_id
            || call.generation != envelope.generation
            || domain
                .attempt(&call.attempt_id)?
                .is_none_or(|attempt| attempt.job_id != envelope.job_id)
        {
            return Ok(());
        }
        if !matches!(call.state.as_str(), "created" | "running") {
            return Ok(());
        }
        // A redelivery does not own the running actor's settlement.
        if !claimed && call.state == "running" {
            return Ok(());
        }
        let mut failure = failure.to_string();
        let mut end = failure.len().min(4096);
        while !failure.is_char_boundary(end) {
            end -= 1;
        }
        failure.truncate(end);
        let unstarted = call.state == "created"
            && domain.dispatch_intent(&call.id)?.is_some_and(|intent| {
                matches!(intent.state.as_str(), "pending" | "queued")
                    && intent.effect_state
                        == crate::orchestration::domain::EffectIntentState::NotStarted
            });
        if unstarted {
            match domain.fail_unclaimed_call(
                &call.id,
                &envelope.attempt_id,
                envelope.generation,
                &failure,
            ) {
                Ok(true) => return Ok(()),
                Ok(false) => {}
                Err(error) => {
                    tracing::error!(%error, call_id = %call.id, "native tool failure settlement rejected; fencing unfinished Call");
                }
            }
        }
        let current = domain.call(&call.id)?;
        if matches!(current.state.as_str(), "created" | "running")
            && (claimed || current.state == "created")
        {
            // A claimed execution without a validated outcome cannot prove
            // its effect. Fencing also handles authority revoked mid-flight.
            domain.fence_dispatch_intent(&call.id, &failure)?;
        }
        Ok(())
    }
}
