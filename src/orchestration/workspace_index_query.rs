//! `workspace_index_query`: parse one source file with tree-sitter and extract
//! structural information. `outline` returns top-level symbols (and direct
//! members of `impl`/class bodies) with source ranges; `syntax_errors` returns
//! tree-sitter ERROR and MISSING nodes. Languages: Rust, TypeScript/TSX,
//! Python. Positions are 1-based lines and 1-based UTF-16 columns. Results are
//! syntax-only: no type checking, no cross-file resolution, no compiler
//! diagnostics.
//!
//! No persistent state, no parser pools, no caching. Each call makes a fresh
//! `Parser`, parses once with a cooperative deadline, extracts results from
//! one immutable snapshot, and releases all resources. Parsing runs off the
//! async executor on ntex's existing blocking thread pool.

use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tree_sitter::{Language, ParseOptions, Parser, Point};

use crate::orchestration::tools::{
    ToolFailure, ToolFuture, ToolHandler, ToolRegistry, ToolRegistryError,
};
use crate::orchestration::workspace_read::Workspace;
use crate::persistence::ToolEffect;

/// The name the tool is offered and recorded under.
pub const TOOL_NAME: &str = "workspace_index_query";

/// Identifies this handler's behavior in every call it records.
const HANDLER_VERSION: &str = "workspace_index_query-1";

/// The largest file the tool parses. Oversize files are refused at statat,
/// never opened, never read.
const MAX_PARSE_FILE_BYTES: usize = 2 * 1024 * 1024;

/// Longest one parse may run before it is cooperatively aborted.
const PARSE_BUDGET: Duration = Duration::from_secs(2);

/// Backstop item count limits, beyond which items are dropped from the tail.
const MAX_SYMBOLS: usize = 10_000;
const MAX_ERRORS: usize = 1_000;

struct WorkspaceIndexQuery {
    workspace: Workspace,
    rust: Language,
    typescript: Language,
    tsx: Language,
    python: Language,
    max_result_bytes: usize,
}

impl ToolHandler for WorkspaceIndexQuery {
    fn version(&self) -> &str {
        HANDLER_VERSION
    }

    fn effect(&self) -> ToolEffect {
        ToolEffect::ReadOnly
    }

    fn call(&self, arguments: Value) -> ToolFuture<'_> {
        Box::pin(async move {
            let path = arguments
                .get("path")
                .and_then(Value::as_str)
                .ok_or_else(|| ToolFailure::not_applied("missing or invalid path"))?;

            let operation = arguments
                .get("operation")
                .and_then(Value::as_str)
                .ok_or_else(|| ToolFailure::not_applied("missing or invalid operation"))?;

            if operation != "outline" && operation != "syntax_errors" {
                return Err(ToolFailure::not_applied("unsupported operation"));
            }

            // Select language by extension
            let language = match detect_language(path) {
                Some(lang) => lang,
                None => return Err(ToolFailure::not_applied("unsupported file type")),
            };

            let language_obj = match language {
                LanguageKind::Rust => self.rust.clone(),
                LanguageKind::TypeScript => self.typescript.clone(),
                LanguageKind::Tsx => self.tsx.clone(),
                LanguageKind::Python => self.python.clone(),
            };

            // Read and validate the file
            let bytes = self
                .workspace
                .read_binary(path, MAX_PARSE_FILE_BYTES)
                .map_err(|refusal| ToolFailure::not_applied(refusal.to_string()))?;

            let source = String::from_utf8(bytes)
                .map_err(|_| ToolFailure::not_applied("the file is not UTF-8 text"))?;

            // Capture owned data for the blocking task
            let operation_owned = operation.to_string();
            let max_result_bytes = self.max_result_bytes;

            // Run parsing and extraction in a blocking task
            let result = ntex::rt::spawn_blocking(move || {
                parse_and_extract(
                    &source,
                    language,
                    language_obj,
                    &operation_owned,
                    max_result_bytes,
                )
            })
            .await
            .map_err(|_| ToolFailure::uncertain("the blocking task panicked or was cancelled"))??;

            Ok(result)
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LanguageKind {
    Rust,
    TypeScript,
    Tsx,
    Python,
}

fn detect_language(path: &str) -> Option<LanguageKind> {
    if let Some(ext_start) = path.rfind('.') {
        let ext = &path[ext_start..];
        match ext {
            ".rs" => Some(LanguageKind::Rust),
            ".ts" => Some(LanguageKind::TypeScript),
            ".tsx" => Some(LanguageKind::Tsx),
            ".py" | ".pyi" => Some(LanguageKind::Python),
            _ => None,
        }
    } else {
        None
    }
}

fn parse_and_extract(
    source: &str,
    language: LanguageKind,
    language_obj: Language,
    operation: &str,
    max_result_bytes: usize,
) -> Result<Value, ToolFailure> {
    // Parse with cooperative deadline
    let deadline = Instant::now() + PARSE_BUDGET;
    let mut on_progress = |_state: &tree_sitter::ParseState| {
        if Instant::now() >= deadline {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    };
    let options = ParseOptions::new().progress_callback(&mut on_progress);

    let mut parser = Parser::new();
    parser
        .set_language(&language_obj)
        .map_err(|e| ToolFailure::not_applied(format!("failed to set language: {}", e)))?;

    let tree = parser
        .parse_with_options(
            &mut |offset: usize, _point| &source.as_bytes()[offset.min(source.len())..],
            None,
            Some(options),
        )
        .ok_or_else(|| ToolFailure::not_applied("parse exceeded the 2s budget"))?;

    // Precompute line starts for coordinate conversion
    let line_starts = compute_line_starts(source);

    let mut result = match operation {
        "outline" => extract_outline(source, &tree, language, &line_starts)?,
        "syntax_errors" => extract_syntax_errors(source, &tree, &line_starts)?,
        _ => return Err(ToolFailure::not_applied("unsupported operation")),
    };

    // Truncate to fit max_result_bytes
    result = truncate_result(result, max_result_bytes);

    Ok(result)
}

fn compute_line_starts(source: &str) -> Vec<usize> {
    let mut line_starts = vec![0];
    for (i, byte) in source.bytes().enumerate() {
        if byte == b'\n' {
            line_starts.push(i + 1);
        }
    }
    line_starts
}

fn point_to_position(
    source: &str,
    point: Point,
    line_starts: &[usize],
) -> Result<(u64, u64), ToolFailure> {
    let line = point.row + 1;
    if point.row >= line_starts.len() {
        return Err(ToolFailure::not_applied("invalid tree-sitter position"));
    }

    let line_start = line_starts[point.row];
    let col_end = line_start + point.column;

    // Validate col_end is within the line
    let line_end = if point.row + 1 < line_starts.len() {
        line_starts[point.row + 1] - 1 // -1 for the \n
    } else {
        source.len()
    };

    if col_end > line_end {
        return Err(ToolFailure::not_applied("invalid tree-sitter position"));
    }

    // Ensure col_end is at a character boundary
    if !source.is_char_boundary(col_end) {
        return Err(ToolFailure::not_applied("invalid tree-sitter position"));
    }

    // Count UTF-16 units from line start to col_end
    let column = 1 + source[line_start..col_end]
        .chars()
        .map(|c| c.len_utf16())
        .sum::<usize>();

    Ok((line as u64, column as u64))
}

fn extract_outline(
    source: &str,
    tree: &tree_sitter::Tree,
    language: LanguageKind,
    line_starts: &[usize],
) -> Result<Value, ToolFailure> {
    let mut symbols = Vec::new();
    let root = tree.root_node();

    match language {
        LanguageKind::Rust => extract_rust_symbols(source, root, &mut symbols, line_starts)?,
        LanguageKind::TypeScript => {
            extract_typescript_symbols(source, root, &mut symbols, line_starts)?
        }
        LanguageKind::Tsx => {
            extract_typescript_symbols(source, root, &mut symbols, line_starts)?
        }
        LanguageKind::Python => {
            extract_python_symbols(source, root, &mut symbols, line_starts)?
        }
    }

    // Apply backstop limit
    let truncated = symbols.len() > MAX_SYMBOLS;
    if truncated {
        symbols.truncate(MAX_SYMBOLS);
    }

    Ok(json!({
        "path": "",
        "operation": "outline",
        "symbols": symbols,
        "truncated": truncated,
    }))
}

fn extract_rust_symbols(
    source: &str,
    root: tree_sitter::Node,
    symbols: &mut Vec<Value>,
    line_starts: &[usize],
) -> Result<(), ToolFailure> {
    let mut cursor = root.walk();
    for child in root.children(&mut cursor) {
        if !child.is_named() {
            continue;
        }

        match child.kind() {
            "function_item" => {
                if let Some(sym) = extract_symbol(source, child, "function", line_starts)? {
                    symbols.push(sym);
                }
            }
            "struct_item" => {
                if let Some(sym) = extract_symbol(source, child, "struct", line_starts)? {
                    symbols.push(sym);
                }
            }
            "enum_item" => {
                if let Some(sym) = extract_symbol(source, child, "enum", line_starts)? {
                    symbols.push(sym);
                }
            }
            "const_item" => {
                if let Some(sym) = extract_symbol(source, child, "constant", line_starts)? {
                    symbols.push(sym);
                }
            }
            "type_item" => {
                if let Some(sym) = extract_symbol(source, child, "type_alias", line_starts)? {
                    symbols.push(sym);
                }
            }
            "impl_item" => {
                // Descend into declaration_list
                let mut impl_cursor = child.walk();
                for impl_child in child.children(&mut impl_cursor) {
                    if impl_child.kind() == "declaration_list" {
                        let mut decl_cursor = impl_child.walk();
                        for decl_child in impl_child.children(&mut decl_cursor) {
                            match decl_child.kind() {
                                "function_item" => {
                                    if let Some(sym) =
                                        extract_symbol(source, decl_child, "method", line_starts)?
                                    {
                                        symbols.push(sym);
                                    }
                                }
                                "const_item" => {
                                    if let Some(sym) =
                                        extract_symbol(source, decl_child, "constant", line_starts)?
                                    {
                                        symbols.push(sym);
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn extract_typescript_symbols(
    source: &str,
    root: tree_sitter::Node,
    symbols: &mut Vec<Value>,
    line_starts: &[usize],
) -> Result<(), ToolFailure> {
    let mut cursor = root.walk();
    for child in root.children(&mut cursor) {
        if !child.is_named() {
            continue;
        }

        // Unwrap export_statement
        let node = if child.kind() == "export_statement" {
            // Process all named children of export_statement
            let mut export_cursor = child.walk();
            for export_child in child.children(&mut export_cursor) {
                if export_child.is_named() {
                    process_typescript_node(source, export_child, symbols, line_starts)?;
                }
            }
            continue;
        } else {
            child
        };

        process_typescript_node(source, node, symbols, line_starts)?;
    }
    Ok(())
}

fn process_typescript_node(
    source: &str,
    node: tree_sitter::Node,
    symbols: &mut Vec<Value>,
    line_starts: &[usize],
) -> Result<(), ToolFailure> {
    match node.kind() {
        "function_declaration" => {
            if let Some(sym) = extract_symbol(source, node, "function", line_starts)? {
                symbols.push(sym);
            }
        }
        "class_declaration" => {
            if let Some(sym) = extract_symbol(source, node, "class", line_starts)? {
                symbols.push(sym);
            }
            // Descend into class_body for methods
            let mut class_cursor = node.walk();
            for class_child in node.children(&mut class_cursor) {
                if class_child.kind() == "class_body" {
                    let mut body_cursor = class_child.walk();
                    for body_child in class_child.children(&mut body_cursor) {
                        if body_child.kind() == "method_definition"
                            && let Some(sym) =
                                extract_symbol(source, body_child, "method", line_starts)?
                        {
                            symbols.push(sym);
                        }
                    }
                }
            }
        }
        "interface_declaration" => {
            if let Some(sym) = extract_symbol(source, node, "interface", line_starts)? {
                symbols.push(sym);
            }
        }
        "type_alias_declaration" => {
            if let Some(sym) = extract_symbol(source, node, "type_alias", line_starts)? {
                symbols.push(sym);
            }
        }
        "lexical_declaration" | "variable_declaration" => {
            // Determine if it's const
            let is_const = {
                let first_child = node.child(0);
                first_child.map(|n| n.kind() == "const").unwrap_or(false)
            };
            let kind = if is_const { "constant" } else { "variable" };

            let mut decl_cursor = node.walk();
            for decl_child in node.children(&mut decl_cursor) {
                if decl_child.kind() == "variable_declarator"
                    && let Some(sym) = extract_symbol(source, decl_child, kind, line_starts)?
                {
                    symbols.push(sym);
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn extract_python_symbols(
    source: &str,
    root: tree_sitter::Node,
    symbols: &mut Vec<Value>,
    line_starts: &[usize],
) -> Result<(), ToolFailure> {
    let mut cursor = root.walk();
    for child in root.children(&mut cursor) {
        if !child.is_named() {
            continue;
        }

        match child.kind() {
            "function_definition" => {
                if let Some(sym) = extract_symbol(source, child, "function", line_starts)? {
                    symbols.push(sym);
                }
            }
            "class_definition" => {
                if let Some(sym) = extract_symbol(source, child, "class", line_starts)? {
                    symbols.push(sym);
                }
                // Descend into block for methods
                let mut class_cursor = child.walk();
                for class_child in child.children(&mut class_cursor) {
                    if class_child.kind() == "block" {
                        let mut block_cursor = class_child.walk();
                        for block_child in class_child.children(&mut block_cursor) {
                            if block_child.kind() == "function_definition"
                                && let Some(sym) =
                                    extract_symbol(source, block_child, "method", line_starts)?
                            {
                                symbols.push(sym);
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn extract_symbol(
    source: &str,
    node: tree_sitter::Node,
    kind: &str,
    line_starts: &[usize],
) -> Result<Option<Value>, ToolFailure> {
    let name_node = match node.child_by_field_name("name") {
        Some(n) => n,
        None => return Ok(None),
    };

    let name_start = name_node.start_byte();
    let name_end = name_node.end_byte();
    if name_start >= source.len() || name_end > source.len() {
        return Ok(None);
    }

    let name = &source[name_start..name_end];
    if name.is_empty() {
        return Ok(None);
    }

    let (start_line, start_column) = point_to_position(source, node.start_position(), line_starts)?;
    let (end_line, end_column) = point_to_position(source, node.end_position(), line_starts)?;
    let (name_start_line, name_start_column) =
        point_to_position(source, name_node.start_position(), line_starts)?;
    let (name_end_line, name_end_column) =
        point_to_position(source, name_node.end_position(), line_starts)?;

    Ok(Some(json!({
        "kind": kind,
        "name": name,
        "range": {
            "start_line": start_line,
            "start_column": start_column,
            "end_line": end_line,
            "end_column": end_column,
        },
        "name_range": {
            "start_line": name_start_line,
            "start_column": name_start_column,
            "end_line": name_end_line,
            "end_column": name_end_column,
        },
    })))
}

fn extract_syntax_errors(
    source: &str,
    tree: &tree_sitter::Tree,
    line_starts: &[usize],
) -> Result<Value, ToolFailure> {
    let mut errors = Vec::new();
    let mut cursor = tree.walk();

    walk_tree(source, &mut cursor, &mut errors, line_starts)?;

    // Apply backstop limit
    let truncated = errors.len() > MAX_ERRORS;
    if truncated {
        errors.truncate(MAX_ERRORS);
    }

    Ok(json!({
        "path": "",
        "operation": "syntax_errors",
        "errors": errors,
        "truncated": truncated,
    }))
}

fn walk_tree(
    source: &str,
    cursor: &mut tree_sitter::TreeCursor,
    errors: &mut Vec<Value>,
    line_starts: &[usize],
) -> Result<(), ToolFailure> {
    let node = cursor.node();

    if node.is_error() {
        let (start_line, start_column) = point_to_position(source, node.start_position(), line_starts)?;
        let (end_line, end_column) = point_to_position(source, node.end_position(), line_starts)?;
        errors.push(json!({
            "kind": "ERROR",
            "range": {
                "start_line": start_line,
                "start_column": start_column,
                "end_line": end_line,
                "end_column": end_column,
            },
        }));
    } else if node.is_missing() {
        let (start_line, start_column) = point_to_position(source, node.start_position(), line_starts)?;
        let (end_line, end_column) = point_to_position(source, node.end_position(), line_starts)?;
        errors.push(json!({
            "kind": "MISSING",
            "expected": node.kind(),
            "range": {
                "start_line": start_line,
                "start_column": start_column,
                "end_line": end_line,
                "end_column": end_column,
            },
        }));
    }

    // Recurse into children
    if cursor.goto_first_child() {
        loop {
            walk_tree(source, cursor, errors, line_starts)?;
            if !cursor.goto_next_sibling() {
                break;
            }
        }
        cursor.goto_parent();
    }

    Ok(())
}

fn truncate_result(mut result: Value, max_bytes: usize) -> Value {
    // Get the array to truncate
    let array_key = if result.get("symbols").is_some() {
        "symbols"
    } else if result.get("errors").is_some() {
        "errors"
    } else {
        return result;
    };

    let original_len = result
        .get(array_key)
        .and_then(Value::as_array)
        .map(|arr| arr.len())
        .unwrap_or(0);

    // Binary search for the longest prefix that fits
    let mut low = 0;
    let mut high = original_len;
    let mut best = 0;

    while low <= high {
        let mid = (low + high) / 2;
        
        // Create a test result with truncated array
        let test_array: Vec<Value> = result
            .get(array_key)
            .and_then(Value::as_array)
            .map(|arr| arr[..mid].to_vec())
            .unwrap_or_default();
        
        let mut test_result = result.clone();
        test_result[array_key] = Value::Array(test_array);
        test_result["truncated"] = Value::Bool(mid < original_len);

        let serialized = serde_json::to_string(&test_result).unwrap();
        if serialized.len() <= max_bytes {
            best = mid;
            if mid == high {
                break;
            }
            low = mid + 1;
        } else {
            if mid == 0 {
                break;
            }
            high = mid - 1;
        }
    }

    // Apply the truncation
    if let Some(array) = result.get_mut(array_key).and_then(Value::as_array_mut) {
        array.truncate(best);
    }
    result["truncated"] = Value::Bool(best < original_len || result["truncated"] == Value::Bool(true));

    result
}

pub(crate) fn register(
    registry: &ToolRegistry,
    workspace: Workspace,
    max_result_bytes: usize,
) -> Result<(), ToolRegistryError> {
    let rust: Language = tree_sitter_rust::LANGUAGE.into();
    let typescript: Language = tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into();
    let tsx: Language = tree_sitter_typescript::LANGUAGE_TSX.into();
    let python: Language = tree_sitter_python::LANGUAGE.into();

    registry.register(
        TOOL_NAME,
        Some(
            "Parse one source file with tree-sitter and extract structural information. \
             `outline` returns top-level symbols (and direct members of impl/class bodies) \
             with source ranges; `syntax_errors` returns tree-sitter ERROR and MISSING nodes. \
             Languages: Rust (.rs), TypeScript (.ts), TSX (.tsx), Python (.py, .pyi). \
             Positions are 1-based lines and 1-based UTF-16 columns. Results are syntax-only: \
             no type checking, no cross-file resolution, no compiler diagnostics.",
        ),
        json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["path", "operation"],
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Project-relative path to a source file"
                },
                "operation": {
                    "type": "string",
                    "enum": ["outline", "syntax_errors"]
                }
            }
        }),
        Arc::new(WorkspaceIndexQuery {
            workspace,
            rust,
            typescript,
            tsx,
            python,
            max_result_bytes,
        }),
    )
}
