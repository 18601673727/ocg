use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use serde_json::{Value, json};
use tree_sitter::{
    Language, Node, ParseOptions, Parser, Point, Query, QueryCursor, QueryCursorOptions,
    StreamingIterator, Tree,
};

use super::blocking::{self, Kind};
use super::tools::{ToolFailure, ToolFuture, ToolHandler, ToolRegistry, ToolRegistryError};
use super::workspace_fts::{self, BuildLimit, FileStamp, FtsLimits, Skipped, Stopped, WalkVisitor};
use super::workspace_read::{Workspace, components};
use crate::persistence::ToolEffect;

pub const TOOL_NAME: &str = "workspace_code";
const HANDLER_VERSION: &str = "workspace_code-1";
const MAX_PARSE_FILE_BYTES: usize = 2 * 1024 * 1024;
const MAX_FILES: usize = 50_000;
const MAX_REF_FILES: usize = 500;

const RUST_TAGS: &str = r#"
(function_item name: (identifier) @name) @definition.fn
(function_signature_item name: (identifier) @name) @definition.fn
(struct_item name: (type_identifier) @name) @definition.struct
(enum_item name: (type_identifier) @name) @definition.enum
(union_item name: (type_identifier) @name) @definition.union
(trait_item name: (type_identifier) @name) @definition.trait
(type_item name: (type_identifier) @name) @definition.type
(mod_item name: (identifier) @name) @definition.mod
(macro_definition name: (identifier) @name) @definition.macro
(const_item name: (identifier) @name) @definition.const
(static_item name: (identifier) @name) @definition.static
(impl_item type: (_) @name) @definition.impl
"#;
const TYPESCRIPT_TAGS: &str = r#"
(function_declaration name: (identifier) @name) @definition.fn
(generator_function_declaration name: (identifier) @name) @definition.fn
(function_signature name: (identifier) @name) @definition.fn
(method_definition name: (property_identifier) @name) @definition.fn
(method_signature name: (property_identifier) @name) @definition.fn
(abstract_method_signature name: (property_identifier) @name) @definition.fn
(class_declaration name: (type_identifier) @name) @definition.class
(abstract_class_declaration name: (type_identifier) @name) @definition.class
(interface_declaration name: (type_identifier) @name) @definition.interface
(type_alias_declaration name: (type_identifier) @name) @definition.type
(enum_declaration name: (identifier) @name) @definition.enum
(internal_module name: (identifier) @name) @definition.mod
(variable_declarator name: (identifier) @name) @definition.var
(public_field_definition name: (property_identifier) @name) @definition.field
"#;
const PYTHON_TAGS: &str = r#"
(class_definition name: (identifier) @name) @definition.class
(function_definition name: (identifier) @name) @definition.fn
(module (expression_statement (assignment left: (identifier) @name) @definition.var))
(class_definition body: (block (expression_statement (assignment left: (identifier) @name) @definition.var)))
"#;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LanguageKind {
    Rust,
    TypeScript,
    Tsx,
    Python,
}

fn detect_language(path: &str) -> Option<LanguageKind> {
    match path.rsplit('.').next()? {
        "rs" => Some(LanguageKind::Rust),
        "ts" => Some(LanguageKind::TypeScript),
        "tsx" => Some(LanguageKind::Tsx),
        "py" | "pyi" => Some(LanguageKind::Python),
        _ => None,
    }
}

struct Grammar {
    language: Language,
    tags: Query,
}
struct Grammars {
    rust: Grammar,
    typescript: Grammar,
    tsx: Grammar,
    python: Grammar,
}
impl Grammars {
    fn new() -> Result<Self, ToolFailure> {
        fn grammar(language: Language, tags: &str) -> Result<Grammar, ToolFailure> {
            let tags = Query::new(&language, tags)
                .map_err(|error| ToolFailure::not_applied(error.to_string()))?;
            Ok(Grammar { language, tags })
        }
        Ok(Self {
            rust: grammar(tree_sitter_rust::LANGUAGE.into(), RUST_TAGS)?,
            typescript: grammar(
                tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into(),
                TYPESCRIPT_TAGS,
            )?,
            tsx: grammar(tree_sitter_typescript::LANGUAGE_TSX.into(), TYPESCRIPT_TAGS)?,
            python: grammar(tree_sitter_python::LANGUAGE.into(), PYTHON_TAGS)?,
        })
    }
    fn get(&self, language: LanguageKind) -> &Grammar {
        match language {
            LanguageKind::Rust => &self.rust,
            LanguageKind::TypeScript => &self.typescript,
            LanguageKind::Tsx => &self.tsx,
            LanguageKind::Python => &self.python,
        }
    }
}

fn grammars() -> Result<&'static Grammars, ToolFailure> {
    static GRAMMARS: OnceLock<Result<Grammars, String>> = OnceLock::new();
    GRAMMARS
        .get_or_init(|| Grammars::new().map_err(|error| error.message))
        .as_ref()
        .map_err(refused)
}

#[derive(Clone, Copy)]
struct Deadline(Instant);
impl Deadline {
    fn check(self) -> Result<(), ToolFailure> {
        if Instant::now() >= self.0 {
            Err(ToolFailure::not_applied("analysis deadline reached"))
        } else {
            Ok(())
        }
    }
}

fn parse(source: &str, grammar: &Grammar, deadline: Deadline) -> Result<Tree, ToolFailure> {
    deadline.check()?;
    let mut on_progress = |_state: &tree_sitter::ParseState| {
        if Instant::now() >= deadline.0 {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    };
    let options = ParseOptions::new().progress_callback(&mut on_progress);
    let mut parser = Parser::new();
    parser
        .set_language(&grammar.language)
        .map_err(|error| ToolFailure::not_applied(error.to_string()))?;
    parser
        .parse_with_options(
            &mut |offset: usize, _point| &source.as_bytes()[offset.min(source.len())..],
            None,
            Some(options),
        )
        .ok_or_else(|| ToolFailure::not_applied("parse deadline reached"))
}

#[derive(Clone)]
struct Symbol {
    name: String,
    qualified: String,
    kind: String,
    enclosing: Option<String>,
    start: usize,
    end: usize,
    start_byte: usize,
    end_byte: usize,
    signature_end: usize,
    doc: Option<(usize, usize)>,
    signature: String,
    outline: bool,
}
struct FileIndex {
    stamp: FileStamp,
    parsed_at: SystemTime,
    symbols: Vec<Symbol>,
    names: BTreeSet<String>,
}
#[derive(Default)]
struct Index {
    files: BTreeMap<Arc<str>, FileIndex>,
    names: BTreeMap<String, BTreeSet<Arc<str>>>,
}
impl Index {
    fn remove(&mut self, path: &str) {
        if let Some(file) = self.files.remove(path) {
            for name in file.names {
                if let Some(paths) = self.names.get_mut(&name) {
                    paths.remove(path);
                    if paths.is_empty() {
                        self.names.remove(&name);
                    }
                }
            }
        }
    }
    fn insert(&mut self, path: Arc<str>, file: FileIndex) {
        self.remove(&path);
        for name in &file.names {
            self.names
                .entry(name.clone())
                .or_default()
                .insert(path.clone());
        }
        self.files.insert(path, file);
    }
}
struct Shared {
    index: Mutex<Index>,
    available: flume::Receiver<()>,
    release: flume::Sender<()>,
}
struct Lease(Arc<Shared>);
impl Drop for Lease {
    fn drop(&mut self) {
        if let Err(error) = self.0.release.try_send(()) {
            log::error!("workspace_code index release: {error}");
        }
    }
}
type RootKey = (PathBuf, u64, u64);
fn shared(workspace: &Workspace) -> Result<Arc<Shared>, ToolFailure> {
    static INDEXES: OnceLock<Mutex<BTreeMap<RootKey, Arc<Shared>>>> = OnceLock::new();
    let metadata = workspace
        .open_root()
        .and_then(|root| {
            root.metadata()
                .map_err(|_| super::workspace_read::Refusal::Unreadable)
        })
        .map_err(refused)?;
    let key = (workspace.root().to_owned(), metadata.dev(), metadata.ino());
    let mut indexes = INDEXES
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    Ok(indexes
        .entry(key)
        .or_insert_with(|| {
            let (release, available) = flume::bounded(1);
            // The channel is new and empty, so the initial permit always fits.
            if let Err(error) = release.try_send(()) {
                log::error!("workspace_code initial permit: {error}");
            }
            Arc::new(Shared {
                index: Mutex::default(),
                available,
                release,
            })
        })
        .clone())
}
fn refused(error: impl std::fmt::Display) -> ToolFailure {
    ToolFailure::not_applied(error.to_string())
}

struct Scan {
    files: BTreeMap<Arc<str>, FileStamp>,
    examined: usize,
    files_seen: usize,
    skipped: usize,
    unsupported: usize,
    limits: BTreeSet<String>,
    deadline: Deadline,
}
impl WalkVisitor for Scan {
    fn checkpoint(&mut self) -> Result<(), Stopped> {
        if Instant::now() >= self.deadline.0 {
            self.omitted(BuildLimit::Time);
            return Err(Stopped);
        }
        if self.files_seen >= MAX_FILES {
            self.omitted(BuildLimit::Files);
            return Err(Stopped);
        }
        Ok(())
    }
    fn examined(&mut self) {
        self.examined += 1;
    }
    fn skipped(&mut self, skipped: Skipped) {
        self.skipped += skipped.protected
            + skipped.symlink
            + skipped.binary
            + skipped.too_large
            + skipped.hard_linked
            + skipped.unreadable
            + skipped.other;
    }
    fn omitted(&mut self, limit: BuildLimit) {
        self.limits.insert(format!("{limit:?}"));
    }
    fn file(
        &mut self,
        _directory: &std::fs::File,
        _name: &str,
        path: &str,
        stamp: FileStamp,
    ) -> Result<(), Stopped> {
        self.files_seen += 1;
        if stamp.bytes > MAX_PARSE_FILE_BYTES as u64 {
            self.skipped += 1;
            self.limits.insert("FileBytes".to_owned());
        } else if detect_language(path).is_none() {
            self.unsupported += 1;
        } else {
            self.files.insert(Arc::from(path), stamp);
        }
        Ok(())
    }
}
fn scan(workspace: &Workspace, deadline: Deadline) -> Result<Scan, ToolFailure> {
    let root = workspace.open_root().map_err(refused)?;
    let device = root.metadata().map_err(refused)?.dev();
    let limits = FtsLimits {
        directory_entries: MAX_FILES,
        depth: 64,
        ..FtsLimits::default()
    };
    let mut scan = Scan {
        files: BTreeMap::new(),
        examined: 0,
        files_seen: 0,
        skipped: 0,
        unsupported: 0,
        limits: BTreeSet::new(),
        deadline,
    };
    if workspace_fts::walk_directory(workspace, &root, ".", 0, device, &limits, &mut scan).is_err()
        && scan.limits.is_empty()
    {
        return Err(refused("workspace walk stopped"));
    }
    workspace.open_root().map_err(refused)?;
    Ok(scan)
}

fn node_text<'a>(node: Node<'_>, source: &'a str) -> &'a str {
    source.get(node.byte_range()).unwrap_or_default()
}
fn identifier(node: Node<'_>) -> bool {
    matches!(
        node.kind(),
        "identifier"
            | "type_identifier"
            | "field_identifier"
            | "property_identifier"
            | "shorthand_property_identifier"
            | "shorthand_property_identifier_pattern"
    )
}
fn visit(
    tree: &Tree,
    deadline: Deadline,
    mut action: impl FnMut(Node<'_>),
) -> Result<(), ToolFailure> {
    let mut cursor = tree.walk();
    'visit: loop {
        deadline.check()?;
        let node = cursor.node();
        action(node);
        if cursor.goto_first_child() {
            continue;
        }
        loop {
            if cursor.goto_next_sibling() {
                continue 'visit;
            }
            if !cursor.goto_parent() {
                return Ok(());
            }
        }
    }
}
fn compact(line: &str) -> String {
    line.trim().chars().take(160).collect()
}
fn symbols(
    source: &str,
    tree: &Tree,
    grammar: &Grammar,
    deadline: Deadline,
) -> Result<Vec<Symbol>, ToolFailure> {
    let mut cursor = QueryCursor::new();
    let mut on_progress = |_state: &tree_sitter::QueryCursorState| {
        if Instant::now() >= deadline.0 {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    };
    let mut matches = cursor.matches_with_options(
        &grammar.tags,
        tree.root_node(),
        source.as_bytes(),
        QueryCursorOptions::new().progress_callback(&mut on_progress),
    );
    let mut raw = Vec::new();
    while let Some(found) = matches.next() {
        deadline.check()?;
        let name = found
            .captures()
            .iter()
            .find(|capture| grammar.tags.capture_names()[capture.index as usize] == "name");
        let definition = found.captures().iter().find(|capture| {
            grammar.tags.capture_names()[capture.index as usize].starts_with("definition.")
        });
        if let (Some(name), Some(definition)) = (name, definition) {
            let mut node = definition.node;
            let body = node.child_by_field_name("body").or_else(|| {
                node.child_by_field_name("value")
                    .filter(|value| {
                        matches!(value.kind(), "arrow_function" | "function_expression")
                    })
                    .and_then(|value| value.child_by_field_name("body"))
            });
            let doc = body
                .and_then(|body| body.named_child(0))
                .filter(|statement| statement.kind() == "expression_statement")
                .and_then(|statement| statement.named_child(0))
                .filter(|node| matches!(node.kind(), "string" | "concatenated_string"))
                .map(|node| (node.start_position().row + 1, node.end_position().row + 1));
            let signature_end = body.map_or(node.end_byte(), |body| {
                if node_text(body, source).starts_with('{') {
                    body.start_byte() + 1
                } else {
                    node.start_byte()
                        + node_text(node, source)
                            .get(..body.start_byte() - node.start_byte())
                            .unwrap_or_default()
                            .trim_end()
                            .len()
                }
            });
            let signature = compact(
                source
                    .get(node.start_byte()..signature_end)
                    .unwrap_or_default()
                    .lines()
                    .next()
                    .unwrap_or_default(),
            );
            if node
                .parent()
                .is_some_and(|parent| parent.kind() == "decorated_definition")
            {
                node = node
                    .parent()
                    .ok_or_else(|| refused("missing decorator parent"))?;
            }
            raw.push((
                node,
                signature_end,
                doc,
                signature,
                node_text(
                    if name.node.kind() == "generic_type" {
                        name.node.child_by_field_name("type").unwrap_or(name.node)
                    } else {
                        name.node
                    },
                    source,
                )
                .to_owned(),
                grammar.tags.capture_names()[definition.index as usize]
                    .trim_start_matches("definition.")
                    .to_owned(),
            ));
        }
    }
    deadline.check()?;
    raw.sort_by_key(|(node, _, _, _, _, _)| {
        (node.start_byte(), std::cmp::Reverse(node.end_byte()))
    });
    let mut result: Vec<Symbol> = Vec::new();
    let mut parents: Vec<usize> = Vec::new();
    for (node, signature_end, doc, signature, name, kind) in raw {
        deadline.check()?;
        while parents
            .last()
            .is_some_and(|&index| result[index].end_byte <= node.start_byte())
        {
            parents.pop();
        }
        let parent = parents.last().map(|&index| &result[index]);
        let enclosing = parent.map(|symbol| symbol.qualified.clone());
        let qualified = enclosing
            .as_ref()
            .map_or_else(|| name.clone(), |parent| format!("{parent}::{name}"));
        let outline = parents.is_empty()
            || parents.len() == 1
                && parent.is_some_and(|symbol| {
                    matches!(
                        symbol.kind.as_str(),
                        "impl" | "class" | "interface" | "trait"
                    )
                });
        result.push(Symbol {
            name,
            qualified,
            kind,
            enclosing,
            start: node.start_position().row + 1,
            end: node.end_position().row + usize::from(node.end_position().column != 0),
            start_byte: node.start_byte(),
            end_byte: node.end_byte(),
            signature_end,
            doc,
            signature,
            outline,
        });
        parents.push(result.len() - 1);
    }
    Ok(result)
}
fn read_parse(
    workspace: &Workspace,
    path: &str,
    deadline: Deadline,
) -> Result<(String, Tree), ToolFailure> {
    let bytes = workspace
        .read_binary(path, MAX_PARSE_FILE_BYTES)
        .map_err(refused)?;
    let source = String::from_utf8(bytes).map_err(refused)?;
    if source.contains('\0') {
        return Err(refused("file is not UTF-8 text"));
    }
    let grammar =
        grammars()?.get(detect_language(path).ok_or_else(|| refused("unsupported file type"))?);
    let tree = parse(&source, grammar, deadline)?;
    Ok((source, tree))
}
fn racy(file: &FileIndex) -> bool {
    let modified = UNIX_EPOCH.checked_add(Duration::new(
        file.stamp.seconds.max(0) as u64,
        file.stamp.nanos,
    ));
    modified.is_none_or(|modified| {
        file.parsed_at.duration_since(modified).unwrap_or_default() < Duration::from_secs(2)
    })
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Op {
    Find,
    Refs,
    Body,
    Outline,
    Errors,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Question {
    op: Op,
    name: Option<String>,
    path: Option<String>,
    line: Option<usize>,
    kind: Option<String>,
    #[serde(default)]
    signature: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Call {
    queries: Vec<Question>,
    max_bytes: Option<usize>,
}
fn call_of(arguments: Value) -> Result<Call, ToolFailure> {
    let call: Call = serde_json::from_value(arguments).map_err(refused)?;
    if call.queries.is_empty() || call.queries.len() > 8 {
        return Err(refused("expected 1 to 8 queries"));
    }
    for question in &call.queries {
        if let Some(path) = &question.path {
            if path != "." && !(matches!(question.op, Op::Errors) && path == "changed") {
                components(path).map_err(refused)?;
            }
        }
        if matches!(question.op, Op::Find | Op::Refs)
            && question.name.as_ref().is_none_or(|name| name.is_empty())
        {
            return Err(refused("find and refs require name"));
        }
        if matches!(question.op, Op::Body)
            && question.name.as_ref().is_none_or(|name| name.is_empty())
            && (question.path.is_none() || question.line.is_none_or(|line| line == 0))
        {
            return Err(refused("body requires name or path and positive line"));
        }
        if matches!(question.op, Op::Outline | Op::Errors) && question.path.is_none() {
            return Err(refused("outline and errors require path"));
        }
        if question.kind.as_ref().is_some_and(|kind| {
            !matches!(question.op, Op::Refs) || !matches!(kind.as_str(), "call" | "type" | "import")
        }) {
            return Err(refused("kind must be call, type or import on refs"));
        }
    }
    Ok(call)
}
fn within(path: &str, prefix: Option<&str>) -> bool {
    prefix.is_none_or(|prefix| {
        prefix == "."
            || path == prefix
            || path
                .strip_prefix(prefix)
                .is_some_and(|suffix| suffix.starts_with('/'))
    })
}
fn name_rank(symbol: &Symbol, name: &str) -> Option<u8> {
    let prefix = name.strip_suffix('*');
    let name = prefix.unwrap_or(name);
    if symbol.name == name
        || symbol.qualified == name
        || symbol.qualified.ends_with(&format!("::{name}"))
    {
        Some(0)
    } else if symbol.name.to_lowercase() == name.to_lowercase()
        || symbol.qualified.to_lowercase() == name.to_lowercase()
        || symbol
            .qualified
            .to_lowercase()
            .ends_with(&format!("::{}", name.to_lowercase()))
    {
        Some(1)
    } else {
        prefix
            .filter(|prefix| {
                symbol
                    .name
                    .to_lowercase()
                    .starts_with(&prefix.to_lowercase())
                    || symbol
                        .qualified
                        .to_lowercase()
                        .starts_with(&prefix.to_lowercase())
            })
            .map(|_| 2)
    }
}

fn proximity(path: &str, hint: Option<&str>) -> (u8, usize) {
    let Some(hint) = hint else {
        return (2, 0);
    };
    if path == hint {
        return (0, 0);
    }
    let common = path
        .split('/')
        .zip(hint.split('/'))
        .take_while(|(left, right)| left == right)
        .count();
    (1, usize::MAX - common)
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

/// The absolute byte offset of a tree-sitter point, with the same validation
/// the positions it feeds had: the row must exist and the column must stay
/// within its line.
fn point_to_offset(
    point: Point,
    line_starts: &[usize],
    source_len: usize,
) -> Result<usize, ToolFailure> {
    if point.row >= line_starts.len() {
        return Err(ToolFailure::not_applied("invalid tree-sitter position"));
    }

    let offset = line_starts[point.row] + point.column;

    // Validate the offset is within the line
    let line_end = if point.row + 1 < line_starts.len() {
        line_starts[point.row + 1] - 1 // -1 for the \n
    } else {
        source_len
    };

    if offset > line_end {
        return Err(ToolFailure::not_applied("invalid tree-sitter position"));
    }

    Ok(offset)
}

/// Converts absolute byte offsets to 1-based `(line, utf16_column)` positions
/// in one pass: the offsets are answered in ascending order while a single
/// forward scan counts lines and UTF-16 units, so any number of positions
/// costs one scan of the source instead of one line-prefix scan each.
fn offsets_to_positions(
    source: &str,
    offsets: &[usize],
    deadline: Deadline,
) -> Result<Vec<(u64, u64)>, ToolFailure> {
    for &offset in offsets {
        // Ensure each offset is at a character boundary
        if offset > source.len() || !source.is_char_boundary(offset) {
            return Err(ToolFailure::not_applied("invalid tree-sitter position"));
        }
    }

    let mut order: Vec<usize> = (0..offsets.len()).collect();
    order.sort_unstable_by_key(|&i| offsets[i]);

    let mut positions = vec![(0u64, 0u64); offsets.len()];
    let mut line = 0u64; // 0-based line at `byte`
    let mut column = 0u64; // UTF-16 units between the line start and `byte`
    let mut byte = 0usize; // scan position; only advances
    let mut chars = source.char_indices();
    let mut next = chars.next();

    for &i in &order {
        deadline.check()?;
        let target = offsets[i];
        while byte < target {
            let Some((start, c)) = next else {
                return Err(ToolFailure::not_applied("invalid tree-sitter position"));
            };
            debug_assert_eq!(start, byte);
            if c == '\n' {
                line += 1;
                column = 0;
            } else {
                column += c.len_utf16() as u64;
            }
            byte += c.len_utf8();
            next = chars.next();
        }
        // `byte == target`: every target is a char boundary, and the scan
        // steps boundary to boundary.
        positions[i] = (line + 1, column + 1);
    }

    Ok(positions)
}

struct RawError<'tree> {
    /// The node kind a MISSING node stands in for; none for an ERROR node.
    expected: Option<&'tree str>,
    start: usize,
    end: usize,
}

fn walk_tree<'tree>(
    source: &str,
    cursor: &mut tree_sitter::TreeCursor<'tree>,
    errors: &mut Vec<RawError<'tree>>,
    line_starts: &[usize],
    deadline: Deadline,
) -> Result<(), ToolFailure> {
    'visit: loop {
        deadline.check()?;
        let node = cursor.node();

        if node.is_error() {
            errors.push(RawError {
                expected: None,
                start: point_to_offset(node.start_position(), line_starts, source.len())?,
                end: point_to_offset(node.end_position(), line_starts, source.len())?,
            });
        } else if node.is_missing() {
            errors.push(RawError {
                expected: Some(node.kind()),
                start: point_to_offset(node.start_position(), line_starts, source.len())?,
                end: point_to_offset(node.end_position(), line_starts, source.len())?,
            });
        }

        // Descend first; when a subtree is done, ascend until a next sibling.
        if cursor.goto_first_child() {
            continue;
        }
        loop {
            if cursor.goto_next_sibling() {
                continue 'visit;
            }
            if !cursor.goto_parent() {
                return Ok(());
            }
        }
    }
}

struct Entry {
    path: Arc<str>,
    line: usize,
    key: String,
    lines: Vec<String>,
}
struct Section {
    heading: String,
    entries: Vec<Entry>,
    total: usize,
    files: BTreeSet<Arc<str>>,
    retained_bytes: usize,
    demand_bytes: usize,
    omitted_files: usize,
    issues: Vec<String>,
    unit: &'static str,
}
impl Section {
    fn new(number: usize, question: &Question) -> Self {
        let (operation, unit) = match question.op {
            Op::Find => ("find", "definitions"),
            Op::Refs => ("refs", "uses"),
            Op::Body => ("body", "symbols"),
            Op::Outline => ("outline", "symbols"),
            Op::Errors => ("errors", "errors"),
        };
        Self {
            heading: format!(
                "#{} {operation} {}{}{}",
                number + 1,
                question.name.as_deref().unwrap_or_default(),
                question
                    .path
                    .as_ref()
                    .map(|path| format!(" path={path}"))
                    .unwrap_or_default(),
                question
                    .kind
                    .as_ref()
                    .map(|kind| format!(" kind={kind}"))
                    .unwrap_or_default()
            ),
            entries: Vec::new(),
            total: 0,
            files: BTreeSet::new(),
            retained_bytes: 0,
            demand_bytes: 0,
            omitted_files: 0,
            issues: Vec::new(),
            unit,
        }
    }
    fn push(&mut self, entry: Entry, budget: usize) {
        self.total += 1;
        self.files.insert(entry.path.clone());
        let bytes =
            entry.lines.iter().map(|line| line.len() + 1).sum::<usize>() + entry.path.len() + 30;
        self.demand_bytes += bytes;
        if self.retained_bytes < budget {
            self.retained_bytes += bytes;
            self.entries.push(entry);
        }
    }

    fn counts(&self, shown: usize) -> String {
        format!(
            "{} {} in {} files, {shown} shown{}",
            self.total,
            self.unit,
            self.files.len(),
            if self.omitted_files == 0 {
                String::new()
            } else {
                format!(", {} candidate files left", self.omitted_files)
            }
        )
    }
}
fn definition_entry(path: Arc<str>, symbol: &Symbol, signature: bool) -> Entry {
    let mut lines = vec![format!(
        "{} {}  {}-{}",
        symbol.kind, symbol.qualified, symbol.start, symbol.end
    )];
    if signature {
        lines.push(format!("  {}", symbol.signature));
    }
    Entry {
        path,
        line: symbol.start,
        key: format!("definition:{}", symbol.start_byte),
        lines,
    }
}
fn is_definition(node: Node<'_>) -> bool {
    node.parent().is_some_and(|parent| {
        parent
            .child_by_field_name("name")
            .is_some_and(|name| name.id() == node.id())
            && matches!(
                parent.kind(),
                "function_item"
                    | "function_signature_item"
                    | "struct_item"
                    | "enum_item"
                    | "union_item"
                    | "trait_item"
                    | "type_item"
                    | "mod_item"
                    | "macro_definition"
                    | "const_item"
                    | "static_item"
                    | "function_declaration"
                    | "generator_function_declaration"
                    | "function_signature"
                    | "method_definition"
                    | "method_signature"
                    | "class_declaration"
                    | "abstract_class_declaration"
                    | "interface_declaration"
                    | "type_alias_declaration"
                    | "enum_declaration"
                    | "variable_declarator"
                    | "public_field_definition"
                    | "function_definition"
                    | "class_definition"
            )
            || parent.kind() == "assignment"
                && parent
                    .child_by_field_name("left")
                    .is_some_and(|left| left.id() == node.id())
    })
}
fn callee_name(mut node: Node<'_>) -> Node<'_> {
    loop {
        let next = ["field", "attribute", "name", "function", "constructor"]
            .into_iter()
            .find_map(|field| node.child_by_field_name(field));
        match next {
            Some(next) => node = next,
            None => return node,
        }
    }
}
fn reference_kind(node: Node<'_>) -> &'static str {
    let mut ancestor = node;
    let mut type_use = node.kind() == "type_identifier";
    while let Some(parent) = ancestor.parent() {
        match parent.kind() {
            "use_declaration"
            | "import_statement"
            | "import_from_statement"
            | "import_alias"
            | "extern_crate_declaration" => return "import",
            "call_expression" | "call" | "macro_invocation" | "new_expression" => {
                let function = parent
                    .child_by_field_name("function")
                    .or_else(|| parent.child_by_field_name("macro"))
                    .or_else(|| parent.child_by_field_name("constructor"));
                if function.is_some_and(|function| callee_name(function).id() == node.id()) {
                    return "call";
                }
            }
            "type_annotation" | "type_arguments" | "generic_type" | "type" | "trait_bounds"
            | "return_type" => type_use = true,
            _ => {}
        }
        if parent.child_by_field_name("type").is_some_and(|field| {
            field.start_byte() <= node.start_byte() && field.end_byte() >= node.end_byte()
        }) {
            type_use = true;
        }
        ancestor = parent;
    }
    if type_use { "type" } else { "name" }
}
fn enclosing(symbols: &[Symbol], node: Node<'_>) -> String {
    symbols
        .iter()
        .rev()
        .find(|symbol| symbol.start_byte <= node.start_byte() && symbol.end_byte >= node.end_byte())
        .map(|symbol| {
            format!(
                "{} {}",
                symbol.kind,
                symbol.enclosing.as_ref().map_or_else(
                    || symbol.name.clone(),
                    |parent| format!("{parent}::{}", symbol.name)
                )
            )
        })
        .unwrap_or_else(|| "file".to_owned())
}
fn signature_lines(source: &str, symbol: &Symbol) -> Vec<(usize, String)> {
    let lines: Vec<&str> = source.lines().collect();
    let mut start = symbol.start.saturating_sub(1);
    while start > 0 {
        let previous = lines.get(start - 1).copied().unwrap_or_default().trim();
        if previous.starts_with("///")
            || previous.starts_with("//!")
            || previous.starts_with("/**")
            || previous.starts_with('*')
            || previous.ends_with("*/")
            || previous.starts_with("#[")
        {
            start -= 1;
        } else {
            break;
        }
    }
    let start_byte = lines
        .iter()
        .take(start)
        .map(|line| line.len() + 1)
        .sum::<usize>();
    let mut signature: Vec<_> = source
        .get(start_byte..symbol.signature_end)
        .unwrap_or_default()
        .lines()
        .enumerate()
        .map(|(index, line)| (start + index + 1, compact(line)))
        .collect();
    if let Some((start, end)) = symbol.doc {
        signature.extend(
            lines
                .iter()
                .enumerate()
                .take(end)
                .skip(start - 1)
                .map(|(index, line)| (index + 1, compact(line))),
        );
    }
    signature
}
fn answer(
    index: &Index,
    workspace: &Workspace,
    question: &Question,
    number: usize,
    changed: &BTreeSet<Arc<str>>,
    budget: usize,
    deadline: Deadline,
) -> Section {
    let mut section = Section::new(number, question);
    if matches!(question.op, Op::Find | Op::Outline) {
        let mut found = Vec::new();
        let candidates: Vec<&Arc<str>> = if let Some(name) = question
            .name
            .as_ref()
            .filter(|name| !name.ends_with('*') && !name.contains("::"))
        {
            index
                .names
                .iter()
                .filter(|(candidate, _)| candidate.to_lowercase() == name.to_lowercase())
                .flat_map(|(_, paths)| paths)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect()
        } else {
            index.files.keys().collect()
        };
        for path in candidates {
            if !within(path, question.path.as_deref()) {
                continue;
            }
            if matches!(question.op, Op::Outline) {
                let suffix = path
                    .strip_prefix(
                        question
                            .path
                            .as_deref()
                            .filter(|path| *path != ".")
                            .unwrap_or_default(),
                    )
                    .unwrap_or(path)
                    .trim_start_matches('/');
                if suffix.matches('/').count() > 2 {
                    continue;
                }
            }
            if deadline.check().is_err() {
                section.issues.push("analysis deadline reached".to_owned());
                break;
            }
            if let Some(file) = index.files.get(path) {
                for symbol in &file.symbols {
                    let rank = if matches!(question.op, Op::Outline) {
                        symbol.outline.then_some(0)
                    } else {
                        question
                            .name
                            .as_deref()
                            .and_then(|name| name_rank(symbol, name))
                    };
                    if let Some(rank) = rank {
                        found.push((
                            rank,
                            proximity(path, question.path.as_deref()),
                            path,
                            symbol,
                        ));
                    }
                }
            }
        }
        found.sort_by(|left, right| {
            (&left.0, &left.1, left.2, left.3.start).cmp(&(
                &right.0,
                &right.1,
                right.2,
                right.3.start,
            ))
        });
        for (_, _, path, symbol) in found {
            section.push(
                definition_entry(path.clone(), symbol, matches!(question.op, Op::Find)),
                budget,
            );
        }
        return section;
    }
    let name = question.name.as_deref().unwrap_or_default();
    let bare_name = name.rsplit("::").next().unwrap_or(name);
    let mut paths: Vec<Arc<str>> = if matches!(question.op, Op::Refs) {
        index
            .names
            .get(bare_name)
            .map(|paths| paths.iter().cloned().collect())
            .unwrap_or_default()
    } else {
        index.files.keys().cloned().collect()
    };
    paths.retain(|path| {
        if matches!(question.op, Op::Errors) && question.path.as_deref() == Some("changed") {
            changed.contains(path)
        } else {
            within(path, question.path.as_deref())
        }
    });
    paths.sort_by(|left, right| {
        (proximity(left, question.path.as_deref()), left)
            .cmp(&(proximity(right, question.path.as_deref()), right))
    });
    if matches!(question.op, Op::Refs) && paths.len() > MAX_REF_FILES {
        section.omitted_files = paths.len() - MAX_REF_FILES;
        paths.truncate(MAX_REF_FILES);
    }
    for (path_index, path) in paths.iter().enumerate() {
        if deadline.check().is_err() {
            section.omitted_files += paths.len() - path_index;
            section.issues.push("analysis deadline reached".to_owned());
            break;
        }
        if matches!(question.op, Op::Body)
            && !index.files.get(path).is_some_and(|file| {
                file.symbols.iter().any(|symbol| {
                    if question.name.is_some() {
                        name_rank(symbol, name).is_some()
                    } else {
                        question
                            .line
                            .is_some_and(|line| symbol.start <= line && line <= symbol.end)
                    }
                })
            })
        {
            continue;
        }
        let grammars = match grammars() {
            Ok(grammars) => grammars,
            Err(error) => {
                section.issues.push(error.message);
                break;
            }
        };
        let (source, tree) = match read_parse(workspace, path, deadline) {
            Ok(parsed) => parsed,
            Err(error) => {
                section.issues.push(format!("{path}: {}", error.message));
                continue;
            }
        };
        if matches!(question.op, Op::Errors) {
            section.files.insert(path.clone());
            let starts = compute_line_starts(&source);
            let mut errors = Vec::new();
            let extracted = walk_tree(&source, &mut tree.walk(), &mut errors, &starts, deadline)
                .and_then(|()| {
                    let offsets: Vec<usize> = errors
                        .iter()
                        .flat_map(|error| [error.start, error.end])
                        .collect();
                    offsets_to_positions(&source, &offsets, deadline)
                });
            match extracted {
                Ok(positions) => {
                    for (error, positions) in errors.iter().zip(positions.chunks_exact(2)) {
                        let (line, column) = positions[0];
                        let kind = error
                            .expected
                            .map(|expected| format!("MISSING {expected}"))
                            .unwrap_or_else(|| "ERROR".to_owned());
                        section.push(
                            Entry {
                                path: path.clone(),
                                line: line as usize,
                                key: format!("error:{}:{kind}", error.start),
                                lines: vec![format!("{line}:{column} {kind}")],
                            },
                            budget,
                        );
                    }
                }
                Err(error) => section.issues.push(error.message),
            }
            continue;
        }
        let grammar = grammars.get(detect_language(path).unwrap_or(LanguageKind::Rust));
        let symbols = match symbols(&source, &tree, grammar, deadline) {
            Ok(symbols) => symbols,
            Err(error) => {
                section.issues.push(error.message);
                continue;
            }
        };
        if matches!(question.op, Op::Body) {
            let mut found: Vec<&Symbol> = symbols
                .iter()
                .filter(|symbol| {
                    if question.name.is_some() {
                        name_rank(symbol, name).is_some()
                    } else {
                        question
                            .line
                            .is_some_and(|line| symbol.start <= line && line <= symbol.end)
                    }
                })
                .collect();
            if question.name.is_none() {
                found.sort_by_key(|symbol| symbol.end_byte - symbol.start_byte);
                found.truncate(1);
            } else {
                found.sort_by_key(|symbol| name_rank(symbol, name));
            }
            for symbol in found {
                let lines = if question.signature {
                    signature_lines(&source, symbol)
                } else {
                    source
                        .lines()
                        .enumerate()
                        .take(symbol.end)
                        .skip(symbol.start - 1)
                        .map(|(index, line)| (index + 1, compact(line)))
                        .collect()
                };
                let mut body = vec![format!(
                    "{} {} {}-{}",
                    symbol.kind, symbol.qualified, symbol.start, symbol.end
                )];
                body.extend(
                    lines
                        .into_iter()
                        .map(|(line, code)| format!("{line} {code}")),
                );
                section.push(
                    Entry {
                        path: path.clone(),
                        line: symbol.start,
                        key: format!("body:{}:{}", symbol.start_byte, question.signature),
                        lines: body,
                    },
                    budget,
                );
            }
        } else {
            let lines: Vec<&str> = source.lines().collect();
            if let Err(error) = visit(&tree, deadline, |node| {
                if identifier(node)
                    && node_text(node, &source) == bare_name
                    && !is_definition(node)
                    && question
                        .kind
                        .as_ref()
                        .is_none_or(|kind| reference_kind(node) == kind)
                {
                    let line = node.start_position().row + 1;
                    section.push(
                        Entry {
                            path: path.clone(),
                            line,
                            key: format!("ref:{line}"),
                            lines: vec![format!(
                                "{line} in {}: {}",
                                enclosing(&symbols, node),
                                compact(lines.get(line - 1).copied().unwrap_or_default())
                            )],
                        },
                        budget,
                    );
                }
            }) {
                section.issues.push(error.message);
            }
        }
    }
    section
}

fn take_bytes(text: &str, limit: usize) -> &str {
    let mut end = limit.min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}
fn render(sections: &[Section], report: &str, budget: usize) -> (String, Vec<Value>) {
    let mut text = String::new();
    let mut counts = Vec::new();
    let mut printed = BTreeSet::new();
    let mut printed_symbols = BTreeSet::new();
    let mut printed_source = BTreeSet::new();
    let overhead: usize = sections
        .iter()
        .map(|section| section.heading.len() + section.counts(0).len() + 60)
        .sum::<usize>()
        + report.len()
        + 1;
    if overhead > budget {
        for (index, section) in sections.iter().enumerate() {
            text.push_str(&format!(
                "#{} {} {}, {} files, 0 shown\n",
                index + 1,
                section.total,
                section.unit,
                section.files.len()
            ));
            counts.push(json!({"found": section.total, "files": section.files.len(), "shown": 0, "omitted_files": section.omitted_files}));
        }
        text.push_str(report);
        text.push('\n');
        return (take_bytes(&text, budget).to_owned(), counts);
    }
    let available = budget.saturating_sub(overhead);
    let weight: usize = sections.iter().map(|section| section.demand_bytes).sum();
    for section in sections {
        text.push_str(&section.heading);
        text.push('\n');
        let share = if weight == 0 {
            0
        } else {
            available.saturating_mul(section.demand_bytes) / weight
        };
        let start = text.len();
        let mut shown = 0;
        let mut last_path: Option<&str> = None;
        let mut cut = false;
        for entry in &section.entries {
            let symbol = entry
                .key
                .strip_prefix("definition:")
                .or_else(|| entry.key.strip_prefix("body:"))
                .map(|key| {
                    (
                        entry.path.clone(),
                        key.split(':').next().unwrap_or_default().to_owned(),
                    )
                });
            let symbol_printed = symbol
                .as_ref()
                .is_some_and(|key| printed_symbols.contains(key));
            let duplicate = printed.contains(&(entry.path.clone(), entry.key.clone()))
                || entry.key.starts_with("definition:") && symbol_printed;
            let mut rows = Vec::new();
            if duplicate {
                rows.push((format!("@{}:{}", entry.path, entry.line), None));
            } else {
                if last_path != Some(&entry.path) {
                    rows.push((entry.path.to_string(), None));
                }
                for (index, line) in entry.lines.iter().enumerate() {
                    if index == 0 && symbol_printed {
                        rows.push((format!("@{}:{}", entry.path, entry.line), None));
                        continue;
                    }
                    let source = if entry.key.starts_with("body:") && index > 0 {
                        line.split_once(' ').and_then(|(number, code)| {
                            number
                                .parse::<usize>()
                                .ok()
                                .map(|number| (entry.path.clone(), number, code.to_owned()))
                        })
                    } else if entry.key.starts_with("definition:") && index == 1 {
                        Some((entry.path.clone(), entry.line, line.trim().to_owned()))
                    } else {
                        None
                    };
                    if let Some(key) = source.as_ref().filter(|key| printed_source.contains(*key)) {
                        rows.push((format!("@{}:{}", entry.path, key.1), None));
                    } else {
                        rows.push((line.clone(), source));
                    }
                }
            }
            for (line, source) in rows {
                if text.len() - start + line.len() + 1 > share {
                    cut = true;
                    break;
                }
                text.push_str(&line);
                text.push('\n');
                if let Some(source) = source {
                    printed_source.insert(source);
                }
            }
            if cut {
                break;
            }
            if let Some(symbol) = symbol {
                printed_symbols.insert(symbol);
            }
            printed.insert((entry.path.clone(), entry.key.clone()));
            last_path = Some(&entry.path);
            shown += 1;
        }

        text.push_str(&section.counts(shown));
        text.push('\n');
        if cut || shown < section.total {
            text.push_str(&format!(
                "+{} more; add path= to narrow\n",
                section.total.saturating_sub(shown)
            ));
        }
        for issue in &section.issues {
            if text.len() + issue.len() + report.len() + 2 <= budget {
                text.push_str(issue);
                text.push('\n');
            }
        }
        counts.push(json!({"found": section.total, "files": section.files.len(), "shown": shown, "omitted_files": section.omitted_files}));
    }
    text.push_str(report);
    text.push('\n');
    (take_bytes(&text, budget).to_owned(), counts)
}

#[derive(Clone)]
struct WorkspaceCode {
    workspace: Workspace,
    shared: Arc<Shared>,
    previous: Arc<Mutex<BTreeMap<Arc<str>, FileStamp>>>,
    max_result_bytes: usize,
    timeout: Duration,
}
impl ToolHandler for WorkspaceCode {
    fn version(&self) -> &str {
        HANDLER_VERSION
    }
    fn effect(&self) -> ToolEffect {
        ToolEffect::ReadOnly
    }
    fn call(&self, arguments: Value) -> ToolFuture<'_> {
        Box::pin(async move {
            let call = call_of(arguments)?;
            let started = Instant::now();
            let deadline = Deadline(started + self.timeout);
            self.shared.available.recv_async().await.map_err(refused)?;
            let lease = Arc::new(Lease(self.shared.clone()));
            let workspace = self.workspace.clone();
            let worker_lease = lease.clone();
            let previous = self.previous.clone();
            let (scan, changed, pending) = blocking::run(Kind::Cpu, move || {
                let scan = scan(&workspace, deadline)?;
                let mut index = worker_lease
                    .0
                    .index
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                let removed: Vec<Arc<str>> = index
                    .files
                    .keys()
                    .filter(|path| !scan.files.contains_key(*path))
                    .cloned()
                    .collect();
                for path in removed {
                    index.remove(&path);
                }
                let previous = previous.lock().unwrap_or_else(PoisonError::into_inner);
                let changed = scan
                    .files
                    .iter()
                    .filter(|(path, stamp)| previous.get(*path) != Some(*stamp))
                    .map(|(path, _)| path.clone())
                    .collect::<BTreeSet<_>>();
                let pending: Vec<_> = scan
                    .files
                    .iter()
                    .filter(|(path, stamp)| {
                        index
                            .files
                            .get(*path)
                            .is_none_or(|file| file.stamp != **stamp || racy(file))
                    })
                    .map(|(path, stamp)| (path.clone(), *stamp))
                    .collect();
                // A failed refresh must never leave a stale definition searchable.
                for (path, _) in &pending {
                    index.remove(path);
                }
                Ok::<_, ToolFailure>((scan, changed, pending))
            })
            .await
            .map_err(ToolFailure::uncertain)??;
            let midpoint = pending.len().div_ceil(2);
            let batches = [pending[..midpoint].to_vec(), pending[midpoint..].to_vec()];
            let workers = batches
                .into_iter()
                .filter(|batch| !batch.is_empty())
                .map(|batch| {
                    let workspace = self.workspace.clone();
                    let lease = lease.clone();
                    blocking::run(Kind::Cpu, move || {
                        let mut parsed = 0;
                        let mut skipped = Vec::new();
                        for (path, stamp) in batch {
                            if deadline.check().is_err() {
                                skipped.push(format!("{path}: deadline"));
                                continue;
                            }
                            let parsed_at = SystemTime::now();
                            let extracted = read_parse(&workspace, &path, deadline).and_then(
                                |(source, tree)| {
                                    let grammar = grammars()?.get(
                                        detect_language(&path)
                                            .ok_or_else(|| refused("unsupported language"))?,
                                    );
                                    let symbols = symbols(&source, &tree, grammar, deadline)?;
                                    let mut names = BTreeSet::new();
                                    visit(&tree, deadline, |node| {
                                        if identifier(node) {
                                            names.insert(node_text(node, &source).to_owned());
                                        }
                                    })?;
                                    Ok(FileIndex {
                                        stamp,
                                        parsed_at,
                                        symbols,
                                        names,
                                    })
                                },
                            );
                            match extracted {
                                Ok(file) => {
                                    lease
                                        .0
                                        .index
                                        .lock()
                                        .unwrap_or_else(PoisonError::into_inner)
                                        .insert(path, file);
                                    parsed += 1;
                                }
                                Err(error) => skipped.push(format!("{path}: {}", error.message)),
                            }
                        }
                        (parsed, skipped)
                    })
                });
            let refreshed = ntex::util::join_all(workers).await;
            let mut reparsed = 0;
            let mut failures = Vec::new();
            for refreshed in refreshed {
                let (count, skipped) = refreshed.map_err(ToolFailure::uncertain)?;
                reparsed += count;
                failures.extend(skipped);
            }
            let workspace = self.workspace.clone();
            let previous = self.previous.clone();
            let max_result_bytes = self.max_result_bytes.min(16 * 1024);
            let budget = call.max_bytes.unwrap_or(6 * 1024).min(max_result_bytes);
            blocking::run(Kind::Cpu, move || {
                workspace.open_root().map_err(refused)?;
                let index = lease.0.index.lock().unwrap_or_else(PoisonError::into_inner);
                let sections: Vec<_> = call.queries.iter().enumerate().map(|(number, question)| answer(&index, &workspace, question, number, &changed, budget, deadline)).collect();
                let report = format!("-- index: {} files, {reparsed} re-parsed, {} ms; {} skipped, {} unsupported{}{}", index.files.len(), started.elapsed().as_millis(), scan.skipped + failures.len(), scan.unsupported, if scan.limits.is_empty() { String::new() } else { format!("; bounds: {}", scan.limits.iter().cloned().collect::<Vec<_>>().join(", ")) }, if failures.is_empty() { String::new() } else { format!("; skipped: {}", compact(&failures.join(", "))) });
                let mut text_budget = budget;
                let result = loop {
                    let (text, counts) = render(&sections, &report, text_budget);
                    let result = json!({"text": text, "counts": counts, "index": {"files": index.files.len(), "reparsed": reparsed, "skipped": scan.skipped + failures.len(), "unsupported": scan.unsupported, "bounds": scan.limits}});
                    // Re-render after accounting for JSON escaping, so summaries survive truncation.
                    let bytes = result.to_string().len();
                    if bytes <= max_result_bytes { break result; }
                    if text_budget == 0 { return Err(refused("result limit too small for counts")); }
                    text_budget = text_budget.saturating_sub(bytes - max_result_bytes);
                };
                *previous.lock().unwrap_or_else(PoisonError::into_inner) = scan.files;
                Ok(result)
            }).await.map_err(ToolFailure::uncertain)?
        })
    }
}

pub(crate) fn register(
    registry: &ToolRegistry,
    workspace: Workspace,
    max_result_bytes: usize,
    timeout: Duration,
) -> Result<(), ToolRegistryError> {
    let shared =
        shared(&workspace).map_err(|error| ToolRegistryError::Unregistered(error.message))?;
    registry.register(TOOL_NAME, Some(
        "Use workspace_code before reading files. Read symbols with body instead of whole files, and batch every question you already have into one call (up to 8 queries). find returns definitions/signatures by Name, Type::name or prefix*; refs returns identifier uses, optionally kind=call/type/import; body takes name or path+line, signature=true includes docs and signature; outline takes a file/folder (depth 2); errors takes path or path=changed since this Job's previous call. Every query supports a path prefix. Compact text shares max_bytes (default 6144, capped at the Job's 16 KiB result limit). Rust, TypeScript/TSX and Python only. Results are by name, not by type: macros, trait dispatch, aliases and runtime attributes may be unresolved, so confirm renames/deletions with cargo check or tests."
    ), json!({"type":"object", "additionalProperties":false, "required":["queries"], "properties":{
        "queries":{"type":"array", "minItems":1, "maxItems":8, "items":{"type":"object", "additionalProperties":false, "required":["op"], "properties":{
            "op":{"type":"string", "enum":["find","refs","body","outline","errors"]},
            "name":{"type":"string", "minLength":1}, "path":{"type":"string", "minLength":1},
            "line":{"type":"integer", "minimum":1}, "kind":{"type":"string", "enum":["call","type","import"]}, "signature":{"type":"boolean"}
        }}}, "max_bytes":{"type":"integer", "minimum":1}
    }}), Arc::new(WorkspaceCode { workspace, shared, previous: Arc::new(Mutex::default()), max_result_bytes, timeout }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{data_directory, temporary_directory};

    fn handler(workspace: Workspace) -> WorkspaceCode {
        WorkspaceCode {
            shared: shared(&workspace).expect("shared index"),
            workspace,
            previous: Arc::new(Mutex::default()),
            max_result_bytes: 16 * 1024,
            timeout: Duration::from_secs(10),
        }
    }
    fn call(handler: &WorkspaceCode, arguments: Value) -> Value {
        let handler = handler.clone();
        ntex::rt::System::new("workspace-code-test", ntex::rt::DefaultRuntime)
            .block_on(async move { handler.call(arguments).await })
            .expect("tool result")
    }
    fn text(result: &Value) -> &str {
        result["text"].as_str().expect("compact text")
    }

    #[test]
    fn batched_navigation_covers_all_grammars_and_identifier_kinds() {
        let (_guard, root) = temporary_directory("ocg-workspace-code", "languages-workspace");
        let (_data, directory) = data_directory("ocg-workspace-code", "languages");
        std::fs::write(root.join("sample.rs"), "use crate::other::helper;\nstruct Runner<T>(T);\nimpl<T> Runner<T> {\n /// Run a round.\n fn run(&self) { helper(); helper(); }\n}\nfn helper() {}\nfn uses(value: Runner<u8>) { helper(); let _ = \"helper\"; /* helper */ }\n").expect("Rust");
        std::fs::write(root.join("sample.ts"), "export function plain(value: number): number { return value; }\nconst arrow = () => plain(1);\nclass Runner { run() { return plain(2); } }\ninterface Shape { size(): number; }\n").expect("TypeScript");
        std::fs::write(
            root.join("sample.tsx"),
            "export function View() { return <div>Hello</div>; }\n",
        )
        .expect("TSX");
        std::fs::write(root.join("sample.py"), "class Worker:\n    @decorate\n    def run(self):\n        '''Run a Python round.'''\n        return helper()\ndef helper():\n    return 'helper' # helper\n").expect("Python");
        let handler = handler(Workspace::authorize(&root, &directory).expect("workspace"));
        let result = call(
            &handler,
            json!({"queries":[
                {"op":"find","name":"Runner::run"},
                {"op":"refs","name":"helper","kind":"call"},
                {"op":"body","name":"run","path":"sample.rs","signature":true},
                {"op":"outline","path":"."},
                {"op":"refs","name":"Runner","kind":"type"},
                {"op":"refs","name":"helper","kind":"import"},
                {"op":"body","path":"sample.py","line":4},
                {"op":"errors","path":"changed"}
            ]}),
        );
        let output = text(&result);
        for expected in [
            "Runner::run",
            "Run a round",
            "plain",
            "arrow",
            "View",
            "Worker::run",
            "return helper()",
        ] {
            assert!(output.contains(expected), "missing {expected}: {output}");
        }
        assert_eq!(result["counts"][1]["found"], 4);
        assert_eq!(result["counts"][4]["found"], 2);
        assert_eq!(result["counts"][5]["found"], 1);
        assert_eq!(result["counts"][7]["found"], 0);
        let signatures = call(
            &handler,
            json!({"queries":[
                {"op":"body","name":"Worker::run","path":"sample.py","signature":true},
                {"op":"body","name":"Runner::run","path":"sample.rs","signature":true}
            ]}),
        );
        assert!(text(&signatures).contains("Run a Python round."));
        assert!(!text(&signatures).contains("helper()"));
        assert_eq!(handler.version(), "workspace_code-1");
        assert_eq!(handler.effect(), ToolEffect::ReadOnly);
        assert!(!handler.spawns());
    }

    #[test]
    fn jobs_share_refreshes_but_track_their_own_changed_files_and_racy_edits() {
        let (_guard, root) = temporary_directory("ocg-workspace-code", "freshness-workspace");
        let (_data, directory) = data_directory("ocg-workspace-code", "freshness");
        let path = root.join("sample.rs");
        std::fs::write(&path, "fn alpha() {}\n").expect("source");
        let workspace = Workspace::authorize(&root, &directory).expect("workspace");
        let first = handler(workspace.clone());
        let second = handler(workspace);
        assert!(Arc::ptr_eq(&first.shared, &second.shared));
        let query =
            json!({"queries":[{"op":"find","name":"alpha"},{"op":"errors","path":"changed"}]});
        assert_eq!(call(&first, query.clone())["counts"][0]["found"], 1);
        call(&second, query.clone());
        let modified = std::fs::metadata(&path)
            .expect("metadata")
            .modified()
            .expect("modified");
        std::fs::write(&path, "fn bravo() {}\n").expect("same-size edit");
        std::fs::File::options()
            .write(true)
            .open(&path)
            .expect("open")
            .set_times(std::fs::FileTimes::new().set_modified(modified))
            .expect("same timestamp tick");
        let result = call(
            &first,
            json!({"queries":[{"op":"find","name":"alpha"},{"op":"body","name":"bravo"}]}),
        );
        assert_eq!(result["counts"][0]["found"], 0);
        assert!(text(&result).contains("fn bravo()"));
        std::fs::write(&path, "fn broken( {\n").expect("broken edit");
        let changed = json!({"queries":[{"op":"errors","path":"changed"}]});
        assert!(
            call(&first, changed.clone())["counts"][0]["found"]
                .as_u64()
                .expect("errors")
                > 0
        );
        assert_eq!(call(&first, changed.clone())["counts"][0]["files"], 0);
        assert!(
            call(&second, changed)["counts"][0]["found"]
                .as_u64()
                .expect("other Job errors")
                > 0
        );
        std::fs::remove_file(&path).expect("delete");
        assert_eq!(call(&first, query)["index"]["files"], 0);
    }

    #[test]
    fn shared_budget_deduplicates_and_walk_respects_workspace_exclusions() {
        use std::os::unix::fs::symlink;
        let (_guard, root) = temporary_directory("ocg-workspace-code", "bounds-workspace");
        let (_data, directory) = data_directory("ocg-workspace-code", "bounds");
        std::fs::write(
            root.join("source.rs"),
            "/// café 😀\npub fn visible() {\n    let value = 1;\n}\n",
        )
        .expect("source");
        std::fs::write(root.join(".hidden.rs"), "fn secret() {}\n").expect("hidden");
        std::fs::create_dir(root.join("target")).expect("target");
        std::fs::write(root.join("target/hidden.rs"), "fn secret() {}\n").expect("skipped");
        std::fs::write(root.join("binary.rs"), b"fn secret() {}\0").expect("binary");
        std::fs::write(root.join("large.rs"), vec![b' '; MAX_PARSE_FILE_BYTES + 1]).expect("large");
        symlink(root.join("source.rs"), root.join("linked.rs")).expect("symlink");
        std::fs::File::options()
            .write(true)
            .open(root.join("source.rs"))
            .expect("source handle")
            .set_times(
                std::fs::FileTimes::new().set_modified(SystemTime::now() - Duration::from_secs(60)),
            )
            .expect("older timestamp");
        let handler = handler(Workspace::authorize(&root, &directory).expect("workspace"));
        let result = call(
            &handler,
            json!({"queries":[{"op":"body","name":"visible"},{"op":"body","name":"visible"},{"op":"find","name":"secret"}],"max_bytes":1024}),
        );
        assert_eq!(result["index"]["files"], 1);
        assert_eq!(result["counts"][2]["found"], 0);
        assert_eq!(text(&result).matches("let value = 1;").count(), 1);
        assert!(text(&result).contains("@source.rs:2"));
        let warm = call(
            &handler,
            json!({"queries":[{"op":"find","name":"visible"}]}),
        );
        assert_eq!(warm["index"]["reparsed"], 0);
        assert_eq!(
            offsets_to_positions(
                "😀x\n",
                &[4],
                Deadline(Instant::now() + Duration::from_secs(1))
            )
            .expect("UTF-16 position"),
            vec![(1, 3)]
        );
        for budget in [1, 100, 512, 16 * 1024, 32 * 1024] {
            let queries: Vec<Value> = (0..8)
                .map(|_| json!({"op":"body","name":"visible","signature":true}))
                .collect();
            let result = call(&handler, json!({"queries":queries,"max_bytes":budget}));
            assert!(text(&result).len() <= budget.min(16 * 1024));
            assert!(result.to_string().len() <= 16 * 1024);
            assert_eq!(result["counts"].as_array().expect("counts").len(), 8);
        }
        std::fs::create_dir(root.join("calls")).expect("callers");
        for index in 0..501 {
            std::fs::write(
                root.join(format!("calls/{index:03}.rs")),
                "fn caller() { visible(); }\n",
            )
            .expect("caller");
        }
        let capped = call(
            &handler,
            json!({"queries":[{"op":"refs","name":"visible","kind":"call","path":"calls"}]}),
        );
        assert_eq!(capped["counts"][0]["found"], 500);
        assert_eq!(capped["counts"][0]["omitted_files"], 1);
        assert!(text(&capped).contains("1 candidate files left"));
        assert!(call_of(json!({"queries":[]})).is_err());
        assert!(call_of(json!({"queries":[{"op":"body","path":"../outside","line":1}]})).is_err());
    }
}
