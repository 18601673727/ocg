use crate::context::{classify, repomap};
use crate::error::{OcgError, Result};
use crate::process::{CaptureRunner, ProcessExit};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, Metadata};
use std::io::Read;
use std::path::{Component, Path};
use std::time::{Instant, UNIX_EPOCH};

const MAX_FILES: usize = 20_000;
pub(crate) const MAX_FILE_BYTES: u64 = 1024 * 1024;
const MAX_TEXT_BYTES: u64 = 128 * 1024 * 1024;
const MAX_CHUNKS: usize = 100_000;
const CHUNK_BYTES: usize = 4096;
/// Changed files applied per write transaction, so a refresh holds the
/// database's writer lock only briefly and other Projects keep executing.
const FILES_PER_BATCH: usize = 64;

/// The Project context index tables in the OCG database: a manifest of the
/// Project's files and their chunks under an FTS5 index. Every row belongs to
/// one Project; the index is derived and rebuilt from the Project's files.
pub(crate) const SCHEMA: &str = r#"
CREATE TABLE context_manifest (
    project_id TEXT NOT NULL REFERENCES domain_projects(id),
    path TEXT NOT NULL,
    size INTEGER NOT NULL,
    stamp TEXT NOT NULL,
    revision TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('indexed','binary','excluded','large','capacity')),
    chunk_count INTEGER NOT NULL,
    indexed_at INTEGER NOT NULL,
    PRIMARY KEY(project_id, path)
) STRICT;
CREATE TABLE context_chunk_content (
    id INTEGER PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES domain_projects(id),
    path TEXT NOT NULL,
    content TEXT NOT NULL,
    line_start INTEGER NOT NULL,
    line_end INTEGER NOT NULL,
    revision TEXT NOT NULL
) STRICT;
CREATE INDEX context_chunk_path ON context_chunk_content(project_id, path);
CREATE VIRTUAL TABLE context_chunks USING fts5(content, content='context_chunk_content', content_rowid='id');
CREATE TRIGGER context_chunk_insert AFTER INSERT ON context_chunk_content BEGIN
    INSERT INTO context_chunks(rowid,content) VALUES(new.id,new.content);
END;
CREATE TRIGGER context_chunk_delete AFTER DELETE ON context_chunk_content BEGIN
    INSERT INTO context_chunks(context_chunks,rowid,content) VALUES('delete',old.id,old.content);
END;
-- Bounded validation evidence, at most 128 recent records per Project.
CREATE TABLE context_validation_evidence (
    project_id TEXT NOT NULL REFERENCES domain_projects(id),
    call_id TEXT NOT NULL,
    finished INTEGER NOT NULL,
    body TEXT NOT NULL CHECK(length(body)<=8192),
    PRIMARY KEY(project_id, call_id)
) STRICT;
"#;

#[derive(Debug, thiserror::Error)]
enum IndexError {
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Ocg(#[from] OcgError),
    #[error("{0}")]
    Boundary(String),
}

type IndexResult<T> = std::result::Result<T, IndexError>;

#[derive(Default, Serialize)]
struct Refresh {
    files_discovered: usize,
    files_rescanned: usize,
    files_metadata_checked: usize,
    files_reindexed: usize,
    changed_files: usize,
    deleted_files: usize,
    bytes_indexed: u64,
    files_indexed: usize,
    total_bytes_indexed: u64,
    elapsed_ms: u128,
    rebuilt: bool,
    limited: bool,
}

struct Entry {
    size: u64,
    stamp: String,
    revision: String,
    state: String,
    chunk_count: usize,
}

/// One change a refresh applies to the stored index.
enum Change {
    /// The file is gone from the Project.
    Delete(String),
    /// The file's manifest row, and its new content when that changed.
    Upsert {
        path: String,
        size: u64,
        stamp: String,
        revision: String,
        state: String,
        chunk_count: usize,
        content: Option<Option<String>>,
    },
}

fn active(cancelled: &dyn Fn() -> bool) -> IndexResult<()> {
    if cancelled() {
        return Err(IndexError::Boundary("context search cancelled".into()));
    }
    Ok(())
}

pub(crate) fn search(
    root: &Path,
    prefix: &Path,
    query: &str,
    limit: usize,
    runner: &dyn CaptureRunner,
    cancelled: &dyn Fn() -> bool,
) -> Result<Value> {
    let terms: Vec<String> = query
        .split(|character: char| !character.is_alphanumeric())
        .filter(|term| !term.is_empty())
        .map(|term| format!("\"{term}\""))
        .collect();
    if terms.is_empty() || query.len() > 512 || !(1..=50).contains(&limit) {
        return Err(OcgError::config(
            "context query requires word tokens and limit 1..50",
        ));
    }
    let result = (|| {
        let (mut connection, project_id) = open_index(root)?;
        refresh_and_query(
            &mut connection,
            &project_id,
            root,
            prefix,
            &terms.join(" AND "),
            limit,
            runner,
            cancelled,
        )
    })();
    result.map_err(|error| OcgError::config(format!("Project context index: {error}")))
}

fn confined(root: &Path, path: &Path) -> IndexResult<()> {
    if fs::symlink_metadata(path)?.file_type().is_symlink()
        || !path.canonicalize()?.starts_with(root)
    {
        return Err(IndexError::Boundary("source escapes Project root".into()));
    }
    Ok(())
}

/// The index a search refreshes and queries, and the Project it belongs to.
///
/// Unconfined, that is the Project's index in the OCG database. A confined
/// helper cannot write OCG state at all: it seeds a bounded in-memory index
/// from the read-only Project snapshot it was given, refreshes that copy and
/// queries it. Nothing it computes outlives the call.
fn open_index(root: &Path) -> IndexResult<(Connection, String)> {
    if crate::remote_execution::native_confined() {
        let snapshot = std::env::var_os("OCG_NATIVE_DOMAIN_SNAPSHOT").ok_or_else(|| {
            IndexError::Boundary("confined context search needs a Project snapshot".into())
        })?;
        let connection = Connection::open_in_memory()?;
        connection.execute_batch(SCHEMA)?;
        connection.execute(
            "ATTACH DATABASE ?1 AS seed",
            [Path::new(&snapshot).to_string_lossy().as_ref()],
        )?;
        let project_id: String =
            connection.query_row("SELECT id FROM seed.domain_projects", [], |row| row.get(0))?;
        connection.execute_batch(
            "INSERT INTO main.context_manifest SELECT * FROM seed.context_manifest;
             INSERT INTO main.context_chunk_content SELECT * FROM seed.context_chunk_content;",
        )?;
        connection.execute_batch("DETACH DATABASE seed;")?;
        return Ok((connection, project_id));
    }
    let project_id = crate::project_store::ProjectDocuments::for_root(root)?
        .project_id()
        .to_string();
    Ok((crate::database::connect()?, project_id))
}

#[allow(clippy::too_many_arguments)]
fn refresh_and_query(
    connection: &mut Connection,
    project_id: &str,
    root: &Path,
    prefix: &Path,
    query: &str,
    limit: usize,
    runner: &dyn CaptureRunner,
    cancelled: &dyn Fn() -> bool,
) -> IndexResult<Value> {
    let started = Instant::now();
    let paths = discover(root, runner, cancelled)?;
    let prior: BTreeMap<String, Entry> = {
        let mut statement = connection.prepare(
            "SELECT path,size,stamp,revision,state,chunk_count FROM context_manifest WHERE project_id=?1",
        )?;
        let rows = statement.query_map([project_id], |row| {
            Ok((
                row.get(0)?,
                Entry {
                    size: row.get::<_, i64>(1)? as u64,
                    stamp: row.get(2)?,
                    revision: row.get(3)?,
                    state: row.get(4)?,
                    chunk_count: row.get::<_, i64>(5)? as usize,
                },
            ))
        })?;
        rows.collect::<std::result::Result<_, _>>()?
    };
    let mut stats = Refresh {
        files_discovered: paths.len(),
        rebuilt: prior.is_empty(),
        ..Refresh::default()
    };
    // Everything that reads the Project happens here, with no transaction
    // open; only the resulting changes are written.
    let mut changes: Vec<Change> = prior
        .keys()
        .filter(|path| !paths.contains(*path))
        .map(|path| Change::Delete(path.clone()))
        .collect();
    stats.deleted_files = changes.len();
    let mut material = String::new();
    let mut total_bytes = 0u64;
    let mut total_chunks = 0usize;
    let mut stale = false;
    let mut observed = BTreeMap::new();
    for path in &paths {
        active(cancelled)?;
        if !Path::new(path)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
        {
            return Err(IndexError::Boundary("invalid discovered path".into()));
        }
        let absolute = root.join(path);
        let metadata = match fs::symlink_metadata(&absolute) {
            Ok(metadata) if metadata.is_file() => metadata,
            Ok(_) | Err(_) => {
                // A disappearing file or symlink cannot keep a previously searchable entry.
                changes.push(Change::Delete(path.clone()));
                stale = true;
                continue;
            }
        };
        confined(root, &absolute)?;
        stats.files_metadata_checked += 1;
        let stamp = metadata_stamp(&metadata);
        observed.insert(path, stamp.clone());
        let old = prior.get(path);
        let excluded = classify::classify(Path::new(path)).sensitive;
        let mut state = if excluded {
            "excluded"
        } else if metadata.len() > MAX_FILE_BYTES {
            "large"
        } else {
            "indexed"
        };
        if state == "indexed"
            && (total_bytes + metadata.len() > MAX_TEXT_BYTES
                || total_chunks + old.map(|old| old.chunk_count).unwrap_or(0) > MAX_CHUNKS)
        {
            state = "capacity";
        }
        stats.limited |= state == "capacity";
        let unchanged = cfg!(unix)
            && old.is_some_and(|old| {
                old.size == metadata.len()
                    && old.stamp == stamp
                    && (old.state == state || (state == "indexed" && old.state == "binary"))
            });
        let mut revision = old.map(|old| old.revision.clone()).unwrap_or_default();
        let mut content = None;
        let mut chunk_count = old.map(|old| old.chunk_count).unwrap_or(0);
        if !unchanged {
            stats.files_rescanned += 1;
            if state == "indexed" {
                let mut bytes = Vec::new();
                File::open(&absolute)?
                    .take(MAX_FILE_BYTES + 1)
                    .read_to_end(&mut bytes)?;
                if bytes.len() as u64 > MAX_FILE_BYTES
                    || metadata_stamp(&fs::symlink_metadata(&absolute)?) != stamp
                {
                    return Err(IndexError::Boundary(
                        "file changed during context refresh; retry search".into(),
                    ));
                }
                revision = format!("sha256:{}", crate::hash::sha256_hex(&bytes));
                if bytes.contains(&0) {
                    state = "binary";
                } else {
                    match String::from_utf8(bytes) {
                        Ok(text) => {
                            chunk_count = chunks(&text).len();
                            if total_chunks + chunk_count > MAX_CHUNKS {
                                state = "capacity";
                                stats.limited = true;
                                revision.clear();
                            } else {
                                content = Some(text);
                            }
                        }
                        Err(_) => state = "binary",
                    }
                }
            } else {
                revision.clear();
            }
            if state != "indexed" {
                chunk_count = 0;
            }
            let same_content =
                old.is_some_and(|old| old.revision == revision && old.state == state);
            if !same_content {
                stats.changed_files += 1;
                if let Some(content) = content.as_deref() {
                    stats.files_reindexed += 1;
                    stats.bytes_indexed += content.len() as u64;
                }
            }
            changes.push(Change::Upsert {
                path: path.clone(),
                size: metadata.len(),
                stamp,
                revision: revision.clone(),
                state: state.to_string(),
                chunk_count,
                content: (!same_content).then_some(content),
            });
        } else if let Some(old) = old {
            state = &old.state;
        }
        if state == "indexed" {
            total_bytes += metadata.len();
            total_chunks += chunk_count;
            stats.files_indexed += 1;
        }
        material.push_str(
            &serde_json::to_string(&(path, &revision, state))
                .map_err(|error| IndexError::Boundary(error.to_string()))?,
        );
    }
    let now = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| IndexError::Boundary(error.to_string()))?
        .as_secs();
    for batch in changes.chunks(FILES_PER_BATCH) {
        active(cancelled)?;
        apply(connection, project_id, batch, now, cancelled)?;
    }
    active(cancelled)?;
    let prefix = prefix.to_string_lossy().replace('\\', "/");
    let prefix = if prefix.is_empty() {
        prefix
    } else {
        format!("{prefix}/")
    };
    let mut matches = Vec::new();
    let mut match_bytes = 0;
    let mut truncated = false;
    {
        let mut statement = connection.prepare("SELECT c.path,c.line_start,c.line_end,snippet(context_chunks,0,'','',' … ',48),c.revision FROM context_chunks JOIN context_chunk_content c ON c.id=context_chunks.rowid WHERE context_chunks MATCH ?1 AND c.project_id=?4 AND (?2='' OR substr(c.path,1,length(?2))=?2) ORDER BY rank,c.path,c.line_start LIMIT ?3")?;
        let mut rows = statement.query(params![query, prefix, (limit + 1) as i64, project_id])?;
        while let Some(row) = rows.next()? {
            active(cancelled)?;
            let snippet: String = row.get(3)?;
            let item = json!({"path":row.get::<_,String>(0)?, "line_start":row.get::<_,i64>(1)?,
                "line_end":row.get::<_,i64>(2)?, "snippet":snippet.chars().take(512).collect::<String>(),
                "revision":row.get::<_,String>(4)?});
            match_bytes += item.to_string().len();
            if matches.len() == limit || match_bytes > crate::native_tools::TOOL_OUTPUT_CAP / 2 {
                truncated = true;
                break;
            }
            matches.push(item);
        }
    }
    for (path, stamp) in observed {
        active(cancelled)?;
        let metadata = fs::symlink_metadata(root.join(path))?;
        if !metadata.is_file() || metadata_stamp(&metadata) != stamp {
            return Err(IndexError::Boundary(
                "Project changed during context query; retry search".into(),
            ));
        }
    }
    stats.total_bytes_indexed = total_bytes;
    stats.elapsed_ms = started.elapsed().as_millis();
    Ok(
        json!({"matches":matches, "index_revision":format!("sha256:{}",crate::hash::sha256_hex(material.as_bytes())),
        "stale":stale, "truncated":truncated, "coverage_limited":stats.limited, "refresh":stats}),
    )
}

/// Apply one batch of changes in one short write transaction. A file's
/// manifest row and its chunks change together.
fn apply(
    connection: &mut Connection,
    project_id: &str,
    batch: &[Change],
    now: u64,
    cancelled: &dyn Fn() -> bool,
) -> IndexResult<()> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    for change in batch {
        active(cancelled)?;
        match change {
            Change::Delete(path) => {
                transaction.execute(
                    "DELETE FROM context_chunk_content WHERE project_id=?1 AND path=?2",
                    params![project_id, path],
                )?;
                transaction.execute(
                    "DELETE FROM context_manifest WHERE project_id=?1 AND path=?2",
                    params![project_id, path],
                )?;
            }
            Change::Upsert {
                path,
                size,
                stamp,
                revision,
                state,
                chunk_count,
                content,
            } => {
                if let Some(content) = content {
                    transaction.execute(
                        "DELETE FROM context_chunk_content WHERE project_id=?1 AND path=?2",
                        params![project_id, path],
                    )?;
                    if let Some(content) = content.as_deref() {
                        insert_chunks(
                            &transaction,
                            project_id,
                            path,
                            content,
                            revision,
                            cancelled,
                        )?;
                    }
                }
                transaction.execute("INSERT INTO context_manifest(project_id,path,size,stamp,revision,state,chunk_count,indexed_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8) ON CONFLICT(project_id,path) DO UPDATE SET size=excluded.size,stamp=excluded.stamp,revision=excluded.revision,state=excluded.state,chunk_count=excluded.chunk_count,indexed_at=CASE WHEN context_manifest.revision=excluded.revision AND context_manifest.state=excluded.state THEN context_manifest.indexed_at ELSE excluded.indexed_at END",
                    params![project_id, path, (*size).min(i64::MAX as u64) as i64, stamp, revision, state, *chunk_count as i64, now as i64])?;
            }
        }
    }
    transaction.commit()?;
    Ok(())
}

pub(crate) fn metadata_stamp(metadata: &Metadata) -> String {
    // ctime catches replacements and same-size writes even when mtime is restored.
    // Platforms without a change clock hash the bounded content on every refresh.
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        format!(
            "{}:{}:{}:{}:{}:{}",
            metadata.dev(),
            metadata.ino(),
            metadata.mtime(),
            metadata.mtime_nsec(),
            metadata.ctime(),
            metadata.ctime_nsec()
        )
    }
    #[cfg(not(unix))]
    {
        format!(
            "{:?}:{:?}:{}",
            metadata.modified(),
            metadata.created(),
            metadata.len()
        )
    }
}

pub(crate) fn status(
    root: &Path,
    revisions: Option<&BTreeMap<String, String>>,
    runner: &dyn CaptureRunner,
    cancelled: &dyn Fn() -> bool,
) -> Result<Value> {
    let read = || -> IndexResult<Value> {
        active(cancelled)?;
        // Status inspects the existing derived manifest. It never creates,
        // repairs or refreshes an index, and never reads chunk content.
        let (connection, project_id) = if crate::remote_execution::native_confined() {
            let Some(snapshot) = std::env::var_os("OCG_NATIVE_DOMAIN_SNAPSHOT") else {
                return Ok(json!({"available":false,"reason":"index_unavailable"}));
            };
            let connection =
                Connection::open_with_flags(snapshot, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
            let project_id: Option<String> = connection
                .query_row("SELECT id FROM domain_projects", [], |row| row.get(0))
                .optional()?;
            (connection, project_id)
        } else {
            let project_id = crate::project_store::ProjectDocuments::existing(root)?
                .map(|documents| documents.project_id().to_string());
            (crate::database::connect()?, project_id)
        };
        let Some(project_id) = project_id else {
            return Ok(json!({"available":false,"reason":"index_missing"}));
        };
        let count: i64 = connection.query_row(
            "SELECT count(*) FROM context_manifest WHERE project_id=?1",
            [&project_id],
            |row| row.get(0),
        )?;
        if count == 0 {
            return Ok(json!({"available":false,"reason":"index_missing"}));
        }
        if !(0..=MAX_FILES as i64).contains(&count) {
            return Err(IndexError::Boundary("manifest exceeds bounds".into()));
        }
        let paths = discover(root, runner, cancelled)?;
        let mut statement = connection.prepare(
            "SELECT path,size,stamp,revision,state,indexed_at FROM context_manifest WHERE project_id=?1 ORDER BY path",
        )?;
        let mut rows = statement.query([&project_id])?;
        let mut material = String::new();
        let mut indexed = 0;
        let mut skipped = 0;
        let mut last_indexed = 0i64;
        let mut stored_paths = BTreeSet::new();
        let mut stale = false;
        let mut freshness_known = revisions.is_some();
        while let Some(row) = rows.next()? {
            active(cancelled)?;
            let path: String = row.get(0)?;
            let size = u64::try_from(row.get::<_, i64>(1)?)
                .map_err(|_| IndexError::Boundary("invalid manifest size".into()))?;
            let stamp: String = row.get(2)?;
            let revision: String = row.get(3)?;
            let state: String = row.get(4)?;
            if path.len() > 4096
                || revision.len() > 71
                || !matches!(
                    state.as_str(),
                    "indexed" | "binary" | "excluded" | "large" | "capacity"
                )
            {
                return Err(IndexError::Boundary("invalid manifest entry".into()));
            }
            // Only paths freshly discovered under the root can authorize a stat.
            if paths.contains(&path) {
                let metadata = fs::symlink_metadata(root.join(&path))?;
                stale |= !metadata.is_file() || metadata.len() != size;
                if revision.is_empty() {
                    stale |= metadata_stamp(&metadata) != stamp;
                } else if let Some(current) = revisions.and_then(|revisions| revisions.get(&path)) {
                    stale |= *current != revision;
                } else {
                    freshness_known = false;
                }
            }
            indexed += usize::from(state == "indexed");
            skipped += usize::from(state != "indexed");
            last_indexed = last_indexed.max(row.get(5)?);
            material.push_str(
                &serde_json::to_string(&(&path, &revision, &state))
                    .map_err(|error| IndexError::Boundary(error.to_string()))?,
            );
            stored_paths.insert(path);
        }
        stale |= stored_paths != paths;
        Ok(
            json!({"available":true,"index_revision":format!("sha256:{}",crate::hash::sha256_hex(material.as_bytes())),
            "stale":if stale {Some(true)} else if freshness_known {Some(false)} else {None},
            "freshness_known":freshness_known,"indexed_files":indexed,"skipped_files":skipped,
            "coverage_limited":skipped>0,"last_indexed_at":last_indexed}),
        )
    };
    read().map_err(|error| OcgError::config(format!("Project context index status: {error}")))
}

fn chunks(content: &str) -> Vec<(usize, usize, i64, i64)> {
    let mut ranges = Vec::new();
    let mut start = 0;
    let mut line_start = 1;
    let mut line = 1;
    let mut lines = 0;
    for (offset, character) in content.char_indices() {
        if offset - start >= CHUNK_BYTES || lines == 32 {
            let end_line = if content[..offset].ends_with('\n') {
                line - 1
            } else {
                line
            };
            ranges.push((start, offset, line_start, end_line));
            start = offset;
            line_start = line;
            lines = 0;
        }
        if character == '\n' {
            line += 1;
            lines += 1;
        }
    }
    if start < content.len() {
        let end_line = if content.ends_with('\n') {
            line - 1
        } else {
            line
        };
        ranges.push((start, content.len(), line_start, end_line));
    }
    ranges
}

fn insert_chunks(
    transaction: &rusqlite::Transaction<'_>,
    project_id: &str,
    path: &str,
    content: &str,
    revision: &str,
    cancelled: &dyn Fn() -> bool,
) -> IndexResult<()> {
    let mut statement = transaction.prepare("INSERT INTO context_chunk_content(project_id,path,content,line_start,line_end,revision) VALUES (?1,?2,?3,?4,?5,?6)")?;
    for (start, end, line_start, line_end) in chunks(content) {
        active(cancelled)?;
        statement.execute(params![
            project_id,
            path,
            &content[start..end],
            line_start,
            line_end,
            revision
        ])?;
    }
    Ok(())
}

fn discover(
    root: &Path,
    runner: &dyn CaptureRunner,
    cancelled: &dyn Fn() -> bool,
) -> IndexResult<BTreeSet<String>> {
    let mut args = vec!["--files".into(), "--hidden".into(), "--null".into()];
    for excluded in repomap::EXCLUDED_DIRS
        .iter()
        .copied()
        .chain([repomap::OCG_DIR, ".codegraph"])
    {
        args.extend(["--glob".into(), format!("!**/{excluded}/**")]);
    }
    args.push(".".into());
    let discovered = runner.run_with_cancellation("rg", &args, root, 4 * 1024 * 1024, cancelled)?;
    active(cancelled)?;
    if discovered.truncated()
        || (!discovered.success && !matches!(discovered.exit, ProcessExit::Code(1)))
    {
        return Err(IndexError::Boundary(
            "bounded repository discovery failed; use filesystem.search".into(),
        ));
    }
    let paths = discovered
        .stdout
        .split(|byte| *byte == 0)
        .filter(|bytes| !bytes.is_empty())
        .map(|bytes| {
            std::str::from_utf8(bytes)
                .map(|path| path.strip_prefix("./").unwrap_or(path).to_owned())
        })
        .collect::<std::result::Result<BTreeSet<_>, _>>()
        .map_err(|_| IndexError::Boundary("repository paths must be UTF-8".into()))?;
    if paths.len() > MAX_FILES {
        return Err(IndexError::Boundary(
            "context manifest exceeds 20000 files".into(),
        ));
    }
    Ok(paths)
}
