use crate::context::{classify, repomap};
use crate::error::{OcgError, Result};
use crate::process::{CaptureRunner, ProcessExit};
use fs2::FileExt;
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, Metadata, OpenOptions};
use std::io::Read;
use std::path::{Component, Path};
use std::time::{Duration, Instant, UNIX_EPOCH};

const MAX_FILES: usize = 20_000;
pub(crate) const MAX_FILE_BYTES: u64 = 1024 * 1024;
const MAX_TEXT_BYTES: u64 = 128 * 1024 * 1024;
const MAX_CHUNKS: usize = 100_000;
const CHUNK_BYTES: usize = 4096;
const SCHEMA: &str = "
PRAGMA auto_vacuum=FULL;
PRAGMA max_page_count=131072;
CREATE TABLE IF NOT EXISTS manifest (
 path TEXT PRIMARY KEY, size INTEGER NOT NULL, stamp TEXT NOT NULL,
 revision TEXT NOT NULL, state TEXT NOT NULL, chunk_count INTEGER NOT NULL, indexed_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS chunk_content (
 id INTEGER PRIMARY KEY, path TEXT NOT NULL, content TEXT NOT NULL,
 line_start INTEGER NOT NULL, line_end INTEGER NOT NULL, revision TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS chunk_path ON chunk_content(path);
CREATE VIRTUAL TABLE IF NOT EXISTS chunks USING fts5(content, content='chunk_content', content_rowid='id');
CREATE TRIGGER IF NOT EXISTS chunk_insert AFTER INSERT ON chunk_content BEGIN
 INSERT INTO chunks(rowid,content) VALUES(new.id,new.content);
END;
CREATE TRIGGER IF NOT EXISTS chunk_delete AFTER DELETE ON chunk_content BEGIN
 INSERT INTO chunks(chunks,rowid,content) VALUES('delete',old.id,old.content);
END;
CREATE TABLE IF NOT EXISTS identity (root TEXT NOT NULL);
PRAGMA user_version=1;";

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
    #[error("unsupported context index version")]
    Version,
}

fn rebuildable(error: &IndexError) -> bool {
    matches!(error, IndexError::Version)
        || matches!(error,
        IndexError::Sql(rusqlite::Error::SqliteFailure(error, _))
        if matches!(error.code, rusqlite::ErrorCode::DatabaseCorrupt | rusqlite::ErrorCode::NotADatabase))
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
    let result = search_locked(root, prefix, &terms.join(" AND "), limit, runner, cancelled);
    result.map_err(|error| OcgError::config(format!("Project context index: {error}")))
}

fn confined(root: &Path, path: &Path) -> IndexResult<()> {
    if fs::symlink_metadata(path)?.file_type().is_symlink()
        || !path.canonicalize()?.starts_with(root)
    {
        return Err(IndexError::Boundary(
            "index state or source escapes Project root".into(),
        ));
    }
    Ok(())
}

fn state_exists(root: &Path, path: &Path) -> IndexResult<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => {
            confined(root, path)?;
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn ensure_directory(root: &Path, path: &Path) -> IndexResult<()> {
    if !state_exists(root, path)? {
        match fs::create_dir(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
    }
    confined(root, path)
}

fn search_locked(
    root: &Path,
    prefix: &Path,
    query: &str,
    limit: usize,
    runner: &dyn CaptureRunner,
    cancelled: &dyn Fn() -> bool,
) -> IndexResult<Value> {
    let started = Instant::now();
    let state = root.join(repomap::OCG_DIR);
    ensure_directory(root, &state)?;
    let directory = state.join(crate::context::index::INDEX_DIR);
    ensure_directory(root, &directory)?;
    let lock_path = directory.join("context-fulltext.lock");
    state_exists(root, &lock_path)?;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)?;
    loop {
        active(cancelled)?;
        match lock.try_lock_exclusive() {
            Ok(()) => break,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => return Err(error.into()),
        }
    }
    let database = directory.join("context-fulltext.sqlite3");
    for suffix in ["", "-journal", "-wal", "-shm"] {
        let path = directory.join(format!("context-fulltext.sqlite3{suffix}"));
        state_exists(root, &path)?;
    }
    let mut rebuilt = !state_exists(root, &database)?;
    // The lock covers connection lifetime, refresh, query and corruption recovery.
    // Only this derived database is discarded; canonical state is never opened here.
    for retry in 0..2 {
        active(cancelled)?;
        let outcome = (|| {
            let mut connection = Connection::open(&database)?;
            connection.busy_timeout(Duration::from_millis(100))?;
            let version: i64 =
                connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
            if version != 0 && version != 1 {
                return Err(IndexError::Version);
            }
            connection.execute_batch(SCHEMA)?;
            let stored_root: Option<String> = connection
                .query_row("SELECT root FROM identity", [], |row| row.get(0))
                .optional()?;
            let current_root = root.to_string_lossy();
            if stored_root.as_deref() != Some(current_root.as_ref()) {
                connection.execute_batch(
                    "DELETE FROM manifest; DELETE FROM chunk_content; DELETE FROM identity;",
                )?;
                connection.execute("INSERT INTO identity VALUES (?1)", [current_root.as_ref()])?;
                rebuilt = true;
            }
            refresh_and_query(
                &mut connection,
                root,
                prefix,
                query,
                limit,
                runner,
                cancelled,
                started,
                rebuilt,
            )
        })();
        match outcome {
            Err(ref error) if retry == 0 && rebuildable(error) => {
                for suffix in ["", "-journal", "-wal", "-shm"] {
                    let path = directory.join(format!("context-fulltext.sqlite3{suffix}"));
                    if state_exists(root, &path)? {
                        fs::remove_file(path)?;
                    }
                }
                rebuilt = true;
            }
            other => return other,
        }
    }
    Err(IndexError::Boundary("cannot rebuild context index".into()))
}

#[allow(clippy::too_many_arguments)]
fn refresh_and_query(
    connection: &mut Connection,
    root: &Path,
    prefix: &Path,
    query: &str,
    limit: usize,
    runner: &dyn CaptureRunner,
    cancelled: &dyn Fn() -> bool,
    started: Instant,
    rebuilt: bool,
) -> IndexResult<Value> {
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
    let transaction = connection.transaction()?;
    let prior: BTreeMap<String, Entry> = {
        let mut statement = transaction
            .prepare("SELECT path,size,stamp,revision,state,chunk_count FROM manifest")?;
        let rows = statement.query_map([], |row| {
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
        rebuilt,
        ..Refresh::default()
    };
    for deleted in prior.keys().filter(|path| !paths.contains(*path)) {
        active(cancelled)?;
        transaction.execute("DELETE FROM chunk_content WHERE path=?1", [deleted])?;
        transaction.execute("DELETE FROM manifest WHERE path=?1", [deleted])?;
        stats.deleted_files += 1;
    }
    let now = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| IndexError::Boundary(error.to_string()))?
        .as_secs();
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
                transaction.execute("DELETE FROM chunk_content WHERE path=?1", [path])?;
                transaction.execute("DELETE FROM manifest WHERE path=?1", [path])?;
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
                transaction.execute("DELETE FROM chunk_content WHERE path=?1", [path])?;
                if let Some(content) = content.as_deref() {
                    insert_chunks(&transaction, path, content, &revision, cancelled)?;
                    stats.files_reindexed += 1;
                    stats.bytes_indexed += content.len() as u64;
                }
            }
            transaction.execute("INSERT INTO manifest VALUES (?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(path) DO UPDATE SET size=excluded.size,stamp=excluded.stamp,revision=excluded.revision,state=excluded.state,chunk_count=excluded.chunk_count,indexed_at=CASE WHEN manifest.revision=excluded.revision AND manifest.state=excluded.state THEN manifest.indexed_at ELSE excluded.indexed_at END",
                params![path, metadata.len().min(i64::MAX as u64) as i64, stamp, revision, state, chunk_count as i64, now as i64])?;
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
        let mut statement = transaction.prepare("SELECT c.path,c.line_start,c.line_end,snippet(chunks,0,'','',' … ',48),c.revision FROM chunks JOIN chunk_content c ON c.id=chunks.rowid WHERE chunks MATCH ?1 AND (?2='' OR substr(c.path,1,length(?2))=?2) ORDER BY rank,c.path,c.line_start LIMIT ?3")?;
        let mut rows = statement.query(params![query, prefix, (limit + 1) as i64])?;
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
    active(cancelled)?;
    transaction.commit()?;
    stats.total_bytes_indexed = total_bytes;
    stats.elapsed_ms = started.elapsed().as_millis();
    Ok(
        json!({"matches":matches, "index_revision":format!("sha256:{}",crate::hash::sha256_hex(material.as_bytes())),
        "stale":stale, "truncated":truncated, "coverage_limited":stats.limited, "refresh":stats}),
    )
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
    path: &str,
    content: &str,
    revision: &str,
    cancelled: &dyn Fn() -> bool,
) -> IndexResult<()> {
    let mut statement = transaction.prepare("INSERT INTO chunk_content(path,content,line_start,line_end,revision) VALUES (?1,?2,?3,?4,?5)")?;
    for (start, end, line_start, line_end) in chunks(content) {
        active(cancelled)?;
        statement.execute(params![
            path,
            &content[start..end],
            line_start,
            line_end,
            revision
        ])?;
    }
    Ok(())
}
