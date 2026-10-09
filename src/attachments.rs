//! Managed Job attachments: binary content OCG keeps for a Job, such as files a
//! user submits with a Job, images in a Job's input, and images a model
//! returns.
//!
//! The bytes live as ordinary files under the attachment root; everything
//! about them lives in the OCG database. The root is the committed
//! `storage.attachments.path` configuration, `./ocg_attachments` by default,
//! resolved against the directory that holds `ocg.db` when relative. It is
//! created when the first attachment is stored. Each file is stored as
//! `<root>/<extension>/<attachment id>[.<extension>]`, where the extension is
//! the original name's, lower-cased and reduced to `[a-z0-9]`, or `no_ext`.
//! The original name is metadata only: it never forms part of a path, and an
//! extension is never taken as evidence of content.
//!
//! Every attachment row records the absolute location of its file, so a
//! changed root only affects new attachments; existing ones stay where their
//! row says they are, and a file that is missing there is reported, never
//! replaced.
//!
//! # Publication
//!
//! A database transaction cannot include a file write, so publication is
//! ordered so that no row ever claims bytes that are not complete:
//!
//! 1. a `staged` row names the attachment and its final location;
//! 2. the bytes are written to a temporary name beside that location, synced,
//!    and renamed into place;
//! 3. the row becomes `available` with its size and checksum.
//!
//! Only `available` rows are ever served. Every file OCG writes is named by a
//! row before it exists, so a publication interrupted at any step is found
//! from the database, never by scanning directories: a `staged` row older
//! than [`STAGED_EXPIRY_SECONDS`] is abandoned, and its files and row are
//! removed. A publication is identified by its operation key, unique per
//! Project, so repeating it returns the attachment it already stored.

use crate::error::{OcgError, Result};
use rusqlite::{params, Connection, OptionalExtension, Row, TransactionBehavior};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::{Path, PathBuf};

/// The configuration key of the attachment root.
pub const ROOT_KEY: &str = "storage.attachments.path";
/// The attachment root when none is configured.
pub const DEFAULT_ROOT: &str = "./ocg_attachments";
/// The largest attachment OCG stores.
pub const MAX_ATTACHMENT_BYTES: usize = 64 * 1024 * 1024;
/// How long a publication may stay `staged` before it is treated as
/// abandoned. Far longer than writing [`MAX_ATTACHMENT_BYTES`] takes.
pub const STAGED_EXPIRY_SECONDS: i64 = 3600;
/// The directory of files without a usable extension.
pub const NO_EXTENSION: &str = "no_ext";

/// The tables this component contributes to the OCG database.
pub(crate) const SCHEMA: &str = r#"
CREATE TABLE attachments (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES domain_projects(id),
    -- The Job that owns the attachment: the one it was produced for, or the
    -- first whose input referenced it. Unset only for an upload no Job has
    -- referenced yet.
    job_id TEXT REFERENCES domain_jobs(id),
    attempt_id TEXT REFERENCES domain_attempts(id),
    call_id TEXT REFERENCES domain_calls(id),
    -- Identifies the publication, so repeating it stores nothing new.
    operation_key TEXT NOT NULL,
    original_name TEXT NOT NULL,
    extension TEXT NOT NULL,
    media_type TEXT,
    size INTEGER NOT NULL CHECK(size >= 0),
    sha256 TEXT NOT NULL,
    -- The absolute path of the file, independent of the current root.
    location TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK(state IN ('staged','available')),
    created_at INTEGER NOT NULL,
    UNIQUE(project_id, operation_key),
    CHECK(attempt_id IS NULL OR job_id IS NOT NULL),
    CHECK(call_id IS NULL OR attempt_id IS NOT NULL)
) STRICT;
CREATE INDEX attachments_by_job ON attachments(job_id);
CREATE INDEX attachments_staged ON attachments(state, created_at) WHERE state='staged';
"#;

/// One stored attachment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attachment {
    pub id: String,
    pub project_id: String,
    pub job_id: Option<String>,
    pub attempt_id: Option<String>,
    pub call_id: Option<String>,
    pub original_name: String,
    pub extension: String,
    pub media_type: Option<String>,
    pub size: u64,
    pub sha256: String,
    pub location: PathBuf,
    pub created_at: i64,
}

/// The execution an attachment was produced for.
#[derive(Debug, Clone, Copy)]
pub struct Producer<'a> {
    pub job_id: &'a str,
    pub attempt_id: Option<&'a str>,
    pub call_id: Option<&'a str>,
}

/// One attachment to publish.
#[derive(Debug, Clone)]
pub struct NewAttachment<'a> {
    pub project_id: &'a str,
    /// Identifies this publication within the Project.
    pub operation_key: &'a str,
    pub original_name: &'a str,
    pub media_type: Option<&'a str>,
    pub bytes: &'a [u8],
    pub producer: Option<Producer<'a>>,
}

/// The directory new attachments are stored under: the committed
/// `storage.attachments.path`, resolved against the database's directory when
/// relative.
pub fn root() -> Result<PathBuf> {
    let store = crate::core_store::CoreStore::locate()?;
    let document = store.document(crate::core_store::CORE_SCOPE)?;
    let configured = document
        .section("storage")
        .and_then(|storage| storage.get("attachments"))
        .and_then(|attachments| attachments.get("path"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or(DEFAULT_ROOT);
    resolve_root(configured)
}

fn resolve_root(configured: &str) -> Result<PathBuf> {
    validate_root(configured)?;
    let path = Path::new(configured);
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(crate::database::installation_dir()?.join(path))
    }
}

/// Reject a configured root that cannot name a directory.
pub fn validate_root(configured: &str) -> Result<()> {
    if configured.trim().is_empty() || configured.contains('\0') {
        return Err(OcgError::config(format!(
            "{ROOT_KEY} must name a directory"
        )));
    }
    Ok(())
}

/// The storage directory for an original file name: its extension,
/// lower-cased and reduced to ASCII letters and digits, or [`NO_EXTENSION`].
pub fn normalized_extension(original_name: &str) -> String {
    let extension: String = Path::new(original_name)
        .extension()
        .map(|extension| extension.to_string_lossy().to_ascii_lowercase())
        .unwrap_or_default()
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .take(16)
        .collect();
    if extension.is_empty() {
        NO_EXTENSION.to_string()
    } else {
        extension
    }
}

/// Store one attachment, or return the one this publication already stored.
pub fn publish(new: NewAttachment<'_>) -> Result<Attachment> {
    if new.bytes.len() > MAX_ATTACHMENT_BYTES {
        return Err(OcgError::config("attachment exceeds the 64 MiB limit"));
    }
    if new.operation_key.is_empty() || new.operation_key.len() > 256 {
        return Err(OcgError::config("invalid attachment operation key"));
    }
    let original_name: String = new
        .original_name
        .chars()
        .filter(|character| !character.is_control())
        .take(255)
        .collect();
    let sha256 = format!("{:x}", Sha256::digest(new.bytes));
    let mut connection = crate::database::connect()?;
    expire_staged(&mut connection)?;
    let existing = by_operation(&connection, new.project_id, new.operation_key)?;
    let (id, location) = match existing {
        Some((attachment, state)) => {
            if attachment.sha256 != sha256 {
                return Err(OcgError::config(
                    "attachment operation was already used for different content",
                ));
            }
            if state == "available" {
                return verified(attachment);
            }
            // A staged publication of the same operation: finish it.
            (attachment.id, attachment.location)
        }
        None => {
            let root = root()?;
            guard_disk(&root, new.bytes.len())?;
            let extension = normalized_extension(&original_name);
            let id = format!("att-{}", uuid::Uuid::now_v7());
            let file_name = if extension == NO_EXTENSION {
                id.clone()
            } else {
                format!("{id}.{extension}")
            };
            let location = root.join(&extension).join(file_name);
            let producer = new.producer;
            connection
                .execute(
                    "INSERT INTO attachments(id,project_id,job_id,attempt_id,call_id,operation_key,original_name,extension,media_type,size,sha256,location,state,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,'staged',?13) ON CONFLICT(project_id,operation_key) DO NOTHING",
                    params![
                        id,
                        new.project_id,
                        producer.map(|producer| producer.job_id),
                        producer.and_then(|producer| producer.attempt_id),
                        producer.and_then(|producer| producer.call_id),
                        new.operation_key,
                        original_name,
                        extension,
                        new.media_type,
                        new.bytes.len() as i64,
                        sha256,
                        location.to_string_lossy(),
                        now()
                    ],
                )
                .map_err(sql)?;
            // A concurrent publisher of the same operation may have staged it
            // first; whichever row exists is the one completed.
            match by_operation(&connection, new.project_id, new.operation_key)? {
                Some((attachment, state)) if attachment.sha256 == sha256 => {
                    if state == "available" {
                        return verified(attachment);
                    }
                    (attachment.id, attachment.location)
                }
                _ => {
                    return Err(OcgError::config(
                        "attachment operation was already used for different content",
                    ))
                }
            }
        }
    };
    write_file(&location, &id, new.bytes)?;
    connection
        .execute(
            "UPDATE attachments SET state='available' WHERE id=?1 AND state='staged'",
            [&id],
        )
        .map_err(sql)?;
    let attachment = by_id(&connection, &id)?
        .ok_or_else(|| OcgError::config("published attachment disappeared"))?;
    verified(attachment)
}

/// One available attachment of a Project.
pub fn get(project_id: &str, id: &str) -> Result<Attachment> {
    let connection = crate::database::connect()?;
    let attachment = by_id(&connection, id)?
        .filter(|attachment| attachment.project_id == project_id)
        .ok_or_else(|| OcgError::config("unknown attachment"))?;
    let state: String = connection
        .query_row("SELECT state FROM attachments WHERE id=?1", [id], |row| {
            row.get(0)
        })
        .map_err(sql)?;
    if state != "available" {
        return Err(OcgError::config("attachment is not available"));
    }
    Ok(attachment)
}

/// One available attachment and its bytes, verified against the recorded
/// size and checksum. A missing or altered file is an error, never a
/// substitute.
pub fn read(project_id: &str, id: &str) -> Result<(Attachment, Vec<u8>)> {
    let attachment = get(project_id, id)?;
    let bytes = std::fs::read(&attachment.location).map_err(|error| {
        OcgError::io(
            format!(
                "attachment {} is missing from {}",
                attachment.id,
                attachment.location.display()
            ),
            error,
        )
    })?;
    if bytes.len() as u64 != attachment.size
        || format!("{:x}", Sha256::digest(&bytes)) != attachment.sha256
    {
        return Err(OcgError::config(format!(
            "attachment {} at {} does not match its recorded content",
            attachment.id,
            attachment.location.display()
        )));
    }
    Ok((attachment, bytes))
}

/// Make `job_id` the owner of every listed attachment that has none yet: the
/// first Job whose input references an upload owns it.
pub fn adopt(project_id: &str, ids: &[String], job_id: &str) -> Result<()> {
    let mut connection = crate::database::connect()?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sql)?;
    for id in ids {
        transaction
            .execute(
                "UPDATE attachments SET job_id=?3 WHERE id=?1 AND project_id=?2 AND job_id IS NULL AND state='available'",
                params![id, project_id, job_id],
            )
            .map_err(sql)?;
    }
    transaction.commit().map_err(sql)
}

/// Remove publications abandoned before they became available: their
/// temporary and final files, then their rows. Returns how many there were.
pub fn expire_staged_now() -> Result<usize> {
    let mut connection = crate::database::connect()?;
    expire_staged(&mut connection)
}

fn expire_staged(connection: &mut Connection) -> Result<usize> {
    let expired: Vec<(String, String)> = {
        let mut statement = connection
            .prepare(
                "SELECT id,location FROM attachments WHERE state='staged' AND created_at<?1 ORDER BY created_at LIMIT 64",
            )
            .map_err(sql)?;
        let rows = statement
            .query_map([now() - STAGED_EXPIRY_SECONDS], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .map_err(sql)?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql)?
    };
    for (id, location) in &expired {
        let location = Path::new(location);
        remove_if_present(&temporary_path(location, id))?;
        remove_if_present(location)?;
        connection
            .execute(
                "DELETE FROM attachments WHERE id=?1 AND state='staged'",
                [id],
            )
            .map_err(sql)?;
    }
    Ok(expired.len())
}

fn remove_if_present(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(OcgError::io(
            format!("remove abandoned attachment {}", path.display()),
            error,
        )),
    }
}

/// The temporary name a publication writes before renaming into place.
fn temporary_path(location: &Path, id: &str) -> PathBuf {
    location.with_file_name(format!(".{id}.partial"))
}

/// Write the bytes completely under a temporary name, then rename them into
/// place, so `location` only ever holds complete content.
fn write_file(location: &Path, id: &str, bytes: &[u8]) -> Result<()> {
    let directory = location
        .parent()
        .ok_or_else(|| OcgError::config("attachment location has no directory"))?;
    std::fs::create_dir_all(directory)
        .map_err(|error| OcgError::io("create attachment directory", error))?;
    let temporary = temporary_path(location, id);
    let result = (|| -> Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temporary)
            .map_err(|error| OcgError::io("create attachment", error))?;
        file.write_all(bytes)
            .map_err(|error| OcgError::io("write attachment", error))?;
        file.sync_all()
            .map_err(|error| OcgError::io("sync attachment", error))?;
        std::fs::rename(&temporary, location)
            .map_err(|error| OcgError::io("publish attachment", error))?;
        #[cfg(unix)]
        std::fs::File::open(directory)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| OcgError::io("sync attachment directory", error))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

/// Refuse a write the disk cannot safely take, under the committed storage
/// guard thresholds.
fn guard_disk(root: &Path, bytes: usize) -> Result<()> {
    let measured = root
        .ancestors()
        .find(|ancestor| ancestor.exists())
        .unwrap_or(root);
    let store = crate::core_store::CoreStore::locate()?;
    let (global, _) = crate::orchestration::canonical_control::global_configuration(&store)?;
    let guard = crate::orchestration::disk_guard::DiskGuard::for_root(
        measured,
        global.storage_guard.unwrap_or_default(),
    )?;
    if let Some(deferral) = guard.check_amplifying_expansion().deferral() {
        return Err(OcgError::storage_full(
            "attachment storage",
            format!("{} ({bytes} bytes not stored)", deferral.message()),
        ));
    }
    Ok(())
}

/// The attachment, provided its file is where the row says.
fn verified(attachment: Attachment) -> Result<Attachment> {
    let metadata = std::fs::metadata(&attachment.location).map_err(|error| {
        OcgError::io(
            format!(
                "attachment {} is missing from {}",
                attachment.id,
                attachment.location.display()
            ),
            error,
        )
    })?;
    if metadata.len() != attachment.size {
        return Err(OcgError::config(format!(
            "attachment {} at {} does not match its recorded size",
            attachment.id,
            attachment.location.display()
        )));
    }
    Ok(attachment)
}

const COLUMNS: &str = "id,project_id,job_id,attempt_id,call_id,original_name,extension,media_type,size,sha256,location,created_at,state";

fn from_row(row: &Row<'_>) -> rusqlite::Result<(Attachment, String)> {
    Ok((
        Attachment {
            id: row.get(0)?,
            project_id: row.get(1)?,
            job_id: row.get(2)?,
            attempt_id: row.get(3)?,
            call_id: row.get(4)?,
            original_name: row.get(5)?,
            extension: row.get(6)?,
            media_type: row.get(7)?,
            size: row.get::<_, i64>(8)?.max(0) as u64,
            sha256: row.get(9)?,
            location: PathBuf::from(row.get::<_, String>(10)?),
            created_at: row.get(11)?,
        },
        row.get(12)?,
    ))
}

fn by_id(connection: &Connection, id: &str) -> Result<Option<Attachment>> {
    connection
        .query_row(
            &format!("SELECT {COLUMNS} FROM attachments WHERE id=?1"),
            [id],
            from_row,
        )
        .optional()
        .map(|found| found.map(|(attachment, _)| attachment))
        .map_err(sql)
}

fn by_operation(
    connection: &Connection,
    project_id: &str,
    operation_key: &str,
) -> Result<Option<(Attachment, String)>> {
    connection
        .query_row(
            &format!("SELECT {COLUMNS} FROM attachments WHERE project_id=?1 AND operation_key=?2"),
            params![project_id, operation_key],
            from_row,
        )
        .optional()
        .map_err(sql)
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or_default()
}

fn sql(error: rusqlite::Error) -> OcgError {
    crate::database::sql(&crate::database::path().unwrap_or_default(), error)
}
