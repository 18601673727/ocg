//! The one OCG database.
//!
//! An installation is an executable and the `ocg.db` beside it. Every piece of
//! persistent OCG state lives in that database: configuration, credentials,
//! the Project registry, every Project's Jobs and their execution history, the
//! event journal, indexes and caches. Projects are isolated logically, by
//! their Project identity, not by separate files, and OCG never writes its own
//! state into a Project.
//!
//! # Location
//!
//! The database is `ocg.db` in the physical directory of the running
//! executable: [`std::env::current_exe`] with every symbolic link resolved.
//! A symlink to the executable therefore uses the database beside the real
//! file, and the working directory never matters. There is no other storage
//! root and no way to configure one.
//!
//! # Installation identity
//!
//! The database records the directory it was initialized for. Replacing the
//! executable in place keeps that directory and therefore the installation.
//! Opening a database whose recorded directory is a different one means the
//! executable and its database were moved or copied together; that is a new
//! installation, so the database is reinitialized from empty inside one
//! exclusive transaction rather than adopted. Only a database positively
//! identified as OCG's (by its SQLite `application_id`) is ever reset; any
//! other file at that path is refused and left untouched.
//!
//! # Journaling
//!
//! The database runs in WAL mode so readers never block the writer and
//! independent Projects make progress concurrently. While connections are
//! open SQLite keeps `ocg.db-wal` and `ocg.db-shm` beside it; they are
//! transient: the last connection to close checkpoints the WAL into `ocg.db`
//! and removes both. After a crash the WAL holds committed transactions not
//! yet checkpointed and is what recovery replays, so OCG never deletes it
//! itself. `secure_delete` is on: deleted content, credentials included, is
//! overwritten rather than left in free pages.

use crate::error::{OcgError, Result};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// The database's file name inside the installation directory.
pub const DATABASE_FILE: &str = "ocg.db";

/// SQLite `application_id` marking a database as OCG's ("OCG1").
const APPLICATION_ID: i32 = 0x4F43_4731;

/// The schema generation, recorded in `user_version`. A database is created at
/// this generation; one written by any other generation is refused, never
/// migrated.
const SCHEMA_VERSION: i64 = 1;

/// The installation-level tables. Every other component contributes its own
/// tables through [`schema`].
const INSTALLATION_SCHEMA: &str = r#"
-- The directory this database was initialized for.
CREATE TABLE ocg_installation (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    installation_id TEXT NOT NULL,
    location TEXT NOT NULL,
    created_at INTEGER NOT NULL
) STRICT;
"#;

static LOCATION: OnceLock<std::result::Result<PathBuf, String>> = OnceLock::new();

/// The installation directory: the physical directory of this executable.
pub fn installation_dir() -> Result<PathBuf> {
    LOCATION
        .get_or_init(|| {
            let executable = std::env::current_exe()
                .map_err(|error| format!("cannot locate the OCG executable: {error}"))?;
            let executable = executable.canonicalize().map_err(|error| {
                format!(
                    "cannot resolve the OCG executable {}: {error}",
                    executable.display()
                )
            })?;
            executable.parent().map(Path::to_path_buf).ok_or_else(|| {
                format!(
                    "the OCG executable {} has no parent directory",
                    executable.display()
                )
            })
        })
        .clone()
        .map_err(OcgError::config)
}

/// The path of this installation's database.
pub fn path() -> Result<PathBuf> {
    Ok(installation_dir()?.join(DATABASE_FILE))
}

/// Open a connection to this installation's database, creating and
/// initializing it on first use.
///
/// Every caller opens its own connection, so threads and processes share the
/// database through SQLite's locking alone.
pub fn connect() -> Result<Connection> {
    let path = path()?;
    let location = installation_dir()?;
    let existed = path.exists();
    let mut connection =
        Connection::open(&path).map_err(|error| open_sql(&location, &path, error))?;
    configure(&path, &connection)?;
    if !existed {
        restrict_permissions(&path)?;
    }
    let current: Option<String> = if generation(&connection, &path)? == SCHEMA_VERSION {
        connection
            .query_row(
                "SELECT location FROM ocg_installation WHERE id=1",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| sql(&path, error))?
    } else {
        None
    };
    if current.as_deref() != Some(location.to_string_lossy().as_ref()) {
        initialize(&path, &location, &mut connection)?;
    }
    Ok(connection)
}

fn configure(path: &Path, connection: &Connection) -> Result<()> {
    connection
        .busy_timeout(std::time::Duration::from_secs(5))
        .map_err(|error| sql(path, error))?;
    for (pragma, value) in [
        ("foreign_keys", "ON"),
        ("secure_delete", "ON"),
        ("synchronous", "FULL"),
    ] {
        connection
            .pragma_update(None, pragma, value)
            .map_err(|error| sql(path, error))?;
    }
    Ok(())
}

fn generation(connection: &Connection, path: &Path) -> Result<i64> {
    connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|error| sql(path, error))
}

/// Bring the database to a current, identified installation for `location`.
///
/// Runs under an exclusive transaction and decides again inside it, so of
/// several processes opening the same database at once exactly one
/// initializes it and the others find it current.
fn initialize(path: &Path, location: &Path, connection: &mut Connection) -> Result<()> {
    // WAL must be selected outside a transaction. It is persistent, so a
    // database that is already in WAL mode is unaffected.
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .map_err(|error| sql(path, error))?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Exclusive)
        .map_err(|error| sql(path, error))?;
    let application_id: i32 = transaction
        .query_row("PRAGMA application_id", [], |row| row.get(0))
        .map_err(|error| sql(path, error))?;
    // Virtual tables first: dropping one drops its shadow tables with it.
    let objects: Vec<(String, String)> = {
        let mut statement = transaction
            .prepare(
                "SELECT type,name FROM sqlite_master WHERE type IN ('table','view') AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' ORDER BY type='view' DESC, sql LIKE 'CREATE VIRTUAL TABLE%' DESC, name",
            )
            .map_err(|error| sql(path, error))?;
        let objects = statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(|error| sql(path, error))?
            .collect::<rusqlite::Result<Vec<(String, String)>>>()
            .map_err(|error| sql(path, error))?;
        objects
    };
    let tables: Vec<&str> = objects
        .iter()
        .filter(|(kind, _)| kind == "table")
        .map(|(_, name)| name.as_str())
        .collect();
    let generation: i64 = transaction
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|error| sql(path, error))?;
    let location_text = location.to_string_lossy().into_owned();
    if application_id == APPLICATION_ID && generation == SCHEMA_VERSION {
        let recorded: Option<String> = transaction
            .query_row(
                "SELECT location FROM ocg_installation WHERE id=1",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| sql(path, error))?;
        if recorded.as_deref() == Some(location_text.as_str()) {
            // Another process initialized it while this one waited.
            return Ok(());
        }
    }
    if application_id != APPLICATION_ID && !tables.is_empty() {
        return Err(OcgError::config(format!(
            "{} is not an OCG database; OCG will not modify it. Move it away to let OCG create its database here.",
            path.display()
        )));
    }
    if application_id == APPLICATION_ID && generation != SCHEMA_VERSION && generation != 0 {
        let recorded: Option<String> = if tables.contains(&"ocg_installation") {
            transaction
                .query_row(
                    "SELECT location FROM ocg_installation WHERE id=1",
                    [],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|error| sql(path, error))?
        } else {
            None
        };
        // A database written by another OCG schema generation is only
        // reinitialized when it belongs to another installation. The same
        // installation is refused so a binary replaced in place can never
        // silently discard its own state.
        if recorded.as_deref() == Some(location_text.as_str()) {
            return Err(OcgError::config(format!(
                "{} was written by OCG schema generation {generation}, not {SCHEMA_VERSION}; this executable cannot use it",
                path.display()
            )));
        }
    }
    // A positively identified OCG database of another installation, or an
    // empty file: start this installation from nothing.
    transaction
        .execute_batch("PRAGMA defer_foreign_keys = ON;")
        .map_err(|error| sql(path, error))?;
    for (kind, name) in &objects {
        let kind = if kind == "view" { "VIEW" } else { "TABLE" };
        transaction
            .execute_batch(&format!(
                "DROP {kind} IF EXISTS \"{}\";",
                name.replace('"', "\"\"")
            ))
            .map_err(|error| sql(path, error))?;
    }
    transaction
        .execute_batch(INSTALLATION_SCHEMA)
        .map_err(|error| sql(path, error))?;
    for (component, statements) in schema() {
        transaction.execute_batch(statements).map_err(|error| {
            OcgError::config(format!(
                "cannot create the {component} schema in {}: {error}",
                path.display()
            ))
        })?;
    }
    crate::orchestration::journal::initialize_boundary(&transaction)?;
    transaction
        .execute(
            "INSERT INTO ocg_installation(id,installation_id,location,created_at) VALUES(1,?1,?2,?3)",
            params![
                format!("inst-{}", uuid::Uuid::now_v7()),
                location_text,
                now()
            ],
        )
        .map_err(|error| sql(path, error))?;
    transaction
        .pragma_update(None, "application_id", APPLICATION_ID)
        .map_err(|error| sql(path, error))?;
    transaction
        .pragma_update(None, "user_version", SCHEMA_VERSION)
        .map_err(|error| sql(path, error))?;
    transaction.commit().map_err(|error| sql(path, error))?;
    Ok(())
}

/// Every component's tables, created together in one transaction.
fn schema() -> [(&'static str, &'static str); 6] {
    [
        ("Core", crate::core_store::SCHEMA),
        ("domain", crate::orchestration::domain::SCHEMA),
        ("journal", crate::orchestration::journal::SCHEMA),
        ("security", crate::control_security::SCHEMA),
        ("Project document", crate::project_store::SCHEMA),
        ("context index", crate::context::fulltext::SCHEMA),
    ]
}

/// This installation's identity.
pub fn installation() -> Result<(String, PathBuf)> {
    let path = path()?;
    let connection = connect()?;
    connection
        .query_row(
            "SELECT installation_id,location FROM ocg_installation WHERE id=1",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    PathBuf::from(row.get::<_, String>(1)?),
                ))
            },
        )
        .map_err(|error| sql(&path, error))
}

/// Classify an open-time SQLite failure. An exhausted filesystem usually fails
/// at open — WAL/shared-memory files cannot be created — with `CANTOPEN`
/// rather than `FULL`. A `CANTOPEN` is therefore confirmed with a writability
/// probe in the database directory before reclassifying: only an actual
/// write rejection (`ENOSPC`, read-only mount) becomes storage safety, so
/// permission or path errors can never masquerade as it.
fn open_sql(directory: &Path, path: &Path, error: rusqlite::Error) -> OcgError {
    let cant_open = matches!(
        &error,
        rusqlite::Error::SqliteFailure(inner, _)
            if inner.code == rusqlite::ErrorCode::CannotOpen
    );
    let mapped = sql(path, error);
    if mapped.is_storage_full() || !cant_open {
        return mapped;
    }
    match probe_writable(directory) {
        ProbeOutcome::Unwritable(reason) => OcgError::storage_full(
            "OCG database",
            format!("{mapped}; host storage rejects writes ({reason})"),
        ),
        ProbeOutcome::Writable => mapped,
    }
}

enum ProbeOutcome {
    Writable,
    Unwritable(&'static str),
}

/// Try to create, write and remove one tiny file beside the database. Only a
/// write rejection proves exhaustion; anything else (including a probe that
/// cannot even be attempted cleanly) leaves the original error untouched.
fn probe_writable(directory: &Path) -> ProbeOutcome {
    use std::io::Write;
    for attempt in 0..4 {
        let path = directory.join(format!(
            ".ocg-open-probe-{}-{attempt}.tmp",
            std::process::id()
        ));
        let created = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path);
        let mut file = match created {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return classify_probe_error(&error),
        };
        let outcome = match file.write_all(&[0]) {
            Ok(()) => ProbeOutcome::Writable,
            Err(error) => classify_probe_error(&error),
        };
        drop(file);
        let _ = std::fs::remove_file(&path);
        return outcome;
    }
    ProbeOutcome::Writable
}

fn classify_probe_error(error: &std::io::Error) -> ProbeOutcome {
    if error.raw_os_error() == Some(libc::ENOSPC) {
        return ProbeOutcome::Unwritable("no space left on device");
    }
    // `libc` exposes `EROFS` on all supported targets; a read-only mount
    // cannot take durable state either.
    if error.raw_os_error() == Some(libc::EROFS) {
        return ProbeOutcome::Unwritable("filesystem is read-only");
    }
    ProbeOutcome::Writable
}

#[cfg(unix)]
fn restrict_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .map_err(|error| OcgError::io("restrict OCG database permissions", error))
}

#[cfg(not(unix))]
fn restrict_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or_default()
}

pub(crate) fn sql(path: &Path, error: rusqlite::Error) -> OcgError {
    if let rusqlite::Error::SqliteFailure(failure, _) = &error {
        if failure.code == rusqlite::ErrorCode::DiskFull {
            return OcgError::storage_full("OCG database", error.to_string());
        }
    }
    OcgError::config(format!("OCG database {}: {error}", path.display()))
}
