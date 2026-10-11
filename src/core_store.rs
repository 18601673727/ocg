//! Configuration, credentials and the Project registry of an installation,
//! stored in the OCG database.
//!
//! # Configuration
//!
//! Configuration is a set of named JSON sections per scope. The `core` scope
//! holds installation-wide configuration (the Profile, routing, prompts,
//! policies, control settings); a Project scope, named by its Project id,
//! holds that Project's settings. A section is only ever changed through
//! [`CoreStore::commit`]: the candidate is validated against the complete
//! resulting scope inside the same immediate transaction that writes it, so a
//! rejected candidate never becomes visible, and a commit records one new
//! installation-wide revision. A writer names the scope revision it computed
//! its change from; any other current revision is a conflict, so concurrent
//! writers cannot overwrite each other silently.
//!
//! Durable execution never reads configuration back: an Attempt carries the
//! frozen execution input and Call requests it was admitted with, so a later
//! revision can only affect work admitted after it commits.
//!
//! # Credentials
//!
//! Provider credentials are stored as plain values in `core_credentials`. The
//! database file is therefore as sensitive as a credentials file: it is
//! created owner-only and `secure_delete` overwrites removed values. A
//! credential value is read only to authorize a provider request; it is never
//! part of configuration, events, snapshots, indexes or logs, and the control
//! API only ever reports credential names.

use crate::error::{OcgError, Result};
use crate::orchestration::canonical_control::ProjectRecord;
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// The installation-wide configuration scope.
pub const CORE_SCOPE: &str = "core";

/// The tables this component contributes to the OCG database.
pub(crate) const SCHEMA: &str = r#"
-- One row per committed configuration change. `revision` is installation-wide
-- and strictly increasing, so the largest one is the active revision and a
-- reader can ask what changed after any revision it has seen.
CREATE TABLE core_config_revisions (
    revision INTEGER PRIMARY KEY CHECK(revision > 0),
    scope TEXT NOT NULL,
    -- The revision of `scope` this change was computed from.
    base_revision INTEGER NOT NULL CHECK(base_revision >= 0 AND base_revision < revision),
    command_id TEXT NOT NULL,
    principal TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    -- The sections this change wrote or removed, as a JSON array.
    sections TEXT NOT NULL,
    -- The digest of the whole scope as committed by this change.
    scope_digest TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    UNIQUE(scope, command_id)
) STRICT;
CREATE INDEX core_config_revisions_by_scope ON core_config_revisions(scope, revision);
-- The committed value of every section, tagged with the revision that wrote it.
CREATE TABLE core_config_sections (
    scope TEXT NOT NULL,
    section TEXT NOT NULL,
    value TEXT NOT NULL CHECK(json_valid(value)),
    revision INTEGER NOT NULL REFERENCES core_config_revisions(revision),
    PRIMARY KEY(scope, section)
) STRICT;
-- Which process owns each Project's execution and recovery, and the fencing
-- generation it acquired at. A released row keeps its generation, so a later
-- owner always acquires a higher one.
CREATE TABLE core_project_owners (
    project_id TEXT PRIMARY KEY REFERENCES domain_projects(id),
    owner TEXT,
    pid INTEGER,
    generation INTEGER NOT NULL CHECK(generation > 0),
    acquired_at INTEGER NOT NULL,
    CHECK((owner IS NULL) = (pid IS NULL))
) STRICT;
-- Provider credentials by the name a Profile provider references.
CREATE TABLE core_credentials (
    name TEXT PRIMARY KEY,
    value TEXT NOT NULL CHECK(length(value) > 0),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
) STRICT;
"#;

/// One scope's committed configuration.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ConfigDocument {
    /// The revision that last changed this scope; `0` when nothing has.
    pub revision: u64,
    pub sections: Map<String, Value>,
}

impl ConfigDocument {
    pub fn section(&self, name: &str) -> Option<&Value> {
        self.sections.get(name)
    }

    /// The scope as one JSON object keyed by section.
    pub fn to_value(&self) -> Value {
        Value::Object(self.sections.clone())
    }
}

/// A configuration change to one scope.
#[derive(Debug, Clone)]
pub struct ConfigChange<'a> {
    pub scope: &'a str,
    /// Identifies this change. Replaying it returns the revision it already
    /// committed; reusing it for different content is refused.
    pub command_id: &'a str,
    pub principal: &'a str,
    /// The scope revision this change was computed from.
    pub expected_revision: u64,
    /// The sections to write, or to remove when `None`.
    pub sections: Vec<(&'a str, Option<Value>)>,
}

/// The outcome of [`CoreStore::commit`].
#[derive(Debug, Clone, PartialEq)]
pub struct ConfigCommit {
    pub revision: u64,
    /// True when this command was already committed; nothing was written.
    pub duplicate: bool,
    /// The scope as it stands after this call.
    pub document: ConfigDocument,
}

/// One committed configuration revision, as observed by a reader.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigRevision {
    pub revision: u64,
    pub scope: String,
    pub base_revision: u64,
    pub command_id: String,
    pub principal: String,
    pub sections: Vec<String>,
    pub scope_digest: String,
    pub created_at: i64,
}

/// The configuration, credentials and Project registry of this installation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CoreStore;

impl CoreStore {
    /// This installation's store.
    pub fn locate() -> Result<Self> {
        crate::database::path()?;
        Ok(Self)
    }

    /// The OCG database the store lives in.
    pub fn path(&self) -> PathBuf {
        crate::database::path().unwrap_or_default()
    }

    fn connect(&self) -> Result<Connection> {
        crate::database::connect()
    }

    /// The installation-wide active configuration revision; `0` before the
    /// first commit. Cheap enough to poll at every safe boundary.
    pub fn active_revision(&self) -> Result<u64> {
        let connection = self.connect()?;
        let revision: i64 = connection
            .query_row(
                "SELECT COALESCE(MAX(revision),0) FROM core_config_revisions",
                [],
                |row| row.get(0),
            )
            .map_err(|error| sql(&self.path(), error))?;
        Ok(revision as u64)
    }

    /// The committed configuration of one scope.
    pub fn document(&self, scope: &str) -> Result<ConfigDocument> {
        let mut connection = self.connect()?;
        let transaction = connection
            .transaction()
            .map_err(|error| sql(&self.path(), error))?;
        read_document(&transaction, scope).map_err(|error| sql(&self.path(), error))
    }

    /// Commit one validated change atomically.
    ///
    /// `validate` sees the complete scope as it would stand after the change
    /// and runs inside the commit transaction; an error leaves nothing
    /// written. The change commits only if the scope is still at
    /// `expected_revision`.
    pub fn commit(
        &self,
        change: ConfigChange<'_>,
        validate: impl FnOnce(&ConfigDocument) -> Result<()>,
    ) -> Result<ConfigCommit> {
        self.commit_with(change, validate, |_| Ok(()))
    }

    /// [`CoreStore::commit`] with one more write that must commit, or fail,
    /// together with the configuration change. Runs after validation, inside
    /// the same transaction, and is skipped for a replayed command.
    pub fn commit_with(
        &self,
        change: ConfigChange<'_>,
        validate: impl FnOnce(&ConfigDocument) -> Result<()>,
        also: impl FnOnce(&Transaction<'_>) -> Result<()>,
    ) -> Result<ConfigCommit> {
        validate_name(change.scope, "configuration scope")?;
        validate_name(change.command_id, "command_id")?;
        validate_name(change.principal, "principal")?;
        if change.sections.is_empty() {
            return Err(OcgError::config(
                "a configuration change must name a section",
            ));
        }
        let mut names = Vec::with_capacity(change.sections.len());
        for (name, _) in &change.sections {
            validate_name(name, "configuration section")?;
            if names.contains(name) {
                return Err(OcgError::config(format!(
                    "configuration section '{name}' is named twice"
                )));
            }
            names.push(*name);
        }
        let digest = request_digest(&change);
        let path = self.path();
        let mut connection = self.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| sql(&path, error))?;
        let recorded: Option<(String, i64)> = transaction
            .query_row(
                "SELECT request_digest,revision FROM core_config_revisions WHERE scope=?1 AND command_id=?2",
                params![change.scope, change.command_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| sql(&path, error))?;
        if let Some((recorded_digest, revision)) = recorded {
            if recorded_digest != digest {
                return Err(OcgError::config(
                    "command_id already used with different configuration content",
                ));
            }
            let document =
                read_document(&transaction, change.scope).map_err(|error| sql(&path, error))?;
            return Ok(ConfigCommit {
                revision: revision as u64,
                duplicate: true,
                document,
            });
        }
        let current =
            read_document(&transaction, change.scope).map_err(|error| sql(&path, error))?;
        if current.revision != change.expected_revision {
            return Err(OcgError::ConfigurationConflict(format!(
                "configuration conflict: '{}' is at revision {}, not {}; re-read and retry",
                change.scope, current.revision, change.expected_revision
            )));
        }
        let mut candidate = current.clone();
        for (name, value) in &change.sections {
            match value {
                Some(value) => {
                    candidate
                        .sections
                        .insert((*name).to_string(), value.clone());
                }
                None => {
                    candidate.sections.remove(*name);
                }
            }
        }
        validate(&candidate)?;
        also(&transaction)?;
        let active: i64 = transaction
            .query_row(
                "SELECT COALESCE(MAX(revision),0) FROM core_config_revisions",
                [],
                |row| row.get(0),
            )
            .map_err(|error| sql(&path, error))?;
        let revision = active + 1;
        let scope_digest = format!(
            "{:x}",
            Sha256::digest(candidate.to_value().to_string().as_bytes())
        );
        transaction
            .execute(
                "INSERT INTO core_config_revisions(revision,scope,base_revision,command_id,principal,request_digest,sections,scope_digest,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                params![
                    revision,
                    change.scope,
                    change.expected_revision as i64,
                    change.command_id,
                    change.principal,
                    digest,
                    Value::from(names.clone()).to_string(),
                    scope_digest,
                    now()
                ],
            )
            .map_err(|error| sql(&path, error))?;
        for (name, value) in &change.sections {
            match value {
                Some(value) => transaction.execute(
                    "INSERT INTO core_config_sections(scope,section,value,revision) VALUES(?1,?2,?3,?4) ON CONFLICT(scope,section) DO UPDATE SET value=excluded.value,revision=excluded.revision",
                    params![change.scope, name, value.to_string(), revision],
                ),
                None => transaction.execute(
                    "DELETE FROM core_config_sections WHERE scope=?1 AND section=?2",
                    params![change.scope, name],
                ),
            }
            .map_err(|error| sql(&path, error))?;
        }
        transaction.commit().map_err(|error| sql(&path, error))?;
        candidate.revision = revision as u64;
        Ok(ConfigCommit {
            revision: revision as u64,
            duplicate: false,
            document: candidate,
        })
    }

    /// The revisions committed after `after`, oldest first, at most `limit`.
    pub fn revisions_since(&self, after: u64, limit: usize) -> Result<Vec<ConfigRevision>> {
        let connection = self.connect()?;
        let mut statement = connection
            .prepare(
                "SELECT revision,scope,base_revision,command_id,principal,sections,scope_digest,created_at FROM core_config_revisions WHERE revision>?1 ORDER BY revision LIMIT ?2",
            )
            .map_err(|error| sql(&self.path(), error))?;
        let rows = statement
            .query_map(params![after as i64, limit as i64], |row| {
                let sections: String = row.get(5)?;
                Ok(ConfigRevision {
                    revision: row.get::<_, i64>(0)? as u64,
                    scope: row.get(1)?,
                    base_revision: row.get::<_, i64>(2)? as u64,
                    command_id: row.get(3)?,
                    principal: row.get(4)?,
                    sections: serde_json::from_str(&sections).unwrap_or_default(),
                    scope_digest: row.get(6)?,
                    created_at: row.get(7)?,
                })
            })
            .map_err(|error| sql(&self.path(), error))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|error| sql(&self.path(), error))
    }

    /// Every registered Project, oldest first.
    pub fn projects(&self) -> Result<Vec<ProjectRecord>> {
        let connection = self.connect()?;
        let mut statement = connection
            .prepare(
                "SELECT id,root,created_at,updated_at FROM domain_projects ORDER BY created_at,id",
            )
            .map_err(|error| sql(&self.path(), error))?;
        let rows = statement
            .query_map([], |row| {
                Ok(ProjectRecord {
                    project_id: row.get(0)?,
                    root: row.get(1)?,
                    created_at: row.get(2)?,
                    updated_at: row.get(3)?,
                })
            })
            .map_err(|error| sql(&self.path(), error))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|error| sql(&self.path(), error))
    }

    pub fn project(&self, project_id: &str) -> Result<Option<ProjectRecord>> {
        Ok(self
            .projects()?
            .into_iter()
            .find(|project| project.project_id == project_id))
    }

    /// The registered Project whose root is `root` or its nearest registered
    /// ancestor.
    pub fn project_containing(&self, root: &Path) -> Result<Option<ProjectRecord>> {
        let root = crate::project::canonicalize(root);
        Ok(self
            .projects()?
            .into_iter()
            .filter(|project| root.starts_with(Path::new(&project.root)))
            .max_by_key(|project| project.root.len()))
    }

    /// Store a credential under `name`, replacing any value it had.
    pub fn set_credential(&self, name: &str, value: &str) -> Result<()> {
        let connection = self.connect()?;
        store_credential(&connection, name, value, true)
    }

    /// Remove a credential; whether one existed.
    pub fn remove_credential(&self, name: &str) -> Result<bool> {
        validate_credential_name(name)?;
        let connection = self.connect()?;
        let removed = connection
            .execute("DELETE FROM core_credentials WHERE name=?1", [name])
            .map_err(|error| sql(&self.path(), error))?;
        Ok(removed > 0)
    }

    /// The value of one credential, for authorizing a provider request.
    pub fn credential(&self, name: &str) -> Result<Option<String>> {
        validate_credential_name(name)?;
        let connection = self.connect()?;
        connection
            .query_row(
                "SELECT value FROM core_credentials WHERE name=?1",
                [name],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| sql(&self.path(), error))
    }

    /// The names of the stored credentials. Values never leave the store here.
    pub fn credential_names(&self) -> Result<BTreeSet<String>> {
        let connection = self.connect()?;
        let mut statement = connection
            .prepare("SELECT name FROM core_credentials ORDER BY name")
            .map_err(|error| sql(&self.path(), error))?;
        let names = statement
            .query_map([], |row| row.get(0))
            .map_err(|error| sql(&self.path(), error))?
            .collect::<rusqlite::Result<BTreeSet<String>>>()
            .map_err(|error| sql(&self.path(), error))?;
        Ok(names)
    }
}

/// Store a credential inside an open transaction or connection. A new
/// credential must not replace an existing one unless `replace`.
pub(crate) fn store_credential(
    connection: &Connection,
    name: &str,
    value: &str,
    replace: bool,
) -> Result<()> {
    validate_credential_name(name)?;
    if value.is_empty() {
        return Err(OcgError::config("credential value cannot be empty"));
    }
    let timestamp = now();
    let written = if replace {
        connection.execute(
            "INSERT INTO core_credentials(name,value,created_at,updated_at) VALUES(?1,?2,?3,?3) ON CONFLICT(name) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",
            params![name, value, timestamp],
        )
    } else {
        connection.execute(
            "INSERT INTO core_credentials(name,value,created_at,updated_at) VALUES(?1,?2,?3,?3) ON CONFLICT(name) DO NOTHING",
            params![name, value, timestamp],
        )
    }
    .map_err(|error| OcgError::config(format!("cannot store credential: {error}")))?;
    if written == 0 {
        return Err(OcgError::config(
            "credential name is already in use; reconnect provider",
        ));
    }
    Ok(())
}

/// A credential name is an environment-variable name: `A-Z`, `0-9` and `_`,
/// not starting with a digit. `OCG_` names and the process variables a
/// credential could shadow are reserved.
pub fn validate_credential_name(name: &str) -> Result<()> {
    const RESERVED: [&str; 10] = [
        "PATH",
        "HOME",
        "USER",
        "SHELL",
        "PWD",
        "TMPDIR",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
    ];
    if name.is_empty()
        || RESERVED.contains(&name)
        || name.starts_with("OCG_")
        || name.len() > 128
        || !name
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_uppercase() || byte == b'_')
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(OcgError::config(
            "credential name must be an environment variable name using A-Z, 0-9, and underscore",
        ));
    }
    Ok(())
}

/// One scope's configuration. The revision and the sections are read inside
/// one transaction, so they come from the same snapshot: a commit between the
/// two reads can never pair an old revision with new content.
fn read_document(transaction: &Transaction<'_>, scope: &str) -> rusqlite::Result<ConfigDocument> {
    let revision: i64 = transaction.query_row(
        "SELECT COALESCE(MAX(revision),0) FROM core_config_revisions WHERE scope=?1",
        [scope],
        |row| row.get(0),
    )?;
    #[cfg(test)]
    tests::between_reads();
    let mut statement = transaction.prepare(
        "SELECT section,value FROM core_config_sections WHERE scope=?1 ORDER BY section",
    )?;
    let rows = statement.query_map([scope], |row| {
        let value: String = row.get(1)?;
        let value = serde_json::from_str(&value).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                1,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?;
        Ok((row.get::<_, String>(0)?, value))
    })?;
    Ok(ConfigDocument {
        revision: revision as u64,
        sections: rows.collect::<rusqlite::Result<Map<_, _>>>()?,
    })
}

fn request_digest(change: &ConfigChange<'_>) -> String {
    let sections: Vec<Value> = change
        .sections
        .iter()
        .map(|(name, value)| serde_json::json!([name, value]))
        .collect();
    let content = serde_json::json!({
        "scope": change.scope,
        "principal": change.principal,
        "expected_revision": change.expected_revision,
        "sections": sections,
    });
    format!("{:x}", Sha256::digest(content.to_string().as_bytes()))
}

fn validate_name(value: &str, what: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(OcgError::config(format!("invalid {what}")));
    }
    Ok(())
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or_default()
}

fn sql(path: &Path, error: rusqlite::Error) -> OcgError {
    crate::database::sql(path, error)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn change<'a>(
        scope: &'a str,
        command_id: &'a str,
        expected: u64,
        value: i64,
    ) -> ConfigChange<'a> {
        ConfigChange {
            scope,
            command_id,
            principal: "test",
            expected_revision: expected,
            sections: vec![("value", Some(Value::from(value)))],
        }
    }

    thread_local! {
        static BETWEEN_READS: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
            std::cell::RefCell::new(None);
    }

    /// Run the action a test placed between a document's revision and
    /// section reads, once.
    pub(super) fn between_reads() {
        if let Some(action) = BETWEEN_READS.with(|slot| slot.borrow_mut().take()) {
            action();
        }
    }

    #[test]
    fn a_document_never_pairs_a_revision_with_another_revisions_content() {
        let store = CoreStore::locate().expect("store");
        let scope = format!("snapshot-{}", uuid::Uuid::now_v7());
        let first = store
            .commit(change(&scope, "first", 0, 1), |_| Ok(()))
            .expect("first");
        let writer = store.clone();
        let concurrent = scope.clone();
        BETWEEN_READS.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                // Another connection commits after the revision was read.
                writer
                    .commit(change(&concurrent, "second", first.revision, 2), |_| Ok(()))
                    .expect("second");
            }))
        });

        let document = store.document(&scope).expect("document");

        assert!(
            BETWEEN_READS.with(|slot| slot.borrow().is_none()),
            "commit ran"
        );
        assert_eq!(document.revision, first.revision);
        assert_eq!(document.section("value"), Some(&Value::from(1)));
        let current = store.document(&scope).expect("current");
        assert!(current.revision > first.revision);
        assert_eq!(current.section("value"), Some(&Value::from(2)));
    }
}
