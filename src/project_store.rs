//! Project-scoped derived documents in the OCG database.
//!
//! Context caches and indexes, checkpoints, verification logs, telemetry,
//! context observations, chat images and the resource registry are each a
//! set of named documents of one kind, owned by one Project. They live here,
//! keyed by Project identity, rather than in the Project's directory: OCG
//! never writes its own state into a Project.
//!
//! Every document is derived or bounded by its owner. A component that keeps
//! a bounded history prunes its own kind; nothing here grows without a caller
//! deciding it may.

use crate::error::{OcgError, Result};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;

/// The tables this component contributes to the OCG database.
pub(crate) const SCHEMA: &str = r#"
CREATE TABLE project_documents (
    project_id TEXT NOT NULL REFERENCES domain_projects(id),
    kind TEXT NOT NULL,
    key TEXT NOT NULL,
    value BLOB NOT NULL,
    -- Milliseconds since the Unix epoch of the last write.
    updated_at INTEGER NOT NULL,
    PRIMARY KEY(project_id, kind, key)
) STRICT;
"#;

/// One stored document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Document {
    pub key: String,
    pub value: Vec<u8>,
    pub updated_at: i64,
}

/// The documents of one Project.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectDocuments {
    project_id: String,
}

impl ProjectDocuments {
    /// The documents of the Project registered at `root`, registering one
    /// when none is. Used by writers acting for a Project.
    pub fn for_root(root: &Path) -> Result<Self> {
        let project = crate::orchestration::domain::DomainRepository::open(root)?
            .project_at_root(root)?
            .ok_or_else(|| OcgError::config("no Project is registered at this root"))?;
        Ok(Self {
            project_id: project.id,
        })
    }

    /// The documents of the Project registered at `root`, if one is. Readers
    /// use this so that reading never registers a Project.
    pub fn existing(root: &Path) -> Result<Option<Self>> {
        Ok(
            crate::orchestration::domain::DomainRepository::open_existing(root)?
                .project_at_root(root)?
                .map(|project| Self {
                    project_id: project.id,
                }),
        )
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn get(&self, kind: &str, key: &str) -> Result<Option<Vec<u8>>> {
        connect()?
            .query_row(
                "SELECT value FROM project_documents WHERE project_id=?1 AND kind=?2 AND key=?3",
                params![self.project_id, kind, key],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql)
    }

    /// Write one document, replacing any with the same key.
    pub fn put(&self, kind: &str, key: &str, value: &[u8]) -> Result<()> {
        connect()?
            .execute(
                "INSERT INTO project_documents(project_id,kind,key,value,updated_at) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(project_id,kind,key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",
                params![self.project_id, kind, key, value, now_ms()],
            )
            .map_err(sql)?;
        Ok(())
    }

    /// Remove one document; whether it existed.
    pub fn remove(&self, kind: &str, key: &str) -> Result<bool> {
        Ok(connect()?
            .execute(
                "DELETE FROM project_documents WHERE project_id=?1 AND kind=?2 AND key=?3",
                params![self.project_id, kind, key],
            )
            .map_err(sql)?
            > 0)
    }

    /// Every document of `kind`, in key order.
    pub fn list(&self, kind: &str) -> Result<Vec<Document>> {
        let connection = connect()?;
        let mut statement = connection
            .prepare(
                "SELECT key,value,updated_at FROM project_documents WHERE project_id=?1 AND kind=?2 ORDER BY key",
            )
            .map_err(sql)?;
        let rows = statement
            .query_map(params![self.project_id, kind], |row| {
                Ok(Document {
                    key: row.get(0)?,
                    value: row.get(1)?,
                    updated_at: row.get(2)?,
                })
            })
            .map_err(sql)?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql)
    }

    /// The key, size and write time of every document of `kind`, in key
    /// order, without reading the values.
    pub fn sizes(&self, kind: &str) -> Result<Vec<(String, u64, i64)>> {
        let connection = connect()?;
        let mut statement = connection
            .prepare(
                "SELECT key,length(value),updated_at FROM project_documents WHERE project_id=?1 AND kind=?2 ORDER BY key",
            )
            .map_err(sql)?;
        let rows = statement
            .query_map(params![self.project_id, kind], |row| {
                Ok((
                    row.get(0)?,
                    row.get::<_, i64>(1)?.max(0) as u64,
                    row.get(2)?,
                ))
            })
            .map_err(sql)?;
        rows.collect::<rusqlite::Result<Vec<_>>>().map_err(sql)
    }

    /// Keep only the `keep` documents of `kind` with the greatest keys; how
    /// many were removed. Kinds whose keys order chronologically use this to
    /// bound their history.
    pub fn retain_newest(&self, kind: &str, keep: usize) -> Result<usize> {
        connect()?
            .execute(
                "DELETE FROM project_documents WHERE project_id=?1 AND kind=?2 AND key NOT IN (SELECT key FROM project_documents WHERE project_id=?1 AND kind=?2 ORDER BY key DESC LIMIT ?3)",
                params![self.project_id, kind, keep as i64],
            )
            .map_err(sql)
    }

    /// Remove every document of `kind`; how many there were.
    pub fn clear(&self, kind: &str) -> Result<usize> {
        connect()?
            .execute(
                "DELETE FROM project_documents WHERE project_id=?1 AND kind=?2",
                params![self.project_id, kind],
            )
            .map_err(sql)
    }
}

fn connect() -> Result<Connection> {
    crate::database::connect()
}

fn sql(error: rusqlite::Error) -> OcgError {
    crate::database::sql(&crate::database::path().unwrap_or_default(), error)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or_default()
}
