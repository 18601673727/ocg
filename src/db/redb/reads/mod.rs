use super::{StorageError, StorageErrorKind};
use crate::db::redb::schema::META;
use crate::db::redb::{job_tree, recovery, rows};
use ::redb::{Database, ReadTransaction, ReadableDatabase};

pub(super) mod catalog;
pub(super) mod conversations;
pub(super) mod execution_context;
pub(super) mod jobs;
pub(super) mod usage;

pub(super) fn read(database: &Database) -> Result<ReadTransaction, StorageError> {
    Ok(database.begin_read()?)
}

pub(crate) fn check_readable(database: &Database) -> Result<(), StorageError> {
    let tx = read(database)?;
    recovery::check_identity(&tx.open_table(META)?)
}
