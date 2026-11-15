use super::*;

mod catalog;
mod conversations;
mod execution_context;
mod jobs;
mod usage;

pub(crate) use catalog::*;
pub(crate) use conversations::*;
pub(crate) use execution_context::*;
pub(crate) use jobs::*;
pub(crate) use usage::*;

pub(super) fn read(database: &Database) -> Result<ReadTransaction, StorageError> {
    Ok(database.begin_read()?)
}

pub(crate) fn check_readable(database: &Database) -> Result<(), StorageError> {
    let tx = read(database)?;
    recovery::check_identity(&tx.open_table(META)?)
}
