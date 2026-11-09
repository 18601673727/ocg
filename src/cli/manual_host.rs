//! What the manually initiated execution commands (`external-trial` and
//! `run-loopback-job`) share: strict argument pieces, the existing database
//! they run against, and the one-shot host they run inside.
//!
//! Neither command may create a database, wait for another host, or run
//! beside one: the host takes the database lock without waiting and fails
//! when it is held.

use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;

use serde_json::json;

use crate::db::DatabaseHandle;
use crate::service::ai::providers::CancellationToken;
use crate::system::data_directory::{DATA_DIRECTORY_VARIABLE, DataDirectory};
use crate::system::runtime::HostError;

pub(super) type Failure = Box<dyn std::error::Error>;

pub(super) fn uuid(text: &str, what: &str) -> Result<u128, Failure> {
    uuid::Uuid::parse_str(text)
        .map(|id| id.as_u128())
        .map_err(|_| format!("the {what} must be a UUID").into())
}

/// Prints the refusal as the command's JSON answer and returns the failure
/// that ends the command. Nothing was sent.
pub(super) fn refused(code: &str, detail: &str) -> Failure {
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "outcome": "refused",
            "code": code,
            "detail": detail,
            "request_sent": false,
            "job_state_changed": false,
        }))
        .unwrap_or_default()
    );
    format!("refused: {code}").into()
}

/// An existing database only: a mistyped directory must not become a new one.
pub(super) fn existing_data_directory(never_creates: &str) -> Result<DataDirectory, Failure> {
    let root = std::env::var_os(DATA_DIRECTORY_VARIABLE)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| refused("data_directory", "OCG_DATA_DIR must be set"))?;
    if !std::path::Path::new(&root)
        .join(crate::db::DATABASE_FILE_NAME)
        .is_file()
    {
        return Err(refused(
            "no_database",
            &format!("OCG_DATA_DIR holds no database; {never_creates} never creates one"),
        ));
    }
    Ok(DataDirectory::bootstrap(root)?)
}

/// Runs `run` once inside a host of its own and returns what it produced,
/// with how the host closed. The operator's stop (SIGINT or SIGTERM) cancels
/// the token `run` was given and lets it settle what it can; it is not a
/// claim about the Provider. `None` means `run` never started because the
/// host did not open, so nothing was sent.
pub(super) fn host_once<T, F>(
    data_directory: &DataDirectory,
    run: impl FnOnce(DatabaseHandle, CancellationToken) -> F + 'static,
) -> (Option<T>, Result<(), HostError>)
where
    T: 'static,
    F: Future<Output = T> + 'static,
{
    let result: Rc<RefCell<Option<T>>> = Rc::new(RefCell::new(None));
    let slot = result.clone();
    let hosted =
        crate::system::runtime::run_hosted(data_directory, true, move |context| async move {
            use ntex::util::{Either, select};
            let cancellation = CancellationToken::default();
            let database = context.database().clone();
            let execution = run(database, cancellation.clone());
            let mut execution = std::pin::pin!(execution);
            let finished = match select(execution.as_mut(), ntex::rt::signals::signal()).await {
                Either::Left(finished) => finished,
                Either::Right(_) => {
                    cancellation.cancel();
                    execution.await
                }
            };
            *slot.borrow_mut() = Some(finished);
        });
    let finished = result.borrow_mut().take();
    (finished, hosted)
}
