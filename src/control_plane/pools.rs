//! Pool routes:
//! - `GET /api/v1/pools`
//! - `POST /api/v1/pools/{execution_pool}`
//!
//! A Pool is a config whose Executors share its identity. The list answers
//! every Pool at once. An update either sets the concurrency limit or
//! enables or disables the Pool. Lowering the limit below the number of
//! Jobs running stops new claims; the Jobs running continue.

use std::num::NonZeroU32;

use super::contracts::*;
use ntex::http::StatusCode;
use serde::Deserialize;

use super::{INVALID_REQUEST, Refusal, Reply, profiles};
use crate::database::{DatabaseError, DatabaseHandle};
use crate::persistence::{ExecutionOperation, ExecutionPool, ExecutionPoolId, Violation};
use crate::storage::MAX_PAGE;

const POOL_NOT_FOUND: Refusal =
    Refusal::new(StatusCode::NOT_FOUND, "pool_not_found", "no such Pool");

/// Exactly one of the two fields.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WirePoolUpdate {
    #[serde(default)]
    concurrency_limit: Option<NonZeroU32>,
    #[serde(default)]
    enabled: Option<bool>,
    /// Whether this Pool also offers typed decision work. Absent leaves the
    /// capability as it is.
    #[serde(default)]
    decision: Option<bool>,
}

pub(super) fn pool_id(text: &str) -> Result<ExecutionPoolId, Refusal> {
    crate::persistence::parse_entity_id(text)
        .map(ExecutionPoolId::new)
        .ok_or(INVALID_REQUEST)
}

/// The operations a Pool offers, as the API names them.
fn operation_str(operation: ExecutionOperation) -> &'static str {
    match operation {
        ExecutionOperation::LlmRequest => "llm_request",
        ExecutionOperation::Decision => "decision",
    }
}

fn pool_json(pool: &ExecutionPool) -> WirePool {
    WirePool {
        id: pool.id.to_string(),
        name: pool.name.clone(),
        concurrency_limit: pool.config.concurrency_limit.get(),
        enabled: pool.enabled,
        operations: Some(
            pool.config
                .capabilities
                .operations
                .iter()
                .map(|operation| operation_str(*operation).to_owned())
                .collect(),
        ),
    }
}

pub(super) async fn list(database: &DatabaseHandle) -> Result<Reply, Refusal> {
    let mut pools = Vec::new();
    let mut before = None;
    loop {
        let page = database.execution_pools(before, MAX_PAGE).await?;
        before = page.last().map(|pool| pool.id);
        let full = page.len() == MAX_PAGE;
        pools.extend(page.iter().map(pool_json));
        if !full {
            break;
        }
    }
    Ok(Reply::ok(envelope(WirePools { pools })))
}

fn refusal(error: DatabaseError) -> Refusal {
    match error {
        DatabaseError::Rejected(Violation::NotFound(_) | Violation::WrongConfigKind { .. }) => {
            POOL_NOT_FOUND
        }
        other => profiles::map_mutation_error(other),
    }
}

pub(super) async fn update(
    database: &DatabaseHandle,
    id: ExecutionPoolId,
    body: &[u8],
) -> Result<Reply, Refusal> {
    let wire: WirePoolUpdate = serde_json::from_slice(body).map_err(|_| INVALID_REQUEST)?;
    match (wire.concurrency_limit, wire.enabled, wire.decision) {
        (Some(concurrency_limit), None, None) => {
            let mut config = database
                .execution_pool(id)
                .await?
                .ok_or(POOL_NOT_FOUND)?
                .config;
            config.concurrency_limit = concurrency_limit;
            database
                .update_execution_pool(id, config)
                .await
                .map_err(refusal)?;
        }
        (None, Some(enabled), None) => {
            database
                .set_execution_pool_enabled(id, enabled)
                .await
                .map_err(refusal)?;
        }
        (None, None, Some(decision)) => {
            let mut config = database
                .execution_pool(id)
                .await?
                .ok_or(POOL_NOT_FOUND)?
                .config;
            if decision {
                config
                    .capabilities
                    .operations
                    .insert(ExecutionOperation::Decision);
            } else {
                config
                    .capabilities
                    .operations
                    .remove(&ExecutionOperation::Decision);
            }
            database
                .update_execution_pool(id, config)
                .await
                .map_err(refusal)?;
        }
        _ => return Err(INVALID_REQUEST),
    }
    let pool = database.execution_pool(id).await?.ok_or(POOL_NOT_FOUND)?;
    Ok(Reply::ok(envelope(WirePoolAnswer {
        pool: pool_json(&pool),
    })))
}
