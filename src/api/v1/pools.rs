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

use super::{INVALID_REQUEST, Refusal, Reply, catalog, profiles};
use crate::db::entities::{
    ConfigBody, ConfigKind, ExecutionOperation, ExecutionPoolId, ExecutionPoolRecord, Violation,
};
use crate::db::{DatabaseError, DatabaseHandle};

const POOL_NOT_FOUND: Refusal =
    Refusal::new(StatusCode::NOT_FOUND, "pool_not_found", "no such Pool");

/// Exactly one of the two fields.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PoolUpdateInput {
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
    crate::db::entities::parse_entity_id(text)
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

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Pool")]
pub(crate) struct PoolView {
    id: String,
    name: String,
    concurrency_limit: u32,
    enabled: bool,
    operations: Vec<String>,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Pools")]
pub(crate) struct PoolsView {
    pools: Vec<PoolView>,
    has_more: bool,
    next_before: Option<String>,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "PoolAnswer")]
pub(crate) struct PoolAnswerView {
    pool: PoolView,
}

fn pool_json(pool: &ExecutionPoolRecord) -> PoolView {
    PoolView {
        id: pool.id.to_string(),
        name: pool.name.clone(),
        concurrency_limit: pool.config.concurrency_limit.get(),
        enabled: pool.enabled,
        operations: pool
            .config
            .capabilities
            .operations
            .iter()
            .map(|operation| operation_str(*operation).to_owned())
            .collect(),
    }
}

pub(super) fn pools_query(query: &str) -> Result<(Option<ExecutionPoolId>, usize), Refusal> {
    catalog::page_query(query, pool_id)
}

pub(super) async fn list(
    database: &DatabaseHandle,
    before: Option<ExecutionPoolId>,
    limit: usize,
) -> Result<Reply, Refusal> {
    let page = database
        .configs(Some(ConfigKind::Pool), before.map(Into::into), limit)
        .await?;
    let next_before = page.next.map(|id| ExecutionPoolId::from(id).to_string());
    let pools = page
        .records
        .into_iter()
        .filter_map(|record| {
            let ConfigBody::Pool(body) = record.body else {
                return None;
            };
            Some(PoolView {
                id: ExecutionPoolId::from(record.id).to_string(),
                name: record.name,
                concurrency_limit: NonZeroU32::new(body.concurrency_limit)?.get(),
                enabled: body.enabled,
                operations: body
                    .capability_set()
                    .operations
                    .iter()
                    .map(|operation| operation_str(*operation).to_owned())
                    .collect(),
            })
        })
        .collect();
    Ok(Reply::ok(envelope(serde_json::json!(PoolsView {
        pools,
        has_more: next_before.is_some(),
        next_before,
    }))))
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
    let wire: PoolUpdateInput = serde_json::from_slice(body).map_err(|_| INVALID_REQUEST)?;
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
    Ok(Reply::ok(envelope(serde_json::json!(PoolAnswerView {
        pool: pool_json(&pool)
    }))))
}
