//! Pool routes:
//! - `GET /api/v2/pools` (cursor-paged; `GET /api/v1/pools` is retired, 410)
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
use crate::db::entities::{ExecutionOperation, ExecutionPoolId, Violation};
use crate::db::{DatabaseError, DatabaseHandle};

const POOL_NOT_FOUND: Refusal =
    Refusal::new(StatusCode::NOT_FOUND, "pool_not_found", "no such Pool");
/// The Pool list's version: `GET /api/v2/pools` pages with a cursor. The
/// unpaged `GET /api/v1/pools` is retired and answers 410.
pub(super) const POOL_API_VERSION: u32 = 2;

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

pub(super) fn pools_query(query: &str) -> Result<(Option<ExecutionPoolId>, usize), Refusal> {
    catalog::page_query(query, pool_id)
}

pub(super) async fn list(
    database: &DatabaseHandle,
    before: Option<ExecutionPoolId>,
    limit: usize,
) -> Result<Reply, Refusal> {
    {
        let page = database
            .native_resources(
                before.map(Into::into),
                limit,
                crate::db::redb::NativeResourceKind::Pool,
            )
            .await?;
        let next_before = page.next.map(|id| id.to_string());
        let pools = page
            .records
            .into_iter()
            .map(|record| match record {
                crate::db::redb::NativeResource::Pool(pool) => Ok(native_pool_json(&pool)),
                crate::db::redb::NativeResource::Executor(_) => Err(super::INCONSISTENT_RECORD),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut body = envelope(
            serde_json::json!({"resource_model":"independent", "pools":pools,"has_more":next_before.is_some(),"next_before":next_before}),
        );
        body["api_version"] = serde_json::json!(POOL_API_VERSION);
        Ok(Reply::ok(body))
    }
}

fn native_pool_json(pool: &crate::db::entities::AllocationPool) -> serde_json::Value {
    serde_json::json!({"id":pool.id.to_string(),"name":pool.name,"capacity":pool.capacity.get(),"enabled":pool.enabled,"archived":pool.archived,"revision":pool.revision})
}

fn native_executor_json(executor: &crate::db::entities::AllocationExecutor) -> serde_json::Value {
    let capabilities: Vec<_> = executor.capabilities.iter().map(|(operation, protocol)| serde_json::json!({"operation":operation_str(*operation),"protocol":protocol.map(|protocol| protocol.as_str())})).collect();
    serde_json::json!({"id":executor.id.to_string(),"pool":executor.pool.to_string(),"name":executor.name,"enabled":executor.enabled,"archived":executor.archived,"revision":executor.revision,"capabilities":capabilities})
}

pub(super) async fn list_executors(
    database: &DatabaseHandle,
    before: Option<crate::db::entities::ConfigId>,
    limit: usize,
) -> Result<Reply, Refusal> {
    let page = database
        .native_resources(before, limit, crate::db::redb::NativeResourceKind::Executor)
        .await?;
    let next_before = page.next.map(|id| id.to_string());
    let executors = page
        .records
        .into_iter()
        .map(|record| match record {
            crate::db::redb::NativeResource::Executor(executor) => {
                Ok(native_executor_json(&executor))
            }
            crate::db::redb::NativeResource::Pool(_) => Err(super::INCONSISTENT_RECORD),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Reply::ok(envelope(
        serde_json::json!({"resource_model":"independent","executors":executors,"has_more":next_before.is_some(),"next_before":next_before}),
    )))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativePoolInput {
    id: String,
    name: String,
    capacity: NonZeroU32,
    enabled: bool,
}

pub(super) async fn create_pool(database: &DatabaseHandle, body: &[u8]) -> Result<Reply, Refusal> {
    let input: NativePoolInput = serde_json::from_slice(body).map_err(|_| INVALID_REQUEST)?;
    let pool = crate::db::entities::AllocationPool {
        id: pool_id(&input.id)?,
        name: input.name,
        capacity: input.capacity,
        enabled: input.enabled,
        archived: false,
        revision: 1,
    };
    database
        .register_native_pool(pool.clone())
        .await
        .map_err(refusal)?;
    Ok(Reply::ok(envelope(
        serde_json::json!({"resource_model":"independent", "pool":native_pool_json(&pool)}),
    )))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeExecutorInput {
    id: String,
    pool: String,
    name: String,
    enabled: bool,
    capabilities: Vec<NativeCapabilityInput>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeCapabilityInput {
    operation: String,
    protocol: Option<String>,
}

pub(super) async fn create_executor(
    database: &DatabaseHandle,
    body: &[u8],
) -> Result<Reply, Refusal> {
    use crate::db::entities::{AllocationExecutor, ExecutorId, LlmTransport};
    let input: NativeExecutorInput = serde_json::from_slice(body).map_err(|_| INVALID_REQUEST)?;
    let id = crate::db::entities::parse_entity_id(&input.id)
        .map(ExecutorId::new)
        .ok_or(INVALID_REQUEST)?;
    let pool = pool_id(&input.pool)?;
    let mut capabilities = std::collections::BTreeSet::new();
    for capability in input.capabilities {
        let pair = match (
            capability.operation.as_str(),
            capability.protocol.as_deref(),
        ) {
            ("llm_request", Some(protocol)) => (
                ExecutionOperation::LlmRequest,
                Some(LlmTransport::parse(protocol).ok_or(INVALID_REQUEST)?),
            ),
            ("decision", None) => (ExecutionOperation::Decision, None),
            _ => return Err(INVALID_REQUEST),
        };
        if !capabilities.insert(pair) {
            return Err(INVALID_REQUEST);
        }
    }
    let executor = AllocationExecutor {
        id,
        pool,
        name: input.name,
        enabled: input.enabled,
        archived: false,
        revision: 1,
        capabilities,
    };
    database
        .register_native_executor(executor.clone())
        .await
        .map_err(refusal)?;
    Ok(Reply::ok(envelope(
        serde_json::json!({"executor":native_executor_json(&executor)}),
    )))
}

fn refusal(error: DatabaseError) -> Refusal {
    match error {
        DatabaseError::Rejected(Violation::InvalidValue(
            "pool",
            "configuration identity already exists",
        )) => Refusal::new(
            StatusCode::CONFLICT,
            "pool_exists",
            "the Pool identity is already in use",
        ),
        DatabaseError::Rejected(Violation::InvalidValue("pool", _)) => Refusal::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_pool",
            "the Pool configuration is invalid",
        ),
        DatabaseError::Rejected(Violation::InvalidValue(
            "executor",
            "configuration identity already exists",
        )) => Refusal::new(
            StatusCode::CONFLICT,
            "executor_exists",
            "the Executor identity is already in use",
        ),
        DatabaseError::Rejected(Violation::InvalidValue("executor", _)) => Refusal::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_executor",
            "the Executor configuration is invalid",
        ),
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
    {
        if wire.decision.is_some() || wire.concurrency_limit.is_some() == wire.enabled.is_some() {
            return Err(INVALID_REQUEST);
        }
        let pool = database
            .update_native_pool(id, wire.concurrency_limit, wire.enabled)
            .await
            .map_err(refusal)?;
        Ok(Reply::ok(envelope(
            serde_json::json!({"resource_model":"independent","pool":native_pool_json(&pool)}),
        )))
    }
}
