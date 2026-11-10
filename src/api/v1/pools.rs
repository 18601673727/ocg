//! Pool routes:
//! - `GET /api/v2/pools` (cursor-paged; `GET /api/v1/pools` is retired, 410)
//! - `POST /api/v1/pools/{execution_pool}`
//! - `POST /api/v1/executors/{executor}` (one change, from `expected_revision`)
//! - `POST /api/v1/executors/{executor}/archive` (retire an idle Executor)
//! - `POST /api/v1/pools/{execution_pool}/archive` (retire a Pool with no
//!   Allocation and no unarchived Executor)
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
use crate::db::entities::{ExecutionOperation, ExecutionPoolId, RecordRef, Violation};
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

/// The capability set a request names: each pair valid, none repeated.
fn capability_set(
    capabilities: Vec<NativeCapabilityInput>,
) -> Result<
    std::collections::BTreeSet<(
        ExecutionOperation,
        Option<crate::db::entities::LlmTransport>,
    )>,
    Refusal,
> {
    use crate::db::entities::LlmTransport;
    let mut set = std::collections::BTreeSet::new();
    for capability in capabilities {
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
        if !set.insert(pair) {
            return Err(INVALID_REQUEST);
        }
    }
    Ok(set)
}

pub(super) async fn create_executor(
    database: &DatabaseHandle,
    body: &[u8],
) -> Result<Reply, Refusal> {
    use crate::db::entities::{AllocationExecutor, ExecutorId};
    let input: NativeExecutorInput = serde_json::from_slice(body).map_err(|_| INVALID_REQUEST)?;
    let id = crate::db::entities::parse_entity_id(&input.id)
        .map(ExecutorId::new)
        .ok_or(INVALID_REQUEST)?;
    let pool = pool_id(&input.pool)?;
    let capabilities = capability_set(input.capabilities)?;
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

/// Exactly one of `enabled`, `name` and `capabilities`, made from
/// `expected_revision`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeExecutorUpdateInput {
    expected_revision: u32,
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    capabilities: Option<Vec<NativeCapabilityInput>>,
}

pub(super) fn executor_id(text: &str) -> Result<crate::db::entities::ExecutorId, Refusal> {
    crate::db::entities::parse_entity_id(text)
        .map(crate::db::entities::ExecutorId::new)
        .ok_or(INVALID_REQUEST)
}

pub(super) async fn update_executor(
    database: &DatabaseHandle,
    id: crate::db::entities::ExecutorId,
    body: &[u8],
) -> Result<Reply, Refusal> {
    use crate::db::entities::ExecutorUpdate;
    let input: NativeExecutorUpdateInput =
        serde_json::from_slice(body).map_err(|_| INVALID_REQUEST)?;
    let update = match (input.enabled, input.name, input.capabilities) {
        (Some(enabled), None, None) => ExecutorUpdate::Enabled(enabled),
        (None, Some(name), None) => ExecutorUpdate::Name(name),
        (None, None, Some(capabilities)) => {
            ExecutorUpdate::Capabilities(capability_set(capabilities)?)
        }
        _ => return Err(INVALID_REQUEST),
    };
    let executor = database
        .update_native_executor(id, input.expected_revision, update)
        .await
        .map_err(refusal)?;
    Ok(Reply::ok(envelope(
        serde_json::json!({"executor":native_executor_json(&executor)}),
    )))
}

/// Exactly `expected_revision`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeExecutorArchiveInput {
    expected_revision: u32,
}

/// Retires one idle Executor for good. The Executor is archived only when it
/// holds no Allocation; nothing is released, stopped or resolved.
pub(super) async fn archive_executor(
    database: &DatabaseHandle,
    id: crate::db::entities::ExecutorId,
    body: &[u8],
) -> Result<Reply, Refusal> {
    let input: NativeExecutorArchiveInput =
        serde_json::from_slice(body).map_err(|_| INVALID_REQUEST)?;
    let executor = database
        .archive_native_executor(id, input.expected_revision)
        .await
        .map_err(refusal)?;
    Ok(Reply::ok(envelope(
        serde_json::json!({"executor":native_executor_json(&executor)}),
    )))
}

/// Exactly `expected_revision`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativePoolArchiveInput {
    expected_revision: u32,
}

/// Retires one Pool for good. The Pool is archived only when it holds no
/// Allocation and every Executor naming it is already archived; nothing is
/// released, stopped, resolved or cascaded.
pub(super) async fn archive_pool(
    database: &DatabaseHandle,
    id: ExecutionPoolId,
    body: &[u8],
) -> Result<Reply, Refusal> {
    let input: NativePoolArchiveInput =
        serde_json::from_slice(body).map_err(|_| INVALID_REQUEST)?;
    let pool = database
        .archive_native_pool(id, input.expected_revision)
        .await
        .map_err(refusal)?;
    Ok(Reply::ok(envelope(
        serde_json::json!({"resource_model":"independent","pool":native_pool_json(&pool)}),
    )))
}

const INVALID_CURSOR: Refusal = Refusal::new(
    StatusCode::BAD_REQUEST,
    "invalid_cursor",
    "the allocation cursor is not one this API issued",
);
const ALLOCATION_INDEX_INCONSISTENT: Refusal = Refusal::new(
    StatusCode::INTERNAL_SERVER_ERROR,
    "allocation_index_inconsistent",
    "the Pool's active Allocation index disagrees with its stored Allocations",
);

/// `after` and `limit`, each at most once. `after` is `{executor}.{job}`, the
/// last entry of the previous page.
pub(super) fn allocations_query(query: &str) -> Result<(Option<(u128, u128)>, usize), Refusal> {
    let (mut after, mut limit) = (None, None);
    if !query.is_empty() {
        for pair in query.split('&') {
            let (key, value) = pair.split_once('=').ok_or(INVALID_REQUEST)?;
            match key {
                "after" if after.is_none() => {
                    let cursor = value
                        .split_once('.')
                        .and_then(|(executor, job)| {
                            Some((
                                crate::db::entities::parse_entity_id(executor)?,
                                crate::db::entities::parse_entity_id(job)?,
                            ))
                        })
                        .ok_or(INVALID_CURSOR)?;
                    after = Some(cursor);
                }
                "limit" if limit.is_none() => {
                    limit = Some(
                        super::decimal::<usize>(value)
                            .filter(|limit| (1..=catalog::MAX_CONVERSATION_PAGE).contains(limit))
                            .ok_or(INVALID_REQUEST)?,
                    );
                }
                _ => return Err(INVALID_REQUEST),
            }
        }
    }
    Ok((after, limit.unwrap_or(ALLOCATIONS_DEFAULT_PAGE)))
}

const ALLOCATIONS_DEFAULT_PAGE: usize = 20;

/// `GET /api/v1/pools/{pool}/allocations`: the Allocations that hold capacity
/// in one Pool, and the Pool's occupancy, read-only. Held may exceed the
/// Pool's capacity after the capacity was lowered; nothing is clamped.
pub(super) async fn list_allocations(
    database: &DatabaseHandle,
    pool: ExecutionPoolId,
    after: Option<(u128, u128)>,
    limit: usize,
) -> Result<Reply, Refusal> {
    let page = database
        .pool_allocations(pool, after, limit)
        .await
        .map_err(|error| match error {
            DatabaseError::Read(storage)
                if storage.kind == crate::db::StorageErrorKind::Corrupted =>
            {
                ALLOCATION_INDEX_INCONSISTENT
            }
            other => other.into(),
        })?
        .ok_or(POOL_NOT_FOUND)?;
    let allocations: Vec<_> = page
        .allocations
        .iter()
        .map(|allocation| {
            let mut view = serde_json::to_value(super::jobs::allocation_view(allocation))
                .map_err(|_| super::INTERNAL)?;
            let entry = view.as_object_mut().ok_or(super::INTERNAL)?;
            entry.insert("job".to_owned(), allocation.token.job.to_string().into());
            entry.insert(
                "claimed_at_unix_micros".to_owned(),
                allocation.claimed_at.as_unix_micros().into(),
            );
            entry.insert(
                "started_at_unix_micros".to_owned(),
                allocation.started_at.map(|at| at.as_unix_micros()).into(),
            );
            Ok(view)
        })
        .collect::<Result<_, Refusal>>()?;
    let next_after = page.next.map(|(executor, job)| {
        format!(
            "{}.{}",
            crate::db::entities::ExecutorId::new(executor),
            crate::db::entities::JobId::new(job)
        )
    });
    let occupancy = match page.occupancy {
        Some(held) => serde_json::json!({
            "available": true,
            "held": held.held,
            "reserved": held.reserved,
            "running": held.running,
            "stop_requested": held.stop_requested,
            "uncertain": held.uncertain,
        }),
        None => serde_json::json!({
            "available": false,
            "held": null,
            "reserved": null,
            "running": null,
            "stop_requested": null,
            "uncertain": null,
        }),
    };
    Ok(Reply::ok(envelope(serde_json::json!({
        "pool": page.pool.id.to_string(),
        "capacity": page.pool.capacity.get(),
        "occupancy": occupancy,
        "allocations": allocations,
        "has_more": next_after.is_some(),
        "next_after": next_after,
    }))))
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
        DatabaseError::Rejected(Violation::InvalidValue("pool", "revision conflict")) => {
            Refusal::new(
                StatusCode::CONFLICT,
                "pool_revision_conflict",
                "the Pool changed since it was read; reload it and retry",
            )
        }
        DatabaseError::Rejected(Violation::InvalidValue("pool", "already archived")) => {
            Refusal::new(
                StatusCode::CONFLICT,
                "pool_archived",
                "the Pool is already archived",
            )
        }
        DatabaseError::Rejected(Violation::InvalidValue("pool", "holds an active Allocation")) => {
            Refusal::new(
                StatusCode::CONFLICT,
                "pool_holds_allocation",
                "the Pool still holds an Allocation; nothing was archived or released",
            )
        }
        DatabaseError::Rejected(Violation::InvalidValue("pool", "has an unarchived Executor")) => {
            Refusal::new(
                StatusCode::CONFLICT,
                "pool_has_active_executor",
                "an Executor of the Pool is not archived; archive each Executor first, nothing was archived",
            )
        }
        DatabaseError::Rejected(Violation::InvalidValue(
            "pool",
            "child Executor verification exceeds the scan bound",
        )) => Refusal::new(
            StatusCode::CONFLICT,
            "pool_child_scan_unavailable",
            "the Pool's Executors could not all be examined within the safety bound; archival was safely refused",
        ),
        DatabaseError::Rejected(Violation::InvalidValue(
            "pool",
            "active Allocation index is inconsistent",
        )) => Refusal::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "allocation_index_inconsistent",
            "the Pool's active Allocation index disagrees with its stored Allocations; nothing was archived",
        ),
        DatabaseError::Rejected(Violation::InvalidValue(
            "pool",
            "configuration storage is inconsistent",
        )) => Refusal::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "pool_storage_inconsistent",
            "a stored Pool or Executor configuration is unreadable; nothing was archived",
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
        DatabaseError::Rejected(Violation::InvalidValue("executor", "revision conflict")) => {
            Refusal::new(
                StatusCode::CONFLICT,
                "executor_revision_conflict",
                "the Executor changed since it was read; reload it and retry",
            )
        }
        DatabaseError::Rejected(Violation::InvalidValue("executor", "already archived")) => {
            Refusal::new(
                StatusCode::CONFLICT,
                "executor_archived",
                "the Executor is already archived",
            )
        }
        DatabaseError::Rejected(Violation::InvalidValue(
            "executor",
            "holds an active Allocation",
        )) => Refusal::new(
            StatusCode::CONFLICT,
            "executor_holds_allocation",
            "the Executor still holds an Allocation; nothing was archived or released",
        ),
        DatabaseError::Rejected(Violation::InvalidValue(
            "executor",
            "active Allocation index is inconsistent",
        )) => Refusal::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "allocation_index_inconsistent",
            "the Executor's active Allocation index disagrees with its stored Allocations; nothing was archived",
        ),
        DatabaseError::Rejected(Violation::NotFound(RecordRef::Executor(_))) => Refusal::new(
            StatusCode::NOT_FOUND,
            "executor_not_found",
            "no such Executor",
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
