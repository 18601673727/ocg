use crate::db::commands::Command;
use crate::db::entities::{
    AllocationExecutor, AllocationPool, CursorPage, ExecutionPoolId, PoolAllocationPage,
    UncertainAllocation,
};
use crate::db::entities::{ExecutorId, OperationKind, Violation};
use crate::db::errors::{CommandOutcome, DatabaseError};
use crate::db::worker::{DatabaseHandle, settle};
use crate::db::{DevelopmentReservationError, entities, redb};
use std::ops::ControlFlow::{Break, Continue};

impl DatabaseHandle {
    pub async fn uncertain_allocations(&self) -> Result<Vec<UncertainAllocation>, DatabaseError> {
        self.read(redb::read_uncertain_allocations).await
    }

    pub(crate) async fn scheduled_external_policy(
        &self,
        id: entities::ScheduledExternalPolicyId,
    ) -> Result<Option<entities::ScheduledExternalPolicyRecord>, DatabaseError> {
        self.read(move |database| redb::read_scheduled_external_policy(database, id))
            .await
    }

    pub(crate) async fn scheduled_external_policies(
        &self,
        after: Option<entities::ScheduledExternalPolicyId>,
        limit: usize,
    ) -> Result<
        CursorPage<entities::ScheduledExternalPolicyRecord, entities::ScheduledExternalPolicyId>,
        DatabaseError,
    > {
        self.read(move |database| redb::read_scheduled_external_policies(database, after, limit))
            .await
    }

    pub(crate) async fn reserve_scheduled_job(
        &self,
        fresh: entities::DevelopmentFreshReservation,
        eligible: fn(&entities::FrozenConfiguration, bool) -> bool,
    ) -> Result<entities::DevelopmentReservation, DevelopmentReservationError> {
        let request = fresh.request();
        let result = self
            .submit(|reply| {
                Command::custom(
                    (request, eligible),
                    reply,
                    |_| 4096,
                    |(request, eligible), reply, database, operation, commit| {
                        let (outcome, declined) = redb::run_batch_declinable(
                            database,
                            operation.epoch,
                            commit,
                            |batch| {
                                let result =
                                    batch.reserve_next_development_job(request, eligible)?;
                                Ok(
                                    if matches!(
                                        result,
                                        entities::DevelopmentReservation::Reserved(_)
                                    ) {
                                        Continue(result)
                                    } else {
                                        Break(result)
                                    },
                                )
                            },
                        );
                        settle(
                            operation,
                            commit,
                            OperationKind::AssignJobExecutor,
                            reply,
                            outcome,
                            declined,
                        )
                    },
                )
            })
            .await;
        match result {
            Ok(result) => Ok(result),
            Err(error)
                if matches!(
                    error.command_outcome(),
                    Some(CommandOutcome::NotAccepted | CommandOutcome::NotCommitted)
                ) =>
            {
                Err(DevelopmentReservationError::DefinitelyNotCommitted(
                    Box::new(error),
                ))
            }
            Err(error) => Err(DevelopmentReservationError::Other(error)),
        }
    }

    pub(crate) async fn request_native_stop(
        &self,
        context: entities::ComposerContextId,
        job: entities::JobId,
    ) -> Result<entities::NativeStopRequest, DatabaseError> {
        self.submit(|reply| {
            Command::custom(
                (context, job),
                reply,
                |_| 8192,
                |(context, job), reply, database, operation, commit| {
                    let (outcome, declined) =
                        redb::run_batch_declinable(database, operation.epoch, commit, |batch| {
                            batch.require_mutable_job_context(context, job)?;
                            let result = batch.request_native_stop(job)?;
                            Ok(match result {
                                entities::NativeStopRequest::AlreadyRequested(_) => Break(result),
                                entities::NativeStopRequest::EndedBeforeStart(_)
                                | entities::NativeStopRequest::Requested(_) => Continue(result),
                            })
                        });
                    settle(
                        operation,
                        commit,
                        OperationKind::StopTurn,
                        reply,
                        outcome,
                        declined,
                    )
                },
            )
        })
        .await
    }

    pub(crate) async fn allocation_pool(
        &self,
        id: ExecutionPoolId,
    ) -> Result<Option<AllocationPool>, DatabaseError> {
        self.read(move |database| redb::read_pool(database, id))
            .await
    }

    pub(crate) async fn allocation_pools(
        &self,
        before: Option<ExecutionPoolId>,
        limit: usize,
    ) -> Result<CursorPage<AllocationPool, ExecutionPoolId>, DatabaseError> {
        self.read(move |database| redb::read_pools(database, before, limit))
            .await
    }

    pub(crate) async fn allocation_executors(
        &self,
        before: Option<ExecutorId>,
        limit: usize,
    ) -> Result<CursorPage<AllocationExecutor, ExecutorId>, DatabaseError> {
        self.read(move |database| redb::read_executors(database, before, limit))
            .await
    }

    pub(crate) async fn pool_allocations(
        &self,
        pool: entities::ExecutionPoolId,
        after: Option<(u128, u128)>,
        limit: usize,
    ) -> Result<Option<PoolAllocationPage>, DatabaseError> {
        self.read(move |database| redb::read_pool_allocations(database, pool, after, limit))
            .await
    }

    pub(crate) async fn register_native_pool(
        &self,
        pool: entities::AllocationPool,
    ) -> Result<(), DatabaseError> {
        self.write(4096, OperationKind::CreateExecutionPool, pool, |b, p| {
            b.register_allocation_pool(p)
        })
        .await
    }

    pub(crate) async fn register_native_executor(
        &self,
        executor: entities::AllocationExecutor,
    ) -> Result<(), DatabaseError> {
        self.write(4096, OperationKind::CreateExecutor, executor, |b, e| {
            b.register_allocation_executor(e)
        })
        .await
    }

    pub(crate) async fn update_native_executor(
        &self,
        id: ExecutorId,
        expected_revision: u32,
        update: entities::ExecutorUpdate,
    ) -> Result<entities::AllocationExecutor, DatabaseError> {
        self.write(
            4096,
            OperationKind::UpdateExecutor,
            (id, expected_revision, update),
            |b, (id, rev, upd)| b.update_native_executor(id, rev, upd),
        )
        .await
    }

    pub(crate) async fn archive_native_executor(
        &self,
        id: ExecutorId,
        expected_revision: u32,
    ) -> Result<entities::AllocationExecutor, DatabaseError> {
        self.write(
            4096,
            OperationKind::ArchiveExecutor,
            (id, expected_revision),
            |b, (id, rev)| b.archive_native_executor(id, rev),
        )
        .await
    }

    pub(crate) async fn archive_native_pool(
        &self,
        id: ExecutionPoolId,
        expected_revision: u32,
    ) -> Result<entities::AllocationPool, DatabaseError> {
        self.write(
            4096,
            OperationKind::ArchivePool,
            (id, expected_revision),
            |b, (id, rev)| b.archive_native_pool(id, rev),
        )
        .await
    }

    pub(crate) async fn update_native_pool(
        &self,
        id: ExecutionPoolId,
        expected_revision: u32,
        capacity: Option<std::num::NonZeroU32>,
        enabled: Option<bool>,
    ) -> Result<entities::AllocationPool, DatabaseError> {
        self.write(
            4096,
            OperationKind::SetExecutionPoolProfile,
            (id, expected_revision, capacity, enabled),
            |b, (id, rev, cap, en)| b.update_native_pool(id, rev, cap, en),
        )
        .await
    }

    pub(crate) async fn create_scheduled_external_policy(
        &self,
        config: entities::ScheduledExternalPolicyConfig,
    ) -> Result<entities::ScheduledExternalPolicyRecord, DatabaseError> {
        self.write(
            8192,
            OperationKind::SetScheduledExternalPolicy,
            config,
            |batch, config| batch.create_scheduled_external_policy(&config),
        )
        .await
    }

    pub(crate) async fn update_scheduled_external_policy(
        &self,
        id: entities::ScheduledExternalPolicyId,
        expected_revision: u32,
        config: entities::ScheduledExternalPolicyConfig,
    ) -> Result<entities::ScheduledExternalPolicyRecord, DatabaseError> {
        self.write(
            8192,
            OperationKind::SetScheduledExternalPolicy,
            (id, expected_revision, config),
            |batch, (id, rev, config)| batch.update_scheduled_external_policy(id, rev, &config),
        )
        .await
    }

    pub(crate) async fn set_scheduled_external_policy_enabled(
        &self,
        id: entities::ScheduledExternalPolicyId,
        expected_revision: u32,
        enabled: bool,
    ) -> Result<entities::ScheduledExternalPolicyRecord, DatabaseError> {
        self.write(
            4096,
            OperationKind::SetScheduledExternalPolicy,
            (id, expected_revision, enabled),
            |batch, (id, rev, enabled)| {
                batch.set_scheduled_external_policy_enabled(id, rev, enabled)
            },
        )
        .await
    }

    pub(crate) async fn revoke_scheduled_external_policy(
        &self,
        id: entities::ScheduledExternalPolicyId,
        expected_revision: u32,
    ) -> Result<entities::ScheduledExternalPolicyRecord, DatabaseError> {
        self.write(
            4096,
            OperationKind::SetScheduledExternalPolicy,
            (id, expected_revision),
            |batch, (id, rev)| batch.revoke_scheduled_external_policy(id, rev),
        )
        .await
    }

    pub(crate) async fn scheduled_external_bound_policy(
        &self,
        token: entities::AllocationToken,
    ) -> Result<Option<entities::ScheduledExternalPolicyRecord>, DatabaseError> {
        let pair = self
            .read(move |database| {
                redb::read_scheduled_external_binding_with_policy(database, token.job)
            })
            .await?;
        let Some((binding, policy)) = pair else {
            return Ok(None);
        };
        if binding.job != token.job
            || binding.allocation_epoch != token.epoch
            || binding.allocation_generation != token.generation
        {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "scheduled_external_policy",
                "the binding does not name this Allocation",
            )));
        }
        if policy.revision != binding.policy_revision {
            return Err(DatabaseError::Rejected(Violation::InvalidValue(
                "scheduled_external_policy",
                "the bound policy revision changed since reservation",
            )));
        }
        Ok(Some(policy))
    }

    pub(crate) async fn claim_due_recurrences(
        &self,
        limit: usize,
    ) -> Result<Option<std::time::Duration>, DatabaseError> {
        self.write(
            4096,
            OperationKind::ClaimExecution,
            limit,
            |batch, limit| batch.claim_due_recurrences(limit),
        )
        .await
    }
}
