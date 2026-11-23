use crate::db::commands::Command;
use crate::db::entities::{
    AllocationCursor, AllocationExecutor, AllocationPool, CursorPage, ExecutionPoolId,
    PoolAllocationPage, UncertainPage,
};
use crate::db::entities::{ExecutorId, OperationKind, Violation};
use crate::db::errors::{CommandOutcome, DatabaseError};
use crate::db::worker::{DatabaseHandle, settle};
use crate::db::{DevelopmentReservationError, entities, redb};
use std::ops::ControlFlow::{Break, Continue};

impl DatabaseHandle {
    pub(crate) fn native_stops(&self) -> crate::db::NativeStops {
        self.shared.native_stops.clone()
    }

    pub(crate) async fn uncertain_allocations(
        &self,
        after: Option<AllocationCursor>,
    ) -> Result<UncertainPage, DatabaseError> {
        self.read(move |db| redb::read_uncertain_allocations(db, after))
            .await
    }

    pub(crate) fn open_epoch(&self) -> u64 {
        self.shared.epoch.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) async fn release_interrupted(
        &self,
        token: entities::AllocationToken,
        revision: u64,
    ) -> Result<bool, DatabaseError> {
        self.write(
            0,
            OperationKind::ReleaseCapacity,
            (token, revision),
            |batch, (token, revision)| {
                batch.release_uncertain(token, revision, entities::StopProof::User, None)
            },
        )
        .await
    }

    pub(crate) async fn recheck_interrupted(&self) -> Result<(), DatabaseError> {
        let mut after = None;
        let epoch = self.open_epoch();
        loop {
            let page = self.uncertain_allocations(after).await?;
            let candidates = page
                .records
                .into_iter()
                .filter(|row| row.allocation.token.epoch < epoch)
                .collect::<Vec<_>>();
            if !candidates.is_empty() {
                let proved = ntex::rt::spawn_blocking(move || {
                    let host = crate::host::identity::current_host_id();
                    let boot = crate::host::identity::current_boot_id();
                    candidates
                        .into_iter()
                        .filter_map(|row| {
                            redb::stop_proof(
                                &row.allocation,
                                &row.commands,
                                host.as_deref(),
                                boot.as_deref(),
                            )
                            .map(|proof| {
                                (
                                    row.allocation.token,
                                    row.allocation.job_revision,
                                    row.allocation_bytes,
                                    proof,
                                )
                            })
                        })
                        .collect::<Vec<_>>()
                })
                .await;
                match proved {
                    Ok(proved) => {
                        for candidate in proved {
                            self.write(
                                0,
                                OperationKind::ReleaseCapacity,
                                candidate,
                                |batch, (token, revision, bytes, proof)| {
                                    batch.release_uncertain(token, revision, proof, Some(&bytes))
                                },
                            )
                            .await?;
                        }
                    }
                    Err(error) => log::warn!("interrupted allocation proof worker failed: {error}"),
                }
            }
            after = page.next;
            if after.is_none() {
                return Ok(());
            }
        }
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
        let stops = self.native_stops();
        self.submit(|reply| {
            Command::custom(
                (context, job),
                reply,
                |_| 8192,
                move |(context, job), reply, database, operation, commit| {
                    let (outcome, declined) =
                        redb::run_batch_declinable(database, operation.epoch, commit, |batch| {
                            batch.native_stops = Some(stops);
                            batch.require_mutable_job_context(context, job)?;
                            let result = batch.request_native_stop(job)?;
                            Ok(match result {
                                entities::NativeStopRequest::AlreadyRequested(_) => Break(result),
                                entities::NativeStopRequest::EndedBeforeStart { .. }
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
        let next = self.preflight_write(redb::read_recurrence_delay).await?;
        if next != Some(std::time::Duration::ZERO) || limit == 0 {
            return Ok(next);
        }
        self.write_declinable(
            limit,
            |_| 4096,
            OperationKind::ClaimExecution,
            redb::Batch::claim_due_recurrences,
        )
        .await
    }
}
