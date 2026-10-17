use super::*;
use crate::persistence::{self, *};
use std::cell::Cell;
use std::num::{NonZeroU32, NonZeroUsize};

struct Fixture {
    database: Database,
    root: crate::test_support::TestRoot,
    path: PathBuf,
    epoch: u64,
    genesis: JobId,
    sequence: Cell<u64>,
    llm: ProfileId,
    profile: ProfileId,
    pool: ExecutionPoolId,
}

impl Fixture {
    fn new(name: &str, limit: u32) -> Self {
        let (root, path) = crate::test_support::temporary_directory("ocg-stage-b-storage", name);
        let opened = open(&path).ok().expect("fresh database");
        let mut fixture = Self {
            database: opened.database,
            root,
            path,
            epoch: opened.epoch,
            genesis: opened.genesis,
            sequence: Cell::new(0),
            llm: ProfileId::new(0),
            profile: ProfileId::new(0),
            pool: ExecutionPoolId::new(0),
        };
        let (llm, profile, pool) = fixture
            .write(OperationKind::CreateExecutionPool, |batch| {
                let (provider, _) = persistence::create_llm_provider(
                    batch,
                    NewLlmProvider {
                        name: "test provider".into(),
                        transport: LlmTransport::OpenAiChatCompletions,
                        endpoint: None,
                        credential_ref: None,
                    },
                )?;
                let (llm, _) = persistence::create_profile(
                    batch,
                    NewProfile {
                        name: "model".into(),
                        config: NewProfileConfig::Llm {
                            provider,
                            config: LlmConfig {
                                model: "model".into(),
                                parameters: Default::default(),
                                declared_capabilities: Default::default(),
                            },
                        },
                    },
                )?;
                let (profile, _) = persistence::create_profile(batch, pool_profile(limit))?;
                let (pool, subjects) = persistence::create_execution_pool(
                    batch,
                    NewExecutionPool {
                        name: "ordinary".into(),
                        profile,
                        enabled: true,
                    },
                )?;
                Ok(((llm, profile, pool), subjects))
            })
            .unwrap();
        fixture.llm = llm;
        fixture.profile = profile;
        fixture.pool = pool;
        fixture
    }

    fn operation(&self) -> OperationId {
        self.sequence.set(self.sequence.get() + 1);
        OperationId::new(self.epoch, self.sequence.get())
    }

    fn write<T>(
        &self,
        kind: OperationKind,
        body: impl FnOnce(&mut Batch) -> Result<(T, MarkerSubjects), BatchError>,
    ) -> Result<T, BatchFailure> {
        run_batch(&self.database, self.operation(), kind, body).result
    }

    fn job(&self, parent: JobId, dependencies: Vec<(JobId, DependencyCondition)>) -> JobId {
        self.write(OperationKind::CreateJobWithInput, |batch| {
            let (input, _) = persistence::save_job_input(batch, None, JobInputDraft::default())?;
            persistence::create_job_with_input(
                batch,
                NewJob {
                    name: None,
                    execution_mode: ExecutionMode::Once,
                    eligible_at: None,
                    scheduled_at: None,
                    next_due_at: None,
                    next_iteration_no: None,
                    iteration_no: None,
                    parent,
                    projects: Vec::new(),
                    dependencies,
                    configuration: Some(ExecutionConfiguration {
                        agent_profile: None,
                        llm_profile: self.llm,
                    }),
                    assigned_executor: None,
                    previous: None,
                    replaces: None,
                },
                input,
            )
        })
        .unwrap()
    }

    fn claim(
        &self,
        job: JobId,
        allowed: BTreeSet<ExecutionPoolId>,
    ) -> (Result<ClaimGrant, ClaimRefusal>, OperationId) {
        let operation = self.operation();
        let mut refusal = None;
        let outcome = run_batch_declinable(
            &self.database,
            operation,
            OperationKind::ClaimExecution,
            Commit::Durable,
            |batch| match batch.claim_execution(
                ClaimRequest {
                    job,
                    allowed_pools: allowed,
                },
                self.epoch,
            )? {
                Ok(grant) => Ok(Some((
                    grant,
                    MarkerSubjects::addressed(RecordRef::Job(job)),
                ))),
                Err(reason) => {
                    refusal = Some(reason);
                    Ok(None)
                }
            },
        )
        .result
        .unwrap();
        (outcome.map_or_else(|| Err(refusal.unwrap()), Ok), operation)
    }

    fn unknown(&self, job: JobId) {
        self.write(OperationKind::FinalizeJob, |batch| {
            persistence::finalize_job(batch, job, JobFinalization::Unknown)
        })
        .unwrap();
    }

    fn stop(&self, job: JobId) {
        self.write(OperationKind::ConfirmJobStopped, |batch| {
            persistence::confirm_job_stopped(batch, job)
        })
        .unwrap();
    }

    fn evaluate(
        &self,
        candidates: Vec<JobId>,
        budget: u32,
        continuation: Option<EvalContinuation>,
    ) -> EvalPage {
        evaluate_pending(
            &self.database,
            EvalRequest {
                candidates,
                budget: NonZeroU32::new(budget).unwrap(),
                continuation,
            },
        )
        .unwrap()
    }
}

fn pool_profile(limit: u32) -> NewProfile {
    NewProfile {
        name: "capacity".into(),
        config: NewProfileConfig::ExecutionPool(ExecutionPoolConfig {
            concurrency_limit: NonZeroU32::new(limit).unwrap(),
            capabilities: CapabilitySet {
                operations: BTreeSet::from([ExecutionOperation::LlmRequest]),
                llm_protocols: BTreeSet::from([LlmTransport::OpenAiChatCompletions]),
            },
        }),
    }
}

#[test]
fn claim_is_atomic_duplicate_safe_and_checks_global_before_pool() {
    let fixture = Fixture::new("capacity", 16);
    let mut jobs = Vec::new();
    for _ in 0..17 {
        jobs.push(fixture.job(fixture.genesis, Vec::new()));
    }
    for (index, job) in jobs.iter().take(16).enumerate() {
        let (grant, marker) = fixture.claim(*job, BTreeSet::from([fixture.pool]));
        let grant = grant.unwrap();
        assert_eq!(grant.global_free_after, 15 - index as u8);
        assert_eq!(
            read_pool_binding(&fixture.database, *job).unwrap(),
            Some(grant.binding)
        );
        assert_eq!(
            read_marker(&fixture.database, marker)
                .unwrap()
                .unwrap()
                .kind,
            OperationKind::ClaimExecution
        );
    }
    let (refusal, operation) = fixture.claim(jobs[16], BTreeSet::from([fixture.pool]));
    assert_eq!(
        refusal,
        Err(ClaimRefusal::NoCapacity(CapacityScope::Global))
    );
    assert!(read_marker(&fixture.database, operation).unwrap().is_none());
    assert!(
        read_job(&fixture.database, jobs[16])
            .unwrap()
            .unwrap()
            .started_at
            .is_none()
    );
    assert_eq!(
        fixture.claim(jobs[0], BTreeSet::from([fixture.pool])).0,
        Err(ClaimRefusal::NotPending)
    );
    assert_eq!(read_occupancy(&fixture.database).unwrap().len(), 16);
    assert_eq!(
        read_pending_jobs(&fixture.database, None, 100)
            .unwrap()
            .visited
            .len(),
        1
    );
}

#[test]
fn allowed_pools_are_exhaustive_and_legacy_starts_never_bind() {
    let fixture = Fixture::new("allowed", 1);
    let (other, _) = fixture
        .write(OperationKind::CreateExecutionPool, |batch| {
            let (profile, _) = persistence::create_profile(batch, pool_profile(1))?;
            persistence::create_execution_pool(
                batch,
                NewExecutionPool {
                    name: "other".into(),
                    profile,
                    enabled: true,
                },
            )
        })
        .map(|id| (id, ()))
        .unwrap();
    let first = fixture.job(fixture.genesis, Vec::new());
    let second = fixture.job(fixture.genesis, Vec::new());
    fixture
        .claim(first, BTreeSet::from([fixture.pool]))
        .0
        .unwrap();
    assert_eq!(
        fixture.claim(second, BTreeSet::from([fixture.pool])).0,
        Err(ClaimRefusal::NoCapacity(CapacityScope::Pools(
            BTreeSet::from([fixture.pool])
        )))
    );
    assert_eq!(
        fixture.claim(second, BTreeSet::new()).0,
        Err(ClaimRefusal::EmptyAllowedPools)
    );
    assert_eq!(
        fixture
            .claim(second, BTreeSet::from([other]))
            .0
            .unwrap()
            .binding
            .pool,
        other
    );
    let legacy = fixture.job(fixture.genesis, Vec::new());
    fixture
        .write(OperationKind::StartLocalJob, |batch| {
            persistence::start_local_job(batch, legacy, fixture.epoch)
        })
        .unwrap();
    assert!(
        read_pool_binding(&fixture.database, legacy)
            .unwrap()
            .is_none()
    );
    assert_eq!(read_occupancy(&fixture.database).unwrap().len(), 2);
}

#[test]
fn unknown_holds_own_occupancy_and_frozen_profile_until_stop_even_with_live_child() {
    let fixture = Fixture::new("unknown", 16);
    let parent = fixture.job(fixture.genesis, Vec::new());
    let child = fixture.job(parent, Vec::new());
    fixture
        .claim(parent, BTreeSet::from([fixture.pool]))
        .0
        .unwrap();
    fixture
        .claim(child, BTreeSet::from([fixture.pool]))
        .0
        .unwrap();
    fixture.unknown(parent);
    assert_eq!(read_occupancy(&fixture.database).unwrap().len(), 2);
    assert!(matches!(
        fixture.write(OperationKind::UpdateProfile, |batch| {
            persistence::update_profile(batch, fixture.profile, pool_profile(2))
        }),
        Err(BatchFailure::Rejected(Violation::ProfileLocked { .. }))
    ));
    fixture.stop(parent);
    assert_eq!(
        read_occupancy(&fixture.database).unwrap(),
        vec![(fixture.pool, child)]
    );
    let dependent = fixture.job(
        fixture.genesis,
        vec![(parent, DependencyCondition::Settled)],
    );
    assert_eq!(
        fixture.evaluate(vec![dependent], 32_768, None).verdicts[0].1,
        Readiness::Blocked(Block::DependencyWaiting)
    );
    fixture.unknown(child);
    fixture.stop(child);
    assert!(matches!(
        fixture.evaluate(vec![dependent], 32_768, None).verdicts[0].1,
        Readiness::Ready { .. }
    ));
    fixture
        .write(OperationKind::UpdateProfile, |batch| {
            persistence::update_profile(batch, fixture.profile, pool_profile(2))
        })
        .unwrap();
    assert_eq!(
        read_pool_binding(&fixture.database, parent)
            .unwrap()
            .unwrap()
            .pool_profile,
        fixture.profile
    );
}

#[test]
fn lowering_current_pool_limit_does_not_preempt_or_rebind_old_profile() {
    let fixture = Fixture::new("lower", 16);
    let first = fixture.job(fixture.genesis, Vec::new());
    let second = fixture.job(fixture.genesis, Vec::new());
    fixture
        .claim(first, BTreeSet::from([fixture.pool]))
        .0
        .unwrap();
    fixture
        .claim(second, BTreeSet::from([fixture.pool]))
        .0
        .unwrap();
    let replacement = fixture
        .write(OperationKind::CreateProfile, |batch| {
            persistence::create_profile(batch, pool_profile(1))
        })
        .unwrap();
    fixture
        .write(OperationKind::SetExecutionPoolProfile, |batch| {
            persistence::set_execution_pool_profile(batch, fixture.pool, replacement)
        })
        .unwrap();
    assert_eq!(read_occupancy(&fixture.database).unwrap().len(), 2);
    assert_eq!(
        read_pool_binding(&fixture.database, first)
            .unwrap()
            .unwrap()
            .pool_profile,
        fixture.profile
    );
    let next = fixture.job(fixture.genesis, Vec::new());
    assert!(matches!(
        fixture.claim(next, BTreeSet::from([fixture.pool])).0,
        Err(ClaimRefusal::NoCapacity(CapacityScope::Pools(_)))
    ));
    fixture
        .write(OperationKind::SetExecutionPoolEnabled, |batch| {
            persistence::set_execution_pool_enabled(batch, fixture.pool, false)
        })
        .unwrap();
    assert_eq!(read_occupancy(&fixture.database).unwrap().len(), 2);
    assert_eq!(
        fixture.claim(next, BTreeSet::from([fixture.pool])).0,
        Err(ClaimRefusal::NoCompatiblePool)
    );
}

#[test]
fn aborted_claim_leaves_no_binding_occupancy_start_or_marker() {
    let fixture = Fixture::new("rollback", 16);
    let job = fixture.job(fixture.genesis, Vec::new());
    let operation = fixture.operation();
    let outcome = run_batch(
        &fixture.database,
        operation,
        OperationKind::ClaimExecution,
        |batch| {
            batch
                .claim_execution(
                    ClaimRequest {
                        job,
                        allowed_pools: BTreeSet::from([fixture.pool]),
                    },
                    fixture.epoch,
                )?
                .unwrap();
            batch.reject::<()>(Violation::InvalidValue("test", "rollback"))?;
            Ok(((), MarkerSubjects::addressed(RecordRef::Job(job))))
        },
    );
    assert!(matches!(outcome.result, Err(BatchFailure::Rejected(_))));
    assert!(read_pool_binding(&fixture.database, job).unwrap().is_none());
    assert!(read_occupancy(&fixture.database).unwrap().is_empty());
    assert!(
        read_job(&fixture.database, job)
            .unwrap()
            .unwrap()
            .started_at
            .is_none()
    );
    assert!(read_marker(&fixture.database, operation).unwrap().is_none());
}

#[test]
fn rebuild_matches_maintained_indexes_and_does_not_use_local_claim_epoch() {
    let fixture = Fixture::new("rebuild", 16);
    let first = fixture.job(fixture.genesis, Vec::new());
    let second = fixture.job(fixture.genesis, Vec::new());
    fixture
        .claim(first, BTreeSet::from([fixture.pool]))
        .0
        .unwrap();
    fixture
        .claim(second, BTreeSet::from([fixture.pool]))
        .0
        .unwrap();
    fixture.unknown(first);
    fixture.unknown(second);
    fixture.stop(second);
    let expected = read_occupancy(&fixture.database).unwrap();
    let transaction = fixture.database.begin_write().unwrap();
    transaction
        .open_table(OCCUPANCY)
        .unwrap()
        .remove((fixture.pool.get(), first.get()))
        .unwrap();
    transaction
        .open_table(LOCAL_EXECUTIONS)
        .unwrap()
        .remove(first.get())
        .unwrap();
    transaction
        .open_table(ASSIGNED_POOL)
        .unwrap()
        .remove(first.get())
        .unwrap();
    transaction.commit().unwrap();
    let Fixture {
        database,
        root,
        path,
        ..
    } = fixture;
    drop(database);
    let reopened = open(&path).ok().unwrap();
    assert_eq!(read_occupancy(&reopened.database).unwrap(), expected);
    assert_eq!(
        read_pool_binding(&reopened.database, first)
            .unwrap()
            .unwrap()
            .pool,
        ExecutionPoolId::new(1)
    );
    assert_eq!(
        reopened
            .database
            .begin_read()
            .unwrap()
            .open_table(ASSIGNED_POOL)
            .unwrap()
            .get(first.get())
            .unwrap()
            .unwrap()
            .value(),
        1
    );
    drop(reopened);
    drop(root);
}

#[test]
fn unexecuted_committed_claim_reconciliation_checks_marker_and_quarantines_without_spawn() {
    let fixture = Fixture::new("unexecuted", 16);
    let job = fixture.job(fixture.genesis, Vec::new());
    let (_, operation) = fixture.claim(job, BTreeSet::from([fixture.pool]));
    assert!(matches!(
        fixture.write(OperationKind::QuarantineAbandonedJob, |batch| {
            batch
                .reconcile_unexecuted_claim(fixture.genesis, operation)
                .map(|value| value.unwrap())
        }),
        Err(BatchFailure::Rejected(_))
    ));
    fixture
        .write(OperationKind::QuarantineAbandonedJob, |batch| {
            batch
                .reconcile_unexecuted_claim(job, operation)
                .map(|value| value.unwrap())
        })
        .unwrap();
    // The commit is what it says: the Job started in an opening that is gone,
    // its outcome is unknown, and its stop is not confirmed because nothing
    // established that the external work it began has ended.
    assert_eq!(
        read_occupancy(&fixture.database).unwrap(),
        vec![(fixture.pool, job)],
        "the slot stays occupied and its capacity is quarantined"
    );
    let quarantined = read_unresolved_executions(&fixture.database).unwrap();
    assert_eq!(quarantined.len(), 1);
    assert_eq!(quarantined[0].0, job);
    assert_eq!(quarantined[0].1.pool, Some(fixture.pool));
    let row = read_job(&fixture.database, job).unwrap().unwrap();
    assert!(row.unknown_at.is_some() && row.stop_confirmed_at.is_none());
    assert!(read_pool_binding(&fixture.database, job).unwrap().is_some());
    // The quarantine is what a later opening reads, and it is the only thing
    // that refuses the release: no stop can be confirmed for it.
    assert!(matches!(
        fixture.write(OperationKind::ConfirmJobStopped, |batch| {
            persistence::confirm_job_stopped(batch, job)
        }),
        Err(BatchFailure::Rejected(Violation::StopUnresolved(released)))
            if released == job
    ));
    // Reconciling the same claim again changes nothing: the quarantine is
    // written once, so a repeated restart cannot weaken it.
    assert_eq!(
        run_batch_declinable(
            &fixture.database,
            fixture.operation(),
            OperationKind::QuarantineAbandonedJob,
            Commit::Durable,
            |batch| batch.reconcile_unexecuted_claim(job, operation),
        )
        .result
        .unwrap(),
        None
    );
}

/// Writes a quarantine record straight into the table, so a durable state that
/// no correct writer could produce can be reconstructed from.
fn forge_quarantine(fixture: &Fixture, job: JobId, record: UnresolvedExecution) {
    fixture
        .write(OperationKind::QuarantineAbandonedJob, |batch| {
            batch.step(|tx| {
                tx.open_table(UNRESOLVED_EXECUTIONS)?
                    .insert(job.get(), record.encode().as_slice())?;
                Ok(())
            })?;
            Ok(((), MarkerSubjects::addressed(RecordRef::Job(job))))
        })
        .unwrap();
}

fn quarantine_record(epoch: u64, pool: Option<ExecutionPoolId>) -> UnresolvedExecution {
    UnresolvedExecution {
        epoch,
        unresolved_at: Timestamp::now(),
        pool,
    }
}

/// Closes `fixture` and reopens the same file, which is where a quarantine
/// that cannot be reconciled with the capacity it claims must be refused.
fn reopen_refused(fixture: Fixture, name: &str, expected: &str) {
    let Fixture {
        database,
        root,
        path,
        ..
    } = fixture;
    drop(database);
    let detail = match open(&path).err().expect("the open must fail closed") {
        OpenError::Storage(StorageError { detail, .. }) => detail,
        _ => panic!("{name}: a storage failure, not a format verdict"),
    };
    assert!(detail.contains(expected), "{name}: {detail}");
    drop(root);
}

/// Unattributed quarantine above the global limit is a saturated state, not
/// damage.
///
/// Every unclean exit leaves behind an unbound abandoned Job, which holds a
/// global slot and has no release path without termination evidence. Enough of
/// them take the total past [`GLOBAL_EXECUTION_LIMIT`]. That must not refuse the
/// database: the condition is ordinary, it is permanent by design, and refusing
/// to open turns it into something no operator can get out of. Admission
/// refuses on its own at or above the limit, so holding the capacity and
/// starting are both possible at once.
#[test]
fn unattributed_quarantine_past_the_global_limit_still_opens_and_refuses_claims() {
    let beyond = usize::from(GLOBAL_EXECUTION_LIMIT) + 1;
    let fixture = Fixture::new("quarantine-over-limit", 16);
    // Unbound Jobs, so none of them holds a Pool's occupancy: each is charged
    // the global domain alone, exactly as an abandoned aggregation service Job
    // or log cleanup Job is.
    let abandoned: Vec<JobId> = (0..beyond)
        .map(|_| fixture.job(fixture.genesis, Vec::new()))
        .collect();
    for &job in &abandoned {
        fixture
            .write(OperationKind::QuarantineAbandonedJob, |batch| {
                batch.step(|tx| {
                    tx.open_table(LOCAL_EXECUTIONS)?
                        .insert(job.get(), fixture.epoch)?;
                    Ok(())
                })?;
                persistence::quarantine_abandoned_job(batch, job, fixture.epoch)
                    .map(|value| value.unwrap())
            })
            .unwrap();
    }
    assert_eq!(
        read_unresolved_executions(&fixture.database).unwrap().len(),
        beyond,
        "{beyond} unattributed quarantines: past the limit of {GLOBAL_EXECUTION_LIMIT}"
    );
    assert!(
        read_occupancy(&fixture.database).unwrap().is_empty(),
        "none of them holds a Pool, so none is charged twice"
    );

    // Admission is refused on its own terms, for the global domain, and every
    // record is still there.
    let candidate = fixture.job(fixture.genesis, Vec::new());
    let _ = fixture.claim(candidate, BTreeSet::from([fixture.pool]));
    let quarantined = read_unresolved_executions(&fixture.database).unwrap();
    let mut held: Vec<JobId> = quarantined.iter().map(|(job, _)| *job).collect();
    held.sort();
    let mut expected = abandoned.clone();
    expected.sort();
    assert_eq!(held, expected, "no record was dropped or merged");
    assert!(
        quarantined.iter().all(|(_, record)| record.pool.is_none()),
        "and each still names no Pool, so no Pool is over-quarantined"
    );

    // Reopening is the point: this is where the total used to be refused.
    let Fixture {
        database,
        root,
        path,
        ..
    } = fixture;
    drop(database);
    let reopened = match open(&path) {
        Ok(opened) => opened,
        Err(_) => panic!("an over-capacity quarantine must still open"),
    };
    let records = read_unresolved_executions(&reopened.database)
        .unwrap()
        .into_iter()
        .map(|(job, _)| job)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        records,
        expected.iter().copied().collect::<BTreeSet<_>>(),
        "every record survives the reopen"
    );
    assert!(
        read_occupancy(&reopened.database).unwrap().is_empty(),
        "and nothing was silently released into a Pool"
    );
    drop(reopened);
    drop(root);
}

/// A quarantine that cannot be reconciled with the capacity it claims to
/// protect refuses the open, rather than letting admission run on a protection
/// that was never confirmed. Over-limit totals are the one disagreement this
/// does not refuse, which the test above pins; these are the ones it does.
#[test]
fn a_quarantine_that_cannot_be_reconstructed_refuses_to_open() {
    // (a) A domain no frozen binding establishes: the record would quarantine
    // a Pool the Job never held, while the occupancy it should be protecting
    // is charged to a different one.
    let mismatch = Fixture::new("quarantine-mismatch", 16);
    let claimed = mismatch.job(mismatch.genesis, Vec::new());
    let _ = mismatch.claim(claimed, BTreeSet::from([mismatch.pool]));
    let elsewhere = ExecutionPoolId::new(mismatch.pool.get() + 1_000);
    forge_quarantine(&mismatch, claimed, quarantine_record(1, Some(elsewhere)));
    reopen_refused(mismatch, "quarantine-mismatch", "bound to");

    // (b) An entry beside a confirmed stop: capacity that has already been
    // handed back must not also be claimed as still held.
    let confirmed = Fixture::new("quarantine-confirmed", 16);
    let job = confirmed.job(confirmed.genesis, Vec::new());
    let _ = confirmed.claim(job, BTreeSet::from([confirmed.pool]));
    confirmed
        .write(OperationKind::FinalizeJob, |batch| {
            let now = batch.now();
            let mut row = batch.require_job(job)?;
            row.unknown_at = Some(now);
            row.stop_confirmed_at = Some(now);
            row.updated_at = now;
            batch.put_job(&row)?;
            Ok(((), MarkerSubjects::addressed(RecordRef::Job(job))))
        })
        .unwrap();
    forge_quarantine(&confirmed, job, quarantine_record(1, Some(confirmed.pool)));
    reopen_refused(confirmed, "quarantine-confirmed", "confirmed stopped");

    // (c) A Job that does not exist.
    let ghost = Fixture::new("quarantine-missing", 16);
    let missing = JobId::new(9_999);
    forge_quarantine(&ghost, missing, quarantine_record(1, None));
    reopen_refused(ghost, "quarantine-missing", "missing Job");

    // (d) A Job that already holds a Pool's occupancy but is quarantined with
    // no domain: the same occupied resource counted twice, which would hide a
    // slot behind a claim that names none.
    let twice = Fixture::new("quarantine-twice", 16);
    let claimed = twice.job(twice.genesis, Vec::new());
    let _ = twice.claim(claimed, BTreeSet::from([twice.pool]));
    forge_quarantine(&twice, claimed, quarantine_record(1, None));
    reopen_refused(twice, "quarantine-twice", "no quarantined domain");

    // A quarantine that reconstructs cleanly still opens, so the refusals
    // above are about the disagreement and not about the table existing.
    let sound = Fixture::new("quarantine-sound", 16);
    let claimed = sound.job(sound.genesis, Vec::new());
    let _ = sound.claim(claimed, BTreeSet::from([sound.pool]));
    forge_quarantine(&sound, claimed, quarantine_record(1, Some(sound.pool)));
    let Fixture {
        database,
        root,
        path,
        ..
    } = sound;
    drop(database);
    assert!(open(&path).is_ok(), "an agreeing quarantine opens");
    drop(root);
}

/// A quarantined execution is released by no assertion, only by evidence, and
/// a confirmed release is idempotent and touches nothing else.
#[test]
fn a_confirmed_stop_releases_once_and_a_quarantined_one_is_never_released() {
    let fixture = Fixture::new("release", 16);
    let live = fixture.job(fixture.genesis, Vec::new());
    let quarantined = fixture.job(fixture.genesis, Vec::new());
    let _ = fixture.claim(live, BTreeSet::from([fixture.pool]));
    let _ = fixture.claim(quarantined, BTreeSet::from([fixture.pool]));
    // Both Jobs are finalized unknown by the trusted path that ran them, so
    // neither is abandoned: one may confirm its stop, and one may not.
    for job in [live, quarantined] {
        fixture
            .write(OperationKind::FinalizeJob, |batch| {
                persistence::finalize_job(batch, job, JobFinalization::Unknown)
            })
            .unwrap();
    }
    // The second is then made abandoned, as a restart would find it.
    fixture
        .write(OperationKind::QuarantineAbandonedJob, |batch| {
            batch.step(|tx| {
                tx.open_table(LOCAL_EXECUTIONS)?
                    .insert(quarantined.get(), fixture.epoch)?;
                Ok(())
            })?;
            persistence::quarantine_abandoned_job(batch, quarantined, fixture.epoch)
                .map(|value| value.unwrap())
        })
        .unwrap();

    assert_eq!(
        read_occupancy(&fixture.database).unwrap(),
        vec![(fixture.pool, live), (fixture.pool, quarantined)]
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>(),
        "an unknown outcome holds its occupancy until its stop is confirmed"
    );

    // The quarantined one is refused, however many times it is asked.
    for _ in 0..2 {
        assert!(matches!(
            fixture.write(OperationKind::ConfirmJobStopped, |batch| {
                persistence::confirm_job_stopped(batch, quarantined)
            }),
            Err(BatchFailure::Rejected(Violation::StopUnresolved(job)))
                if job == quarantined
        ));
    }
    assert!(
        read_unresolved_executions(&fixture.database)
            .unwrap()
            .iter()
            .any(|(job, _)| *job == quarantined)
    );

    // The live one confirms once and releases exactly its own occupancy.
    fixture
        .write(OperationKind::ConfirmJobStopped, |batch| {
            persistence::confirm_job_stopped(batch, live)
        })
        .unwrap();
    let once = read_occupancy(&fixture.database).unwrap();
    assert_eq!(once, vec![(fixture.pool, quarantined)]);
    // Confirming again is refused as already confirmed rather than freeing
    // anything a second time.
    assert!(matches!(
        fixture.write(OperationKind::ConfirmJobStopped, |batch| {
            persistence::confirm_job_stopped(batch, live)
        }),
        Err(BatchFailure::Rejected(Violation::StopAlreadyConfirmed(job))) if job == live
    ));
    assert_eq!(read_occupancy(&fixture.database).unwrap(), once);
}

#[test]
fn fenced_pages_are_bounded_exclusive_and_snapshot_complete_at_exact_budget() {
    let fixture = Fixture::new("pages", 16);
    let jobs: Vec<_> = (0..3)
        .map(|_| fixture.job(fixture.genesis, Vec::new()))
        .collect();
    let first = read_pending_capture_page(&fixture.database, None, NonZeroUsize::new(2).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(first.fence.len, 3);
    assert_eq!(first.fence.high_water, jobs[2]);
    assert_eq!(first.page.visited.len(), 2);
    assert!(!first.page.exhausted);
    let last = read_pending_capture_page(
        &fixture.database,
        first.page.last_visited,
        NonZeroUsize::new(2).unwrap(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(last.page.visited.len(), 1);
    let end = read_pending_capture_page(
        &fixture.database,
        last.page.last_visited,
        NonZeroUsize::new(2).unwrap(),
    )
    .unwrap()
    .unwrap();
    assert!(end.page.visited.is_empty() && end.page.exhausted);
    assert_eq!(end.page.last_visited, last.page.last_visited);
    let exact = read_pending_snapshot(&fixture.database, NonZeroUsize::new(3).unwrap())
        .unwrap()
        .unwrap();
    assert!(exact.complete);
    let truncated = read_pending_snapshot(&fixture.database, NonZeroUsize::new(2).unwrap())
        .unwrap()
        .unwrap();
    assert!(!truncated.complete);
    assert_eq!(truncated.keys.len(), 2);
    assert_eq!(exact.fence, first.fence);
}

#[test]
fn typed_first_page_index_fault_keeps_fence_and_keys_fallback_works() {
    let fixture = Fixture::new("fault", 16);
    let job = fixture.job(fixture.genesis, Vec::new());
    fixture.job(fixture.genesis, Vec::new());
    let transaction = fixture.database.begin_write().unwrap();
    let key = JobRow::decode(
        transaction
            .open_table(JOBS)
            .unwrap()
            .get(job.get())
            .unwrap()
            .unwrap()
            .value(),
    )
    .unwrap()
    .pending_key();
    transaction
        .open_table(JOBS)
        .unwrap()
        .remove(job.get())
        .unwrap();
    transaction.commit().unwrap();
    let fault = read_pending_capture_page(&fixture.database, None, NonZeroUsize::MIN)
        .unwrap()
        .unwrap_err();
    assert!(matches!(
        fault,
        PendingReadFault::IndexEntry {
            fence: PendingFence { len: 2, .. },
            kind: PendingIndexFault::MissingJob,
            ..
        }
    ));
    let keys = read_pending_keys(&fixture.database, None, NonZeroUsize::MIN)
        .unwrap()
        .unwrap();
    assert_eq!(keys.page.visited[0].job, job);
    assert_eq!(keys.page.visited[0].created_at.as_unix_micros(), key.0);
    assert!(
        !read_pending_snapshot(&fixture.database, NonZeroUsize::MIN)
            .unwrap()
            .unwrap()
            .complete
    );
    assert_eq!(
        fixture.evaluate(vec![job], 32_768, None).verdicts[0].1,
        Readiness::Indeterminate(Why::MissingJob)
    );
    let transaction = fixture.database.begin_write().unwrap();
    transaction
        .open_table(SEQUENCES)
        .unwrap()
        .remove(Sequence::Job.key())
        .unwrap();
    transaction.commit().unwrap();
    assert_eq!(
        read_pending_capture_page(&fixture.database, None, NonZeroUsize::MIN)
            .unwrap_err()
            .kind,
        StorageErrorKind::Corrupted
    );
}

#[test]
fn budget_continuation_makes_monotone_progress_and_shared_prerequisites_are_memoized() {
    let fixture = Fixture::new("budget", 16);
    let prerequisites: Vec<_> = (0..4)
        .map(|_| fixture.job(fixture.genesis, Vec::new()))
        .collect();
    for job in &prerequisites {
        fixture.unknown(*job);
        fixture.stop(*job);
    }
    let candidate = fixture.job(
        fixture.genesis,
        prerequisites
            .iter()
            .map(|job| (*job, DependencyCondition::Settled))
            .collect(),
    );
    let mut continuation = None;
    let mut previous = JobId::new(0);
    let mut requests = 0;
    loop {
        let page = fixture.evaluate(vec![candidate], 1, continuation);
        requests += 1;
        assert!(page.work_used <= 1 + 30_005 + 8);
        match &page.verdicts[0].1 {
            Readiness::BudgetExhausted => {
                let cursor = page.continuation.unwrap();
                assert!(cursor.after_prerequisite > previous);
                previous = cursor.after_prerequisite;
                continuation = Some(cursor);
            }
            Readiness::Ready { .. } => break,
            verdict => panic!("unexpected {verdict:?}"),
        }
        assert!(requests <= prerequisites.len());
    }
    assert_eq!(requests, prerequisites.len());
    let other = fixture.job(
        fixture.genesis,
        vec![(prerequisites[0], DependencyCondition::Settled)],
    );
    let page = fixture.evaluate(vec![candidate, other], 32_768, None);
    assert_eq!(page.verdicts.len(), 2);
    let one = fixture.evaluate(vec![candidate], 32_768, None);
    let two = fixture.evaluate(vec![other], 32_768, None);
    assert!(page.work_used < one.work_used + two.work_used);
}

#[test]
fn pool_projection_tracks_profile_edits_and_disable_without_per_pool_reads() {
    let fixture = Fixture::new("projection", 16);
    let job = fixture.job(fixture.genesis, Vec::new());
    let baseline = fixture.evaluate(vec![job], 32_768, None);
    for index in 0..80 {
        fixture
            .write(OperationKind::CreateExecutionPool, |batch| {
                persistence::create_execution_pool(
                    batch,
                    NewExecutionPool {
                        name: format!("p{index}"),
                        profile: fixture.profile,
                        enabled: true,
                    },
                )
            })
            .unwrap();
    }
    let page = fixture.evaluate(vec![job], 32_768, None);
    assert_eq!(page.work_used, baseline.work_used);
    assert!(
        matches!(&page.verdicts[0].1, Readiness::Ready { compatible_pools } if compatible_pools.len() == 81)
    );
    let mut incompatible = pool_profile(16);
    if let NewProfileConfig::ExecutionPool(config) = &mut incompatible.config {
        config.capabilities.llm_protocols.clear();
    }
    fixture
        .write(OperationKind::UpdateProfile, |batch| {
            persistence::update_profile(batch, fixture.profile, incompatible)
        })
        .unwrap();
    assert_eq!(
        fixture.evaluate(vec![job], 32_768, None).verdicts[0].1,
        Readiness::Blocked(Block::NoCompatiblePool)
    );
}

#[test]
fn unbounded_dependency_is_distinct_from_budget_and_claim_refuses() {
    let fixture = Fixture::new("unbounded", 16);
    // Build the historical shape directly: each stopped Unknown ancestor still
    // has a locally stopped Unknown child, so certification cannot short-cut it.
    let root = fixture.job(fixture.genesis, Vec::new());
    let descendants: Vec<_> = (0..65)
        .map(|_| fixture.job(fixture.genesis, Vec::new()))
        .collect();
    let candidate = fixture.job(fixture.genesis, vec![(root, DependencyCondition::Settled)]);
    let transaction = fixture.database.begin_write().unwrap();
    let mut jobs = transaction.open_table(JOBS).unwrap();
    let mut parent = root;
    for child in descendants {
        transaction
            .open_table(CHILDREN)
            .unwrap()
            .remove((fixture.genesis.get(), child.get()))
            .unwrap();
        transaction
            .open_table(CHILDREN)
            .unwrap()
            .insert((parent.get(), child.get()), ())
            .unwrap();
        transaction
            .open_table(CHILD_OF)
            .unwrap()
            .insert(child.get(), parent.get())
            .unwrap();
        parent = child;
    }
    let rows: Vec<_> = jobs
        .iter()
        .unwrap()
        .map(|entry| JobRow::decode(entry.unwrap().1.value()).unwrap())
        .collect();
    for mut row in rows {
        if row.id != fixture.genesis && row.id != candidate {
            row.unknown_at = Some(Timestamp::now());
            row.stop_confirmed_at = Some(Timestamp::now());
            jobs.insert(row.id.get(), row.encode().as_slice()).unwrap();
        }
    }
    drop(jobs);
    transaction.commit().unwrap();
    let page = fixture.evaluate(vec![candidate], 1, None);
    assert_eq!(page.verdicts[0].1, Readiness::Indeterminate(Why::Unbounded));
    assert!(page.continuation.is_none());
    assert!(page.work_used <= 1 + 30_005 + 8);
    assert_eq!(
        fixture.claim(candidate, BTreeSet::from([fixture.pool])).0,
        Err(ClaimRefusal::Unbounded { prerequisite: root })
    );
}
