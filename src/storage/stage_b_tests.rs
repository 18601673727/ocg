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
fn unexecuted_committed_claim_reconciliation_checks_marker_and_stops_without_spawn() {
    let fixture = Fixture::new("unexecuted", 16);
    let job = fixture.job(fixture.genesis, Vec::new());
    let (_, operation) = fixture.claim(job, BTreeSet::from([fixture.pool]));
    assert!(matches!(
        fixture.write(OperationKind::ReconcileAbandonedJob, |batch| {
            batch
                .reconcile_unexecuted_claim(fixture.genesis, operation)
                .map(|value| value.unwrap())
        }),
        Err(BatchFailure::Rejected(_))
    ));
    fixture
        .write(OperationKind::ReconcileAbandonedJob, |batch| {
            batch
                .reconcile_unexecuted_claim(job, operation)
                .map(|value| value.unwrap())
        })
        .unwrap();
    assert!(read_occupancy(&fixture.database).unwrap().is_empty());
    let row = read_job(&fixture.database, job).unwrap().unwrap();
    assert!(row.unknown_at.is_some() && row.stop_confirmed_at.is_some());
    assert!(read_pool_binding(&fixture.database, job).unwrap().is_some());
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
