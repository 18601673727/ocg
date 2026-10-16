use super::*;

impl Batch {
    // -- relationships --

    pub(super) fn has_pair(
        &mut self,
        table: PairSet,
        from: u64,
        to: u64,
    ) -> Result<bool, BatchError> {
        self.step(|tx| Ok(tx.open_table(table)?.get((from, to))?.is_some()))
    }

    /// Inserts `(from, to)` into a pair set without an incoming index.
    /// Returns false, writing nothing, when the pair exists.
    pub(super) fn insert_pair(
        &mut self,
        table: PairSet,
        from: u64,
        to: u64,
    ) -> Result<bool, BatchError> {
        self.step(|tx| {
            let mut table = tx.open_table(table)?;
            if table.get((from, to))?.is_some() {
                return Ok(false);
            }
            table.insert((from, to), ())?;
            Ok(true)
        })
    }

    /// Inserts `(from, to)` into a pair set and `(to, from)` into its
    /// incoming index. Returns false, writing nothing, when the pair exists.
    pub(super) fn insert_indexed_pair(
        &mut self,
        forward: PairSet,
        incoming: PairSet,
        from: u64,
        to: u64,
    ) -> Result<bool, BatchError> {
        self.step(|tx| {
            let mut table = tx.open_table(forward)?;
            if table.get((from, to))?.is_some() {
                return Ok(false);
            }
            table.insert((from, to), ())?;
            drop(table);
            tx.open_table(incoming)?.insert((to, from), ())?;
            Ok(true)
        })
    }

    /// Sets `from -> to` in a single-valued link and `(to, from)` in its
    /// incoming index. Returns false, writing nothing, when `from` is linked.
    pub(super) fn insert_indexed_link(
        &mut self,
        forward: Link,
        incoming: PairSet,
        from: u64,
        to: u64,
    ) -> Result<bool, BatchError> {
        self.step(|tx| {
            let mut table = tx.open_table(forward)?;
            if table.get(from)?.is_some() {
                return Ok(false);
            }
            table.insert(from, to)?;
            drop(table);
            tx.open_table(incoming)?.insert((to, from), ())?;
            Ok(true)
        })
    }

    pub(super) fn link_target(
        &mut self,
        table: Link,
        from: u64,
    ) -> Result<Option<u64>, BatchError> {
        self.step(|tx| Ok(tx.open_table(table)?.get(from)?.map(|v| v.value())))
    }

    /// Whether any Job has been submitted with the input.
    pub(crate) fn input_has_jobs(&mut self, input: JobInputId) -> Result<bool, BatchError> {
        self.step(|tx| {
            Ok(tx
                .open_table(INPUT_JOBS)?
                .range((input.get(), 0)..=(input.get(), u64::MAX))?
                .next()
                .is_some())
        })
    }

    /// The input a stored Job was submitted with.
    pub(crate) fn input_of(&mut self, job: JobId) -> Result<JobInputId, BatchError> {
        match self.link_target(HAS_INPUT, job.get())? {
            Some(input) => Ok(JobInputId::new(input)),
            None => self.corrupted(format!("{job} has no input")),
        }
    }

    /// HAS_INPUT: one input per Job; an input may serve several Jobs.
    pub(crate) fn link_input(&mut self, job: JobId, input: JobInputId) -> Result<(), BatchError> {
        self.require_job(job)?;
        self.require_input(input)?;
        if self.insert_indexed_link(HAS_INPUT, INPUT_JOBS, job.get(), input.get())? {
            Ok(())
        } else {
            self.reject(Violation::DuplicateId(RecordRef::Job(job)))
        }
    }

    /// The parent of a stored Job; the Genesis Job is its own.
    pub(crate) fn parent_of(&mut self, job: JobId) -> Result<JobId, BatchError> {
        match self.link_target(CHILD_OF, job.get())? {
            Some(parent) => Ok(JobId::new(parent)),
            None => self.corrupted(format!("{job} has no parent")),
        }
    }

    /// CHILD_OF: exactly one existing parent per Job, fixed once written,
    /// never the child itself, and never a descendant of the child. The
    /// parent's ancestor chain must reach the Genesis Job.
    pub(crate) fn link_child(&mut self, child: JobId, parent: JobId) -> Result<(), BatchError> {
        if child == parent {
            return self.reject(Violation::SelfParent(child));
        }
        self.require_job(child)?;
        self.require_job(parent)?;
        let genesis = self.genesis()?;
        let mut ancestor = parent;
        let mut visited = HashSet::new();
        loop {
            if ancestor == child {
                return self.reject(Violation::ParentCycle { child, parent });
            }
            if ancestor == genesis {
                break;
            }
            if !visited.insert(ancestor) {
                return self.corrupted(format!(
                    "the ancestors of {parent} loop without reaching the Genesis Job"
                ));
            }
            ancestor = self.parent_of(ancestor)?;
        }
        if self.insert_indexed_link(CHILD_OF, CHILDREN, child.get(), parent.get())? {
            Ok(())
        } else {
            self.reject(Violation::ParentFixed(child))
        }
    }

    /// Ordinary children only: the Genesis root marker is never listed.
    pub(crate) fn children(&mut self, parent: JobId) -> Result<Vec<JobId>, BatchError> {
        self.step(|tx| {
            Ok(
                pair_targets(&tx.open_table(CHILDREN)?, parent.get(), None, usize::MAX)?
                    .into_iter()
                    .map(JobId::new)
                    .collect(),
            )
        })
    }

    /// The Job's explicit prerequisites with their conditions, in Job order.
    pub(crate) fn dependencies(
        &mut self,
        job: JobId,
    ) -> Result<Vec<(JobId, DependencyCondition)>, BatchError> {
        self.step(|tx| {
            let mut dependencies = Vec::new();
            for entry in tx
                .open_table(DEPENDS_ON)?
                .range((job.get(), 0)..=(job.get(), u64::MAX))?
            {
                let (key, condition) = entry?;
                let condition = DependencyCondition::from_code(condition.value())
                    .ok_or_else(|| StorageError::corrupted("invalid dependency condition"))?;
                dependencies.push((JobId::new(key.value().1), condition));
            }
            Ok(dependencies)
        })
    }

    /// Whether the Job and all its descendants have stopped, read inside
    /// this transaction. A Job that may still run answers without touching
    /// a table, and so does a confirmed outcome in a file whose open
    /// certified that none has a running descendant; otherwise the
    /// descendants are followed. A traversal that reaches its bound answers
    /// `false`, so every caller refuses rather than proceeds on a guess.
    pub(crate) fn subtree_settled(&mut self, job: &JobRow) -> Result<bool, BatchError> {
        let certified = self.closure_certified()?;
        if let Some(settlement) = Settlement::of_row(job, certified) {
            return Ok(settlement == Settlement::Settled);
        }
        self.step(|tx| {
            let tables = WaitingTables::open(tx, certified, None)?;
            Ok(tables.subtree_settlement(job)? == Settlement::Settled)
        })
    }

    /// Whether the file's open certified its confirmed outcomes, read once
    /// per batch.
    fn closure_certified(&mut self) -> Result<bool, BatchError> {
        if let Some(certified) = self.certified {
            return Ok(certified);
        }
        let certified = self.step(closure_certified)?;
        self.certified = Some(certified);
        Ok(certified)
    }

    /// The Jobs that directly retry `original`, in creation order.
    pub(crate) fn retries(&mut self, original: JobId) -> Result<Vec<JobId>, BatchError> {
        self.step(|tx| {
            Ok(
                pair_targets(&tx.open_table(RETRIES)?, original.get(), None, usize::MAX)?
                    .into_iter()
                    .map(JobId::new)
                    .collect(),
            )
        })
    }

    pub(crate) fn link_dependency(
        &mut self,
        job: JobId,
        prerequisite: JobId,
        condition: DependencyCondition,
    ) -> Result<(), BatchError> {
        self.require_job(job)?;
        self.require_job(prerequisite)?;
        let taken = self.step(|tx| {
            let mut t = tx.open_table(DEPENDS_ON)?;
            if t.get((job.get(), prerequisite.get()))?.is_some() {
                return Ok(true);
            }
            t.insert((job.get(), prerequisite.get()), condition.code())?;
            Ok(false)
        })?;
        if taken {
            self.reject(Violation::DuplicateDependency(prerequisite))
        } else {
            Ok(())
        }
    }

    /// LOCAL_EXECUTIONS: records that the opening of `epoch` started `job`
    /// as its own local execution.
    pub(crate) fn claim_local_execution(
        &mut self,
        job: JobId,
        epoch: u64,
    ) -> Result<(), BatchError> {
        self.step(|tx| {
            tx.open_table(LOCAL_EXECUTIONS)?.insert(job.get(), epoch)?;
            Ok(())
        })
    }

    /// The epoch of the opening that started `job` as its own local
    /// execution, while the Job is unsettled.
    pub(crate) fn local_execution(&mut self, job: JobId) -> Result<Option<u64>, BatchError> {
        self.step(|tx| {
            Ok(tx
                .open_table(LOCAL_EXECUTIONS)?
                .get(job.get())?
                .map(|v| v.value()))
        })
    }

    /// Removes the Job's local execution claim, if it has one.
    pub(crate) fn release_local_execution(&mut self, job: JobId) -> Result<(), BatchError> {
        self.step(|tx| {
            tx.open_table(LOCAL_EXECUTIONS)?.remove(job.get())?;
            Ok(())
        })
    }

    /// The submission recorded under `key`, if any.
    pub(crate) fn submission(
        &mut self,
        key: SubmissionKey,
    ) -> Result<Option<SubmissionRecord>, BatchError> {
        let bytes = self.step(|tx| {
            Ok(tx
                .open_table(SUBMISSIONS)?
                .get(key.as_bytes().as_slice())?
                .map(|v| v.value().to_vec()))
        })?;
        let record = bytes
            .map(|bytes| decode_submission(key.as_bytes(), &bytes))
            .transpose();
        self.decoded(record)
    }

    /// SUBMISSIONS: records a submission of a Job created in this batch. A
    /// key is never overwritten; the caller has checked that it is free.
    pub(crate) fn insert_submission(
        &mut self,
        record: &SubmissionRecord,
    ) -> Result<(), BatchError> {
        self.require_job(record.job)?;
        let bytes = encode_submission(record);
        let existed = self.step(|tx| {
            let mut table = tx.open_table(SUBMISSIONS)?;
            if table.get(record.key.as_bytes().as_slice())?.is_some() {
                return Ok(true);
            }
            table.insert(record.key.as_bytes().as_slice(), bytes.as_slice())?;
            Ok(false)
        })?;
        if existed {
            return self.corrupted(format!("{:?} was recorded twice", record.key));
        }
        Ok(())
    }

    /// ASSIGNED_TO: one per Job; replacing it is the caller's decision.
    pub(crate) fn set_assigned_executor(
        &mut self,
        job: JobId,
        executor: ExecutorId,
    ) -> Result<(), BatchError> {
        self.require_job(job)?;
        self.require_executor(executor)?;
        self.step(|tx| {
            tx.open_table(ASSIGNED_TO)?
                .insert(job.get(), executor.get())?;
            Ok(())
        })
    }

    /// PREVIOUS and REPLACES are fixed at creation and are not waiting edges.
    pub(crate) fn link_lineage(
        &mut self,
        job: JobId,
        previous: Option<JobId>,
        replaces: Option<JobId>,
    ) -> Result<(), BatchError> {
        for (table, target) in [(PREVIOUS, previous), (REPLACES, replaces)] {
            let Some(target) = target else { continue };
            self.require_job(target)?;
            let written = self.step(|tx| {
                let mut t = tx.open_table(table)?;
                if t.get(job.get())?.is_some() {
                    return Ok(false);
                }
                t.insert(job.get(), target.get())?;
                Ok(true)
            })?;
            if !written {
                return self.reject(Violation::DuplicateId(RecordRef::Job(job)));
            }
        }
        Ok(())
    }

    /// RETRY_OF: one original per retry Job, fixed at creation.
    /// `job` and every Job it retries through RETRY_OF, nearest first. A
    /// RETRY_OF edge to a missing Job, or one that leads back into the chain,
    /// is corruption: the chain is never cut short.
    pub(crate) fn retry_lineage(&mut self, job: JobId) -> Result<Vec<JobId>, BatchError> {
        let mut lineage = vec![job];
        let mut current = job;
        while let Some(original) = self.link_target(RETRY_OF, current.get())? {
            let original = JobId::new(original);
            if lineage.contains(&original) {
                return self.corrupted(format!("the retry lineage of {job} returns to {original}"));
            }
            if self.job(original)?.is_none() {
                return self.corrupted(format!("{current} retries the missing {original}"));
            }
            lineage.push(original);
            current = original;
        }
        Ok(lineage)
    }

    pub(crate) fn link_retry(&mut self, retry: JobId, original: JobId) -> Result<(), BatchError> {
        self.require_job(retry)?;
        self.require_job(original)?;
        if self.insert_indexed_link(RETRY_OF, RETRIES, retry.get(), original.get())? {
            Ok(())
        } else {
            self.reject(Violation::DuplicateId(RecordRef::Job(retry)))
        }
    }

    /// IN_PROJECT: any number of Projects per Job and Jobs per Project.
    pub(crate) fn link_project(
        &mut self,
        job: JobId,
        project: ProjectId,
    ) -> Result<(), BatchError> {
        self.require_job(job)?;
        self.require_project(project)?;
        if self.insert_indexed_pair(IN_PROJECT, PROJECT_JOBS, job.get(), project.get())? {
            Ok(())
        } else {
            self.reject(Violation::AlreadyInProject { job, project })
        }
    }

    /// USES_PROFILE: an Agent and one of its Agent Profiles.
    pub(crate) fn link_agent_profile(
        &mut self,
        agent: AgentId,
        profile: ProfileId,
    ) -> Result<(), BatchError> {
        self.require_exists(AGENTS, RecordRef::Agent(agent), agent.get())?;
        self.require_profile_kind(profile, ProfileKind::Agent)?;
        if self.insert_indexed_pair(USES_PROFILE, PROFILE_AGENTS, agent.get(), profile.get())? {
            Ok(())
        } else {
            self.reject(Violation::DuplicateReference(RecordRef::Profile(profile)))
        }
    }

    /// An LLM Profile's configuration and Provider as they are now.
    pub(crate) fn freeze_llm(&mut self, profile: ProfileId) -> Result<FrozenLlmConfig, BatchError> {
        let row = self.require_profile_row(profile)?;
        let ProfileConfig::Llm(config) = row.config else {
            return self.reject(
                WrongProfileKind {
                    profile,
                    expected: ProfileKind::Llm,
                    found: ProfileKind::Agent,
                }
                .into(),
            );
        };
        let Some(provider) = self.link_target(USES_PROVIDER, profile.get())? else {
            return self.corrupted(format!("{profile} has no Provider"));
        };
        let Some(bytes) = self.get_bytes(LLM_PROVIDERS, provider)? else {
            return self.corrupted(format!("{profile} names missing Provider {provider}"));
        };
        let decoded = decode_llm_provider(&bytes);
        let provider = self.decoded(decoded)?;
        Ok(FrozenLlmConfig {
            config,
            provider: provider_snapshot(provider),
        })
    }

    /// Validates a submission's configuration and reads the values it will
    /// execute with from its Profiles as they are now.
    /// No Agent configuration is read, or made up, for work submitted
    /// without an Agent Profile.
    pub(crate) fn freeze_configuration(
        &mut self,
        configuration: ExecutionConfiguration,
    ) -> Result<FrozenExecutionConfig, BatchError> {
        self.require_configuration(configuration)?;
        let agent = match configuration.agent_profile {
            Some(profile) => {
                let ProfileConfig::Agent(agent) = self.require_profile_row(profile)?.config else {
                    return self.corrupted(format!("{profile} changed kind"));
                };
                Some(agent)
            }
            None => None,
        };
        let llm = self.freeze_llm(configuration.llm_profile)?;
        Ok(FrozenExecutionConfig { agent, llm })
    }

    /// The LLM Profile a stored Job was submitted with, if any.
    pub(crate) fn job_llm_profile(&mut self, job: JobId) -> Result<Option<ProfileId>, BatchError> {
        Ok(self
            .link_target(RUNS_LLM_PROFILE, job.get())?
            .map(ProfileId::new))
    }

    /// The LLM configuration a Job's input froze for it, if the Job is configured.
    pub(crate) fn job_frozen_llm(
        &mut self,
        job: JobId,
    ) -> Result<Option<FrozenLlmConfig>, BatchError> {
        if self.job_llm_profile(job)?.is_none() {
            return Ok(None);
        }
        let input = self.input_of(job)?;
        match self.require_input(input)?.configuration {
            Some(frozen) => Ok(Some(frozen.llm)),
            None => self.corrupted(format!(
                "the configured {job} has no frozen configuration in {input}"
            )),
        }
    }

    /// The Jobs holding a Profile, see [`JobRow::holds_profiles`]. Returns at
    /// most `limit` in ID order, and whether there are more.
    pub(crate) fn profile_holders(
        &mut self,
        profile: ProfileId,
        limit: usize,
    ) -> Result<(Vec<JobId>, bool), BatchError> {
        let kind = self.require_profile_row(profile)?.config.kind();
        self.step(|tx| {
            profile_holders(
                &tx.open_table(profile_jobs_index(kind))?,
                &tx.open_table(JOBS)?,
                profile,
                limit,
            )
        })
    }

    /// RUNS_LLM_PROFILE and, when an Agent Profile is named, APPLIES_PROFILE:
    /// the configuration an execution child Job is submitted with. Refused
    /// for a Job that already has one, whatever the new Profiles are.
    pub(crate) fn link_configuration(
        &mut self,
        job: JobId,
        configuration: ExecutionConfiguration,
    ) -> Result<(), BatchError> {
        self.require_job(job)?;
        self.require_configuration(configuration)?;
        let bound = self.link_target(APPLIES_PROFILE, job.get())?.is_some()
            || self.link_target(RUNS_LLM_PROFILE, job.get())?.is_some();
        if bound {
            return self.reject(Violation::ConfigurationFixed(job));
        }
        if let Some(agent_profile) = configuration.agent_profile {
            self.insert_indexed_link(
                APPLIES_PROFILE,
                AGENT_PROFILE_JOBS,
                job.get(),
                agent_profile.get(),
            )?;
        }
        self.insert_indexed_link(
            RUNS_LLM_PROFILE,
            LLM_PROFILE_JOBS,
            job.get(),
            configuration.llm_profile.get(),
        )?;
        Ok(())
    }

    /// USES_PROVIDER: one LLM Provider per LLM Profile, fixed at creation.
    pub(super) fn link_profile_provider(
        &mut self,
        profile: ProfileId,
        provider: LlmProviderId,
    ) -> Result<(), BatchError> {
        self.require_profile_kind(profile, ProfileKind::Llm)?;
        self.require_llm_provider(provider)?;
        if self.insert_indexed_link(
            USES_PROVIDER,
            PROVIDER_PROFILES,
            profile.get(),
            provider.get(),
        )? {
            Ok(())
        } else {
            self.reject(Violation::DuplicateId(RecordRef::Profile(profile)))
        }
    }

    pub(super) fn write_marker(&mut self, marker: &OperationMarker) -> Result<(), BatchError> {
        let key = (marker.operation.epoch, marker.operation.seq);
        let bytes = encode_marker(marker);
        let existed = self.step(|tx| {
            let mut t = tx.open_table(OPERATIONS)?;
            if t.get(key)?.is_some() {
                return Ok(true);
            }
            t.insert(key, bytes.as_slice())?;
            Ok(false)
        })?;
        if existed {
            return self.fail(Failure::Storage(StorageError::corrupted(format!(
                "{} already has a marker",
                marker.operation
            ))));
        }
        Ok(())
    }

    /// Materializes the waiting graph reachable from the proposed edges'
    /// targets inside this transaction, adds the proposed edges and rejects
    /// any cycle. Every new cycle passes through a proposed edge and therefore
    /// through its target. Reaching a bound rejects instead of skipping the check.
    /// `candidate` is the Job the admission is about to create, the only one
    /// the graph may reach without a stored record.
    pub(crate) fn check_waiting(
        &mut self,
        candidate: JobId,
        proposed: &[(JobId, JobId)],
    ) -> Result<(), BatchError> {
        if proposed.is_empty() {
            return Ok(());
        }
        let certified = self.closure_certified()?;
        let verdict = self.step(|tx| waiting_verdict(tx, certified, candidate, proposed))?;
        match verdict {
            WaitingVerdict::Acyclic => Ok(()),
            WaitingVerdict::Cycle(jobs) => self.reject(Violation::WaitingCycle(jobs)),
            WaitingVerdict::Limit => self.reject(Violation::WaitingGraphLimit {
                max_layers: MAX_WAITING_LAYERS,
                max_jobs: MAX_WAITING_JOBS,
            }),
        }
    }
}

/// The index of the Jobs submitted with a Profile of `kind`.
pub(super) fn profile_jobs_index(kind: ProfileKind) -> PairSet {
    match kind {
        ProfileKind::Agent => AGENT_PROFILE_JOBS,
        ProfileKind::Llm => LLM_PROFILE_JOBS,
    }
}

/// The lock on a Profile, derived from canonical Job facts on every call: a
/// Job submitted with it holds it until the Job is completed, failed or
/// cancelled. Queued, started and paused Jobs hold it, and so does an
/// unknown outcome, which may still be executing, until its stop is
/// confirmed.
pub(super) fn profile_holders(
    index: &impl ReadableTable<(u64, u64), ()>,
    jobs: &impl ReadableTable<u64, &'static [u8]>,
    profile: ProfileId,
    limit: usize,
) -> Result<(Vec<JobId>, bool), StorageError> {
    let mut holders = Vec::new();
    for entry in index.range((profile.get(), 0)..=(profile.get(), u64::MAX))? {
        let job = entry?.0.value().1;
        let bytes = jobs.get(job)?.ok_or_else(|| {
            StorageError::corrupted(format!("{profile} is held by missing job {job}"))
        })?;
        if !JobRow::decode(bytes.value())?.holds_profiles() {
            continue;
        }
        if holders.len() == limit {
            return Ok((holders, true));
        }
        holders.push(JobId::new(job));
    }
    Ok((holders, false))
}

pub(super) fn last_seq(
    table: &impl ReadableTable<(u64, u64), u64>,
    job: u64,
) -> Result<u64, StorageError> {
    Ok(match table.range((job, 0)..=(job, u64::MAX))?.next_back() {
        Some(entry) => entry?.0.value().1,
        None => 0,
    })
}

/// The second components of the pairs starting with `from`, in order,
/// strictly after `after` when given.
pub(super) fn pair_targets(
    table: &impl ReadableTable<(u64, u64), ()>,
    from: u64,
    after: Option<u64>,
    limit: usize,
) -> Result<Vec<u64>, StorageError> {
    let start = match after {
        Some(u64::MAX) => return Ok(Vec::new()),
        Some(after) => after + 1,
        None => 0,
    };
    let mut targets = Vec::new();
    for entry in table.range((from, start)..=(from, u64::MAX))?.take(limit) {
        targets.push(entry?.0.value().1);
    }
    Ok(targets)
}
