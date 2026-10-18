//! The transactions of section 8 of the design: T1 submit and chat
//! continuation, the claim batch, tool decision and outcome, finalize,
//! confirm stop, settlement, placement and promotion.
//!
//! Every function runs inside the write transaction of one [`Batch`]. The
//! derived tables change in the same transaction as the facts they derive
//! from, so no commit leaves one without the other.

use redb::ReadableTableMetadata;

use super::*;
use crate::persistence::{
    ClaimGrant, ClaimRecord, ClaimRefusal, ConfigBody, ConfigId, ConfigKind, DependencyCondition,
    DependencyState, ExecutionConfiguration, FrozenPoolBinding, GLOBAL_EXECUTION_LIMIT, LlmConfig,
    OutcomeCause, OutcomeKind, OutcomeRecord, PoolConfig, Recorded, RequestFingerprint,
    SubmissionKey, SubmissionRecord, ToolCallRef, ToolDecision, ToolEvidenceRecord, ToolOutcome,
    Violation,
};

/// The key of a Job in `ready`: what a Pool must offer to run it. The high
/// byte is the operation (0 for work no Pool is asked to run) and the low byte
/// is the protocol (0 for none).
pub(super) fn requirement(
    operation: Option<ExecutionOperation>,
    protocol: Option<LlmTransport>,
) -> u16 {
    let operation = operation.map_or(0, |operation| u16::from(operation.code()));
    let protocol = protocol.map_or(0, |protocol| u16::from(protocol.code()));
    (operation << 8) | protocol
}

/// What a Pool must offer to run the Job: an LLM request over the protocol
/// of the Connection it froze. A Job without an execution needs nothing a
/// Pool offers.
pub(super) fn job_requirement(job: &JobRow) -> u16 {
    match &job.execution {
        Some(execution) => requirement(
            Some(ExecutionOperation::LlmRequest),
            Some(execution.llm.provider.transport),
        ),
        None => 0,
    }
}

/// A Pool as the claim batch sees it.
struct OpenPool {
    id: ConfigId,
    edit_seq: u32,
    body: PoolConfig,
    held: u32,
}

impl OpenPool {
    fn has_room(&self) -> bool {
        self.body.enabled && self.held < self.body.concurrency_limit
    }

    fn offers(&self, requirement_key: u16) -> bool {
        self.body.capabilities.iter().any(|(operation, protocol)| {
            requirement(Some(*operation), *protocol) == requirement_key
        })
    }
}

impl Batch {
    // ---- T1 submit (8.1) and chat continuation (8.2) -------------------------------

    /// T1_SUBMIT. `spec` is the immutable Job spec; its `id`, `created_at`
    /// and `submission` are set here. Returns the new Job.
    pub(crate) fn t1_submit(
        &mut self,
        submission: Option<(SubmissionKey, RequestFingerprint)>,
        mut spec: JobRow,
    ) -> Result<JobId, BatchError> {
        if let Some((key, fingerprint)) = submission
            && let Some(recorded) = self.submission(key)?
        {
            return self.reject(Violation::SubmissionKeyUsed {
                job: recorded.job,
                same_request: recorded.fingerprint == fingerprint,
            });
        }
        self.check_spec_references(&spec)?;
        if let Some(parent) = spec.parent_id {
            let state = self.require_job_state(parent)?;
            if state.outcome.is_some() || state.children_closed_at.is_some() {
                return self.reject(Violation::ParentClosed(parent));
            }
        }
        let mut previous = None;
        for (dependency, condition) in &spec.dependencies {
            if previous.is_some_and(|previous| *dependency <= previous) {
                return self.reject(Violation::InvalidValue(
                    "dependencies",
                    "must be sorted by Job and unique",
                ));
            }
            previous = Some(*dependency);
            self.require_job(*dependency)?;
            if self.is_subtree_settled(*dependency)? {
                let completed = self
                    .require_job_state(*dependency)?
                    .outcome
                    .is_some_and(|outcome| outcome.kind == OutcomeKind::Completed);
                if *condition == DependencyCondition::Success && !completed {
                    return self.reject(Violation::UnsatisfiedDependency {
                        job: JobId::new(0),
                        prerequisite: *dependency,
                        state: DependencyState::Impossible,
                    });
                }
            }
        }
        for attachment in &spec.attachments {
            if !self.require_attachment(*attachment)?.is_available() {
                return self.reject(Violation::AttachmentCleanedUp(*attachment));
            }
        }
        for lineage in [spec.retry_of, spec.continues].into_iter().flatten() {
            self.require_job(lineage)?;
        }
        let pending = self.step(|tx| Ok(tx.open_table(PENDING)?.len()?))?;
        if pending + 1 > P_MAX {
            return self.reject(Violation::PendingJobsLimit {
                max_pending_jobs: P_MAX,
            });
        }

        let job = JobId::new(self.next_id(Sequence::Job)?);
        spec.id = job;
        spec.created_at = self.now;
        spec.submission = submission.map(|(key, _)| key);
        self.insert_job(&spec)?;
        self.put_job_state(job, &JobStateRow::new(job))?;

        let mut open_dependencies = 0u32;
        self.step(|tx| {
            tx.open_table(PENDING)?.insert(job.get(), ())?;
            Ok(())
        })?;
        for (dependency, condition) in &spec.dependencies {
            if !self.is_subtree_settled(*dependency)? {
                open_dependencies += 1;
                self.step(|tx| {
                    tx.open_table(DEPENDENCY_WAITERS)?
                        .insert((dependency.get(), job.get()), condition.code())?;
                    Ok(())
                })?;
            }
        }
        self.step(|tx| {
            tx.open_table(UNSETTLED)?
                .insert(job.get(), (open_dependencies, 0))?;
            Ok(())
        })?;
        if let Some(parent) = spec.parent_id {
            self.adjust_open_children(parent, 1)?;
        }
        if open_dependencies == 0 {
            self.place(job)?;
        }
        let parent_key = spec.parent_id.map_or(0, JobId::get);
        self.step(|tx| {
            tx.open_table(CHILDREN)?
                .insert((parent_key, job.get()), ())?;
            for attachment in &spec.attachments {
                tx.open_table(ATTACHMENT_REFS)?
                    .insert((attachment.get(), job.get()), ())?;
            }
            if let Some(original) = spec.retry_of {
                tx.open_table(RETRIES)?
                    .insert((original.get(), job.get()), ())?;
            }
            Ok(())
        })?;
        if let Some((key, fingerprint)) = submission {
            let record = SubmissionRecord {
                key,
                fingerprint,
                job,
                recorded_at: self.now,
            };
            self.step(|tx| {
                tx.open_table(SUBMISSIONS)?.insert(
                    key.as_bytes().as_slice(),
                    encode_submission(&record).as_slice(),
                )?;
                Ok(())
            })?;
            self.probe = Some(Probe::Submission(key));
        }
        Ok(job)
    }

    /// T1_CONTINUE: T1_SUBMIT of a Chat turn that continues `predecessor`.
    pub(crate) fn t1_continue(
        &mut self,
        submission: Option<(SubmissionKey, RequestFingerprint)>,
        mut spec: JobRow,
        predecessor: JobId,
    ) -> Result<JobId, BatchError> {
        let previous = self.require_job(predecessor)?;
        if previous.agent_id != spec.agent_id {
            return self.reject(Violation::InvalidValue(
                "agent",
                "a continuation names the Agent of its predecessor",
            ));
        }
        let is_chat = |row: &JobRow| {
            row.execution
                .as_ref()
                .and_then(|execution| execution.agent.as_ref())
                .is_some_and(|agent| agent.behavior == AgentBehavior::Chat)
        };
        if !is_chat(&previous) || !is_chat(&spec) {
            return self.reject(Violation::InvalidValue(
                "behavior",
                "both turns of a conversation have Chat behavior",
            ));
        }
        if !self.is_subtree_settled(predecessor)? {
            return self.reject(Violation::RetryOfUnsettledJob(predecessor));
        }
        let tip = self.step(|tx| {
            Ok(tx
                .open_table(SUCCESSORS)?
                .get(predecessor.get())?
                .map(|tip| tip.value()))
        })?;
        if let Some(tip) = tip {
            return self.reject(Violation::ConversationForked {
                predecessor,
                tip: JobId::new(tip),
            });
        }
        spec.continues = Some(predecessor);
        let job = self.t1_submit(submission, spec)?;
        self.step(|tx| {
            tx.open_table(SUCCESSORS)?
                .insert(predecessor.get(), job.get())?;
            if previous.continues.is_none()
                && let Some(agent) = previous.agent_id
            {
                tx.open_table(CHAT_THREADS)?
                    .insert((agent.get(), predecessor.get()), ())?;
            }
            Ok(())
        })?;
        Ok(job)
    }

    /// Checks that every config a spec names exists with the kind its field
    /// requires (I17).
    fn check_spec_references(&mut self, spec: &JobRow) -> Result<(), BatchError> {
        if let Some(agent) = spec.agent_id {
            self.require_config_kind(agent, ConfigKind::Agent)?;
        }
        if let Some(execution) = &spec.execution {
            if let Some(agent) = &execution.agent_ref {
                self.require_config_kind(agent.id, ConfigKind::Agent)?;
            }
            self.require_config_kind(execution.llm.model.id, ConfigKind::Model)?;
            self.require_config_kind(execution.llm.provider.id.into(), ConfigKind::Connection)?;
        }
        Ok(())
    }

    /// Reads the configs a Job is submitted with and freezes their values
    /// (section 4.1.4): the Model, its Connection and, when named, the Agent.
    /// The Agent must list the Model.
    pub(crate) fn freeze_configuration(
        &mut self,
        configuration: ExecutionConfiguration,
    ) -> Result<FrozenExecutionConfig, BatchError> {
        let model =
            self.require_config_kind(configuration.llm_profile.into(), ConfigKind::Model)?;
        let ConfigBody::Model(model_body) = model.body.clone() else {
            return self.corrupted(format!("{} has a mismatched body", model.id));
        };
        let connection =
            self.require_config_kind(model_body.connection_id, ConfigKind::Connection)?;
        let ConfigBody::Connection(connection_body) = connection.body.clone() else {
            return self.corrupted(format!("{} has a mismatched body", connection.id));
        };
        let (agent, agent_ref) = match configuration.agent_profile {
            None => (None, None),
            Some(agent_profile) => {
                let agent = self.require_config_kind(agent_profile.into(), ConfigKind::Agent)?;
                let ConfigBody::Agent(agent_body) = agent.body.clone() else {
                    return self.corrupted(format!("{} has a mismatched body", agent.id));
                };
                if !agent_body.models.contains(&model.id) {
                    return self.reject(Violation::NotAConfiguredTarget {
                        agent_profile,
                        llm_profile: configuration.llm_profile,
                    });
                }
                (
                    Some(agent_body),
                    Some(ConfigRef {
                        id: agent.id,
                        edit_seq: agent.edit_seq,
                    }),
                )
            }
        };
        Ok(FrozenExecutionConfig {
            agent,
            agent_ref,
            llm: FrozenLlmConfig {
                config: LlmConfig {
                    model: model_body.model,
                    parameters: model_body.parameters,
                    declared_capabilities: model_body.declared_capabilities,
                },
                provider: ProviderSnapshot {
                    id: connection.id.into(),
                    edit_seq: connection.edit_seq,
                    name: connection.name,
                    transport: connection_body.protocol,
                    endpoint: connection_body.endpoint,
                    credential_ref: connection_body.credential_ref,
                },
                model: ConfigRef {
                    id: model.id,
                    edit_seq: model.edit_seq,
                },
            },
        })
    }

    /// The Jobs that retry `original`.
    pub(crate) fn retries(&mut self, original: JobId) -> Result<Vec<JobId>, BatchError> {
        self.step(|tx| {
            let table = tx.open_table(RETRIES)?;
            let mut retries = Vec::new();
            for entry in table.range((original.get(), 0)..=(original.get(), u64::MAX))? {
                retries.push(JobId::new(entry?.0.value().1));
            }
            Ok(retries)
        })
    }

    // ---- placement (8.10) ---------------------------------------------------------

    /// PLACE: a pending Job with no open dependency becomes `ready`, or
    /// `delayed` until its `eligible_at`.
    pub(crate) fn place(&mut self, job: JobId) -> Result<(), BatchError> {
        let row = self.require_job(job)?;
        let requirement = job_requirement(&row);
        let eligible_at = row.eligible_at.map(Timestamp::as_unix_micros);
        let clock = open::promotion_clock();
        self.touched_ready_or_delayed = true;
        self.step(|tx| {
            match eligible_at {
                Some(eligible_at) if eligible_at > clock => {
                    tx.open_table(DELAYED)?
                        .insert((eligible_at, job.get()), ())?;
                }
                _ => {
                    tx.open_table(READY)?.insert((requirement, job.get()), ())?;
                }
            }
            Ok(())
        })
    }

    /// PROMOTE: the `delayed` rows whose time has come become `ready`.
    pub(crate) fn promote(&mut self) -> Result<(), BatchError> {
        let clock = open::promotion_clock();
        let due = self.step(|tx| {
            let delayed = tx.open_table(DELAYED)?;
            let mut due = Vec::new();
            for entry in delayed.range((i64::MIN, 0)..=(clock, u64::MAX))? {
                due.push(entry?.0.value());
            }
            Ok(due)
        })?;
        for (eligible_at, job) in due {
            let row = self.require_job(JobId::new(job))?;
            let requirement = job_requirement(&row);
            self.touched_ready_or_delayed = true;
            self.step(|tx| {
                tx.open_table(DELAYED)?.remove((eligible_at, job))?;
                tx.open_table(READY)?.insert((requirement, job), ())?;
                Ok(())
            })?;
        }
        Ok(())
    }

    // ---- T2 claim batch (8.3) -------------------------------------------------------

    fn open_pools(&mut self) -> Result<Vec<OpenPool>, BatchError> {
        self.step(|tx| {
            let configs = tx.open_table(CONFIGS)?;
            let held = tx.open_table(HELD_SLOTS)?;
            let mut pools = Vec::new();
            for entry in configs.iter()? {
                let row = ConfigRow::decode(entry?.1.value())?;
                let ConfigBody::Pool(body) = row.body else {
                    continue;
                };
                let id = row.id.get();
                let mut count = 0u32;
                for slot in held.range((id, 0)..=(id, u64::MAX))? {
                    slot?;
                    count += 1;
                }
                pools.push(OpenPool {
                    id: row.id,
                    edit_seq: row.edit_seq,
                    body,
                    held: count,
                });
            }
            Ok(pools)
        })
    }

    fn held_slot_count(&mut self) -> Result<u32, BatchError> {
        let count = self.step(|tx| Ok(tx.open_table(HELD_SLOTS)?.len()?))?;
        Ok(u32::try_from(count).unwrap_or(u32::MAX))
    }

    /// CLAIM_BATCH: promotes due Jobs, then claims ready Jobs, smallest Job
    /// id first, while a global slot and a compatible Pool are free.
    pub(crate) fn claim_batch(&mut self) -> Result<Vec<JobId>, BatchError> {
        self.promote()?;
        let mut free = u32::from(GLOBAL_EXECUTION_LIMIT).saturating_sub(self.held_slot_count()?);
        let mut pools: Vec<OpenPool> = self
            .open_pools()?
            .into_iter()
            .filter(OpenPool::has_room)
            .collect();
        let mut grants = Vec::new();
        while free > 0 {
            let offered: BTreeSet<u16> = pools
                .iter()
                .flat_map(|pool| {
                    pool.body
                        .capabilities
                        .iter()
                        .map(|(operation, protocol)| requirement(Some(*operation), *protocol))
                })
                .collect();
            let mut candidate: Option<(u16, u64)> = None;
            for key in offered {
                let first = self.step(|tx| {
                    let ready = tx.open_table(READY)?;
                    let first = ready
                        .range((key, 0)..=(key, u64::MAX))?
                        .next()
                        .transpose()?
                        .map(|(entry, _)| entry.value().1);
                    Ok(first)
                })?;
                if let Some(job) = first
                    && candidate.is_none_or(|(_, best)| job < best)
                {
                    candidate = Some((key, job));
                }
            }
            let Some((key, job)) = candidate else {
                break;
            };
            let Some(index) = pools
                .iter()
                .enumerate()
                .filter(|(_, pool)| pool.offers(key))
                .min_by_key(|(_, pool)| (pool.body.capabilities.len(), pool.id))
                .map(|(index, _)| index)
            else {
                break;
            };
            let (pool, edit_seq, capabilities) = {
                let pool = &mut pools[index];
                pool.held += 1;
                (pool.id, pool.edit_seq, pool.body.capabilities.clone())
            };
            self.write_claim(JobId::new(job), key, pool, edit_seq, capabilities)?;
            grants.push(JobId::new(job));
            free -= 1;
            if !pools[index].has_room() {
                pools.remove(index);
            }
        }
        if let Some(first) = grants.first() {
            self.probe = Some(Probe::Claim(*first));
        }
        Ok(grants)
    }

    /// Writes the claim of `job` into `pool`: the claim itself, the Job's
    /// leaving `ready` and `pending`, and its held slot.
    fn write_claim(
        &mut self,
        job: JobId,
        requirement_key: u16,
        pool: ConfigId,
        pool_edit_seq: u32,
        capabilities: BTreeSet<(ExecutionOperation, Option<LlmTransport>)>,
    ) -> Result<(), BatchError> {
        let mut state = self.require_job_state(job)?;
        state.claim = Some(ClaimRecord {
            started_at: self.now,
            pool_id: pool,
            pool_edit_seq,
            capabilities,
            epoch: self.epoch,
            host_id: self.host_id.clone(),
            boot_id: self.boot_id.clone(),
        });
        self.put_job_state(job, &state)?;
        let epoch = self.epoch;
        self.step(|tx| {
            tx.open_table(READY)?.remove((requirement_key, job.get()))?;
            tx.open_table(PENDING)?.remove(job.get())?;
            tx.open_table(HELD_SLOTS)?
                .insert((pool.get(), job.get()), (epoch, false))?;
            Ok(())
        })
    }

    /// Claims one named ready Job into the first of `allowed` that is
    /// enabled, offers what the Job needs and has room. The same limits as
    /// [`Self::claim_batch`] apply; a refusal changes nothing.
    pub(crate) fn claim_job(
        &mut self,
        job: JobId,
        allowed: &BTreeSet<ConfigId>,
    ) -> Result<Result<ClaimGrant, ClaimRefusal>, BatchError> {
        let Some(row) = self.job(job)? else {
            return Ok(Err(ClaimRefusal::NotFound));
        };
        let state = self.require_job_state(job)?;
        if state.claim.is_some() || state.outcome.is_some() {
            return Ok(Err(ClaimRefusal::NotPending));
        }
        if allowed.is_empty() {
            return Ok(Err(ClaimRefusal::EmptyAllowedPools));
        }
        self.promote()?;
        let requirement_key = job_requirement(&row);
        let (ready, delayed) = self.step(|tx| {
            let ready = tx
                .open_table(READY)?
                .get((requirement_key, job.get()))?
                .is_some();
            let mut delayed = None;
            if let Some(eligible_at) = row.eligible_at.map(Timestamp::as_unix_micros)
                && tx
                    .open_table(DELAYED)?
                    .get((eligible_at, job.get()))?
                    .is_some()
            {
                delayed = Some(eligible_at);
            }
            Ok((ready, delayed))
        })?;
        if !ready {
            if let Some(eligible_at) = delayed {
                return Ok(Err(ClaimRefusal::NotYetEligible {
                    eligible_at: Timestamp::from_unix_micros(eligible_at),
                }));
            }
            let prerequisite = self.first_open_dependency(&row)?;
            return Ok(Err(match prerequisite {
                Some(prerequisite) => ClaimRefusal::DependencyUnsatisfied {
                    prerequisite,
                    state: DependencyState::Waiting,
                },
                None => ClaimRefusal::NotPending,
            }));
        }
        let held = self.held_slot_count()?;
        if held >= u32::from(GLOBAL_EXECUTION_LIMIT) {
            return Ok(Err(ClaimRefusal::NoCapacity(CapacityScope::Global)));
        }
        let pools = self.open_pools()?;
        let mut compatible = false;
        let mut chosen = None;
        for pool in pools.iter().filter(|pool| allowed.contains(&pool.id)) {
            if !pool.body.enabled || !pool.offers(requirement_key) {
                continue;
            }
            compatible = true;
            if pool.held < pool.body.concurrency_limit {
                chosen = Some(pool);
                break;
            }
        }
        let Some(pool) = chosen else {
            return Ok(Err(if compatible {
                ClaimRefusal::NoCapacity(CapacityScope::Pools(
                    allowed
                        .iter()
                        .map(|pool| ExecutionPoolId::from(*pool))
                        .collect(),
                ))
            } else {
                ClaimRefusal::NoCompatiblePool
            }));
        };
        let (pool_id, edit_seq, pool_capabilities) =
            (pool.id, pool.edit_seq, pool.body.capabilities.clone());
        self.write_claim(
            job,
            requirement_key,
            pool_id,
            edit_seq,
            pool_capabilities.clone(),
        )?;
        self.probe = Some(Probe::Claim(job));
        Ok(Ok(ClaimGrant {
            job,
            binding: FrozenPoolBinding {
                pool: pool_id.into(),
                edit_seq,
                capabilities: pool_capabilities,
            },
            epoch: self.epoch,
            global_free_after: u8::try_from(
                u32::from(GLOBAL_EXECUTION_LIMIT).saturating_sub(held + 1),
            )
            .unwrap_or(0),
        }))
    }

    fn first_open_dependency(&mut self, row: &JobRow) -> Result<Option<JobId>, BatchError> {
        for (dependency, _) in &row.dependencies {
            if !self.is_subtree_settled(*dependency)? {
                return Ok(Some(*dependency));
            }
        }
        Ok(None)
    }

    /// Starts a Job in `pool` without the claim batch's limits, as this
    /// process's own execution. The Job must be ready now; one still waiting
    /// on a dependency is refused. The Job holds a slot in `pool` until it is
    /// finalized or its stop is confirmed.
    pub(crate) fn claim_local(&mut self, job: JobId, pool: ConfigId) -> Result<(), BatchError> {
        let row = self.require_job(job)?;
        let state = self.require_job_state(job)?;
        if state.outcome.is_some() {
            return self.reject(Violation::JobHasTerminalOutcome(job));
        }
        if state.claim.is_some() {
            return self.reject(Violation::InvalidValue("job", "is already started"));
        }
        if let Some(prerequisite) = self.first_open_dependency(&row)? {
            return self.reject(Violation::UnsatisfiedDependency {
                job,
                prerequisite,
                state: DependencyState::Waiting,
            });
        }
        let config = self.require_config_kind(pool, ConfigKind::Pool)?;
        let ConfigBody::Pool(body) = config.body else {
            return self.reject(Violation::NotFound(RecordRef::ExecutionPool(pool.into())));
        };
        self.promote()?;
        // A delayed Job starts when it is asked to: the caller states the time.
        let requirement_key = job_requirement(&row);
        if let Some(eligible_at) = row.eligible_at.map(Timestamp::as_unix_micros) {
            self.step(|tx| {
                tx.open_table(DELAYED)?.remove((eligible_at, job.get()))?;
                Ok(())
            })?;
        }
        self.write_claim(
            job,
            requirement_key,
            pool,
            config.edit_seq,
            body.capabilities,
        )?;
        self.probe = Some(Probe::Claim(job));
        Ok(())
    }

    /// Creates a system Job that this process runs itself, claimed into
    /// `pool` in the same transaction. It never goes through T1 or the claim
    /// batch: it is never pending, never ready and has no dependency.
    pub(crate) fn start_system_job(
        &mut self,
        mut spec: JobRow,
        pool: ConfigId,
    ) -> Result<JobId, BatchError> {
        let config = self.require_config_kind(pool, ConfigKind::Pool)?;
        let ConfigBody::Pool(body) = config.body else {
            return self.reject(Violation::NotFound(RecordRef::ExecutionPool(pool.into())));
        };
        if let Some(parent) = spec.parent_id {
            let state = self.require_job_state(parent)?;
            if state.outcome.is_some() || state.children_closed_at.is_some() {
                return self.reject(Violation::ParentClosed(parent));
            }
        }
        let job = JobId::new(self.next_id(Sequence::Job)?);
        spec.id = job;
        spec.created_at = self.now;
        self.insert_job(&spec)?;
        let mut state = JobStateRow::new(job);
        state.claim = Some(ClaimRecord {
            started_at: self.now,
            pool_id: pool,
            pool_edit_seq: config.edit_seq,
            capabilities: body.capabilities,
            epoch: self.epoch,
            host_id: self.host_id.clone(),
            boot_id: self.boot_id.clone(),
        });
        self.put_job_state(job, &state)?;
        let epoch = self.epoch;
        let parent_key = spec.parent_id.map_or(0, JobId::get);
        self.step(|tx| {
            tx.open_table(UNSETTLED)?.insert(job.get(), (0, 0))?;
            tx.open_table(CHILDREN)?
                .insert((parent_key, job.get()), ())?;
            tx.open_table(HELD_SLOTS)?
                .insert((pool.get(), job.get()), (epoch, false))?;
            Ok(())
        })?;
        if let Some(parent) = spec.parent_id {
            self.adjust_open_children(parent, 1)?;
        }
        Ok(job)
    }

    fn adjust_open_children(&mut self, job: JobId, delta: i64) -> Result<(), BatchError> {
        self.step(|tx| {
            let mut unsettled = tx.open_table(UNSETTLED)?;
            let counts = unsettled.get(job.get())?.map(|counts| counts.value());
            if let Some((dependencies, children)) = counts {
                let children = u32::try_from(i64::from(children) + delta).unwrap_or(0);
                unsettled.insert(job.get(), (dependencies, children))?;
            }
            Ok(())
        })
    }

    // ---- tool decision and outcome (8.4) --------------------------------------------

    /// TOOL_DECIDE: records OCG's decision about a call before anything about
    /// it can run (I12). `spawns` says whether the admitted handler can start
    /// an OS process.
    pub(crate) fn tool_decide(
        &mut self,
        call: ToolCallRef,
        tool_call_id: &str,
        name: &str,
        decision: ToolDecision,
        spawns: bool,
    ) -> Result<(), BatchError> {
        self.require_executing_here(call.job)?;
        if !self.request_observed(call.job, call.request)? {
            return self.reject(Violation::RequestNotObserved {
                job: call.job,
                request: call.request,
            });
        }
        let key = tool_call_key(call);
        if self.tool_evidence(call)?.is_some() {
            return self.reject(Violation::DuplicateToolCall(call));
        }
        let existing = self.step(|tx| {
            Ok(tx
                .open_table(TOOL_CALL_IDS)?
                .get((call.job.get(), tool_call_id))?
                .map(|found| found.value()))
        })?;
        if let Some((request, index)) = existing {
            let existing = tool_call_ref((call.job.get(), request, index))
                .map_err(|error| BatchError(Failure::Storage(error)))?;
            return self.reject(Violation::DuplicateToolCallId { call, existing });
        }
        if let ToolDecision::Admitted(admission) = &decision {
            let (mut arguments, mut schemas) =
                (admission.arguments.len(), admission.parameters.len());
            for record in self.job_tool_evidence(call.job)? {
                if let Some(earlier) = record.admission() {
                    arguments = arguments.saturating_add(earlier.arguments.len());
                    schemas = schemas.saturating_add(earlier.parameters.len());
                }
            }
            if arguments > MAX_STORED_TOOL_ARGUMENTS_PER_JOB_BYTES {
                return self.reject(Violation::ToolEvidenceLimit {
                    call,
                    limit: "the arguments of one Job",
                });
            }
            if schemas > MAX_STORED_TOOL_SCHEMAS_PER_JOB_BYTES {
                return self.reject(Violation::ToolEvidenceLimit {
                    call,
                    limit: "the schemas of one Job",
                });
            }
        }
        let record = ToolEvidenceRecord {
            call,
            tool_call_id: tool_call_id.to_owned(),
            name: name.to_owned(),
            spawns: spawns && matches!(decision, ToolDecision::Admitted(_)),
            decision: Recorded {
                fact: decision,
                at: self.now,
            },
            outcome: None,
            reconciliation: None,
        };
        self.step(|tx| {
            tx.open_table(TOOL_CALLS)?
                .insert(key, encode_tool_evidence(&record).as_slice())?;
            tx.open_table(TOOL_CALL_IDS)?.insert(
                (call.job.get(), tool_call_id),
                (call.request.get(), call.index),
            )?;
            Ok(())
        })?;
        self.probe = Some(Probe::ToolCall(call));
        Ok(())
    }

    /// TOOL_OUTCOME: records what is known of an admitted call's execution,
    /// once.
    pub(crate) fn tool_outcome(
        &mut self,
        call: ToolCallRef,
        outcome: ToolOutcome,
    ) -> Result<(), BatchError> {
        self.require_executing_here(call.job)?;
        let Some(mut record) = self.tool_evidence(call)? else {
            return self.reject(Violation::ToolCallNotFound(call));
        };
        if record.admission().is_none() {
            return self.reject(Violation::ToolCallNotAdmitted(call));
        }
        if record.outcome.is_some() {
            return self.reject(Violation::ToolOutcomeAlreadyRecorded(call));
        }
        record.outcome = Some(Recorded {
            fact: outcome,
            at: self.now,
        });
        self.put_tool_evidence(&record)?;
        self.probe = Some(Probe::ToolOutcome(call));
        Ok(())
    }

    /// Refuses unless the Job is claimed by this opening of the database and
    /// has no outcome (I13).
    fn require_executing_here(&mut self, job: JobId) -> Result<(), BatchError> {
        let state = self.require_job_state(job)?;
        match &state.claim {
            Some(claim) if claim.epoch == self.epoch && state.outcome.is_none() => Ok(()),
            _ => self.reject(Violation::NotExecutingLocally(job)),
        }
    }

    // ---- finalize (8.5), confirm stop (8.6) ------------------------------------------

    /// FINALIZE. A confirmed outcome of a claimed Job whose direct children
    /// have not all settled, or whose admitted tool calls are not all
    /// resolved, is recorded as Unknown (I6). Returns the outcome kind that
    /// was recorded and the sequence number of the Result output, if any.
    pub(crate) fn finalize(
        &mut self,
        job: JobId,
        mut kind: OutcomeKind,
        cause: Option<OutcomeCause>,
        result: Option<FinalResult>,
    ) -> Result<(OutcomeKind, Option<u32>), BatchError> {
        let mut state = self.require_job_state(job)?;
        if state.outcome.is_some() {
            return self.reject(Violation::JobAlreadyFinalized(job));
        }
        if state.claim.is_none() && kind != OutcomeKind::Cancelled {
            return self.reject(Violation::InvalidValue(
                "outcome",
                "a Job without a claim can only be Cancelled",
            ));
        }
        if state.claim.is_some() && kind != OutcomeKind::Unknown {
            let open_children = self.step(|tx| {
                Ok(tx
                    .open_table(UNSETTLED)?
                    .get(job.get())?
                    .map_or(0, |counts| counts.value().1))
            })?;
            if open_children > 0 || !self.tool_effects_resolved(job)? {
                kind = OutcomeKind::Unknown;
            }
        }
        state.outcome = Some(OutcomeRecord {
            kind,
            at: self.now,
            cause,
        });
        state.children_closed_at.get_or_insert(self.now);
        let mut appended = None;
        if let Some(result) = result
            && kind != OutcomeKind::Unknown
        {
            appended = Some(self.append_output(
                job,
                OutputKind::Result,
                result.summary,
                result.code,
                result.goal_reached,
                result.value,
            )?);
        }
        if kind == OutcomeKind::Unknown && self.no_live_process(job)? {
            state.stop_confirmed_at = Some(self.now);
        }
        if let Some(claim) = &state.claim {
            let pool = claim.pool_id.get();
            let epoch = claim.epoch;
            if kind != OutcomeKind::Unknown || state.stop_confirmed_at.is_some() {
                self.freed_capacity = true;
                self.step(|tx| {
                    tx.open_table(HELD_SLOTS)?.remove((pool, job.get()))?;
                    Ok(())
                })?;
            } else {
                self.step(|tx| {
                    tx.open_table(HELD_SLOTS)?
                        .insert((pool, job.get()), (epoch, true))?;
                    Ok(())
                })?;
            }
        } else {
            self.cancel_pending_writes(job)?;
        }
        self.put_job_state(job, &state)?;
        self.settle(job)?;
        self.probe = Some(Probe::Outcome(job));
        Ok((kind, appended))
    }

    /// CONFIRM_STOP: the stop of an Unknown Job is proven, so its slot is
    /// released.
    pub(crate) fn confirm_stop(&mut self, job: JobId) -> Result<(), BatchError> {
        let mut state = self.require_job_state(job)?;
        if !state
            .outcome
            .as_ref()
            .is_some_and(|outcome| outcome.kind == OutcomeKind::Unknown)
        {
            return self.reject(Violation::StopNeedsUnknownOutcome(job));
        }
        if state.stop_confirmed_at.is_some() {
            return self.reject(Violation::StopAlreadyConfirmed(job));
        }
        state.stop_confirmed_at = Some(self.now);
        if let Some(claim) = &state.claim {
            let pool = claim.pool_id.get();
            self.freed_capacity = true;
            self.step(|tx| {
                tx.open_table(HELD_SLOTS)?.remove((pool, job.get()))?;
                Ok(())
            })?;
        }
        self.put_job_state(job, &state)?;
        self.settle(job)
    }

    /// CANCEL_PENDING_WRITES: a Job cancelled before its claim leaves every
    /// readiness table.
    pub(crate) fn cancel_pending_writes(&mut self, job: JobId) -> Result<(), BatchError> {
        let row = self.require_job(job)?;
        let requirement_key = job_requirement(&row);
        self.step(|tx| {
            tx.open_table(PENDING)?.remove(job.get())?;
            tx.open_table(READY)?.remove((requirement_key, job.get()))?;
            if let Some(eligible_at) = row.eligible_at {
                tx.open_table(DELAYED)?
                    .remove((eligible_at.as_unix_micros(), job.get()))?;
            }
            let mut waiters = tx.open_table(DEPENDENCY_WAITERS)?;
            for (dependency, _) in &row.dependencies {
                waiters.remove((dependency.get(), job.get()))?;
            }
            let mut unsettled = tx.open_table(UNSETTLED)?;
            let open_children = unsettled.get(job.get())?.map(|counts| counts.value().1);
            if let Some(open_children) = open_children {
                unsettled.insert(job.get(), (0, open_children))?;
            }
            Ok(())
        })
    }

    /// SETTLE: from `start`, deletes the `unsettled` row of every Job that has
    /// become SubtreeSettled, releases or cancels its waiters and moves up to
    /// its parent.
    pub(crate) fn settle(&mut self, start: JobId) -> Result<(), BatchError> {
        let mut work = vec![start];
        while let Some(job) = work.pop() {
            let counts = self.step(|tx| {
                Ok(tx
                    .open_table(UNSETTLED)?
                    .get(job.get())?
                    .map(|counts| counts.value()))
            })?;
            let Some((_, open_children)) = counts else {
                continue;
            };
            let state = self.require_job_state(job)?;
            if !state.locally_stopped() || open_children > 0 {
                continue;
            }
            self.step(|tx| {
                tx.open_table(UNSETTLED)?.remove(job.get())?;
                Ok(())
            })?;
            let completed = state
                .outcome
                .as_ref()
                .is_some_and(|outcome| outcome.kind == OutcomeKind::Completed);
            let waiters = self.step(|tx| {
                let table = tx.open_table(DEPENDENCY_WAITERS)?;
                let mut waiters = Vec::new();
                for entry in table.range((job.get(), 0)..=(job.get(), u64::MAX))? {
                    let (key, condition) = entry?;
                    waiters.push((key.value().1, condition.value()));
                }
                Ok(waiters)
            })?;
            for (waiter, condition) in waiters {
                self.step(|tx| {
                    tx.open_table(DEPENDENCY_WAITERS)?
                        .remove((job.get(), waiter))?;
                    Ok(())
                })?;
                let waiter = JobId::new(waiter);
                let condition = DependencyCondition::from_code(condition)
                    .ok_or_else(|| StorageError::corrupted("invalid dependency condition"));
                let condition = self.decoded(condition)?;
                if condition == DependencyCondition::Settled || completed {
                    let remaining = self.step(|tx| {
                        let mut unsettled = tx.open_table(UNSETTLED)?;
                        let counts = unsettled.get(waiter.get())?.map(|counts| counts.value());
                        let Some((dependencies, children)) = counts else {
                            return Ok(None);
                        };
                        let dependencies = dependencies.saturating_sub(1);
                        unsettled.insert(waiter.get(), (dependencies, children))?;
                        Ok(Some(dependencies))
                    })?;
                    if remaining == Some(0) {
                        self.place(waiter)?;
                    }
                } else {
                    let mut cancelled = self.require_job_state(waiter)?;
                    cancelled.outcome = Some(OutcomeRecord {
                        kind: OutcomeKind::Cancelled,
                        at: self.now,
                        cause: Some(OutcomeCause::DependencyImpossible),
                    });
                    cancelled.children_closed_at.get_or_insert(self.now);
                    self.put_job_state(waiter, &cancelled)?;
                    self.cancel_pending_writes(waiter)?;
                    work.push(waiter);
                }
            }
            if let Some(parent) = self.require_job(job)?.parent_id {
                self.adjust_open_children(parent, -1)?;
                work.push(parent);
            }
        }
        Ok(())
    }

    // ---- predicates over a Job's tool calls ----------------------------------------------

    /// NO_LIVE_PROCESS: every admitted call that can spawn has an
    /// outcome, so no OS process of the Job can remain (section 11.4).
    pub(crate) fn no_live_process(&mut self, job: JobId) -> Result<bool, BatchError> {
        let calls = self.job_tool_evidence(job)?;
        Ok(calls
            .iter()
            .all(|record| !record.spawns || record.outcome.is_some()))
    }

    /// Whether every admitted call has a definite outcome or a reconciliation.
    fn tool_effects_resolved(&mut self, job: JobId) -> Result<bool, BatchError> {
        let calls = self.job_tool_evidence(job)?;
        Ok(calls.iter().all(|record| !record.is_unresolved()))
    }
}
