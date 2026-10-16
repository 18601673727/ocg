//! The only module that touches redb.
//!
//! redb is the durable source of truth. Every record is stored under its
//! stable business ID with an explicit, versioned encoding and holds only its
//! own attributes; every relationship has its own table, keyed so that its
//! cardinality is structural, plus an incoming index where a rule or query
//! needs one. An incoming index is written only by the [`Batch`] operation
//! that writes its relationship, in the same transaction, so it is never an
//! authority of its own. The database, its transactions and its tables never
//! leave this module: mutations go through [`Batch`], whose typed operations
//! are the only write path, and every relationship write checks that its
//! endpoints exist and have the required type.
//!
//! A batch is one redb write transaction. Its first failure, a rule
//! violation as much as a storage error, poisons it: every later operation
//! returns that failure again and [`run_batch`] aborts instead of committing,
//! even when the operation's caller ignored the error. Records,
//! relationships, indexes, sequences and the operation marker therefore
//! commit or abort together.
//!
//! The instance has exactly one Genesis Job, created with the database file
//! and named by the `genesis` meta entry. It is the only Job that is its own
//! parent; that self-parent edge is a root marker kept in CHILD_OF only, so
//! child enumeration, the waiting graph and descendant traversal never see it.
//!
//! The waiting graph is never cached. [`Batch::check_waiting`] builds a
//! bounded petgraph view from records read inside the current write
//! transaction, adds the proposed edges and rejects any cycle.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};

use petgraph::graph::{DiGraph, NodeIndex};
use redb::{
    CommitError, Database, Durability, ReadTransaction, ReadableDatabase, ReadableTable,
    TableDefinition, WriteTransaction,
};

use crate::openai_compatible::{
    ChatFinishReason, NormalizedUsage, ProviderFailure, ToolCallRefusal,
};
use crate::persistence::{
    AgentConfig, AgentId, AgentRecord, AttachmentCleanup, AttachmentId, AttachmentOrigin,
    AttachmentRecord, AuthorizationBasis, AvailabilityObservation, AvailabilityStatId,
    AvailabilityStatRecord, CleanupKind, ContentDigest, DependencyCondition, DependencyState,
    EffectConclusion, ExecutionConfiguration, ExecutionMode, ExecutorId, ExecutorRecord,
    ExecutorType, FrozenExecutionConfig, FrozenLlmConfig, JobId, JobInputId, JobInputRecord,
    JobOutputId, JobOutputRecord, JobRecord, LlmConfig, LlmParameters, LlmProviderId,
    LlmProviderRecord, LlmTarget, LlmTransport, Measurements, MetricsId, MetricsRecord,
    ModelCapability, NotInvokedReason, ObservationCursor, ObservationFilter, ObservationPage,
    ObservedModel, ObservedModelChoice, ObservedModelFilter, ObservedModelPage, OperationId,
    OperationKind, OperationMarker, OutputKind, ProfileConfig, ProfileEditability, ProfileId,
    ProfileKind, ProfileRecord, ProjectId, ProjectRecord, ProviderSnapshot, RecordRef, Recorded,
    RequestFingerprint, RequestNumber, RequestRef, SubmissionKey, SubmissionRecord,
    SubmittedConfiguration, Timestamp, ToolAdmission, ToolCallRef, ToolDecision, ToolEffect,
    ToolEvidenceRecord, ToolOutcome, ToolPayload, ToolReconciliation, UncertainCause, Violation,
    WithheldReason, WrongProfileKind,
};
use crate::token_usage::{RequestEligibility, TokenAnnotation, UsageFinality};

mod availability_aggregation;
mod batch;
mod batch_links;
mod codec;
#[cfg(test)]
mod integrity;
mod observations;
mod open;
mod reads;
mod rows;
mod schema;
mod sequences;
mod token_aggregation;
mod waiting;
pub(crate) use availability_aggregation::{
    AvailabilityPlan, MAX_AGGREGATION_ATTEMPTS, commit_availability_aggregation,
    prepare_availability_aggregation, read_availability_minute, read_availability_progress,
    read_request_sample,
};
#[cfg(test)]
pub(crate) use availability_aggregation::{WriteProbe, replace_minute};
use batch::Failure;
#[cfg(test)]
pub(crate) use batch::run_batch;
pub(crate) use batch::{
    Batch, BatchError, BatchFailure, BatchOutcome, Commit, CompactFailure, MarkerSubjects,
    allocated_bytes, compact, persist_staged, run_batch_declinable, run_batch_with,
};
use batch_links::{last_seq, pair_targets, profile_holders, profile_jobs_index};
use codec::{
    Code, Dec, DecodeResult, Enc, TAG_AGENT, TAG_AGGREGATION_PROGRESS, TAG_ATTACHMENT,
    TAG_AVAILABILITY_STAT, TAG_EXECUTOR, TAG_INPUT, TAG_JOB, TAG_LLM_PROVIDER, TAG_METRICS,
    TAG_MINUTE_COUNTS, TAG_OPERATION, TAG_OUTPUT, TAG_PROFILE, TAG_PROJECT, TAG_REQUEST_SAMPLE,
    TAG_SUBMISSION, TAG_TOKEN_MINUTE, TAG_TOKEN_REQUEST, TAG_TOOL_EVIDENCE,
};
#[cfg(test)]
pub(crate) use integrity::{
    break_project_links, relationship_inconsistencies, remove_input_link, remove_job_row,
};
use observations::{availability_from, job_target, observation_job};
pub(crate) use observations::{
    read_availability_stats, read_job_availability, read_job_metrics, read_metrics,
    read_observed_models,
};
pub(crate) use open::{OpenError, OpenedFile, database_file, open};
use open::{allocate, genesis_id};
#[cfg(test)]
pub(crate) use reads::record_attachment_cleanup;
pub(crate) use reads::{
    abandoned_local_executions, content_available, read_agent, read_attachment,
    read_attachment_inputs, read_attachments, read_children, read_compatible_executors,
    read_executor, read_executors, read_input_jobs, read_job, read_job_compatible_executors,
    read_job_configuration, read_job_input, read_job_input_for_job, read_jobs, read_latest_output,
    read_llm_provider, read_llm_providers, read_marker, read_markers, read_outputs, read_profile,
    read_profile_editability, read_profile_jobs, read_profiles, read_project, read_project_jobs,
    read_projects, read_provider_profiles, read_retries, read_submission, read_tool_evidence,
    unneeded_content,
};
use reads::{read, read_link, read_pairs};
pub(crate) use rows::{
    AgentRow, AvailabilityStatRow, ExecutorRow, JobInputRow, JobRow, MetricsRow, ProfileLinks,
    ProfileRow,
};
use rows::{
    decode_attachment, decode_llm_provider, decode_marker, decode_output, decode_project,
    decode_submission, decode_tool_evidence, encode_attachment, encode_llm_provider, encode_marker,
    encode_output, encode_project, encode_submission, encode_tool_evidence, job_tool_evidence,
    provider_snapshot, tool_call_key, tool_call_ref,
};
use schema::*;
pub(crate) use schema::{
    CACHE_BYTES, DATABASE_FILE_NAME, FORMAT_NAME, FORMAT_VERSION, MAX_OBSERVATION_SCAN,
    MAX_OBSERVED_MODEL_SCAN, MAX_PAGE, MAX_WAITING_JOBS, MAX_WAITING_LAYERS,
};
pub(crate) use sequences::Sequence;
use sequences::{OPEN_EPOCH, Scope, filter_matches};
pub(crate) use token_aggregation::{
    TokenPlan, commit_token_aggregation, prepare_token_aggregation, read_token_minute_window,
    read_token_progress, read_token_request,
};
#[cfg(test)]
pub(crate) use token_aggregation::{replace_token_minute, replace_token_request};
pub(crate) use waiting::dependency_state;
use waiting::{WaitingVerdict, waiting_verdict};

// ---- storage errors -------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageErrorKind {
    /// An operating-system I/O error, such as `StorageFull`.
    Io(std::io::ErrorKind),
    /// An earlier I/O error was latched; the open database refuses further work.
    PreviousIo,
    Corrupted,
    AlreadyOpen,
    Closed,
    ValueTooLarge,
    Other,
}

/// A storage-engine failure. Its kind is structured; the detail is for humans.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{kind:?}: {detail}")]
pub struct StorageError {
    pub kind: StorageErrorKind,
    /// The raw OS error number for I/O failures (28 is ENOSPC on macOS and Linux).
    pub os_error: Option<i32>,
    pub detail: String,
}

impl StorageError {
    pub(crate) fn corrupted(detail: impl Into<String>) -> Self {
        Self {
            kind: StorageErrorKind::Corrupted,
            os_error: None,
            detail: detail.into(),
        }
    }
}

impl From<redb::Error> for StorageError {
    fn from(error: redb::Error) -> Self {
        let (kind, os_error) = match &error {
            redb::Error::Io(io) => (StorageErrorKind::Io(io.kind()), io.raw_os_error()),
            redb::Error::PreviousIo => (StorageErrorKind::PreviousIo, None),
            redb::Error::Corrupted(_) => (StorageErrorKind::Corrupted, None),
            redb::Error::DatabaseAlreadyOpen => (StorageErrorKind::AlreadyOpen, None),
            redb::Error::DatabaseClosed => (StorageErrorKind::Closed, None),
            redb::Error::ValueTooLarge(_) => (StorageErrorKind::ValueTooLarge, None),
            _ => (StorageErrorKind::Other, None),
        };
        Self {
            kind,
            os_error,
            detail: error.to_string(),
        }
    }
}

macro_rules! storage_from {
    ($($t:ty),*) => {$(
        impl From<$t> for StorageError {
            fn from(error: $t) -> Self { redb::Error::from(error).into() }
        }
    )*};
}
storage_from!(
    redb::StorageError,
    redb::TableError,
    redb::TransactionError,
    redb::DatabaseError,
    redb::SetDurabilityError,
    CommitError
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::{self, JobInputDraft, NewJob};
    use redb::ReadableTableMetadata;
    use std::fs;

    /// Removes its directory when dropped.
    struct TestRoot(PathBuf);

    impl Drop for TestRoot {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).ok();
        }
    }

    struct TestDatabase {
        database: Database,
        epoch: u64,
        genesis: JobId,
        // Declared last so the database closes before its directory goes.
        root: TestRoot,
    }

    impl TestDatabase {
        fn new(name: &str) -> Self {
            let root =
                std::env::temp_dir().join(format!("ocg-storage-{}-{name}", std::process::id()));
            fs::remove_dir_all(&root).ok();
            fs::create_dir_all(&root).expect("create storage test directory");
            Self::open(TestRoot(root))
        }

        fn open(root: TestRoot) -> Self {
            let opened = open(&root.0).unwrap_or_else(|_| panic!("open storage test database"));
            Self {
                database: opened.database,
                epoch: opened.epoch,
                genesis: opened.genesis,
                root,
            }
        }

        /// Closes and reopens the same file.
        fn reopen(self) -> Self {
            let Self { database, root, .. } = self;
            drop(database);
            Self::open(root)
        }

        fn batch<T>(
            &self,
            sequence: u64,
            body: impl FnOnce(&mut Batch) -> Result<(T, MarkerSubjects), BatchError>,
        ) -> Result<T, BatchFailure> {
            run_batch(
                &self.database,
                OperationId::new(self.epoch, sequence),
                OperationKind::CreateJobWithInput,
                body,
            )
            .result
        }

        fn assert_consistent(&self) {
            let problems =
                relationship_inconsistencies(&self.database).expect("scan relationships");
            assert!(
                problems.is_empty(),
                "relationship inconsistencies: {problems:#?}"
            );
        }
    }

    fn new_job(
        name: &str,
        parent: JobId,
        dependencies: Vec<(JobId, DependencyCondition)>,
    ) -> NewJob {
        NewJob {
            name: Some(name.to_owned()),
            execution_mode: ExecutionMode::Once,
            eligible_at: None,
            scheduled_at: None,
            next_due_at: None,
            next_iteration_no: None,
            iteration_no: None,
            parent,
            projects: Vec::new(),
            dependencies,
            configuration: None,
            assigned_executor: None,
            previous: None,
            replaces: None,
        }
    }

    fn create_job(test: &TestDatabase, sequence: u64, job: NewJob) -> Result<JobId, BatchFailure> {
        test.batch(sequence, |batch| {
            let (input, _) = persistence::save_job_input(
                batch,
                None,
                JobInputDraft {
                    objective: job.name.clone(),
                    ..JobInputDraft::default()
                },
            )?;
            persistence::create_job_with_input(batch, job, input)
        })
    }

    #[test]
    fn poisoned_batch_rolls_back_records_sequences_and_marker() {
        let test = TestDatabase::new("poison");
        let outcome = run_batch(
            &test.database,
            OperationId::new(test.epoch, 1),
            OperationKind::SaveJobInput,
            |batch| {
                let (input, _) = persistence::save_job_input(
                    batch,
                    None,
                    JobInputDraft {
                        objective: Some("must roll back".into()),
                        ..JobInputDraft::default()
                    },
                )?;
                let _ = batch.reject::<()>(Violation::ValueOutOfRange("poisoned test"));
                Ok((
                    input,
                    MarkerSubjects::created(RecordRef::JobInput(input), RecordRef::JobInput(input)),
                ))
            },
        );

        assert!(matches!(
            outcome.result,
            Err(BatchFailure::Rejected(Violation::ValueOutOfRange(
                "poisoned test"
            )))
        ));
        // The Genesis Job's input took the first input ID.
        let rolled_back = JobInputId::new(2);
        assert!(
            read_job_input(&test.database, rolled_back)
                .expect("read rolled-back input")
                .is_none()
        );
        assert!(
            read_marker(&test.database, OperationId::new(test.epoch, 1))
                .expect("read rolled-back marker")
                .is_none()
        );

        let committed = run_batch(
            &test.database,
            OperationId::new(test.epoch, 2),
            OperationKind::SaveJobInput,
            |batch| {
                let (input, subjects) =
                    persistence::save_job_input(batch, None, JobInputDraft::default())?;
                Ok((input, subjects))
            },
        )
        .result
        .expect("unpoisoned batch commits");
        assert_eq!(
            committed, rolled_back,
            "the rolled-back sequence allocation must be reusable"
        );
        assert!(
            read_marker(&test.database, OperationId::new(test.epoch, 2))
                .expect("read committed marker")
                .is_some()
        );
    }

    #[test]
    fn waiting_cycle_is_rejected_atomically() {
        let test = TestDatabase::new("cycle");
        let first = create_job(&test, 1, new_job("first", test.genesis, Vec::new()))
            .expect("create first job");
        let second =
            create_job(&test, 2, new_job("second", first, Vec::new())).expect("create second job");

        let cycle = create_job(
            &test,
            3,
            new_job("cycle", second, vec![(first, DependencyCondition::Success)]),
        );
        match cycle {
            Err(BatchFailure::Rejected(Violation::WaitingCycle(jobs))) => assert!(!jobs.is_empty()),
            other => panic!("expected waiting-cycle rejection, got {other:?}"),
        }

        let jobs = read_jobs(&test.database, None, 100).expect("read jobs after rejection");
        assert_eq!(
            jobs.len(),
            3,
            "the cyclic job and its input must not commit beside Genesis and the two jobs"
        );
        let third = create_job(&test, 4, new_job("third", test.genesis, Vec::new()))
            .expect("create after rejection");
        assert_eq!(
            third.get(),
            second.get() + 1,
            "the rejected transaction must roll back its job sequence"
        );
        test.assert_consistent();
    }

    #[test]
    fn genesis_is_created_once_and_reused_after_reopening() {
        let test = TestDatabase::new("genesis");
        let genesis = test.genesis;
        let record = read_job(&test.database, genesis)
            .expect("read Genesis")
            .expect("Genesis exists");
        assert_eq!(record.parent, genesis, "Genesis is its own parent");
        assert!(
            read_job_input(&test.database, record.input)
                .expect("read Genesis input")
                .is_some_and(|input| input.submitted_at.is_some())
        );
        assert!(
            read_children(&test.database, genesis, None, 100)
                .expect("read children")
                .is_empty(),
            "the root marker is not a child"
        );
        let child = create_job(&test, 1, new_job("child", genesis, Vec::new()))
            .expect("create child of Genesis");
        test.assert_consistent();

        let test = test.reopen();
        assert_eq!(
            test.genesis, genesis,
            "reopening reuses the recorded Genesis Job"
        );
        let roots: Vec<_> = read_jobs(&test.database, None, 100)
            .expect("read jobs")
            .into_iter()
            .filter(|job| job.parent == job.id)
            .map(|job| job.id)
            .collect();
        assert_eq!(roots, [genesis], "no second root is created on reopen");
        assert_eq!(
            read_children(&test.database, genesis, None, 100).expect("read children"),
            [child]
        );
        test.assert_consistent();
    }

    #[test]
    fn parent_edges_are_validated_fixed_and_acyclic_without_partial_writes() {
        let test = TestDatabase::new("parents");
        let parent = create_job(&test, 1, new_job("parent", test.genesis, Vec::new()))
            .expect("create parent");
        let child =
            create_job(&test, 2, new_job("child", parent, Vec::new())).expect("create child");
        let other =
            create_job(&test, 3, new_job("other", test.genesis, Vec::new())).expect("create other");

        let missing = JobId::new(999);
        match create_job(&test, 4, new_job("orphan", missing, Vec::new())) {
            Err(BatchFailure::Rejected(Violation::NotFound(RecordRef::Job(id)))) => {
                assert_eq!(id, missing)
            }
            other => panic!("expected a missing parent to be refused, got {other:?}"),
        }
        type Expected = fn(&Violation) -> bool;
        let attempts: [(JobId, JobId, Expected); 4] = [
            (other, other, |violation| {
                matches!(violation, Violation::SelfParent(_))
            }),
            (child, other, |violation| {
                matches!(violation, Violation::ParentFixed(_))
            }),
            (parent, child, |violation| {
                matches!(violation, Violation::ParentCycle { .. })
            }),
            (test.genesis, other, |violation| {
                matches!(violation, Violation::ParentCycle { .. })
            }),
        ];
        for (sequence, (from, to, expected)) in (5..).zip(attempts) {
            let outcome = test.batch(sequence, |batch| {
                batch.link_child(from, to)?;
                Ok(((), MarkerSubjects::addressed(RecordRef::Job(from))))
            });
            match outcome {
                Err(BatchFailure::Rejected(violation)) => assert!(
                    expected(&violation),
                    "{from} -> {to}: unexpected {violation:?}"
                ),
                other => panic!("{from} -> {to} must be refused, got {other:?}"),
            }
        }

        for (job, parent) in [
            (parent, test.genesis),
            (child, parent),
            (other, test.genesis),
            (test.genesis, test.genesis),
        ] {
            assert_eq!(
                read_job(&test.database, job)
                    .expect("read job")
                    .expect("job exists")
                    .parent,
                parent
            );
        }
        assert_eq!(
            read_jobs(&test.database, None, 100)
                .expect("read jobs")
                .len(),
            4,
            "no orphan was committed"
        );
        assert_eq!(
            read_children(&test.database, other, None, 100).expect("read children"),
            Vec::<JobId>::new()
        );
        test.assert_consistent();
    }

    #[test]
    fn insert_paths_cannot_rebind_a_job_profile_or_executor() {
        use crate::persistence::{
            AgentConfig, ExecutionConfiguration, LlmConfig, LlmParameters, LlmTransport,
            NewExecutor, NewLlmProvider, NewProfile, NewProfileConfig,
        };
        let test = TestDatabase::new("configuration");
        let (first, second, agent, other_agent, job, executor) = test
            .batch(1, |batch| {
                let provider = NewLlmProvider {
                    name: "p".into(),
                    transport: LlmTransport::OpenAiChatCompletions,
                    endpoint: None,
                    credential_ref: None,
                };
                let (provider, _) = persistence::create_llm_provider(batch, provider)?;
                let mut llm = |model: &str| {
                    let config = LlmConfig {
                        model: model.into(),
                        parameters: LlmParameters::default(),
                        declared_capabilities: BTreeSet::new(),
                    };
                    persistence::create_profile(
                        batch,
                        NewProfile {
                            name: model.into(),
                            config: NewProfileConfig::Llm { provider, config },
                        },
                    )
                    .map(|(id, _)| id)
                };
                let (first, second) = (llm("first")?, llm("second")?);
                let mut agent = |llm_targets| {
                    let config = NewProfileConfig::Agent {
                        config: AgentConfig::default(),
                        llm_targets,
                    };
                    persistence::create_profile(
                        batch,
                        NewProfile {
                            name: "agent".into(),
                            config,
                        },
                    )
                    .map(|(id, _)| id)
                };
                let (agent, other_agent) = (agent(vec![first, second])?, agent(vec![second])?);
                let (executor, _) = persistence::create_executor(
                    batch,
                    NewExecutor {
                        name: "llm".into(),
                        executor_type: ExecutorType::Llm,
                        concurrency_limit: std::num::NonZeroU32::MIN,
                        llm_transports: BTreeSet::from([LlmTransport::OpenAiChatCompletions]),
                    },
                )?;
                let (input, _) =
                    persistence::save_job_input(batch, None, JobInputDraft::default())?;
                let configuration = Some(ExecutionConfiguration {
                    agent_profile: Some(agent),
                    llm_profile: first,
                });
                let (job, _) = persistence::create_job_with_input(
                    batch,
                    NewJob {
                        configuration,
                        ..new_job("child", test.genesis, Vec::new())
                    },
                    input,
                )?;
                Ok((
                    (first, second, agent, other_agent, job, executor),
                    MarkerSubjects::addressed(RecordRef::Job(job)),
                ))
            })
            .expect("create configuration");
        let before = (
            read_job(&test.database, job).expect("read job"),
            read_profile(&test.database, agent).expect("read Agent Profile"),
            read_profile(&test.database, first).expect("read LLM Profile"),
            read_executor(&test.database, executor).expect("read Executor"),
        );

        // Every rebinding names Profiles that exist and would be valid on a new Job.
        for (sequence, rebinding) in (2..).zip([
            ExecutionConfiguration {
                agent_profile: Some(agent),
                llm_profile: second,
            },
            ExecutionConfiguration {
                agent_profile: Some(other_agent),
                llm_profile: second,
            },
            ExecutionConfiguration {
                agent_profile: None,
                llm_profile: second,
            },
        ]) {
            let outcome = test.batch(sequence, |batch| {
                batch.link_configuration(job, rebinding)?;
                Ok(((), MarkerSubjects::addressed(RecordRef::Job(job))))
            });
            assert!(
                matches!(
                    outcome,
                    Err(BatchFailure::Rejected(Violation::ConfigurationFixed(_)))
                ),
                "got {outcome:?}"
            );
        }
        let readd = test.batch(5, |batch| {
            let row = ProfileRow {
                id: agent,
                name: "agent".into(),
                config: ProfileConfig::Agent(AgentConfig::default()),
                created_at: batch.now(),
                updated_at: batch.now(),
            };
            batch.insert_profile(
                &row,
                &ProfileLinks::Agent {
                    llm_targets: vec![second],
                },
            )?;
            Ok(((), MarkerSubjects::addressed(RecordRef::Profile(agent))))
        });
        assert!(
            matches!(
                readd,
                Err(BatchFailure::Rejected(Violation::DuplicateId(
                    RecordRef::Profile(_)
                )))
            ),
            "got {readd:?}"
        );
        let reregister = test.batch(6, |batch| {
            let row = batch.require_executor_row(executor)?;
            batch.insert_executor(&ExecutorRow {
                llm_transports: BTreeSet::new(),
                ..row
            })?;
            Ok(((), MarkerSubjects::addressed(RecordRef::Executor(executor))))
        });
        assert!(
            matches!(
                reregister,
                Err(BatchFailure::Rejected(Violation::DuplicateId(
                    RecordRef::Executor(_)
                )))
            ),
            "got {reregister:?}"
        );

        let after = (
            read_job(&test.database, job).expect("read job"),
            read_profile(&test.database, agent).expect("read Agent Profile"),
            read_profile(&test.database, first).expect("read LLM Profile"),
            read_executor(&test.database, executor).expect("read Executor"),
        );
        assert_eq!(after, before, "refused writes changed nothing");
        test.assert_consistent();
    }

    #[test]
    fn a_file_of_another_format_version_is_refused_before_any_write() {
        let test = TestDatabase::new("format");
        let path = database_file(&test.root.0);
        let write_meta = |database: &Database, version: u32| {
            let tx = database.begin_write().expect("begin write");
            tx.open_table(META)
                .expect("open meta")
                .insert("format_version", version.to_le_bytes().as_slice())
                .expect("write version");
            tx.commit().expect("commit version");
        };
        // The version before this build's, which is refused like any other.
        let previous = FORMAT_VERSION - 1;
        write_meta(&test.database, previous);
        let state = |database: &Database| {
            let rx = database.begin_read().expect("begin read");
            let epoch = rx
                .open_table(SEQUENCES)
                .expect("open sequences")
                .get(OPEN_EPOCH)
                .expect("read epoch")
                .map(|v| v.value());
            let version = rx
                .open_table(META)
                .expect("open meta")
                .get("format_version")
                .expect("read version")
                .map(|v| v.value().to_vec());
            let jobs = rx
                .open_table(JOBS)
                .expect("open jobs")
                .len()
                .expect("count jobs");
            (epoch, version, jobs)
        };
        let before = state(&test.database);
        let TestDatabase { root, database, .. } = test;
        drop(database);
        let bytes = fs::read(&path).expect("read the file");

        match open(&root.0) {
            Err(OpenError::UnsupportedFormat { found }) => assert_eq!(found, previous),
            Err(_) => panic!("expected an unsupported-format refusal"),
            Ok(_) => panic!("a file of the previous format must not open"),
        }
        assert!(
            fs::read(&path).expect("reread the file") == bytes,
            "the refused file is byte-for-byte unchanged"
        );
        let database = Database::create(&path).expect("inspect the refused file");
        assert_eq!(
            state(&database),
            before,
            "the refused open must not change the file"
        );
        write_meta(&database, FORMAT_VERSION);
        drop(database);
        assert!(
            open(&root.0).is_ok(),
            "the untouched file still opens at the supported version"
        );
    }

    fn retry(original: JobId, acknowledge_repeated_effects: bool) -> persistence::JobRetry {
        persistence::JobRetry {
            original,
            input: persistence::RetryInput::Unchanged,
            name: None,
            execution_mode: ExecutionMode::Once,
            eligible_at: None,
            scheduled_at: None,
            next_due_at: None,
            next_iteration_no: None,
            iteration_no: None,
            projects: Vec::new(),
            dependencies: Vec::new(),
            configuration: None,
            assigned_executor: None,
            acknowledge_repeated_effects,
        }
    }

    fn fail(test: &TestDatabase, sequence: u64, job: JobId) {
        test.batch(sequence, |batch| {
            persistence::finalize_job(
                batch,
                job,
                persistence::JobFinalization::Failed(persistence::FinalResult {
                    summary: None,
                    code: None,
                    goal_reached: Some(false),
                    value: None,
                }),
            )
        })
        .expect("fail the Job");
    }

    /// Three Jobs created as they would be and linked by `edges` as
    /// (retry, original) directly.
    fn retry_chain(test: &TestDatabase, edges: &[(usize, usize)]) -> [JobId; 3] {
        let jobs = [1, 2, 3].map(|sequence| {
            create_job(test, sequence, new_job("job", test.genesis, Vec::new())).expect("create")
        });
        test.batch(4, |batch| {
            for (retry, original) in edges {
                batch.link_retry(jobs[*retry], jobs[*original])?;
            }
            Ok(((), MarkerSubjects::addressed(RecordRef::Job(jobs[0]))))
        })
        .expect("link the retries");
        jobs
    }

    #[test]
    fn a_retry_is_refused_over_an_unresolved_effect_anywhere_in_its_lineage() {
        let test = TestDatabase::new("retry-lineage-unresolved");
        // The first Job's effect is unresolved; the two retries after it
        // ran no tool and failed.
        let [first, second, third] = retry_chain(&test, &[(1, 0), (2, 1)]);
        let call = ToolCallRef {
            job: first,
            request: RequestNumber::FIRST,
            index: 0,
        };
        test.batch(8, |batch| {
            batch.insert_tool_evidence(&ToolEvidenceRecord {
                call,
                tool_call_id: "c".into(),
                name: "add".into(),
                decision: Recorded {
                    fact: ToolDecision::Admitted(ToolAdmission {
                        handler_version: "test-1".into(),
                        effect: ToolEffect::NonIdempotent,
                        parameters: "{}".into(),
                        arguments: ToolPayload::new("{}"),
                        authorization: AuthorizationBasis::RegistryGrant,
                    }),
                    at: batch.now(),
                },
                outcome: None,
                reconciliation: None,
            })?;
            Ok(((), MarkerSubjects::addressed(RecordRef::Job(first))))
        })
        .expect("record the unresolved admission");
        test.batch(9, |batch| {
            persistence::finalize_job(batch, first, persistence::JobFinalization::Unknown)?;
            persistence::confirm_job_stopped(batch, first)
        })
        .expect("settle the first Job unknown");
        fail(&test, 10, second);
        fail(&test, 11, third);
        for acknowledge in [false, true] {
            assert!(
                matches!(
                    test.batch(12, |batch| persistence::retry_job(batch, retry(third, acknowledge))),
                    Err(BatchFailure::Rejected(Violation::UnresolvedToolEffect(found))) if found == call
                ),
                "acknowledged: {acknowledge}"
            );
        }
        test.assert_consistent();
    }

    #[test]
    fn a_malformed_retry_lineage_refuses_the_retry() {
        // A cycle of retries.
        let test = TestDatabase::new("retry-lineage-cycle");
        let [first, second, _] = retry_chain(&test, &[(0, 1), (1, 0)]);
        fail(&test, 5, first);
        fail(&test, 6, second);
        assert!(matches!(
            test.batch(9, |batch| persistence::retry_job(batch, retry(first, true))),
            Err(BatchFailure::NotCommitted(error)) if error.kind == StorageErrorKind::Corrupted
        ));
        assert!(
            test.batch(10, |batch| batch.retries(first).map(|retries| (
                retries,
                MarkerSubjects::addressed(RecordRef::Job(first))
            )))
            .expect("read the retries")
                == [second],
            "no retry was added"
        );

        // A retry of a Job that is gone.
        let test = TestDatabase::new("retry-lineage-missing");
        let [first, second, _] = retry_chain(&test, &[(1, 0)]);
        fail(&test, 5, second);
        remove_job_row(&test.database, first).expect("remove the original's row");
        assert!(matches!(
            test.batch(9, |batch| persistence::retry_job(batch, retry(second, true))),
            Err(BatchFailure::NotCommitted(error)) if error.kind == StorageErrorKind::Corrupted
        ));
    }
}
