//! Contract tests for the persisted schema, driven through the database
//! worker exactly as callers use it: commands on the write thread, reads on
//! the read pool, and close/reopen of the same file.

use std::num::NonZeroU32;

use crate::data_directory::DataDirectory;
use crate::database::{DatabaseError, DatabaseHandle, DatabaseWorker};
use std::collections::BTreeSet;

use serde_json::json;

use crate::openai_compatible::{ChatFinishReason, NormalizedUsage, ProviderFailure};
use crate::orchestration::execution_runtime::{
    ExecutionPlan, ExecutionRuntime, ExecutionRuntimeError, settle_provider_error,
};
use crate::persistence::{
    AgentConfig, AvailabilityObservation, AvailabilityStatId, AvailabilityStatRecord,
    DependencyCondition, ExecutionConfiguration, ExecutionMode, ExecutorId, ExecutorType,
    FinalResult, IntermediateOutputKind, JobFinalization, JobId, JobInputDraft, JobOutcome,
    JobRecord, JobRetry, LlmConfig, LlmParameters, LlmProviderId, LlmTarget, LlmTransport,
    Measurements, MetricsRecord, ModelCapability, NewAgent, NewAvailabilityStat, NewExecutor,
    NewJob, NewJobOutput, NewLlmProvider, NewMetrics, NewProfile, NewProfileConfig, NewProject,
    ObservationCursor, ObservationFilter, OutputKind, ProfileId, ProfileKind, ProjectId, RecordRef,
    RetryInput, TimeRange, Timestamp, Violation, WrongProfileKind,
};
use crate::provider_execution::{ProviderExecutionError, ResponseFacts};
use crate::test_support::{TestRoot, data_directory as test_data_directory, poll_once, wait};

const SUITE: &str = "ocg-schema";
/// The transport of the only LLM adapter OCG implements.
const OPENAI: LlmTransport = LlmTransport::OpenAiChatCompletions;

fn data_directory(name: &str) -> (TestRoot, DataDirectory) {
    test_data_directory(SUITE, name)
}

fn new_job(name: &str, parent: JobId, projects: Vec<ProjectId>) -> NewJob {
    NewJob {
        name: Some(name.into()),
        execution_mode: ExecutionMode::Once,
        eligible_at: None,
        scheduled_at: None,
        next_due_at: None,
        next_iteration_no: None,
        iteration_no: None,
        parent,
        projects,
        dependencies: Vec::new(),
        configuration: None,
        assigned_executor: None,
        previous: None,
        replaces: None,
    }
}

fn create_job(handle: &DatabaseHandle, job: NewJob) -> JobId {
    let input = wait(handle.save_job_input(
        None,
        JobInputDraft {
            objective: job.name.clone(),
            ..JobInputDraft::default()
        },
    ))
    .expect("save input");
    wait(handle.create_job_with_input(job, input)).expect("create job")
}

fn violation<T: std::fmt::Debug>(result: Result<T, DatabaseError>) -> Violation {
    match result {
        Err(DatabaseError::Rejected(violation)) => violation,
        other => panic!("expected a rule violation, got {other:?}"),
    }
}

fn result(summary: &str) -> FinalResult {
    FinalResult {
        summary: Some(summary.into()),
        code: None,
        goal_reached: None,
        value: None,
    }
}

fn project(handle: &DatabaseHandle, name: &str) -> ProjectId {
    wait(handle.create_project(NewProject {
        name: name.into(),
        description: None,
    }))
    .expect("create project")
}

fn provider(handle: &DatabaseHandle, name: &str) -> LlmProviderId {
    wait(handle.create_llm_provider(NewLlmProvider {
        name: name.into(),
        transport: OPENAI,
        endpoint: Some(format!("https://{name}.invalid/v1")),
        credential_ref: Some(format!("{}_KEY", name.to_uppercase())),
    }))
    .expect("create provider")
}

fn llm_config(model: &str, declared: &[ModelCapability]) -> LlmConfig {
    LlmConfig {
        model: model.into(),
        parameters: LlmParameters {
            temperature: Some(0.2),
            max_tokens: Some(256),
            ..LlmParameters::default()
        },
        declared_capabilities: declared.iter().copied().collect(),
    }
}

fn llm_profile(handle: &DatabaseHandle, provider: LlmProviderId, config: LlmConfig) -> ProfileId {
    wait(handle.create_profile(NewProfile {
        name: config.model.clone(),
        config: NewProfileConfig::Llm { provider, config },
    }))
    .expect("create LLM Profile")
}

fn agent_profile(
    handle: &DatabaseHandle,
    instructions: &str,
    llm_targets: Vec<ProfileId>,
) -> ProfileId {
    wait(handle.create_profile(NewProfile {
        name: "assistant".into(),
        config: NewProfileConfig::Agent {
            config: AgentConfig {
                instructions: Some(instructions.into()),
            },
            llm_targets,
        },
    }))
    .expect("create Agent Profile")
}

fn provider_failure(message: &str, status_code: Option<u16>, retryable: bool) -> ProviderFailure {
    ProviderFailure {
        message: message.into(),
        status_code,
        provider_code: status_code.map(|_| "invalid_api_key".into()),
        response_body: None,
        request_id: Some("request-1".into()),
        retry_after_ms: None,
        retryable,
    }
}

fn at(micros: i64) -> Timestamp {
    Timestamp::from_unix_micros(micros)
}

fn observed_between(from: i64, until: i64) -> ObservationFilter {
    ObservationFilter {
        observed: TimeRange {
            from: at(from),
            until: at(until),
        },
        llm_profile: None,
        provider: None,
        model: None,
    }
}

fn all_time() -> ObservationFilter {
    observed_between(i64::MIN, i64::MAX)
}

fn of_profile(llm_profile: ProfileId) -> ObservationFilter {
    ObservationFilter {
        llm_profile: Some(llm_profile),
        ..all_time()
    }
}

/// Reads every page of a query with `page` records each, checking that
/// every cursor moves forward.
fn every_page<T, I: Copy + Ord + std::fmt::Debug>(
    mut read: impl FnMut(Option<ObservationCursor<I>>) -> crate::persistence::ObservationPage<T, I>,
) -> Vec<T> {
    let (mut records, mut after) = (Vec::new(), None);
    for _ in 0..10_000 {
        let page = read(after);
        records.extend(page.records);
        match page.next {
            None => return records,
            Some(next) => {
                if let Some(previous) = after {
                    assert!(
                        (next.observed_at, next.record) > (previous.observed_at, previous.record),
                        "the cursor must advance"
                    );
                }
                after = Some(next);
            }
        }
    }
    panic!("the query did not finish");
}

fn all_metrics(
    handle: &DatabaseHandle,
    filter: &ObservationFilter,
    page: usize,
) -> Vec<MetricsRecord> {
    every_page(|after| wait(handle.metrics(filter.clone(), after, page)).expect("read Metrics"))
}

fn all_availability(
    handle: &DatabaseHandle,
    filter: &ObservationFilter,
    page: usize,
) -> Vec<AvailabilityStatRecord> {
    every_page(|after| {
        wait(handle.availability_stats(filter.clone(), after, page))
            .expect("read AvailabilityStats")
    })
}

fn response(status_code: u16) -> AvailabilityObservation {
    AvailabilityObservation {
        status_code: Some(status_code),
        finish_reason: Some(ChatFinishReason::Stop),
        raw_finish_reason: Some("stop".into()),
        failure: None,
    }
}

fn failure(failure: ProviderFailure) -> AvailabilityObservation {
    AvailabilityObservation {
        status_code: failure.status_code,
        finish_reason: None,
        raw_finish_reason: None,
        failure: Some(failure),
    }
}

fn assert_relationships_consistent(directory: &DataDirectory) {
    let opened = crate::storage::open(directory.database_path())
        .unwrap_or_else(|_| panic!("open the closed database file"));
    let problems =
        crate::storage::relationship_inconsistencies(&opened.database).expect("scan relationships");
    assert!(
        problems.is_empty(),
        "relationship inconsistencies: {problems:#?}"
    );
}

#[test]
fn nodes_and_relationships_round_trip_through_reopening() {
    let (_root, directory) = data_directory("round-trip");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let genesis = handle.genesis();
    let alpha = project(&handle, "alpha");
    let beta = project(&handle, "beta");
    let first_provider = provider(&handle, "first");
    let second_provider = provider(&handle, "second");
    let llm = llm_profile(
        &handle,
        first_provider,
        llm_config("model-a", &[ModelCapability::Chat, ModelCapability::Tools]),
    );
    let assistant = agent_profile(&handle, "Answer briefly.", Vec::new());
    let agent = wait(handle.create_agent(NewAgent {
        name: "helper".into(),
        profiles: vec![assistant],
    }))
    .expect("create Agent");
    let executor = wait(handle.create_executor(NewExecutor {
        name: "worker".into(),
        executor_type: ExecutorType::Llm,
        concurrency_limit: NonZeroU32::MIN,
        llm_transports: BTreeSet::from([OPENAI]),
    }))
    .expect("create Executor");
    let parent = create_job(
        &handle,
        NewJob {
            assigned_executor: Some(executor),
            ..new_job("parent", genesis, vec![alpha, beta])
        },
    );
    let child = create_job(&handle, new_job("child", parent, Vec::new()));
    let dependent = create_job(
        &handle,
        NewJob {
            dependencies: vec![(parent, DependencyCondition::Settled)],
            ..new_job("dependent", genesis, vec![beta])
        },
    );
    wait(handle.append_job_output(
        parent,
        NewJobOutput {
            kind: IntermediateOutputKind::Progress,
            summary: Some("halfway".into()),
            code: None,
            goal_reached: None,
            value: None,
        },
    ))
    .expect("append output");
    let check = create_job(
        &handle,
        NewJob {
            configuration: llm_only(llm),
            assigned_executor: Some(executor),
            ..new_job("chat check", genesis, Vec::new())
        },
    );
    wait(handle.finalize_job(
        check,
        JobFinalization::Completed(FinalResult {
            value: Some(json!({"capability": "chat", "observed": "supported"})),
            ..result("chat answered")
        }),
    ))
    .expect("finalize check");
    let measured = wait(handle.record_metrics(NewMetrics {
        job: check,
        observed_at: at(1_000),
        measurements: Measurements {
            ttfb_ms: Some(120),
            usage: NormalizedUsage {
                input_tokens: Some(12),
                output_tokens: Some(3),
                raw: Some(json!({"prompt_tokens": 12, "completion_tokens": 3})),
                ..NormalizedUsage::default()
            },
            ..Measurements::default()
        },
    }))
    .expect("record Metrics");
    let observed = wait(handle.record_availability_stat(NewAvailabilityStat {
        job: check,
        observed_at: at(1_000),
        observation: response(200),
    }))
    .expect("record AvailabilityStat");

    let read_all = |handle: &DatabaseHandle| {
        (
            wait(handle.jobs(None, 100)).expect("read jobs"),
            wait(handle.job_outputs(parent, None, 100)).expect("read outputs"),
            wait(handle.projects(None, 100)).expect("read projects"),
            wait(handle.project_jobs(beta, None, 100)).expect("read Project Jobs"),
            wait(handle.llm_providers(None, 100)).expect("read providers"),
            wait(handle.profiles(None, 100)).expect("read Profiles"),
            wait(handle.agent(agent)).expect("read Agent"),
            wait(handle.job_metrics(check, None, 100)).expect("read Metrics"),
            wait(handle.children(parent, None, 100)).expect("read children"),
            wait(handle.provider_profiles(first_provider)).expect("read provider Profiles"),
            wait(handle.job_availability(check, None, 100)).expect("read AvailabilityStats"),
            wait(handle.job_outputs(check, None, 100)).expect("read check outputs"),
        )
    };
    let before = read_all(&handle);
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    let handle = worker.handle();
    assert_eq!(handle.genesis(), genesis);
    let after = read_all(&handle);
    assert_eq!(
        after, before,
        "every node and relationship reads back the same after reopening"
    );

    let (
        jobs,
        outputs,
        projects,
        beta_jobs,
        providers,
        profiles,
        agent_record,
        metrics,
        children,
        provider_profiles,
        availability,
        check_outputs,
    ) = after;
    assert_eq!(jobs.len(), 5, "Genesis and the four created Jobs");
    let parent_record = jobs
        .iter()
        .find(|job| job.id == parent)
        .expect("parent persisted");
    assert_eq!(
        (parent_record.parent, parent_record.projects.clone()),
        (genesis, vec![alpha, beta])
    );
    assert_eq!(parent_record.assigned_executor, Some(executor));
    assert_eq!(outputs.len(), 1);
    assert_eq!(projects.len(), 2);
    assert_eq!(beta_jobs, [parent, dependent]);
    assert_eq!(
        providers.len(),
        2,
        "the schema itself does not limit the number of Providers"
    );
    for kind in [ProfileKind::Agent, ProfileKind::Llm] {
        assert!(
            profiles.iter().any(|profile| profile.config.kind() == kind),
            "a {kind} Profile is in the Profile table"
        );
    }
    assert_eq!(agent_record.expect("Agent persisted").profiles, [assistant]);
    assert_eq!(
        metrics.iter().map(|metrics| metrics.id).collect::<Vec<_>>(),
        [measured]
    );
    assert_eq!(metrics[0].measurements.usage.input_tokens, Some(12));
    assert_eq!(
        availability.iter().map(|stat| stat.id).collect::<Vec<_>>(),
        [observed]
    );
    assert_eq!(availability[0].target.llm_profile, llm);
    assert_eq!(
        check_outputs[0].value,
        Some(json!({"capability": "chat", "observed": "supported"}))
    );
    assert_eq!(children, [child]);
    assert_eq!(provider_profiles, [llm]);
    assert!(
        wait(handle.provider_profiles(second_provider))
            .expect("read provider Profiles")
            .is_empty()
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn genesis_is_one_protected_root_that_is_never_its_own_child() {
    let (_root, directory) = data_directory("genesis");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let genesis = handle.genesis();
    let record = wait(handle.job(genesis))
        .expect("read Genesis")
        .expect("Genesis exists");
    assert_eq!(
        record.parent, genesis,
        "the root marker: Genesis is its own parent"
    );
    assert!(
        wait(handle.children(genesis, None, 100))
            .expect("read children")
            .is_empty()
    );

    let top = create_job(&handle, new_job("top", genesis, Vec::new()));
    assert_eq!(
        wait(handle.children(genesis, None, 100)).expect("read children"),
        [top]
    );

    for finalization in [
        JobFinalization::Cancelled(result("stop everything")),
        JobFinalization::Completed(result("done")),
        JobFinalization::Failed(result("broken")),
        JobFinalization::Unknown,
    ] {
        assert_eq!(
            violation(wait(handle.finalize_job(genesis, finalization))),
            Violation::GenesisProtected(genesis)
        );
    }
    let retry = JobRetry {
        original: genesis,
        input: RetryInput::Unchanged,
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
    };
    assert_eq!(
        violation(wait(handle.retry_job(retry))),
        Violation::GenesisProtected(genesis)
    );
    assert_eq!(
        wait(handle.job(genesis)).expect("read Genesis"),
        Some(record.clone()),
        "refusals left Genesis untouched"
    );
    assert!(
        wait(handle.job_outputs(genesis, None, 10))
            .expect("read outputs")
            .is_empty()
    );

    // Genesis waits for its ordinary children only, never for itself.
    wait(handle.finalize_job(top, JobFinalization::Completed(result("done"))))
        .expect("finalize a child of Genesis");
    worker.shutdown().expect("shutdown");

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    let handle = worker.handle();
    assert_eq!(
        handle.genesis(),
        genesis,
        "the persisted Genesis identity is reused"
    );
    let roots: Vec<_> = wait(handle.jobs(None, 100))
        .expect("read jobs")
        .into_iter()
        .filter(|job| job.parent == job.id)
        .map(|job| job.id)
        .collect();
    assert_eq!(roots, [genesis], "exactly one Job is its own parent");
    worker.shutdown().expect("shutdown");
}

#[test]
fn job_creation_refuses_missing_endpoints_without_partial_writes() {
    let (_root, directory) = data_directory("endpoints");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let genesis = handle.genesis();
    let existing = project(&handle, "existing");
    let input = wait(handle.save_job_input(
        None,
        JobInputDraft {
            objective: Some("try".into()),
            ..JobInputDraft::default()
        },
    ))
    .expect("save input");
    let jobs_before = wait(handle.jobs(None, 100)).expect("read jobs");

    let missing_job = JobId::new(9_999);
    let missing_project = ProjectId::new(9_999);
    let attempts = [
        (
            new_job("orphan", missing_job, Vec::new()),
            Violation::NotFound(RecordRef::Job(missing_job)),
        ),
        (
            new_job("lost", genesis, vec![existing, missing_project]),
            Violation::NotFound(RecordRef::Project(missing_project)),
        ),
        (
            NewJob {
                dependencies: vec![(missing_job, DependencyCondition::Success)],
                ..new_job("waits", genesis, Vec::new())
            },
            Violation::NotFound(RecordRef::Job(missing_job)),
        ),
        (
            new_job("twice", genesis, vec![existing, existing]),
            Violation::DuplicateReference(RecordRef::Project(existing)),
        ),
    ];
    for (job, expected) in attempts {
        assert_eq!(
            violation(wait(handle.create_job_with_input(job, input))),
            expected
        );
    }

    assert_eq!(
        wait(handle.jobs(None, 100)).expect("read jobs"),
        jobs_before,
        "no Job was committed"
    );
    assert!(
        wait(handle.project_jobs(existing, None, 100))
            .expect("read Project Jobs")
            .is_empty()
    );
    let input_record = wait(handle.job_input(input))
        .expect("read input")
        .expect("input exists");
    assert_eq!(
        input_record.submitted_at, None,
        "the input was not frozen by a refused submission"
    );
    wait(handle.save_job_input(Some(input), JobInputDraft::default()))
        .expect("the input is still editable");
    worker.shutdown().expect("shutdown");
}

#[test]
fn project_sets_are_independent_of_the_parent_tree() {
    let (_root, directory) = data_directory("projects");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let genesis = handle.genesis();
    let (alpha, beta, gamma) = (
        project(&handle, "alpha"),
        project(&handle, "beta"),
        project(&handle, "gamma"),
    );

    let parent = create_job(&handle, new_job("parent", genesis, vec![alpha, beta]));
    let unassociated = create_job(&handle, new_job("unassociated", parent, Vec::new()));
    let elsewhere = create_job(&handle, new_job("elsewhere", parent, vec![gamma]));
    let projects_of = |job| {
        wait(handle.job(job))
            .expect("read job")
            .expect("job exists")
            .projects
    };
    assert_eq!(projects_of(parent), [alpha, beta]);
    assert!(
        projects_of(unassociated).is_empty(),
        "a child does not inherit its parent's Projects"
    );
    assert_eq!(projects_of(elsewhere), [gamma]);

    wait(handle.associate_job_project(unassociated, alpha)).expect("associate later");
    assert_eq!(projects_of(unassociated), [alpha]);
    assert_eq!(
        projects_of(parent),
        [alpha, beta],
        "associating a child leaves its parent's set alone"
    );
    assert_eq!(
        violation(wait(handle.associate_job_project(unassociated, alpha))),
        Violation::AlreadyInProject {
            job: unassociated,
            project: alpha
        }
    );
    let missing = ProjectId::new(9_999);
    assert_eq!(
        violation(wait(handle.associate_job_project(unassociated, missing))),
        Violation::NotFound(RecordRef::Project(missing))
    );
    assert_eq!(
        wait(handle.project_jobs(alpha, None, 100)).expect("read Project Jobs"),
        [parent, unassociated]
    );
    assert_eq!(
        wait(handle.project_jobs(gamma, None, 100)).expect("read Project Jobs"),
        [elsewhere]
    );
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

#[test]
fn both_profile_kinds_share_one_table_and_typed_reads_refuse_the_other_kind() {
    let (_root, directory) = data_directory("profiles");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let llm_provider = provider(&handle, "first");
    let llm = llm_profile(
        &handle,
        llm_provider,
        llm_config("model-a", &[ModelCapability::Chat]),
    );
    let assistant = agent_profile(&handle, "Answer briefly.", Vec::new());

    let all = wait(handle.profiles(None, 100)).expect("read Profiles");
    assert_eq!(
        all.iter()
            .map(|profile| (profile.id, profile.config.kind()))
            .collect::<Vec<_>>(),
        [(assistant, ProfileKind::Agent), (llm, ProfileKind::Llm)]
    );
    let typed_llm = wait(handle.llm_profile(llm))
        .expect("read LLM Profile")
        .expect("LLM Profile exists");
    assert_eq!(
        (typed_llm.config.model.as_str(), typed_llm.links),
        ("model-a", llm_provider)
    );
    let typed_agent = wait(handle.agent_profile(assistant))
        .expect("read Agent Profile")
        .expect("Agent Profile exists");
    assert_eq!(
        typed_agent.config.instructions.as_deref(),
        Some("Answer briefly.")
    );
    match wait(handle.agent_profile(llm)) {
        Err(DatabaseError::WrongProfileKind(mismatch)) => {
            assert_eq!(
                mismatch,
                WrongProfileKind {
                    profile: llm,
                    expected: ProfileKind::Agent,
                    found: ProfileKind::Llm
                }
            )
        }
        other => panic!("an LLM Profile is not an Agent Profile, got {other:?}"),
    }
    assert!(matches!(
        wait(handle.llm_profile(assistant)),
        Err(DatabaseError::WrongProfileKind(_))
    ));

    let wrong_kind = violation(wait(handle.create_agent(NewAgent {
        name: "confused".into(),
        profiles: vec![llm],
    })));
    assert_eq!(
        wrong_kind,
        Violation::WrongProfileKind(WrongProfileKind {
            profile: llm,
            expected: ProfileKind::Agent,
            found: ProfileKind::Llm
        })
    );
    let agent = wait(handle.create_agent(NewAgent {
        name: "helper".into(),
        profiles: vec![assistant],
    }))
    .expect("create Agent");
    assert_eq!(
        wait(handle.agent(agent))
            .expect("read Agent")
            .expect("Agent exists")
            .profiles,
        [assistant]
    );

    let missing = LlmProviderId::new(9_999);
    let refused = [
        (
            NewProfileConfig::Llm {
                provider: missing,
                config: llm_config("model-b", &[]),
            },
            Violation::NotFound(RecordRef::LlmProvider(missing)),
        ),
        (
            NewProfileConfig::Llm {
                provider: llm_provider,
                config: llm_config("", &[]),
            },
            Violation::InvalidValue("model", "cannot be empty"),
        ),
        (
            NewProfileConfig::Llm {
                provider: llm_provider,
                config: LlmConfig {
                    parameters: LlmParameters {
                        top_p: Some(f64::NAN),
                        ..LlmParameters::default()
                    },
                    ..llm_config("model-c", &[])
                },
            },
            Violation::InvalidValue("top_p", "must be a finite number"),
        ),
    ];
    for (config, expected) in refused {
        assert_eq!(
            violation(wait(handle.create_profile(NewProfile {
                name: "refused".into(),
                config
            }))),
            expected
        );
    }
    assert_eq!(
        violation(wait(handle.create_llm_provider(NewLlmProvider {
            name: "blank".into(),
            transport: OPENAI,
            endpoint: None,
            credential_ref: Some(String::new())
        }))),
        Violation::InvalidValue("credential_ref", "cannot be empty")
    );
    assert_eq!(
        wait(handle.profiles(None, 100))
            .expect("read Profiles")
            .len(),
        2,
        "refused Profiles were not stored"
    );
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

#[test]
fn capability_checks_are_ordinary_llm_only_jobs_kept_apart_from_declarations() {
    let (_root, directory) = data_directory("capability-checks");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let genesis = handle.genesis();
    let llm_provider = provider(&handle, "first");
    let llm = llm_profile(
        &handle,
        llm_provider,
        llm_config("model-a", &[ModelCapability::Chat]),
    );
    let declaration = wait(handle.profile(llm))
        .expect("read Profile")
        .expect("Profile exists");
    let checker =
        executor(&handle, "checker", ExecutorType::Llm, &[OPENAI]).expect("register Executor");
    let assistant = agent_profile(&handle, "Answer briefly.", Vec::new());
    let input = wait(handle.save_job_input(
        None,
        JobInputDraft {
            objective: Some("check tool calling".into()),
            ..JobInputDraft::default()
        },
    ))
    .expect("save input");

    let missing = ProfileId::new(9_999);
    let refused = [
        (
            llm_only(assistant),
            Violation::WrongProfileKind(WrongProfileKind {
                profile: assistant,
                expected: ProfileKind::Llm,
                found: ProfileKind::Agent,
            }),
        ),
        (
            llm_only(missing),
            Violation::NotFound(RecordRef::Profile(missing)),
        ),
        // An Agent Profile, when one is named, is still validated.
        (
            configuration(llm, llm),
            Violation::WrongProfileKind(WrongProfileKind {
                profile: llm,
                expected: ProfileKind::Agent,
                found: ProfileKind::Llm,
            }),
        ),
        (
            configuration(assistant, llm),
            Violation::NotAConfiguredTarget {
                agent_profile: assistant,
                llm_profile: llm,
            },
        ),
    ];
    let jobs_before = wait(handle.jobs(None, 100)).expect("read jobs");
    for (configuration, expected) in refused {
        let job = NewJob {
            configuration,
            assigned_executor: Some(checker),
            ..new_job("tools check", genesis, Vec::new())
        };
        assert_eq!(
            violation(wait(handle.create_job_with_input(job, input))),
            expected
        );
    }
    assert_eq!(
        wait(handle.jobs(None, 100)).expect("read jobs"),
        jobs_before,
        "no partial Job was committed"
    );
    assert!(
        wait(handle.profile_jobs(llm, None, 100))
            .expect("read Profile Jobs")
            .is_empty()
    );
    assert_eq!(
        wait(handle.job_input(input))
            .expect("read input")
            .expect("input exists")
            .configuration,
        None,
        "the input was neither frozen nor given a configuration"
    );

    // The server checks a capability the user never declared, with an LLM
    // Executor and no Agent or Agent Profile at all.
    let check = wait(handle.create_job_with_input(
        NewJob {
            configuration: llm_only(llm),
            assigned_executor: Some(checker),
            ..new_job("tools check", genesis, Vec::new())
        },
        input,
    ))
    .expect("submit an LLM-only check");
    let submitted = wait(handle.job_configuration(check))
        .expect("read configuration")
        .expect("the check is configured");
    assert_eq!(
        (
            submitted.agent_profile,
            submitted.llm_profile,
            &submitted.frozen.agent
        ),
        (None, llm, &None),
        "no Agent configuration is made up for LLM-only work"
    );
    assert_eq!(
        (
            submitted.frozen.llm.config.model.as_str(),
            submitted.frozen.llm.provider.id,
            submitted.frozen.llm.provider.transport
        ),
        ("model-a", llm_provider, OPENAI)
    );
    assert_eq!(
        wait(handle.job(check))
            .expect("read job")
            .expect("job exists")
            .configuration,
        llm_only(llm)
    );
    assert_eq!(
        blocking(&handle, llm),
        [check],
        "the submitted check holds its LLM Profile"
    );

    wait(handle.start_job(check)).expect("start");
    let unsupported = json!({"capability": "tools", "observed": "unsupported", "evidence": "no tool call in the response"});
    wait(handle.finalize_job(
        check,
        JobFinalization::Completed(FinalResult {
            value: Some(unsupported.clone()),
            ..result("the check ran to the end")
        }),
    ))
    .expect("publish the conclusion");
    let record = wait(handle.job(check))
        .expect("read job")
        .expect("job exists");
    assert!(
        record.completed_at.is_some() && record.failed_at.is_none(),
        "a negative conclusion does not fail the Job"
    );
    assert_eq!(
        wait(handle.latest_output(check))
            .expect("read latest output")
            .and_then(|output| output.value),
        Some(unsupported.clone())
    );

    // A positive conclusion does not complete a Job either.
    let cancelled = create_job(
        &handle,
        NewJob {
            configuration: llm_only(llm),
            ..new_job("chat check", genesis, Vec::new())
        },
    );
    let supported = json!({"capability": "chat", "observed": "supported"});
    wait(handle.append_job_output(
        cancelled,
        NewJobOutput {
            kind: IntermediateOutputKind::Evaluation,
            summary: None,
            code: None,
            goal_reached: Some(true),
            value: Some(supported.clone()),
        },
    ))
    .expect("publish an evaluation");
    wait(handle.finalize_job(
        cancelled,
        JobFinalization::Cancelled(result("stopped by the user")),
    ))
    .expect("cancel");
    let record = wait(handle.job(cancelled))
        .expect("read job")
        .expect("job exists");
    assert!(record.cancelled_at.is_some() && record.completed_at.is_none());
    assert_eq!(
        wait(handle.profile(llm)).expect("read Profile"),
        Some(declaration.clone()),
        "neither conclusion edits the declared capabilities"
    );

    // Checking again is a new Job.
    let again = wait(handle.retry_job(JobRetry {
        original: cancelled,
        input: RetryInput::Unchanged,
        name: Some("chat check again".into()),
        execution_mode: ExecutionMode::Once,
        eligible_at: None,
        scheduled_at: None,
        next_due_at: None,
        next_iteration_no: None,
        iteration_no: None,
        projects: Vec::new(),
        dependencies: Vec::new(),
        configuration: llm_only(llm),
        assigned_executor: Some(checker),
    }))
    .expect("retry the check");
    let again_record = wait(handle.job(again))
        .expect("read retry")
        .expect("retry exists");
    assert_eq!(
        (
            again_record.retry_of,
            again_record.input,
            again_record.configuration
        ),
        (Some(cancelled), record.input, llm_only(llm))
    );
    assert_eq!(wait(handle.job(cancelled)).expect("read job"), Some(record));
    let outputs = |handle: &DatabaseHandle| {
        [check, cancelled].map(|job| {
            wait(handle.job_outputs(job, None, 100))
                .expect("read outputs")
                .into_iter()
                .map(|output| output.value)
                .collect::<Vec<_>>()
        })
    };
    assert_eq!(
        outputs(&handle),
        [vec![Some(unsupported)], vec![Some(supported), None]]
    );
    let before = outputs(&handle);
    worker.shutdown().expect("shutdown");

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    let handle = worker.handle();
    assert_eq!(outputs(&handle), before, "conclusions survive reopening");
    assert_eq!(
        wait(handle.job_configuration(check)).expect("read configuration"),
        Some(submitted)
    );
    assert_eq!(
        wait(handle.profile(llm)).expect("read Profile"),
        Some(declaration)
    );
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

#[test]
fn frozen_inputs_stay_immutable_and_a_retry_is_a_new_job() {
    let (_root, directory) = data_directory("retry");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let genesis = handle.genesis();
    let (alpha, beta) = (project(&handle, "alpha"), project(&handle, "beta"));
    let parent = create_job(&handle, new_job("parent", genesis, Vec::new()));
    let draft = JobInputDraft {
        objective: Some("summarize the report".into()),
        ..JobInputDraft::default()
    };
    let input = wait(handle.save_job_input(None, draft.clone())).expect("save input");
    let original =
        wait(handle.create_job_with_input(new_job("original", parent, vec![alpha]), input))
            .expect("create original");

    let frozen = wait(handle.job_input(input))
        .expect("read input")
        .expect("input exists");
    assert!(frozen.submitted_at.is_some());
    assert_eq!(
        violation(wait(handle.save_job_input(
            Some(input),
            JobInputDraft {
                objective: Some("changed".into()),
                ..draft.clone()
            }
        ))),
        Violation::InputAlreadySubmitted(input)
    );
    assert_eq!(
        violation(wait(handle.create_job_with_input(
            new_job("reuse", parent, Vec::new()),
            input
        ))),
        Violation::InputAlreadySubmitted(input),
        "only a retry of its Job may run with a frozen input"
    );
    wait(handle.finalize_job(
        original,
        JobFinalization::Failed(result("first try failed")),
    ))
    .expect("finalize original");
    let original_record = wait(handle.job(original))
        .expect("read original")
        .expect("original exists");
    let original_outputs =
        wait(handle.job_outputs(original, None, 100)).expect("read original outputs");

    let retry = |input, projects| JobRetry {
        original,
        input,
        name: Some("second try".into()),
        execution_mode: ExecutionMode::Once,
        eligible_at: None,
        scheduled_at: None,
        next_due_at: None,
        next_iteration_no: None,
        iteration_no: None,
        projects,
        dependencies: Vec::new(),
        configuration: None,
        assigned_executor: None,
    };
    let unchanged = wait(handle.retry_job(retry(RetryInput::Unchanged, vec![beta])))
        .expect("retry with the frozen input");
    let unchanged_record = wait(handle.job(unchanged))
        .expect("read retry")
        .expect("retry exists");
    assert_ne!(unchanged, original);
    assert_eq!(
        unchanged_record.input, input,
        "the unchanged input is shared, not copied"
    );
    assert_eq!(
        unchanged_record.parent, parent,
        "the parent is derived from the original"
    );
    assert_eq!(unchanged_record.retry_of, Some(original));
    assert_eq!(
        unchanged_record.projects,
        [beta],
        "a retry need not keep the original's Project set"
    );

    assert_eq!(
        violation(wait(
            handle.retry_job(retry(RetryInput::Changed(input), Vec::new()))
        )),
        Violation::InputAlreadySubmitted(input)
    );
    let changed_input = wait(handle.save_job_input(
        None,
        JobInputDraft {
            objective: Some("summarize section 2".into()),
            ..draft
        },
    ))
    .expect("save changed input");
    let changed = wait(handle.retry_job(retry(RetryInput::Changed(changed_input), Vec::new())))
        .expect("retry with a new input");
    assert_eq!(
        wait(handle.job(changed))
            .expect("read retry")
            .expect("retry exists")
            .input,
        changed_input
    );
    assert!(matches!(
        violation(wait(
            handle.save_job_input(Some(changed_input), JobInputDraft::default())
        )),
        Violation::InputAlreadySubmitted(_)
    ));

    assert_eq!(
        wait(handle.job(original)).expect("read original"),
        Some(original_record),
        "the original Job is untouched"
    );
    assert_eq!(
        wait(handle.job_outputs(original, None, 100)).expect("read outputs"),
        original_outputs
    );
    assert_eq!(
        wait(handle.job_input(input)).expect("read input"),
        Some(frozen),
        "the frozen input is untouched"
    );
    assert_eq!(
        wait(handle.retries(original)).expect("read retries"),
        [unchanged, changed]
    );
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

#[test]
fn outputs_keep_their_order_and_finalization_stays_atomic() {
    let (_root, directory) = data_directory("outputs");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let job = create_job(
        &handle,
        new_job("orchestrator", handle.genesis(), Vec::new()),
    );
    let child = create_job(&handle, new_job("worker", job, Vec::new()));
    for kind in [
        IntermediateOutputKind::Progress,
        IntermediateOutputKind::Evaluation,
    ] {
        wait(handle.append_job_output(
            job,
            NewJobOutput {
                kind,
                summary: None,
                code: None,
                goal_reached: None,
                value: None,
            },
        ))
        .expect("append output");
    }

    assert_eq!(
        violation(wait(handle.finalize_job(
            job,
            JobFinalization::Completed(result("too early"))
        ))),
        Violation::UnfinishedChild(job)
    );
    let pending = wait(handle.job(job))
        .expect("read job")
        .expect("job exists");
    assert!(
        pending.completed_at.is_none() && pending.sterilized_at.is_none(),
        "a refused finalization changes nothing"
    );
    assert_eq!(
        wait(handle.job_outputs(job, None, 100))
            .expect("read outputs")
            .len(),
        2,
        "and publishes no result"
    );

    let child_result =
        wait(handle.finalize_job(child, JobFinalization::Completed(result("child output"))))
            .expect("finalize child");
    assert_eq!(
        child_result.map(|appended| appended.seq),
        Some(1),
        "the child publishes its own output"
    );
    let appended =
        wait(handle.finalize_job(job, JobFinalization::Completed(result("overall output"))))
            .expect("finalize parent")
            .expect("a confirmed outcome publishes a result");
    let outputs = wait(handle.job_outputs(job, None, 100)).expect("read outputs");
    assert_eq!(
        outputs
            .iter()
            .map(|output| (output.seq, output.kind))
            .collect::<Vec<_>>(),
        [
            (1, OutputKind::Progress),
            (2, OutputKind::Evaluation),
            (3, OutputKind::Result)
        ]
    );
    assert_eq!(
        wait(handle.latest_output(job))
            .expect("read latest")
            .map(|output| output.id),
        Some(appended.output)
    );
    let finalized = wait(handle.job(job))
        .expect("read job")
        .expect("job exists");
    assert_eq!(
        (finalized.completed_at, finalized.sterilized_at.is_some()),
        (Some(outputs[2].created_at), true)
    );
    assert_eq!(
        violation(wait(handle.append_job_output(
            job,
            NewJobOutput {
                kind: IntermediateOutputKind::Progress,
                summary: None,
                code: None,
                goal_reached: None,
                value: None,
            }
        ))),
        Violation::JobHasTerminalOutcome(job)
    );
    assert_eq!(
        violation(wait(handle.finalize_job(job, JobFinalization::Unknown))),
        Violation::JobAlreadyFinalized(job)
    );
    let input = wait(handle.save_job_input(None, JobInputDraft::default())).expect("save input");
    assert_eq!(
        violation(wait(
            handle.create_job_with_input(new_job("late", job, Vec::new()), input)
        )),
        Violation::ParentClosed(job)
    );

    let unknown = create_job(&handle, new_job("unknown", handle.genesis(), Vec::new()));
    assert_eq!(
        wait(handle.finalize_job(unknown, JobFinalization::Unknown))
            .expect("record unknown outcome"),
        None
    );
    assert!(
        wait(handle.job_outputs(unknown, None, 100))
            .expect("read outputs")
            .is_empty()
    );
    worker.shutdown().expect("shutdown");
}

fn executor(
    handle: &DatabaseHandle,
    name: &str,
    executor_type: ExecutorType,
    llm_transports: &[LlmTransport],
) -> Result<ExecutorId, DatabaseError> {
    wait(handle.create_executor(NewExecutor {
        name: name.into(),
        executor_type,
        concurrency_limit: NonZeroU32::MIN,
        llm_transports: llm_transports.iter().copied().collect(),
    }))
}

/// One Provider, two LLM Profiles and an Agent Profile that targets only the first.
struct Configured {
    provider: LlmProviderId,
    targeted: ProfileId,
    untargeted: ProfileId,
    agent: ProfileId,
}

fn configured(handle: &DatabaseHandle) -> Configured {
    let provider = provider(handle, "first");
    let targeted = llm_profile(
        handle,
        provider,
        llm_config("model-a", &[ModelCapability::Chat, ModelCapability::Tools]),
    );
    let untargeted = llm_profile(
        handle,
        provider,
        llm_config("model-b", &[ModelCapability::Chat]),
    );
    let agent = agent_profile(handle, "Answer briefly.", vec![targeted]);
    Configured {
        provider,
        targeted,
        untargeted,
        agent,
    }
}

fn configuration(
    agent_profile: ProfileId,
    llm_profile: ProfileId,
) -> Option<ExecutionConfiguration> {
    Some(ExecutionConfiguration {
        agent_profile: Some(agent_profile),
        llm_profile,
    })
}

/// Work that runs an LLM directly, without an Agent Profile.
fn llm_only(llm_profile: ProfileId) -> Option<ExecutionConfiguration> {
    Some(ExecutionConfiguration {
        agent_profile: None,
        llm_profile,
    })
}

#[test]
fn agent_profiles_target_llm_profiles_of_the_right_kind_only() {
    let (_root, directory) = data_directory("llm-targets");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let provider = provider(&handle, "first");
    let first = llm_profile(
        &handle,
        provider,
        llm_config("model-a", &[ModelCapability::Chat]),
    );
    let second = llm_profile(
        &handle,
        provider,
        llm_config("model-b", &[ModelCapability::Chat]),
    );
    let agent = agent_profile(&handle, "Answer briefly.", vec![second, first]);

    let typed = wait(handle.agent_profile(agent))
        .expect("read Agent Profile")
        .expect("Agent Profile exists");
    assert_eq!(
        typed.links,
        [first, second],
        "targets read back as a set in ID order, not as a preference order"
    );
    assert_eq!(
        wait(handle.profile(first))
            .expect("read LLM Profile")
            .expect("LLM Profile exists")
            .llm_targets,
        Vec::<ProfileId>::new()
    );

    let other_agent = agent_profile(&handle, "Other.", Vec::new());
    let missing = ProfileId::new(9_999);
    let refused = [
        (
            vec![first, other_agent],
            Violation::WrongProfileKind(WrongProfileKind {
                profile: other_agent,
                expected: ProfileKind::Llm,
                found: ProfileKind::Agent,
            }),
        ),
        (
            vec![missing],
            Violation::NotFound(RecordRef::Profile(missing)),
        ),
        (
            vec![first, first],
            Violation::DuplicateReference(RecordRef::Profile(first)),
        ),
    ];
    let count = wait(handle.profiles(None, 100))
        .expect("read Profiles")
        .len();
    for (llm_targets, expected) in refused {
        let profile = NewProfile {
            name: "refused".into(),
            config: NewProfileConfig::Agent {
                config: AgentConfig::default(),
                llm_targets,
            },
        };
        assert_eq!(violation(wait(handle.create_profile(profile))), expected);
    }
    assert_eq!(
        wait(handle.profiles(None, 100))
            .expect("read Profiles")
            .len(),
        count,
        "refused Profiles left nothing behind"
    );
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

#[test]
fn submission_binds_input_parent_and_configuration_together_or_not_at_all() {
    let (_root, directory) = data_directory("submission");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        provider,
        targeted,
        untargeted,
        agent,
    } = configured(&handle);
    let llm_executor =
        executor(&handle, "llm worker", ExecutorType::Llm, &[OPENAI]).expect("register Executor");
    let other_executor =
        executor(&handle, "no adapter", ExecutorType::Llm, &[]).expect("register Executor");
    let parent = create_job(
        &handle,
        new_job("conversation", handle.genesis(), Vec::new()),
    );
    let input = wait(handle.save_job_input(
        None,
        JobInputDraft {
            objective: Some("answer".into()),
            ..JobInputDraft::default()
        },
    ))
    .expect("save input");
    let missing = ProfileId::new(9_999);
    let child = |configuration, assigned_executor| NewJob {
        configuration,
        assigned_executor,
        ..new_job("model request", parent, Vec::new())
    };
    let refused = [
        (
            child(configuration(agent, untargeted), None),
            Violation::NotAConfiguredTarget {
                agent_profile: agent,
                llm_profile: untargeted,
            },
        ),
        (
            child(configuration(targeted, targeted), None),
            Violation::WrongProfileKind(WrongProfileKind {
                profile: targeted,
                expected: ProfileKind::Agent,
                found: ProfileKind::Llm,
            }),
        ),
        (
            child(configuration(agent, missing), None),
            Violation::NotFound(RecordRef::Profile(missing)),
        ),
        (
            child(configuration(agent, targeted), Some(other_executor)),
            Violation::ExecutorLacksTransport {
                executor: other_executor,
                transport: OPENAI,
            },
        ),
    ];
    let jobs_before = wait(handle.jobs(None, 100)).expect("read jobs");
    for (job, expected) in refused {
        assert_eq!(
            violation(wait(handle.create_job_with_input(job, input))),
            expected
        );
    }
    assert_eq!(
        wait(handle.jobs(None, 100)).expect("read jobs"),
        jobs_before,
        "no partial child was committed"
    );
    assert!(
        wait(handle.children(parent, None, 100))
            .expect("read children")
            .is_empty()
    );
    assert!(
        wait(handle.profile_jobs(agent, None, 100))
            .expect("read Profile Jobs")
            .is_empty()
    );
    assert!(
        wait(handle.profile_jobs(targeted, None, 100))
            .expect("read Profile Jobs")
            .is_empty()
    );
    assert_eq!(
        wait(handle.job_input(input))
            .expect("read input")
            .expect("input exists")
            .submitted_at,
        None,
        "the input is not frozen"
    );

    let request = wait(handle.create_job_with_input(
        child(configuration(agent, targeted), Some(llm_executor)),
        input,
    ))
    .expect("submit child");
    let record = wait(handle.job(request))
        .expect("read child")
        .expect("child exists");
    assert_eq!(
        (record.input, record.parent, record.assigned_executor),
        (input, parent, Some(llm_executor))
    );
    assert_eq!(record.configuration, configuration(agent, targeted));
    assert_eq!(
        wait(handle.profile_jobs(agent, None, 100)).expect("read Profile Jobs"),
        [request]
    );
    assert_eq!(
        wait(handle.profile_jobs(targeted, None, 100)).expect("read Profile Jobs"),
        [request]
    );
    assert!(
        wait(handle.job(parent))
            .expect("read parent")
            .expect("parent exists")
            .configuration
            .is_none(),
        "orchestrating work needs no LLM Profile"
    );
    assert!(
        wait(handle.job(handle.genesis()))
            .expect("read Genesis")
            .expect("Genesis exists")
            .configuration
            .is_none()
    );

    let effective = wait(handle.job_configuration(request))
        .expect("read configuration")
        .expect("child is configured");
    assert_eq!(
        (
            effective.agent_profile,
            effective.llm_profile,
            effective.frozen.llm.provider.id
        ),
        (Some(agent), targeted, provider)
    );
    assert_eq!(
        effective
            .frozen
            .agent
            .as_ref()
            .and_then(|agent| agent.instructions.as_deref()),
        Some("Answer briefly.")
    );
    assert_eq!(effective.frozen.llm.config.model, "model-a");
    assert_eq!(
        effective.frozen.llm.config.parameters.temperature,
        Some(0.2)
    );
    assert_eq!(
        effective
            .frozen
            .llm
            .config
            .declared_capabilities
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        [ModelCapability::Chat, ModelCapability::Tools]
    );
    assert_eq!(
        wait(handle.job_configuration(parent)).expect("read configuration"),
        None
    );
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    let handle = worker.handle();
    assert_eq!(
        wait(handle.job_configuration(request)).expect("read configuration"),
        Some(effective),
        "the effective configuration survives reopening"
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn using_a_profile_never_changes_it() {
    let (_root, directory) = data_directory("immutable");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        targeted,
        untargeted,
        agent,
        ..
    } = configured(&handle);
    let read_profiles = |handle: &DatabaseHandle| {
        [agent, targeted, untargeted].map(|profile| {
            wait(handle.profile(profile))
                .expect("read Profile")
                .expect("Profile exists")
        })
    };
    let before = read_profiles(&handle);

    // Everything that uses, measures or observes the Profiles leaves them as created.
    let llm_executor =
        executor(&handle, "worker", ExecutorType::Llm, &[OPENAI]).expect("register Executor");
    wait(handle.create_agent(NewAgent {
        name: "helper".into(),
        profiles: vec![agent],
    }))
    .expect("create Agent");
    let job = create_job(
        &handle,
        NewJob {
            configuration: configuration(agent, targeted),
            ..new_job("request", handle.genesis(), Vec::new())
        },
    );
    wait(handle.assign_job_executor(job, llm_executor)).expect("assign Executor");
    let failed = provider_failure("invalid API key", Some(401), false);
    wait(handle.record_availability_stat(NewAvailabilityStat {
        job,
        observed_at: at(1_000),
        observation: failure(failed),
    }))
    .expect("record AvailabilityStat");
    wait(handle.record_metrics(NewMetrics {
        job,
        observed_at: at(1_000),
        measurements: Measurements {
            request_finished_in_ms: Some(40),
            ..Measurements::default()
        },
    }))
    .expect("record Metrics");
    wait(handle.finalize_job(job, JobFinalization::Cancelled(result("stopped"))))
        .expect("stop the work");
    assert_eq!(read_profiles(&handle), before);
    worker.shutdown().expect("shutdown");

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    assert_eq!(
        read_profiles(&worker.handle()),
        before,
        "using a Profile does not edit its payload, Provider or LLM targets"
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn one_llm_executor_runs_different_providers_and_models_exactly_as_frozen() {
    let (_root, directory) = data_directory("executors");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let genesis = handle.genesis();
    let (first_provider, second_provider) =
        (provider(&handle, "first"), provider(&handle, "second"));
    let model_a = llm_profile(
        &handle,
        first_provider,
        llm_config("model-a", &[ModelCapability::Chat]),
    );
    let model_b = llm_profile(
        &handle,
        second_provider,
        LlmConfig {
            parameters: LlmParameters {
                temperature: Some(0.9),
                top_p: Some(0.5),
                max_tokens: Some(64),
                ..LlmParameters::default()
            },
            ..llm_config("model-b", &[ModelCapability::Chat])
        },
    );
    let agent = agent_profile(&handle, "Answer briefly.", vec![model_a, model_b]);

    // Registration names transports, never Profiles, Providers or models.
    assert_eq!(
        violation(executor(&handle, "tool", ExecutorType::Tool, &[OPENAI])),
        Violation::InvalidValue(
            "llm_transports",
            "only an Llm Executor can run LLM transports"
        )
    );
    let shared =
        executor(&handle, "shared", ExecutorType::Llm, &[OPENAI]).expect("register Executor");
    let no_adapter =
        executor(&handle, "no adapter", ExecutorType::Llm, &[]).expect("register Executor");
    let tool = executor(&handle, "tool", ExecutorType::Tool, &[]).expect("register tool Executor");
    let registration = wait(handle.executor(shared))
        .expect("read Executor")
        .expect("Executor exists");
    assert_eq!(registration.llm_transports, BTreeSet::from([OPENAI]));
    for profile in [model_a, model_b] {
        assert_eq!(
            wait(handle.compatible_executors(profile)).expect("read candidates"),
            [shared]
        );
    }
    assert!(
        wait(handle.compatible_executors(agent))
            .expect("read candidates")
            .is_empty(),
        "an Agent Profile has no transport of its own"
    );

    let on_a = create_job(
        &handle,
        NewJob {
            configuration: configuration(agent, model_a),
            assigned_executor: Some(shared),
            ..new_job("request a", genesis, Vec::new())
        },
    );
    let on_b = create_job(
        &handle,
        NewJob {
            configuration: llm_only(model_b),
            assigned_executor: Some(shared),
            ..new_job("request b", genesis, Vec::new())
        },
    );
    let pending = create_job(
        &handle,
        NewJob {
            configuration: configuration(agent, model_b),
            ..new_job("pending", genesis, Vec::new())
        },
    );
    let unassigned = wait(handle.job(pending))
        .expect("read job")
        .expect("job exists");
    assert_eq!(
        violation(wait(handle.assign_job_executor(pending, no_adapter))),
        Violation::ExecutorLacksTransport {
            executor: no_adapter,
            transport: OPENAI
        }
    );
    assert_eq!(
        violation(wait(handle.assign_job_executor(pending, tool))),
        Violation::IncompatibleExecutorKind {
            executor: tool,
            found: ExecutorType::Tool
        }
    );
    let jobs_before = wait(handle.jobs(None, 100)).expect("read jobs");
    let input = wait(handle.save_job_input(None, JobInputDraft::default())).expect("save input");
    assert!(matches!(
        violation(wait(handle.create_job_with_input(
            NewJob {
                configuration: llm_only(model_a),
                assigned_executor: Some(no_adapter),
                ..new_job("refused", genesis, Vec::new())
            },
            input
        ))),
        Violation::ExecutorLacksTransport { .. }
    ));
    assert_eq!(
        wait(handle.jobs(None, 100)).expect("read jobs"),
        jobs_before
    );
    assert_eq!(
        wait(handle.job(pending)).expect("read job"),
        Some(unassigned),
        "refused assignments changed nothing"
    );
    wait(handle.assign_job_executor(pending, shared)).expect("assign the shared Executor");
    let unconfigured = create_job(&handle, new_job("system work", genesis, Vec::new()));
    wait(handle.assign_job_executor(unconfigured, tool))
        .expect("work without an LLM configuration keeps any Executor");

    let frozen_values = |handle: &DatabaseHandle, job| {
        let frozen = wait(handle.job_configuration(job))
            .expect("read configuration")
            .expect("job is configured")
            .frozen
            .llm;
        (
            frozen.provider.id,
            frozen.config.model,
            frozen.config.parameters,
        )
    };
    let a_parameters = llm_config("model-a", &[]).parameters;
    let b_parameters = LlmParameters {
        temperature: Some(0.9),
        top_p: Some(0.5),
        max_tokens: Some(64),
        ..LlmParameters::default()
    };
    for (job, expected) in [
        (
            on_a,
            (first_provider, "model-a".to_owned(), a_parameters.clone()),
        ),
        (
            on_b,
            (second_provider, "model-b".to_owned(), b_parameters.clone()),
        ),
        (
            pending,
            (second_provider, "model-b".to_owned(), b_parameters),
        ),
    ] {
        assert_eq!(
            frozen_values(&handle, job),
            expected,
            "{job} runs exactly what it froze"
        );
        assert_eq!(
            wait(handle.job(job))
                .expect("read job")
                .expect("job exists")
                .assigned_executor,
            Some(shared)
        );
        assert_eq!(
            wait(handle.job_compatible_executors(job)).expect("read candidates"),
            [shared]
        );
    }

    // Changed request parameters reach later work on the same Executor
    // without a new registration.
    let history = [on_a, on_b, pending]
        .map(|job| wait(handle.job_configuration(job)).expect("read configuration"));
    for job in [on_a, on_b, pending] {
        wait(handle.finalize_job(job, JobFinalization::Completed(result("done")))).expect("finish");
    }
    let changed = LlmParameters {
        temperature: Some(1.0),
        max_tokens: Some(1_024),
        ..LlmParameters::default()
    };
    wait(handle.update_profile(
        model_a,
        NewProfile {
            name: "model-a".into(),
            config: NewProfileConfig::Llm {
                provider: first_provider,
                config: LlmConfig {
                    parameters: changed.clone(),
                    ..llm_config("model-a", &[ModelCapability::Chat])
                },
            },
        },
    ))
    .expect("edit request parameters");
    let later = create_job(
        &handle,
        NewJob {
            configuration: configuration(agent, model_a),
            assigned_executor: Some(shared),
            ..new_job("later request", genesis, Vec::new())
        },
    );
    assert_eq!(
        frozen_values(&handle, later),
        (first_provider, "model-a".to_owned(), changed)
    );
    assert_eq!(
        [on_a, on_b, pending]
            .map(|job| wait(handle.job_configuration(job)).expect("read configuration")),
        history,
        "earlier work keeps what it froze"
    );
    worker.shutdown().expect("shutdown");

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    let handle = worker.handle();
    assert_eq!(
        [on_a, on_b, pending]
            .map(|job| wait(handle.job_configuration(job)).expect("read configuration")),
        history
    );
    assert_eq!(
        wait(handle.executor(shared)).expect("read Executor"),
        Some(registration)
    );
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

#[test]
fn the_execution_runtime_runs_a_configured_child_only_on_a_compatible_executor() {
    let (_root, directory) = data_directory("runtime");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        targeted, agent, ..
    } = configured(&handle);
    let compatible =
        executor(&handle, "llm worker", ExecutorType::Llm, &[OPENAI]).expect("register Executor");
    let incompatible =
        executor(&handle, "no adapter", ExecutorType::Llm, &[]).expect("register Executor");
    let runtime = ExecutionRuntime::start(handle.clone()).expect("start runtime");
    let plan = |executor_id| ExecutionPlan {
        executor_id,
        input: JobInputDraft {
            objective: Some("answer".into()),
            ..JobInputDraft::default()
        },
        job: NewJob {
            configuration: configuration(agent, targeted),
            ..new_job("model request", handle.genesis(), Vec::new())
        },
        outcome: JobOutcome::Completed,
        result: result("answered"),
    };
    let jobs_before = wait(handle.jobs(None, 100)).expect("read jobs").len();
    match wait(runtime.scheduler().schedule(plan(incompatible))) {
        Err(ExecutionRuntimeError::Database(DatabaseError::Rejected(violation))) => {
            assert_eq!(
                violation,
                Violation::ExecutorLacksTransport {
                    executor: incompatible,
                    transport: OPENAI
                }
            )
        }
        other => panic!("an incompatible Executor must be refused, got {other:?}"),
    }
    assert_eq!(
        wait(handle.jobs(None, 100)).expect("read jobs").len(),
        jobs_before,
        "the refused plan created no Job"
    );

    let receipt =
        wait(runtime.scheduler().schedule(plan(compatible))).expect("run on a compatible Executor");
    let record = wait(handle.job(receipt.job))
        .expect("read job")
        .expect("job exists");
    assert_eq!(
        (record.assigned_executor, record.configuration),
        (Some(compatible), configuration(agent, targeted))
    );
    assert!(record.completed_at.is_some());
    runtime.shutdown().expect("shutdown runtime");
    worker.shutdown().expect("shutdown");
}

#[test]
fn retries_share_one_frozen_input_without_overwriting_it() {
    let (_root, directory) = data_directory("shared-input");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let input = wait(handle.save_job_input(
        None,
        JobInputDraft {
            objective: Some("summarize".into()),
            ..JobInputDraft::default()
        },
    ))
    .expect("save input");
    let original = wait(
        handle.create_job_with_input(new_job("original", handle.genesis(), Vec::new()), input),
    )
    .expect("create original");
    let retry = |original| JobRetry {
        original,
        input: RetryInput::Unchanged,
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
    };
    let first = wait(handle.retry_job(retry(original))).expect("retry");
    let of_retry = wait(handle.retry_job(retry(first))).expect("retry the retry");
    let second = wait(handle.retry_job(retry(original))).expect("retry again");

    assert_eq!(
        wait(handle.input_jobs(input)).expect("read input Jobs"),
        [original, first, of_retry, second]
    );
    for job in [original, first, of_retry, second] {
        assert_eq!(
            wait(handle.job(job))
                .expect("read job")
                .expect("job exists")
                .input,
            input,
            "{job} keeps the shared input"
        );
    }
    assert_eq!(
        wait(handle.job(of_retry))
            .expect("read job")
            .expect("job exists")
            .retry_of,
        Some(first)
    );
    assert_eq!(
        wait(handle.retries(original)).expect("read retries"),
        [first, second]
    );
    assert_eq!(
        wait(handle.retries(first)).expect("read retries"),
        [of_retry]
    );
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

fn llm_edit(provider: LlmProviderId, model: &str) -> NewProfile {
    NewProfile {
        name: model.into(),
        config: NewProfileConfig::Llm {
            provider,
            config: llm_config(model, &[ModelCapability::Chat]),
        },
    }
}

fn agent_edit(instructions: &str, llm_targets: Vec<ProfileId>) -> NewProfile {
    NewProfile {
        name: "assistant".into(),
        config: NewProfileConfig::Agent {
            config: AgentConfig {
                instructions: Some(instructions.into()),
            },
            llm_targets,
        },
    }
}

/// Polls a command once, which admits it to the write worker's queue, and
/// returns a waiter for its result. Commands run in admission order.
fn admit<F: std::future::Future>(future: F) -> impl FnOnce() -> F::Output {
    let mut future = Box::pin(future);
    let ready = poll_once(future.as_mut());
    move || match ready {
        Some(output) => output,
        None => wait(future),
    }
}

fn configured_child(handle: &DatabaseHandle, agent: ProfileId, llm: ProfileId) -> JobId {
    create_job(
        handle,
        NewJob {
            configuration: configuration(agent, llm),
            ..new_job("model request", handle.genesis(), Vec::new())
        },
    )
}

fn blocking(handle: &DatabaseHandle, profile: ProfileId) -> Vec<JobId> {
    let editability = wait(handle.profile_editability(profile))
        .expect("read editability")
        .expect("Profile exists");
    assert_eq!(editability.editable(), editability.blocking_jobs.is_empty());
    editability.blocking_jobs
}

fn frozen_model(handle: &DatabaseHandle, job: JobId) -> String {
    wait(handle.job_configuration(job))
        .expect("read configuration")
        .expect("job is configured")
        .frozen
        .llm
        .config
        .model
}

#[test]
fn unlocked_profiles_are_edited_with_their_links_and_rejected_edits_change_nothing() {
    let (_root, directory) = data_directory("edit");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        provider: first_provider,
        targeted,
        untargeted,
        agent,
    } = configured(&handle);
    let second_provider = provider(&handle, "second");
    let other_agent = agent_profile(&handle, "Other.", Vec::new());

    wait(handle.update_profile(targeted, llm_edit(second_provider, "model-c")))
        .expect("edit an unlocked LLM Profile");
    wait(handle.update_profile(
        agent,
        agent_edit("Answer in detail.", vec![untargeted, targeted]),
    ))
    .expect("edit an unlocked Agent Profile");
    let edited_llm = wait(handle.llm_profile(targeted))
        .expect("read LLM Profile")
        .expect("LLM Profile exists");
    assert_eq!(
        (edited_llm.config.model.as_str(), edited_llm.links),
        ("model-c", second_provider),
        "identity kept, payload and Provider replaced"
    );
    assert!(edited_llm.updated_at >= edited_llm.created_at);
    let edited_agent = wait(handle.agent_profile(agent))
        .expect("read Agent Profile")
        .expect("Agent Profile exists");
    assert_eq!(
        edited_agent.config.instructions.as_deref(),
        Some("Answer in detail.")
    );
    assert_eq!(edited_agent.links, [targeted, untargeted]);
    assert_eq!(
        wait(handle.provider_profiles(first_provider)).expect("read provider Profiles"),
        [untargeted]
    );
    assert_eq!(
        wait(handle.provider_profiles(second_provider)).expect("read provider Profiles"),
        [targeted]
    );

    let missing_profile = ProfileId::new(9_999);
    let missing_provider = LlmProviderId::new(9_999);
    let refused = [
        (
            agent,
            llm_edit(first_provider, "model-d"),
            Violation::WrongProfileKind(WrongProfileKind {
                profile: agent,
                expected: ProfileKind::Agent,
                found: ProfileKind::Llm,
            }),
        ),
        (
            agent,
            agent_edit("x", vec![other_agent]),
            Violation::WrongProfileKind(WrongProfileKind {
                profile: other_agent,
                expected: ProfileKind::Llm,
                found: ProfileKind::Agent,
            }),
        ),
        (
            agent,
            agent_edit("x", vec![missing_profile]),
            Violation::NotFound(RecordRef::Profile(missing_profile)),
        ),
        (
            agent,
            agent_edit("x", vec![untargeted, untargeted]),
            Violation::DuplicateReference(RecordRef::Profile(untargeted)),
        ),
        (
            targeted,
            llm_edit(missing_provider, "model-d"),
            Violation::NotFound(RecordRef::LlmProvider(missing_provider)),
        ),
        (
            targeted,
            llm_edit(first_provider, ""),
            Violation::InvalidValue("model", "cannot be empty"),
        ),
        (
            missing_profile,
            llm_edit(first_provider, "model-d"),
            Violation::NotFound(RecordRef::Profile(missing_profile)),
        ),
    ];
    let snapshot = |handle: &DatabaseHandle| {
        (
            wait(handle.profiles(None, 100)).expect("read Profiles"),
            wait(handle.provider_profiles(first_provider)).expect("read provider Profiles"),
            wait(handle.provider_profiles(second_provider)).expect("read provider Profiles"),
        )
    };
    let before = snapshot(&handle);
    for (profile, edit, expected) in refused {
        assert_eq!(
            violation(wait(handle.update_profile(profile, edit))),
            expected
        );
    }
    assert_eq!(
        snapshot(&handle),
        before,
        "refused edits left payloads, links and indexes alone"
    );
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

#[test]
fn unfinished_work_locks_both_profiles_until_the_last_holder_finishes() {
    let (_root, directory) = data_directory("locks");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    type Finish = fn() -> JobFinalization;
    let outcomes: [(&str, Finish); 3] = [
        ("completed", || JobFinalization::Completed(result("done"))),
        ("failed", || JobFinalization::Failed(result("broken"))),
        ("cancelled", || {
            JobFinalization::Cancelled(result("stopped"))
        }),
    ];
    for (name, finish) in outcomes {
        let Configured {
            provider,
            targeted,
            untargeted,
            agent,
        } = configured(&handle);
        // Idle uses hold nothing: Agent membership and Executor registration.
        wait(handle.create_agent(NewAgent {
            name: "helper".into(),
            profiles: vec![agent],
        }))
        .expect("create Agent");
        executor(&handle, "worker", ExecutorType::Llm, &[OPENAI]).expect("register Executor");
        assert!(
            blocking(&handle, agent).is_empty() && blocking(&handle, targeted).is_empty(),
            "{name}: idle Profiles are editable"
        );

        let first = configured_child(&handle, agent, targeted);
        let second = configured_child(&handle, agent, targeted);
        for profile in [agent, targeted] {
            assert_eq!(
                blocking(&handle, profile),
                [first, second],
                "{name}: queued work holds {profile}"
            );
        }
        assert!(
            blocking(&handle, untargeted).is_empty(),
            "{name}: an unchosen target is not held"
        );
        assert_eq!(
            violation(wait(
                handle.update_profile(agent, agent_edit("changed", vec![targeted]))
            )),
            Violation::ProfileLocked {
                profile: agent,
                blocking_jobs: vec![first, second]
            }
        );
        assert!(matches!(
            violation(wait(
                handle.update_profile(targeted, llm_edit(provider, "model-z"))
            )),
            Violation::ProfileLocked { .. }
        ));

        wait(handle.start_job(first)).expect("start");
        wait(handle.finalize_job(first, finish())).expect("finish the first holder");
        assert_eq!(
            blocking(&handle, targeted),
            [second],
            "{name}: one finished Job does not unlock a shared Profile"
        );
        assert!(matches!(
            violation(wait(
                handle.update_profile(targeted, llm_edit(provider, "model-z"))
            )),
            Violation::ProfileLocked { .. }
        ));

        wait(handle.finalize_job(second, finish())).expect("finish the last holder");
        assert!(
            blocking(&handle, agent).is_empty() && blocking(&handle, targeted).is_empty(),
            "{name}: the last finished holder unlocks"
        );
        wait(handle.update_profile(targeted, llm_edit(provider, "model-z")))
            .expect("edit after unlock");
        wait(handle.update_profile(agent, agent_edit("changed", vec![targeted])))
            .expect("edit after unlock");
    }

    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

#[test]
fn edits_and_submissions_take_effect_in_admission_order() {
    let (_root, directory) = data_directory("ordering");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        provider,
        targeted,
        agent,
        ..
    } = configured(&handle);
    let submission = |input| {
        handle.create_job_with_input(
            NewJob {
                configuration: configuration(agent, targeted),
                ..new_job("request", handle.genesis(), Vec::new())
            },
            input,
        )
    };

    // Edit first: the submission queued behind it freezes the new values.
    let input = wait(handle.save_job_input(None, JobInputDraft::default())).expect("save input");
    let edit = admit(handle.update_profile(targeted, llm_edit(provider, "model-new")));
    let submit = admit(submission(input));
    edit().expect("the edit runs first");
    let after_edit = submit().expect("the submission runs second");
    assert_eq!(frozen_model(&handle, after_edit), "model-new");

    // Submit first: the edit queued behind it finds the Profile held.
    let input = wait(handle.save_job_input(None, JobInputDraft::default())).expect("save input");
    let submit = admit(submission(input));
    let edit = admit(handle.update_profile(targeted, llm_edit(provider, "model-newer")));
    let before_edit = submit().expect("the submission runs first");
    assert!(matches!(violation(edit()), Violation::ProfileLocked { .. }));
    assert_eq!(frozen_model(&handle, before_edit), "model-new");
    assert_eq!(
        wait(handle.llm_profile(targeted))
            .expect("read")
            .expect("exists")
            .config
            .model,
        "model-new"
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn snapshots_and_locks_survive_reopening_and_later_edits_reach_only_later_work() {
    let (_root, directory) = data_directory("snapshots");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        provider,
        targeted,
        agent,
        ..
    } = configured(&handle);
    let earlier = configured_child(&handle, agent, targeted);
    let earlier_configuration = wait(handle.job_configuration(earlier))
        .expect("read configuration")
        .expect("configured");
    let earlier_input = wait(handle.job(earlier))
        .expect("read job")
        .expect("job exists")
        .input;
    assert_eq!(
        wait(handle.job_input(earlier_input))
            .expect("read input")
            .expect("input exists")
            .configuration
            .as_ref(),
        Some(&earlier_configuration.frozen),
        "the snapshot lives in the frozen input"
    );
    assert_eq!(
        earlier_configuration
            .frozen
            .llm
            .provider
            .credential_ref
            .as_deref(),
        Some("FIRST_KEY"),
        "the credential is named, not copied"
    );
    worker.shutdown().expect("shutdown");

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    let handle = worker.handle();
    assert_eq!(
        blocking(&handle, targeted),
        [earlier],
        "the derived lock survives reopening"
    );
    wait(handle.finalize_job(earlier, JobFinalization::Completed(result("done")))).expect("finish");
    wait(handle.update_profile(targeted, llm_edit(provider, "model-b"))).expect("edit LLM Profile");
    wait(handle.update_profile(agent, agent_edit("Answer in detail.", vec![targeted])))
        .expect("edit Agent Profile");
    let later = configured_child(&handle, agent, targeted);
    let later_configuration = wait(handle.job_configuration(later))
        .expect("read configuration")
        .expect("configured");
    assert_eq!(later_configuration.frozen.llm.config.model, "model-b");
    assert_eq!(
        later_configuration
            .frozen
            .agent
            .as_ref()
            .and_then(|agent| agent.instructions.as_deref()),
        Some("Answer in detail.")
    );
    assert_eq!(
        wait(handle.job_configuration(earlier)).expect("read configuration"),
        Some(earlier_configuration.clone()),
        "the earlier Job keeps what it froze"
    );
    assert_eq!(
        (
            later_configuration.agent_profile,
            later_configuration.llm_profile
        ),
        (Some(agent), targeted),
        "provenance names the same Profiles"
    );
    worker.shutdown().expect("shutdown");

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    let handle = worker.handle();
    assert_eq!(
        wait(handle.job_configuration(earlier)).expect("read configuration"),
        Some(earlier_configuration)
    );
    assert_eq!(
        wait(handle.job_configuration(later)).expect("read configuration"),
        Some(later_configuration)
    );
    assert_eq!(blocking(&handle, targeted), [later]);
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

#[test]
fn an_unknown_outcome_holds_its_profiles_until_its_stop_is_confirmed() {
    let (_root, directory) = data_directory("unknown");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let genesis = handle.genesis();
    let Configured {
        provider,
        targeted,
        agent,
        ..
    } = configured(&handle);
    let running = configured_child(&handle, agent, targeted);
    assert_eq!(
        violation(wait(handle.confirm_job_stopped(running))),
        Violation::StopNeedsUnknownOutcome(running),
        "work without an unknown outcome has nothing to confirm"
    );
    let unknown = configured_child(&handle, agent, targeted);
    let child = create_job(&handle, new_job("helper", unknown, Vec::new()));
    wait(handle.start_job(unknown)).expect("start");
    wait(handle.finalize_job(unknown, JobFinalization::Unknown)).expect("record unknown outcome");
    wait(handle.finalize_job(running, JobFinalization::Completed(result("done")))).expect("finish");
    for profile in [agent, targeted] {
        assert_eq!(
            blocking(&handle, profile),
            [unknown],
            "an unknown outcome may still be executing"
        );
    }
    let recorded = wait(handle.job(unknown))
        .expect("read job")
        .expect("job exists");
    assert_eq!(recorded.stop_confirmed_at, None);
    worker.shutdown().expect("shutdown");

    // A restart alone is no proof that the execution stopped.
    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    let handle = worker.handle();
    assert_eq!(blocking(&handle, targeted), [unknown]);

    // The confirmation is ordered with edits on the write worker.
    let early_edit = admit(handle.update_profile(targeted, llm_edit(provider, "model-z")));
    let confirm = admit(handle.confirm_job_stopped(unknown));
    let late_edit = admit(handle.update_profile(targeted, llm_edit(provider, "model-z")));
    assert_eq!(
        violation(early_edit()),
        Violation::ProfileLocked {
            profile: targeted,
            blocking_jobs: vec![unknown]
        }
    );
    confirm().expect("the trusted path confirms the stop");
    late_edit().expect("the edit queued after the confirmation applies");
    assert!(blocking(&handle, agent).is_empty());
    assert_eq!(
        violation(wait(handle.confirm_job_stopped(unknown))),
        Violation::StopAlreadyConfirmed(unknown)
    );

    let confirmed = wait(handle.job(unknown))
        .expect("read job")
        .expect("job exists");
    assert!(confirmed.stop_confirmed_at.is_some());
    assert_eq!(
        (
            confirmed.unknown_at,
            confirmed.completed_at,
            confirmed.failed_at,
            confirmed.cancelled_at
        ),
        (recorded.unknown_at, None, None, None),
        "the outcome stays unknown"
    );
    assert!(
        wait(handle.job_outputs(unknown, None, 10))
            .expect("read outputs")
            .is_empty(),
        "no result is published"
    );

    // A success dependency on the stopped Job still waits: the waiting
    // graph refuses the edge that would close a cycle through it.
    let dependent = create_job(
        &handle,
        NewJob {
            dependencies: vec![(unknown, DependencyCondition::Success)],
            ..new_job("needs success", genesis, Vec::new())
        },
    );
    let input = wait(handle.save_job_input(None, JobInputDraft::default())).expect("save input");
    assert!(matches!(
        violation(wait(handle.create_job_with_input(
            NewJob {
                dependencies: vec![(dependent, DependencyCondition::Success)],
                ..new_job("closes a cycle", child, Vec::new())
            },
            input
        ))),
        Violation::WaitingCycle(_)
    ));
    worker.shutdown().expect("shutdown");

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    let handle = worker.handle();
    assert!(
        blocking(&handle, targeted).is_empty(),
        "the confirmation survives reopening"
    );
    assert_eq!(
        wait(handle.job(unknown)).expect("read job"),
        Some(confirmed)
    );
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

#[test]
fn an_idle_service_holds_nothing_and_each_execution_holds_what_it_froze() {
    let (_root, directory) = data_directory("service");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        provider,
        targeted,
        agent,
        ..
    } = configured(&handle);
    let service = create_job(
        &handle,
        NewJob {
            execution_mode: ExecutionMode::Continuous,
            ..new_job("service", handle.genesis(), Vec::new())
        },
    );
    wait(handle.start_job(service)).expect("start the service");
    assert!(
        blocking(&handle, agent).is_empty() && blocking(&handle, targeted).is_empty(),
        "a service waiting for triggers holds nothing"
    );
    wait(handle.update_profile(targeted, llm_edit(provider, "model-b")))
        .expect("edit while only the service exists");

    let execution = |configuration| {
        create_job(
            &handle,
            NewJob {
                configuration,
                ..new_job("triggered execution", service, Vec::new())
            },
        )
    };
    let first = execution(configuration(agent, targeted));
    let second = execution(llm_only(targeted));
    assert_eq!(
        blocking(&handle, targeted),
        [first, second],
        "queued executions share the LLM Profile"
    );
    assert_eq!(blocking(&handle, agent), [first]);
    assert!(matches!(
        violation(wait(
            handle.update_profile(targeted, llm_edit(provider, "model-c"))
        )),
        Violation::ProfileLocked { .. }
    ));

    wait(handle.finalize_job(first, JobFinalization::Completed(result("done")))).expect("finish");
    wait(handle.update_profile(agent, agent_edit("Answer in detail.", vec![targeted])))
        .expect("the Agent Profile is released by its last holder");
    assert_eq!(
        blocking(&handle, targeted),
        [second],
        "the remaining holder keeps the LLM Profile"
    );
    wait(handle.finalize_job(second, JobFinalization::Failed(result("broken")))).expect("finish");
    wait(handle.update_profile(targeted, llm_edit(provider, "model-c")))
        .expect("edit after the last holder");

    let third = execution(configuration(agent, targeted));
    assert_eq!(frozen_model(&handle, third), "model-c");
    assert_eq!(
        wait(handle.job_configuration(third))
            .expect("read configuration")
            .and_then(|submitted| submitted.frozen.agent)
            .and_then(|agent| agent.instructions),
        Some("Answer in detail.".into())
    );
    for job in [first, second] {
        assert_eq!(
            frozen_model(&handle, job),
            "model-b",
            "{job} keeps what it froze"
        );
    }
    assert_eq!(
        wait(handle.job_configuration(service)).expect("read configuration"),
        None
    );
    assert_eq!(
        wait(handle.profile_jobs(targeted, None, 100)).expect("read Profile Jobs"),
        [first, second, third]
    );
    worker.shutdown().expect("shutdown");

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    assert_eq!(blocking(&worker.handle(), targeted), [third]);
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

#[test]
fn metrics_and_availability_keep_raw_facts_about_the_llm_their_job_froze() {
    let (_root, directory) = data_directory("observations");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        provider,
        targeted,
        untargeted,
        agent,
    } = configured(&handle);
    let metrics = |job, observed_at, measurements| {
        wait(handle.record_metrics(NewMetrics {
            job,
            observed_at: at(observed_at),
            measurements,
        }))
    };
    let observe = |job, observed_at, observation| {
        wait(handle.record_availability_stat(NewAvailabilityStat {
            job,
            observed_at: at(observed_at),
            observation,
        }))
    };

    // A request that got an answer; only some token counts were reported.
    let answered = configured_child(&handle, agent, targeted);
    let answered_metrics = metrics(
        answered,
        1_000,
        Measurements {
            ttfb_ms: Some(120),
            output_ttft_ms: Some(130),
            request_finished_in_ms: Some(250),
            usage: NormalizedUsage {
                input_tokens: Some(12),
                ..NormalizedUsage::default()
            },
            ..Measurements::default()
        },
    )
    .expect("record Metrics");
    let answered_stat = observe(answered, 1_000, response(200)).expect("record AvailabilityStat");
    // A Job may be measured any number of times.
    let answered_again = metrics(
        answered,
        1_500,
        Measurements {
            request_finished_in_ms: Some(90),
            ..Measurements::default()
        },
    )
    .expect("record more Metrics");

    // Provider failures are kept as the transport reported them, and one Job
    // may make any number of observed requests.
    let unauthorized = provider_failure("invalid API key", Some(401), false);
    let timed_out = provider_failure("the request timed out", None, true);
    let refused_by_provider = configured_child(&handle, agent, targeted);
    let unauthorized_stat = observe(refused_by_provider, 2_000, failure(unauthorized.clone()))
        .expect("record AvailabilityStat");
    let timed_out_stat = observe(refused_by_provider, 3_000, failure(timed_out.clone()))
        .expect("record AvailabilityStat");

    // A request refused locally before it was sent, and a cancellation
    // before any response, keep their Job facts and real measurements but
    // make no availability observation.
    let rejected_locally = configured_child(&handle, agent, targeted);
    let local_metrics = metrics(
        rejected_locally,
        4_000,
        Measurements {
            request_finished_in_ms: Some(1),
            ..Measurements::default()
        },
    )
    .expect("record Metrics");
    wait(handle.finalize_job(
        rejected_locally,
        JobFinalization::Failed(FinalResult {
            code: Some("REQUEST_REJECTED_LOCALLY".into()),
            ..result("the request was invalid")
        }),
    ))
    .expect("finish");
    let cancelled = configured_child(&handle, agent, targeted);
    wait(handle.finalize_job(
        cancelled,
        JobFinalization::Cancelled(result("stopped by the user")),
    ))
    .expect("finish");
    wait(handle.finalize_job(answered, JobFinalization::Completed(result("answered"))))
        .expect("finish");
    wait(handle.finalize_job(
        refused_by_provider,
        JobFinalization::Failed(result("unavailable")),
    ))
    .expect("finish");

    // Metrics are generic; work without an LLM can be measured, but it makes
    // no LLM observation.
    let orchestrator = create_job(
        &handle,
        new_job("orchestrator", handle.genesis(), Vec::new()),
    );
    let orchestrator_metrics = metrics(
        orchestrator,
        5_000,
        Measurements {
            request_finished_in_ms: Some(7),
            ..Measurements::default()
        },
    )
    .expect("record Metrics");
    let missing = JobId::new(9_999);
    let markers_before = wait(handle.operation_markers(None, 1_000)).expect("read markers");
    assert_eq!(
        violation(observe(orchestrator, 5_000, response(200))),
        Violation::NotAnLlmExecution(orchestrator)
    );
    assert_eq!(
        violation(observe(missing, 5_000, response(200))),
        Violation::NotFound(RecordRef::Job(missing))
    );
    assert_eq!(
        violation(metrics(
            missing,
            5_000,
            Measurements {
                ttfb_ms: Some(1),
                ..Measurements::default()
            }
        )),
        Violation::NotFound(RecordRef::Job(missing))
    );
    assert_eq!(
        violation(observe(
            answered,
            5_000,
            AvailabilityObservation {
                status_code: None,
                finish_reason: None,
                raw_finish_reason: None,
                failure: None
            }
        )),
        Violation::InvalidValue("observation", "records no response or failure")
    );
    assert_eq!(
        violation(metrics(answered, 5_000, Measurements::default())),
        Violation::InvalidValue("measurements", "records no measurement")
    );
    assert_eq!(
        wait(handle.operation_markers(None, 1_000)).expect("read markers"),
        markers_before,
        "refused observations committed nothing"
    );

    let read = |handle: &DatabaseHandle| {
        (
            all_metrics(handle, &of_profile(targeted), 100),
            all_availability(handle, &of_profile(targeted), 100),
        )
    };
    let (llm_metrics, availability) = read(&handle);
    assert_eq!(
        llm_metrics
            .iter()
            .map(|record| (record.id, record.job))
            .collect::<Vec<_>>(),
        [
            (answered_metrics, answered),
            (answered_again, answered),
            (local_metrics, rejected_locally)
        ]
    );
    assert_eq!(
        availability
            .iter()
            .map(|record| (record.id, record.job))
            .collect::<Vec<_>>(),
        [
            (answered_stat, answered),
            (unauthorized_stat, refused_by_provider),
            (timed_out_stat, refused_by_provider)
        ],
        "only requests that were observed count, whatever the Jobs' outcomes"
    );
    for job in [rejected_locally, cancelled] {
        assert!(
            wait(handle.job_availability(job, None, 100))
                .expect("read AvailabilityStats")
                .is_empty(),
            "{job} made no availability observation"
        );
    }
    let usage = &llm_metrics[0].measurements.usage;
    assert_eq!(
        (
            usage.input_tokens,
            usage.output_tokens,
            usage.reasoning_tokens
        ),
        (Some(12), None, None),
        "unreported token counts stay missing, not zero"
    );
    assert_eq!(llm_metrics[0].measurements.reasoning_ttft_ms, None);
    assert_eq!(
        (
            availability[1].observation.failure.as_ref(),
            availability[2].observation.failure.as_ref()
        ),
        (Some(&unauthorized), Some(&timed_out))
    );
    let frozen = LlmTarget {
        llm_profile: targeted,
        frozen: wait(handle.job_configuration(answered))
            .expect("read configuration")
            .expect("configured")
            .frozen
            .llm,
    };
    assert!(availability.iter().all(|record| record.target == frozen));
    assert!(
        llm_metrics
            .iter()
            .all(|record| record.target.as_ref() == Some(&frozen))
    );
    assert_eq!(
        wait(handle.job_metrics(orchestrator, None, 100))
            .expect("read Metrics")
            .into_iter()
            .map(|record| (record.id, record.target))
            .collect::<Vec<_>>(),
        [(orchestrator_metrics, None)]
    );
    assert!(all_availability(&handle, &of_profile(untargeted), 100).is_empty());

    // Time ranges include `from`, exclude `until`, and page by position.
    let ids = |records: Vec<AvailabilityStatRecord>| {
        records
            .into_iter()
            .map(|record| record.id)
            .collect::<Vec<_>>()
    };
    let window = ObservationFilter {
        llm_profile: Some(targeted),
        ..observed_between(2_000, 3_000)
    };
    assert_eq!(
        ids(all_availability(&handle, &window, 100)),
        [unauthorized_stat]
    );
    let first =
        wait(handle.availability_stats(of_profile(targeted), None, 2)).expect("read first page");
    assert_eq!(
        first.next,
        Some(ObservationCursor {
            observed_at: at(2_000),
            record: unauthorized_stat
        })
    );
    assert_eq!(ids(first.records), [answered_stat, unauthorized_stat]);
    let second = wait(handle.availability_stats(of_profile(targeted), first.next, 2))
        .expect("read next page");
    assert_eq!(
        (ids(second.records), second.next),
        (vec![timed_out_stat], None)
    );

    // Every holder has finished, so the Profile can be edited; history stays
    // with the configuration its Jobs froze.
    wait(handle.update_profile(targeted, llm_edit(provider, "model-b"))).expect("edit LLM Profile");
    let later = configured_child(&handle, agent, targeted);
    let later_stat = observe(later, 6_000, response(200)).expect("record AvailabilityStat");
    let (metrics_after, availability_after) = read(&handle);
    assert_eq!(metrics_after, llm_metrics);
    assert_eq!(
        availability_after[..3],
        availability[..],
        "earlier observations keep model-a"
    );
    assert_eq!(
        (
            availability_after[3].id,
            availability_after[3].target.frozen.config.model.as_str()
        ),
        (later_stat, "model-b")
    );
    worker.shutdown().expect("shutdown");

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    assert_eq!(
        read(&worker.handle()),
        (metrics_after, availability_after),
        "observations survive reopening unchanged"
    );
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

#[test]
fn observation_queries_select_frozen_targets_in_stable_bounded_pages() {
    let (_root, directory) = data_directory("observation-queries");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let genesis = handle.genesis();
    let (first, second) = (provider(&handle, "first"), provider(&handle, "second"));
    let a = llm_profile(
        &handle,
        first,
        llm_config("model-x", &[ModelCapability::Chat]),
    );
    let b = llm_profile(
        &handle,
        second,
        llm_config("model-x", &[ModelCapability::Chat]),
    );
    let c = llm_profile(
        &handle,
        first,
        llm_config("model-y", &[ModelCapability::Chat]),
    );
    let llm_job = |profile| {
        create_job(
            &handle,
            NewJob {
                configuration: llm_only(profile),
                ..new_job("request", genesis, Vec::new())
            },
        )
    };
    let (on_a, on_b, on_c) = (llm_job(a), llm_job(b), llm_job(c));
    let plain = create_job(&handle, new_job("system work", genesis, Vec::new()));
    let observe = |job, observed_at| {
        wait(handle.record_availability_stat(NewAvailabilityStat {
            job,
            observed_at: at(observed_at),
            observation: response(200),
        }))
        .expect("record AvailabilityStat")
    };
    let measure = |job, observed_at| {
        wait(handle.record_metrics(NewMetrics {
            job,
            observed_at: at(observed_at),
            measurements: Measurements {
                request_finished_in_ms: Some(10),
                ..Measurements::default()
            },
        }))
        .expect("record Metrics")
    };

    // Recording order differs from observation order, and several
    // observations share a time.
    let (s1, s2, s3) = (observe(on_a, 100), observe(on_a, 100), observe(on_a, 200));
    let s4 = observe(on_b, 100);
    let s6 = observe(on_c, 150);
    let (m1, m2, m3, m4) = (
        measure(on_a, 100),
        measure(plain, 100),
        measure(on_c, 150),
        measure(on_a, 200),
    );

    // Cancelling a Job neither erases an observation it already obtained
    // nor refuses one obtained earlier and reported after the cancellation.
    wait(handle.finalize_job(
        on_b,
        JobFinalization::Cancelled(result("stopped by the user")),
    ))
    .expect("cancel");
    let s5 = observe(on_b, 300);
    assert_eq!(
        wait(handle.job_availability(on_b, None, 100))
            .expect("read AvailabilityStats")
            .into_iter()
            .map(|record| record.id)
            .collect::<Vec<_>>(),
        [s4, s5]
    );

    // Refused writes leave no record and no index entry behind.
    let markers_before = wait(handle.operation_markers(None, 1_000)).expect("read markers");
    assert_eq!(
        violation(wait(handle.record_availability_stat(NewAvailabilityStat {
            job: plain,
            observed_at: at(100),
            observation: response(200),
        }))),
        Violation::NotAnLlmExecution(plain)
    );
    assert_eq!(
        violation(wait(handle.record_metrics(NewMetrics {
            job: on_a,
            observed_at: at(100),
            measurements: Measurements::default(),
        }))),
        Violation::InvalidValue("measurements", "records no measurement")
    );
    assert_eq!(
        wait(handle.operation_markers(None, 1_000)).expect("read markers"),
        markers_before
    );

    // Edit `a` to another Provider and model once its work has finished.
    for job in [on_a, on_c] {
        wait(handle.finalize_job(job, JobFinalization::Completed(result("done")))).expect("finish");
    }
    wait(handle.update_profile(a, llm_edit(second, "model-z"))).expect("edit LLM Profile");
    let on_a_later = llm_job(a);
    let s7 = observe(on_a_later, 400);

    let filter = |llm_profile, provider, model: Option<&str>| ObservationFilter {
        llm_profile,
        provider,
        model: model.map(str::to_owned),
        ..all_time()
    };
    let cases: Vec<(&str, ObservationFilter, Vec<AvailabilityStatId>)> = vec![
        ("everything", all_time(), vec![s1, s2, s4, s6, s3, s5, s7]),
        (
            "first Provider",
            filter(None, Some(first), None),
            vec![s1, s2, s6, s3],
        ),
        (
            "second Provider",
            filter(None, Some(second), None),
            vec![s4, s5, s7],
        ),
        (
            "model-x",
            filter(None, None, Some("model-x")),
            vec![s1, s2, s4, s3, s5],
        ),
        ("model-z", filter(None, None, Some("model-z")), vec![s7]),
        (
            "first Provider and model-x",
            filter(None, Some(first), Some("model-x")),
            vec![s1, s2, s3],
        ),
        (
            "second Provider and model-x",
            filter(None, Some(second), Some("model-x")),
            vec![s4, s5],
        ),
        (
            "Profile a, across its edit",
            filter(Some(a), None, None),
            vec![s1, s2, s3, s7],
        ),
        (
            "Profile a on the second Provider",
            filter(Some(a), Some(second), None),
            vec![s7],
        ),
        (
            "Profile a with model-x",
            filter(Some(a), None, Some("model-x")),
            vec![s1, s2, s3],
        ),
        (
            "Profile a, second Provider, model-z",
            filter(Some(a), Some(second), Some("model-z")),
            vec![s7],
        ),
        (
            "Profile b on the first Provider",
            filter(Some(b), Some(first), None),
            vec![],
        ),
        (
            "an unknown model",
            filter(None, None, Some("model-unknown")),
            vec![],
        ),
        (
            "from 100 until 150",
            observed_between(100, 150),
            vec![s1, s2, s4],
        ),
        ("from 150 until 200", observed_between(150, 200), vec![s6]),
        ("an empty range", observed_between(100, 100), vec![]),
        ("a reversed range", observed_between(200, 100), vec![]),
        (
            "around the time the records were committed",
            observed_between(Timestamp::now().as_unix_micros() - 600_000_000, i64::MAX),
            vec![],
        ),
    ];
    let check = |handle: &DatabaseHandle| {
        for (name, filter, expected) in &cases {
            for page in [1, 2, 3, 1_000] {
                let found: Vec<_> = all_availability(handle, filter, page)
                    .into_iter()
                    .map(|record| record.id)
                    .collect();
                assert_eq!(&found, expected, "{name}, {page} per page");
            }
        }
        let empty =
            wait(handle.availability_stats(filter(None, None, Some("model-unknown")), None, 10))
                .expect("read");
        assert_eq!(
            (empty.records.len(), empty.next),
            (0, None),
            "an empty result has no continuation"
        );
        let records = all_availability(handle, &filter(Some(a), None, None), 10);
        assert_eq!(
            records
                .iter()
                .map(|record| (
                    record.target.frozen.provider.id,
                    record.target.frozen.config.model.as_str()
                ))
                .collect::<Vec<_>>(),
            [
                (first, "model-x"),
                (first, "model-x"),
                (first, "model-x"),
                (second, "model-z")
            ],
            "records keep the target their Job froze"
        );
        assert!(
            records
                .iter()
                .all(|record| record.recorded_at > record.observed_at),
            "the committed time is kept apart from the observation time"
        );
        let metric_ids = |filter: ObservationFilter| {
            all_metrics(handle, &filter, 2)
                .into_iter()
                .map(|record| record.id)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            metric_ids(all_time()),
            [m1, m2, m3, m4],
            "Metrics without an LLM are listed too"
        );
        assert_eq!(
            metric_ids(filter(None, Some(first), None)),
            [m1, m3, m4],
            "but match no LLM filter"
        );
        assert_eq!(metric_ids(filter(None, None, Some("model-x"))), [m1, m4]);
        assert_eq!(
            wait(handle.job_metrics(plain, None, 10))
                .expect("read Metrics")
                .into_iter()
                .map(|record| (record.id, record.target))
                .collect::<Vec<_>>(),
            [(m2, None)]
        );
        assert_eq!(
            wait(handle.job_metrics(on_a, None, 10))
                .expect("read Metrics")
                .into_iter()
                .map(|record| record.id)
                .collect::<Vec<_>>(),
            [m1, m4]
        );
    };
    check(&handle);
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    check(&worker.handle());
    worker.shutdown().expect("shutdown");

    // A filter the scanned scope cannot narrow reads a bounded number of
    // entries per call and continues even after a segment with no match.
    let opened = crate::storage::open(directory.database_path())
        .unwrap_or_else(|_| panic!("open the closed database file"));
    let read = |filter: &ObservationFilter, after, scan_limit| {
        crate::storage::read_availability_stats(&opened.database, filter, after, 100, scan_limit)
            .expect("read")
    };
    let on_second = filter(Some(a), Some(second), None);
    let page = read(&on_second, None, 2);
    assert_eq!(
        (page.records.len(), page.next),
        (
            0,
            Some(ObservationCursor {
                observed_at: at(100),
                record: s2
            })
        ),
        "two entries of Profile a were read and neither matched"
    );
    let page = read(&on_second, page.next, 2);
    assert_eq!(
        (
            page.records
                .into_iter()
                .map(|record| record.id)
                .collect::<Vec<_>>(),
            page.next
        ),
        (vec![s7], None)
    );
    let mut after = None;
    let mut found = Vec::new();
    loop {
        let page = read(&filter(Some(a), None, Some("model-x")), after, 1);
        found.extend(page.records.into_iter().map(|record| record.id));
        match page.next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    assert_eq!(found, [s1, s2, s3]);
}

#[test]
fn observed_model_choices_come_from_the_frozen_targets_of_actual_observations() {
    use crate::persistence::{
        ObservedModel, ObservedModelChoice, ObservedModelFilter, ObservedModelPage,
    };

    let (_root, directory) = data_directory("observed-models");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let genesis = handle.genesis();
    let (first, second) = (provider(&handle, "first"), provider(&handle, "second"));
    let profile = |provider, model: &str| {
        llm_profile(
            &handle,
            provider,
            llm_config(model, &[ModelCapability::Chat]),
        )
    };
    // The same name at two Providers, a name another one starts with, a
    // name differing only in case, two Profiles of one pair, and a
    // configured model that is never observed.
    let (a, b, c, d, f) = (
        profile(first, "model"),
        profile(second, "model"),
        profile(first, "model-x"),
        profile(first, "Model"),
        profile(first, "model"),
    );
    profile(second, "unobserved");
    let llm_job = |profile| {
        create_job(
            &handle,
            NewJob {
                configuration: llm_only(profile),
                ..new_job("request", genesis, Vec::new())
            },
        )
    };
    let (on_a, on_b, on_c, on_d, on_f) =
        (llm_job(a), llm_job(b), llm_job(c), llm_job(d), llm_job(f));
    let plain = create_job(&handle, new_job("system work", genesis, Vec::new()));
    let observe = |job, observed_at| {
        wait(handle.record_availability_stat(NewAvailabilityStat {
            job,
            observed_at: at(observed_at),
            observation: response(200),
        }))
        .expect("record AvailabilityStat")
    };
    let measure = |job, observed_at| {
        wait(handle.record_metrics(NewMetrics {
            job,
            observed_at: at(observed_at),
            measurements: Measurements {
                request_finished_in_ms: Some(10),
                ..Measurements::default()
            },
        }))
        .expect("record Metrics")
    };
    for offset in 0..30 {
        observe(on_a, 100 + offset);
    }
    measure(on_f, 500);
    measure(on_b, 200);
    observe(on_c, 300);
    measure(on_d, 50);
    measure(plain, 100);
    // Once `a` is edited to another Provider and model, its old target
    // stays a choice beside the new one.
    wait(handle.finalize_job(on_a, JobFinalization::Completed(result("done")))).expect("finish");
    wait(handle.update_profile(a, llm_edit(second, "model-z"))).expect("edit LLM Profile");
    observe(llm_job(a), 600);

    let pair = |provider, model: &str| ObservedModel {
        provider,
        model: model.into(),
    };
    let choice = |provider, model: &str, has_metrics, has_availability_stats| ObservedModelChoice {
        model: pair(provider, model),
        has_metrics,
        has_availability_stats,
    };
    let filter = |from, until, provider| ObservedModelFilter {
        observed: observed_between(from, until).observed,
        provider,
    };
    let everything = filter(i64::MIN, i64::MAX, None);
    let all_choices = |handle: &DatabaseHandle, filter: ObservedModelFilter, page: usize| {
        let (mut choices, mut after) = (Vec::new(), None::<ObservedModel>);
        for _ in 0..1_000 {
            let ObservedModelPage {
                choices: found,
                next,
            } = wait(handle.observed_models(filter, after.clone(), page)).expect("read choices");
            choices.extend(found);
            match next {
                None => return choices,
                Some(next) => {
                    assert!(
                        after.is_none_or(|after| next > after),
                        "the cursor must advance"
                    );
                    after = Some(next);
                }
            }
        }
        panic!("the query did not finish");
    };
    let cases = [
        (
            "everything",
            everything,
            vec![
                choice(first, "Model", true, false),
                choice(first, "model", true, true),
                choice(first, "model-x", false, true),
                choice(second, "model", true, false),
                choice(second, "model-z", false, true),
            ],
        ),
        (
            "the second Provider",
            filter(i64::MIN, i64::MAX, Some(second)),
            vec![
                choice(second, "model", true, false),
                choice(second, "model-z", false, true),
            ],
        ),
        (
            "from 100 until 300",
            filter(100, 300, None),
            vec![
                choice(first, "model", false, true),
                choice(second, "model", true, false),
            ],
        ),
        (
            "from 300 until 301",
            filter(300, 301, None),
            vec![choice(first, "model-x", false, true)],
        ),
        (
            "the first Provider from 500",
            filter(500, i64::MAX, Some(first)),
            vec![choice(first, "model", true, false)],
        ),
        ("an empty range", filter(300, 300, None), vec![]),
        ("a reversed range", filter(300, 100, None), vec![]),
        (
            "a Provider with no observations",
            filter(i64::MIN, i64::MAX, Some(LlmProviderId::new(9_999))),
            vec![],
        ),
    ];
    let check = |handle: &DatabaseHandle| {
        for (name, filter, expected) in &cases {
            for page in [1, 2, 1_000] {
                assert_eq!(
                    &all_choices(handle, *filter, page),
                    expected,
                    "{name}, {page} per page"
                );
            }
        }
        let empty =
            wait(handle.observed_models(filter(300, 300, None), None, 10)).expect("read choices");
        assert_eq!(
            empty,
            ObservedModelPage {
                choices: Vec::new(),
                next: None
            }
        );
        // A cursor outside the Provider filter neither widens nor breaks it.
        let page = wait(handle.observed_models(
            filter(i64::MIN, i64::MAX, Some(second)),
            Some(pair(first, "Model")),
            10,
        ))
        .expect("read choices");
        assert_eq!(page.choices.len(), 2);
        let page = wait(handle.observed_models(
            filter(i64::MIN, i64::MAX, Some(first)),
            Some(pair(second, "model")),
            10,
        ))
        .expect("read choices");
        assert_eq!(
            page,
            ObservedModelPage {
                choices: Vec::new(),
                next: None
            }
        );
    };
    check(&handle);
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    check(&worker.handle());
    worker.shutdown().expect("shutdown");

    // The scan bound counts pairs, not records: thirty records of one pair
    // cost one examined pair, and pairs outside the range still advance.
    let opened = crate::storage::open(directory.database_path())
        .unwrap_or_else(|_| panic!("open the closed database file"));
    let read = |filter, after: Option<&ObservedModel>| {
        crate::storage::read_observed_models(&opened.database, &filter, after, 10, 1)
            .expect("read choices")
    };
    let page = read(everything, Some(&pair(first, "Model")));
    assert_eq!(
        page,
        ObservedModelPage {
            choices: vec![choice(first, "model", true, true)],
            next: Some(pair(first, "model"))
        }
    );
    let late = filter(600, i64::MAX, None);
    let mut pages = vec![read(late, None)];
    while let Some(next) = pages.last().and_then(|page| page.next.clone()) {
        pages.push(read(late, Some(&next)));
    }
    assert_eq!(pages.len(), 5, "one examined pair per call");
    assert!(
        pages[..4]
            .iter()
            .all(|page| page.choices.is_empty() && page.next.is_some())
    );
    assert_eq!(pages[4].choices, [choice(second, "model-z", false, true)]);
}

#[test]
fn statistics_target_lookup_reads_no_project_associations() {
    use crate::persistence::ObservedModelFilter;

    let (_root, directory) = data_directory("target-lookup");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let genesis = handle.genesis();
    let projects: Vec<_> = (0..3)
        .map(|index| project(&handle, &format!("project {index}")))
        .collect();
    let llm_provider = provider(&handle, "first");
    let llm = llm_profile(
        &handle,
        llm_provider,
        llm_config("model-a", &[ModelCapability::Chat]),
    );
    let configured = create_job(
        &handle,
        NewJob {
            configuration: llm_only(llm),
            ..new_job("request", genesis, projects.clone())
        },
    );
    let plain = create_job(&handle, new_job("system work", genesis, projects));
    for job in [configured, plain] {
        wait(handle.record_metrics(NewMetrics {
            job,
            observed_at: at(100),
            measurements: Measurements {
                request_finished_in_ms: Some(10),
                ..Measurements::default()
            },
        }))
        .expect("record Metrics");
    }
    wait(handle.record_availability_stat(NewAvailabilityStat {
        job: configured,
        observed_at: at(100),
        observation: response(200),
    }))
    .expect("record AvailabilityStat");

    let on_provider = ObservationFilter {
        provider: Some(llm_provider),
        ..all_time()
    };
    let everything = ObservedModelFilter {
        observed: all_time().observed,
        provider: None,
    };
    let read = |handle: &DatabaseHandle| {
        (
            all_metrics(handle, &all_time(), 10),
            all_metrics(handle, &on_provider, 10),
            all_availability(handle, &all_time(), 10),
            wait(handle.job_metrics(configured, None, 10)).expect("read Metrics"),
            wait(handle.job_metrics(plain, None, 10)).expect("read Metrics"),
            wait(handle.job_availability(configured, None, 10)).expect("read AvailabilityStats"),
            wait(handle.observed_models(everything, None, 10)).expect("read choices"),
        )
    };
    let before = read(&handle);
    assert_eq!((before.0.len(), before.1.len(), before.2.len()), (2, 1, 1));
    assert_eq!(
        before.4[0].target, None,
        "work without an LLM keeps no target"
    );

    // Any Project traversal now fails; a Job-detail read proves the probe is armed.
    wait(handle.with_database(crate::storage::break_project_links)).expect("arm the probe");
    assert!(
        matches!(wait(handle.job(configured)), Err(DatabaseError::Read(_))),
        "a Job-detail read traverses Projects and must trip the probe"
    );
    assert_eq!(
        read(&handle),
        before,
        "statistics reads resolve targets without Projects"
    );

    // The configured Job's frozen target is still required.
    wait(
        handle
            .with_database(move |database| crate::storage::remove_input_link(database, configured)),
    )
    .expect("remove the input link");
    fn corrupted<T>(result: Result<T, DatabaseError>) -> bool {
        matches!(result, Err(DatabaseError::Read(error)) if error.kind == crate::database::StorageErrorKind::Corrupted)
    }
    assert!(corrupted(wait(handle.metrics(all_time(), None, 10))));
    assert!(corrupted(wait(handle.availability_stats(
        all_time(),
        None,
        10
    ))));
    assert!(corrupted(wait(handle.job_metrics(configured, None, 10))));
    assert_eq!(
        wait(handle.job_metrics(plain, None, 10)).expect("read Metrics"),
        before.4,
        "a Job without an LLM needs no input to be measured"
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn job_observation_reads_are_empty_without_records_and_corrupt_only_when_dangling() {
    let (_root, directory) = data_directory("job-observation-reads");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        targeted, agent, ..
    } = configured(&handle);
    let unknown = JobId::new(9_999);
    let unobserved = configured_child(&handle, agent, targeted);
    let measured = configured_child(&handle, agent, targeted);
    wait(handle.record_metrics(NewMetrics {
        job: measured,
        observed_at: at(100),
        measurements: Measurements {
            request_finished_in_ms: Some(10),
            ..Measurements::default()
        },
    }))
    .expect("record Metrics");
    wait(handle.record_availability_stat(NewAvailabilityStat {
        job: measured,
        observed_at: at(100),
        observation: response(200),
    }))
    .expect("record AvailabilityStat");
    fn corrupted<T>(result: Result<T, DatabaseError>) -> bool {
        matches!(result, Err(DatabaseError::Read(error)) if error.kind == crate::database::StorageErrorKind::Corrupted)
    }
    let empty = |handle: &DatabaseHandle, job| {
        wait(handle.job_metrics(job, None, 10))
            .expect("read Metrics of a Job without records")
            .is_empty()
            && wait(handle.job_availability(job, None, 10))
                .expect("read AvailabilityStats of a Job without records")
                .is_empty()
    };

    assert!(
        empty(&handle, unknown),
        "an unknown Job has no records and reads an empty page"
    );
    assert!(
        empty(&handle, unobserved),
        "a configured Job without records reads an empty page"
    );

    // An empty page never resolves the frozen target, so a broken input is not read.
    wait(
        handle
            .with_database(move |database| crate::storage::remove_input_link(database, unobserved)),
    )
    .expect("remove the input link");
    assert!(
        empty(&handle, unobserved),
        "a Job without records needs no frozen target"
    );

    // Records whose Job row is gone are dangling references and must not read as empty.
    wait(handle.with_database(move |database| crate::storage::remove_job_row(database, measured)))
        .expect("remove the Job row");
    assert!(corrupted(wait(handle.job_metrics(measured, None, 10))));
    assert!(corrupted(wait(handle.job_availability(measured, None, 10))));
    worker.shutdown().expect("shutdown");
}

/// A started, configured Job whose provider outcome `error` settles.
fn settled(
    handle: &DatabaseHandle,
    agent: ProfileId,
    targeted: ProfileId,
    executor: ExecutorId,
    error: ProviderExecutionError,
) -> (JobRecord, Vec<AvailabilityStatRecord>) {
    let job = configured_child(handle, agent, targeted);
    wait(handle.start_job(job)).expect("start job");
    let input = wait(handle.job(job))
        .expect("read job")
        .expect("job exists")
        .input;
    wait(settle_provider_error(
        handle.clone(),
        executor,
        input,
        job,
        error,
    ))
    .expect("settle the provider outcome");
    let record = wait(handle.job(job))
        .expect("read job")
        .expect("job exists");
    let observations =
        wait(handle.job_availability(job, None, 10)).expect("read AvailabilityStats");
    (record, observations)
}

#[test]
fn a_local_stop_settles_unknown_and_records_only_the_response_facts_it_had() {
    let (_root, directory) = data_directory("local-stop");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        targeted, agent, ..
    } = configured(&handle);
    let executor =
        executor(&handle, "llm worker", ExecutorType::Llm, &[OPENAI]).expect("register Executor");
    let facts = |finish_reason, raw_finish_reason: Option<&str>| ResponseFacts {
        status_code: 200,
        finish_reason,
        raw_finish_reason: raw_finish_reason.map(str::to_owned),
    };

    // A user cancellation after the response began: the finish reason was seen.
    let (cancelled, observations) = settled(
        &handle,
        agent,
        targeted,
        executor,
        ProviderExecutionError::Stopped {
            message: "provider execution was cancelled while streaming".into(),
            facts: facts(Some(ChatFinishReason::Stop), Some("stop")),
        },
    );
    assert!(
        cancelled.unknown_at.is_some() && cancelled.failed_at.is_none(),
        "a local stop leaves the Job's outcome unknown, not failed"
    );
    assert_eq!(
        observations
            .iter()
            .map(|record| record.observation.clone())
            .collect::<Vec<_>>(),
        [response(200)],
        "the response facts are kept, and no provider failure is invented"
    );

    // A local output-write failure before any finish event.
    let (written, observations) = settled(
        &handle,
        agent,
        targeted,
        executor,
        ProviderExecutionError::Stopped {
            message: "database could not persist provider output".into(),
            facts: facts(None, None),
        },
    );
    assert!(written.unknown_at.is_some() && written.failed_at.is_none());
    assert_eq!(
        observations
            .iter()
            .map(|record| record.observation.clone())
            .collect::<Vec<_>>(),
        [AvailabilityObservation {
            status_code: Some(200),
            finish_reason: None,
            raw_finish_reason: None,
            failure: None,
        }],
        "a local write failure is not a provider failure"
    );
    worker.shutdown().expect("shutdown");
}

#[test]
fn provider_and_transport_failures_are_observed_and_refused_requests_are_not() {
    let (_root, directory) = data_directory("provider-failures");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        targeted, agent, ..
    } = configured(&handle);
    let executor =
        executor(&handle, "llm worker", ExecutorType::Llm, &[OPENAI]).expect("register Executor");

    let transport = ProviderExecutionError::Unknown {
        message: "connection reset".into(),
    };
    let transport_message = transport.to_string();
    let (job, observations) = settled(&handle, agent, targeted, executor, transport);
    assert!(job.unknown_at.is_some() && job.failed_at.is_none());
    assert_eq!(
        observations
            .iter()
            .map(|record| record.observation.clone())
            .collect::<Vec<_>>(),
        [failure(ProviderFailure {
            message: transport_message,
            ..ProviderFailure::default()
        })],
        "a transport failure is a genuine observation"
    );

    let rejected = provider_failure("rate limited", Some(429), true);
    let (job, observations) = settled(
        &handle,
        agent,
        targeted,
        executor,
        ProviderExecutionError::Rejected {
            failure: rejected.clone(),
        },
    );
    assert!(job.failed_at.is_some() && job.unknown_at.is_none());
    assert_eq!(
        observations
            .iter()
            .map(|record| record.observation.clone())
            .collect::<Vec<_>>(),
        [failure(rejected)],
        "a genuine HTTP rejection keeps its status and failure"
    );

    for error in [
        ProviderExecutionError::NotDelivered {
            message: "refused".into(),
        },
        ProviderExecutionError::Build {
            message: "bad request".into(),
        },
    ] {
        let (job, observations) = settled(&handle, agent, targeted, executor, error);
        assert!(job.failed_at.is_some() && job.unknown_at.is_none());
        assert!(
            observations.is_empty(),
            "a request refused before delivery has no observation"
        );
    }
    worker.shutdown().expect("shutdown");
}

#[test]
fn a_retry_shares_a_frozen_input_only_with_the_same_configuration() {
    let (_root, directory) = data_directory("configured-retry");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        provider,
        targeted,
        agent,
        ..
    } = configured(&handle);
    let original = configured_child(&handle, agent, targeted);
    let input = wait(handle.job(original))
        .expect("read job")
        .expect("job exists")
        .input;
    wait(handle.finalize_job(
        original,
        JobFinalization::Failed(result("first try failed")),
    ))
    .expect("finish");
    let retry = |original, input, configuration| JobRetry {
        original,
        input,
        name: None,
        execution_mode: ExecutionMode::Once,
        eligible_at: None,
        scheduled_at: None,
        next_due_at: None,
        next_iteration_no: None,
        iteration_no: None,
        projects: Vec::new(),
        dependencies: Vec::new(),
        configuration,
        assigned_executor: None,
    };
    let same = wait(handle.retry_job(retry(
        original,
        RetryInput::Unchanged,
        configuration(agent, targeted),
    )))
    .expect("retry with the same configuration");
    let of_retry = wait(handle.retry_job(retry(
        same,
        RetryInput::Unchanged,
        configuration(agent, targeted),
    )))
    .expect("retry the retry");
    assert_eq!(
        wait(handle.input_jobs(input)).expect("read input Jobs"),
        [original, same, of_retry]
    );
    assert_eq!(
        violation(wait(handle.retry_job(retry(
            original,
            RetryInput::Unchanged,
            None
        )))),
        Violation::ConfigurationMismatch(input),
        "dropping the configuration changes it"
    );
    assert_eq!(
        violation(wait(handle.retry_job(retry(
            original,
            RetryInput::Unchanged,
            llm_only(targeted)
        )))),
        Violation::ConfigurationMismatch(input),
        "dropping only the Agent Profile changes it too"
    );
    for job in [same, of_retry] {
        wait(handle.finalize_job(job, JobFinalization::Cancelled(result("stopped"))))
            .expect("finish");
    }

    wait(handle.update_profile(targeted, llm_edit(provider, "model-b")))
        .expect("edit after every holder finished");
    let jobs_before = wait(handle.jobs(None, 100)).expect("read jobs");
    assert_eq!(
        violation(wait(handle.retry_job(retry(
            original,
            RetryInput::Unchanged,
            configuration(agent, targeted)
        )))),
        Violation::ConfigurationMismatch(input),
        "the same Profile IDs no longer mean the configuration the input froze"
    );
    assert_eq!(
        wait(handle.jobs(None, 100)).expect("read jobs"),
        jobs_before,
        "the refused retry created nothing"
    );
    let frozen = wait(handle.job_input(input))
        .expect("read input")
        .expect("input exists")
        .configuration
        .expect("frozen");
    assert_eq!(
        frozen.llm.config.model, "model-a",
        "the old input keeps its snapshot"
    );

    let new_input =
        wait(handle.save_job_input(None, JobInputDraft::default())).expect("save a new input");
    let changed = wait(handle.retry_job(retry(
        original,
        RetryInput::Changed(new_input),
        configuration(agent, targeted),
    )))
    .expect("retry with a new input");
    assert_eq!(frozen_model(&handle, changed), "model-b");
    assert_eq!(
        wait(handle.input_jobs(new_input)).expect("read input Jobs"),
        [changed]
    );
    assert_eq!(
        wait(handle.input_jobs(input)).expect("read input Jobs"),
        [original, same, of_retry]
    );
    assert_eq!(
        wait(handle.retries(original)).expect("read retries"),
        [same, changed]
    );
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}
