//! Contract tests for the persisted schema, driven through the database
//! worker exactly as callers use it: commands on the write thread, reads on
//! the read pool, and close/reopen of the same file.

use std::num::NonZeroU32;

use crate::data_directory::DataDirectory;
use crate::database::{DatabaseError, DatabaseHandle, DatabaseWorker};
use crate::openai_compatible::{
    ChatFinishReason, NormalizedUsage, ProviderFailure, TransportError,
};
use crate::orchestration::execution_runtime::{
    ExecutionPlan, ExecutionRuntime, ExecutionRuntimeError,
};
use crate::persistence::{
    AgentConfig, DependencyCondition, ExecutionConfiguration, ExecutionMode, ExecutorId,
    ExecutorType, FinalResult, FrozenLlmConfig, HealthCheckOutcome, HealthCheckTiming,
    IntermediateOutputKind, JobFinalization, JobId, JobInputDraft, JobOutcome, JobRetry, LlmConfig,
    LlmParameters, LlmProviderId, ModelCapability, NewAgent, NewExecutor, NewHealthCheck, NewJob,
    NewJobOutput, NewLlmProvider, NewProfile, NewProfileConfig, NewProject, OutputKind, ProfileId,
    ProfileKind, ProjectId, RecordRef, RetryInput, Timestamp, Violation, WrongProfileKind,
};
use crate::test_support::{TestRoot, data_directory as test_data_directory, poll_once, wait};

const SUITE: &str = "ocg-schema";

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

/// The configuration a HealthCheck of `profile` captures when it starts.
fn check_start(handle: &DatabaseHandle, profile: ProfileId) -> FrozenLlmConfig {
    wait(handle.current_llm_configuration(profile))
        .expect("read the configuration to check")
        .expect("the LLM Profile exists")
}

fn check(
    checked: FrozenLlmConfig,
    capability: ModelCapability,
    outcome: HealthCheckOutcome,
    usage: NormalizedUsage,
) -> NewHealthCheck {
    NewHealthCheck {
        checked,
        capability,
        outcome,
        usage,
        finish_reason: None,
        timing: HealthCheckTiming {
            started_at: Timestamp::from_unix_micros(1_000),
            finished_at: Timestamp::from_unix_micros(251_000),
            ttfb_ms: Some(120),
            reasoning_ttft_ms: None,
            output_ttft_ms: Some(130),
            request_finished_in_ms: Some(250),
        },
    }
}

fn provider_failure(
    message: &str,
    status_code: Option<u16>,
    retryable: bool,
) -> HealthCheckOutcome {
    HealthCheckOutcome::Failed(TransportError::Provider(ProviderFailure {
        message: message.into(),
        status_code,
        provider_code: status_code.map(|_| "invalid_api_key".into()),
        response_body: None,
        request_id: Some("request-1".into()),
        retry_after_ms: None,
        retryable,
    }))
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
        serves: Vec::new(),
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
        },
    ))
    .expect("append output");
    let checked = wait(handle.record_health_check(
        llm,
        check(
            check_start(&handle, llm),
            ModelCapability::Chat,
            HealthCheckOutcome::Passed,
            NormalizedUsage {
                input_tokens: Some(12),
                output_tokens: Some(3),
                raw: Some(serde_json::json!({"prompt_tokens": 12, "completion_tokens": 3})),
                ..NormalizedUsage::default()
            },
        ),
    ))
    .expect("record HealthCheck");

    let read_all = |handle: &DatabaseHandle| {
        (
            wait(handle.jobs(None, 100)).expect("read jobs"),
            wait(handle.job_outputs(parent, None, 100)).expect("read outputs"),
            wait(handle.projects(None, 100)).expect("read projects"),
            wait(handle.project_jobs(beta, None, 100)).expect("read Project Jobs"),
            wait(handle.llm_providers(None, 100)).expect("read providers"),
            wait(handle.profiles(None, 100)).expect("read Profiles"),
            wait(handle.agent(agent)).expect("read Agent"),
            wait(handle.health_checks(llm, None, 100)).expect("read HealthChecks"),
            wait(handle.children(parent, None, 100)).expect("read children"),
            wait(handle.provider_profiles(first_provider)).expect("read provider Profiles"),
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
        checks,
        children,
        provider_profiles,
    ) = after;
    assert_eq!(jobs.len(), 4, "Genesis and the three created Jobs");
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
        checks.iter().map(|check| check.id).collect::<Vec<_>>(),
        [checked]
    );
    assert_eq!(checks[0].usage.input_tokens, Some(12));
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
fn health_checks_are_kept_apart_from_declarations_and_verify_one_capability() {
    let (_root, directory) = data_directory("health-checks");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let llm_provider = provider(&handle, "first");
    let declared = [ModelCapability::Chat, ModelCapability::Tools];
    let llm = llm_profile(&handle, llm_provider, llm_config("model-a", &declared));
    let declaration = wait(handle.profile(llm))
        .expect("read Profile")
        .expect("Profile exists");

    let unauthorized = provider_failure("invalid API key", Some(401), false);
    let timed_out = provider_failure("the request timed out", None, true);
    let usage = NormalizedUsage {
        input_tokens: Some(12),
        output_tokens: Some(3),
        ..NormalizedUsage::default()
    };
    let first = wait(handle.record_health_check(
        llm,
        check(
            check_start(&handle, llm),
            ModelCapability::Chat,
            unauthorized.clone(),
            NormalizedUsage::default(),
        ),
    ))
    .expect("record failed check");
    let second = wait(handle.record_health_check(
        llm,
        check(
            check_start(&handle, llm),
            ModelCapability::Chat,
            timed_out.clone(),
            NormalizedUsage::default(),
        ),
    ))
    .expect("record failed check");
    assert_eq!(
        wait(handle.profile(llm)).expect("read Profile"),
        Some(declaration.clone()),
        "failures leave the declaration as declared"
    );

    let passed = NewHealthCheck {
        finish_reason: Some(ChatFinishReason::Stop),
        ..check(
            check_start(&handle, llm),
            ModelCapability::Chat,
            HealthCheckOutcome::Passed,
            usage.clone(),
        )
    };
    let third = wait(handle.record_health_check(llm, passed)).expect("record passed check");

    let history = wait(handle.health_checks(llm, None, 100)).expect("read history");
    assert_eq!(
        history.iter().map(|check| check.id).collect::<Vec<_>>(),
        [first, second, third],
        "earlier failures are kept"
    );
    assert_eq!(
        history[0].outcome, unauthorized,
        "the authentication failure is stored as observed"
    );
    assert_eq!(
        history[1].outcome, timed_out,
        "the timeout is stored as observed"
    );
    assert_eq!(
        history[0].usage,
        NormalizedUsage::default(),
        "unreported usage stays unreported, not zero"
    );
    assert_eq!(
        (
            history[2].usage.input_tokens,
            history[2].usage.cache_read_tokens
        ),
        (Some(12), None)
    );
    assert_eq!(
        (
            history[2].timing.ttfb_ms,
            history[2].timing.reasoning_ttft_ms
        ),
        (Some(120), None)
    );
    assert_eq!(history[2].finish_reason, Some(ChatFinishReason::Stop));
    assert!(history.iter().all(|check| check.profile == llm));

    let latest = wait(handle.current_health_checks(llm)).expect("read current checks");
    assert_eq!(
        latest.keys().copied().collect::<Vec<_>>(),
        [ModelCapability::Chat],
        "a chat check verifies chat only"
    );
    assert_eq!(
        latest[&ModelCapability::Chat].outcome,
        HealthCheckOutcome::Passed
    );
    assert_eq!(
        wait(handle.profile(llm)).expect("read Profile"),
        Some(declaration),
        "a pass does not rewrite the declaration either"
    );

    let assistant = agent_profile(&handle, "Answer briefly.", Vec::new());
    assert!(matches!(
        violation(wait(handle.record_health_check(
            assistant,
            check(
                check_start(&handle, llm),
                ModelCapability::Chat,
                HealthCheckOutcome::Passed,
                NormalizedUsage::default()
            )
        ))),
        Violation::WrongProfileKind(_)
    ));
    worker.shutdown().expect("shutdown");

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    assert_eq!(
        wait(worker.handle().health_checks(llm, None, 100)).expect("read history"),
        history,
        "history survives reopening"
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
                goal_reached: None
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
    serves: Vec<ProfileId>,
) -> Result<ExecutorId, DatabaseError> {
    wait(handle.create_executor(NewExecutor {
        name: name.into(),
        executor_type,
        concurrency_limit: NonZeroU32::MIN,
        serves,
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
        agent_profile,
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
    let llm_executor = executor(&handle, "model-a worker", ExecutorType::Llm, vec![targeted])
        .expect("register Executor");
    let other_executor = executor(
        &handle,
        "model-b worker",
        ExecutorType::Llm,
        vec![untargeted],
    )
    .expect("register Executor");
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
            Violation::ExecutorCannotServe {
                executor: other_executor,
                llm_profile: targeted,
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
        (agent, targeted, provider)
    );
    assert_eq!(
        effective.frozen.agent.instructions.as_deref(),
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

    // Everything that uses or checks the Profiles leaves them as created.
    let llm_executor = executor(
        &handle,
        "worker",
        ExecutorType::Llm,
        vec![targeted, untargeted],
    )
    .expect("register Executor");
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
    wait(handle.record_health_check(
        targeted,
        check(
            check_start(&handle, targeted),
            ModelCapability::Tools,
            failed,
            NormalizedUsage::default(),
        ),
    ))
    .expect("record check");
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
fn only_an_executor_registered_for_the_llm_profile_is_assigned() {
    let (_root, directory) = data_directory("executors");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        targeted,
        untargeted,
        agent,
        ..
    } = configured(&handle);
    assert_eq!(
        violation(executor(
            &handle,
            "tool",
            ExecutorType::Tool,
            vec![targeted]
        )),
        Violation::InvalidValue("serves", "only an Llm Executor can serve LLM Profiles")
    );
    assert!(matches!(
        violation(executor(
            &handle,
            "confused",
            ExecutorType::Llm,
            vec![agent]
        )),
        Violation::WrongProfileKind(_)
    ));
    let tool =
        executor(&handle, "tool", ExecutorType::Tool, Vec::new()).expect("register tool Executor");
    let other = executor(
        &handle,
        "model-b worker",
        ExecutorType::Llm,
        vec![untargeted],
    )
    .expect("register Executor");
    let first = executor(&handle, "model-a worker", ExecutorType::Llm, vec![targeted])
        .expect("register Executor");
    assert_eq!(
        wait(handle.compatible_executors(targeted)).expect("read candidates"),
        [first]
    );

    let request = create_job(
        &handle,
        NewJob {
            configuration: configuration(agent, targeted),
            ..new_job("request", handle.genesis(), Vec::new())
        },
    );
    let unassigned = wait(handle.job(request))
        .expect("read job")
        .expect("job exists");
    assert_eq!(
        violation(wait(handle.assign_job_executor(request, other))),
        Violation::ExecutorCannotServe {
            executor: other,
            llm_profile: targeted
        }
    );
    assert_eq!(
        violation(wait(handle.assign_job_executor(request, tool))),
        Violation::IncompatibleExecutorKind {
            executor: tool,
            found: ExecutorType::Tool
        }
    );
    assert_eq!(
        wait(handle.job(request)).expect("read job"),
        Some(unassigned),
        "refused assignments changed nothing"
    );
    wait(handle.assign_job_executor(request, first)).expect("assign a compatible Executor");

    // A replacement instance serves the same Profile; nothing about the
    // configuration changes when it takes over or serves a later child.
    let profiles =
        [agent, targeted].map(|profile| wait(handle.profile(profile)).expect("read Profile"));
    let replacement = executor(
        &handle,
        "model-a replacement",
        ExecutorType::Llm,
        vec![targeted],
    )
    .expect("register replacement");
    assert_eq!(
        wait(handle.compatible_executors(targeted)).expect("read candidates"),
        [first, replacement]
    );
    wait(handle.assign_job_executor(request, replacement)).expect("replace before start");
    let later = create_job(
        &handle,
        NewJob {
            configuration: configuration(agent, targeted),
            assigned_executor: Some(replacement),
            ..new_job("later request", handle.genesis(), Vec::new())
        },
    );
    for job in [request, later] {
        let record = wait(handle.job(job))
            .expect("read job")
            .expect("job exists");
        assert_eq!(
            (record.configuration, record.assigned_executor),
            (configuration(agent, targeted), Some(replacement))
        );
    }
    assert_eq!(
        [agent, targeted].map(|profile| wait(handle.profile(profile)).expect("read Profile")),
        profiles
    );

    let unconfigured = create_job(
        &handle,
        new_job("system work", handle.genesis(), Vec::new()),
    );
    wait(handle.assign_job_executor(unconfigured, tool))
        .expect("work without an LLM Profile keeps any Executor");
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

#[test]
fn the_execution_runtime_runs_a_configured_child_only_on_a_compatible_executor() {
    let (_root, directory) = data_directory("runtime");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        targeted,
        untargeted,
        agent,
        ..
    } = configured(&handle);
    let compatible = executor(&handle, "model-a worker", ExecutorType::Llm, vec![targeted])
        .expect("register Executor");
    let incompatible = executor(
        &handle,
        "model-b worker",
        ExecutorType::Llm,
        vec![untargeted],
    )
    .expect("register Executor");
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
                Violation::ExecutorCannotServe {
                    executor: incompatible,
                    llm_profile: targeted
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
        // Idle uses hold nothing: Agent membership, Executor registration and checks.
        wait(handle.create_agent(NewAgent {
            name: "helper".into(),
            profiles: vec![agent],
        }))
        .expect("create Agent");
        executor(&handle, "worker", ExecutorType::Llm, vec![targeted]).expect("register Executor");
        wait(handle.record_health_check(
            targeted,
            check(
                check_start(&handle, targeted),
                ModelCapability::Chat,
                HealthCheckOutcome::Passed,
                NormalizedUsage::default(),
            ),
        ))
        .expect("record check");
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

    // An unknown outcome may still be executing, so it keeps holding.
    let Configured {
        provider,
        targeted,
        agent,
        ..
    } = configured(&handle);
    let unknown = configured_child(&handle, agent, targeted);
    wait(handle.finalize_job(unknown, JobFinalization::Unknown)).expect("record unknown outcome");
    assert_eq!(blocking(&handle, targeted), [unknown]);
    assert!(matches!(
        violation(wait(
            handle.update_profile(targeted, llm_edit(provider, "model-z"))
        )),
        Violation::ProfileLocked { .. }
    ));
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
        later_configuration.frozen.agent.instructions.as_deref(),
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
        (agent, targeted),
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
fn executors_match_the_frozen_configuration_not_just_the_profile_id() {
    let (_root, directory) = data_directory("registrations");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        provider,
        targeted,
        agent,
        ..
    } = configured(&handle);
    let before_edit = executor(&handle, "model-a worker", ExecutorType::Llm, vec![targeted])
        .expect("register Executor");
    let earlier = configured_child(&handle, agent, targeted);
    wait(handle.finalize_job(earlier, JobFinalization::Unknown))
        .expect("leave the earlier Job unresolved");

    // That unknown-outcome Job keeps holding the Profile, so the edit below
    // uses a second, releasable setup.
    let Configured {
        provider: other_provider,
        targeted: edited,
        agent: other_agent,
        ..
    } = configured(&handle);
    let registered =
        executor(&handle, "worker", ExecutorType::Llm, vec![edited]).expect("register Executor");
    let old_work = configured_child(&handle, other_agent, edited);
    wait(handle.finalize_job(old_work, JobFinalization::Completed(result("done"))))
        .expect("finish");
    let pending = configured_child(&handle, other_agent, edited);
    assert_eq!(
        wait(handle.job_compatible_executors(pending)).expect("read candidates"),
        [registered]
    );
    wait(handle.finalize_job(pending, JobFinalization::Failed(result("failed")))).expect("finish");
    wait(handle.update_profile(edited, llm_edit(other_provider, "model-b")))
        .expect("edit after unlock");

    assert!(
        wait(handle.compatible_executors(edited))
            .expect("read candidates")
            .is_empty(),
        "a registration for the old configuration does not serve the new one"
    );
    let new_work = configured_child(&handle, other_agent, edited);
    assert_eq!(
        violation(wait(handle.assign_job_executor(new_work, registered))),
        Violation::ExecutorCannotServe {
            executor: registered,
            llm_profile: edited
        }
    );
    assert_eq!(
        wait(handle.job(new_work))
            .expect("read job")
            .expect("job exists")
            .assigned_executor,
        None
    );
    let reregistered = executor(&handle, "model-b worker", ExecutorType::Llm, vec![edited])
        .expect("register for the new configuration");
    assert_eq!(
        wait(handle.compatible_executors(edited)).expect("read candidates"),
        [reregistered]
    );
    assert_eq!(
        wait(handle.job_compatible_executors(new_work)).expect("read candidates"),
        [reregistered]
    );
    assert_eq!(
        wait(handle.job_compatible_executors(old_work)).expect("read candidates"),
        [registered],
        "old work still matches the registration it froze against"
    );
    wait(handle.assign_job_executor(new_work, reregistered)).expect("assign the matching Executor");
    let served = wait(handle.executor(registered))
        .expect("read Executor")
        .expect("Executor exists")
        .serves;
    assert_eq!(
        served
            .iter()
            .map(|served| served.configuration.config.model.as_str())
            .collect::<Vec<_>>(),
        ["model-a"]
    );

    // The first setup's Profile stays locked by its unknown-outcome Job.
    assert_eq!(blocking(&handle, targeted), [earlier]);
    assert!(matches!(
        violation(wait(
            handle.update_profile(targeted, llm_edit(provider, "model-b"))
        )),
        Violation::ProfileLocked { .. }
    ));
    assert_eq!(
        wait(handle.job_compatible_executors(earlier)).expect("read candidates"),
        [before_edit]
    );
    worker.shutdown().expect("shutdown");
    assert_relationships_consistent(&directory);
}

#[test]
fn health_checks_keep_the_configuration_they_checked() {
    let (_root, directory) = data_directory("checked");
    let worker = DatabaseWorker::start(&directory).expect("start database");
    let handle = worker.handle();
    let Configured {
        provider, targeted, ..
    } = configured(&handle);
    let passed = wait(handle.record_health_check(
        targeted,
        check(
            check_start(&handle, targeted),
            ModelCapability::Chat,
            HealthCheckOutcome::Passed,
            NormalizedUsage::default(),
        ),
    ))
    .expect("record check");
    // This check started before the edit and finishes after it.
    let started_before_edit = check_start(&handle, targeted);
    wait(handle.update_profile(targeted, llm_edit(provider, "model-b")))
        .expect("edit the unlocked Profile");
    let late = wait(handle.record_health_check(
        targeted,
        check(
            started_before_edit,
            ModelCapability::Tools,
            HealthCheckOutcome::Passed,
            NormalizedUsage::default(),
        ),
    ))
    .expect("record the late result");

    let history = wait(handle.health_checks(targeted, None, 100)).expect("read history");
    assert_eq!(
        history
            .iter()
            .map(|check| (check.id, check.checked.config.model.as_str()))
            .collect::<Vec<_>>(),
        [(passed, "model-a"), (late, "model-a")]
    );
    assert!(
        wait(handle.current_health_checks(targeted))
            .expect("read current checks")
            .is_empty(),
        "checks of model-a do not verify model-b"
    );
    assert_eq!(
        wait(handle.llm_profile(targeted))
            .expect("read")
            .expect("exists")
            .config
            .declared_capabilities,
        std::collections::BTreeSet::from([ModelCapability::Chat])
    );

    let failed = provider_failure("invalid API key", Some(401), false);
    let current = wait(handle.record_health_check(
        targeted,
        check(
            check_start(&handle, targeted),
            ModelCapability::Chat,
            failed.clone(),
            NormalizedUsage::default(),
        ),
    ))
    .expect("record check of the current configuration");
    let verified = wait(handle.current_health_checks(targeted)).expect("read current checks");
    assert_eq!(
        verified.keys().copied().collect::<Vec<_>>(),
        [ModelCapability::Chat]
    );
    assert_eq!(
        (
            verified[&ModelCapability::Chat].id,
            &verified[&ModelCapability::Chat].outcome
        ),
        (current, &failed)
    );
    worker.shutdown().expect("shutdown");

    let worker = DatabaseWorker::start(&directory).expect("reopen database");
    assert_eq!(
        wait(worker.handle().health_checks(targeted, None, 2)).expect("read history"),
        history,
        "historical checks survive reopening unchanged"
    );
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
