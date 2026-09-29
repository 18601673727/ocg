//! Canonical execution contract tests.
//!
//! These pin the invariants of the single canonical execution authority
//! (`Project -> Job -> Attempt -> Executor -> Call -> DispatchIntent`) exposed
//! by `ocg::orchestration::domain`, plus the Call schema admission contract.

use ocg::orchestration::call_schema;
use ocg::orchestration::domain::{
    AttemptState, CanonicalAdmission, DomainRepository, ExecutionWitness, JobState,
};
use serde_json::json;
use tempfile::TempDir;

fn domain() -> (TempDir, DomainRepository) {
    let root = tempfile::tempdir().expect("temporary project root");
    let repository = DomainRepository::open(root.path()).expect("open canonical domain");
    (root, repository)
}

/// Admit one externally bound execution. Every admission in a test shares the
/// same canonical Project because the repository root is stable.
fn admitted(repository: &mut DomainRepository, binding: &str) -> CanonicalAdmission {
    let project = repository
        .ensure_project(repository.path().parent().expect("state parent"))
        .expect("ensure project");
    repository
        .admit_job(project, binding, "{\"tool\":\"value\"}", "compio")
        .expect("admit canonical job")
}

fn project_of(repository: &DomainRepository) -> ocg::orchestration::domain::Project {
    repository
        .ensure_project(repository.path().parent().expect("state parent"))
        .expect("ensure project")
}

#[test]
fn attempt_identity_and_frozen_configuration_are_immutable() {
    let (_root, mut repository) = domain();
    let project = project_of(&repository);
    let job = repository
        .create_job(&project.id, "objective")
        .expect("create Job");

    // Configuration revisions advance while the Job is still unstarted.
    let first = repository
        .set_job_configuration(&job.id, &json!({"profile": "fast"}))
        .expect("first configuration");
    let second = repository
        .set_job_configuration(&job.id, &json!({"profile": "slow"}))
        .expect("second configuration");
    assert_eq!((first, second), (1, 2));

    // Admission fixes the Attempt identity and freezes the configuration.
    let (attempt, executor) = repository
        .dispatch_job(&job.id, "lead")
        .expect("dispatch Job");
    assert_eq!(attempt.generation, 1);
    assert_eq!(executor.kind, "lead");
    assert!(repository
        .set_job_configuration(&job.id, &json!({"profile": "other"}))
        .is_err());
    let (frozen, revision) = repository
        .job_configuration(&job.id)
        .expect("read configuration")
        .expect("configuration exists");
    assert_eq!(frozen, json!({"profile": "slow"}));
    assert_eq!(revision, 2);

    // Replacement publishes a brand-new Attempt identity; the frozen
    // configuration is unchanged for that Job.
    let replacement = repository
        .replace_attempt_checked(&job.id, "worker", Some(&attempt.id))
        .expect("replace Attempt");
    assert_ne!(replacement.attempt.id, attempt.id);
    assert_eq!(
        repository
            .job_configuration(&job.id)
            .expect("read configuration")
            .expect("configuration exists"),
        (json!({"profile": "slow"}), 2)
    );
}

#[test]
fn attempt_ownership_is_authoritative() {
    let (_root, mut repository) = domain();
    let admission = admitted(&mut repository, "session-attempt");
    let authority = repository
        .authority_for_binding(&admission.project.id, "session-attempt")
        .expect("read authority")
        .expect("authoritative attempt");
    assert_eq!(authority.attempt_id, admission.attempt.id);
    assert_eq!(authority.job_id, admission.job.id);
    assert_eq!(authority.generation, admission.attempt.generation);
    assert_eq!(admission.attempt.state, AttemptState::Queued);
    assert!(admission.attempt.authoritative);
}

#[test]
fn call_ownership_rejects_an_executor_from_another_attempt() {
    let (_root, mut repository) = domain();
    let first = admitted(&mut repository, "session-call-1");
    let second = admitted(&mut repository, "session-call-2");
    let error = repository.create_call(
        &first.attempt.id,
        Some(&second.executor.id),
        first.attempt.generation,
        true,
        "{\"side_effect\":true}",
    );
    assert!(error.is_err(), "cross-attempt executor ownership must fail");

    let call = repository
        .create_call(
            &first.attempt.id,
            Some(&first.executor.id),
            first.attempt.generation,
            true,
            "{\"side_effect\":true}",
        )
        .expect("owned executor may create a call");
    assert_eq!(call.attempt_id, first.attempt.id);
    assert_eq!(
        call.executor_id.as_deref(),
        Some(first.executor.id.as_str())
    );
}

#[test]
fn late_result_is_fenced_after_attempt_replacement() {
    let (_root, mut repository) = domain();
    let admission = admitted(&mut repository, "session-late");
    let call = repository
        .create_call(
            &admission.attempt.id,
            Some(&admission.executor.id),
            admission.attempt.generation,
            true,
            "{\"side_effect\":true}",
        )
        .expect("create Call");
    repository
        .start_call(
            &call.id,
            &admission.attempt.id,
            admission.attempt.generation,
        )
        .expect("start Call");
    let witness = repository
        .witness_for_call(
            &call.id,
            &admission.attempt.id,
            admission.attempt.generation,
        )
        .expect("build ExecutionWitness");

    let replacement = repository
        .replace_attempt_checked(&admission.job.id, "worker", Some(&admission.attempt.id))
        .expect("replace Attempt");

    // A result for the replaced Attempt is retained as evidence, never applied.
    assert_eq!(
        repository
            .deliver_result(&witness, "\"late result\"", true)
            .expect("deliver late result"),
        "late_evidence"
    );
    assert_eq!(
        repository
            .job(&admission.job.id)
            .expect("read Job")
            .expect("Job")
            .authoritative_attempt_id,
        Some(replacement.attempt.id.clone())
    );
    let inspection = repository
        .inspect_job(&admission.job.id)
        .expect("inspect Job");
    let evidence = inspection["result_evidence"]
        .as_array()
        .expect("result evidence array");
    assert!(evidence
        .iter()
        .any(|entry| { entry["disposition"] == "late" && entry["call_id"] == json!(call.id) }));
}

#[test]
fn replacement_always_publishes_a_new_attempt_identity() {
    let (_root, mut repository) = domain();
    let admission = admitted(&mut repository, "session-replacement");
    let replacement = repository
        .replace_attempt_checked(&admission.job.id, "worker", Some(&admission.attempt.id))
        .expect("replace Attempt");

    assert_ne!(admission.attempt.id, replacement.attempt.id);
    assert_ne!(admission.executor.id, replacement.executor.id);
    assert_eq!(
        replacement.attempt.generation,
        admission.attempt.generation + 1
    );
    assert_eq!(replacement.job.generation, replacement.attempt.generation);
    assert!(replacement.attempt.authoritative);

    // The replaced Attempt is terminal and no longer authoritative.
    let old = repository
        .attempt(&admission.attempt.id)
        .expect("read old Attempt")
        .expect("old Attempt");
    assert!(!old.authoritative);
    assert_eq!(old.state, AttemptState::Failed);
}

#[test]
fn dependency_revision_is_monotonic() {
    let (_root, mut repository) = domain();
    let project = project_of(&repository);
    let prerequisite = repository
        .create_job(&project.id, "prerequisite")
        .expect("create prerequisite");
    let dependent = repository
        .create_job(&project.id, "dependent")
        .expect("create dependent");
    let initial = repository
        .dependency_revision(&project.id)
        .expect("initial revision");
    let added = repository
        .set_dependency(&project.id, &dependent.id, &prerequisite.id)
        .expect("add dependency");
    let removed = repository
        .remove_dependency(&project.id, &dependent.id, &prerequisite.id)
        .expect("remove dependency");
    assert!(initial < added && added < removed);
}

#[test]
fn dependency_dag_rejects_cycles() {
    let (_root, mut repository) = domain();
    let project = project_of(&repository);
    let first = repository
        .create_job(&project.id, "first")
        .expect("first job");
    let second = repository
        .create_job(&project.id, "second")
        .expect("second job");
    repository
        .set_dependency(&project.id, &first.id, &second.id)
        .expect("first edge");
    assert!(repository
        .set_dependency(&project.id, &second.id, &first.id)
        .is_err());
}

#[test]
fn unsatisfied_dependency_is_not_runnable() {
    let (_root, mut repository) = domain();
    let project = project_of(&repository);
    let prerequisite = repository
        .create_job(&project.id, "prerequisite")
        .expect("create prerequisite");
    let dependent = repository
        .create_job(&project.id, "dependent")
        .expect("create dependent");
    repository
        .set_dependency(&project.id, &dependent.id, &prerequisite.id)
        .expect("add dependency");

    // Blocked work is neither eligible nor dispatchable.
    let ready = repository.ready_jobs(&project.id).expect("ready Jobs");
    assert!(ready.iter().any(|job| job.id == prerequisite.id));
    assert!(!ready.iter().any(|job| job.id == dependent.id));
    assert!(repository.dispatch_job(&dependent.id, "worker").is_err());

    // Completing the prerequisite releases the dependent Job.
    let (attempt, _executor) = repository
        .dispatch_job(&prerequisite.id, "worker")
        .expect("dispatch prerequisite");
    repository
        .finish_attempt(&attempt.id, true)
        .expect("complete prerequisite");
    let ready = repository.ready_jobs(&project.id).expect("ready Jobs");
    assert!(ready.iter().any(|job| job.id == dependent.id));
    assert!(repository.dispatch_job(&dependent.id, "worker").is_ok());
}

#[test]
fn terminal_attempt_cannot_roll_back() {
    let (_root, mut repository) = domain();
    let admission = admitted(&mut repository, "session-terminal");
    repository
        .finish_attempt(&admission.attempt.id, true)
        .expect("complete Attempt");
    assert_eq!(
        repository
            .attempt(&admission.attempt.id)
            .expect("read Attempt")
            .expect("Attempt")
            .state,
        AttemptState::Completed
    );
    assert!(repository
        .finish_attempt(&admission.attempt.id, false)
        .is_err());
    assert_eq!(
        repository
            .attempt(&admission.attempt.id)
            .expect("read Attempt")
            .expect("Attempt")
            .state,
        AttemptState::Completed
    );
}

#[test]
fn cancellation_revokes_side_effect_authority() {
    let (_root, mut repository) = domain();
    let admission = admitted(&mut repository, "session-cancel");
    let call = repository
        .create_call(
            &admission.attempt.id,
            Some(&admission.executor.id),
            admission.attempt.generation,
            true,
            "{\"side_effect\":true}",
        )
        .expect("create side-effect Call");

    let authority = repository
        .request_cancel(&admission.attempt.id)
        .expect("request cancellation");
    assert_eq!(authority.attempt_id, admission.attempt.id);

    // A cancelling Attempt cannot admit a new side-effecting Call.
    assert!(repository
        .create_call(
            &admission.attempt.id,
            Some(&admission.executor.id),
            admission.attempt.generation,
            true,
            "{\"side_effect\":true}",
        )
        .is_err());

    // A late result for the fenced Call is retained as evidence, never applied.
    let witness = ExecutionWitness {
        job_id: admission.job.id.clone(),
        attempt_id: admission.attempt.id.clone(),
        executor_id: admission.executor.id.clone(),
        call_id: call.id.clone(),
        generation: admission.attempt.generation,
    };
    assert_eq!(
        repository
            .deliver_result(&witness, "\"late result\"", true)
            .expect("deliver late result"),
        "late_evidence"
    );

    repository
        .confirm_cancel(&admission.attempt.id, true)
        .expect("confirm cancellation");
    assert_eq!(
        repository
            .attempt(&admission.attempt.id)
            .expect("read Attempt")
            .expect("Attempt")
            .state,
        AttemptState::Cancelled
    );
}

#[test]
fn canonical_state_is_not_changed_by_projection_reads() {
    let (root, mut repository) = domain();
    let project = project_of(&repository);
    let prerequisite = repository
        .create_job(&project.id, "prerequisite")
        .expect("prerequisite");
    let dependent = repository
        .create_job(&project.id, "dependent")
        .expect("dependent");
    repository
        .set_dependency(&project.id, &dependent.id, &prerequisite.id)
        .expect("dependency");
    let projection = repository
        .rebuild_dependency_projection(&project.id)
        .expect("projection");
    assert_eq!(
        projection.prerequisites(&dependent.id),
        vec![prerequisite.id]
    );
    assert!(repository
        .projection_is_current(&projection, &project.id)
        .expect("projection freshness"));
    let canonical = repository
        .job(&dependent.id)
        .expect("read canonical job")
        .expect("job");
    assert_eq!(canonical.state, JobState::Pending);
    assert_eq!(canonical.authoritative_attempt_id, None);

    drop(repository);
    let reopened = DomainRepository::open(root.path()).expect("reopen canonical domain");
    let persisted = reopened
        .job(&dependent.id)
        .expect("read persisted canonical job")
        .expect("persisted job");
    assert_eq!(persisted.state, JobState::Pending);
    assert_eq!(persisted.authoritative_attempt_id, None);
}

#[test]
fn invalid_call_cannot_pass_schema_admission() {
    assert!(call_schema::validate_input(&json!(null)).is_err());
    assert!(call_schema::validate_output(&json!(null)).is_err());
}
