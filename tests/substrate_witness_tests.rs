//! Stable dispatch witness, authority and recovery on the canonical lane.
//!
//! Every assertion here identifies a Run through its durable witness. None of
//! them consult prompt text, agent role, runtime session ordering or
//! ready-queue position.

use ocg::orchestration::substrate::{
    DispatchWitness, MissionId, RunContract, RunId, RunState, SubstrateRepository,
    WitnessDisposition, WorkNodeId, WorkState,
};
use serde_json::json;

fn contract(executor: &str, role: &str) -> RunContract {
    RunContract {
        executor: executor.into(),
        model: "provider/model".into(),
        role: role.into(),
    }
}

/// A report shaped like one the trusted verification runner produced.
fn report() -> serde_json::Value {
    json!({
        "schema_version": 1,
        "engine_version": "test",
        "stage": "normal",
        "enabled": true,
        "ran": true,
        "results": [{"command": "cargo", "args": ["test"], "success": true}],
        "ok": true
    })
}

/// A report explicitly labelled as an operator assertion.
fn asserted_report() -> serde_json::Value {
    json!({"evidence": "operator-assertion", "note": "looked fine"})
}

fn passing(repo: &mut SubstrateRepository, witness: &DispatchWitness, now: i64) {
    repo.record_verification(witness, true, &report(), &["cargo test".into()], now)
        .unwrap();
}

#[test]
fn every_dispatch_commits_a_durable_witness_before_execution() {
    let dir = tempfile::tempdir().unwrap();
    let mission = MissionId::new("wn-witness").unwrap();
    let mut repo = SubstrateRepository::open(dir.path()).unwrap();
    let root = repo
        .create_live_mission(
            &mission,
            "goal",
            contract("lead", "lead"),
            "session-lead",
            10,
        )
        .unwrap();
    assert_eq!(root.mission_id, "wn-witness");
    assert_eq!(root.work_node_id, 0);
    assert_eq!(root.run_id, 0);
    assert_eq!(root.run_generation, 1);
    assert_eq!(root.runtime_execution_id, "session-lead");
    assert!(!root.dispatch_id.is_empty());
    assert_eq!(
        repo.validate_witness(&root).unwrap(),
        WitnessDisposition::Authoritative
    );
    // The witness is durable, not an in-memory projection.
    let stored = repo.witness(&mission, &root.dispatch_id).unwrap().unwrap();
    assert_eq!(stored, root);
    let by_run = repo.witness_for_run(&mission, RunId(0)).unwrap().unwrap();
    assert_eq!(by_run, root);
    assert_eq!(
        repo.pending_dispatches(&mission).unwrap(),
        vec![root.clone()]
    );
}

#[test]
fn concurrent_same_role_children_are_correlated_by_witness_only() {
    let dir = tempfile::tempdir().unwrap();
    let mission = MissionId::new("wn-concurrent").unwrap();
    let mut repo = SubstrateRepository::open(dir.path()).unwrap();
    let root = repo
        .create_live_mission(
            &mission,
            "goal",
            contract("lead", "lead"),
            "session-lead",
            10,
        )
        .unwrap();
    assert_eq!(root.mission_id, "wn-concurrent");
    // Two children with the identical role and the identical objective text.
    let first = repo
        .create_child_work(&mission, WorkNodeId(0), RunId(0), "same objective", &[], 11)
        .unwrap();
    let second = repo
        .create_child_work(&mission, WorkNodeId(0), RunId(0), "same objective", &[], 11)
        .unwrap();
    let a = repo
        .dispatch_run(
            &mission,
            first,
            RunId(0),
            contract("ocg-build", "worker"),
            "session-worker-a",
            12,
        )
        .unwrap();
    let b = repo
        .dispatch_run(
            &mission,
            second,
            RunId(0),
            contract("ocg-build", "worker"),
            "session-worker-b",
            12,
        )
        .unwrap();
    assert_ne!(a.run_id, b.run_id);
    assert_ne!(a.dispatch_id, b.dispatch_id);
    assert_eq!(a.work_node_id, first.0);
    assert_eq!(b.work_node_id, second.0);

    // The second result arrives first and still lands on its own Run.
    passing(&mut repo, &b, 13);
    let done = repo
        .complete_dispatch(&b, RunState::Completed, "B result", 13)
        .unwrap();
    assert_eq!(done.disposition, "authoritative");
    assert_eq!(done.node_state, "completed");
    assert!(!done.evidence_only);

    passing(&mut repo, &a, 14);
    let done = repo
        .complete_dispatch(&a, RunState::Completed, "A result", 14)
        .unwrap();
    assert_eq!(done.witness.run_id, a.run_id);
    assert_eq!(done.node_state, "completed");

    let state = repo.load(&mission).unwrap().unwrap();
    assert_eq!(
        state.runs[RunId(a.run_id)].result.as_deref(),
        Some("A result")
    );
    assert_eq!(
        state.runs[RunId(b.run_id)].result.as_deref(),
        Some("B result")
    );
    assert_eq!(state.work_nodes[first].state, WorkState::Completed);
    assert_eq!(state.work_nodes[second].state, WorkState::Completed);
    // The exact Run is recoverable from the witness alone.
    assert_eq!(
        repo.witness_for_run(&mission, RunId(a.run_id))
            .unwrap()
            .unwrap(),
        a
    );
    assert_eq!(
        repo.witness_for_run(&mission, RunId(b.run_id))
            .unwrap()
            .unwrap(),
        b
    );
}

#[test]
fn duplicate_and_mismatched_deliveries_never_mutate_authoritative_state() {
    let dir = tempfile::tempdir().unwrap();
    let mission = MissionId::new("wn-duplicate").unwrap();
    let mut repo = SubstrateRepository::open(dir.path()).unwrap();
    let root = repo
        .create_live_mission(
            &mission,
            "goal",
            contract("lead", "lead"),
            "session-lead",
            10,
        )
        .unwrap();
    let child = repo
        .create_child(&mission, WorkNodeId(0), RunId(0), "child", 11)
        .unwrap();
    let dispatched = repo
        .dispatch_run(
            &mission,
            child,
            RunId(0),
            contract("ocg-build", "worker"),
            "session-worker",
            12,
        )
        .unwrap();
    // A witness naming a different node, run, generation or binding fails
    // closed before anything is read as authority.
    for forged in [
        DispatchWitness {
            work_node_id: WorkNodeId(0).0,
            ..dispatched.clone()
        },
        DispatchWitness {
            run_id: dispatched.run_id + 7,
            ..dispatched.clone()
        },
        DispatchWitness {
            run_generation: dispatched.run_generation + 1,
            ..dispatched.clone()
        },
        DispatchWitness {
            runtime_execution_id: "session-somebody-else".into(),
            ..dispatched.clone()
        },
        DispatchWitness {
            dispatch_id: "d-unknown-r0-g1".into(),
            ..dispatched.clone()
        },
    ] {
        assert!(repo.validate_witness(&forged).is_err(), "{forged:?}");
        assert!(repo
            .complete_dispatch(&forged, RunState::Completed, "forged", 13)
            .is_err());
        assert!(repo.bind_host_session(&forged, "session-forged").is_err());
    }
    // A valid witness without evidence cannot complete the node.
    assert!(repo
        .complete_dispatch(&dispatched, RunState::Completed, "model said so", 13)
        .is_err());
    // Failed verification is recorded and still cannot complete.
    repo.record_verification(&dispatched, false, &report(), &["cargo test".into()], 13)
        .unwrap();
    assert!(repo
        .complete_dispatch(&dispatched, RunState::Completed, "model said so", 13)
        .is_err());
    // The duplicate first delivery is acknowledged, and never applied twice.
    passing(&mut repo, &dispatched, 14);
    let first = repo
        .complete_dispatch(&dispatched, RunState::Completed, "done", 14)
        .unwrap();
    assert_eq!(first.disposition, "authoritative");
    let second = repo
        .complete_dispatch(&dispatched, RunState::Completed, "done again", 15)
        .unwrap();
    assert_eq!(second.disposition, "already_applied");
    assert!(!second.evidence_only);
    let state = repo.load(&mission).unwrap().unwrap();
    assert_eq!(
        state.runs[RunId(dispatched.run_id)].result.as_deref(),
        Some("done")
    );
    assert_eq!(state.work_nodes[child].state, WorkState::Completed);
    assert_eq!(state.runs[RunId(0)].state, RunState::Active);
    assert_eq!(root.run_id, 0);
}

#[test]
fn replacement_fences_the_old_generation_and_late_results_are_evidence_only() {
    let dir = tempfile::tempdir().unwrap();
    let mission = MissionId::new("wn-replace").unwrap();
    let mut repo = SubstrateRepository::open(dir.path()).unwrap();
    repo.create_live_mission(
        &mission,
        "goal",
        contract("lead", "lead"),
        "session-lead",
        10,
    )
    .unwrap();
    let child = repo
        .create_child(&mission, WorkNodeId(0), RunId(0), "child", 11)
        .unwrap();
    let first = repo
        .dispatch_run(
            &mission,
            child,
            RunId(0),
            contract("ocg-build", "worker"),
            "session-worker-1",
            12,
        )
        .unwrap();
    // The attempt fails; a replacement Run is created, not a mutation.
    repo.complete_dispatch(&first, RunState::Failed, "provider lost", 13)
        .unwrap();
    let second = repo
        .replace_bound_run(
            &mission,
            child,
            RunId(first.run_id),
            contract("ocg-build", "worker"),
            "session-worker-2",
            14,
        )
        .unwrap();
    assert_eq!(second.work_node_id, first.work_node_id);
    assert!(second.run_id > first.run_id);
    assert_eq!(second.run_generation, first.run_generation + 1);
    assert_ne!(second.dispatch_id, first.dispatch_id);

    // The fenced generation keeps its own contract and accepts evidence only.
    assert_eq!(
        repo.validate_witness(&first).unwrap(),
        WitnessDisposition::LateEvidence
    );
    let late = repo
        .complete_dispatch(&first, RunState::Completed, "stale success", 15)
        .unwrap();
    assert_eq!(late.disposition, "late_evidence");
    assert!(late.evidence_only);
    assert_eq!(late.node_state, "running");
    let state = repo.load(&mission).unwrap().unwrap();
    assert_eq!(state.runs[RunId(first.run_id)].state, RunState::Fenced);
    // The generation's own terminal result is never rewritten by a late
    // delivery; the late text is retained separately as inspectable evidence.
    assert_eq!(
        state.runs[RunId(first.run_id)].result.as_deref(),
        Some("provider lost")
    );
    let late = repo.late_results(&mission).unwrap();
    assert_eq!(late.len(), 1);
    assert_eq!(late[0].0, child.0);
    assert_eq!(late[0].1, first.run_id);
    assert_eq!(late[0].3, "stale success");
    assert_eq!(state.work_nodes[child].state, WorkState::Running);
    assert_eq!(
        state.work_nodes[child].active_run_id,
        Some(RunId(second.run_id))
    );
    // A fenced witness can never create a child or mutate the replacement.
    assert!(repo
        .create_child(&mission, child, RunId(first.run_id), "stale child", 15)
        .is_err());
    assert!(repo
        .record_verification(&first, true, &report(), &["cargo test".into()], 15)
        .is_err());
    passing(&mut repo, &second, 16);
    let done = repo
        .complete_dispatch(&second, RunState::Completed, "replacement result", 16)
        .unwrap();
    assert_eq!(done.disposition, "authoritative");
    let state = repo.load(&mission).unwrap().unwrap();
    assert_eq!(state.work_nodes[child].state, WorkState::Completed);
    assert!(state
        .events
        .iter()
        .any(|event| event.kind == "late_result_retained"));
    assert!(state.events.iter().any(|event| event.kind == "run_fenced"));
}

#[test]
fn restart_between_dispatch_and_completion_recovers_the_witness() {
    let dir = tempfile::tempdir().unwrap();
    let mission = MissionId::new("wn-restart").unwrap();
    let mut repo = SubstrateRepository::open(dir.path()).unwrap();
    let root = repo
        .create_live_mission(
            &mission,
            "goal",
            contract("lead", "lead"),
            "session-lead",
            10,
        )
        .unwrap();
    let child = repo
        .create_child(&mission, WorkNodeId(0), RunId(0), "child", 11)
        .unwrap();
    let dispatched = repo
        .dispatch_run(
            &mission,
            child,
            RunId(0),
            contract("ocg-build", "worker"),
            "session-worker",
            12,
        )
        .unwrap();
    repo.bind_host_session(&dispatched, "host-session-7")
        .unwrap();
    drop(repo);

    let mut repo = SubstrateRepository::open(dir.path()).unwrap();
    let pending = repo.pending_dispatches(&mission).unwrap();
    // The root Lead dispatch is still pending too: recovery reports durable
    // undelivered work instead of guessing what the session was doing.
    assert_eq!(pending.len(), 2);
    assert!(pending.contains(&dispatched));
    assert!(pending.contains(&root));
    assert_eq!(
        repo.witness(&mission, &dispatched.dispatch_id)
            .unwrap()
            .unwrap(),
        dispatched
    );
    assert_eq!(
        repo.host_session(&mission, RunId(dispatched.run_id))
            .unwrap(),
        Some("host-session-7".to_string())
    );
    assert_eq!(
        repo.validate_witness(&dispatched).unwrap(),
        WitnessDisposition::Authoritative
    );
    let state = repo.load(&mission).unwrap().unwrap();
    assert!(state.authoritative(child, RunId(dispatched.run_id)));
    assert_eq!(state.ready(12), Vec::<WorkNodeId>::new());
    passing(&mut repo, &dispatched, 13);
    let done = repo
        .complete_dispatch(&dispatched, RunState::Completed, "resumed result", 13)
        .unwrap();
    assert_eq!(done.disposition, "authoritative");
    let remaining = repo.pending_dispatches(&mission).unwrap();
    assert!(!remaining.contains(&dispatched));
    // The root Lead Run has still not delivered a result, so it stays pending
    // until the terminal Mission transition closes it.
    assert_eq!(remaining, vec![root.clone()]);
    // The root witness is still authoritative across the restart.
    assert_eq!(
        repo.validate_witness(&root).unwrap(),
        WitnessDisposition::Authoritative
    );
}

#[test]
fn terminal_mission_requires_root_evidence_and_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let mission = MissionId::new("wn-terminal").unwrap();
    let mut repo = SubstrateRepository::open(dir.path()).unwrap();
    let root = repo
        .create_live_mission(
            &mission,
            "goal",
            contract("lead", "lead"),
            "session-lead",
            10,
        )
        .unwrap();
    assert!(repo.complete_mission(&mission, "completed", 11).is_err());
    passing(&mut repo, &root, 11);
    let done = repo
        .complete_dispatch(&root, RunState::Completed, "root result", 11)
        .unwrap();
    assert_eq!(done.node_state, "completed");
    repo.complete_mission(&mission, "completed", 12).unwrap();
    let events = repo.load(&mission).unwrap().unwrap().events.len();
    // Idempotent: a repeated terminal call adds no second terminal event.
    repo.complete_mission(&mission, "completed", 13).unwrap();
    assert_eq!(repo.load(&mission).unwrap().unwrap().events.len(), events);
    assert_eq!(
        repo.mission_state_row(&mission).unwrap().unwrap().0,
        "completed"
    );
    assert_eq!(root.run_generation, 1);
}

#[test]
fn pre_run_configuration_is_frozen_by_the_first_dispatch() {
    let dir = tempfile::tempdir().unwrap();
    let mission = MissionId::new("wn-config").unwrap();
    let mut repo = SubstrateRepository::open(dir.path()).unwrap();
    repo.create_mission(&mission, "goal", 1).unwrap();
    repo.set_mission_config(
        &mission,
        &json!({"profile":"careful","hard_budget":12.5}),
        2,
    )
    .unwrap();
    let (config, revision) = repo.mission_config(&mission).unwrap().unwrap();
    assert_eq!(config["profile"], json!("careful"));
    assert_eq!(revision, 1);
    repo.set_mission_config(&mission, &json!({"profile":"fast"}), 3)
        .unwrap();
    assert_eq!(repo.mission_config(&mission).unwrap().unwrap().1, 2);
    assert_eq!(
        repo.mission_config(&mission).unwrap().unwrap().0["profile"],
        json!("fast")
    );
    // An unknown Mission has no pre-run contract at all.
    let unknown = MissionId::new("wn-absent").unwrap();
    assert!(repo.mission_config(&unknown).unwrap().is_none());
    assert!(repo
        .set_mission_config(&unknown, &json!({"profile":"fast"}), 4)
        .is_err());
}

#[test]
fn dispatched_runs_freeze_pre_run_configuration() {
    let dir = tempfile::tempdir().unwrap();
    let mission = MissionId::new("wn-freeze").unwrap();
    let mut repo = SubstrateRepository::open(dir.path()).unwrap();
    repo.create_live_mission(
        &mission,
        "goal",
        contract("lead", "lead"),
        "session-lead",
        10,
    )
    .unwrap();
    let error = repo
        .set_mission_config(&mission, &json!({"profile":"fast"}), 11)
        .unwrap_err()
        .to_string();
    assert!(error.contains("frozen"), "{error}");
    // An undispatched sibling Mission keeps its editable pre-run contract.
    let other = MissionId::new("wn-freeze-2").unwrap();
    repo.create_mission(&other, "goal", 10).unwrap();
    assert_eq!(
        repo.set_mission_config(&other, &json!({"profile":"fast"}), 11)
            .unwrap(),
        1
    );
}

#[test]
fn a_substrate_from_another_repository_boundary_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let sibling = tempfile::tempdir().unwrap();
    let mission = MissionId::new("wn-boundary").unwrap();
    let mut repo = SubstrateRepository::open(dir.path()).unwrap();
    repo.create_live_mission(
        &mission,
        "goal",
        contract("lead", "lead"),
        "session-lead",
        10,
    )
    .unwrap();
    let path = repo.path().to_path_buf();
    drop(repo);
    // A copied database in a sibling checkout must not be accepted.
    let target = sibling.path().join(".ocg/orchestration");
    std::fs::create_dir_all(&target).unwrap();
    std::fs::copy(&path, target.join("substrate.sqlite3")).unwrap();
    std::fs::write(
        target.join("substrate.initialized"),
        b"sqlite-worknode-v2-witness\n",
    )
    .unwrap();
    let error = match SubstrateRepository::open(sibling.path()) {
        Ok(_) => panic!("a foreign substrate must not be accepted"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("another repository boundary"), "{error}");
}

#[test]
fn witness_structural_validation_rejects_malformed_payloads() {
    let complete = json!({
        "mission_id":"wn-shape","work_node_id":0,"run_id":0,"run_generation":1,
        "runtime_execution_id":"session-lead","dispatch_id":"d-x-r0-g1"
    });
    let witness = DispatchWitness::from_json(&complete).unwrap();
    assert_eq!(witness.dispatch_id, "d-x-r0-g1");
    for broken in [
        json!({}),
        json!({"mission_id":"","work_node_id":0,"run_id":0,"run_generation":1,"runtime_execution_id":"s","dispatch_id":"d"}),
        json!({"mission_id":"wn-shape","work_node_id":-1,"run_id":0,"run_generation":1,"runtime_execution_id":"s","dispatch_id":"d"}),
        json!({"mission_id":"wn-shape","work_node_id":0,"run_id":0,"run_generation":0,"runtime_execution_id":"s","dispatch_id":"d"}),
        json!({"mission_id":"wn shape","work_node_id":0,"run_id":0,"run_generation":1,"runtime_execution_id":"s","dispatch_id":"d"}),
        json!({"mission_id":"wn-shape","work_node_id":0,"run_id":0,"run_generation":1,"runtime_execution_id":"s","dispatch_id":"../escape"}),
    ] {
        assert!(DispatchWitness::from_json(&broken).is_err(), "{broken}");
    }
}

#[test]
fn bindings_map_a_lead_session_to_its_canonical_mission() {
    let dir = tempfile::tempdir().unwrap();
    let mission = MissionId::new("wn-binding").unwrap();
    let mut repo = SubstrateRepository::open(dir.path()).unwrap();
    let root = repo
        .create_live_mission(
            &mission,
            "goal",
            contract("lead", "lead"),
            "session-lead",
            10,
        )
        .unwrap();
    assert_eq!(
        repo.missions_by_binding("session-lead").unwrap(),
        vec![mission.clone()]
    );
    assert!(repo
        .missions_by_binding("session-other")
        .unwrap()
        .is_empty());
    assert!(repo.missions_by_binding("").is_err());
    let listed = repo.list_missions().unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].0, "wn-binding");
    assert_eq!(listed[0].2, "active");
    assert_eq!(root.run_id, 0);
}

/// An operator assertion is recorded, but it is not verification evidence and
/// can never complete a Run. Only a trusted command result can.
#[test]
fn an_operator_assertion_is_recorded_but_cannot_complete_a_run() {
    let dir = tempfile::tempdir().unwrap();
    let mission = MissionId::new("wn-assertion").unwrap();
    let mut repo = SubstrateRepository::open(dir.path()).unwrap();
    repo.create_live_mission(
        &mission,
        "goal",
        contract("lead", "lead"),
        "session-lead",
        10,
    )
    .unwrap();
    let child = repo
        .create_child(&mission, WorkNodeId(0), RunId(0), "child", 11)
        .unwrap();
    let witness = repo
        .dispatch_run(
            &mission,
            child,
            RunId(0),
            contract("ocg-build", "build"),
            "session-worker",
            12,
        )
        .unwrap();

    // An operator assertion, even a passing one, is not trusted evidence.
    let asserted = repo
        .record_verification(&witness, true, &asserted_report(), &[], 13)
        .unwrap();
    assert!(!asserted.trusted, "an operator assertion is never trusted");
    let stored = repo.verification_for(&mission, &witness).unwrap().unwrap();
    assert!(!stored.trusted);
    assert!(
        stored.passed,
        "the assertion's own verdict is preserved verbatim"
    );
    let refused = repo
        .complete_dispatch(&witness, RunState::Completed, "done", 13)
        .unwrap_err()
        .to_string();
    assert!(refused.contains("not trusted"), "{refused}");

    // A trusted command result for the same dispatch is accepted.
    repo.record_verification(&witness, true, &report(), &["cargo test".to_string()], 14)
        .unwrap();
    let latest = repo.verification_for(&mission, &witness).unwrap().unwrap();
    assert!(latest.trusted);
    assert!(latest.passed);
    let done = repo
        .complete_dispatch(&witness, RunState::Completed, "done", 14)
        .unwrap();
    assert_eq!(done.disposition, "authoritative");
    // Both records are retained: the assertion is evidence of the attempt, the
    // runner result is the evidence of the transition.
    let history = repo.verifications(&mission).unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history.iter().filter(|record| record.trusted).count(), 1);
}

/// The terminal Mission transition accepts only trusted evidence for the root
/// Run, exactly as a WorkNode completion does. An operator assertion is never
/// enough to close a Mission.
#[test]
fn a_mission_cannot_complete_on_an_operator_assertion_alone() {
    let dir = tempfile::tempdir().unwrap();
    let asserted_mission = MissionId::new("wn-assertion-mission").unwrap();
    let mut repo = SubstrateRepository::open(dir.path()).unwrap();
    let root = repo
        .create_live_mission(
            &asserted_mission,
            "goal",
            contract("lead", "lead"),
            "session-lead",
            10,
        )
        .unwrap();
    // A passing operator assertion, then a low-level root completion that
    // deliberately bypasses the dispatch-level evidence gate.
    repo.record_verification(&root, true, &asserted_report(), &[], 11)
        .unwrap();
    repo.finish_run(
        &asserted_mission,
        WorkNodeId(0),
        RunId(0),
        RunState::Completed,
        Some("done"),
        12,
    )
    .unwrap();
    let refused = repo
        .complete_mission(&asserted_mission, "completed", 13)
        .unwrap_err()
        .to_string();
    assert!(
        refused.contains("verification evidence"),
        "an operator assertion must not close a Mission: {refused}"
    );
    assert_eq!(
        repo.mission_state_row(&asserted_mission)
            .unwrap()
            .unwrap()
            .0,
        "active"
    );

    // A Mission whose root has trusted evidence does close.
    let trusted_mission = MissionId::new("wn-trusted-mission").unwrap();
    let root = repo
        .create_live_mission(
            &trusted_mission,
            "goal",
            contract("lead", "lead"),
            "session-lead-2",
            14,
        )
        .unwrap();
    repo.record_verification(&root, true, &report(), &["cargo test".into()], 15)
        .unwrap();
    repo.finish_run(
        &trusted_mission,
        WorkNodeId(0),
        RunId(0),
        RunState::Completed,
        Some("done"),
        16,
    )
    .unwrap();
    repo.complete_mission(&trusted_mission, "completed", 17)
        .unwrap();
    assert_eq!(
        repo.mission_state_row(&trusted_mission).unwrap().unwrap().0,
        "completed"
    );
}
