use ocg::orchestration::substrate::{
    MissionId, RunContract, RunId, RunState, SubstrateRepository, WorkNodeId, WorkState,
};

fn contract(executor: &str) -> RunContract {
    RunContract {
        executor: executor.into(),
        model: "model".into(),
        role: "lead-or-worker".into(),
    }
}

#[test]
fn ownership_dependency_readiness_and_dense_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let mission = MissionId::new("task-dense").unwrap();
    let mut repo = SubstrateRepository::open(dir.path()).unwrap();
    assert_eq!(repo.journal_mode().unwrap(), "wal");
    let state = repo.create_mission(&mission, "root", 1).unwrap();
    assert_eq!(state.work_nodes.len(), 1);
    assert_eq!(state.root(), WorkNodeId(0));
    assert_eq!(state.ready(1), vec![WorkNodeId(0)]);
    let root = repo
        .start_run(&mission, WorkNodeId(0), contract("lead"), 2)
        .unwrap();
    let a = repo
        .create_child(&mission, WorkNodeId(0), root, "A", 3)
        .unwrap();
    let b = repo
        .create_child(&mission, WorkNodeId(0), root, "B", 3)
        .unwrap();
    let run_a = repo.start_run(&mission, a, contract("worker"), 4).unwrap();
    let c = repo.create_child(&mission, a, run_a, "C", 5).unwrap();
    assert_eq!((a, b, c), (WorkNodeId(1), WorkNodeId(2), WorkNodeId(3)));
    repo.add_dependency(&mission, c, b).unwrap();
    assert!(repo.add_dependency(&mission, b, c).is_err());
    drop(repo);
    let mut repo = SubstrateRepository::open(dir.path()).unwrap();
    let state = repo.load(&mission).unwrap().unwrap();
    assert_eq!(state.children(WorkNodeId(0)), vec![a, b]);
    assert_eq!(state.children(a), vec![c]);
    assert_eq!(state.work_nodes[c].parent_node_id, Some(a));
    assert_eq!(state.dependencies.len(), 1);
    assert_eq!(state.ready(6), vec![b]);
    let run_b = repo
        .start_run(&mission, b, contract("worker-b"), 6)
        .unwrap();
    assert_eq!(run_b, RunId(2));
    repo.finish_run(&mission, b, run_b, RunState::Completed, Some("result"), 7)
        .unwrap();
    assert_eq!(repo.load(&mission).unwrap().unwrap().ready(7), vec![c]);
}

#[test]
fn independent_children_are_ready_together() {
    let dir = tempfile::tempdir().unwrap();
    let mission = MissionId::new("task-parallel").unwrap();
    let mut repo = SubstrateRepository::open(dir.path()).unwrap();
    repo.create_mission(&mission, "root", 1).unwrap();
    let lead = repo
        .start_run(&mission, WorkNodeId(0), contract("lead"), 2)
        .unwrap();
    let a = repo
        .create_child(&mission, WorkNodeId(0), lead, "A", 3)
        .unwrap();
    let b = repo
        .create_child(&mission, WorkNodeId(0), lead, "B", 3)
        .unwrap();
    assert_eq!(repo.load(&mission).unwrap().unwrap().ready(3), vec![a, b]);
    repo.start_run(&mission, a, contract("worker"), 4).unwrap();
    assert_eq!(repo.load(&mission).unwrap().unwrap().ready(4), vec![b]);
}

#[test]
fn shared_replacement_fences_old_authority_and_preserves_children() {
    let dir = tempfile::tempdir().unwrap();
    let mission = MissionId::new("task-replace").unwrap();
    let mut repo = SubstrateRepository::open(dir.path()).unwrap();
    repo.create_mission(&mission, "root", 1).unwrap();
    let first = repo
        .start_run(&mission, WorkNodeId(0), contract("lead-a"), 2)
        .unwrap();
    let child = repo
        .create_child(&mission, WorkNodeId(0), first, "child", 3)
        .unwrap();
    let child_run = repo
        .start_run(&mission, child, contract("worker-a"), 4)
        .unwrap();
    let replacement = repo
        .replace_run(&mission, WorkNodeId(0), first, contract("lead-b"), 5)
        .unwrap();
    assert_eq!((first, replacement), (RunId(0), RunId(2)));
    assert!(repo
        .create_child(&mission, WorkNodeId(0), first, "stale", 6)
        .is_err());
    assert!(repo
        .replace_run(&mission, WorkNodeId(0), first, contract("lead-c"), 6)
        .is_err());
    assert!(repo
        .finish_run(&mission, WorkNodeId(0), first, RunState::Completed, None, 6)
        .is_err());
    repo.record_fenced_result(&mission, first, "late evidence")
        .unwrap();
    assert!(repo
        .record_fenced_result(&mission, first, "conflict")
        .is_err());
    let next_child = repo
        .create_child(&mission, WorkNodeId(0), replacement, "next", 6)
        .unwrap();
    let worker_b = repo
        .replace_run(&mission, child, child_run, contract("worker-b"), 7)
        .unwrap();
    assert_eq!(worker_b, RunId(3));
    drop(repo);
    let mut repo = SubstrateRepository::open(dir.path()).unwrap();
    let state = repo.load(&mission).unwrap().unwrap();
    assert_eq!(state.work_nodes[WorkNodeId(0)].generation, 2);
    assert_eq!(
        state.work_nodes[WorkNodeId(0)].active_run_id,
        Some(replacement)
    );
    assert_eq!(state.work_nodes[child].parent_node_id, Some(WorkNodeId(0)));
    assert_eq!(state.work_nodes[child].spawned_by_run_id, Some(first));
    assert_eq!(state.work_nodes[child].active_run_id, Some(worker_b));
    assert_eq!(
        state.work_nodes[next_child].spawned_by_run_id,
        Some(replacement)
    );
    assert_eq!(state.runs[first].state, RunState::Fenced);
    assert_eq!(state.runs[first].result.as_deref(), Some("late evidence"));
    assert_eq!(state.runs[first].contract().executor, "lead-a");
    assert_eq!(state.runs[replacement].contract().executor, "lead-b");
    assert_eq!(state.runs[child_run].state, RunState::Fenced);
    assert!(!state.authoritative(WorkNodeId(0), first));
    assert!(state.authoritative(WorkNodeId(0), replacement));
    assert!(state.authoritative(child, worker_b));
    assert_eq!(
        state
            .events
            .iter()
            .filter(|e| e.kind == "run_replaced")
            .count(),
        2
    );
}

#[test]
fn replacement_rolls_back_on_invalid_contract_and_terminal_history_remains() {
    let dir = tempfile::tempdir().unwrap();
    let mission = MissionId::new("task-rollback").unwrap();
    let mut repo = SubstrateRepository::open(dir.path()).unwrap();
    repo.create_mission(&mission, "root", 1).unwrap();
    let old = repo
        .start_run(&mission, WorkNodeId(0), contract("lead"), 2)
        .unwrap();
    let mut invalid = contract("new");
    invalid.executor.clear();
    assert!(repo
        .replace_run(&mission, WorkNodeId(0), old, invalid, 3)
        .is_err());
    let state = repo.load(&mission).unwrap().unwrap();
    assert_eq!(state.runs.len(), 1);
    assert!(state.authoritative(WorkNodeId(0), old));
    assert!(state.events.iter().all(|e| e.kind != "run_replaced"));
    let new = repo
        .replace_run(&mission, WorkNodeId(0), old, contract("new"), 4)
        .unwrap();
    repo.finish_run(
        &mission,
        WorkNodeId(0),
        new,
        RunState::Cancelled,
        Some("reason"),
        5,
    )
    .unwrap();
    drop(repo);
    let mut repo = SubstrateRepository::open(dir.path()).unwrap();
    let state = repo.load(&mission).unwrap().unwrap();
    assert_eq!(state.runs[old].state, RunState::Fenced);
    assert_eq!(state.runs[new].state, RunState::Cancelled);
    assert_eq!(state.work_nodes[WorkNodeId(0)].state, WorkState::Cancelled);
    assert_eq!(state.runs[new].result.as_deref(), Some("reason"));
}

#[test]
fn failed_and_superseded_runs_remain_queryable() {
    let dir = tempfile::tempdir().unwrap();
    let mission = MissionId::new("task-terminal-history").unwrap();
    let mut repo = SubstrateRepository::open(dir.path()).unwrap();
    repo.create_mission(&mission, "root", 1).unwrap();
    let lead = repo
        .start_run(&mission, WorkNodeId(0), contract("lead"), 2)
        .unwrap();
    let failed = repo
        .create_child(&mission, WorkNodeId(0), lead, "failed", 3)
        .unwrap();
    let superseded = repo
        .create_child(&mission, WorkNodeId(0), lead, "superseded", 3)
        .unwrap();
    let failed_run = repo.start_run(&mission, failed, contract("a"), 4).unwrap();
    let superseded_run = repo
        .start_run(&mission, superseded, contract("b"), 4)
        .unwrap();
    repo.finish_run(&mission, failed, failed_run, RunState::Failed, None, 5)
        .unwrap();
    repo.finish_run(
        &mission,
        superseded,
        superseded_run,
        RunState::Superseded,
        None,
        5,
    )
    .unwrap();
    drop(repo);
    let state = SubstrateRepository::open(dir.path())
        .unwrap()
        .load(&mission)
        .unwrap()
        .unwrap();
    assert_eq!(state.runs[failed_run].state, RunState::Failed);
    assert_eq!(state.runs[superseded_run].state, RunState::Superseded);
    assert_eq!(state.runs.len(), 3);
}
