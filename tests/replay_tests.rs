//! Focused tests for the Phase 2B-1 durable replay authority.
//!
//! These tests are deterministic and use only local temporary directories. The
//! cross-process lock is the real advisory file lock used by the service; the
//! concurrency tests exercise it from multiple threads, which the OS treats as
//! independent lock owners because each operation opens its own handle.

use ocg::orchestration::{
    mission, policy, state_path, ApprovalRecord, ApprovalStatus, AuthoritativeSnapshot, Cursor,
    DomainEvent, Mission, ReplayAfter, SnapshotConfig, SnapshotService, MAX_APPROVALS,
};
use ocg::resources::{self, ResourceIdentity, ResourceObservation};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

fn mission(index: u64, now: i64) -> Mission {
    Mission::admit(&format!("task-{index:016}"), "task", "session-1", now)
}

fn appended(service: &SnapshotService, index: u64) -> Cursor {
    service
        .append(DomainEvent::MissionUpsert {
            mission: mission(index, index as i64),
        })
        .unwrap()
        .expect("a distinct mission is always a state change")
}

#[test]
fn concurrent_appends_have_a_unique_strict_total_order() {
    let dir = tempfile::tempdir().unwrap();
    let service = SnapshotService::open(dir.path()).unwrap();
    let threads = 8usize;
    let per_thread = 16usize;
    let mut handles = Vec::new();
    for thread_index in 0..threads {
        let service = service.clone();
        handles.push(thread::spawn(move || {
            let mut seqs = Vec::new();
            for offset in 0..per_thread {
                let index = (thread_index * per_thread + offset + 1) as u64;
                seqs.push(appended(&service, index).seq);
            }
            seqs
        }));
    }
    let mut seqs: Vec<u64> = handles
        .into_iter()
        .flat_map(|handle| handle.join().unwrap())
        .collect();
    seqs.sort_unstable();
    let expected: Vec<u64> = (1..=(threads * per_thread) as u64).collect();
    assert_eq!(seqs, expected, "every append gets a unique, gap-free seq");

    let (snapshot, head) = service.snapshot_with_cursor().unwrap();
    assert_eq!(
        head,
        Cursor {
            epoch: 1,
            seq: expected.len() as u64
        }
    );
    assert_eq!(snapshot.missions.len(), expected.len());
}

#[test]
fn restart_preserves_epoch_and_head() {
    let dir = tempfile::tempdir().unwrap();
    let service = SnapshotService::open(dir.path()).unwrap();
    appended(&service, 1);
    appended(&service, 2);
    let head = appended(&service, 3);
    drop(service);

    let reopened = SnapshotService::open(dir.path()).unwrap();
    let (snapshot, cursor) = reopened.snapshot_with_cursor().unwrap();
    assert_eq!(cursor, head);
    assert_eq!(cursor.epoch, 1);
    assert_eq!(snapshot.missions.len(), 3);
}

#[test]
fn replay_after_a_cursor_is_strict() {
    let dir = tempfile::tempdir().unwrap();
    let service = SnapshotService::open(dir.path()).unwrap();
    appended(&service, 1);
    let cursor = appended(&service, 2);
    appended(&service, 3);
    appended(&service, 4);

    let ReplayAfter::Success { events } = service.replay_after(cursor) else {
        panic!("expected a strict suffix");
    };
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].cursor.seq, 3);
    assert_eq!(events[1].cursor.seq, 4);
    // The hash chain is intact across the returned suffix.
    assert_eq!(events[0].prev_hash, service_cursor_hash(&service, cursor));
}

/// The hash of the envelope at `cursor` (its own hash), read from the store.
fn service_cursor_hash(service: &SnapshotService, cursor: Cursor) -> String {
    let ReplayAfter::Success { events } = service.replay_after(Cursor {
        epoch: cursor.epoch,
        seq: cursor.seq.saturating_sub(1),
    }) else {
        panic!("expected to read the previous envelope");
    };
    let first = events.first().expect("previous envelope is retained");
    assert_eq!(first.cursor, cursor);
    first.hash.clone()
}

#[test]
fn replay_after_head_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let service = SnapshotService::open(dir.path()).unwrap();
    appended(&service, 1);
    let head = service.head().unwrap();
    assert_eq!(service.replay_after(head), ReplayAfter::Empty);
}

#[test]
fn replay_distinguishes_wrong_epoch_and_future() {
    let dir = tempfile::tempdir().unwrap();
    let service = SnapshotService::open(dir.path()).unwrap();
    appended(&service, 1);
    let head = service.head().unwrap();

    assert!(matches!(
        service.replay_after(Cursor {
            epoch: head.epoch + 1,
            seq: head.seq,
        }),
        ReplayAfter::WrongEpoch { .. }
    ));
    assert!(matches!(
        service.replay_after(Cursor {
            epoch: head.epoch,
            seq: head.seq + 5,
        }),
        ReplayAfter::Future { .. }
    ));
}

#[test]
fn expired_replay_never_returns_a_partial_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let service = SnapshotService::open_with_config(dir.path(), SnapshotConfig::new(4)).unwrap();
    for index in 1..=10 {
        appended(&service, index);
    }
    let head = service.head().unwrap();
    assert_eq!(head, Cursor { epoch: 1, seq: 10 });

    let ReplayAfter::Expired { floor_seq, .. } = service.replay_after(Cursor { epoch: 1, seq: 0 })
    else {
        panic!("the pruned prefix must be reported as expired");
    };
    assert_eq!(floor_seq, 7, "ten events with retention four keep 7..=10");

    // The floor boundary itself is still fully replayable.
    let ReplayAfter::Success { events } = service.replay_after(Cursor { epoch: 1, seq: 6 }) else {
        panic!("the retained suffix must be complete");
    };
    assert_eq!(events.len(), 4);
    assert_eq!(events[0].cursor.seq, 7);
    assert_eq!(events[3].cursor.seq, 10);
}

#[test]
fn corrupt_hash_chain_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let service = SnapshotService::open(dir.path()).unwrap();
    appended(&service, 1);
    appended(&service, 2);
    let path = state_path(dir.path());
    let mut value: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
    value["journal"][0]["hash"] = serde_json::Value::String("deadbeef".to_string());
    fs::write(&path, value.to_string()).unwrap();

    assert!(SnapshotService::open(dir.path()).is_err());
    assert!(matches!(
        service.replay_after(Cursor { epoch: 1, seq: 0 }),
        ReplayAfter::PersistenceFailure { .. }
    ));
}

#[test]
fn corrupt_document_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let service = SnapshotService::open(dir.path()).unwrap();
    appended(&service, 1);
    fs::write(state_path(dir.path()), "{ this is not a valid document").unwrap();

    assert!(SnapshotService::open(dir.path()).is_err());
    assert!(matches!(
        service.replay_after(Cursor { epoch: 1, seq: 0 }),
        ReplayAfter::PersistenceFailure { .. }
    ));
}

#[test]
fn a_missing_store_after_the_marker_is_a_persistence_failure() {
    let dir = tempfile::tempdir().unwrap();
    let service = SnapshotService::open(dir.path()).unwrap();
    appended(&service, 1);
    fs::remove_file(state_path(dir.path())).unwrap();
    // The initialization marker is still present.
    assert!(dir
        .path()
        .join(".ocg/orchestration/replay.initialized")
        .is_file());

    assert!(SnapshotService::open(dir.path()).is_err());
    assert!(matches!(
        service.replay_after(Cursor { epoch: 1, seq: 0 }),
        ReplayAfter::PersistenceFailure { .. }
    ));
}

#[test]
fn epoch_changes_only_by_explicit_assertion() {
    let dir = tempfile::tempdir().unwrap();
    let service = SnapshotService::open(dir.path()).unwrap();
    let old = appended(&service, 1);

    // A blank reason is rejected; normal corruption never resets automatically.
    assert!(SnapshotService::begin_new_epoch_after_continuity_loss(
        dir.path(),
        AuthoritativeSnapshot::default(),
        "   ",
    )
    .is_err());

    let reset = SnapshotService::begin_new_epoch_after_continuity_loss(
        dir.path(),
        AuthoritativeSnapshot::default(),
        "operator reset after the durable volume was lost",
    )
    .unwrap();
    let (snapshot, cursor) = reset.snapshot_with_cursor().unwrap();
    assert_eq!(cursor, Cursor { epoch: 2, seq: 0 });
    assert!(snapshot.missions.is_empty());
    assert!(matches!(
        reset.replay_after(old),
        ReplayAfter::WrongEpoch {
            expected: 2,
            got: 1
        }
    ));
}

#[test]
fn first_initialization_bootstraps_existing_domains() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();

    let mission = mission(1, 1);
    let missions = mission::missions_dir(root);
    fs::create_dir_all(&missions).unwrap();
    fs::write(
        missions.join(format!("{}.json", mission.mission_id)),
        serde_json::to_vec(&mission).unwrap(),
    )
    .unwrap();

    let approval = ApprovalRecord {
        approval_id: "apr-0000000000000001".to_string(),
        mission_id: mission.mission_id.clone(),
        generation: 1,
        action: "ensure_execution".to_string(),
        status: ApprovalStatus::Pending,
        requested_at: 1,
        ..ApprovalRecord::default()
    };
    let approvals = policy::approval_dir(root);
    fs::create_dir_all(&approvals).unwrap();
    fs::write(
        approvals.join(format!("{}.json", approval.approval_id)),
        serde_json::to_vec(&approval).unwrap(),
    )
    .unwrap();

    let identity = ResourceIdentity::for_model("openai", "gpt");
    let observation = ResourceObservation::for_identity(&identity, 1);
    let mut records = serde_json::Map::new();
    records.insert(
        observation.resource_id.as_str().to_string(),
        serde_json::to_value(&observation).unwrap(),
    );
    let registry = serde_json::json!({
        "schema_version": 1,
        "updated_at": 1,
        "resources": serde_json::Value::Object(records),
    });
    let registry_path = resources::registry_path(root);
    fs::create_dir_all(registry_path.parent().unwrap()).unwrap();
    fs::write(&registry_path, serde_json::to_vec(&registry).unwrap()).unwrap();

    let service = SnapshotService::open(root).unwrap();
    let (snapshot, cursor) = service.snapshot_with_cursor().unwrap();
    assert_eq!(cursor, Cursor { epoch: 1, seq: 0 });
    assert!(snapshot.missions.contains_key(&mission.mission_id));
    assert!(snapshot.approvals.contains_key(&approval.approval_id));
    assert_eq!(snapshot.resources.len(), 1);
    assert_eq!(service.replay_after(cursor), ReplayAfter::Empty);
}

#[test]
fn bootstrap_fails_closed_on_corrupt_durable_input() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let missions = mission::missions_dir(root);
    fs::create_dir_all(&missions).unwrap();
    fs::write(
        missions.join("task-0000000000000009.json"),
        "{ this is not a mission",
    )
    .unwrap();
    assert!(SnapshotService::open(root).is_err());
}

#[test]
fn bootstrap_rejects_a_resource_document_without_the_required_map() {
    let dir = tempfile::tempdir().unwrap();
    let path = resources::registry_path(dir.path());
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(
        path,
        serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "updated_at": 1,
            "resources": null,
        }))
        .unwrap(),
    )
    .unwrap();

    assert!(SnapshotService::open(dir.path()).is_err());
    assert!(!state_path(dir.path()).exists());
}

#[test]
fn low_level_saves_record_through_the_authority() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();

    let mission = mission(7, 3);
    mission::save(root, &mission).unwrap();

    let approval = ApprovalRecord {
        approval_id: "apr-0000000000000007".to_string(),
        mission_id: mission.mission_id.clone(),
        generation: 1,
        action: "recover_execution".to_string(),
        requested_at: 3,
        ..ApprovalRecord::default()
    };
    policy::save_approval(root, &approval).unwrap();

    let mut registry = resources::ResourceRegistry::new(3);
    let resource_id = registry.observe_available(
        &ResourceIdentity::for_model("openai", "gpt"),
        "reachable",
        3,
    );
    resources::save(root, &registry).unwrap();

    let service = SnapshotService::open(root).unwrap();
    let snapshot = service.snapshot().unwrap();
    assert!(snapshot.missions.contains_key(&mission.mission_id));
    assert!(snapshot.approvals.contains_key(&approval.approval_id));
    assert!(snapshot.resources.contains_key(resource_id.as_str()));
}

#[test]
fn snapshot_and_cursor_are_atomic_under_a_race() {
    let dir = tempfile::tempdir().unwrap();
    let service = SnapshotService::open(dir.path()).unwrap();
    let total = 200u64;
    let done = Arc::new(AtomicBool::new(false));

    let writer = {
        let service = service.clone();
        let done = Arc::clone(&done);
        thread::spawn(move || {
            for index in 1..=total {
                appended(&service, index);
            }
            done.store(true, Ordering::SeqCst);
        })
    };

    let mut violations = Vec::new();
    while !done.load(Ordering::SeqCst) {
        let Ok((snapshot, cursor)) = service.snapshot_with_cursor() else {
            violations.push("snapshot read failed".to_string());
            break;
        };
        // Every append adds exactly one new mission, so a state/cursor pair
        // from different writes would violate count == head.
        if snapshot.missions.len() as u64 != cursor.seq {
            violations.push(format!(
                "snapshot/cursor torn: {} missions at seq {}",
                snapshot.missions.len(),
                cursor.seq
            ));
            break;
        }
    }
    writer.join().unwrap();

    let (snapshot, cursor) = service.snapshot_with_cursor().unwrap();
    assert_eq!(snapshot.missions.len() as u64, total);
    assert_eq!(
        cursor,
        Cursor {
            epoch: 1,
            seq: total
        }
    );
    assert!(violations.is_empty(), "{violations:?}");
}

#[test]
fn resource_replacement_is_one_atomic_transition() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let service = SnapshotService::open(root).unwrap();

    let mut registry = resources::ResourceRegistry::new(1);
    let openai = registry.observe_available(
        &ResourceIdentity::for_model("openai", "gpt"),
        "reachable",
        1,
    );
    let anthropic = registry.observe_available(
        &ResourceIdentity::for_model("anthropic", "claude"),
        "reachable",
        1,
    );
    resources::save(root, &registry).unwrap();

    // Two observations committed by one save are exactly one journal event.
    assert_eq!(service.head().unwrap().seq, 1);
    let snapshot = service.snapshot().unwrap();
    assert_eq!(snapshot.resources.len(), 2);
    assert!(snapshot.resources.contains_key(openai.as_str()));
    assert!(snapshot.resources.contains_key(anthropic.as_str()));

    // Replace the complete set with a subset: the removed observation is gone
    // in the same durable write, not left behind.
    let mut subset = resources::ResourceRegistry::new(2);
    subset.observe_available(
        &ResourceIdentity::for_model("anthropic", "claude"),
        "reachable",
        2,
    );
    resources::save(root, &subset).unwrap();
    let snapshot = service.snapshot().unwrap();
    assert_eq!(snapshot.resources.len(), 1);
    assert!(!snapshot.resources.contains_key(openai.as_str()));
    assert!(snapshot.resources.contains_key(anthropic.as_str()));

    // An emptied registry is representable; it is not a no-op that leaves the
    // previous observations in the authority.
    resources::save(root, &resources::ResourceRegistry::new(3)).unwrap();
    let snapshot = service.snapshot().unwrap();
    assert!(snapshot.resources.is_empty());
    let loaded = resources::load(root);
    assert!(loaded.exists && !loaded.corrupt && loaded.issues.is_empty());
    assert!(loaded.registry.is_empty());
}

#[test]
fn authoritative_approvals_stay_bounded_and_never_drop_pending() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let service = SnapshotService::open(root).unwrap();
    let total = MAX_APPROVALS + 6;

    for index in 0..total {
        let approval = ApprovalRecord {
            approval_id: format!("apr-{index:016}"),
            mission_id: "task-0000000000000001".to_string(),
            generation: 1,
            action: "recover_execution".to_string(),
            status: ApprovalStatus::Approved,
            requested_at: index as i64,
            resolved_at: Some(index as i64),
            ..ApprovalRecord::default()
        };
        service
            .append(DomainEvent::ApprovalUpsert { approval })
            .unwrap()
            .expect("a fresh approval is a state change");
    }

    // A pending record older than every resolved one is never pruned.
    let pending = ApprovalRecord {
        approval_id: "apr-pending-000000000000".to_string(),
        mission_id: "task-0000000000000001".to_string(),
        generation: 1,
        action: "recover_execution".to_string(),
        status: ApprovalStatus::Pending,
        requested_at: 0,
        ..ApprovalRecord::default()
    };
    service
        .append(DomainEvent::ApprovalUpsert {
            approval: pending.clone(),
        })
        .unwrap()
        .unwrap();

    let snapshot = service.snapshot().unwrap();
    assert!(
        snapshot.approvals.len() <= MAX_APPROVALS,
        "resolved approvals are pruned to the domain bound"
    );
    assert!(snapshot.approvals.contains_key(&pending.approval_id));
    assert!(snapshot
        .approvals
        .contains_key(&format!("apr-{:016}", total - 1)));
    assert!(!snapshot.approvals.contains_key("apr-0000000000000000"));

    // The public read agrees with the authority, including the bound.
    let listed = policy::list_approvals(root);
    assert!(listed.issues.is_empty());
    assert_eq!(listed.approvals.len(), snapshot.approvals.len());
}

#[test]
fn mission_upsert_cannot_roll_back_or_change_at_the_same_revision() {
    let dir = tempfile::tempdir().unwrap();
    let service = SnapshotService::open(dir.path()).unwrap();
    let base = mission(1, 10);
    // `Mission::admit` records the admission event, so the initial revision is
    // discovered rather than assumed.
    let base_revision = base.revision;
    service
        .append(DomainEvent::MissionUpsert {
            mission: base.clone(),
        })
        .unwrap()
        .unwrap();

    let mut revision_two = base.clone();
    revision_two.revision = base_revision + 1;
    revision_two.updated_at = 11;
    service
        .append(DomainEvent::MissionUpsert {
            mission: revision_two.clone(),
        })
        .unwrap()
        .unwrap();

    let mut revision_three = revision_two.clone();
    revision_three.revision = base_revision + 2;
    revision_three.updated_at = 12;
    service
        .append(DomainEvent::MissionUpsert {
            mission: revision_three.clone(),
        })
        .unwrap()
        .unwrap();
    let head = service.head().unwrap();

    // An exact no-op is accepted and records nothing.
    assert!(service
        .append(DomainEvent::MissionUpsert {
            mission: revision_three.clone(),
        })
        .unwrap()
        .is_none());

    // The same revision with different content is a rollback.
    let mut same_revision = revision_three.clone();
    same_revision.updated_at = 99;
    let error = service
        .append(DomainEvent::MissionUpsert {
            mission: same_revision,
        })
        .unwrap_err();
    assert!(error.to_string().contains("rolls back"), "{error}");

    // A lower revision is a rollback.
    assert!(service
        .append(DomainEvent::MissionUpsert {
            mission: revision_two,
        })
        .is_err());

    // A generation may only advance, even if the revision is higher.
    let mut generation_two = revision_three.clone();
    generation_two.generation = 2;
    generation_two.revision = base_revision + 3;
    service
        .append(DomainEvent::MissionUpsert {
            mission: generation_two.clone(),
        })
        .unwrap()
        .unwrap();
    let mut rolled_generation = generation_two.clone();
    rolled_generation.generation = 1;
    rolled_generation.revision = base_revision + 4;
    assert!(service
        .append(DomainEvent::MissionUpsert {
            mission: rolled_generation,
        })
        .is_err());

    // A strictly newer revision is accepted.
    let mut revision_six = generation_two.clone();
    revision_six.revision = base_revision + 5;
    revision_six.updated_at = 13;
    assert!(service
        .append(DomainEvent::MissionUpsert {
            mission: revision_six,
        })
        .unwrap()
        .is_some());

    // Rejected events never advanced the cursor; only the two accepted newer
    // generations/revisions did.
    assert_eq!(service.head().unwrap().seq, head.seq + 2);
}

#[test]
fn authority_backs_reads_after_projection_removal() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();

    let mission = mission(1, 1);
    mission::save(root, &mission).unwrap();
    let mission_file = mission::mission_path(root, &mission.mission_id).unwrap();
    fs::remove_file(&mission_file).unwrap();
    assert_eq!(
        mission::load(root, &mission.mission_id).unwrap().unwrap(),
        mission
    );
    let (summaries, corrupt) = mission::list(root);
    assert_eq!(corrupt, 0, "authority-only state is not corruption");
    assert!(summaries
        .iter()
        .any(|summary| summary.mission_id == mission.mission_id));
    assert!(!mission_file.exists(), "reads never rewrite the projection");

    let approval = ApprovalRecord {
        approval_id: "apr-0000000000000001".to_string(),
        mission_id: mission.mission_id.clone(),
        generation: 1,
        action: "recover_execution".to_string(),
        status: ApprovalStatus::Approved,
        requested_at: 1,
        resolved_at: Some(1),
        ..ApprovalRecord::default()
    };
    policy::save_approval(root, &approval).unwrap();
    fs::remove_file(policy::approval_path(root, &approval.approval_id).unwrap()).unwrap();
    let loaded = policy::load_approval(root, &approval.approval_id)
        .unwrap()
        .unwrap();
    assert_eq!(loaded.approval_id, approval.approval_id);
    assert_eq!(loaded.status, approval.status);

    let mut registry = resources::ResourceRegistry::new(1);
    let resource_id = registry.observe_available(
        &ResourceIdentity::for_model("openai", "gpt"),
        "reachable",
        1,
    );
    resources::save(root, &registry).unwrap();
    fs::remove_file(resources::registry_path(root)).unwrap();
    let loaded_registry = resources::load(root);
    assert!(loaded_registry.exists && !loaded_registry.corrupt);
    assert!(loaded_registry.registry.observation(&resource_id).is_some());
}

#[test]
fn unresolved_corrupt_mission_artifact_blocks_bootstrap() {
    let dir = tempfile::tempdir().unwrap();
    let missions = mission::missions_dir(dir.path());
    fs::create_dir_all(&missions).unwrap();
    fs::write(
        missions.join("task-0000000000000001.corrupt.json"),
        "{ quarantined bytes",
    )
    .unwrap();
    // Unresolved corruption must never be bootstrapped over.
    assert!(SnapshotService::open(dir.path()).is_err());
}

#[test]
fn marker_epoch_disagreement_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let service = SnapshotService::open(dir.path()).unwrap();
    appended(&service, 1);
    fs::write(
        dir.path().join(".ocg/orchestration/replay.initialized"),
        "epoch=2\n",
    )
    .unwrap();
    assert!(SnapshotService::open(dir.path()).is_err());
}

const CHILD_ROOT_ENV: &str = "OCG_REPLAY_TEST_ROOT";
const CHILD_OFFSET_ENV: &str = "OCG_REPLAY_TEST_OFFSET";
const CHILD_TEST: &str = "replay_child_process_worker";

/// Worker entry point: when the parent test sets the environment this appends a
/// disjoint block of Missions through the real cross-process lock. Without the
/// environment it is a no-op so an ordinary `cargo test` run stays fast.
#[test]
fn replay_child_process_worker() {
    let Ok(root) = std::env::var(CHILD_ROOT_ENV) else {
        return;
    };
    let offset: u64 = std::env::var(CHILD_OFFSET_ENV)
        .expect("worker offset")
        .parse()
        .expect("numeric worker offset");
    let service = SnapshotService::open(Path::new(&root)).unwrap();
    for step in 1..=8u64 {
        appended(&service, offset + step);
    }
}

#[test]
fn concurrent_processes_have_a_unique_strict_total_order() {
    let dir = tempfile::tempdir().unwrap();
    let root = PathBuf::from(dir.path());
    let exe = std::env::current_exe().unwrap();
    let mut children = Vec::new();
    for process in 0..4u64 {
        children.push(
            Command::new(&exe)
                .args(["--exact", CHILD_TEST, "--nocapture"])
                .env(CHILD_ROOT_ENV, &root)
                .env(CHILD_OFFSET_ENV, (process * 8).to_string())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn replay worker process"),
        );
    }
    for mut child in children {
        assert!(child.wait().unwrap().success(), "worker process failed");
    }

    // Four independent processes wrote through the same advisory lock. The
    // authority must show one gap-free total order regardless.
    let service = SnapshotService::open(&root).unwrap();
    let (snapshot, cursor) = service.snapshot_with_cursor().unwrap();
    assert_eq!(cursor.seq, 32);
    assert_eq!(snapshot.missions.len(), 32);
}
