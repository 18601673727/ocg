use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use rusqlite::{Connection, OpenFlags};
use serde_json::{json, Value};

use super::harness::{array, string, Result, SmokeHarness};
use super::provider::Reply;

fn smoke() -> Result<SmokeHarness> {
    SmokeHarness::start(Reply::NativePwd)
}

fn durable_projects(database: &Path, root: &Path) -> Result<Vec<Value>> {
    let connection = Connection::open_with_flags(database, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|error| format!("open durable database {}: {error}", database.display()))?;
    connection.busy_timeout(std::time::Duration::ZERO)?;
    let mut statement = connection.prepare(
        "SELECT id,root,created_at FROM domain_projects WHERE root=?1 ORDER BY created_at,id",
    )?;
    let root = root.canonicalize()?;
    let rows = statement.query_map([root.to_string_lossy().as_ref()], |row| Ok(json!({
        "id": row.get::<_, String>(0)?, "root": row.get::<_, String>(1)?, "created_at": row.get::<_, i64>(2)?
    })))?;
    Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
}

fn configuration(smoke: &mut SmokeHarness, project_id: &str) -> Result<Value> {
    let response = smoke.json(
        "GET",
        &format!("/api/v1/canonical/configuration?project_id={project_id}"),
        None,
    )?;
    Ok(response["configuration"].clone())
}

fn registry_entry(smoke: &mut SmokeHarness, id: &str) -> Result<Value> {
    smoke.json("GET", "/api/v1/canonical/projects", None)?["projects"]
        .as_array()
        .ok_or("registry is not an array")?
        .iter()
        .find(|project| project["project_id"] == id)
        .cloned()
        .ok_or_else(|| "Project missing from registry".into())
}

fn native_turn(smoke: &mut SmokeHarness, id: &str, session: &str) -> Result<(String, Value)> {
    let chat = smoke.send_chat(id, session, "report the working directory")?;
    assert_eq!(smoke.consume_chat(&chat)?, "mock done");
    let snapshot = smoke.assert_terminal_execution(&chat, "completed")?;
    for call in array(&snapshot["job"], "calls")? {
        let request: Value = serde_json::from_str(&string(call, "request")?)?;
        if request["kind"] != "native_tool" {
            continue;
        }
        let response: Value = serde_json::from_str(&string(call, "response")?)?;
        for output in [&response["output"]["output"], &response["output"]] {
            if let Some(stdout) = output.get("stdout").and_then(Value::as_str) {
                return Ok((stdout.trim().to_owned(), snapshot));
            }
        }
    }
    Err(format!("no native pwd result: {snapshot}").into())
}

#[test]
fn concurrent_configuration_scopes() -> Result<()> {
    let mut smoke = smoke()?;
    let a = smoke.add_project("pa")?;
    let b = smoke.add_project("pb")?;
    let a = string(&a, "project_id")?;
    let b = string(&b, "project_id")?;
    for round in 0..30 {
        let before_global = configuration(&mut smoke, &a)?["global_revision"]
            .as_u64()
            .ok_or("missing global revision")?;
        smoke.json("PUT", "/api/v1/canonical/configuration", Some(&json!({"command_id": format!("seed-g-{round}"), "expected_revision": before_global, "configuration": {"provider": "G0"}})))?;
        for (id, model) in [(&a, "A0"), (&b, "B0")] {
            let revision = configuration(&mut smoke, id)?["project_revision"]
                .as_u64()
                .ok_or("missing project revision")?;
            smoke.json("PUT", &format!("/api/v1/canonical/configuration/projects/{id}"), Some(&json!({"command_id": format!("seed-{id}-{round}"), "expected_revision": revision, "defaults": {"model": model}})))?;
        }
        let before = configuration(&mut smoke, &a)?["global_revision"]
            .as_u64()
            .ok_or("missing revision")?;
        let a_revision = configuration(&mut smoke, &a)?["project_revision"]
            .as_u64()
            .ok_or("missing Project A revision")?;
        let b_revision = configuration(&mut smoke, &b)?["project_revision"]
            .as_u64()
            .ok_or("missing Project B revision")?;
        let results = smoke.parallel_json(vec![
            (
                "PUT",
                "/api/v1/canonical/configuration".into(),
                json!({"command_id": format!("g-{round}"), "expected_revision": before, "configuration": {"provider": "G1"}}),
            ),
            (
                "PUT",
                format!("/api/v1/canonical/configuration/projects/{a}"),
                json!({"command_id": format!("a-{round}"), "expected_revision": a_revision, "defaults": {"model": "A1"}}),
            ),
            (
                "PUT",
                format!("/api/v1/canonical/configuration/projects/{b}"),
                json!({"command_id": format!("b-{round}"), "expected_revision": b_revision, "defaults": {"model": "B1"}}),
            ),
        ])?;
        let mut revisions = results
            .iter()
            .map(|result| {
                result["revision"]
                    .as_u64()
                    .ok_or("missing response revision")
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        revisions.sort_unstable();
        assert!(revisions
            .windows(2)
            .all(|window| window[1] == window[0] + 1));
        let global = results
            .iter()
            .find(|result| result["command_id"] == format!("g-{round}"))
            .ok_or("missing global configuration result")?;
        let project_a = results
            .iter()
            .find(|result| result["command_id"] == format!("a-{round}"))
            .ok_or("missing Project A configuration result")?;
        let project_b = results
            .iter()
            .find(|result| result["command_id"] == format!("b-{round}"))
            .ok_or("missing Project B configuration result")?;
        assert!(
            global["configuration"]["global_revision"]
                .as_u64()
                .ok_or("missing returned global revision")?
                > before
        );
        assert!(
            project_a["configuration"]["project_revision"]
                .as_u64()
                .ok_or("missing returned Project A revision")?
                > a_revision
        );
        assert!(
            project_b["configuration"]["project_revision"]
                .as_u64()
                .ok_or("missing returned Project B revision")?
                > b_revision
        );
        let stored = configuration(&mut smoke, &a)?;
        assert_eq!(stored["global"]["provider"], "G1", "{stored}");
        assert_eq!(
            stored["project_defaults"]["defaults"]["model"], "A1",
            "{stored}"
        );
        let stored_b = configuration(&mut smoke, &b)?;
        assert_eq!(
            stored_b["project_defaults"]["defaults"]["model"], "B1",
            "{stored_b}"
        );
        assert!(
            stored["global_revision"]
                .as_u64()
                .ok_or("missing persisted global revision")?
                > before,
            "{stored}"
        );
        smoke.assert_no_temp_files()?;
    }
    smoke.finish()
}

#[test]
fn stale_unrelated_project_isolation() -> Result<()> {
    let mut smoke = smoke()?;
    let a = smoke.add_project("pa")?;
    smoke.add_project("pb")?;
    let id = string(&a, "project_id")?;
    let job = smoke.admit_job(&smoke.path("pa"), "stale")?;
    let path = format!("/api/v1/canonical/jobs/{job}/configuration");
    smoke.json("GET", &path, None)?;
    fs::rename(smoke.path("pb"), smoke.path("pb-away"))?;
    smoke.json("GET", &path, None)?;
    let (status, frozen) = smoke.request(
        "PUT",
        &path,
        Some(&json!({"command_id": "stale-put", "configuration": {"probe": 1}})),
    )?;
    assert_eq!(status, 400, "{frozen}");
    assert!(
        frozen
            .to_string()
            .contains("Job configuration is frozen after execution admission"),
        "{frozen}"
    );
    let dashboard = format!("/api/v1/canonical/dashboard?project_id={id}");
    smoke.json("GET", &dashboard, None)?;
    let (status, unknown) = smoke.request(
        "GET",
        "/api/v1/canonical/jobs/job-that-does-not-exist/configuration",
        None,
    )?;
    assert_ne!(status, 200);
    assert!(
        unknown.to_string().contains("unknown canonical Job"),
        "{unknown}"
    );
    fs::rename(smoke.path("pb-away"), smoke.path("pb"))?;
    smoke.json("GET", &path, None)?;
    smoke.json("GET", &dashboard, None)?;
    smoke.finish()
}

#[test]
fn moved_project_registers_new_identity_and_preserves_old_history() -> Result<()> {
    let mut smoke = smoke()?;
    let choice = smoke.configure_provider()?;
    let project = smoke.add_project("pm-old")?;
    let id = string(&project, "project_id")?;
    let registry_before = registry_entry(&mut smoke, &id)?;
    let project_revision = configuration(&mut smoke, &id)?["project_revision"].clone();
    smoke.json(
        "PUT",
        &format!("/api/v1/canonical/configuration/projects/{id}"),
        Some(&json!({
            "command_id": "move-defaults",
            "expected_revision": project_revision,
            "defaults": {"provider": "mock", "model": choice}
        })),
    )?;
    let defaults_before = configuration(&mut smoke, &id)?["project_defaults"].clone();
    let old = smoke.path("pm-old").canonicalize()?;
    let durable_job = smoke.admit_job(&old, "move")?;
    let identity = durable_projects(&smoke.database_path(), &old)?;
    let (pwd, pre) = native_turn(&mut smoke, &id, "move-pre")?;
    assert_eq!(pwd, old.to_string_lossy());
    let live_workers = smoke.worker_threads()?;
    if let Some(workers) = live_workers {
        assert_eq!(workers.len(), 2, "{workers:?}");
    }
    let moved = smoke.path("pm-new");
    fs::rename(&old, &moved)?;
    let moved = moved.canonicalize()?;
    let registered = smoke.import_project(&moved)?;
    let moved_id = string(&registered, "project_id")?;
    assert_ne!(moved_id, id);
    let after = durable_projects(&smoke.database_path(), &moved)?;
    assert_eq!(after.len(), 1);
    assert_eq!(after[0]["id"], moved_id);
    assert_ne!(after[0]["id"], identity[0]["id"]);
    assert_eq!(after[0]["root"], json!(moved));
    let registry = registry_entry(&mut smoke, &moved_id)?;
    assert_eq!(registry["root"], json!(moved));
    assert_eq!(
        registry_entry(&mut smoke, &id)?["root"],
        registry_before["root"]
    );
    assert_eq!(
        configuration(&mut smoke, &id)?["project_defaults"],
        defaults_before
    );
    if let Some(workers) = smoke.worker_threads()? {
        assert!(workers.is_empty(), "{workers:?}");
    }
    for job in [&durable_job, &string(&pre["job"]["job"], "id")?] {
        let (status, unavailable) = smoke.request(
            "GET",
            &format!("/api/v1/canonical/jobs/{job}/configuration"),
            None,
        )?;
        assert_ne!(status, 200, "{unavailable}");
        assert!(
            unavailable
                .to_string()
                .contains("registered Project root is unavailable"),
            "{unavailable}"
        );
    }
    let conversations = smoke.json(
        "GET",
        &format!("/api/v1/canonical/chat/conversations?project_id={moved_id}"),
        None,
    )?;
    assert!(array(&conversations, "conversations")?.is_empty());
    assert_eq!(
        native_turn(&mut smoke, &moved_id, "move-post")?.0,
        moved.to_string_lossy()
    );
    if let Some(workers) = smoke.worker_threads()? {
        assert_eq!(workers.len(), 2, "{workers:?}");
    }
    smoke.finish()
}

#[test]
fn claimed_project_identity_cannot_replace_boundary_identity() -> Result<()> {
    let mut smoke = smoke()?;
    let a = smoke.add_project("pa")?;
    let c = smoke.add_project("pc")?;
    let a_id = string(&a, "project_id")?;
    let c_id = string(&c, "project_id")?;
    let project_revision = configuration(&mut smoke, &a_id)?["project_revision"].clone();
    smoke.json(
        "PUT",
        &format!("/api/v1/canonical/configuration/projects/{a_id}"),
        Some(&json!({
            "command_id": "a-defaults",
            "expected_revision": project_revision,
            "defaults": {"model": "A0"}
        })),
    )?;
    let before = configuration(&mut smoke, &a_id)?["project_defaults"].clone();
    let imported = smoke.json(
        "POST",
        "/api/v1/canonical/projects/import",
        Some(&json!({
            "command_id": "claim-a-from-c", "root": smoke.path("pc"), "project_id": a_id
        })),
    )?;
    assert_eq!(imported["project"]["project_id"], c_id);
    assert_eq!(registry_entry(&mut smoke, &a_id)?["root"], a["root"]);
    assert_eq!(
        durable_projects(&smoke.database_path(), &smoke.path("pa"))?
            .iter()
            .map(|row| row["id"].clone())
            .collect::<Vec<_>>(),
        vec![json!(a_id)]
    );
    assert_eq!(
        durable_projects(&smoke.database_path(), &smoke.path("pc"))?
            .iter()
            .map(|row| row["id"].clone())
            .collect::<Vec<_>>(),
        vec![json!(c_id)]
    );
    assert_eq!(
        configuration(&mut smoke, &a_id)?["project_defaults"],
        before
    );
    smoke.finish()
}

#[cfg(unix)]
#[test]
fn symlinked_project_root_converges_on_canonical_identity() -> Result<()> {
    let mut smoke = smoke()?;
    let project = smoke.add_project("pa")?;
    let id = string(&project, "project_id")?;
    let link = smoke.path("pa-link");
    std::os::unix::fs::symlink(smoke.path("pa"), &link)?;
    assert_eq!(smoke.import_project(&link)?["project_id"], id);
    let registry = smoke.json("GET", "/api/v1/canonical/projects", None)?["projects"].clone();
    let entries = registry.as_array().ok_or("registry is not an array")?;
    assert_eq!(
        entries
            .iter()
            .filter(|entry| entry["project_id"] == id)
            .count(),
        1
    );
    assert!(!entries.iter().any(|entry| entry["root"] == json!(link)));
    assert_eq!(
        registry_entry(&mut smoke, &id)?["root"],
        json!(smoke.path("pa").canonicalize()?)
    );
    assert_eq!(
        durable_projects(&smoke.database_path(), &smoke.path("pa"))?
            .iter()
            .map(|row| row["id"].clone())
            .collect::<Vec<_>>(),
        vec![json!(id)]
    );
    smoke.finish()
}

#[test]
fn corrupt_candidate_is_reported_and_recovers() -> Result<()> {
    let mut smoke = smoke()?;
    smoke.add_project("pa")?;
    smoke.add_project("pc")?;
    let job = smoke.admit_job(&smoke.path("pa"), "corrupt")?;
    let path = format!("/api/v1/canonical/jobs/{job}/configuration");
    smoke.json("GET", &path, None)?;
    let database = smoke.database_path();
    let backup = fs::read(&database)?;
    {
        let mut file = fs::OpenOptions::new().write(true).open(&database)?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(b"OCG-CORRUPT-NOT-A-SQLITE-DATABASE")?;
        file.sync_all()?;
    }
    let (status, corrupt) = smoke.request("GET", &path, None)?;
    assert_ne!(status, 200, "{corrupt}");
    assert!(
        !corrupt.to_string().contains("unknown canonical Job"),
        "{corrupt}"
    );
    fs::write(database, backup)?;
    smoke.json("GET", &path, None)?;
    smoke.finish()
}

#[test]
fn concurrent_project_registration_keeps_one_identity() -> Result<()> {
    let mut smoke = smoke()?;
    let project = smoke.add_project("pb")?;
    let id = string(&project, "project_id")?;
    let root = smoke.path("pb").canonicalize()?;
    let results = smoke.parallel_json(
        (0..8)
            .map(|index| {
                (
                    "POST",
                    "/api/v1/canonical/projects/import".into(),
                    json!({"command_id": format!("concurrent-{index}"), "root": root}),
                )
            })
            .collect(),
    )?;
    assert!(results
        .iter()
        .all(|result| result["project"]["project_id"] == id));
    assert_eq!(
        smoke.json("GET", "/api/v1/canonical/projects", None)?["projects"]
            .as_array()
            .ok_or("registry is not an array")?
            .iter()
            .filter(|entry| entry["root"] == json!(root))
            .count(),
        1
    );
    assert_eq!(
        durable_projects(&smoke.database_path(), &root)?
            .iter()
            .map(|row| row["id"].clone())
            .collect::<Vec<_>>(),
        vec![json!(id)]
    );
    smoke.assert_no_temp_files()?;
    smoke.finish()
}
