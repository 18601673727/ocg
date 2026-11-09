//! `ocg reconcile-reserved <data-directory> <job> --confirm-no-start`
//!
//! Ends one Reserved Allocation that survived a crash, and the Job it was
//! reserved for, when nothing shows the Job ever started. It is not recovery:
//! it adopts nothing, retries nothing and contacts no Provider.
//!
//! Why this is safe. A Provider request, a tool run or any other effect needs
//! a `DevelopmentHandoff`, whose only constructor runs in the same Worker
//! command as the checked start and is answered only after that start is
//! durable. A Reserved Allocation with no committed start therefore never had
//! execution authority. The database is never opened for writing in place:
//!
//! 1. The exclusive host lock is taken without blocking and held to the end,
//!    so no OCG host runs and none can start.
//! 2. The eligibility checks run on a private, unlinked copy; a refusal
//!    touches nothing in the data directory.
//! 3. A work copy in the same directory receives the one transaction the
//!    runtime itself uses (`fail_unstarted_reservation`), then is verified in
//!    full.
//! 4. A rollback artifact, a byte-identical copy of the original, is written
//!    and verified by hash, with an evidence file naming both hashes and the
//!    original's identity; both are synced, and the directory is, before the
//!    rename.
//! 5. The work copy replaces `ocg.redb` with one `rename`, then the
//!    directory is synced.
//!
//! Crash windows. Before the rename `ocg.redb` is never touched; leftover
//! `.ocg-reconcile-*` files are inert and may be deleted. The rename is the
//! only step that changes what `ocg.redb` names and it is atomic, so there is
//! no moment without a database: after a crash at or around it, the file is
//! either the complete original (run again) or the complete verified result.
//! Both exist as whole, synced files before the rename, so neither can be
//! partial. Rolling back means renaming the artifact over `ocg.redb`.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use aws_lc_rs::digest::{Context, SHA256};
use serde_json::{Value, json};

use crate::db::DATABASE_FILE_NAME;
use crate::db::entities::JobId;
use crate::system::data_directory::{DATABASE_LOCK_FILE, private_file};

const USAGE: &str = "usage: ocg reconcile-reserved <data-directory> <job-id> --confirm-no-start";
const STALE_PREFIX: &str = ".ocg-reconcile-";

type Failure = Box<dyn std::error::Error>;

/// A stopped point for crash qualification. Compiled out of release builds.
#[cfg(debug_assertions)]
fn crash_point(stage: &str) {
    if std::env::var("OCG_RECONCILE_CRASH_AT").is_ok_and(|value| value == stage) {
        std::process::exit(86);
    }
}

#[cfg(not(debug_assertions))]
fn crash_point(_stage: &str) {}

/// What names a file: enough to notice it changed or was replaced.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Identity {
    device: u64,
    inode: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    length: u64,
    modified_ns: i128,
}

fn identity(metadata: &std::fs::Metadata) -> Identity {
    Identity {
        device: metadata.dev(),
        inode: metadata.ino(),
        mode: metadata.mode() & 0o7777,
        uid: metadata.uid(),
        gid: metadata.gid(),
        length: metadata.len(),
        modified_ns: i128::from(metadata.mtime()) * 1_000_000_000
            + i128::from(metadata.mtime_nsec()),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// SHA-256 and length of the whole file, read from the start.
fn digest(file: &mut File) -> std::io::Result<(String, u64)> {
    file.seek(SeekFrom::Start(0))?;
    let mut context = Context::new(&SHA256);
    let mut buffer = vec![0u8; 1 << 20];
    let mut length = 0u64;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        context.update(&buffer[..read]);
        length += read as u64;
    }
    Ok((hex(context.finish().as_ref()), length))
}

/// Copies `source` to `destination` from the start and returns what was
/// copied, hashed on the way.
fn copy_with_digest(source: &mut File, destination: &mut File) -> std::io::Result<(String, u64)> {
    source.seek(SeekFrom::Start(0))?;
    let mut context = Context::new(&SHA256);
    let mut buffer = vec![0u8; 1 << 20];
    let mut length = 0u64;
    loop {
        let read = source.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        context.update(&buffer[..read]);
        destination.write_all(&buffer[..read])?;
        length += read as u64;
    }
    Ok((hex(context.finish().as_ref()), length))
}

fn sync_directory(directory: &Path) -> std::io::Result<()> {
    File::open(directory)?.sync_all()
}

/// Removes a file it names unless it was published; a crash leaves it behind.
struct Scratch(Option<PathBuf>);

impl Scratch {
    fn keep(&mut self) {
        self.0 = None;
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

struct Refused {
    code: String,
    detail: String,
}

impl From<crate::db::ReservedRefusal> for Refused {
    fn from(refusal: crate::db::ReservedRefusal) -> Self {
        Self {
            code: refusal.code.to_owned(),
            detail: refusal.detail,
        }
    }
}

fn refused(code: &str, detail: impl Into<String>) -> Refused {
    Refused {
        code: code.to_owned(),
        detail: detail.into(),
    }
}

fn io(context: &str, error: std::io::Error) -> Refused {
    refused("io", format!("{context}: {error}"))
}

pub fn run(arguments: Vec<String>) -> Result<(), Failure> {
    let [directory, job, flag] = arguments.as_slice() else {
        return Err(USAGE.into());
    };
    if flag != "--confirm-no-start" {
        return Err(USAGE.into());
    }
    let job = uuid::Uuid::parse_str(job)
        .map(|id| JobId::new(id.as_u128()))
        .map_err(|_| "the Job must be a UUID")?;
    match reconcile(Path::new(directory), job) {
        Ok(report) => {
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(())
        }
        Err(refusal) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "outcome": "refused",
                    "code": refusal.code,
                    "detail": refusal.detail,
                }))?
            );
            Err(format!("refused: {}", refusal.code).into())
        }
    }
}

fn open_original(path: &Path) -> Result<File, Refused> {
    let mut options = OpenOptions::new();
    options.read(true).custom_flags(libc::O_NOFOLLOW);
    let file = options
        .open(path)
        .map_err(|error| io("open the database", error))?;
    let metadata = file
        .metadata()
        .map_err(|error| io("stat the database", error))?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(refused(
            "not_a_plain_database",
            "the database must be one regular file without other hard links",
        ));
    }
    Ok(file)
}

fn reconcile(directory: &Path, job: JobId) -> Result<Value, Refused> {
    let directory = std::fs::canonicalize(directory)
        .map_err(|error| io("resolve the data directory", error))?;
    let database_path = directory.join(DATABASE_FILE_NAME);

    // 1. The exclusive host lock, without blocking, held to the end. The lock
    // file is opened, never created: a directory no host ever used is refused.
    let mut lock_options = OpenOptions::new();
    lock_options
        .read(true)
        .write(true)
        .custom_flags(libc::O_NOFOLLOW);
    let lock = lock_options
        .open(directory.join(DATABASE_LOCK_FILE))
        .map_err(|error| io("open the host lock file", error))?;
    if lock.try_lock().is_err() {
        return Err(refused(
            "host_running",
            "an OCG host (or another reconciliation) holds the database lock",
        ));
    }

    let mut original = open_original(&database_path)?;
    let before = identity(&original.metadata().map_err(|error| io("stat", error))?);
    let (source_sha256, source_length) =
        digest(&mut original).map_err(|error| io("read the database", error))?;
    if source_length != before.length || source_length == 0 {
        return Err(refused(
            "unstable_source",
            "the database changed while it was read",
        ));
    }

    // 2. The eligibility checks, on a private unlinked copy. A refusal here
    // has written nothing in the data directory.
    let scratch_name = std::env::temp_dir().join(format!("{STALE_PREFIX}{}", uuid::Uuid::now_v7()));
    let mut create = OpenOptions::new();
    create.read(true).write(true).create_new(true);
    let mut private = private_file(&mut create)
        .open(&scratch_name)
        .map_err(|error| io("create the private copy", error))?;
    std::fs::remove_file(&scratch_name).map_err(|error| io("unlink the private copy", error))?;
    let (copied_sha256, _) = copy_with_digest(&mut original, &mut private)
        .map_err(|error| io("copy the database", error))?;
    if copied_sha256 != source_sha256 {
        return Err(refused(
            "unstable_source",
            "the database changed while it was copied",
        ));
    }
    private
        .sync_all()
        .map_err(|error| io("sync the private copy", error))?;
    crate::db::reconcile_reserved_preflight(private, job)?;

    // 3. The work copy, in the directory so the final rename stays on one
    // filesystem, receives the transaction and the full verification.
    let token = uuid::Uuid::now_v7();
    let work_path = directory.join(format!("{STALE_PREFIX}{token}.work"));
    let mut work_guard = Scratch(Some(work_path.clone()));
    let mut work_create = OpenOptions::new();
    work_create.read(true).write(true).create_new(true);
    let mut work = private_file(&mut work_create)
        .open(&work_path)
        .map_err(|error| io("create the work copy", error))?;
    let (work_sha256, _) = copy_with_digest(&mut original, &mut work)
        .map_err(|error| io("copy the database", error))?;
    if work_sha256 != source_sha256 {
        return Err(refused(
            "unstable_source",
            "the database changed while it was copied",
        ));
    }
    work.sync_all()
        .map_err(|error| io("sync the work copy", error))?;
    crash_point("after-work-copy");
    let applied = crate::db::reconcile_reserved_apply(work, job)?;
    crash_point("after-apply");

    // Ownership and permissions follow the original; a mismatch that cannot
    // be repaired refuses before anything is published.
    let mut work = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&work_path)
        .map_err(|error| io("reopen the work copy", error))?;
    work.set_permissions(std::fs::Permissions::from_mode(before.mode))
        .map_err(|error| io("set the work copy mode", error))?;
    let current = work.metadata().map_err(|error| io("stat", error))?;
    if (current.uid(), current.gid()) != (before.uid, before.gid) {
        std::os::unix::fs::fchown(&work, Some(before.uid), Some(before.gid))
            .map_err(|error| io("give the work copy the original's owner", error))?;
    }
    work.sync_all()
        .map_err(|error| io("sync the work copy", error))?;
    let result_identity = identity(&work.metadata().map_err(|error| io("stat", error))?);
    if (
        result_identity.mode,
        result_identity.uid,
        result_identity.gid,
    ) != (before.mode, before.uid, before.gid)
        || work.metadata().map_err(|error| io("stat", error))?.nlink() != 1
    {
        return Err(refused(
            "identity_mismatch",
            "the work copy could not take the original's owner and permissions",
        ));
    }
    let (result_sha256, _) = digest(&mut work).map_err(|error| io("hash the work copy", error))?;
    drop(work);

    // 4. The rollback artifact: a byte-identical copy of the original,
    // verified by reading it back.
    let artifact_path = directory.join(format!("{DATABASE_FILE_NAME}.pre-reconcile-{token}"));
    let mut artifact_guard = Scratch(Some(artifact_path.clone()));
    let mut artifact_create = OpenOptions::new();
    artifact_create.read(true).write(true).create_new(true);
    let mut artifact = private_file(&mut artifact_create)
        .open(&artifact_path)
        .map_err(|error| io("create the rollback artifact", error))?;
    let (artifact_written, _) = copy_with_digest(&mut original, &mut artifact)
        .map_err(|error| io("write the rollback artifact", error))?;
    artifact
        .set_permissions(std::fs::Permissions::from_mode(before.mode))
        .map_err(|error| io("set the artifact mode", error))?;
    artifact
        .sync_all()
        .map_err(|error| io("sync the artifact", error))?;
    let (artifact_sha256, _) =
        digest(&mut artifact).map_err(|error| io("verify the artifact", error))?;
    if artifact_written != source_sha256 || artifact_sha256 != source_sha256 {
        return Err(refused(
            "artifact_unverified",
            "the rollback artifact does not equal the original",
        ));
    }
    drop(artifact);

    // The evidence beside the artifact: what was replaced and what replaces
    // it, so a rollback never depends on this command's standard output.
    let manifest_path = directory.join(format!(
        "{DATABASE_FILE_NAME}.pre-reconcile-{token}.evidence.json"
    ));
    let mut manifest_guard = Scratch(Some(manifest_path.clone()));
    let mut manifest_create = OpenOptions::new();
    manifest_create.write(true).create_new(true);
    let mut manifest = private_file(&mut manifest_create)
        .open(&manifest_path)
        .map_err(|error| io("create the evidence file", error))?;
    let evidence = json!({
        "kind": "ocg-reconcile-reserved-evidence",
        "job": job.to_string(),
        "original": {
            "sha256": source_sha256,
            "bytes": source_length,
            "device": before.device,
            "inode": before.inode,
            "mode": format!("{:o}", before.mode),
            "uid": before.uid,
            "gid": before.gid,
            "modified_unix_ns": before.modified_ns.to_string(),
        },
        "rollback_artifact": {
            "file": artifact_path.file_name().map(|name| name.to_string_lossy().into_owned()),
            "sha256": artifact_sha256,
            "verified_identical": true,
        },
        "replacement": {"sha256": result_sha256},
        "rollback": "with no OCG host running, rename the rollback artifact over ocg.redb and sync the directory",
    });
    manifest
        .write_all(
            serde_json::to_string_pretty(&evidence)
                .map_err(|error| refused("io", format!("encode the evidence file: {error}")))?
                .as_bytes(),
        )
        .map_err(|error| io("write the evidence file", error))?;
    manifest
        .sync_all()
        .map_err(|error| io("sync the evidence file", error))?;
    drop(manifest);
    sync_directory(&directory).map_err(|error| io("sync the directory", error))?;
    crash_point("after-artifact");

    // The original must be exactly what was checked and copied.
    let mut recheck = open_original(&database_path)?;
    let named = identity(&recheck.metadata().map_err(|error| io("stat", error))?);
    let (recheck_sha256, _) =
        digest(&mut recheck).map_err(|error| io("reread the database", error))?;
    if named != before || recheck_sha256 != source_sha256 {
        return Err(refused(
            "source_changed",
            "the database changed after it was checked; nothing was published",
        ));
    }
    drop(recheck);
    drop(original);

    // 5. Publication: one atomic rename, then the directory.
    crash_point("before-rename");
    std::fs::rename(&work_path, &database_path)
        .map_err(|error| io("publish the reconciled database", error))?;
    work_guard.keep();
    artifact_guard.keep();
    manifest_guard.keep();
    crash_point("after-rename");
    sync_directory(&directory).map_err(|error| {
        refused(
            "published_unsynced",
            format!("the database was published but the directory sync failed: {error}"),
        )
    })?;
    crash_point("after-directory-sync");

    // 6. An independent read of what was published, after the lock is
    // released so the offline checker may take its shared lock.
    drop(lock);
    let mut published = open_original(&database_path)?;
    let (published_sha256, _) =
        digest(&mut published).map_err(|error| io("hash the published database", error))?;
    let published_identity = identity(&published.metadata().map_err(|error| io("stat", error))?);
    let inspection = crate::db::verify_offline(&directory, true)
        .map_err(|error| refused("post_publication_verification", error.to_string()))?;
    let passed = inspection["logical_checks"]["passed"] == json!(true)
        && inspection["page_checksums"] == json!("every page checksum verified");
    if published_sha256 != result_sha256 || !passed {
        return Err(refused(
            "post_publication_verification",
            format!(
                "the published database does not match the verified result or failed its check; restore the rollback artifact {}",
                artifact_path.display()
            ),
        ));
    }

    Ok(json!({
        "outcome": "reconciled",
        "job": job.to_string(),
        "no_provider_contacted": true,
        "applied": applied,
        "original": {"sha256": source_sha256, "bytes": source_length},
        "rollback_artifact": {"path": artifact_path.display().to_string(), "sha256": artifact_sha256, "verified_identical": true},
        "evidence_file": manifest_path.display().to_string(),
        "published": {
            "sha256": published_sha256,
            "bytes": published_identity.length,
            "mode": format!("{:o}", published_identity.mode),
            "uid": published_identity.uid,
            "gid": published_identity.gid,
            "inode_changed": published_identity.inode != before.inode,
        },
        "post_publication_check": inspection,
    }))
}
