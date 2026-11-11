//! Offline reconciliation publishes only a verified copy, while retaining the
//! exclusive host lock through verification and durable audit finalization.
//! No Provider is contacted and no recovery execution authority is enabled.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use aws_lc_rs::digest::{Context, SHA256};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::db::entities::JobId;
use crate::db::{DATABASE_FILE_NAME, storage_format_versions};
use crate::system::data_directory::{DATABASE_LOCK_FILE, private_file};

const USAGE: &str = "usage: ocg reconcile-reserved <data-directory> <job-id> --confirm-no-start | ocg reconcile-reserved <data-directory> --finalize";
const HELD_USAGE: &str =
    "usage: ocg reconcile-held <data-directory> --dry-run | --apply <plan-digest> | --finalize";
const STALE_PREFIX: &str = ".ocg-reconcile-";
const PREPARED_SCHEMA: &str = "ocg-publication-prepared-v1";
const COMPLETION_SCHEMA: &str = "ocg-publication-finalized-v1";
const PREPARED_NOT_PUBLISHED: &str = "PREPARED_NOT_PUBLISHED";
const PUBLISHED_UNFINALIZED: &str = "PUBLISHED_UNFINALIZED";
const PUBLISHED_FINALIZED: &str = "PUBLISHED_FINALIZED";
const INCONSISTENT: &str = "INCONSISTENT";
const MAX_AUDIT_BYTES: u64 = 64 * 1024 * 1024;

type Failure = Box<dyn std::error::Error>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecoveryKind {
    Reserved,
    Held,
}

impl RecoveryKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Reserved => "reserved",
            Self::Held => "held",
        }
    }
}

enum Operation {
    Reserved(JobId),
    Held { expected_digest: Option<String> },
}

impl Operation {
    fn kind(&self) -> RecoveryKind {
        match self {
            Self::Reserved(_) => RecoveryKind::Reserved,
            Self::Held { .. } => RecoveryKind::Held,
        }
    }
}

/// Existing debug-only crash qualification mechanism. Release builds contain
/// no fault-injection authority or alternate publication behavior.
#[cfg(debug_assertions)]
fn crash_point(stage: &str) {
    if std::env::var("OCG_RECONCILE_CRASH_AT").is_ok_and(|value| value == stage) {
        std::process::exit(86);
    }
}

#[cfg(not(debug_assertions))]
fn crash_point(_stage: &str) {}

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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileFact {
    sha256: String,
    bytes: u64,
    device: u64,
    inode: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    modified_unix_ns: String,
}

impl FileFact {
    fn new(identity: &Identity, sha256: String) -> Self {
        Self {
            sha256,
            bytes: identity.length,
            device: identity.device,
            inode: identity.inode,
            mode: identity.mode,
            uid: identity.uid,
            gid: identity.gid,
            modified_unix_ns: identity.modified_ns.to_string(),
        }
    }

    fn matches_identity(&self, actual: &Identity) -> bool {
        self.bytes == actual.length
            && self.device == actual.device
            && self.inode == actual.inode
            && self.mode == actual.mode
            && self.uid == actual.uid
            && self.gid == actual.gid
            && self.modified_unix_ns == actual.modified_ns.to_string()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactFact {
    file: String,
    sha256: String,
    bytes: u64,
    device: u64,
    inode: u64,
    mode: u32,
    uid: u32,
    gid: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvidenceRef {
    file: String,
    sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreparedEvidence {
    schema: String,
    sequence: u64,
    operation: String,
    recovery_plan: Value,
    plan_digest: Option<String>,
    targets: Vec<Value>,
    source: FileFact,
    replacement: FileFact,
    rollback_artifact: ArtifactFact,
    expected_logical_row_delta: Value,
    previous_finalized: Option<EvidenceRef>,
    source_relation: String,
    supersedes_prepared: Option<EvidenceRef>,
    database_format: u64,
    record_version: u8,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CompletionEvidence {
    schema: String,
    sequence: u64,
    prepared: EvidenceRef,
    published: FileFact,
    verification: Value,
    completed_at_unix_ns: String,
    reconstructed_after_crash: bool,
}

#[derive(Clone, Debug)]
struct PreparedRecord {
    path: PathBuf,
    reference: EvidenceRef,
    evidence: PreparedEvidence,
}

#[derive(Clone, Debug)]
struct CompletionRecord {
    path: PathBuf,
    reference: EvidenceRef,
    evidence: CompletionEvidence,
}

#[derive(Default)]
struct AuditChain {
    prepared: Vec<PreparedRecord>,
    completed: Vec<CompletionRecord>,
    active: Option<usize>,
    latest_finalized: Option<usize>,
    next_sequence: u64,
}

impl AuditChain {
    fn latest_reference(&self) -> Option<EvidenceRef> {
        self.latest_finalized
            .map(|index| self.completed[index].reference.clone())
    }
}

struct Refused {
    code: String,
    detail: String,
    state: Option<&'static str>,
}

impl From<crate::db::ReservedRefusal> for Refused {
    fn from(refusal: crate::db::ReservedRefusal) -> Self {
        Self {
            code: refusal.code.to_owned(),
            detail: refusal.detail,
            state: None,
        }
    }
}

fn refused(code: &str, detail: impl Into<String>) -> Refused {
    Refused {
        code: code.to_owned(),
        detail: detail.into(),
        state: None,
    }
}

fn refused_in_state(code: &str, state: &'static str, detail: impl Into<String>) -> Refused {
    Refused {
        code: code.to_owned(),
        detail: detail.into(),
        state: Some(state),
    }
}

fn io(context: &str, error: std::io::Error) -> Refused {
    refused("io", format!("{context}: {error}"))
}

fn publication_io(context: &str, error: std::io::Error) -> Refused {
    refused_in_state(
        "publication_finalization",
        PUBLISHED_UNFINALIZED,
        format!(
            "{context}: {error}; publication is not reported finalized, no automatic rollback is safe; use --finalize after checking the evidence"
        ),
    )
}

fn unfinalized(error: Refused) -> Refused {
    Refused {
        state: Some(PUBLISHED_UNFINALIZED),
        ..error
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(DIGITS[(byte >> 4) as usize] as char);
        encoded.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn digest_bytes(bytes: &[u8]) -> String {
    let mut context = Context::new(&SHA256);
    context.update(bytes);
    hex(context.finish().as_ref())
}

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

fn open_plain(path: &Path, write: bool) -> Result<(File, Identity), Refused> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(write)
        .custom_flags(libc::O_NOFOLLOW);
    let file = options
        .open(path)
        .map_err(|error| io("open a publication file", error))?;
    let metadata = file
        .metadata()
        .map_err(|error| io("stat a publication file", error))?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(refused(
            "not_a_plain_file",
            format!(
                "{} must be one regular file without other hard links",
                path.display()
            ),
        ));
    }
    let actual = identity(&metadata);
    Ok((file, actual))
}

fn open_original(path: &Path) -> Result<File, Refused> {
    open_plain(path, false).map(|(file, _)| file)
}

fn establish_security(file: &File, uid: u32, gid: u32, mode: u32) -> Result<Identity, Refused> {
    let metadata = file
        .metadata()
        .map_err(|error| io("stat before ownership revalidation", error))?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(refused(
            "identity_mismatch",
            "publication artifacts must remain unlinked regular files with exactly one name",
        ));
    }
    if (metadata.uid(), metadata.gid()) != (uid, gid) {
        std::os::unix::fs::fchown(file, Some(uid), Some(gid))
            .map_err(|error| io("establish the source database owner", error))?;
    }
    file.set_permissions(std::fs::Permissions::from_mode(mode))
        .map_err(|error| io("establish publication permissions", error))?;
    file.sync_all()
        .map_err(|error| io("sync ownership and permission changes", error))?;
    let actual = identity(
        &file
            .metadata()
            .map_err(|error| io("revalidate ownership and permissions", error))?,
    );
    if (actual.uid, actual.gid, actual.mode) != (uid, gid, mode) {
        return Err(refused(
            "identity_mismatch",
            "the required owner or permission mode could not be established",
        ));
    }
    Ok(actual)
}

fn revalidate_named(path: &Path, expected: &Identity) -> Result<File, Refused> {
    let (file, actual) = open_plain(path, false)?;
    if &actual != expected {
        return Err(refused(
            "identity_mismatch",
            format!("{} changed identity or metadata", path.display()),
        ));
    }
    Ok(file)
}

fn read_record<T: for<'de> Deserialize<'de>>(
    path: &Path,
) -> Result<(T, EvidenceRef, Identity), Refused> {
    let (file, actual) = open_plain(path, false)?;
    if actual.mode != 0o600 {
        return Err(refused(
            "audit_permissions",
            format!("{} is not mode 0600", path.display()),
        ));
    }
    if actual.length > MAX_AUDIT_BYTES {
        return Err(refused(
            "audit_evidence",
            format!("{} is too large to be an audit record", path.display()),
        ));
    }
    let mut bytes = Vec::with_capacity(actual.length as usize);
    file.take(MAX_AUDIT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| io("read audit evidence", error))?;
    if bytes.len() as u64 != actual.length {
        return Err(refused(
            "audit_evidence",
            format!("{} changed while it was read", path.display()),
        ));
    }
    let digest = digest_bytes(&bytes);
    let record = serde_json::from_slice(&bytes)
        .map_err(|error| refused("audit_evidence", format!("{}: {error}", path.display())))?;
    let file = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .ok_or_else(|| refused("audit_evidence", "audit record has no file name"))?;
    Ok((
        record,
        EvidenceRef {
            file,
            sha256: digest,
        },
        actual,
    ))
}

fn read_audit_record<T: for<'de> Deserialize<'de>>(
    path: &Path,
) -> Result<(T, EvidenceRef, Identity), Refused> {
    read_record(path).map_err(|error| Refused {
        state: Some(INCONSISTENT),
        ..error
    })
}

fn load_audit_chain(directory: &Path) -> Result<AuditChain, Refused> {
    let mut chain = AuditChain {
        next_sequence: 1,
        ..AuditChain::default()
    };
    let mut sequences = BTreeSet::new();
    for entry in std::fs::read_dir(directory).map_err(|error| io("scan audit evidence", error))? {
        let entry = entry.map_err(|error| io("scan audit evidence", error))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with(&format!("{DATABASE_FILE_NAME}.pre-reconcile-"))
            && name.ends_with(".evidence.json")
        {
            return Err(refused_in_state(
                "legacy_unfinalized_evidence",
                INCONSISTENT,
                format!("{name} is an R1 prepared manifest without durable completion evidence"),
            ));
        }
        let prepared = name.starts_with(STALE_PREFIX) && name.ends_with(".prepared.json");
        let completed = name.starts_with(STALE_PREFIX) && name.ends_with(".finalized.json");
        if !prepared && !completed {
            continue;
        }
        let path = directory.join(&name);
        if prepared {
            let (evidence, reference, actual): (PreparedEvidence, _, _) = read_audit_record(&path)?;
            validate_prepared(&evidence, &actual)?;
            if !sequences.insert(evidence.sequence) {
                return Err(refused_in_state(
                    "audit_chain_ambiguous",
                    INCONSISTENT,
                    format!("duplicate prepared evidence sequence {}", evidence.sequence),
                ));
            }
            chain.next_sequence =
                chain
                    .next_sequence
                    .max(evidence.sequence.checked_add(1).ok_or_else(|| {
                        refused(
                            "audit_chain",
                            "evidence sequence exhausted its representation",
                        )
                    })?);
            chain.prepared.push(PreparedRecord {
                path,
                reference,
                evidence,
            });
        } else {
            let (evidence, reference, _): (CompletionEvidence, _, _) = read_audit_record(&path)?;
            if evidence.schema != COMPLETION_SCHEMA
                || evidence.sequence == 0
                || !valid_digest(&evidence.prepared.sha256)
                || evidence.verification["passed"] != json!(true)
            {
                return Err(refused_in_state(
                    "audit_evidence",
                    INCONSISTENT,
                    format!("{name} is not a valid completion record"),
                ));
            }
            chain.completed.push(CompletionRecord {
                path,
                reference,
                evidence,
            });
        }
    }
    chain
        .prepared
        .sort_by_key(|record| record.evidence.sequence);
    chain
        .completed
        .sort_by_key(|record| record.evidence.sequence);

    let mut completion_by_prepared = BTreeMap::new();
    let mut finalized_sequences = BTreeSet::new();
    for (index, completion) in chain.completed.iter().enumerate() {
        let matching = chain
            .prepared
            .iter()
            .position(|prepared| prepared.reference == completion.evidence.prepared)
            .ok_or_else(|| {
                refused_in_state(
                    "orphaned_completion",
                    INCONSISTENT,
                    format!("{} has no exact prepared record", completion.path.display()),
                )
            })?;
        let prepared = &chain.prepared[matching];
        if completion.evidence.sequence != prepared.evidence.sequence
            || !finalized_sequences.insert(completion.evidence.sequence)
            || completion.evidence.published != prepared.evidence.replacement
            || completion
                .evidence
                .completed_at_unix_ns
                .parse::<u128>()
                .is_err()
        {
            return Err(refused_in_state(
                "audit_evidence",
                INCONSISTENT,
                format!(
                    "{} contradicts its prepared record",
                    completion.path.display()
                ),
            ));
        }
        let owner = read_audit_record::<Value>(&completion.path)?.2;
        if (owner.uid, owner.gid) != (prepared.evidence.source.uid, prepared.evidence.source.gid) {
            return Err(refused_in_state(
                "audit_permissions",
                INCONSISTENT,
                "completion evidence does not preserve the source owner",
            ));
        }
        if completion_by_prepared
            .insert(completion.evidence.prepared.clone(), index)
            .is_some()
        {
            return Err(refused_in_state(
                "audit_chain_ambiguous",
                INCONSISTENT,
                "multiple completion records name the same prepared evidence",
            ));
        }
    }

    let mut previous: Option<EvidenceRef> = None;
    let mut superseded = BTreeSet::new();
    for (index, prepared) in chain.prepared.iter().enumerate() {
        if prepared.evidence.previous_finalized != previous {
            return Err(refused_in_state(
                "audit_chain",
                INCONSISTENT,
                format!(
                    "{} has an ambiguous previous-finalized link",
                    prepared.path.display()
                ),
            ));
        }
        if completion_by_prepared.contains_key(&prepared.reference) {
            let completion_index = completion_by_prepared[&prepared.reference];
            previous = Some(chain.completed[completion_index].reference.clone());
            chain.latest_finalized = Some(completion_index);
        }
        if let Some(supersedes) = &prepared.evidence.supersedes_prepared {
            let old = chain
                .prepared
                .iter()
                .position(|candidate| &candidate.reference == supersedes)
                .ok_or_else(|| {
                    refused_in_state(
                        "audit_chain",
                        INCONSISTENT,
                        "a prepared record supersedes missing evidence",
                    )
                })?;
            if chain.prepared[old].evidence.sequence >= prepared.evidence.sequence
                || chain.prepared[old].evidence.source.sha256 != prepared.evidence.source.sha256
                || completion_by_prepared.contains_key(supersedes)
                || !superseded.insert(supersedes.clone())
            {
                return Err(refused_in_state(
                    "audit_chain_ambiguous",
                    INCONSISTENT,
                    "prepared-evidence supersession is contradictory or ambiguous",
                ));
            }
        }
        if !completion_by_prepared.contains_key(&prepared.reference) {
            chain.active = Some(index);
        }
    }
    if let Some(active) = chain.active
        && superseded.contains(&chain.prepared[active].reference)
    {
        chain.active = None;
    }
    let unresolved = chain
        .prepared
        .iter()
        .filter(|record| {
            !completion_by_prepared.contains_key(&record.reference)
                && !superseded.contains(&record.reference)
        })
        .count();
    if unresolved > 1 {
        return Err(refused_in_state(
            "audit_chain_ambiguous",
            INCONSISTENT,
            "multiple prepared records are unresolved candidates",
        ));
    }
    if chain.active.is_none() {
        chain.active = chain.prepared.iter().position(|record| {
            !completion_by_prepared.contains_key(&record.reference)
                && !superseded.contains(&record.reference)
        });
    }
    Ok(chain)
}

fn validate_prepared(evidence: &PreparedEvidence, actual: &Identity) -> Result<(), Refused> {
    let (database_format, record_version) = storage_format_versions();
    if evidence.schema != PREPARED_SCHEMA
        || evidence.sequence == 0
        || !matches!(evidence.operation.as_str(), "reserved" | "held")
        || evidence.database_format != database_format
        || evidence.record_version != record_version
        || !valid_digest(&evidence.source.sha256)
        || !valid_digest(&evidence.replacement.sha256)
        || !valid_digest(&evidence.rollback_artifact.sha256)
        || evidence.source.sha256 != evidence.rollback_artifact.sha256
        || evidence.source.bytes != evidence.rollback_artifact.bytes
        || evidence.rollback_artifact.mode != 0o600
        || (
            evidence.replacement.uid,
            evidence.replacement.gid,
            evidence.replacement.mode,
        ) != (
            evidence.source.uid,
            evidence.source.gid,
            evidence.source.mode,
        )
        || actual.mode != 0o600
        || (actual.uid, actual.gid) != (evidence.source.uid, evidence.source.gid)
        || (
            evidence.rollback_artifact.uid,
            evidence.rollback_artifact.gid,
        ) != (evidence.source.uid, evidence.source.gid)
        || Path::new(&evidence.rollback_artifact.file).file_name()
            != Some(std::ffi::OsStr::new(&evidence.rollback_artifact.file))
        || evidence
            .plan_digest
            .as_ref()
            .is_some_and(|digest| !valid_digest(digest))
    {
        return Err(refused_in_state(
            "audit_evidence",
            INCONSISTENT,
            "prepared evidence is damaged, contradictory, or has unsafe ownership",
        ));
    }
    if let Some(previous) = &evidence.previous_finalized
        && !valid_digest(&previous.sha256)
    {
        return Err(refused_in_state(
            "audit_chain",
            INCONSISTENT,
            "the previous-finalized evidence identity is malformed",
        ));
    }
    Ok(())
}

fn unix_time_ns() -> Result<String, Refused> {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| refused("clock", error.to_string()))?;
    Ok(format!(
        "{}{:09}",
        elapsed.as_secs(),
        elapsed.subsec_nanos()
    ))
}

/// Atomically names a complete, owned, synced JSON record. A crash while the
/// temporary file is written leaves no partially valid audit-chain entry.
fn write_immutable_json<T: Serialize>(
    directory: &Path,
    final_path: &Path,
    value: &T,
    uid: u32,
    gid: u32,
    crash_prefix: &str,
    post_publication: bool,
) -> Result<EvidenceRef, Refused> {
    let temporary = directory.join(format!(
        "{}.tmp-{}",
        final_path
            .file_name()
            .map(|name| name.to_string_lossy())
            .ok_or_else(|| refused("audit_evidence", "audit record has no file name"))?,
        uuid::Uuid::now_v7()
    ));
    let mut guard = Scratch(Some(temporary.clone()));
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(true)
        .create_new(true)
        .custom_flags(libc::O_NOFOLLOW);
    let mut file = private_file(&mut options)
        .open(&temporary)
        .map_err(|error| io("create immutable audit evidence", error))?;
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(|error| refused("audit_evidence", error.to_string()))?;
    file.write_all(&bytes)
        .map_err(|error| io("write immutable audit evidence", error))?;
    let written = establish_security(&file, uid, gid, 0o600)?;
    let named = revalidate_named(&temporary, &written)?;
    drop(named);
    crash_point(&format!("{crash_prefix}-file-sync"));
    if std::fs::symlink_metadata(final_path).is_ok() {
        return Err(refused(
            "audit_chain_ambiguous",
            "the immutable audit record pathname already exists",
        ));
    }
    std::fs::rename(&temporary, final_path)
        .map_err(|error| io("name immutable audit evidence", error))?;
    guard.keep();
    let named = revalidate_named(final_path, &written)?;
    drop(named);
    if post_publication {
        crash_point(&format!("{crash_prefix}-before-directory-sync"));
    }
    sync_directory(directory).map_err(|error| {
        if post_publication {
            publication_io("sync the audit directory", error)
        } else {
            io("sync the audit directory", error)
        }
    })?;
    crash_point(&format!("{crash_prefix}-directory-sync"));
    let digest = digest_bytes(&bytes);
    let file = final_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .ok_or_else(|| refused("audit_evidence", "audit record has no file name"))?;
    Ok(EvidenceRef {
        file,
        sha256: digest,
    })
}

fn acquire_lock(directory: &Path) -> Result<File, Refused> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(true)
        .custom_flags(libc::O_NOFOLLOW);
    let lock = options
        .open(directory.join(DATABASE_LOCK_FILE))
        .map_err(|error| io("open the host lock file", error))?;
    let metadata = lock
        .metadata()
        .map_err(|error| io("stat the host lock file", error))?;
    if !metadata.is_file() || metadata.nlink() != 1 {
        return Err(refused(
            "not_a_plain_file",
            "the host lock must be one regular file without hard links",
        ));
    }
    if lock.try_lock().is_err() {
        return Err(refused(
            "host_running",
            "an OCG host (or another reconciliation) holds the database lock",
        ));
    }
    Ok(lock)
}

pub fn run(arguments: Vec<String>) -> Result<(), Failure> {
    if let [directory, flag] = arguments.as_slice()
        && flag == "--finalize"
    {
        return finish(finalize(Path::new(directory), RecoveryKind::Reserved));
    }
    let [directory, job, flag] = arguments.as_slice() else {
        return Err(USAGE.into());
    };
    if flag != "--confirm-no-start" {
        return Err(USAGE.into());
    }
    let job = uuid::Uuid::parse_str(job)
        .map(|id| JobId::new(id.as_u128()))
        .map_err(|_| "the Job must be a UUID")?;
    finish(reconcile(Path::new(directory), Operation::Reserved(job)))
}

pub fn run_held(arguments: Vec<String>) -> Result<(), Failure> {
    let (directory, expected_digest) = match arguments.as_slice() {
        [directory, flag] if flag == "--dry-run" => (directory, None),
        [directory, flag] if flag == "--finalize" => {
            return finish(finalize(Path::new(directory), RecoveryKind::Held));
        }
        [directory, flag, digest] if flag == "--apply" => {
            if !valid_digest(digest) {
                return Err("the plan digest must be 64 lowercase hexadecimal characters".into());
            }
            (directory, Some(digest.clone()))
        }
        _ => return Err(HELD_USAGE.into()),
    };
    finish(reconcile(
        Path::new(directory),
        Operation::Held { expected_digest },
    ))
}

fn finish(result: Result<Value, Refused>) -> Result<(), Failure> {
    match result {
        Ok(report) => {
            println!("{}", serde_json::to_string_pretty(&report)?);
            Ok(())
        }
        Err(refusal) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "outcome": "refused",
                    "state": refusal.state,
                    "code": refusal.code,
                    "detail": refusal.detail,
                }))?
            );
            Err(format!("refused: {}", refusal.code).into())
        }
    }
}

fn private_copy(original: &mut File, source_sha256: &str) -> Result<File, Refused> {
    let scratch = std::env::temp_dir().join(format!("{STALE_PREFIX}{}", uuid::Uuid::now_v7()));
    let mut create = OpenOptions::new();
    create.read(true).write(true).create_new(true);
    let mut private = private_file(&mut create)
        .open(&scratch)
        .map_err(|error| io("create the private copy", error))?;
    std::fs::remove_file(&scratch).map_err(|error| io("unlink the private copy", error))?;
    let (copied, _) =
        copy_with_digest(original, &mut private).map_err(|error| io("copy the database", error))?;
    if copied != source_sha256 {
        return Err(refused(
            "unstable_source",
            "the database changed while it was copied",
        ));
    }
    private
        .sync_all()
        .map_err(|error| io("sync the private copy", error))?;
    Ok(private)
}

fn reconcile(directory: &Path, operation: Operation) -> Result<Value, Refused> {
    let directory = std::fs::canonicalize(directory)
        .map_err(|error| io("resolve the data directory", error))?;
    let database_path = directory.join(DATABASE_FILE_NAME);
    let lock = acquire_lock(&directory)?;

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
    let chain = load_audit_chain(&directory)?;
    let supersedes = match chain.active {
        Some(index) => {
            let active = &chain.prepared[index].evidence;
            if active.replacement.sha256 == source_sha256 {
                return Err(refused_in_state(
                    "finalization_required",
                    PUBLISHED_UNFINALIZED,
                    "the exact prepared replacement is published without durable completion; use --finalize",
                ));
            }
            if active.source.sha256 != source_sha256 {
                return Err(refused_in_state(
                    "inconsistent",
                    INCONSISTENT,
                    format!(
                        "the current database SHA-256 {source_sha256} equals neither prepared source {} nor replacement {}",
                        active.source.sha256, active.replacement.sha256
                    ),
                ));
            }
            chain.prepared[index].reference.clone().into()
        }
        None => None,
    };

    let private = private_copy(&mut original, &source_sha256)?;
    match &operation {
        Operation::Reserved(job) => {
            crate::db::reconcile_reserved_preflight(private, *job)?;
        }
        Operation::Held { expected_digest } => {
            let plan = crate::db::reconcile_held_preflight(private, &source_sha256)?;
            if expected_digest
                .as_ref()
                .is_some_and(|expected| expected != &plan.digest)
            {
                return Err(refused(
                    "plan_digest_mismatch",
                    "the source or its recovery decisions changed; repeat the dry-run",
                ));
            }
            if expected_digest.is_none() {
                return Ok(json!({
                    "outcome": "planned",
                    "state": if supersedes.is_some() { json!(PREPARED_NOT_PUBLISHED) } else { Value::Null },
                    "plan": plan.canonical,
                    "plan_digest": plan.digest,
                    "original": {"sha256": source_sha256, "bytes": source_length},
                    "source_unchanged": true,
                    "no_provider_contacted": true,
                    "apply": "ocg reconcile-held <data-directory> --apply <plan-digest>",
                }));
            }
        }
    }

    let token = uuid::Uuid::now_v7();
    let work_path = directory.join(format!("{STALE_PREFIX}{token}.work"));
    let mut work_guard = Scratch(Some(work_path.clone()));
    let mut work_create = OpenOptions::new();
    work_create
        .read(true)
        .write(true)
        .create_new(true)
        .custom_flags(libc::O_NOFOLLOW);
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
    establish_security(&work, before.uid, before.gid, before.mode)?;
    crash_point("after-work-copy");
    let applied = match &operation {
        Operation::Reserved(job) => crate::db::reconcile_reserved_apply(work, *job)?,
        Operation::Held { expected_digest } => crate::db::reconcile_held_apply(
            work,
            &source_sha256,
            expected_digest
                .as_deref()
                .ok_or_else(|| refused("plan_required", "an apply digest is required"))?,
        )?,
    };
    crash_point("after-apply");

    let mut work = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&work_path)
        .map_err(|error| io("reopen the work copy", error))?;
    establish_security(&work, before.uid, before.gid, before.mode)?;
    let (result_sha256, _) = digest(&mut work).map_err(|error| io("hash the work copy", error))?;
    let result_identity = identity(&work.metadata().map_err(|error| io("stat", error))?);
    let replacement = FileFact::new(&result_identity, result_sha256);
    revalidate_named(&work_path, &result_identity)?;

    let artifact_path = directory.join(format!("{DATABASE_FILE_NAME}.pre-reconcile-{token}"));
    let mut artifact_guard = Scratch(Some(artifact_path.clone()));
    let mut artifact_create = OpenOptions::new();
    artifact_create
        .read(true)
        .write(true)
        .create_new(true)
        .custom_flags(libc::O_NOFOLLOW);
    let mut artifact = private_file(&mut artifact_create)
        .open(&artifact_path)
        .map_err(|error| io("create the rollback artifact", error))?;
    let (artifact_written, _) = copy_with_digest(&mut original, &mut artifact)
        .map_err(|error| io("write the rollback artifact", error))?;
    establish_security(&artifact, before.uid, before.gid, 0o600)?;
    let (artifact_sha256, _) =
        digest(&mut artifact).map_err(|error| io("verify the artifact", error))?;
    let artifact_identity = identity(&artifact.metadata().map_err(|error| io("stat", error))?);
    if artifact_written != source_sha256 || artifact_sha256 != source_sha256 {
        return Err(refused(
            "artifact_unverified",
            "the rollback artifact does not equal the original",
        ));
    }
    revalidate_named(&artifact_path, &artifact_identity)?;

    let sequence = chain.next_sequence;
    let audit_stem = format!("{STALE_PREFIX}{sequence:020}-{token}");
    let manifest_path = directory.join(format!("{audit_stem}.prepared.json"));
    let mut manifest_guard = Scratch(Some(manifest_path.clone()));
    let previous_finalized = chain.latest_reference();
    let source_relation = match &previous_finalized {
        Some(previous)
            if chain.completed.iter().any(|completion| {
                &completion.reference == previous
                    && completion.evidence.published.sha256 == source_sha256
            }) =>
        {
            "byte_contiguous_with_previous_finalized"
        }
        Some(_) => "source_changed_since_previous_finalized_normal_writes_or_operator_restore",
        None => "no_previous_finalized_chain",
    };
    let (recovery_plan, plan_digest, targets, expected_delta) = match &operation {
        Operation::Reserved(job) => (
            json!({"kind": "ocg-reserved-recovery-plan-v1", "job": job.to_string(), "source_sha256": source_sha256}),
            None,
            vec![json!({"job": job.to_string(), "allocation": job.to_string()})],
            json!({"released_allocations": 1, "failed_jobs": 1, "unrelated_rows": "unchanged"}),
        ),
        Operation::Held { .. } => {
            let actions = applied["plan"]["actions"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            (
                applied["plan"].clone(),
                Some(
                    applied["plan_digest"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned(),
                ),
                actions.clone(),
                json!({
                    "released_allocations": actions.len(),
                    "unknown_jobs": actions.len(),
                    "unrelated_allocations_and_jobs": "byte-identical",
                }),
            )
        }
    };
    let prepared = PreparedEvidence {
        schema: PREPARED_SCHEMA.to_owned(),
        sequence,
        operation: operation.kind().as_str().to_owned(),
        recovery_plan,
        plan_digest,
        targets,
        source: FileFact::new(&before, source_sha256.clone()),
        replacement: replacement.clone(),
        rollback_artifact: ArtifactFact {
            file: artifact_path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .ok_or_else(|| refused("audit_evidence", "rollback artifact has no name"))?,
            sha256: artifact_sha256.clone(),
            bytes: artifact_identity.length,
            device: artifact_identity.device,
            inode: artifact_identity.inode,
            mode: artifact_identity.mode,
            uid: artifact_identity.uid,
            gid: artifact_identity.gid,
        },
        expected_logical_row_delta: expected_delta,
        previous_finalized,
        source_relation: source_relation.to_owned(),
        supersedes_prepared: supersedes,
        database_format: storage_format_versions().0,
        record_version: storage_format_versions().1,
    };
    crash_point("before-prepared-evidence");
    let prepared_reference = write_immutable_json(
        &directory,
        &manifest_path,
        &prepared,
        before.uid,
        before.gid,
        "after-prepared",
        false,
    )?;

    let mut recheck = open_original(&database_path)?;
    let named = identity(&recheck.metadata().map_err(|error| io("stat", error))?);
    let (recheck_sha256, _) =
        digest(&mut recheck).map_err(|error| io("reread the database", error))?;
    if named != before || recheck_sha256 != source_sha256 {
        return Err(refused_in_state(
            "source_changed",
            PREPARED_NOT_PUBLISHED,
            "the database changed after it was checked; nothing was published",
        ));
    }
    drop(recheck);
    revalidate_named(&work_path, &result_identity)?;
    revalidate_named(&artifact_path, &artifact_identity)?;
    let (manifest_check, manifest_reference, _) = read_record::<PreparedEvidence>(&manifest_path)?;
    if manifest_reference != prepared_reference || manifest_check.replacement != replacement {
        return Err(refused(
            "audit_evidence",
            "prepared evidence changed before publication",
        ));
    }

    crash_point("before-rename");
    std::fs::rename(&work_path, &database_path)
        .map_err(|error| io("publish the reconciled database", error))?;
    work_guard.keep();
    artifact_guard.keep();
    manifest_guard.keep();
    crash_point("after-rename");
    sync_directory(&directory)
        .map_err(|error| publication_io("sync the published database directory", error))?;
    crash_point("after-directory-sync");

    let (verification, published) = verify_publication(
        &directory,
        &database_path,
        &lock,
        &operation,
        &prepared,
        &artifact_path,
        &replacement,
    )
    .map_err(unfinalized)?;
    crash_point("after-post-publication-verification");
    let completion_path = directory.join(format!("{audit_stem}.finalized.json"));
    let completion = CompletionEvidence {
        schema: COMPLETION_SCHEMA.to_owned(),
        sequence,
        prepared: prepared_reference,
        published: published.clone(),
        verification,
        completed_at_unix_ns: unix_time_ns().map_err(unfinalized)?,
        reconstructed_after_crash: false,
    };
    crash_point("before-completion-record");
    let completion_reference = write_immutable_json(
        &directory,
        &completion_path,
        &completion,
        before.uid,
        before.gid,
        "after-completion",
        true,
    )
    .map_err(|error| Refused {
        state: Some(PUBLISHED_UNFINALIZED),
        ..error
    })?;

    let (completion_check, completion_digest, _) =
        read_record::<CompletionEvidence>(&completion_path).map_err(|error| Refused {
            state: Some(INCONSISTENT),
            ..error
        })?;
    if completion_digest != completion_reference
        || completion_check.published != published
        || completion_check.verification["passed"] != json!(true)
    {
        return Err(refused_in_state(
            "audit_evidence",
            INCONSISTENT,
            "durable completion evidence changed during final confirmation; do not roll back automatically",
        ));
    }
    let final_identity = confirm_final_published(&database_path, &published)?;
    drop(final_identity);
    crash_point("after-finalization-before-lock-release");
    drop(lock);

    let mut report = json!({
        "outcome": "reconciled",
        "state": PUBLISHED_FINALIZED,
        "no_provider_contacted": true,
        "applied": applied,
        "original": {"sha256": source_sha256, "bytes": source_length},
        "rollback_artifact": {"path": artifact_path.display().to_string(), "sha256": artifact_sha256, "verified_identical": true, "mode": "600", "uid": before.uid, "gid": before.gid},
        "prepared_evidence": manifest_path.display().to_string(),
        "completion_evidence": completion_path.display().to_string(),
        "completion_evidence_sha256": completion_reference.sha256,
        "published": published,
        "post_publication_check": completion.verification,
        "exclusive_lock_released_after_final_confirmation": true,
    });
    match operation {
        Operation::Reserved(job) => report["job"] = json!(job.to_string()),
        Operation::Held { .. } => {
            report["plan"] = report["applied"]["plan"].clone();
            report["plan_digest"] = report["applied"]["plan_digest"].clone();
        }
    }
    Ok(report)
}

#[allow(clippy::too_many_arguments)]
fn verify_publication(
    directory: &Path,
    database_path: &Path,
    lock: &File,
    operation: &Operation,
    prepared: &PreparedEvidence,
    artifact_path: &Path,
    replacement: &FileFact,
) -> Result<(Value, FileFact), Refused> {
    crash_point("before-post-publication-verification");
    let (mut published_file, published_identity) =
        open_plain(database_path, false).map_err(|error| Refused {
            state: Some(PUBLISHED_UNFINALIZED),
            ..error
        })?;
    let (published_sha256, _) = digest(&mut published_file)
        .map_err(|error| publication_io("hash the published database", error))?;
    if published_sha256 != replacement.sha256 || !replacement.matches_identity(&published_identity)
    {
        return Err(refused_in_state(
            "post_publication_verification",
            PUBLISHED_UNFINALIZED,
            "the published database is not the exact expected replacement; do not restore a rollback artifact automatically",
        ));
    }
    if (
        published_identity.uid,
        published_identity.gid,
        published_identity.mode,
    ) != (
        prepared.source.uid,
        prepared.source.gid,
        prepared.source.mode,
    ) {
        return Err(refused_in_state(
            "post_publication_verification",
            PUBLISHED_UNFINALIZED,
            "published ownership or permissions differ from the source; do not roll back automatically",
        ));
    }
    let inspection = match operation {
        Operation::Reserved(_) => crate::db::verify_reserved_publication(
            directory,
            &published_file,
            lock,
            replacement.device,
            replacement.inode,
        ),
        Operation::Held { .. } => crate::db::verify_recovered_publication(
            directory,
            &published_file,
            lock,
            replacement.device,
            replacement.inode,
        ),
    }
    .map_err(|error| {
        refused_in_state(
            "post_publication_verification",
            PUBLISHED_UNFINALIZED,
            format!("{error}; do not restore a rollback artifact automatically"),
        )
    })?;
    if inspection["logical_checks"]["passed"] != json!(true)
        || inspection["page_checksums"] != json!("every page checksum verified")
        || inspection["exclusive_lock_held"] != json!(true)
    {
        return Err(refused_in_state(
            "post_publication_verification",
            PUBLISHED_UNFINALIZED,
            "published redb or logical verification failed; do not restore a rollback artifact automatically",
        ));
    }
    let (mut artifact, artifact_identity) = open_plain(artifact_path, false)?;
    if artifact_identity.uid != prepared.source.uid
        || artifact_identity.gid != prepared.source.gid
        || artifact_identity.mode != 0o600
        || artifact_identity.device != prepared.rollback_artifact.device
        || artifact_identity.inode != prepared.rollback_artifact.inode
    {
        return Err(refused_in_state(
            "artifact_unverified",
            PUBLISHED_UNFINALIZED,
            "the rollback artifact identity, owner, or mode changed",
        ));
    }
    let (artifact_digest, _) = digest(&mut artifact)
        .map_err(|error| publication_io("verify the rollback artifact", error))?;
    if artifact_digest != prepared.rollback_artifact.sha256 {
        return Err(refused_in_state(
            "artifact_unverified",
            PUBLISHED_UNFINALIZED,
            "the rollback artifact digest differs from prepared evidence",
        ));
    }
    let delta = match operation {
        Operation::Reserved(job) => {
            crate::db::verify_reserved_publication_delta(&artifact, &published_file, *job)
        }
        Operation::Held { .. } => crate::db::verify_held_publication_delta(
            &artifact,
            &published_file,
            &prepared.source.sha256,
            &prepared.recovery_plan,
            prepared.plan_digest.as_deref().unwrap_or_default(),
        ),
    }
    .map_err(|error| Refused {
        state: Some(PUBLISHED_UNFINALIZED),
        ..Refused::from(error)
    })?;
    Ok((
        json!({
            "passed": true,
            "redb_and_logical": inspection,
            "authorized_delta": delta,
            "ownership_classification": "consistent",
            "unrelated_rows_preserved": true,
        }),
        FileFact::new(&published_identity, published_sha256),
    ))
}

fn confirm_final_published(database_path: &Path, expected: &FileFact) -> Result<File, Refused> {
    let (mut file, actual) = open_plain(database_path, false).map_err(|error| Refused {
        state: Some(INCONSISTENT),
        ..error
    })?;
    let (actual_digest, _) = digest(&mut file).map_err(|error| {
        refused_in_state(
            "final_confirmation",
            INCONSISTENT,
            format!("cannot rehash the finalized publication: {error}"),
        )
    })?;
    if actual_digest != expected.sha256 || !expected.matches_identity(&actual) {
        return Err(refused_in_state(
            "final_confirmation",
            INCONSISTENT,
            "the final published identity or digest differs from durable completion evidence; do not roll back automatically",
        ));
    }
    Ok(file)
}

fn finalize(directory: &Path, kind: RecoveryKind) -> Result<Value, Refused> {
    let directory = std::fs::canonicalize(directory)
        .map_err(|error| io("resolve the data directory", error))?;
    let database_path = directory.join(DATABASE_FILE_NAME);
    let lock = acquire_lock(&directory)?;
    let chain = load_audit_chain(&directory)?;
    let Some(active_index) = chain.active else {
        let Some(latest) = chain.latest_finalized else {
            return Err(refused(
                "no_publication_evidence",
                "there is no prepared or finalized publication to finalize",
            ));
        };
        let completion = &chain.completed[latest];
        let prepared = chain
            .prepared
            .iter()
            .find(|prepared| prepared.reference == completion.evidence.prepared)
            .ok_or_else(|| refused("audit_chain", "finalized prepared evidence is missing"))?;
        if prepared.evidence.operation != kind.as_str() {
            return Err(refused(
                "operation_mismatch",
                format!(
                    "the latest finalized publication is for {}",
                    prepared.evidence.operation
                ),
            ));
        }
        let mut current = open_original(&database_path)?;
        let (current_sha256, _) =
            digest(&mut current).map_err(|error| io("read the database", error))?;
        drop(current);
        if current_sha256 == completion.evidence.published.sha256 {
            let final_file =
                confirm_final_published(&database_path, &completion.evidence.published)?;
            drop(final_file);
            sync_directory(&directory).map_err(|error| {
                refused_in_state(
                    "publication_finalization",
                    PUBLISHED_FINALIZED,
                    format!(
                        "sync the finalized audit directory: {error}; durable completion evidence remains finalized; reconfirm the published identity before ordinary use"
                    ),
                )
            })?;
            let final_file =
                confirm_final_published(&database_path, &completion.evidence.published)?;
            drop(final_file);
        }
        drop(lock);
        return Ok(json!({
            "outcome": "already_finalized",
            "state": PUBLISHED_FINALIZED,
            "completion_evidence": completion.path.display().to_string(),
            "completion_evidence_sha256": completion.reference.sha256,
            "prepared_evidence_sha256": completion.evidence.prepared.sha256,
            "published_sha256": completion.evidence.published.sha256,
            "current_sha256": current_sha256,
            "current_matches_published": current_sha256 == completion.evidence.published.sha256,
            "allocation_transaction_reexecuted": false,
            "note": "a later current digest may represent ordinary writes or an operator restore; the finalized chain was not reset",
        }));
    };
    let prepared_record = &chain.prepared[active_index];
    let prepared = &prepared_record.evidence;
    if prepared.operation != kind.as_str() {
        return Err(refused_in_state(
            "operation_mismatch",
            PUBLISHED_UNFINALIZED,
            format!(
                "the unresolved prepared publication is for {}",
                prepared.operation
            ),
        ));
    }
    let operation = match kind {
        RecoveryKind::Reserved => {
            let job = prepared
                .targets
                .first()
                .and_then(|target| target["job"].as_str());
            let job = job
                .ok_or_else(|| refused("audit_evidence", "reserved target Job is missing"))
                .and_then(|job| {
                    uuid::Uuid::parse_str(job)
                        .map(|id| JobId::new(id.as_u128()))
                        .map_err(|_| refused("audit_evidence", "reserved target Job is invalid"))
                })?;
            Operation::Reserved(job)
        }
        RecoveryKind::Held => Operation::Held {
            expected_digest: prepared.plan_digest.clone(),
        },
    };
    let mut current = open_original(&database_path)?;
    let (current_sha256, _) =
        digest(&mut current).map_err(|error| io("read the current database", error))?;
    if current_sha256 == prepared.source.sha256 {
        return Err(refused_in_state(
            "not_published",
            PREPARED_NOT_PUBLISHED,
            "the current database is exactly the prepared source; publication did not occur and no completion was created",
        ));
    }
    if current_sha256 != prepared.replacement.sha256 {
        return Err(refused_in_state(
            "inconsistent",
            INCONSISTENT,
            format!(
                "current database SHA-256 {current_sha256} equals neither source {} nor replacement {}; automatic finalization and rollback are refused",
                prepared.source.sha256, prepared.replacement.sha256
            ),
        ));
    }
    drop(current);

    let (verification, published) = verify_publication(
        &directory,
        &database_path,
        &lock,
        &operation,
        prepared,
        &directory.join(&prepared.rollback_artifact.file),
        &prepared.replacement,
    )
    .map_err(unfinalized)?;
    let completion_path = directory.join(format!(
        "{}.finalized.json",
        prepared_record
            .path
            .file_name()
            .and_then(|name| name
                .to_string_lossy()
                .strip_suffix(".prepared.json")
                .map(|stem| stem.to_owned()))
            .ok_or_else(|| refused("audit_evidence", "prepared evidence has an invalid name"))?
    ));
    let completion = CompletionEvidence {
        schema: COMPLETION_SCHEMA.to_owned(),
        sequence: prepared.sequence,
        prepared: prepared_record.reference.clone(),
        published: published.clone(),
        verification,
        completed_at_unix_ns: unix_time_ns().map_err(unfinalized)?,
        reconstructed_after_crash: true,
    };
    crash_point("before-completion-record");
    let completion_reference = write_immutable_json(
        &directory,
        &completion_path,
        &completion,
        prepared.source.uid,
        prepared.source.gid,
        "after-completion",
        true,
    )
    .map_err(unfinalized)?;
    let (completion_check, actual_reference, _) =
        read_record::<CompletionEvidence>(&completion_path).map_err(|error| Refused {
            state: Some(INCONSISTENT),
            ..error
        })?;
    if actual_reference != completion_reference || completion_check.published != published {
        return Err(refused_in_state(
            "audit_evidence",
            INCONSISTENT,
            "reconstructed completion evidence is inconsistent",
        ));
    }
    let final_file = confirm_final_published(&database_path, &published)?;
    drop(final_file);
    crash_point("after-finalization-before-lock-release");
    drop(lock);
    Ok(json!({
        "outcome": "finalized",
        "state": PUBLISHED_FINALIZED,
        "reconstructed_after_crash": true,
        "allocation_transaction_reexecuted": false,
        "prepared_evidence": prepared_record.path.display().to_string(),
        "completion_evidence": completion_path.display().to_string(),
        "completion_evidence_sha256": completion_reference.sha256,
        "published": published,
        "verification": completion.verification,
        "exclusive_lock_released_after_final_confirmation": true,
    }))
}
