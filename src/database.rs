use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, PoisonError, RwLock, RwLockWriteGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use redb::Database;

use crate::attachments::{
    AttachmentCleanupReport, AttachmentContent, AttachmentError, AttachmentStore, Reclamation,
    ReclamationError, StoreError,
};
use crate::data_directory::DataDirectory;
use crate::disk_protection::{
    self, DiskProtectionStatus, FilesystemConfig, FilesystemConfigError, ProtectedArea,
    ProtectionCell, ProtectionLock, ProtectionReason, ProtectionWatch, RecoveryError,
    RecoveryReport, ReopenFailure, SharedRequest, SpaceMeasurement, SpaceShortage,
};
use crate::persistence::{
    self, AgentId, AgentRecord, AppendedOutput, AttachmentId, AttachmentOrigin, AttachmentRecord,
    AvailabilityAggregationProgress, AvailabilityBatch, AvailabilityCounts, AvailabilityStatId,
    AvailabilityStatRecord, CleanupKind, ContentDigest, ExecutorId, JobFinalization, JobId,
    JobInputDraft, JobInputId, JobOutputRecord, JobRecord, JobRetry, LlmProviderId,
    LlmProviderRecord, MetricsId, MetricsRecord, NewAgent, NewAttachment, NewAvailabilityStat,
    NewExecutor, NewJob, NewJobOutput, NewLlmProvider, NewMetrics, NewProfile, NewProfileConfig,
    NewProject, NewSubmission, NewToolEvidence, ObservationCursor, ObservationFilter,
    ObservationPage, ObservedModel, ObservedModelFilter, ObservedModelPage, OperationId,
    OperationKind, OperationMarker, Profile, ProfileId, ProfileRecord, ProfileVariant, ProjectId,
    ProjectRecord, PublishedAttachment, ReadSnapshot, RequestNumber, RequestRef,
    RequestSampleRecord, SubmissionKey, SubmissionRecord, Timestamp, ToolCallRef, ToolDecision,
    ToolEvidenceRecord, ToolOutcome, ToolReconciliation, Violation, WrongProfileKind,
};
use crate::storage::{
    self, BatchError, BatchFailure, Commit, CompactFailure, MarkerSubjects, OpenError, OpenedFile,
};
pub use crate::storage::{StorageError, StorageErrorKind};
use crate::system_policy::{
    self, PolicyOperationError, PolicySaveResult, SystemPolicy, SystemPolicyError,
};
use crate::token_usage::{
    TokenAggregationProgress, TokenBatch, TokenEvidenceError, TokenMinuteTotals,
    TokenRequestRecord, TokenSnapshot,
};

pub const COMMAND_QUEUE_CAPACITY: usize = 256;
/// The most queued commands one group commit covers. Commands that queue up
/// while the worker is busy form a group without any waiting, so a lone
/// command is never delayed. A group is also the most commands whose outcome
/// a failed durable commit leaves unknown, and the longest stretch readers
/// can be held back, so it stays small.
const GROUP_LIMIT: usize = 32;
/// Compaction policy. `compact` is uninterruptible and holds off every read
/// and command while it runs, so it is only worth running on a large file
/// with a lot of free space, and not again soon after an attempt. These are
/// deliberately conservative and not tuned to a measured workload.
const COMPACTION_MIN_FILE_BYTES: u64 = 64 * 1024 * 1024;
const COMPACTION_MIN_RECLAIMABLE_BYTES: u64 = 32 * 1024 * 1024;
/// The estimated free space must also be at least `1 / this` of the file.
const COMPACTION_MIN_RECLAIMABLE_DIVISOR: u64 = 4;
const COMPACTION_COOLDOWN: Duration = Duration::from_secs(24 * 60 * 60);
/// While admission is locked, the protected filesystems are checked at
/// least this often, so a lock that only lacked space lifts soon after the
/// space returns, however long the configured interval is.
pub(crate) const PROTECTED_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// How long to wait for the next full check: the configured interval, but
/// no longer than [`PROTECTED_CHECK_INTERVAL`] while admission is locked.
pub(crate) fn next_check_interval(configured: Duration, locked: bool) -> Duration {
    if locked {
        configured.min(PROTECTED_CHECK_INTERVAL)
    } else {
        configured
    }
}
/// Reads run on this many dedicated threads, separate from the write worker.
pub const READ_WORKERS: usize = 2;
pub const READ_QUEUE_CAPACITY: usize = 64;
/// The most index entries one observation query reads, matching or not.
pub const MAX_OBSERVATION_SCAN: usize = storage::MAX_OBSERVATION_SCAN;
/// The most distinct (Provider, model) pairs one choice query examines.
pub const MAX_OBSERVED_MODEL_SCAN: usize = storage::MAX_OBSERVED_MODEL_SCAN;
/// At most one space check and one recovery are pending at a time; later
/// requests join them.
const CONTROL_QUEUE_CAPACITY: usize = 2;
/// The space a command's transaction is allowed beyond twice its
/// variable-length payload. This is an allowance, not a prediction of redb
/// file growth, which depends on page reuse, B-tree splits and region
/// allocation; real I/O errors are still handled after a passing check.
const TRANSACTION_ALLOWANCE_BYTES: u64 = 1024 * 1024;

mod commands;
mod errors;
mod handle;
mod shared;
mod worker;
#[cfg(test)]
use commands::{ByteCounter, counted_json_bytes, json_bytes};
use commands::{Command, Control, Envelope, Message, Reply, byte_count};
use errors::operation_status;
pub use errors::{
    CommandOutcome, CompactionOutcome, CompactionReport, CompactionSkip, DatabaseError,
    DatabaseStatus, MaintenanceError, OperationStatus, TokenConsumeError, UnknownCause,
};
pub(crate) use shared::AggregationClaim;
#[cfg(test)]
pub(crate) use shared::GroupProbe;
use shared::{
    Admission, Opened, ReadJob, ReadMessage, ReadWorkerExit, ReleaseOnPanic, Shared,
    StagingBarrier, admitted, flush_staged, serve_reads,
};
pub use worker::{DatabaseHandle, DatabaseWorker};
use worker::{Executed, run, settle};

#[cfg(test)]
mod read_pool_tests {
    //! Read-pool failure and shutdown. Faults are injected by submitting
    //! test-only read closures that block on a channel and then panic, which
    //! makes a read worker exit; every wait is bounded.

    use std::future::Future;
    use std::pin::Pin;
    use std::time::Instant;

    use super::*;
    use crate::test_support::{WAIT, data_directory as test_data_directory, poll_once, wait};

    const SUITE: &str = "ocg-read-pool";
    const QUEUED: usize = 5;

    type PendingRead<'a, T> = Pin<Box<dyn Future<Output = Result<T, DatabaseError>> + 'a>>;

    fn data_directory(name: &str) -> (crate::test_support::TestRoot, DataDirectory) {
        test_data_directory(SUITE, name)
    }

    /// Occupies every read worker with a read that reports it has started,
    /// waits for release and then panics, so the worker exits.
    struct HeldWorkers {
        release: flume::Sender<()>,
    }

    fn hold_every_read_worker<'a>(
        handle: &'a DatabaseHandle,
    ) -> (HeldWorkers, Vec<PendingRead<'a, ()>>) {
        let (entered_sender, entered) = flume::unbounded();
        let (release, released) = flume::unbounded::<()>();
        let mut reads: Vec<PendingRead<'a, ()>> = Vec::new();
        for _ in 0..READ_WORKERS {
            let (entered_sender, released) = (entered_sender.clone(), released.clone());
            let mut read: PendingRead<'a, ()> = Box::pin(handle.read(move |_| {
                entered_sender.send(()).expect("test observer");
                released.recv_timeout(WAIT).expect("test release");
                panic!("test: read worker exits unexpectedly");
            }));
            assert!(
                poll_once(read.as_mut()).is_none(),
                "a held read must be admitted, not answered"
            );
            reads.push(read);
        }
        for _ in 0..READ_WORKERS {
            entered
                .recv_timeout(WAIT)
                .expect("every read worker picked up a held read");
        }
        (HeldWorkers { release }, reads)
    }

    impl HeldWorkers {
        fn release(&self) {
            for _ in 0..READ_WORKERS {
                self.release.send(()).expect("held reads are waiting");
            }
        }
    }

    fn queue_reads(handle: &DatabaseHandle) -> Vec<PendingRead<'_, Vec<JobRecord>>> {
        (0..QUEUED)
            .map(|_| {
                let mut read: PendingRead<'_, Vec<JobRecord>> = Box::pin(handle.jobs(None, 1));
                assert!(
                    poll_once(read.as_mut()).is_none(),
                    "a queued read must be admitted, not answered"
                );
                read
            })
            .collect()
    }

    fn pool_stopped(result: &Result<impl std::fmt::Debug, DatabaseError>) -> bool {
        matches!(result, Err(DatabaseError::ReadPoolStopped))
    }

    fn reopen_and_read(directory: &DataDirectory) {
        let worker = DatabaseWorker::start(directory).expect("immediate reopen");
        let handle = worker.handle();
        assert!(
            wait(handle.jobs(None, 1)).is_ok(),
            "reads work after reopening"
        );
        worker.shutdown().expect("clean shutdown after reopening");
    }

    #[test]
    fn both_read_workers_exiting_resolve_every_queued_read() {
        let (_root, directory) = data_directory("queued");
        let worker = DatabaseWorker::start(&directory).unwrap();
        let handle = worker.handle();
        let (held, executing) = hold_every_read_worker(&handle);
        let queued = queue_reads(&handle);

        held.release();
        for read in executing {
            assert!(
                matches!(wait(read), Err(DatabaseError::ReadInterrupted)),
                "an executing read stays ReadInterrupted"
            );
        }
        for read in queued {
            let result = wait(read);
            assert!(
                pool_stopped(&result),
                "a queued read is answered explicitly, got {result:?}"
            );
        }

        assert!(matches!(
            worker.shutdown(),
            Err(DatabaseError::WorkerPanicked)
        ));
        reopen_and_read(&directory);
    }

    #[test]
    fn new_reads_are_refused_once_the_pool_has_stopped() {
        let (_root, directory) = data_directory("refused");
        let worker = DatabaseWorker::start(&directory).unwrap();
        let handle = worker.handle();
        let (held, executing) = hold_every_read_worker(&handle);
        held.release();
        for read in executing {
            assert!(matches!(wait(read), Err(DatabaseError::ReadInterrupted)));
        }
        // A caller sees ReadInterrupted while its worker is still unwinding;
        // the pool stops when the last worker's exit has run. A read issued
        // in between is admitted and then answered by that exit.
        let deadline = Instant::now() + WAIT;
        while !*handle.shared.reads_stopped.read().unwrap() {
            assert!(
                Instant::now() < deadline,
                "the read pool did not stop in time"
            );
            let result = wait(handle.jobs(None, 1));
            assert!(
                pool_stopped(&result),
                "a read during pool exit is answered explicitly, got {result:?}"
            );
        }

        for _ in 0..3 {
            let mut read = Box::pin(handle.job(JobId::new(1)));
            let result = poll_once(read.as_mut())
                .expect("a read is refused at admission once the pool has stopped");
            assert!(pool_stopped(&result), "got {result:?}");
        }
        // A read-worker panic is not a storage failure: commands still run.
        assert_eq!(handle.status(), DatabaseStatus::Open);
        assert!(wait(handle.save_job_input(None, JobInputDraft::default())).is_ok());

        assert!(matches!(
            worker.shutdown(),
            Err(DatabaseError::WorkerPanicked)
        ));
        reopen_and_read(&directory);
    }

    #[test]
    fn read_pool_failure_racing_shutdown_resolves_every_caller() {
        let (_root, directory) = data_directory("shutdown");
        let worker = DatabaseWorker::start(&directory).unwrap();
        let handle = worker.handle();
        let (held, executing) = hold_every_read_worker(&handle);
        let queued = queue_reads(&handle);

        // Shutdown closes admission, then waits for the executing reads,
        // which hold the database open.
        let (finished_sender, finished) = flume::bounded(1);
        let shutdown = std::thread::spawn(move || finished_sender.send(worker.shutdown()).ok());
        let deadline = Instant::now() + WAIT;
        while handle.status() == DatabaseStatus::Open {
            assert!(
                Instant::now() < deadline,
                "shutdown did not close admission in time"
            );
            std::thread::yield_now();
        }
        assert!(
            finished.try_recv().is_err(),
            "shutdown cannot finish while reads hold the database"
        );
        assert!(matches!(
            wait(handle.jobs(None, 1)),
            Err(DatabaseError::ReadsClosed)
        ));
        assert!(matches!(
            wait(handle.save_job_input(None, JobInputDraft::default())),
            Err(DatabaseError::ShuttingDown)
        ));

        held.release();
        for read in executing {
            assert!(matches!(wait(read), Err(DatabaseError::ReadInterrupted)));
        }
        for read in queued {
            let result = wait(read);
            assert!(
                pool_stopped(&result) || matches!(result, Err(DatabaseError::ReadsClosed)),
                "a queued read is answered explicitly, got {result:?}"
            );
        }
        let result = finished.recv_timeout(WAIT).expect("shutdown finished");
        assert!(
            matches!(result, Err(DatabaseError::WorkerPanicked)),
            "got {result:?}"
        );
        shutdown.join().unwrap();
        reopen_and_read(&directory);
    }
}

#[cfg(test)]
mod json_bytes_tests {
    use serde_json::json;

    use super::*;
    use crate::persistence::IntermediateOutputKind;

    fn reference(value: &serde_json::Value) -> u64 {
        byte_count(serde_json::to_vec(value).expect("a Value serializes").len())
    }

    #[test]
    fn matches_compact_serialization() {
        let large = serde_json::Value::Array(
            (0..5_000)
                .map(|index| {
                    json!({
                        "index": index,
                        "text": format!("line \"{index}\"\n\t\\ \u{1F600} \u{e9}"),
                        "nested": [null, true, 1.5, {"deep": ["a", "b"]}],
                    })
                })
                .collect(),
        );
        let values = [
            json!(null),
            json!(true),
            json!(false),
            json!(0),
            json!(-12345),
            json!(u64::MAX),
            json!(3.25),
            json!(""),
            json!("plain"),
            json!("héllo 世界 😀"),
            json!("quote \" backslash \\ newline \n tab \t control \u{1} slash /"),
            json!([]),
            json!({}),
            json!({"a": [1, {"b": null}], "ключ": "значение", "k\"ey": ["x\ny"]}),
            large,
        ];
        for value in values {
            assert_eq!(
                json_bytes(&Some(value.clone())),
                reference(&value),
                "{value}"
            );
        }
    }

    #[test]
    fn absent_is_zero_and_null_is_four() {
        assert_eq!(json_bytes(&None), 0);
        assert_eq!(json_bytes(&Some(serde_json::Value::Null)), 4);
    }

    #[test]
    fn counter_overflow_is_never_a_short_count() {
        let mut counter = ByteCounter {
            bytes: u64::MAX - 1,
        };
        assert!(counter.write(b"ab").is_err());

        let value = json!("abc");
        let near_limit = ByteCounter {
            bytes: u64::MAX - 2,
        };
        assert_eq!(counted_json_bytes(&value, near_limit), u64::MAX);
        let fits = ByteCounter { bytes: 10 };
        assert_eq!(counted_json_bytes(&value, fits), 10 + reference(&value));
    }

    #[test]
    fn json_bearing_command_allowance_uses_the_reference_formula() {
        let value = json!({"tokens": [1, 2, 3], "note": "héllo \"x\""});
        let output = NewJobOutput {
            kind: IntermediateOutputKind::Progress,
            summary: Some("summary".to_owned()),
            code: Some("code".to_owned()),
            goal_reached: None,
            value: Some(value.clone()),
        };
        let (reply, _answer) = flume::bounded(1);
        let command = Command::AppendJobOutput {
            job: JobId::new(1),
            output,
            reply,
        };
        let payload = byte_count("summary".len())
            .saturating_add(byte_count("code".len()))
            .saturating_add(reference(&value));
        assert_eq!(
            command.expected_write_bytes(),
            payload
                .saturating_mul(2)
                .saturating_add(TRANSACTION_ALLOWANCE_BYTES)
        );
    }
}
