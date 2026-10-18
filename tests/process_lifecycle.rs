//! The normal executable, started and stopped as real processes in sequence.
//! It checks hosting only: no credential is set and no Provider is called, so
//! it says nothing about real HTTPS acceptance.

use std::future::Future;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use ocg::data_directory::DataDirectory;
use ocg::database::{DatabaseHandle, DatabaseWorker};
use ocg::system_policy::SystemPolicy;

const WAIT: Duration = Duration::from_secs(30);

struct Unpark(std::thread::Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

fn wait<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let deadline = Instant::now() + WAIT;
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut Context::from_waker(&waker)) {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "a database call did not resolve in time"
        );
        std::thread::park_timeout(Duration::from_millis(50));
    }
}

fn open(root: &Path) -> (DatabaseWorker, DatabaseHandle) {
    let directory = DataDirectory::bootstrap(root.as_os_str().to_owned()).expect("data directory");
    let policy = SystemPolicy::load_for_startup(&directory).expect("system policy");
    let worker = DatabaseWorker::start_with_policy(&directory, policy).expect("open the database");
    let handle = worker.handle();
    (worker, handle)
}

/// Starts the executable on `root` and waits until it reports that it runs.
fn start(root: &Path) -> Child {
    let mut child = Command::new(env!("CARGO_BIN_EXE_ocg"))
        .env("OCG_DATA_DIR", root)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start the executable");
    let (lines, received) = flume::unbounded();
    let stderr = child.stderr.take().expect("stderr");
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            lines.send(line).ok();
        }
    });
    let deadline = Instant::now() + WAIT;
    let mut seen = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let Ok(line) = received.recv_timeout(remaining) else {
            child.kill().ok();
            child.wait().ok();
            panic!("the executable did not report that it runs in time; it said {seen:?}");
        };
        seen.push(line.clone());
        if line.trim() == "OCG host running" {
            break;
        }
    }
    assert!(
        child.try_wait().expect("poll the child").is_none(),
        "the process stays alive until it is told to stop"
    );
    child
}

fn stop_gracefully(mut child: Child) -> ExitStatus {
    let signalled = Command::new("kill")
        .arg("-TERM")
        .arg(child.id().to_string())
        .status()
        .expect("send SIGTERM");
    assert!(signalled.success());
    let deadline = Instant::now() + WAIT;
    loop {
        if let Some(status) = child.try_wait().expect("poll the child") {
            return status;
        }
        if Instant::now() > deadline {
            child.kill().ok();
            panic!("the executable did not exit after SIGTERM");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// A process told to stop leaves nothing running behind it, and one killed
/// outright leaves a file the next start reopens: nothing it ran needs
/// settling, and the file is checked and rebuilt as after any unclean exit.
#[test]
fn a_stopped_process_and_a_killed_one_both_leave_a_file_that_reopens() {
    let root = std::env::temp_dir().join(format!("ocg-process-lifecycle-{}", std::process::id()));
    std::fs::remove_dir_all(&root).ok();
    std::fs::create_dir_all(&root).expect("create the data directory");

    let status = stop_gracefully(start(&root));
    assert!(
        status.success(),
        "a graceful stop exits cleanly: {status:?}"
    );
    let (worker, handle) = open(&root);
    assert!(handle.abandoned_at_open().is_empty());
    let epoch = handle.epoch();
    worker.shutdown().expect("close the database");

    // SIGKILL on Unix, TerminateProcess elsewhere: nothing runs on the way out.
    let mut child = start(&root);
    child.kill().expect("kill the executable");
    child.wait().expect("reap the executable");

    let (worker, handle) = open(&root);
    assert!(
        handle.abandoned_at_open().is_empty(),
        "no Job was running, so none is abandoned"
    );
    assert!(
        handle.epoch() > epoch,
        "every start has an epoch of its own"
    );
    assert!(wait(handle.jobs(None, 10)).expect("read").is_empty());
    worker.shutdown().expect("close the database");
    std::fs::remove_dir_all(&root).ok();
}
