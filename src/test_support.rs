//! Helpers shared by unit-test modules. Every wait is bounded by [`WAIT`].

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use crate::data_directory::DataDirectory;

/// Upper bound for any single wait in a test.
pub(crate) const WAIT: Duration = Duration::from_secs(10);

struct Unpark(std::thread::Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// Polls a future once on this thread. Returns `None` while it is pending.
pub(crate) fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Option<F::Output> {
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    match future.poll(&mut Context::from_waker(&waker)) {
        Poll::Ready(value) => Some(value),
        Poll::Pending => None,
    }
}

/// Drives a future on this thread, failing the test if it does not resolve in time.
pub(crate) fn wait<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let deadline = Instant::now() + WAIT;
    loop {
        if let Some(value) = poll_once(future.as_mut()) {
            return value;
        }
        let now = Instant::now();
        assert!(now < deadline, "a caller did not resolve within {WAIT:?}");
        std::thread::park_timeout(deadline - now);
    }
}

/// Removes its directory when dropped.
pub(crate) struct TestRoot(PathBuf);

impl Drop for TestRoot {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).ok();
    }
}

/// Bootstraps a fresh data directory under the system temp dir. `suite`
/// namespaces the directory so tests in different modules never collide.
pub(crate) fn data_directory(suite: &str, name: &str) -> (TestRoot, DataDirectory) {
    let root = std::env::temp_dir().join(format!("{suite}-{}-{name}", std::process::id()));
    std::fs::remove_dir_all(&root).ok();
    let directory =
        DataDirectory::bootstrap(root.clone().into_os_string()).expect("bootstrap data directory");
    (TestRoot(root), directory)
}

/// Creates a fresh, empty directory under the system temp dir and returns its
/// resolved path. `suite` namespaces it as in [`data_directory`].
pub(crate) fn temporary_directory(suite: &str, name: &str) -> (TestRoot, PathBuf) {
    let root = std::env::temp_dir().join(format!("{suite}-{}-{name}", std::process::id()));
    std::fs::remove_dir_all(&root).ok();
    std::fs::create_dir_all(&root).expect("create a temporary directory");
    let resolved = root.canonicalize().expect("resolve a temporary directory");
    (TestRoot(root), resolved)
}

pub(crate) fn on_system<Make, Work, Output>(make: Make) -> Output
where
    Make: FnOnce() -> Work + Send + 'static,
    Work: Future<Output = Output> + 'static,
    Output: Send + 'static,
{
    static SYSTEM: std::sync::OnceLock<ntex::rt::System> = std::sync::OnceLock::new();
    let system = SYSTEM.get_or_init(|| {
        let (ready, started) = flume::bounded(1);
        std::thread::spawn(move || {
            let runner = ntex::rt::System::new("test-components", ntex::rt::DefaultRuntime);
            runner
                .run(move || {
                    ready.send(ntex::rt::System::current()).expect("test host");
                    Ok(())
                })
                .expect("test System");
        });
        started.recv().expect("test System started")
    });
    let (done, finished) = flume::bounded(1);
    system
        .arbiter()
        .handle()
        .spawn(async move {
            ntex::rt::spawn(async move {
                let output = make().await;
                if done.send(output).is_err() {
                    panic!("test stopped waiting for its System task");
                }
            });
        })
        .detach();
    wait(finished.recv_async()).expect("System task completed")
}
