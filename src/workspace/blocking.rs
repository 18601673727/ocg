use std::sync::{
    Arc, Condvar, Mutex, PoisonError,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Poll, Waker};
use std::time::Instant;

use crate::workspace::process::StopSignal;

const TOTAL: usize = 4;
const COMMANDS: usize = 1;
const CPU: usize = 2;
const FILES: usize = 4;
const MAX_WAITERS: usize = 64;
/// The queue was full before any work ran. The call can be retried.
pub(crate) const QUEUE_FULL: &str = "the blocking work queue is full";

#[derive(Clone, Copy)]
pub enum Kind {
    Command,
    Cpu,
    File,
}

#[derive(Default)]
struct Counts {
    total: usize,
    commands: usize,
    cpu: usize,
    files: usize,
    waiters: Vec<(u64, Waker)>,
    next_waiter: u64,
}

#[derive(Default)]
struct Governor {
    counts: Mutex<Counts>,
    changed: Condvar,
}

static GOVERNOR: std::sync::OnceLock<Arc<Governor>> = std::sync::OnceLock::new();

pub(crate) struct Permit {
    governor: Arc<Governor>,
    kind: Kind,
}

impl Permit {
    pub(crate) async fn acquire_async(
        kind: Kind,
        stop: Option<&StopSignal>,
    ) -> Result<Self, &'static str> {
        struct Registration {
            governor: Arc<Governor>,
            id: Option<u64>,
        }
        impl Drop for Registration {
            fn drop(&mut self) {
                if let Some(id) = self.id {
                    let mut counts = self
                        .governor
                        .counts
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner);
                    counts.waiters.retain(|(key, _)| *key != id);
                }
            }
        }
        let governor = GOVERNOR
            .get_or_init(|| Arc::new(Governor::default()))
            .clone();
        let mut registration = Registration {
            governor: governor.clone(),
            id: None,
        };
        std::future::poll_fn(|context| {
            let mut counts = governor
                .counts
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if stop.is_some_and(StopSignal::requested) {
                return Poll::Ready(Err("the blocking task was stopped before it started"));
            }
            let category = match kind {
                Kind::Command => counts.commands < COMMANDS,
                Kind::Cpu => counts.cpu < CPU,
                Kind::File => counts.files < FILES,
            };
            if counts.total < TOTAL && category {
                counts.total += 1;
                match kind {
                    Kind::Command => counts.commands += 1,
                    Kind::Cpu => counts.cpu += 1,
                    Kind::File => counts.files += 1,
                }
                return Poll::Ready(Ok(Self {
                    governor: governor.clone(),
                    kind,
                }));
            }
            if let Some(id) = registration.id {
                if let Some((_, waker)) = counts.waiters.iter_mut().find(|(key, _)| *key == id)
                    && !waker.will_wake(context.waker())
                {
                    *waker = context.waker().clone();
                }
            } else {
                if counts.waiters.len() >= MAX_WAITERS {
                    return Poll::Ready(Err(QUEUE_FULL));
                }
                let Some(next) = counts.next_waiter.checked_add(1) else {
                    return Poll::Ready(Err("the blocking work queue cannot register a waiter"));
                };
                counts.next_waiter = next;
                counts.waiters.push((next, context.waker().clone()));
                registration.id = Some(next);
            }
            Poll::Pending
        })
        .await
    }

    pub(crate) fn acquire_until_stopped(
        kind: Kind,
        stop: Option<&StopSignal>,
        deadline: Option<Instant>,
    ) -> std::io::Result<Self> {
        let governor = GOVERNOR
            .get_or_init(|| Arc::new(Governor::default()))
            .clone();
        Self::acquire_from(governor, kind, stop, deadline)
    }

    fn acquire_from(
        governor: Arc<Governor>,
        kind: Kind,
        stop: Option<&StopSignal>,
        deadline: Option<Instant>,
    ) -> std::io::Result<Self> {
        let mut counts = governor
            .counts
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        loop {
            if stop.is_some_and(StopSignal::requested) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "command cancelled before it started",
                ));
            }
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                let message = if counts.commands >= COMMANDS {
                    "another command is still running"
                } else {
                    "too many tool operations are running"
                };
                return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, message));
            }
            let category = match kind {
                Kind::Command => counts.commands < COMMANDS,
                Kind::Cpu => counts.cpu < CPU,
                Kind::File => counts.files < FILES,
            };
            if counts.total < TOTAL && category {
                counts.total += 1;
                match kind {
                    Kind::Command => counts.commands += 1,
                    Kind::Cpu => counts.cpu += 1,
                    Kind::File => counts.files += 1,
                }
                drop(counts);
                return Ok(Self {
                    governor: governor.clone(),
                    kind,
                });
            }
            counts = if let Some(deadline) = deadline {
                governor
                    .changed
                    .wait_timeout(counts, deadline.saturating_duration_since(Instant::now()))
                    .unwrap_or_else(PoisonError::into_inner)
                    .0
            } else {
                governor
                    .changed
                    .wait(counts)
                    .unwrap_or_else(PoisonError::into_inner)
            };
        }
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut counts = self
            .governor
            .counts
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        counts.total -= 1;
        match self.kind {
            Kind::Command => counts.commands -= 1,
            Kind::Cpu => counts.cpu -= 1,
            Kind::File => counts.files -= 1,
        }
        let waiters = counts
            .waiters
            .iter()
            .map(|(_, waker)| waker.clone())
            .collect::<Vec<_>>();
        drop(counts);
        self.governor.changed.notify_all();
        for waker in waiters {
            waker.wake();
        }
    }
}

pub(crate) fn signal_stop(stopped: &AtomicBool) {
    let governor = GOVERNOR.get_or_init(|| Arc::new(Governor::default()));
    let counts = governor
        .counts
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    stopped.store(true, Ordering::Release);
    let waiters = counts
        .waiters
        .iter()
        .map(|(_, waker)| waker.clone())
        .collect::<Vec<_>>();
    drop(counts);
    governor.changed.notify_all();
    for waker in waiters {
        waker.wake();
    }
}

// Cancellation and worker startup race to take the queued work and its permit.
pub(crate) struct Queued<T>(Arc<Mutex<Option<T>>>);

impl<T> Queued<T> {
    pub(crate) fn new(value: T) -> Self {
        Self(Arc::new(Mutex::new(Some(value))))
    }

    pub(crate) fn handle(&self) -> Arc<Mutex<Option<T>>> {
        self.0.clone()
    }
}

impl<T> Drop for Queued<T> {
    fn drop(&mut self) {
        let value = self.0.lock().unwrap_or_else(PoisonError::into_inner).take();
        drop(value);
    }
}

pub async fn run<T, F>(kind: Kind, work: F) -> Result<T, &'static str>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let permit = Permit::acquire_async(kind, None).await?;
    run_permitted(permit, work).await
}

pub(crate) async fn run_permitted<T, F>(permit: Permit, work: F) -> Result<T, &'static str>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let (_permit, result) = run_permitted_retained(permit, work).await?;
    Ok(result)
}

pub(crate) async fn run_permitted_retained<T, F>(
    permit: Permit,
    work: F,
) -> Result<(Permit, T), &'static str>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let queued = Queued::new((permit, work));
    let pending = queued.handle();
    ntex::rt::spawn_blocking(move || {
        let (permit, work) = pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()?;
        let result = work();
        Some((permit, result))
    })
    .await
    .map_err(|_| "the blocking task ended without a result")?
    .ok_or("the blocking task was discarded before it started")
}
