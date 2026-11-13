//! Limits and process supervision for blocking work started by tool calls.

use std::collections::BTreeSet;
use std::os::fd::OwnedFd;
use std::os::unix::process::ExitStatusExt;
use std::process::{Child, Command, ExitStatus};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::task::{Poll, Waker};
use std::thread;
use std::time::{Duration, Instant};

use rustix::io::{FdFlags, fcntl_getfd, fcntl_setfd};
use rustix::pipe::pipe;
use rustix::process::{Pid, Signal, WaitId, WaitIdOptions};

use crate::db::entities::{JobId, ToolCallRef};

const TOTAL: usize = 4;
const COMMANDS: usize = 1;
const COMMAND_WAIT: Duration = Duration::from_secs(10 * 60);
const CPU: usize = 2;
const FILES: usize = 4;
const MAX_WAITERS: usize = 64;
pub(crate) const STOP_GRACE: Duration = Duration::from_secs(5);
const STARTUP_GRACE: Duration = Duration::from_millis(50);
const POLL: Duration = Duration::from_millis(10);

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
    async fn acquire_async(kind: Kind, stop: Option<&StopSignal>) -> Result<Self, &'static str> {
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
                    return Poll::Ready(Err("the blocking work queue is full"));
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

    fn acquire_until_stopped(
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

pub(crate) struct Sentinel {
    read: OwnedFd,
    write: OwnedFd,
}

impl Sentinel {
    pub(crate) fn new() -> std::io::Result<Self> {
        let (read, write) = pipe()?;
        fcntl_setfd(&read, fcntl_getfd(&read)? | FdFlags::CLOEXEC)?;
        fcntl_setfd(&write, fcntl_getfd(&write)? | FdFlags::CLOEXEC)?;
        rustix::fs::fcntl_setfl(&read, rustix::fs::OFlags::NONBLOCK)?;
        Ok(Self { read, write })
    }

    pub(crate) fn configure_child(&self, command: &mut Command) -> std::io::Result<()> {
        use std::os::fd::AsRawFd;
        use std::os::unix::process::CommandExt;
        let fd = self.write.as_raw_fd();
        let flags = fcntl_getfd(&self.write).map_err(std::io::Error::from)?;
        unsafe {
            command.pre_exec(move || {
                let flags = flags & !FdFlags::CLOEXEC;
                let descriptor = std::os::fd::BorrowedFd::borrow_raw(fd);
                fcntl_setfd(descriptor, flags).map_err(std::io::Error::from)
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChildUsage {
    pub(crate) user: Duration,
    pub(crate) system: Duration,
    pub(crate) peak_rss_bytes: u64,
}

#[derive(Debug)]
pub(crate) struct Finished {
    pub(crate) status: ExitStatus,
    pub(crate) wall: Duration,
    pub(crate) usage: Option<ChildUsage>,
}

pub(crate) struct Process {
    child: Child,
    sentinel: OwnedFd,
    permit: Permit,
    group: Pid,
    stopped: Arc<AtomicBool>,
    started_at: Instant,
    job: Option<JobId>,
    running: Option<RunningGroup>,
}

/// Where a spawning call stands. The entry exists from before its admission
/// can commit until its Job's stop is confirmed or no process can have
/// started; it is the single record of the call's process ownership.
#[derive(Clone, Copy, PartialEq, Eq)]
enum CallState {
    /// A supervisor may still start the call's process.
    Starting,
    /// The call's process group may be alive.
    Running(Pid),
    /// The group was proven empty, or no process can have started although
    /// the call's admission may be committed; the proof stays until the
    /// Job's stop or outcome is durably settled, see [`stop_settled`].
    Cleaned,
}

static CALLS: Mutex<Vec<(ToolCallRef, CallState)>> = Mutex::new(Vec::new());

fn set_call(call: ToolCallRef, from: impl Fn(CallState) -> bool, to: Option<CallState>) {
    let mut calls = CALLS.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(index) = calls
        .iter()
        .position(|(key, state)| *key == call && from(*state))
    {
        match to {
            Some(state) => calls[index].1 = state,
            None => {
                calls.remove(index);
            }
        }
    }
}

/// The obligation to supervise a process that a spawning call may still
/// start. Dropping it means no process can start, unless a panic is
/// unwinding: then the entry stays and the Job keeps answering as tracked.
/// What the entry becomes depends on the call's admission: one that may have
/// committed leaves the proof that nothing started, see
/// [`Self::admission_pending`]; one that did not leaves nothing.
pub(crate) struct StartingCall {
    call: ToolCallRef,
    armed: bool,
    /// Whether the call's admission may be committed.
    admitted: bool,
}

/// Registers `call` as starting. Done before the call's admission is
/// recorded, so no interval in which the admission may be committed is
/// untracked.
pub(crate) fn begin_call(call: ToolCallRef) -> StartingCall {
    CALLS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push((call, CallState::Starting));
    StartingCall {
        call,
        armed: true,
        admitted: false,
    }
}

impl StartingCall {
    pub(crate) fn job(&self) -> JobId {
        self.call.job
    }

    /// The call's admission is about to be sent, and from here on may be
    /// committed, whatever becomes of the request or of the task that made
    /// it. Without a process, the entry then becomes the settled proof that
    /// nothing started, because a committed admission without an outcome can
    /// quarantine the Job.
    pub(crate) fn admission_pending(&mut self) {
        self.admitted = true;
    }

    /// The admission was refused by the database: nothing was committed, so
    /// no proof is owed if no process starts.
    pub(crate) fn admission_refused(&mut self) {
        self.admitted = false;
    }

    /// The process exists: the same lock acquisition that ends the starting
    /// state begins the running one.
    fn running(mut self, group: Pid) -> RunningGroup {
        self.armed = false;
        set_call(
            self.call,
            |state| state == CallState::Starting,
            Some(CallState::Running(group)),
        );
        RunningGroup {
            call: self.call,
            proven: false,
        }
    }
}

impl Drop for StartingCall {
    fn drop(&mut self) {
        if self.armed && !thread::panicking() {
            // No process can start from here. When the admission may have
            // committed, that is a proof to keep until the Job's stop or
            // outcome is settled; otherwise the call left no trace.
            let to = self.admitted.then_some(CallState::Cleaned);
            set_call(self.call, |state| state == CallState::Starting, to);
        }
    }
}

struct RunningGroup {
    call: ToolCallRef,
    proven: bool,
}

impl RunningGroup {
    /// The group was proven empty. The proof is kept, not forgotten, until
    /// the Job's stop is settled.
    fn proven(mut self) {
        self.proven = true;
        set_call(
            self.call,
            |state| matches!(state, CallState::Running(_)),
            Some(CallState::Cleaned),
        );
    }
}

impl Drop for RunningGroup {
    fn drop(&mut self) {
        // A panic inside Process::finish unwinds through the process group it
        // was supervising: its processes may still run, so the entry stays and
        // the job keeps answering slot_live. The operator restarts the host
        // rather than freeing a slot whose group may still be alive.
        if self.proven || thread::panicking() {
            return;
        }
        set_call(
            self.call,
            |state| matches!(state, CallState::Running(_)),
            None,
        );
    }
}

/// What keeps an unproven group's sentinel, leader and command permit alive.
struct GroupHandles {
    sentinel: OwnedFd,
    leader: Child,
    /// Held until the group is proven empty, so no new command runs beside it.
    _permit: Permit,
}

/// A process group that could not be proven empty. Its record stays in
/// [`UNPROVEN_GROUPS`] until a probe proves the group, so its Job stays
/// unresolved; the handles leave the record while a probe checks the group.
struct UnprovenGroup {
    id: u64,
    job: Option<JobId>,
    group: Pid,
    /// `None` while a [`Probe`] holds them, which is what probing means.
    handles: Option<GroupHandles>,
}

static UNPROVEN_GROUPS: Mutex<Vec<UnprovenGroup>> = Mutex::new(Vec::new());

/// Gives each record an identity of its own. A group's pid is reused once a
/// probe reaps its leader, so the pid cannot name the record.
static NEXT_UNPROVEN_ID: AtomicU64 = AtomicU64::new(0);

fn record_unproven_group(job: Option<JobId>, group: Pid, handles: GroupHandles) {
    UNPROVEN_GROUPS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push(UnprovenGroup {
            id: NEXT_UNPROVEN_ID.fetch_add(1, Ordering::Relaxed),
            job,
            group,
            handles: Some(handles),
        });
}

/// Jobs whose every group was proven empty but whose stop is not yet
/// committed. Kept until the commit, or a restart would leave the slot
/// quarantined with nothing left to prove it free.
static UNCONFIRMED_STOPS: Mutex<BTreeSet<JobId>> = Mutex::new(BTreeSet::new());

/// Whether this process still tracks a group that may be alive, or a proven
/// stop not yet committed. None of them survives a restart.
pub(crate) fn groups_tracked() -> bool {
    !CALLS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .is_empty()
        || !UNPROVEN_GROUPS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
        || !UNCONFIRMED_STOPS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_empty()
}

/// The stop of `job` is committed, or can no longer be: the retained proof
/// of its cleaned calls is released. Calls still starting or running stay.
pub(crate) fn stop_settled(job: JobId) {
    CALLS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .retain(|(call, state)| call.job != job || *state != CallState::Cleaned);
    UNCONFIRMED_STOPS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(&job);
}

/// An unproven group that a prober is checking. Its handles are out of the
/// record for the check, so no lock is held while the group is signalled and
/// polled, and its permit stays with the group. Dropped unsettled, as when a
/// prober panics, the handles return to the record.
struct Probe {
    id: u64,
    job: Option<JobId>,
    group: Pid,
    handles: Option<GroupHandles>,
}

/// Takes the handles of each record that `wanted` selects and no prober
/// holds, and marks those records as probing.
fn take_probes(wanted: impl Fn(&UnprovenGroup) -> bool) -> Vec<Probe> {
    let mut groups = UNPROVEN_GROUPS
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let mut probes = Vec::new();
    for entry in groups.iter_mut() {
        if !wanted(entry) {
            continue;
        }
        if let Some(handles) = entry.handles.take() {
            probes.push(Probe {
                id: entry.id,
                job: entry.job,
                group: entry.group,
                handles: Some(handles),
            });
        }
    }
    probes
}

impl Probe {
    /// Signals the group and waits up to `timeout` for it to empty, then reaps
    /// its leader. Called with no lock held.
    fn prove(&mut self, timeout: Duration) -> bool {
        let Some(handles) = self.handles.as_mut() else {
            return false;
        };
        signal(self.group, Signal::KILL);
        if prove_empty_timeout(self.group, &handles.sentinel, timeout).is_err() {
            return false;
        }
        if let Err(error) = handles.leader.wait() {
            log::warn!("failed to reap unproven process leader: {error}");
        }
        true
    }

    /// The group is proven empty: its record leaves, and its Job becomes an
    /// unconfirmed stop if no other group of the Job remains. The stop is
    /// inserted first, in the same critical section, so the Job never goes
    /// untracked.
    fn proven(mut self) {
        let handles = self.handles.take();
        let mut groups = UNPROVEN_GROUPS
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(job) = self.job
            && !groups
                .iter()
                .any(|entry| entry.id != self.id && entry.job == Some(job))
        {
            UNCONFIRMED_STOPS
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(job);
        }
        groups.retain(|entry| entry.id != self.id);
        drop(groups);
        drop(handles);
    }
}

impl Drop for Probe {
    fn drop(&mut self) {
        // Not proven, or unwinding: the handles go back, so the permit stays
        // with a group that may still be alive.
        let Some(handles) = self.handles.take() else {
            return;
        };
        let mut groups = UNPROVEN_GROUPS
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(entry) = groups.iter_mut().find(|entry| entry.id == self.id) {
            entry.handles = Some(handles);
        }
    }
}

/// Kills every unproven group again; a Job whose groups are all proven
/// empty joins the unconfirmed stops before its last group is forgotten.
pub(crate) fn retry_unproven_groups() {
    for mut probe in take_probes(|_| true) {
        if probe.prove(Duration::from_millis(50)) {
            probe.proven();
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum FinishError {
    #[error("process cleanup was not proven: {0}")]
    NotProven(std::io::Error),
    #[error("process cleanup was proven, but waiting failed: {0}")]
    Wait(std::io::Error),
}

type CancelQueued = Box<dyn FnOnce() + Send>;

#[derive(Clone)]
pub(crate) struct StopSignal {
    stopped: Arc<AtomicBool>,
    queued: Arc<Mutex<Option<CancelQueued>>>,
}

impl StopSignal {
    pub(crate) fn new() -> Self {
        Self {
            stopped: Arc::new(AtomicBool::new(false)),
            queued: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn request(&self) {
        let governor = GOVERNOR.get_or_init(|| Arc::new(Governor::default()));
        let counts = governor
            .counts
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        self.stopped.store(true, Ordering::Release);
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
        let cancel = self
            .queued
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(cancel) = cancel {
            cancel();
        }
    }

    fn cancel_queued(&self, cancel: impl FnOnce() + Send + 'static) {
        let mut queued = self.queued.lock().unwrap_or_else(PoisonError::into_inner);
        if self.requested() {
            drop(queued);
            cancel();
        } else {
            *queued = Some(Box::new(cancel));
        }
    }

    pub(crate) fn requested(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }
}

// Cancellation and worker startup compete to take the payload. Once the
// worker takes it, its resources remain held until the work actually ends.
struct Queued<T>(Arc<Mutex<Option<T>>>);

impl<T> Queued<T> {
    fn new(value: T) -> Self {
        Self(Arc::new(Mutex::new(Some(value))))
    }

    fn handle(&self) -> Arc<Mutex<Option<T>>> {
        self.0.clone()
    }
}

impl<T> Drop for Queued<T> {
    fn drop(&mut self) {
        let value = self.0.lock().unwrap_or_else(PoisonError::into_inner).take();
        drop(value);
    }
}

impl Process {
    pub(crate) fn spawn(
        mut command: Command,
        kind: Kind,
        stop_signal: StopSignal,
        starting: Option<StartingCall>,
    ) -> std::io::Result<Self> {
        use std::os::unix::process::CommandExt;

        command.process_group(0);
        let sentinel = Sentinel::new()?;
        sentinel.configure_child(&mut command)?;
        let deadline = matches!(kind, Kind::Command).then(|| Instant::now() + COMMAND_WAIT);
        let permit = Permit::acquire_until_stopped(kind, Some(&stop_signal), deadline)?;
        if stop_signal.requested() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "command cancelled before it started",
            ));
        }
        let child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                drop(permit);
                return Err(error);
            }
        };
        let group = Pid::from_child(&child);
        drop(sentinel.write);
        let job = starting.as_ref().map(StartingCall::job);
        let running = starting.map(|starting| starting.running(group));
        Ok(Self {
            child,
            sentinel: sentinel.read,
            permit,
            group,
            stopped: stop_signal.stopped,
            started_at: Instant::now(),
            job,
            running,
        })
    }

    pub(crate) fn take_output(
        &mut self,
    ) -> (
        Option<std::process::ChildStdout>,
        Option<std::process::ChildStderr>,
    ) {
        (self.child.stdout.take(), self.child.stderr.take())
    }

    pub(crate) fn finish(self, stop_requested: impl Fn() -> bool) -> Result<Finished, FinishError> {
        let mut term_sent = false;
        let mut deadline = None;
        let mut wait_error = None;
        let mut wall = None;
        loop {
            match wait_leader(self.group) {
                Ok(true) => {
                    wall.get_or_insert_with(|| self.started_at.elapsed());
                    break;
                }
                Ok(false) => {}
                Err(error) => {
                    wait_error = Some(error);
                    break;
                }
            }
            if (stop_requested() || self.stopped.load(Ordering::Acquire)) && !term_sent {
                if let Some(remaining) = STARTUP_GRACE.checked_sub(self.started_at.elapsed()) {
                    thread::sleep(remaining);
                    continue;
                }
                signal(self.group, Signal::TERM);
                term_sent = true;
                deadline = Some(Instant::now() + STOP_GRACE);
            }
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                break;
            }
            thread::sleep(POLL);
        }
        signal(self.group, Signal::KILL);
        loop {
            match wait_leader(self.group) {
                Ok(true) => {
                    wall.get_or_insert_with(|| self.started_at.elapsed());
                    break;
                }
                Ok(false) => thread::sleep(POLL),
                Err(error) => {
                    wait_error.get_or_insert(error);
                    break;
                }
            }
        }
        if let Err(error) = prove_empty(self.group, &self.sentinel) {
            record_unproven_group(
                self.job,
                self.group,
                GroupHandles {
                    sentinel: self.sentinel,
                    leader: self.child,
                    _permit: self.permit,
                },
            );
            drop(self.running);
            return Err(FinishError::NotProven(error));
        }
        // The group is empty, yet the proof is only useful once kept until
        // the Job's outcome or stop is durable.
        if let Some(running) = self.running {
            running.proven();
        }
        let mut raw_status: libc::c_int = 0;
        let mut rusage: libc::rusage = unsafe { std::mem::zeroed() };
        let pid = self.child.id() as libc::pid_t;
        loop {
            let res = unsafe { libc::wait4(pid, &mut raw_status, 0, &mut rusage) };
            if res == -1 {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() == Some(libc::EINTR) {
                    continue;
                }
                drop(self.permit);
                return Err(FinishError::Wait(err));
            }
            break;
        }
        drop(self.permit);
        if let Some(error) = wait_error {
            return Err(FinishError::Wait(error));
        }
        let status = ExitStatus::from_raw(raw_status);
        let wall = wall.unwrap_or_else(|| self.started_at.elapsed());
        #[cfg(target_os = "macos")]
        let peak_rss_bytes = rusage.ru_maxrss.max(0) as u64;
        #[cfg(not(target_os = "macos"))]
        let peak_rss_bytes = (rusage.ru_maxrss.max(0) as u64).saturating_mul(1024);

        let user = Duration::from_secs(rusage.ru_utime.tv_sec.max(0) as u64)
            + Duration::from_micros(rusage.ru_utime.tv_usec.max(0) as u64);
        let system = Duration::from_secs(rusage.ru_stime.tv_sec.max(0) as u64)
            + Duration::from_micros(rusage.ru_stime.tv_usec.max(0) as u64);

        let usage = Some(ChildUsage {
            user,
            system,
            peak_rss_bytes,
        });
        Ok(Finished {
            status,
            wall,
            usage,
        })
    }
}

/// Waits asynchronously for a bounded permit, then runs synchronous work on
/// the ntex blocking pool. A full waiting queue or a failed task returns an error.
pub async fn run<T, F>(kind: Kind, work: F) -> Result<T, &'static str>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let permit = Permit::acquire_async(kind, None).await?;
    let queued = Queued::new((permit, work));
    let pending = queued.handle();
    ntex::rt::spawn_blocking(move || {
        let (permit, work) = pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()?;
        let _permit = permit;
        Some(work())
    })
    .await
    .map_err(|_| "the blocking task ended without a result")?
    .ok_or("the blocking task was discarded before it started")
}

/// A Job's tracked work: its stop signal and a channel that disconnects
/// when the work ends or is discarded before it starts.
type TrackedWorker = (JobId, StopSignal, flume::Receiver<()>);

#[derive(Default)]
pub(crate) struct WorkerTracker {
    workers: Mutex<Vec<TrackedWorker>>,
}

impl WorkerTracker {
    /// Like [`run`], for work that mutates: waiting for a permit stops when
    /// `stop` is requested, and `work` checks that signal before changing
    /// anything. [`Self::stop_job`] waits for tracked work to finish.
    pub(crate) async fn run_tracked<T, F>(
        &self,
        job: Option<JobId>,
        kind: Kind,
        stop: StopSignal,
        work: F,
    ) -> Result<T, &'static str>
    where
        T: Send + 'static,
        F: FnOnce(&StopSignal) -> T + Send + 'static,
    {
        let (alive, ended) = flume::bounded::<()>(1);
        {
            let mut tracked = self.workers.lock().unwrap_or_else(PoisonError::into_inner);
            tracked.retain(|(_, _, ended)| !ended.is_disconnected());
            if let Some(job) = job {
                tracked.push((job, stop.clone(), ended));
            }
        }
        let permit = Permit::acquire_async(kind, Some(&stop)).await?;
        let queued = Queued::new((permit, alive, work));
        let cancel = queued.handle();
        stop.cancel_queued(move || {
            let value = cancel.lock().unwrap_or_else(PoisonError::into_inner).take();
            drop(value);
        });
        let pending = queued.handle();
        ntex::rt::spawn_blocking(move || {
            let (permit, alive, work) = pending
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()?;
            let _alive = alive;
            let _permit = permit;
            Some(work(&stop))
        })
        .await
        .map_err(|_| "the blocking task ended without a result")?
        .ok_or("the blocking task was discarded before it started")
    }

    /// Requests a stop of the tracked work of `job` and waits until it has
    /// ended or been discarded before starting.
    pub(crate) async fn stop_job(&self, job: JobId) {
        let workers = {
            let mut tracked = self.workers.lock().unwrap_or_else(PoisonError::into_inner);
            tracked.retain(|(_, _, ended)| !ended.is_disconnected());
            tracked
                .iter()
                .filter(|(owner, _, _)| *owner == job)
                .map(|(_, stop, ended)| (stop.clone(), ended.clone()))
                .collect::<Vec<_>>()
        };
        for (stop, _) in &workers {
            stop.request();
        }
        for (_, ended) in workers {
            let _ = ended.recv_async().await;
        }
    }
}

fn signal(group: Pid, signal: Signal) {
    if let Err(error) = rustix::process::kill_process_group(group, signal)
        && error != rustix::io::Errno::SRCH
    {
        log::error!("process group signal failed: {error}");
    }
}

fn wait_leader(pid: Pid) -> std::io::Result<bool> {
    loop {
        match rustix::process::waitid(
            WaitId::Pid(pid),
            WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT,
        ) {
            Ok(Some(_)) => return Ok(true),
            Ok(None) => return Ok(false),
            Err(rustix::io::Errno::INTR) => continue,
            Err(error) => return Err(std::io::Error::from(error)),
        }
    }
}

fn prove_empty(pid: Pid, sentinel: &OwnedFd) -> std::io::Result<()> {
    prove_empty_timeout(pid, sentinel, STOP_GRACE)
}

fn prove_empty_timeout(pid: Pid, sentinel: &OwnedFd, timeout: Duration) -> std::io::Result<()> {
    let deadline = Instant::now() + timeout;
    use std::io::Read;
    let mut pipe = std::fs::File::from(sentinel.try_clone()?);
    loop {
        let eof = {
            let mut byte = [0u8; 1];
            match pipe.read(&mut byte) {
                Ok(0) => true,
                Ok(_) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "process sentinel contained unexpected data",
                    ));
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => false,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => false,
                Err(error) => return Err(error),
            }
        };
        if eof && !live_group_members(pid)? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "process group did not empty in time",
            ));
        }
        thread::sleep(POLL);
    }
}

#[cfg(target_os = "linux")]
fn live_group_members(group: Pid) -> std::io::Result<bool> {
    for entry in std::fs::read_dir("/proc")? {
        let entry = entry?;
        let Some(_name) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        let Some(stat) = read_process_stat(&entry.path().join("stat"))? else {
            continue;
        };
        let Some((_, fields)) = stat.rsplit_once(") ") else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "a process stat entry was malformed",
            ));
        };
        let fields = fields.split_whitespace().collect::<Vec<_>>();
        if fields.len() > 2
            && fields[0] != "Z"
            && fields[2].parse::<i32>().ok() == Some(group.as_raw_pid())
        {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(target_os = "linux")]
fn read_process_stat(path: &std::path::Path) -> std::io::Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(stat) => Ok(Some(stat)),
        Err(error)
            if error.kind() == std::io::ErrorKind::NotFound
                || error.kind() == std::io::ErrorKind::PermissionDenied
                || error.raw_os_error() == Some(rustix::io::Errno::SRCH.raw_os_error()) =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

#[cfg(target_os = "macos")]
fn live_group_members(group: Pid) -> std::io::Result<bool> {
    // `PROC_ALL_PIDS` is not exposed by every libc crate release; it is the
    // documented value for the `proc_listpids` type argument.
    const PROC_ALL_PIDS: u32 = 1;
    let count = unsafe { libc::proc_listpids(PROC_ALL_PIDS, 0, std::ptr::null_mut(), 0) };
    if count <= 0 {
        return Err(std::io::Error::last_os_error());
    }
    let mut pids = vec![0i32; count as usize];
    let bytes = unsafe {
        libc::proc_listpids(
            PROC_ALL_PIDS,
            0,
            pids.as_mut_ptr().cast(),
            (pids.len() * std::mem::size_of::<i32>()) as i32,
        )
    };
    if bytes < 0 {
        return Err(std::io::Error::last_os_error());
    }
    for pid in pids
        .into_iter()
        .take(bytes as usize / std::mem::size_of::<i32>())
    {
        if pid <= 0 {
            continue;
        }
        let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
        let read = unsafe {
            libc::proc_pidinfo(
                pid,
                libc::PROC_PIDTBSDINFO,
                0,
                (&mut info as *mut libc::proc_bsdinfo).cast(),
                std::mem::size_of::<libc::proc_bsdinfo>() as i32,
            )
        };
        if read as usize == std::mem::size_of::<libc::proc_bsdinfo>()
            && info.pbi_pgid as i32 == group.as_raw_pid()
            && info.pbi_status != libc::SZOMB
        {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn live_group_members(_: Pid) -> std::io::Result<bool> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "process-group membership scanning is unsupported on this platform",
    ))
}
