//! Limits and process supervision for blocking work started by tool calls.

use std::os::fd::OwnedFd;
use std::process::{Child, Command, ExitStatus};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use rustix::io::{FdFlags, fcntl_getfd, fcntl_setfd};
use rustix::pipe::pipe;
use rustix::process::{Pid, Signal, WaitId, WaitIdOptions};

const TOTAL: usize = 4;
const COMMANDS: usize = 1;
const CPU: usize = 2;
const FILES: usize = 4;
pub(crate) const STOP_GRACE: Duration = Duration::from_secs(5);
const STARTUP_GRACE: Duration = Duration::from_millis(50);
const POLL: Duration = Duration::from_millis(10);

#[derive(Clone, Copy)]
pub(crate) enum Kind {
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
    pub(crate) fn acquire(kind: Kind) -> Self {
        match Self::acquire_until_stopped(kind, None) {
            Some(permit) => permit,
            None => unreachable!("a permit without cancellation cannot be stopped"),
        }
    }

    fn acquire_until_stopped(kind: Kind, stop: Option<&StopSignal>) -> Option<Self> {
        let governor = GOVERNOR
            .get_or_init(|| Arc::new(Governor::default()))
            .clone();
        let mut counts = governor
            .counts
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        loop {
            if stop.is_some_and(StopSignal::requested) {
                return None;
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
                return Some(Self {
                    governor: governor.clone(),
                    kind,
                });
            }
            counts = governor
                .changed
                .wait(counts)
                .unwrap_or_else(PoisonError::into_inner);
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
        self.governor.changed.notify_all();
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

pub(crate) struct Process {
    child: Child,
    sentinel: OwnedFd,
    permit: Permit,
    group: Pid,
    stopped: Arc<AtomicBool>,
    started_at: Instant,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum FinishError {
    #[error("process cleanup was not proven: {0}")]
    NotProven(std::io::Error),
    #[error("process cleanup was proven, but waiting failed: {0}")]
    Wait(std::io::Error),
}

#[derive(Clone)]
pub(crate) struct StopSignal {
    stopped: Arc<AtomicBool>,
}

impl StopSignal {
    pub(crate) fn new() -> Self {
        Self {
            stopped: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn request(&self) {
        if let Some(governor) = GOVERNOR.get() {
            // The same lock as the permit wait closes the check/wait race.
            let _counts = governor
                .counts
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            self.stopped.store(true, Ordering::Release);
            governor.changed.notify_all();
        } else {
            self.stopped.store(true, Ordering::Release);
        }
    }

    pub(crate) fn requested(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }
}

impl Process {
    pub(crate) fn spawn(
        mut command: Command,
        kind: Kind,
        stop_signal: StopSignal,
    ) -> std::io::Result<Self> {
        use std::os::unix::process::CommandExt;

        command.process_group(0);
        let sentinel = Sentinel::new()?;
        sentinel.configure_child(&mut command)?;
        let permit = Permit::acquire_until_stopped(kind, Some(&stop_signal)).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "command cancelled before it started",
            )
        })?;
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
        Ok(Self {
            child,
            sentinel: sentinel.read,
            permit,
            group,
            stopped: stop_signal.stopped,
            started_at: Instant::now(),
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

    pub(crate) fn finish(
        mut self,
        stop_requested: impl Fn() -> bool,
    ) -> Result<ExitStatus, FinishError> {
        let mut term_sent = false;
        let mut deadline = None;
        let mut wait_error = None;
        loop {
            match wait_leader(self.group) {
                Ok(true) => break,
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
                Ok(true) => break,
                Ok(false) => thread::sleep(POLL),
                Err(error) => {
                    wait_error.get_or_insert(error);
                    break;
                }
            }
        }
        prove_empty(self.group, &self.sentinel).map_err(FinishError::NotProven)?;
        let status = self.child.wait().map_err(FinishError::Wait)?;
        drop(self.permit);
        match wait_error {
            Some(error) => Err(FinishError::Wait(error)),
            None => Ok(status),
        }
    }
}

/// Runs synchronous work on a dedicated worker so neither the ntex executor
/// nor its blocking pool waits for a permit or filesystem operation.
pub(crate) async fn run<T, F>(kind: Kind, work: F) -> Result<T, &'static str>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let (send, receive) = flume::bounded(1);
    thread::Builder::new()
        .name("ocg-blocking-work".to_owned())
        .spawn(move || {
            let _permit = Permit::acquire(kind);
            if send.send(work()).is_err() {
                log::debug!("blocking work result was dropped");
            }
        })
        .map_err(|_| "the blocking task could not be started")?;
    receive
        .recv_async()
        .await
        .map_err(|_| "the blocking task ended without a result")
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

#[cfg(any(target_os = "linux", test))]
fn read_process_stat(path: &std::path::Path) -> std::io::Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(stat) => Ok(Some(stat)),
        Err(error)
            if error.kind() == std::io::ErrorKind::NotFound
                || error.raw_os_error() == Some(rustix::io::Errno::SRCH.raw_os_error()) =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_process_entry_that_vanishes_before_reading_stat_is_gone() {
        let directory =
            std::env::temp_dir().join(format!("ocg-vanished-proc-{}", std::process::id()));
        let process = directory.join("123");
        std::fs::create_dir_all(&process).expect("a process entry");
        let entry = std::fs::read_dir(&directory)
            .expect("process entries")
            .next()
            .expect("one entry")
            .expect("a readable entry");
        std::fs::remove_dir(&process).expect("the process exits");
        assert_eq!(
            super::read_process_stat(&entry.path().join("stat")).expect("a gone process"),
            None
        );
        std::fs::remove_dir(directory).expect("remove the fixture");
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
