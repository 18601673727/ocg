use std::path::Path;

use crate::db::protection::ProtectedArea;
use crate::host::disk::measure;

pub(crate) fn database_disk_usage(directory: &Path) -> (Option<u64>, Option<u64>) {
    let database_bytes = std::fs::metadata(crate::db::database_file(directory))
        .ok()
        .map(|metadata| metadata.len());
    let available_bytes = measure(ProtectedArea::Database, directory).ok();
    (database_bytes, available_bytes)
}

pub(crate) fn process_memory() -> (Option<u64>, Option<u64>) {
    #[cfg(unix)]
    let peak = {
        let mut rusage: libc::rusage = unsafe { std::mem::zeroed() };
        (unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut rusage) } == 0).then(|| {
            let rss = rusage.ru_maxrss.max(0) as u64;
            if cfg!(target_os = "macos") {
                rss
            } else {
                rss.saturating_mul(1024)
            }
        })
    };
    #[cfg(not(unix))]
    let peak = None;

    #[cfg(target_os = "linux")]
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        let mut current = None;
        let mut peak = peak;
        for line in status.lines() {
            let Some((name, value)) = line.split_once(':') else {
                continue;
            };
            let metric = match name {
                "VmRSS" => &mut current,
                "VmHWM" => &mut peak,
                _ => continue,
            };
            if let Some(kb) = value
                .split_whitespace()
                .next()
                .and_then(|text| text.parse::<u64>().ok())
            {
                *metric = Some(kb.saturating_mul(1024));
            }
        }
        return (current, peak);
    }
    (None, peak)
}
