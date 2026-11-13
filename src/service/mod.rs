pub mod ai;
// Restored for the native tool loop; nothing registers them until it exists.
#[cfg(unix)]
#[allow(dead_code)]
pub mod fts;
pub(crate) mod jobs;
#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
#[allow(dead_code)]
pub mod lsp;
