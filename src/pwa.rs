//! Interactive UI launch owns a loopback control service and opens its
//! embedded product UI. It does not start an inference request.

use crate::control_server::{ControlServer, ServerConfig};
use crate::error::{OcgError, Result};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

fn open_browser(url: &str) -> bool {
    let program = if cfg!(target_os = "macos") {
        "open"
    } else {
        "xdg-open"
    };
    let graphical = cfg!(target_os = "macos")
        || std::env::var_os("DISPLAY").is_some()
        || std::env::var_os("WAYLAND_DISPLAY").is_some();
    graphical
        && Command::new(program)
            .arg(url)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .is_ok()
}

fn entry_path() -> &'static str {
    "/onboarding?scenario=local-first-run"
}

/// Bind the loopback server, serve the embedded UI, and wait for termination.
pub fn run(root: &Path, profile_path: &Path, _has_profile: bool) -> Result<()> {
    if !crate::ui_assets::is_packaged() {
        return Err(OcgError::config(format!(
            "the OCG product UI is not embedded; {}",
            crate::ui_assets::build_note()
        )));
    }
    let control = ControlServer::bind_with_profile(
        "127.0.0.1:0",
        root,
        profile_path,
        ServerConfig::default(),
    )?;
    let base_url = control.base_url();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_server = Arc::clone(&stop);
    let control_thread = std::thread::spawn(move || control.serve(stop_server));
    let page = entry_path();
    // The UI and control API share this loopback origin. Keeping the control
    // endpoint out of the address bar avoids turning a connection hint into a
    // query-string token and lets the frontend use same-origin requests.
    let url = format!("{base_url}{page}");
    println!("OCG PWA: {url}");
    if !open_browser(&url) {
        eprintln!("ocg: browser could not be opened here; visit {url}");
    }

    let (shutdown_sender, shutdown_receiver) = std::sync::mpsc::channel();
    ctrlc::set_handler(move || {
        if shutdown_sender.send(()).is_err() {
            tracing::debug!("UI shutdown receiver already closed");
        }
    })
    .map_err(|error| OcgError::config(format!("cannot handle UI termination: {error}")))?;
    shutdown_receiver
        .recv()
        .map_err(|error| OcgError::config(format!("UI shutdown signal failed: {error}")))?;
    stop.store(true, Ordering::SeqCst);
    control_thread
        .join()
        .map_err(|_| OcgError::config("control service panicked"))?
}
