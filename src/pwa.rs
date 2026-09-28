//! Interactive UI launch owns a loopback control service and opens its
//! embedded product UI. It does not start an inference request.

use crate::control_server::{ControlServer, ServerConfig};
use crate::error::{OcgError, Result};
use anyhow::Context;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

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

fn entry_path(has_profile: bool) -> &'static str {
    if has_profile {
        "/?scenario=local-ready"
    } else {
        "/onboarding?scenario=local-first-run"
    }
}

fn build_signal_runtime() -> anyhow::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("cannot initialize UI signal handling")
}

/// Bind the loopback server, serve the embedded UI, and wait for termination.
pub fn run(root: &Path, profile_path: &Path, has_profile: bool) -> Result<()> {
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
    let shutdown = CancellationToken::new();

    let page = entry_path(has_profile);
    // The UI and control API share this loopback origin. Keeping the control
    // endpoint out of the address bar avoids turning a connection hint into a
    // query-string token and lets the frontend use same-origin requests.
    let url = format!("{base_url}{page}");
    println!("OCG PWA: {url}");
    if !open_browser(&url) {
        eprintln!("ocg: browser could not be opened here; visit {url}");
    }

    let runtime = build_signal_runtime().map_err(|error| OcgError::config(error.to_string()))?;
    let shutdown_signal = shutdown.clone();
    let wait = runtime.block_on(async {
        #[cfg(unix)]
        {
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .map_err(|error| OcgError::io("cannot handle UI termination", error))?;
            tokio::select! {
                _ = tokio::signal::ctrl_c() => shutdown_signal.cancel(),
                _ = term.recv() => shutdown_signal.cancel(),
            }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c()
                .await
                .map_err(|error| OcgError::io("cannot handle UI termination", error))?;
            shutdown_signal.cancel();
        }
        shutdown_signal.cancelled().await;
        Ok::<(), OcgError>(())
    });
    shutdown.cancel();
    stop.store(true, Ordering::SeqCst);
    let result = control_thread
        .join()
        .map_err(|_| OcgError::config("control service panicked"))?;
    wait?;
    result
}
