//! Generate the TypeScript projection of the Rust wire contract.
//!
//! Rust is the single source of truth. This binary renders that contract into
//! `frontend/components/ocg/contracts/generated.ts`; nothing in the frontend
//! hand-maintains a mirror of it.
//!
//! Usage:
//!   cargo run --bin ocg-rs-ts              # write the file
//!   cargo run --bin ocg-rs-ts --check      # fail if it is stale (CI gate)
//!   cargo run --bin ocg-rs-ts --stdout     # print, for inspection

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use ocg::error::{OcgError, Result};

/// The generated module's canonical location, relative to the repository root.
const RELATIVE_OUTPUT: &str = "frontend/components/ocg/contracts/generated.ts";

/// Where the generator's scratch directory lives. OCG keeps its ignored
/// intermediate state under `.ocg/`, which is persistent disk rather
/// than a small tmpfs, and is already excluded from Git.
const STAGING_ROOT: &str = ".ocg/rs-ts";

/// The repository root, resolved at run time.
///
/// This deliberately does not use `CARGO_MANIFEST_DIR`. That constant is baked
/// in when the binary is *compiled*, and Cargo's build cache is shared across
/// worktrees, so a binary built in a worktree that has since been removed would
/// resolve its output path inside a directory that no longer exists. Walking up
/// from the working directory finds the tree the caller is actually standing in;
/// `cargo run` sets that to the package root.
fn repository_root() -> Result<PathBuf> {
    let start = std::env::current_dir()
        .map_err(|error| OcgError::io("read the working directory", error))?;
    for candidate in start.ancestors() {
        if candidate.join("Cargo.toml").is_file() && candidate.join("frontend").is_dir() {
            return Ok(candidate.to_path_buf());
        }
    }
    Err(OcgError::config(format!(
        "no repository root above {}: expected a Cargo.toml and a frontend/ directory",
        start.display()
    )))
}

fn output_path() -> Result<PathBuf> {
    Ok(repository_root()?.join(RELATIVE_OUTPUT))
}

fn main() -> ExitCode {
    let outcome = run();
    prune_staging();
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ocg-rs-ts: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let root = repository_root()?;
    let staging = root.join(STAGING_ROOT);
    let rendered = ocg::contracts::render_at(&staging).map_err(OcgError::config)?;
    let path = output_path()?;

    if args.iter().any(|arg| arg == "--stdout") {
        print!("{rendered}");
        return Ok(());
    }

    // A stale generated file is a broken contract, not a formatting nit: the
    // TypeScript would no longer describe the bytes the server actually sends.
    if args.iter().any(|arg| arg == "--check") {
        let current = std::fs::read_to_string(&path).unwrap_or_default();
        if current == rendered {
            return Ok(());
        }
        return Err(OcgError::config(format!(
            "{} is out of date with the Rust contract in src/contracts.rs.\n\
             Run `make contracts` (cargo run --bin ocg-rs-ts) and commit the result.",
            path.display()
        )));
    }

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| OcgError::io("create contract output directory", error))?;
    }
    // Only write when the content actually changes, so a no-op run does not
    // churn the working tree.
    let current = std::fs::read_to_string(&path).unwrap_or_default();
    if current != rendered {
        std::fs::write(&path, rendered.as_bytes())
            .map_err(|error| OcgError::io("write generated contract", error))?;
        println!("wrote {}", relative(&path));
    } else {
        println!("{} is already up to date", relative(&path));
    }
    Ok(())
}

fn relative(path: &Path) -> String {
    path.strip_prefix(repository_root().unwrap_or_else(|_| PathBuf::from("/")))
        .unwrap_or(path)
        .display()
        .to_string()
}

/// The staging root is scratch, not an artifact. Leaving an empty directory
/// behind after a run would be noise, so it is removed once rendering is done.
fn prune_staging() {
    let Ok(root) = repository_root() else { return };
    let staging = root.join(STAGING_ROOT);
    let _ = std::fs::remove_dir_all(&staging);
}
