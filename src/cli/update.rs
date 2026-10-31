use std::cmp::Ordering;
use std::error::Error;
use std::fs::{self, DirBuilder};
use std::os::unix::fs::DirBuilderExt;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use ntex::client::{Client, Connector};
use ntex::http::header;
use ntex::time::Millis;

use crate::system::http;
use crate::system::update::{self, Install, Version, fetch, install};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
async fn wait_jobs(install: &Install) {
    let client = Client::builder()
        .connector::<()>(Connector::default())
        .response_timeout(Millis(2_000))
        .response_payload_timeout(Millis(2_000))
        .build(ntex::SharedCfg::new("update-health"))
        .await;
    let Ok(client) = client else { return };
    let Ok(auth) = http::authorization(&install.token) else {
        return;
    };
    let url = format!("http://127.0.0.1:{}/api/v1/health", install.port);
    loop {
        let Ok(response) = client
            .get(url.as_str())
            .header(header::AUTHORIZATION, auth.clone())
            .send()
            .await
        else {
            break;
        };
        if !response.status().is_success() {
            break;
        }
        let Ok(body) = response.body().await else {
            break;
        };
        let Ok(value) = serde_json::from_slice::<serde_json::Value>(&body) else {
            break;
        };
        let Some(running) = value
            .pointer("/queue/running")
            .and_then(serde_json::Value::as_u64)
        else {
            break;
        };
        if running == 0 {
            break;
        }
        println!("waiting for {running} running Jobs (use --yes to update now)");
        ntex::time::sleep(Duration::from_secs(5)).await;
    }
}

struct Download {
    version: String,
    installer: Vec<u8>,
}
async fn prepare(
    install: &Install,
    requested: Option<&str>,
    check: bool,
    yes: bool,
) -> Result<Option<Download>> {
    let (client, base, local, version, checksum) = update::lookup(requested).await?;
    let current = env!("CARGO_PKG_VERSION");
    let order = Version::parse(&version)
        .ok_or("invalid release version")?
        .cmp(&Version::parse(current).ok_or("invalid running version")?);
    if check {
        println!("running {current}, latest {version}");
        return Ok(None);
    }
    if order == Ordering::Equal || (requested.is_none() && order != Ordering::Greater) {
        println!("up to date");
        return Ok(None);
    }
    if order == Ordering::Less {
        println!("downgrading from {current} to {version}");
    }
    if !yes {
        wait_jobs(install).await;
    }
    let installer = fetch(&client, &format!("{base}/install.sh"), local).await?;
    let actual = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &installer)
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    if actual != checksum {
        return Err("installer checksum mismatch".into());
    }
    Ok(Some(Download { version, installer }))
}

pub(super) fn run(args: Vec<String>) -> Result<()> {
    let mut check = false;
    let mut yes = false;
    let mut version = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--check" if !check => check = true,
            "--yes" if !yes => yes = true,
            "--version" if version.is_none() => {
                let value = args.next().ok_or("--version requires a version")?;
                Version::parse(&value).ok_or("invalid version")?;
                version = Some(value);
            }
            _ => return Err("usage: ocg update [--check] [--version X.Y.Z] [--yes]".into()),
        }
    }
    let install = install()?;
    let download = ntex::rt::System::build()
        .name("ocg-update")
        .signals(false)
        .build(ntex::rt::DefaultRuntime)
        .block_on({
            let install = install.clone();
            async move { prepare(&install, version.as_deref(), check, yes).await }
        })?;
    let Some(download) = download else {
        return Ok(());
    };
    let dir = std::env::temp_dir().join(format!("ocg-update-{}", uuid::Uuid::now_v7()));
    DirBuilder::new().mode(0o700).create(&dir)?;
    let result = run_installer(&dir, &install.prefix, &download);
    let cleanup = fs::remove_dir_all(&dir);
    let status = result?;
    cleanup?;
    if !status.success() {
        std::process::exit(status.code().unwrap_or(1));
    }
    Ok(())
}

fn run_installer(
    dir: &Path,
    prefix: &Path,
    download: &Download,
) -> Result<std::process::ExitStatus> {
    let file = dir.join("install.sh");
    fs::write(&file, &download.installer)?;
    let home = std::env::var_os("HOME").ok_or("HOME is not set")?;
    let home = Path::new(&home);
    let service = update::service_file(home);
    let mut command = Command::new("/bin/sh");
    command
        .arg(file)
        .arg("--version")
        .arg(&download.version)
        .arg("--prefix")
        .arg(prefix);
    if !service.is_file() {
        command.arg("--no-service");
    }
    Ok(command.status()?)
}
