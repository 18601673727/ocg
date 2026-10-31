use std::error::Error;
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::db::redb::{
    MigrationFormat, migration_apply, migration_inspect, migration_snapshot, migration_source,
};
use crate::system::data_directory::DATA_DIRECTORY_VARIABLE;

pub(super) fn run(arguments: Vec<String>) -> Result<(), Box<dyn Error>> {
    let destination = match arguments.as_slice() {
        [] => None,
        [single] if single == "--check" => None,
        [flag, path] if flag == "--to" => Some(PathBuf::from(path)),
        _ => return Err("usage: ocg migrate [--check | --to <directory>]".into()),
    };
    let source = std::env::var_os(DATA_DIRECTORY_VARIABLE)
        .filter(|value| !value.is_empty())
        .ok_or("OCG_DATA_DIR must name an existing data directory")?;
    let source = fs::canonicalize(source)?;
    if !source.is_dir() {
        return Err("source is not a directory".into());
    }
    let db_path = source.join("ocg.redb");
    if !fs::symlink_metadata(&db_path)?.file_type().is_file() {
        return Err("source database must be an existing regular file".into());
    }
    let destination = destination
        .map(|path| destination_path(&source, &path))
        .transpose()?;
    let db = migration_source(&db_path)?;
    let format = migration_inspect(&db)?;
    match format {
        MigrationFormat::Legacy => println!(
            "Database format: legacy unmarked; plan: rebuild conversation search and migrate to format 16."
        ),
        MigrationFormat::UnmarkedCurrent => {
            println!("Database format: unmarked; plan: migrate to format 16.")
        }
        MigrationFormat::Format15 => println!("Database format: 15; plan: migrate to format 16."),
        MigrationFormat::Current => println!("Database format: 16; plan: no migration required."),
    }
    let Some(destination) = destination else {
        return Ok(());
    };
    if format == MigrationFormat::Current {
        return Err("database is already format 16; no destination was created".into());
    }
    let expected = migration_snapshot(&db, format)?;
    let parent = destination.parent().ok_or("destination has no parent")?;
    let staging = parent.join(format!(".ocg-migrate-{}", uuid::Uuid::now_v7()));
    copy_tree(&source, &staging)?;
    verify_tree(&source, &staging, false)?;
    let copied = migration_source(&staging.join("ocg.redb"))?;
    if migration_inspect(&copied)? != format || migration_snapshot(&copied, format)? != expected {
        return Err("copied database does not match the source".into());
    }
    drop(copied);
    migration_apply(&staging.join("ocg.redb"), format, &expected)?;
    let migrated = migration_source(&staging.join("ocg.redb"))?;
    if migration_inspect(&migrated)? != MigrationFormat::Current
        || migration_snapshot(&migrated, MigrationFormat::Current)? != expected
    {
        return Err("migrated authoritative data differs from the source".into());
    }
    verify_tree(&source, &staging, true)?;
    drop(migrated);
    publish(&staging, &destination)?;
    let _ = writeln!(
        io::stdout().lock(),
        "Migration complete; authoritative records and copied files verified."
    );
    Ok(())
}

#[cfg(any(target_vendor = "apple", target_os = "linux", target_os = "android"))]
fn publish(staging: &Path, destination: &Path) -> io::Result<()> {
    rustix::fs::renameat_with(
        rustix::fs::CWD,
        staging,
        rustix::fs::CWD,
        destination,
        rustix::fs::RenameFlags::NOREPLACE,
    )?;
    Ok(())
}

#[cfg(not(any(target_vendor = "apple", target_os = "linux", target_os = "android")))]
fn publish(_staging: &Path, _destination: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "atomic no-replace directory publication is unsupported on this platform",
    ))
}

fn destination_path(source: &Path, path: &Path) -> Result<PathBuf, Box<dyn Error>> {
    let name = path
        .file_name()
        .ok_or("destination must have a directory name")?;
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let parent = fs::canonicalize(parent)?;
    let destination = parent.join(name);
    if destination.starts_with(source) || source.starts_with(&destination) {
        return Err("source and destination must not overlap".into());
    }
    match fs::symlink_metadata(&destination) {
        Ok(_) => Err("destination already exists".into()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(destination),
        Err(error) => Err(error.into()),
    }
}

fn copy_tree(source: &Path, destination: &Path) -> Result<(), Box<dyn Error>> {
    let metadata = fs::symlink_metadata(source)?;
    if !metadata.file_type().is_dir() {
        return Err("source directory contains an unsupported entry".into());
    }
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let from = entry.path();
        let to = destination.join(entry.file_name());
        let metadata = fs::symlink_metadata(&from)?;
        if metadata.file_type().is_dir() {
            copy_tree(&from, &to)?;
        } else if metadata.file_type().is_file() {
            let mut from = File::open(from)?;
            let mut to = OpenOptions::new().write(true).create_new(true).open(&to)?;
            io::copy(&mut from, &mut to)?;
            to.set_permissions(metadata.permissions())?;
            to.sync_all()?;
        } else {
            return Err("source directory contains a symbolic link or unsupported entry".into());
        }
    }
    fs::set_permissions(destination, metadata.permissions())?;
    Ok(())
}

fn file_digest(path: &Path) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut file = File::open(path)?;
    let mut hash = aws_lc_rs::digest::Context::new(&aws_lc_rs::digest::SHA256);
    let mut buffer = [0; 65536];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(hash.finish().as_ref().to_vec())
}

fn verify_tree(
    source: &Path,
    destination: &Path,
    skip_database: bool,
) -> Result<(), Box<dyn Error>> {
    let mut source_names = fs::read_dir(source)?
        .map(|e| e.map(|e| e.file_name()))
        .collect::<Result<Vec<OsString>, _>>()?;
    let mut destination_names = fs::read_dir(destination)?
        .map(|e| e.map(|e| e.file_name()))
        .collect::<Result<Vec<OsString>, _>>()?;
    source_names.sort();
    destination_names.sort();
    if source_names != destination_names {
        return Err("copied directory entries differ".into());
    }
    for name in source_names {
        let from = source.join(&name);
        let to = destination.join(&name);
        let source_type = fs::symlink_metadata(&from)?.file_type();
        let dest_type = fs::symlink_metadata(&to)?.file_type();
        if source_type.is_dir() && dest_type.is_dir() {
            verify_tree(&from, &to, false)?;
        } else if source_type.is_file() && dest_type.is_file() {
            if !(skip_database && name == "ocg.redb") && file_digest(&from)? != file_digest(&to)? {
                return Err("copied file contents differ".into());
            }
        } else {
            return Err("copied file types differ".into());
        }
    }
    Ok(())
}
