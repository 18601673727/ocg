//! Safe release-archive extraction.
//!
//! Both OpenCode standalone archives contain a single executable named
//! `opencode`. We never unpack archive paths wholesale: the only thing written
//! is the bytes of the entry whose file name is exactly `opencode`, into a
//! destination path chosen by the caller.

use crate::error::{OcgError, Result};
use crate::platform::ArchiveKind;
use std::fs;
use std::io::{Cursor, Write};
use std::path::Path;

const BINARY_NAME: &str = "opencode";

/// Extract the `opencode` executable from an archive.
pub fn extract_opencode(kind: ArchiveKind, bytes: &[u8], dest: &Path) -> Result<()> {
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| OcgError::io(format!("cannot create {}", parent.display()), error))?;
    }
    match kind {
        ArchiveKind::TarGz => extract_from_tar_gz(bytes, dest),
        ArchiveKind::Zip => extract_from_zip(bytes, dest),
    }
}

fn extract_from_tar_gz(bytes: &[u8], dest: &Path) -> Result<()> {
    let decoder = flate2::read::GzDecoder::new(bytes);
    let mut archive = tar::Archive::new(decoder);
    let entries = archive
        .entries()
        .map_err(|error| OcgError::config(format!("cannot read the release archive: {error}")))?;
    for entry in entries {
        let mut entry = entry.map_err(|error| {
            OcgError::config(format!("cannot read the release archive: {error}"))
        })?;
        if !entry.header().entry_type().is_file() {
            continue;
        }
        let path = entry
            .path()
            .map_err(|error| OcgError::config(format!("cannot read an archive entry: {error}")))?;
        if path.file_name().and_then(|name| name.to_str()) != Some(BINARY_NAME) {
            continue;
        }
        let mut file = fs::File::create(dest).map_err(|error| OcgError::write(dest, error))?;
        std::io::copy(&mut entry, &mut file).map_err(|error| OcgError::write(dest, error))?;
        file.flush().map_err(|error| OcgError::write(dest, error))?;
        return Ok(());
    }
    Err(missing_binary())
}

fn extract_from_zip(bytes: &[u8], dest: &Path) -> Result<()> {
    let mut archive = zip::ZipArchive::new(Cursor::new(bytes))
        .map_err(|error| OcgError::config(format!("cannot read the release archive: {error}")))?;
    for index in 0..archive.len() {
        let mut entry = archive
            .by_index(index)
            .map_err(|error| OcgError::config(format!("cannot read an archive entry: {error}")))?;
        if entry.is_dir() {
            continue;
        }
        // `name()` returns the archive-visible path; match on the file name.
        let file_name = Path::new(entry.name())
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default()
            .to_string();
        if file_name != BINARY_NAME {
            continue;
        }
        let mut file = fs::File::create(dest).map_err(|error| OcgError::write(dest, error))?;
        std::io::copy(&mut entry, &mut file).map_err(|error| OcgError::write(dest, error))?;
        file.flush().map_err(|error| OcgError::write(dest, error))?;
        return Ok(());
    }
    Err(missing_binary())
}

fn missing_binary() -> OcgError {
    OcgError::config(format!(
        "release archive does not contain a '{BINARY_NAME}' executable"
    ))
}

/// Mark a file executable (best effort on platforms without POSIX modes).
pub fn make_executable(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let metadata = fs::metadata(path).map_err(|error| OcgError::read(path, error))?;
        let mut permissions = metadata.permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).map_err(|error| OcgError::write(path, error))?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn tar_gz_with(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
            Vec::new(),
            flate2::Compression::default(),
        ));
        for (name, body) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_size(body.len() as u64);
            header.set_mode(0o755);
            header.set_cksum();
            builder.append_data(&mut header, name, *body).unwrap();
        }
        builder.into_inner().unwrap().finish().unwrap()
    }

    fn zip_with(entries: &[(&str, &[u8])]) -> Vec<u8> {
        use zip::write::SimpleFileOptions;
        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        for (name, body) in entries {
            writer
                .start_file(*name, SimpleFileOptions::default())
                .unwrap();
            writer.write_all(body).unwrap();
        }
        writer.finish().unwrap().into_inner()
    }

    #[test]
    fn extracts_only_the_expected_binary_from_tar_gz() {
        let archive = tar_gz_with(&[
            ("README.md", b"ignore me"),
            ("nested/opencode", b"#!/bin/sh\necho hi\n"),
        ]);
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("opencode");
        extract_opencode(ArchiveKind::TarGz, &archive, &dest).unwrap();
        assert_eq!(fs::read(&dest).unwrap(), b"#!/bin/sh\necho hi\n");
        assert!(!dir.path().join("README.md").exists());
        assert!(!dir.path().join("nested").exists());
    }

    #[test]
    fn extracts_only_the_expected_binary_from_zip() {
        let archive = zip_with(&[("docs/readme.txt", b"ignore"), ("opencode", b"binary")]);
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("opencode");
        extract_opencode(ArchiveKind::Zip, &archive, &dest).unwrap();
        assert_eq!(fs::read(&dest).unwrap(), b"binary");
        assert!(!dir.path().join("docs").exists());
    }

    #[test]
    fn missing_binary_is_an_error() {
        let archive = tar_gz_with(&[("README.md", b"nothing")]);
        let dir = tempfile::tempdir().unwrap();
        assert!(
            extract_opencode(ArchiveKind::TarGz, &archive, &dir.path().join("opencode")).is_err()
        );
    }

    #[test]
    fn make_executable_sets_the_mode_on_unix() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("opencode");
        let mut file = fs::File::create(&path).unwrap();
        file.write_all(b"x").unwrap();
        make_executable(&path).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert!(fs::metadata(&path).unwrap().permissions().mode() & 0o111 != 0);
        }
    }
}
