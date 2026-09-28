//! SHA-256 helpers shared by managed installs and OCG self-update.

use crate::error::{OcgError, Result};
use sha2::{Digest, Sha256};

/// Lowercase hex SHA-256 of a byte slice.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut text = String::with_capacity(digest.len() * 2);
    for byte in digest {
        text.push_str(&format!("{byte:02x}"));
    }
    text
}

/// Verify bytes against an expected lowercase hex digest.
pub fn verify_sha256(bytes: &[u8], expected: &str, label: &str) -> Result<()> {
    let actual = sha256_hex(bytes);
    if actual.eq_ignore_ascii_case(expected.trim()) {
        Ok(())
    } else {
        Err(OcgError::config(format!(
            "checksum mismatch for {label}: expected {}, got {actual}",
            expected.trim()
        )))
    }
}

/// Find the hex digest for `name` in a `SHA256SUMS` file.
///
/// The file format is `<hex>  <name>` (two spaces), but any whitespace split
/// is accepted and a leading `*` on the name is ignored.
pub fn checksum_for(text: &str, name: &str) -> Option<String> {
    for line in text.lines() {
        let mut parts = line.split_whitespace();
        let Some(hash) = parts.next() else { continue };
        let Some(file) = parts.next() else { continue };
        let file = file.strip_prefix('*').unwrap_or(file);
        if file == name && hash.len() == 64 {
            return Some(hash.to_ascii_lowercase());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashes_known_bytes() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn verifies_case_insensitively() {
        let hash = sha256_hex(b"abc");
        assert!(verify_sha256(b"abc", &hash.to_uppercase(), "fixture").is_ok());
        assert!(verify_sha256(b"abc", "00", "fixture").is_err());
    }

    #[test]
    fn parses_sha256sums_lines() {
        let text =
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad  ocg-linux-x86_64\n\
                    deadbeef  ocg-darwin-arm64\n";
        assert_eq!(
            checksum_for(text, "ocg-linux-x86_64"),
            Some("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".to_string())
        );
        assert_eq!(checksum_for(text, "ocg-darwin-arm64"), None);
        assert_eq!(checksum_for(text, "missing"), None);
    }
}
