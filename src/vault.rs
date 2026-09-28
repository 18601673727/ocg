//! Encrypted user-global credentials for OpenCode child processes.

use crate::error::{OcgError, Result};
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

const KEYCHAIN_SERVICE: &str = "ocg.credentials.v1";
const KEYCHAIN_USER: &str = "vault-key";
const KEY_LEN: usize = 32;
const NONCE_LEN: usize = 12;

#[derive(Serialize, Deserialize)]
struct Envelope {
    version: u8,
    nonce: Vec<u8>,
    ciphertext: Vec<u8>,
}

#[derive(Default, Serialize, Deserialize)]
struct Credentials(BTreeMap<String, String>);

#[derive(Clone, Debug)]
pub struct Vault {
    path: PathBuf,
}

impl Vault {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn user_global() -> Result<Self> {
        let dirs = directories::BaseDirs::new().ok_or_else(|| {
            OcgError::config("cannot resolve the user data directory for OCG credentials")
        })?;
        Ok(Self::new(
            dirs.data_dir().join("ocg").join("credentials.enc"),
        ))
    }

    pub fn list(&self) -> Result<Vec<String>> {
        Ok(self.read()?.0.into_keys().collect())
    }

    pub fn set(&self, name: &str, value: &str) -> Result<()> {
        validate_name(name)?;
        if value.is_empty() {
            return Err(OcgError::config("credential value cannot be empty"));
        }
        let mut credentials = self.read()?;
        credentials.0.insert(name.to_string(), value.to_string());
        self.write(&credentials)
    }

    pub fn remove(&self, name: &str) -> Result<bool> {
        validate_name(name)?;
        let mut credentials = self.read()?;
        let removed = credentials.0.remove(name).is_some();
        if removed {
            self.write(&credentials)?;
        }
        Ok(removed)
    }

    /// Return credential values only for the child process environment.
    pub fn child_environment(&self) -> Result<Vec<(std::ffi::OsString, std::ffi::OsString)>> {
        self.read()?
            .0
            .into_iter()
            .map(|(key, value)| {
                validate_name(&key)?;
                Ok((key.into(), value.into()))
            })
            .collect()
    }

    fn read(&self) -> Result<Credentials> {
        if !self.path.exists() {
            return Ok(Credentials::default());
        }
        let bytes = std::fs::read(&self.path)
            .map_err(|error| OcgError::io("cannot read OCG credential vault", error))?;
        let envelope: Envelope = serde_json::from_slice(&bytes)
            .map_err(|_| OcgError::config("OCG credential vault is invalid"))?;
        if envelope.version != 1 || envelope.nonce.len() != NONCE_LEN {
            return Err(OcgError::config("unsupported OCG credential vault format"));
        }
        let key = encryption_key(false)?;
        let cipher = LessSafeKey::new(
            UnboundKey::new(&AES_256_GCM, &key)
                .map_err(|_| OcgError::config("cannot initialize credential encryption"))?,
        );
        let nonce: [u8; NONCE_LEN] = envelope.nonce.try_into().unwrap();
        let mut plaintext = envelope.ciphertext;
        let clear = cipher
            .open_in_place(
                Nonce::assume_unique_for_key(nonce),
                Aad::empty(),
                &mut plaintext,
            )
            .map_err(|_| {
                OcgError::config(
                    "cannot decrypt OCG credential vault; OS keychain key is unavailable",
                )
            })?;
        serde_json::from_slice(clear)
            .map_err(|_| OcgError::config("OCG credential vault contents are invalid"))
    }

    fn write(&self, credentials: &Credentials) -> Result<()> {
        let key = encryption_key(true)?;
        let cipher = LessSafeKey::new(
            UnboundKey::new(&AES_256_GCM, &key)
                .map_err(|_| OcgError::config("cannot initialize credential encryption"))?,
        );
        let rng = SystemRandom::new();
        let mut nonce = [0; NONCE_LEN];
        rng.fill(&mut nonce)
            .map_err(|_| OcgError::config("cannot generate credential encryption nonce"))?;
        let mut ciphertext = serde_json::to_vec(credentials).map_err(|error| {
            OcgError::config(format!("cannot serialize credential vault: {error}"))
        })?;
        cipher
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce),
                Aad::empty(),
                &mut ciphertext,
            )
            .map_err(|_| OcgError::config("cannot encrypt credential vault"))?;
        let envelope = Envelope {
            version: 1,
            nonce: nonce.to_vec(),
            ciphertext,
        };
        let bytes = serde_json::to_vec(&envelope).map_err(|error| {
            OcgError::config(format!("cannot serialize credential vault: {error}"))
        })?;
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| OcgError::io("cannot create OCG data directory", error))?;
            restrict_directory_permissions(parent)?;
        }
        let temp = self.path.with_extension("enc.tmp");
        std::fs::write(&temp, bytes)
            .map_err(|error| OcgError::io("cannot write OCG credential vault", error))?;
        restrict_permissions(&temp)?;
        std::fs::rename(&temp, &self.path)
            .map_err(|error| OcgError::io("cannot install OCG credential vault", error))?;
        restrict_permissions(&self.path)
    }
}

fn encryption_key(create: bool) -> Result<Vec<u8>> {
    let entry = keyring::Entry::new(KEYCHAIN_SERVICE, KEYCHAIN_USER)
        .map_err(|_| OcgError::config("cannot access the operating-system credential store"))?;
    match entry.get_password() {
        Ok(encoded) => decode_key(&encoded),
        Err(keyring::Error::NoEntry) if create => {
            let mut key = vec![0; KEY_LEN];
            SystemRandom::new()
                .fill(&mut key)
                .map_err(|_| OcgError::config("cannot generate credential encryption key"))?;
            entry.set_password(&encode_key(&key)).map_err(|_| OcgError::config("cannot store the credential encryption key in the operating-system credential store"))?;
            Ok(key)
        }
        Err(_) => Err(OcgError::config(
            "cannot access the operating-system credential store",
        )),
    }
}

fn encode_key(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD_NO_PAD.encode(bytes)
}

fn decode_key(value: &str) -> Result<Vec<u8>> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(value)
        .map_err(|_| OcgError::config("OS credential store contains an invalid OCG vault key"))?;
    if bytes.len() != KEY_LEN {
        return Err(OcgError::config(
            "OS credential store contains an invalid OCG vault key",
        ));
    }
    Ok(bytes)
}

fn validate_name(name: &str) -> Result<()> {
    let reserved = [
        "PATH",
        "HOME",
        "USER",
        "SHELL",
        "PWD",
        "TMPDIR",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
        "OPENCODE_CONFIG",
        "OPENCODE_CONFIG_CONTENT",
        "OPENCODE_CONFIG_DIR",
        "OPENCODE_SERVER_PASSWORD",
    ];
    if name.is_empty()
        || reserved.contains(&name)
        || name.starts_with("OCG_")
        || name.starts_with("OPENCODE_")
        || name.len() > 128
        || !name
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_uppercase() || byte == b'_')
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(OcgError::config(
            "credential name must be an environment variable name using A-Z, 0-9, and underscore",
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn restrict_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .map_err(|error| OcgError::io("cannot restrict credential vault permissions", error))
}

#[cfg(not(unix))]
fn restrict_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn restrict_directory_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
        .map_err(|error| OcgError::io("cannot restrict OCG data directory permissions", error))
}

#[cfg(not(unix))]
fn restrict_directory_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypted_envelope_round_trips_without_exposing_values() {
        let key = vec![17; KEY_LEN];
        let secret = b"credential-value-that-must-not-be-plaintext";
        let cipher = LessSafeKey::new(UnboundKey::new(&AES_256_GCM, &key).unwrap());
        let mut nonce = [0; NONCE_LEN];
        nonce[0] = 1;
        let mut ciphertext = secret.to_vec();
        cipher
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce),
                Aad::empty(),
                &mut ciphertext,
            )
            .unwrap();
        let envelope = Envelope {
            version: 1,
            nonce: nonce.to_vec(),
            ciphertext,
        };
        let encoded = serde_json::to_vec(&envelope).unwrap();
        assert!(!String::from_utf8_lossy(&encoded).contains("credential-value"));

        let mut decoded: Envelope = serde_json::from_slice(&encoded).unwrap();
        let cipher = LessSafeKey::new(UnboundKey::new(&AES_256_GCM, &key).unwrap());
        let nonce: [u8; NONCE_LEN] = decoded.nonce.try_into().unwrap();
        let plaintext = cipher
            .open_in_place(
                Nonce::assume_unique_for_key(nonce),
                Aad::empty(),
                &mut decoded.ciphertext,
            )
            .unwrap();
        assert_eq!(plaintext, secret);
    }

    #[test]
    fn credential_names_are_environment_variable_names() {
        assert!(validate_name("ANTHROPIC_API_KEY").is_ok());
        assert!(validate_name("invalid-name").is_err());
        assert!(validate_name("OCG_BRIDGE").is_err());
        assert!(validate_name("OPENCODE_CONFIG_CONTENT").is_err());
        assert!(validate_name("").is_err());
    }
}
