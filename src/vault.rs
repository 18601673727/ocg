//! Encrypted user-global credentials for OCG-owned provider execution.

use crate::error::{OcgError, Result};
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

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

/// The Vault encryption key, read from the OS credential store ahead of a
/// locked commit so the commit itself never waits on the store.
pub struct VaultKey(Vec<u8>);

#[derive(Clone, Debug)]
pub struct Vault {
    path: PathBuf,
}

impl Vault {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn user_global() -> Result<Self> {
        if let Some(path) = std::env::var_os("OCG_VAULT_PATH") {
            return Ok(Self::new(path));
        }
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
        let key = self.write_key()?;
        let _lock = self.lock()?;
        let mut credentials = self.read_with(&key)?;
        credentials.0.insert(name.to_string(), value.to_string());
        self.write_with(&key, &credentials)
    }

    pub fn remove(&self, name: &str) -> Result<bool> {
        validate_name(name)?;
        if !self.path.exists() {
            return Ok(false);
        }
        let key = VaultKey(shared_encryption_key(None)?);
        let _lock = self.lock()?;
        let mut credentials = self.read_with(&key)?;
        let removed = credentials.0.remove(name).is_some();
        if removed {
            self.write_with(&key, &credentials)?;
        }
        Ok(removed)
    }

    fn lock(&self) -> Result<std::fs::File> {
        self.lock_file("lock", "Vault")
    }

    fn lock_file(&self, extension: &str, what: &str) -> Result<std::fs::File> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| OcgError::io("cannot create Vault directory", error))?;
        }
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(self.path.with_extension(extension))
            .map_err(|error| OcgError::io(format!("cannot open {what} lock"), error))?;
        fs2::FileExt::lock_exclusive(&lock)
            .map_err(|error| OcgError::io(format!("cannot lock {what}"), error))?;
        Ok(lock)
    }

    /// The key a Vault write encrypts with, creating it on first use. The OS
    /// credential store may wait on a person, so call this before taking any
    /// Profile or Vault lock; the locked commit then never touches the store.
    pub fn write_key(&self) -> Result<VaultKey> {
        if let Some(key) = shared_stored_key(None)? {
            return Ok(VaultKey(key));
        }
        // Only a process that found no key at all waits here, and it cannot
        // write without one; Vault and Profile commits never take this lock.
        // Creation re-checks under it so concurrent first writers converge on
        // one key instead of each sealing the Vault with its own.
        let _creating = self.lock_file("key.lock", "Vault key")?;
        if let Some(key) = stored_key()? {
            return Ok(VaultKey(key));
        }
        if self.path.exists() {
            return Err(OcgError::config(
                "cannot access the operating-system credential store",
            ));
        }
        create_key().map(VaultKey)
    }

    /// Store a new credential and run `commit` while the Vault stays locked.
    /// `name` picks the credential name from the names stored right now, so
    /// two concurrent inserts can never claim the same one. A failed commit
    /// removes the credential again before the lock is released, so nothing
    /// else can have observed or referenced it.
    pub fn insert_with<P, T>(
        &self,
        key: &VaultKey,
        value: &str,
        name: impl FnOnce(&BTreeSet<String>) -> Result<(String, P)>,
        commit: impl FnOnce(P) -> Result<T>,
    ) -> Result<T> {
        if value.is_empty() {
            return Err(OcgError::config("credential value cannot be empty"));
        }
        let _lock = self.lock()?;
        let mut credentials = self.read_with(key)?;
        let stored = credentials.0.keys().cloned().collect();
        let (name, prepared) = name(&stored)?;
        validate_name(&name)?;
        if credentials.0.contains_key(&name) {
            return Err(OcgError::config(
                "credential name is already in use; reconnect provider",
            ));
        }
        credentials.0.insert(name.clone(), value.to_string());
        self.write_with(key, &credentials)?;
        match commit(prepared) {
            Ok(result) => Ok(result),
            Err(error) => {
                credentials.0.remove(&name);
                self.write_with(key, &credentials).map_err(|rollback| {
                    OcgError::config(format!(
                        "Provider save failed: {error}; credential rollback failed: {rollback}"
                    ))
                })?;
                Err(error)
            }
        }
    }

    /// Get a single credential value by name.
    pub fn get(&self, name: &str) -> Result<Option<String>> {
        validate_name(name)?;
        Ok(self.read()?.0.get(name).cloned())
    }

    /// Names that hold a non-empty credential, decided by one Vault read. The
    /// values never leave this function. With `wait`, a credential store that
    /// has not answered in time is [`OcgError::CredentialStorePending`].
    pub fn credential_names(&self, wait: Option<Duration>) -> Result<BTreeSet<String>> {
        Ok(self
            .read_within(wait)?
            .0
            .into_iter()
            .filter(|(name, value)| validate_name(name).is_ok() && !value.is_empty())
            .map(|(name, _)| name)
            .collect())
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
        self.read_within(None)
    }

    fn read_within(&self, wait: Option<Duration>) -> Result<Credentials> {
        self.read_using(|| shared_encryption_key(wait))
    }

    fn read_with(&self, key: &VaultKey) -> Result<Credentials> {
        self.read_using(|| Ok(key.0.clone()))
    }

    fn read_using(&self, key: impl FnOnce() -> Result<Vec<u8>>) -> Result<Credentials> {
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
        let key = key()?;
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

    fn write_with(&self, key: &VaultKey, credentials: &Credentials) -> Result<()> {
        let cipher = LessSafeKey::new(
            UnboundKey::new(&AES_256_GCM, &key.0)
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
        Ok(())
    }
}

/// The outcome of one in-flight key lookup, shared by every reader that joined it.
type KeyLookup = Arc<(
    Mutex<Option<std::result::Result<Option<Vec<u8>>, String>>>,
    Condvar,
)>;

/// At most one OS credential-store read per process is outstanding. The OS
/// store can block on an approval prompt for as long as nobody answers it, so
/// readers join the outstanding lookup instead of each stacking another prompt
/// and another parked thread behind it.
static KEY_LOOKUP: Mutex<Option<KeyLookup>> = Mutex::new(None);

/// Read the existing Vault key through the shared lookup. `None` waits for the
/// outcome; `Some(bound)` stops waiting after `bound` while the lookup runs on.
fn shared_encryption_key(wait: Option<Duration>) -> Result<Vec<u8>> {
    shared_stored_key(wait)?
        .ok_or_else(|| OcgError::config("cannot access the operating-system credential store"))
}

/// [`stored_key`] through the one outstanding lookup of this process.
fn shared_stored_key(wait: Option<Duration>) -> Result<Option<Vec<u8>>> {
    let lookup = {
        let mut slot = KEY_LOOKUP
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match slot.as_ref() {
            Some(lookup) => Arc::clone(lookup),
            None => {
                let lookup = KeyLookup::default();
                let worker = Arc::clone(&lookup);
                std::thread::Builder::new()
                    .name("ocg-vault-key".to_string())
                    .spawn(move || {
                        let outcome = stored_key().map_err(|error| error.to_string());
                        // Retire the lookup before publishing it, so a later
                        // reader starts a fresh read rather than reusing this one.
                        *KEY_LOOKUP
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
                        let (state, ready) = &*worker;
                        *state
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(outcome);
                        ready.notify_all();
                    })
                    .map_err(|error| OcgError::io("cannot start Vault key lookup", error))?;
                *slot = Some(Arc::clone(&lookup));
                lookup
            }
        }
    };
    let (state, ready) = &*lookup;
    let state = state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let state = match wait {
        None => ready
            .wait_while(state, |outcome| outcome.is_none())
            .unwrap_or_else(|poisoned| poisoned.into_inner()),
        Some(bound) => {
            ready
                .wait_timeout_while(state, bound, |outcome| outcome.is_none())
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .0
        }
    };
    match state.as_ref() {
        Some(outcome) => outcome.clone().map_err(OcgError::config),
        None => Err(OcgError::CredentialStorePending(
            "the operating-system credential store has not answered yet; approve the OCG \
             keychain prompt if one is showing, then retry"
                .to_string(),
        )),
    }
}

/// The existing Vault key; `None` when the OS credential store has none yet.
fn stored_key() -> Result<Option<Vec<u8>>> {
    // An explicit key file lets isolated/headless processes avoid the shared
    // OS keychain. A missing or invalid override must never fall back to it.
    if let Some(path) = std::env::var_os("OCG_VAULT_KEY_FILE") {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let metadata = std::fs::symlink_metadata(&path)
                .map_err(|error| OcgError::io("cannot inspect explicit Vault key file", error))?;
            if metadata.file_type().is_symlink() {
                return Err(OcgError::config(
                    "explicit Vault key file must not be a symbolic link",
                ));
            }
            if !metadata.is_file() {
                return Err(OcgError::config(
                    "explicit Vault key file must be a regular file",
                ));
            }
            if metadata.permissions().mode() & 0o077 != 0 {
                return Err(OcgError::config(
                    "explicit Vault key file must not grant group or other permissions",
                ));
            }
        }
        let key = std::fs::read(path)
            .map_err(|error| OcgError::io("cannot read explicit Vault key file", error))?;
        if key.len() != KEY_LEN {
            return Err(OcgError::config(
                "explicit Vault key file must contain 32 bytes",
            ));
        }
        return Ok(Some(key));
    }
    let entry = keyring::Entry::new(KEYCHAIN_SERVICE, KEYCHAIN_USER)
        .map_err(|_| OcgError::config("cannot access the operating-system credential store"))?;
    match entry.get_password() {
        Ok(encoded) => decode_key(&encoded).map(Some),
        Err(keyring::Error::NoEntry) => Ok(None),
        Err(_) => Err(OcgError::config(
            "cannot access the operating-system credential store",
        )),
    }
}

fn create_key() -> Result<Vec<u8>> {
    let entry = keyring::Entry::new(KEYCHAIN_SERVICE, KEYCHAIN_USER)
        .map_err(|_| OcgError::config("cannot access the operating-system credential store"))?;
    let mut key = vec![0; KEY_LEN];
    SystemRandom::new()
        .fill(&mut key)
        .map_err(|_| OcgError::config("cannot generate credential encryption key"))?;
    entry.set_password(&encode_key(&key)).map_err(|_| {
        OcgError::config(
            "cannot store the credential encryption key in the operating-system credential store",
        )
    })?;
    Ok(key)
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
    ];
    if name.is_empty()
        || reserved.contains(&name)
        || name.starts_with("OCG_")
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
