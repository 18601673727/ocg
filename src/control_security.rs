use crate::error::{OcgError, Result};
use crate::http::NativeHttp;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use ring::signature::{RsaPublicKeyComponents, RSA_PKCS1_2048_8192_SHA256};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone)]
pub(crate) enum Authentication {
    Local,
    Access(Arc<AccessVerifier>),
}

#[derive(Debug, Clone)]
pub(crate) struct Identity {
    pub user_id: String,
    pub expires_at: i64,
    pub session_id: String,
}

#[derive(Deserialize)]
struct JwtHeader {
    alg: String,
    kid: String,
    #[serde(default)]
    crit: Vec<String>,
}

#[derive(Deserialize)]
struct Claims {
    iss: String,
    sub: String,
    aud: Vec<String>,
    exp: i64,
    iat: i64,
    nbf: Option<i64>,
    email: String,
    #[serde(rename = "type")]
    token_type: String,
    common_name: Option<String>,
}

#[derive(Deserialize)]
struct JsonWebKey {
    kid: String,
    kty: String,
    alg: Option<String>,
    #[serde(rename = "use")]
    usage: Option<String>,
    n: String,
    e: String,
}

#[derive(Deserialize)]
struct JsonWebKeys {
    keys: Vec<JsonWebKey>,
}

struct KeyCache {
    keys: Vec<JsonWebKey>,
    fetched_at: Instant,
}

pub(crate) struct AccessVerifier {
    issuer: String,
    audience: String,
    origins: Vec<String>,
    transport: NativeHttp,
    keys: Mutex<Option<KeyCache>>,
}

fn invalid() -> OcgError {
    OcgError::config("Access authentication failed")
}

fn required_env(name: &str) -> Result<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| OcgError::config(format!("{name} is required")))
}

pub(crate) fn valid_https_origin(raw: &str) -> bool {
    url::Url::parse(raw).is_ok_and(|url| {
        url.scheme() == "https"
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && url.path() == "/"
            && url.origin().ascii_serialization() == raw
    })
}

impl Authentication {
    pub fn from_env() -> Result<Self> {
        match std::env::var("OCG_AUTH_MODE").as_deref() {
            Err(std::env::VarError::NotPresent) | Ok("local") => {
                if [
                    "OCG_ACCESS_ISSUER",
                    "OCG_ACCESS_AUDIENCE",
                    "OCG_PUBLIC_ORIGIN",
                ]
                .iter()
                .any(|name| std::env::var_os(name).is_some())
                {
                    return Err(OcgError::config(
                        "remote settings require OCG_AUTH_MODE=cloudflare-access",
                    ));
                }
                Ok(Self::Local)
            }
            Ok("cloudflare-access") => {
                let issuer = required_env("OCG_ACCESS_ISSUER")?;
                let host = url::Url::parse(&issuer)
                    .ok()
                    .and_then(|url| url.host_str().map(str::to_string))
                    .unwrap_or_default();
                let team = host.strip_suffix(".cloudflareaccess.com").unwrap_or("");
                if !valid_https_origin(&issuer)
                    || team.is_empty()
                    || !team
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                {
                    return Err(OcgError::config(
                        "OCG_ACCESS_ISSUER must be a Cloudflare Access HTTPS team origin",
                    ));
                }
                let origins: Vec<String> = required_env("OCG_PUBLIC_ORIGIN")?
                    .split(',')
                    .map(|value| value.trim().to_string())
                    .collect();
                if !origins.iter().all(|origin| valid_https_origin(origin)) {
                    return Err(OcgError::config(
                        "OCG_PUBLIC_ORIGIN must contain exact HTTPS origins",
                    ));
                }
                Ok(Self::Access(Arc::new(AccessVerifier {
                    issuer,
                    audience: required_env("OCG_ACCESS_AUDIENCE")?,
                    origins,
                    transport: NativeHttp::new()?,
                    keys: Mutex::new(None),
                })))
            }
            _ => Err(OcgError::config("unsupported OCG_AUTH_MODE")),
        }
    }

    pub fn is_remote(&self) -> bool {
        matches!(self, Self::Access(_))
    }

    pub fn accepts_origin(&self, origin: &str) -> bool {
        match self {
            Self::Local => url::Url::parse(origin).is_ok_and(|url| {
                url.scheme() == "http"
                    && matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"))
                    && url.origin().ascii_serialization() == origin
            }),
            Self::Access(verifier) => verifier.origins.iter().any(|allowed| allowed == origin),
        }
    }

    pub fn authenticate(
        &self,
        headers: &HashMap<String, String>,
        store: &OwnershipStore,
        now: i64,
    ) -> Result<Option<Identity>> {
        match self {
            Self::Local => {
                if headers.keys().any(|name| {
                    name == "forwarded"
                        || name == "x-real-ip"
                        || name.starts_with("x-forwarded-")
                        || name.starts_with("cf-")
                }) {
                    return Err(invalid());
                }
                Ok(None)
            }
            Self::Access(verifier) => {
                let token = headers.get("cf-access-jwt-assertion").ok_or_else(invalid)?;
                let claims = verifier.verify(token, now)?;
                let session_id = format!("{:x}", Sha256::digest(token.as_bytes()));
                if store.revoked(&session_id)? {
                    return Err(invalid());
                }
                let user_id = store.record_user(&claims.iss, &claims.sub)?;
                Ok(Some(Identity {
                    user_id,
                    expires_at: claims.exp,
                    session_id,
                }))
            }
        }
    }
}

impl AccessVerifier {
    fn verify(&self, token: &str, now: i64) -> Result<Claims> {
        if token.len() > 12 * 1024 || now <= 0 {
            return Err(invalid());
        }
        let parts: Vec<&str> = token.split('.').collect();
        let [head, payload, signature] = parts.as_slice() else {
            return Err(invalid());
        };
        let decode = |part: &str| URL_SAFE_NO_PAD.decode(part).map_err(|_| invalid());
        let header: JwtHeader = serde_json::from_slice(&decode(head)?).map_err(|_| invalid())?;
        if header.alg != "RS256" || header.kid.is_empty() || !header.crit.is_empty() {
            return Err(invalid());
        }
        let signature = decode(signature)?;
        let mut cache = self.keys.lock().map_err(|_| invalid())?;
        // An unknown kid cannot trigger unbounded outbound requests. Rotation
        // refreshes after a short cooldown; stale keys are never used on failure.
        let needs_refresh = cache.as_ref().is_none_or(|cached| {
            cached.fetched_at.elapsed() >= Duration::from_secs(300)
                || (!cached.keys.iter().any(|key| key.kid == header.kid)
                    && cached.fetched_at.elapsed() >= Duration::from_secs(30))
        });
        if needs_refresh {
            let bytes = self
                .transport
                .get_security_document(&format!("{}/cdn-cgi/access/certs", self.issuer))
                .map_err(|_| invalid())?;
            if bytes.len() > 256 * 1024 {
                return Err(invalid());
            }
            let keys: JsonWebKeys = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
            if keys.keys.is_empty() || keys.keys.len() > 16 {
                return Err(invalid());
            }
            *cache = Some(KeyCache {
                keys: keys.keys,
                fetched_at: Instant::now(),
            });
        }
        let key = cache
            .as_ref()
            .and_then(|cached| {
                cached.keys.iter().find(|key| {
                    key.kid == header.kid
                        && key.kty == "RSA"
                        && key.alg.as_deref().is_none_or(|alg| alg == "RS256")
                        && key.usage.as_deref().is_none_or(|usage| usage == "sig")
                })
            })
            .ok_or_else(invalid)?;
        let modulus = decode(&key.n)?;
        let exponent = decode(&key.e)?;
        RsaPublicKeyComponents {
            n: modulus.as_slice(),
            e: exponent.as_slice(),
        }
        .verify(
            &RSA_PKCS1_2048_8192_SHA256,
            format!("{head}.{payload}").as_bytes(),
            &signature,
        )
        .map_err(|_| invalid())?;
        let claims: Claims = serde_json::from_slice(&decode(payload)?).map_err(|_| invalid())?;
        if claims.iss != self.issuer
            || !claims.aud.contains(&self.audience)
            || claims.exp <= now
            || claims.iat > now
            || claims.iat >= claims.exp
            || claims.nbf.is_some_and(|not_before| not_before > now)
            || claims.token_type != "app"
            || claims.common_name.is_some()
            || claims.sub.trim().is_empty()
            || claims.sub.len() > 256
            || !claims.email.contains('@')
            || claims.email.len() > 320
        {
            return Err(invalid());
        }
        Ok(claims)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OwnershipStore(Arc<Mutex<Connection>>);

fn user_id(issuer: &str, subject: &str) -> String {
    let mut digest = Sha256::new();
    digest.update((issuer.len() as u64).to_be_bytes());
    digest.update(issuer.as_bytes());
    digest.update(subject.as_bytes());
    format!("user-{:x}", digest.finalize())
}

impl OwnershipStore {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| OcgError::io("create security state", error))?;
        }
        let connection =
            Connection::open(path).map_err(|_| OcgError::config("cannot open security state"))?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(|_| invalid())?;
        connection.execute_batch("PRAGMA foreign_keys = ON;
            CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, issuer TEXT NOT NULL, subject TEXT NOT NULL, UNIQUE(issuer, subject));
            CREATE TABLE IF NOT EXISTS project_owners (project_id TEXT PRIMARY KEY, user_id TEXT NOT NULL REFERENCES users(id));
            CREATE TABLE IF NOT EXISTS revoked_sessions (session_id TEXT PRIMARY KEY, expires_at INTEGER NOT NULL);")
            .map_err(|_| OcgError::config("cannot initialize security state"))?;
        Ok(Self(Arc::new(Mutex::new(connection))))
    }

    pub fn record_user(&self, issuer: &str, subject: &str) -> Result<String> {
        let id = user_id(issuer, subject);
        self.0
            .lock()
            .map_err(|_| invalid())?
            .execute(
                "INSERT OR IGNORE INTO users (id, issuer, subject) VALUES (?1, ?2, ?3)",
                params![id, issuer, subject],
            )
            .map_err(|_| invalid())?;
        Ok(id)
    }

    pub fn owns(&self, user: &str, project: &str) -> Result<bool> {
        let owner: Option<String> = self
            .0
            .lock()
            .map_err(|_| invalid())?
            .query_row(
                "SELECT user_id FROM project_owners WHERE project_id = ?1",
                [project],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| invalid())?;
        Ok(owner.as_deref() == Some(user))
    }

    pub fn revoked(&self, session: &str) -> Result<bool> {
        self.0
            .lock()
            .map_err(|_| invalid())?
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM revoked_sessions WHERE session_id = ?1)",
                [session],
                |row| row.get(0),
            )
            .map_err(|_| invalid())
    }

    pub fn revoke(&self, identity: &Identity, now: i64) -> Result<()> {
        let mut connection = self.0.lock().map_err(|_| invalid())?;
        let transaction = connection.transaction().map_err(|_| invalid())?;
        transaction
            .execute("DELETE FROM revoked_sessions WHERE expires_at <= ?1", [now])
            .map_err(|_| invalid())?;
        transaction
            .execute(
                "INSERT OR IGNORE INTO revoked_sessions VALUES (?1, ?2)",
                params![identity.session_id, identity.expires_at],
            )
            .map_err(|_| invalid())?;
        transaction.commit().map_err(|_| invalid())
    }

    pub fn assign_legacy(&self, project: &str, issuer: &str, subject: &str) -> Result<String> {
        if !valid_https_origin(issuer) || subject.trim().is_empty() || subject.len() > 256 {
            return Err(OcgError::config(
                "an explicit HTTPS issuer and subject are required",
            ));
        }
        let mut connection = self.0.lock().map_err(|_| invalid())?;
        let transaction = connection.transaction().map_err(|_| invalid())?;
        let id = user_id(issuer, subject);
        transaction
            .execute(
                "INSERT OR IGNORE INTO users (id, issuer, subject) VALUES (?1, ?2, ?3)",
                params![id, issuer, subject],
            )
            .map_err(|_| invalid())?;
        let owner: Option<String> = transaction
            .query_row(
                "SELECT user_id FROM project_owners WHERE project_id = ?1",
                [project],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| invalid())?;
        if owner.as_ref().is_some_and(|owner| owner != &id) {
            return Err(OcgError::config(
                "Project already has an owner; migration cannot reassign it",
            ));
        }
        transaction
            .execute(
                "INSERT OR IGNORE INTO project_owners (project_id, user_id) VALUES (?1, ?2)",
                params![project, id],
            )
            .map_err(|_| invalid())?;
        transaction.commit().map_err(|_| invalid())?;
        Ok(id)
    }
}
