//! Custom HTTP request fields sent with Provider requests.
//!
//! A field is configured globally or on one Connection. A Connection's field
//! replaces the global field of the same name, and either replaces OCG's
//! built-in default (`User-Agent`). Names are compared case-insensitively and
//! kept in lower case, so a configuration never holds two spellings of one
//! field. A value is either plain text stored in the configuration row, or a
//! secret stored once in the credential store and named here by reference;
//! only a Connection may hold a secret, because a global secret would be sent
//! to every Provider host.

use std::fmt;

/// Fields one scope may configure.
pub const MAX_HEADER_FIELDS: usize = 32;
pub const MAX_HEADER_NAME_BYTES: usize = 128;
pub const MAX_HEADER_VALUE_BYTES: usize = 4096;

/// Fields OCG owns. Each is set by OCG for the wire protocol it speaks, names
/// the credential it manages, or frames the message, so a configured value
/// would either be overwritten, break the exchange, or redirect a credential.
const PROTECTED: &[&str] = &[
    "authorization",
    "proxy-authorization",
    "x-api-key",
    "host",
    "content-length",
    "transfer-encoding",
    "connection",
    "keep-alive",
    "te",
    "trailer",
    "upgrade",
    "expect",
    "content-type",
    "content-encoding",
    "accept",
    "accept-encoding",
    "cache-control",
    "anthropic-version",
    "x-opencode-session",
    "set-cookie",
];

/// Prefixes of fields a client never sets: the proxy and fetch metadata
/// families.
const PROTECTED_PREFIXES: &[&str] = &["proxy-", "sec-"];

/// Name fragments that mark a field as carrying a credential. Such a field
/// can only be set as a secret, on one Connection.
const CREDENTIAL_FRAGMENTS: &[&str] = &[
    "auth",
    "key",
    "token",
    "secret",
    "password",
    "passwd",
    "credential",
    "cookie",
];

#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OutboundHeaderValue {
    Plain(String),
    /// The name of the credential holding the value.
    Secret(String),
}

#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboundHeader {
    /// Lower case; see [`canonical_name`].
    pub name: String,
    pub value: OutboundHeaderValue,
}

/// A field's value is never printed, so a plain value mistaken for harmless
/// cannot reach a log through `Debug` either.
impl fmt::Debug for OutboundHeader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self.value {
            OutboundHeaderValue::Plain(_) => "plain",
            OutboundHeaderValue::Secret(_) => "secret",
        };
        write!(formatter, "OutboundHeader({}: <{kind}>)", self.name)
    }
}

/// The global fields and the revision they were saved at; revision 0 is "never
/// configured".
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GlobalHeaders {
    pub revision: u32,
    pub fields: Vec<OutboundHeader>,
}

/// The fields a Job was frozen with: the merge of the global and the
/// Connection fields, and the global revision it was merged from. Secrets are
/// named by credential reference, never copied.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenHeaders {
    pub global_revision: u32,
    pub fields: Vec<OutboundHeader>,
}

impl FrozenHeaders {
    /// The credential references the fields name.
    pub fn secret_refs(&self) -> impl Iterator<Item = &str> {
        self.fields.iter().filter_map(|field| match &field.value {
            OutboundHeaderValue::Secret(reference) => Some(reference.as_str()),
            OutboundHeaderValue::Plain(_) => None,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HeaderScope {
    Global,
    Connection,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum HeaderError {
    #[error("header name must be 1..=128 characters of letters, digits and !#$%&'*+-.^_`|~")]
    InvalidName,
    #[error("header value must be 1..=4096 visible ASCII characters without surrounding spaces")]
    InvalidValue,
    #[error("header `{0}` is managed by OCG and cannot be configured")]
    Protected(String),
    #[error(
        "header `{0}` appears to carry a credential: set it as a secret value on one Connection"
    )]
    CredentialLike(String),
    #[error("a global header cannot be a secret: it would be sent to every Provider")]
    GlobalSecret,
    #[error("header `{0}` is defined more than once")]
    Duplicate(String),
    #[error("at most 32 headers may be configured in one scope")]
    TooMany,
}

impl HeaderError {
    /// A fixed description for a rejection, which names no field value.
    pub fn reason(&self) -> &'static str {
        match self {
            Self::InvalidName => "header name is not a valid HTTP field name",
            Self::InvalidValue => "header value is not a valid HTTP field value",
            Self::Protected(_) => "header is managed by OCG and cannot be configured",
            Self::CredentialLike(_) => {
                "a header that carries a credential must be a secret value on one Connection"
            }
            Self::GlobalSecret => "a global header cannot be a secret",
            Self::Duplicate(_) => "a header is defined more than once",
            Self::TooMany => "too many headers in one scope",
        }
    }
}

/// The lower-case name, if it is a valid HTTP field name (an RFC 9110 token).
pub fn canonical_name(name: &str) -> Result<String, HeaderError> {
    let valid = !name.is_empty()
        && name.len() <= MAX_HEADER_NAME_BYTES
        && name.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        });
    if valid {
        Ok(name.to_ascii_lowercase())
    } else {
        Err(HeaderError::InvalidName)
    }
}

/// Whether OCG owns the field (the name is canonical).
pub fn is_protected(name: &str) -> bool {
    PROTECTED.contains(&name)
        || PROTECTED_PREFIXES
            .iter()
            .any(|prefix| name.starts_with(prefix))
}

/// Whether the name marks a credential-bearing field (the name is canonical).
pub fn is_credential_like(name: &str) -> bool {
    CREDENTIAL_FRAGMENTS
        .iter()
        .any(|fragment| name.contains(fragment))
}

/// A value is visible ASCII and spaces or tabs between, so it cannot hold a
/// line break, a NUL or any other control byte that could split or smuggle a
/// field.
pub fn validate_value(value: &str) -> Result<(), HeaderError> {
    let valid = !value.is_empty()
        && value.len() <= MAX_HEADER_VALUE_BYTES
        && value == value.trim_matches([' ', '\t'])
        && value
            .bytes()
            .all(|byte| byte == b'\t' || (0x20..=0x7e).contains(&byte));
    if valid {
        Ok(())
    } else {
        Err(HeaderError::InvalidValue)
    }
}

/// Checks one scope's fields: names canonical and unique, values valid,
/// nothing OCG owns, credential-bearing names only as Connection secrets.
pub fn validate_fields(fields: &[OutboundHeader], scope: HeaderScope) -> Result<(), HeaderError> {
    if fields.len() > MAX_HEADER_FIELDS {
        return Err(HeaderError::TooMany);
    }
    let mut seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for field in fields {
        let canonical = canonical_name(&field.name)?;
        if canonical != field.name {
            return Err(HeaderError::InvalidName);
        }
        if !seen.insert(canonical.clone()) {
            return Err(HeaderError::Duplicate(canonical));
        }
        if is_protected(&canonical) {
            return Err(HeaderError::Protected(canonical));
        }
        match (&field.value, scope) {
            (OutboundHeaderValue::Plain(value), _) => {
                validate_value(value)?;
                if is_credential_like(&canonical) {
                    return Err(HeaderError::CredentialLike(canonical));
                }
            }
            (OutboundHeaderValue::Secret(_), HeaderScope::Global) => {
                return Err(HeaderError::GlobalSecret);
            }
            (OutboundHeaderValue::Secret(_), HeaderScope::Connection) => {}
        }
    }
    Ok(())
}

/// The fields a Connection's requests carry: the Connection's own, and the
/// global ones it does not replace, ordered by name.
pub fn merge(global: &[OutboundHeader], connection: &[OutboundHeader]) -> Vec<OutboundHeader> {
    let mut merged: std::collections::BTreeMap<&str, &OutboundHeader> = global
        .iter()
        .map(|field| (field.name.as_str(), field))
        .collect();
    for field in connection {
        merged.insert(field.name.as_str(), field);
    }
    merged.into_values().cloned().collect()
}
