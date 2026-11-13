use std::fmt;

pub const MAX_HEADER_FIELDS: usize = 32;
pub const MAX_HEADER_NAME_BYTES: usize = 128;
pub const MAX_HEADER_VALUE_BYTES: usize = 4096;

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

const PROTECTED_PREFIXES: &[&str] = &["proxy-", "sec-"];

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
    Secret(String),
}

#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboundHeader {
    pub name: String,
    pub value: OutboundHeaderValue,
}

impl fmt::Debug for OutboundHeader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self.value {
            OutboundHeaderValue::Plain(_) => "plain",
            OutboundHeaderValue::Secret(_) => "secret",
        };
        write!(formatter, "OutboundHeader({}: <{kind}>)", self.name)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GlobalHeaders {
    pub revision: u32,
    pub fields: Vec<OutboundHeader>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenHeaders {
    pub global_revision: u32,
    pub fields: Vec<OutboundHeader>,
}

impl FrozenHeaders {
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

pub fn is_protected(name: &str) -> bool {
    PROTECTED.contains(&name)
        || PROTECTED_PREFIXES
            .iter()
            .any(|prefix| name.starts_with(prefix))
}

pub fn is_credential_like(name: &str) -> bool {
    CREDENTIAL_FRAGMENTS
        .iter()
        .any(|fragment| name.contains(fragment))
}

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
