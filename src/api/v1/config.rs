use super::*;

pub const PORT_VARIABLE: &str = "OCG_CONTROL_PORT";
pub const TOKEN_VARIABLE: &str = "OCG_CONTROL_TOKEN";
pub const UI_VARIABLE: &str = "OCG_UI_DIR";

pub const API_VERSION: u32 = 1;

pub const MIN_TOKEN_CHARS: usize = 32;
const MAX_TOKEN_CHARS: usize = 512;
const MIN_TOKEN_DISTINCT: usize = 8;

pub const MAX_SUBMISSION_BYTES: usize = 64 * 1024;

pub const MAX_ATTACHMENT_UPLOAD_BYTES: usize =
    crate::service::ai::providers::openai_compatible::image::MAX_IMAGE_BYTES.div_ceil(3) * 4
        + 4 * 1024;

pub const MAX_OUTPUT_PAGE: usize = 100;
pub(super) const DEFAULT_OUTPUT_PAGE: usize = 50;

pub(super) const MAX_PATH_BYTES: usize = 256;
pub(super) const MAX_QUERY_BYTES: usize = 512;
pub(super) const MAX_HEADERS: usize = 32;
pub(super) const MAX_HEADER_BYTES: usize = 8 * 1024;

const RESPONSE_BYTES: usize = 1024 * 1024;

const ADMISSION_WAIT: Duration = Duration::from_secs(3);

#[derive(Clone)]
pub(super) struct Token(pub(super) Arc<str>);

impl fmt::Debug for Token {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Token(<redacted>)")
    }
}

impl Token {
    pub(super) fn matches(&self, presented: &[u8]) -> bool {
        let expected = digest(&SHA256, self.0.as_bytes());
        let presented = digest(&SHA256, presented);
        verify_slices_are_equal(expected.as_ref(), presented.as_ref()).is_ok()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    #[error("{PORT_VARIABLE} is set but {TOKEN_VARIABLE} is not; the control plane needs both")]
    TokenMissing,
    #[error("{TOKEN_VARIABLE} is set but {PORT_VARIABLE} is not; the control plane needs both")]
    PortMissing,
    #[error("{PORT_VARIABLE} must be a TCP port from 1 to 65535")]
    InvalidPort,
    #[error(
        "{TOKEN_VARIABLE} must be a random token of {MIN_TOKEN_CHARS} to {MAX_TOKEN_CHARS} characters of A-Z, a-z, 0-9, '-', '.', '_', '~', '+', '/' or '='"
    )]
    InvalidToken,
    #[error("{UI_VARIABLE} must name an existing directory of the exported frontend")]
    InvalidUiDirectory,
}

#[derive(Clone, Debug)]
pub struct ControlPlaneConfig {
    pub(super) port: u16,
    pub(super) token: Token,
    pub(super) ui_dir: Option<PathBuf>,
    pub(super) response_bytes: usize,
    pub(super) admission_wait: Duration,
    pub(super) stream_limits: events::StreamLimits,
}

impl ControlPlaneConfig {
    pub fn from_environment() -> Result<Option<Self>, ConfigError> {
        Self::from_source(|name| std::env::var_os(name).map(|value| value.into_string()))
    }

    pub fn from_source(
        source: impl Fn(&str) -> Option<Result<String, std::ffi::OsString>>,
    ) -> Result<Option<Self>, ConfigError> {
        match (source(PORT_VARIABLE), source(TOKEN_VARIABLE)) {
            (None, None) => Ok(None),
            (Some(_), None) => Err(ConfigError::TokenMissing),
            (None, Some(_)) => Err(ConfigError::PortMissing),
            (Some(port), Some(token)) => {
                let port = port
                    .ok()
                    .and_then(|port| port.parse::<u16>().ok())
                    .filter(|port| *port != 0)
                    .ok_or(ConfigError::InvalidPort)?;
                let token = token.map_err(|_| ConfigError::InvalidToken)?;
                let ui_dir = match source(UI_VARIABLE) {
                    None => None,
                    Some(Err(_)) => return Err(ConfigError::InvalidUiDirectory),
                    Some(Ok(directory)) => {
                        let directory = PathBuf::from(directory);
                        if !directory.is_dir() {
                            return Err(ConfigError::InvalidUiDirectory);
                        }
                        Some(directory)
                    }
                };
                Self::new(port, &token).map(|config| Some(Self { ui_dir, ..config }))
            }
        }
    }

    pub fn new(port: u16, token: &str) -> Result<Self, ConfigError> {
        let length = token.chars().count();
        let distinct = token
            .chars()
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        if !(MIN_TOKEN_CHARS..=MAX_TOKEN_CHARS).contains(&length)
            || distinct < MIN_TOKEN_DISTINCT
            || !token
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || "-._~+/=".contains(character))
        {
            return Err(ConfigError::InvalidToken);
        }
        Ok(Self {
            port,
            token: Token(token.into()),
            ui_dir: None,
            response_bytes: RESPONSE_BYTES,
            admission_wait: ADMISSION_WAIT,
            stream_limits: events::StreamLimits::default(),
        })
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}
