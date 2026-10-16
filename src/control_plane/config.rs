use super::*;

/// The loopback TCP port the listener binds, from 1 to 65535.
pub const PORT_VARIABLE: &str = "OCG_CONTROL_PORT";
/// The bearer token every request must present. It is never logged.
pub const TOKEN_VARIABLE: &str = "OCG_CONTROL_TOKEN";
/// The directory of the exported frontend the listener serves to the
/// operator's browser. Optional: without it no file is served and no
/// browser signs in.
pub const UI_VARIABLE: &str = "OCG_UI_DIR";

/// The version of the JSON this listener answers.
pub const API_VERSION: u32 = 1;

/// The shortest token accepted, in characters.
pub const MIN_TOKEN_CHARS: usize = 32;
/// The longest token accepted, in characters.
const MAX_TOKEN_CHARS: usize = 512;
/// The fewest distinct characters a token may have.
const MIN_TOKEN_DISTINCT: usize = 8;

/// The largest submission body, in bytes.
pub const MAX_SUBMISSION_BYTES: usize = 64 * 1024;

/// The most outputs one page returns, and the default.
pub const MAX_OUTPUT_PAGE: usize = 100;
pub(super) const DEFAULT_OUTPUT_PAGE: usize = 50;

/// The longest path and query a request may have, in bytes.
pub(super) const MAX_PATH_BYTES: usize = 256;
pub(super) const MAX_QUERY_BYTES: usize = 128;
/// The most headers, and header bytes, a request may have.
pub(super) const MAX_HEADERS: usize = 32;
pub(super) const MAX_HEADER_BYTES: usize = 8 * 1024;

/// The largest response body, by default.
const RESPONSE_BYTES: usize = 1024 * 1024;

/// How long a submission waits for its admission before it answers
/// `pending`. The admission runs on either way.
const ADMISSION_WAIT: Duration = Duration::from_secs(3);

/// The bearer token. Its value is never displayed.
#[derive(Clone)]
pub(super) struct Token(pub(super) Arc<str>);

impl fmt::Debug for Token {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Token(<redacted>)")
    }
}

impl Token {
    /// Whether `presented` is the token, compared in constant time over
    /// digests, so neither its content nor its length is timed.
    pub(super) fn matches(&self, presented: &[u8]) -> bool {
        let expected = digest(&SHA256, self.0.as_bytes());
        let presented = digest(&SHA256, presented);
        verify_slices_are_equal(expected.as_ref(), presented.as_ref()).is_ok()
    }
}

/// Why the Control Plane configuration is refused. It never repeats the
/// token.
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

/// What the operator configured for the Control Plane.
#[derive(Clone, Debug)]
pub struct ControlPlaneConfig {
    pub(super) port: u16,
    pub(super) token: Token,
    /// The directory of the exported frontend, when one is served.
    pub(super) ui_dir: Option<PathBuf>,
    /// The largest response body.
    pub(super) response_bytes: usize,
    /// How long a submission waits for its admission.
    pub(super) admission_wait: Duration,
    /// The bounds of event streams.
    pub(super) stream_limits: events::StreamLimits,
}

impl ControlPlaneConfig {
    /// The configuration in OCG's environment: `None` when neither variable
    /// is set, which leaves the Control Plane off.
    ///
    /// # Errors
    ///
    /// Returns why a configuration that was given is refused.
    pub fn from_environment() -> Result<Option<Self>, ConfigError> {
        Self::from_source(|name| std::env::var_os(name).map(|value| value.into_string()))
    }

    /// The configuration in the variables `source` gives; a value that is
    /// not Unicode is `Some(Err(_))`.
    ///
    /// # Errors
    ///
    /// As [`Self::from_environment`].
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

    /// A configuration of `port` and `token`, checked as the environment's
    /// is, except that port 0, any free port, is allowed.
    ///
    /// # Errors
    ///
    /// [`ConfigError::InvalidToken`] for a token that is refused.
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

    /// The configured port; 0 for any free one.
    pub fn port(&self) -> u16 {
        self.port
    }
}
