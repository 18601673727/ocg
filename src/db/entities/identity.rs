use super::*;

/// Microseconds since the Unix epoch, as recorded by the database worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, ts_rs::TS)]
pub struct Timestamp(i64);

impl Timestamp {
    pub fn now() -> Self {
        let micros = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(elapsed) => i64::try_from(elapsed.as_micros()).unwrap_or(i64::MAX),
            Err(before) => {
                i64::try_from(before.duration().as_micros()).map_or(i64::MIN, |micros| -micros)
            }
        };
        Self(micros)
    }

    pub const fn from_unix_micros(micros: i64) -> Self {
        Self(micros)
    }

    pub const fn as_unix_micros(self) -> i64 {
        self.0
    }
}

/// Parses only canonical lowercase, hyphenated UUIDv7 identities.
pub fn parse_entity_id(text: &str) -> Option<u128> {
    let id = uuid::Uuid::parse_str(text).ok()?;
    (id.hyphenated().to_string() == text
        && id.get_version_num() == 7
        && id.get_variant() == uuid::Variant::RFC4122)
        .then_some(id.as_u128())
}

macro_rules! record_id_type {
    ($name:ident, $table:literal) => {
        /// A durable UUID identity. New entities receive a monotonic UUIDv7
        /// from the database worker; sequences within an entity remain numeric.
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, ts_rs::TS)]
        pub struct $name(#[ts(type = "string")] pub(super) u128);

        impl $name {
            pub const fn new(value: u128) -> Self {
                Self(value)
            }

            pub const fn get(self) -> u128 {
                self.0
            }
        }

        impl std::str::FromStr for $name {
            type Err = &'static str;
            fn from_str(text: &str) -> Result<Self, Self::Err> {
                parse_entity_id(text)
                    .map(Self)
                    .ok_or("expected a canonical UUID")
            }
        }

        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.collect_str(self)
            }
        }
        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let text = <String as serde::Deserialize>::deserialize(deserializer)?;
                text.parse().map_err(serde::de::Error::custom)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                uuid::Uuid::from_u128(self.0).hyphenated().fmt(f)
            }
        }
    };
}

record_id_type!(JobId, "job");
record_id_type!(JobInputId, "job_input");
record_id_type!(JobOutputId, "job_output");
record_id_type!(ConfigId, "config");
record_id_type!(ExecutorId, "executor");
record_id_type!(ExecutionPoolId, "execution_pool");
record_id_type!(ProjectId, "project");
record_id_type!(ProfileId, "profile");
record_id_type!(LlmProviderId, "llm_provider");
record_id_type!(MetricsId, "metrics");
record_id_type!(AvailabilityStatId, "availability_stat");
record_id_type!(AttachmentId, "attachment");
record_id_type!(RequestEventId, "request_event");

impl From<ConfigId> for ProfileId {
    fn from(c: ConfigId) -> Self {
        ProfileId(c.0)
    }
}
impl From<ProfileId> for ConfigId {
    fn from(p: ProfileId) -> Self {
        ConfigId(p.0)
    }
}
impl From<ConfigId> for ExecutorId {
    fn from(c: ConfigId) -> Self {
        ExecutorId(c.0)
    }
}
impl From<ExecutorId> for ConfigId {
    fn from(e: ExecutorId) -> Self {
        ConfigId(e.0)
    }
}
impl From<ConfigId> for LlmProviderId {
    fn from(c: ConfigId) -> Self {
        LlmProviderId(c.0)
    }
}
impl From<LlmProviderId> for ConfigId {
    fn from(p: LlmProviderId) -> Self {
        ConfigId(p.0)
    }
}
impl From<ExecutionPoolId> for ExecutorId {
    fn from(p: ExecutionPoolId) -> Self {
        ExecutorId(p.0)
    }
}
impl From<ExecutorId> for ExecutionPoolId {
    fn from(e: ExecutorId) -> Self {
        ExecutionPoolId(e.0)
    }
}
impl From<ConfigId> for ExecutionPoolId {
    fn from(c: ConfigId) -> Self {
        ExecutionPoolId(c.0)
    }
}
impl From<ExecutionPoolId> for ConfigId {
    fn from(p: ExecutionPoolId) -> Self {
        ConfigId(p.0)
    }
}

/// The number of one Provider request within the Job that sent it, from 1
/// in the order the Job sent them. A Job may send several requests, such as
/// the rounds of a tool loop; every Metrics and AvailabilityStat record
/// belongs to exactly one of them, and evidence is reduced per request,
/// never across the requests of a Job.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(try_from = "u32", into = "u32")]
pub struct RequestNumber(u32);

impl RequestNumber {
    /// A Job's first request, and the only request of a Job recorded before
    /// requests were numbered.
    pub const FIRST: Self = Self(1);

    /// `None` for 0, which numbers no request.
    #[must_use]
    pub const fn new(value: u32) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }

    /// The request after this one; `None` when numbers ran out.
    #[must_use]
    pub fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

impl TryFrom<u32> for RequestNumber {
    type Error = &'static str;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        Self::new(value).ok_or("request number must be nonzero")
    }
}
impl From<RequestNumber> for u32 {
    fn from(value: RequestNumber) -> Self {
        value.get()
    }
}

impl fmt::Display for RequestNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "request {}", self.0)
    }
}

/// One Provider request: the Job that sent it and its number in that Job.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct RequestRef {
    pub job: JobId,
    pub request: RequestNumber,
}

impl fmt::Display for RequestRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} of {}", self.request, self.job)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RecordRef {
    Job(JobId),
    JobInput(JobInputId),
    JobOutput(JobOutputId),
    Executor(ExecutorId),
    ExecutionPool(ExecutionPoolId),
    Project(ProjectId),
    Profile(ProfileId),
    LlmProvider(LlmProviderId),
    Metrics(MetricsId),
    AvailabilityStat(AvailabilityStatId),
    Attachment(AttachmentId),
}

impl fmt::Display for RecordRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Job(id) => id.fmt(f),
            Self::JobInput(id) => id.fmt(f),
            Self::JobOutput(id) => id.fmt(f),
            Self::Executor(id) => id.fmt(f),
            Self::ExecutionPool(id) => id.fmt(f),
            Self::Project(id) => id.fmt(f),
            Self::Profile(id) => id.fmt(f),
            Self::LlmProvider(id) => id.fmt(f),
            Self::Metrics(id) => id.fmt(f),
            Self::AvailabilityStat(id) => id.fmt(f),
            Self::Attachment(id) => id.fmt(f),
        }
    }
}
