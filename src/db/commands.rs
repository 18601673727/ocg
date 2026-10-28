use super::*;

pub(super) type Reply<T> = flume::Sender<Result<T, DatabaseError>>;

pub(super) enum Message {
    Run(Box<Envelope>),
    Shutdown,
}

/// Write-worker requests that are not commands. They have their own queue,
/// so a full command queue never delays them, and the worker takes them
/// before its next command.
pub(super) enum Control {
    FilesystemFailure {
        reason: ProtectionReason,
        reply: flume::Sender<bool>,
    },
    Recover,
    CompactStorage(flume::Sender<Result<CompactionOutcome, MaintenanceError>>),
}

impl Control {
    pub(super) fn refuse_policy(self, stopped: bool) {
        match self {
            Self::CompactStorage(reply) => {
                reply
                    .send(Err(if stopped {
                        MaintenanceError::WorkerStopped
                    } else {
                        MaintenanceError::ShuttingDown
                    }))
                    .ok();
            }
            Self::Recover => {}
            Self::FilesystemFailure { reply, .. } => {
                reply.send(false).ok();
            }
        }
    }
}

/// An admitted command with its execution mark, which the worker sets before
/// the command touches storage. The submitter keeps the other reference, so
/// when the reply is lost it can tell a command that never began from one
/// that began.
pub(super) struct Envelope {
    pub(super) operation: OperationId,
    pub(super) started: Arc<AtomicBool>,
    pub(super) command: Command,
}

pub(super) struct Command {
    action: Box<dyn PendingCommand>,
}

trait PendingCommand: Send {
    fn expected_write_bytes(&self) -> u64;
    fn execute(
        self: Box<Self>,
        database: &Database,
        operation: OperationId,
        commit: Commit,
    ) -> Executed;
    fn refuse(self: Box<Self>, error: DatabaseError);
}

struct CommandData<P, T, F> {
    payload: P,
    reply: Reply<T>,
    estimate: fn(&P) -> u64,
    execute: F,
}

impl<
    P: Send + 'static,
    T: Send + 'static,
    F: FnOnce(P, Reply<T>, &Database, OperationId, Commit) -> Executed + Send + 'static,
> PendingCommand for CommandData<P, T, F>
{
    fn expected_write_bytes(&self) -> u64 {
        (self.estimate)(&self.payload)
            .saturating_mul(2)
            .saturating_add(TRANSACTION_ALLOWANCE_BYTES)
    }
    fn execute(
        self: Box<Self>,
        database: &Database,
        operation: OperationId,
        commit: Commit,
    ) -> Executed {
        (self.execute)(self.payload, self.reply, database, operation, commit)
    }
    fn refuse(self: Box<Self>, error: DatabaseError) {
        self.reply.send(Err(error)).ok();
    }
}

impl Command {
    pub(super) fn new<P: Send + 'static, T: Send + 'static>(
        payload: P,
        reply: Reply<T>,
        estimate: fn(&P) -> u64,
        kind: OperationKind,
        body: fn(P, &mut redb::Batch) -> Result<T, BatchError>,
    ) -> Self {
        Self::custom(
            payload,
            reply,
            estimate,
            move |payload, reply, database, operation, commit| {
                run(database, operation, commit, kind, reply, |batch| {
                    body(payload, batch)
                })
            },
        )
    }
    pub(super) fn custom<P: Send + 'static, T: Send + 'static>(
        payload: P,
        reply: Reply<T>,
        estimate: fn(&P) -> u64,
        execute: impl FnOnce(P, Reply<T>, &Database, OperationId, Commit) -> Executed + Send + 'static,
    ) -> Self {
        Self {
            action: Box::new(CommandData {
                payload,
                reply,
                estimate,
                execute,
            }),
        }
    }
    pub(super) fn expected_write_bytes(&self) -> u64 {
        self.action.expected_write_bytes()
    }
    pub(super) fn execute(
        self,
        database: &Database,
        operation: OperationId,
        commit: Commit,
    ) -> Executed {
        self.action.execute(database, operation, commit)
    }
    pub(super) fn refuse(self, error: impl Fn() -> DatabaseError) {
        self.action.refuse(error());
    }
}

// At most three gram keys per lowercased scalar; each 53-byte key has
// an additional allowance for its index entry. The command doubles this too.
pub(super) fn search_index_bytes(text: &str) -> u64 {
    byte_count(text.chars().flat_map(char::to_lowercase).count())
        .saturating_mul(3)
        .saturating_mul(64)
}

pub(super) fn compaction_bytes(policy: &Option<crate::db::entities::CompactionPolicy>) -> u64 {
    policy.as_ref().map_or(0, |policy| {
        256u64.saturating_add(
            policy
                .predictive
                .as_ref()
                .and_then(|prediction| prediction.acceptance.as_ref())
                .map_or(0, |pin| {
                    byte_count(pin.reported_model.len())
                        .saturating_add(byte_count(pin.confidence_semantics.len()))
                }),
        )
    })
}

pub(super) fn command_bytes(commands: &[ProjectCommand]) -> u64 {
    commands.iter().fold(0, |total, command| {
        command.argv.iter().fold(
            total
                .saturating_add(byte_count(command.name.len()))
                .saturating_add(19),
            |total, entry| total.saturating_add(byte_count(entry.len())),
        )
    })
}

pub(super) fn byte_count(length: usize) -> u64 {
    u64::try_from(length).unwrap_or(u64::MAX)
}

pub(super) fn text_bytes(text: &Option<String>) -> u64 {
    text.as_ref().map_or(0, |text| byte_count(text.len()))
}

pub(super) fn json_bytes(value: &Option<serde_json::Value>) -> u64 {
    value
        .as_ref()
        .map_or(0, |value| counted_json_bytes(value, ByteCounter::default()))
}

/// Measures the compact serialization of `value` on top of what `counter`
/// already holds, without retaining the serialized bytes. An overflow or an
/// unexpected serialization failure answers `u64::MAX`, never a short count.
pub(super) fn counted_json_bytes(value: &serde_json::Value, mut counter: ByteCounter) -> u64 {
    match serde_json::to_writer(&mut counter, value) {
        Ok(()) => counter.bytes,
        Err(_) => u64::MAX,
    }
}

/// A write sink that only counts the bytes written to it.
#[derive(Default)]
pub(super) struct ByteCounter {
    pub(super) bytes: u64,
}

impl Write for ByteCounter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(byte_count(buf.len()))
            .ok_or_else(|| std::io::Error::other("serialized byte count overflowed"))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) fn output_bytes(
    summary: &Option<String>,
    code: &Option<String>,
    value: &Option<serde_json::Value>,
) -> u64 {
    text_bytes(summary)
        .saturating_add(text_bytes(code))
        .saturating_add(json_bytes(value))
}

pub(super) fn entry_bytes(
    first: &Entry,
    format: &Option<crate::db::entities::ResponseFormat>,
) -> u64 {
    byte_count(format!("{first:?}").len())
        .saturating_add(match first {
            Entry::UserMessage { text, attachments } => {
                search_index_bytes(text).saturating_add((attachments.len() as u64).saturating_mul(
                    3 * 64 * (crate::db::entities::MAX_ATTACHMENT_NAME_BYTES as u64 + 1),
                ))
            }
            Entry::AgentMessage { text, .. } => search_index_bytes(text),
            Entry::Notice { .. }
            | Entry::Decision { .. }
            | Entry::RetryOf { .. }
            | Entry::Iteration { .. }
            | Entry::UserMessageEdit { .. }
            | Entry::UserMessageWithdraw { .. }
            | Entry::ContextPreparation { .. }
            | Entry::ContextUse { .. }
            | Entry::ResponseFormat { .. } => 0,
        })
        .saturating_add(format.as_ref().map_or(0, |format| {
            byte_count(redb::encode_response_format(format).len())
        }))
        .saturating_add(match first {
            Entry::UserMessage { text, .. } | Entry::AgentMessage { text, .. }
                if text.len() <= 64 * 1024 =>
            {
                byte_count(text.len())
                    .saturating_mul(6)
                    .saturating_add(64 * 1024)
            }
            _ => 0,
        })
}

pub(super) fn config_bytes(config: &Config) -> u64 {
    byte_count(config.name.len()).saturating_add(match &config.body {
        ConfigBody::Agent(agent) => text_bytes(&agent.instructions)
            .saturating_add(compaction_bytes(&agent.compaction_policy))
            .saturating_add(byte_count(agent.models.len()).saturating_mul(8)),
        ConfigBody::Model(model) => byte_count(model.model.len()),
        ConfigBody::Connection(connection) => {
            text_bytes(&connection.endpoint).saturating_add(text_bytes(&connection.credential_ref))
        }
        ConfigBody::Pool(_) => 32,
        ConfigBody::DecisionModel(dm) => byte_count(dm.model.len())
            .saturating_add(2048)
            .saturating_add(16)
            .saturating_add(byte_count(dm.declared_capabilities.len()))
            .saturating_add(32),
        ConfigBody::DecisionAgent(da) => text_bytes(&da.instructions)
            .saturating_add(byte_count(da.models.len().saturating_mul(16)))
            .saturating_add(32),
    })
}
