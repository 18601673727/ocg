use super::*;

/// A command a Project offers its Agents by name. `argv[0]` is a bare
/// program name or an absolute path; it runs in the Project's root with the
/// same filtered environment and timeout as every other command.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize, ts_rs::TS)]
#[serde(deny_unknown_fields)]
pub struct ProjectCommand {
    pub name: String,
    pub argv: Vec<String>,
    #[serde(default)]
    pub owner: CommandOwner,
    #[serde(default)]
    pub status: CommandStatus,
    #[serde(default)]
    pub proposal: Option<JobId>,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize, ts_rs::TS,
)]
#[serde(rename_all = "snake_case")]
pub enum CommandOwner {
    #[default]
    User,
    Decision,
}

#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize, ts_rs::TS,
)]
#[serde(rename_all = "snake_case")]
pub enum CommandStatus {
    #[default]
    Allowed,
    Review,
    Blocked,
}

#[derive(Clone, Debug, PartialEq, Eq, ts_rs::TS)]
pub struct ProjectDecisionConfig {
    pub enabled: bool,
    pub decision_profile: Option<ConfigId>,
    pub minimum_confidence_millis: u16,
}

impl Default for ProjectDecisionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            decision_profile: None,
            minimum_confidence_millis: 600,
        }
    }
}

impl ProjectRecord {
    pub fn decision_policy(&self) -> ProjectDecisionConfig {
        self.decision.clone().unwrap_or_default()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Project {
    pub name: String,
    pub description: Option<String>,
    /// The canonical, UTF-8 root the control plane resolved. It is fixed
    /// for the life of the Project.
    pub root: String,
    pub commands: Vec<ProjectCommand>,
}

#[derive(Clone, Debug, PartialEq, Eq, ts_rs::TS)]
pub struct ProjectRecord {
    pub id: ProjectId,
    pub name: String,
    pub description: Option<String>,
    pub root: String,
    /// In the order given, so that their encoding is stable.
    pub commands: Vec<ProjectCommand>,
    pub decision: Option<ProjectDecisionConfig>,
    pub decision_revision: u64,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

/// The commands every Project offers with a `code` grant.
pub const BUILT_IN_COMMANDS: [&str; 2] = ["cargo_check", "cargo_test"];

/// The most active commands a Project may have.
const MAX_PROJECT_COMMANDS: usize = 32;
/// Bounds the entire command list, including retained user blocks.
const MAX_PROJECT_COMMANDS_BYTES: usize = 16 * 1024;
const MAX_COMMAND_NAME_BYTES: usize = 64;
const MAX_ARGV_ENTRIES: usize = 64;
const MAX_ARGV_ENTRY_BYTES: usize = 4096;

/// Why `commands` cannot be a Project's. Create and update share it.
pub fn validate_project_commands(commands: &[ProjectCommand]) -> Result<(), &'static str> {
    if commands
        .iter()
        .filter(|command| command.status != CommandStatus::Blocked)
        .count()
        > MAX_PROJECT_COMMANDS
    {
        return Err("a Project has at most 32 active commands");
    }
    // As stored: a 4-byte count, then each string with a 4-byte length.
    let mut encoded = 4usize;
    for (index, command) in commands.iter().enumerate() {
        encoded = encoded.saturating_add(3 + if command.proposal.is_some() { 16 } else { 0 });
        let mut name = command.name.bytes();
        let well_formed = command.name.len() <= MAX_COMMAND_NAME_BYTES
            && name.next().is_some_and(|byte| byte.is_ascii_lowercase())
            && name.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_');
        if !well_formed {
            return Err("a command name matches [a-z][a-z0-9_]{0,63}");
        }
        if BUILT_IN_COMMANDS.contains(&command.name.as_str()) {
            return Err("cargo_check and cargo_test are built in");
        }
        if commands[..index]
            .iter()
            .any(|earlier| earlier.name == command.name)
        {
            return Err("command names are unique");
        }
        if command.argv.is_empty() || command.argv.len() > MAX_ARGV_ENTRIES {
            return Err("a command has 1 to 64 argv entries");
        }
        if command.argv.iter().any(|entry| {
            entry.is_empty() || entry.len() > MAX_ARGV_ENTRY_BYTES || entry.contains('\0')
        }) {
            return Err("each argv entry is 1 to 4096 bytes without NUL");
        }
        let program = &command.argv[0];
        if program.contains('/') && !program.starts_with('/') {
            return Err("argv[0] is a bare program name or an absolute path");
        }
        encoded += 4 + command.name.len() + 4;
        encoded += command
            .argv
            .iter()
            .map(|entry| 4 + entry.len())
            .sum::<usize>();
    }
    if encoded > MAX_PROJECT_COMMANDS_BYTES {
        return Err("the commands take at most 16 KiB");
    }
    Ok(())
}
