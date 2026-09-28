//! Trusted verification commands.
//!
//! A command is always a structured `program` plus an argument vector. The
//! convenient string form (`"cargo check"`) is parsed here with a strict,
//! deterministic word splitter — never through `sh -c`. Shell control
//! operators, pipelines, redirections, command substitution and backticks are
//! rejected, so a command string can only ever describe a single program
//! invocation with literal arguments.
//!
//! The object form `{"program": "cargo", "args": ["test"]}` skips parsing and
//! takes the arguments literally, but it is validated too: shell-interpreter
//! escape hatches (`sh -c`, `bash -lc`, `cmd /c`, `powershell -Command`, ...)
//! and control characters are rejected in both forms. Direct execution of a
//! script path (for example `{"program": "./tools/check.sh"}`) remains allowed.

use crate::error::{OcgError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A single external command, stored as a program and an argument vector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandSpec {
    pub program: String,
    #[serde(default)]
    pub args: Vec<String>,
}

impl CommandSpec {
    pub fn new(program: impl Into<String>, args: impl IntoIterator<Item = String>) -> Self {
        Self {
            program: program.into(),
            args: args.into_iter().collect(),
        }
    }

    /// Parse one configured command value: a string word list or an object.
    pub fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::String(text) => Self::parse(text),
            Value::Object(object) => Self::from_object(object),
            _ => Err(OcgError::config(
                "verification command must be a string or {\"program\": ..., \"args\": [...]}",
            )),
        }
    }

    fn from_object(object: &serde_json::Map<String, Value>) -> Result<Self> {
        let program = object
            .get("program")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                OcgError::config("verification command object needs a string 'program'")
            })?
            .trim()
            .to_string();
        let mut args = Vec::new();
        if let Some(values) = object.get("args") {
            let array = values.as_array().ok_or_else(|| {
                OcgError::config("verification command 'args' must be a list of strings")
            })?;
            for value in array {
                let arg = value.as_str().ok_or_else(|| {
                    OcgError::config("verification command 'args' must contain only strings")
                })?;
                args.push(arg.to_string());
            }
        }
        let command = Self { program, args };
        command.validate()?;
        Ok(command)
    }

    /// Parse a convenient command string without invoking a shell.
    pub fn parse(text: &str) -> Result<Self> {
        let words = split_words(text)?;
        if words.is_empty() {
            return Err(OcgError::config("verification command must not be empty"));
        }
        let mut words = words.into_iter();
        let command = Self {
            program: words.next().unwrap_or_default(),
            args: words.collect(),
        };
        command.validate()?;
        Ok(command)
    }

    fn validate(&self) -> Result<()> {
        if self.program.trim().is_empty() {
            return Err(OcgError::config(
                "verification command program must not be empty",
            ));
        }
        for value in std::iter::once(&self.program).chain(self.args.iter()) {
            if value.contains('\0') {
                return Err(OcgError::config(
                    "verification command must not contain a NUL byte",
                ));
            }
            if value.chars().any(char::is_control) {
                return Err(OcgError::config(
                    "verification command must not contain control characters (including newlines); a raw log header could be forged otherwise",
                ));
            }
        }
        reject_shell_interpreter(&self.program, &self.args)?;
        Ok(())
    }

    /// A display form used in reports and logs. Arguments are shown verbatim.
    pub fn display(&self) -> String {
        if self.args.is_empty() {
            return self.program.clone();
        }
        format!("{} {}", self.program, self.args.join(" "))
    }
}

/// Split a command string into words with shell-like quoting but no shell.
///
/// Supported: single quotes (fully literal), double quotes (literal except for
/// backslash escapes) and backslash escapes. Everything else is a literal
/// character. Any shell metacharacter is rejected rather than interpreted.
fn split_words(text: &str) -> Result<Vec<String>> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut has_word = false;
    let mut chars = text.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '\'' => {
                has_word = true;
                loop {
                    match chars.next() {
                        Some('\'') => break,
                        Some(inner) => current.push(inner),
                        None => {
                            return Err(OcgError::config(
                                "verification command has an unterminated single quote",
                            ))
                        }
                    }
                }
            }
            '"' => {
                has_word = true;
                loop {
                    match chars.next() {
                        Some('"') => break,
                        Some('\\') => match chars.next() {
                            Some(escaped) => current.push(escaped),
                            None => {
                                return Err(OcgError::config(
                                    "verification command ends with a dangling escape",
                                ))
                            }
                        },
                        Some(inner) => current.push(inner),
                        None => {
                            return Err(OcgError::config(
                                "verification command has an unterminated double quote",
                            ))
                        }
                    }
                }
            }
            '\\' => {
                has_word = true;
                match chars.next() {
                    Some(escaped) => current.push(escaped),
                    None => {
                        return Err(OcgError::config(
                            "verification command ends with a dangling escape",
                        ))
                    }
                }
            }
            _ if ch.is_whitespace() => {
                if has_word {
                    words.push(std::mem::take(&mut current));
                    has_word = false;
                }
            }
            _ => {
                reject_metacharacter(ch)?;
                has_word = true;
                current.push(ch);
            }
        }
    }
    if has_word {
        words.push(current);
    }
    Ok(words)
}

/// Reject the characters a shell would treat specially. The parser never runs
/// a shell, so these can only be an attempt at shell syntax and are refused.
fn reject_metacharacter(ch: char) -> Result<()> {
    let forbidden = match ch {
        ';' | '|' | '&' | '<' | '>' | '`' => true,
        // Command / arithmetic / parameter substitution openings. A bare `$`
        // is harmless without a shell but is rejected as well to stay strict.
        '$' => true,
        _ => false,
    };
    if forbidden {
        return Err(OcgError::config(format!(
            "verification command contains a forbidden shell operator: '{ch}' (commands are run without a shell)"
        )));
    }
    Ok(())
}

/// The final path component of a program, lowercased and without `.exe`.
fn interpreter_basename(program: &str) -> String {
    let name = program
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(program)
        .to_ascii_lowercase();
    name.strip_suffix(".exe").unwrap_or(&name).to_string()
}

/// Whether an argument is a POSIX-shell command switch (`-c`, `-lc`, `-ec`,
/// ...). Long options such as `--color` are not command switches.
fn is_posix_command_switch(arg: &str) -> bool {
    if !arg.starts_with('-') || arg.starts_with("--") {
        return false;
    }
    let cluster = &arg[1..];
    !cluster.is_empty()
        && cluster.chars().all(|ch| ch.is_ascii_alphabetic())
        && cluster.contains('c')
}

/// Whether an argument is a PowerShell command / encoded-command switch.
fn is_powershell_command_switch(arg: &str) -> bool {
    let normalized = arg.trim_start_matches(['-', '/']).to_ascii_lowercase();
    matches!(
        normalized.as_str(),
        "command" | "c" | "encodedcommand" | "enc" | "e" | "ec"
    )
}

/// Reject a shell/interpreter invoked with a command-execution switch. The
/// check is on the program *basename* and only when the dangerous switch is
/// present, so `{"program": "./tools/check.sh"}` and `bash script.sh` remain
/// direct structured executions.
fn reject_shell_interpreter(program: &str, args: &[String]) -> Result<()> {
    let name = interpreter_basename(program);
    let is_posix_shell = matches!(
        name.as_str(),
        "sh" | "bash" | "dash" | "zsh" | "ksh" | "ash" | "busybox"
    );
    let is_cmd = matches!(name.as_str(), "cmd");
    let is_powershell = matches!(name.as_str(), "powershell" | "pwsh");

    for arg in args {
        let lower = arg.to_ascii_lowercase();
        let dangerous = (is_posix_shell && is_posix_command_switch(&lower))
            || (is_cmd && matches!(lower.as_str(), "/c" | "/k"))
            || (is_powershell && is_powershell_command_switch(&lower));
        if dangerous {
            return Err(OcgError::config(format!(
                "verification command must not invoke a shell interpreter with a command switch: '{} {}'",
                program, arg
            )));
        }
    }
    Ok(())
}
