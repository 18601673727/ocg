//! Platform normalization.
//!
//! OCG only supports the standalone OpenCode release platforms:
//! Linux and macOS on x86_64 and arm64. Everything that asks "what is this
//! machine?" goes through [`Platform`], which keeps the mapping between the
//! canonical platform, the OpenCode release archive and the OCG release
//! artifact in one place.

use crate::error::{OcgError, Result};

/// Supported operating systems.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Os {
    Darwin,
    Linux,
}

impl Os {
    pub fn as_str(self) -> &'static str {
        match self {
            Os::Darwin => "darwin",
            Os::Linux => "linux",
        }
    }

    fn parse(raw: &str) -> Option<Self> {
        match raw.to_ascii_lowercase().as_str() {
            "darwin" | "macos" | "mac" | "osx" => Some(Os::Darwin),
            "linux" => Some(Os::Linux),
            _ => None,
        }
    }
}

/// Supported CPU architectures, normalized to two names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arch {
    X86_64,
    Arm64,
}

impl Arch {
    pub fn as_str(self) -> &'static str {
        match self {
            Arch::X86_64 => "x86_64",
            Arch::Arm64 => "arm64",
        }
    }

    fn parse(raw: &str) -> Option<Self> {
        match raw.to_ascii_lowercase().as_str() {
            "x86_64" | "amd64" | "x64" => Some(Arch::X86_64),
            "arm64" | "aarch64" => Some(Arch::Arm64),
            _ => None,
        }
    }
}

/// How a release archive is packaged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveKind {
    TarGz,
    Zip,
}

/// A normalized (operating system, architecture) pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Platform {
    pub os: Os,
    pub arch: Arch,
}

impl Platform {
    /// The platform the current binary was compiled for.
    pub fn current() -> Result<Self> {
        let os = if cfg!(target_os = "macos") {
            Os::Darwin
        } else if cfg!(target_os = "linux") {
            Os::Linux
        } else {
            return Err(OcgError::config(format!(
                "unsupported operating system '{}'; OCG supports linux and darwin",
                std::env::consts::OS
            )));
        };
        let arch = if cfg!(target_arch = "x86_64") {
            Arch::X86_64
        } else if cfg!(target_arch = "aarch64") {
            Arch::Arm64
        } else {
            return Err(OcgError::config(format!(
                "unsupported CPU architecture '{}'; OCG supports x86_64 and arm64",
                std::env::consts::ARCH
            )));
        };
        Ok(Self { os, arch })
    }

    /// Normalize user- or uname-supplied strings into a [`Platform`].
    pub fn parse(os: &str, arch: &str) -> Option<Self> {
        Some(Self {
            os: Os::parse(os)?,
            arch: Arch::parse(arch)?,
        })
    }

    pub fn os_name(&self) -> &'static str {
        self.os.as_str()
    }

    pub fn arch_name(&self) -> &'static str {
        self.arch.as_str()
    }

    /// Stable machine-readable slug, e.g. `linux-x86_64`.
    pub fn slug(&self) -> String {
        format!("{}-{}", self.os_name(), self.arch_name())
    }

    /// The standalone OpenCode release asset for this platform.
    pub fn opencode_asset(&self) -> &'static str {
        match (self.os, self.arch) {
            (Os::Linux, Arch::X86_64) => "opencode-linux-x64.tar.gz",
            (Os::Linux, Arch::Arm64) => "opencode-linux-arm64.tar.gz",
            (Os::Darwin, Arch::X86_64) => "opencode-darwin-x64.zip",
            (Os::Darwin, Arch::Arm64) => "opencode-darwin-arm64.zip",
        }
    }

    /// The OCG release artifact for this platform.
    pub fn ocg_artifact(&self) -> &'static str {
        match (self.os, self.arch) {
            (Os::Darwin, Arch::Arm64) => "ocg-darwin-arm64",
            (Os::Darwin, Arch::X86_64) => "ocg-darwin-x86_64",
            (Os::Linux, Arch::Arm64) => "ocg-linux-arm64",
            (Os::Linux, Arch::X86_64) => "ocg-linux-x86_64",
        }
    }

    pub fn archive_kind(&self) -> ArchiveKind {
        match self.os {
            Os::Linux => ArchiveKind::TarGz,
            Os::Darwin => ArchiveKind::Zip,
        }
    }
}
