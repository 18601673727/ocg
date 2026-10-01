#![forbid(unsafe_code)]
//! OCG: project-agnostic multi-model orchestration Core and PWA control plane.
//!
//! The library resolves the OCG-owned Profile (project YAML), layers permitted
//! user policy, validates it, assembles prompts and emits deterministic
//! configuration. It also owns OCG self-update. The `ocg` binary
//! is a thin CLI over these modules.
//!
//! OCG owns:
//! - Project
//! - orchestration
//! - execution authority
//! - provider authority
//! - Call lifecycle
//! - OpenAI-compatible protocol boundary
//! - tools (future)
//! - conversation (future)
//! - PWA/control plane
//!
//! OCG does not depend on any external agent runtime to work.

pub mod capabilities;
pub mod cli;
pub mod clock;
pub mod compaction;
pub mod config;
pub mod config_command;
pub mod context;
pub mod contracts;
pub mod control_server;
pub mod core_contract;
pub mod defaults;
pub mod derived;
pub mod edit;
pub mod error;
pub mod fingerprint;
pub mod hash;
pub mod http;
pub mod install;
pub mod instructions;
pub mod json;
pub mod model;
pub mod native_tools;
pub mod observability;
pub mod observation;
pub mod openai_compatible;
pub mod orchestration;
pub mod platform;
pub mod process;
pub mod provider_loop;
pub mod profile;
pub mod project;
pub mod prompt;
pub mod proxy;
pub mod pwa;
pub mod release;
pub mod report;
pub mod resources;
pub mod self_update;
pub mod skills;
pub mod telemetry;
pub mod ui_assets;
pub mod validate;
pub mod vault;
pub mod verification;
pub mod yaml;
