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
//! - provider protocol boundaries (OpenAI native, Anthropic native,
//!   OpenAI-compatible)
//! - tools (future)
//! - conversation (future)
//! - PWA/control plane
//!
//! OCG does not depend on any external agent runtime to work.

pub mod anthropic;
pub mod call_recovery;
pub mod capabilities;
pub(crate) mod chat_images;
pub mod cli;
pub mod clock;
pub mod compaction;
pub mod compiler_feedback;
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
pub mod profile;
pub mod project;
pub mod prompt;
pub mod provider_context;
pub mod provider_loop;
pub mod provider_protocol;
pub mod proxy;
pub mod pwa;
pub mod release;
pub mod report;
pub mod resources;
pub mod self_update;
pub mod setup;
pub mod skills;
pub mod telemetry;
pub mod ui_assets;
pub mod validate;
pub mod vault;
pub mod verification;
pub mod yaml;
