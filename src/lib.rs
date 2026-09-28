#![forbid(unsafe_code)]
//! OCG: project-agnostic multi-model orchestration for OpenCode.
//!
//! The library resolves the OCG-owned Profile (project YAML), layers permitted
//! user policy, validates it, assembles prompts and emits a deterministic
//! OpenCode compatibility config. It also owns the managed OpenCode runtime
//! (discovery, install, update checks and OCG self-update). The `ocg` binary
//! is a thin CLI over these modules.
//!
//! Fixed execution tiers were removed in v0.4.1: `throttle`, `low`/`mid`/`high`
//! and `lead-low`/`lead-mid`/`lead-high` are migration diagnostics and inert
//! test labels, never canonical configuration.

pub mod build;
pub mod capabilities;
pub mod cli;
pub mod clock;
pub mod config;
pub mod config_command;
pub mod context;
pub mod contracts;
pub mod control_server;
pub mod core_contract;
pub mod defaults;
pub mod edit;
pub mod error;
pub mod fingerprint;
pub mod http;
pub mod json;
pub mod mcp;
pub mod model;
pub mod observability;
pub mod orchestration;
pub mod platform;
pub mod preflight;
pub mod process;
pub mod profile;
pub mod project;
pub mod prompt;
pub mod provider_gateway;
pub mod provider_transport;
pub mod proxy;
pub mod pwa;
pub mod report;
pub mod reports;
pub mod resources;
pub mod runtime;
pub mod telemetry;
pub mod ui_assets;
pub mod validate;
pub mod vault;
pub mod verification;
pub mod yaml;
