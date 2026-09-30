/**
 * The generated wire contract and its runtime decoders.
 *
 * Rust owns the control protocol: `src/contracts.rs` declares every struct
 * that crosses the loopback boundary and `cargo run --bin ocg-rs-ts` projects
 * it into `./generated`. Nothing in the PWA hand-maintains a mirror of a Rust
 * struct, and `tests/contracts.rs` fails if the projection drifts.
 *
 * Frontend code should import from here rather than from `./generated`
 * directly, so the types and the decoders that check them stay together.
 */

export type {
  ApiErrorBody,
  ApiErrorEnvelope,
  CanonicalApiVersion,
  CanonicalConfigurationEnvelope,
  CanonicalConfigurationResponse,
  CanonicalDashboardResponse,
  CanonicalEventsEnvelope,
  CanonicalJobConfigEnvelope,
  CanonicalJobConfigResponse,
  CanonicalJobEvent,
  CanonicalJobSnapshot,
  CanonicalProjectResponse,
  CanonicalProjectsResponse,
  Candidate,
  GlobalConfiguration,
  JsonValue,
  Model,
  Origin,
  Profile,
  ProfileApiVersion,
  ProfileBootstrapRequest,
  ProfileReplaceRequest,
  ProfileView,
  ProjectConfiguration,
  ProjectConfigurationView,
  ProjectRecord,
  Provider,
} from "./generated";

export { CANONICAL_API_VERSION, PROFILE_API_VERSION } from "./generated";

export { ContractError, decode } from "./decode";
export type { DecodeResult, Decoder } from "./decode";

export {
  decodeCandidate,
  decodeProfile,
  decodeProfileView,
  decodeProvider,
  tryProfileView,
} from "./profile-decode";

export {
  decodeConfigurationAck,
  decodeConfigurationEnvelope,
  decodeConfigurationView,
  decodeDashboardResponse,
  decodeEventsEnvelope,
  decodeExecutionState,
  decodeJobConfigEnvelope,
  decodeJobConfigResponse,
  decodeJobSnapshot,
  decodeProjectRecord,
  decodeProjectResponse,
  decodeProjectsResponse,
  tryEventsEnvelope,
} from "./canonical-decode";
export type {
  ATTEMPT_STATES,
  CanonicalAttempt,
  CanonicalAttemptState,
  CanonicalCall,
  CanonicalEffectKind,
  CanonicalExecutionState,
  CanonicalJob,
  CanonicalJobState,
  ConfigurationAck,
  EFFECT_KINDS,
  JOB_STATES,
} from "./canonical-decode";
