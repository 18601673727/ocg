/**
 * Control Centre status colours and words.
 *
 * Profile health and route status are the Control Centre's own readings, so
 * their tones live here rather than in the shared status maps, still in the
 * shared `Tone` vocabulary. Model availability is not here: it is the
 * bootstrap's fact and comes from `bootstrap/presentation`.
 */

import type { BootstrapCapabilitySupport, BootstrapProviderState } from "../bootstrap/types";
import type { Tone } from "@/components/ocg/primitives";
import type { ProfileHealth, RouteStatus } from "./domain";

export const HEALTH_TONE: Record<ProfileHealth, Tone> = {
  healthy: "emerald",
  degraded: "amber",
  unavailable: "red",
  incomplete: "violet",
  unknown: "slate",
};

export const HEALTH_LABEL: Record<ProfileHealth, string> = {
  healthy: "Healthy",
  degraded: "Degraded",
  unavailable: "Unavailable",
  incomplete: "Incomplete",
  unknown: "Unknown",
};

export const ROUTE_TONE: Record<RouteStatus, Tone> = {
  ready: "emerald",
  degraded: "amber",
  fallback: "amber",
  unavailable: "red",
  pending: "violet",
  unknown: "slate",
  unassigned: "violet",
  "auth-required": "sky",
};

export const ROUTE_LABEL: Record<RouteStatus, string> = {
  ready: "Ready",
  degraded: "Degraded",
  fallback: "Fallback",
  unavailable: "Unavailable",
  pending: "Pending",
  unknown: "Unknown",
  unassigned: "Unassigned",
  "auth-required": "Auth required",
};

export const PROVIDER_STATE_TONE: Record<BootstrapProviderState, Tone> = {
  connected: "emerald",
  "auth-required": "sky",
  degraded: "amber",
  unavailable: "red",
  unknown: "slate",
};

export const CAPABILITY_TONE: Record<BootstrapCapabilitySupport, Tone> = {
  supported: "emerald",
  unsupported: "red",
  unknown: "slate",
};

export const CAPABILITY_LABEL: Record<BootstrapCapabilitySupport, string> = {
  supported: "Supported",
  unsupported: "Unsupported",
  unknown: "Unknown",
};

