/**
 * Shared presentation primitives for OCG surfaces.
 *
 * These are the pieces that were previously copied into every feature folder:
 * the pill, the metric tile, the section heading, the segmented tab bar. A
 * surface may still compose its own control when the shared one does not fit,
 * but it should read this list first.
 */

export { PageSurface } from "./page-surface";
export { StatusDot } from "./status-dot";
export {
  JOB_STATE,
  LOG_LEVEL,
  RUNTIME_AUTHORITY_LABEL,
  RUNTIME_CONNECTION,
  RUNTIME_CONNECTION_LABEL,
  SYNC_STATUS,
  TOOL_STATUS,
  WORKER_STATUS,
  type StatusVisual,
  syncStatusLabel,
} from "./status-tone";
export { Pill } from "./pill";
export { BORDER_TONE, DOT_TONE, FILL_TONE, SURFACE_TONE, TEXT_TONE, TONE_CLASS, type Tone, STATUS_BG, STATUS_TEXT, STATUS_DOT, STATUS_CLASS, type StatusTone } from "./tone";
