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
export { Pill } from "./pill";
export { SELECTED_OPTION, SELECTED_ROW } from "./selection";
export {
  BORDER_TONE,
  DOT_TONE,
  FILL_TONE,
  SURFACE_TONE,
  TEXT_TONE,
  TONE_CLASS,
  type Tone,
} from "./tone";
