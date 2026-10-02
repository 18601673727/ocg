/**
 * Shared presentation primitives for OCG surfaces.
 *
 * These are the pieces that were previously copied into every feature folder:
 * the pill, the metric tile, the section heading, the segmented tab bar. A
 * surface may still compose its own control when the shared one does not fit,
 * but it should read this list first.
 */

export { EmptyPanel, EmptyState } from "./empty-state";
export { FilterOption, FilterSelect } from "./filter-select";
export { KeyValue, KeyValueList } from "./key-value";
export { Metric } from "./metric";
export { Panel } from "./panel";
export { ProgressBar } from "./progress-bar";
export { SegmentedTabs, type TabItem } from "./segmented-tabs";
export { SectionHeading, SectionTitle } from "./section-title";
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
export { BORDER_TONE, DOT_TONE, FILL_TONE, SURFACE_TONE, TEXT_TONE, TONE_CLASS, type Tone } from "./tone";
