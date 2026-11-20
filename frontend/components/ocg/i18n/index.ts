export { I18nProvider, type TranslateFn, useI18n } from "./context";
export type { I18nKey } from "./dictionaries";
export { LanguageSwitcher } from "./language-switcher";

import type { TranslateFn } from "./context";
import type { I18nKey } from "./dictionaries";

const STATE_KEYS: Record<string, I18nKey> = {
  pending: "state.pending",
  eligible: "state.eligible",
  running: "state.running",
  cancelling: "state.cancelling",
  completed: "state.completed",
  failed: "state.failed",
  cancelled: "state.cancelled",
  superseded: "state.superseded",
  orphaned: "state.orphaned",
  unknown: "state.unknown",
  blocked: "state.blocked",
  planned: "state.planned",
  ready: "state.ready",
  queued: "state.queued",
  dispatched: "state.dispatched",
  settled: "state.settled",
  fenced: "state.fenced",
  deleted: "state.deleted",
  streaming: "state.streaming",
  connected: "state.connected",
  connecting: "state.connecting",
  disconnected: "state.disconnected",
  active: "state.active",
  idle: "state.idle",
  starting: "state.starting",
  waiting: "state.waiting",
  success: "state.success",
  failure: "state.failure",
  retrying: "state.retrying",
  "waiting-approval": "state.waiting-approval",
  waiting_for_user: "state.waiting-approval",
};

export function runtimeStateLabel(t: TranslateFn, state: string): string {
  const key = STATE_KEYS[state];
  return key ? t(key) : state;
}
