export { I18nProvider, useI18n, useT, type TranslateFn } from "./context";
export {
  DEFAULT_LOCALE,
  LOCALES,
  LOCALE_LABEL,
  LOCALE_SHORT_LABEL,
  LOCALE_STORAGE_KEY,
  detectLocale,
  isLocale,
  readStoredLocale,
  resolveLocale,
  type Locale,
} from "./locale";
export {
  dictionaries,
  en,
  zh,
  formatTemplate,
  type Dictionary,
  type I18nKey,
} from "./dictionaries";
export { LanguageSwitcher } from "./language-switcher";
export { LOCALE_BOOTSTRAP_SCRIPT } from "./locale-bootstrap";

import type { TranslateFn } from "./context";
import type { I18nKey } from "./dictionaries";

const STATE_KEYS: Record<string, I18nKey> = {
  "pending": "state.pending",
  "running": "state.running",
  "completed": "state.completed",
  "failed": "state.failed",
  "cancelled": "state.cancelled",
  "superseded": "state.superseded",
  "orphaned": "state.orphaned",
  "unknown": "state.unknown",
  "blocked": "state.blocked",
  "planned": "state.planned",
  "ready": "state.ready",
  "queued": "state.queued",
  "dispatched": "state.dispatched",
  "settled": "state.settled",
  "fenced": "state.fenced",
  "deleted": "state.deleted",
  "streaming": "state.streaming",
  "connected": "state.connected",
  "connecting": "state.connecting",
  "disconnected": "state.disconnected",
  "active": "state.active",
  "idle": "state.idle",
  "starting": "state.starting",
  "waiting": "state.waiting",
  "success": "state.success",
  "failure": "state.failure",
  "retrying": "state.retrying",
  "waiting-approval": "state.waiting-approval",
};

export function runtimeStateLabel(t: TranslateFn, state: string): string {
  const key = STATE_KEYS[state];
  return key ? t(key) : state;
}

export function runtimeStatusDetail(t: TranslateFn, status: import("../types").RuntimeStatus): string | undefined {
  if (status.detailCode === "provider-ready") return t("chat.runtimeConnected");
  if (status.detailCode === "configuration-required") return t("chat.runtimeUnconfigured");
  return status.detail;
}

export function chatFailureReason(t: TranslateFn, message: import("../types").ChatMessage): string {
  if (message.failureCode === "project-missing") return t("chat.projectMissing");
  if (message.failureCode === "configuration-required") return t("chat.runtimeUnconfigured");
  if (message.failureCode === "stream-closed") return t("chat.streamClosed");
  return message.failureReason || t("chat.failureMissing");
}
