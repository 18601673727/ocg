import type { TranslateFn } from "../i18n";
import { ApiError } from "../runtime/api-client";
import { settingsPath } from "../settings/settings-sections";

export type SubmissionFailure = {
  message: string;
  detail: string;
  link: { href: string; label: string };
};

const CONTEXT_WINDOW = /declare a context window/i;

export function modelFieldFailure(
  cause: unknown,
  model: { id: string | undefined; name: string | undefined },
  detail: string,
  t: TranslateFn,
): SubmissionFailure | undefined {
  if (!(cause instanceof ApiError) || !model.id) return undefined;
  if (!CONTEXT_WINDOW.test(cause.detail)) return undefined;
  return {
    message: t("chat.missingField.contextWindow", { name: model.name || model.id }),
    detail,
    link: {
      href: `${settingsPath("models")}?edit=${encodeURIComponent(model.id)}&field=context_window`,
      label: t("chat.missingField.contextWindowLink"),
    },
  };
}
