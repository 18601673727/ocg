import type { ReactNode } from "react";
import { ControlTransportError } from "../runtime/control-client";
import type { I18nKey, TranslateFn } from "../i18n";

export function Section({
  id,
  title,
  description,
  action,
  children,
}: {
  id: string;
  title: string;
  description: string;
  action?: ReactNode;
  children: ReactNode;
}) {
  return (
    <section
      aria-labelledby={id}
      className="rounded-lg border border-border bg-background px-3 py-2.5 sm:px-4"
    >
      <div className="flex items-start justify-between gap-3 border-b border-border pb-3">
        <div className="min-w-0">
          <h2 id={id} className="text-base font-semibold tracking-tight">
            {title}
          </h2>
          <p className="mt-1 text-[13px] leading-5 text-muted-foreground">{description}</p>
        </div>
        {action}
      </div>
      <div className="space-y-4 pt-4">{children}</div>
    </section>
  );
}

export function describeError(error: unknown, t: TranslateFn): string {
  if (error instanceof ControlTransportError) {
    return error.detail
      ? `${error.detail} (${error.code})`
      : t("settings.error.refused", { code: error.code, status: error.status });
  }
  return error instanceof Error ? error.message : t("settings.error.incomplete");
}

export function refusalText(
  t: TranslateFn,
  error: unknown,
  known: Partial<Record<string, I18nKey>>,
): string {
  if (error instanceof ControlTransportError && error.code in known) {
    const key = known[error.code];
    if (key) {
      return t(key, { detail: error.detail });
    }
  }
  return describeError(error, t);
}
