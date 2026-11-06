import type { ReactNode } from "react";
import type { ControlLlmProfile } from "../contracts";
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
    <section aria-labelledby={id} className="rounded-xl border border-border bg-card p-4 sm:p-6">
      <div className="flex flex-wrap items-start justify-between gap-3 border-b border-border pb-4">
        <div className="min-w-0 flex-[1_1_15rem]">
          <h2 id={id} className="text-lg font-semibold tracking-tight">
            {title}
          </h2>
          <p className="mt-1 max-w-prose text-sm leading-6 text-muted-foreground">{description}</p>
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

/** Enabled LLM profiles, plus any disabled one that is still selected. */
export function selectableLlmProfiles(
  profiles: readonly ControlLlmProfile[],
  ...selected: (string | null | undefined)[]
): ControlLlmProfile[] {
  return profiles.filter((profile) => profile.enabled || selected.includes(profile.id));
}
