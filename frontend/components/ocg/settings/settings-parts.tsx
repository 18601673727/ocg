import { Input } from "@/components/ui/input";
import { type ElementProps, splitProps, type View } from "@/lib/ui-state";
import { cn } from "@/lib/utils";
import type { LlmProfile } from "../contracts";
import type { I18nKey, TranslateFn } from "../i18n";
import { ApiError } from "../runtime/api-client";

export function ApiKeyInput(viewProps: Omit<ElementProps<"input">, "type">) {
  const [, props] = splitProps(viewProps, ["class"]);

  return (
    <Input
      {...props}
      type="text"
      class={cn("[&:not(:placeholder-shown)]:[-webkit-text-security:disc]", viewProps.class)}
      autocomplete="off"
      autocapitalize="none"
      autocorrect="off"
      spellcheck="false"
    />
  );
}

export function Section(props: {
  id: string;
  title: string;
  description: string;
  action?: View;
  children: View;
}) {
  return (
    <section aria-labelledby={props.id} class="rounded-xl border border-border bg-card p-4 sm:p-6">
      <div class="flex flex-wrap items-start justify-between gap-3 border-b border-border pb-4">
        <div class="min-w-0 flex-[1_1_15rem]">
          <h2 id={props.id} class="text-lg font-semibold tracking-tight">
            {props.title}
          </h2>
          <p class="mt-1 max-w-prose text-sm leading-6 text-muted-foreground">
            {props.description}
          </p>
        </div>
        {props.action}
      </div>
      <div class="space-y-4 pt-4">{props.children}</div>
    </section>
  );
}

export function describeError(error: unknown, t: TranslateFn): string {
  if (error instanceof ApiError) {
    if (error.code === "executor_unavailable") return t("settings.error.executorUnavailable");
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
  if (error instanceof ApiError && error.code in known) {
    const key = known[error.code];
    if (key) {
      return t(key, { detail: error.detail });
    }
  }
  return describeError(error, t);
}

export function selectableLlmProfiles(
  profiles: readonly LlmProfile[],
  ...selected: (string | null | undefined)[]
): LlmProfile[] {
  return profiles.filter((profile) => profile.enabled || selected.includes(profile.id));
}
