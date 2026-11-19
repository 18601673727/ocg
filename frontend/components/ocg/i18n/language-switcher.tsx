import { Select, SelectItem } from "@/components/ui/select";
import { ariaState } from "@/lib/ui-state";
import { cn } from "@/lib/utils";
import { SELECTED_OPTION } from "../primitives/selection";
import { useI18n } from "./context";
import { LOCALE_LABEL, LOCALES, type Locale } from "./locale";

export function LanguageSwitcher(props: { variant?: "segmented" | "select"; class?: string }) {
  const i18n = useI18n();

  return (
    <>
      {(() => {
        if ((props.variant === undefined ? "segmented" : props.variant) === "select") {
          return (
            <label class={cn("inline-flex items-center gap-2 text-xs", props.class)}>
              <span class="sr-only">{i18n.t("language.title")}</span>
              <Select
                value={i18n.locale}
                items={LOCALES.map((option) => ({ value: option, label: LOCALE_LABEL[option] }))}
                onValueChange={(next) => {
                  if (next !== null) i18n.setLocale(next as Locale);
                }}
                size="sm"
                aria-label={i18n.t("language.title")}
                class="rounded-md border-border bg-background px-2 py-1 text-xs text-foreground"
              >
                {LOCALES.map((option) => (
                  <SelectItem value={option}>{LOCALE_LABEL[option]}</SelectItem>
                ))}
              </Select>
            </label>
          );
        }

        return (
          <div
            role="group"
            aria-label={i18n.t("language.title")}
            class={cn("flex flex-wrap gap-2", props.class)}
          >
            {LOCALES.map((option) => (
              <button
                type="button"
                aria-pressed={ariaState(i18n.locale === option)}
                onClick={() => i18n.setLocale(option)}
                class={cn(
                  "min-h-11 rounded-md border px-3 py-2 text-sm outline-none transition-colors focus-visible:ring-2 focus-visible:ring-ring/30",
                  i18n.locale === option
                    ? cn(SELECTED_OPTION, "font-medium")
                    : "border-border text-muted-foreground hover:bg-muted/50 hover:text-foreground",
                )}
              >
                {LOCALE_LABEL[option]}
              </button>
            ))}
          </div>
        );
      })()}
    </>
  );
}
