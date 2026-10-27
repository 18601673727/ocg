"use client";

import { cn } from "@/lib/utils";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { SELECTED_OPTION } from "../primitives/selection";
import { LOCALES, LOCALE_LABEL, type Locale } from "./locale";
import { useI18n } from "./context";

export function LanguageSwitcher({
  variant = "segmented",
  className,
}: {
  variant?: "segmented" | "select";
  className?: string;
}) {
  const { locale, setLocale, t } = useI18n();

  if (variant === "select") {
    return (
      <label className={cn("inline-flex items-center gap-2 text-xs", className)}>
        <span className="sr-only">{t("language.title")}</span>
        <Select
          value={locale}
          items={LOCALES.map((option) => ({ value: option, label: LOCALE_LABEL[option] }))}
          onValueChange={(next) => {
            if (next !== null) setLocale(next as Locale);
          }}
        >
          <SelectTrigger
            size="sm"
            aria-label={t("language.title")}
            className="rounded-md border-border bg-background px-2 py-1 text-xs text-foreground"
          >
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {LOCALES.map((option) => (
              <SelectItem key={option} value={option}>
                {LOCALE_LABEL[option]}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </label>
    );
  }

  return (
    <div
      role="group"
      aria-label={t("language.title")}
      className={cn("flex flex-wrap gap-2", className)}
    >
      {LOCALES.map((option) => (
        <button
          key={option}
          type="button"
          aria-pressed={locale === option}
          onClick={() => setLocale(option)}
          className={cn(
            "min-h-11 rounded-md border px-3 py-2 text-sm outline-none transition-colors focus-visible:ring-2 focus-visible:ring-ring/30",
            locale === option
              ? cn(SELECTED_OPTION, "font-medium")
              : "border-border text-muted-foreground hover:bg-muted/50 hover:text-foreground",
          )}
        >
          {LOCALE_LABEL[option]}
        </button>
      ))}
    </div>
  );
}
