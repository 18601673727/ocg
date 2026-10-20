"use client";

import { cn } from "@/lib/utils";
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
        <select
          value={locale}
          onChange={(event) => {
            const next = event.target.value as Locale;
            setLocale(next);
          }}
          className="rounded-md border border-border bg-background px-2 py-1 text-xs text-foreground"
        >
          {LOCALES.map((option) => (
            <option key={option} value={option}>
              {LOCALE_LABEL[option]}
            </option>
          ))}
        </select>
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
              ? "border-foreground/30 bg-muted font-medium text-foreground"
              : "border-border text-muted-foreground hover:bg-muted/50 hover:text-foreground",
          )}
        >
          {LOCALE_LABEL[option]}
        </button>
      ))}
    </div>
  );
}
