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
  const { locale, setLocale } = useI18n();

  if (variant === "select") {
    return (
      <label className={cn("inline-flex items-center gap-2 text-[11px]", className)}>
        <span className="sr-only">Language</span>
        <select
          value={locale}
          onChange={(event) => {
            const next = event.target.value as Locale;
            setLocale(next);
          }}
          className="rounded-md border border-border bg-background px-2 py-1 text-[11px] text-foreground"
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
      aria-label="Language"
      className={cn("mt-2 flex flex-wrap gap-1", className)}
    >
      {LOCALES.map((option) => (
        <button
          key={option}
          type="button"
          aria-pressed={locale === option}
          onClick={() => setLocale(option)}
          className={cn(
            "rounded-md border px-2 py-1 text-[10px] transition-colors",
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
