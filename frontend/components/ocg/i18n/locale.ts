export type Locale = "en-US" | "zh-CN";

export const LOCALES: readonly Locale[] = ["en-US", "zh-CN"];

export const DEFAULT_LOCALE: Locale = "en-US";

export const LOCALE_STORAGE_KEY = "ocg.locale.v1";

export const LOCALE_LABEL: Record<Locale, string> = {
  "en-US": "English (US)",
  "zh-CN": "简体中文",
};

export const LOCALE_SHORT_LABEL: Record<Locale, string> = {
  "en-US": "EN",
  "zh-CN": "中文",
};

export function isLocale(value: unknown): value is Locale {
  return value === "en-US" || value === "zh-CN";
}

export function resolveLocale(value: unknown): Locale {
  if (typeof value !== "string") return DEFAULT_LOCALE;
  const normalized = value.replace("_", "-").toLowerCase();
  if (
    normalized === "zh-cn" ||
    normalized === "zh-hans" ||
    normalized === "zh" ||
    normalized.startsWith("zh-")
  ) {
    return "zh-CN";
  }
  if (normalized.startsWith("en")) return "en-US";
  return DEFAULT_LOCALE;
}

export function readStoredLocale(
  storage: Pick<Storage, "getItem"> | null | undefined,
): Locale | null {
  try {
    const raw = storage?.getItem(LOCALE_STORAGE_KEY);
    if (!raw) return null;
    const resolved = resolveLocale(raw.trim().replace(/^"|"$/g, ""));
    return raw.trim().length > 0 ? resolved : null;
  } catch {
    return null;
  }
}

export function detectLocale(
  storage: Pick<Storage, "getItem"> | null | undefined,
  navigatorLanguage?: string | null,
): Locale {
  return readStoredLocale(storage) ?? resolveLocale(navigatorLanguage ?? DEFAULT_LOCALE);
}
