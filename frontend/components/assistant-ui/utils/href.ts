export function safeHref(value?: string) {
  if (!value) return undefined;
  try {
    const url = new URL(value, window.location.href);
    return ["http:", "https:", "mailto:"].includes(url.protocol) ? value : undefined;
  } catch {
    return undefined;
  }
}
export function hostOf(value?: string) {
  try {
    return value ? new URL(value).hostname : undefined;
  } catch {
    return undefined;
  }
}
