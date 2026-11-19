import { createSubscription } from "@/lib/ui-state";

export function isLoopbackApiUrl(raw: string): boolean {
  try {
    const url = new URL(raw);
    return (
      url.protocol === "http:" &&
      ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname) &&
      !url.username &&
      !url.password &&
      url.pathname === "/" &&
      !url.search &&
      !url.hash
    );
  } catch {
    return false;
  }
}

export function isTrustedApiUrl(raw: string): boolean {
  if (isLoopbackApiUrl(raw)) return true;
  if (typeof window === "undefined") return false;
  try {
    const url = new URL(raw);
    return (
      url.protocol === "https:" &&
      url.origin === window.location.origin &&
      !url.username &&
      !url.password &&
      url.pathname === "/" &&
      !url.search &&
      !url.hash
    );
  } catch {
    return false;
  }
}

export function useOcgApiUrl() {
  const url = createSubscription(
    (listener) => {
      window.addEventListener("popstate", listener);
      return () => window.removeEventListener("popstate", listener);
    },
    () => {
      const origin = window.location.origin;
      if (!isLoopbackApiUrl(origin)) {
        return isTrustedApiUrl(origin) ? origin : null;
      }
      const selected = import.meta.env.PUBLIC_OCG_API_URL ?? origin;
      return selected && isLoopbackApiUrl(selected) ? selected.replace(/\/$/, "") : null;
    },
  );
  return url;
}
