"use client";

import { useSyncExternalStore } from "react";

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

export function useOcgApiUrl(): string | null {
  const url = useSyncExternalStore(
    (listener) => {
      window.addEventListener("popstate", listener);
      return () => window.removeEventListener("popstate", listener);
    },
    () => {
      const origin = window.location.origin;
      if (!isLoopbackApiUrl(origin)) {
        return isTrustedApiUrl(origin) ? origin : null;
      }
      const selected = process.env.NEXT_PUBLIC_OCG_API_URL ?? window.location.origin;
      return selected && isLoopbackApiUrl(selected) ? selected.replace(/\/$/, "") : null;
    },
    () => null,
  );
  return url;
}
