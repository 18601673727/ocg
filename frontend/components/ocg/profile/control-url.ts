"use client";

import { useSyncExternalStore } from "react";


export function isLoopbackControlUrl(raw: string): boolean {
  try {
    const url = new URL(raw);
    return url.protocol === "http:" && ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname) && !url.username && !url.password && url.pathname === "/" && !url.search && !url.hash;
  } catch {
    return false;
  }
}

export function isTrustedControlUrl(raw: string): boolean {
  if (isLoopbackControlUrl(raw)) return true;
  if (typeof window === "undefined") return false;
  try {
    const url = new URL(raw);
    return url.protocol === "https:" && url.origin === window.location.origin && !url.username && !url.password && url.pathname === "/" && !url.search && !url.hash;
  } catch {
    return false;
  }
}


/** One invocation-scoped connection hint; never a source of Profile truth. */
export function useOcgControlUrl(): string | null {
  const url = useSyncExternalStore(
    (listener) => { window.addEventListener("popstate", listener); return () => window.removeEventListener("popstate", listener); },
    () => {
      const origin = window.location.origin;
      if (!isLoopbackControlUrl(origin)) {
        return isTrustedControlUrl(origin) ? origin : null;
      }
      // Demo must be explicit; a scenario never overrides a real endpoint.
      if (!process.env.NEXT_PUBLIC_OCG_CONTROL_URL && new URLSearchParams(window.location.search).get("demo") === "1") return null;
      const selected = process.env.NEXT_PUBLIC_OCG_CONTROL_URL ?? window.location.origin;
      return selected && isLoopbackControlUrl(selected) ? selected.replace(/\/$/, "") : null;
    },
    () => null,
  );
  return url;
}
