"use client";

import { useSyncExternalStore } from "react";
import { isLoopbackControlUrl } from "./profile-client";

/** One invocation-scoped connection hint; never a source of Profile truth. */
export function useOcgControlUrl(): string | null {
  const url = useSyncExternalStore(
    (listener) => { window.addEventListener("popstate", listener); return () => window.removeEventListener("popstate", listener); },
    () => {
      const selected = process.env.NEXT_PUBLIC_OCG_CONTROL_URL ?? window.location.origin;
      return selected && isLoopbackControlUrl(selected) ? selected.replace(/\/$/, "") : null;
    },
    () => null,
  );
  return url;
}
