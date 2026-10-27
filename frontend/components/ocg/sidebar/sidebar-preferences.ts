"use client";

import { useMemo, useSyncExternalStore } from "react";

const key = "ocg.sidebar.pinned";
const event = "ocg-sidebar-pins";
let fallback = "[]";
let storageUnavailable = false;
function read() {
  if (storageUnavailable) return fallback;
  try {
    return window.localStorage.getItem(key) ?? fallback;
  } catch {
    return fallback;
  }
}
function subscribe(changed: () => void) {
  const storage = (value: StorageEvent) => {
    if (value.key === key || value.key === null) changed();
  };
  window.addEventListener("storage", storage);
  window.addEventListener(event, changed);
  return () => {
    window.removeEventListener("storage", storage);
    window.removeEventListener(event, changed);
  };
}

export function usePinnedConversations() {
  const raw = useSyncExternalStore(subscribe, read, () => "[]");
  const pinned = useMemo(() => {
    try {
      const value: unknown = JSON.parse(raw);
      return new Set<string>(
        Array.isArray(value)
          ? value.filter((item): item is string => typeof item === "string")
          : [],
      );
    } catch {
      return new Set<string>();
    }
  }, [raw]);
  return {
    pinned,
    toggle(id: string) {
      const next = new Set(pinned);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      fallback = JSON.stringify([...next]);
      try {
        window.localStorage.setItem(key, fallback);
      } catch {
        storageUnavailable = true;
      }
      window.dispatchEvent(new Event(event));
    },
  };
}
