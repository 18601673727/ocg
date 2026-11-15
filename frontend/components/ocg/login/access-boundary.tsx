"use client";

import { createContext, useContext, type ReactNode } from "react";

const RemoteLogout = createContext<(() => Promise<void>) | null>(null);
export function SessionActions({
  onSignOut,
  children,
}: {
  onSignOut: () => Promise<void>;
  children: ReactNode;
}) {
  return <RemoteLogout.Provider value={onSignOut}>{children}</RemoteLogout.Provider>;
}
export function useRemoteLogout() {
  return useContext(RemoteLogout);
}
export function useAccountEmail(): string | null {
  return null;
}
