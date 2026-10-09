"use client";

import { createContext, useCallback, useContext, useEffect, useState, useSyncExternalStore, type ReactNode } from "react";
import { decodeAuthenticationSession, type AuthenticationSession } from "../contracts";

const subscribe = () => () => {};
const browserLocation = () => window.location.origin;
const serverLocation = () => "";
const RemoteLogout = createContext<(() => Promise<void>) | null>(null);

export function useRemoteLogout() {
  return useContext(RemoteLogout);
}

export function AccessBoundary({ children }: { children: ReactNode }) {
  const origin = useSyncExternalStore(subscribe, browserLocation, serverLocation);
  if (!origin) return null;
  const hostname = new URL(origin).hostname;
  const local = hostname === "localhost" || hostname === "[::1]" || /^127\.(?:\d+\.){2}\d+$/.test(hostname);
  return local ? children : <RemoteAccess>{children}</RemoteAccess>;
}

function RemoteAccess({ children }: { children: ReactNode }) {
  const [session, setSession] = useState<AuthenticationSession | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [signingOut, setSigningOut] = useState(false);

  useEffect(() => {
    const controller = new AbortController();
    const refresh = async () => {
      try {
        const response = await fetch("/api/v1/auth/session", { cache: "no-store", signal: controller.signal });
        if (!response.ok || !response.headers.get("content-type")?.includes("application/json")) {
          throw new Error("Your Access session has expired or access was denied. Sign in again to continue.");
        }
        const current = decodeAuthenticationSession(await response.json());
        if (current.mode !== "cloudflare-access") throw new Error("Remote access requires Cloudflare Access authentication.");
        if (!current.user_id || current.expires_at === null || current.expires_at <= Date.now() / 1000) {
          throw new Error("Your Access session has expired. Sign in again to continue.");
        }
        if (!controller.signal.aborted) { setSession(current); setError(null); }
      } catch (cause: unknown) {
        if (!controller.signal.aborted) {
          setSession(null);
          setError(cause instanceof Error ? cause.message : "Access session unavailable.");
        }
      }
    };
    void refresh();
    const timer = setInterval(() => { void refresh(); }, 15_000);
    return () => { controller.abort(); clearInterval(timer); };
  }, []);

  useEffect(() => {
    if (session?.expires_at == null) return;
    const timer = setTimeout(() => {
      setSession(null);
      setError("Your Access session has expired. Sign in again to continue.");
    }, Math.max(0, Math.min(session.expires_at * 1000 - Date.now(), 2_147_483_647)));
    return () => clearTimeout(timer);
  }, [session?.expires_at]);

  const logout = useCallback(async () => {
    setSigningOut(true);
    try {
      const response = await fetch("/api/v1/auth/logout", { method: "POST", redirect: "manual" });
      if (response.type !== "opaqueredirect" && !response.ok && response.status !== 401) {
        throw new Error("Session could not be revoked. Retry signing out.");
      }
      setSession(null);
      window.location.assign(new URL("/cdn-cgi/access/logout", window.location.origin).href);
    } catch (cause: unknown) {
      setError(cause instanceof Error ? cause.message : "Sign out failed.");
      setSigningOut(false);
    }
  }, []);

  if (session?.remote_execution && !error && !signingOut) {
    return <RemoteLogout value={logout}>{children}</RemoteLogout>;
  }

  return (
    <main className="flex min-h-dvh items-center justify-center bg-background px-4 text-foreground">
      <section className="w-full max-w-md rounded-lg border border-border p-6">
        <h1 className="text-base font-semibold">OCG remote access</h1>
        <p role={error ? "alert" : "status"} className="mt-3 text-sm text-muted-foreground">
          {error ?? (session
            ? "Cloudflare Access authenticated your session. The remote workspace is disabled until the single-operator deployment is verified. Project ownership and provider credentials must be configured by the local operator."
            : "Checking your Cloudflare Access session…")}
        </p>
        <div className="mt-4 flex gap-3 text-sm">
          <button className="rounded border px-3 py-2" onClick={() => window.location.reload()}>Refresh access</button>
          <button className="rounded border px-3 py-2" disabled={signingOut} onClick={() => { void logout(); }}>Sign out</button>
        </div>
      </section>
    </main>
  );
}
