"use client";

import type * as Api from "@/components/ocg/contracts/api";

import { useEffect, useRef, useState } from "react";

import { LoaderCircle, RotateCw } from "lucide-react";

import { AccessScreen } from "../login/access-screen";
import { Button } from "@/components/ui/button";
import { ApiError, createApiClient, type ApiClient } from "@/components/ocg/runtime/api-client";
import { OcgRuntimeProvider } from "../runtime/runtime-context";
import { SessionActions } from "../login/access-boundary";
import { AppShell } from "@/components/ocg/layout/app-shell";
import { useI18n } from "../i18n";

type Phase = "checking" | "out" | "in" | "unreachable";

type Health = Api.Health;
type OperatorSession = Awaited<ReturnType<ApiClient["readSession"]>>;

export function OperatorConsole() {
  const { t } = useI18n();
  const [client, setClient] = useState<ApiClient | null>(null);
  const [phase, setPhase] = useState<Phase>("checking");
  const [session, setSession] = useState<OperatorSession | null>(null);
  const [health, setHealth] = useState<Health | null>(null);
  const [signingOut, setSigningOut] = useState(false);
  const [signOutError, setSignOutError] = useState<string | null>(null);
  const csrf = useRef("");

  useEffect(() => {
    const controller = new AbortController();
    const created = createApiClient({
      baseUrl: window.location.origin,
      fetch: window.fetch.bind(window),
      csrfToken: () => csrf.current,
    });
    setClient(created);
    created
      .readSession(controller.signal)
      .then((signed) => {
        if (controller.signal.aborted) return;
        csrf.current = signed.csrfToken;
        setSession(signed);
        setPhase("in");
      })
      .catch(() => {
        if (!controller.signal.aborted) setPhase("unreachable");
      });
    return () => {
      controller.abort();
      created.close();
    };
  }, []);

  useEffect(() => {
    if (phase !== "in" || !client || signingOut) return;
    const controller = new AbortController();
    const { signal } = controller;
    let reading = false;
    const unauthorized = (error: unknown) => error instanceof ApiError && error.status === 401;
    const disconnected = () => {
      csrf.current = "";
      setSession(null);
      setPhase("unreachable");
    };
    const read = async () => {
      if (reading) return;
      reading = true;
      try {
        const answer = await client.readHealth(signal);
        if (!signal.aborted) setHealth(answer);
      } catch (error: unknown) {
        if (signal.aborted) return;
        setHealth(null);
        if (!unauthorized(error)) return;
        let renewed: OperatorSession;
        try {
          renewed = await client.readSession(signal);
        } catch {
          if (!signal.aborted) disconnected();
          return;
        }
        if (signal.aborted) return;
        csrf.current = renewed.csrfToken;
        setSession(renewed);
        try {
          const answer = await client.readHealth(signal);
          if (!signal.aborted) setHealth(answer);
        } catch (retry: unknown) {
          if (!signal.aborted && unauthorized(retry)) disconnected();
        }
      } finally {
        reading = false;
      }
    };
    const unsubscribe = client.subscribeCatalog(
      () => void read(),
      () => {
        if (!signal.aborted) setHealth(null);
      },
    );
    const timer = setInterval(() => void read(), 5000);
    void read();
    return () => {
      controller.abort();
      clearInterval(timer);
      unsubscribe();
    };
  }, [client, phase, signingOut]);

  async function signOut() {
    if (!client || signingOut) return;
    setSigningOut(true);
    setSignOutError(null);
    try {
      await client.deleteSession();
    } catch (error: unknown) {
      if (!(error instanceof ApiError && error.status === 401)) {
        const code = error instanceof ApiError ? error.code : "invalid_response";
        setSignOutError(t("session.signOutUnconfirmed", { code }));
        return;
      }
    } finally {
      setSigningOut(false);
    }
    csrf.current = "";
    setSession(null);
    setHealth(null);
    setPhase("out");
  }

  if (phase === "checking") {
    return (
      <main className="flex flex-1 items-center justify-center">
        <p role="status" className="flex items-center gap-2 text-sm text-muted-foreground">
          <LoaderCircle
            aria-hidden="true"
            className="size-4 text-primary motion-safe:animate-spin"
          />
          {t("session.checking")}
        </p>
      </main>
    );
  }

  if (phase === "unreachable" || phase === "out") {
    const signedOut = phase === "out";
    return (
      <AccessScreen
        title={t(signedOut ? "session.signedOutTitle" : "session.offlineTitle")}
        description={t(signedOut ? "session.signedOutDescription" : "session.unreachable")}
      >
        <Button className="self-start" onClick={() => window.location.reload()}>
          <RotateCw aria-hidden="true" />
          {t(signedOut ? "session.reopen" : "session.retry")}
        </Button>
      </AccessScreen>
    );
  }

  if (!client) return null;

  return (
    <SessionActions onSignOut={signOut}>
      <OcgRuntimeProvider apiClient={client}>
        <AppShell
          client={client}
          health={health}
          sessionExpiresAtUs={session?.expiresAtUs ?? null}
          signingOut={signingOut}
          signOutError={signOutError}
          onSignOut={() => void signOut()}
        />
      </OcgRuntimeProvider>
    </SessionActions>
  );
}
