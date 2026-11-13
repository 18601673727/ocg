"use client";

import type * as Api from "@/components/ocg/contracts/api";

import { useEffect, useRef, useState } from "react";

import { LoaderCircle, RotateCw } from "lucide-react";

import { AccessScreen } from "../login/access-screen";
import { Button } from "@/components/ui/button";
import {
  ControlTransportError,
  createHttpCanonicalControlClient,
  type HttpCanonicalControlClient,
} from "@/components/ocg/runtime/control-client";
import { OperatorLogin, type OperatorSession } from "@/components/ocg/login/operator-login";
import { OcgRuntimeProvider } from "../runtime/runtime-context";
import { SessionActions } from "../login/access-boundary";
import { AppShell } from "@/components/ocg/layout/app-shell";
import { useI18n } from "../i18n";

type Phase = "checking" | "out" | "in" | "unreachable";

type Health = Api.Health;

export function OperatorConsole() {
  const { t } = useI18n();
  const [client, setClient] = useState<HttpCanonicalControlClient | null>(null);
  const [phase, setPhase] = useState<Phase>("checking");
  const [session, setSession] = useState<OperatorSession | null>(null);
  const [health, setHealth] = useState<Health | null>(null);
  const [signingOut, setSigningOut] = useState(false);
  const [signOutError, setSignOutError] = useState<string | null>(null);
  const csrf = useRef("");

  useEffect(() => {
    const created = createHttpCanonicalControlClient({
      baseUrl: window.location.origin,
      fetch: window.fetch.bind(window),
      csrfToken: () => csrf.current,
    });
    setClient(created);
    created
      .readSession()
      .then((signed) => {
        csrf.current = signed.csrfToken;
        setSession(signed);
        setPhase("in");
      })
      .catch((error: unknown) => {
        if (error instanceof ControlTransportError && error.status === 401) {
          setPhase("out");
        } else {
          setPhase("unreachable");
        }
      });
    return () => created.close();
  }, []);

  useEffect(() => {
    if (phase !== "in" || !client) return;
    let alive = true;
    let reading = false;
    const unauthorized = (error: unknown) =>
      error instanceof ControlTransportError && error.status === 401;
    const signedOut = () => {
      csrf.current = "";
      setSession(null);
      setPhase("out");
    };
    const read = async () => {
      if (reading) return;
      reading = true;
      try {
        const answer = await client.readHealth();
        if (alive) setHealth(answer);
      } catch (error: unknown) {
        if (!alive) return;
        setHealth(null);
        if (!unauthorized(error)) return;
        let renewed: OperatorSession;
        try {
          renewed = await client.readSession();
        } catch {
          if (alive) signedOut();
          return;
        }
        if (!alive) return;
        csrf.current = renewed.csrfToken;
        setSession(renewed);
        try {
          const answer = await client.readHealth();
          if (alive) setHealth(answer);
        } catch (retry: unknown) {
          if (alive && unauthorized(retry)) signedOut();
        }
      } finally {
        reading = false;
      }
    };
    const unsubscribe = client.subscribeCatalog(
      () => void read(),
      () => {
        if (alive) setHealth(null);
      },
    );
    const timer = setInterval(() => void read(), 5000);
    void read();
    return () => {
      alive = false;
      clearInterval(timer);
      unsubscribe();
    };
  }, [client, phase]);

  function signedIn(next: OperatorSession) {
    setSignOutError(null);
    csrf.current = next.csrfToken;
    setSession(next);
    setPhase("in");
  }

  async function signOut() {
    if (!client || signingOut) return;
    setSigningOut(true);
    setSignOutError(null);
    try {
      await client.deleteSession();
    } catch (error: unknown) {
      if (!(error instanceof ControlTransportError && error.status === 401)) {
        const code = error instanceof ControlTransportError ? error.code : "invalid_response";
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

  if (phase === "unreachable") {
    return (
      <AccessScreen title={t("session.offlineTitle")} description={t("session.unreachable")}>
        <Button className="self-start" onClick={() => window.location.reload()}>
          <RotateCw aria-hidden="true" />
          {t("session.retry")}
        </Button>
      </AccessScreen>
    );
  }

  if (phase === "out" || !client) {
    return client ? <OperatorLogin client={client} onSignedIn={signedIn} /> : null;
  }

  return (
    <SessionActions onSignOut={signOut}>
      <OcgRuntimeProvider controlClient={client}>
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
