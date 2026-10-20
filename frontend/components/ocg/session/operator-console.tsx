"use client";

import { useEffect, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  ControlTransportError,
  createHttpCanonicalControlClient,
  type HttpCanonicalControlClient,
} from "@/components/ocg/runtime/control-client";
import { decodeHealth } from "@/components/ocg/contracts";
import { OperatorLogin, type OperatorSession } from "@/components/ocg/login/operator-login";
import { OcgRuntimeProvider } from "../runtime/runtime-context";
import { SessionActions } from "../login/access-boundary";
import { AppShell } from "@/components/ocg/layout/app-shell";
import { useI18n } from "../i18n";

type Phase = "checking" | "out" | "in" | "unreachable";

type Health = ReturnType<typeof decodeHealth>;

/** The local operator console: one browser session on this machine. */
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
      // The CSRF token never becomes a credential of its own: it signs the
      // requests of the session cookie beside it, and nothing else.
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
    const read = () => {
      client
        .readHealth()
        .then((answer) => {
          if (alive) setHealth(answer);
        })
        .catch((error: unknown) => {
          if (!alive) return;
          if (error instanceof ControlTransportError && error.status === 401) {
            // The session ended; the console asks to sign in again.
            csrf.current = "";
            setSession(null);
            setPhase("out");
          } else {
            setHealth(null);
          }
        });
    };
    read();
    const timer = setInterval(read, 5000);
    return () => {
      alive = false;
      clearInterval(timer);
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
      // A server rejection of this session confirms it is no longer authorized.
      if (!(error instanceof ControlTransportError && error.status === 401)) {
        const code = error instanceof ControlTransportError ? error.code : "invalid_response";
        setSignOutError(
          t("session.signOutUnconfirmed", { code }),
        );
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
      <main className="flex flex-1 items-center justify-center text-sm text-muted-foreground">
        {t("session.checking")}
      </main>
    );
  }

  if (phase === "unreachable") {
    return (
      <main className="mx-auto flex w-full max-w-md flex-1 flex-col justify-center gap-4 px-6 py-16">
        <p className="text-sm text-muted-foreground">
          {t("session.unreachable")}
        </p>
        <Button variant="outline" onClick={() => window.location.reload()}>
          {t("session.retry")}
        </Button>
      </main>
    );
  }

  if (phase === "out" || !client) {
    return client ? <OperatorLogin client={client} onSignedIn={signedIn} /> : null;
  }

  return (
    <SessionActions onSignOut={signOut}><OcgRuntimeProvider scenario="local-ready" controlClient={client}><AppShell
      client={client}
      health={health}
      sessionExpiresAtUs={session?.expiresAtUs ?? null}
      signingOut={signingOut}
      signOutError={signOutError}
      onSignOut={() => void signOut()}
    /></OcgRuntimeProvider></SessionActions>
  );
}
