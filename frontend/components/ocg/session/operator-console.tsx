"use client";

import { useEffect, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  ControlTransportError,
  createHttpCanonicalControlClient,
  type HttpCanonicalControlClient,
} from "@/components/ocg/runtime/canonical-client";
import { decodeHealth } from "@/components/ocg/contracts";
import { OperatorLogin, type OperatorSession } from "@/components/ocg/login/operator-login";
import { JobSubmissionForm } from "@/components/ocg/job/job-submission-form";
import { JobWatch } from "@/components/ocg/job/job-watch";

type Phase = "checking" | "out" | "in" | "unreachable";

type Health = ReturnType<typeof decodeHealth>;

/** The local operator console: one browser session on this machine. */
export function OperatorConsole() {
  const [client, setClient] = useState<HttpCanonicalControlClient | null>(null);
  const [phase, setPhase] = useState<Phase>("checking");
  const [session, setSession] = useState<OperatorSession | null>(null);
  const [health, setHealth] = useState<Health | null>(null);
  const [watched, setWatched] = useState<string | null>(null);
  const [inspect, setInspect] = useState("");
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
    csrf.current = next.csrfToken;
    setSession(next);
    setPhase("in");
  }

  async function signOut() {
    if (!client) return;
    try {
      await client.deleteSession();
    } catch {
      // The session may already be gone; the browser ends it either way.
    }
    csrf.current = "";
    setSession(null);
    setHealth(null);
    setWatched(null);
    setPhase("out");
  }

  if (phase === "checking") {
    return (
      <main className="flex flex-1 items-center justify-center text-sm text-muted-foreground">
        Checking this session…
      </main>
    );
  }

  if (phase === "unreachable") {
    return (
      <main className="mx-auto flex w-full max-w-md flex-1 flex-col justify-center gap-4 px-6 py-16">
        <p className="text-sm text-muted-foreground">
          The OCG Control Plane did not answer. It may be restarting.
        </p>
        <Button variant="outline" onClick={() => window.location.reload()}>
          Try again
        </Button>
      </main>
    );
  }

  if (phase === "out" || !client) {
    return client ? <OperatorLogin client={client} onSignedIn={signedIn} /> : null;
  }

  const database = health?.database;

  return (
    <main className="mx-auto flex w-full max-w-3xl flex-1 flex-col gap-4 px-6 py-8">
      <header className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex flex-col gap-1">
          <h1 className="font-heading text-2xl tracking-tight">OCG</h1>
          <p className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
            <span
              className={`rounded-full px-2.5 py-0.5 font-semibold tracking-widest uppercase ${
                database?.accepting_commands ? "bg-primary/10 text-primary" : "bg-destructive/10 text-destructive"
              }`}
            >
              {health ? `host ${health.host}` : "host unreachable"}
            </span>
            <span
              className={`rounded-full px-2.5 py-0.5 font-semibold tracking-widest uppercase ${
                database?.readable ? "bg-primary/10 text-primary" : "bg-destructive/10 text-destructive"
              }`}
            >
              database {database?.status ?? "unknown"}
            </span>
            <span className="rounded-full bg-muted px-2.5 py-0.5 font-semibold tracking-widest uppercase">
              {database?.accepting_commands ? "accepting commands" : "not accepting commands"}
            </span>
          </p>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-xs text-muted-foreground">
            signed in until {session ? new Date(session.expiresAtUs / 1000).toLocaleTimeString() : "—"}
          </span>
          <Button variant="outline" size="sm" onClick={() => void signOut()}>
            Sign out
          </Button>
        </div>
      </header>
      <JobSubmissionForm client={client} onJob={setWatched} />
      <section className="flex flex-col gap-3 rounded-lg border border-border bg-card p-5 text-card-foreground">
        <h2 className="font-heading text-lg">Inspect a Job</h2>
        <form
          className="flex flex-wrap items-end gap-2"
          onSubmit={(event) => {
            event.preventDefault();
            const job = inspect.trim();
            if (/^job:(0|[1-9][0-9]*)$/.test(job)) setWatched(job);
          }}
        >
          <label className="flex flex-1 flex-col gap-1.5 text-xs font-semibold tracking-widest uppercase text-muted-foreground">
            Job
            <Input
              value={inspect}
              onChange={(event) => setInspect(event.target.value)}
              placeholder="job:<n>"
              spellCheck={false}
            />
          </label>
          <Button type="submit" variant="outline">
            Watch
          </Button>
        </form>
      </section>
      {watched ? <JobWatch key={watched} client={client} jobId={watched} /> : null}
    </main>
  );
}
