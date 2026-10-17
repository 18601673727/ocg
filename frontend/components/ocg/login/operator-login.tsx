"use client";

import { useState } from "react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  ControlTransportError,
  type HttpCanonicalControlClient,
} from "@/components/ocg/runtime/canonical-client";
import { ContractError } from "@/components/ocg/contracts";

export type OperatorSession = {
  csrfToken: string;
  expiresAtUs: number;
};

/** The one sign-in: this run's pairing code, nothing else. */
export function OperatorLogin({
  client,
  onSignedIn,
}: {
  client: HttpCanonicalControlClient;
  onSignedIn: (session: OperatorSession) => void;
}) {
  const [pairingCode, setPairingCode] = useState("");
  const [busy, setBusy] = useState(false);
  const [failure, setFailure] = useState<string | null>(null);

  async function signIn() {
    setBusy(true);
    setFailure(null);
    try {
      onSignedIn(await client.createSession(pairingCode.trim()));
      setPairingCode("");
    } catch (error) {
      if (error instanceof ControlTransportError) {
        setFailure(
          error.code === "invalid_pairing_code"
            ? "That is not this run's pairing code."
            : error.code === "pairing_disabled"
              ? "Too many attempts failed; signing in is closed for this run."
              : `Sign-in refused (${error.code}).`,
        );
      } else if (error instanceof ContractError) {
        setFailure("The sign-in answer did not match the control contract.");
      } else {
        setFailure("OCG could not be reached.");
      }
    } finally {
      setBusy(false);
    }
  }

  return (
    <main className="mx-auto flex w-full max-w-md flex-1 flex-col justify-center gap-6 px-6 py-16">
      <header className="flex flex-col gap-2">
        <h1 className="font-heading text-2xl tracking-tight">OCG</h1>
        <p className="text-sm text-muted-foreground">
          Sign in with the pairing code this run printed to its operator, on the
          machine that runs OCG.
        </p>
      </header>
      <form
        className="flex flex-col gap-4"
        onSubmit={(event) => {
          event.preventDefault();
          void signIn();
        }}
      >
        <label className="flex flex-col gap-1.5 text-xs font-semibold tracking-widest uppercase text-muted-foreground">
          Pairing code
          <Input
            value={pairingCode}
            onChange={(event) => setPairingCode(event.target.value)}
            placeholder="the code from the OCG console"
            autoComplete="off"
            spellCheck={false}
            aria-invalid={failure !== null}
          />
        </label>
        {failure ? <p className="text-sm text-destructive">{failure}</p> : null}
        <Button type="submit" disabled={busy || pairingCode.trim().length === 0}>
          {busy ? "Signing in…" : "Sign in"}
        </Button>
      </form>
    </main>
  );
}
