"use client";

import { useState } from "react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  ControlTransportError,
  type HttpCanonicalControlClient,
} from "@/components/ocg/runtime/control-client";
import { ContractError } from "@/components/ocg/contracts";
import { useI18n } from "../i18n";
import { OcgLogo } from "../primitives/ocg-logo";

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
  const { t } = useI18n();
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
            ? t("login.pairing.invalid")
            : error.code === "pairing_disabled"
              ? t("login.pairing.disabled")
              : t("login.pairing.refused", { code: error.code }),
        );
      } else if (error instanceof ContractError) {
        setFailure(t("login.pairing.contract"));
      } else {
        setFailure(t("login.pairing.unreachable"));
      }
    } finally {
      setBusy(false);
    }
  }

  return (
    <main className="mx-auto flex w-full max-w-md flex-1 flex-col justify-center gap-6 px-6 py-16">
      <header className="flex flex-col gap-2">
        <h1>
          <OcgLogo />
        </h1>
        <p className="text-sm text-muted-foreground">{t("login.pairing.description")}</p>
      </header>
      <form
        className="flex flex-col gap-4"
        onSubmit={(event) => {
          event.preventDefault();
          void signIn();
        }}
      >
        <label className="flex flex-col gap-1.5 text-xs font-semibold tracking-widest uppercase text-muted-foreground">
          {t("login.pairing.code")}
          <Input
            value={pairingCode}
            onChange={(event) => setPairingCode(event.target.value)}
            placeholder={t("login.pairing.placeholder")}
            autoComplete="off"
            spellCheck={false}
            aria-invalid={failure !== null}
          />
        </label>
        {failure ? <p className="text-sm text-destructive">{failure}</p> : null}
        <Button type="submit" disabled={busy || pairingCode.trim().length === 0}>
          {busy ? t("login.pairing.signingIn") : t("login.pairing.signIn")}
        </Button>
      </form>
    </main>
  );
}
