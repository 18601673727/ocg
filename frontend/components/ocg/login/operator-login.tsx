"use client";

import { useState } from "react";
import { ArrowRight, KeyRound, LoaderCircle } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  ControlTransportError,
  type HttpCanonicalControlClient,
} from "@/components/ocg/runtime/control-client";
import { ContractError } from "@/components/ocg/contracts";
import { useI18n, type I18nKey } from "../i18n";
import { AccessScreen } from "./access-screen";

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
  const [failure, setFailure] = useState<{ key: I18nKey; code?: string } | null>(null);

  async function signIn() {
    setBusy(true);
    setFailure(null);
    try {
      onSignedIn(await client.createSession(pairingCode.trim()));
      setPairingCode("");
    } catch (error) {
      if (error instanceof ControlTransportError) {
        if (error.code === "invalid_pairing_code") {
          setFailure({ key: "login.pairing.invalid" });
        } else if (error.code === "pairing_disabled") {
          setFailure({ key: "login.pairing.disabled" });
        } else {
          setFailure({ key: "login.pairing.refused", code: error.code });
        }
      } else if (error instanceof ContractError) {
        setFailure({ key: "login.pairing.contract" });
      } else {
        setFailure({ key: "login.pairing.unreachable" });
      }
    } finally {
      setBusy(false);
    }
  }

  return (
    <AccessScreen title={t("login.pairing.title")} description={t("login.pairing.description")}>
      <form
        className="flex flex-col gap-4"
        aria-busy={busy}
        onSubmit={(event) => {
          event.preventDefault();
          if (!busy && pairingCode.trim()) void signIn();
        }}
      >
        <div className="space-y-2">
          <label htmlFor="pairing-code" className="text-sm font-medium">
            {t("login.pairing.code")}
          </label>
          <div className="relative">
            <KeyRound
              aria-hidden="true"
              className="pointer-events-none absolute top-1/2 left-3 size-4 -translate-y-1/2 text-muted-foreground"
            />
            <Input
              id="pairing-code"
              className="min-h-11 pl-10"
              value={pairingCode}
              onChange={(event) => {
                setPairingCode(event.target.value);
                setFailure(null);
              }}
              placeholder={t("login.pairing.placeholder")}
              autoComplete="off"
              autoCapitalize="none"
              spellCheck={false}
              readOnly={busy}
              aria-invalid={failure?.key === "login.pairing.invalid"}
              aria-describedby={failure ? "pairing-error" : undefined}
            />
          </div>
        </div>
        {failure && (
          <p id="pairing-error" role="alert" className="text-sm text-destructive">
            {t(failure.key, failure.code ? { code: failure.code } : undefined)}
          </p>
        )}
        <Button type="submit" size="lg" disabled={busy || pairingCode.trim().length === 0}>
          {busy ? t("login.pairing.signingIn") : t("login.pairing.signIn")}
          {busy ? (
            <LoaderCircle aria-hidden="true" className="motion-safe:animate-spin" />
          ) : (
            <ArrowRight aria-hidden="true" />
          )}
        </Button>
      </form>
    </AccessScreen>
  );
}
