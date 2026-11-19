import { RotateCw } from "@lucide/solid";
import type * as Api from "@/components/ocg/contracts/api";
import { AppShell } from "@/components/ocg/layout/app-shell";
import type { ApiClient } from "@/components/ocg/runtime/api-client";
import { Button } from "@/components/ui/button";
import { useI18n } from "../i18n";
import { SessionActions } from "../login/access-boundary";
import { AccessScreen } from "../login/access-screen";
import { OcgRuntimeProvider } from "../runtime/runtime-context";

export type OperatorConsoleProps = {
  phase: "out" | "in" | "unreachable";
  client: ApiClient;
  health: Api.Health | null;
  sessionExpiresAtUs: number | null;
  signingOut: boolean;
  signOutErrorCode: string | null;
  signOut: () => Promise<void>;
};

export function OperatorConsole(props: OperatorConsoleProps) {
  const i18n = useI18n();

  return (
    <>
      {(() => {
        if (props.phase === "unreachable" || props.phase === "out") {
          const signedOut = props.phase === "out";
          return (
            <AccessScreen
              title={i18n.t(signedOut ? "session.signedOutTitle" : "session.offlineTitle")}
              description={i18n.t(
                signedOut ? "session.signedOutDescription" : "session.unreachable",
              )}
            >
              <Button class="self-start" onClick={() => window.location.reload()}>
                <RotateCw aria-hidden="true" />
                {i18n.t(signedOut ? "session.reopen" : "session.retry")}
              </Button>
            </AccessScreen>
          );
        }

        return (
          <SessionActions onSignOut={props.signOut}>
            <OcgRuntimeProvider apiClient={props.client}>
              <AppShell
                client={props.client}
                health={props.health}
                sessionExpiresAtUs={props.sessionExpiresAtUs}
                signingOut={props.signingOut}
                signOutError={
                  props.signOutErrorCode
                    ? i18n.t("session.signOutUnconfirmed", { code: props.signOutErrorCode })
                    : null
                }
                onSignOut={() => void props.signOut()}
              />
            </OcgRuntimeProvider>
          </SessionActions>
        );
      })()}
    </>
  );
}
