import { Loading, lazy } from "solid-js";
import type * as Api from "@/components/ocg/contracts/api";
import { AppShell } from "@/components/ocg/layout/app-shell";
import type { ApiClient } from "@/components/ocg/runtime/api-client";
import { useI18n } from "../i18n";
import { SessionActions } from "../login/access-boundary";
import { OcgRuntimeProvider } from "../runtime/runtime-context";

const SessionAccess = lazy(() =>
  import("../login/access-screen").then(({ SessionAccess }) => ({ default: SessionAccess })),
);

export type OperatorConsoleProps = {
  phase: "out" | "in" | "token" | "unreachable";
  client: ApiClient;
  health: Api.Health | null;
  sessionExpiresAtUs: number | null;
  signingOut: boolean;
  signOutErrorCode: string | null;
  signIn: (token: string) => Promise<void>;
  signOut: () => Promise<void>;
};

export function OperatorConsole(props: OperatorConsoleProps) {
  const i18n = useI18n();

  return (
    <>
      {(() => {
        if (props.phase !== "in") {
          return (
            <Loading fallback={<p role="status">{i18n.t("session.checking")}</p>}>
              <SessionAccess phase={props.phase} signIn={props.signIn} />
            </Loading>
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
