import { createEffect, createSignal } from "solid-js";
import { WorkspaceLink } from "@/components/ocg/layout/workspace-link";
import { Button } from "@/components/ui/button";
import type { ProfilesSnapshot } from "../contracts";
import type { GlobalCompactionSettings } from "../contracts/api";
import { useI18n } from "../i18n";
import { DismissibleNotice } from "../primitives/dismissible-notice";
import type { ApiClient } from "../runtime/api-client";
import { CompactionPolicyFields } from "./compaction-policy-fields";
import { describeError, Section } from "./settings-parts";

export function GlobalCompactionSection(props: {
  client: ApiClient;
  profiles: ProfilesSnapshot | null;
}) {
  const i18n = useI18n();
  const [draft, setDraft] = createSignal<GlobalCompactionSettings>();
  const [active, setActive] =
    createSignal<Awaited<ReturnType<ApiClient["readActiveSystemSettings"]>>>();
  const [configError, setConfigError] = createSignal<string>();
  const [error, setError] = createSignal<string>();
  const [saved, setSaved] = createSignal(false);
  const [busy, setBusy] = createSignal(false);
  createEffect(
    () => [props.client, i18n.t],
    () => {
      const controller = new AbortController();
      void props.client.readActiveSystemSettings(controller.signal).then(
        (value) => {
          if (!controller.signal.aborted) setActive(value);
        },
        (cause) => {
          if (!controller.signal.aborted) setConfigError(describeError(cause, i18n.t));
        },
      );
      void props.client.readGlobalCompactionSettings(controller.signal).then(
        (value) => {
          if (!controller.signal.aborted) setDraft(value);
        },
        (cause) => {
          if (!controller.signal.aborted) setError(describeError(cause, i18n.t));
        },
      );
      return () => controller.abort();
    },
  );
  return (
    <>
      <Section
        id="settings-global-compaction"
        title={i18n.t("context.globalTitle")}
        description={i18n.t("context.globalDescription")}
      >
        {error() && <DismissibleNotice message={error()!} />}
        {draft() && props.profiles ? (
          <form
            class="space-y-4 py-4"
            onSubmit={(event) => {
              event.preventDefault();
              setBusy(true);
              setError(undefined);
              setSaved(false);
              void props.client
                .saveGlobalCompactionSettings(draft()!)
                .then(
                  (value) => {
                    setDraft(value);
                    setSaved(true);
                  },
                  (cause) => setError(describeError(cause, i18n.t)),
                )
                .finally(() => setBusy(false));
            }}
          >
            <label class="flex items-center gap-2 text-sm">
              <input
                type="checkbox"
                disabled={busy()}
                checked={draft()!.prediction_enabled}
                onChange={(event) => {
                  setSaved(false);
                  setDraft({ ...draft()!, prediction_enabled: event.target.checked });
                }}
              />
              {i18n.t("context.predictionDefault")}
            </label>
            <p class="text-sm text-muted-foreground">
              {i18n.t("context.configurePredictor")}{" "}
              <WorkspaceLink class="underline" href="/settings/projects">
                {i18n.t("settings.nav.projects")}
              </WorkspaceLink>
            </p>
            <CompactionPolicyFields
              value={draft()!.policy}
              onChange={(policy) => {
                if (policy) {
                  setSaved(false);
                  setDraft({ ...draft()!, policy });
                }
              }}
              profiles={props.profiles}
              disabled={busy()}
            />
            {saved() && (
              <p role="status" class="text-sm">
                {i18n.t("context.saved")}
              </p>
            )}
            <Button type="submit" disabled={busy()}>
              {busy() ? i18n.t("context.saving") : i18n.t("common.save")}
            </Button>
          </form>
        ) : (
          !error() && (
            <p role="status" class="py-4 text-sm">
              {i18n.t("common.loading")}
            </p>
          )
        )}
      </Section>
      <Section
        id="settings-active-configuration"
        title={i18n.t("context.activeConfiguration")}
        description={i18n.t("context.activeConfigurationHint")}
      >
        {configError() && <DismissibleNotice message={configError()!} />}
        {active() && (
          <div class="space-y-3 py-4">
            <p class="break-all font-mono text-xs text-muted-foreground">{active()!.path}</p>
            <pre class="overflow-x-auto rounded-md bg-muted p-3 text-xs leading-6">
              {active()!.content}
            </pre>
          </div>
        )}
      </Section>
    </>
  );
}
