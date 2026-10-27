"use client";

import Link from "next/link";

import { useEffect, useState } from "react";
import type { ControlProfilesSnapshot } from "../contracts";
import type { GlobalCompactionSettings as ControlGlobalCompactionSettings } from "../contracts/api";
import type { HttpCanonicalControlClient } from "../runtime/control-client";
import { useI18n } from "../i18n";
import { Section, describeError } from "./settings-parts";
import { Button } from "@/components/ui/button";
import { DismissibleNotice } from "../primitives/dismissible-notice";
import { CompactionPolicyFields } from "./compaction-policy-fields";

export function GlobalCompactionSection({
  client,
  profiles,
}: {
  client: HttpCanonicalControlClient;
  profiles: ControlProfilesSnapshot | null;
}) {
  const { t } = useI18n();
  const [draft, setDraft] = useState<ControlGlobalCompactionSettings>();
  const [active, setActive] =
    useState<Awaited<ReturnType<HttpCanonicalControlClient["readActiveSystemSettings"]>>>();
  const [configError, setConfigError] = useState<string>();
  const [error, setError] = useState<string>();
  const [saved, setSaved] = useState(false);
  const [busy, setBusy] = useState(false);
  useEffect(() => {
    const controller = new AbortController();
    void client.readActiveSystemSettings(controller.signal).then(
      (value) => {
        if (!controller.signal.aborted) setActive(value);
      },
      (cause) => {
        if (!controller.signal.aborted) setConfigError(describeError(cause, t));
      },
    );
    void client.readGlobalCompactionSettings(controller.signal).then(
      (value) => {
        if (!controller.signal.aborted) setDraft(value);
      },
      (cause) => {
        if (!controller.signal.aborted) setError(describeError(cause, t));
      },
    );
    return () => controller.abort();
  }, [client, t]);
  return (
    <>
      <Section
        id="settings-global-compaction"
        title={t("context.globalTitle")}
        description={t("context.globalDescription")}
      >
        {error && <DismissibleNotice message={error} />}
        {draft && profiles ? (
          <form
            className="space-y-4 py-4"
            onSubmit={(event) => {
              event.preventDefault();
              setBusy(true);
              setError(undefined);
              setSaved(false);
              void client
                .saveGlobalCompactionSettings(draft)
                .then(
                  (value) => {
                    setDraft(value);
                    setSaved(true);
                  },
                  (cause) => setError(describeError(cause, t)),
                )
                .finally(() => setBusy(false));
            }}
          >
            <label className="flex items-center gap-2 text-sm">
              <input
                type="checkbox"
                disabled={busy}
                checked={draft.prediction_enabled}
                onChange={(event) => {
                  setSaved(false);
                  setDraft({ ...draft, prediction_enabled: event.target.checked });
                }}
              />
              {t("context.predictionDefault")}
            </label>
            <p className="text-sm text-muted-foreground">
              {t("context.configurePredictor")}{" "}
              <Link className="underline" href="/settings/projects">
                {t("settings.nav.projects")}
              </Link>
            </p>
            <CompactionPolicyFields
              value={draft.policy}
              onChange={(policy) => {
                if (policy) {
                  setSaved(false);
                  setDraft({ ...draft, policy });
                }
              }}
              profiles={profiles}
              disabled={busy}
            />
            {saved && (
              <p role="status" className="text-sm">
                {t("context.saved")}
              </p>
            )}
            <Button type="submit" disabled={busy}>
              {busy ? t("context.saving") : t("common.save")}
            </Button>
          </form>
        ) : (
          !error && (
            <p role="status" className="py-4 text-sm">
              {t("common.loading")}
            </p>
          )
        )}
      </Section>
      <Section
        id="settings-active-configuration"
        title={t("context.activeConfiguration")}
        description={t("context.activeConfigurationHint")}
      >
        {configError && <DismissibleNotice message={configError} />}
        {active && (
          <div className="space-y-3 py-4">
            <p className="break-all font-mono text-xs text-muted-foreground">{active.path}</p>
            <pre className="overflow-x-auto rounded-md bg-muted p-3 text-xs leading-6">
              {active.content}
            </pre>
          </div>
        )}
      </Section>
    </>
  );
}
