"use client";

import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import type { GlobalOutboundHeaders } from "../contracts/api";
import { useI18n } from "../i18n";
import { DismissibleNotice } from "../primitives/dismissible-notice";
import { ControlTransportError, type HttpCanonicalControlClient } from "../runtime/control-client";
import {
  type HeaderDraft,
  type HeaderDraftError,
  draftsToInputs,
  globalHeaderDrafts,
} from "./header-drafts";
import { HeadersEditor } from "./headers-editor";
import { Section, describeError } from "./settings-parts";

export function GlobalHeadersSection({ client }: { client: HttpCanonicalControlClient }) {
  const { t } = useI18n();
  const [settings, setSettings] = useState<GlobalOutboundHeaders>();
  const [drafts, setDrafts] = useState<HeaderDraft[]>([]);
  const [invalid, setInvalid] = useState<HeaderDraftError>();
  const [error, setError] = useState<string>();
  const [saved, setSaved] = useState(false);
  const [busy, setBusy] = useState(false);
  const load = (value: GlobalOutboundHeaders) => {
    setSettings(value);
    setDrafts(globalHeaderDrafts(value));
  };
  useEffect(() => {
    const controller = new AbortController();
    void client.readGlobalOutboundHeaders(controller.signal).then(
      (value) => {
        if (!controller.signal.aborted) load(value);
      },
      (cause) => {
        if (!controller.signal.aborted) setError(describeError(cause, t));
      },
    );
    return () => controller.abort();
  }, [client, t]);
  return (
    <Section
      id="settings-global-headers"
      title={t("headers.globalTitle")}
      description={t("headers.globalDescription")}
    >
      {error && <DismissibleNotice message={error} />}
      {settings ? (
        <form
          className="space-y-4 py-4"
          onSubmit={(event) => {
            event.preventDefault();
            const request = draftsToInputs(drafts);
            if ("error" in request) {
              setInvalid(request.error);
              return;
            }
            setInvalid(undefined);
            setBusy(true);
            setError(undefined);
            setSaved(false);
            void client
              .saveGlobalOutboundHeaders(settings.revision, request.inputs)
              .then(
                (value) => {
                  load(value);
                  setSaved(true);
                },
                (cause) =>
                  setError(
                    cause instanceof ControlTransportError &&
                      cause.code === "headers_revision_changed"
                      ? t("headers.conflict")
                      : describeError(cause, t),
                  ),
              )
              .finally(() => setBusy(false));
          }}
        >
          <HeadersEditor
            legend={t("headers.title")}
            hint={t("headers.globalHint", { agent: settings.default_user_agent })}
            value={drafts}
            onChange={(next) => {
              setSaved(false);
              setDrafts(next);
            }}
            disabled={busy}
            allowSecret={false}
            error={invalid}
          />
          {saved && (
            <p role="status" className="text-sm">
              {t("headers.saved")}
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
  );
}
