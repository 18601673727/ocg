import { createEffect, createSignal } from "solid-js";
import { Button } from "@/components/ui/button";
import type { GlobalOutboundHeaders } from "../contracts/api";
import { useI18n } from "../i18n";
import { DismissibleNotice } from "../primitives/dismissible-notice";
import { type ApiClient, ApiError } from "../runtime/api-client";
import {
  draftsToInputs,
  globalHeaderDrafts,
  type HeaderDraft,
  type HeaderDraftError,
} from "./header-drafts";
import { HeadersEditor } from "./headers-editor";
import { describeError, Section } from "./settings-parts";

export function GlobalHeadersSection(props: { client: ApiClient }) {
  const i18n = useI18n();
  const [settings, setSettings] = createSignal<GlobalOutboundHeaders>();
  const [drafts, setDrafts] = createSignal<HeaderDraft[]>([]);
  const [invalid, setInvalid] = createSignal<HeaderDraftError>();
  const [error, setError] = createSignal<string>();
  const [saved, setSaved] = createSignal(false);
  const [busy, setBusy] = createSignal(false);
  const load = (value: GlobalOutboundHeaders) => {
    setSettings(value);
    setDrafts(globalHeaderDrafts(value));
  };
  createEffect(
    () => [props.client, load, i18n.t],
    () => {
      const controller = new AbortController();
      void props.client.readGlobalOutboundHeaders(controller.signal).then(
        (value) => {
          if (!controller.signal.aborted) load(value);
        },
        (cause) => {
          if (!controller.signal.aborted) setError(describeError(cause, i18n.t));
        },
      );
      return () => controller.abort();
    },
  );
  return (
    <Section
      id="settings-global-headers"
      title={i18n.t("headers.globalTitle")}
      description={i18n.t("headers.globalDescription")}
    >
      {error() && <DismissibleNotice message={error()!} />}
      {settings() ? (
        <form
          class="space-y-4 py-4"
          onSubmit={(event) => {
            event.preventDefault();
            const request = draftsToInputs(drafts());
            if ("error" in request) {
              setInvalid(request.error);
              return;
            }
            setInvalid(undefined);
            setBusy(true);
            setError(undefined);
            setSaved(false);
            void props.client
              .saveGlobalOutboundHeaders(settings()!.revision, request.inputs)
              .then(
                (value) => {
                  load(value);
                  setSaved(true);
                },
                (cause) =>
                  setError(
                    cause instanceof ApiError && cause.code === "headers_revision_changed"
                      ? i18n.t("headers.conflict")
                      : describeError(cause, i18n.t),
                  ),
              )
              .finally(() => setBusy(false));
          }}
        >
          <HeadersEditor
            legend={i18n.t("headers.title")}
            hint={i18n.t("headers.globalHint", { agent: settings()!.default_user_agent })}
            value={drafts()}
            onChange={(next) => {
              setSaved(false);
              setDrafts(next);
            }}
            disabled={busy()}
            allowSecret={false}
            error={invalid()}
          />
          {saved() && (
            <p role="status" class="text-sm">
              {i18n.t("headers.saved")}
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
  );
}
