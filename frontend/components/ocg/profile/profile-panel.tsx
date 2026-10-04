"use client";

import { useI18n } from "../i18n";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Check, Loader2, Plus, RefreshCcw, Save, Trash2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useOcgControlUrl } from "./control-url";
import { createProfileClient, type Profile, type ProfileView } from "./profile-client";

const selectClass = "h-11 w-full min-w-0 rounded-md border border-border bg-background px-3 text-base outline-none focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/30 sm:text-sm";
const actionClass = "min-h-11 rounded-md tracking-normal normal-case";

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return <label className="flex min-w-0 flex-col gap-1.5 text-[13px] font-medium">{label}{children}</label>;
}

/** Shared backend-backed editor for onboarding and Configuration. A draft is
 * never authoritative: every successful mutation installs the backend reply. */
export function ProfilePanel({ onEstablished }: { onEstablished?: () => void }) {
  const { t } = useI18n();
  const baseUrl = useOcgControlUrl();
  const client = useMemo(() => baseUrl ? createProfileClient(baseUrl, fetch) : null, [baseUrl]);
  const [view, setView] = useState<ProfileView | null>(null);
  const [draft, setDraft] = useState<Profile | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const actionLock = useRef(false);
  const [notice, setNotice] = useState<"profile.saved" | "profile.credentialSaved" | null>(null);
  const [providerKey, setProviderKey] = useState("");
  const [modelKey, setModelKey] = useState("");
  const [modelId, setModelId] = useState("");
  const [modelProvider, setModelProvider] = useState("");
  const [credentialName, setCredentialName] = useState("");
  const credentialInput = useRef<HTMLInputElement>(null);
  const [hasSecret, setHasSecret] = useState(false);
  const dirty = Boolean(draft && view?.profile && JSON.stringify(draft) !== JSON.stringify(view.profile));
  const hasUnsaved = dirty || Boolean(providerKey || modelKey || modelId || credentialName || hasSecret);

  useEffect(() => {
    if (!hasUnsaved) return;
    const warn = (event: BeforeUnloadEvent) => { event.preventDefault(); event.returnValue = ""; };
    window.addEventListener("beforeunload", warn);
    return () => window.removeEventListener("beforeunload", warn);
  }, [hasUnsaved]);

  const install = useCallback((next: ProfileView) => {
    setView(next);
    setDraft(next.profile ? structuredClone(next.profile) : null);
  }, []);
  const refresh = useCallback(async () => {
    if (!client || actionLock.current || hasUnsaved) return;
    actionLock.current = true;
    setBusy(true);
    setError(null);
    setNotice(null);
    try { install(await client.read()); }
    catch (cause) { setError(cause instanceof Error ? cause.message : String(cause)); }
    finally { actionLock.current = false; setBusy(false); }
  }, [client, install, hasUnsaved]);
  useEffect(() => {
    if (!client) return;
    let active = true;
    void client.read().then(next => { if (active) install(next); })
      .catch(cause => { if (active) setError(cause instanceof Error ? cause.message : String(cause)); });
    return () => { active = false; };
  }, [client, install]);

  async function mutate(operation: () => Promise<ProfileView>, preserveDraft = false) {
    if (actionLock.current) return false;
    actionLock.current = true;
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      const next = await operation();
      if (preserveDraft) {
        // Keep the draft's revision when another client has changed the profile.
        setView(current => current?.revision === next.revision ? next : current);
      } else install(next);
      setNotice(preserveDraft ? "profile.credentialSaved" : "profile.saved");
      // Execution readiness comes from the backend alone: only a
      // backend-confirmed executable choice establishes the profile.
      if (next.runnable_choices.length > 0) onEstablished?.();
      return true;
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
      return false;
    } finally { actionLock.current = false; setBusy(false); }
  }
  function change(transform: (profile: Profile) => Profile) {
    setNotice(null);
    setDraft(current => current ? transform(structuredClone(current)) : current);
  }
  function addProvider() {
    const key = providerKey.trim();
    if (!key || !draft || draft.providers[key]) return;
    change(profile => { profile.providers[key] = { label: key, protocol: "openai_compatible" }; return profile; });
    setModelProvider(key);
    setProviderKey("");
  }
  function saveCredential() {
    const name = credentialName.trim();
    const secret = credentialInput.current?.value ?? "";
    if (!client || !name || !secret || busy) return;
    void mutate(() => client.saveCredential(name, secret), true).then(saved => {
      if (!saved) return;
      if (credentialInput.current) credentialInput.current.value = "";
      setCredentialName("");
      setHasSecret(false);
    });
  }
  function addModel() {
    const key = modelKey.trim();
    const id = modelId.trim();
    if (!key || !id || !modelProvider || !draft || draft.models[key] || !draft.providers[modelProvider]) return;
    // A new model is draft-only, so it cannot become the default here: the
    // backend has not confirmed it executable and this panel never establishes
    // a default the backend has not reported in `runnable_choices`.
    change(profile => {
      profile.models[key] = { provider: modelProvider, id };
      return profile;
    });
    setModelKey(""); setModelId("");
  }

  function discard() {
    if (view) install(view);
    setProviderKey(""); setModelKey(""); setModelId(""); setModelProvider("");
    setCredentialName(""); setHasSecret(false);
    if (credentialInput.current) credentialInput.current.value = "";
    setNotice(null); setError(null);
  }

  if (!baseUrl) return <p role="status" className="text-sm text-muted-foreground">{t("profile.noEndpoint")}</p>;
  const duplicateProvider = Boolean(draft && providerKey.trim() && draft.providers[providerKey.trim()]);
  const duplicateModel = Boolean(draft && modelKey.trim() && draft.models[modelKey.trim()]);
  // Every configured model stays visible so an incomplete setup is readable,
  // but only the backend-confirmed executable ones can become the default.
  const defaultModelOptions = draft && view
    ? Object.keys(draft.models).map(key => ({ key, executable: view.runnable_choices.includes(key) }))
    : [];
  return (
    <section className="space-y-4 rounded-lg border border-border bg-background p-4" aria-label={t("profile.title")} aria-busy={busy}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0"><h2 className="text-base font-semibold">{t("profile.title")}</h2><p className="mt-1 text-[13px] leading-5 text-muted-foreground">{t("profile.description")}</p></div>
        <Button size="sm" className={actionClass} variant="outline" onClick={() => void refresh()} disabled={busy || hasUnsaved || (!view && !error)} title={hasUnsaved ? t("profile.refreshBlocked") : undefined}>
          <RefreshCcw className="size-4" />{t("common.refresh")}
        </Button>
      </div>
      {error && <p role="alert" className="break-words rounded-md border border-destructive/30 bg-destructive/5 p-3 text-sm text-destructive">{error} {t("profile.errorHint")}</p>}
      {notice && <p role="status" className="flex items-start gap-2 rounded-md border border-emerald-500/30 bg-emerald-500/5 p-3 text-sm"><Check className="mt-0.5 size-4 shrink-0 text-emerald-600" />{t(notice)}</p>}
      {!view ? !error && <p role="status" className="flex items-center gap-2 text-sm text-muted-foreground"><Loader2 className="size-4 animate-spin" />{t("profile.loading")}</p> : !draft ? (
        <div className="space-y-3">
          <p className="text-sm">{t("profile.empty")}</p>
          <Button size="sm" className={actionClass} disabled={busy} onClick={() => void mutate(() => client!.createNew())}>{t("profile.create")}</Button>
        </div>
      ) : (
        <details className="space-y-4">
          <summary className="min-h-11 cursor-pointer py-2 text-sm font-medium outline-none focus-visible:ring-2 focus-visible:ring-ring/30">{t("profile.advanced")}</summary>
          <p className="text-[13px] text-muted-foreground">{t("profile.counts", { providers: Object.keys(draft.providers).length, models: Object.keys(draft.models).length, runnable: view.runnable_choices.length })}</p>
          {view.runnable_choices.length === 0 && <p role="status" className="text-sm text-muted-foreground">{t("profile.unavailable")}</p>}
          <fieldset disabled={busy} className="min-w-0 space-y-6 disabled:opacity-60">
            <legend className="sr-only">{t("profile.advanced")}</legend>
            <section className="space-y-3">
              <h3 className="text-sm font-semibold">{t("profile.providers")}</h3>
              {Object.entries(draft.providers).map(([key, provider]) => (
                <div key={key} className="space-y-3 rounded-lg border border-border bg-muted/10 p-3">
                  <div className="flex items-center justify-between gap-2"><span className="min-w-0 break-all font-mono text-sm font-medium">{key}</span><Button size="sm" className={actionClass} variant="ghost" aria-label={t("profile.removeProvider", { key })} onClick={() => change(profile => {
                    delete profile.providers[key];
                    for (const [name, model] of Object.entries(profile.models)) if (model.provider === key) delete profile.models[name];
                    if (profile.defaultModel && !profile.models[profile.defaultModel]) profile.defaultModel = null;
                    return profile;
                  })}><Trash2 className="size-4" /></Button></div>
                  <div className="grid min-w-0 gap-3 sm:grid-cols-2">
                    <Field label={t("profile.label", { key })}><Input aria-label={t("profile.label", { key })} value={provider.label} onChange={event => change(profile => { profile.providers[key].label = event.target.value; return profile; })} /></Field>
                    <Field label={t("profile.protocol", { key })}><select aria-label={t("profile.protocol", { key })} className={selectClass} value={provider.protocol ?? "openai_compatible"} onChange={event => {
                      const protocol = event.target.value;
                      if (protocol === "anthropic" || protocol === "openai" || protocol === "openai_compatible") change(profile => { profile.providers[key].protocol = protocol; return profile; });
                    }}><option value="openai_compatible">{t("profile.compatibleOpenAI")}</option><option value="openai">{t("profile.nativeOpenAI")}</option><option value="anthropic">{t("profile.nativeAnthropic")}</option></select></Field>
                    <Field label={t("profile.endpoint", { key })}><Input type="url" autoComplete="off" spellCheck={false} aria-label={t("profile.endpoint", { key })} placeholder="https://api.example.com/v1" value={provider.endpoint ?? ""} onChange={event => change(profile => { profile.providers[key].endpoint = event.target.value || null; return profile; })} /></Field>
                    <Field label={t("profile.credentialRef", { key })}><Input autoComplete="off" spellCheck={false} aria-label={t("profile.credentialRef", { key })} placeholder={t("profile.credentialPlaceholder")} value={provider.credential_ref ?? ""} onChange={event => change(profile => { profile.providers[key].credential_ref = event.target.value || null; return profile; })} /></Field>
                  </div>
                </div>
              ))}
              <div className="flex flex-wrap items-end gap-2">
                <div className="min-w-0 flex-1"><Field label={t("profile.providerKey")}><Input aria-label={t("profile.providerKey")} value={providerKey} aria-invalid={duplicateProvider} aria-describedby={duplicateProvider ? "profile-provider-error" : undefined} onChange={event => setProviderKey(event.target.value)} /></Field></div>
                <Button size="sm" className={actionClass} variant="outline" disabled={!providerKey.trim() || duplicateProvider} onClick={addProvider}><Plus className="size-4" />{t("canonical.provider")}</Button>
              </div>
              {duplicateProvider && <p id="profile-provider-error" role="status" className="text-sm text-destructive">{t("profile.duplicateProvider")}</p>}
            </section>
            <section className="space-y-3">
              <h3 className="text-sm font-semibold">{t("profile.vault")}</h3>
              <p className="text-[13px] leading-5 text-muted-foreground">{t("profile.vaultDesc")}</p>
              <div className="grid min-w-0 gap-3 sm:grid-cols-2">
                <Field label={t("profile.credentialName")}><Input aria-label={t("profile.credentialName")} value={credentialName} onChange={event => setCredentialName(event.target.value)} /></Field>
                <Field label={t("profile.credentialSecret")}><Input aria-label={t("profile.credentialSecret")} type="password" ref={credentialInput} autoComplete="off" onChange={event => setHasSecret(Boolean(event.target.value))} /></Field>
              </div>
              <Button size="sm" className={actionClass} variant="outline" disabled={!credentialName.trim() || !hasSecret} onClick={saveCredential}><Save className="size-4" />{t("profile.saveCredential")}</Button>
            </section>
            <section className="space-y-3">
              <h3 className="text-sm font-semibold">{t("profile.models")}</h3>
              {Object.entries(draft.models).map(([key, model]) => (
                <div key={key} className="space-y-3 rounded-lg border border-border bg-muted/10 p-3">
                  <div className="flex items-center justify-between gap-2"><div className="min-w-0"><p className="break-all font-mono text-sm font-medium">{key}</p><p className="break-all text-xs text-muted-foreground">{model.provider}</p></div><Button size="sm" className={actionClass} variant="ghost" aria-label={t("profile.removeModel", { key })} onClick={() => change(profile => { delete profile.models[key]; if (profile.defaultModel === key) profile.defaultModel = null; return profile; })}><Trash2 className="size-4" /></Button></div>
                  <div className="grid min-w-0 gap-3 sm:grid-cols-2">
                    <Field label={t("profile.modelId", { key })}><Input aria-label={t("profile.modelId", { key })} value={model.id} onChange={event => change(profile => { profile.models[key].id = event.target.value; return profile; })} /></Field>
                    <Field label={t("profile.variant", { key })}><Input aria-label={t("profile.variant", { key })} placeholder={t("profile.variantPlaceholder")} value={model.variant ?? ""} onChange={event => change(profile => { profile.models[key].variant = event.target.value || null; return profile; })} /></Field>
                  </div>
                  {!!model.variants?.length && <p className="break-words text-xs text-muted-foreground">{t("profile.availableVariants", { variants: model.variants.join(", ") })}</p>}
                </div>
              ))}
              <div className="grid min-w-0 gap-3 sm:grid-cols-2">
                <Field label={t("profile.modelKey")}><Input aria-label={t("profile.modelKey")} value={modelKey} aria-invalid={duplicateModel} aria-describedby={duplicateModel ? "profile-model-error" : undefined} onChange={event => setModelKey(event.target.value)} /></Field>
                <Field label={t("profile.newModelId")}><Input aria-label={t("profile.newModelId")} value={modelId} onChange={event => setModelId(event.target.value)} /></Field>
                <Field label={t("profile.modelProvider")}><select aria-label={t("profile.modelProvider")} className={selectClass} value={draft.providers[modelProvider] ? modelProvider : ""} onChange={event => setModelProvider(event.target.value)}><option value="">{t("profile.selectProvider")}</option>{Object.keys(draft.providers).map(key => <option key={key} value={key}>{key}</option>)}</select></Field>
                <Button size="sm" className={actionClass + " self-end"} variant="outline" disabled={!modelKey.trim() || !modelId.trim() || !draft.providers[modelProvider] || duplicateModel} onClick={addModel}><Plus className="size-4" />{t("canonical.model")}</Button>
              </div>
              {duplicateModel && <p id="profile-model-error" role="status" className="text-sm text-destructive">{t("profile.duplicateModel")}</p>}
            </section>
            {/* Execution readiness is the backend's authority: every configured
             * model is listed, but only a `view.runnable_choices` model may be
             * chosen as the active default. */}
            <Field label={t("profile.defaultModel")}><select aria-label={t("profile.defaultModel")} className={selectClass} value={draft.defaultModel ?? ""} onChange={event => change(profile => { profile.defaultModel = event.target.value || null; return profile; })}><option value="">{t("profile.selectAtExecution")}</option>{defaultModelOptions.map(({ key, executable }) => <option key={key} value={key} disabled={!executable}>{key}{executable ? "" : ` · ${t("profile.notExecutable")}`}</option>)}</select></Field>
          </fieldset>
          <div className="sticky bottom-0 flex flex-wrap items-center gap-3 border-t border-border bg-background py-3">
            <p role="status" className="min-w-0 flex-1 text-[13px] text-muted-foreground">{hasUnsaved ? t("profile.unsaved") : t("profile.saved")}</p>
            {hasUnsaved && <Button size="sm" className={actionClass} variant="outline" disabled={busy} onClick={discard}>{t("profile.discard")}</Button>}
            <Button size="sm" className={actionClass} disabled={busy || !view.revision || !dirty} onClick={() => void mutate(() => client!.replace(view.revision!, draft))}>{busy ? <Loader2 className="size-4 animate-spin" /> : <Save className="size-4" />}{t(busy ? "profile.saving" : "profile.save")}</Button>
          </div>
        </details>
      )}
    </section>
  );
}
