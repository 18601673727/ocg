"use client";

import { useI18n } from "../i18n";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Plus, RefreshCcw, Save, Trash2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useOcgControlUrl } from "./control-url";
import { createProfileClient, type Profile, type ProfileView } from "./profile-client";

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
  const [providerKey, setProviderKey] = useState("");
  const [modelKey, setModelKey] = useState("");
  const [modelId, setModelId] = useState("");
  const [modelProvider, setModelProvider] = useState("");
  const [credentialName, setCredentialName] = useState("");
  const credentialInput = useRef<HTMLInputElement>(null);

  const install = useCallback((next: ProfileView) => {
    setView(next);
    setDraft(next.profile ? structuredClone(next.profile) : null);
  }, []);
  const refresh = useCallback(async () => {
    if (!client) return;
    try { install(await client.read()); }
    catch (cause) { setError(cause instanceof Error ? cause.message : String(cause)); }
  }, [client, install]);
  useEffect(() => {
    if (!client) return;
    let active = true;
    void client.read().then(next => { if (active) install(next); })
      .catch(cause => { if (active) setError(cause instanceof Error ? cause.message : String(cause)); });
    return () => { active = false; };
  }, [client, install]);

  async function mutate(operation: () => Promise<ProfileView>) {
    setBusy(true);
    setError(null);
    try {
      const next = await operation();
      install(next);
      // Execution readiness comes from the backend alone: only a
      // backend-confirmed executable choice establishes the profile.
      if (next.runnable_choices.length > 0) onEstablished?.();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally { setBusy(false); }
  }
  function change(transform: (profile: Profile) => Profile) {
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
    void mutate(() => client.saveCredential(name, secret)).finally(() => {
      if (credentialInput.current) credentialInput.current.value = "";
      setCredentialName("");
    });
  }
  function addModel() {
    const key = modelKey.trim();
    const id = modelId.trim();
    if (!key || !id || !modelProvider || !draft || draft.models[key] || !draft.providers[modelProvider]) return;
    change(profile => {
      profile.models[key] = { provider: modelProvider, id };
      profile.defaultModel ??= key;
      return profile;
    });
    setModelKey(""); setModelId("");
  }

  if (!baseUrl) return <p role="status" className="text-sm text-muted-foreground">{t("profile.noEndpoint")}</p>;
  return (
    <section className="space-y-4 rounded-md border border-border bg-background p-4" aria-label={t("profile.title")}>
      <div className="flex items-center justify-between gap-2">
        <div><h2 className="text-sm font-semibold">{t("profile.title")}</h2><p className="text-xs text-muted-foreground">{t("profile.description")}</p></div>
        <Button size="sm" variant="outline" onClick={() => void refresh()} disabled={busy}><RefreshCcw className="size-3" /> {t("common.refresh")}</Button>
      </div>
      {error && <p role="alert" className="text-xs text-destructive">{error} {t("profile.errorHint")}</p>}
      {!view ? <p role="status" className="text-xs">{t("profile.loading")}</p> : !draft ? (
        <div className="space-y-3">
          <p className="text-xs">{t("profile.empty")}</p>
          <Button size="sm" disabled={busy} onClick={() => void mutate(() => client!.createNew())}>{t("profile.create")}</Button>
        </div>
      ) : (
        <details className="space-y-3 text-xs"><summary className="cursor-pointer font-medium">{t("profile.advanced")}</summary>
          <p>{t("profile.counts", { providers: Object.keys(draft.providers).length, models: Object.keys(draft.models).length, runnable: view.runnable_choices.length })}</p>
          {view.runnable_choices.length === 0 && <p role="status">{t("profile.unavailable")}</p>}
          <div className="space-y-1"><h3 className="font-medium">{t("profile.providers")}</h3>
            {Object.entries(draft.providers).map(([key, provider]) => <div key={key} className="flex flex-wrap items-center gap-2"><span className="min-w-24">{key}</span><Input aria-label={t("profile.label", { key })} value={provider.label} onChange={event => change(profile => { profile.providers[key].label = event.target.value; return profile; })} /><select aria-label={t("profile.protocol", { key })} className="rounded border border-border bg-background px-2" value={provider.protocol ?? "openai_compatible"} onChange={event => { const protocol = event.target.value; if (protocol === "anthropic" || protocol === "openai" || protocol === "openai_compatible") change(profile => { profile.providers[key].protocol = protocol; return profile; }); }}><option value="openai_compatible">{t("profile.compatibleOpenAI")}</option><option value="openai">{t("profile.nativeOpenAI")}</option><option value="anthropic">{t("profile.nativeAnthropic")}</option></select><Input aria-label={t("profile.endpoint", { key })} placeholder="https://api.example.com/v1" value={provider.endpoint ?? ""} onChange={event => change(profile => { profile.providers[key].endpoint = event.target.value || null; return profile; })} /><Input aria-label={t("profile.credentialRef", { key })} placeholder={t("profile.credentialPlaceholder")} value={provider.credential_ref ?? ""} onChange={event => change(profile => { profile.providers[key].credential_ref = event.target.value || null; return profile; })} /><Button size="xs" variant="outline" aria-label={t("profile.removeProvider", { key })} onClick={() => change(profile => { delete profile.providers[key]; for (const [name, model] of Object.entries(profile.models)) if (model.provider === key) delete profile.models[name]; if (profile.defaultModel && !profile.models[profile.defaultModel]) profile.defaultModel = null; return profile; })}><Trash2 className="size-3" /></Button></div>)}
            <div className="flex gap-2"><Input aria-label={t("profile.providerKey")} placeholder={t("profile.providerKey")} value={providerKey} onChange={event => setProviderKey(event.target.value)} /><Button size="sm" variant="outline" onClick={addProvider}><Plus className="size-3" /> {t("canonical.provider")}</Button></div>
          </div>
          <div className="space-y-1"><h3 className="font-medium">{t("profile.vault")}</h3>
            <p className="text-muted-foreground">{t("profile.vaultDesc")}</p>
            <div className="flex flex-wrap gap-2"><Input aria-label={t("profile.credentialName")} placeholder={t("profile.credentialName")} value={credentialName} onChange={event => setCredentialName(event.target.value)} /><Input aria-label={t("profile.credentialSecret")} placeholder={t("profile.credentialSecret")} type="password" ref={credentialInput} autoComplete="off" /><Button size="sm" variant="outline" disabled={busy || !credentialName.trim()} onClick={saveCredential}><Save className="size-3" /> {t("profile.saveCredential")}</Button></div>
          </div>
          <div className="space-y-1"><h3 className="font-medium">{t("profile.models")}</h3>
            {Object.entries(draft.models).map(([key, model]) => <div key={key} className="flex flex-wrap items-center gap-2"><span className="min-w-24">{key}</span><Input aria-label={t("profile.modelId", { key })} value={model.id} onChange={event => change(profile => { profile.models[key].id = event.target.value; return profile; })} /><span>{model.provider}</span><Input aria-label={t("profile.variant", { key })} placeholder={t("profile.variantPlaceholder")} value={model.variant ?? ""} onChange={event => change(profile => { profile.models[key].variant = event.target.value || null; return profile; })} />{model.variants?.length ? <span>{t("profile.availableVariants", { variants: model.variants.join(", ") })}</span> : null}<Button size="xs" variant="outline" aria-label={t("profile.removeModel", { key })} onClick={() => change(profile => { delete profile.models[key]; if (profile.defaultModel === key) profile.defaultModel = null; return profile; })}><Trash2 className="size-3" /></Button></div>)}
            <div className="flex flex-wrap gap-2"><Input aria-label={t("profile.modelKey")} placeholder={t("profile.modelKey")} value={modelKey} onChange={event => setModelKey(event.target.value)} /><Input aria-label={t("profile.newModelId")} placeholder={t("profile.newModelId")} value={modelId} onChange={event => setModelId(event.target.value)} /><select aria-label={t("profile.modelProvider")} className="rounded border border-border bg-background px-2" value={modelProvider} onChange={event => setModelProvider(event.target.value)}><option value="">{t("profile.selectProvider")}</option>{Object.keys(draft.providers).map(key => <option key={key} value={key}>{key}</option>)}</select><Button size="sm" variant="outline" onClick={addModel}><Plus className="size-3" /> {t("canonical.model")}</Button></div>
          </div>
          <label className="flex items-center gap-2">{t("profile.defaultModel")} <select className="rounded border border-border bg-background px-2 py-1" value={draft.defaultModel ?? ""} onChange={event => change(profile => { profile.defaultModel = event.target.value || null; return profile; })}><option value="">{t("profile.selectAtExecution")}</option>{view.runnable_choices.map(key => <option key={key} value={key}>{key}</option>)}</select></label>
          <Button size="sm" disabled={busy || !view.revision} onClick={() => void mutate(() => client!.replace(view.revision!, draft))}><Save className="size-3" /> {t("profile.save")}</Button>
        </details>
      )}
    </section>
  );
}
