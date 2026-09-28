"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { Plus, RefreshCcw, Save, Trash2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useOcgControlUrl } from "./control-url";
import { createProfileClient, runnableChoices, type Candidate, type Profile, type ProfileView } from "./profile-client";

/** Shared backend-backed editor for onboarding and Configuration. A draft is
 * never authoritative: every successful mutation installs the backend reply. */
export function ProfilePanel({ onEstablished }: { onEstablished?: () => void }) {
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
      if (next.profile) onEstablished?.();
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
    change(profile => { profile.providers[key] = { placeholder: false, label: key }; return profile; });
    setModelProvider(key);
    setProviderKey("");
  }
  function addModel() {
    const key = modelKey.trim();
    const id = modelId.trim();
    if (!key || !id || !modelProvider || !draft || draft.models[key] || !draft.providers[modelProvider]) return;
    change(profile => {
      profile.models[key] = { placeholder: false, provider: modelProvider, id };
      profile.defaultModel ??= key;
      return profile;
    });
    setModelKey(""); setModelId("");
  }

  if (!baseUrl) return <p role="status" className="text-sm text-muted-foreground">No loopback OCG control endpoint is connected. Start the UI through <code>ocg</code> or configure a local endpoint.</p>;
  return (
    <section className="space-y-4 rounded-md border border-border bg-background p-4" aria-label="OCG Profile configuration">
      <div className="flex items-center justify-between gap-2">
        <div><h2 className="text-sm font-semibold">OCG Profile</h2><p className="text-xs text-muted-foreground">Owned by .ocg.yaml; external configuration is a one-time import source.</p></div>
        <Button size="sm" variant="outline" onClick={() => void refresh()} disabled={busy}><RefreshCcw className="size-3" /> Refresh</Button>
      </div>
      {error && <p role="alert" className="text-xs text-destructive">{error} Refresh and compare again before retrying an import or edit.</p>}
      {!view ? <p role="status" className="text-xs">Loading Profile…</p> : !draft ? (
        <div className="space-y-3">
          <p className="text-xs">No OCG Profile exists. Choose one source; Global and Local are never merged automatically.</p>
          {view.candidates.map((candidate: Candidate) => (
            <div key={`${candidate.location}:${candidate.sha256}`} className="rounded border border-border p-3 text-xs">
              <p className="font-medium">{candidate.source} · {candidate.scope}</p>
              <p className="break-all text-muted-foreground">{candidate.location}</p>
              <p>Providers ({candidate.provider_names.length}): {candidate.provider_names.join(", ") || "none"}</p>
              <p>Models ({candidate.model_ids.length}): {candidate.model_ids.join(", ") || "none"}</p>
              {Object.entries(candidate.variants).map(([model, variants]) => <p key={model}>{model}: {variants.join(", ")}</p>)}
              <p>Importable: {candidate.importable_fields.join(", ") || "none"} · Ignored: {candidate.ignored_fields.join(", ") || "none"}</p>
              <Button size="sm" variant="outline" disabled={busy} onClick={() => void mutate(() => client!.importCandidate(candidate))}>Import this snapshot</Button>
            </div>
          ))}
          <Button size="sm" disabled={busy} onClick={() => void mutate(() => client!.createNew())}>Create New Profile (non-runnable placeholders)</Button>
        </div>
      ) : (
        <div className="space-y-3 text-xs">
          <p>Origin: {draft.origin === "new" ? "New" : `Imported ${draft.origin.imported.scope} from ${draft.origin.imported.source}`}</p>
          <p>Providers: {Object.keys(draft.providers).length} · Models: {Object.keys(draft.models).length} · Runnable choices: {runnableChoices(draft).length}</p>
          {runnableChoices(draft).length === 0 && <p role="status">Placeholder-only Profile: configuration is valid, inference is unavailable.</p>}
          <div className="space-y-1"><h3 className="font-medium">Providers</h3>
            {Object.entries(draft.providers).map(([key, provider]) => <div key={key} className="flex items-center gap-2"><span className="min-w-24">{key}{provider.placeholder ? " (placeholder)" : ""}</span><Input aria-label={`${key} label`} value={provider.label} onChange={event => change(profile => { profile.providers[key].label = event.target.value; return profile; })} /><Button size="xs" variant="outline" aria-label={`Remove provider ${key}`} onClick={() => change(profile => { delete profile.providers[key]; for (const [name, model] of Object.entries(profile.models)) if (model.provider === key) delete profile.models[name]; if (profile.defaultModel && !profile.models[profile.defaultModel]) profile.defaultModel = null; return profile; })}><Trash2 className="size-3" /></Button></div>)}
            <div className="flex gap-2"><Input aria-label="New provider key" placeholder="provider key" value={providerKey} onChange={event => setProviderKey(event.target.value)} /><Button size="sm" variant="outline" onClick={addProvider}><Plus className="size-3" /> Provider</Button></div>
          </div>
          <div className="space-y-1"><h3 className="font-medium">Models</h3>
            {Object.entries(draft.models).map(([key, model]) => <div key={key} className="flex flex-wrap items-center gap-2"><span className="min-w-24">{key}{model.placeholder ? " (placeholder)" : ""}</span><Input aria-label={`${key} model id`} value={model.id} onChange={event => change(profile => { profile.models[key].id = event.target.value; return profile; })} /><span>{model.provider}</span><Input aria-label={`${key} variant`} placeholder="provider default variant" value={model.variant ?? ""} onChange={event => change(profile => { profile.models[key].variant = event.target.value || null; return profile; })} />{model.variants?.length ? <span>Available: {model.variants.join(", ")}</span> : null}<Button size="xs" variant="outline" aria-label={`Remove model ${key}`} onClick={() => change(profile => { delete profile.models[key]; if (profile.defaultModel === key) profile.defaultModel = null; return profile; })}><Trash2 className="size-3" /></Button></div>)}
            <div className="flex flex-wrap gap-2"><Input aria-label="New model key" placeholder="model key" value={modelKey} onChange={event => setModelKey(event.target.value)} /><Input aria-label="New model id" placeholder="runtime model id" value={modelId} onChange={event => setModelId(event.target.value)} /><select aria-label="New model provider" className="rounded border border-border bg-background px-2" value={modelProvider} onChange={event => setModelProvider(event.target.value)}><option value="">Select provider</option>{Object.keys(draft.providers).map(key => <option key={key} value={key}>{key}</option>)}</select><Button size="sm" variant="outline" onClick={addModel}><Plus className="size-3" /> Model</Button></div>
          </div>
          <label className="flex items-center gap-2">Default model <select className="rounded border border-border bg-background px-2 py-1" value={draft.defaultModel ?? ""} onChange={event => change(profile => { profile.defaultModel = event.target.value || null; return profile; })}><option value="">Select at execution</option>{runnableChoices(draft).map(key => <option key={key} value={key}>{key}</option>)}</select></label>
          <Button size="sm" disabled={busy || !view.revision || Object.keys(draft.providers).length === 0 || Object.keys(draft.models).length === 0} onClick={() => void mutate(() => client!.replace(view.revision!, draft))}><Save className="size-3" /> Save OCG Profile</Button>
        </div>
      )}
    </section>
  );
}
