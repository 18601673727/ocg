"use client";

import { useEffect, useMemo, useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import type { ControlCapability, ControlProfilesSnapshot, ControlProviderModel } from "../contracts";
import type { HttpCanonicalControlClient } from "../runtime/control-client";
import { useI18n } from "../i18n";
import { ProtocolSelect, type LlmProtocol } from "./protocol-select";
import { describeError } from "./settings-parts";

export function ProviderModelImport({ client, snapshot, onImported }: {
  client: HttpCanonicalControlClient;
  snapshot: ControlProfilesSnapshot | null;
  onImported: (message: string) => void;
}) {
  const { t } = useI18n();
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  return <>
    <Button variant="outline" size="sm" onClick={() => setOpen(true)}>{t("settings.llm.importModels")}</Button>
    <Dialog open={open} onOpenChange={(next) => { if (!busy) setOpen(next); }}>
      {open && <ImportContent client={client} snapshot={snapshot} onBusyChange={setBusy} close={() => setOpen(false)} onImported={onImported} />}
    </Dialog>
  </>;
}

function ImportContent({ client, snapshot, close, onImported, onBusyChange }: {
  client: HttpCanonicalControlClient;
  snapshot: ControlProfilesSnapshot | null;
  onBusyChange: (busy: boolean) => void;
  close: () => void;
  onImported: (message: string) => void;
}) {
  const { t } = useI18n();
  const providers = snapshot?.providers ?? [];
  const [sourceId, setSourceId] = useState(providers[0]?.profile_ids[0] ?? "new");
  const [newName, setNewName] = useState("");
  const [newProtocol, setNewProtocol] = useState<LlmProtocol>("openai_chat_completions");
  const [newEndpoint, setNewEndpoint] = useState("");
  const [newSecret, setNewSecret] = useState("");
  const [newReference] = useState(() => `OCG_IMPORT_${crypto.randomUUID().replaceAll("-", "").toUpperCase()}_API_KEY`);
  const [newConnection, setNewConnection] = useState<{ protocol: LlmProtocol; name: string; endpoint: string; credential?: string; credential_ref?: string } | null>(null);
  const provider = providers.find((entry) => entry.profile_ids.includes(sourceId));
  const [models, setModels] = useState<ControlProviderModel[]>([]);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [capabilities, setCapabilities] = useState<ControlCapability[]>(["chat"]);
  const [query, setQuery] = useState("");
  const [revision, setRevision] = useState(0);
  const [loading, setLoading] = useState(Boolean(provider));
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // A committed import stays committed even when the following catalog refresh fails.
  const [result, setResult] = useState<string | null>(null);
  const endpoint = sourceId === "new" ? newConnection?.endpoint : provider?.endpoint ?? "https://api.openai.com/v1";
  const credentialRef = sourceId === "new" ? undefined : provider?.credential_ref;
  const credential = sourceId === "new" ? newConnection?.credential : undefined;
  useEffect(() => {
    if (!endpoint) return;
    const controller = new AbortController();
    let active = true;
    void client.discoverProviderModels({ endpoint, credential_ref: credentialRef, credential }, controller.signal)
      .then((answer) => {
        if (active) setModels([...new Map(answer.models.map((model) => [model.id, model])).values()].sort((a, b) => a.id.localeCompare(b.id)));
      })
      .catch((reason) => { if (active) setError(describeError(reason, t)); })
      .finally(() => { if (active) setLoading(false); });
    return () => { active = false; controller.abort(); };
  }, [client, endpoint, credentialRef, credential, sourceId, revision, t]);
  const existing = new Set(provider?.models ?? []);
  const matching = useMemo(() => models.filter((model) => model.id.toLocaleLowerCase().includes(query.trim().toLocaleLowerCase())), [models, query]);
  function reload(id = sourceId) {
    setSourceId(id); setModels([]); setSelected(new Set()); setQuery(""); setError(null); setResult(null); setLoading(id !== "new"); setRevision((value) => value + 1);
  }
  function changeNewConnection(update: () => void) {
    update(); setNewConnection(null); setModels([]); setSelected(new Set()); setLoading(false); setError(null); setResult(null);
  }
  function fetchNewModels() {
    if (!newName.trim() || !newEndpoint.trim()) { setError(t("settings.llm.newProviderRequired")); return; }
    setError(null); setModels([]); setSelected(new Set()); setLoading(true);
    setNewConnection({ protocol: newProtocol, name: newName.trim(), endpoint: newEndpoint.trim(), ...(newSecret.trim() ? { credential: newSecret.trim(), credential_ref: newReference } : {}) });
    setRevision((value) => value + 1);
  }
  async function save() {
    const source = sourceId === "new" ? newConnection : sourceId;
    if (!source) return;
    setSaving(true); onBusyChange(true); setError(null);
    try {
      const answer = await client.importModels(source, models.filter((model) => selected.has(model.id) && !existing.has(model.id)), capabilities);
      const message = t("settings.llm.imported", { created: answer.created, existing: answer.existing });
      setResult(message);
      setSelected(new Set());
      onImported(message);
      if (sourceId === "new") { setNewSecret(""); setNewConnection(null); close(); }
    } catch (reason) { setError(describeError(reason, t)); }
    finally { setSaving(false); onBusyChange(false); }
  }
  return <DialogContent className="flex max-h-[85dvh] flex-col gap-4 sm:max-w-2xl" showCloseButton={!saving}>
    <DialogHeader>
      <DialogTitle>{t("settings.llm.importModels")}</DialogTitle>
      <DialogDescription>{t("settings.llm.importDescription")}</DialogDescription>
    </DialogHeader>
    <div className="flex min-h-0 flex-1 flex-col gap-3 overflow-y-auto overscroll-contain">
      <div role="group" aria-label={t("settings.llm.provider")} className="flex max-h-28 shrink-0 flex-wrap gap-2 overflow-y-auto">
        {providers.map((entry) => {
          const id = entry.profile_ids[0];
          const name = snapshot?.llm_profiles.find((profile) => profile.id === id)?.name;
          return <Button key={id} variant={entry === provider ? "secondary" : "outline"} size="sm" aria-pressed={entry === provider} disabled={saving} className="h-auto max-w-full flex-col items-start gap-0.5 whitespace-normal text-left" onClick={() => { setNewSecret(""); setNewConnection(null); reload(id); }}>
            <span>{name}</span><span className="break-all text-xs text-muted-foreground">{entry.endpoint ?? "api.openai.com"}</span>
          </Button>;
        })}
        <Button variant={sourceId === "new" ? "secondary" : "outline"} size="sm" aria-pressed={sourceId === "new"} disabled={saving} onClick={() => reload("new")}>{t("settings.llm.newProvider")}</Button>
      </div>
      {sourceId === "new" && <div className="grid shrink-0 grid-cols-1 gap-3 sm:grid-cols-2">
        <ProtocolSelect value={newProtocol} disabled={saving} onChange={(value) => changeNewConnection(() => setNewProtocol(value))} />
        <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">{t("settings.llm.provider")}<Input value={newName} disabled={saving} onChange={(event) => changeNewConnection(() => setNewName(event.target.value))} placeholder="Provider" autoComplete="off" /></label>
        <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">{t("settings.llm.importEndpoint")}<Input value={newEndpoint} disabled={saving} onChange={(event) => changeNewConnection(() => setNewEndpoint(event.target.value))} placeholder="https://api.example.com/v1" spellCheck={false} autoComplete="off" /></label>
        <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">{t("settings.llm.apiKey")}<Input type="password" value={newSecret} disabled={saving} onChange={(event) => changeNewConnection(() => setNewSecret(event.target.value))} autoComplete="new-password" spellCheck={false} /></label>
        <Button className="self-end" variant="outline" disabled={saving || loading} onClick={fetchNewModels}>{loading ? t("settings.llm.fetching") : t("settings.llm.fetchModels")}</Button>
      </div>}
      <div className="flex gap-2">
        <Input aria-label={t("settings.llm.searchModels")} placeholder={t("settings.llm.searchModels")} value={query} disabled={saving} onChange={(event) => setQuery(event.target.value)} />
        {sourceId !== "new" && <Button variant="outline" size="sm" disabled={loading || saving} onClick={() => reload()}>{t("settings.llm.fetchModels")}</Button>}
      </div>
      <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
        <Button variant="ghost" size="xs" disabled={saving || loading} onClick={() => setSelected(new Set(matching.filter((model) => !existing.has(model.id)).slice(0, 200).map((model) => model.id)))}>{t("settings.llm.selectVisible")}</Button>
        <Button variant="ghost" size="xs" disabled={saving || !selected.size} onClick={() => setSelected(new Set())}>{t("settings.llm.clearSelection")}</Button>
        <span className="ml-auto tabular-nums">{selected.size} / 200</span>
      </div>
      <div aria-busy={loading} className="min-h-24 flex-1 overflow-y-auto overscroll-contain rounded-md border border-border">
        {loading ? <p role="status" className="p-3 text-muted-foreground">{t("settings.llm.fetching")}</p> : matching.length === 0 ? <p className="p-3 text-muted-foreground">{t("settings.llm.noModels")}</p> : matching.map((model) => <label key={model.id} className="flex cursor-pointer items-start gap-3 border-b border-border/50 p-3 last:border-b-0 hover:bg-muted/40 has-disabled:cursor-default has-disabled:text-muted-foreground">
          <input type="checkbox" className="mt-0.5 size-4 shrink-0 accent-primary" checked={existing.has(model.id) || selected.has(model.id)} disabled={saving || existing.has(model.id) || (!selected.has(model.id) && selected.size >= 200)} onChange={() => setSelected((current) => { const next = new Set(current); if (!next.delete(model.id)) next.add(model.id); return next; })} />
          <span className="min-w-0 flex-1 break-all">{model.id}{model.context_window ? <span className="mt-1 block text-xs text-muted-foreground">{model.context_window.toLocaleString()} Token</span> : null}</span>
          {existing.has(model.id) && <span className="shrink-0 text-xs">{t("settings.llm.alreadyAdded")}</span>}
        </label>)}
      </div>
      <fieldset disabled={saving} className="shrink-0">
        <legend className="mb-2 text-xs font-medium text-muted-foreground">{t("settings.llm.capabilities")}</legend>
        <div className="flex flex-wrap gap-4">
          {(["chat", "tools", "vision", "reasoning"] as const).map((cap) => <label key={cap} className="flex items-center gap-1.5 text-sm"><input type="checkbox" className="size-4 accent-primary" checked={capabilities.includes(cap)} onChange={() => setCapabilities((current) => current.includes(cap) ? current.filter((value) => value !== cap) : [...current, cap])} />{t(cap === "chat" ? "settings.llm.capabilityChat" : cap === "tools" ? "settings.llm.capabilityTools" : cap === "vision" ? "settings.llm.capabilityVision" : "settings.llm.capabilityReasoning")}</label>)}
        </div>
        <p className="mt-2 text-xs leading-5 text-muted-foreground">{t("settings.llm.capabilityNote")}</p>
      </fieldset>
      <p className="text-xs text-muted-foreground">{t("settings.llm.importLimit")}</p>
    </div>
    {error && <p role="alert" className="break-words text-sm text-destructive">{error}</p>}
    {result && <p role="status" className="text-sm text-emerald-700 dark:text-emerald-400">{result}</p>}
    <DialogFooter>
      <Button variant="outline" disabled={saving} onClick={close}>{t("common.cancel")}</Button>
      <Button disabled={saving || loading || !selected.size || !capabilities.length} onClick={() => void save()}>{saving ? t("settings.llm.importing") : t("settings.llm.importSelected", { count: selected.size })}</Button>
    </DialogFooter>
  </DialogContent>;
}
