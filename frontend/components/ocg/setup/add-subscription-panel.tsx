"use client";

import { useCallback, useMemo, useRef, useState } from "react";
import { Check, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useI18n } from "../i18n";
import { useOcgControlUrl } from "../profile/control-url";
import { createProfileClient } from "../profile/profile-client";
import type { SetupModel } from "../contracts";
import { PROVIDER_PRESETS } from "./provider-presets";
import { createSetupClient } from "./setup-client";

interface Connected {
  providerKey: string;
  revision: string;
  models: SetupModel[];
}

/**
 * Adds a provider or subscription to an already configured OCG. Everything is
 * backend-owned: connect lists `/models` and stores the key in the Vault before
 * writing anything, and saving models keeps the current default model. The API
 * key lives only in the uncontrolled input and is cleared after each attempt.
 */
export function AddSubscriptionPanel() {
  const { t } = useI18n();
  const controlUrl = useOcgControlUrl();
  const apiKeyInput = useRef<HTMLInputElement>(null);
  const [name, setName] = useState("");
  const [endpoint, setEndpoint] = useState("");
  const [connected, setConnected] = useState<Connected | null>(null);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [busy, setBusy] = useState<"connect" | "save" | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [savedCount, setSavedCount] = useState<number | null>(null);
  const setupClient = useMemo(() => (controlUrl ? createSetupClient(controlUrl, fetch) : null), [controlUrl]);

  const connect = useCallback(async () => {
    const apiKey = apiKeyInput.current?.value.trim() ?? "";
    if (!controlUrl || !setupClient) {
      setError(t("setup.noEndpoint"));
      return;
    }
    if (!name.trim() || !endpoint.trim() || !apiKey) {
      setError(t("setup.required"));
      return;
    }
    setBusy("connect");
    setError(null);
    setSavedCount(null);
    try {
      const profileClient = createProfileClient(controlUrl, fetch);
      if (!(await profileClient.read())?.profile) await profileClient.createNew();
      const result = await setupClient.connectProvider(name.trim(), endpoint.trim(), apiKey);
      setConnected({ providerKey: result.provider_key, revision: result.revision, models: result.models });
      setSelected(new Set(result.models[0] ? [result.models[0].key] : []));
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : t("setup.connectFailed"));
    } finally {
      if (apiKeyInput.current) apiKeyInput.current.value = "";
      setBusy(null);
    }
  }, [controlUrl, setupClient, name, endpoint, t]);

  const save = useCallback(async () => {
    if (!connected || !setupClient) return;
    if (selected.size === 0) {
      setError(t("setup.selectModel"));
      return;
    }
    setBusy("save");
    setError(null);
    try {
      const models = connected.models.filter((model) => selected.has(model.key)).map((model) => ({ key: model.key, id: model.id }));
      // No default is sent: adding a subscription must not change what new Chats use.
      const result = await setupClient.saveModels(connected.providerKey, models, undefined, connected.revision);
      setSavedCount(result.selected_models.length);
      setConnected(null);
      setSelected(new Set());
      setName("");
      setEndpoint("");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : t("setup.saveFailed"));
    } finally {
      setBusy(null);
    }
  }, [connected, setupClient, selected, t]);

  function toggle(key: string) {
    setSelected((current) => {
      const next = new Set(current);
      if (!next.delete(key)) next.add(key);
      return next;
    });
  }

  const loading = busy !== null;
  return (
    <div className="space-y-4 rounded-lg border border-border p-4" aria-busy={loading}>
      <p className="text-sm leading-6 text-muted-foreground">{t("settings.addSubscriptionDesc")}</p>
      {error && <p role="alert" className="break-words rounded-md border border-destructive/30 bg-destructive/5 p-3 text-sm text-destructive">{error}</p>}
      {savedCount !== null && (
        <p role="status" className="flex items-start gap-2 rounded-md border border-emerald-500/30 bg-emerald-500/5 p-3 text-sm">
          <Check className="mt-0.5 size-4 shrink-0 text-emerald-600" />{t("settings.addSubscriptionSaved", { count: savedCount })}
        </p>
      )}
      {!connected ? (
        <div className="space-y-3">
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-[13px] font-medium">{t("setup.presets")}</span>
            {PROVIDER_PRESETS.map((preset) => (
              <Button key={preset.id} type="button" size="sm" variant="outline" className="min-h-11 rounded-md tracking-normal normal-case" disabled={loading} onClick={() => { setName(preset.label); setEndpoint(preset.endpoint); }}>
                {preset.label}
              </Button>
            ))}
          </div>
          <div className="grid min-w-0 gap-3 sm:grid-cols-2">
            <label className="flex min-w-0 flex-col gap-1.5 text-[13px] font-medium">{t("setup.providerName")}
              <Input value={name} onChange={(event) => setName(event.target.value)} disabled={loading} />
            </label>
            <label className="flex min-w-0 flex-col gap-1.5 text-[13px] font-medium">{t("setup.providerEndpoint")}
              <Input type="url" autoComplete="off" spellCheck={false} placeholder="https://api.example.com/v1" value={endpoint} onChange={(event) => setEndpoint(event.target.value)} disabled={loading} />
            </label>
            <label className="flex min-w-0 flex-col gap-1.5 text-[13px] font-medium sm:col-span-2">{t("setup.apiKey")}
              <Input type="password" autoComplete="off" ref={apiKeyInput} disabled={loading} />
            </label>
          </div>
          <Button size="sm" className="min-h-11 rounded-md tracking-normal normal-case" disabled={loading} onClick={() => void connect()}>
            {busy === "connect" && <Loader2 className="size-4 animate-spin" />}{t(busy === "connect" ? "setup.connecting" : "setup.connect")}
          </Button>
        </div>
      ) : (
        <div className="space-y-3">
          <p className="text-[13px] text-muted-foreground">{t("settings.addSubscriptionModels")}</p>
          <ul className="max-h-72 space-y-1 overflow-y-auto rounded-md border border-border p-2">
            {connected.models.map((model) => (
              <li key={model.key}>
                <label className="flex min-h-11 cursor-pointer items-center gap-2 rounded-md px-2 text-sm hover:bg-muted">
                  <input type="checkbox" checked={selected.has(model.key)} onChange={() => toggle(model.key)} disabled={loading} aria-label={t("setup.selectNamedModel", { model: model.label })} />
                  <span className="min-w-0 break-all">{model.label}</span>
                </label>
              </li>
            ))}
          </ul>
          <Button size="sm" className="min-h-11 rounded-md tracking-normal normal-case" disabled={loading} onClick={() => void save()}>
            {busy === "save" && <Loader2 className="size-4 animate-spin" />}{t("settings.addSubscriptionSave")}
          </Button>
        </div>
      )}
    </div>
  );
}
