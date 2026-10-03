"use client";

import { useEffect, useState } from "react";
import type { ChatModelSelection, Model, ProfileView } from "../contracts";
import { useI18n } from "../i18n";
import { useOcgControlUrl } from "../profile/control-url";
import { createProfileClient } from "../profile/profile-client";

const WIRE_EFFORTS = new Set(["none", "minimal", "low", "medium", "high", "xhigh"]);

function efforts(model: Model | undefined, protocol: string | null | undefined): string[] {
  if (!model || protocol === "anthropic") return [];
  return [...new Set([
    ...(model.metadata?.efforts ?? []), ...(model.variants ?? []),
    ...(model.metadata?.variants ?? []),
  ])].filter(effort => WIRE_EFFORTS.has(effort));
}

export function ModelSelector({ selection, onChange, busy }: {
  selection?: ChatModelSelection;
  onChange: (selection: ChatModelSelection) => void;
  busy: boolean;
}) {
  const { t } = useI18n();
  const baseUrl = useOcgControlUrl();
  const [view, setView] = useState<ProfileView | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [refresh, setRefresh] = useState(0);

  useEffect(() => {
    if (!baseUrl) return;
    let disposed = false;
    void createProfileClient(baseUrl, fetch).read().then(result => {
      if (disposed) return;
      setView(result);
      setError(null);
    }).catch((cause: unknown) => {
      if (!disposed) setError(cause instanceof Error ? cause.message : String(cause));
    });
    return () => { disposed = true; };
  }, [baseUrl, refresh, busy]);

  const profile = view?.profile;
  const choices = view?.runnable_choices ?? [];
  const modelKey = selection && choices.includes(selection.model) ? selection.model : (choices.includes(profile?.defaultModel ?? "")
    ? profile?.defaultModel ?? "" : choices[0] ?? "");
  const model = profile?.models[modelKey];
  const providerKey = model?.provider ?? "";
  const providers = [...new Set(choices.flatMap(key => profile?.models[key]?.provider ?? []))];
  const options = choices.filter(key => profile?.models[key]?.provider === providerKey);
  const availableEfforts = efforts(model, profile?.providers[providerKey]?.protocol);
  const configuredEffort = model?.variant ?? model?.metadata?.effort ?? model?.metadata?.variant;
  const effort = selection?.model === modelKey ? selection.effort ?? "" : (configuredEffort && availableEfforts.includes(configuredEffort) ? configuredEffort : "");
  const disabled = busy || !model || Boolean(error);
  const selectModel = (key: string) => {
    const next = profile?.models[key];
    const supported = efforts(next, profile?.providers[next?.provider ?? ""]?.protocol);
    const configured = next?.variant ?? next?.metadata?.effort ?? next?.metadata?.variant;
    onChange({ model: key, effort: configured && supported.includes(configured) ? configured : null });
  };

  // Publish the displayed default too: project defaults must not silently override it.
  useEffect(() => {
    if (!busy && (!selection || selection.model !== modelKey) && modelKey && !error) onChange({ model: modelKey, effort: effort || null });
  }, [busy, selection, modelKey, effort, error, onChange]);

  return (
    <div className="border-b border-border px-3 py-2">
      <fieldset disabled={disabled} aria-describedby="chat-model-status" className="flex flex-wrap gap-2 disabled:opacity-60">
        <legend className="sr-only">{t("chat.executionSettings")}</legend>
        <label className="flex min-w-0 flex-1 flex-col gap-1 text-[10px] text-muted-foreground">
          {t("chat.provider")}
          <select aria-label={t("chat.provider")} value={providerKey} onChange={event => {
            const key = choices.find(key => profile?.models[key]?.provider === event.target.value);
            if (key) selectModel(key);
          }} className="w-full rounded border border-border bg-background p-1 text-xs text-foreground">
            {!providers.length && <option value="">{t("chat.noModels")}</option>}
            {providers.map(key => <option key={key} value={key}>{profile?.providers[key]?.label || key}</option>)}
          </select>
        </label>
        <label className="flex min-w-0 flex-[2] flex-col gap-1 text-[10px] text-muted-foreground">
          {t("chat.model")}
          <select aria-label={t("chat.model")} value={modelKey} onChange={event => selectModel(event.target.value)} className="w-full rounded border border-border bg-background p-1 text-xs text-foreground">
            {!options.length && <option value="">{t("chat.noModels")}</option>}
            {options.map(key => <option key={key} value={key}>{profile?.models[key]?.label || profile?.models[key]?.id || key}</option>)}
          </select>
        </label>
        <label className="flex min-w-0 flex-1 flex-col gap-1 text-[10px] text-muted-foreground">
          {t("chat.effort")}
          <select aria-label={t("chat.effort")} value={effort} disabled={!availableEfforts.length} onChange={event => onChange({ model: modelKey, effort: event.target.value || null })} className="w-full rounded border border-border bg-background p-1 text-xs text-foreground">
            <option value="">{availableEfforts.length ? t("chat.providerDefault") : t("chat.effortUnsupported")}</option>
            {availableEfforts.map(value => <option key={value} value={value}>{value}</option>)}
          </select>
        </label>
      </fieldset>
      <p id="chat-model-status" role="status" className="mt-1.5 flex items-center gap-1.5 text-[11px] text-muted-foreground">
        {busy && <span className="size-1.5 animate-pulse rounded-full bg-amber-500" aria-hidden="true" />}
        {busy ? t("chat.settingsLocked") : !baseUrl ? t("chat.selectorUnavailable") : error ?? (!view ? t("chat.modelsLoading") : !model ? t("chat.noModels") : t("chat.settingsNextTurn"))}
        {!busy && baseUrl && <button type="button" onClick={() => setRefresh(value => value + 1)} className="ml-auto underline">{t("common.refresh")}</button>}
      </p>
    </div>
  );
}
