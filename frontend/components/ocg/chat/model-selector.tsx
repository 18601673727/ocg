"use client";

import { useEffect, useState } from "react";
import type { ChatModelSelection, Model, ProfileView } from "../contracts";
import { useI18n } from "../i18n";
import type { ActiveExecutionTarget, ActivityStep, ExecutionPhase } from "./execution-status";
import { useOcgControlUrl } from "../profile/control-url";
import { createProfileClient } from "../profile/profile-client";
import { ModelSelectorView } from "./model-selector-view";

const EFFORT_LABELS = {
  none: "chat.effortNone", minimal: "chat.effortMinimal", low: "chat.effortLow",
  medium: "chat.effortMedium", high: "chat.effortHigh", xhigh: "chat.effortXhigh",
} as const;

function efforts(model: Model | undefined, protocol: string | null | undefined): string[] {
  if (!model || model.metadata?.reasoning === false || protocol === "anthropic") return [];
  const supported = new Set([
    ...(model.metadata?.efforts ?? []), ...(model.variants ?? []), ...(model.metadata?.variants ?? []),
  ]);
  return Object.keys(EFFORT_LABELS).filter(effort => supported.has(effort));
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export function ModelSelector({ selection, onChange, onPreference, busy, active, activity }: {
  selection?: ChatModelSelection;
  onChange: (selection: ChatModelSelection) => void;
  /** Called only for the user's own next-turn choices, never for defaults. */
  onPreference?: (selection: ChatModelSelection) => void;
  busy: boolean;
  /** The running Job's frozen target. It replaces the next-turn selection on screen while set. */
  active?: ActiveExecutionTarget | null;
  /** Observable steps of the running Job. */
  activity?: { current: ActivityStep | ExecutionPhase | null; steps: ActivityStep[] };
}) {
  const { t } = useI18n();
  const baseUrl = useOcgControlUrl();
  const [refresh, setRefresh] = useState(0);
  const [result, setResult] = useState<{ baseUrl: string; refresh: number; view?: ProfileView; error?: string } | null>(null);

  useEffect(() => {
    if (!baseUrl) return;
    let disposed = false;
    void createProfileClient(baseUrl, fetch).read().then(view => {
      if (!disposed) setResult({ baseUrl, refresh, view });
    }).catch((cause: unknown) => {
      if (!disposed) setResult({ baseUrl, refresh, error: cause instanceof Error ? cause.message : String(cause) });
    });
    return () => { disposed = true; };
  }, [baseUrl, refresh, busy]);

  const current = result?.baseUrl === baseUrl && result?.refresh === refresh;
  const view = result?.baseUrl === baseUrl ? result?.view : undefined;
  const error = current ? result?.error : undefined;
  const loading = Boolean(baseUrl && !current);
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
  const requestedEffort = selection?.model === modelKey ? selection.effort : configuredEffort;
  const effort = requestedEffort && availableEfforts.includes(requestedEffort) ? requestedEffort : "";
  const effortLabel = (value: string) => {
    const key = Object.entries(EFFORT_LABELS).find(([effort]) => effort === value)?.[1];
    return key ? t(key) : value;
  };
  const disabled = busy || loading || !baseUrl || !model || Boolean(error);
  const selectModel = (key: string) => {
    if (disabled || !choices.includes(key)) return;
    const next = profile?.models[key];
    const supported = efforts(next, profile?.providers[next?.provider ?? ""]?.protocol);
    const configured = next?.variant ?? next?.metadata?.effort ?? next?.metadata?.variant;
    const chosen = { model: key, effort: configured && supported.includes(configured) ? configured : null };
    onChange(chosen);
    onPreference?.(chosen);
  };

  // Publish the displayed default too: project defaults must not silently override it.
  useEffect(() => {
    if (!disabled && modelKey && (!selection || selection.model !== modelKey || (selection.effort ?? "") !== effort)) {
      onChange({ model: modelKey, effort: effort || null });
    }
  }, [disabled, selection, modelKey, effort, onChange]);

  // While a Job runs, the header reports its frozen target, never the composer.
  // A historical identity the current Profile no longer offers is shown as
  // recorded. It is never rewritten to a model the Profile still has.
  const activeEffort = !active ? "" : active.effort ? effortLabel(active.effort) : t("chat.providerDefault");
  const title = active ? active.model
    : busy ? t("chat.executionPending") : model ? model.label || model.id : t("chat.executionSettings");
  const subtitle = active ? [active.providerKey, activeEffort].filter(Boolean).join(" · ")
    : busy ? null : model ? [profile?.providers[providerKey]?.label || providerKey, availableEfforts.length ? effort ? effortLabel(effort) : t("chat.providerDefault") : t("chat.effortUnsupported")].join(" · ") : null;
  const status = busy ? t("chat.settingsLocked") : !baseUrl ? t("chat.selectorUnavailable")
    : loading ? t("chat.modelsLoading") : error ? t("chat.modelsFailed")
      : !model ? t("chat.noModels") : t("chat.settingsNextTurn");
  return (
    <ModelSelectorView
      title={title}
      subtitle={subtitle}
      status={status}
      active={Boolean(active)}
      locked={busy || Boolean(active)}
      disabled={disabled}
      loading={loading}
      error={error}
      refresh={baseUrl ? { disabled: busy || loading, onRefresh: () => setRefresh(value => value + 1) } : undefined}
      provider={{
        value: providerKey,
        options: providers.map(key => ({ value: key, label: profile?.providers[key]?.label || key })),
        onChange: value => {
          const key = choices.find(key => profile?.models[key]?.provider === value);
          if (key) selectModel(key);
        },
      }}
      model={{
        value: modelKey,
        options: options.map(key => ({ value: key, label: profile?.models[key]?.label || profile?.models[key]?.id || key })),
        onChange: selectModel,
      }}
      effort={{
        value: effort,
        options: availableEfforts.map(value => ({ value, label: effortLabel(value) })),
        onChange: value => {
          if (disabled || (value && !availableEfforts.includes(value))) return;
          const chosen = { model: modelKey, effort: value || null };
          onChange(chosen);
          onPreference?.(chosen);
        },
      }}
      modelId={model?.label && model.label !== model.id ? model.id : undefined}
      effortUnsupported={Boolean(model && !availableEfforts.length)}
    />
  );
}
