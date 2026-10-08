"use client";

import { useEffect, useId, useState } from "react";
import { ChevronDown, LockKeyhole, RefreshCw, SlidersHorizontal } from "lucide-react";
import { cn } from "@/lib/utils";
import type { ChatModelSelection, Model, ProfileView } from "../contracts";
import { useI18n } from "../i18n";
import type { ActiveExecutionTarget, ActivityStep, ExecutionPhase } from "./execution-status";
import { useOcgControlUrl } from "../profile/control-url";
import { createProfileClient } from "../profile/profile-client";

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
  const id = useId();
  const [expanded, setExpanded] = useState(false);
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
    if (disabled) return;
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
  const selectClass = "h-11 w-full min-w-0 rounded-md border border-border bg-background px-2 text-base text-foreground outline-none focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/20 disabled:cursor-not-allowed disabled:opacity-60 sm:h-9 sm:text-sm";

  return (
    <div className="mb-2 px-1 py-1">
      <div className="flex min-w-0 items-center gap-2">
        <button
          type="button"
          aria-label={t("chat.executionSettings")}
          aria-expanded={expanded}
          aria-controls={id + "-panel"}
          aria-describedby={id + "-status"}
          onClick={() => setExpanded(value => !value)}
          className="flex min-h-11 min-w-0 flex-1 items-center gap-2 rounded-md py-1 text-left outline-none hover:bg-muted/50 focus-visible:ring-2 focus-visible:ring-ring/30"
        >
          <SlidersHorizontal className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
          <span className="min-w-0 flex-1">
            <span className="block truncate text-[12px] font-medium" title={active ? t("chat.activeExecution") : undefined}>{title}</span>
            <span className="block truncate text-[10px] text-muted-foreground">
              {subtitle ?? status}
            </span>
          </span>
          {(busy || active) && <LockKeyhole className="size-3.5 shrink-0 text-amber-600 dark:text-amber-400" aria-hidden="true" />}
          <ChevronDown className={cn("size-3.5 shrink-0 text-muted-foreground transition-transform", expanded && "rotate-180")} aria-hidden="true" />
        </button>
        {baseUrl && <button
          type="button"
          aria-label={t(error ? "common.retry" : "chat.refreshModels")}
          title={t(error ? "common.retry" : "chat.refreshModels")}
          disabled={busy || loading}
          onClick={() => setRefresh(value => value + 1)}
          className="flex size-11 shrink-0 items-center justify-center rounded-md text-muted-foreground outline-none hover:bg-muted hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring/30 disabled:cursor-not-allowed disabled:opacity-40 sm:size-9"
        >
          <RefreshCw className={cn("size-3.5", loading && "animate-spin")} aria-hidden="true" />
        </button>}
      </div>
      {expanded && <div id={id + "-panel"} className="mt-2 max-h-[min(25dvh,12rem)] space-y-2 overflow-y-auto overscroll-contain border-t border-border pt-2 [@media(max-height:600px)]:max-h-[18dvh]">
        <fieldset disabled={disabled} aria-describedby={id + "-status"} className="grid min-w-0 grid-cols-1 gap-2 sm:grid-cols-12">
          <legend className="sr-only">{t("chat.executionSettings")}</legend>
          <label className="flex min-w-0 flex-col gap-1 text-[11px] text-muted-foreground sm:col-span-3">
            {t("chat.provider")}
            <select aria-label={t("chat.provider")} value={providerKey} onChange={event => {
              const key = choices.find(key => profile?.models[key]?.provider === event.target.value);
              if (key) selectModel(key);
            }} className={selectClass}>
              {!providers.length && <option value="">{loading ? t("chat.modelsLoading") : t("chat.noModels")}</option>}
              {providers.map(key => <option key={key} value={key}>{profile?.providers[key]?.label || key}</option>)}
            </select>
          </label>
          <label className="flex min-w-0 flex-col gap-1 text-[11px] text-muted-foreground sm:col-span-6">
            {t("chat.model")}
            <select aria-label={t("chat.model")} value={modelKey} onChange={event => selectModel(event.target.value)} className={selectClass}>
              {!options.length && <option value="">{loading ? t("chat.modelsLoading") : t("chat.noModels")}</option>}
              {options.map(key => <option key={key} value={key}>{profile?.models[key]?.label || profile?.models[key]?.id || key}</option>)}
            </select>
          </label>
          <label className="flex min-w-0 flex-col gap-1 text-[11px] text-muted-foreground sm:col-span-3">
            {t("chat.effort")}
            <select aria-label={t("chat.effort")} value={effort} disabled={!availableEfforts.length} onChange={event => {
              if (disabled) return;
              const chosen = { model: modelKey, effort: event.target.value || null };
              onChange(chosen);
              onPreference?.(chosen);
            }} className={selectClass}>
              <option value="">{availableEfforts.length ? t("chat.providerDefault") : t("chat.effortUnsupported")}</option>
              {availableEfforts.map(value => <option key={value} value={value}>{effortLabel(value)}</option>)}
            </select>
          </label>
        </fieldset>
        {model?.label && model.label !== model.id && <p className="truncate font-mono text-[10px] text-muted-foreground" title={model.id}>{model.id}</p>}
        {model && !availableEfforts.length && <p className="text-[10px] text-muted-foreground">{t("chat.effortUnsupportedHint")}</p>}
      </div>}
      {error && <p role="alert" className="mt-2 break-words text-[11px] text-destructive">{error}</p>}
    </div>
  );
}
