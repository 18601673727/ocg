"use client";

import { useId, useState } from "react";
import { ChevronDown, LockKeyhole, RefreshCw, SlidersHorizontal } from "lucide-react";
import { cn } from "@/lib/utils";
import { STATUS_TEXT } from "../primitives/tone";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { useI18n } from "../i18n";

type SelectorField = {
  value: string;
  options: readonly { value: string; label: string }[];
  onChange: (value: string) => void;
};

export type ModelSelectorViewProps = {
  providerLabel?: string;
  effortLabel?: string;
  title: string;
  subtitle: string | null;
  status: string;
  active: boolean;
  locked: boolean;
  disabled: boolean;
  loading: boolean;
  error?: string;
  refresh?: { disabled: boolean; onRefresh: () => void };
  provider: SelectorField;
  model: SelectorField;
  effort: SelectorField;
  modelId?: string;
  effortUnsupported: boolean;
};

export function ModelSelectorView({
  title, subtitle, status, active, locked, disabled, loading, error, refresh,
  provider, model, effort, modelId, effortUnsupported, providerLabel, effortLabel,
}: ModelSelectorViewProps) {
  const { t } = useI18n();
  const id = useId();
  const [expanded, setExpanded] = useState(false);
  const selectClass = "h-11 w-full min-w-0 rounded-md border border-border bg-background px-2 text-base text-foreground outline-none focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/20 disabled:cursor-not-allowed disabled:opacity-60 sm:h-9 sm:text-sm";

  return (
    // A compact dropdown button beside the Project button, not a full-width
    // bar: it hugs its label up to a share of the row, and long model names
    // ellipsize. The raised panel still resolves against the composer's
    // selector row, so it can open wider than the button itself.
    <div className="min-w-0 max-w-[min(26rem,58%)] shrink rounded-full border border-border bg-background/80 px-1.5 shadow-sm">
      <div className="flex min-w-0 items-center gap-1">
        <button
          type="button"
          aria-label={t("chat.executionSettings")}
          aria-expanded={expanded}
          aria-controls={id + "-panel"}
          aria-describedby={id + "-status"}
          onClick={() => setExpanded(value => !value)}
          className="flex min-h-9 min-w-0 flex-1 items-center gap-2 rounded-full px-1 py-1 text-left outline-none hover:bg-muted/50 focus-visible:ring-2 focus-visible:ring-ring/30 pointer-coarse:min-h-11"
        >
          <SlidersHorizontal className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
          <span className="min-w-0 flex-1">
            <span className="block truncate text-xs font-medium" title={active ? t("chat.activeExecution") : undefined}>{title}</span>
            <span
              id={id + "-status"}
              className={cn("block truncate text-xs", error ? "text-destructive" : "text-muted-foreground")}
              title={error}
            >
              {error ?? subtitle ?? status}
            </span>
          </span>          {locked && <LockKeyhole className={`size-3.5 shrink-0 ${STATUS_TEXT['warning']}`} aria-hidden="true" />}
          <ChevronDown className={cn("size-3.5 shrink-0 text-muted-foreground transition-transform", expanded && "rotate-180")} aria-hidden="true" />
        </button>
        {refresh && <Tooltip><TooltipTrigger render={<button
          type="button"
          aria-label={t(error ? "common.retry" : "chat.refreshModels")}
          disabled={refresh.disabled}
          onClick={refresh.onRefresh}
          className="flex size-9 shrink-0 items-center justify-center rounded-full text-muted-foreground outline-none hover:bg-muted hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring/30 disabled:cursor-not-allowed disabled:opacity-40"
        >
          <RefreshCw className={cn("size-3.5", loading && "animate-spin")} aria-hidden="true" />
        </button>} /><TooltipContent>{t(error ? "common.retry" : "chat.refreshModels")}</TooltipContent></Tooltip>}
      </div>
      {expanded && <div id={id + "-panel"} className="absolute bottom-full left-0 z-30 mb-2 max-h-[min(40dvh,20rem)] w-full space-y-2 overflow-y-auto overscroll-contain rounded-lg border border-border bg-popover p-2 text-popover-foreground shadow-md [@media(max-height:600px)]:max-h-[30dvh]">
        <fieldset disabled={disabled} aria-describedby={id + "-status"} className="grid min-w-0 grid-cols-1 gap-2 sm:grid-cols-12">
          <legend className="sr-only">{t("chat.executionSettings")}</legend>
          <label className="flex min-w-0 flex-col gap-1 text-xs text-muted-foreground sm:col-span-3">
            {providerLabel ?? t("chat.provider")}
            <select aria-label={providerLabel ?? t("chat.provider")} value={provider.value} onChange={event => provider.onChange(event.target.value)} className={selectClass}>
              {!provider.options.length && <option value="">{loading ? t("chat.modelsLoading") : t("chat.noModels")}</option>}
              {provider.options.map(option => <option key={option.value} value={option.value}>{option.label}</option>)}
            </select>
          </label>
          <label className="flex min-w-0 flex-col gap-1 text-xs text-muted-foreground sm:col-span-6">
            {t("chat.model")}
            <select aria-label={t("chat.model")} value={model.value} onChange={event => model.onChange(event.target.value)} className={selectClass}>
              {!model.options.length && <option value="">{loading ? t("chat.modelsLoading") : t("chat.noModels")}</option>}
              {model.options.map(option => <option key={option.value} value={option.value}>{option.label}</option>)}
            </select>
          </label>
          <label className="flex min-w-0 flex-col gap-1 text-xs text-muted-foreground sm:col-span-3">
            {effortLabel ?? t("chat.effort")}
            <select aria-label={effortLabel ?? t("chat.effort")} value={effort.value} disabled={!effort.options.length} onChange={event => effort.onChange(event.target.value)} className={selectClass}>
              {!effortLabel && <option value="">{effort.options.length ? t("chat.providerDefault") : t("chat.effortUnsupported")}</option>}
              {effort.options.map(option => <option key={option.value} value={option.value}>{option.label}</option>)}
            </select>
          </label>
        </fieldset>
        {modelId && <p className="truncate font-mono text-xs text-muted-foreground" title={modelId}>{modelId}</p>}
        {effortUnsupported && <p className="text-xs text-muted-foreground">{t("chat.effortUnsupportedHint")}</p>}
      </div>}
      {error && <p role="alert" className="mt-2 break-words text-xs text-destructive">{error}</p>}
    </div>
  );
}
