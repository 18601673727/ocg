"use client";

import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import { useId } from "react";
import { useI18n } from "../i18n";
import { useConversationError } from "./conversation-errors";

type SelectorField = {
  value: string;
  options: readonly { value: string; label: string }[];
  onChange: (value: string) => void;
};

export type ModelSelectorViewProps = {
  providerLabel?: string;
  effortLabel?: string;
  status: string;
  disabled: boolean;
  loading: boolean;
  error?: string;
  refresh?: { disabled: boolean; onRefresh: () => void };
  project: SelectorField & { disabled: boolean; placeholder: string };
  provider: SelectorField;
  model: SelectorField;
  effort: SelectorField;
};

export function ModelSelectorView({
  status, disabled, loading, error, refresh, project,
  provider, model, effort, providerLabel, effortLabel,
}: ModelSelectorViewProps) {
  const { t } = useI18n();
  const id = useId();
  const errorInHeader = useConversationError(error);
  const selectClass = "h-11 w-full min-w-0 rounded-md border border-border bg-background px-2 text-base text-foreground outline-none focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/20 disabled:cursor-not-allowed disabled:opacity-60 sm:h-9 sm:text-sm";

  return (
    <div className="w-full min-w-0 rounded-lg border border-border bg-muted/20 p-2">
      <p id={id + "-status"} className="sr-only">{status}</p>
      <fieldset aria-describedby={id + "-status"} aria-busy={loading} className="grid min-w-0 grid-cols-2 gap-2 sm:grid-cols-12">
        <legend className="sr-only">{t("chat.executionSettings")}</legend>
        <label className="flex min-w-0 flex-col gap-1 text-xs text-muted-foreground sm:col-span-3">
          {t("chat.project")}
          <select aria-label={t("chat.project")} value={project.value} disabled={project.disabled} onChange={event => project.onChange(event.target.value)} className={selectClass}>
            <option value="">{project.placeholder}</option>
            {project.options.map(option => <option key={option.value} value={option.value}>{option.label}</option>)}
          </select>
        </label>
        <label className="flex min-w-0 flex-col gap-1 text-xs text-muted-foreground sm:col-span-3">
          {providerLabel ?? t("chat.provider")}
          <select aria-label={providerLabel ?? t("chat.provider")} value={provider.value} disabled={disabled} onChange={event => provider.onChange(event.target.value)} className={selectClass}>
            {!provider.options.length && <option value="">{loading ? t("chat.modelsLoading") : t("chat.noModels")}</option>}
            {provider.options.map(option => <option key={option.value} value={option.value}>{option.label}</option>)}
          </select>
        </label>
        <div className="flex min-w-0 flex-col gap-1 text-xs text-muted-foreground sm:col-span-3">
          <div className="flex items-center justify-between gap-2">
            <label htmlFor={id + "-model"}>{t("chat.model")}</label>
            {refresh && <button
              type="button"
              aria-label={t(error ? "common.retry" : "chat.refreshModels")}
              disabled={refresh.disabled}
              onClick={refresh.onRefresh}
              className="rounded-sm text-xs text-muted-foreground underline-offset-2 outline-none hover:text-foreground hover:underline focus-visible:ring-2 focus-visible:ring-ring/30 disabled:cursor-not-allowed disabled:opacity-40"
            >{t(error ? "common.retry" : "common.refresh")}</button>}
          </div>
          <select id={id + "-model"} aria-label={t("chat.model")} value={model.value} disabled={disabled} onChange={event => model.onChange(event.target.value)} className={selectClass}>
            {!model.options.length && <option value="">{loading ? t("chat.modelsLoading") : t("chat.noModels")}</option>}
            {model.options.map(option => <option key={option.value} value={option.value}>{option.label}</option>)}
          </select>
        </div>
        <label className="flex min-w-0 flex-col gap-1 text-xs text-muted-foreground sm:col-span-3">
          {effortLabel ?? t("chat.effort")}
          <select aria-label={effortLabel ?? t("chat.effort")} value={effort.value} disabled={disabled || !effort.options.length} onChange={event => effort.onChange(event.target.value)} className={selectClass}>
            {!effortLabel && <option value="">{effort.options.length ? t("chat.providerDefault") : t("chat.effortUnsupported")}</option>}
            {effort.options.map(option => <option key={option.value} value={option.value}>{option.label}</option>)}
          </select>
        </label>
      </fieldset>
      {error && !errorInHeader && <DismissibleNotice message={error}  className="mt-2 break-words text-xs text-destructive">{error}</DismissibleNotice>}
    </div>
  );
}
