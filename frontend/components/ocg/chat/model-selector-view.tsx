"use client";

import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
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
  executor: SelectorField;
  defaultEffort?: string | null;
  status: string;
  disabled: boolean;
  loading: boolean;
  error?: string;
  project: SelectorField & { disabled: boolean; placeholder: string };
  provider: SelectorField;
  model: SelectorField;
  effort: SelectorField;
};

export function ModelSelectorView({
  status,
  disabled,
  loading,
  error,
  project,
  provider,
  model,
  effort,
  providerLabel,
  executor,
  defaultEffort,
}: ModelSelectorViewProps) {
  const { t } = useI18n();
  const id = useId();
  const errorInHeader = useConversationError(error);
  const selectClass =
    "h-11 w-full min-w-0 rounded-md border border-border bg-background px-2 text-base text-foreground outline-none focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/20 disabled:cursor-not-allowed disabled:opacity-60 sm:h-9 sm:text-sm";

  return (
    <div className="w-full min-w-0 rounded-lg border border-border bg-muted/20 p-2">
      <p id={id + "-status"} className="sr-only">
        {status}
      </p>
      <fieldset
        aria-describedby={id + "-status"}
        aria-busy={loading}
        className="grid min-w-0 grid-cols-2 gap-2 sm:grid-cols-5"
      >
        <legend className="sr-only">{t("chat.executionSettings")}</legend>
        <label className="flex min-w-0 flex-col gap-1 text-xs text-muted-foreground">
          {t("chat.project")}
          <Select
            value={project.value}
            items={[{ value: "", label: project.placeholder }, ...project.options]}
            disabled={project.disabled}
            onValueChange={(next) => {
              if (next !== null) project.onChange(next);
            }}
          >
            <SelectTrigger aria-label={t("chat.project")} className={selectClass}>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="">{project.placeholder}</SelectItem>
              {project.options.map((option) => (
                <SelectItem key={option.value} value={option.value}>
                  {option.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </label>
        <label className="flex min-w-0 flex-col gap-1 text-xs text-muted-foreground">
          {providerLabel ?? t("chat.provider")}
          <Select
            value={provider.value}
            items={
              provider.options.length
                ? provider.options
                : [
                    {
                      value: "",
                      label: loading ? t("chat.modelsLoading") : t("chat.noModels"),
                    },
                  ]
            }
            disabled={disabled}
            onValueChange={(next) => {
              if (next !== null) provider.onChange(next);
            }}
          >
            <SelectTrigger aria-label={providerLabel ?? t("chat.provider")} className={selectClass}>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {!provider.options.length && (
                <SelectItem value="">
                  {loading ? t("chat.modelsLoading") : t("chat.noModels")}
                </SelectItem>
              )}
              {provider.options.map((option) => (
                <SelectItem key={option.value} value={option.value}>
                  {option.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </label>
        <div className="flex min-w-0 flex-col gap-1 text-xs text-muted-foreground">
          <label htmlFor={id + "-model"}>{t("chat.model")}</label>
          <Select
            value={model.value}
            items={
              model.options.length
                ? model.options
                : [
                    {
                      value: "",
                      label: loading ? t("chat.modelsLoading") : t("chat.noModels"),
                    },
                  ]
            }
            disabled={disabled}
            onValueChange={(next) => {
              if (next !== null) model.onChange(next);
            }}
          >
            <SelectTrigger id={id + "-model"} aria-label={t("chat.model")} className={selectClass}>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {!model.options.length && (
                <SelectItem value="">
                  {loading ? t("chat.modelsLoading") : t("chat.noModels")}
                </SelectItem>
              )}
              {model.options.map((option) => (
                <SelectItem key={option.value} value={option.value}>
                  {option.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <label className="flex min-w-0 flex-col gap-1 text-xs text-muted-foreground">
          {t("chat.executor")}
          <Select
            value={executor.value}
            items={
              executor.options.length
                ? executor.options
                : [{ value: "", label: t("chat.noModels") }]
            }
            disabled={disabled || !executor.options.length}
            onValueChange={(next) => {
              if (next !== null) executor.onChange(next);
            }}
          >
            <SelectTrigger aria-label={t("chat.executor")} className={selectClass}>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {!executor.options.length && <SelectItem value="">{t("chat.noModels")}</SelectItem>}
              {executor.options.map((option) => (
                <SelectItem key={option.value} value={option.value}>
                  {option.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </label>
        <label className="flex min-w-0 flex-col gap-1 text-xs text-muted-foreground">
          {t("chat.effort")}
          <Select
            value={effort.value}
            items={[
              {
                value: "",
                label: `${t("chat.profileDefault")}${defaultEffort ? ` (${defaultEffort})` : ""}`,
              },
              ...effort.options,
            ]}
            disabled={disabled || !effort.options.length}
            onValueChange={(next) => {
              if (next !== null) effort.onChange(next);
            }}
          >
            <SelectTrigger aria-label={t("chat.effort")} className={selectClass}>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="">
                {t("chat.profileDefault")}
                {defaultEffort ? ` (${defaultEffort})` : ""}
              </SelectItem>
              {effort.options.map((option) => (
                <SelectItem key={option.value} value={option.value}>
                  {option.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </label>
      </fieldset>
      {error && !errorInHeader && (
        <DismissibleNotice message={error} className="mt-2 break-words text-xs text-destructive">
          {error}
        </DismissibleNotice>
      )}
    </div>
  );
}
