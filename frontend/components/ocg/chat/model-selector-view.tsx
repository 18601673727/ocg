"use client";

import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Popover } from "@base-ui/react/popover";
import { ChevronDown, Cpu, Folder } from "lucide-react";
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
  defaultEffort?: string | null;
  status: string;
  disabled: boolean;
  loading: boolean;
  error?: string;
  project: SelectorField & { disabled: boolean; placeholder: string };
  provider: SelectorField;
  model: SelectorField;
  executor: SelectorField;
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
  executor,
  effort,
  providerLabel,
  defaultEffort,
}: ModelSelectorViewProps) {
  const { t } = useI18n();
  const id = useId();
  const errorInHeader = useConversationError(error);
  const providerName = provider.options.find((option) => option.value === provider.value)?.label;
  const modelName = model.options.find((option) => option.value === model.value)?.label;
  const effortName =
    effort.value || `${t("chat.profileDefault")}${defaultEffort ? ` (${defaultEffort})` : ""}`;
  const selectionLabel =
    providerName && modelName
      ? `${providerName} / ${modelName} / ${effortName}`
      : t(loading ? "chat.modelsLoading" : "chat.noModels");
  const compactClass =
    "h-11 min-w-0 rounded-lg border border-border/70 bg-card px-2.5 text-sm text-foreground transition-colors hover:border-ring/50 hover:bg-muted outline-none focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/20 disabled:cursor-not-allowed disabled:opacity-60 pointer-fine:h-9";
  const selectClass =
    "h-11 w-full min-w-0 rounded-md border border-border bg-background px-2 text-base text-foreground outline-none focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/20 disabled:cursor-not-allowed disabled:opacity-60 sm:h-9 sm:text-sm";

  return (
    <div className="w-full min-w-0">
      <p id={id + "-status"} className="sr-only">
        {status}
      </p>
      <fieldset
        aria-describedby={id + "-status"}
        aria-busy={loading}
        className="flex min-w-0 items-center gap-1.5"
      >
        <legend className="sr-only">{t("chat.executionSettings")}</legend>
        <div className="max-w-[45%] min-w-0 shrink">
          <Select
            value={project.value}
            items={[{ value: "", label: project.placeholder }, ...project.options]}
            disabled={project.disabled}
            onValueChange={(next) => {
              if (next !== null) project.onChange(next);
            }}
          >
            <SelectTrigger
              aria-label={t("chat.project")}
              className={`${compactClass} max-w-full data-[size=default]:h-11 pointer-fine:data-[size=default]:h-9`}
            >
              <Folder className="size-3.5 text-muted-foreground" aria-hidden="true" />
              <SelectValue className="min-w-0 truncate" />
            </SelectTrigger>
            <SelectContent side="top" align="start">
              <SelectItem value="">{project.placeholder}</SelectItem>
              {project.options.map((option) => (
                <SelectItem key={option.value} value={option.value}>
                  {option.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="min-w-0 flex-1">
          <Popover.Root>
            <Popover.Trigger
              disabled={disabled}
              aria-label={`${t("chat.executionSettings")} · ${selectionLabel}`}
              title={selectionLabel}
              className={`${compactClass} flex max-w-full items-center justify-between gap-2 text-left`}
            >
              <Cpu className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
              <span className="truncate">
                {providerName && modelName ? (
                  <>
                    <span className="text-muted-foreground">{providerName} / </span>
                    <span className="font-medium">{modelName}</span>
                    <span className="text-muted-foreground"> / {effortName}</span>
                  </>
                ) : (
                  selectionLabel
                )}
              </span>
              <ChevronDown className="size-4 shrink-0 opacity-50" aria-hidden="true" />
            </Popover.Trigger>
            <Popover.Portal>
              <Popover.Positioner side="top" sideOffset={4} align="start" className="z-50">
                <Popover.Popup className="grid max-h-(--available-height) w-72 max-w-[calc(100vw-2rem)] gap-3 overflow-y-auto rounded-xl border border-border bg-popover p-4 text-popover-foreground shadow-lg outline-none">
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
                      <SelectTrigger
                        aria-label={providerLabel ?? t("chat.provider")}
                        className={selectClass}
                      >
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent alignItemWithTrigger={false}>
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
                      <SelectTrigger
                        id={id + "-model"}
                        aria-label={t("chat.model")}
                        className={selectClass}
                      >
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent alignItemWithTrigger={false}>
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
                  {executor.options.length > 1 && (
                    <label className="flex min-w-0 flex-col gap-1 text-xs text-muted-foreground">
                      {t("chat.executor")}
                      <Select
                        value={executor.value}
                        items={executor.options}
                        disabled={disabled}
                        onValueChange={(next) => {
                          if (next !== null) executor.onChange(next);
                        }}
                      >
                        <SelectTrigger aria-label={t("chat.executor")} className={selectClass}>
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent alignItemWithTrigger={false}>
                          {executor.options.map((option) => (
                            <SelectItem key={option.value} value={option.value}>
                              {option.label}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </label>
                  )}
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
                      <SelectContent alignItemWithTrigger={false}>
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
                </Popover.Popup>
              </Popover.Positioner>
            </Popover.Portal>
          </Popover.Root>
        </div>
      </fieldset>
      {error && !errorInHeader && (
        <DismissibleNotice message={error} className="mt-2 break-words text-xs text-destructive">
          {error}
        </DismissibleNotice>
      )}
    </div>
  );
}
