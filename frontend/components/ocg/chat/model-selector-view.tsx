import { ChevronDown, Cpu } from "@lucide/solid";
import { createMemo, createUniqueId } from "solid-js";
import { Select, SelectItem } from "@/components/ui/select";
import { ariaState, assignRef, type Ref } from "@/lib/ui-state";
import { useI18n } from "../i18n";

type SelectorField = {
  value: string;
  options: readonly { value: string; label: string }[];
  onChange: (value: string) => void;
};

export type ModelSelectorViewProps = {
  providerLabel?: string;
  triggerRef?: Ref<HTMLButtonElement>;
  defaultEffort?: string | null;
  status: string;
  disabled: boolean;
  loading: boolean;
  project: SelectorField & { disabled: boolean; placeholder: string };
  provider: SelectorField;
  model: SelectorField;
  effort: SelectorField;
};

export function ModelSelectorView(props: ModelSelectorViewProps) {
  const i18n = useI18n();
  const id = createUniqueId();
  let trigger: HTMLButtonElement | undefined;
  const providerName = createMemo(
    () => props.provider.options.find((option) => option.value === props.provider.value)?.label,
  );
  const modelName = createMemo(
    () => props.model.options.find((option) => option.value === props.model.value)?.label,
  );
  const effortName = createMemo(
    () =>
      props.effort.value ||
      `${i18n.t("chat.profileDefault")}${props.defaultEffort ? ` (${props.defaultEffort})` : ""}`,
  );
  const selectionLabel = createMemo(() =>
    providerName() && modelName()
      ? `${providerName()} / ${modelName()} / ${effortName()}`
      : i18n.t(props.loading ? "chat.modelsLoading" : "chat.noModels"),
  );
  const compactClass =
    "h-11 min-w-0 rounded-full border border-transparent bg-transparent px-2.5 text-sm text-muted-foreground transition-colors hover:bg-muted/65 hover:text-foreground outline-none focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/20 disabled:cursor-not-allowed disabled:opacity-60 pointer-fine:h-8";
  const selectClass =
    "h-11 w-full min-w-0 rounded-md border border-border bg-background px-2 text-base text-foreground outline-none focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/20 disabled:cursor-not-allowed disabled:opacity-60 sm:h-9 sm:text-sm";

  return (
    <div class="w-fit min-w-0 max-w-full">
      <p id={id + "-status"} class="sr-only">
        {props.status}
      </p>
      <fieldset
        aria-describedby={id + "-status"}
        aria-busy={ariaState(props.loading)}
        class="flex w-fit min-w-0 max-w-full items-center gap-1.5"
      >
        <legend class="sr-only">{i18n.t("chat.executionSettings")}</legend>
        <div class="min-w-0 shrink">
          <Select
            fitSelected
            value={props.project.value}
            items={[{ value: "", label: props.project.placeholder }, ...props.project.options]}
            disabled={props.project.disabled}
            onValueChange={(next) => {
              if (next !== null) props.project.onChange(next);
            }}
            aria-label={i18n.t("chat.project")}
            class={`${compactClass} max-w-full data-[size=default]:h-11 pointer-fine:data-[size=default]:h-8`}
          >
            <SelectItem value="">{props.project.placeholder}</SelectItem>
            {props.project.options.map((option) => (
              <SelectItem value={option.value}>{option.label}</SelectItem>
            ))}
          </Select>
        </div>
        <div class="min-w-0 shrink">
          <button
            type="button"
            popovertarget={id + "-settings"}
            ref={(element) => {
              trigger = element;
              assignRef(props.triggerRef, element);
            }}
            disabled={props.disabled}
            aria-label={`${i18n.t("chat.executionSettings")} · ${selectionLabel()}`}
            title={selectionLabel()}
            class={`${compactClass} flex max-w-full items-center justify-between gap-2 text-left`}
          >
            <Cpu class="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
            <span class="truncate">
              {providerName() && modelName() ? (
                <>
                  <span class="text-muted-foreground">{providerName()} / </span>
                  <span class="font-medium">{modelName()}</span>
                  <span class="text-muted-foreground"> / {effortName()}</span>
                </>
              ) : (
                selectionLabel()
              )}
            </span>
            <ChevronDown class="size-4 shrink-0 opacity-50" aria-hidden="true" />
          </button>

          <div
            id={id + "-settings"}
            popover="auto"
            onBeforeToggle={(event) => {
              if (event.newState !== "open" || !trigger) return;
              const rect = trigger.getBoundingClientRect();
              event.currentTarget.style.left = `${Math.max(8, Math.min(rect.left, window.innerWidth - 296))}px`;
              event.currentTarget.style.bottom = `${window.innerHeight - rect.top + 4}px`;
            }}
            class="fixed m-0 top-auto z-50 open:grid max-h-(--available-height) w-72 max-w-[calc(100vw-2rem)] gap-3 overflow-y-auto rounded-xl border border-border bg-popover p-4 text-popover-foreground shadow-lg outline-none"
          >
            <label class="flex min-w-0 flex-col gap-1 text-xs text-muted-foreground">
              {props.providerLabel ?? i18n.t("chat.provider")}
              <Select
                value={props.provider.value}
                items={
                  props.provider.options.length
                    ? props.provider.options
                    : [
                        {
                          value: "",
                          label: props.loading
                            ? i18n.t("chat.modelsLoading")
                            : i18n.t("chat.noModels"),
                        },
                      ]
                }
                disabled={props.disabled}
                onValueChange={(next) => {
                  if (next !== null) props.provider.onChange(next);
                }}
                aria-label={props.providerLabel ?? i18n.t("chat.provider")}
                class={selectClass}
              >
                {!props.provider.options.length && (
                  <SelectItem value="">
                    {props.loading ? i18n.t("chat.modelsLoading") : i18n.t("chat.noModels")}
                  </SelectItem>
                )}
                {props.provider.options.map((option) => (
                  <SelectItem value={option.value}>{option.label}</SelectItem>
                ))}
              </Select>
            </label>
            <div class="flex min-w-0 flex-col gap-1 text-xs text-muted-foreground">
              <label for={id + "-model"}>{i18n.t("chat.model")}</label>
              <Select
                value={props.model.value}
                items={
                  props.model.options.length
                    ? props.model.options
                    : [
                        {
                          value: "",
                          label: props.loading
                            ? i18n.t("chat.modelsLoading")
                            : i18n.t("chat.noModels"),
                        },
                      ]
                }
                disabled={props.disabled}
                onValueChange={(next) => {
                  if (next !== null) props.model.onChange(next);
                }}
                id={id + "-model"}
                aria-label={i18n.t("chat.model")}
                class={selectClass}
              >
                {!props.model.options.length && (
                  <SelectItem value="">
                    {props.loading ? i18n.t("chat.modelsLoading") : i18n.t("chat.noModels")}
                  </SelectItem>
                )}
                {props.model.options.map((option) => (
                  <SelectItem value={option.value}>{option.label}</SelectItem>
                ))}
              </Select>
            </div>
            <label class="flex min-w-0 flex-col gap-1 text-xs text-muted-foreground">
              {i18n.t("chat.effort")}
              <Select
                value={props.effort.value}
                items={[
                  {
                    value: "",
                    label: `${i18n.t("chat.profileDefault")}${props.defaultEffort ? ` (${props.defaultEffort})` : ""}`,
                  },
                  ...props.effort.options,
                ]}
                disabled={props.disabled || !props.effort.options.length}
                onValueChange={(next) => {
                  if (next !== null) props.effort.onChange(next);
                }}
                aria-label={i18n.t("chat.effort")}
                class={selectClass}
              >
                <SelectItem value="">
                  {i18n.t("chat.profileDefault")}
                  {props.defaultEffort ? ` (${props.defaultEffort})` : ""}
                </SelectItem>
                {props.effort.options.map((option) => (
                  <SelectItem value={option.value}>{option.label}</SelectItem>
                ))}
              </Select>
            </label>
          </div>
        </div>
      </fieldset>
    </div>
  );
}
