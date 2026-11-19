import { For } from "solid-js";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ariaState } from "@/lib/ui-state";
import { useI18n } from "../i18n";
import { blankHeaderDraft, type HeaderDraft, type HeaderDraftError } from "./header-drafts";

export function HeadersEditor(props: {
  value: HeaderDraft[];
  onChange: (value: HeaderDraft[]) => void;
  disabled?: boolean;
  allowSecret: boolean;
  error?: HeaderDraftError;
  legend: string;
  hint?: string;
}) {
  const i18n = useI18n();
  const update = (index: number, patch: Partial<HeaderDraft>) =>
    props.onChange(
      props.value.map((draft, row) => (row === index ? { ...draft, ...patch } : draft)),
    );
  return (
    <fieldset class="flex flex-col gap-2" disabled={props.disabled}>
      <legend class="text-xs font-medium text-muted-foreground">{props.legend}</legend>
      {props.hint && <p class="text-xs text-muted-foreground">{props.hint}</p>}
      <For each={props.value} keyed={false}>
        {(draft, index) => {
          const bad = () => props.error?.row === index;
          return (
            <div class="flex flex-col gap-1">
              <div class="flex flex-wrap items-center gap-2">
                <Input
                  aria-label={i18n.t("headers.name")}
                  aria-invalid={ariaState(bad() && props.error?.kind !== "value")}
                  class="w-48"
                  value={draft().name}
                  onInput={(event) => update(index, { name: event.target.value })}
                  placeholder={i18n.t("headers.name")}
                  spellcheck="false"
                  autocomplete="off"
                />
                <Input
                  aria-label={i18n.t("headers.value")}
                  aria-invalid={ariaState(bad() && props.error?.kind === "value")}
                  class="min-w-48 flex-1"
                  type={draft().secret ? "password" : "text"}
                  value={draft().value}
                  onInput={(event) => update(index, { value: event.target.value })}
                  placeholder={
                    draft().secret && draft().stored
                      ? i18n.t("headers.secretKeep")
                      : i18n.t("headers.value")
                  }
                  spellcheck="false"
                  autocomplete={draft().secret ? "new-password" : "off"}
                />
                {props.allowSecret && (
                  <label class="flex items-center gap-1.5 text-xs">
                    <input
                      type="checkbox"
                      class="size-4 accent-primary"
                      checked={draft().secret}
                      onChange={(event) =>
                        update(index, { secret: event.target.checked, value: "", stored: false })
                      }
                    />
                    {i18n.t("headers.secret")}
                  </label>
                )}
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  onClick={() => props.onChange(props.value.filter((_, row) => row !== index))}
                >
                  {i18n.t("headers.remove")}
                </Button>
              </div>
              {bad() && (
                <p role="alert" class="text-xs text-destructive">
                  {i18n.t(
                    props.error!.kind === "name"
                      ? "headers.invalidName"
                      : props.error!.kind === "duplicate"
                        ? "headers.duplicate"
                        : props.allowSecret
                          ? "headers.invalidValue"
                          : "headers.invalidValuePlain",
                  )}
                </p>
              )}
            </div>
          );
        }}
      </For>
      <div>
        <Button
          type="button"
          variant="outline"
          size="sm"
          onClick={() => props.onChange([...props.value, blankHeaderDraft()])}
        >
          {i18n.t("headers.add")}
        </Button>
      </div>
    </fieldset>
  );
}
