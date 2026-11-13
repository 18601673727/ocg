"use client";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useI18n } from "../i18n";
import { type HeaderDraft, type HeaderDraftError, blankHeaderDraft } from "./header-drafts";

export function HeadersEditor({
  value,
  onChange,
  disabled,
  allowSecret,
  error,
  legend,
  hint,
}: {
  value: HeaderDraft[];
  onChange: (value: HeaderDraft[]) => void;
  disabled?: boolean;
  allowSecret: boolean;
  error?: HeaderDraftError;
  legend: string;
  hint?: string;
}) {
  const { t } = useI18n();
  const update = (index: number, patch: Partial<HeaderDraft>) =>
    onChange(value.map((draft, row) => (row === index ? { ...draft, ...patch } : draft)));
  return (
    <fieldset className="flex flex-col gap-2" disabled={disabled}>
      <legend className="text-xs font-medium text-muted-foreground">{legend}</legend>
      {hint && <p className="text-xs text-muted-foreground">{hint}</p>}
      {value.map((draft, index) => {
        const bad = error?.row === index;
        return (
          <div key={draft.key} className="flex flex-col gap-1">
            <div className="flex flex-wrap items-center gap-2">
              <Input
                aria-label={t("headers.name")}
                aria-invalid={bad && error?.kind !== "value"}
                className="w-48"
                value={draft.name}
                onChange={(event) => update(index, { name: event.target.value })}
                placeholder={t("headers.name")}
                spellCheck={false}
                autoComplete="off"
              />
              <Input
                aria-label={t("headers.value")}
                aria-invalid={bad && error?.kind === "value"}
                className="min-w-48 flex-1"
                type={draft.secret ? "password" : "text"}
                value={draft.value}
                onChange={(event) => update(index, { value: event.target.value })}
                placeholder={
                  draft.secret && draft.stored ? t("headers.secretKeep") : t("headers.value")
                }
                spellCheck={false}
                autoComplete={draft.secret ? "new-password" : "off"}
              />
              {allowSecret && (
                <label className="flex items-center gap-1.5 text-xs">
                  <input
                    type="checkbox"
                    className="size-4 accent-primary"
                    checked={draft.secret}
                    onChange={(event) =>
                      update(index, { secret: event.target.checked, value: "", stored: false })
                    }
                  />
                  {t("headers.secret")}
                </label>
              )}
              <Button
                type="button"
                variant="outline"
                size="sm"
                onClick={() => onChange(value.filter((_, row) => row !== index))}
              >
                {t("headers.remove")}
              </Button>
            </div>
            {bad && (
              <p role="alert" className="text-xs text-destructive">
                {t(
                  error.kind === "name"
                    ? "headers.invalidName"
                    : error.kind === "duplicate"
                      ? "headers.duplicate"
                      : allowSecret
                        ? "headers.invalidValue"
                        : "headers.invalidValuePlain",
                )}
              </p>
            )}
          </div>
        );
      })}
      <div>
        <Button
          type="button"
          variant="outline"
          size="sm"
          onClick={() => onChange([...value, blankHeaderDraft()])}
        >
          {t("headers.add")}
        </Button>
      </div>
    </fieldset>
  );
}
