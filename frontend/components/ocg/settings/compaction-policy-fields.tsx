"use client";

import type { ControlProfilesSnapshot } from "../contracts";
import type {
  CompactionPolicy as ControlCompactionPolicy,
  PredictiveCompaction as ControlPredictiveCompaction,
} from "../contracts/api";
import { useI18n, type I18nKey } from "../i18n";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

const off: ControlCompactionPolicy = {
  mode: { kind: "off" },
  summarization_model: null,
  predictive: null,
};
const selectClass = "min-h-10 w-full rounded-md border border-border bg-background px-2 text-sm";
export function CompactionPolicyFields({
  value,
  onChange,
  profiles,
  disabled = false,
}: {
  value: ControlCompactionPolicy | null;
  onChange(value: ControlCompactionPolicy | null): void;
  profiles: ControlProfilesSnapshot;
  disabled?: boolean;
}) {
  const { t } = useI18n();
  const policy = value ?? off;
  const prediction = policy.predictive;
  const change = (patch: Partial<ControlCompactionPolicy>) => onChange({ ...policy, ...patch });
  const updatePrediction = (patch: Partial<ControlPredictiveCompaction>) => {
    if (prediction) change({ predictive: { ...prediction, ...patch } });
  };
  const number = (
    label: I18nKey,
    value: number,
    update: (value: number) => void,
    min = 1,
    max = 1_000_000,
  ) => (
    <label className="grid gap-1 text-xs">
      {t(label)}
      <Input
        type="number"
        min={min}
        max={max}
        step={1}
        required
        value={value === 0 && min > 0 ? "" : value}
        onChange={(event) => update(event.currentTarget.valueAsNumber || 0)}
      />
    </label>
  );
  const mode = policy.mode.kind === "auto" ? policy.mode.preset : policy.mode.kind;
  return (
    <fieldset disabled={disabled} className="space-y-3 rounded-md border border-border p-3">
      <legend className="px-1 text-sm font-medium">{t("context.title")}</legend>
      <label className="grid gap-1 text-xs">
        {t("context.automatic")}
        <Select
          value={mode}
          items={(["off", "conservative", "balanced", "aggressive", "custom"] as const).map(
            (mode) => ({ value: mode, label: t(`context.${mode}`) }),
          )}
          onValueChange={(mode) => {
            if (mode === "off")
              change({
                mode: { kind: "off" },
                predictive:
                  prediction?.mode === "prepare" ? { ...prediction, mode: "shadow" } : prediction,
              });
            else if (mode === "custom")
              change({
                mode: {
                  kind: "custom",
                  policy: {
                    trigger_tokens: 0,
                    target_tokens: 0,
                    reserved_output_tokens: 0,
                    minimum_source_tokens: 0,
                    minimum_savings_tokens: 0,
                    preserve_recent_turns: 2,
                    cooldown_seconds: 300,
                  },
                },
              });
            else if (mode === "conservative" || mode === "balanced" || mode === "aggressive")
              change({
                mode: { kind: "auto", preset: mode },
                predictive: prediction ? { ...prediction, mode: "prepare" } : null,
              });
          }}
        >
          <SelectTrigger className={selectClass}>
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {(["off", "conservative", "balanced", "aggressive", "custom"] as const).map((mode) => (
              <SelectItem key={mode} value={mode}>
                {t(`context.${mode}`)}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </label>
      <p className="text-xs text-muted-foreground">{t("context.experimental")}</p>
      {value !== null && (
        <>
          {policy.mode.kind === "custom" && (
            <div className="grid grid-cols-2 gap-2">
              {(
                [
                  ["trigger_tokens", "context.trigger"],
                  ["target_tokens", "context.target"],
                  ["reserved_output_tokens", "context.reserve"],
                  ["minimum_source_tokens", "context.minimumSource"],
                  ["minimum_savings_tokens", "context.minimumSavings"],
                  ["preserve_recent_turns", "context.recent"],
                  ["cooldown_seconds", "context.cooldown"],
                ] as const
              ).map(([field, label]) => {
                const mode = policy.mode;
                return mode.kind === "custom" ? (
                  <div key={field}>
                    {number(
                      label,
                      mode.policy[field],
                      (n) => change({ mode: { ...mode, policy: { ...mode.policy, [field]: n } } }),
                      field === "preserve_recent_turns" || field === "cooldown_seconds" ? 0 : 1,
                    )}
                  </div>
                ) : null;
              })}
            </div>
          )}
          <label className="grid gap-1 text-xs">
            {t("context.summarizer")}
            <Select
              value={policy.summarization_model ?? ""}
              items={[
                { value: "", label: t("context.foregroundFallback") },
                ...profiles.llm_profiles
                  .filter((model) => model.enabled)
                  .map((model) => ({
                    value: model.id,
                    label: model.name,
                  })),
              ]}
              onValueChange={(next) => change({ summarization_model: next || null })}
            >
              <SelectTrigger className={selectClass}>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="">{t("context.foregroundFallback")}</SelectItem>
                {profiles.llm_profiles
                  .filter((model) => model.enabled)
                  .map((model) => (
                    <SelectItem key={model.id} value={model.id}>
                      {model.name}
                    </SelectItem>
                  ))}
              </SelectContent>
            </Select>
          </label>
          <div className="space-y-3">
            <p className="text-xs text-muted-foreground">{t("context.predictionHint")}</p>
            {prediction && (
              <details className="space-y-3">
                <summary className="cursor-pointer text-sm">{t("context.advanced")}</summary>
                <div className="grid grid-cols-2 gap-2">
                  {number("context.interval", prediction.minimum_interval_seconds, (n) =>
                    updatePrediction({ minimum_interval_seconds: n }),
                  )}
                  {number("context.predictionBudget", prediction.max_prediction_tokens, (n) =>
                    updatePrediction({ max_prediction_tokens: n }),
                  )}
                  {number("context.preparationBudget", prediction.max_preparation_tokens, (n) =>
                    updatePrediction({ max_preparation_tokens: n }),
                  )}
                </div>
                {prediction.mode === "prepare" && prediction.acceptance && (
                  <div className="space-y-2">
                    <div className="grid grid-cols-2 gap-2">
                      {number(
                        "context.probability",
                        prediction.acceptance.minimum_probability_millionths,
                        (n) =>
                          prediction.acceptance &&
                          updatePrediction({
                            acceptance: {
                              ...prediction.acceptance,
                              minimum_probability_millionths: n,
                            },
                          }),
                      )}
                      {number(
                        "context.confidence",
                        prediction.acceptance.minimum_confidence_millionths,
                        (n) =>
                          prediction.acceptance &&
                          updatePrediction({
                            acceptance: {
                              ...prediction.acceptance,
                              minimum_confidence_millionths: n,
                            },
                          }),
                      )}
                      {number(
                        "context.hourlyPreparations",
                        prediction.max_preparations_per_hour ?? 0,
                        (n) => updatePrediction({ max_preparations_per_hour: n }),
                      )}
                      {number(
                        "context.hourlyRequests",
                        prediction.max_requests_per_hour ?? 0,
                        (n) => updatePrediction({ max_requests_per_hour: n }),
                      )}
                      {number(
                        "context.hourlyTokens",
                        prediction.max_tokens_per_hour ?? 0,
                        (n) => updatePrediction({ max_tokens_per_hour: n }),
                        1,
                        4_000_000_000,
                      )}
                    </div>
                  </div>
                )}
              </details>
            )}
          </div>
        </>
      )}
    </fieldset>
  );
}
