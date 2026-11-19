import { createMemo } from "solid-js";
import { Input } from "@/components/ui/input";
import { Select, SelectItem } from "@/components/ui/select";
import type { ProfilesSnapshot } from "../contracts";
import type {
  CompactionPolicy as CompactionPolicy,
  PredictiveCompaction as PredictiveCompaction,
} from "../contracts/api";
import { type I18nKey, useI18n } from "../i18n";
import { selectableLlmProfiles } from "./settings-parts";

const off: CompactionPolicy = {
  mode: { kind: "off" },
  summarization_model: null,
  predictive: null,
};
const selectClass = "min-h-10 w-full rounded-md border border-border bg-background px-2 text-sm";
export function CompactionPolicyFields(props: {
  value: CompactionPolicy | null;
  onChange(value: CompactionPolicy | null): void;
  profiles: ProfilesSnapshot;
  disabled?: boolean;
}) {
  const i18n = useI18n();
  const policy = createMemo(() => props.value ?? off);
  const prediction = createMemo(() => policy().predictive);
  const change = (patch: Partial<CompactionPolicy>) => props.onChange({ ...policy(), ...patch });
  const updatePrediction = (patch: Partial<PredictiveCompaction>) => {
    if (prediction()) change({ predictive: { ...prediction()!, ...patch } });
  };
  const number = (
    label: I18nKey,
    value: number,
    update: (value: number) => void,
    min = 1,
    max = 1_000_000,
  ) => (
    <label class="grid gap-1 text-xs">
      {i18n.t(label)}
      <Input
        type="number"
        min={min}
        max={max}
        step={1}
        required
        value={value === 0 && min > 0 ? "" : value}
        onInput={(event) => update(event.currentTarget.valueAsNumber || 0)}
      />
    </label>
  );
  const mode = createMemo(() => {
    const mode = policy().mode;
    return mode.kind === "auto" ? mode.preset : mode.kind;
  });
  return (
    <fieldset
      disabled={props.disabled === undefined ? false : props.disabled}
      class="space-y-3 rounded-md border border-border p-3"
    >
      <legend class="px-1 text-sm font-medium">{i18n.t("context.title")}</legend>
      <label class="grid gap-1 text-xs">
        {i18n.t("context.automatic")}
        <Select
          value={mode()}
          items={(["off", "conservative", "balanced", "aggressive", "custom"] as const).map(
            (mode) => ({ value: mode, label: i18n.t(`context.${mode}`) }),
          )}
          onValueChange={(mode) => {
            if (mode === "off")
              change({
                mode: { kind: "off" },
                predictive:
                  prediction()?.mode === "prepare"
                    ? { ...prediction()!, mode: "shadow" }
                    : prediction(),
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
                predictive: prediction() ? { ...prediction()!, mode: "prepare" } : null,
              });
          }}
          class={selectClass}
        >
          {(["off", "conservative", "balanced", "aggressive", "custom"] as const).map((mode) => (
            <SelectItem value={mode}>{i18n.t(`context.${mode}`)}</SelectItem>
          ))}
        </Select>
      </label>
      <p class="text-xs text-muted-foreground">{i18n.t("context.experimental")}</p>
      {props.value !== null && (
        <>
          {policy().mode.kind === "custom" && (
            <div class="grid grid-cols-2 gap-2">
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
                const mode = policy().mode;
                return mode.kind === "custom" ? (
                  <div>
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
          <label class="grid gap-1 text-xs">
            {i18n.t("context.summarizer")}
            <Select
              value={policy().summarization_model ?? ""}
              items={[
                { value: "", label: i18n.t("context.foregroundFallback") },
                ...selectableLlmProfiles(
                  props.profiles.llm_profiles,
                  policy().summarization_model,
                ).map((model) => ({
                  value: model.id,
                  label: model.enabled
                    ? model.name
                    : `${model.name} (${i18n.t("settings.llm.disabled")})`,
                })),
              ]}
              onValueChange={(next) => change({ summarization_model: next || null })}
              class={selectClass}
            >
              <SelectItem value="">{i18n.t("context.foregroundFallback")}</SelectItem>
              {selectableLlmProfiles(props.profiles.llm_profiles, policy().summarization_model).map(
                (model) => (
                  <SelectItem value={model.id} disabled={!model.enabled}>
                    {model.name}
                    {!model.enabled && ` (${i18n.t("settings.llm.disabled")})`}
                  </SelectItem>
                ),
              )}
            </Select>
          </label>
          <div class="space-y-3">
            <p class="text-xs text-muted-foreground">{i18n.t("context.predictionHint")}</p>
            {prediction() && (
              <details class="space-y-3">
                <summary class="cursor-pointer text-sm">{i18n.t("context.advanced")}</summary>
                <div class="grid grid-cols-2 gap-2">
                  {number("context.interval", prediction()!.minimum_interval_seconds, (n) =>
                    updatePrediction({ minimum_interval_seconds: n }),
                  )}
                  {number("context.predictionBudget", prediction()!.max_prediction_tokens, (n) =>
                    updatePrediction({ max_prediction_tokens: n }),
                  )}
                  {number("context.preparationBudget", prediction()!.max_preparation_tokens, (n) =>
                    updatePrediction({ max_preparation_tokens: n }),
                  )}
                </div>
                {prediction()!.mode === "prepare" && prediction()!.acceptance && (
                  <div class="space-y-2">
                    <div class="grid grid-cols-2 gap-2">
                      {number(
                        "context.probability",
                        prediction()!.acceptance!.minimum_probability_millionths,
                        (n) =>
                          prediction()!.acceptance &&
                          updatePrediction({
                            acceptance: {
                              ...prediction()!.acceptance!,
                              minimum_probability_millionths: n,
                            },
                          }),
                      )}
                      {number(
                        "context.confidence",
                        prediction()!.acceptance!.minimum_confidence_millionths,
                        (n) =>
                          prediction()!.acceptance &&
                          updatePrediction({
                            acceptance: {
                              ...prediction()!.acceptance!,
                              minimum_confidence_millionths: n,
                            },
                          }),
                      )}
                      {number(
                        "context.hourlyPreparations",
                        prediction()!.max_preparations_per_hour ?? 0,
                        (n) => updatePrediction({ max_preparations_per_hour: n }),
                      )}
                      {number(
                        "context.hourlyRequests",
                        prediction()!.max_requests_per_hour ?? 0,
                        (n) => updatePrediction({ max_requests_per_hour: n }),
                      )}
                      {number(
                        "context.hourlyTokens",
                        prediction()!.max_tokens_per_hour ?? 0,
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
