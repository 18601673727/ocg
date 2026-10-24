"use client";

import { useState } from "react";
import type { ControlCompactionPolicy, ControlProfilesSnapshot, ControlPredictiveCompaction } from "../contracts";
import { useI18n, type I18nKey } from "../i18n";
import { Input } from "@/components/ui/input";

const off: ControlCompactionPolicy = { mode: { kind: "off" }, summarization_model: null, predictive: null };
const selectClass = "min-h-10 w-full rounded-md border border-border bg-background px-2 text-sm";
export function CompactionPolicyFields({ value, onChange, profiles, disabled = false }: {
  value: ControlCompactionPolicy | null; onChange(value: ControlCompactionPolicy | null): void; profiles: ControlProfilesSnapshot; disabled?: boolean;
}) {
  const { t } = useI18n();
  const [assigned, setAssigned] = useState(value?.predictive?.decision_agent ?? "");
  const policy = value ?? off;
  const prediction = policy.predictive;
  const candidates = profiles.decision_agent_profiles.filter(agent => {
    const model = profiles.decision_profiles.find(model => model.id === (agent.default_model ?? agent.models[0]));
    return agent.workspace_access === "none" && model?.declared_capabilities.includes("choice");
  });
  const selected = candidates.find(agent => agent.id === assigned);
  const model = profiles.decision_profiles.find(model => model.id === (selected?.default_model ?? selected?.models[0]));
  const change = (patch: Partial<ControlCompactionPolicy>) => onChange({ ...policy, ...patch });
  const updatePrediction = (patch: Partial<ControlPredictiveCompaction>) => {
    if (prediction) change({ predictive: { ...prediction, ...patch } });
  };
  const number = (label: I18nKey, value: number, update: (value: number) => void, min = 1, max = 1_000_000) => <label className="grid gap-1 text-xs">
    {t(label)}<Input type="number" min={min} max={max} step={1} required value={value === 0 && min > 0 ? "" : value} onChange={event => update(event.currentTarget.valueAsNumber || 0)} />
  </label>;
  const mode = value === null ? "inherit" : policy.mode.kind === "auto" ? policy.mode.preset : policy.mode.kind;
  return <fieldset disabled={disabled} className="space-y-3 rounded-md border border-border p-3">
    <legend className="px-1 text-sm font-medium">{t("context.title")}</legend>
    <label className="grid gap-1 text-xs">{t("context.automatic")}
      <select className={selectClass} value={mode} onChange={event => {
        const mode = event.target.value;
        if (mode === "inherit") onChange(null);
        else if (mode === "off") change({ mode: {kind: "off"}, predictive: prediction?.mode === "prepare" ? { ...prediction, mode: "shadow" } : prediction });
        else if (mode === "custom") change({ mode: {kind: "custom", policy: {trigger_tokens: 0, target_tokens: 0, reserved_output_tokens: 0, minimum_source_tokens: 0, minimum_savings_tokens: 0, preserve_recent_turns: 2, cooldown_seconds: 300}} });
        else if (mode === "conservative" || mode === "balanced" || mode === "aggressive") change({ mode: {kind: "auto", preset: mode} });
      }}>
        {(["inherit", "off", "conservative", "balanced", "aggressive", "custom"] as const).map(mode => <option key={mode} value={mode}>{t(`context.${mode}`)}</option>)}
      </select>
    </label>
    <p className="text-xs text-muted-foreground">{t("context.experimental")}</p>
    {value !== null && <>
      {policy.mode.kind === "custom" && <div className="grid grid-cols-2 gap-2">
        {([ ["trigger_tokens","context.trigger"], ["target_tokens","context.target"], ["reserved_output_tokens","context.reserve"], ["minimum_source_tokens","context.minimumSource"], ["minimum_savings_tokens","context.minimumSavings"], ["preserve_recent_turns","context.recent"], ["cooldown_seconds","context.cooldown"] ] as const).map(([field, label]) => {
          const mode = policy.mode;
          return mode.kind === "custom" ? <div key={field}>{number(label, mode.policy[field], n => change({mode: {...mode, policy: {...mode.policy, [field]: n}}}), field === "preserve_recent_turns" || field === "cooldown_seconds" ? 0 : 1)}</div> : null;
        })}
      </div>}
      <label className="grid gap-1 text-xs">{t("context.summarizer")}
        <select className={selectClass} value={policy.summarization_model ?? ""} onChange={event => change({summarization_model: event.target.value || null})}>
          <option value="">{t("context.foregroundFallback")}</option>
          {profiles.llm_profiles.map(model => <option key={model.id} value={model.id}>{model.name}</option>)}
        </select>
      </label>
      <details className="space-y-3"><summary className="cursor-pointer text-sm">{t("context.predictive")}</summary>
        <p className="text-xs text-muted-foreground">{t("context.predictionHint")}</p>
        <label className="grid gap-1 text-xs">{t("context.predictor")}
          <select className={selectClass} value={assigned} onChange={event => {setAssigned(event.target.value); change({predictive: null});}}>
            <option value="">{t("context.selectPredictor")}</option>
            {candidates.map(agent => <option key={agent.id} value={agent.id}>{agent.name}</option>)}
          </select>
        </label>
        <label className="flex items-center gap-2 text-sm"><input type="checkbox" disabled={!selected && !prediction} checked={!!prediction} onChange={event => change({predictive: event.target.checked && selected ? {decision_agent: selected.id, mode: "shadow", acceptance: null, minimum_interval_seconds: 60, max_preparations: 1, max_prediction_tokens: 1024, max_preparation_tokens: 16384, max_preparations_per_hour: 0, max_requests_per_hour: 0, max_tokens_per_hour: 0} : null})} />{t("context.enablePrediction")}</label>
        {!selected && <p className="text-xs text-muted-foreground">{t("context.assignmentRequired")}</p>}
        {prediction && <>
          <div className="grid grid-cols-2 gap-2">
            {number("context.interval", prediction.minimum_interval_seconds, n => updatePrediction({minimum_interval_seconds:n}))}
            {number("context.predictionBudget", prediction.max_prediction_tokens, n => updatePrediction({max_prediction_tokens:n}))}
            {number("context.preparationBudget", prediction.max_preparation_tokens, n => updatePrediction({max_preparation_tokens:n}))}
          </div>
          <label className="flex items-center gap-2 text-sm"><input type="checkbox" disabled={prediction.mode !== "prepare" && (policy.mode.kind === "off" || !model)} checked={prediction.mode === "prepare"} onChange={event => updatePrediction({mode: event.target.checked ? "prepare" : "shadow", acceptance: event.target.checked && model ? {model:model.id, model_edit_seq:model.edit_seq, reported_model:"", confidence_semantics:"", minimum_probability_millionths:0, minimum_confidence_millionths:0} : null})} />{t("context.prepareOptIn")}</label>
          <p className="text-xs text-muted-foreground">{t("context.prepareHint")}</p>
          {prediction.mode === "prepare" && prediction.acceptance && <div className="space-y-2">
            <p className="text-xs text-muted-foreground">{t("context.pin", {model:model?.model ?? prediction.acceptance.model, version:prediction.acceptance.model_edit_seq})}</p>
            {([ ["reported_model","context.reportedModel"], ["confidence_semantics","context.semantics"] ] as const).map(([field,label]) => <label key={field} className="grid gap-1 text-xs">{t(label)}<Input required value={prediction.acceptance?.[field] ?? ""} onChange={event => prediction.acceptance && updatePrediction({acceptance:{...prediction.acceptance,[field]:event.target.value}})} /></label>)}
            <div className="grid grid-cols-2 gap-2">
              {number("context.probability", prediction.acceptance.minimum_probability_millionths, n => prediction.acceptance && updatePrediction({acceptance:{...prediction.acceptance,minimum_probability_millionths:n}}))}
              {number("context.confidence", prediction.acceptance.minimum_confidence_millionths, n => prediction.acceptance && updatePrediction({acceptance:{...prediction.acceptance,minimum_confidence_millionths:n}}))}
              {number("context.hourlyPreparations", prediction.max_preparations_per_hour ?? 0, n => updatePrediction({max_preparations_per_hour:n}))}
              {number("context.hourlyRequests", prediction.max_requests_per_hour ?? 0, n => updatePrediction({max_requests_per_hour:n}))}
              {number("context.hourlyTokens", prediction.max_tokens_per_hour ?? 0, n => updatePrediction({max_tokens_per_hour:n}),1,4_000_000_000)}
            </div>
          </div>}
        </>}
      </details>
    </>}
  </fieldset>;
}
