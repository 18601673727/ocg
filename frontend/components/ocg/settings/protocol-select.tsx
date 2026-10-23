"use client";

import { useI18n } from "../i18n";

export type LlmProtocol = "openai_chat_completions" | "openai_responses";

export function ProtocolSelect({ value, onChange, disabled = false }: {
  value: LlmProtocol;
  onChange: (value: LlmProtocol) => void;
  disabled?: boolean;
}) {
  const { t } = useI18n();
  return <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
    {t("settings.llm.protocolLabel")}
    <select value={value} disabled={disabled} onChange={(event) => {
      const next = event.target.value;
      if (next === "openai_chat_completions" || next === "openai_responses") onChange(next);
    }} className="h-9 rounded-md border border-input bg-background px-3 text-sm text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">
      <option value="openai_chat_completions">OpenAI Chat Completions</option>
      <option value="openai_responses">OpenAI Responses</option>
    </select>
  </label>;
}
