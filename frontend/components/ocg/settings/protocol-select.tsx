"use client";

import { useI18n } from "../i18n";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

export type LlmProtocol = "openai_chat_completions" | "openai_responses" | "anthropic_messages";

export function ProtocolSelect({
  value,
  onChange,
  disabled = false,
}: {
  value: LlmProtocol;
  onChange: (value: LlmProtocol) => void;
  disabled?: boolean;
}) {
  const { t } = useI18n();
  return (
    <label className="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
      {t("settings.llm.protocolLabel")}
      <Select
        value={value}
        items={[
          { value: "openai_chat_completions", label: "OpenAI Chat Completions" },
          { value: "openai_responses", label: "OpenAI Responses" },
          { value: "anthropic_messages", label: "Anthropic Messages" },
        ]}
        disabled={disabled}
        onValueChange={(next) => {
          if (
            next === "openai_chat_completions" ||
            next === "openai_responses" ||
            next === "anthropic_messages"
          )
            onChange(next);
        }}
      >
        <SelectTrigger className="h-9 w-full rounded-md border-input bg-background px-3 text-sm text-foreground">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="openai_chat_completions">OpenAI Chat Completions</SelectItem>
          <SelectItem value="openai_responses">OpenAI Responses</SelectItem>
          <SelectItem value="anthropic_messages">Anthropic Messages</SelectItem>
        </SelectContent>
      </Select>
    </label>
  );
}
