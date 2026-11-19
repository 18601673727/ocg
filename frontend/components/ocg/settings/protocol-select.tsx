import { Select, SelectItem } from "@/components/ui/select";
import { useI18n } from "../i18n";

export type LlmProtocol = "openai_chat_completions" | "openai_responses" | "anthropic_messages";

export function ProtocolSelect(props: {
  value: LlmProtocol;
  onChange: (value: LlmProtocol) => void;
  disabled?: boolean;
}) {
  const i18n = useI18n();
  return (
    <label class="flex flex-col gap-1 text-xs font-medium text-muted-foreground">
      {i18n.t("settings.llm.protocolLabel")}
      <Select
        value={props.value}
        items={[
          { value: "openai_chat_completions", label: "OpenAI Chat Completions" },
          { value: "openai_responses", label: "OpenAI Responses" },
          { value: "anthropic_messages", label: "Anthropic Messages" },
        ]}
        disabled={props.disabled === undefined ? false : props.disabled}
        onValueChange={(next) => {
          if (
            next === "openai_chat_completions" ||
            next === "openai_responses" ||
            next === "anthropic_messages"
          )
            props.onChange(next);
        }}
        class="h-9 w-full rounded-md border-input bg-background px-3 text-sm text-foreground"
      >
        <SelectItem value="openai_chat_completions">OpenAI Chat Completions</SelectItem>
        <SelectItem value="openai_responses">OpenAI Responses</SelectItem>
        <SelectItem value="anthropic_messages">Anthropic Messages</SelectItem>
      </Select>
      {props.value !== "openai_chat_completions" && (
        <span>{i18n.t("settings.llm.executionUnavailable")}</span>
      )}
    </label>
  );
}
