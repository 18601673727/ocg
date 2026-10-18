/** Known OpenAI-compatible services; selecting one only prefills the connect form. */
export const PROVIDER_PRESETS: readonly { id: string; label: string; endpoint: string }[] = [
  { id: "opencode-go", label: "OpenCode Go", endpoint: "https://opencode.ai/zen/go/v1" },
  { id: "command-code", label: "Command Code Plan", endpoint: "https://api.commandcode.ai/provider/v1" },
];
