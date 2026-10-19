import type { ControlProfileWriteRequest } from "../components/ocg/runtime/control-client";

export const profileSaveSupportsReasoning = [
  { kind: "llm", name: "reasoning", model: "mock", declared_capabilities: [],
    parameters: { reasoning_effort: "high", max_completion_tokens: 4096 } },
  { kind: "llm", name: "default", model: "mock", declared_capabilities: [], parameters: {} },
  { kind: "llm", name: "clear", model: "mock", declared_capabilities: [],
    parameters: { reasoning_effort: null, max_completion_tokens: null } },
] satisfies ControlProfileWriteRequest[];
