export type ComposerIntentSource = "plain-text" | "slash-command" | "resolver";

export type ComposerJobCreateIntent = {
  kind: "job.create";
  seed?: string;
  raw: string;
  source: Exclude<ComposerIntentSource, "plain-text">;
};

export type ComposerChatIntent = {
  images?: import("../contracts").ChatImage[];
  selection?: import("../contracts").ChatModelSelection;
  mode?: "queue" | "steer";
  kind: "chat";
  text: string;
  raw: string;
  source: Exclude<ComposerIntentSource, "slash-command">;
};

export type ComposerUnknownCommandIntent = {
  kind: "unknown-command";
  command: string;
  raw: string;
  reason: string;
  source: "slash-command";
};

export type ComposerIntent =
  | ComposerChatIntent
  | ComposerJobCreateIntent
  | ComposerUnknownCommandIntent;

const JOB_CREATE_PATTERN = /^\/job\s+create(?:\s+([\s\S]*))?$/i;
const JOB_ONLY_PATTERN = /^\/job\s*$/i;

export function parseComposerIntent(raw: string): ComposerIntent {
  const text = raw ?? "";
  const trimmed = text.trim();

  if (!trimmed.startsWith("/")) {
    return { kind: "chat", text: trimmed, raw: text, source: "plain-text" };
  }

  const jobCreate = JOB_CREATE_PATTERN.exec(trimmed);
  if (jobCreate) {
    const seed = jobCreate[1]?.trim();
    return {
      kind: "job.create",
      ...(seed ? { seed } : {}),
      raw: text,
      source: "slash-command",
    };
  }

  if (JOB_ONLY_PATTERN.test(trimmed)) {
    return {
      kind: "unknown-command",
      command: "/job",
      raw: text,
      reason: "Incomplete command. Use /job create to open a Job draft.",
      source: "slash-command",
    };
  }

  const command = trimmed.split(/\s+/)[0] ?? trimmed;
  return {
    kind: "unknown-command",
    command,
    raw: text,
    reason: `Unknown command ${command}. Try /job create.`,
    source: "slash-command",
  };
}

export type ComposerSuggestionKind = "job" | "job.create";

export type ComposerSuggestionAction = "insert" | "create-job";

export type ComposerSuggestion = {
  id: string;
  kind: ComposerSuggestionKind;
  label: string;
  command: string;
  description: string;
  keywords: readonly string[];
  action: ComposerSuggestionAction;
};

export const COMPOSER_SUGGESTIONS: readonly ComposerSuggestion[] = [
  {
    id: "composer-job",
    kind: "job",
    label: "Job",
    command: "/job ",
    description: "Keep typing a Job command. Use /job create to open a draft.",
    keywords: ["job"],
    action: "insert",
  },
  {
    id: "composer-create-job",
    kind: "job.create",
    label: "Create Job",
    command: "/job create",
    description: "Open an inline Job draft for the active Project.",
    keywords: ["job", "create", "draft", "new"],
    action: "create-job",
  },
  {
    id: "composer-job-create-command",
    kind: "job.create",
    label: "/job create",
    command: "/job create",
    description: "Explicit command form; accepts an optional seed objective.",
    keywords: ["job", "create", "seed"],
    action: "create-job",
  },
];

export type ComposerSuggestionQuery = {
  active: boolean;
  query: string;
};

export function parseComposerSuggestionQuery(raw: string): ComposerSuggestionQuery {
  const text = raw ?? "";
  if (text.includes("\n")) return { active: false, query: "" };
  const leftTrimmed = text.replace(/^\s+/, "");
  if (!leftTrimmed.startsWith("/")) return { active: false, query: "" };
  return { active: true, query: leftTrimmed.slice(1).trim().toLowerCase() };
}

function suggestionMatchesToken(suggestion: ComposerSuggestion, token: string): boolean {
  const haystack = [suggestion.label, suggestion.command, ...suggestion.keywords]
    .join(" ")
    .toLowerCase();
  return haystack.includes(token);
}

export function matchComposerSuggestions(
  raw: string,
  limit = COMPOSER_SUGGESTIONS.length,
): ComposerSuggestion[] {
  const { active, query } = parseComposerSuggestionQuery(raw);
  if (!active) return [];
  const tokens = query.length > 0 ? query.split(/\s+/) : [];
  const matches = COMPOSER_SUGGESTIONS.filter((suggestion) =>
    tokens.every((token) => suggestionMatchesToken(suggestion, token)),
  );
  return matches.slice(0, Math.max(0, limit));
}

export function moveComposerSuggestionIndex(
  current: number,
  delta: number,
  length: number,
): number {
  if (length <= 0) return -1;
  if (current < 0) return delta >= 0 ? 0 : length - 1;
  return (current + delta + length) % length;
}

export function applyComposerSuggestion(suggestion: ComposerSuggestion): {
  action: ComposerSuggestionAction;
  text: string;
} {
  return { action: suggestion.action, text: suggestion.command };
}
