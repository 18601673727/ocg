/**
 * Pure composer intent domain.
 *
 * The composer converts raw operator text into a small discriminated union.
 * Nothing here touches React, the runtime, or a transport. A future resolver
 * (structured command palette or natural-language intent) can be plugged in
 * through `ComposerIntentResolver` without changing the result shape; no NLP
 * is implemented in this slice.
 */

export type ComposerIntentSource =
  | "plain-text"
  | "slash-command"
  /** Reserved for a future structured or natural-language intent resolver. */
  | "resolver";

export type ComposerJobCreateIntent = {
  kind: "job.create";
  /** Optional trailing objective typed after `/job create`. */
  seed?: string;
  raw: string;
  source: Exclude<ComposerIntentSource, "plain-text">;
};

export type ComposerChatIntent = {
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

/**
 * Command handlers are kept outside the composer presentation. Adding a
 * structured command therefore extends this registry boundary instead of
 * teaching ChatView about the command's business logic.
 */
export type ComposerIntentHandlers = {
  chat: (intent: ComposerChatIntent) => void;
  "job.create": (intent: ComposerJobCreateIntent) => void;
  "unknown-command"?: (intent: ComposerUnknownCommandIntent) => void;
};

export function dispatchComposerIntent(
  intent: ComposerIntent,
  handlers: ComposerIntentHandlers,
): void {
  switch (intent.kind) {
    case "chat":
      handlers.chat(intent);
      return;
    case "job.create":
      handlers["job.create"](intent);
      return;
    case "unknown-command":
      handlers["unknown-command"]?.(intent);
      return;
  }
}


const JOB_CREATE_PATTERN = /^\/job\s+create(?:\s+([\s\S]*))?$/i;
const JOB_ONLY_PATTERN = /^\/job\s*$/i;

/** Deterministic parse. Normal text stays chat; malformed slash commands stay explicit. */
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


/* -------------------------------------------------------------------------- */
/* Suggestions                                                                */
/* -------------------------------------------------------------------------- */

export type ComposerSuggestionKind = "job" | "job.create";

export type ComposerSuggestionAction = "insert" | "create-job";

export type ComposerSuggestion = {
  /** Stable identity for keys and `aria-activedescendant`. */
  id: string;
  kind: ComposerSuggestionKind;
  label: string;
  /** Text inserted into the composer (or sent as the intent). */
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

/**
 * A suggestion query is active only while the composer starts with a slash
 * command on one line. Newlines (multiline drafting) always close suggestions.
 */
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

/**
 * Pure matcher. Every query token must appear in the suggestion's label,
 * command, or keywords, so "/" and "/job" both return the full list while
 * a narrower query such as "/job create" drops the generic Job entry.
 */
export function matchComposerSuggestions(raw: string, limit = COMPOSER_SUGGESTIONS.length): ComposerSuggestion[] {
  const { active, query } = parseComposerSuggestionQuery(raw);
  if (!active) return [];
  const tokens = query.length > 0 ? query.split(/\s+/) : [];
  const matches = COMPOSER_SUGGESTIONS.filter((suggestion) =>
    tokens.every((token) => suggestionMatchesToken(suggestion, token)),
  );
  return matches.slice(0, Math.max(0, limit));
}

/** Keyboard navigation helper. Returns -1 when there is nothing to select. */
export function moveComposerSuggestionIndex(
  current: number,
  delta: number,
  length: number,
): number {
  if (length <= 0) return -1;
  if (current < 0) return delta >= 0 ? 0 : length - 1;
  return (current + delta + length) % length;
}

/** Applies a suggestion without mutating it: either insert text or open a draft. */
export function applyComposerSuggestion(
  suggestion: ComposerSuggestion,
): { action: ComposerSuggestionAction; text: string } {
  return { action: suggestion.action, text: suggestion.command };
}
