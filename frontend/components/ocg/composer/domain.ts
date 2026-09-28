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

export type ComposerMissionCreateIntent = {
  kind: "mission.create";
  /** Optional trailing objective typed after `/mission create`. */
  seed?: string;
  raw: string;
  source: Exclude<ComposerIntentSource, "plain-text">;
};

export type ComposerChatIntent = {
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
  | ComposerMissionCreateIntent
  | ComposerUnknownCommandIntent;

/**
 * Command handlers are kept outside the composer presentation. Adding a
 * structured command therefore extends this registry boundary instead of
 * teaching ChatView about the command's business logic.
 */
export type ComposerIntentHandlers = {
  chat: (intent: ComposerChatIntent) => void;
  "mission.create": (intent: ComposerMissionCreateIntent) => void;
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
    case "mission.create":
      handlers["mission.create"](intent);
      return;
    case "unknown-command":
      handlers["unknown-command"]?.(intent);
      return;
  }
}

/**
 * Converts raw composer text into a typed intent. `null` means "this resolver
 * does not recognize the input", so the deterministic parser remains the
 * fallback. No natural-language resolver is shipped yet.
 */
export type ComposerIntentResolver = (input: string) => ComposerIntent | null;

const MISSION_CREATE_PATTERN = /^\/mission\s+create(?:\s+([\s\S]*))?$/i;
const MISSION_ONLY_PATTERN = /^\/mission\s*$/i;

/** Deterministic parse. Normal text stays chat; malformed slash commands stay explicit. */
export function parseComposerIntent(raw: string): ComposerIntent {
  const text = raw ?? "";
  const trimmed = text.trim();

  if (!trimmed.startsWith("/")) {
    return { kind: "chat", text: trimmed, raw: text, source: "plain-text" };
  }

  const missionCreate = MISSION_CREATE_PATTERN.exec(trimmed);
  if (missionCreate) {
    const seed = missionCreate[1]?.trim();
    return {
      kind: "mission.create",
      ...(seed ? { seed } : {}),
      raw: text,
      source: "slash-command",
    };
  }

  if (MISSION_ONLY_PATTERN.test(trimmed)) {
    return {
      kind: "unknown-command",
      command: "/mission",
      raw: text,
      reason: "Incomplete command. Use /mission create to open a Mission draft.",
      source: "slash-command",
    };
  }

  const command = trimmed.split(/\s+/)[0] ?? trimmed;
  return {
    kind: "unknown-command",
    command,
    raw: text,
    reason: `Unknown command ${command}. Try /mission create.`,
    source: "slash-command",
  };
}

/** Runs an optional future resolver first, then falls back to deterministic parsing. */
export function resolveComposerIntent(
  raw: string,
  resolver?: ComposerIntentResolver,
): ComposerIntent {
  return resolver?.(raw) ?? parseComposerIntent(raw);
}

/* -------------------------------------------------------------------------- */
/* Suggestions                                                                */
/* -------------------------------------------------------------------------- */

export type ComposerSuggestionKind = "mission" | "mission.create";

export type ComposerSuggestionAction = "insert" | "create-mission";

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
    id: "composer-mission",
    kind: "mission",
    label: "Mission",
    command: "/mission ",
    description: "Keep typing a Mission command. Use /mission create to open a draft.",
    keywords: ["mission"],
    action: "insert",
  },
  {
    id: "composer-create-mission",
    kind: "mission.create",
    label: "Create Mission",
    command: "/mission create",
    description: "Open an inline Mission draft for the active Project.",
    keywords: ["mission", "create", "draft", "new"],
    action: "create-mission",
  },
  {
    id: "composer-mission-create-command",
    kind: "mission.create",
    label: "/mission create",
    command: "/mission create",
    description: "Explicit command form; accepts an optional seed objective.",
    keywords: ["mission", "create", "seed"],
    action: "create-mission",
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
 * command, or keywords, so "/" and "/mission" both return the full list while
 * a narrower query such as "/mission create" drops the generic Mission entry.
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
