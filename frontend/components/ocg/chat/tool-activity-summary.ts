import type { TranslateFn } from "../i18n";

export type ToolActivity = {
  name: string;
  arguments?: string | null;
  result?: unknown;
  durationMs?: number;
  truncated?: boolean;
};

export function jobPlanTitles(args: unknown): string {
  if (!args || typeof args !== "object" || Array.isArray(args)) return "";
  const steps: unknown = Reflect.get(args, "steps");
  if (!Array.isArray(steps)) return "";
  return steps
    .map((step: unknown) => {
      if (!step || typeof step !== "object" || Array.isArray(step)) return "";
      const title: unknown = Reflect.get(step, "title");
      return typeof title === "string" ? title.replace(/\s+/g, " ").trim() : "";
    })
    .filter(Boolean)
    .join(" · ");
}

export function formatToolJson(text: string): string {
  try {
    const value: unknown = JSON.parse(text);
    if (value === null || typeof value !== "object") return text;
  } catch {
    return text;
  }

  let formatted = "";
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let i = 0; i < text.length; i++) {
    const char = text[i];
    if (inString) {
      formatted += char;
      if (escaped) escaped = false;
      else if (char === "\\") escaped = true;
      else if (char === '"') inString = false;
    } else if (char === '"') {
      inString = true;
      formatted += char;
    } else if (char === "{" || char === "[") {
      depth++;
      formatted += char;
      let next = i + 1;
      while (next < text.length && /\s/.test(text[next])) next++;
      if (text[next] !== "}" && text[next] !== "]") {
        formatted += `\n${"  ".repeat(depth)}`;
      }
    } else if (char === "}" || char === "]") {
      depth--;
      if (!formatted.endsWith("{") && !formatted.endsWith("[")) {
        formatted += `\n${"  ".repeat(depth)}`;
      }
      formatted += char;
    } else if (char === ",") {
      formatted += `,\n${"  ".repeat(depth)}`;
    } else if (char === ":") {
      formatted += ": ";
    } else if (!/\s/.test(char)) {
      formatted += char;
    }
  }
  return formatted;
}

const categories = ["read", "search", "code", "edit", "command", "agent", "other"] as const;
type Category = (typeof categories)[number];

export function toolCategory(name: string): Category {
  switch (name) {
    case "workspace_read_file":
    case "workspace_list_directory":
      return "read";
    case "workspace_search":
    case "workspace_fts_search":
      return "search";
    case "workspace_code":
      return "code";
    case "workspace_write_file":
    case "workspace_update_file":
    case "workspace_create_file":
      return "edit";
    case "workspace_run_command":
      return "command";
    case "ask_user":
      return "agent";
    default:
      return name.startsWith("job_") ? "agent" : "other";
  }
}

export function toolActivitySummary(tools: readonly ToolActivity[]) {
  const counts = new Map<Category, number>();
  let bytes = 0;
  let durationMs = 0;
  let timed = 0;
  const encoder = new TextEncoder();
  for (const tool of tools) {
    const category = toolCategory(tool.name);
    counts.set(category, (counts.get(category) ?? 0) + 1);
    const result =
      typeof tool.result === "string"
        ? tool.result
        : tool.result === undefined
          ? ""
          : JSON.stringify(tool.result);
    bytes += encoder.encode(tool.arguments ?? "").length + encoder.encode(result).length;
    if (
      typeof tool.durationMs === "number" &&
      Number.isFinite(tool.durationMs) &&
      tool.durationMs >= 0
    ) {
      durationMs += tool.durationMs;
      timed++;
    }
  }
  return {
    categories: categories.flatMap((category) =>
      counts.has(category) ? [{ category, count: counts.get(category)! }] : [],
    ),
    tokens: Math.ceil(bytes / 3),
    truncated: tools.some((tool) => tool.truncated),
    durationMs: timed ? durationMs : null,
    durationPartial: timed < tools.length,
  };
}

export function toolActivityLabels(tools: readonly ToolActivity[], t: TranslateFn, locale: string) {
  const summary = toolActivitySummary(tools);
  const format = new Intl.NumberFormat(locale, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
  return {
    categories: summary.categories
      .map(({ category, count }) =>
        t(`chat.tools.${category}.${count === 1 ? "one" : "other"}`, { count }),
      )
      .join(" · "),
    tokens: t("chat.tools.tokens", {
      count: `≈${new Intl.NumberFormat(locale).format(summary.tokens)}${summary.truncated ? "+" : ""}`,
    }),
    duration:
      summary.durationMs === null
        ? t("chat.tools.durationUnknown")
        : t("chat.tools.duration", {
            seconds: `${summary.durationPartial ? "≥" : ""}${format.format(summary.durationMs / 1000)}`,
          }),
  };
}
