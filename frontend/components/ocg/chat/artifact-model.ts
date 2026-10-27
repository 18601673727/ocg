import type { ControlToolCall } from "../contracts";

export type FileArtifact = {
  path: string;
  before?: string;
  after?: string;
  patch?: string;
  truncated?: boolean;
  excerpt: boolean;
};

function object(text: string | null): Record<string, unknown> | null {
  if (!text) return null;
  try {
    const value: unknown = JSON.parse(text);
    return value !== null && typeof value === "object" && !Array.isArray(value)
      ? Object.fromEntries(Object.entries(value))
      : null;
  } catch {
    return null;
  }
}

export function isArtifactCall(call: ControlToolCall): boolean {
  if (call.outcome_kind !== "completed") return false;
  return (
    call.name === "workspace_write_file" ||
    (call.name === "workspace_run_command" && fileArtifact(call) !== null)
  );
}

export function fileArtifact(call: ControlToolCall): FileArtifact | null {
  if (call.outcome_kind !== "completed" || call.arguments_cut || call.result_cut) return null;
  const args = object(call.arguments);
  if (call.name === "workspace_write_file" && args && typeof args.path === "string") {
    const content = typeof args.content === "string" ? args.content : undefined;
    const expected = typeof args.expected_content === "string" ? args.expected_content : undefined;
    switch (args.operation) {
      case "create":
        return content === undefined
          ? null
          : { path: args.path, before: "", after: content, excerpt: false };
      case "update":
        return content === undefined
          ? null
          : { path: args.path, before: expected, after: content, excerpt: false };
      case "replace":
        return typeof args.old === "string" && typeof args.new === "string"
          ? { path: args.path, before: args.old, after: args.new, excerpt: true }
          : null;
      case "delete":
        return expected === undefined
          ? null
          : { path: args.path, before: expected, after: "", excerpt: false };
    }
  }
  const result = object(call.result);
  const patch = typeof result?.stdout === "string" ? result.stdout : call.result;
  return patch && /^(?:diff --git |--- |@@ )/m.test(patch)
    ? {
        path: typeof args?.command === "string" ? args.command : call.name,
        patch,
        truncated: result?.truncated === true,
        excerpt: false,
      }
    : null;
}

export function artifactTitle(call: ControlToolCall): string {
  const args = object(call.arguments);
  const result = object(call.result);
  return typeof args?.path === "string"
    ? args.path
    : typeof result?.path === "string"
      ? result.path
      : typeof args?.command === "string"
        ? args.command
        : call.name;
}

export function changedLines(
  before: string,
  after: string,
): { added: number; removed: number } | null {
  const lines = (text: string) => {
    if (!text) return [];
    const parts = text.split("\n");
    if (parts.at(-1) === "") parts.pop();
    return parts;
  };
  const old = lines(before),
    next = lines(after);
  let start = 0,
    oldEnd = old.length,
    nextEnd = next.length;
  while (start < oldEnd && start < nextEnd && old[start] === next[start]) start++;
  while (oldEnd > start && nextEnd > start && old[oldEnd - 1] === next[nextEnd - 1]) {
    oldEnd--;
    nextEnd--;
  }
  const removed = oldEnd - start,
    added = nextEnd - start;
  if (!removed || !added) return { added, removed };
  if (removed * added > 1_000_000) return null;
  const common = new Uint32Array(added + 1);
  for (let i = start; i < oldEnd; i++) {
    let diagonal = 0;
    for (let j = 1; j <= added; j++) {
      const previous = common[j];
      common[j] =
        old[i] === next[start + j - 1] ? diagonal + 1 : Math.max(common[j], common[j - 1]);
      diagonal = previous;
    }
  }
  return { added: added - common[added], removed: removed - common[added] };
}

export function managedArtifactImages(text: string): { name: string; url: string }[] {
  return [
    ...text.matchAll(
      /!\[([^\]]*)\]\((\/api\/v1\/attachments\/[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\/content)\)/g,
    ),
  ].map((match) => ({ name: match[1], url: match[2] }));
}
