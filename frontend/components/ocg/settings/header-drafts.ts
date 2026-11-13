import type { GlobalOutboundHeaders, OutboundHeader, OutboundHeaderInput } from "../contracts/api";

export type HeaderDraft = {
  key: string;
  name: string;
  secret: boolean;
  value: string;
  stored: boolean;
};

let nextKey = 0;

function draftKey(): string {
  nextKey += 1;
  return `header-${nextKey}`;
}

export function blankHeaderDraft(secret = false): HeaderDraft {
  return { key: draftKey(), name: "", secret, value: "", stored: false };
}

export function headersToDrafts(headers: readonly OutboundHeader[] | undefined): HeaderDraft[] {
  return (headers ?? []).map((header) => ({
    key: draftKey(),
    name: header.name,
    secret: header.secret,
    value: header.secret ? "" : (header.value ?? ""),
    stored: header.secret && header.has_secret,
  }));
}

export type HeaderDraftError =
  | { kind: "name"; row: number }
  | { kind: "value"; row: number }
  | { kind: "duplicate"; row: number };

export function draftsToInputs(
  drafts: readonly HeaderDraft[],
): { inputs: OutboundHeaderInput[] } | { error: HeaderDraftError } {
  const seen = new Set<string>();
  const inputs: OutboundHeaderInput[] = [];
  for (const [row, draft] of drafts.entries()) {
    const name = draft.name.trim();
    if (!/^[A-Za-z0-9!#$%&'*+.^_`|~-]+$/.test(name)) return { error: { kind: "name", row } };
    if (seen.has(name.toLowerCase())) return { error: { kind: "duplicate", row } };
    seen.add(name.toLowerCase());
    if (draft.secret) {
      if (draft.value === "" && !draft.stored) return { error: { kind: "value", row } };
      inputs.push({ name, secret: true, value: draft.value === "" ? null : draft.value });
    } else {
      if (draft.value === "") return { error: { kind: "value", row } };
      inputs.push({ name, secret: false, value: draft.value });
    }
  }
  return { inputs };
}

export function globalHeaderDrafts(settings: GlobalOutboundHeaders): HeaderDraft[] {
  return headersToDrafts(settings.headers);
}
