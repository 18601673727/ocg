"use client";

import { type ReactNode } from "react";
import { Check, Copy, FileCode2, Search } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useCopyToClipboard } from "@/hooks/use-copy-to-clipboard";
import type { ControlToolCall } from "../contracts";
import { useI18n } from "../i18n";
import { highlightCode } from "../markdown/markdown-pipeline";
import { sourceLanguage } from "../markdown/source-snapshot";
import { toolObject } from "./artifact-model";

type SearchLine = { line: number; text: string; truncated: boolean };
type SearchFile = {
  path: string;
  bytes?: number;
  matches: SearchLine[];
  more: boolean;
};

function object(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? Object.fromEntries(Object.entries(value))
    : null;
}

function count(value: unknown): number | undefined {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : undefined;
}

function searchFiles(result: Record<string, unknown>, indexed: boolean): SearchFile[] | null {
  const records = indexed ? result.results : result.files;
  if (!Array.isArray(records)) return null;
  const files = new Map<string, SearchFile>();
  for (const value of records) {
    const record = object(value);
    if (!record || typeof record.path !== "string" || !record.path) return null;
    if (indexed) {
      const line = count(record.line);
      if (!line || typeof record.excerpt !== "string") return null;
      const file = files.get(record.path) ?? {
        path: record.path,
        matches: [],
        more: false,
      };
      file.matches.push({
        line,
        text: record.excerpt,
        truncated: record.excerpt_truncated === true,
      });
      files.set(record.path, file);
    } else {
      if (record.matches !== undefined && !Array.isArray(record.matches)) return null;
      const matches: SearchLine[] = [];
      for (const item of record.matches ?? []) {
        const match = object(item);
        const line = count(match?.line);
        if (!line || typeof match?.text !== "string") return null;
        matches.push({ line, text: match.text, truncated: false });
      }
      files.set(record.path, {
        path: record.path,
        bytes: count(record.bytes),
        matches,
        more: record.more_matches === true,
      });
    }
  }
  return [...files.values()];
}

function SearchFileResult({ file }: { file: SearchFile }) {
  const { t, locale } = useI18n();
  const { isCopied, copyToClipboard } = useCopyToClipboard();
  return (
    <article data-slot="search-file" className="min-w-0 border-b border-border last:border-b-0">
      <header className="sticky top-0 z-10 flex min-w-0 items-center gap-2 border-b border-border bg-muted px-3 py-2 text-xs">
        <FileCode2 className="size-4 shrink-0 text-muted-foreground" aria-hidden="true" />
        <span className="min-w-0 flex-1 truncate font-mono" title={file.path}>
          {file.path}
        </span>
        {file.bytes !== undefined && (
          <span className="shrink-0 tabular-nums text-muted-foreground">
            {new Intl.NumberFormat(locale).format(file.bytes)} B
          </span>
        )}
        <Button
          type="button"
          variant="ghost"
          size="icon-xs"
          aria-label={t("chat.search.copyPath")}
          onClick={() => copyToClipboard(file.path)}
        >
          {isCopied ? <Check aria-hidden="true" /> : <Copy aria-hidden="true" />}
        </Button>
      </header>
      {file.matches.length > 0 && (
        <ol className="py-1">
          {file.matches.map((match, index) => {
            const highlighted = highlightCode(match.text, sourceLanguage(file.path));
            return (
              <li
                key={`${match.line}:${index}`}
                data-slot="search-match"
                className="grid min-w-0 grid-cols-[auto_minmax(0,1fr)] font-mono text-xs leading-5"
              >
                <span
                  className="min-w-12 border-r border-border bg-muted/30 px-3 py-1 text-right tabular-nums text-muted-foreground"
                  aria-label={t("chat.search.line", { line: match.line })}
                >
                  {match.line}
                </span>
                <div className="min-w-0 px-3 py-1">
                  <pre className="whitespace-pre-wrap wrap-anywhere">
                    {highlighted ? (
                      <code
                        className="hljs bg-transparent! p-0!"
                        dangerouslySetInnerHTML={{ __html: highlighted.html }}
                      />
                    ) : (
                      <code>{match.text || "\u00a0"}</code>
                    )}
                  </pre>
                  {match.truncated && (
                    <span className="font-sans text-muted-foreground">
                      {t("chat.search.excerptTruncated")}
                    </span>
                  )}
                </div>
              </li>
            );
          })}
        </ol>
      )}
      {file.more && (
        <p className="px-3 py-2 text-xs text-muted-foreground">{t("chat.search.moreMatches")}</p>
      )}
    </article>
  );
}

export function SearchTool({ call, children }: { call: ControlToolCall; children: ReactNode }) {
  const { t, locale } = useI18n();
  const { isCopied, copyToClipboard } = useCopyToClipboard();
  const args = toolObject(call.arguments);
  const result = toolObject(call.result);
  const indexed = call.name === "workspace_fts_search";
  if (!args || !result || call.outcome_kind !== "completed") return children;
  const files = searchFiles(result, indexed);
  if (!files || typeof result.complete !== "boolean") return children;
  if (
    indexed
      ? typeof args.query !== "string"
      : ![args.name, args.text].some((v) => typeof v === "string")
  )
    return children;
  const number = new Intl.NumberFormat(locale);
  const fields = indexed
    ? [
        { label: t("chat.search.query"), value: args.query },
        {
          label: t("chat.search.limit"),
          value: number.format(count(args.limit) ?? 10),
        },
      ]
    : [
        {
          label: t("chat.search.path"),
          value: args.path ?? result.path ?? ".",
        },
        { label: t("chat.search.name"), value: args.name },
        { label: t("chat.search.text"), value: args.text },
      ];
  const matches = files.reduce((sum, file) => sum + file.matches.length, 0);
  const skipped = count(result.skipped);
  const index = object(result.index);
  const generation = count(index?.generation);
  const age = count(index?.age_seconds);
  const filesIndexed = count(index?.files_indexed);
  return (
    <section
      data-slot="workspace-search"
      className="min-w-0 overflow-hidden rounded-lg border border-border bg-background font-sans"
    >
      <header className="flex min-w-0 items-center gap-2 border-b border-border bg-muted/40 px-3 py-2 text-xs">
        <Search className="size-4 shrink-0 text-muted-foreground" aria-hidden="true" />
        <h3 className="min-w-0 flex-1 font-medium">
          {t(indexed ? "chat.search.indexed" : "chat.search.files")}
        </h3>
        {args.regex === true && (
          <span className="text-muted-foreground">{t("chat.search.regex")}</span>
        )}
        {indexed && args.refresh === true && (
          <span className="text-muted-foreground">{t("chat.search.refresh")}</span>
        )}
        <Button
          type="button"
          variant="ghost"
          size="icon-xs"
          aria-label={t("chat.search.copyInput")}
          onClick={() => copyToClipboard(call.arguments ?? "")}
        >
          {isCopied ? <Check aria-hidden="true" /> : <Copy aria-hidden="true" />}
        </Button>
      </header>
      <dl data-slot="search-input" className="space-y-2 px-3 py-3 text-xs">
        {fields.map((field) =>
          typeof field.value === "string" ? (
            <div key={field.label} className="grid min-w-0 grid-cols-[5rem_minmax(0,1fr)] gap-3">
              <dt className="text-muted-foreground">{field.label}</dt>
              <dd className="min-w-0 whitespace-pre-wrap wrap-anywhere font-mono">{field.value}</dd>
            </div>
          ) : null,
        )}
      </dl>
      <div className="flex flex-wrap gap-x-4 gap-y-1 border-t border-border px-3 py-2 text-xs text-muted-foreground">
        <span>{t("chat.search.filesShown", { count: number.format(files.length) })}</span>
        {(indexed || typeof args.text === "string") && (
          <span>{t("chat.search.matchesShown", { count: number.format(matches) })}</span>
        )}
      </div>
      {files.length > 0 ? (
        <div
          data-slot="search-results"
          className="ocg-subtle-scrollbar max-h-96 overflow-auto border-t border-border outline-none focus-visible:ring-2 focus-visible:ring-ring"
          tabIndex={0}
          role="region"
          aria-label={t("chat.search.results")}
        >
          {files.map((file) => (
            <SearchFileResult key={file.path} file={file} />
          ))}
        </div>
      ) : (
        <p className="border-t border-border px-3 py-5 text-sm text-muted-foreground">
          {t(result.complete ? "chat.search.empty" : "chat.search.emptyPartial")}
        </p>
      )}
      {(!result.complete || (skipped ?? 0) > 0) && (
        <div className="space-y-1 border-t border-border px-3 py-2 text-xs text-muted-foreground">
          {!result.complete && (
            <p>
              {t("chat.search.partial")}
              {typeof result.limit === "string" ? ` (${result.limit})` : ""}
            </p>
          )}
          {skipped !== undefined && skipped > 0 && (
            <p>{t("chat.search.skipped", { count: number.format(skipped) })}</p>
          )}
        </div>
      )}
      {!indexed && matches > 0 && (
        <p className="border-t border-border px-3 py-2 text-xs text-muted-foreground">
          {t("chat.search.lineExcerpts")}
        </p>
      )}
      {indexed && index && (
        <footer className="space-y-1 border-t border-border bg-muted/20 px-3 py-2 text-xs text-muted-foreground">
          <div className="flex flex-wrap gap-x-4 gap-y-1 tabular-nums">
            {generation !== undefined && <span>{t("chat.search.snapshot", { generation })}</span>}
            {age !== undefined && (
              <span>{t("chat.search.age", { seconds: number.format(age) })}</span>
            )}
            {filesIndexed !== undefined && (
              <span>
                {t("chat.search.indexedFiles", {
                  count: number.format(filesIndexed),
                })}
              </span>
            )}
          </div>
          {(index.known_modifications === true ||
            index.freshness === "known_dirty" ||
            index.lifecycle === "known_dirty") && <p>{t("chat.search.dirty")}</p>}
          {index.lifecycle === "refreshing" && <p>{t("chat.search.refreshing")}</p>}
          {index.lifecycle === "disposed" && <p>{t("chat.search.disposed")}</p>}
          {index.complete === false && (
            <p>
              {t("chat.search.partialIndex")}
              {typeof index.limit === "string" ? ` (${index.limit})` : ""}
            </p>
          )}
          {index.external_modifications === "unverified" && (
            <p>{t("chat.search.externalUnverified")}</p>
          )}
        </footer>
      )}
    </section>
  );
}
