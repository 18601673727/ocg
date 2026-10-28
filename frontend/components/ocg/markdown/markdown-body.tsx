"use client";

import { Fragment, type ReactNode, useCallback, useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { Check, Copy } from "lucide-react";

import {
  highlightCode,
  KATEX_OPTIONS,
  markdownParser,
  sanitizeUrl,
  type Token,
} from "./markdown-pipeline";
import { cn } from "@/lib/utils";
import { useTheme } from "../appearance/theme-provider";
import { useI18n } from "../i18n";
import { SourceLink, sourceReference, type FileSnapshot } from "./source-snapshot";
import { Chart, type ChatChartDefinition, type ChatChartSeries } from "../dashboard/chat-chart";

export interface MarkdownBodyProps {
  content: string;
  sourceSnapshots?: readonly FileSnapshot[];
  className?: string;
  /** Renders code and prose in the muted tone the reasoning panel uses. */
  inMuted?: boolean;
}

function CopyButton({ text, label }: { text: string; label: string }) {
  const [copied, setCopied] = useState(false);

  const onCopy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // A denied clipboard permission leaves the code on screen unchanged.
    }
  }, [text]);

  return (
    <button
      type="button"
      onClick={onCopy}
      className="p-1.5 rounded hover:bg-muted transition-colors"
      aria-label={label}
    >
      {copied ? <Check className="h-3.5 w-3.5 text-green-600" /> : <Copy className="h-3.5 w-3.5" />}
    </button>
  );
}

function MermaidDiagram({
  code,
  inMuted,
  errorLabel,
  diagramLabel,
}: {
  code: string;
  inMuted: boolean;
  errorLabel: string;
  diagramLabel: string;
}) {
  const { resolvedTheme, hydrated } = useTheme();
  const containerRef = useRef<HTMLDivElement>(null);
  const [error, setError] = useState<string | null>(null);
  const attempt = useRef(0);

  useEffect(() => {
    const container = containerRef.current;
    if (!hydrated || !container) return;

    const token = ++attempt.current;
    let cancelled = false;
    setError(null);
    container.innerHTML = "";

    void import("@crafter/mermaid")
      .then(({ render, THEMES }) => {
        if (cancelled || attempt.current !== token) return;
        container.innerHTML = render(code, {
          theme: resolvedTheme === "dark" ? THEMES["zinc-dark"] : THEMES["zinc-light"],
          transparent: true,
        });
        setError(null);
      })
      .catch((cause: unknown) => {
        if (cancelled || attempt.current !== token) return;
        setError(cause instanceof Error ? cause.message : errorLabel);
      });

    return () => {
      cancelled = true;
    };
  }, [code, errorLabel, hydrated, resolvedTheme]);

  return (
    <div className={cn("my-3", inMuted && "text-muted-foreground")}>
      <div
        ref={containerRef}
        className={cn("flex justify-center", error && "hidden")}
        role="img"
        aria-label={diagramLabel}
      />
      {error && (
        <div className="border border-destructive/50 rounded-md p-4 bg-destructive/5">
          <div className="text-sm font-medium text-destructive mb-2">{errorLabel}</div>
          <pre className="text-xs overflow-x-auto">
            <code>{code}</code>
          </pre>
        </div>
      )}
    </div>
  );
}

/**
 * KaTeX math.
 *
 * `trust` stays off so no `\href`, `\url` or `\includegraphics` in the source
 * can become a live link or a remote fetch, and an unrenderable expression
 * falls back to its own source instead of dropping it.
 */
function MathBlock({
  content,
  inline,
  inMuted,
}: {
  content: string;
  inline: boolean;
  inMuted: boolean;
}) {
  const hostRef = useRef<HTMLSpanElement>(null);
  const [failed, setFailed] = useState(false);
  // Identifies the newest render so a superseded async result — older content
  // resolving after newer, or after unmount — never writes into the host or
  // clears a failure that belongs to different content.
  const attempt = useRef(0);

  useEffect(() => {
    const host = hostRef.current;
    // The host stays mounted across failures, so it is always available and a
    // later valid expression can render into it again.
    if (!host) return;

    const token = ++attempt.current;
    let cancelled = false;
    host.innerHTML = "";

    void import("katex")
      .then(({ default: katex }) => {
        if (cancelled || attempt.current !== token) return;
        katex.render(content, host, {
          displayMode: !inline,
          ...KATEX_OPTIONS,
        });
        setFailed(false);
      })
      .catch(() => {
        if (!cancelled && attempt.current === token) setFailed(true);
      });

    return () => {
      cancelled = true;
    };
  }, [content, inline]);

  return (
    <>
      <span
        ref={hostRef}
        className={cn(
          inline ? "inline-math" : "block my-3 text-center",
          inMuted && "text-muted-foreground",
          failed && "hidden",
        )}
      />
      {failed && (
        <span className={cn("font-mono text-sm", inMuted && "text-muted-foreground")}>
          {inline ? `$${content}$` : `$$${content}$$`}
        </span>
      )}
    </>
  );
}

function parseChart(code: string): ChatChartDefinition | null {
  try {
    const value: unknown = JSON.parse(code);
    if (!value || typeof value !== "object") return null;
    const record = value as Record<string, unknown>;
    if (
      !["line", "bar", "pie", "combo"].includes(String(record.type)) ||
      !Array.isArray(record.labels) ||
      !Array.isArray(record.series)
    )
      return null;
    if (
      record.labels.length === 0 ||
      record.labels.length > 500 ||
      record.series.length === 0 ||
      record.series.length > 20
    )
      return null;
    if (!record.labels.every((label) => typeof label === "string" && label.length <= 200))
      return null;
    const series: ChatChartSeries[] = [];
    for (const raw of record.series) {
      if (!raw || typeof raw !== "object") return null;
      const item = raw as Record<string, unknown>;
      if (
        typeof item.label !== "string" ||
        !Array.isArray(item.data) ||
        item.data.length !== record.labels.length ||
        !item.data.every((number) => typeof number === "number" && Number.isFinite(number))
      )
        return null;
      if (item.type !== undefined && item.type !== "line" && item.type !== "bar") return null;
      if (item.axis !== undefined && item.axis !== "left" && item.axis !== "right") return null;
      series.push({
        label: item.label,
        data: item.data as number[],
        type: item.type as ChatChartSeries["type"],
        axis: item.axis as ChatChartSeries["axis"],
      });
    }
    if (
      record.type === "pie" &&
      (series.length !== 1 || series[0].data.some((number) => number < 0))
    )
      return null;
    return {
      type: record.type as ChatChartDefinition["type"],
      labels: record.labels as string[],
      series,
      ariaLabel: typeof record.ariaLabel === "string" ? record.ariaLabel : undefined,
    };
  } catch {
    return null;
  }
}

function CodeBlock({
  code,
  language,
  inMuted,
  copyLabel,
}: {
  code: string;
  language: string;
  inMuted: boolean;
  copyLabel: string;
}) {
  const { t } = useI18n();
  const chart = language === "ocg-chart" ? parseChart(code) : null;
  const highlight = highlightCode(code, language);
  const caption = highlight?.language || language || "text";
  const muted = inMuted ? "text-muted-foreground" : "";

  if (language === "mermaid") {
    return (
      <MermaidDiagram
        code={code}
        inMuted={inMuted}
        errorLabel={t("chat.mermaidError")}
        diagramLabel={t("chat.mermaidDiagram")}
      />
    );
  }

  if (language === "ocg-chart") {
    return chart ? (
      <Chart definition={chart} />
    ) : (
      <pre className="my-3 overflow-x-auto rounded-md border bg-muted p-3 text-sm">
        <code>{code}</code>
      </pre>
    );
  }

  return (
    <div className="relative group my-3">
      <div className="flex items-center justify-between px-3 py-1.5 bg-muted rounded-t-md border border-b-0">
        <span className={cn("text-xs font-mono", muted)}>{caption}</span>
        <CopyButton text={code} label={copyLabel} />
      </div>
      <pre
        className={cn(
          "overflow-x-auto p-3 bg-muted rounded-b-md border text-[13px] leading-relaxed",
          muted,
        )}
      >
        {highlight ? (
          // highlight.js escapes its input and emits only its own token spans.
          <code
            className="hljs bg-transparent p-0"
            dangerouslySetInnerHTML={{ __html: highlight.html }}
          />
        ) : (
          <code className={highlight ? "" : "hljs"}>{code}</code>
        )}
      </pre>
    </div>
  );
}

interface WalkContext {
  inMuted: boolean;
  sourceSnapshots?: readonly FileSnapshot[];
  copyLabel: string;
  nextKey: { current: number };
}

/** `attrGet` is typed as `string | number`; every attribute read here is textual. */
function attr(token: Token, name: string): string {
  const value = token.attrGet(name);
  return typeof value === "string" ? value : value == null ? "" : String(value);
}

function key(ctx: WalkContext): number {
  return ctx.nextKey.current++;
}

/**
 * Renders an inline token run.
 *
 * `i` is advanced past every token the renderer consumes, so a run of emphasis,
 * links, images and escapes nests correctly instead of re-reading the same
 * tokens.
 */
function inline(ctx: WalkContext, tokens: Token[], from = 0, to = tokens.length): ReactNode[] {
  const out: ReactNode[] = [];

  for (let i = from; i < to; i++) {
    const token = tokens[i];

    switch (token.type) {
      case "text":
        out.push(<Fragment key={key(ctx)}>{token.content}</Fragment>);
        break;
      case "code_inline":
        out.push(
          <code key={key(ctx)} className="px-1.5 py-0.5 rounded bg-muted font-mono text-sm">
            {token.content}
          </code>,
        );
        break;
      case "math_inline":
        out.push(<MathBlock key={key(ctx)} content={token.content} inline inMuted={ctx.inMuted} />);
        break;
      case "softbreak":
      case "hardbreak":
        out.push(<br key={key(ctx)} />);
        break;
      case "strong_open":
      case "em_open":
      case "s_open": {
        const tag = token.type === "strong_open" ? "strong" : token.type === "em_open" ? "em" : "s";
        const close = findClose(tokens, i, to, `${tag}_close`);
        const Tag = tag;
        out.push(<Tag key={key(ctx)}>{inline(ctx, tokens, i + 1, close)}</Tag>);
        i = close;
        break;
      }
      case "link_open": {
        const rawHref = attr(token, "href");
        const href = sanitizeUrl(rawHref);
        const close = findClose(tokens, i, to, "link_close");
        const body = inline(ctx, tokens, i + 1, close);
        i = close;
        if (ctx.sourceSnapshots && sourceReference(rawHref)) {
          out.push(
            <SourceLink key={key(ctx)} href={rawHref} snapshots={ctx.sourceSnapshots}>
              {body}
            </SourceLink>,
          );
          break;
        }
        if (!href) {
          // An unsafe target degrades to plain text rather than a live link.
          out.push(<Fragment key={key(ctx)}>{body}</Fragment>);
          break;
        }
        out.push(
          <Link
            key={key(ctx)}
            href={href}
            className="text-primary hover:underline"
            target="_blank"
            rel="noopener noreferrer"
          >
            {body}
          </Link>,
        );
        break;
      }
      case "image": {
        const src = sanitizeUrl(attr(token, "src"));
        if (src) {
          out.push(
            <span key={key(ctx)} className="block my-3">
              {/* eslint-disable-next-line @next/next/no-img-element */}
              <img src={src} alt={token.content} className="max-w-full h-auto rounded" />
            </span>,
          );
        }
        break;
      }
      case "html_inline": {
        // The task-list plugin injects its checkbox as raw HTML. Re-express it
        // as a real control; any other raw HTML the parser emitted is dropped.
        if (token.content.includes("task-list-item-checkbox")) {
          out.push(
            <input
              key={key(ctx)}
              type="checkbox"
              checked={token.content.includes("checked")}
              disabled
              readOnly
              className="task-list-item-checkbox absolute top-1.5 left-0 m-0"
            />,
          );
          if (
            i + 1 < to &&
            tokens[i + 1].type === "text" &&
            tokens[i + 1].content.startsWith(" ")
          ) {
            const trimmed = tokens[i + 1].content.slice(1);
            if (trimmed) {
              out.push(<Fragment key={key(ctx)}>{trimmed}</Fragment>);
            }
            i += 1;
          }
        }
        break;
      }
      default:
        break;
    }
  }

  return out;
}

/**
 * Index of the token that closes the element opened at `open`, searching to
 * `limit`. Returns `limit` when the run is truncated, which is what a stream
 * that stops mid-element produces; the partial content still renders.
 *
 * A nested element of the same kind opens before its sibling closes, so the
 * close is found by depth: the first matching close token at depth zero is
 * the one that belongs to the element opened at `open`.
 */
const CLOSE_FOR: Readonly<Record<string, string>> = {
  list_item_close: "list_item_open",
  bullet_list_close: "bullet_list_open",
  ordered_list_close: "ordered_list_open",
  blockquote_close: "blockquote_open",
  strong_close: "strong_open",
  em_close: "em_open",
  s_close: "s_open",
  link_close: "link_open",
  table_close: "table_open",
};

function findClose(tokens: Token[], open: number, limit: number, type: string): number {
  const openType = CLOSE_FOR[type];
  let depth = 0;
  for (let i = open + 1; i < limit; i++) {
    const current = tokens[i].type;
    if (openType && current === openType) {
      depth += 1;
      continue;
    }
    if (current === type) {
      if (depth === 0) return i;
      depth -= 1;
    }
  }
  return limit;
}

function headings(level: number, children: ReactNode): ReactNode {
  switch (level) {
    case 1:
      return <h1 className="text-2xl font-bold mb-3 mt-4">{children}</h1>;
    case 2:
      return <h2 className="text-lg font-semibold mb-2 pt-2">{children}</h2>;
    case 3:
      return <h3 className="text-base font-semibold mb-2 pt-2">{children}</h3>;
    case 5:
      return (
        <h5 className="text-xs font-semibold mb-2 pt-1 uppercase tracking-wide">{children}</h5>
      );
    case 6:
      return <h6 className="text-[13px] font-semibold mb-2 pt-1">{children}</h6>;
    default:
      return <h4 className="text-sm font-semibold mb-2 pt-1">{children}</h4>;
  }
}

/**
 * One `<li>`, starting at its `list_item_open`. Returns the element and the
 * index of its `list_item_close` so the caller can continue.
 */
function listItem(
  ctx: WalkContext,
  tokens: Token[],
  open: number,
  limit: number,
): [ReactNode, number] {
  const close = findClose(tokens, open, limit, "list_item_close");
  const children = walk(ctx, tokens, open + 1, close);
  const isTask = attr(tokens[open], "class").includes("task-list-item");

  return [
    <li key={key(ctx)} className={cn("leading-relaxed", isTask && "task-list-item relative pl-6")}>
      {children}
    </li>,
    close,
  ];
}

function walk(ctx: WalkContext, tokens: Token[], from = 0, to = tokens.length): ReactNode[] {
  const out: ReactNode[] = [];

  for (let i = from; i < to; i++) {
    const token = tokens[i];

    switch (token.type) {
      case "heading_open": {
        const level = Number.parseInt(token.tag.slice(1), 10);
        const content = tokens[i + 1];
        const body =
          content?.type === "inline" && content.children ? inline(ctx, content.children) : [];
        i += 2;
        out.push(<Fragment key={key(ctx)}>{headings(level, body)}</Fragment>);
        break;
      }
      case "paragraph_open": {
        const content = tokens[i + 1];
        const body =
          content?.type === "inline" && content.children ? inline(ctx, content.children) : [];
        i += 2;
        if (token.hidden) {
          out.push(<Fragment key={key(ctx)}>{body}</Fragment>);
        } else {
          out.push(
            <p key={key(ctx)} className="mb-3">
              {body}
            </p>,
          );
        }
        break;
      }
      case "bullet_list_open":
      case "ordered_list_open": {
        const ordered = token.type === "ordered_list_open";
        const taskList = attr(token, "class").includes("task-list");
        const startAttr = attr(token, "start");
        const closeType = ordered ? "ordered_list_close" : "bullet_list_close";
        const close = findClose(tokens, i, to, closeType);
        const items: ReactNode[] = [];

        for (let j = i + 1; j < close; j++) {
          if (tokens[j].type === "list_item_open") {
            const [item, itemClose] = listItem(ctx, tokens, j, close);
            items.push(item);
            j = itemClose;
          }
        }
        i = close;

        const listClass = taskList
          ? "mb-3 space-y-1 list-none"
          : ordered
            ? "list-decimal list-outside pl-6 mb-3 space-y-1"
            : "list-disc list-outside pl-6 mb-3 space-y-1";

        out.push(
          ordered ? (
            <ol
              key={key(ctx)}
              className={listClass}
              start={startAttr ? Number.parseInt(startAttr, 10) : undefined}
            >
              {items}
            </ol>
          ) : (
            <ul key={key(ctx)} className={listClass}>
              {items}
            </ul>
          ),
        );
        break;
      }
      case "blockquote_open": {
        const close = findClose(tokens, i, to, "blockquote_close");
        out.push(
          <blockquote
            key={key(ctx)}
            className="border-l-4 border-muted-foreground/30 pl-4 my-3 italic"
          >
            {walk(ctx, tokens, i + 1, close)}
          </blockquote>,
        );
        i = close;
        break;
      }
      case "fence":
      case "code_block":
        out.push(
          <CodeBlock
            key={key(ctx)}
            code={token.content}
            language={token.info.trim()}
            inMuted={ctx.inMuted}
            copyLabel={ctx.copyLabel}
          />,
        );
        break;
      case "math_block":
        out.push(
          <MathBlock key={key(ctx)} content={token.content} inline={false} inMuted={ctx.inMuted} />,
        );
        break;
      case "hr":
        out.push(<hr key={key(ctx)} className="my-6 border-border" />);
        break;
      case "table_open":
        out.push(table(ctx, tokens, i));
        i = tableEnd(tokens, i);
        break;
      case "html_block":
        // Raw HTML never reaches the DOM.
        break;
      default:
        break;
    }
  }

  return out;
}

/** Maps markdown-it's inline alignment style onto the CSS values React accepts. */
function textAlign(style: string): "left" | "center" | "right" | undefined {
  const value = style
    .replace(/^text-align:\s*/, "")
    .replace(/;.*$/, "")
    .trim();
  return value === "center" || value === "right" || value === "left" ? value : undefined;
}

function table(ctx: WalkContext, tokens: Token[], start: number): ReactNode {
  const head: ReactNode[] = [];
  const body: ReactNode[] = [];
  let row: ReactNode[] = [];
  let inHead = false;

  for (let i = start + 1; i < tokens.length && tokens[i].type !== "table_close"; i++) {
    const token = tokens[i];

    if (token.type === "thead_open") inHead = true;
    else if (token.type === "thead_close") inHead = false;
    else if (token.type === "tr_open") row = [];
    else if (token.type === "tr_close") {
      const rendered = (
        <tr key={key(ctx)} className={inHead ? "" : "border-t"}>
          {row}
        </tr>
      );
      (inHead ? head : body).push(rendered);
    } else if (token.type === "th_open" || token.type === "td_open") {
      const content = tokens[i + 1];
      const cells =
        content?.type === "inline" && content.children ? inline(ctx, content.children) : [];
      const align = textAlign(attr(token, "style"));
      row.push(
        token.type === "th_open" ? (
          <th
            key={key(ctx)}
            className="px-4 py-3 text-left align-top font-semibold leading-6"
            style={align ? { textAlign: align } : undefined}
          >
            {cells}
          </th>
        ) : (
          <td
            key={key(ctx)}
            className="px-4 py-3 align-top leading-6"
            style={align ? { textAlign: align } : undefined}
          >
            {cells}
          </td>
        ),
      );
      i++;
    }
  }

  return (
    <div key={key(ctx)} className="my-3 max-w-full overflow-x-auto rounded-lg border">
      <table className="min-w-full border-collapse text-sm">
        {head.length > 0 && <thead className="bg-muted/50">{head}</thead>}
        {body.length > 0 && <tbody>{body}</tbody>}
      </table>
    </div>
  );
}

function tableEnd(tokens: Token[], start: number): number {
  for (let i = start + 1; i < tokens.length; i++) {
    if (tokens[i].type === "table_close") return i;
  }
  return start;
}

/**
 * Renders Chat Markdown to React elements.
 *
 * The token stream is walked directly so every element is a React node with
 * class names this app controls. Nothing is injected as HTML except highlight.js
 * token spans, KaTeX output, and Mermaid SVG, each of which is produced by a
 * library from the code rather than passed through from the document.
 */
export function MarkdownBody({
  content,
  className,
  inMuted = false,
  sourceSnapshots,
}: MarkdownBodyProps) {
  const { t } = useI18n();
  const tokens = useMemo(() => markdownParser().parse(content, {}), [content]);
  const ctx: WalkContext = {
    inMuted,
    sourceSnapshots,
    copyLabel: t("chat.copyCode"),
    nextKey: { current: 0 },
  };

  return (
    <div className={cn(inMuted ? "text-[13px] leading-6" : "text-base leading-7", className)}>
      {walk(ctx, tokens)}
    </div>
  );
}
