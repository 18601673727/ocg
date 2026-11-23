import { Check, Copy, Maximize2, Minus, Plus } from "@lucide/solid";
import { Dynamic } from "@solidjs/web";
import { createEffect, createMemo, createSignal, For } from "solid-js";
import { Dialog, DialogContent, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import { bindElementRef, elementRef, Group, type View } from "@/lib/ui-state";
import { cn } from "@/lib/utils";
import { useTheme } from "../appearance/theme-provider";
import { Chart, type ChatChartDefinition, type ChatChartSeries } from "../dashboard/chat-chart";
import { useI18n } from "../i18n";
import {
  highlightCode,
  KATEX_OPTIONS,
  markdownParser,
  sanitizeUrl,
  type Token,
} from "./markdown-pipeline";
import { type FileSnapshot, SourceLink, sourceReference } from "./source-snapshot";

export interface MarkdownBodyProps {
  content: string;
  sourceSnapshots?: readonly FileSnapshot[];
  class?: string;
  inMuted?: boolean;
}

function CopyButton(props: { text: string; label: string }) {
  const [copied, setCopied] = createSignal(false);

  const onCopy = async () => {
    try {
      await navigator.clipboard.writeText(props.text);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {}
  };

  return (
    <button
      type="button"
      onClick={onCopy}
      class="p-1.5 rounded hover:bg-muted transition-colors"
      aria-label={props.label}
    >
      {copied() ? <Check class="h-3.5 w-3.5 text-green-600" /> : <Copy class="h-3.5 w-3.5" />}
    </button>
  );
}

function MermaidDiagram(props: {
  code: string;
  inMuted: boolean;
  errorLabel: string;
  diagramLabel: string;
}) {
  const themeContext19d = useTheme();
  const { theme } = themeContext19d.preferences;
  const containerRef = elementRef<HTMLDivElement>(null);
  const [error, setError] = createSignal<string | null>(null);
  const [svg, setSvg] = createSignal("");
  const [open, setOpen] = createSignal(false);
  const [zoom, setZoom] = createSignal(1);
  const i18n = useI18n();
  const attempt = elementRef(0);

  createEffect(
    () => [props.code, props.errorLabel, themeContext19d.hydrated, theme],
    () => {
      const container = containerRef.current;
      if (!themeContext19d.hydrated || !container) return;

      const token = ++attempt.current;
      let cancelled = false;
      setError(null);
      setSvg("");
      container.innerHTML = "";

      void import("@crafter/mermaid")
        .then(({ render, THEMES }) => {
          if (cancelled || attempt.current !== token) return;
          const rendered = render(props.code, {
            theme: theme === "dark" ? THEMES["zinc-dark"] : THEMES["zinc-light"],
            transparent: true,
          });
          container.innerHTML = rendered;
          setSvg(rendered);
          setError(null);
        })
        .catch((cause: unknown) => {
          if (cancelled || attempt.current !== token) return;
          setError(cause instanceof Error ? cause.message : props.errorLabel);
        });

      return () => {
        cancelled = true;
      };
    },
  );

  return (
    <div class={cn("group relative my-3", props.inMuted && "text-muted-foreground")}>
      <div
        ref={bindElementRef(containerRef)}
        class={cn("flex justify-center", error() && "hidden")}
        role="img"
        aria-label={props.diagramLabel}
      />
      {svg() && !error() && (
        <Dialog
          open={open()}
          onOpenChange={(next) => {
            setOpen(next);
            if (!next) setZoom(1);
          }}
        >
          <DialogTrigger
            render={
              <button
                type="button"
                class="absolute top-2 right-2 rounded-md border bg-popover p-2 text-popover-foreground shadow-sm transition-opacity hover:bg-muted focus-visible:opacity-100 max-md:opacity-100 md:opacity-0 md:group-hover:opacity-100"
                aria-label={i18n.t("chat.expandDiagram")}
                title={i18n.t("chat.expandDiagram")}
              />
            }
          >
            <Maximize2 class="h-4 w-4" />
          </DialogTrigger>
          <DialogContent class="flex h-[calc(100dvh-2rem)] w-[calc(100vw-2rem)] max-w-none flex-col gap-0 overflow-hidden p-0 sm:max-w-none">
            <div class="flex shrink-0 items-center gap-3 border-b px-6 py-4 pr-14">
              <DialogTitle class="mr-auto">{props.diagramLabel}</DialogTitle>
              <button
                type="button"
                class="rounded p-1.5 hover:bg-muted disabled:opacity-50"
                onClick={() => setZoom((value) => Math.max(0.5, value - 0.25))}
                disabled={zoom() <= 0.5}
                aria-label={i18n.t("chat.zoomOut")}
                title={i18n.t("chat.zoomOut")}
              >
                <Minus class="h-4 w-4" />
              </button>
              <button
                type="button"
                class="min-w-12 rounded p-1.5 tabular-nums hover:bg-muted"
                onClick={() => setZoom(1)}
                aria-label={i18n.t("chat.resetZoom")}
                title={i18n.t("chat.resetZoom")}
              >
                {Math.round(zoom() * 100)}%
              </button>
              <button
                type="button"
                class="rounded p-1.5 hover:bg-muted disabled:opacity-50"
                onClick={() => setZoom((value) => Math.min(3, value + 0.25))}
                disabled={zoom() >= 3}
                aria-label={i18n.t("chat.zoomIn")}
                title={i18n.t("chat.zoomIn")}
              >
                <Plus class="h-4 w-4" />
              </button>
            </div>
            <div class="ocg-subtle-scrollbar min-h-0 flex-1 overflow-auto p-6">
              <div class="w-max min-w-full" style={{ zoom: zoom() }}>
                <div
                  class="[&>svg]:mx-auto [&>svg]:max-w-none"
                  role="img"
                  aria-label={props.diagramLabel}
                  innerHTML={svg()}
                />
              </div>
            </div>
          </DialogContent>
        </Dialog>
      )}
      {error() && (
        <div class="border border-destructive/50 rounded-md p-4 bg-destructive/5">
          <div class="text-sm font-medium text-destructive mb-2">{props.errorLabel}</div>
          <pre class="text-xs overflow-x-auto">
            <code>{props.code}</code>
          </pre>
        </div>
      )}
    </div>
  );
}

function MathBlock(props: { content: string; inline: boolean; inMuted: boolean }) {
  const hostRef = elementRef<HTMLSpanElement>(null);
  const [failed, setFailed] = createSignal(false);
  const attempt = elementRef(0);

  createEffect(
    () => [props.content, props.inline],
    () => {
      const host = hostRef.current;
      if (!host) return;

      const token = ++attempt.current;
      let cancelled = false;
      host.innerHTML = "";

      void import("katex")
        .then(({ default: katex }) => {
          if (cancelled || attempt.current !== token) return;
          katex.render(props.content, host, {
            displayMode: !props.inline,
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
    },
  );

  return (
    <>
      <span
        ref={bindElementRef(hostRef)}
        class={cn(
          props.inline ? "inline-math" : "block my-3 text-center",
          props.inMuted && "text-muted-foreground",
          failed() && "hidden",
        )}
      />
      {failed() && (
        <span class={cn("font-mono text-sm", props.inMuted && "text-muted-foreground")}>
          {props.inline ? `$${props.content}$` : `$$${props.content}$$`}
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

function CodeBlock(props: { code: string; language: string; inMuted: boolean; copyLabel: string }) {
  const i18n = useI18n();
  const chart = createMemo(() => (props.language === "ocg-chart" ? parseChart(props.code) : null));
  const highlight = createMemo(() => highlightCode(props.code, props.language));
  const caption = createMemo(() => highlight()?.language || props.language || "text");
  const muted = createMemo(() => (props.inMuted ? "text-muted-foreground" : ""));

  return (
    <>
      {(() => {
        if (props.language === "mermaid") {
          return (
            <MermaidDiagram
              code={props.code}
              inMuted={props.inMuted}
              errorLabel={i18n.t("chat.mermaidError")}
              diagramLabel={i18n.t("chat.mermaidDiagram")}
            />
          );
        }

        if (props.language === "ocg-chart") {
          return chart() ? (
            <Chart definition={chart()!} />
          ) : (
            <pre class="my-3 overflow-x-auto rounded-md border bg-muted p-3 text-sm">
              <code>{props.code}</code>
            </pre>
          );
        }

        return (
          <div class="relative group my-3">
            <div class="flex items-center justify-between px-3 py-1.5 bg-muted rounded-t-md border border-b-0">
              <span class={cn("text-xs font-mono", muted())}>{caption()}</span>
              <CopyButton text={props.code} label={props.copyLabel} />
            </div>
            <pre
              class={cn(
                "overflow-x-auto p-3 bg-muted rounded-b-md border text-[13px] leading-relaxed",
                muted(),
              )}
            >
              {highlight() ? (
                <code class="hljs bg-transparent p-0" innerHTML={highlight()!.html} />
              ) : (
                <code class="hljs">{props.code}</code>
              )}
            </pre>
          </div>
        );
      })()}
    </>
  );
}

interface WalkContext {
  inMuted: boolean;
  sourceSnapshots?: readonly FileSnapshot[];
  copyLabel: string;
}

function attr(token: Token, name: string): string {
  const value = token.attrGet(name);
  return typeof value === "string" ? value : value == null ? "" : String(value);
}

function inline(parent: WalkContext, tokens: Token[], from = 0, to = tokens.length): View[] {
  const ctx = parent;
  const out: View[] = [];

  for (let i = from; i < to; i++) {
    const token = tokens[i];

    switch (token.type) {
      case "text":
        out.push(<Group>{token.content}</Group>);
        break;
      case "code_inline":
        out.push(
          <code class="px-1.5 py-0.5 rounded bg-muted font-mono text-sm">{token.content}</code>,
        );
        break;
      case "math_inline":
        out.push(<MathBlock content={token.content} inline inMuted={ctx.inMuted} />);
        break;
      case "softbreak":
      case "hardbreak":
        out.push(<br />);
        break;
      case "strong_open":
      case "em_open":
      case "s_open": {
        const tag = token.type === "strong_open" ? "strong" : token.type === "em_open" ? "em" : "s";
        const close = findClose(tokens, i, to, `${tag}_close`);
        const body = inline(ctx, tokens, i + 1, close);
        out.push(<Dynamic component={tag}>{body}</Dynamic>);
        i = close;
        break;
      }
      case "link_open": {
        const rawHref = attr(token, "href");
        const href = sanitizeUrl(rawHref);
        const close = findClose(tokens, i, to, "link_close");
        const body = inline(ctx, tokens, i + 1, close);
        i = close;
        if (sourceReference(rawHref) && ctx.sourceSnapshots) {
          out.push(
            <SourceLink href={rawHref} snapshots={ctx.sourceSnapshots}>
              {body}
            </SourceLink>,
          );
          break;
        }
        if (!href) {
          out.push(<Group>{body}</Group>);
          break;
        }
        out.push(
          <a
            href={href}
            class="text-primary hover:underline"
            target="_blank"
            rel="noopener noreferrer"
          >
            {body}
          </a>,
        );
        break;
      }
      case "image": {
        const src = sanitizeUrl(attr(token, "src"));
        if (src) {
          out.push(
            <span class="block my-3">
              <img src={src} alt={token.content} class="max-w-full h-auto rounded" />
            </span>,
          );
        }
        break;
      }
      case "html_inline": {
        if (token.content.includes("task-list-item-checkbox")) {
          out.push(
            <input
              type="checkbox"
              checked={token.content.includes("checked")}
              disabled
              readonly
              class="task-list-item-checkbox absolute top-1.5 left-0 m-0"
            />,
          );
          if (
            i + 1 < to &&
            tokens[i + 1].type === "text" &&
            tokens[i + 1].content.startsWith(" ")
          ) {
            const trimmed = tokens[i + 1].content.slice(1);
            if (trimmed) {
              out.push(<Group>{trimmed}</Group>);
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

function headings(level: number, children: View): View {
  switch (level) {
    case 1:
      return <h1 class="text-2xl font-bold mb-3 mt-4">{children}</h1>;
    case 2:
      return <h2 class="text-lg font-semibold mb-2 pt-2">{children}</h2>;
    case 3:
      return <h3 class="text-base font-semibold mb-2 pt-2">{children}</h3>;
    case 5:
      return <h5 class="text-xs font-semibold mb-2 pt-1 uppercase tracking-wide">{children}</h5>;
    case 6:
      return <h6 class="text-[13px] font-semibold mb-2 pt-1">{children}</h6>;
    default:
      return <h4 class="text-sm font-semibold mb-2 pt-1">{children}</h4>;
  }
}

function listItem(ctx: WalkContext, tokens: Token[], open: number, limit: number): [View, number] {
  const close = findClose(tokens, open, limit, "list_item_close");
  const children = walk(ctx, tokens, open + 1, close);
  const isTask = attr(tokens[open], "class").includes("task-list-item");

  return [
    <li class={cn("leading-relaxed", isTask && "task-list-item relative pl-6")}>{children}</li>,
    close,
  ];
}

function walk(parent: WalkContext, tokens: Token[], from = 0, to = tokens.length): View[] {
  const ctx = parent;
  const out: View[] = [];

  for (let i = from; i < to; i++) {
    const token = tokens[i];

    switch (token.type) {
      case "heading_open": {
        const level = Number.parseInt(token.tag.slice(1), 10);
        const content = tokens[i + 1];
        const body =
          content?.type === "inline" && content.children ? inline(ctx, content.children) : [];
        i += 2;
        out.push(<Group>{headings(level, body)}</Group>);
        break;
      }
      case "paragraph_open": {
        const content = tokens[i + 1];
        const body =
          content?.type === "inline" && content.children ? inline(ctx, content.children) : [];
        i += 2;
        if (token.hidden) {
          out.push(<Group>{body}</Group>);
        } else {
          out.push(<p class="mb-3">{body}</p>);
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
        const items: View[] = [];

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
            <ol class={listClass} start={startAttr ? Number.parseInt(startAttr, 10) : undefined}>
              {items}
            </ol>
          ) : (
            <ul class={listClass}>{items}</ul>
          ),
        );
        break;
      }
      case "blockquote_open": {
        const close = findClose(tokens, i, to, "blockquote_close");
        out.push(
          <blockquote class="border-l-4 border-muted-foreground/30 pl-4 my-3 italic">
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
            code={token.content}
            language={token.info.trim()}
            inMuted={ctx.inMuted}
            copyLabel={ctx.copyLabel}
          />,
        );
        break;
      case "math_block":
        out.push(<MathBlock content={token.content} inline={false} inMuted={ctx.inMuted} />);
        break;
      case "hr":
        out.push(<hr class="my-6 border-border" />);
        break;
      case "table_open":
        out.push(<Group>{table(ctx, tokens, i)}</Group>);
        i = tableEnd(tokens, i);
        break;
      case "html_block":
        break;
      default:
        break;
    }
  }

  return out;
}

function textAlign(style: string): "left" | "center" | "right" | undefined {
  const value = style
    .replace(/^text-align:\s*/, "")
    .replace(/;.*$/, "")
    .trim();
  return value === "center" || value === "right" || value === "left" ? value : undefined;
}

function table(ctx: WalkContext, tokens: Token[], start: number): View {
  const head: View[] = [];
  const body: View[] = [];
  let row: View[] = [];
  let inHead = false;

  for (let i = start + 1; i < tokens.length && tokens[i].type !== "table_close"; i++) {
    const token = tokens[i];

    if (token.type === "thead_open") inHead = true;
    else if (token.type === "thead_close") inHead = false;
    else if (token.type === "tr_open") row = [];
    else if (token.type === "tr_close") {
      const rendered = <tr class={inHead ? "" : "border-t"}>{row}</tr>;
      (inHead ? head : body).push(rendered);
    } else if (token.type === "th_open" || token.type === "td_open") {
      const content = tokens[i + 1];
      const cells =
        content?.type === "inline" && content.children ? inline(ctx, content.children) : [];
      const align = textAlign(attr(token, "style"));
      row.push(
        token.type === "th_open" ? (
          <th
            class="px-4 py-3 text-left align-top font-semibold leading-6"
            style={align ? { "text-align": align } : undefined}
          >
            {cells}
          </th>
        ) : (
          <td
            class="px-4 py-3 align-top leading-6"
            style={align ? { "text-align": align } : undefined}
          >
            {cells}
          </td>
        ),
      );
      i++;
    }
  }

  return (
    <div class="my-3 max-w-full overflow-x-auto rounded-lg border">
      <table class="min-w-full border-collapse text-sm">
        {head.length > 0 && <thead class="bg-muted/50">{head}</thead>}
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

function jsonDocument(content: string): string | null {
  const first = content.search(/\S/);
  if (first < 0 || (content[first] !== "{" && content[first] !== "[")) return null;
  const end = content.trimEnd();
  if (!end.endsWith(content[first] === "{" ? "}" : "]")) return null;
  const code = end.slice(first);
  try {
    const value: unknown = JSON.parse(code);
    if (typeof value !== "object" || value === null) return null;
    return JSON.stringify(value) === code ? JSON.stringify(value, null, 2) : code;
  } catch {
    return null;
  }
}

type MarkdownBlock = { start: number; source: string; tokens: Token[] };
type MarkdownDocument = { content: string; blocks: MarkdownBlock[]; resume: number };

function sameRenderedTokens(left: Token[], right: Token[]) {
  const fields = ["type", "tag", "content", "attrs", "info", "hidden", "children"];
  return JSON.stringify(left, fields) === JSON.stringify(right, fields);
}

function markdownBlocks(content: string, previous?: MarkdownDocument): MarkdownDocument {
  // A reference definition at the end can change links anywhere in the document.
  const references = content.includes("]:");
  const appended = previous && content.startsWith(previous.content);
  const resume = appended && !references ? previous.resume : 0;
  const base = resume > 0 ? previous!.blocks[resume].start : 0;
  const suffix = content.slice(base);
  const tokens = markdownParser().parse(suffix, {});
  const blocks = previous?.blocks.slice(0, resume) ?? [];
  let nextResume = resume;

  const starts = [0];
  for (const match of suffix.matchAll(/\r\n?|\n/g)) starts.push(match.index + match[0].length);
  for (let i = 0; i < tokens.length; ) {
    const first = tokens[i];
    let j = i + 1;
    if (first.nesting === 1) {
      while (j < tokens.length && tokens[j].level !== 0) j++;
      if (j < tokens.length) j++;
    }
    const lines = first.map;
    if (!lines)
      return {
        content,
        resume: 0,
        blocks: [{ start: 0, source: content, tokens: markdownParser().parse(content, {}) }],
      };
    const start = starts[lines[0]];
    const source = suffix.slice(start, starts[lines[1]] ?? suffix.length);
    const chunk = tokens.slice(i, j);
    const prior = previous?.blocks[blocks.length];

    // Resume only at a confirmed top-level block after a blank line. A partial
    // last line can still change the preceding paragraph, list or table.
    if (
      lines[0] > 0 &&
      lines[0] + 1 < starts.length &&
      /^[ \t]*(?:\r\n?|\n)$/.test(suffix.slice(starts[lines[0] - 1], start))
    ) {
      nextResume = blocks.length;
    }
    const sameSource = prior?.start === base + start && prior.source === source;
    const local = appended && (!references || !source.includes("["));
    blocks.push(
      sameSource && (local || sameRenderedTokens(prior.tokens, chunk))
        ? prior
        : { start: base + start, source, tokens: chunk },
    );
    i = j;
  }
  return { content, blocks, resume: nextResume };
}

export function MarkdownBody(props: MarkdownBodyProps) {
  const i18n = useI18n();
  const json = createMemo(() => jsonDocument(props.content));
  const document = createMemo<MarkdownDocument>((previous) =>
    markdownBlocks(json() === null ? props.content : "", previous),
  );
  const snapshots = createMemo(() => props.sourceSnapshots, {
    equals: (previous, next) =>
      previous === next ||
      (previous !== undefined &&
        next !== undefined &&
        previous.length === next.length &&
        previous.every((snapshot, i) => snapshot === next[i])),
  });
  const ctx: WalkContext = {
    get inMuted() {
      return props.inMuted ?? false;
    },
    get sourceSnapshots() {
      return snapshots();
    },
    get copyLabel() {
      return i18n.t("chat.copyCode");
    },
  };

  return (
    <div
      class={cn(
        (props.inMuted === undefined ? false : props.inMuted)
          ? "text-[13px] leading-6"
          : "text-base leading-7",
        props.class,
      )}
    >
      {json() === null ? (
        <For each={document().blocks}>{(block) => <>{walk(ctx, block.tokens)}</>}</For>
      ) : (
        <CodeBlock
          code={json()!}
          language="json"
          inMuted={props.inMuted === undefined ? false : props.inMuted}
          copyLabel={ctx.copyLabel}
        />
      )}
    </div>
  );
}
