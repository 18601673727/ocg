"use client";

import { useCallback, useEffect, useRef, useState, type ReactNode } from "react";
import { ChevronDown, ChevronRight, CirclePause, Repeat2 } from "lucide-react";

import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { TONE_CLASS, type Tone } from "../primitives/tone";
import type { ControlTreeNode, ControlTreeState } from "../contracts";
import type { HttpCanonicalControlClient } from "../runtime/control-client";
import { runtimeStateLabel, useI18n, type TranslateFn } from "../i18n";

/** What a state means to the eye: the same vocabulary every surface uses. */
const STATE_TONE: Record<ControlTreeState, Tone> = {
  running: "violet",
  queued: "slate",
  waiting: "slate",
  open: "slate",
  closing: "violet",
  settled: "slate",
  completed: "emerald",
  failed: "red",
  cancelled: "slate",
  unknown: "slate",
};

const DEPENDENCY_TONE: Record<"waiting" | "satisfied" | "impossible", Tone> = {
  waiting: "slate",
  satisfied: "emerald",
  impossible: "red",
};

function stateLabel(t: TranslateFn, state: ControlTreeState): string {
  if (state === "open") return t("jobs.tree.state.open");
  if (state === "closing") return t("jobs.tree.state.closing");
  return runtimeStateLabel(t, state);
}

/** The children a node already holds, then only the incoming ones no id names. */
function mergeChildren(existing: ControlTreeNode[], incoming: ControlTreeNode[]): ControlTreeNode[] {
  const seen = new Set<string>();
  const merged: ControlTreeNode[] = [];
  for (const child of [...existing, ...incoming]) {
    if (seen.has(child.id)) continue;
    seen.add(child.id);
    merged.push(child);
  }
  return merged;
}

/** One node with its children replaced, wherever it sits in the tree. */
function replaceNode(
  node: ControlTreeNode,
  id: string,
  update: (node: ControlTreeNode) => ControlTreeNode,
): ControlTreeNode {
  if (node.id === id) return update(node);
  if (!node.children) return node;
  let changed = false;
  const children = node.children.map((child) => {
    const next = replaceNode(child, id, update);
    if (next !== child) changed = true;
    return next;
  });
  return changed ? { ...node, children } : node;
}

/**
 * The tree of the work below a Job: who runs what, what waits on what, and
 * which conversations hold which turns. The server builds the tree; this view
 * only reads it and asks for the levels it was not given.
 */
export function JobTree({ client, root, selected, onSelect, onReroot, onOpenChat }: {
  client: HttpCanonicalControlClient;
  root: string;
  selected: string | null;
  onSelect: (job: string) => void;
  onReroot: (job: string) => void;
  onOpenChat?: (job: string) => void;
}) {
  const { t } = useI18n();
  const [revision, setRevision] = useState(0);
  const [tree, setTree] = useState<ControlTreeNode | null>(null);
  const [ancestors, setAncestors] = useState<string[]>([]);
  const [failure, setFailure] = useState<string | null>(null);
  const [loaded, setLoaded] = useState<{ root: string; revision: number } | null>(null);
  const [expanded, setExpanded] = useState<ReadonlySet<string>>(new Set<string>());
  // One page per node at a time: a second click while it is in flight is ignored.
  const loadingNodes = useRef(new Set<string>());

  const loading = loaded?.root !== root || loaded?.revision !== revision;

  useEffect(() => {
    const controller = new AbortController();
    client.readJobTree(root, { depth: 2, signal: controller.signal }).then(
      (answer) => {
        if (controller.signal.aborted) return;
        setTree(answer.root);
        setAncestors(answer.ancestors);
        setExpanded(new Set<string>([answer.root.id]));
        setFailure(null);
        setLoaded({ root, revision });
      },
      (cause: unknown) => {
        if (controller.signal.aborted) return;
        setFailure(cause instanceof Error ? cause.message : String(cause));
        setLoaded({ root, revision });
      },
    );
    return () => controller.abort();
  }, [client, root, revision]);

  /** Reads the children of a node the answer did not list, or the page after them. */
  const loadChildren = useCallback(async (node: ControlTreeNode, after?: string) => {
    if (loadingNodes.current.has(node.id)) return;
    loadingNodes.current.add(node.id);
    try {
      const answer = await client.readJobTree(
        node.id,
        after === undefined ? { depth: 2 } : { after, depth: 2 },
      );
      const fetched = answer.root.children ?? [];
      setTree((current) => {
        if (!current) return current;
        return replaceNode(current, node.id, (existing) => ({
          ...existing,
          children: mergeChildren(after === undefined ? [] : (existing.children ?? []), fetched),
          has_more_children: answer.root.has_more_children,
          next_after: answer.root.next_after,
        }));
      });
      setExpanded((current) => new Set([...current, node.id]));
    } finally {
      loadingNodes.current.delete(node.id);
    }
  }, [client]);

  const toggle = useCallback((node: ControlTreeNode) => {
    if ((node.children?.length ?? 0) > 0) {
      setExpanded((current) => {
        const next = new Set(current);
        if (next.has(node.id)) next.delete(node.id);
        else next.add(node.id);
        return next;
      });
      return;
    }
    if (!node.has_more_children) return;
    void loadChildren(node).catch((cause: unknown) => {
      setFailure(cause instanceof Error ? cause.message : String(cause));
    });
  }, [loadChildren]);

  const render = (node: ControlTreeNode, level: number, isRoot = false): ReactNode => {
    const isExpanded = expanded.has(node.id);
    const hasChildren = (node.children?.length ?? 0) > 0 || node.has_more_children;
    const agent = node.agent_name ?? node.agent ?? t("jobs.tree.noAgent");
    const activeTurn = node.active_turn;
    const nextAfter = node.next_after;
    return (
      <li
        key={node.id}
        role="treeitem"
        aria-expanded={hasChildren ? isExpanded : undefined}
        aria-selected={node.id === selected}
        className={cn("py-0.5", node.id === selected && "text-foreground")}
      >
        <div className="flex flex-wrap items-center gap-x-2 gap-y-1 text-sm">
          {hasChildren ? (
            <button
              type="button"
              onClick={() => toggle(node)}
              aria-label={isExpanded ? t("jobs.tree.collapse") : t("jobs.tree.expand")}
              className="rounded p-0.5 text-muted-foreground hover:bg-muted focus-visible:outline-2 focus-visible:outline-ring"
            >
              {isExpanded ? <ChevronDown className="size-4" /> : <ChevronRight className="size-4" />}
            </button>
          ) : (
            <span aria-hidden="true" className="inline-block w-5" />
          )}
          <button
            type="button"
            onClick={() => onSelect(node.id)}
            className="flex min-w-0 flex-1 flex-wrap items-center gap-2 rounded px-1 text-left hover:bg-muted focus-visible:outline-2 focus-visible:outline-ring"
          >
            <code className="font-geist-mono text-xs">{node.id}</code>
            <span className="break-words">{node.title || t("jobs.unnamed")}</span>
          </button>
          <span className={cn("rounded-full px-2 py-0.5 text-xs font-semibold tracking-widest uppercase", TONE_CLASS[STATE_TONE[node.state]])}>
            {stateLabel(t, node.state)}
          </span>
          {node.on_hold ? (
            <span title={t("jobs.tree.held")} aria-label={t("jobs.tree.held")} className="text-muted-foreground">
              <CirclePause className="size-4" />
            </span>
          ) : null}
          {node.recurrence ? (
            <span title={t("jobs.tree.recurring")} className="flex items-center gap-1 text-xs text-muted-foreground">
              <Repeat2 className="size-4" />
              {t(`chat.child.${node.recurrence.phase}`)}
            </span>
          ) : null}
          {node.origin ? (
            <span className="text-xs text-muted-foreground">{t(`jobs.tree.origin.${node.origin}`)}</span>
          ) : null}
          <span className="text-xs text-muted-foreground">{agent}</span>
          {node.model ? <span className="text-xs text-muted-foreground">{node.model.model}</span> : null}
          {(node.kind === "conversation" || node.conversation !== null) && onOpenChat ? (
            <Button
              type="button"
              variant="outline"
              size="xs"
              className="shrink-0"
              onClick={(event) => {
                event.stopPropagation();
                onOpenChat(node.id);
              }}
            >
              {t("jobs.openInChat")}
            </Button>
          ) : null}
        </div>
        {node.depends_on.length > 0 ? (
          <div className="flex flex-wrap items-center gap-2 pl-7 text-xs">
            <span className="text-muted-foreground">{t("jobs.tree.waitsOn")}</span>
            {node.depends_on.map((dependency) => (
              <button
                key={dependency.job}
                type="button"
                onClick={() => onSelect(dependency.job)}
                className="flex items-center gap-1 rounded px-1 hover:bg-muted focus-visible:outline-2 focus-visible:outline-ring"
              >
                <code className="font-geist-mono">{dependency.job}</code>
                <span className={cn("rounded-full px-2 py-0.5 font-semibold tracking-widest uppercase", TONE_CLASS[DEPENDENCY_TONE[dependency.state]])}>
                  {t(`jobs.tree.dependency.${dependency.state}`)}
                </span>
              </button>
            ))}
          </div>
        ) : null}
        {activeTurn ? (
          <div className="flex flex-wrap items-center gap-2 pl-7 text-xs">
            <span className="text-muted-foreground">{t("jobs.tree.running")}</span>
            <button
              type="button"
              onClick={() => onSelect(activeTurn)}
              className="rounded px-1 hover:bg-muted focus-visible:outline-2 focus-visible:outline-ring"
            >
              <code className="font-geist-mono">{activeTurn}</code>
            </button>
          </div>
        ) : null}
        {isExpanded && node.children ? (
          <ul role="group" className="ml-3 border-l border-border pl-3">
            {node.children.map((child) => render(child, level + 1))}
          </ul>
        ) : null}
        {isExpanded && nextAfter ? (
          <div className="pl-7">
            <Button
              type="button"
              variant="outline"
              size="xs"
              onClick={() => {
                void loadChildren(node, nextAfter).catch((cause: unknown) => {
                  setFailure(cause instanceof Error ? cause.message : String(cause));
                });
              }}
            >
              {t("jobs.tree.more")}
            </Button>
          </div>
        ) : null}
        {isRoot && !hasChildren ? (
          <p className="pl-7 text-sm text-muted-foreground">{t("jobs.tree.empty")}</p>
        ) : null}
      </li>
    );
  };

  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-wrap items-center justify-between gap-3">
        {ancestors.length > 0 ? (
          <nav aria-label={t("jobs.tree.ancestors")} className="flex flex-wrap items-center gap-1 text-xs">
            {ancestors.map((ancestor) => (
              <button
                key={ancestor}
                type="button"
                onClick={() => onReroot(ancestor)}
                className="rounded px-1 hover:bg-muted focus-visible:outline-2 focus-visible:outline-ring"
              >
                <code className="font-geist-mono">{ancestor}</code>
              </button>
            ))}
          </nav>
        ) : (
          <span />
        )}
        <Button variant="outline" size="sm" disabled={loading} onClick={() => setRevision((current) => current + 1)}>
          {t("common.refresh")}
        </Button>
      </div>
      {failure ? <p role="alert" className="text-sm text-destructive">{failure}</p> : null}
      {loading && !tree ? <p role="status" className="text-sm text-muted-foreground">{t("common.loading")}</p> : null}
      {tree ? (
        <ul role="tree" aria-label={t("jobs.tree.title")} className="flex flex-col">
          {render(tree, 0, true)}
        </ul>
      ) : null}
    </div>
  );
}
