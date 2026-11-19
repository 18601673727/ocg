import { ChevronDown, ChevronRight, CirclePause, Repeat2 } from "@lucide/solid";
import { createEffect, createMemo, createSignal } from "solid-js";
import { DismissibleNotice } from "@/components/ocg/primitives/dismissible-notice";
import { Button } from "@/components/ui/button";
import { ariaState, elementRef, type View } from "@/lib/ui-state";
import { cn } from "@/lib/utils";
import type { TreeNode, TreeState } from "../contracts";
import { runtimeStateLabel, type TranslateFn, useI18n } from "../i18n";
import { TONE_CLASS, type Tone } from "../primitives/tone";
import type { ApiClient } from "../runtime/api-client";

const STATE_TONE: Record<TreeState, Tone> = {
  running: "violet",
  waiting_for_user: "slate",
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

function stateLabel(t: TranslateFn, state: TreeState): string {
  if (state === "open") return t("jobs.tree.state.open");
  if (state === "closing") return t("jobs.tree.state.closing");
  return runtimeStateLabel(t, state);
}

export function mergeChildren(existing: TreeNode[], incoming: TreeNode[]): TreeNode[] {
  const merged = new Map(existing.map((child) => [child.id, child]));
  for (const child of incoming) {
    const previous = merged.get(child.id);
    const children = previous?.children
      ? mergeChildren(previous.children, child.children ?? [])
      : child.children;
    const page = child.children === null && previous?.children ? previous : child;
    merged.set(child.id, {
      ...child,
      children,
      has_more_children: page.has_more_children,
      next_after: page.next_after === null ? null : (children?.at(-1)?.id ?? page.next_after),
    });
  }
  return [...merged.values()];
}

function replaceNode(node: TreeNode, id: string, update: (node: TreeNode) => TreeNode): TreeNode {
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

export function JobTree(props: {
  client: ApiClient;
  root: string;
  selected: string | null;
  onSelect: (job: string) => void;
  onReroot: (job: string) => void;
  onOpenChat?: (job: string) => void;
}) {
  const i18n = useI18n();
  const [tree, setTree] = createSignal<TreeNode | null>(null);
  const [ancestors, setAncestors] = createSignal<string[]>([]);
  const [failure, setFailure] = createSignal<string | null>(null);
  const [loaded, setLoaded] = createSignal<string | null>(null);
  const [expanded, setExpanded] = createSignal<ReadonlySet<string>>(new Set<string>());
  const loadingNodes = elementRef(new Map<string, AbortController>());

  const loading = createMemo(() => loaded() !== props.root);

  createEffect(
    () => [props.client, props.root],
    () => {
      const controller = new AbortController();
      const pending = loadingNodes.current;
      let initialized = false;
      let reading = false;
      let queued = false;
      let timer: ReturnType<typeof setTimeout> | undefined;
      const refresh = async () => {
        if (reading) {
          queued = true;
          return;
        }
        reading = true;
        try {
          const answer = await props.client.readJobTree(props.root, {
            depth: 2,
            signal: controller.signal,
          });
          if (controller.signal.aborted) return;
          setTree((current) =>
            current && initialized
              ? {
                  ...answer.root,
                  children: mergeChildren(current.children ?? [], answer.root.children ?? []),
                  has_more_children: current.has_more_children,
                  next_after: current.next_after,
                }
              : answer.root,
          );
          setAncestors(answer.ancestors);
          if (!initialized) setExpanded(new Set<string>([answer.root.id]));
          initialized = true;
          setFailure(null);
          setLoaded(props.root);
        } catch (cause: unknown) {
          if (controller.signal.aborted) return;
          setFailure(cause instanceof Error ? cause.message : String(cause));
          setLoaded(props.root);
        } finally {
          reading = false;
          if (queued && !controller.signal.aborted) {
            queued = false;
            void refresh();
          }
        }
      };
      const unsubscribe = props.client.subscribeCatalog(
        () => {
          if (!timer)
            timer = setTimeout(() => {
              timer = undefined;
              void refresh();
            }, 300);
        },
        (cause) => {
          if (!controller.signal.aborted)
            setFailure(cause instanceof Error ? cause.message : String(cause));
        },
      );
      return () => {
        controller.abort();
        unsubscribe();
        if (timer) clearTimeout(timer);
        for (const child of pending.values()) child.abort();
        pending.clear();
      };
    },
  );

  const loadChildren = async (node: TreeNode, after?: string) => {
    if (loadingNodes.current.has(node.id)) return;
    const controller = new AbortController();
    loadingNodes.current.set(node.id, controller);
    try {
      const answer = await props.client.readJobTree(node.id, {
        after,
        depth: 2,
        signal: controller.signal,
      });
      if (controller.signal.aborted) return;
      const fetched = answer.root.children ?? [];
      setTree((current) => {
        if (!current) return current;
        return replaceNode(current, node.id, (existing) => ({
          ...answer.root,
          children: mergeChildren(after === undefined ? [] : (existing.children ?? []), fetched),
          has_more_children: answer.root.has_more_children,
          next_after: answer.root.next_after,
        }));
      });
      setExpanded((current) => new Set([...current, node.id]));
    } catch (cause: unknown) {
      if (!controller.signal.aborted) throw cause;
    } finally {
      if (loadingNodes.current.get(node.id) === controller) loadingNodes.current.delete(node.id);
    }
  };

  const toggle = (node: TreeNode) => {
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
  };

  const render = (node: TreeNode, isRoot = false): View => {
    const isExpanded = expanded().has(node.id);
    const hasChildren = (node.children?.length ?? 0) > 0 || node.has_more_children;
    const agent = node.agent_name ?? node.agent ?? i18n.t("jobs.tree.noAgent");
    const activeTurn = node.active_turn;
    const nextAfter = node.next_after;
    return (
      <li class={cn("py-0.5", node.id === props.selected && "text-foreground")}>
        <div class="flex items-start gap-2 text-sm">
          {hasChildren ? (
            <button
              type="button"
              disabled={loading()}
              onClick={() => toggle(node)}
              aria-expanded={ariaState(isExpanded)}
              aria-label={isExpanded ? i18n.t("jobs.tree.collapse") : i18n.t("jobs.tree.expand")}
              class="mt-0.5 shrink-0 rounded p-0.5 text-muted-foreground hover:bg-muted focus-visible:outline-2 focus-visible:outline-ring"
            >
              {isExpanded ? <ChevronDown class="size-4" /> : <ChevronRight class="size-4" />}
            </button>
          ) : (
            <span aria-hidden="true" class="mt-0.5 inline-block w-5 shrink-0" />
          )}
          <div class="min-w-0 flex-1 space-y-1">
            <button
              type="button"
              onClick={() => props.onSelect(node.id)}
              aria-current={node.id === props.selected ? "true" : undefined}
              class="block w-full min-w-0 rounded px-1 text-left hover:bg-muted focus-visible:outline-2 focus-visible:outline-ring"
            >
              <code class="block break-all font-mono text-xs">{node.id}</code>
              <span class="break-words">{node.title || i18n.t("jobs.unnamed")}</span>
            </button>
            <div class="flex flex-wrap items-center gap-x-2 gap-y-1 px-1">
              <span
                class={cn(
                  "rounded-full px-2 py-0.5 text-xs font-semibold tracking-widest uppercase",
                  TONE_CLASS[STATE_TONE[node.state]],
                )}
              >
                {stateLabel(i18n.t, node.state)}
              </span>
              {node.on_hold ? (
                <span title={i18n.t("jobs.tree.held")} class="text-muted-foreground">
                  <CirclePause role="img" aria-label={i18n.t("jobs.tree.held")} class="size-4" />
                </span>
              ) : null}
              {node.recurrence ? (
                <span
                  title={i18n.t("jobs.tree.recurring")}
                  class="flex items-center gap-1 text-xs text-muted-foreground"
                >
                  <Repeat2 role="img" aria-label={i18n.t("jobs.tree.recurring")} class="size-4" />
                  {i18n.t(`chat.child.${node.recurrence.phase}`)}
                </span>
              ) : null}
              {node.origin ? (
                <span class="text-xs text-muted-foreground">
                  {i18n.t(`jobs.tree.origin.${node.origin}`)}
                </span>
              ) : null}
              <span class="text-xs text-muted-foreground">{agent}</span>
              {node.model ? (
                <span class="break-all text-xs text-muted-foreground">{node.model.model}</span>
              ) : null}
              {(node.kind === "conversation" || node.conversation !== null) && props.onOpenChat ? (
                <Button
                  type="button"
                  variant="outline"
                  size="xs"
                  class="shrink-0"
                  onClick={(event) => {
                    event.stopPropagation();
                    const target = node.kind === "conversation" ? node.id : node.conversation;
                    if (target !== null) props!.onOpenChat!(target);
                  }}
                >
                  {i18n.t("jobs.openInChat")}
                </Button>
              ) : null}
            </div>
          </div>
        </div>
        {node.depends_on.length > 0 ? (
          <div class="flex flex-wrap items-center gap-2 pl-7 text-xs">
            <span class="text-muted-foreground">{i18n.t("jobs.tree.waitsOn")}</span>
            {node.depends_on.map((dependency) => (
              <button
                type="button"
                onClick={() => props.onSelect(dependency.job)}
                class="flex max-w-full min-w-0 flex-wrap items-center gap-1 rounded px-1 hover:bg-muted focus-visible:outline-2 focus-visible:outline-ring"
              >
                <code class="break-all font-mono">{dependency.job}</code>
                <span
                  class={cn(
                    "rounded-full px-2 py-0.5 font-semibold tracking-widest uppercase",
                    TONE_CLASS[DEPENDENCY_TONE[dependency.state]],
                  )}
                >
                  {i18n.t(`jobs.tree.dependency.${dependency.state}`)}
                </span>
              </button>
            ))}
          </div>
        ) : null}
        {activeTurn ? (
          <div class="flex flex-wrap items-center gap-2 pl-7 text-xs">
            <span class="text-muted-foreground">{i18n.t("jobs.tree.running")}</span>
            <button
              type="button"
              onClick={() => props.onSelect(activeTurn)}
              class="max-w-full min-w-0 rounded px-1 hover:bg-muted focus-visible:outline-2 focus-visible:outline-ring"
            >
              <code class="break-all font-mono">{activeTurn}</code>
            </button>
          </div>
        ) : null}
        {isExpanded && node.children ? (
          <ul class="ml-3 border-l border-border pl-3">
            {node.children.map((child) => render(child))}
          </ul>
        ) : null}
        {isExpanded && nextAfter ? (
          <div class="pl-7">
            <Button
              type="button"
              variant="outline"
              size="xs"
              disabled={loading()}
              onClick={() => {
                void loadChildren(node, nextAfter).catch((cause: unknown) => {
                  setFailure(cause instanceof Error ? cause.message : String(cause));
                });
              }}
            >
              {i18n.t("jobs.tree.more")}
            </Button>
          </div>
        ) : null}
        {isRoot && !hasChildren ? (
          <p class="pl-7 text-sm text-muted-foreground">{i18n.t("jobs.tree.empty")}</p>
        ) : null}
      </li>
    );
  };

  return (
    <div class="flex flex-col gap-3">
      {ancestors().length > 0 ? (
        <nav
          aria-label={i18n.t("jobs.tree.ancestors")}
          class="flex flex-wrap items-center gap-1 text-xs"
        >
          {ancestors().map((ancestor) => (
            <button
              type="button"
              onClick={() => props.onReroot(ancestor)}
              class="max-w-full min-w-0 rounded px-1 hover:bg-muted focus-visible:outline-2 focus-visible:outline-ring"
            >
              <code class="break-all font-mono">{ancestor}</code>
            </button>
          ))}
        </nav>
      ) : null}
      {failure() ? (
        <DismissibleNotice message={failure()!} class="text-sm text-destructive">
          {failure()}
        </DismissibleNotice>
      ) : null}
      {loading() && !tree() ? (
        <p role="status" class="text-sm text-muted-foreground">
          {i18n.t("common.loading")}
        </p>
      ) : null}
      {tree() ? (
        <ul aria-label={i18n.t("jobs.tree.title")} class="flex flex-col">
          {render(tree()!, true)}
        </ul>
      ) : null}
    </div>
  );
}
