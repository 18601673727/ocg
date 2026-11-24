import { FileDiff } from "@lucide/solid";
import {
  createContext,
  createEffect,
  createMemo,
  createSignal,
  For,
  Show,
  useContext,
} from "solid-js";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import type { View } from "@/lib/ui-state";
import type { ToolCall } from "../contracts";
import { useI18n } from "../i18n";
import { useOcgRuntime } from "../runtime/runtime-context";
import { artifactChangedLines, artifactTitle, fileArtifact } from "./artifact-model";
import { ArtifactPreview } from "./artifact-preview";

export type FileChangeCall = { job: string; call: ToolCall };
type ChangesPreview = { title: string; calls: readonly FileChangeCall[] };
const ChangesPreviewContext = createContext<(preview: ChangesPreview) => void>();

export function FileChangesProvider(props: { children: View }) {
  const i18n = useI18n();
  const [selected, setSelected] = createSignal<ChangesPreview | null>(null);
  return (
    <ChangesPreviewContext value={(preview) => setSelected(preview)}>
      {props.children}
      <Dialog
        open={selected() !== null}
        onOpenChange={(open) => {
          if (!open) setSelected(null);
        }}
      >
        <DialogContent class="flex max-h-[85dvh] min-w-0 flex-col sm:max-w-5xl">
          <DialogHeader class="pr-6">
            <DialogTitle class="break-all font-mono text-sm normal-case tracking-normal">
              {selected()?.title}
            </DialogTitle>
            <DialogDescription>{i18n.t("chat.changes.note")}</DialogDescription>
          </DialogHeader>
          <div
            data-slot="file-changes-preview"
            class="ocg-subtle-scrollbar min-h-0 min-w-0 space-y-4 overflow-y-auto"
          >
            {selected()?.calls.map(({ job, call }) => (
              <section class="min-w-0">
                <ArtifactPreview
                  artifact={{
                    id: `${job}:${call.request}:${call.index}`,
                    title: artifactTitle(call),
                    job,
                    call,
                  }}
                />
              </section>
            ))}
          </div>
        </DialogContent>
      </Dialog>
    </ChangesPreviewContext>
  );
}

export function FileChanges(props: { changes: readonly FileChangeCall[] }) {
  const i18n = useI18n();
  const { api } = useOcgRuntime();
  const openPreview = useContext(ChangesPreviewContext);
  if (!openPreview) throw new Error("File changes provider is missing");
  const changes = createMemo(
    () =>
      props.changes.filter(
        ({ call }) => call.name === "workspace_write_file" && call.outcome_kind === "completed",
      ),
    {
      equals: (left, right) =>
        left.length === right.length &&
        left.every(({ job, call }, index) => {
          const other = right[index];
          return (
            job === other.job &&
            call.request === other.call.request &&
            call.index === other.call.index &&
            call.arguments === other.call.arguments &&
            call.result === other.call.result &&
            call.arguments_cut === other.call.arguments_cut &&
            call.result_cut === other.call.result_cut
          );
        }),
    },
  );
  const [details, setDetails] = createSignal<Record<string, ToolCall | null>>({});
  createEffect(
    () => [api, changes()],
    () => {
      if (!api) return;
      const pending = changes().filter(
        ({ job, call }) =>
          call.name === "workspace_write_file" &&
          call.outcome_kind === "completed" &&
          (call.arguments_cut || call.result_cut) &&
          !(`${job}:${call.request}:${call.index}` in details()),
      );
      if (!pending.length) return;
      const controller = new AbortController();
      let next = 0;
      const loaded: Record<string, ToolCall | null> = {};
      const read = async () => {
        while (next < pending.length && !controller.signal.aborted) {
          const { job, call } = pending[next++],
            key = `${job}:${call.request}:${call.index}`;
          try {
            const value = await api.readJobTool(job, call.request, call.index, controller.signal);
            if (!controller.signal.aborted) {
              loaded[key] = value;
              setDetails((previous) => ({ ...previous, [key]: value }));
            }
          } catch {
            if (!controller.signal.aborted) loaded[key] = null;
          }
        }
      };
      void Promise.all(Array.from({ length: Math.min(4, pending.length) }, read)).then(() => {
        if (!controller.signal.aborted) setDetails((previous) => ({ ...previous, ...loaded }));
      });
      return () => controller.abort();
    },
  );
  const complete = createMemo(() =>
    changes().map(({ job, call }) => ({
      job,
      call: details()[`${job}:${call.request}:${call.index}`] ?? call,
    })),
  );
  const unknown = createMemo(
    () =>
      complete().filter(
        ({ call }) =>
          call.name === "workspace_write_file" &&
          call.outcome_kind === "completed" &&
          artifactTitle(call) === call.name,
      ).length,
  );

  const files = createMemo(() => {
    const grouped = new Map<
      string,
      { path: string; calls: FileChangeCall[]; added: number; removed: number; known: boolean }
    >();
    const seen = new Set<string>();
    for (const { job, call } of complete()) {
      if (call.name !== "workspace_write_file" || call.outcome_kind !== "completed") continue;
      const key = `${job}:${call.request}:${call.index}`;
      if (seen.has(key)) continue;
      seen.add(key);
      const path = artifactTitle(call);
      if (path === call.name) continue;
      const file = grouped.get(path) ?? { path, calls: [], added: 0, removed: 0, known: true };
      const artifact = fileArtifact(call);
      const counts = artifact ? artifactChangedLines(artifact) : null;
      file.calls.push({ job, call });
      if (counts) {
        file.added += counts.added;
        file.removed += counts.removed;
      } else file.known = false;
      grouped.set(path, file);
    }
    return [...grouped.values()];
  });
  const total = createMemo(() =>
    files().reduce(
      (sum, file) => ({
        added: sum.added + file.added,
        removed: sum.removed + file.removed,
        known: sum.known && file.known,
      }),
      { added: 0, removed: 0, known: true },
    ),
  );
  const counts = (value: { added: number; removed: number; known: boolean }) => (
    <span
      class="shrink-0 whitespace-nowrap text-sm tabular-nums"
      title={!value.known ? i18n.t("chat.changes.unknownCounts") : undefined}
    >
      <span class="text-diff-added">+{value.known ? value.added : "?"}</span>{" "}
      <span class="text-destructive">-{value.known ? value.removed : "?"}</span>
    </span>
  );
  return (
    <Show when={files().length > 0 || unknown() > 0}>
      <section
        class="my-3 overflow-hidden rounded-xl border border-border bg-card text-card-foreground"
        data-slot="file-changes"
      >
        <div class="flex items-center gap-2.5 border-b border-border px-3 py-3">
          <span class="rounded-xl bg-muted p-2.5 text-muted-foreground">
            <FileDiff class="size-5" aria-hidden="true" />
          </span>
          <div class="min-w-0 flex-1">
            <p class="text-sm font-medium leading-5">
              {files().length
                ? i18n.t("chat.changes.files", { count: files().length })
                : i18n.t("chat.changes.title")}
            </p>
            {counts(total())}
          </div>
          <Button
            class="rounded-xl border-border bg-muted/50 text-card-foreground hover:bg-accent"
            size="sm"
            variant="outline"
            disabled={!complete().length}
            onClick={() =>
              openPreview({
                title: i18n.t("chat.changes.files", { count: files().length }),
                calls: complete(),
              })
            }
          >
            {i18n.t("chat.changes.view")}
          </Button>
        </div>
        {unknown() > 0 && (
          <p role="status" class="px-4 py-2 text-xs text-muted-foreground">
            {i18n.t("chat.changes.unknownFiles", { count: unknown() })}
          </p>
        )}
        <ul class="bg-background/50">
          <For each={files()} keyed={(file) => file.path}>
            {(file) => (
              <li>
                <button
                  type="button"
                  class="flex w-full items-center justify-between gap-3 px-3 py-2 text-left text-sm hover:bg-accent focus-visible:outline-ring"
                  onClick={() => openPreview({ title: file().path, calls: file().calls })}
                >
                  <span class="min-w-0 truncate">
                    <span class="text-muted-foreground">
                      {file().path.slice(0, file().path.lastIndexOf("/") + 1)}
                    </span>
                    <span>{file().path.slice(file().path.lastIndexOf("/") + 1)}</span>
                  </span>
                  {counts(file())}
                </button>
              </li>
            )}
          </For>
        </ul>
      </section>
    </Show>
  );
}
