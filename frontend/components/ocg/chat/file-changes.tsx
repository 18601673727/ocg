"use client";

import { useEffect, useMemo, useState } from "react";
import { FileDiff } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import type { ControlToolCall } from "../contracts";
import { useOcgRuntime } from "../runtime/runtime-context";
import { useI18n } from "../i18n";
import { artifactTitle, changedLines, fileArtifact } from "./artifact-model";
import { ArtifactPreview } from "./artifact-preview";

export type FileChangeCall = { job: string; call: ControlToolCall };

export function FileChanges({ changes }: { changes: readonly FileChangeCall[] }) {
  const { t } = useI18n();
  const { control } = useOcgRuntime();
  const [details, setDetails] = useState<Record<string, ControlToolCall | null>>({});
  useEffect(() => {
    if (!control) return;
    const pending = changes.filter(
      ({ job, call }) =>
        call.name === "workspace_write_file" &&
        call.outcome_kind === "completed" &&
        (call.arguments_cut || call.result_cut) &&
        !(`${job}:${call.request}:${call.index}` in details),
    );
    if (!pending.length) return;
    const controller = new AbortController();
    let next = 0;
    const loaded: Record<string, ControlToolCall | null> = {};
    const read = async () => {
      while (next < pending.length && !controller.signal.aborted) {
        const { job, call } = pending[next++],
          key = `${job}:${call.request}:${call.index}`;
        try {
          loaded[key] = await control.readJobTool(job, call.request, call.index, controller.signal);
        } catch {
          if (!controller.signal.aborted) loaded[key] = null;
        }
      }
    };
    void Promise.all(Array.from({ length: Math.min(4, pending.length) }, read)).then(() => {
      if (!controller.signal.aborted) setDetails((previous) => ({ ...previous, ...loaded }));
    });
    return () => controller.abort();
  }, [control, changes, details]);
  const complete = useMemo(
    () =>
      changes.map(({ job, call }) => ({
        job,
        call: details[`${job}:${call.request}:${call.index}`] ?? call,
      })),
    [changes, details],
  );
  const unknown = complete.filter(
    ({ call }) =>
      call.name === "workspace_write_file" &&
      call.outcome_kind === "completed" &&
      artifactTitle(call) === call.name,
  ).length;

  const [selected, setSelected] = useState<string | null>(null);
  const files = useMemo(() => {
    const grouped = new Map<
      string,
      { path: string; calls: FileChangeCall[]; added: number; removed: number; known: boolean }
    >();
    const seen = new Set<string>();
    for (const { job, call } of complete) {
      if (call.name !== "workspace_write_file" || call.outcome_kind !== "completed") continue;
      const key = `${job}:${call.request}:${call.index}`;
      if (seen.has(key)) continue;
      seen.add(key);
      const path = artifactTitle(call);
      if (path === call.name) continue;
      const file = grouped.get(path) ?? { path, calls: [], added: 0, removed: 0, known: true };
      const artifact = fileArtifact(call);
      const counts =
        artifact?.before !== undefined && artifact.after !== undefined
          ? changedLines(artifact.before, artifact.after)
          : null;
      file.calls.push({ job, call });
      if (counts) {
        file.added += counts.added;
        file.removed += counts.removed;
      } else file.known = false;
      grouped.set(path, file);
    }
    return [...grouped.values()];
  }, [complete]);
  if (!files.length && !unknown) return null;
  const total = files.reduce(
    (sum, file) => ({
      added: sum.added + file.added,
      removed: sum.removed + file.removed,
      known: sum.known && file.known,
    }),
    { added: 0, removed: 0, known: true },
  );
  const current = files.find((file) => file.path === selected);
  const counts = (value: { added: number; removed: number; known: boolean }) => (
    <span
      className="shrink-0 whitespace-nowrap text-sm tabular-nums"
      title={!value.known ? t("chat.changes.unknownCounts") : undefined}
    >
      <span className="text-diff-added">+{value.known ? value.added : "?"}</span>{" "}
      <span className="text-destructive">-{value.known ? value.removed : "?"}</span>
    </span>
  );
  return (
    <section
      className="my-3 overflow-hidden rounded-xl border border-border bg-card text-card-foreground"
      data-slot="file-changes"
    >
      <div className="flex items-center gap-2.5 border-b border-border px-3 py-3">
        <span className="rounded-xl bg-muted p-2.5 text-muted-foreground">
          <FileDiff className="size-5" aria-hidden="true" />
        </span>
        <div className="min-w-0 flex-1">
          <p className="text-sm font-medium leading-5">
            {files.length
              ? t("chat.changes.files", { count: files.length })
              : t("chat.changes.title")}
          </p>
          {counts(total)}
        </div>
        <Button
          className="rounded-xl border-border bg-muted/50 text-card-foreground hover:bg-accent"
          size="sm"
          variant="outline"
          disabled={!files.length}
          onClick={() => setSelected(files[0].path)}
        >
          {t("chat.changes.view")}
        </Button>
      </div>
      {unknown > 0 && (
        <p role="status" className="px-4 py-2 text-xs text-muted-foreground">
          {t("chat.changes.unknownFiles", { count: unknown })}
        </p>
      )}
      <ul className="bg-background/50">
        {files.map((file) => (
          <li key={file.path}>
            <button
              type="button"
              className="flex w-full items-center justify-between gap-3 px-3 py-2 text-left text-sm hover:bg-accent focus-visible:outline-ring"
              onClick={() => setSelected(file.path)}
            >
              <span className="min-w-0 truncate">
                <span className="text-muted-foreground">
                  {file.path.slice(0, file.path.lastIndexOf("/") + 1)}
                </span>
                <span>{file.path.slice(file.path.lastIndexOf("/") + 1)}</span>
              </span>
              {counts(file)}
            </button>
          </li>
        ))}
      </ul>
      <Dialog
        open={current !== undefined}
        onOpenChange={(open) => {
          if (!open) setSelected(null);
        }}
      >
        <DialogContent className="flex max-h-[85dvh] min-w-0 flex-col sm:max-w-5xl">
          <DialogHeader className="pr-6">
            <DialogTitle className="break-all font-mono text-sm normal-case tracking-normal">
              {current?.path}
            </DialogTitle>
            <DialogDescription>{t("chat.changes.note")}</DialogDescription>
          </DialogHeader>
          <div className="ocg-subtle-scrollbar min-h-0 min-w-0 space-y-4 overflow-y-auto">
            {current?.calls.map(({ job, call }) => (
              <section key={`${job}:${call.request}:${call.index}`} className="min-w-0">
                <ArtifactPreview
                  artifact={{
                    id: `${job}:${call.request}:${call.index}`,
                    title: current.path,
                    job,
                    call,
                  }}
                />
              </section>
            ))}
          </div>
        </DialogContent>
      </Dialog>
    </section>
  );
}
