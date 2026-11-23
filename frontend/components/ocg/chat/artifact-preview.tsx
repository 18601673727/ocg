import { createEffect, createMemo, createSignal } from "solid-js";
import { Button } from "@/components/ui/button";
import { DiffViewer } from "@/components/ui/diff-viewer";
import type { ToolCall } from "../contracts";
import { useI18n } from "../i18n";
import { useOcgRuntime } from "../runtime/runtime-context";
import { artifactHunks, fileArtifact } from "./artifact-model";

export type Artifact = { id: string; title: string } & (
  | { job: string; call: ToolCall }
  | { url: string }
);

export function ArtifactPreview(props: { artifact: Artifact }) {
  const { api } = useOcgRuntime();
  const i18n = useI18n();
  const [call, setCall] = createSignal<ToolCall | null>(
    "call" in props.artifact ? props.artifact.call : null,
  );
  const [error, setError] = createSignal<string | null>(null);
  const [attempt, setAttempt] = createSignal(0);
  const needsRead = createMemo(
    () =>
      "call" in props.artifact &&
      (props.artifact.call.arguments_cut || props.artifact.call.result_cut),
  );
  createEffect(
    () => [props.artifact, api, needsRead(), attempt()],
    () => {
      if (!needsRead() || !("call" in props.artifact) || !api) return;
      const controller = new AbortController();
      void api
        .readJobTool(
          props.artifact.job,
          props.artifact.call.request,
          props.artifact.call.index,
          controller.signal,
        )
        .then((value) => {
          if (!controller.signal.aborted) {
            setCall(value);
            setError(null);
          }
        })
        .catch((cause) => {
          if (!controller.signal.aborted)
            setError(cause instanceof Error ? cause.message : String(cause));
        });
      return () => controller.abort();
    },
  );
  return (
    <>
      {(() => {
        if ("url" in props.artifact)
          return (
            <div class="min-w-0">
              <img
                src={props.artifact.url}
                alt={props.artifact.title || i18n.t("chat.artifacts.image")}
                loading="lazy"
                decoding="async"
                width={1024}
                height={768}
                class="mx-auto h-auto w-auto max-h-[65dvh] max-w-full object-contain"
              />
            </div>
          );
        if (error())
          return (
            <div role="alert" class="space-y-2 text-sm">
              <p class="text-destructive">{error()}</p>
              <Button
                variant="outline"
                onClick={() => {
                  setError(null);
                  setAttempt((value) => value + 1);
                }}
              >
                {i18n.t("common.retry")}
              </Button>
            </div>
          );
        if (needsRead() && (call()?.arguments_cut || call()?.result_cut))
          return <p role="status">{i18n.t("common.loading")}</p>;
        const file = createMemo(() => (call() ? fileArtifact(call()!) : null));
        const hunks = createMemo(() => artifactHunks(file()));
        if (!file()) return <p>{i18n.t("chat.artifacts.unavailable")}</p>;
        if (file()!.patch !== undefined)
          return (
            <div class="min-w-0">
              {file()!.truncated && (
                <p class="mb-2 text-xs text-muted-foreground">
                  {i18n.t("chat.artifacts.truncated")}
                </p>
              )}
              <DiffViewer
                patch={file()!.patch}
                viewMode="split"
                size="sm"
                class="max-w-full rounded-md"
              />
            </div>
          );
        return (
          <div class="min-w-0">
            {file()!.excerpt && (
              <p class="mb-2 text-xs text-muted-foreground">{i18n.t("chat.artifacts.excerpt")}</p>
            )}
            {hunks().length ? (
              <div class="space-y-3">
                {hunks().map((hunk) => (
                  <DiffViewer
                    oldFile={{ content: hunk.before, name: file()!.path }}
                    newFile={{ content: hunk.after, name: file()!.path }}
                    viewMode="split"
                    size="sm"
                    showLineNumbers={!file()!.excerpt}
                    class="max-w-full rounded-md"
                  />
                ))}
              </div>
            ) : (
              <>
                <p class="mb-2 break-all font-mono text-xs text-muted-foreground">{file()!.path}</p>
                <div class="grid min-w-0 gap-3 sm:grid-cols-2">
                  <section class="min-w-0">
                    <h3 class="mb-2 text-sm font-medium">{i18n.t("chat.artifacts.before")}</h3>
                    <pre class="ocg-subtle-scrollbar max-w-full overflow-x-auto rounded-md border border-border p-3 text-xs text-destructive">
                      {file()!.before ?? i18n.t("chat.artifacts.beforeUnavailable")}
                    </pre>
                  </section>
                  <section class="min-w-0">
                    <h3 class="mb-2 text-sm font-medium">{i18n.t("chat.artifacts.after")}</h3>
                    <pre class="ocg-subtle-scrollbar max-w-full overflow-x-auto rounded-md border border-border p-3 text-xs text-diff-added">
                      {file()!.after}
                    </pre>
                  </section>
                </div>
              </>
            )}
          </div>
        );
      })()}
    </>
  );
}
