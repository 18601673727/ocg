"use client";

import { useEffect, useState } from "react";
import Image from "next/image";
import { Button } from "@/components/ui/button";
import { DiffViewer } from "@/components/ui/diff-viewer";
import type { ControlToolCall } from "../contracts";
import { useOcgRuntime } from "../runtime/runtime-context";
import { useI18n } from "../i18n";
import { fileArtifact } from "./artifact-model";

export type Artifact = { id: string; title: string } & (
  | { job: string; call: ControlToolCall }
  | { url: string }
);

export function ArtifactPreview({ artifact }: { artifact: Artifact }) {
  const { control } = useOcgRuntime();
  const { t } = useI18n();
  const [call, setCall] = useState<ControlToolCall | null>(
    "call" in artifact ? artifact.call : null,
  );
  const [error, setError] = useState<string | null>(null);
  const [attempt, setAttempt] = useState(0);
  const needsRead = "call" in artifact && (artifact.call.arguments_cut || artifact.call.result_cut);
  useEffect(() => {
    if (!needsRead || !("call" in artifact) || !control) return;
    const controller = new AbortController();
    void control
      .readJobTool(artifact.job, artifact.call.request, artifact.call.index, controller.signal)
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
  }, [artifact, control, needsRead, attempt]);
  if ("url" in artifact)
    return (
      <div className="min-w-0">
        <Image
          src={artifact.url}
          alt={artifact.title || t("chat.artifacts.image")}
          unoptimized
          width={1024}
          height={768}
          className="mx-auto h-auto w-auto max-h-[65dvh] max-w-full object-contain"
        />
      </div>
    );
  if (error)
    return (
      <div role="alert" className="space-y-2 text-sm">
        <p className="text-destructive">{error}</p>
        <Button
          variant="outline"
          onClick={() => {
            setError(null);
            setAttempt((value) => value + 1);
          }}
        >
          {t("common.retry")}
        </Button>
      </div>
    );
  if (needsRead && (call?.arguments_cut || call?.result_cut))
    return <p role="status">{t("common.loading")}</p>;
  const file = call ? fileArtifact(call) : null;
  if (!file) return <p>{t("chat.artifacts.unavailable")}</p>;
  if (file.patch !== undefined)
    return (
      <div className="min-w-0">
        {file.truncated && (
          <p className="mb-2 text-xs text-muted-foreground">{t("chat.artifacts.truncated")}</p>
        )}
        <DiffViewer
          patch={file.patch}
          viewMode="split"
          size="sm"
          className="max-w-full rounded-md"
        />
      </div>
    );
  return (
    <div className="min-w-0">
      {file.excerpt && (
        <p className="mb-2 text-xs text-muted-foreground">{t("chat.artifacts.excerpt")}</p>
      )}
      {file.before !== undefined && file.after !== undefined ? (
        <DiffViewer
          oldFile={{ content: file.before, name: file.path }}
          newFile={{ content: file.after, name: file.path }}
          viewMode="split"
          size="sm"
          showLineNumbers={!file.excerpt}
          className="max-w-full rounded-md"
        />
      ) : (
        <>
          <p className="mb-2 break-all font-mono text-xs text-muted-foreground">{file.path}</p>
          <div className="grid min-w-0 gap-3 sm:grid-cols-2">
            <section className="min-w-0">
              <h3 className="mb-2 text-sm font-medium">{t("chat.artifacts.before")}</h3>
              <pre className="ocg-subtle-scrollbar max-w-full overflow-x-auto rounded-md border border-border p-3 text-xs text-destructive">
                {file.before ?? t("chat.artifacts.beforeUnavailable")}
              </pre>
            </section>
            <section className="min-w-0">
              <h3 className="mb-2 text-sm font-medium">{t("chat.artifacts.after")}</h3>
              <pre className="ocg-subtle-scrollbar max-w-full overflow-x-auto rounded-md border border-border p-3 text-xs text-diff-added">
                {file.after}
              </pre>
            </section>
          </div>
        </>
      )}
    </div>
  );
}
