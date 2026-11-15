"use client";

import { useMemo, useState } from "react";
import { FileCode2, ImageIcon } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { useOcgRuntime } from "../runtime/runtime-context";
import { useI18n } from "../i18n";
import { artifactTitle, isArtifactCall, managedArtifactImages } from "./artifact-model";
import { ArtifactPreview, type Artifact } from "./artifact-preview";

export function ConversationArtifacts({ sessionId }: { sessionId: string }) {
  const { snapshot } = useOcgRuntime();
  const { t } = useI18n();
  const [selected, setSelected] = useState<Artifact | null>(null);
  const artifacts = useMemo(() => {
    const found = new Map<string, Artifact>();
    for (const message of snapshot.messagesBySession[sessionId] ?? []) {
      if (message.role !== "assistant") continue;
      for (const call of message.toolCalls ?? []) {
        if (!message.jobId || !isArtifactCall(call)) continue;
        const id = `${message.jobId}:${call.request}:${call.index}`;
        found.set(id, { id, title: artifactTitle(call), job: message.jobId, call });
      }
      for (const image of [
        ...(message.images ?? []).map((image) => ({ name: image.name, url: image.url })),
        ...managedArtifactImages(message.content),
      ]) {
        if (!/^\/api\/v1\/attachments\/[0-9a-f-]+\/content$/.test(image.url)) continue;
        found.set(image.url, { id: image.url, title: image.name, url: image.url });
      }
    }
    return [...found.values()].reverse();
  }, [snapshot.messagesBySession, sessionId]);
  const remaining = artifacts.filter(
    (artifact) => !("call" in artifact) || artifact.call.name !== "workspace_write_file",
  );
  if (!remaining.length) return null;
  return (
    <>
      {remaining.length > 0 && (
        <section
          className="my-3 rounded-lg border border-border p-3"
          data-slot="conversation-artifacts"
        >
          <details>
            <summary className="cursor-pointer text-sm font-medium">
              {t("chat.artifacts.title", { count: remaining.length })}
            </summary>
            <ul className="mt-2 grid gap-1">
              {remaining.map((artifact) => (
                <li key={artifact.id}>
                  <Button
                    variant="ghost"
                    className="h-auto max-w-full justify-start py-2 text-left normal-case tracking-normal"
                    onClick={() => setSelected(artifact)}
                  >
                    {"call" in artifact ? (
                      <FileCode2 aria-hidden="true" />
                    ) : (
                      <ImageIcon aria-hidden="true" />
                    )}
                    <span className="min-w-0 truncate">
                      {artifact.title || t("chat.artifacts.image")}
                    </span>
                  </Button>
                </li>
              ))}
            </ul>
          </details>
          <Dialog
            open={selected !== null}
            onOpenChange={(open) => {
              if (!open) setSelected(null);
            }}
          >
            <DialogContent className="flex max-h-[85dvh] min-w-0 flex-col sm:max-w-5xl">
              <DialogHeader className="min-w-0 pr-6">
                <DialogTitle className="break-all normal-case tracking-normal">
                  {selected?.title || t("chat.artifacts.image")}
                </DialogTitle>
                <DialogDescription>{t("chat.artifacts.note")}</DialogDescription>
              </DialogHeader>
              {selected && (
                <div className="ocg-subtle-scrollbar min-h-0 min-w-0 overflow-y-auto">
                  <ArtifactPreview key={selected.id} artifact={selected} />
                </div>
              )}
            </DialogContent>
          </Dialog>
        </section>
      )}
    </>
  );
}
