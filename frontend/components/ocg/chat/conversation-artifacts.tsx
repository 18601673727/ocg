import { FileCode2, ImageIcon } from "@lucide/solid";
import { createMemo, createSignal } from "solid-js";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { useI18n } from "../i18n";
import { useOcgRuntimeSnapshot } from "../runtime/runtime-context";
import { artifactTitle, isArtifactCall, managedArtifactImages } from "./artifact-model";
import { type Artifact, ArtifactPreview } from "./artifact-preview";

export function ConversationArtifacts(props: { sessionId: string }) {
  const messages = useOcgRuntimeSnapshot((snapshot) => snapshot.messagesBySession[props.sessionId]);
  const i18n = useI18n();
  const [selected, setSelected] = createSignal<Artifact | null>(null);
  const artifacts = createMemo(() => {
    const found = new Map<string, Artifact>();
    for (const message of messages() ?? []) {
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
  });
  const remaining = createMemo(() =>
    artifacts().filter(
      (artifact) => !("call" in artifact) || artifact.call.name !== "workspace_write_file",
    ),
  );
  return (
    <>
      {(() => {
        if (!remaining().length) return null;
        return (
          <>
            {remaining().length > 0 && (
              <section
                class="my-3 rounded-lg border border-border p-3"
                data-slot="conversation-artifacts"
              >
                <details>
                  <summary class="cursor-pointer text-sm font-medium">
                    {i18n.t("chat.artifacts.title", { count: remaining().length })}
                  </summary>
                  <ul class="mt-2 grid gap-1">
                    {remaining().map((artifact) => (
                      <li>
                        <Button
                          variant="ghost"
                          class="h-auto max-w-full justify-start py-2 text-left normal-case tracking-normal"
                          onClick={() => setSelected(artifact)}
                        >
                          {"call" in artifact ? (
                            <FileCode2 aria-hidden="true" />
                          ) : (
                            <ImageIcon aria-hidden="true" />
                          )}
                          <span class="min-w-0 truncate">
                            {artifact.title || i18n.t("chat.artifacts.image")}
                          </span>
                        </Button>
                      </li>
                    ))}
                  </ul>
                </details>
                <Dialog
                  open={selected() !== null}
                  onOpenChange={(open) => {
                    if (!open) setSelected(null);
                  }}
                >
                  <DialogContent class="flex max-h-[85dvh] min-w-0 flex-col sm:max-w-5xl">
                    <DialogHeader class="min-w-0 pr-6">
                      <DialogTitle class="break-all normal-case tracking-normal">
                        {selected()?.title || i18n.t("chat.artifacts.image")}
                      </DialogTitle>
                      <DialogDescription>{i18n.t("chat.artifacts.note")}</DialogDescription>
                    </DialogHeader>
                    {selected() && (
                      <div class="ocg-subtle-scrollbar min-h-0 min-w-0 overflow-y-auto">
                        <ArtifactPreview artifact={selected()!} />
                      </div>
                    )}
                  </DialogContent>
                </Dialog>
              </section>
            )}
          </>
        );
      })()}
    </>
  );
}
