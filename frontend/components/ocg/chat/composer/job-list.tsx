import { createMemo, For, Show } from "solid-js";
import { Button } from "@/components/ui/button";
import type { ComposerDraft } from "./draft-state";

export default function ComposerJobList(props: { draft: ComposerDraft }) {
  const {
    confirmed,
    unfinished,
    busy,
    uncertain,
    conflict,
    editingQueue,
    openUnfinished,
    hasQueued,
    queued,
    queuedLoading,
    inputDisabled,
    editQueued,
    withdraw,
    cancelUnsupported,
    attachments,
  } = props.draft;
  const { pendingImages, uploading } = attachments;
  const others = createMemo(() => unfinished().filter((job) => job.id !== confirmed()?.id));
  return (
    <>
      {others().length > 0 && (
        <div role="group" aria-label="Unsubmitted Jobs" class="space-y-1 text-sm">
          {others().map((job) => (
            <div class="flex items-center gap-2">
              <span class="min-w-0 flex-1 truncate">
                Unsubmitted: {job.input.text || "Image Job"}
              </span>
              <Button
                type="button"
                size="xs"
                variant="outline"
                disabled={busy() || uncertain() || conflict() || uploading() > 0 || editingQueue()}
                onClick={() => void openUnfinished(job)}
              >
                Open
              </Button>
            </div>
          ))}
        </div>
      )}
      <Show when={hasQueued()}>
        <div role="group" aria-label="Queued Jobs" class="space-y-1 text-sm">
          <For each={queued()} keyed={(item) => item.id}>
            {(row) => (
              <div class="flex items-center gap-2">
                <span
                  class="min-w-0 flex-1 truncate"
                  title={
                    row().autonomous_loopback_eligibility &&
                    !row().autonomous_loopback_eligibility!.eligible
                      ? `Not eligible for automatic dispatch (${row().autonomous_loopback_eligibility!.code}): ${row().autonomous_loopback_eligibility!.detail}`
                      : undefined
                  }
                >
                  {row().unsupported_decision_history ? "Blocked by Decision history" : row().phase}
                  {row().autonomous_loopback_eligibility &&
                  !row().autonomous_loopback_eligibility!.eligible
                    ? ` (not schedulable: ${row().autonomous_loopback_eligibility!.code})`
                    : ""}
                  : {row().input.text || "Image Job"}
                </span>
                <Button
                  type="button"
                  size="xs"
                  variant="outline"
                  disabled={
                    row().phase !== "queued" ||
                    queuedLoading() ||
                    inputDisabled() ||
                    pendingImages().length > 0 ||
                    editingQueue()
                  }
                  onClick={() => void editQueued(row())}
                >
                  Edit
                </Button>
                <Button
                  type="button"
                  size="xs"
                  variant="outline"
                  disabled={busy() || queuedLoading() || uncertain() || editingQueue()}
                  onClick={() => void withdraw(row())}
                >
                  Withdraw
                </Button>
                {row().unsupported_decision_history && (
                  <Button
                    type="button"
                    size="xs"
                    variant="outline"
                    disabled={
                      row().phase !== "queued" ||
                      queuedLoading() ||
                      busy() ||
                      uncertain() ||
                      editingQueue()
                    }
                    onClick={() => void cancelUnsupported(row())}
                  >
                    Cancel blocked Job
                  </Button>
                )}
              </div>
            )}
          </For>
        </div>
      </Show>
    </>
  );
}
