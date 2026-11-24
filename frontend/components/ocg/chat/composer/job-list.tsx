import { createMemo, For, Show } from "solid-js";
import { Button } from "@/components/ui/button";
import { useI18n } from "../../i18n";
import type { MutableJob } from "../../runtime/api-client";
import type { ComposerDraft } from "./draft-state";

export default function ComposerJobList(props: { draft: ComposerDraft }) {
  const { t } = useI18n();
  const phaseLabel = (phase: MutableJob["phase"]) =>
    t(phase === "executing" ? "job.phase.running" : `job.phase.${phase}`);
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
        <div role="group" aria-label={t("chat.draft.unsubmittedJobs")} class="space-y-1 text-sm">
          {others().map((job) => (
            <div class="flex items-center gap-2">
              <span class="min-w-0 flex-1 truncate">
                {t("chat.draft.unsubmitted", { text: job.input.text || t("chat.draft.imageJob") })}
              </span>
              <Button
                type="button"
                size="xs"
                variant="outline"
                disabled={busy() || uncertain() || conflict() || uploading() > 0 || editingQueue()}
                onClick={() => void openUnfinished(job)}
              >
                {t("chat.draft.open")}
              </Button>
            </div>
          ))}
        </div>
      )}
      <Show when={hasQueued()}>
        <div role="group" aria-label={t("chat.draft.queuedJobs")} class="space-y-1 text-sm">
          <For each={queued()} keyed={(item) => item.id}>
            {(row) => (
              <div class="flex items-center gap-2">
                <span
                  class="min-w-0 flex-1 truncate"
                  title={
                    row().autonomous_loopback_eligibility &&
                    !row().autonomous_loopback_eligibility!.eligible
                      ? t("chat.draft.ineligible", {
                          code: row().autonomous_loopback_eligibility!.code,
                          detail: row().autonomous_loopback_eligibility!.detail,
                        })
                      : undefined
                  }
                >
                  {row().unsupported_decision_history
                    ? t("chat.draft.blockedDecision")
                    : phaseLabel(row().phase)}
                  {row().autonomous_loopback_eligibility &&
                  !row().autonomous_loopback_eligibility!.eligible
                    ? t("chat.draft.notSchedulable", {
                        code: row().autonomous_loopback_eligibility!.code,
                      })
                    : ""}
                  : {row().input.text || t("chat.draft.imageJob")}
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
                  {t("common.edit")}
                </Button>
                <Button
                  type="button"
                  size="xs"
                  variant="outline"
                  disabled={busy() || queuedLoading() || uncertain() || editingQueue()}
                  onClick={() => void withdraw(row())}
                >
                  {t("chat.draft.withdraw")}
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
                    {t("chat.draft.cancelBlocked")}
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
