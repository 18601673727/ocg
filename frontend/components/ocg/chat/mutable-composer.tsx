import { createMemo, createSignal, For, Loading, lazy, Show } from "solid-js";
import {
  Composer,
  ComposerActions,
  ComposerAttachButton,
  ComposerAttachments,
  ComposerBar,
  ComposerSend,
  ComposerToolbar,
} from "@/components/ocg/chat/ui/composer";
import { Button } from "@/components/ui/button";
import { bindElementRef, elementRef } from "@/lib/ui-state";
import { useI18n } from "../i18n";
import { useOcgRuntime, useOcgRuntimeSnapshot } from "../runtime/runtime-context";
import { IMAGE_TYPES, imageFiles } from "./composer/attachments";
import { ComposerSelectors } from "./composer/composer-selectors";
import { type ComposerProps, createComposerDraft } from "./composer/draft-state";
import { useConversationView } from "./conversation-view";

export { bindComposerContext, contextForSession, MUTABLE_CONTEXT_KEY } from "./composer/context";
export { confirmMutableComposerNavigation } from "./composer/draft-state";

const AttachmentTiles = lazy(() => import("./composer/attachment-tiles"));
const ComposerJobList = lazy(() => import("./composer/job-list"));

export function MutableComposer(props: ComposerProps) {
  const draft = createComposerDraft(props);
  const {
    input,
    confirmed,
    saving,
    error,
    busy,
    uncertain,
    conflict,
    ready,
    inputDisabled,
    sendDisabled,
    images,
    attachments,
    canAttach,
    unfinished,
    editingQueue,
    queueError,
    hasQueued,
    refreshQueue,
    refreshUnfinished,
    changeText,
    send,
    reconcileSubmission,
    loadSaved,
    flush,
  } = draft;
  const { pendingImages, uploading, selectImages, retryUploads, paste } = attachments;
  const i18n = useI18n();
  const { t } = i18n;
  const { api } = useOcgRuntime();
  const { reportSubmissionNotice } = useConversationView();
  const [stopping, setStopping] = createSignal(false);
  const conversation = useOcgRuntimeSnapshot(
    (snapshot) =>
      snapshot.sessions.find((session) => session.id === props.sessionId)?.conversationId,
  );
  const active = useOcgRuntimeSnapshot((snapshot) =>
    snapshot.messagesBySession[props.sessionId]?.findLast(({ execution }) =>
      execution
        ? execution.holdsCapacity ||
          execution.phase === "queued" ||
          execution.phase === "reserved" ||
          execution.phase === "running" ||
          execution.phase === "stop_requested"
        : false,
    ),
  );
  const actionMode = createMemo(() => {
    if (!ready() || busy() || stopping() || active()?.execution?.phase === "stop_requested")
      return "loading";
    if (active() && !editingQueue()) return "stop";
    if (saving() || uploading() > 0) return "loading";
    return "send";
  });
  const actionLabel = createMemo(() =>
    actionMode() === "stop"
      ? i18n.t("aui.stopGenerating")
      : actionMode() === "loading"
        ? i18n.t("common.loading")
        : editingQueue()
          ? t("chat.draft.saveQueued")
          : i18n.t("aui.sendMessage"),
  );
  async function stop() {
    const root = conversation();
    const message = active();
    if (!root || !message || stopping()) return;
    setStopping(true);
    try {
      await api.stopConversation(root);
    } catch (cause) {
      reportSubmissionNotice(
        props.sessionId,
        cause instanceof Error ? cause.message : String(cause),
        { content: message.content, jobId: message.jobId },
      );
    } finally {
      setStopping(false);
    }
  }
  const [dragActive, setDragActive] = createSignal(false);
  const inputRef = elementRef<HTMLInputElement>(null);
  const modelTriggerRef = elementRef<HTMLButtonElement>(null);
  const [shortcutIndex, setShortcutIndex] = createSignal(0);
  const [shortcutDismissed, setShortcutDismissed] = createSignal("");
  const shortcuts = createMemo(() => {
    const match = /(?:^|\s)\/([^\s/]*)$/.exec(input().text);
    if (!match || input().text === shortcutDismissed()) return [];
    const query = match[1].toLowerCase();
    const available = [
      {
        key: "attach",
        label: i18n.t("chat.composer.attachImage"),
        enabled: canAttach(),
        action: () => inputRef.current?.click(),
      },
      {
        key: "model",
        label: i18n.t("chat.executionSettings"),
        enabled: !inputDisabled() && !!props.selection?.llmProfile,
        action: () => modelTriggerRef.current?.click(),
      },
    ];
    return available.filter(
      ({ key, label, enabled }) =>
        enabled && (key.startsWith(query) || label.toLowerCase().includes(query)),
    );
  });
  function runShortcut(shortcut: ReturnType<typeof shortcuts>[number]) {
    changeText(input().text.replace(/(?:^|\s)\/[^\s/]*$/, ""));
    shortcut.action();
  }

  return (
    <Composer class="max-w-none space-y-2">
      <Show when={hasQueued() || unfinished().some((job) => job.id !== confirmed()?.id)}>
        <Loading fallback={null}>
          <ComposerJobList draft={draft} />
        </Loading>
      </Show>
      <form
        class="aui-composer-root relative"
        onSubmit={(event) => {
          event.preventDefault();
          if (!sendDisabled()) void send();
        }}
        onDragOver={(event) => {
          if (!event.dataTransfer || !event.dataTransfer.types.includes("Files")) return;
          event.preventDefault();
          event.dataTransfer.dropEffect = canAttach() ? "copy" : "none";
          if (canAttach()) setDragActive(true);
        }}
        onDragLeave={(event) => {
          if (
            event.relatedTarget instanceof Node &&
            event.currentTarget.contains(event.relatedTarget)
          )
            return;
          setDragActive(false);
        }}
        onDrop={(event) => {
          setDragActive(false);
          if (!event.dataTransfer || !event.dataTransfer.files.length) return;
          event.preventDefault();
          selectImages(imageFiles(event.dataTransfer.files));
        }}
      >
        <ComposerBar
          dragActive={dragActive() && canAttach()}
          class="border-border shadow-sm focus-within:border-ring/50"
        >
          <ComposerAttachments class="aui-composer-attachments px-2 pt-1 empty:hidden">
            <Show when={images().length > 0 || pendingImages().length > 0}>
              <Loading fallback={null}>
                <AttachmentTiles draft={draft} />
              </Loading>
            </Show>
          </ComposerAttachments>
          {uncertain() && (
            <div role="alert" class="text-xs text-destructive">
              {t("chat.draft.uncertain")}{" "}
              <Button
                type="button"
                size="xs"
                variant="ghost"
                disabled={busy()}
                onClick={() => void reconcileSubmission()}
              >
                {t("chat.draft.reconcile")}
              </Button>
            </div>
          )}
          {queueError() && (
            <div role="alert" class="text-xs text-destructive">
              {queueError()}
              <Button type="button" size="xs" variant="ghost" onClick={() => void refreshQueue()}>
                {t("common.retry")}
              </Button>
            </div>
          )}
          {(error() || conflict()) && (
            <div role="alert" class="text-xs text-destructive">
              {error()}{" "}
              {!uncertain() && (
                <Button
                  type="button"
                  size="xs"
                  variant="ghost"
                  disabled={busy() || (conflict() && uploading() > 0)}
                  onClick={() =>
                    !ready()
                      ? window.location.reload()
                      : conflict()
                        ? void loadSaved()
                        : (void flush().catch(() => {}),
                          void refreshQueue(),
                          void refreshUnfinished())
                  }
                >
                  {!ready()
                    ? t("common.reload")
                    : conflict()
                      ? t("chat.draft.loadSaved")
                      : t("common.retry")}
                </Button>
              )}
            </div>
          )}
          <textarea
            id="mutable-job-input"
            data-slot="composer-input"
            class="aui-composer-input field-sizing-content max-h-48 min-h-11 w-full resize-none overflow-y-auto bg-transparent px-3 py-2 text-base leading-6 caret-primary outline-none placeholder:text-muted-foreground"
            placeholder={i18n.t("aui.composerPlaceholder")}
            aria-label={i18n.t("aui.composerLabel")}
            rows={1}
            value={input().text}
            disabled={inputDisabled()}
            onInput={(event) => {
              setShortcutIndex(0);
              changeText(event.currentTarget.value);
            }}
            onKeyDown={(event) => {
              if (event.isComposing || event.keyCode === 229) return;
              if (shortcuts().length && !event.ctrlKey && !event.metaKey && !event.altKey) {
                if (event.key === "Escape") {
                  event.preventDefault();
                  setShortcutDismissed(input().text);
                } else if (event.key === "ArrowDown" || event.key === "ArrowUp") {
                  event.preventDefault();
                  const count = shortcuts().length;
                  setShortcutIndex(
                    (index) => (index + (event.key === "ArrowDown" ? 1 : count - 1)) % count,
                  );
                } else if (event.key === "Enter" && !event.shiftKey) {
                  const shortcut = shortcuts()[shortcutIndex()];
                  if (shortcut) {
                    event.preventDefault();
                    runShortcut(shortcut);
                    return;
                  }
                }
              }
              if (event.key === "Enter" && !event.shiftKey && !event.altKey) {
                event.preventDefault();
                if (!event.repeat && !sendDisabled()) event.currentTarget.form?.requestSubmit();
              }
            }}
            onPaste={paste}
          />
          <input
            ref={bindElementRef(inputRef)}
            type="file"
            accept={IMAGE_TYPES.join(",")}
            multiple
            hidden
            onChange={(event) => {
              selectImages(Array.from(event.target.files ?? []));
              event.target.value = "";
            }}
          />
          {pendingImages().some((item) => item.failed) && (
            <div role="alert" class="text-xs text-destructive">
              {t("chat.draft.uploadFailed")}{" "}
              <Button
                type="button"
                size="xs"
                variant="ghost"
                disabled={busy() || uploading() > 0}
                onClick={retryUploads}
              >
                {t("chat.draft.retryUpload")}
              </Button>
            </div>
          )}
          <ComposerToolbar class="gap-2">
            <ComposerAttachButton
              aria-label={i18n.t("chat.composer.attachImage")}
              title={i18n.t("chat.composer.attachImage")}
              class="shrink-0"
              disabled={!canAttach()}
              onClick={() => inputRef.current?.click()}
            />
            <ComposerSelectors composer={props} draft={draft} trigger={modelTriggerRef} />
            <ComposerActions class="ms-auto min-w-0 shrink-0">
              <ComposerSend
                mode={actionMode()}
                stopLabel={i18n.t("common.stop")}
                type={actionMode() === "send" ? "submit" : "button"}
                idle={actionMode() === "send" && sendDisabled()}
                disabled={actionMode() === "loading" || (actionMode() === "send" && sendDisabled())}
                onClick={() => {
                  if (actionMode() === "stop") void stop();
                }}
                aria-label={actionLabel()}
                title={actionLabel()}
              />
            </ComposerActions>
          </ComposerToolbar>
        </ComposerBar>
        <Show when={shortcuts().length}>
          <div class="absolute inset-x-2 bottom-full z-20 mb-2 grid gap-1 rounded-lg border border-border bg-popover p-1 shadow-md">
            <For each={shortcuts()} keyed={(shortcut) => shortcut.key}>
              {(shortcut, index) => (
                <button
                  type="button"
                  class={[
                    "rounded px-3 py-2 text-left text-sm hover:bg-muted focus-visible:outline-2",
                    index() === shortcutIndex() ? "bg-muted" : "",
                  ]
                    .filter(Boolean)
                    .join(" ")}
                  onClick={() => runShortcut(shortcut())}
                >
                  /{shortcut().key} · {shortcut().label}
                </button>
              )}
            </For>
          </div>
        </Show>
      </form>
    </Composer>
  );
}
