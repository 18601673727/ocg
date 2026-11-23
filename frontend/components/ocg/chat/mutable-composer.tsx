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
import { IMAGE_TYPES, imageFiles } from "./composer/attachments";
import { ComposerSelectors } from "./composer/composer-selectors";
import { type ComposerProps, createComposerDraft } from "./composer/draft-state";

export { bindComposerContext, contextForSession, MUTABLE_CONTEXT_KEY } from "./composer/context";
export { confirmMutableComposerNavigation } from "./composer/draft-state";

const AttachmentTiles = lazy(() => import("./composer/attachment-tiles"));
const ComposerJobList = lazy(() => import("./composer/job-list"));

export function MutableComposer(props: ComposerProps) {
  const draft = createComposerDraft(props);
  const {
    input,
    confirmed,
    statusText,
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
              if (event.isComposing || !shortcuts().length) return;
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
                }
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
              Image upload failed.{" "}
              <Button
                type="button"
                size="xs"
                variant="ghost"
                disabled={busy() || uploading() > 0}
                onClick={retryUploads}
              >
                Retry upload
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
            <ComposerActions class="shrink-0">
              {editingQueue() ? (
                <Button type="submit" size="sm" disabled={sendDisabled()}>
                  Save queued Job
                </Button>
              ) : (
                <ComposerSend
                  type="submit"
                  idle={sendDisabled()}
                  disabled={sendDisabled()}
                  aria-label={i18n.t("aui.sendMessage")}
                  title={i18n.t("aui.sendMessage")}
                />
              )}
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

      <p role="status" class="text-xs text-muted-foreground">
        {pendingImages().length ? "Images not yet saved" : statusText()}
        {confirmed() ? ` · revision ${confirmed()!.revision}` : ""}
        {uploading() ? ` · uploading ${uploading()}` : ""}
      </p>
      {uncertain() && (
        <div role="alert" class="text-xs text-destructive">
          Submission outcome is uncertain.{" "}
          <Button
            type="button"
            size="xs"
            variant="ghost"
            disabled={busy()}
            onClick={() => void reconcileSubmission()}
          >
            Reconcile submission
          </Button>
        </div>
      )}
      {queueError() && (
        <div role="alert" class="text-xs text-destructive">
          {queueError()}
          <Button type="button" size="xs" variant="ghost" onClick={() => void refreshQueue()}>
            Retry
          </Button>
        </div>
      )}
      {error() && (
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
                    : (void flush().catch(() => {}), void refreshQueue(), void refreshUnfinished())
              }
            >
              {!ready() ? "Reload" : conflict() ? "Load saved version" : "Retry"}
            </Button>
          )}
        </div>
      )}
    </Composer>
  );
}
