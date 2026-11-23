import { createEffect, createSignal, For, Show } from "solid-js";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import type * as Api from "../contracts/api";
import { useI18n } from "../i18n";
import type { ApiClient } from "../runtime/api-client";

export function ArchivedConversations(props: {
  api: ApiClient;
  onClose: () => void;
  onOpenConversation: (conversation: Api.Conversation) => Promise<boolean>;
  onRestore: (conversation: Api.Conversation) => Promise<void>;
}) {
  const i18n = useI18n();
  const [rows, setRows] = createSignal<Api.Conversation[]>([]);
  const [cursor, setCursor] = createSignal<string | null>();
  const [loading, setLoading] = createSignal(false);
  const [loadFailed, setLoadFailed] = createSignal(false);
  const [restoring, setRestoring] = createSignal<string>();
  const [restoreError, setRestoreError] = createSignal<string>();
  const request = new AbortController();

  const load = async () => {
    const { signal } = request;
    const before = cursor();
    if (signal.aborted || loading() || restoring() || before === null) return;
    setLoading(true);
    setLoadFailed(false);
    try {
      const page = await props.api.readConversations({ archived: true, before, signal });
      if (signal.aborted) return;
      setRows((current) => [...current, ...page.conversations]);
      setCursor(page.next_before);
    } catch {
      if (!signal.aborted) setLoadFailed(true);
    } finally {
      if (!signal.aborted) setLoading(false);
    }
  };

  createEffect(
    () => [],
    () => {
      void load();
      return () => request.abort();
    },
  );

  const restore = async (item: Api.Conversation) => {
    if (loading() || restoring()) return;
    setRestoring(item.root_job);
    setRestoreError(undefined);
    try {
      await props.onRestore(item);
      setRows((current) => current.filter((row) => row.root_job !== item.root_job));
    } catch (error) {
      setRestoreError(
        i18n.t("sidebar.restoreChatFailed", {
          error: error instanceof Error ? error.message : String(error),
        }),
      );
    } finally {
      setRestoring(undefined);
    }
  };

  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open && !restoring()) props.onClose();
      }}
    >
      <DialogContent class="flex max-h-[min(36rem,85dvh)] flex-col" showCloseButton={!restoring()}>
        <DialogHeader class="pr-8">
          <DialogTitle class="normal-case tracking-normal">
            {i18n.t("sidebar.archivedChats")}
          </DialogTitle>
          <DialogDescription>{i18n.t("sidebar.archivedChatsBody")}</DialogDescription>
        </DialogHeader>
        <div class="ocg-subtle-scrollbar min-h-0 overflow-y-auto">
          <ul class="divide-y divide-border">
            <For each={rows()} keyed={(item) => item.root_job}>
              {(item) => (
                <li class="flex min-w-0 items-center gap-2 py-1">
                  <Button
                    type="button"
                    variant="ghost"
                    class="min-w-0 flex-1 justify-start tracking-normal normal-case"
                    disabled={Boolean(restoring())}
                    title={item().title}
                    onClick={() => {
                      void props.onOpenConversation(item()).then((opened) => {
                        if (opened && !request.signal.aborted && !restoring()) props.onClose();
                      });
                    }}
                  >
                    <span class="truncate">{item().title || i18n.t("sidebar.newChat")}</span>
                  </Button>
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    disabled={loading() || Boolean(restoring())}
                    onClick={() => void restore(item())}
                  >
                    {i18n.t(
                      restoring() === item().root_job
                        ? "sidebar.restoringChat"
                        : "sidebar.restoreChat",
                    )}
                  </Button>
                </li>
              )}
            </For>
          </ul>
          <Show when={!rows().length && cursor() === null}>
            <p class="py-4 text-sm text-muted-foreground">{i18n.t("sidebar.archivedChatsEmpty")}</p>
          </Show>
          <Show when={loadFailed()}>
            <p role="alert" class="py-2 text-sm text-destructive">
              {i18n.t("sidebar.archivedChatsFailed")}
            </p>
          </Show>
          <Show when={restoreError()}>
            <p role="alert" class="py-2 text-sm text-destructive">
              {restoreError()}
            </p>
          </Show>
          <Show when={loading()}>
            <p role="status" class="py-2 text-sm text-muted-foreground">
              {i18n.t("common.loading")}
            </p>
          </Show>
          <Show when={!loading() && cursor() !== null}>
            <Button
              type="button"
              variant="ghost"
              size="sm"
              class="mt-2"
              disabled={Boolean(restoring())}
              onClick={() => void load()}
            >
              {i18n.t(loadFailed() ? "common.retry" : "sidebar.archivedChatsMore")}
            </Button>
          </Show>
        </div>
      </DialogContent>
    </Dialog>
  );
}
