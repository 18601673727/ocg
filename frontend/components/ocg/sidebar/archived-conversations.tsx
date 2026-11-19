import { Archive } from "@lucide/solid";
import { createSignal, For, onCleanup, Show } from "solid-js";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import type * as Api from "../contracts/api";
import { useI18n } from "../i18n";
import type { ApiClient } from "../runtime/api-client";

export function ArchivedConversations(props: {
  api: ApiClient;
  compact: boolean;
  onOpenConversation: (conversation: Api.Conversation) => Promise<boolean>;
  onRestore: (conversation: Api.Conversation) => Promise<void>;
}) {
  const i18n = useI18n();
  const [open, setOpen] = createSignal(false);
  const [rows, setRows] = createSignal<Api.Conversation[]>([]);
  const [cursor, setCursor] = createSignal<string | null>();
  const [loading, setLoading] = createSignal(false);
  const [loadFailed, setLoadFailed] = createSignal(false);
  const [restoring, setRestoring] = createSignal<string>();
  const [restoreError, setRestoreError] = createSignal<string>();
  let request: AbortController | undefined;
  onCleanup(() => request?.abort());

  const load = async () => {
    const signal = request?.signal;
    const before = cursor();
    if (!signal || loading() || restoring() || before === null) return;
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

  const changeOpen = (value: boolean) => {
    if (restoring()) return;
    setOpen(value);
    request?.abort();
    if (!value) return;
    request = new AbortController();
    setRows([]);
    setCursor(undefined);
    setLoading(false);
    setRestoreError(undefined);
    void load();
  };

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
    <Dialog open={open()} onOpenChange={changeOpen}>
      <DialogTrigger
        render={
          <Button
            type="button"
            variant="ghost"
            size={props.compact ? "icon" : "sm"}
            class={
              props.compact
                ? "size-9 rounded-lg text-muted-foreground"
                : "h-9 w-full justify-start gap-2.5 rounded-lg px-3 text-sm font-normal tracking-normal text-muted-foreground normal-case"
            }
            aria-label={i18n.t("sidebar.archivedChats")}
            title={props.compact ? i18n.t("sidebar.archivedChats") : undefined}
          >
            <Archive class="size-4" aria-hidden="true" />
            {!props.compact && i18n.t("sidebar.archivedChats")}
          </Button>
        }
      />
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
                        if (opened) changeOpen(false);
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
