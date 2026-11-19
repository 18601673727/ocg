import { LoaderCircle, X } from "@lucide/solid";
import type { JSX } from "@solidjs/web";
import { Show } from "solid-js";
import { useI18n } from "@/components/ocg/i18n";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogTitle, DialogTrigger } from "@/components/ui/dialog";

export function AttachmentTile(props: {
  src: string;
  name: string;
  isComposer?: boolean;
  isUploading?: boolean;
  isError?: boolean;
  errorMessage?: string;
  remove?: JSX.Element;
}) {
  return (
    <div class="group relative inline-flex items-center gap-2 rounded-lg border border-border bg-background px-2 py-1">
      <Dialog>
        <DialogTrigger
          render={
            <button
              type="button"
              class="flex min-w-0 items-center gap-2 rounded text-left text-xs focus-visible:outline-2"
              title={props.name}
            >
              <img alt="" src={props.src} class="size-8 rounded object-cover" />
              <span class="max-w-36 truncate">{props.name}</span>
            </button>
          }
        />
        <DialogContent class="sm:max-w-[min(64rem,calc(100vw-2rem))]">
          <DialogTitle>{props.name}</DialogTitle>
          <img src={props.src} alt={props.name} class="max-h-[65dvh] w-full object-contain" />
        </DialogContent>
      </Dialog>
      <Show when={props.isUploading}>
        <LoaderCircle class="size-4 motion-safe:animate-spin" />
      </Show>
      <Show when={props.isError && props.errorMessage}>
        <span role="alert" class="text-xs text-destructive">
          {props.errorMessage}
        </span>
      </Show>
      {props.remove}
    </div>
  );
}

export function AttachmentTileRemove(props: {
  name: string;
  disabled?: boolean;
  onClick: () => void;
}) {
  const { t } = useI18n();
  return (
    <Button
      type="button"
      aria-label={`${t("common.remove")} ${props.name}`}
      variant="ghost"
      size="icon-xs"
      disabled={props.disabled}
      onClick={props.onClick}
    >
      <X />
    </Button>
  );
}
