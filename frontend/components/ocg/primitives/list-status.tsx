import { Inbox, LoaderCircle } from "@lucide/solid";

export function ListStatus(props: { message: string; loading?: boolean }) {
  return (
    <div role="status" class="flex flex-col items-center gap-3 px-4 py-10 text-center">
      <span
        aria-hidden="true"
        class="grid size-10 place-items-center rounded-xl bg-muted text-muted-foreground"
      >
        {(props.loading === undefined ? false : props.loading) ? (
          <LoaderCircle class="size-5 motion-safe:animate-spin" />
        ) : (
          <Inbox class="size-5" />
        )}
      </span>
      <p class="text-sm text-muted-foreground">{props.message}</p>
    </div>
  );
}
