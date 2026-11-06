import { Inbox, LoaderCircle } from "lucide-react";

export function ListStatus({ message, loading = false }: { message: string; loading?: boolean }) {
  return (
    <div role="status" className="flex flex-col items-center gap-3 px-4 py-10 text-center">
      <span
        aria-hidden="true"
        className="grid size-10 place-items-center rounded-xl bg-muted text-muted-foreground"
      >
        {loading ? (
          <LoaderCircle className="size-5 motion-safe:animate-spin" />
        ) : (
          <Inbox className="size-5" />
        )}
      </span>
      <p className="text-sm text-muted-foreground">{message}</p>
    </div>
  );
}
