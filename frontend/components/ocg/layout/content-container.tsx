import { type View } from "@/lib/ui-state";
import { cn } from "@/lib/utils";
import { PageTitle } from "./page-title";

export function ContentContainer(props: { children: View; class?: string }) {
  return (
    <div
      class={cn(
        "mx-auto flex w-full max-w-7xl flex-col gap-6 px-5 pt-4 pb-8 sm:px-8 lg:px-10",
        props.class,
      )}
    >
      {props.children}
    </div>
  );
}

export function ContentHeader(props: { title: View; description?: View; actions?: View }) {
  return (
    <div class="flex flex-wrap items-end justify-between gap-4">
      <div class="min-w-0 space-y-1.5">
        <PageTitle class="font-heading text-3xl font-semibold tracking-tight">
          {props.title}
        </PageTitle>
        {props.description && (
          <p class="max-w-prose text-sm leading-6 text-muted-foreground">{props.description}</p>
        )}
      </div>
      {props.actions ? <div class="flex flex-wrap items-center gap-2">{props.actions}</div> : null}
    </div>
  );
}
