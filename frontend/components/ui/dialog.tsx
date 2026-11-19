import { XIcon } from "@lucide/solid";
import type { JSX } from "@solidjs/web";
import {
  createContext,
  createEffect,
  createSignal,
  createUniqueId,
  onCleanup,
  Show,
  useContext,
} from "solid-js";
import { composeButton, splitProps, type View } from "@/lib/ui-state";
import { cn } from "@/lib/utils";
import { Button } from "./button";

type DialogState = {
  open: () => boolean;
  setOpen: (open: boolean) => void;
  title: string;
  description: string;
};
const DialogContext = createContext<DialogState>();

export function Dialog(props: {
  open?: boolean;
  defaultOpen?: boolean;
  onOpenChange?: (open: boolean) => void;
  children: View;
}) {
  const [internal, setInternal] = createSignal(props.defaultOpen ?? false);
  const id = createUniqueId();
  const state: DialogState = {
    open: () => props.open ?? internal(),
    setOpen(open) {
      if (props.open === undefined) setInternal(open);
      props.onOpenChange?.(open);
    },
    title: `${id}-title`,
    description: `${id}-description`,
  };
  return <DialogContext value={state}>{props.children}</DialogContext>;
}

export function DialogTrigger(props: {
  render?: View;
  children?: View;
  disabled?: boolean;
  title?: string;
}) {
  const state = useContext(DialogContext);
  function open(event: globalThis.MouseEvent) {
    if (!event.defaultPrevented && !props.disabled) state?.setOpen(true);
  }
  if (props.render) {
    const trigger = composeButton(props.render, {
      children: props.children,
      attrs: { "aria-haspopup": "dialog", "data-slot": "dialog-trigger" },
      onClick: open,
    });
    createEffect(
      () => [props.disabled, props.title] as const,
      ([disabled, title]) => {
        if (trigger instanceof HTMLButtonElement && disabled !== undefined)
          trigger.disabled = disabled;
        if (trigger instanceof HTMLElement && title !== undefined) trigger.title = title;
      },
    );
    return trigger;
  }
  return (
    <Button
      data-slot="dialog-trigger"
      aria-haspopup="dialog"
      disabled={props.disabled}
      title={props.title}
      onClick={open}
    >
      {props.children}
    </Button>
  );
}

export function DialogClose(props: { render?: View; children?: View }) {
  const state = useContext(DialogContext);
  function close(event: globalThis.MouseEvent) {
    if (!event.defaultPrevented) state?.setOpen(false);
  }
  if (props.render)
    return composeButton(props.render, { children: props.children, onClick: close });
  return (
    <Button data-slot="dialog-close" onClick={close}>
      {props.children}
    </Button>
  );
}

type FocusTarget = HTMLElement | boolean | null | (() => HTMLElement | boolean | null);
function focus(target: FocusTarget | undefined) {
  const element = typeof target === "function" ? target() : target;
  if (element instanceof HTMLElement) element.focus();
}

export function DialogContent(
  props: JSX.IntrinsicElements["dialog"] & {
    showCloseButton?: boolean;
    closeButtonClassName?: string;
    finalFocus?: FocusTarget;
    initialFocus?: FocusTarget;
  },
) {
  const state = useContext(DialogContext);
  const [local, rest] = splitProps(props, [
    "children",
    "class",
    "showCloseButton",
    "closeButtonClassName",
    "finalFocus",
    "initialFocus",
  ]);
  let dialog!: HTMLDialogElement;
  createEffect(
    () => state?.open() ?? false,
    (open) => {
      if (open && !dialog.open) {
        dialog.showModal();
        queueMicrotask(() => {
          if (dialog.open) focus(local.initialFocus);
        });
      } else if (!open && dialog.open) {
        dialog.close();
        focus(local.finalFocus);
      }
    },
  );
  onCleanup(() => {
    if (dialog.open) dialog.close();
  });
  return (
    <dialog
      {...rest}
      ref={dialog}
      aria-labelledby={state?.title}
      aria-describedby={state?.description}
      data-slot="dialog-content"
      class={cn(
        "fixed m-0 max-h-[calc(100dvh-2rem)] top-1/2 left-1/2 z-50 w-full max-w-[calc(100%-2rem)] -translate-x-1/2 -translate-y-1/2 gap-6 rounded-lg bg-popover p-6 text-sm text-popover-foreground shadow-md ring-1 ring-foreground/10 outline-none grid backdrop:bg-black/20 backdrop:backdrop-blur-sm sm:max-w-md [&:not([open])]:hidden",
        local.class,
      )}
      onCancel={(event) => {
        event.preventDefault();
        state?.setOpen(false);
      }}
      onClick={(event) => {
        if (event.target !== dialog) return;
        const bounds = dialog.getBoundingClientRect();
        if (
          event.clientX < bounds.left ||
          event.clientX > bounds.right ||
          event.clientY < bounds.top ||
          event.clientY > bounds.bottom
        )
          state?.setOpen(false);
      }}
    >
      <Show when={state?.open()}>
        {local.children}
        <Show when={local.showCloseButton ?? true}>
          <Button
            type="button"
            data-slot="dialog-close"
            variant="ghost"
            size="icon-sm"
            class={cn("absolute top-4 right-4 text-muted-foreground", local.closeButtonClassName)}
            onClick={() => state?.setOpen(false)}
          >
            <XIcon />
            <span class="sr-only">Close</span>
          </Button>
        </Show>
      </Show>
    </dialog>
  );
}

export function DialogHeader(props: JSX.IntrinsicElements["div"]) {
  return (
    <div {...props} data-slot="dialog-header" class={cn("flex flex-col gap-2", props.class)} />
  );
}
export function DialogFooter(props: JSX.IntrinsicElements["div"] & { showCloseButton?: boolean }) {
  const [local, rest] = splitProps(props, ["children", "showCloseButton"]);
  return (
    <div
      {...rest}
      data-slot="dialog-footer"
      class={cn("flex flex-col-reverse gap-2 sm:flex-row sm:justify-end", props.class)}
    >
      {local.children}
      <Show when={local.showCloseButton}>
        <DialogClose render={<Button variant="outline" />}>Close</DialogClose>
      </Show>
    </div>
  );
}
export function DialogTitle(props: JSX.IntrinsicElements["h2"]) {
  const state = useContext(DialogContext);
  return (
    <h2
      id={state?.title}
      {...props}
      data-slot="dialog-title"
      class={cn(
        "font-heading text-lg leading-none font-semibold tracking-wider uppercase",
        props.class,
      )}
    />
  );
}
export function DialogDescription(props: JSX.IntrinsicElements["p"]) {
  const state = useContext(DialogContext);
  return (
    <p
      id={state?.description}
      {...props}
      data-slot="dialog-description"
      class={cn(
        "mt-0.5 text-sm leading-relaxed text-muted-foreground *:[a]:underline",
        props.class,
      )}
    />
  );
}
