"use client";

import * as React from "react";
import { cn } from "@/lib/utils";

type DropdownMenuProps = {
  children: React.ReactNode;
  open?: boolean;
  onOpenChange?: (open: boolean) => void;
};

type DropdownMenuContextValue = {
  open: boolean;
  setOpen: (open: boolean) => void;
  triggerRef: React.RefObject<HTMLElement | null>;
};

export type MenuItemState = { index: number; disabled: boolean };

export type MenuNavigationKey = "ArrowUp" | "ArrowDown" | "Home" | "End";

export type MenuFocusable = { index: number; disabled: boolean; focus: () => void };

export function menuFocusables(root: HTMLElement | null): MenuFocusable[] {
  const items = root?.querySelectorAll<HTMLButtonElement>('[role="menuitem"]') ?? [];
  return [...items].map((element, index) => ({
    index,
    disabled: element.disabled,
    focus: () => element.focus(),
  }));
}

export function enabledMenuIndexes(items: readonly MenuItemState[]): number[] {
  return items
    .filter((item) => !item.disabled)
    .map((item) => item.index)
    .sort((first, second) => first - second);
}

export function menuFocusTarget(
  items: readonly MenuItemState[],
  current: number,
  key: MenuNavigationKey,
): number | null {
  const enabled = enabledMenuIndexes(items);
  if (enabled.length === 0) return null;
  if (key === "Home") return enabled[0];
  if (key === "End") return enabled[enabled.length - 1];
  const position = enabled.indexOf(current);
  if (position === -1) return key === "ArrowDown" ? enabled[0] : enabled[enabled.length - 1];
  const step = key === "ArrowDown" ? 1 : -1;
  return enabled[(position + step + enabled.length) % enabled.length];
}

const DropdownMenuContext = React.createContext<DropdownMenuContextValue | null>(null);

function useDropdownMenu() {
  const context = React.useContext(DropdownMenuContext);
  if (!context) throw new Error("Dropdown components must be used within DropdownMenu");
  return context;
}

export function DropdownMenu({ children, open: controlledOpen, onOpenChange }: DropdownMenuProps) {
  const [internalOpen, setInternalOpen] = React.useState(false);
  const triggerRef = React.useRef<HTMLElement | null>(null);
  const isControlled = controlledOpen !== undefined;
  const open = isControlled ? controlledOpen : internalOpen;

  const setOpen = React.useCallback(
    (value: boolean) => {
      if (!isControlled) setInternalOpen(value);
      onOpenChange?.(value);
    },
    [isControlled, onOpenChange],
  );

  const contextValue = React.useMemo(() => ({ open, setOpen, triggerRef }), [open, setOpen]);

  return (
    <DropdownMenuContext.Provider value={contextValue}>{children}</DropdownMenuContext.Provider>
  );
}

type DropdownMenuTriggerProps = {
  children: React.ReactNode;
  asChild?: boolean;
  className?: string;
};

export const DropdownMenuTrigger = React.forwardRef<HTMLButtonElement, DropdownMenuTriggerProps>(
  function DropdownMenuTrigger({ children, asChild, className }, ref) {
    const { open, setOpen, triggerRef } = useDropdownMenu();
    const localRef = React.useRef<HTMLButtonElement>(null);

    React.useImperativeHandle(ref, () => localRef.current!);
    React.useImperativeHandle(triggerRef, () => localRef.current as HTMLElement);

    if (asChild && React.isValidElement(children)) {
      const childProps = children.props as Record<string, unknown>;
      const clonedProps: Record<string, unknown> = {
        onClick: (e: React.MouseEvent) => {
          setOpen(!open);
          if (typeof childProps.onClick === "function") {
            childProps.onClick(e);
          }
        },
        "aria-expanded": open,
        "aria-haspopup": "menu",
        ref: localRef,
      };
      return React.cloneElement(children, clonedProps);
    }

    return (
      <button
        ref={localRef}
        type="button"
        onClick={() => setOpen(!open)}
        aria-expanded={open}
        aria-haspopup="menu"
        className={className}
      >
        {children}
      </button>
    );
  },
);

type DropdownMenuContentProps = {
  children: React.ReactNode;
  align?: "start" | "end";
  className?: string;
  onEscapeKeyDown?: (event: KeyboardEvent) => void;
  onInteractOutside?: (event: Event) => void;
};

export const DropdownMenuContent = React.forwardRef<HTMLDivElement, DropdownMenuContentProps>(
  function DropdownMenuContent(
    { children, align = "end", className, onEscapeKeyDown, onInteractOutside },
    ref,
  ) {
    const { open, setOpen, triggerRef } = useDropdownMenu();
    const contentRef = React.useRef<HTMLDivElement>(null);
    const focusedIndex = React.useRef(-1);

    React.useImperativeHandle(ref, () => contentRef.current!);

    const focusByKey = React.useCallback((key: MenuNavigationKey) => {
      const items = menuFocusables(contentRef.current);
      const target = menuFocusTarget(items, focusedIndex.current, key);
      if (target === null) return false;
      focusedIndex.current = target;
      items[target]?.focus();
      return true;
    }, []);

    const navigate = React.useRef(focusByKey);
    React.useEffect(() => {
      navigate.current = focusByKey;
    }, [focusByKey]);

    React.useEffect(() => {
      if (!open) return;
      focusedIndex.current = -1;
      const timer = setTimeout(() => {
        navigate.current("ArrowDown");
      }, 0);
      return () => clearTimeout(timer);
    }, [open]);

    React.useEffect(() => {
      if (!open) return;

      const handleKeyDown = (event: KeyboardEvent) => {
        if (event.key === "Escape") {
          onEscapeKeyDown?.(event);
          if (!event.defaultPrevented) {
            event.preventDefault();
            setOpen(false);
            triggerRef.current?.focus();
          }
        } else if (
          event.key === "ArrowDown" ||
          event.key === "ArrowUp" ||
          event.key === "Home" ||
          event.key === "End"
        ) {
          if (focusByKey(event.key)) event.preventDefault();
        }
      };

      const handleClickOutside = (event: MouseEvent | TouchEvent) => {
        if (contentRef.current && !contentRef.current.contains(event.target as Node)) {
          const isClickOnTrigger = triggerRef.current?.contains(event.target as Node);

          if (!isClickOnTrigger) {
            onInteractOutside?.(event);
            if (!event.defaultPrevented) {
              setOpen(false);
            }
          }
        }
      };

      document.addEventListener("keydown", handleKeyDown);
      document.addEventListener("mousedown", handleClickOutside);
      document.addEventListener("touchstart", handleClickOutside);

      return () => {
        document.removeEventListener("keydown", handleKeyDown);
        document.removeEventListener("mousedown", handleClickOutside);
        document.removeEventListener("touchstart", handleClickOutside);
      };
    }, [open, setOpen, triggerRef, focusByKey, onEscapeKeyDown, onInteractOutside]);

    if (!open) return null;

    return (
      <div
        ref={contentRef}
        role="menu"
        className={cn(
          "absolute z-50 min-w-[8rem] rounded-md border border-border bg-popover p-1 text-popover-foreground shadow-md",
          align === "end" ? "right-0" : "left-0",
          "top-full mt-1",
          className,
        )}
      >
        {children}
      </div>
    );
  },
);

type DropdownMenuItemProps = {
  children: React.ReactNode;
  onSelect?: () => void;
  disabled?: boolean;
  className?: string;
};

export const DropdownMenuItem = React.forwardRef<HTMLButtonElement, DropdownMenuItemProps>(
  function DropdownMenuItem({ children, onSelect, disabled, className }, ref) {
    const { setOpen, triggerRef } = useDropdownMenu();
    const localRef = React.useRef<HTMLButtonElement>(null);

    React.useImperativeHandle(ref, () => localRef.current!);

    const handleSelect = () => {
      if (disabled) return;
      onSelect?.();
      setOpen(false);
      setTimeout(() => {
        triggerRef.current?.focus();
      }, 0);
    };

    return (
      <button
        ref={localRef}
        type="button"
        role="menuitem"
        disabled={disabled}
        onClick={handleSelect}
        className={cn(
          "flex w-full items-center rounded-sm px-2 py-1.5 text-sm outline-none transition-colors",
          "hover:bg-muted focus-visible:bg-muted",
          disabled && "pointer-events-none opacity-50",
          className,
        )}
      >
        {children}
      </button>
    );
  },
);
