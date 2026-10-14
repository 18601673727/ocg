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
  registerItem: (index: number, ref: HTMLButtonElement | null) => void;
  getItemCount: () => number;
  focusItem: (index: number) => void;
};

const DropdownMenuContext = React.createContext<DropdownMenuContextValue | null>(null);

function useDropdownMenu() {
  const context = React.useContext(DropdownMenuContext);
  if (!context) throw new Error("Dropdown components must be used within DropdownMenu");
  return context;
}

export function DropdownMenu({ children, open: controlledOpen, onOpenChange }: DropdownMenuProps) {
  const [internalOpen, setInternalOpen] = React.useState(false);
  const [itemRefs, setItemRefs] = React.useState<Map<number, HTMLButtonElement | null>>(new Map());
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

  const registerItem = React.useCallback((index: number, ref: HTMLButtonElement | null) => {
    setItemRefs(prev => {
      const next = new Map(prev);
      if (ref) {
        next.set(index, ref);
      } else {
        next.delete(index);
      }
      return next;
    });
  }, []);

  const getItemCount = React.useCallback(() => itemRefs.size, [itemRefs.size]);

  const focusItem = React.useCallback((index: number) => {
    const item = itemRefs.get(index);
    if (item && !item.disabled) {
      item.focus();
    }
  }, [itemRefs]);

  const contextValue = React.useMemo(
    () => ({ open, setOpen, triggerRef, registerItem, getItemCount, focusItem }),
    [open, setOpen, registerItem, getItemCount, focusItem]
  );

  return (
    <DropdownMenuContext.Provider value={contextValue}>
      {children}
    </DropdownMenuContext.Provider>
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
  function DropdownMenuContent({ children, align = "end", className, onEscapeKeyDown, onInteractOutside }, ref) {
    const { open, setOpen, triggerRef, focusItem, getItemCount } = useDropdownMenu();
    const contentRef = React.useRef<HTMLDivElement>(null);
    const [focusedIndex, setFocusedIndex] = React.useState(0);

    React.useImperativeHandle(ref, () => contentRef.current!);

    // Focus first item when menu opens
    React.useEffect(() => {
      if (open) {
        // Small delay to ensure items are registered
        const timer = setTimeout(() => {
          focusItem(0);
          setFocusedIndex(0);
        }, 0);
        return () => clearTimeout(timer);
      }
    }, [open, focusItem]);

    React.useEffect(() => {
      if (!open) return;

      const handleKeyDown = (event: KeyboardEvent) => {
        const itemCount = getItemCount();
        
        if (event.key === "Escape") {
          // The consumer sees the event first and may cancel the dismissal;
          // only the uncancelled Escape closes the menu and restores focus.
          onEscapeKeyDown?.(event);
          if (!event.defaultPrevented) {
            event.preventDefault();
            setOpen(false);
            // Restore focus to trigger
            triggerRef.current?.focus();
          }
        } else if (event.key === "ArrowDown") {
          event.preventDefault();
          const nextIndex = (focusedIndex + 1) % itemCount;
          focusItem(nextIndex);
          setFocusedIndex(nextIndex);
        } else if (event.key === "ArrowUp") {
          event.preventDefault();
          const prevIndex = (focusedIndex - 1 + itemCount) % itemCount;
          focusItem(prevIndex);
          setFocusedIndex(prevIndex);
        } else if (event.key === "Home") {
          event.preventDefault();
          focusItem(0);
          setFocusedIndex(0);
        } else if (event.key === "End") {
          event.preventDefault();
          const lastIndex = itemCount - 1;
          focusItem(lastIndex);
          setFocusedIndex(lastIndex);
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
    }, [open, setOpen, triggerRef, focusedIndex, focusItem, getItemCount, onEscapeKeyDown, onInteractOutside]);

    if (!open) return null;

    // Assign indices to menu items
    let itemIndex = 0;
    const enhancedChildren = React.Children.map(children, (child) => {
      if (React.isValidElement(child) && child.type === DropdownMenuItem) {
        const currentIndex = itemIndex++;
        return React.cloneElement(child as React.ReactElement<{ itemIndex?: number }>, {
          itemIndex: currentIndex,
        });
      }
      return child;
    });

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
        {enhancedChildren}
      </div>
    );
  },
);

type DropdownMenuItemProps = {
  children: React.ReactNode;
  onSelect?: () => void;
  disabled?: boolean;
  className?: string;
  itemIndex?: number;
};

export const DropdownMenuItem = React.forwardRef<HTMLButtonElement, DropdownMenuItemProps>(
  function DropdownMenuItem({ children, onSelect, disabled, className, itemIndex }, ref) {
    const { setOpen, triggerRef, registerItem } = useDropdownMenu();
    const localRef = React.useRef<HTMLButtonElement>(null);

    React.useImperativeHandle(ref, () => localRef.current!);

    React.useEffect(() => {
      if (typeof itemIndex === "number") {
        registerItem(itemIndex, localRef.current);
        return () => {
          registerItem(itemIndex, null);
        };
      }
    }, [itemIndex, registerItem]);

    const handleSelect = () => {
      if (disabled) return;
      onSelect?.();
      setOpen(false);
      // Restore focus to trigger after selection
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
